from .logger import logger
from .process_runner import process_runner

import yt.wrapper as yt
from yt.wrapper.client import YtClient
from yt.wrapper.http_helpers import get_proxy_url
from yt.common import wait, WaitFailed

from time import sleep
import copy
import random
import string

def random_string_stdlib(n=100000):
    return "".join(random.choices(tuple(string.ascii_uppercase), k=n))

random_string = random_string_stdlib

try:
    import numpy.random as np_random
    if hasattr(np_random, "choice"):
        random_string = lambda n: "".join(np_random.choice(tuple(string.ascii_uppercase), n))
except ModuleNotFoundError:
    pass

def create_client(proxy=None, config=yt.config.config, api_version=None):
    if proxy is None:
        proxy = get_proxy_url()
    if api_version is not None:
        config = copy.deepcopy(config)
        config["api_version"] = api_version
    return YtClient(proxy, config=config)

def wait_for_preload(table):
    if yt.get(table + "/@in_memory_mode") == "none":
        return

    # Polls a single attribute. Rather slow due to high tablet heartbeat period.
    # Not used, left here in case if any problems with wait_via_node_orchid show up.
    def wait_via_master():
        wait(
            lambda: yt.get(table + "/@preload_state") == "completed",
            sleep_backoff=0.3,
            timeout=120)

    # Polls multiple node orchids. Does not depend on heartbeat period.
    def wait_via_node_orchid():
        node_set = set(
            tablet["cell_leader_address"]
            for tablet
            in yt.get(table + "/@tablets"))

        def _check():
            batch_client = yt.create_batch_client()
            node_list = list(node_set)
            rsps : list[yt.batch_execution.BatchResponse] = []
            for node in node_list:
                rsps.append(batch_client.get(
                    f"//sys/cluster_nodes/{node}/orchid/tablet_slot_manager" +
                    "/memory_usage_statistics/tables" +
                    "/" + table.replace("/", "\\/")))

            batch_client.commit_batch()

            for node, rsp in zip(node_list, rsps):
                if not rsp.is_ok():
                    continue
                rsp = rsp.get_result()
                if rsp["preload_pending_store_count"] + rsp["preload_failed_store_count"] == 0:
                    node_set.discard(node)

            return len(node_set) == 0

        timeout = 120
        try:
            wait(_check, sleep_backoff=1, timeout=timeout)
        except WaitFailed:
            raise Exception(f"Chunk data did not preload in {timeout} seconds; nodes left: {node_set}")

    wait_via_node_orchid();

def mount_table(path):
    #  sys.stdout.write("Mounting table %s... " % (path))
    yt.mount_table(path, sync=True)
    wait_for_preload(path)
    #  print "done"
def unmount_table(path):
    #  sys.stdout.write("Unmounting table %s... " % (path))
    yt.unmount_table(path, sync=True)
    #  print "done"
def freeze_table(path):
    #  sys.stdout.write("Freezing table %s... " % (path))
    yt.freeze_table(path, sync=True)
    #  print "done"
def unfreeze_table(path):
    #  sys.stdout.write("Unfreezing table %s... " % (path))
    yt.unfreeze_table(path, sync=True)
    #  print "done"

def sync_flush_table(path):
    freeze_table(path)
    unfreeze_table(path)

def sync_switch_bundle_options(tablet_cell_bundle, account, acl):
    assert tablet_cell_bundle not in ["sys", "default"]

    bundle_path = "//sys/tablet_cell_bundles/{}".format(tablet_cell_bundle)

    tablet_cell_ids = yt.get("{}/@tablet_cell_ids".format(bundle_path))
    def _check_cells_are_healthy():
        for cell_id in tablet_cell_ids:
            if yt.get("//sys/tablet_cells/{}/@health".format(cell_id)) != "good":
                return False
        return True

    wait(_check_cells_are_healthy)

    logger.info("Switching '{}' bundle's options (new account: '{}', new acl: '{}')".format(
        tablet_cell_bundle, account, acl))
    yt.set("{}/@options/changelog_account".format(bundle_path), account)
    yt.set("{}/@options/snapshot_account".format(bundle_path), account)
    yt.set("{}/@options/changelog_acl".format(bundle_path), acl)
    yt.set("{}/@options/snapshot_acl".format(bundle_path), acl)

    # TODO(akozhikhov): add checks for each tablet cell.

    wait(_check_cells_are_healthy)

def sync_compact_table(path, compaction_path, get_chunks, get_remaining_chunks):
    logger.info("Compacting table %s", path)
    chunks = get_chunks(path)
    yt.set(path + "/@" + compaction_path, 1)
    yt.remount_table(path)
    iter = 0
    while True:
        new_chunks = get_chunks(path)
        remaining_chunks = get_remaining_chunks(chunks, new_chunks)
        if not remaining_chunks:
            return
        sleep(0.5)
        iter += 1
        if iter % 10 == 0:
            logger.info("Still compacting table %s, iter = %s, %s of %s chunks remaining",
                path, iter, len(remaining_chunks), len(chunks))
            if iter >= 100 and len(remaining_chunks) < 5:
                logger.info("Chunks yet to compact: %s", ", ".join(remaining_chunks))

def get_chunk_views(path):
    root_chunk_list_id = yt.get(path + "/@chunk_list_id")
    root_tree = yt.get("#{}/@tree".format(root_chunk_list_id))

    chunk_views = set()

    def _find_chunk_views(tree):
        type = tree.attributes.get("type")
        if type == "chunk_list":
            for child in tree:
                _find_chunk_views(child)
        elif type == "chunk_view":
            chunk_views.add(tree.attributes.get("id"))

    _find_chunk_views(root_tree)
    return chunk_views

def compact_chunk_views(path):
    if get_chunk_views(path):
        logger.info("Table %s contains chunk views, will compact", path)
        sync_compact_table(
            path,
            compaction_path="forced_chunk_view_compaction_revision",
            get_chunks=lambda table_path: get_chunk_views(table_path),
            get_remaining_chunks=lambda _, after: after)

def remove_existing(paths, force):
    for path in paths:
        if yt.exists(path):
            if force:
                yt.remove(path)
            else:
                raise Exception("Table %s already exists. Use --force" % path)

def get_tablet_sizes(tablet_size_table, tablet_count, min_index=0):
    # XXX: order by
    tablets_info = list(yt.select_rows(
        "* from [%s] where tablet_index >= %s and tablet_index < %s" % (
            tablet_size_table, min_index, tablet_count,
        )
    ))
    tablets_info.sort(key=lambda x: int(x["tablet_index"]))
    return [tablet_info["size"] for tablet_info in tablets_info]

def equal_table_rows(columns, lhs, rhs):
    if (lhs == None) + (rhs == None) > 0:
        return (lhs == None) == (rhs == None)
    for c in columns:
        if ((c in lhs) != (c in rhs)) or ((c in lhs) and (lhs[c] != rhs[c])):
            return False
    return True

def is_table_empty(table):
    assert yt.exists(table)
    return yt.get("{}/@row_count".format(table)) == 0

def run_operation_and_wrap_error(op: yt.Operation, error_message):
    try:
        op.wait()
    except yt.YtError as e:
        raise yt.YtError(f"{error_message} operation failed: {op.url}", inner_errors=[e]) from None
