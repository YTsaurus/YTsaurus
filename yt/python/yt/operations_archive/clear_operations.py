from .queues import (Timer, ThreadSafeCounter, NonBlockingQueue,
                     queue_worker, run_workers, run_queue_workers,
                     run_batching_queue_workers, wait_for_queue)

from yt.common import date_string_to_timestamp_mcs, datetime_to_string, update, utcnow
from yt.wrapper.config import get_config, set_option
from yt.wrapper.http_helpers import _get_session

import yt.logger as logger
import yt.wrapper as yt
import yt.yson as yson
try:
    import yt.json_wrapper as json
except ImportError:
    import yt.json as json

import yt.packages.requests as requests

from dateutil.parser import parse
from collections import namedtuple, Counter
from itertools import izip
# Import is necessary due to: http://bugs.python.org/issue7980
import _strptime
from datetime import datetime, timedelta
from copy import deepcopy

from time import mktime

OPERATIONS_ARCHIVE_PATH = "//sys/operations_archive"
BY_ID_ARCHIVE_PATH = "{}/ordered_by_id".format(OPERATIONS_ARCHIVE_PATH)
BY_START_TIME_ARCHIVE_PATH = "{}/ordered_by_start_time".format(OPERATIONS_ARCHIVE_PATH)
STDERRS_PATH = "{}/stderrs".format(OPERATIONS_ARCHIVE_PATH)
JOBS_PATH = "{}/jobs".format(OPERATIONS_ARCHIVE_PATH)

STORAGE_MODE_COMPATIBLE = 0
STORAGE_MODE_HASH_BUCKETS = 1

Operation = namedtuple("Operation", ["start_time", "finish_time", "id", "user", "state", "storage_mode"])

def iter_chunks(iterable, size):
   chunk = []
   for item in iterable:
       chunk.append(item)
       if len(chunk) == size:
           yield chunk
           chunk = []
   if chunk:
       yield chunk

def get_filter_factors(op, attributes):
    brief_spec = attributes.get("brief_spec", {})

    def join_paths(paths):
        if paths is None:
            return ""

        # NOTE(asaitgalin): Using TablePath here to drop attributes.
        return " ".join([str(yt.TablePath(path)) for path in paths])

    return " ".join([
        op,
        attributes.get("key", ""),
        attributes.get("authenticated_user", ""),
        attributes.get("state", ""),
        attributes.get("operation_type", ""),
        brief_spec.get("pool", ""),
        brief_spec.get("title", ""),
        join_paths(brief_spec.get("input_table_paths")),
        join_paths(brief_spec.get("output_table_paths")),
    ]).lower()

def datestr_to_timestamp_legacy(time_str):
    dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    return int(mktime(dt.timetuple()) * 1000000 + dt.microsecond)

def id_to_parts_old(id):
    id_parts = id.split("-")
    id_hi = long(id_parts[3], 16) << 32 | int(id_parts[2], 16)
    id_lo = long(id_parts[1], 16) << 32 | int(id_parts[0], 16)
    return id_hi, id_lo

def id_to_parts_new(id):
    id_parts = id.split("-")
    id_hi = long(id_parts[2], 16) << 32 | int(id_parts[3], 16)
    id_lo = long(id_parts[0], 16) << 32 | int(id_parts[1], 16)
    return id_hi, id_lo

def id_to_parts(id, version):
    return (id_to_parts_new if version >= 6 else id_to_parts_old)(id)

def get_op_path(op_id):
    return "//sys/operations/{}".format(op_id)

def get_op_new_path(op_id):
    return "//sys/operations/{}/{}".format("%02x" % (long(op_id.split("-")[3], 16) % 256), op_id)

def format_op_path(op):
    if op.storage_mode == STORAGE_MODE_COMPATIBLE:
        return get_op_path(op.id)
    return get_op_new_path(op.id)

class NullMetrics(object):
    def add(self, *args):
        pass

class JobsCountGetter(object):
    def __init__(self, client_factory, operations_with_job_counts):
        self.operations_with_job_counts = operations_with_job_counts
        self.client = client_factory()

    def __call__(self, operations):
        batch_client = yt.create_batch_client(max_batch_size=100, client=self.client)
        responses = []
        for op in operations:
            responses.append(batch_client.get(format_op_path(op) + "/jobs/@count"))
        batch_client.commit_batch()

        for op, rsp in zip(operations, responses):
            self.operations_with_job_counts.append((op, 0 if not rsp.is_ok() else rsp.get_result()))

class OperationsArchiver(object):
    ATTRIBUTES = [
        "brief_statistics",
        "error",
        "job_type",
        "state",
        # TODO(aleksandr.gaev): Remove.
        "address",
        "addresses",
        "uncompressed_data_size",
        "error",
        "size",
        "start_time",
        "finish_time",
        "uncompressed_data_size",
    ]

    def __init__(self, client_factory, clean_queue, stderr_queue, version, archive_jobs, metrics=NullMetrics()):
        self.clean_queue = clean_queue
        self.stderr_queue = stderr_queue
        self.version = version
        self.archive_jobs = archive_jobs
        self.metrics = metrics
        self.client = client_factory()

        if not self.client.exists(BY_ID_ARCHIVE_PATH) or not self.client.exists(BY_START_TIME_ARCHIVE_PATH):
            raise Exception("Operations archive tables do not exist")

    def get_archive_rows(self, op_id, data):
        logger.info("Archiving operation %s", op_id)

        index_columns = ["state", "authenticated_user", "operation_type"]
        value_columns = ["progress", "brief_progress", "spec", "brief_spec", "result"]

        if self.version >= 5:
            value_columns.append("events")
        if self.version >= 10:
            value_columns.append("alerts")
        if self.version >= 13:
            value_columns.append("slot_index")
        if self.version >= 17:
            value_columns.append("full_spec")
            value_columns.append("unrecognized_spec")
        if self.version >= 22:
            value_columns.append("runtime_parameters")
            # will be merged to runtime_parameters, check YT-14806
            value_columns.append("heavy_runtime_parameters")
        if self.version >= 27:
            value_columns.append("slot_index_per_pool_tree")
        if self.version >= 35:
            value_columns.append("task_names")
        if self.version >= 42:
            value_columns.append("controller_features")

        by_id_row = {}
        for key in index_columns + value_columns:
            if key in data:
                value = data.get(key)
                if hasattr(value, "attributes"):
                    value.attributes = {}
                by_id_row[key] = data.get(key)
                # Do not forget to strip top-level attributes like <opaque=%true>.
                by_id_row[key].attributes.clear()

        if "heavy_runtime_parameters" in by_id_row:
            if "runtime_parameters" not in by_id_row:
                by_id_row["runtime_parameters"] = {}
            by_id_row["runtime_parameters"].update(by_id_row["heavy_runtime_parameters"])
            by_id_row.pop("heavy_runtime_parameters", None)

        id_hi, id_lo = id_to_parts(op_id, self.version)

        if self.version == 0:
            datestr_to_timestamp = datestr_to_timestamp_legacy
        else:
            datestr_to_timestamp = date_string_to_timestamp_mcs

        by_id_row["id_hi"] = yson.YsonUint64(id_hi)
        by_id_row["id_lo"] = yson.YsonUint64(id_lo)
        by_id_row["start_time"] = datestr_to_timestamp(data["start_time"])
        by_id_row["finish_time"] = datestr_to_timestamp(data["finish_time"])
        by_id_row["filter_factors"] = get_filter_factors(op_id, data)

        by_start_time_row = {
            "id_hi": by_id_row["id_hi"],
            "id_lo": by_id_row["id_lo"],
            "start_time": by_id_row["start_time"]
        }

        if self.version < 2:
            by_start_time_row["dummy"] = 0
        else:
            by_start_time_row["filter_factors"] = get_filter_factors(op_id, data)
            for key in index_columns:
                if key in data:
                    by_start_time_row[key] = data[key]

        if self.version >= 15:
            by_start_time_row["pool"] = by_id_row.get("brief_spec", {}).get("pool")

        return by_id_row, by_start_time_row

    def get_insert_rows(self, op, jobs):
        op_id = op.id
        op_id_hi, op_id_lo = id_to_parts(op_id, self.version)
        rows = []
        for job_id, value in jobs.iteritems():
            job_id_hi, job_id_lo = id_to_parts(job_id, self.version)
            attributes = value.attributes

            row = {}
            row["operation_id_hi"] = yson.YsonUint64(op_id_hi)
            row["operation_id_lo"] = yson.YsonUint64(op_id_lo)
            row["job_id_hi"] = yson.YsonUint64(job_id_hi)
            row["job_id_lo"] = yson.YsonUint64(job_id_lo)
            row["error"] = attributes.get("error")
            row["type" if self.version >= 6 else "job_type"] = attributes["job_type"]
            row["state"] = attributes["state"]
            # TODO(aleksandr.gaev): Remove.
            row["address"] = attributes["address"]
            row["addresses"] = attributes["addresses"]
            row["start_time"] = date_string_to_timestamp_mcs(attributes["start_time"])
            row["finish_time"] = date_string_to_timestamp_mcs(attributes["finish_time"])

            if "stderr" in value:
                self.stderr_queue.put((op, job_id))
                stderr = value["stderr"]
                if self.version >= 4:
                    row["stderr_size"] = yson.YsonUint64(stderr.attributes["uncompressed_data_size"])

            rows.append(row)

        return rows

    def do_insert_rows(self, path, rows):
        atomicity = "none" if self.version >= 16 else "full"
        self.client.insert_rows(path, rows, update=True, atomicity=atomicity)

    def do_archive_jobs(self, ops):
        batch_client = yt.create_batch_client(max_batch_size=32, client=self.client)
        responses = []
        for op in ops:
            responses.append(batch_client.get(format_op_path(op) + "/jobs", attributes=self.ATTRIBUTES))
        batch_client.commit_batch()

        archived_ops = []
        rows = []
        failed_count = 0
        for op, rsp in zip(ops, responses):
            if not rsp.is_ok():
                failed_count += 1
                logger.info("Failed to get jobs for operations %s", op.id)
            else:
                archived_ops.append(op)
                rows.extend(self.get_insert_rows(op, rsp.get_result()))

        logger.info("Inserting %d jobs", len(rows))

        try:
            self.do_insert_rows(JOBS_PATH, rows)
        except:
            failed_count += len(rows)
            raise
        finally:
            self.metrics.add("failed_to_archive_job_count", failed_count)

        self.metrics.add("archived_job_count", len(rows))
        self.clean_queue.put_many([op.id for op in archived_ops])

    def __call__(self, ops):
        batch_client = yt.create_batch_client(max_batch_size=32, client=self.client)
        responses = []
        for op in ops:
            responses.append(batch_client.get(format_op_path(op) + "/@"))
        batch_client.commit_batch()

        by_id_rows = []
        by_start_time_rows = []
        archived_ops = []
        failed_count = 0
        for op, rsp in zip(ops, responses):
            if not rsp.is_ok():
                failed_count += 1
                logger.info("Failed to get attributes of operations %s", op.id)
            else:
                by_id_row, by_start_time_row = self.get_archive_rows(op.id, rsp.get_result())
                by_id_rows.append(by_id_row)
                by_start_time_rows.append(by_start_time_row)
                archived_ops.append(op)

        try:
            self.client.insert_rows(BY_ID_ARCHIVE_PATH, by_id_rows, update=True)
            self.client.insert_rows(BY_START_TIME_ARCHIVE_PATH, by_start_time_rows, update=True)
        except:
            failed_count += len(by_id_rows)
            raise
        finally:
            self.metrics.add("failed_to_archive_count", failed_count)

        self.metrics.add("archived_count", len(archived_ops))

        if self.archive_jobs:
            self.do_archive_jobs(archived_ops)
        else:
            self.clean_queue.put_many([op.id for op in archived_ops])

class StderrDownloader(object):
    def __init__(self, client_factory, insert_queue, version, metrics=NullMetrics()):
        self.insert_queue = insert_queue
        self.version = version
        self.metrics = metrics
        self.client = client_factory()
        self.client.config["proxy"]["retries"]["count"] = 1

    def __call__(self, element):
        op, job_id = element
        path = format_op_path(op)

        stderr = self.client.read_file("{}/jobs/{}/stderr".format(path, job_id)).read()

        op_id_hi, op_id_lo = id_to_parts(op.id, self.version)
        id_hi, id_lo = id_to_parts(job_id, self.version)

        row = {}
        row["operation_id_hi"] = yson.YsonUint64(op_id_hi)
        row["operation_id_lo"] = yson.YsonUint64(op_id_lo)
        row["job_id_hi"] = yson.YsonUint64(id_hi)
        row["job_id_lo"] = yson.YsonUint64(id_lo)
        row["stderr"] = stderr
        self.insert_queue.put(row)

class StderrInserter(object):
    def __init__(self, client_factory, version, metrics=NullMetrics()):
        self.metrics = metrics
        self.version = version
        self.client = client_factory()

    def __call__(self, rowset):
        logger.info("Inserting %d stderrs", len(rowset))

        atomicity = "none" if self.version >= 19 else "full"
        self.client.insert_rows(STDERRS_PATH, rowset, update=True, atomicity=atomicity)

        self.metrics.add("archived_stderr_count", len(rowset))
        self.metrics.add("archived_stderr_size", sum(len(row["stderr"]) for row in rowset))

class OperationCleaner(object):
    def __init__(self, client_factory):
        self.client = client_factory()

    def _set_is_archived_attribute(self, op_ids):
        batch_client = yt.create_batch_client(max_batch_size=256, client=self.client)
        responses = []
        for op_id in op_ids:
            responses.append(batch_client.set(get_op_new_path(op_id) + "/@is_archived", True))
        batch_client.commit_batch()

        errors = []
        for rsp in responses:
            if rsp.is_ok():
                continue

            error = yt.YtResponseError(rsp.get_error())
            if error.is_resolve_error():
                continue

            errors.append(error)

        if errors:
            raise yt.YtError("Failed to set archived attribute", inner_errors=errors)

    def __call__(self, op_ids):
        for op_id in op_ids:
            logger.info("Removing operation %s", op_id)

        self._set_is_archived_attribute(op_ids)

        batch_client = yt.create_batch_client(max_batch_size=256, client=self.client)
        responses = []
        for op_id in op_ids:
            for path in (get_op_path(op_id), get_op_new_path(op_id)):
                responses.append(batch_client.remove(path, recursive=True))
        batch_client.commit_batch()

        errors = []
        for rsp in responses:
            if rsp.is_ok():
                continue

            error = yt.YtResponseError(rsp.get_error())
            if error.is_resolve_error():
                continue

            # This kind or error is expected. It can happen if operation started under transaction
            # and transaction is still alive.
            if error.is_concurrent_transaction_lock_conflict():
                continue

            errors.append(error)

        if errors:
            raise yt.YtError("Failed to remove operations", inner_errors=errors)

class SimpleHashBucketOperationsCleaner(object):
    def __init__(self, client_factory):
        self.client = client_factory()

    def __call__(self, op_paths):
        batch_client = yt.create_batch_client(max_batch_size=200, client=self.client)

        responses = []
        for op_path in op_paths:
            logger.info("Removing node %s", op_path)
            responses.append(batch_client.remove("//sys/operations/" + op_path, recursive=True))
        batch_client.commit_batch()

        unresolved = 0
        errors = []
        for rsp in responses:
            if rsp.is_ok():
                continue

            error = yt.YtResponseError(rsp.get_error())

            if error.is_resolve_error():
                unresolved += 1
                continue

            # This kind or error is expected. It can happen if operation started under transaction
            # and transaction is still alive.
            if error.is_concurrent_transaction_lock_conflict():
                continue

            errors.append(error)

        logger.warning("Number of paths that was failed to resolve: %d", unresolved)
        if errors:
            logger.warning("%s", str(yt.YtError("Failed to remove operation new nodes", inner_errors=errors)))

# Push metrics

def push_to_solomon(values_map, cluster, ts):
    data = {
        "commonLabels": {
            "project": "yt",
            "cluster": cluster,
            "service": "operations_archive",
            "host": "none"
        },
        "sensors": [{
            "labels": {
                "sensor": name
            },
            "ts": datetime_to_string(ts),
            "value": value
        } for name, value in values_map.iteritems()]
    }

    try:
        rsp = requests.post("http://api.solomon.search.yandex.net/push/json", headers={"Content-Type": "application/json"}, data=json.dumps(data), allow_redirects=True, timeout=20)
        if not rsp.ok:
            logger.info(rsp.content)
    except:
        logger.exception("Failed to push metrics to Solomon")

def create_operation_from_node(node, storage_mode):
    maybe_parse_time = lambda value: parse(value).replace(tzinfo=None) if value is not None else None

    return Operation(maybe_parse_time(node.attributes.get("start_time", None)),
        maybe_parse_time(node.attributes.get("finish_time", None)),
        str(node),
        node.attributes["authenticated_user"],
        node.attributes["state"],
        storage_mode)

def request_operations_recursive(yt_client, root_operation_ids, prefixes):
    candidates_to_remove = []
    operations = []

    batch_client = yt.create_batch_client(max_batch_size=32, client=yt_client)
    responses = []
    for prefix in prefixes:
        rsp = batch_client.list(
            "//sys/operations/" + prefix,
            attributes=["state", "authenticated_user", "start_time", "finish_time", "is_archived"])
        responses.append(rsp)
    batch_client.commit_batch()

    for prefix, response in izip(prefixes, responses):
        if response.is_ok():
            for op in response.get_result():
                if str(op) not in root_operation_ids:
                    if not op.attributes.get("is_archived", False) and "state" in op.attributes:
                        operations.append(create_operation_from_node(op, STORAGE_MODE_HASH_BUCKETS))
                    else:
                        candidates_to_remove.append((prefix, op))
        else:
            error = yt.YtResponseError(response.get_error())
            if not error.is_resolve_error():
                raise error

    # It is important to make additional existence check due to possible races.
    exists_responses = []
    batch_client = yt.create_batch_client(max_batch_size=200, client=yt_client)
    for _, op in candidates_to_remove:
        exists_responses.append(batch_client.exists("//sys/operations/" + op))
    batch_client.commit_batch()

    to_remove = []
    for candidate, response in izip(candidates_to_remove, exists_responses):
        if not response.is_ok():
            raise yt.YtResponseError(response.get_error())
        if not response.get_result():
            to_remove.append("/".join(candidate))

    return operations, NonBlockingQueue(to_remove)

class ClientFactory(object):
    def __init__(self, base_client):
        patch = {
            # Fail fast.
            "read_retries": {
                "enable": False
            },
            "dynamic_table_retries": {
                "enable": False
            }
        }
        self._config = update(deepcopy(get_config(base_client)), patch)
        self._session = _get_session(base_client)

    def __call__(self):
        client = yt.YtClient(config=deepcopy(self._config))
        # NOTE(asaitgalin): See https://st.yandex-team.ru/YT-8325
        set_option("_requests_session", self._session, client)
        return client

def clear_operations(soft_limit, hard_limit, grace_timeout, archive_timeout, execution_timeout,
                     max_operations_per_user, robots, archive, archive_jobs, thread_count,
                     stderr_thread_count, push_metrics, remove_threshold, client):

    now = utcnow()
    end_time_limit = now + execution_timeout
    archiving_time_limit = now + execution_timeout * 7 / 8

    client_factory = ClientFactory(client)

    #
    # Step 1: Fetch data from Cypress.
    #

    metrics = Counter({name: 0 for name in [
        "failed_to_archive_count",
        "archived_count",
        "failed_to_archive_job_count",
        "archived_job_count",
        "failed_to_archive_stderr_count",
        "archived_stderr_count",
        "archived_stderr_size",
        "failed_to_archive_stderr_count",
        "removed_count_stale",
        "failed_to_remove_count_stale"]})

    timers = {name: Timer() for name in [
        "getting_job_counts",
        "getting_operations_list",
        "archiving_operations",
        "archiving_stderrs",
        "removing_operations",
        "removing_simple_hash_bucket_operations"]}

    with timers["getting_operations_list"]:
        prefixes = set(["%02x" % prefix for prefix in xrange(256)])

        operations = []
        root_operation_set = set()

        operation_list = client.list(
            "//sys/operations",
            max_size=100000,
            attributes=["state", "start_time", "finish_time", "authenticated_user"])

        for operation in operation_list:
            # This is hash-bucket, just skip it.
            if str(operation) in prefixes:
                continue

            op = create_operation_from_node(operation, STORAGE_MODE_COMPATIBLE)
            operations.append(op)
            root_operation_set.add(op.id)

        bucket_operations, simple_hash_bucket_operations_to_remove_queue = \
            request_operations_recursive(client, root_operation_set, prefixes)

        operations.extend(bucket_operations)
        operations.sort(key=lambda op: op.start_time, reverse=True)

    failed_to_remove_stale = []
    remove_count_stale = len(simple_hash_bucket_operations_to_remove_queue)

    logger.info("Removing %d stale operation nodes", remove_count_stale)
    with timers["removing_simple_hash_bucket_operations"]:
        run_batching_queue_workers(
            simple_hash_bucket_operations_to_remove_queue,
            SimpleHashBucketOperationsCleaner,
            thread_count,
            args=(client_factory,),
            batch_size=8,
            failed_items=failed_to_remove_stale)

        failed_to_remove_stale.extend(
            wait_for_queue(simple_hash_bucket_operations_to_remove_queue, "simple_hash_buckets_operation_queue", end_time_limit))

    metrics["initial_count"] = len(operations)

    #
    # Step 2: Filter out irrelevant operations.
    #

    def can_consider_archiving(op):
        # Ignore operations in progress or in transient states.
        if op.state not in ["completed", "aborted", "failed"]:
            return False
        # Ignore fresh operations to avoid conflicts with scheduler.
        if now - op.finish_time < grace_timeout:
            return False
        return True

    operations_to_consider = filter(can_consider_archiving, operations)
    metrics["considered_count"] = len(operations_to_consider)

    #
    # Step 3: Select operations to archive.
    #

    operations_with_job_counts = []
    with timers["getting_job_counts"]:
        consider_queue = NonBlockingQueue(operations_to_consider)
        run_batching_queue_workers(consider_queue, JobsCountGetter, thread_count, (client_factory, operations_with_job_counts,))
        wait_for_queue(consider_queue, "get_job_count", archiving_time_limit)

    user_counts = Counter()
    number_of_retained_operations = 0

    def can_archive(op, job_count):
        if op.user in robots and op.state != "failed":
            return True
        if (now - op.start_time) > archive_timeout:
            return True
        if number_of_retained_operations >= hard_limit:
            return True
        if user_counts[op.user] > max_operations_per_user:
            return True
        if number_of_retained_operations >= soft_limit and op.state != "failed" and job_count == 0:
            return True
        return False

    operations_to_archive = []
    for op, job_count in operations_with_job_counts:
        user_counts[op.user] += 1
        if can_archive(op, job_count):
            operations_to_archive.append(op)
        else:
            number_of_retained_operations += 1

    metrics["approved_to_archive_count"] = len(operations_to_archive)

    now_before_clean = utcnow()

    if archive:
        operation_archiving_time_limit = utcnow() + (archiving_time_limit - utcnow()) / 2

        logger.info("Archiving %d operations", len(operations_to_archive))
        version = client.get("{}/@".format(OPERATIONS_ARCHIVE_PATH)).get("version", 0)

        archive_jobs = archive_jobs and version >= 3

        archive_queue = NonBlockingQueue(operations_to_archive)
        remove_queue = NonBlockingQueue()
        stderr_queue = NonBlockingQueue()
        failed_to_archive = []

        thread_safe_metrics = ThreadSafeCounter(metrics)
        with timers["archiving_operations"]:
            run_batching_queue_workers(
                archive_queue,
                OperationsArchiver,
                thread_count,
                (client_factory, remove_queue, stderr_queue, version, archive_jobs, thread_safe_metrics),
                batch_size=32,
                failed_items=failed_to_archive)
            failed_to_archive.extend(wait_for_queue(archive_queue, "archive_operation", operation_archiving_time_limit))

        if archive_jobs:
            logger.info("Archiving %d stderrs", len(stderr_queue))

            with timers["archiving_stderrs"]:
                insert_queue = NonBlockingQueue()
                run_queue_workers(stderr_queue, StderrDownloader, stderr_thread_count, (client_factory, insert_queue, version, thread_safe_metrics))
                run_batching_queue_workers(insert_queue, StderrInserter, thread_count, (client_factory, version, thread_safe_metrics,))

                failed_stderr_count = 0
                failed_stderr_count += len(wait_for_queue(stderr_queue, "fetch_stderr", archiving_time_limit))
                failed_stderr_count += len(wait_for_queue(insert_queue, "insert_jobs", archiving_time_limit))
                thread_safe_metrics.add("failed_to_archive_stderr_count", failed_stderr_count)

        if len(failed_to_archive) > remove_threshold:
            remove_queue.put_many([op.id for op in failed_to_archive[remove_threshold:]])
    else:
        remove_queue = NonBlockingQueue([op.id for op in operations_to_archive])

    remove_count = len(remove_queue)
    logger.info("Removing %d operations", len(remove_queue))

    failed_to_remove = []
    with timers["removing_operations"]:
        run_batching_queue_workers(remove_queue, OperationCleaner, thread_count, args=(client_factory,), batch_size=8, failed_items=failed_to_remove)
        failed_to_remove.extend(wait_for_queue(remove_queue, "remove_operations", end_time_limit))

    now_after_clean = utcnow()

    total_time = (now_after_clean - now).total_seconds()

    logger.info(
        "; ".join([
            "Done",
            "processed %s operations in %.2fs",
            "%s were considered %s in %.2fs",
            "%s were %s in %.2fs",
            "%s (of %s) stale were removed",
            "%s were retained"]),
        len(operations),
        total_time,
        len(operations_to_consider),
        "archiving" if archive else "removing",
        (now_before_clean - now).total_seconds(),
        remove_count,
        "archived" if archive else "removed",
        (now_after_clean - now_before_clean).total_seconds(),
        remove_count_stale - len(failed_to_remove_stale),
        remove_count_stale,
        number_of_retained_operations)

    logger.info("Times: (%s)", "; ".join(["{}: {}".format(name, str(timer)) for name, timer in timers.iteritems()]))

    metrics["total_time_ms"] = total_time * 1000
    metrics["per_operation_time_ms"] = total_time * 1000 / len(operations) if len(operations) > 0 else 0
    metrics["removed_count"] = remove_count - len(failed_to_remove)
    metrics["removed_count_stale"] = remove_count_stale - len(failed_to_remove_stale)
    metrics["failed_to_remove_count"] = len(failed_to_remove)
    metrics["failed_to_remove_count_stale"] = len(failed_to_remove_stale)

    logger.info("Metrics: (%s)", "; ".join(["{}: {}".format(name, value) for name, value in metrics.iteritems()]))

    if push_metrics:
        for name, timer in timers.iteritems():
            metrics["{}_time_ms".format(name)] = timer.value() * 1000

        cluster_name = client.config["proxy"]["url"].split(".")[0]
        push_to_solomon(metrics, cluster_name, now)

