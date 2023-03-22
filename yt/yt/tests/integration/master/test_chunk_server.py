from yt_env_setup import YTEnvSetup, Restarter, NODES_SERVICE, MASTERS_SERVICE

from yt_commands import (
    authors, create_user, wait, create, ls, get, set, remove, exists,
    start_transaction, insert_rows, build_snapshot, gc_collect, concatenate, create_account,
    read_table, write_table, write_journal, merge, sync_create_cells, sync_mount_table, sync_unmount_table, sync_control_chunk_replicator, get_singular_chunk_id,
    multicell_sleep, update_nodes_dynamic_config, switch_leader, ban_node, add_maintenance, remove_maintenance,
    set_node_decommissioned, execute_command, is_active_primary_master_leader, is_active_primary_master_follower,
    get_active_primary_master_leader_address, get_active_primary_master_follower_address, create_tablet_cell_bundle, print_debug)

from yt_helpers import profiler_factory

from yt.environment.helpers import assert_items_equal
from yt.common import YtError, WaitFailed
import yt.yson as yson

import pytest
from flaky import flaky

import json
import os
from time import sleep, time

##################################################################


class TestChunkServer(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 21
    NUM_TEST_PARTITIONS = 4

    DELTA_NODE_CONFIG = {
        "data_node": {
            "disk_health_checker": {
                "check_period": 1000,
            },
        },
    }

    @authors("babenko", "ignat")
    def test_owning_nodes1(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})
        chunk_id = get_singular_chunk_id("//tmp/t")
        assert get("#" + chunk_id + "/@owning_nodes") == ["//tmp/t"]

    @authors("babenko", "ignat")
    def test_owning_nodes2(self):
        create("table", "//tmp/t")
        tx = start_transaction()
        write_table("//tmp/t", {"a": "b"}, tx=tx)
        chunk_id = get_singular_chunk_id("//tmp/t", tx=tx)
        assert get("#" + chunk_id + "/@owning_nodes") == \
            [yson.to_yson_type("//tmp/t", attributes={"transaction_id": tx})]

    @authors("babenko", "shakurov")
    def test_replication(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})

        assert get("//tmp/t/@replication_factor") == 3

        chunk_id = get_singular_chunk_id("//tmp/t")

        wait(lambda: len(get("#%s/@stored_replicas" % chunk_id)) == 3)

    def _test_decommission(self, path, replica_count, node_to_decommission_count=1):
        assert replica_count >= node_to_decommission_count

        chunk_id = get_singular_chunk_id(path)

        nodes_to_decommission = self._decommission_chunk_replicas(chunk_id, replica_count, node_to_decommission_count)

        wait(
            lambda: not self._nodes_have_chunk(nodes_to_decommission, chunk_id)
            and len(get("#%s/@stored_replicas" % chunk_id)) == replica_count
        )

    def _decommission_chunk_replicas(self, chunk_id, replica_count, node_to_decommission_count):
        nodes_to_decommission = get("#%s/@stored_replicas" % chunk_id)
        assert len(nodes_to_decommission) == replica_count

        nodes_to_decommission = nodes_to_decommission[:node_to_decommission_count]
        assert self._nodes_have_chunk(nodes_to_decommission, chunk_id)

        for node in nodes_to_decommission:
            set_node_decommissioned(node, True)

        return nodes_to_decommission

    def _nodes_have_chunk(self, nodes, id):
        def id_to_hash(id):
            return id.split("-")[3]

        for node in nodes:
            if not (
                id_to_hash(id) in [id_to_hash(id_) for id_ in ls("//sys/cluster_nodes/%s/orchid/stored_chunks" % node)]
            ):
                return False
        return True

    def _wait_for_replicas_removal(self, path, service_to_restart):
        chunk_id = get_singular_chunk_id(path)
        wait(lambda: len(get("#{0}/@stored_replicas".format(chunk_id))) > 0)
        node = get("#{0}/@stored_replicas".format(chunk_id))[0]

        wait(lambda: get("//sys/cluster_nodes/{0}/@destroyed_chunk_replica_count".format(node)) == 0)

        set(
            "//sys/cluster_nodes/{0}/@resource_limits_overrides/removal_slots".format(node),
            0,
        )

        remove(path)
        wait(lambda: get("//sys/cluster_nodes/{0}/@destroyed_chunk_replica_count".format(node)) > 0)

        with Restarter(self.Env, service_to_restart):
            pass

        remove("//sys/cluster_nodes/{0}/@resource_limits_overrides/removal_slots".format(node))
        wait(lambda: get("//sys/cluster_nodes/{0}/@destroyed_chunk_replica_count".format(node)) == 0)

    @authors("babenko")
    def test_decommission_regular1(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"}, table_writer={"upload_replication_factor": 3})
        self._test_decommission("//tmp/t", 3)

    @authors("shakurov")
    def test_decommission_regular2(self):
        create("table", "//tmp/t", attributes={"replication_factor": 4})
        write_table("//tmp/t", {"a": "b"}, table_writer={"upload_replication_factor": 4})

        chunk_id = get_singular_chunk_id("//tmp/t")

        self._decommission_chunk_replicas(chunk_id, 4, 2)
        set("//tmp/t/@replication_factor", 3)
        # Now 2 replicas are decommissioned and 2 aren't.
        # The chunk should be both under- and overreplicated.

        wait(lambda: len(get("#%s/@stored_replicas" % chunk_id)) == 3)
        nodes = get("#%s/@stored_replicas" % chunk_id)
        for node in nodes:
            assert not get("//sys/cluster_nodes/%s/@decommissioned" % node)

    @authors("babenko")
    def test_decommission_erasure1(self):
        create("table", "//tmp/t")
        set("//tmp/t/@erasure_codec", "lrc_12_2_2")
        write_table("//tmp/t", {"a": "b"})
        self._test_decommission("//tmp/t", 16)

    @authors("shakurov")
    def test_decommission_erasure2(self):
        create("table", "//tmp/t")
        set("//tmp/t/@erasure_codec", "lrc_12_2_2")
        write_table("//tmp/t", {"a": "b"})
        self._test_decommission("//tmp/t", 16, 4)

    @authors("ignat")
    def test_decommission_erasure3(self):
        create("table", "//tmp/t")
        set("//tmp/t/@erasure_codec", "lrc_12_2_2")
        write_table("//tmp/t", {"a": "b"})

        sync_control_chunk_replicator(False)

        chunk_id = get_singular_chunk_id("//tmp/t")
        nodes = get("#%s/@stored_replicas" % chunk_id)

        for index in (4, 6, 11, 15):
            ban_node(nodes[index], "test decommission erasure 3")
        set_node_decommissioned(nodes[0], True)

        wait(lambda: len(get("#%s/@stored_replicas" % chunk_id)) == 12)

        sync_control_chunk_replicator(True)

        wait(lambda: get("//sys/cluster_nodes/%s/@decommissioned" % nodes[0]))
        wait(lambda: len(get("#%s/@stored_replicas" % chunk_id)) == 16)

    @authors("babenko")
    def test_decommission_journal(self):
        create("journal", "//tmp/j")
        write_journal("//tmp/j", [{"payload": "payload" + str(i)} for i in range(0, 10)])
        self._test_decommission("//tmp/j", 3)

    @authors("babenko")
    def test_list_chunk_owners(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", [{"a": "b"}])
        ls("//sys/chunks", attributes=["owning_nodes"])

    @authors("babenko")
    def test_disable_replicator_when_few_nodes_are_online(self):
        set("//sys/@config/chunk_manager/safe_online_node_count", 3)

        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 21

        assert get("//sys/@chunk_replicator_enabled")

        for i in range(19):
            ban_node(nodes[i], "test disable replicator when few nodes are online")

        wait(lambda: not get("//sys/@chunk_replicator_enabled"))

    @authors("babenko")
    def test_disable_replicator_when_explicitly_requested_so(self):
        assert get("//sys/@chunk_replicator_enabled")

        set("//sys/@config/chunk_manager/enable_chunk_replicator", False, recursive=True)

        wait(lambda: not get("//sys/@chunk_replicator_enabled"))

    @authors("babenko", "ignat")
    def test_hide_chunk_attrs(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})
        chunks = ls("//sys/chunks")
        for c in chunks:
            assert len(c.attributes) == 0

        chunks_json = execute_command("list", {"path": "//sys/chunks", "output_format": "json"})
        for c in json.loads(chunks_json):
            assert isinstance(c, str)

    @authors("shakurov")
    def test_chunk_requisition_registry_orchid(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})

        master = ls("//sys/primary_masters")[0]
        master_orchid_path = "//sys/primary_masters/{0}/orchid/chunk_manager/requisition_registry".format(master)

        known_requisition_indexes = frozenset(ls(master_orchid_path))
        set("//tmp/t/@replication_factor", 4)
        sleep(0.3)
        new_requisition_indexes = frozenset(ls(master_orchid_path)) - known_requisition_indexes
        assert len(new_requisition_indexes) == 1
        new_requisition_index = next(iter(new_requisition_indexes))

        new_requisition = get("{0}/{1}".format(master_orchid_path, new_requisition_index))
        assert new_requisition["ref_counter"] == 2  # one for 'local', one for 'aggregated' requisition
        assert new_requisition["vital"]
        assert len(new_requisition["entries"]) == 1
        assert new_requisition["entries"][0]["replication_policy"]["replication_factor"] == 4

    @authors("shakurov")
    def test_node_chunk_replica_count(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})

        chunk_id = get_singular_chunk_id("//tmp/t")
        wait(lambda: len(get("#{0}/@stored_replicas".format(chunk_id))) == 3)

        def check_replica_count():
            nodes = get("#{0}/@stored_replicas".format(chunk_id))
            for node in nodes:
                if get("//sys/cluster_nodes/{0}/@chunk_replica_count/default".format(node)) == 0:
                    return False
            return True

        wait(check_replica_count, sleep_backoff=1.0)

    @authors("babenko", "aleksandra-zh")
    def test_max_replication_factor(self):
        old_max_rf = get("//sys/media/default/@config/max_replication_factor")
        try:
            MAX_RF = 5
            set("//sys/media/default/@config/max_replication_factor", MAX_RF)

            multicell_sleep()

            create("table", "//tmp/t", attributes={"replication_factor": 10})
            assert get("//tmp/t/@replication_factor") == 10

            write_table("//tmp/t", {"a": "b"})
            chunk_id = get_singular_chunk_id("//tmp/t")

            def ok_replication_status():
                status = get("#{}/@replication_status/default".format(chunk_id))
                return not status["underreplicated"] and not status["overreplicated"]
            wait(ok_replication_status)

            assert len(get("#{0}/@stored_replicas".format(chunk_id))) == MAX_RF
        finally:
            set("//sys/media/default/@config/max_replication_factor", old_max_rf)

    @authors("aleksandra-zh")
    @pytest.mark.parametrize("service_to_restart", [NODES_SERVICE, MASTERS_SERVICE])
    def test_chunk_replica_removal(self, service_to_restart):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})

        self._wait_for_replicas_removal("//tmp/t", service_to_restart)

    @authors("aleksandra-zh")
    @pytest.mark.parametrize("service_to_restart", [NODES_SERVICE, MASTERS_SERVICE])
    def test_journal_chunk_replica_removal(self, service_to_restart):
        create("journal", "//tmp/j")
        write_journal("//tmp/j", [{"payload": "payload" + str(i)} for i in range(0, 10)])

        self._wait_for_replicas_removal("//tmp/j", service_to_restart)

    @authors("gritukan")
    def test_disable_store_location(self):
        update_nodes_dynamic_config({"data_node": {"abort_on_location_disabled": False}})
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})

        chunk_id = get_singular_chunk_id("//tmp/t")
        wait(lambda: len(get("#{0}/@stored_replicas".format(chunk_id))) == 3)

        node_id = get("#{0}/@stored_replicas".format(chunk_id))[0]
        location_path = get("//sys/cluster_nodes/{}/orchid/stored_chunks/{}/location".format(node_id, chunk_id))

        with open("{}/disabled".format(location_path), "wb") as f:
            f.write(b"{foo=bar}")

        wait(lambda: node_id not in get("#{0}/@stored_replicas".format(chunk_id)))
        assert not exists("//sys/cluster_nodes/{}/orchid/stored_chunks/{}".format(node_id, chunk_id))

        # Repair node for future tests.
        os.remove("{}/disabled".format(location_path))
        with Restarter(self.Env, NODES_SERVICE):
            pass

    @authors("kvk1920")
    def test_last_finished_job(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})
        chunk_id = get_singular_chunk_id("//tmp/t")

        def check_if_job_finished():
            jobs = get("//sys/chunks/{}/@jobs".format(chunk_id))
            return len(jobs) == 1 and jobs[0]["state"] in {"completed", "aborted", "failed"}

        wait(check_if_job_finished)

    def _reproduce_missing_requisition_update(self):
        create_account("a")
        create_account("b")
        create("table", "//tmp/a", attributes={"account": "a"})
        write_table("//tmp/a", {"a": 1, "b": 2})
        create("table", "//tmp/b", attributes={"account": "b"})
        concatenate(["//tmp/a"], "//tmp/b")
        chunk = get_singular_chunk_id("//tmp/a")
        assert chunk == get_singular_chunk_id("//tmp/b")
        assert get("//tmp/b/@account") == "b"
        wait(lambda: {req["account"] for req in get(f"#{chunk}/@requisition")} == {"a", "b"})
        write_table("//tmp/a", {"a": 2, "b": 3})
        wait(lambda: {req["account"] for req in get(f"#{chunk}/@requisition")} == {"b"})

    @authors("kvk1920")
    def test_missing_requisition_update_yt17756(self):
        with pytest.raises(WaitFailed):
            self._reproduce_missing_requisition_update()

    @authors("kvk1920")
    def test_fix_missing_requisition_update_yt17756(self):
        path = "//sys/@config/chunk_manager/enable_fix_requisition_update_on_merge"
        set(path, True)
        try:
            self._reproduce_missing_requisition_update()
        finally:
            set(path, False)

    @authors("gritukan")
    def test_historically_non_vital(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})
        chunk_id = get_singular_chunk_id("//tmp/t")

        assert not get(f"#{chunk_id}/@historically_non_vital")

        set("//tmp/t/@replication_factor", 1)
        wait(lambda: get(f"#{chunk_id}/@historically_non_vital"))
        wait(lambda: len(get(f"#{chunk_id}/@stored_replicas")) == 1)

        node = get(f"#{chunk_id}/@stored_replicas")[0]
        ban_node(node, "test_historically_non_vital")

        wait(lambda: chunk_id in get("//sys/lost_chunks"))
        assert chunk_id not in get("//sys/lost_vital_chunks")


##################################################################


class TestChunkServerMulticell(TestChunkServer):
    NUM_SECONDARY_MASTER_CELLS = 2
    NUM_SCHEDULERS = 1

    @authors("babenko")
    def test_validate_chunk_host_cell_role(self):
        set("//sys/@config/multicell_manager/cell_descriptors", {"11": {"roles": ["cypress_node_host"]}})
        with pytest.raises(YtError):
            create(
                "table",
                "//tmp/t",
                attributes={"external": True, "external_cell_tag": 11},
            )

    @authors("babenko")
    def test_owning_nodes3(self):
        create("table", "//tmp/t0", attributes={"external": False})
        create("table", "//tmp/t1", attributes={"external_cell_tag": 11})
        create("table", "//tmp/t2", attributes={"external_cell_tag": 12})

        write_table("//tmp/t1", {"a": "b"})

        merge(mode="ordered", in_="//tmp/t1", out="//tmp/t0")
        merge(mode="ordered", in_="//tmp/t1", out="//tmp/t2")

        chunk_ids0 = get("//tmp/t0/@chunk_ids")
        chunk_ids1 = get("//tmp/t1/@chunk_ids")
        chunk_ids2 = get("//tmp/t2/@chunk_ids")

        assert chunk_ids0 == chunk_ids1
        assert chunk_ids1 == chunk_ids2
        chunk_ids = chunk_ids0
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]

        assert_items_equal(get("#" + chunk_id + "/@owning_nodes"), ["//tmp/t0", "//tmp/t1", "//tmp/t2"])

    @authors("babenko")
    def test_chunk_requisition_registry_orchid(self):
        pass

    @authors("h0pless")
    def test_dedicated_chunk_host_role_alert(self):
        def has_alert(msg):
            for alert in get("//sys/@master_alerts"):
                if msg in str(alert):
                    return True
            return False

        set("//sys/@config/multicell_manager/cell_descriptors",
            {"11": {"roles": ["chunk_host", "dedicated_chunk_host"]}})
        wait(lambda: has_alert("Cell received conflicting chunk_host and dedicated_chunk_host roles") is True)

        set("//sys/@config/multicell_manager/cell_descriptors",
            {"11": {"roles": ["chunk_host"]}})
        wait(lambda: has_alert("Cell received conflicting chunk_host and dedicated_chunk_host roles") is False)

    @authors("h0pless")
    def test_dedicated_chunk_host_roles_only(self):
        set("//sys/@config/multicell_manager/cell_descriptors", {
            "11": {"roles": ["dedicated_chunk_host"]},
            "12": {"roles": ["dedicated_chunk_host"]}})

        with pytest.raises(YtError, match="No secondary masters with a chunk host role were found"):
            create("table", "//tmp/t", attributes={
                "schema": [
                    {"name": "a", "type": "int64"},
                    {"name": "b", "type": "int64"},
                    {"name": "c", "type": "int64"}
                ]})

    @authors("h0pless")
    def test_dedicated_chunk_host_role(self):
        dedicated_host_cell_tag = 11
        chunk_host_cell_tag = 12

        set("//sys/@config/multicell_manager/cell_descriptors", {
            "11": {"roles": ["dedicated_chunk_host"]},
            "12": {"roles": ["chunk_host"]}})

        create("table", "//tmp/t1", attributes={
            "schema": [
                {"name": "a", "type": "int64"},
                {"name": "b", "type": "int64"},
                {"name": "c", "type": "int64"}
            ],
            "external_cell_tag": dedicated_host_cell_tag})

        create("table", "//tmp/t2", attributes={
            "schema": [
                {"name": "a", "type": "int64"},
                {"name": "b", "type": "int64"},
                {"name": "c", "type": "int64"}
            ],
            "external_cell_tag": chunk_host_cell_tag})

        create("table", "//tmp/t3", attributes={
            "schema": [
                {"name": "a", "type": "int64"},
                {"name": "b", "type": "int64"},
                {"name": "c", "type": "int64"}
            ]})

        create_tablet_cell_bundle("b1")
        create_tablet_cell_bundle("b2")

        set("//sys/tablet_cell_bundles/b1/@options/changelog_external_cell_tag", dedicated_host_cell_tag)
        set("//sys/tablet_cell_bundles/b2/@options/changelog_external_cell_tag", chunk_host_cell_tag)

        sync_create_cells(2, tablet_cell_bundle="b1")
        sync_create_cells(2, tablet_cell_bundle="b2")

        multicell_sleep()

        tablet_cells = ls("//sys/tablet_cells")
        for cell in tablet_cells:
            if (get("//sys/tablet_cells/{0}/@tablet_cell_bundle".format(cell)) == "b1"):
                expected_cell_tag = dedicated_host_cell_tag
            else:
                expected_cell_tag = chunk_host_cell_tag
            changelogs = ls("//sys/tablet_cells/{0}/changelogs".format(cell))
            assert changelogs
            for changelog in changelogs:
                changelog_path = "//sys/tablet_cells/{0}/changelogs/{1}".format(cell, changelog)
                is_external = get("{0}/@external".format(changelog_path))
                if is_external:
                    assert get("{0}/@external_cell_tag".format(changelog_path)) == expected_cell_tag
                else:
                    assert get("{0}/@native_cell_tag".format(changelog_path)) == expected_cell_tag
                chunk_id = get("{0}/@chunk_ids/0".format(changelog_path))
                assert get("#{}/@native_cell_tag".format(chunk_id)) == expected_cell_tag


##################################################################


class TestLastFinishedJobStoreLimit(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 4

    @authors("kvk1920")
    @pytest.mark.timeout(150)
    @pytest.mark.parametrize("limit", [0, 1, 10])
    def test_last_finished_job_limit(self, limit):
        set("//sys/@config/chunk_manager/finished_jobs_queue_size", limit)

        finished_jobs = []

        for table_num in range(limit + 1):
            table = "//tmp/t{}".format(table_num)
            create("table", table)
            write_table(table, {"a": "b"})
            chunk_id = get_singular_chunk_id(table)

            def check_if_job_finished():
                if len(get("#{}/@stored_replicas".format(chunk_id))) != 3:
                    return False
                jobs = get("//sys/chunks/{}/@jobs".format(chunk_id))
                if (0 == limit and 0 == len(jobs)) or (1 == len(jobs) and "completed" == jobs[0]["state"]):
                    if jobs:
                        finished_jobs.append(jobs[0])
                    return True
                else:
                    return False

            wait(check_if_job_finished)

        if limit == 0:
            assert len(finished_jobs) == 0
        else:
            chunk_id = get_singular_chunk_id("//tmp/t0")
            assert finished_jobs[0] not in get("//sys/chunks/{}/@jobs".format(chunk_id))


##################################################################


class TestMultipleErasurePartsPerNode(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1

    DELTA_MASTER_CONFIG = {
        "chunk_manager": {
            "allow_multiple_erasure_parts_per_node": True
        }
    }

    @authors("babenko")
    def test_allow_multiple_erasure_parts_per_node(self):
        create("table", "//tmp/t", attributes={"erasure_codec": "lrc_12_2_2"})
        rows = [{"a": "b"}]
        write_table("//tmp/t", rows)
        assert read_table("//tmp/t") == rows

        chunk_id = get_singular_chunk_id("//tmp/t")

        status = get("#" + chunk_id + "/@replication_status/default")
        assert not status["data_missing"]
        assert not status["parity_missing"]
        assert not status["overreplicated"]
        assert not status["underreplicated"]


##################################################################


class TestConsistentChunkReplicaPlacementBase(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 20
    USE_DYNAMIC_TABLES = True

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "chunk_manager": {
            "consistent_replica_placement": {
                "enable": True,
                "token_redistribution_period": 500,
            },
        },
    }

    def _mount_insert_unmount(self, table_path, row):
        sync_mount_table(table_path)
        insert_rows(table_path, [row])
        sync_unmount_table(table_path)

    def _create_table_with_two_consistently_placed_chunks(self, table_path, attributes=None):
        if attributes is None:
            attributes = {}

        schema = [
            {"name": "key", "type": "string", "sort_order": "ascending"},
            {"name": "value", "type": "string"}
        ]

        attributes["dynamic"] = True
        attributes["enable_consistent_chunk_replica_placement"] = True
        attributes["schema"] = schema

        sync_create_cells(1)
        create("table", table_path, attributes=attributes)

        row1 = {"key": "k1", "value": "v1"}
        self._mount_insert_unmount(table_path, row1)

        row2 = {"key": "k2", "value": "v2"}
        self._mount_insert_unmount(table_path, row2)

        chunk_ids = get("{}/@chunk_ids".format(table_path))
        assert len(chunk_ids) == 2

        assert get("#{}/@consistent_replica_placement_hash".format(chunk_ids[0])) == \
            get("#{}/@consistent_replica_placement_hash".format(chunk_ids[1]))

    def _disable_token_redistribution(self):
        old_period = get("//sys/@config/chunk_manager/consistent_replica_placement/token_redistribution_period")
        remove("//sys/@config/chunk_manager/consistent_replica_placement/token_redistribution_period")
        sleep(old_period / 1000.0)

    def _are_chunk_replicas_collocated(self, *stored_replicas):
        def by_index_then_node_address(replica):
            # NB: -1 is for regular chunks.
            # NB: media are ignored.
            return (replica.attributes.get("index", -1), str(replica))

        def to_addresses(replicas):
            return [str(replica) for replica in replicas]

        stored_replicas = [to_addresses(sorted(replicas, key=by_index_then_node_address)) for replicas in stored_replicas]

        return all(str(replicas) == str(stored_replicas[0]) for replicas in stored_replicas[1:])

    def _are_chunks_collocated(self, chunk_ids):
        stored_replicas = [get("#{}/@stored_replicas".format(chunk_id)) for chunk_id in chunk_ids]
        return self._are_chunk_replicas_collocated(stored_replicas)


##################################################################


class TestConsistentChunkReplicaPlacement(TestConsistentChunkReplicaPlacementBase):
    NUM_TEST_PARTITIONS = 3
    NUM_SECONDARY_MASTER_CELLS = 2

    @authors("shakurov")
    def test_token_count_attribute(self):
        set("//sys/@config/chunk_manager/consistent_replica_placement", {
            "token_distribution_bucket_count": 10,
            "tokens_per_node": 7,
            "token_redistribution_period": 500
        })
        sleep(1.5)

        for node in ls("//sys/cluster_nodes", attributes=["consistent_replica_placement_token_count"]):
            token_count = node.attributes["consistent_replica_placement_token_count"]["default"]
            assert token_count >= 7
            assert token_count <= 70

    @authors("shakurov")
    def test_regular(self):
        self._create_table_with_two_consistently_placed_chunks("//tmp/t1")
        assert get("//tmp/t1/@enable_consistent_chunk_replica_placement")
        chunk_ids = get("//tmp/t1/@chunk_ids")
        wait(lambda: len(get("#{}/@stored_replicas".format(chunk_ids[0]))) == 3)
        wait(lambda: self._are_chunks_collocated(chunk_ids))

    @authors("shakurov")
    def test_chunk_destruction(self):
        self._create_table_with_two_consistently_placed_chunks("//tmp/t4")
        chunk_ids = get("//tmp/t4/@chunk_ids")
        wait(lambda: self._are_chunks_collocated(chunk_ids))

        remove("//tmp/t4")

        # Must not crash here.
        gc_collect()

        self._create_table_with_two_consistently_placed_chunks("//tmp/t4")
        chunk_ids = get("//tmp/t4/@chunk_ids")
        wait(lambda: self._are_chunks_collocated(chunk_ids))

    @authors("shakurov")
    @pytest.mark.parametrize("trouble_mode", ["ban", "decommission", "disable_write_sessions"])
    def test_node_trouble(self, trouble_mode):
        self._disable_token_redistribution()

        self._create_table_with_two_consistently_placed_chunks("//tmp/t5")
        chunk_ids = get("//tmp/t5/@chunk_ids")

        troubled_node = get("#{}/@stored_replicas".format(chunk_ids[0]))[1]
        add_maintenance("cluster_node", troubled_node, trouble_mode, "test node trouble")

        def are_chunks_collocated():
            chunk0_replicas = get("#{}/@stored_replicas".format(chunk_ids[0]))
            if len(chunk0_replicas) != 3:
                return False

            if troubled_node in chunk0_replicas:
                return False  # Wait for banning/decommissioning to take effect.

            chunk1_replicas = get("#{}/@stored_replicas".format(chunk_ids[1]))

            if len(chunk1_replicas) != 3:
                return False

            if troubled_node in chunk1_replicas:
                return False  # Wait for banning/decommissioning to take effect.

            return self._are_chunk_replicas_collocated(chunk0_replicas, chunk1_replicas)

        # Decommissioning may take some time.
        wait(are_chunks_collocated, iter=30, sleep_backoff=1.0)

    @authors("shakurov")
    @pytest.mark.parametrize("trouble_mode", ["ban", "decommission", "disable_write_sessions"])
    def test_troubled_node_restart(self, trouble_mode):
        self._disable_token_redistribution()

        self._create_table_with_two_consistently_placed_chunks("//tmp/t5")
        chunk_ids = get("//tmp/t5/@chunk_ids")

        troubled_node = get("#{}/@stored_replicas".format(chunk_ids[0]))[1]
        maintenance_id = add_maintenance("cluster_node", troubled_node, trouble_mode, "test roubled node restart")

        def are_chunks_collocated(troubled_node_ok):
            chunk0_replicas = get("#{}/@stored_replicas".format(chunk_ids[0]))

            if len(chunk0_replicas) != 3:
                return False

            if not troubled_node_ok and (troubled_node in chunk0_replicas):
                return False  # Wait for banning/decommissioning to take effect.

            chunk1_replicas = get("#{}/@stored_replicas".format(chunk_ids[1]))

            if len(chunk1_replicas) != 3:
                return False

            if not troubled_node_ok and (troubled_node in chunk1_replicas):
                return False  # Wait for banning/decommissioning to take effect.

            return self._are_chunk_replicas_collocated(chunk0_replicas, chunk1_replicas)

        wait(lambda: are_chunks_collocated(False), iter=120, sleep_backoff=1.0)

        with Restarter(self.Env, NODES_SERVICE):
            if trouble_mode == "ban":
                # Otherwise the node won't restart.
                remove_maintenance("cluster_node", troubled_node, id=maintenance_id)

        wait(lambda: are_chunks_collocated(True), iter=120, sleep_backoff=1.0)

    @authors("shakurov")
    def test_rf_change(self):
        self._create_table_with_two_consistently_placed_chunks("//tmp/t6")
        chunk_ids = get("//tmp/t6/@chunk_ids")
        wait(lambda: self._are_chunks_collocated(chunk_ids))
        wait(lambda: self._are_chunks_collocated(chunk_ids))

        set("//tmp/t6/@replication_factor", 7)
        wait(lambda: len(get("#{}/@stored_replicas".format(chunk_ids[0]))) == 7)
        wait(lambda: self._are_chunks_collocated(chunk_ids))

        set("//tmp/t6/@replication_factor", 2)
        wait(lambda: len(get("#{}/@stored_replicas".format(chunk_ids[0]))) == 2)
        wait(lambda: self._are_chunks_collocated(chunk_ids))

    def _pull_replication_queues_empty(self):
        nodes = ls("//sys/cluster_nodes")
        for node in nodes:
            if get("//sys/cluster_nodes/{}/@pull_replication_queue_size".format(node)) > 0:
                return False
            if get("//sys/cluster_nodes/{}/@pull_replication_chunk_count".format(node)) > 0:
                return False
        return True

    @authors("aleksandra-zh")
    def test_crp_pull_replication_simple(self):
        set("//sys/@config/chunk_manager/consistent_replica_placement/enable_pull_replication", True)
        self._create_table_with_two_consistently_placed_chunks("//tmp/t6")

        chunk_ids = get("//tmp/t6/@chunk_ids")
        wait(lambda: self._are_chunks_collocated(chunk_ids))

        set("//tmp/t6/@replication_factor", 7)
        wait(lambda: len(get("#{}/@stored_replicas".format(chunk_ids[0]))) == 7)
        wait(lambda: self._are_chunks_collocated(chunk_ids))

        set("//tmp/t6/@replication_factor", 2)
        wait(lambda: len(get("#{}/@stored_replicas".format(chunk_ids[0]))) == 2)

        set("//tmp/t6/@replication_factor", 7)
        wait(lambda: len(get("#{}/@stored_replicas".format(chunk_ids[0]))) == 7)

        wait(self._pull_replication_queues_empty)

    @authors("aleksandra-zh")
    def test_crp_pull_replication_dead_chunk(self):
        set("//sys/@config/chunk_manager/consistent_replica_placement/enable_pull_replication", True)
        self._create_table_with_two_consistently_placed_chunks("//tmp/t6")

        chunk_ids = get("//tmp/t6/@chunk_ids")
        wait(lambda: self._are_chunks_collocated(chunk_ids))

        set("//tmp/t6/@replication_factor", 2)
        wait(lambda: len(get("#{}/@stored_replicas".format(chunk_ids[0]))) == 2)

        set("//tmp/t6/@replication_factor", 7)
        remove("//tmp/t6")

        wait(self._pull_replication_queues_empty)

    @authors("shakurov")
    def test_rf_change_consistency(self):
        self._disable_token_redistribution()

        self._create_table_with_two_consistently_placed_chunks("//tmp/t7")
        chunk_ids = get("//tmp/t7/@chunk_ids")
        wait(lambda: self._are_chunks_collocated(chunk_ids))

        wait(lambda: self._are_chunk_replicas_collocated(
            get("#{}/@stored_replicas".format(chunk_ids[0])),
            get("#{}/@consistent_replica_placement".format(chunk_ids[0]))))

        replicas_before = get("#{}/@stored_replicas".format(chunk_ids[0]))

        set("//tmp/t7/@replication_factor", 7)
        wait(lambda: len(get("#{}/@stored_replicas".format(chunk_ids[0]))) == 7)
        wait(lambda: self._are_chunks_collocated(chunk_ids))

        set("//tmp/t7/@replication_factor", 3)
        wait(lambda: len(get("#{}/@stored_replicas".format(chunk_ids[0]))) == 3)
        wait(lambda: self._are_chunks_collocated(chunk_ids))

        wait(lambda: self._are_chunk_replicas_collocated(
            get("#{}/@stored_replicas".format(chunk_ids[0])),
            get("#{}/@consistent_replica_placement".format(chunk_ids[0]))))

        replicas_after = get("#{}/@stored_replicas".format(chunk_ids[0]))

        assert self._are_chunk_replicas_collocated(replicas_before, replicas_after)

    @authors("shakurov")
    def test_disable_enable(self):
        set("//sys/@config/chunk_manager/consistent_replica_placement/enable", False)
        assert len(ls("//sys/inconsistently_placed_chunks")) == 0

        self._create_table_with_two_consistently_placed_chunks("//tmp/t8")
        chunk_ids = get("//tmp/t8/@chunk_ids")

        set("//sys/@config/chunk_manager/consistent_replica_placement/enable", True)

        wait(lambda: self._are_chunks_collocated(chunk_ids))

    @authors("shakurov")
    def test_enable_disable(self):
        set("//sys/@config/chunk_manager/consistent_replica_placement/enable", True)

        self._create_table_with_two_consistently_placed_chunks("//tmp/t9")
        chunk_ids = get("//tmp/t9/@chunk_ids")
        wait(lambda: self._are_chunks_collocated(chunk_ids))

        set("//sys/@config/chunk_manager/consistent_replica_placement/enable", False)
        assert len(ls("//sys/inconsistently_placed_chunks")) == 0

    @authors("shakurov")
    def test_replica_count_change(self):
        self._disable_token_redistribution()

        self._create_table_with_two_consistently_placed_chunks("//tmp/t10")
        chunk_ids_1 = get("//tmp/t10/@chunk_ids")
        wait(lambda: len(get("#{}/@stored_replicas".format(chunk_ids_1[0]))) == 3)
        wait(lambda: self._are_chunks_collocated(chunk_ids_1))

        self._create_table_with_two_consistently_placed_chunks("//tmp/t11", {"replication_factor": 6})
        chunk_ids_2 = get("//tmp/t11/@chunk_ids")
        wait(lambda: len(get("#{}/@stored_replicas".format(chunk_ids_2[0]))) == 6)
        wait(lambda: self._are_chunks_collocated(chunk_ids_2))

        set("//sys/@config/chunk_manager/consistent_replica_placement/replicas_per_chunk", 50)
        wait(lambda: self._are_chunks_collocated(chunk_ids_1))
        wait(lambda: self._are_chunks_collocated(chunk_ids_2))


##################################################################


class TestConsistentChunkReplicaPlacementSnapshotLoading(TestConsistentChunkReplicaPlacementBase):
    @authors("shakurov")
    def test_after_snapshot_loading_consistency(self):
        self._disable_token_redistribution()

        self._create_table_with_two_consistently_placed_chunks("//tmp/t1")
        chunk_ids = get("//tmp/t1/@chunk_ids")
        wait(lambda: self._are_chunks_collocated(chunk_ids))

        replicas_before = get("#{}/@stored_replicas".format(chunk_ids[0]))

        build_snapshot(cell_id=None)

        with Restarter(self.Env, MASTERS_SERVICE):
            pass

        wait(lambda: self._are_chunks_collocated(chunk_ids))
        replicas_after = get("#{}/@stored_replicas".format(chunk_ids[0]))

        assert self._are_chunk_replicas_collocated(replicas_before, replicas_after)


##################################################################


class TestConsistentChunkReplicaPlacementLeaderSwitch(TestConsistentChunkReplicaPlacementBase):
    @authors("shakurov")
    def test_leader_switch_consistency(self):
        self._disable_token_redistribution()

        self._create_table_with_two_consistently_placed_chunks("//tmp/t1")
        chunk_ids = get("//tmp/t1/@chunk_ids")
        wait(lambda: self._are_chunks_collocated(chunk_ids))

        replicas_before = get("#{}/@stored_replicas".format(chunk_ids[0]))

        old_leader_rpc_address = get_active_primary_master_leader_address(self)
        new_leader_rpc_address = get_active_primary_master_follower_address(self)
        cell_id = get("//sys/@cell_id")
        switch_leader(cell_id, new_leader_rpc_address)
        wait(lambda: is_active_primary_master_leader(new_leader_rpc_address))
        wait(lambda: is_active_primary_master_follower(old_leader_rpc_address))

        wait(lambda: self._are_chunks_collocated(chunk_ids))
        replicas_after = get("#{}/@stored_replicas".format(chunk_ids[0]))

        assert self._are_chunk_replicas_collocated(replicas_before, replicas_after)


##################################################################


class TestChunkWeightStatisticsHistogram(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3

    def _validate_chunk_weight_statistic_histograms(self, histogram_name, attribute_name):
        chunk_ids = get("//tmp/t/@chunk_ids")

        bounds = [1, 2, 4, 8, 16, 32, 64, 125, 250, 500]
        for i in range(0, 28):
            bounds.append(bounds[i] * 1000)

        def find_correct_bucket(bounds, value):
            if value <= bounds[0]:
                return 0

            for i in range(1, len(bounds)):
                if bounds[i - 1] < value <= bounds[i]:
                    return i

            return len(bounds)

        def validate_histogram(histogram_name, attribute_name, bounds, chunk_ids):
            result = []
            for i in range(0, 39):
                result.append(0)

            for chunk_id in chunk_ids:
                value = get("#{0}/@{1}".format(chunk_id, attribute_name))
                result[find_correct_bucket(bounds, value)] += 1

            master_address = ls("//sys/primary_masters")[0]
            profiler = profiler_factory().at_primary_master(master_address)
            histogram = profiler.histogram("chunk_server/histograms/" + histogram_name)

            histogram_bins = histogram.get_bins(verbose=False)
            for i in range(0, 39):
                assert int(histogram_bins[i]['count']) == result[i]

        validate_histogram(histogram_name, attribute_name, bounds, chunk_ids)

    def _check_chunk_weight_statistics_snapshot(self):
        create("table", "//tmp/t")
        value1 = ""
        for i in range(0, 1000):
            value1 += "a"
        value2 = ""
        for i in range(0, 500):
            value2 += "a"
        value3 = ""
        for i in range(0, 50):
            value3 += "a"

        write_table("<append=%true>//tmp/t", {"a": "b"})
        write_table("<append=%true>//tmp/t", {"a": value1})
        write_table("<append=%true>//tmp/t", {"a": value2})
        write_table("<append=%true>//tmp/t", {"a": value3})
        write_table("<append=%true>//tmp/t", {"a": "c"})
        write_table("<append=%true>//tmp/t", {"a": "d"})
        write_table("<append=%true>//tmp/t", {"a": "e"})

        yield

        sleep(10)

        self._validate_chunk_weight_statistic_histograms("chunk_row_count_histogram", "row_count")
        self._validate_chunk_weight_statistic_histograms("chunk_compressed_data_size_histogram", "compressed_data_size")
        self._validate_chunk_weight_statistic_histograms("chunk_uncompressed_data_size_histogram", "uncompressed_data_size")
        self._validate_chunk_weight_statistic_histograms("chunk_data_weight_histogram", "data_weight")

    @authors("h0pless")
    def test_chunk_weight_statistics_histogram(self):
        create("table", "//tmp/t")
        value1 = ""
        for i in range(0, 1000):
            value1 += "a"
        value2 = ""
        for i in range(0, 500):
            value2 += "a"
        value3 = ""
        for i in range(0, 50):
            value3 += "a"

        write_table("<append=%true>//tmp/t", {"a": "b"})
        write_table("<append=%true>//tmp/t", {"a": value1})
        write_table("<append=%true>//tmp/t", {"a": value2})
        write_table("<append=%true>//tmp/t", {"a": value3})
        write_table("<append=%true>//tmp/t", {"a": "c"})
        write_table("<append=%true>//tmp/t", {"a": "d"})
        write_table("<append=%true>//tmp/t", {"a": "e"})

        sleep(10)

        self._validate_chunk_weight_statistic_histograms("chunk_row_count_histogram", "row_count")
        self._validate_chunk_weight_statistic_histograms("chunk_compressed_data_size_histogram", "compressed_data_size")
        self._validate_chunk_weight_statistic_histograms("chunk_uncompressed_data_size_histogram", "uncompressed_data_size")
        self._validate_chunk_weight_statistic_histograms("chunk_data_weight_histogram", "data_weight")

    @authors("h0pless")
    def test_chunk_weight_statistics_histogram_snapshots(self):

        checker_state = iter(self._check_chunk_weight_statistics_snapshot())

        next(checker_state)

        build_snapshot(cell_id=None)

        with Restarter(self.Env, MASTERS_SERVICE):
            pass

        with pytest.raises(StopIteration):
            next(checker_state)


##################################################################


class TestChunkCreationThrottler(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    def _measure_write_time(self, user="root", write_chunk_count=15):
        start = time()
        for _ in range(write_chunk_count):
            write_table("<append=%true>//tmp/t", {"a": 1}, timeout=20, authenticated_user=user)
        return time() - start

    def _measure_operation_time(self, user="root"):
        start = time()
        merge(
            in_="//tmp/in",
            out="//tmp/out",
            authenticated_user=user,
            spec={
                "job_io": {
                    "table_writer": {
                        "desired_chunk_size": 64,
                        "block_size": 64,
                    },
                    "buffer_row_count": 16,
                },
                "job_count": 1,
                "force_transform": True
            },
        )
        return time() - start

    def _check_per_user_throttler_after_snapshot_load(self):
        non_root_user = "juan"

        create("table", "//tmp/t")
        create_user(non_root_user)

        # Warm up.
        self._measure_write_time(non_root_user)
        print_debug("--------->>> warmed up <<<---------")

        usual_time = self._measure_write_time(non_root_user)
        print_debug("usual time:", usual_time)

        # This throttler throttles based on request size, which is rather difficult to dynamically compute.
        # This constant was chosen empirically.
        # If this test becomes too flaky - tuning it might be a good place to start.
        new_limit = 300.

        print_debug("new limit:", new_limit, "bytes/sec")

        set("//sys/@config/chunk_service/enable_per_user_request_bytes_throttling", True)
        set("//sys/users/{}/@chunk_service_request_bytes_throttler".format(non_root_user), {"limit": new_limit})
        sleep(1)

        assert get("//sys/users/{}/@chunk_service_request_bytes_throttler/limit".format(non_root_user)) == new_limit
        assert self._measure_write_time(non_root_user) >= usual_time * 2.5

        yield

        assert get("//sys/users/{}/@chunk_service_request_bytes_throttler/limit".format(non_root_user)) == new_limit

        # Warm up (again).
        self._measure_write_time("root")
        print_debug("--------->>> warmed up <<<---------")

        assert self._measure_write_time(non_root_user) >= usual_time * 2.5

        remove("//sys/users/{}/@chunk_service_request_bytes_throttler/limit".format(non_root_user))
        with pytest.raises(YtError, match="Error resolving path /limit"):
            get("//sys/users/{}/@chunk_service_request_bytes_throttler/limit".format(non_root_user))
        assert self._measure_write_time(non_root_user) < usual_time * 1.2

    @authors("h0pless")
    def test_per_user_bytes_throttler_root(self):
        with pytest.raises(YtError, match="Cannot set \"chunk_service_request_bytes_throttler\" for \"root\""):
            set("//sys/users/root/@chunk_service_request_bytes_throttler", {"limit": 1337})

    @authors("h0pless")
    @flaky(max_runs=5)
    def test_per_user_bytes_throttler_snapshot(self):

        checker_state = iter(self._check_per_user_throttler_after_snapshot_load())

        next(checker_state)

        build_snapshot(cell_id=None)

        with Restarter(self.Env, MASTERS_SERVICE):
            pass

        with pytest.raises(StopIteration):
            next(checker_state)

    @authors("h0pless")
    def test_per_user_bytes_throttler_profiling(self):
        userName = "GregorzBrzeczyszczykiewicz"
        create_user(userName)

        create("table", "//tmp/t")

        set("//sys/@config/chunk_service/enable_per_user_request_bytes_throttling", True)
        set("//sys/users/{}/@chunk_service_request_bytes_throttler".format(userName), {"limit": 300})
        sleep(1)

        master_address = ls("//sys/primary_masters")[0]
        profiler = profiler_factory().at_primary_master(master_address)
        value_counter = profiler.counter("chunk_service/bytes_throttler/value", tags={"user": userName})

        write_table("//tmp/t", {"place": "gmina Grzmiszczoslawice"}, timeout=20, authenticated_user=userName)
        write_table("//tmp/t", {"place": "powiat lekolody"}, timeout=20, authenticated_user=userName)
        wait(lambda: value_counter.get() > 0)

    @authors("h0pless")
    @flaky(max_runs=5)
    def test_per_user_bytes_throttler_default(self):
        throttled_user = "juan"
        non_throttled_user = "not_juan"
        root_user = "root"

        create("table", "//tmp/t")
        create_user(throttled_user)
        create_user(non_throttled_user)

        # Warm up.
        self._measure_write_time(non_throttled_user)
        print_debug("--------->>> warmed up <<<---------")

        usual_time = self._measure_write_time(non_throttled_user)
        print_debug("usual time:", usual_time)

        # This throttler throttles based on request size, which is rather difficult to dynamically compute.
        # This constant was chosen empirically.
        # If this test becomes too flaky - tuning it might be a good place to start.
        new_general_limit = 300.
        as_good_as_infinity = 100000000.

        print_debug("new limit:", new_general_limit, "bytes/sec")
        set("//sys/@config/chunk_service/enable_per_user_request_bytes_throttling", True)
        set("//sys/users/{}/@chunk_service_request_bytes_throttler".format(non_throttled_user), {"limit": as_good_as_infinity})
        set("//sys/@config/chunk_service/default_per_user_request_bytes_throttler_config/limit", new_general_limit)
        sleep(1)

        assert get("//sys/@config/chunk_service/default_per_user_request_bytes_throttler_config/limit") == new_general_limit
        assert get("//sys/users/{}/@chunk_service_request_bytes_throttler/limit".format(non_throttled_user)) == as_good_as_infinity
        assert self._measure_write_time(throttled_user) >= usual_time * 2.5
        assert self._measure_write_time(root_user) < usual_time * 1.2
        assert self._measure_write_time(non_throttled_user) < usual_time * 1.2

        set("//sys/@config/chunk_service/enable_per_user_request_bytes_throttling", False)
        sleep(1)
        assert self._measure_write_time(throttled_user) < usual_time * 1.2

        set("//sys/@config/chunk_service/enable_per_user_request_bytes_throttling", True)
        remove("//sys/@config/chunk_service/default_per_user_request_bytes_throttler_config")
        sleep(1)
        assert self._measure_write_time(throttled_user) < usual_time * 1.2
