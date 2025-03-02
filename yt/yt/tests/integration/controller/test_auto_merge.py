from yt_env_setup import (
    YTEnvSetup, Restarter, SCHEDULERS_SERVICE)

from yt_commands import (
    authors, print_debug, wait, create, get, set, exists,
    create_account, read_table, write_file, sorted_dicts,
    write_table, map, reduce, merge, sync_create_cells, sync_mount_table,
    get_operation, assert_statistics, extract_statistic_v2)

from yt_type_helpers import normalize_schema, make_schema

from yt.common import YtError, update

import yt.yson as yson

import pytest

from time import sleep

##################################################################


@pytest.mark.enabled_multidaemon
class TestSchedulerAutoMergeBase(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_TEST_PARTITIONS = 12
    NUM_MASTERS = 1
    NUM_NODES = 4
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True
    ENABLE_BULK_INSERT = True

    ENABLE_SHALLOW_MERGE = False

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "operations_update_period": 10,
            "running_allocations_update_period": 10,
        },
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 3000,
            "operations_update_period": 10,
            "chunk_unstage_period": 10,
        }
    }

    DELTA_MASTER_CONFIG = {
        "chunk_manager": {
            "allow_multiple_erasure_parts_per_node": True,
        },
    }

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "user_slots": 4,
                "cpu": 4,
                "memory": 4 * 1024 ** 3,
            }
        }
    }

    @classmethod
    def modify_controller_agent_config(cls, config, cluster_index):
        for op_type in ["map", "reduce"]:
            config["controller_agent"] = update(config.get('controller_agent'), {
                op_type + "_operation_options": {
                    "spec_template": {
                        "auto_merge": {
                            "shallow_merge_min_data_weight_per_chunk": 0,
                            "enable_shallow_merge": cls.ENABLE_SHALLOW_MERGE,
                        },
                    },
                },
            })

    def _get_auto_merge_job_counts(self, operation):
        def none_to_zero(value):
            return 0 if value is None else value

        statistics = get(operation.get_path() + "/@progress/job_statistics_v2")
        result = {}
        for state in ("completed", "aborted"):
            result[state] = {
                "shallow": none_to_zero(extract_statistic_v2(
                    statistics,
                    key="time.total",
                    job_state=state,
                    job_type="shallow_auto_merge",
                    summary_type="count")),
                "deep": none_to_zero(extract_statistic_v2(
                    statistics,
                    key="time.total",
                    job_state=state,
                    job_type="auto_merge",
                    summary_type="count"))
            }
        return result

    def _verify_auto_merge_job_types(self, operation, allow_zero_merge_jobs=False):
        job_types = self._get_auto_merge_job_counts(operation)
        if self.ENABLE_SHALLOW_MERGE:
            assert job_types["completed"]["deep"] == 0
            if not allow_zero_merge_jobs:
                assert job_types["completed"]["shallow"] > 0
        else:
            assert job_types["completed"]["shallow"] == 0
            if not allow_zero_merge_jobs:
                assert job_types["completed"]["deep"] > 0

    def _verify_shallow_merge_attempted(self, operation, allow_shallow_jobs=False):
        job_types = self._get_auto_merge_job_counts(operation)
        assert job_types["completed"]["deep"] > 0
        if not allow_shallow_jobs:
            assert job_types["completed"]["shallow"] == 0
        assert job_types["aborted"]["shallow"] > 0

        data_flow_graph = get(operation.get_path() + "/@progress/data_flow_graph")
        data_weight = data_flow_graph["edges"]["map"]["auto_merge"]["statistics"]["data_weight"]
        assert data_weight > 0
        assert data_flow_graph["edges"]["auto_merge"]["sink"]["statistics"]["data_weight"] == data_weight

        if not allow_shallow_jobs:
            assert data_flow_graph["edges"].get("map", {}).get("shallow_auto_merge", {}).get("statistics", {}).get("data_weight", 0) == 0
            assert data_flow_graph["edges"].get("shallow_auto_merge", {}).get("sink", {}).get("statistics", {}).get("data_weight", 0) == 0


class TestSchedulerAutoMerge(TestSchedulerAutoMergeBase):
    ENABLE_MULTIDAEMON = False  # There are component restarts.

    def _create_account(self, chunk_count):
        create_account("acc")
        set("//sys/accounts/acc/@resource_limits/chunk_count", chunk_count)

    def _track_and_report_peak_chunk_count(self, op, with_revive=False):
        peak_chunk_count = 0
        i = 0
        while True:
            suspended = get(op.get_path() + "/@suspended", default=False, verbose=False)
            if suspended:
                print_debug("Operation suspended, trying to re-suspend it")
                op.resume()

            state = op.get_state(verbose=False)
            if state == "completed":
                break
            if op.get_state() == "failed":
                op.track()  # this should raise an exception
            current_chunk_count = get("//sys/accounts/acc/@resource_usage/chunk_count", verbose=False)
            peak_chunk_count = max(peak_chunk_count, current_chunk_count)
            print_debug(f"Peak chunk count: {peak_chunk_count}, current chunk count: {current_chunk_count}")
            sleep(2)
            if with_revive:
                i += 1
                if i == 5:
                    with Restarter(self.Env, SCHEDULERS_SERVICE):
                        pass
                    i = 0
        print_debug(f"Peak chunk count: {peak_chunk_count}")

    # Bugs in auto-merge usually lead to the operation being stuck without scheduling any new jobs.
    # This is why we use the pytest timeout decorator.
    @authors("max42")
    @pytest.mark.timeout(480)
    @pytest.mark.parametrize("op_type", ["map", "reduce"])
    @pytest.mark.skipif(True, reason="Currently too slow; max42@ is investigating")
    def test_auto_merge_does_not_stuck(self, op_type):
        create(
            "table",
            "//tmp/t_in",
            attributes={"schema": [{"name": "a", "type": "int64", "sort_order": "ascending"}]},
        )
        create("table", "//tmp/t_out")

        parameters = [
            [1, 1, 1],
            [9, 1, 1],
            [9, 9, 1],
            [9, 9, 9],
            [9, 4, 2],
            [16, 5, 3],
            [100, 28, 14],
            [9, 20, 15],
            [9, 20, 3],
        ]

        run_op = map if op_type == "map" else reduce

        for (
            row_count,
            max_intermediate_chunk_count,
            chunk_count_per_merge_job,
        ) in parameters:
            write_table(
                "//tmp/t_in",
                [{"a": i} for i in range(row_count)],
                max_row_buffer_size=1,
                table_writer={"desired_chunk_size": 1},
            )

            assert get("//tmp/t_in/@chunk_count") == row_count

            op = run_op(
                track=False,
                in_="//tmp/t_in",
                out="//tmp/t_out",
                command="cat",
                reduce_by=["a"],
                spec={
                    "auto_merge": {
                        "mode": "manual",
                        "max_intermediate_chunk_count": max_intermediate_chunk_count,
                        "chunk_count_per_merge_job": chunk_count_per_merge_job,
                    },
                    "data_size_per_job": 1,
                },
            )
            op.track()
            self._verify_auto_merge_job_types(op, allow_zero_merge_jobs=True)
            assert (
                get("//tmp/t_out/@chunk_count")
                == (row_count - 1) // min(chunk_count_per_merge_job, max_intermediate_chunk_count) + 1
            )
            assert get("//tmp/t_out/@row_count") == row_count

    @authors("max42")
    @pytest.mark.timeout(480)
    @pytest.mark.parametrize("op_type", ["map", "reduce"])
    def test_account_chunk_limit(self, op_type):
        self._create_account(35)

        create(
            "table",
            "//tmp/t_in",
            attributes={"schema": [{"name": "a", "type": "int64", "sort_order": "ascending"}]},
        )
        create("table", "//tmp/t_out")

        row_count = 300
        write_table(
            "//tmp/t_in",
            [{"a": i} for i in range(row_count)],
            max_row_buffer_size=1,
            table_writer={"desired_chunk_size": 1},
        )

        assert get("//tmp/t_in/@chunk_count") == row_count

        run_op = map if op_type == "map" else reduce
        op = run_op(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            reduce_by=["a"],  # ignored for maps
            command="cat",
            spec={
                "auto_merge": {
                    "mode": "manual",
                    "max_intermediate_chunk_count": 35,
                    "chunk_count_per_merge_job": 20,
                    "use_intermediate_data_account": True,
                },
                "data_size_per_job": 1,
                "suspend_operation_if_account_limit_exceeded": True,
                "intermediate_data_account": "acc",
            },
        )

        self._track_and_report_peak_chunk_count(op)
        self._verify_auto_merge_job_types(op)

        assert get("//tmp/t_out/@row_count") == row_count

    @authors("max42")
    def test_several_auto_merge_output_tables(self):
        self._create_account(20)

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out1")
        create("table", "//tmp/t_out2")

        row_count = 100
        write_table("//tmp/t_in", [{"a": i} for i in range(row_count)])

        op = map(
            track=False,
            in_="//tmp/t_in",
            out=["//tmp/t_out1", "//tmp/t_out2"],
            command="read x; echo $x >&$(($x % 2 * 3 + 1))",
            format="<columns=[a]>schemaful_dsv",
            spec={
                "auto_merge": {
                    "mode": "manual",
                    "max_intermediate_chunk_count": 20,
                    "chunk_count_per_merge_job": 15,
                    "use_intermediate_data_account": True,
                },
                "mapper": {"format": yson.loads(b"<columns=[a]>schemaful_dsv")},
                "data_size_per_job": 1,
                "suspend_operation_if_account_limit_exceeded": True,
                "intermediate_data_account": "acc",
            },
        )

        self._track_and_report_peak_chunk_count(op)
        self._verify_auto_merge_job_types(op)

        assert get("//tmp/t_out1/@row_count") == row_count // 2
        assert get("//tmp/t_out2/@row_count") == row_count // 2

    @authors("max42")
    @pytest.mark.timeout(480)
    @pytest.mark.parametrize("with_revive", [True, False])
    def test_only_auto_merge_output_table(self, with_revive):
        chunk_limit = 1000 if with_revive else 20
        self._create_account(chunk_limit)

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out1")
        create("table", "//tmp/t_out2")

        row_count = 100
        write_table("//tmp/t_in", [{"a": i} for i in range(row_count)])

        op = map(
            track=False,
            in_="//tmp/t_in",
            out=["<auto_merge=%false>//tmp/t_out1", "//tmp/t_out2"],
            command="read x; if [[ $(($x % 10)) == 0 ]]; then echo $x >&1; else echo $x >&4; fi",
            spec={
                "auto_merge": {
                    "mode": "manual",
                    "max_intermediate_chunk_count": 20,
                    "chunk_count_per_merge_job": 15,
                    "use_intermediate_data_account": True,
                },
                "mapper": {"format": yson.loads(b"<columns=[a]>schemaful_dsv")},
                "data_size_per_job": 1,
                "suspend_operation_if_account_limit_exceeded": True,
                "intermediate_data_account": "acc",
            },
        )

        self._track_and_report_peak_chunk_count(op, with_revive=with_revive)
        self._verify_auto_merge_job_types(op)

        assert get("//tmp/t_out1/@row_count") == row_count // 10
        assert get("//tmp/t_out2/@row_count") == row_count * 9 // 10

    @authors("max42", "ermolovd")
    @pytest.mark.timeout(240)
    def test_auto_merge_with_schema_and_append(self):
        schema_in = make_schema(
            [
                {
                    "name": "a",
                    "type": "int64",
                    "sort_order": "ascending",
                },
                {"name": "b", "type": "string"},
            ],
            unique_keys=False,
            strict=True,
        )
        create("table", "//tmp/t_in", attributes={"schema": schema_in})

        schema_out = make_schema(
            [
                {
                    "name": "a",
                    "type": "int64",
                    "required": False,
                },
                {
                    "name": "b",
                    "type": "string",
                    "required": False,
                },
            ],
            unique_keys=False,
            strict=True,
        )
        create("table", "//tmp/t_out", attributes={"schema": schema_out})

        data = [{"a": i, "b": str(i * i)} for i in range(10)]
        write_table("<append=%true>//tmp/t_in", data)

        init_content = [{"a": -1, "b": "1"}]
        write_table("<append=%true>//tmp/t_out", init_content)

        op = map(
            in_="//tmp/t_in",
            out=["<append=%true>//tmp/t_out"],
            command="cat",
            spec={
                "auto_merge": {
                    "mode": "manual",
                    "max_intermediate_chunk_count": 4,
                    "chunk_count_per_merge_job": 2,
                },
                "data_size_per_job": 1,
            },
        )
        self._verify_auto_merge_job_types(op)

        assert get("//tmp/t_out/@row_count") == 11
        assert get("//tmp/t_out/@chunk_count") == 6
        content = read_table("//tmp/t_out")
        assert sorted_dicts(content[1:]) == sorted_dicts(data)
        assert content[:1] == init_content
        assert normalize_schema(get("//tmp/t_out/@schema")) == schema_out

    @authors("yuryalekseev", "psushin")
    @pytest.mark.timeout(60)
    @pytest.mark.parametrize("teleport_by", ["size", "weight"])
    def test_teleport_large_chunks(self, teleport_by):
        max_chunk_size_or_weight = 1024

        spec = {
            "auto_merge": {
                "mode": "manual",
                "max_intermediate_chunk_count": 5,
                "chunk_count_per_merge_job": 5,
            },
            "data_size_per_job": 1,
            "mapper": {"format": yson.loads(b"<columns=[a]>schemaful_dsv")},
        }

        if teleport_by == "size":
            # Chunks are teleported because of chunk_size_threshold.
            spec["auto_merge"].update({"chunk_size_threshold": max_chunk_size_or_weight})
            spec["auto_merge"].update({"job_io": {"table_writer": {"desired_chunk_weight": 4 * max_chunk_size_or_weight}}})
        else:
            # Chunks are teleported because of desired_chunk_weight.
            spec["auto_merge"].update({"chunk_size_threshold": max_chunk_size_or_weight})
            spec["auto_merge"].update({"job_io": {"table_writer": {"desired_chunk_weight": max_chunk_size_or_weight}}})

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        for i in range(15):
            write_table("<append=%true>//tmp/t_in", [{"a": i}])

        # For even lines output a long random string, for odd lines output a single character.
        op = map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            # First 8 jobs produce teleportable output, to ensure that teleports are handled correctly and operation doesn't get stuck.
            command="read x; if (($YT_JOB_INDEX < 8)); then head -c {} /dev/urandom | base64 -w 0; echo -ne '\n'; else echo $x; fi".format(10 * max_chunk_size_or_weight),
            spec=spec,
        )

        op.track()

        self._verify_auto_merge_job_types(op)
        assert get("//tmp/t_out/@chunk_count") == 10
        chunk_ids = get("//tmp/t_out/@chunk_ids")
        row_counts = []
        for chunk_id in chunk_ids:
            row_counts.append(get("#{0}/@row_count".format(chunk_id)))
        row_counts = sorted(row_counts)
        assert row_counts == [1] * 8 + [2, 5]

        data_flow = get_operation(op.id, attributes=["progress"])["progress"]["data_flow"]
        directions = {
            (direction["source_name"], direction["target_name"]) : direction
            for direction in data_flow
        }

        merge_vertex = "shallow_auto_merge" if self.ENABLE_SHALLOW_MERGE else "auto_merge"

        assert len(directions) == 3
        assert directions[("input", "map")]["job_data_statistics"]["chunk_count"] == 0
        assert directions[("input", "map")]["teleport_data_statistics"]["chunk_count"] == 15
        assert directions[("map", merge_vertex)]["job_data_statistics"]["chunk_count"] == 15
        assert directions[("map", merge_vertex)]["teleport_data_statistics"]["chunk_count"] == 0
        assert directions[(merge_vertex, "output")]["job_data_statistics"]["chunk_count"] == 2
        assert directions[(merge_vertex, "output")]["teleport_data_statistics"]["chunk_count"] == 8

    @authors("yuryalekseev")
    @pytest.mark.timeout(60)
    @pytest.mark.parametrize("single_chunk_teleport_strategy", ["enabled", "disabled"])
    def test_teleport_single_chunk(self, single_chunk_teleport_strategy):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("<append=%true>//tmp/t_in", [{"a": 1}])

        op = map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="cat",
            spec={
                "auto_merge": {
                    "mode": "manual",
                    "max_intermediate_chunk_count": 5,
                    "chunk_count_per_merge_job": 5,
                    "single_chunk_teleport_strategy": single_chunk_teleport_strategy,
                },
                "data_size_per_job": 1,
                "mapper": {"format": yson.loads(b"<columns=[a]>schemaful_dsv")},
            },
        )

        op.track()

        assert get("//tmp/t_out/@chunk_count") == 1

        data_flow = get_operation(op.id, attributes=["progress"])["progress"]["data_flow"]
        directions = {
            (direction["source_name"], direction["target_name"]) : direction
            for direction in data_flow
        }
        merge_vertex = "shallow_auto_merge" if self.ENABLE_SHALLOW_MERGE else "auto_merge"

        assert len(directions) == 3

        if single_chunk_teleport_strategy == "enabled":
            assert directions[("input", "map")]["job_data_statistics"]["chunk_count"] == 0
            assert directions[("input", "map")]["teleport_data_statistics"]["chunk_count"] == 1
            assert directions[("map", merge_vertex)]["job_data_statistics"]["chunk_count"] == 1
            assert directions[("map", merge_vertex)]["teleport_data_statistics"]["chunk_count"] == 0
            assert directions[(merge_vertex, "output")]["job_data_statistics"]["chunk_count"] == 0
            assert directions[(merge_vertex, "output")]["teleport_data_statistics"]["chunk_count"] == 1

            assert directions[("input", "map")]["job_data_statistics"]["row_count"] == 0
            assert directions[("input", "map")]["teleport_data_statistics"]["row_count"] == 1
            assert directions[("map", merge_vertex)]["job_data_statistics"]["row_count"] == 1
            assert directions[("map", merge_vertex)]["teleport_data_statistics"]["row_count"] == 0
            assert directions[(merge_vertex, "output")]["job_data_statistics"]["row_count"] == 0
            assert directions[(merge_vertex, "output")]["teleport_data_statistics"]["row_count"] == 1
        else:
            assert directions[("input", "map")]["job_data_statistics"]["chunk_count"] == 0
            assert directions[("input", "map")]["teleport_data_statistics"]["chunk_count"] == 1
            assert directions[("map", merge_vertex)]["job_data_statistics"]["chunk_count"] == 1
            assert directions[("map", merge_vertex)]["teleport_data_statistics"]["chunk_count"] == 0
            assert directions[(merge_vertex, "output")]["job_data_statistics"]["chunk_count"] == 1
            assert directions[(merge_vertex, "output")]["teleport_data_statistics"]["chunk_count"] == 0

            assert directions[("input", "map")]["job_data_statistics"]["row_count"] == 0
            assert directions[("input", "map")]["teleport_data_statistics"]["row_count"] == 1
            assert directions[("map", merge_vertex)]["job_data_statistics"]["row_count"] == 1
            assert directions[("map", merge_vertex)]["teleport_data_statistics"]["row_count"] == 0
            assert directions[(merge_vertex, "output")]["job_data_statistics"]["row_count"] == 1
            assert directions[(merge_vertex, "output")]["teleport_data_statistics"]["row_count"] == 0

    @authors("max42")
    @pytest.mark.timeout(60)
    @pytest.mark.ignore_in_opensource_ci
    def test_erasure_output(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        set("//tmp/t_out/@erasure_codec", "lrc_12_2_2")
        for i in range(10):
            write_table("<append=%true>//tmp/t_in", [{"a": i}])

        map(
            in_="//tmp/t_in",
            out=["//tmp/t_out"],
            command="cat",
            spec={
                "auto_merge": {
                    "mode": "manual",
                    "chunk_count_per_merge_job": 2,
                    "max_intermediate_chunk_count": 100,
                },
                "data_size_per_job": 1,
                "mapper": {"format": yson.loads(b"<columns=[a]>schemaful_dsv")},
            },
        )
        # Shallow merge for erasure chunks is not supported now (see YT-15431).
        # TODO(gepardo): Uncomment the following line when shallow merge of erasure chunks is supported.
        # self._verify_auto_merge_job_types(op)

        assert get("//tmp/t_out/@chunk_count") == 5
        chunk_ids = get("//tmp/t_out/@chunk_ids")
        for chunk_id in chunk_ids:
            assert get("#{0}/@erasure_codec".format(chunk_id)) == "lrc_12_2_2"

    @authors("max42")
    @pytest.mark.timeout(60)
    def test_replicated_and_compressed_output(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        set("//tmp/t_out/@replication_factor", 5)
        set("//tmp/t_out/@compression_codec", "zstd_17")
        for i in range(10):
            write_table("<append=%true>//tmp/t_in", [{"a": i}])

        op = map(
            in_="//tmp/t_in",
            out=["//tmp/t_out"],
            command="cat",
            spec={
                "auto_merge": {
                    "mode": "manual",
                    "chunk_count_per_merge_job": 2,
                    "max_intermediate_chunk_count": 100,
                },
                "data_size_per_job": 1,
                "mapper": {"format": yson.loads(b"<columns=[a]>schemaful_dsv")},
            },
        )
        self._verify_auto_merge_job_types(op)
        assert get("//tmp/t_out/@chunk_count") == 5
        chunk_ids = get("//tmp/t_out/@chunk_ids")
        for chunk_id in chunk_ids:
            assert get("#{0}/@media/default/replication_factor".format(chunk_id)) == 5
            assert get("#{0}/@compression_codec".format(chunk_id)) == "zstd_17"

    @authors("max42")
    @pytest.mark.timeout(60)
    def test_row_count_limit_disables_auto_merge(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        for i in range(10):
            write_table("<append=%true>//tmp/t_in", [{"a": i}])

        op = map(
            in_="//tmp/t_in",
            out=["<row_count_limit=5>//tmp/t_out"],
            command="cat",
            spec={
                "auto_merge": {
                    "mode": "manual",
                    "chunk_count_per_merge_job": 4,
                    "max_intermediate_chunk_count": 100,
                },
                "data_size_per_job": 1,
                "mapper": {"format": yson.loads(b"<columns=[a]>schemaful_dsv")},
            },
        )
        assert get("//tmp/t_out/@chunk_count") >= 5
        assert "auto_merge_disabled" in op.get_alerts()

    @authors("max42")
    @pytest.mark.timeout(60)
    def test_sorted_output_disables_auto_merge(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        schema_out = make_schema(
            [
                {
                    "name": "a",
                    "type": "string",
                    "sort_order": "ascending",
                },
            ],
            unique_keys=False,
            strict=True,
        )
        create("table", "//tmp/t_out_with_schema", attributes={"schema": schema_out})

        for i in range(10):
            write_table("<append=%true>//tmp/t_in", [{"a": i}])

        op = map(
            in_="//tmp/t_in",
            out=["<sorted_by=[a]>//tmp/t_out"],
            command="cat",
            spec={
                "auto_merge": {
                    "mode": "manual",
                    "chunk_count_per_merge_job": 4,
                    "max_intermediate_chunk_count": 100,
                },
                "data_size_per_job": 1,
                "mapper": {"format": yson.loads(b"<columns=[a]>schemaful_dsv")},
            },
        )
        assert get("//tmp/t_out/@chunk_count") >= 5
        assert "auto_merge_disabled" in op.get_alerts()

        op = map(
            in_="//tmp/t_in",
            out=["//tmp/t_out_with_schema"],
            command="cat",
            spec={
                "auto_merge": {
                    "mode": "manual",
                    "chunk_count_per_merge_job": 4,
                    "max_intermediate_chunk_count": 100,
                },
                "data_size_per_job": 1,
                "mapper": {"format": yson.loads(b"<columns=[a]>schemaful_dsv")},
            },
        )
        assert get("//tmp/t_out_with_schema/@chunk_count") >= 5
        assert "auto_merge_disabled" in op.get_alerts()

    @authors("max42")
    @pytest.mark.timeout(60)
    def test_live_preview(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out1")
        create("table", "//tmp/t_out2")

        row_count = 50
        write_table("//tmp/t_in", [{"a": i} for i in range(row_count)])

        op = map(
            track=False,
            in_="//tmp/t_in",
            out=["//tmp/t_out1", "//tmp/t_out2"],
            command="read x; echo $x >&$(($x % 2 * 3 + 1))",
            format="<columns=[a]>schemaful_dsv",
            spec={
                "auto_merge": {
                    "mode": "manual",
                    "max_intermediate_chunk_count": 20,
                    "chunk_count_per_merge_job": 15,
                },
                "mapper": {"format": yson.loads(b"<columns=[a]>schemaful_dsv")},
                "data_size_per_job": 1,
            },
        )

        # We check that each of 4 possible live previews was non-empty at some moment.
        live_preview_appeared = {"map": [False, False], "auto_merge": [False, False]}

        while True:
            state = op.get_state(verbose=False)
            if state == "completed":
                break
            elif state == "failed":
                op.track()  # this should raise an exception
            for i in range(2):
                for vertex in live_preview_appeared:
                    path = op.get_path() + "/orchid/data_flow_graph/vertices/{0}/live_previews/{1}".format(vertex, i)
                    try:
                        if not exists(path, verbose=False):
                            continue
                        data = read_table(
                            path,
                            verbose=False,
                            table_reader={"unavailable_chunk_strategy": "skip"},
                        )
                        if len(data) > 0 and not live_preview_appeared[vertex][i]:
                            print_debug(f"Live preview of type {vertex} and index {i} appeared")
                        live_preview_appeared[vertex][i] = True
                    except YtError:
                        pass
            sleep(0.5)
            print_debug(f"{op.get_job_count('completed')} jobs completed")

            if all(live_preview_appeared["map"]) and all(live_preview_appeared["auto_merge"]):
                break

        op.track()
        self._verify_auto_merge_job_types(op)

    @authors("ifsmirnov")
    def test_unversioned_update_no_auto_merge(self):
        sync_create_cells(1)
        create(
            "table",
            "//tmp/t_out",
            attributes={
                "dynamic": True,
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ],
            },
        )
        sync_mount_table("//tmp/t_out")
        create("table", "//tmp/t_in")

        rows = [
            {"key": 1, "$change_type": 0, "$value:value": "a"},
            {"key": 2, "$change_type": 0, "$value:value": "b"},
        ]
        versioned_rows = [
            {"key": 1, "value": "a"},
            {"key": 2, "value": "b"},
        ]
        write_table("//tmp/t_in", rows)

        map(
            in_="//tmp/t_in",
            out="<append=%true;schema_modification=unversioned_update>//tmp/t_out",
            command="cat",
            spec={
                "auto_merge": {"mode": "relaxed"},
                "job_count": 2,
            },
        )

        assert read_table("//tmp/t_out") == versioned_rows
        wait(lambda: get("//tmp/t_out/@chunk_count") == 2)

    @authors("gritukan")
    def test_auto_merge_disabled_in_unordered_merge(self):
        create("table", "//tmp/in")
        create("table", "//tmp/out")
        write_table("//tmp/in", [{"x": 1}, {"x": 2}])

        op = merge(
            mode="unordered",
            in_=["//tmp/in"],
            out="//tmp/out",
            spec={
                "auto_merge": {
                    "mode": "relaxed",
                },
            },
        )
        op.track()
        assert read_table("//tmp/out") == [{"x": 1}, {"x": 2}]
        assert "auto_merge_disabled" in op.get_alerts()

    @authors("cookiedoth")
    def test_transaction_chunk_usage(self):
        self._create_account(35)

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        row_count = 300
        write_table(
            "//tmp/t_in",
            [{"a": i} for i in range(row_count)],
            max_row_buffer_size=1,
            table_writer={"desired_chunk_size": 1},
        )
        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="cat",
            spec={
                "auto_merge": {
                    "mode": "manual",
                    "max_intermediate_chunk_count": 35,
                    "chunk_count_per_merge_job": 20,
                    "use_intermediate_data_account": True,
                },
                "data_size_per_job": 1,
                "suspend_operation_if_account_limit_exceeded": True,
                "intermediate_data_account": "acc",
            }
        )

        wait(lambda: op.get_job_count("completed") >= 100, None, 300)
        op.suspend()

        if (exists(op.get_path())):
            tx = get(op.get_path() + "/@output_transaction_id")
            transaction_chunk_count = get("#" + tx + "/@resource_usage/acc/chunk_count")
            chunk_count = get("//sys/accounts/acc/@resource_usage/chunk_count")
            print_debug(f"Chunk count: {chunk_count}")
            print_debug(f"Transaction chunk count: {transaction_chunk_count}")
            assert abs(transaction_chunk_count - chunk_count) <= 10
            assert transaction_chunk_count <= 40
            assert chunk_count <= 40

    @authors("gepardo")
    @pytest.mark.parametrize("op_type", ["map", "reduce"])
    def test_progress_yson(self, op_type):
        create("table", "//tmp/t_in",
               attributes={"schema": [{"name": "a", "type": "int64", "sort_order": "ascending"}]})
        create("table", "//tmp/t_out")
        for i in range(10):
            write_table("<append=%true>//tmp/t_in", [{"a": i}])

        run_op = map if op_type == "map" else reduce
        op = run_op(
            in_="//tmp/t_in",
            out=["//tmp/t_out"],
            command="cat",
            reduce_by=["a"],  # ignored for maps
            spec={
                "auto_merge": {
                    "mode": "manual",
                    "chunk_count_per_merge_job": 2,
                    "max_intermediate_chunk_count": 100,
                },
                "data_size_per_job": 1,
                "mapper": {"format": yson.loads(b"<columns=[a]>schemaful_dsv")},
            },
        )
        self._verify_auto_merge_job_types(op)

        merge_name = "shallow_auto_merge" if self.ENABLE_SHALLOW_MERGE else "auto_merge"
        op_name = "sorted_reduce" if op_type == "reduce" else "map"
        data_flow_graph = get(op.get_path() + "/@progress/data_flow_graph")
        data_weight = data_flow_graph["edges"][op_name][merge_name]["statistics"]["data_weight"]
        assert data_weight > 0
        assert data_flow_graph["edges"][merge_name]["sink"]["statistics"]["data_weight"] == data_weight

        tasks = get(op.get_path() + "/@progress/tasks")
        assert len(tasks) == 2
        assert tasks[0]["task_name"] == ("map" if op_type == "map" else "sorted_reduce")
        assert tasks[1]["task_name"] == "auto_merge"

        map_job_count = tasks[0]["job_counter"]["completed"]["total"]
        auto_merge_job_count = tasks[1]["job_counter"]["completed"]["total"]
        assert map_job_count == 10
        assert auto_merge_job_count > 0

        total_job_counter = get(op.get_path() + "/@progress/total_job_counter")
        print(total_job_counter)
        assert total_job_counter["completed"]["total"] == map_job_count + auto_merge_job_count

    @authors("gepardo")
    def test_aggregated_statistics(self):
        create("table", "//tmp/t_in",
               attributes={"schema": [{"name": "a", "type": "int64", "sort_order": "ascending"}]})
        create("table", "//tmp/t_out")
        for i in range(10):
            write_table("<append=%true>//tmp/t_in", [{"a": i}])

        op = map(
            in_="//tmp/t_in",
            out=["//tmp/t_out"],
            command="cat",
            spec={
                "auto_merge": {
                    "mode": "manual",
                    "chunk_count_per_merge_job": 2,
                    "max_intermediate_chunk_count": 100,
                },
                "data_size_per_job": 1,
                "mapper": {"format": yson.loads(b"<columns=[a]>schemaful_dsv")},
            },
        )
        self._verify_auto_merge_job_types(op)

        merge_type = "shallow_auto_merge" if self.ENABLE_SHALLOW_MERGE else "auto_merge"
        wrong_merge_type = "auto_merge" if self.ENABLE_SHALLOW_MERGE else "shallow_auto_merge"
        wait(lambda: assert_statistics(
            operation=op,
            key="data.output.0.compressed_data_size",
            assertion=lambda data_transmitted: data_transmitted > 0,
            job_type=merge_type))
        wait(lambda: assert_statistics(
            operation=op,
            key="data.output.0.compressed_data_size",
            assertion=lambda data_transmitted_to_wrong_merge: data_transmitted_to_wrong_merge in [None, 0],
            job_type=wrong_merge_type))


class TestSchedulerShallowAutoMerge(TestSchedulerAutoMerge):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    ENABLE_SHALLOW_MERGE = True


@pytest.mark.enabled_multidaemon
class TestSchedulerAutoMergeAborted(TestSchedulerAutoMergeBase):
    ENABLE_MULTIDAEMON = True
    ENABLE_SHALLOW_MERGE = True

    @authors("gepardo")
    def test_shallow_to_deep_merge_switch(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        data = [{"a": i, "b": str(i * i)} for i in range(10)]
        write_table("//tmp/t_in", data)

        op = map(
            in_="//tmp/t_in",
            out=["//tmp/t_out"],
            command="cat",
            spec={
                "auto_merge": {
                    "mode": "manual",
                    "max_intermediate_chunk_count": 4,
                    "chunk_count_per_merge_job": 2,
                },
                "data_size_per_job": 1,
                "job_testing_options": {"throw_in_shallow_merge": True}
            },
        )
        self._verify_shallow_merge_attempted(op)

        assert get("//tmp/t_out/@row_count") == 10
        assert get("//tmp/t_out/@chunk_count") == 5
        content = read_table("//tmp/t_out")
        assert sorted_dicts(content) == sorted_dicts(data)

    @authors("gepardo")
    @pytest.mark.ignore_in_opensource_ci
    def test_incompatible_metas_switch(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        data = [{"a": i} for i in range(10)]
        write_table("//tmp/t_in", data)

        mapper = b"""
import json
import sys

input = json.loads(sys.stdin.readline())
if input["a"] % 2 == 0:
    print(json.dumps({"a": input["a"], "b": input["a"] // 2}))
else:
    print(json.dumps({"a": input["a"], "c": input["a"] * 3 + 1}))
"""
        create("file", "//tmp/mapper.py")
        write_file("//tmp/mapper.py", mapper)

        op = map(
            in_="//tmp/t_in",
            out=["//tmp/t_out"],
            command="python3 mapper.py",
            file="//tmp/mapper.py",
            spec={
                "auto_merge": {
                    "mode": "manual",
                    "max_intermediate_chunk_count": 4,
                    "chunk_count_per_merge_job": 2,
                },
                "mapper": {
                    "input_format": "json",
                    "output_format": "json",
                },
                "data_size_per_job": 1
            },
        )
        self._verify_shallow_merge_attempted(op, allow_shallow_jobs=True)

        expected_data = [{"a": i, "b": i // 2} if i % 2 == 0 else {"a": i, "c": i * 3 + 1} for i in range(10)]
        assert get("//tmp/t_out/@row_count") == 10
        assert get("//tmp/t_out/@chunk_count") == 5
        output_data = read_table("//tmp/t_out")
        assert sorted_dicts(expected_data) == sorted_dicts(output_data)

    @authors("gepardo")
    def test_too_many_blocks_for_shallow_merge(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        data = [{"a": i, "b": str(i * i)} for i in range(500)]
        write_table("//tmp/t_in", data)

        op = map(
            in_="//tmp/t_in",
            out=["//tmp/t_out"],
            command="cat",
            spec={
                "auto_merge": {
                    "mode": "manual",
                    "max_intermediate_chunk_count": 4,
                    "chunk_count_per_merge_job": 2,
                    "max_block_count": 5,
                },
                "job_io": {
                    "table_writer": {
                        "block_size": 32,
                    },
                },
                "data_size_per_job": 1024,
            },
        )
        self._verify_shallow_merge_attempted(op)

        assert get("//tmp/t_out/@row_count") == 500
        content = read_table("//tmp/t_out")
        assert sorted_dicts(content) == sorted_dicts(data)

    @authors("gepardo")
    def test_too_small_chunks_for_shallow_merge(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        data = [{"a": i, "b": str(i * i)} for i in range(10)]
        write_table("//tmp/t_in", data)

        op = map(
            in_="//tmp/t_in",
            out=["//tmp/t_out"],
            command="cat",
            spec={
                "auto_merge": {
                    "mode": "relaxed",
                    "shallow_merge_min_data_weight_per_chunk": 10 ** 7,
                },
                "data_size_per_job": 1,
            },
        )
        job_types = self._get_auto_merge_job_counts(op)
        assert job_types["completed"]["deep"] > 0
        assert job_types["completed"]["shallow"] == 0

        content = read_table("//tmp/t_out")
        assert sorted_dicts(content) == sorted_dicts(data)

        op2 = map(
            in_="//tmp/t_in",
            out=["//tmp/t_out"],
            command="cat",
            spec={
                "auto_merge": {
                    "mode": "relaxed",
                    "shallow_merge_min_data_weight_per_chunk": 0,
                },
                "data_size_per_job": 1,
            },
        )
        self._verify_auto_merge_job_types(op2)

        content = read_table("//tmp/t_out")
        assert sorted_dicts(content) == sorted_dicts(data)
