from yt_env_setup import (
    YTEnvSetup,
    Restarter,
    SCHEDULERS_SERVICE,
    CONTROLLER_AGENTS_SERVICE,
)
from yt_commands import (
    authors, create_test_tables, extract_statistic_v2, extract_deprecated_statistic,
    print_debug, wait, wait_breakpoint, release_breakpoint, with_breakpoint,
    create, ls, get, set, copy, move, remove, exists,
    create_user, create_pool,
    start_transaction, abort_transaction,
    read_table, write_table, read_file,
    map, merge, sort, get_job,
    run_test_vanilla, run_sleeping_vanilla, get_job_fail_context, dump_job_context,
    get_singular_chunk_id, PrepareTables,
    raises_yt_error, update_scheduler_config, update_controller_agent_config,
    assert_statistics, sorted_dicts,
    set_node_banned, disable_scheduler_jobs_on_node, enable_scheduler_jobs_on_node,
    update_nodes_dynamic_config)

import yt_error_codes

from yt_type_helpers import make_schema

from yt_scheduler_helpers import scheduler_orchid_path

import yt.yson as yson

from yt.wrapper import JsonFormat
from yt.common import date_string_to_timestamp, YtError

import pytest

import io
import time
import zstandard as zstd
import itertools


##################################################################


class TestSchedulerCommon(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_TEST_PARTITIONS = 4
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "operations_update_period": 10,
            "running_allocations_update_period": 10,
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 500,
            "operations_update_period": 10,
            "map_operation_options": {
                "job_splitter": {
                    "min_job_time": 5000,
                    "min_total_data_size": 1024,
                    "update_period": 100,
                    "candidate_percentile": 0.8,
                    "max_jobs_per_split": 3,
                },
            },
            "controller_throttling_log_backoff": 0,
        }
    }

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "user_slots": 5,
                "cpu": 5,
                "memory": 5 * 1024 ** 3,
            }
        }
    }
    USE_PORTO = False

    @authors("ignat")
    def test_failed_jobs_twice(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"} for _ in range(200)])

        op = map(
            track=False,
            in_="//tmp/t1",
            out="//tmp/t2",
            command='trap "" HUP; bash -c "sleep 60" &; sleep $[( $RANDOM % 5 )]s; exit 42;',
            spec={"max_failed_job_count": 1, "job_count": 200},
        )

        with pytest.raises(YtError):
            op.track()

        for job_id in op.list_jobs():
            job_desc = get_job(op.id, job_id)
            if job_desc["state"] == "running":
                continue
            assert "Process exited with code " in job_desc["error"]["inner_errors"][0]["message"]

    @authors("ignat")
    def test_job_progress(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"} for _ in range(10)])

        op = map(
            track=False,
            label="job_progress",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"),
            spec={"test_flag": yson.to_yson_type("value", attributes={"attr": 0})},
        )

        jobs = wait_breakpoint()
        progress = get(op.get_path() + "/controller_orchid/running_jobs/" + jobs[0] + "/progress")
        assert progress >= 0

        test_flag = get("//sys/scheduler/orchid/scheduler/operations/{0}/spec/test_flag".format(op.id))
        assert str(test_flag) == "value"
        assert test_flag.attributes == {"attr": 0}

        release_breakpoint()
        op.track()

    @authors("ermolovd")
    def test_job_stderr_size(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"} for _ in range(10)])

        op = map(
            track=False,
            label="job_progress",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("echo FOOBAR >&2 ; BREAKPOINT; cat"),
        )

        jobs = wait_breakpoint()

        def get_stderr_size():
            return get(op.get_path() + "/controller_orchid/running_jobs/" + jobs[0] + "/stderr_size")

        wait(lambda: get_stderr_size() == len("FOOBAR\n"))

        release_breakpoint()
        op.track()

    @authors("ignat")
    def test_estimated_statistics(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"key": i} for i in range(5)])

        sort(in_="//tmp/t1", out="//tmp/t1", sort_by="key")
        op = map(command="cat", in_="//tmp/t1[:1]", out="//tmp/t2")

        statistics = get(op.get_path() + "/@progress/estimated_input_statistics")
        for key in [
            "uncompressed_data_size",
            "compressed_data_size",
            "row_count",
            "data_weight",
        ]:
            assert statistics[key] > 0
        assert statistics["unavailable_chunk_count"] == 0
        assert statistics["chunk_count"] == 1

    @authors("ignat")
    def test_invalid_output_record(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"key": "foo", "value": "ninja"})

        command = """awk '($1=="foo"){print "bar"}'"""

        with pytest.raises(YtError):
            map(
                in_="//tmp/t1",
                out="//tmp/t2",
                command=command,
                spec={"mapper": {"format": "yamr"}},
            )

    @authors("ignat")
    def test_fail_context(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        op = map(
            track=False,
            in_="//tmp/t1",
            out="//tmp/t2",
            command='python3 -c "import os; os.read(0, 1);"',
            spec={"mapper": {"input_format": "dsv", "check_input_fully_consumed": True}, "max_failed_job_count": 2},
        )

        # If all jobs failed then operation is also failed
        with pytest.raises(YtError):
            op.track()

        for job_id in op.list_jobs():
            fail_context = get_job_fail_context(op.id, job_id)
            assert len(fail_context) > 0

    @authors("ignat")
    def test_dump_job_context(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        op = map(
            track=False,
            label="dump_job_context",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"),
            spec={"mapper": {"input_format": "json", "output_format": "json"}},
        )

        jobs = wait_breakpoint()
        # Wait till job starts reading input
        wait(lambda: get(op.get_path() + "/controller_orchid/running_jobs/" + jobs[0] + "/progress") >= 0.5)

        dump_job_context(jobs[0], "//tmp/input_context")

        release_breakpoint()
        op.track()

        context = read_file("//tmp/input_context")
        assert get("//tmp/input_context/@description/type") == "input_context"
        assert JsonFormat().loads_row(context)["foo"] == "bar"

    @authors("ignat")
    def test_dump_job_context_permissions(self):
        create_user("abc")
        create(
            "map_node",
            "//tmp/dir",
            attributes={"acl": [{"action": "deny", "subjects": ["abc"], "permissions": ["write"]}]},
        )

        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        op = map(
            track=False,
            label="dump_job_context",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"),
            spec={"mapper": {"input_format": "json", "output_format": "json"}},
            authenticated_user="abc",
        )

        jobs = wait_breakpoint()
        # Wait till job starts reading input
        wait(lambda: get(op.get_path() + "/controller_orchid/running_jobs/" + jobs[0] + "/progress") >= 0.5)

        with pytest.raises(YtError):
            dump_job_context(jobs[0], "//tmp/dir/input_context", authenticated_user="abc")

        assert not exists("//tmp/dir/input_context")

        release_breakpoint()
        op.track()

    @authors("ignat")
    def test_large_spec(self):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", [{"a": "b"}])

        with pytest.raises(YtError):
            map(
                in_="//tmp/t1",
                out="//tmp/t2",
                command="cat",
                spec={"attribute": "really_large" * (2 * 10 ** 6)},
                verbose=False,
            )

    @authors("ignat")
    def test_job_with_exit_immediately_flag(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        op = map(
            track=False,
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command="set -e; /non_existed_command; echo stderr >&2;",
            spec={"max_failed_job_count": 1},
        )

        with pytest.raises(YtError):
            op.track()

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        assert op.read_stderr(job_ids[0]).endswith(b"/non_existed_command: No such file or directory\n")

    @authors("ignat")
    def test_pipe_statistics(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        op = map(command="cat", in_="//tmp/t_input", out="//tmp/t_output")

        wait(lambda: assert_statistics(
            op,
            "user_job.pipes.input.bytes",
            lambda bytes: bytes == 15))
        wait(lambda: assert_statistics(
            op,
            "user_job.pipes.output.0.bytes",
            lambda bytes: bytes == 15))

    @authors("ignat")
    def test_writer_config(self):
        create("table", "//tmp/t_in")
        create(
            "table",
            "//tmp/t_out",
            attributes={
                "chunk_writer": {"block_size": 1024},
                "compression_codec": "none",
            },
        )

        write_table("//tmp/t_in", [{"value": "A" * 1024} for _ in range(10)])

        map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"job_count": 1})

        chunk_id = get_singular_chunk_id("//tmp/t_out")
        assert get("#" + chunk_id + "/@compressed_data_size") > 1024 * 10
        assert get("#" + chunk_id + "/@max_block_size") < 1024 * 2

    @authors("ignat")
    def test_invalid_schema_in_path(self):
        create("table", "//tmp/input")
        create("table", "//tmp/output")

        with pytest.raises(YtError):
            map(
                in_="//tmp/input",
                out="<schema=[{name=key; type=int64}; {name=key;type=string}]>//tmp/output",
                command="cat",
            )

    @authors("ignat")
    def test_ypath_attributes_on_output_tables(self):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", {"a": "b" * 10000})

        for optimize_for in ["lookup", "scan"]:
            create("table", "//tmp/tout1_" + optimize_for)
            map(
                in_="//tmp/t1",
                out="<optimize_for={0}>//tmp/tout1_{0}".format(optimize_for),
                command="cat",
            )
            assert get("//tmp/tout1_{}/@optimize_for".format(optimize_for)) == optimize_for

        for compression_codec in ["none", "lz4"]:
            create("table", "//tmp/tout2_" + compression_codec)
            map(
                in_="//tmp/t1",
                out="<compression_codec={0}>//tmp/tout2_{0}".format(compression_codec),
                command="cat",
            )

            stats = get("//tmp/tout2_{}/@compression_statistics".format(compression_codec))
            assert compression_codec in stats, str(stats)
            assert stats[compression_codec]["chunk_count"] > 0

    @authors("ignat")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_unique_keys_validation(self, optimize_for):
        create("table", "//tmp/t1")
        create(
            "table",
            "//tmp/t2",
            attributes={
                "optimize_for": optimize_for,
                "schema": make_schema(
                    [
                        {"name": "key", "type": "int64", "sort_order": "ascending"},
                        {"name": "value", "type": "string"},
                    ],
                    unique_keys=True,
                ),
            },
        )

        for i in range(2):
            write_table("<append=true>//tmp/t1", {"key": "foo", "value": "ninja"})

        command = 'cat >/dev/null; echo "{key=1; value=one}"'

        with pytest.raises(YtError):
            map(in_="//tmp/t1", out="//tmp/t2", command=command, spec={"job_count": 2})

        command = 'cat >/dev/null; echo "{key=1; value=one}; {key=1; value=two}"'

        with pytest.raises(YtError):
            map(in_="//tmp/t1", out="//tmp/t2", command=command, spec={"job_count": 1})

    @authors("dakovalkov")
    def test_append_to_sorted_table_simple(self):
        create(
            "table",
            "//tmp/sorted_table",
            attributes={
                "schema": make_schema(
                    [{"name": "key", "type": "int64", "sort_order": "ascending"}],
                    unique_keys=False,
                )
            },
        )
        write_table("//tmp/sorted_table", [{"key": 1}, {"key": 5}, {"key": 10}])
        map(
            in_="//tmp/sorted_table",
            out="<append=%true>//tmp/sorted_table",
            command="echo '{key=30};{key=39}'",
            spec={"job_count": 1},
        )

        assert read_table("//tmp/sorted_table") == [
            {"key": 1},
            {"key": 5},
            {"key": 10},
            {"key": 30},
            {"key": 39},
        ]

    @authors("dakovalkov")
    def test_append_to_sorted_table_failed(self):
        create(
            "table",
            "//tmp/sorted_table",
            attributes={
                "schema": make_schema(
                    [{"name": "key", "type": "int64", "sort_order": "ascending"}],
                    unique_keys=False,
                )
            },
        )
        write_table("//tmp/sorted_table", [{"key": 1}, {"key": 5}, {"key": 10}])

        with pytest.raises(YtError):
            map(
                in_="//tmp/sorted_table",
                out="<append=%true>//tmp/sorted_table",
                command="echo '{key=7};{key=39}'",
                spec={"job_count": 1},
            )

    @authors("dakovalkov")
    def test_append_to_sorted_table_unique_keys(self):
        create(
            "table",
            "//tmp/sorted_table",
            attributes={
                "schema": make_schema(
                    [{"name": "key", "type": "int64", "sort_order": "ascending"}],
                    unique_keys=False,
                )
            },
        )
        write_table("//tmp/sorted_table", [{"key": 1}, {"key": 5}, {"key": 10}])
        map(
            in_="//tmp/sorted_table",
            out="<append=%true>//tmp/sorted_table",
            command="echo '{key=10};{key=39}'",
            spec={"job_count": 1},
        )

        assert read_table("//tmp/sorted_table") == [
            {"key": 1},
            {"key": 5},
            {"key": 10},
            {"key": 10},
            {"key": 39},
        ]

    @authors("dakovalkov")
    def test_append_to_sorted_table_unique_keys_failed(self):
        create(
            "table",
            "//tmp/sorted_table",
            attributes={
                "schema": make_schema(
                    [{"name": "key", "type": "int64", "sort_order": "ascending"}],
                    unique_keys=True,
                )
            },
        )
        write_table("//tmp/sorted_table", [{"key": 1}, {"key": 5}, {"key": 10}])

        with pytest.raises(YtError):
            map(
                in_="//tmp/sorted_table",
                out="<append=%true>//tmp/sorted_table",
                command="echo '{key=10};{key=39}'",
                spec={"job_count": 1},
            )

    @authors("dakovalkov")
    def test_append_to_sorted_table_empty_table(self):
        create(
            "table",
            "//tmp/sorted_table",
            attributes={
                "schema": make_schema(
                    [{"name": "key", "type": "int64", "sort_order": "ascending"}],
                    unique_keys=False,
                )
            },
        )
        create("table", "//tmp/t1")
        write_table("//tmp/t1", [{}])
        map(
            in_="//tmp/t1",
            out="<append=%true>//tmp/sorted_table",
            command="echo '{key=30};{key=39}'",
            spec={"job_count": 1},
        )

        assert read_table("//tmp/sorted_table") == [{"key": 30}, {"key": 39}]

    @authors("dakovalkov")
    def test_append_to_sorted_table_empty_row_empty_table(self):
        create(
            "table",
            "//tmp/sorted_table",
            attributes={
                "schema": make_schema(
                    [{"name": "key", "type": "int64", "sort_order": "ascending"}],
                    unique_keys=False,
                )
            },
        )
        create("table", "//tmp/t1")
        write_table("//tmp/t1", [{}])
        map(
            in_="//tmp/t1",
            out="<append=%true>//tmp/sorted_table",
            command="echo '{ }'",
            spec={"job_count": 1},
        )

        assert read_table("//tmp/sorted_table") == [{"key": yson.YsonEntity()}]

    @authors("dakovalkov")
    def test_append_to_sorted_table_exclusive_lock(self):
        create(
            "table",
            "//tmp/sorted_table",
            attributes={
                "schema": make_schema(
                    [{"name": "key", "type": "int64", "sort_order": "ascending"}],
                    unique_keys=False,
                )
            },
        )
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/sorted_table", [{"key": 5}])
        write_table("//tmp/t1", [{"key": 6}, {"key": 10}])
        write_table("//tmp/t2", [{"key": 8}, {"key": 12}])

        map(
            track=False,
            in_="//tmp/t1",
            out="<append=%true>//tmp/sorted_table",
            command="sleep 10; cat",
        )

        time.sleep(5)

        with pytest.raises(YtError):
            map(in_="//tmp/t2", out="<append=%true>//tmp/sorted_table", command="cat")

    @authors("ignat")
    @pytest.mark.timeout(150)
    def test_many_parallel_operations(self):
        create("table", "//tmp/input")

        testing_options = {"schedule_job_delay": {"duration": 100}}

        job_count = 20
        original_data = [{"index": i} for i in range(job_count)]
        write_table("//tmp/input", original_data)

        operation_count = 5
        ops = []
        for index in range(operation_count):
            output = "//tmp/output" + str(index)
            create("table", output)
            ops.append(
                map(
                    in_="//tmp/input",
                    out=[output],
                    command="sleep 0.1; cat",
                    spec={"data_size_per_job": 1, "testing": testing_options},
                    track=False,
                )
            )

        failed_ops = []
        for index in range(operation_count):
            output = "//tmp/failed_output" + str(index)
            create("table", output)
            failed_ops.append(
                map(
                    in_="//tmp/input",
                    out=[output],
                    command="sleep 0.1; exit 1",
                    spec={
                        "data_size_per_job": 1,
                        "max_failed_job_count": 1,
                        "testing": testing_options,
                    },
                    track=False,
                )
            )

        for index, op in enumerate(failed_ops):
            "//tmp/failed_output" + str(index)
            with pytest.raises(YtError):
                op.track()

        for index, op in enumerate(ops):
            output = "//tmp/output" + str(index)
            op.track()
            assert sorted_dicts(read_table(output)) == original_data

        def check():
            statistics = get("//sys/scheduler/orchid/monitoring/ref_counted/statistics")
            operation_objects = [
                "NYT::NScheduler::TOperation",
                "NYT::NScheduler::TSchedulerOperationElement",
            ]
            records = [record for record in statistics if record["name"] in operation_objects]
            return len(records) == 2 and all(record["objects_alive"] == 0 for record in records)
        wait(check)

    @authors("ignat")
    def test_concurrent_fail(self):
        create("table", "//tmp/input")

        testing_options = {"schedule_job_delay": {"duration": 250}}

        job_count = 1000
        original_data = [{"index": i} for i in range(job_count)]
        write_table("//tmp/input", original_data)

        create("table", "//tmp/output")
        with pytest.raises(YtError):
            map(
                in_="//tmp/input",
                out="//tmp/output",
                command="sleep 0.250; exit 1",
                spec={
                    "data_size_per_job": 1,
                    "max_failed_job_count": 10,
                    "testing": testing_options,
                },
            )

    @authors("ignat")
    def test_YT_5629(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")

        data = [{"a": i} for i in range(5)]
        write_table("//tmp/t1", data)

        map(in_="//tmp/t1", out="//tmp/t2", command="sleep 1; cat /proc/self/fd/0")

        assert read_table("//tmp/t2") == data

    @authors("ignat")
    def test_range_count_limit(self):
        create("table", "//tmp/in")
        create("table", "//tmp/out")
        write_table("//tmp/in", {"key": "a", "value": "value"})

        def gen_table(range_count):
            return "<ranges=[" + ("{exact={row_index=0}};" * range_count) + "]>//tmp/in"

        map(in_=[gen_table(20)], out="//tmp/out", command="cat")

        with pytest.raises(YtError):
            map(in_=[gen_table(2000)], out="//tmp/out", command="cat")

    @authors("ignat")
    def test_complete_op(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        for i in range(5):
            write_table("<append=true>//tmp/t1", {"key": str(i), "value": "foo"})

        op = map(
            track=False,
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("echo job_index=$YT_JOB_INDEX ; BREAKPOINT"),
            spec={
                "mapper": {"format": "dsv"},
                "data_size_per_job": 1,
                "max_failed_job_count": 1,
            },
        )
        jobs = wait_breakpoint(job_count=5)

        for job_id in jobs[:3]:
            release_breakpoint(job_id=job_id)

        assert op.get_state() != "completed"
        wait(lambda: op.get_job_count("completed") >= 3)

        op.complete()
        assert op.get_state() == "completed"
        op.track()
        assert len(read_table("//tmp/t2")) == 3
        assert "operation_completed_by_user_request" in op.get_alerts()

    @authors("ignat")
    def test_abort_op(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"foo": "bar"})

        op = map(track=False, in_="//tmp/t", out="//tmp/t", command="sleep 1")

        op.abort()
        assert op.get_state() == "aborted"

    @authors("ignat")
    def test_input_with_custom_transaction(self):
        custom_tx = start_transaction(timeout=30000)

        create("table", "//tmp/in", tx=custom_tx)
        write_table("//tmp/in", {"foo": "bar"}, tx=custom_tx)

        create("table", "//tmp/out")

        with pytest.raises(YtError):
            map(command="cat", in_="//tmp/in", out="//tmp/out")

        map(
            command="cat",
            in_='<transaction_id="{}">//tmp/in'.format(custom_tx),
            out="//tmp/out",
        )

        assert list(read_table("//tmp/out")) == [{"foo": "bar"}]

    @authors("babenko")
    def test_input_created_in_user_transaction(self):
        custom_tx = start_transaction()
        create("table", "//tmp/in", tx=custom_tx)
        write_table("//tmp/in", {"foo": "bar"}, tx=custom_tx)
        create("table", "//tmp/out")
        with pytest.raises(YtError):
            map(command="cat", in_="//tmp/in", out="//tmp/out")

    @authors("ignat")
    def test_nested_input_transactions(self):
        custom_tx = start_transaction(timeout=60000)

        create("table", "//tmp/in", tx=custom_tx)
        write_table("//tmp/in", {"foo": "bar"}, tx=custom_tx)

        create("table", "//tmp/out")

        op = map(
            track=False,
            command=with_breakpoint("BREAKPOINT; sleep 100"),
            in_='<transaction_id="{}">//tmp/in'.format(custom_tx),
            out="//tmp/out",
        )

        wait_breakpoint()

        nested_input_transaction_ids = get(op.get_path() + "/@nested_input_transaction_ids")
        assert len(nested_input_transaction_ids) == 1
        nested_tx = nested_input_transaction_ids[0]

        assert list(read_table("//tmp/in", tx=nested_tx)) == [{"foo": "bar"}]
        assert get("#{}/@parent_id".format(nested_tx)) == custom_tx

        op.wait_for_fresh_snapshot()

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        op.ensure_running()
        assert get(op.get_path() + "/@nested_input_transaction_ids") == [nested_tx]

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            abort_transaction(nested_tx)

        op.ensure_running()
        new_nested_input_transaction_ids = get(op.get_path() + "/@nested_input_transaction_ids")
        assert len(new_nested_input_transaction_ids) == 1
        assert new_nested_input_transaction_ids[0] != nested_tx
        assert get(f"#{new_nested_input_transaction_ids[0]}/@parent_id") == custom_tx

    @authors("ignat")
    def test_nested_input_transaction_duplicates(self):
        custom_tx = start_transaction(timeout=60000)

        create("table", "//tmp/in", tx=custom_tx)
        write_table("//tmp/in", {"foo": "bar"}, tx=custom_tx)

        create("table", "//tmp/out")

        op = map(
            track=False,
            command=with_breakpoint("BREAKPOINT; sleep 100"),
            in_=['<transaction_id="{}">//tmp/in'.format(custom_tx)] * 2,
            out="//tmp/out",
        )

        wait_breakpoint()

        nested_input_transaction_ids = get(op.get_path() + "/@nested_input_transaction_ids")
        assert len(nested_input_transaction_ids) == 2
        assert nested_input_transaction_ids[0] == nested_input_transaction_ids[1]

        nested_tx = nested_input_transaction_ids[0]
        assert list(read_table("//tmp/in", tx=nested_tx)) == [{"foo": "bar"}]
        assert get("#{}/@parent_id".format(nested_tx)) == custom_tx

        op.wait_for_fresh_snapshot()

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        op.ensure_running()
        assert get(op.get_path() + "/@nested_input_transaction_ids") == [
            nested_tx,
            nested_tx,
        ]

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            abort_transaction(nested_tx)

        op.ensure_running()
        new_nested_input_transaction_ids = get(op.get_path() + "/@nested_input_transaction_ids")
        assert len(new_nested_input_transaction_ids) == 2
        assert new_nested_input_transaction_ids[0] == new_nested_input_transaction_ids[1]
        assert new_nested_input_transaction_ids[0] != nested_tx

    @authors("babenko")
    def test_update_lock_transaction_timeout(self):
        lock_tx = get("//sys/scheduler/lock/@locks/0/transaction_id")
        new_timeout = get("#{}/@timeout".format(lock_tx)) + 1234
        set(
            "//sys/scheduler/config/lock_transaction_timeout",
            new_timeout,
            recursive=True,
        )
        wait(lambda: get("#{}/@timeout".format(lock_tx)) == new_timeout)

    @authors("ignat")
    def test_user_transaction_abort_for_pending_operation(self):
        create_pool(
            "test_pool",
            pool_tree="default",
            attributes={
                "max_running_operation_count": 1,
                "max_operation_count": 1,
            })

        op_first = run_sleeping_vanilla(pool="test_pool")
        wait(lambda: op_first.get_state() == "running")

        user_tx = start_transaction(timeout=5000)

        with raises_yt_error(yt_error_codes.TooManyOperations):
            run_sleeping_vanilla(pool="test_pool", tx=user_tx)

        abort_transaction(user_tx)

        with raises_yt_error("Error checking user transaction"):
            run_sleeping_vanilla(pool="test_pool", tx=user_tx)

    @authors("max42", "galtsev")
    def test_controller_throttling(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        for i in range(25):
            write_table("<append=%true>//tmp/t_in", [{"a": i}])

        def get_controller_throttling_schedule_job_fail_count():
            op = map(
                in_=["//tmp/t_in"],
                out=["//tmp/t_out"],
                command="cat",
                spec={
                    "job_count": 5,
                    "testing": {
                        "build_job_spec_proto_delay": 1000,
                    },
                },
            )
            schedule_job_statistics = get(op.get_path() + "/@progress/schedule_job_statistics")
            return schedule_job_statistics.get("failed", {}).get("controller_throttling", 0)

        if not exists("//sys/controller_agents/config/operation_options"):
            set("//sys/controller_agents/config/operation_options", {})

        operation_options = "config/operation_options"
        instances = ls("//sys/controller_agents/instances")
        assert len(instances) == 1
        instance = instances[0]
        orchid = f"//sys/controller_agents/instances/{instance}/orchid/controller_agent"
        initial_operation_options = get(f"{orchid}/{operation_options}")

        def set_option(option, value):
            option_path = f"{operation_options}/{option}"
            dyncfg_path = f"//sys/controller_agents/{option_path}"
            orchid_path = f"{orchid}/{option_path}"
            set(dyncfg_path, value)
            wait(lambda: get(orchid_path) == value)
            return dyncfg_path

        assert get_controller_throttling_schedule_job_fail_count() == 0

        try:
            job_spec_count_limit_path = set_option("controller_building_job_spec_count_limit", 1)
            assert get_controller_throttling_schedule_job_fail_count() > 0
        finally:
            remove(job_spec_count_limit_path, force=True)

        try:
            total_job_spec_slice_count_limit_path = set_option("controller_total_building_job_spec_slice_count_limit", 5)
            assert get_controller_throttling_schedule_job_fail_count() > 0
        finally:
            remove(total_job_spec_slice_count_limit_path, force=True)

        wait(lambda: get(f"{orchid}/{operation_options}") == initial_operation_options)
        assert get_controller_throttling_schedule_job_fail_count() == 0

    @authors("alexkolodezny")
    def test_suspension_on_job_failure(self):
        op = run_test_vanilla(
            "exit 1",
            spec={"suspend_on_job_failure": True},
            fail_fast=False
        )
        wait(lambda: get(op.get_path() + "/@suspended"))


class TestSchedulerCommonMulticell(TestSchedulerCommon):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_TEST_PARTITIONS = 6
    NUM_SECONDARY_MASTER_CELLS = 2


##################################################################


@pytest.mark.opensource
@pytest.mark.enabled_multidaemon
class TestMultipleSchedulers(YTEnvSetup, PrepareTables):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 2

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "connect_retry_backoff_time": 1000,
            "fair_share_update_period": 100,
            "profiling_update_period": 100,
            "testing_options": {
                "master_disconnect_delay": 3000,
            },
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 500,
        }
    }

    def _get_scheduler_transaction(self):
        while True:
            scheduler_locks = get("//sys/scheduler/lock/@locks", verbose=False)
            if len(scheduler_locks) > 0:
                scheduler_transaction = scheduler_locks[0]["transaction_id"]
                return scheduler_transaction
            time.sleep(0.01)

    @authors("ignat")
    @pytest.mark.ignore_in_opensource_ci
    def test_hot_standby(self):
        self._prepare_tables()

        op = map(track=False, in_="//tmp/t_in", out="//tmp/t_out", command="cat; sleep 15")

        op.wait_for_fresh_snapshot()

        transaction_id = self._get_scheduler_transaction()

        def get_transaction_title(transaction_id):
            return get("#{0}/@title".format(transaction_id), verbose=False)

        title = get_transaction_title(transaction_id)

        while True:
            abort_transaction(transaction_id)

            new_transaction_id = self._get_scheduler_transaction()
            new_title = get_transaction_title(new_transaction_id)
            if title != new_title:
                break

            title = new_title
            transaction_id = new_transaction_id
            time.sleep(0.3)

        op.track()

        assert read_table("//tmp/t_out") == [{"foo": "bar"}]


##################################################################


@pytest.mark.enabled_multidaemon
class TestSchedulerMaxChunkPerJob(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "map_operation_options": {
                "max_data_slices_per_job": 1,
            },
            "ordered_merge_operation_options": {
                "max_data_slices_per_job": 1,
            },
        }
    }

    @authors("ignat")
    def test_max_data_slices_per_job(self):
        data = [{"foo": i} for i in range(5)]
        create("table", "//tmp/in1")
        create("table", "//tmp/in2")
        create("table", "//tmp/out")
        write_table("//tmp/in1", data, sorted_by="foo")
        write_table("//tmp/in2", data, sorted_by="foo")

        op = merge(
            mode="ordered",
            in_=["//tmp/in1", "//tmp/in2"],
            out="//tmp/out",
            spec={"force_transform": True},
        )
        assert data + data == read_table("//tmp/out")

        # Must be 2 jobs since input has 2 chunks.
        assert get(op.get_path() + "/@progress/jobs/total") == 2

        op = map(command="cat >/dev/null", in_=["//tmp/in1", "//tmp/in2"], out="//tmp/out")
        assert get(op.get_path() + "/@progress/jobs/total") == 2

    @authors("gepardo")
    def test_parallelism_yt_17457(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")

        for i in range(10):
            write_table("<append=%true>//tmp/t_input", [{"a": i}])

        op = map(
            track=False,
            command=with_breakpoint("cat && BREAKPOINT"),
            in_="//tmp/t_input",
            out="//tmp/t_output",
        )
        wait_breakpoint(job_count=1)
        wait(lambda: exists(op.get_path() + "/@brief_progress"))
        wait(lambda: get(op.get_path() + "/@brief_progress/jobs")["total"] > 0, ignore_exceptions=True)
        counters = get(op.get_path() + "/@brief_progress/jobs")
        assert counters["running"] + counters["pending"] == 10
        release_breakpoint()
        op.track()

    @authors("babenko")
    def test_lock_revisions_yt_13962(self):
        tx = start_transaction()
        create("table", "//tmp/t", tx=tx)
        data = [{"key": "value"}]
        write_table("//tmp/t", data, tx=tx)
        merge(in_=["//tmp/t"], out="//tmp/t", mode="ordered", tx=tx)
        assert read_table("//tmp/t", tx=tx) == data


##################################################################

@pytest.mark.enabled_multidaemon
class TestSchedulerMaxInputOutputTableCount(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "max_input_table_count": 2,
            "max_output_table_count": 2
        }
    }

    @authors("alexkolodezny")
    def test_max_input_table_count(self):
        create("table", "//tmp/in1")
        create("table", "//tmp/in2")
        create("table", "//tmp/in3")
        create("table", "//tmp/out")

        with raises_yt_error("Too many input tables: maximum allowed 2, actual 3"):
            map(
                command="",
                in_=["//tmp/in1", "//tmp/in2/", "//tmp/in3"],
                out="//tmp/out"
            )

    @authors("alexkolodezny")
    def test_max_output_table_count(self):
        create("table", "//tmp/in")
        create("table", "//tmp/out1")
        create("table", "//tmp/out2")
        create("table", "//tmp/out3")

        with raises_yt_error("Too many output tables: maximum allowed 2, actual 3"):
            map(
                command="",
                in_="//tmp/in",
                out=["//tmp/out1", "//tmp/out2", "//tmp/out3"]
            )


##################################################################

@pytest.mark.enabled_multidaemon
class TestSchedulerMaxChildrenPerAttachRequest(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "max_children_per_attach_request": 1,
        }
    }

    @authors("ignat")
    def test_max_children_per_attach_request(self):
        data = [{"foo": i} for i in range(3)]
        create("table", "//tmp/in")
        create("table", "//tmp/out")
        write_table("//tmp/in", data)

        map(
            command="cat",
            in_="//tmp/in",
            out="//tmp/out",
            spec={"data_size_per_job": 1},
        )

        assert sorted_dicts(read_table("//tmp/out")) == sorted_dicts(data)
        assert get("//tmp/out/@row_count") == 3

    @authors("ignat")
    def test_max_children_per_attach_request_in_live_preview(self):
        data = [{"foo": i} for i in range(3)]
        create("table", "//tmp/in")
        create("table", "//tmp/out")
        write_table("//tmp/in", data)

        op = map(
            track=False,
            command=with_breakpoint("cat ; BREAKPOINT"),
            in_="//tmp/in",
            out="//tmp/out",
            spec={"data_size_per_job": 1},
        )

        jobs = wait_breakpoint(job_count=3)

        for job_id in jobs[:2]:
            release_breakpoint(job_id=job_id)

        for _ in range(100):
            jobs_exist = exists(op.get_path() + "/@progress/jobs")
            if jobs_exist:
                completed_jobs = get(op.get_path() + "/@progress/jobs/completed/total")
                if completed_jobs == 2:
                    break
            time.sleep(0.1)

        transaction_id = get(op.get_path() + "/@async_scheduler_transaction_id")
        wait(lambda: get(op.get_path() + "/output_0/@row_count", tx=transaction_id) == 2)

        release_breakpoint()
        op.track()


##################################################################


@pytest.mark.enabled_multidaemon
class TestSchedulerOperationSnapshots(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "max_concurrent_controller_schedule_job_calls": 1,
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 500,
            "operation_controller_suspend_timeout": 2000,
        }
    }

    @authors("ignat")
    def test_snapshots(self):
        create("table", "//tmp/in")
        write_table("//tmp/in", [{"foo": i} for i in range(5)])
        create("table", "//tmp/out")

        testing_options = {"schedule_job_delay": {"duration": 500}}

        op = map(
            track=False,
            command=with_breakpoint("cat ; BREAKPOINT"),
            in_="//tmp/in",
            out="//tmp/out",
            spec={"data_weight_per_job": 1, "testing": testing_options},
        )

        snapshot_path = op.get_path() + "/snapshot"
        wait(lambda: exists(snapshot_path))

        # This is done to avoid read failures due to snapshot file rewriting.
        snapshot_backup_path = snapshot_path + ".backup"
        copy(snapshot_path, snapshot_backup_path)
        assert len(read_file(snapshot_backup_path, verbose=False)) > 0

        ts_str = get(op.get_path() + "/controller_orchid/progress/last_successful_snapshot_time")
        assert time.time() - date_string_to_timestamp(ts_str) < 60

        release_breakpoint()
        op.track()

    @authors("ignat")
    def test_parallel_snapshots(self):
        create("table", "//tmp/input")

        testing_options = {"schedule_job_delay": {"duration": 100}}

        job_count = 1
        original_data = [{"index": i} for i in range(job_count)]
        write_table("//tmp/input", original_data)

        operation_count = 5
        ops = []
        for index in range(operation_count):
            output = "//tmp/output" + str(index)
            create("table", output)
            ops.append(
                map(
                    track=False,
                    command=with_breakpoint("cat ; BREAKPOINT"),
                    in_="//tmp/input",
                    out=[output],
                    spec={"data_size_per_job": 1, "testing": testing_options},
                )
            )

        for op in ops:
            snapshot_path = op.get_path() + "/snapshot"
            wait(lambda: exists(snapshot_path))

            snapshot_backup_path = snapshot_path + ".backup"
            copy(snapshot_path, snapshot_backup_path)
            assert len(read_file(snapshot_backup_path, verbose=False)) > 0

        # All our operations use 'default' breakpoint so we release it and all operations continue execution.
        release_breakpoint()

        for op in ops:
            op.track()

    @authors("ignat")
    def test_suspend_time_limit(self):
        create("table", "//tmp/in")
        write_table("//tmp/in", [{"foo": i} for i in range(5)])

        create("table", "//tmp/out1")
        create("table", "//tmp/out2")

        while True:
            op2 = map(
                track=False,
                command="cat",
                in_="//tmp/in",
                out="//tmp/out2",
                spec={
                    "data_size_per_job": 1,
                    "testing": {"delay_inside_suspend": 15000},
                },
            )

            time.sleep(2)
            snapshot_path2 = op2.get_path() + "/snapshot"
            if exists(snapshot_path2):
                op2.abort()
                continue
            else:
                break

        op1 = map(
            track=False,
            command="sleep 10; cat",
            in_="//tmp/in",
            out="//tmp/out1",
            spec={"data_size_per_job": 1},
        )

        snapshot_path1 = op1.get_path() + "/snapshot"
        snapshot_path2 = op2.get_path() + "/snapshot"

        wait(lambda: exists(snapshot_path1))
        assert not exists(snapshot_path2)

        op1.track()
        op2.track()


##################################################################


@pytest.mark.enabled_multidaemon
class TestSchedulerHeterogeneousConfiguration(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        if not hasattr(cls, "node_counter"):
            cls.node_counter = 0
        cls.node_counter += 1
        if cls.node_counter == 1:
            config["job_resource_manager"]["resource_limits"]["user_slots"] = 0

    @authors("renadeen", "ignat")
    def test_job_count(self):
        data = [{"foo": i} for i in range(3)]
        create("table", "//tmp/in")
        create("table", "//tmp/out")
        write_table("//tmp/in", data)

        wait(lambda: get("//sys/scheduler/orchid/scheduler/cluster/resource_limits/user_slots") == 2)
        wait(lambda: get("//sys/scheduler/orchid/scheduler/cluster/resource_usage/user_slots") == 0)

        wait(
            lambda: get(
                "//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/default/resource_limits/user_slots"
            )
            == 2
        )
        wait(
            lambda: get(
                "//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/default/resource_usage/user_slots"
            )
            == 0
        )

        op = map(
            track=False,
            command="sleep 100",
            in_="//tmp/in",
            out="//tmp/out",
            spec={"data_size_per_job": 1, "locality_timeout": 0},
        )

        wait(
            lambda: op.get_runtime_progress("scheduling_info_per_pool_tree/default/resource_usage/user_slots", 0) == 2
        )
        wait(lambda: get("//sys/scheduler/orchid/scheduler/cluster/resource_limits/user_slots") == 2)
        wait(lambda: get("//sys/scheduler/orchid/scheduler/cluster/resource_usage/user_slots") == 2)

        wait(
            lambda: get(
                "//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/default/resource_limits/user_slots"
            )
            == 2
        )
        wait(
            lambda: get(
                "//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/default/resource_usage/user_slots"
            )
            == 2
        )


###############################################################################################


@pytest.mark.enabled_multidaemon
class TestSchedulerJobStatistics(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    def _create_table(self, table):
        create("table", table)
        set(table + "/@replication_factor", 1)

    def _get_running_job_statistics(self, op, job_id):
        def check():
            print_debug(ls(op.get_path() + "/controller_orchid/running_jobs/{}".format(job_id)))
            return exists(op.get_path() + "/controller_orchid/running_jobs/{}/statistics".format(job_id))
        wait(lambda: check())
        return get(op.get_path() + "/controller_orchid/running_jobs/{}/statistics".format(job_id))

    @authors("ignat")
    def test_scheduler_job_statistics(self):
        self._create_table("//tmp/in")
        self._create_table("//tmp/out")
        write_table("//tmp/in", [{"foo": i} for i in range(10)])

        op = map(
            track=False,
            label="scheduler_job_statistics",
            in_="//tmp/in",
            out="//tmp/out",
            command=with_breakpoint("cat ; BREAKPOINT"),
        )

        wait_breakpoint()
        running_jobs = op.get_running_jobs()
        job_id = next(iter(running_jobs.keys()))

        statistics_appeared = False
        for _ in range(300):
            statistics = self._get_running_job_statistics(op, job_id)
            data = statistics.get("data", {})
            _input = data.get("input", {})
            row_count = _input.get("row_count", {})
            _sum = row_count.get("sum", 0)
            if _sum == 10:
                statistics_appeared = True
                break
            time.sleep(0.1)

        assert statistics_appeared

        traffic_statistics = statistics["job_proxy"]["traffic"]
        assert traffic_statistics["inbound"]["from_"]["sum"] > 0
        assert traffic_statistics["duration_ms"]["sum"] > 0
        assert traffic_statistics["_to_"]["sum"] > 0

        release_breakpoint()
        op.track()

    @authors("ignat")
    def test_scheduler_operation_statistics(self):
        self._create_table("//tmp/in")
        self._create_table("//tmp/out")
        write_table("//tmp/in", [{"foo": i} for i in range(10)])

        op = map(
            in_="//tmp/in",
            out="//tmp/out",
            command="cat",
            spec={"data_size_per_job": 1})

        statistics = get(op.get_path() + "/@progress/job_statistics_v2")
        assert extract_statistic_v2(statistics, "time.exec", summary_type="count") == 10
        assert extract_statistic_v2(statistics, "time.exec") <= \
            extract_statistic_v2(statistics, "time.total")

    @authors("ignat")
    def test_statistics_for_aborted_operation(self):
        self._create_table("//tmp/in")
        self._create_table("//tmp/out")
        write_table("//tmp/in", [{"foo": i} for i in range(3)])

        op = map(
            track=False,
            in_="//tmp/in",
            out="//tmp/out",
            command=with_breakpoint("cat ; BREAKPOINT"),
            spec={"data_size_per_job": 1})

        wait_breakpoint()

        running_jobs = op.get_running_jobs()

        for job_id in running_jobs:
            statistics_appeared = False
            for _ in range(300):
                statistics = self._get_running_job_statistics(op, job_id)
                data = statistics.get("data", {})
                _input = data.get("input", {})
                row_count = _input.get("row_count", {})
                _sum = row_count.get("sum", 0)
                if _sum > 0:
                    statistics_appeared = True
                    break
                time.sleep(0.1)
            assert statistics_appeared

        op.abort()

        wait(lambda: assert_statistics(
            op,
            key="time.total",
            assertion=lambda count: count == 3,
            job_state="aborted",
            job_type="map",
            summary_type="count"))


##################################################################


class TestConnectToMaster(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 0

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "connect_retry_backoff_time": 1000
        }
    }

    @authors("max42")
    def test_scheduler_doesnt_connect_to_master_in_safe_mode(self):
        set("//sys/@config/enable_safe_mode", True)
        self.Env.kill_schedulers()
        self.Env.start_schedulers(sync=False)
        time.sleep(1)

        wait(lambda: self.has_safe_mode_error_in_log())

    def has_safe_mode_error_in_log(self):
        with open(self.path_to_run + "/logs/scheduler-0.log.zst", "rb") as file:
            decompressor = zstd.ZstdDecompressor()
            binary_reader = decompressor.stream_reader(file, read_size=8192)
            text_stream = io.TextIOWrapper(binary_reader, encoding='utf-8')
            for line in text_stream:
                if "Error connecting to master" in line and "Cluster is in safe mode" in line:
                    return True
        return False

    @authors("renadeen")
    def test_scheduler_doesnt_start_with_invalid_pools(self):
        alerts = get("//sys/scheduler/@alerts")
        assert [element for element in alerts if element["attributes"]["alert_type"] == "scheduler_cannot_connect"] == []
        with Restarter(self.Env, SCHEDULERS_SERVICE, sync=False):
            move("//sys/pool_trees", "//sys/pool_trees_bak")
            set("//sys/pool_trees", {"default": {"invalid_pool": 1}})
            set("//sys/pool_trees/default/@config", {})

        wait(
            lambda: [
                element for element in get("//sys/scheduler/@alerts")
                if element["attributes"]["alert_type"] == "scheduler_cannot_connect"
            ] != []
        )
        alerts = [
            element for element in get("//sys/scheduler/@alerts")
            if element["attributes"]["alert_type"] == "scheduler_cannot_connect"
        ]
        assert len(alerts) == 1
        assert alerts[0]["attributes"]["alert_type"] == "scheduler_cannot_connect"

        scheduler = ls("//sys/scheduler/instances")[0]
        assert not get("//sys/scheduler/instances/" + scheduler + "/orchid/scheduler/service/connected")

        remove("//sys/pool_trees")
        move("//sys/pool_trees_bak", "//sys/pool_trees")
        wait(lambda: get("//sys/scheduler/instances/" + scheduler + "/orchid/scheduler/service/connected"))


##################################################################


@pytest.mark.enabled_multidaemon
class TestJobStatisticsPorto(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_SCHEDULERS = 1
    USE_PORTO = True

    @authors("babenko")
    def test_statistics(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": "b"}])
        op = map(
            in_="//tmp/t1",
            out="//tmp/t2",
            command='cat; bash -c "for (( I=0 ; I<=100*1000 ; I++ )) ; do echo $(( I+I*I )); done; sleep 2" >/dev/null && sleep 2',
        )

        def check_statistics(statistics, statistic_extractor):
            result = True
            for component in ["user_job", "job_proxy"]:
                print_debug(component)
                result = result and statistic_extractor(statistics, component + ".cpu.user") > 0
                result = result and statistic_extractor(statistics, component + ".cpu.system") > 0
                result = result and statistic_extractor(statistics, component + ".cpu.context_switches") is not None
                result = result and statistic_extractor(statistics, component + ".cpu.peak_thread_count", summary_type="max") is not None
                result = result and statistic_extractor(statistics, component + ".cpu.wait") is not None
                result = result and statistic_extractor(statistics, component + ".cpu.throttled") is not None
                result = result and statistic_extractor(statistics, component + ".block_io.bytes_read") is not None
                result = result and statistic_extractor(statistics, component + ".max_memory") > 0
            result = result and statistic_extractor(statistics, "user_job.cumulative_memory_mb_sec") > 0
            return result

        wait(lambda: check_statistics(get(op.get_path() + "/@progress/job_statistics_v2"), extract_statistic_v2))
        wait(lambda: check_statistics(get(op.get_path() + "/@progress/job_statistics"), extract_deprecated_statistic))

    @authors("max42")
    def test_statistics_truncation(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", [{"a": 1}])
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        create("table", "//tmp/t3")

        def run_op():
            return map(
                in_="//tmp/t",
                out=["//tmp/t1", "//tmp/t2", "//tmp/t3"],
                command='echo "{a=1};" >&1; echo "{a=2};{a=2};" >&4; echo "{a=3};{a=3};{a=3};" >&7',
            )

        op = run_op()
        statistics_v2 = get(op.get_path() + "/@progress/job_statistics_v2")
        assert extract_statistic_v2(statistics_v2, "data.input.row_count") == 1
        assert extract_statistic_v2(statistics_v2, "data.output.0.row_count") == 1
        assert extract_statistic_v2(statistics_v2, "data.output.1.row_count") == 2
        assert extract_statistic_v2(statistics_v2, "data.output.2.row_count") == 3

        data_flow_graph = get(op.get_path() + "/@progress/data_flow_graph")
        row_count = data_flow_graph["edges"]["map"]["sink"]["statistics"]["row_count"]
        assert row_count == 6

        update_nodes_dynamic_config({
            "exec_node": {
                "job_controller": {
                    "job_common": {
                        "statistics_output_table_count_limit": 1,
                    },
                },
            },
        })

        op = run_op()
        # Statistics must be truncated.
        statistics_v2 = get(op.get_path() + "/@progress/job_statistics_v2")
        assert extract_statistic_v2(statistics_v2, "data.input.row_count") == 1
        assert extract_statistic_v2(statistics_v2, "data.output.0.row_count") == 1
        assert extract_statistic_v2(statistics_v2, "data.output.1.row_count") is None
        assert extract_statistic_v2(statistics_v2, "data.output.2.row_count") is None

        # But data flow graph must not.
        data_flow_graph = get(op.get_path() + "/@progress/data_flow_graph")
        row_count = data_flow_graph["edges"]["map"]["sink"]["statistics"]["row_count"]
        assert row_count == 6


##################################################################


@pytest.mark.enabled_multidaemon
class TestSchedulerObjectsDestruction(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "scheduler_connector": {
                    "heartbeat_executor": {
                        "period": 1,  # 1 msec
                    },
                },
                "controller_agent_connector": {
                    "heartbeat_executor": {
                        "period": 1,  # 1 msec
                    }
                },
            },
        },
    }

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "user_slots": 5,
                "cpu": 5,
                "memory": 5 * 1024 ** 3,
            },
        }
    }

    @authors("pogorelov")
    def test_schedule_job_result_destruction(self):
        create_test_tables(row_count=100)

        try:
            map(
                command="""test $YT_JOB_INDEX -eq "1" && exit 1; cat""",
                in_="//tmp/t_in",
                out="//tmp/t_out",
                spec={
                    "data_size_per_job": 1,
                    "max_failed_job_count": 1
                },
            )
        except YtError:
            pass

        def check():
            statistics = get("//sys/scheduler/orchid/monitoring/ref_counted/statistics")
            schedule_job_entry_object_type = \
                "NYT::NDetail::TPromiseState<NYT::TIntrusivePtr<NYT::NScheduler::TControllerScheduleAllocationResult>>"
            records = [record for record in statistics if record["name"] == schedule_job_entry_object_type]
            return len(records) == 1 and all(record["objects_alive"] == 0 for record in records)
        wait(check)


##################################################################


class TestScheduleJobDelayAndRevive(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "scheduler_connector": {
                    "heartbeat_executor": {
                        "period": 1,  # 1 msec
                    },
                },
                "controller_agent_connector": {
                    "heartbeat_executor": {
                        "period": 1,  # 1 msec
                    },
                }
            }
        }
    }

    @authors("ignat")
    def test_schedule_job_delay(self):
        testing_options = {"schedule_job_delay_scheduler": {"duration": 5000}}
        op = run_test_vanilla("sleep 2", job_count=2, spec={"testing": testing_options})

        wait(lambda: op.get_state() == "running")

        time.sleep(2)
        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        op.track()


##################################################################


@pytest.mark.enabled_multidaemon
class TestDelayInNodeHeartbeat(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    # YT-17272
    # Scenario:
    # 1) Operation started.
    # 2) Node comes to scheduler with heartbeat and scheduler starts to process it.
    # 3) Node is banned.
    # 4) Scheduler aborts all jobs at node.
    # 5) New jobs are scheduled on the node, and scheduler replies to node.
    # 6) Node does not come to scheduler with heartbeats anymore since it is not connected to master.
    # 7) Scheduler does not abort job at node, since it is offline on scheduler and registration lease is long.
    # 8) Job hangs.

    NUM_MASTERS = 1
    NUM_NODES = 2
    NUM_SCHEDULERS = 1

    DELTA_DYNAMIC_NODE_CONFIG = {
        "exec_node": {
            "controller_agent_connector": {
                "heartbeat_executor": {
                    "period": 10,  # 10 msec
                },
            },
        },
    }

    @authors("pogorelov")
    def test_node_heartbeat_delay(self):
        def get_ongoing_heartbeats_count():
            ongoing_heartbeat_count_orchid_path = scheduler_orchid_path() + "/scheduler/node_shards/ongoing_heartbeat_count"
            return get(ongoing_heartbeat_count_orchid_path)

        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 2

        first_node, second_node = nodes

        update_controller_agent_config("safe_online_node_count", 10000)

        # We want to control the moment we start schedule jobs on node.
        disable_scheduler_jobs_on_node(first_node, "test node heartbeat delay")
        wait(
            lambda: get("//sys/cluster_nodes/{}/orchid/exec_node/job_resource_manager/resource_limits/user_slots".format(first_node)) == 0
        )

        set_node_banned(second_node, True, wait_for_scheduler=True)

        update_scheduler_config("testing_options/node_heartbeat_processing_delay", {
            "duration": 3000,
            "type": "async",
        })

        op = run_test_vanilla("sleep 5", job_count=1)

        # Scheduler starts making a delay in heartbeat processing here.
        enable_scheduler_jobs_on_node(first_node)
        wait(
            lambda: get("//sys/cluster_nodes/{}/orchid/exec_node/job_resource_manager/resource_limits/user_slots".format(first_node)) > 0
        )

        # We want to ban node during delay in heartbeat.
        wait(lambda: get_ongoing_heartbeats_count() > 0)
        assert get_ongoing_heartbeats_count() == 1

        # Increase period to controller agent do not know that node is not online anymore.
        update_controller_agent_config("exec_nodes_update_period", 5000)

        print_debug("Ban node", first_node)
        set_node_banned(first_node, True, wait_for_scheduler=True)

        # We want to unban nodes only when heartbeat processing of the banned node is finished.
        wait(lambda: get_ongoing_heartbeats_count() == 0)

        update_controller_agent_config("exec_nodes_update_period", 100)

        update_scheduler_config("testing_options/node_heartbeat_processing_delay", {
            "duration": 0,
            "type": "sync",
        })

        set_node_banned(second_node, False, wait_for_scheduler=True)

        op.track()


##################################################################


class TestSchedulerTracing(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # Checks structured logs.
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    LOG_WRITE_WAIT_TIME = 0.5

    DELTA_SCHEDULER_CONFIG = {
        "rpc_server": {
            "tracing_mode": "enable",
        },
        "jaeger": {
            "collector_channel_config": {
                "address": "localhost:12345",
            },
            "service_name": "scheduler",
            "flush_period": 100,
            "test_drop_spans": True,
        },
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "rpc_server": {
            "tracing_mode": "enable",
        },
        "jaeger": {
            "collector_channel_config": {
                "address": "localhost:12345",
            },
            "service_name": "controller_agent",
            "flush_period": 100,
            "test_drop_spans": True,
        },
    }

    DELTA_NODE_CONFIG = {
        "jaeger": {
            "collector_channel_config": {
                "address": "localhost:12345",
            },
            "service_name": "node",
            "flush_period": 100,
            "test_drop_spans": True,
        },
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "scheduler_connector": {
                    "enable_tracing": True,
                    "tracing_sampler": {
                        "global_sample_rate": 1.0,
                    },
                },
            },
        },
    }

    @authors("ignat", "pogorelov")
    def test_tracing(self):
        logs_path = self.path_to_run + "/logs"

        scheduler_log_file = logs_path + "/scheduler-0.debug.log.zst"
        controller_agent_log_file = logs_path + "/controller-agent-0.debug.log.zst"
        node_log_file = logs_path + "/node-0.debug.log.zst"

        # This whole test is not ideal and should be rewritten at some point.
        scheduler_log_start_line = 0
        decompressor = zstd.ZstdDecompressor()
        with open(scheduler_log_file, "rb") as fin:
            binary_reader = decompressor.stream_reader(fin, read_size=8192)
            text_stream = io.TextIOWrapper(binary_reader, encoding="utf-8")
            for line in text_stream:
                scheduler_log_start_line += 1

        run_test_vanilla("sleep 0.1", track=True)

        time.sleep(self.LOG_WRITE_WAIT_TIME)

        trace_id = None

        decompressor = zstd.ZstdDecompressor()
        with open(scheduler_log_file, "rb") as fin:
            binary_reader = decompressor.stream_reader(fin, read_size=8192)
            text_stream = io.TextIOWrapper(binary_reader, encoding="utf-8")
            for line in itertools.islice(text_stream, scheduler_log_start_line, None):
                if "AllocationTrackerService.Heartbeat ->" not in line:
                    continue
                if "StartedAllocations:" not in line:
                    continue

                started_allocation_count = int(line.split("StartedAllocations:", 1)[1].split(",", 1)[0].split()[1])
                if started_allocation_count == 0:
                    continue

                parts = line.strip().split("\t")

                # datetime \t log_level \t logger_name \t message \t txid \t fid \t trace_id
                assert len(parts) == 7

                trace_id = parts[-1]

        assert trace_id is not None

        has_allocation_registered_line = False
        has_spans_dropped_line_scheduler = False
        with open(scheduler_log_file, "rb") as fin:
            binary_reader = decompressor.stream_reader(fin, read_size=8192)
            text_stream = io.TextIOWrapper(binary_reader, encoding="utf-8")

            for line in text_stream:
                if line.strip().endswith(trace_id) and "Allocation registered" in line:
                    has_allocation_registered_line = True
                if "Spans dropped in test" in line:
                    has_spans_dropped_line_scheduler = True

        assert has_allocation_registered_line

        has_processing_scheduling_allocation_request_line = False
        has_spans_dropped_line_controller_agent = False
        with open(controller_agent_log_file, "rb") as fin:
            binary_reader = decompressor.stream_reader(fin, read_size=8192)
            text_stream = io.TextIOWrapper(binary_reader, encoding="utf-8")

            for line in text_stream:
                if line.strip().endswith(trace_id) and "Processing schedule allocation request" in line:
                    has_processing_scheduling_allocation_request_line = True
                if "Spans dropped in test" in line:
                    has_spans_dropped_line_controller_agent = True

        assert has_processing_scheduling_allocation_request_line

        has_preparing_scheduler_heartbeat_line = False
        has_successfully_reported_heartbeat_line = False
        has_allocation_created_line = False
        has_spans_dropped_line_node = False
        with open(node_log_file, "rb") as fin:
            binary_reader = decompressor.stream_reader(fin, read_size=8192)
            text_stream = io.TextIOWrapper(binary_reader, encoding="utf-8")

            for line in text_stream:
                if line.strip().endswith(trace_id):
                    if "Preparing scheduler heartbeat request" in line:
                        has_preparing_scheduler_heartbeat_line = True
                    if "Successfully reported heartbeat to scheduler" in line:
                        has_successfully_reported_heartbeat_line = True
                    if "Allocation created" in line:
                        has_allocation_created_line = True
                if "Spans dropped in test" in line:
                    has_spans_dropped_line_node = True

        assert has_preparing_scheduler_heartbeat_line
        assert has_successfully_reported_heartbeat_line
        assert has_allocation_created_line

        assert has_spans_dropped_line_scheduler
        assert has_spans_dropped_line_controller_agent
        assert has_spans_dropped_line_node

    @authors("eshcherbin")
    def test_disable_tracing(self):
        update_nodes_dynamic_config({
            "exec_node": {
                "scheduler_connector": {
                    "enable_tracing": False,
                },
            },
        })

        run_test_vanilla("sleep 0.1", track=True)
