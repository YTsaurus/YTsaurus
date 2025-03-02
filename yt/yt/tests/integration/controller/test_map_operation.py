from yt_env_setup import (
    YTEnvSetup,
    skip_if_porto,
    is_asan_build,
)

from yt_commands import (
    authors, events_on_fs, print_debug, raises_yt_error, remove, set_nodes_banned, wait, wait_breakpoint, release_breakpoint, with_breakpoint, create,
    ls, get, sorted_dicts,
    set, exists, create_user, make_ace, alter_table, write_file, read_table, write_table,
    map, merge, sort, interrupt_job, get_first_chunk_id,
    get_singular_chunk_id, check_all_stderrs,
    create_test_tables, assert_statistics, extract_statistic_v2,
    set_node_banned, update_inplace, update_controller_agent_config, update_nodes_dynamic_config, get_table_columnar_statistics)

from yt_type_helpers import make_schema, normalize_schema, make_column, list_type, tuple_type, optional_type

from yt_helpers import skip_if_no_descending, skip_if_old, skip_if_renaming_disabled

import yt.yson as yson
from yt.test_helpers import assert_items_equal
from yt.common import YtError

from flaky import flaky

import pytest
import random
import string
import base64

##################################################################


@pytest.mark.enabled_multidaemon
class TestSchedulerMapCommands(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_TEST_PARTITIONS = 12
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    ENABLE_UNIQUE_COUNT_CHECK = True

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "operations_update_period": 10,
            "running_allocations_update_period": 10,
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operations_update_period": 10,
            "map_operation_options": {
                "job_splitter": {
                    "min_job_time": 5000,
                    "min_total_data_size": 1024,
                    "update_period": 100,
                    "candidate_percentile": 0.8,
                    "max_jobs_per_split": 3,
                    "max_input_table_count": 5,
                },
            },
            # COMPAT(shakurov): change the default to false and remove
            # this delta once masters are up to date.
            "enable_prerequisites_for_starting_completion_transactions": False,
        }
    }

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "user_slots": 5,
                "cpu": 5,
                "memory": 5 * 1024 ** 3,
            }
        },
        # COMPAT(arkady-e1ppa):
        "exec_node": {
            "job_controller": {
                "resource_limits": {
                    "user_slots": 5,
                    "cpu": 5,
                    "memory": 5 * 1024 ** 3,
                }
            },
        },
    }

    @authors("ignat")
    def test_empty_table(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        map(in_="//tmp/t1", out="//tmp/t2", command="cat")

        assert read_table("//tmp/t2") == []

    @authors("babenko")
    def test_no_outputs(self):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", [{"key": "value"}])
        op = map(in_="//tmp/t1", command="cat > /dev/null; echo stderr>&2")
        check_all_stderrs(op, b"stderr\n", 1)

    @authors("acid", "ignat")
    def test_empty_range(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")

        original_data = [{"index": i} for i in range(10)]
        write_table("//tmp/t1", original_data)

        command = "cat"
        map(
            in_="<ranges=[{lower_limit={row_index=1}; upper_limit={row_index=1}}]>//tmp/t1",
            out="//tmp/t2",
            command=command,
        )

        assert [] == read_table("//tmp/t2", verbose=False)

    @authors("ignat")
    def test_one_chunk(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b"})
        op = map(
            track=False,
            in_="//tmp/t1",
            out="//tmp/t2",
            command=r'cat; echo "{v1=\"$V1\"};{v2=\"$TMPDIR\"}"',
            spec={
                "mapper": {"environment": {"V1": "Some data", "TMPDIR": "$(SandboxPath)/mytmp"}},
                "title": "MyTitle",
            },
        )

        get(op.get_path() + "/@spec")
        op.track()

        res = read_table("//tmp/t2")
        assert len(res) == 3
        assert res[0] == {"a": "b"}
        assert res[1] == {"v1": "Some data"}
        assert "v2" in res[2]
        assert res[2]["v2"].endswith("/mytmp")
        assert res[2]["v2"].startswith("/")

        if self.ENABLE_UNIQUE_COUNT_CHECK:
            all_statistics = get_table_columnar_statistics("[\"//tmp/t2{v1,v2}\"]")
            assert all_statistics[0]['column_estimated_unique_counts'] == {'v1': 1, 'v2': 1}

    @authors("ignat")
    @pytest.mark.skipif(is_asan_build(), reason="Test is too slow to fit into timeout")
    @pytest.mark.timeout(150)
    def test_big_input(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")

        count = 1000 * 1000
        original_data = [{"index": i} for i in range(count)]
        write_table("//tmp/t1", original_data)

        command = "cat"
        map(in_="//tmp/t1", out="//tmp/t2", command=command)

        new_data = read_table("//tmp/t2", verbose=False)
        assert sorted_dicts(new_data) == sorted_dicts([{"index": i} for i in range(count)])

        if self.ENABLE_UNIQUE_COUNT_CHECK:
            all_statistics = get_table_columnar_statistics("[\"//tmp/t2{index}\"]")
            assert all_statistics[0]['column_estimated_unique_counts'] == {'index': 790262}

    @authors("ignat")
    def test_two_outputs_at_the_same_time(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output1")
        create("table", "//tmp/t_output2")

        count = 1000
        original_data = [{"index": i} for i in range(count)]
        write_table("//tmp/t_input", original_data)

        file = "//tmp/some_file.txt"
        create("file", file)
        write_file(file, b"{value=42};\n")

        command = 'bash -c "cat <&0 & sleep 0.1; cat some_file.txt >&4; wait;"'
        map(
            in_="//tmp/t_input",
            out=["//tmp/t_output1", "//tmp/t_output2"],
            command=command,
            file=[file],
            verbose=True,
        )

        assert read_table("//tmp/t_output2") == [{"value": 42}]
        assert sorted_dicts(read_table("//tmp/t_output1")) == [{"index": i} for i in range(count)]

        if self.ENABLE_UNIQUE_COUNT_CHECK:
            all_statistics = get_table_columnar_statistics("[\"//tmp/t_output1{index}\";\"//tmp/t_output2{value}\"]")
            assert all_statistics[0]['column_estimated_unique_counts'] == {'index': 683}
            assert all_statistics[1]['column_estimated_unique_counts'] == {'value': 1}

    @authors("ignat")
    def test_write_two_outputs_consistently(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output1")
        create("table", "//tmp/t_output2")

        count = 10000
        original_data = [{"index": i} for i in range(count)]
        write_table("//tmp/t_input", original_data)

        file1 = "//tmp/some_file.txt"
        create("file", file1)
        write_file(file1, b"}}}}};\n")

        with pytest.raises(YtError):
            map(
                in_="//tmp/t_input",
                out=["//tmp/t_output1", "//tmp/t_output2"],
                command='cat some_file.txt >&4; cat >&4; echo "{value=42}"',
                file=[file1],
                verbose=True,
            )

    @authors("psushin")
    def test_in_equal_to_out(self):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", {"foo": "bar"})

        map(in_="//tmp/t1", out="<append=true>//tmp/t1", command="cat")

        assert read_table("//tmp/t1") == [{"foo": "bar"}, {"foo": "bar"}]

    @authors("psushin")
    def test_input_row_count(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"key": i} for i in range(5)])

        sort(in_="//tmp/t1", out="//tmp/t1", sort_by="key")
        op = map(command="cat", in_="//tmp/t1[:1]", out="//tmp/t2")

        assert get("//tmp/t2/@row_count") == 1

        wait(lambda: assert_statistics(op, "data.input.row_count", lambda row_count: row_count == 1))

    @authors("psushin")
    def test_multiple_output_row_count(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        create("table", "//tmp/t3")
        write_table("//tmp/t1", [{"key": i} for i in range(5)])

        op = map(
            command="cat; echo {hello=world} >&4",
            in_="//tmp/t1",
            out=["//tmp/t2", "//tmp/t3"],
        )
        assert get("//tmp/t2/@row_count") == 5

        wait(lambda: assert_statistics(op, "data.output.0.row_count", lambda row_count: row_count == 5))
        wait(lambda: assert_statistics(op, "data.output.1.row_count", lambda row_count: row_count == 1))

    @authors("renadeen")
    def test_codec_statistics(self):
        create("table", "//tmp/t1", attributes={"compression_codec": "lzma_9"})
        create("table", "//tmp/t2", attributes={"compression_codec": "lzma_1"})

        def random_string(n):
            return "".join(random.choice(string.printable) for _ in range(n))

        write_table(
            "//tmp/t1", [{str(i): random_string(1000)} for i in range(100)]
        )  # so much to see non-zero decode CPU usage in release mode

        op = map(command="cat", in_="//tmp/t1", out="//tmp/t2")
        wait(lambda: assert_statistics(op, "codec.cpu.decode.lzma_9", lambda decode_time: decode_time > 0))
        wait(lambda: assert_statistics(op, "codec.cpu.encode.0.lzma_1", lambda encode_time: encode_time > 0))

    @authors("psushin")
    @pytest.mark.parametrize("sort_kind", ["sorted_by", "ascending", "descending"])
    def test_sorted_output(self, sort_kind):
        if sort_kind == "descending":
            skip_if_no_descending(self.Env)

        create("table", "//tmp/t1")
        for i in range(2):
            write_table("<append=true>//tmp/t1", {"key": "foo", "value": "ninja"})

        echoed_rows = ["{key=$k1; value=one}", "{key=$k2; value=two}"]
        if sort_kind == "descending":
            echoed_rows = echoed_rows[::-1]

        command = """cat >/dev/null;
           if [ "$YT_JOB_INDEX" = "0" ]; then
               k1=0; k2=1;
           else
               k1=0; k2=0;
           fi
           echo "{}"
        """.format("; ".join(echoed_rows))

        attributes = {}
        if sort_kind == "sorted_by":
            out_table = "<sorted_by=[key];append=true>//tmp/t2"
        else:
            out_table = "<append=true>//tmp/t2"
            attributes["schema"] = [{"name": "key", "sort_order": sort_kind, "type": "int64"},
                                    {"name": "value", "type": "string"}]
        create("table", "//tmp/t2", attributes=attributes)

        map(
            in_="//tmp/t1",
            out=out_table,
            command=command,
            spec={"job_count": 2},
        )

        expected_rows = [
            {"key": 0, "value": "one"},
            {"key": 0, "value": "two"},
            {"key": 0, "value": "one"},
            {"key": 1, "value": "two"},
        ]

        if sort_kind == "descending":
            expected_rows = expected_rows[::-1]

        assert get("//tmp/t2/@sorted")
        assert get("//tmp/t2/@sorted_by") == ["key"]
        assert read_table("//tmp/t2") == expected_rows

    @authors("psushin")
    @pytest.mark.parametrize("sort_kind", ["sorted_by", "ascending", "descending"])
    def test_sorted_output_overlap(self, sort_kind):
        if sort_kind == "descending":
            skip_if_no_descending(self.Env)

        create("table", "//tmp/t1")
        for i in range(2):
            write_table("<append=true>//tmp/t1", {"key": "foo", "value": "ninja"})

        echoed_rows = ["{key=1; value=one}", "{key=2; value=two}"]
        if sort_kind == "descending":
            echoed_rows = echoed_rows[::-1]

        command = 'cat >/dev/null; echo "{}"'.format("; ".join(echoed_rows))

        attributes = {}
        if sort_kind == "sorted_by":
            out_table = "<sorted_by=[key];append=true>//tmp/t2"
        else:
            out_table = "<append=true>//tmp/t2"
            attributes["schema"] = [{"name": "key", "sort_order": sort_kind, "type": "int64"},
                                    {"name": "value", "type": "string"}]
        create("table", "//tmp/t2", attributes=attributes)

        with pytest.raises(YtError):
            map(
                in_="//tmp/t1",
                out=out_table,
                command=command,
                spec={"job_count": 2},
            )

    @authors("psushin")
    @pytest.mark.parametrize("sort_kind", ["sorted_by", "ascending", "descending"])
    def test_sorted_output_job_failure(self, sort_kind):
        if sort_kind == "descending":
            skip_if_no_descending(self.Env)

        create("table", "//tmp/t1")
        for i in range(2):
            write_table("<append=true>//tmp/t1", {"key": "foo", "value": "ninja"})

        echoed_rows = ["{key=2; value=one}", "{key=1; value=two}"]
        if sort_kind == "descending":
            echoed_rows = echoed_rows[::-1]

        command = 'cat >/dev/null; echo "{}"'.format("; ".join(echoed_rows))

        attributes = {}
        if sort_kind == "sorted_by":
            out_table = "<sorted_by=[key];append=true>//tmp/t2"
        else:
            out_table = "<append=true>//tmp/t2"
            attributes["schema"] = [{"name": "key", "sort_order": sort_kind, "type": "int64"},
                                    {"name": "value", "type": "string"}]
        create("table", "//tmp/t2", attributes=attributes)

        with pytest.raises(YtError):
            map(
                in_="//tmp/t1",
                out=out_table,
                command=command,
                spec={"job_count": 2},
            )

    @authors("psushin")
    def test_job_count(self):
        create("table", "//tmp/t1")
        for i in range(5):
            write_table("<append=true>//tmp/t1", {"foo": "bar"})

        command = "cat > /dev/null; echo {hello=world}"

        def check(table_name, job_count, expected_num_records):
            create("table", table_name)
            map(
                in_="//tmp/t1",
                out=table_name,
                command=command,
                spec={"job_count": job_count},
            )
            assert read_table(table_name) == [{"hello": "world"} for _ in range(expected_num_records)]

        check("//tmp/t2", 3, 3)
        check("//tmp/t3", 10, 5)  # number of jobs cannot be more than number of rows.

    @authors("max42")
    def test_skewed_rows(self):
        create("table", "//tmp/t1")
        # 5 small rows
        write_table("<append=true>//tmp/t1", [{"foo": "bar"}] * 5)
        # and one large row
        write_table("<append=true>//tmp/t1", {"foo": "".join(["r"] * 1024)})

        create("table", "//tmp/t2")
        map(
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat > /dev/null; echo {hello=world}",
            spec={"job_count": 6},
        )
        assert read_table("//tmp/t2") == [{"hello": "world"} for _ in range(6)]

    # We skip this one in Porto because it requires a lot of interaction with Porto
    # (since there are a lot of operations with large number of jobs).
    # There is completely nothing Porto-specific here.
    @authors("ignat")
    @skip_if_porto
    @pytest.mark.timeout(120)
    def test_job_per_row(self):
        create("table", "//tmp/input")

        job_count = 976
        original_data = [{"index": str(i)} for i in range(job_count)]
        write_table("//tmp/input", original_data)

        create("table", "//tmp/output", ignore_existing=True)

        for job_count in range(976, 960, -1):
            op = map(
                track=False,
                in_="//tmp/input",
                out="//tmp/output",
                command="sleep 100",
                spec={"job_count": job_count},
            )

            wait(lambda: len(op.get_running_jobs()) > 1)

            assert op.get_job_count("total") == job_count
            op.abort()

    def run_many_output_tables(self, yamr_mode=False):
        output_tables = ["//tmp/t%d" % i for i in range(3)]

        create("table", "//tmp/t_in")
        for table_path in output_tables:
            create("table", table_path)

        write_table("//tmp/t_in", {"a": "b"})

        if yamr_mode:
            mapper = b"cat  > /dev/null; echo {v = 0} >&3; echo {v = 1} >&4; echo {v = 2} >&5"
        else:
            mapper = b"cat  > /dev/null; echo {v = 0} >&1; echo {v = 1} >&4; echo {v = 2} >&7"

        create("file", "//tmp/mapper.sh")
        write_file("//tmp/mapper.sh", mapper)

        map(
            in_="//tmp/t_in",
            out=output_tables,
            command="bash mapper.sh",
            file="//tmp/mapper.sh",
            spec={"mapper": {"use_yamr_descriptors": yamr_mode}},
        )

        assert read_table(output_tables[0]) == [{"v": 0}]
        assert read_table(output_tables[1]) == [{"v": 1}]
        assert read_table(output_tables[2]) == [{"v": 2}]

    @authors("ignat")
    def test_many_output_yt(self):
        self.run_many_output_tables()

    @authors("ignat")
    def test_many_output_yamr(self):
        self.run_many_output_tables(True)

    @authors("ignat")
    def test_output_tables_switch(self):
        output_tables = ["//tmp/t%d" % i for i in range(3)]

        create("table", "//tmp/t_in")
        for table_path in output_tables:
            create("table", table_path)

        write_table("//tmp/t_in", {"a": "b"})
        mapper = b'cat  > /dev/null; echo "<table_index=2>#;{v = 0};{v = 1};<table_index=0>#;{v = 2}"'

        create("file", "//tmp/mapper.sh")
        write_file("//tmp/mapper.sh", mapper)

        map(
            in_="//tmp/t_in",
            out=output_tables,
            command="bash mapper.sh",
            file="//tmp/mapper.sh",
        )

        assert read_table(output_tables[0]) == [{"v": 2}]
        assert read_table(output_tables[1]) == []
        assert read_table(output_tables[2]) == [{"v": 0}, {"v": 1}]

    @authors("ignat")
    def test_executable_mapper(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})

        mapper = b"""
#!/bin/bash
cat > /dev/null; echo {hello=world}
"""

        create("file", "//tmp/mapper.sh")
        write_file("//tmp/mapper.sh", mapper)

        set("//tmp/mapper.sh/@executable", True)

        create("table", "//tmp/t_out")
        map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="./mapper.sh",
            file="//tmp/mapper.sh",
        )

        assert read_table("//tmp/t_out") == [{"hello": "world"}]

    @authors("ignat")
    @pytest.mark.ignore_in_opensource_ci
    def test_table_index(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        create("table", "//tmp/out")

        write_table("//tmp/t1", {"key": "a", "value": "value"})
        write_table("//tmp/t2", {"key": "b", "value": "value"})

        mapper = b"""
import sys
table_index = sys.stdin.readline().strip()
row = sys.stdin.readline().strip()
print(row + table_index)

table_index = sys.stdin.readline().strip()
row = sys.stdin.readline().strip()
print(row + table_index)
"""

        create("file", "//tmp/mapper.py")
        write_file("//tmp/mapper.py", mapper)

        map(
            in_=["//tmp/t1", "//tmp/t2"],
            out="//tmp/out",
            command="python3 mapper.py",
            file="//tmp/mapper.py",
            spec={"mapper": {"format": yson.loads(b"<enable_table_index=true>yamr")}},
        )

        expected = [{"key": "a", "value": "value0"}, {"key": "b", "value": "value1"}]
        assert_items_equal(read_table("//tmp/out"), expected)

    @authors("ignat")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_range_index(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        create("table", "//tmp/t_in", attributes={
            "schema": make_schema([
                {"name": "key", "type": "string", "sort_order": sort_order},
                {"name": "value", "type": "string", "sort_order": sort_order},
            ])})
        create("table", "//tmp/out")

        rows = [0, 1, 2]
        if sort_order == "descending":
            rows = rows[::-1]
        for row in rows:
            write_table(
                "<append=true>//tmp/t_in",
                [
                    {"key": "%05d" % row, "value": "value"},
                ],
            )

        if sort_order == "ascending":
            t_in = '<ranges=['\
                '{lower_limit={key=["00002"]};upper_limit={key=["00003"]}};'\
                '{lower_limit={key=["00002"]};upper_limit={key=["00003"]}}'\
                ']>//tmp/t_in'
        else:
            t_in = '<ranges=['\
                '{lower_limit={key=["00002"]};upper_limit={key=["00001"]}};'\
                '{lower_limit={key=["00002"]};upper_limit={key=["00001"]}}'\
                ']>//tmp/t_in'

        op = map(
            track=False,
            in_=[t_in],
            out="//tmp/out",
            command="cat >& 2",
            spec={
                "job_io": {
                    "control_attributes": {
                        "enable_range_index": True,
                        "enable_row_index": True,
                    }
                },
                "mapper": {
                    "input_format": yson.loads(b"<format=text>yson"),
                    "output_format": "dsv",
                },
            },
        )

        op.track()
        check_all_stderrs(op, b'"range_index"=0;', 1, substring=True)
        check_all_stderrs(op, b'"range_index"=1;', 1, substring=True)

    @authors("ogorod")
    def test_insane_demand(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", {"cool": "stuff"})

        with pytest.raises(YtError):
            map(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                command="cat",
                spec={"mapper": {"memory_limit": 1000000000000}},
            )

    def check_input_fully_consumed_statistics_base(
        self, cmd, throw_on_failure=False, expected_value=1, expected_output=None
    ):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"foo": "bar"} for _ in range(10000)])

        op = map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            track=False,
            command=cmd,
            spec={
                "mapper": {
                    "input_format": "dsv",
                    "output_format": "dsv",
                    "max_failed_job_count": 1,
                    "check_input_fully_consumed": throw_on_failure,
                }
            },
        )

        if throw_on_failure:
            with pytest.raises(YtError):
                op.track()
        else:
            op.track()

        if expected_output is not None:
            assert read_table("//tmp/t_out") == expected_output

        wait(lambda: assert_statistics(
            op,
            key="data.input.not_fully_consumed",
            assertion=lambda value: value == expected_value,
            job_state="failed" if throw_on_failure else "completed",
            job_type="map",
            summary_type="max"))

    @authors("ogorod")
    def test_check_input_fully_consumed_statistics_simple(self):
        self.check_input_fully_consumed_statistics_base("python3 -c 'print(input())'", expected_output=[{"foo": "bar"}])

    @authors("acid", "ogorod")
    def test_check_input_fully_consumed_statistics_all_consumed(self):
        self.check_input_fully_consumed_statistics_base("cat", expected_value=0)

    @authors("tramsmm", "ogorod")
    def test_check_input_fully_consumed_statistics_throw_on_failure(self):
        self.check_input_fully_consumed_statistics_base("exit 0", throw_on_failure=True, expected_output=[])

    @authors("max42")
    def test_live_preview(self):
        create_user("u")

        data = [{"foo": i} for i in range(5)]

        create("table", "//tmp/t1")
        write_table("//tmp/t1", data)

        create("table", "//tmp/t2")
        set("//tmp/t2/@acl", [make_ace("allow", "u", "write")])
        effective_acl = get("//tmp/t2/@effective_acl")

        schema = make_schema(
            [{"name": "foo", "type": "int64", "required": False}],
            strict=True,
            unique_keys=False,
        )
        alter_table("//tmp/t2", schema=schema)

        op = map(
            track=False,
            command=with_breakpoint("cat && BREAKPOINT"),
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"data_size_per_job": 1},
        )
        jobs = wait_breakpoint(job_count=2)

        operation_path = op.get_path()
        async_transaction_id = get(operation_path + "/@async_scheduler_transaction_id")
        assert exists(operation_path + "/output_0", tx=async_transaction_id)
        assert effective_acl == get(operation_path + "/output_0/@acl", tx=async_transaction_id)
        assert schema == normalize_schema(get(operation_path + "/output_0/@schema", tx=async_transaction_id))

        for job_id in jobs[:2]:
            release_breakpoint(job_id=job_id)

        wait(lambda: op.get_job_count("completed") >= 2)

        def check():
            live_preview_data = read_table(operation_path + "/output_0", tx=async_transaction_id)
            return len(live_preview_data) == 2 and all(record in data for record in live_preview_data)

        wait(check)

        release_breakpoint()
        op.track()
        assert sorted_dicts(read_table("//tmp/t2")) == sorted_dicts(data)

    @authors("max42")
    def test_new_live_preview_multiple_output_tables(self):
        create_user("u")

        data = [{"foo": i} for i in range(5)]

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", data)

        create("table", "//tmp/t_out1")
        create("table", "//tmp/t_out2")

        op = map(
            track=False,
            command=with_breakpoint('echo "{a=$YT_JOB_INDEX}" >&1; echo "{b=$YT_JOB_INDEX}" >&4; BREAKPOINT'),
            in_="//tmp/t_in",
            out=["//tmp/t_out1", "//tmp/t_out2"],
            spec={"data_size_per_job": 1},
        )

        jobs = wait_breakpoint(job_count=2)
        operation_path = op.get_path()
        for job_id in jobs[:2]:
            release_breakpoint(job_id=job_id)

        wait(lambda: op.get_job_count("completed") == 2)
        wait(lambda: exists(operation_path + "/controller_orchid"))

        live_preview_paths = ["data_flow_graph/vertices/map/live_previews/"]
        if self.Env.get_component_version("ytserver-controller-agent").abi >= (23, 3):
            live_preview_paths.append("live_previews/output_")

        for live_preview_path in live_preview_paths:
            live_preview_data1 = read_table(
                f"{operation_path}/controller_orchid/{live_preview_path}0"
            )
            live_preview_data2 = read_table(
                f"{operation_path}/controller_orchid/{live_preview_path}1"
            )
            live_preview_data1 = [d["a"] for d in live_preview_data1]
            live_preview_data2 = [d["b"] for d in live_preview_data2]
            assert sorted(live_preview_data1) == sorted(live_preview_data2)
            assert len(live_preview_data1) == 2

        release_breakpoint()
        op.track()

    @authors("max42", "ignat")
    def test_row_sampling(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        create("table", "//tmp/t3")

        count = 1000
        original_data = [{"index": i} for i in range(count)]
        write_table("//tmp/t1", original_data)

        command = "cat"
        sampling_rate = 0.5
        spec = {"job_io": {"table_reader": {"sampling_seed": 42, "sampling_rate": sampling_rate}}}

        map(in_="//tmp/t1", out="//tmp/t2", command=command, spec=spec)
        map(in_="//tmp/t1", out="//tmp/t3", command=command, spec=spec)

        new_data_t2 = read_table("//tmp/t2", verbose=False)
        new_data_t3 = read_table("//tmp/t3", verbose=False)

        assert sorted_dicts(new_data_t2) == sorted_dicts(new_data_t3)

        actual_rate = len(new_data_t2) * 1.0 / len(original_data)
        variation = sampling_rate * (1 - sampling_rate)
        assert sampling_rate - variation <= actual_rate <= sampling_rate + variation

    @authors("achulkov2")
    def test_truncate_composite_partitioning_sample(self):
        # TODO(achulkov2): Lower/remove after cherry-picks.
        if self.Env.get_component_version("ytserver-job-proxy").abi <= (24, 1):
            pytest.skip()

        max_sample_size = 64 * 1024  # 64KiB
        big_size = 15 * 1024 * 1024  # 15MiB, a bit smaller than the default row limit.

        schema = [{"name": "data", "type_v3": tuple_type(["string", optional_type("int64"), list_type("string")])}]

        create("table", "//tmp/t_in", attributes={"schema": schema})
        create("table", "//tmp/t_out", attributes={"schema": schema})

        # The last two strings are pretty big and definitely shouldn't be in the sample.
        data = [{"data": ["is", 42, ["the", "answer", "to", "life" * (max_sample_size // 8), "the" * (max_sample_size // 9),
                                     "universe" * (max_sample_size // 32), "and" , "everything" * (big_size // 10)]]}]

        write_table("//tmp/t_in", data, verbose=False)
        map(in_="//tmp/t_in", out="//tmp/t_out", command="cat", spec={"max_failed_job_count": 1})

        assert read_table("//tmp/t_out") == data

        chunk_ids = get("//tmp/t_out/@chunk_ids")
        assert len(chunk_ids) == 1

        assert get(f"#{chunk_ids[0]}/@meta_size") < 2 * max_sample_size

    @authors("psushin")
    @pytest.mark.parametrize("ordered", [False, True])
    def test_map_row_count_limit(self, ordered):
        create("table", "//tmp/input")
        for i in range(5):
            write_table("<append=true>//tmp/input", {"key": "%05d" % i, "value": "foo"})

        create("table", "//tmp/output")
        op = map(
            track=False,
            in_="//tmp/input",
            out="<row_count_limit=3>//tmp/output",
            command=with_breakpoint("cat ; BREAKPOINT"),
            ordered=ordered,
            spec={"data_size_per_job": 1, "max_failed_job_count": 1},
        )
        jobs = wait_breakpoint(job_count=5)
        assert len(jobs) == 5

        for job_id in jobs[:3]:
            release_breakpoint(job_id=job_id)

        op.track()
        assert len(read_table("//tmp/output")) == 3

    @authors("psushin")
    def test_job_controller_orchid(self):
        create("table", "//tmp/input")
        for i in range(5):
            write_table("<append=true>//tmp/input", {"key": "%05d" % i, "value": "foo"})

        create("table", "//tmp/output")
        op = map(
            track=False,
            in_="//tmp/input",
            out="//tmp/output",
            command=with_breakpoint("cat && BREAKPOINT"),
            spec={"data_size_per_job": 1, "max_failed_job_count": 1},
        )
        wait_breakpoint(job_count=5)

        nodes_with_flavors = get("//sys/cluster_nodes", attributes=["flavors"])

        exec_nodes = [
            node
            for node, attr in nodes_with_flavors.items() if "exec" in attr.attributes.get("flavors", ["exec"])
        ]

        for node in exec_nodes:
            scheduler_jobs = get("//sys/cluster_nodes/{0}/orchid/exec_node/job_controller/active_jobs".format(node))
            slot_manager = get("//sys/cluster_nodes/{0}/orchid/exec_node/slot_manager".format(node))

            for job_id, values in scheduler_jobs.items():
                assert "start_time" in values
                assert "operation_id" in values
                assert "statistics" in values
                assert "job_type" in values
                assert "duration" in values

            assert "free_slot_count" in slot_manager
            assert "slot_count" in slot_manager

        op.abort()

    @authors("psushin")
    def test_map_row_count_limit_second_output(self):
        create("table", "//tmp/input")
        for i in range(5):
            write_table("<append=true>//tmp/input", {"key": "%05d" % i, "value": "foo"})

        create("table", "//tmp/out_1")
        create("table", "//tmp/out_2")
        op = map(
            track=False,
            in_="//tmp/input",
            out=["//tmp/out_1", "<row_count_limit=3>//tmp/out_2"],
            command=with_breakpoint("cat >&4 ; BREAKPOINT"),
            spec={"data_size_per_job": 1, "max_failed_job_count": 1},
        )

        jobs = wait_breakpoint(job_count=5)
        for job_id in jobs[:3]:
            release_breakpoint(job_id=job_id)

        op.track()
        assert len(read_table("//tmp/out_1")) == 0
        assert len(read_table("//tmp/out_2")) == 3

    @authors("psushin")
    def test_multiple_row_count_limit(self):
        create("table", "//tmp/input")
        create("table", "//tmp/out_1")
        create("table", "//tmp/out_2")
        with pytest.raises(YtError):
            map(
                in_="//tmp/input",
                out=[
                    "<row_count_limit=1>//tmp/out_1",
                    "<row_count_limit=1>//tmp/out_2",
                ],
                command="cat",
            )

    @authors("gritukan")
    def test_negative_row_count_limit(self):
        create("table", "//tmp/input")
        create("table", "//tmp/output")
        with pytest.raises(YtError):
            map(
                in_="//tmp/input",
                out=[
                    "<row_count_limit=-1>//tmp/output",
                ],
                command="cat",
            )

    @authors("psushin")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_schema_validation(self, optimize_for):
        schema = make_schema(
            [
                {"name": "key", "type": "int64", "required": False},
                {"name": "value", "type": "string", "required": False},
            ],
            strict=True,
            unique_keys=False,
        )
        create("table", "//tmp/input")
        create(
            "table",
            "//tmp/output",
            attributes={"optimize_for": optimize_for, "schema": schema},
        )
        create("table", "//tmp/output2")

        for i in range(10):
            write_table("<append=true>//tmp/input", {"key": i, "value": "foo"})

        map(in_="//tmp/input", out="//tmp/output", command="cat")

        assert get("//tmp/output/@schema_mode") == "strong"
        assert get("//tmp/output/@schema/@strict")
        assert normalize_schema(get("//tmp/output/@schema")) == schema
        assert_items_equal(read_table("//tmp/output"), [{"key": i, "value": "foo"} for i in range(10)])

        map(
            in_="//tmp/input",
            out="<schema=%s>//tmp/output2" % yson.dumps(schema).decode(),
            command="cat",
        )

        assert get("//tmp/output2/@schema_mode") == "strong"
        assert get("//tmp/output2/@schema/@strict")
        assert normalize_schema(get("//tmp/output2/@schema")) == schema
        assert_items_equal(
            read_table("//tmp/output2"),
            [{"key": i, "value": "foo"} for i in range(10)],
        )

        write_table("//tmp/input", {"key": "1", "value": "foo"})

        with pytest.raises(YtError):
            map(in_="//tmp/input", out="//tmp/output", command="cat")

    @authors("dakovalkov")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_rename_columns_simple(seld, optimize_for):
        create(
            "table",
            "//tmp/tin",
            attributes={
                "schema": [{"name": "a", "type": "int64"}],
                "optimize_for": optimize_for,
            },
        )
        create("table", "//tmp/tout")
        write_table("//tmp/tin", [{"a": 42}])

        map(in_="<rename_columns={a=b}>//tmp/tin", out="//tmp/tout", command="cat")
        assert read_table("//tmp/tout") == [{"b": 42}]

    @authors("dakovalkov")
    def test_rename_columns_without_schema(self):
        create("table", "//tmp/tin")
        create("table", "//tmp/tout")
        write_table("//tmp/tin", [{"a": 42}])

        with pytest.raises(YtError):
            map(in_="<rename_columns={a=b}>//tmp/tin", out="//tmp/tout", command="cat")

    @authors("dakovalkov")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_raname_columns_overlapping_names_in_schema(self, optimize_for):
        create(
            "table",
            "//tmp/tin",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64"},
                    {"name": "b", "type": "int64"},
                ],
                "optimize_for": optimize_for,
            },
        )
        create("table", "//tmp/tout")
        write_table("//tmp/tin", [{"a": 42, "b": 34}])

        with pytest.raises(YtError):
            map(in_="<rename_columns={a=b}>//tmp/tin", out="//tmp/tout", command="cat")

    @authors("dakovalkov")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_rename_columns_overlapping_names_in_chunk(self, optimize_for):
        create("table", "//tmp/tin", attributes={"optimize_for": optimize_for})
        create("table", "//tmp/tout")
        write_table("//tmp/tin", [{"a": 42, "b": 34}])

        # Set weak schema
        sort(in_="//tmp/tin", out="//tmp/tin", sort_by="a")

        with pytest.raises(YtError):
            map(in_="<rename_columns={a=b}>//tmp/tin", out="//tmp/tout", command="cat")

    @authors("dakovalkov")
    def test_rename_columns_wrong_name(self):
        create(
            "table",
            "//tmp/tin",
            attributes={
                "schema": [{"name": "a", "type": "int64"}],
            },
        )
        create("table", "//tmp/tout")
        write_table("//tmp/tin", [{"a": 42}])
        with pytest.raises(YtError):
            map(
                in_='<rename_columns={a="$wrong_name"}>//tmp/tin',
                out="//tmp/tout",
                command="cat",
            )

        with pytest.raises(YtError):
            map(in_='<rename_columns={a=""}>//tmp/tin', out="//tmp/tout", command="cat")

        with pytest.raises(YtError):
            map(
                in_="<rename_columns={a=" + "b" * 1000 + "}>//tmp/tin",
                out="//tmp/tout",
                command="cat",
            )

    @authors("dakovalkov")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_rename_columns_swap(self, optimize_for):
        create(
            "table",
            "//tmp/tin",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64"},
                    {"name": "b", "type": "int64"},
                ],
                "optimize_for": optimize_for,
            },
        )
        create("table", "//tmp/tout")
        write_table("//tmp/tin", [{"a": 42, "b": 34}])

        map(in_="<rename_columns={a=b;b=a}>//tmp/tin", out="//tmp/tout", command="cat")

        assert read_table("//tmp/tout") == [{"b": 42, "a": 34}]

    @authors("dakovalkov")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_rename_columns_filter(self, optimize_for):
        create(
            "table",
            "//tmp/tin",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64"},
                    {"name": "b", "type": "int64"},
                ],
                "optimize_for": optimize_for,
            },
        )
        create("table", "//tmp/tout")
        write_table("//tmp/tin", [{"a": 42, "b": 34}])

        map(in_="<rename_columns={a=d}>//tmp/tin{d}", out="//tmp/tout", command="cat")

        assert read_table("//tmp/tout") == [{"d": 42}]

    @authors("levysotsky")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_rename_schema(self, optimize_for):
        skip_if_renaming_disabled(self.Env)

        input_table = "//tmp/tin"
        output_table = "//tmp/tout"

        schema1 = make_schema([
            make_column("a", "int64"),
            make_column("b", "string"),
            make_column("c", "bool"),
        ])
        create(
            "table",
            input_table,
            attributes={
                "schema": schema1,
                "optimize_for": optimize_for,
            },
        )
        create("table", output_table)
        write_table(input_table, [{"a": 42, "b": "42", "c": False}])

        schema2 = make_schema([
            make_column("b", "string"),
            make_column("c_new", "bool", stable_name="c"),
            make_column("a_new", "int64", stable_name="a"),
        ])
        alter_table(input_table, schema=schema2)

        write_table("<append=%true>" + input_table, [{"a_new": 43, "b": "43", "c_new": False}])

        map(in_=input_table, out=output_table, command="cat")
        assert sorted(read_table(output_table), key=lambda r: r["a_new"]) == [
            {"a_new": 42, "b": "42", "c_new": False},
            {"a_new": 43, "b": "43", "c_new": False},
        ]

        input_table_with_filter = "<columns=[a_new;b]>" + input_table
        map(in_=input_table_with_filter, out=output_table, command="cat")
        assert sorted(read_table(output_table), key=lambda r: r["a_new"]) == [
            {"a_new": 42, "b": "42"},
            {"a_new": 43, "b": "43"},
        ]

        input_table_with_rename = "<rename_columns={a_new=a_renamed;b=b_renamed}>" + input_table
        map(in_=input_table_with_rename, out=output_table, command="cat")
        assert sorted(read_table(output_table), key=lambda r: r["a_renamed"]) == [
            {"a_renamed": 42, "b_renamed": "42", "c_new": False},
            {"a_renamed": 43, "b_renamed": "43", "c_new": False},
        ]

        attributes = "<columns=[a_renamed;c_new]; rename_columns={a_new=a_renamed;b=b_renamed}>"
        input_table_with_rename_and_filter = attributes + input_table
        map(in_=input_table_with_rename_and_filter, out=output_table, command="cat")
        assert sorted(read_table(output_table), key=lambda r: r["a_renamed"]) == [
            {"a_renamed": 42, "c_new": False},
            {"a_renamed": 43, "c_new": False},
        ]

    @authors("levysotsky")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    @pytest.mark.parametrize("strict_schema", [True, False])
    def test_rename_with_both_columns_in_filter(self, optimize_for, strict_schema):
        input_table = "//tmp/tin"
        output_table = "//tmp/tout"

        create(
            "table",
            input_table,
            attributes={
                "schema": make_schema(
                    [
                        {"name": "original", "type": "int64"},
                    ],
                    strict=strict_schema,
                ),
            }
        )
        write_table(input_table, [{"original": None}, {"original": 1}])

        create("table", output_table)
        map(
            in_=[
                '<columns=["original";"new"]; rename_columns={"original"="new"}>' + input_table,
            ],
            out=[output_table],
            command="cat",
        )

        result = read_table(output_table)
        assert result == [
            {"new": None},
            {"new": 1},
        ]

    @authors("evgenstf")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_two_tables_with_column_filter(self, optimize_for):
        create(
            "table",
            "//tmp/input_table_1",
            attributes={
                "schema": [
                    {"name": "first_column_1", "type": "int64"},
                    {"name": "second_column_1", "type": "int64"},
                    {"name": "third_column_1", "type": "int64"},
                ],
                "optimize_for": optimize_for,
            },
        )
        write_table(
            "//tmp/input_table_1",
            [{"first_column_1": 1001, "second_column_1": 2001, "third_column_1": 3001}],
        )

        create(
            "table",
            "//tmp/input_table_2",
            attributes={
                "schema": [
                    {"name": "first_column_2", "type": "int64"},
                    {"name": "second_column_2", "type": "int64"},
                    {"name": "third_column_2", "type": "int64"},
                ],
                "optimize_for": optimize_for,
            },
        )
        write_table(
            "//tmp/input_table_2",
            [{"first_column_2": 1002, "second_column_2": 2002, "third_column_2": 3002}],
        )

        create(
            "table",
            "//tmp/input_table_3",
            attributes={
                "schema": [
                    {"name": "first_column_2", "type": "int64"},
                    {"name": "second_column_2", "type": "int64"},
                    {"name": "third_column_2", "type": "int64"},
                ],
                "optimize_for": optimize_for,
            },
        )
        write_table(
            "//tmp/input_table_3",
            [{"first_column_2": 1002, "second_column_2": 2002, "third_column_2": 3002}],
        )

        create("table", "//tmp/output_table_1")
        create("table", "//tmp/output_table_2")
        create("table", "//tmp/output_table_3")

        map(
            in_=[
                '<columns=["first_column_1";"third_column_1"]>//tmp/input_table_1',
                '<columns=["first_column_2";"second_column_2"]>//tmp/input_table_2',
                '<columns=["first_column_2";"second_column_2"]>//tmp/input_table_3',
            ],
            out=[
                "//tmp/output_table_1",
                "//tmp/output_table_2",
                "//tmp/output_table_3",
            ],
            command="cat",
        )

        assert read_table("//tmp/output_table_1") == [{"first_column_1": 1001, "third_column_1": 3001}]
        assert read_table("//tmp/output_table_2") == [{"first_column_2": 1002, "second_column_2": 2002}]
        assert read_table("//tmp/output_table_3") == [{"first_column_2": 1002, "second_column_2": 2002}]

    @authors("lukyan")
    @pytest.mark.parametrize("mode", ["unordered", "ordered"])
    def test_computed_columns(self, mode):
        create("table", "//tmp/t1")
        create(
            "table",
            "//tmp/t2",
            attributes={
                "schema": [
                    {"name": "k1", "type": "int64", "expression": "k2 * 2"},
                    {"name": "k2", "type": "int64"},
                ]
            },
        )

        write_table("//tmp/t1", [{"k2": i} for i in range(2)])

        map(mode=mode, in_="//tmp/t1", out="//tmp/t2", command="cat")

        assert get("//tmp/t2/@schema_mode") == "strong"
        assert read_table("//tmp/t2") == [{"k1": i * 2, "k2": i} for i in range(2)]

    @authors("psushin", "ignat")
    def test_map_max_data_size_per_job(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        op = map(
            track=False,
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command="cat",
            spec={"max_data_size_per_job": 1},
        )

        with pytest.raises(YtError):
            op.track()

    @authors("psushin")
    def test_ordered_map_multiple_ranges(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        rows = [
            {"field": 1},
            {"field": 42},
            {"field": 63},
            {"field": 100500},
        ]
        write_table("<sorted_by=[field]>//tmp/t_input", rows)

        map(
            ordered=True,
            in_="//tmp/t_input[1,42,63,100500]",
            out="<sorted_by=[field]>//tmp/t_output",
            command="cat",
        )

        assert read_table("//tmp/t_output") == rows

    def _run_interruptible_map_operation(self, ordered, output, spec_patch={}):
        spec = {
            "mapper": {"format": "json"},
            "max_failed_job_count": 1,
            "job_io": {
                "buffer_row_count": 1,
            },
            "enable_job_splitting": False,
            "max_speculative_job_count_per_task": 0,
        }

        update_inplace(spec, spec_patch)

        mapper = b"""#!/usr/bin/env python3

import json

input = json.loads(input())
old_value = input["value"]
input["value"] = "(job)"
print(json.dumps(input))
input["value"] = old_value
print(json.dumps(input))

"""

        create("file", "//tmp/mapper.py")
        write_file("//tmp/mapper.py", mapper)

        # NB(arkady-e1ppa): we force no bufferisation because otherwise we may read something like
        # "row1End\nrow2Start" and discard row2start completely.
        map_cmd = """python3 -u mapper.py ; BREAKPOINT ; cat"""

        op = map(
            ordered=ordered,
            track=False,
            in_="//tmp/in_1",
            out=output,
            file="//tmp/mapper.py",
            command=with_breakpoint(map_cmd),
            spec=spec,
        )

        jobs = wait_breakpoint()
        op.interrupt_job(jobs[0])

        release_breakpoint()
        op.track()

        return op

    @authors("klyachin")
    @pytest.mark.parametrize("ordered", [False, True])
    def test_map_interrupt_job(self, ordered):
        create("table", "//tmp/in_1")
        write_table(
            "//tmp/in_1",
            [{"key": "%08d" % i, "value": "(t_1)", "data": "a" * (2 * 1024 * 1024)} for i in range(3)],
            table_writer={"block_size": 1024, "desired_chunk_size": 1024},
            output_format="json",
        )

        output = "//tmp/output"
        job_type = "map"
        if ordered:
            output = "<sorted_by=[key]>" + output
            job_type = "ordered_map"
        create("table", output)

        op = self._run_interruptible_map_operation(ordered=ordered, output=output)

        result = read_table("//tmp/output", verbose=False)
        for row in result:
            print_debug(f"Key: {row['key']}, value: {row['value']}")
        assert len(result) == 5
        if not ordered:
            result = sorted_dicts(result)
        row_index = 0
        job_indexes = []
        for row in result:
            assert row["key"] == "%08d" % row_index
            if row["value"] == "(job)":
                job_indexes.append(int(row["key"]))
            else:
                row_index += 1
        assert 0 < job_indexes[1] < 99999

        wait(lambda: assert_statistics(
            op,
            key="data.input.row_count",
            assertion=lambda row_count: row_count == len(result) - 2,
            job_type=job_type))

    @authors("arkady-e1ppa")
    @pytest.mark.parametrize("ordered", [False, True])
    @pytest.mark.parametrize("small_pipe", [False, True])
    @pytest.mark.ignore_in_opensource_ci
    def test_map_interrupt_job_with_pipe_capacity(self, ordered, small_pipe):
        create("table", "//tmp/in_1")
        write_table(
            "//tmp/in_1",
            [{"key": "%08d" % i, "value": "(t_1)", "data": "a" * (1 * 4 * 1024)} for i in range(3)],
            table_writer={"block_size": 1024, "desired_chunk_size": 1024},
            output_format="json",
        )

        output = "//tmp/output"
        job_type = "map"
        if ordered:
            output = "<sorted_by=[key]>" + output
            job_type = "ordered_map"
        create("table", output)

        pipe_capacity = 4096
        if not small_pipe:
            pipe_capacity = 16 * 4096

        op = self._run_interruptible_map_operation(ordered, output, spec_patch={
            "job_io": {
                "pipe_capacity": pipe_capacity,
            },
        })

        result = read_table("//tmp/output")
        print_debug(result)
        for row in result:
            print_debug("key:", row["key"], "value:", row["value"], "data size:", len(row["data"]))

        added_rows = 2
        if not small_pipe:
            added_rows = 1

        assert len(result) == 3 + added_rows
        if not ordered:
            result = sorted_dicts(result)
        row_index = 0
        job_indexes = []
        for row in result:
            assert row["key"] == "%08d" % row_index
            if row["value"] == "(job)":
                job_indexes.append(int(row["key"]))
            else:
                row_index += 1

        if small_pipe:
            assert job_indexes[1] > 0

        wait(lambda: assert_statistics(
            op,
            key="data.input.row_count",
            assertion=lambda row_count: row_count == len(result) - added_rows,
            job_type=job_type))

    @authors("arkady-e1ppa")
    @pytest.mark.skip(reason="broken until YIML-219 is closed")
    @pytest.mark.parametrize("ordered", [False, True])
    @pytest.mark.parametrize("fmt", ["json", "dsv"])
    @pytest.mark.ignore_in_opensource_ci
    def test_map_interrupt_job_with_delivery_fenced_pipe_writer(self, ordered, fmt):
        if any(version in getattr(self, "ARTIFACT_COMPONENTS", {}) for version in ["23_2", "24_1"]):
            pytest.xfail("Is not supported for older versions of server components")

        # Test explanation:
        # Each job reads one row and writes it twice
        # first time with a marker, which indicates
        # that a job has started and read something
        # and then job dumps whatever is left in the pipe.
        # This way, total row count indicates, how many
        # lines each job has received.
        # We have a very small table which ensures job count
        # up to 2 (including the one after interruption)
        # Since the row count is 3, then we either see
        # each job read at least one row (thus writing >= 2 rows)
        # or the test fails.
        create("table", "//tmp/in_1")
        write_table(
            "//tmp/in_1",
            [{"key": "%08d" % i, "value": "(t_1)"} for i in range(3)],
            table_writer={"block_size": 1024, "desired_chunk_size": 1024},
            output_format=fmt,
        )

        output = "//tmp/output"
        job_type = "map"
        if ordered:
            output = "<sorted_by=[key]>" + output
            job_type = "ordered_map"
        create("table", output)

        # Read line -> find pattern (???) in line and replace it with (job)
        # -> print modified line -> prinit initial line -> wait at BREAKPOINT
        # -> print whatever is left in pipe.
        map_cmd = """read; echo "${REPLY/(???)/(job)}"; echo "$REPLY"; BREAKPOINT ; cat """

        op = map(
            ordered=ordered,
            track=False,
            in_="//tmp/in_1",
            out=output,
            command=with_breakpoint(map_cmd),
            spec={
                "mapper": {"format": fmt},
                "max_failed_job_count": 1,
                "job_io": {
                    "buffer_row_count": 1,
                    "use_delivery_fenced_pipe_writer": True,
                },
                "enable_job_splitting": False,
            },
        )

        jobs = wait_breakpoint()
        op.interrupt_job(jobs[0])

        release_breakpoint()
        op.track()

        result = read_table("//tmp/output")
        print_debug(result)
        for row in result:
            print_debug("key:", row["key"], "value:", row["value"])

        assert len(result) == 5
        if not ordered:
            result = sorted_dicts(result)
        row_index = 0
        job_indexes = []
        for row in result:
            assert row["key"] == "%08d" % row_index
            if row["value"] == "(job)":
                job_indexes.append(int(row["key"]))
            else:
                row_index += 1

        assert job_indexes[1] > 0

        wait(lambda: assert_statistics(
            op,
            key="data.input.row_count",
            assertion=lambda row_count: row_count == len(result) - 2,
            job_type=job_type))

    @authors("arkady-e1ppa")
    @pytest.mark.parametrize("ordered", [False, True])
    def test_adaptive_buffer_row_count(self, ordered):
        skip_if_old(self.Env, (24, 2), "Option is not present in older binaries")

        input_table = "//tmp/in"
        output_table = "//tmp/out"
        row_count = 1000

        create("table", input_table)
        write_table(
            input_table,
            [{"value": f"{i}"} for i in range(row_count + 1)]
        )

        attributes = "<sorted_by=[key]>" if ordered else ""
        output = f"{attributes}{output_table}"
        create("table", output)

        update_nodes_dynamic_config(value=row_count * 100, path="exec_node/job_controller/job_proxy/pipe_reader_timeout_threshold")

        op = map(
            ordered=ordered,
            track=False,
            in_=input_table,
            out=output,
            command=with_breakpoint("""read row; echo $row; BREAKPOINT; cat"""),
            spec={
                "job_count": 1,
                "job_io": {
                    "buffer_row_count": 1,
                    "use_adaptive_buffer_row_count": True,
                },
                "max_failed_job_count": 1,
            },
        )

        (job_id, ) = wait_breakpoint()

        buffer_row_count = -1

        for node in ls("//sys/cluster_nodes"):
            orchid_prefix = f"//sys/cluster_nodes/{node}/orchid/exec_node/job_controller/active_jobs"
            orchid_suffix = "job_proxy/job_io/buffer_row_count"
            if job_id not in get(orchid_prefix):
                continue

            buffer_row_count = get(f"{orchid_prefix}/{job_id}/{orchid_suffix}")
            break

        assert buffer_row_count > 1

        release_breakpoint()
        op.track()

    @authors("galtsev")
    @pytest.mark.parametrize("job_count", list(range(1, 4)))
    @pytest.mark.parametrize("ordered", [False, True])
    @pytest.mark.ignore_in_opensource_ci
    def test_force_allow_job_interruption(self, job_count, ordered):
        skip_if_old(self.Env, (24, 1), "Operations with explicit job count are not interruptible in old controller agents")

        input_table = "//tmp/in"
        output_table = "//tmp/out"

        create("table", input_table)
        write_table(
            input_table,
            [{"key": f"{i:08d}", "data": "a" * (2 * 1024 * 1024)} for i in range(10 * job_count)],
        )

        attributes = "<sorted_by=[key]>" if ordered else ""
        output = f"{attributes}{output_table}"
        create("table", output)

        op = map(
            ordered=ordered,
            track=False,
            in_=input_table,
            out=output,
            command=with_breakpoint("""read row; echo $row; BREAKPOINT; sleep 5; cat"""),
            spec={
                "force_allow_job_interruption": True,
                "job_count": job_count,
                "job_io": {
                    "buffer_row_count": 1,
                },
            },
        )

        jobs = wait_breakpoint()
        interrupt_job(jobs[0])
        release_breakpoint()
        op.track()

        assert get(f"{input_table}/@row_count") == get(f"{output_table}/@row_count")

        assert op.get_job_count("completed") > job_count

    @authors("dakovalkov", "gritukan")
    @pytest.mark.xfail(run=False, reason="YT-14467")
    @flaky(max_runs=3)
    def test_map_soft_interrupt_job(self):
        create_test_tables(row_count=1)

        command = """(trap "echo '{interrupt=42}'; exit 0" SIGINT; BREAKPOINT;)"""

        op = map(
            track=False,
            command=with_breakpoint(command),
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "mapper": {
                    "interruption_signal": "SIGINT",
                },
            },
        )

        jobs = wait_breakpoint()
        interrupt_job(jobs[0])
        op.track()

        assert read_table("//tmp/t_out") == [{"interrupt": 42}]

    # YT-6324: false job interrupt when it does not consume any input data.
    @authors("klyachin")
    @pytest.mark.parametrize("ordered", [False, True])
    def test_map_no_consumption(self, ordered):
        create("table", "//tmp/in_1")
        write_table(
            "//tmp/in_1",
            [{"key": "%08d" % i, "value": "(t_1)", "data": "a" * (2 * 1024 * 1024)} for i in range(3)],
            table_writer={"block_size": 1024, "desired_chunk_size": 1024},
        )

        output = "//tmp/output"
        if ordered:
            output = "<sorted_by=[key]>" + output
        create("table", output)

        op = map(
            ordered=ordered,
            track=False,
            in_="//tmp/in_1",
            out=output,
            command="true",
            spec={
                "mapper": {"format": "dsv"},
                "max_failed_job_count": 1,
                "job_io": {
                    "buffer_row_count": 1,
                },
                "enable_job_splitting": False,
            },
        )
        op.track()

        assert get(op.get_path() + "/@progress/jobs/completed/total") == 1
        assert get(op.get_path() + "/@progress/jobs/completed/non-interrupted") == 1

    @authors("savrus")
    def test_ordered_map_many_jobs(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        original_data = [{"index": i} for i in range(10)]
        for row in original_data:
            write_table("<append=true>//tmp/t_input", row)

        op = map(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command="cat; echo stderr 1>&2",
            ordered=True,
            spec={"data_size_per_job": 1},
        )

        assert len(op.list_jobs()) == 10
        assert read_table("//tmp/t_output") == original_data

    @authors("achulkov2")
    def test_batch_row_count(self):
        # TODO(achulkov2): Lower/remove after cherry-picks.
        if self.Env.get_component_version("ytserver-controller-agent").abi <= (24, 1):
            pytest.skip()

        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")

        chunk_sizes = [15, 43, 57, 179, 2, 239, 13, 29, 315]
        original_data = [
            [{"chunk": chunk_index, "index": i} for i in range(chunk_sizes[chunk_index])]
            for chunk_index in range(len(chunk_sizes))
        ]
        for rows in original_data:
            write_table("<append=true>//tmp/t_input", rows)

        batch_row_count = 32

        map(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command="cat; echo stderr 1>&2",
            ordered=True,
            spec={"data_size_per_job": 1000, "batch_row_count": batch_row_count},
        )

        assert read_table("//tmp/t_output") == sum(original_data, start=[])
        chunk_ids = get("//tmp/t_output/@chunk_ids")
        assert sum(get(f"#{chunk_id}/@row_count") % batch_row_count == 0 for chunk_id in chunk_ids) >= len(chunk_ids) - 1

    @authors("max42", "savrus")
    @pytest.mark.parametrize("with_output_schema", [False, True])
    def test_ordered_map_remains_sorted(self, with_output_schema):
        create(
            "table",
            "//tmp/t_input",
            attributes={"schema": [{"name": "key", "sort_order": "ascending", "type": "int64"}]},
        )
        create("table", "//tmp/t_output")
        original_data = [{"key": i} for i in range(1000)]
        for i in range(10):
            write_table("<append=true>//tmp/t_input", original_data[100 * i:100 * (i + 1)])

        map(
            in_="//tmp/t_input",
            out="<sorted_by=[key]>//tmp/t_output" if with_output_schema else "//tmp/t_output",
            command="cat; sleep $(($RANDOM % 5)); echo stderr 1>&2",
            ordered=True,
            spec={"job_count": 5},
        )

        if with_output_schema:
            assert get("//tmp/t_output/@sorted")
            assert get("//tmp/t_output/@sorted_by") == ["key"]
        assert read_table("//tmp/t_output") == original_data

    # This is a really strange case that was added after YT-7507.
    @authors("max42")
    def test_ordered_map_job_count_consider_only_primary_size(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        for i in range(20):
            write_table("<append=true>//tmp/t_input", [{"a": "x" * 1024 * 1024}])

        op = map(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            ordered=True,
            command="cat >/dev/null; echo stderr 1>&2",
            spec={"job_count": 10, "consider_only_primary_size": True},
        )

        assert len(op.list_jobs()) == 10

    @authors("klyachin")
    @pytest.mark.parametrize("ordered", [False, True])
    def test_map_job_splitter(self, ordered):
        create("table", "//tmp/in_1")
        write_table(
            "//tmp/in_1",
            [{"key": "%08d" % i, "value": "(t_1)", "data": "a" * (1024 * 1024)} for i in range(20)],
        )

        input_ = "//tmp/in_1"
        output = "//tmp/output"
        create("table", output)

        command = """
while read ROW; do
    if [ "$YT_JOB_INDEX" == 0 ]; then
        sleep 10
    else
        sleep 0.1
    fi
    echo "$ROW"
done
"""

        op = map(
            ordered=ordered,
            track=False,
            in_=input_,
            out=output,
            command=command,
            spec={
                "mapper": {
                    "format": "dsv",
                },
                "data_size_per_job": 21 * 1024 * 1024,
                "max_failed_job_count": 1,
                "job_io": {
                    "buffer_row_count": 1,
                },
            },
        )

        op.track()

        completed = get(op.get_path() + "/@progress/jobs/completed")
        interrupted = completed["interrupted"]
        assert completed["total"] >= 2
        assert interrupted["job_split"] >= 1
        expected = read_table("//tmp/in_1", verbose=False)
        for row in expected:
            del row["data"]
        got = read_table(output, verbose=False)
        for row in got:
            del row["data"]
        assert sorted_dicts(got) == sorted_dicts(expected)

    @authors("babenko")
    def test_job_splitter_max_input_table_count(self):
        create("table", "//tmp/in_1")
        write_table(
            "//tmp/in_1",
            [{"key": "%08d" % i, "value": "(t_1)", "data": "a" * (1024 * 1024)} for i in range(20)],
        )

        input_ = "//tmp/in_1"
        output = "//tmp/output"
        create("table", output)

        op = map(
            in_=[input_] * 10,
            out=output,
            command="sleep 5; echo '{a=1}'")
        op.track()

        completed = get(op.get_path() + "/@progress/jobs/completed")
        interrupted = completed["interrupted"]
        assert interrupted["job_split"] == 0

    @authors("ifsmirnov")
    def test_disallow_partially_sorted_output(self):
        create(
            "table",
            "//tmp/t",
            attributes={"schema": [{"name": "key", "type": "int64", "sort_order": "ascending"}]},
        )
        write_table("//tmp/t", [{"key": 1}])

        with pytest.raises(YtError):
            map(in_="//tmp/t", out="<partially_sorted=%true>//tmp/t", command="cat")

    @authors("gritukan")
    def test_data_flow(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b"})

        def get_directions(op):
            data_flow = get(op.get_path() + "/@progress/data_flow")
            directions = {}
            for direction in data_flow:
                directions[(direction["source_name"], direction["target_name"])] = direction
            return directions

        op = map(track=False, in_="//tmp/t1", out="//tmp/t2", command="cat")
        op.track()

        directions = get_directions(op)
        assert len(directions) == 2
        assert directions[("input", "map")]["job_data_statistics"]["data_weight"] == 0
        assert directions[("input", "map")]["teleport_data_statistics"]["data_weight"] == 2
        assert directions[("map", "output")]["job_data_statistics"]["data_weight"] == 2
        assert directions[("map", "output")]["teleport_data_statistics"]["data_weight"] == 0

        op = map(
            track=False,
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat",
            spec={"auto_merge": {"mode": "relaxed"}},
        )
        op.track()
        assert read_table("//tmp/t2") == [{"a": "b"}]

        directions = get_directions(op)
        assert len(directions) == 3
        assert directions[("input", "map")]["job_data_statistics"]["data_weight"] == 0
        assert directions[("input", "map")]["teleport_data_statistics"]["data_weight"] == 2
        assert directions[("map", "auto_merge")]["job_data_statistics"]["data_weight"] == 2
        assert directions[("map", "auto_merge")]["teleport_data_statistics"]["data_weight"] == 0
        assert directions[("auto_merge", "output")]["job_data_statistics"]["data_weight"] + directions[("auto_merge", "output")]["teleport_data_statistics"]["data_weight"] == 2

    @authors("gritukan")
    def test_data_flow_graph(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b"})

        def get_edges(graph):
            result = {}
            for from_ in graph["edges"].keys():
                for to in graph["edges"][from_].keys():
                    result[(from_, to)] = graph["edges"][from_][to]

            return result

        op = map(track=False, in_="//tmp/t1", out="//tmp/t2", command="cat")
        op.track()

        data_flow_graph = get(op.get_path() + "/@progress/data_flow_graph")
        assert data_flow_graph["topological_ordering"] == ["source", "map", "sink"]

        edges = get_edges(data_flow_graph)
        assert len(edges) == 2
        assert edges[("source", "map")]["statistics"]["row_count"] == 1
        assert edges[("map", "sink")]["statistics"]["row_count"] == 1

        op = map(
            track=False,
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat",
            spec={"auto_merge": {"mode": "relaxed"}},
        )
        op.track()

        data_flow_graph = get(op.get_path() + "/@progress/data_flow_graph")
        assert data_flow_graph["topological_ordering"] == [
            "source",
            "map",
            "auto_merge",
            "sink",
        ]

        edges = get_edges(data_flow_graph)
        assert len(edges) == 3
        assert edges[("source", "map")]["statistics"]["row_count"] == 1
        assert edges[("map", "auto_merge")]["statistics"]["row_count"] == 1
        assert edges[("auto_merge", "sink")]["statistics"]["row_count"] == 1

    @authors("gritukan")
    def test_end_of_stream(self):
        create("table", "//tmp/in1")
        create("table", "//tmp/in2")
        create("table", "//tmp/out")

        write_table("//tmp/in1", [{"x": 1}])
        write_table("//tmp/in2", [{"x": 2}])

        op = map(
            ordered=True,
            in_=["//tmp/in1", "//tmp/in2"],
            out="//tmp/out",
            command="cat 1>&2",
            spec={
                "job_io": {"control_attributes": {"enable_end_of_stream": True}},
                "mapper": {"format": yson.loads(b"<format=text>yson")},
                "job_count": 1,
            },
        )

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        stderr_bytes = op.read_stderr(job_ids[0])

        assert (
            stderr_bytes
            == b"""<"table_index"=0;>#;
{"x"=1;};
<"table_index"=1;>#;
{"x"=2;};
<"end_of_stream"=%true;>#;
"""
        )

    @authors("gritukan")
    def test_block_cache(self):
        if self.Env.get_component_version("ytserver-job-proxy").abi <= (20, 3):
            pytest.skip()
        if self.Env.get_component_version("ytserver-controller-agent").abi <= (21, 3):
            pytest.skip()

        create("table", "//tmp/in")
        create("table", "//tmp/out")
        write_table("//tmp/in", [{"x": i, "y": "A" * 10000} for i in range(100)])

        input_tables = ["//tmp/in[#{}:#{}]".format(i, i + 1) for i in range(100)]

        op = map(
            in_=input_tables,
            out="//tmp/out",
            command="grep xx || true",
            spec={
                "job_count": 1,
                "job_io": {
                    "table_reader": {
                        "use_async_block_cache": True,
                    },
                    "block_cache": {
                        "compressed_data": {
                            "capacity": 1024 ** 2
                        },
                    },
                },
            },
        )
        op.track()

        statistics = get(op.get_path() + "/@progress/job_statistics_v2")

        read_from_disk = extract_statistic_v2(statistics, "chunk_reader_statistics.data_bytes_read_from_disk")
        read_from_cache = extract_statistic_v2(statistics, "chunk_reader_statistics.data_bytes_read_from_cache")
        assert read_from_cache == 99 * read_from_disk

    @authors("alexkolodezny")
    def test_chunk_reader_timing_statistics(self):
        if self.Env.get_component_version("ytserver-job-proxy").abi < (21, 3):
            pytest.skip()
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        write_table("//tmp/t_in", [{"x": i, "y": "A" * 10000} for i in range(100)])

        op = map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="sleep 1",
        )

        wait(lambda: assert_statistics(op, "chunk_reader_statistics.idle_time", lambda idle_time: idle_time > 1000))

    @authors("egor-gutrov")
    def test_auto_create(self):
        create("table", "//tmp/t_input")
        write_table("//tmp/t_input", [{"a": "b"}])

        with pytest.raises(YtError):
            map(
                in_="//tmp/t_input",
                out="//tmp/t_output",
                command="cat",
            )
        map(
            in_="//tmp/t_input",
            out="<create=true>//tmp/t_output",
            spec={
                "core_table_path": "<create=true>//tmp/core_table",
                "stderr_table_path": "<create=true>//tmp/stderr_table",
            },
            command="cat",
        )
        assert read_table("//tmp/t_output") == [{"a": "b"}]
        assert exists("//tmp/stderr_table")
        assert exists("//tmp/core_table")

        with pytest.raises(YtError):
            map(
                in_="//tmp/t_input",
                out="<create=true>//tmp/t_output1",
                spec={
                    "core_table_path": "<create=true>//tmp/core_table1",
                    "stderr_table_path": "<create=true>//tmp/stderr_table1",
                },
                command="exit 1",
            )
        assert not exists("//tmp/t_output1")
        assert exists("//tmp/stderr_table1")
        assert exists("//tmp/core_table1")

    @authors("galtsev")
    @pytest.mark.parametrize("hunks", [True, False])
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_interrupts_disabled_for_static_table_with_hunks(self, hunks, optimize_for):
        if self.Env.get_component_version("ytserver-controller-agent").abi <= (23, 2):
            pytest.skip()

        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "any"},
        ]

        if hunks:
            schema[1]["max_inline_hunk_size"] = 10

        create("table", "//tmp/t_in", attributes={
            "optimize_for": optimize_for,
            "schema": schema,
        })
        create("table", "//tmp/t_out")

        rows = [{"key": 0, "value": "0"}, {"key": 2, "value": "z" * 100}]
        write_table("//tmp/t_in", rows)

        def run_map():
            op = map(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                spec={"mapper": {"format": "json"}},
                command=with_breakpoint("""read row; echo $row; BREAKPOINT; cat"""),
                track=False,
            )

            jobs = wait_breakpoint()
            interrupt_job(jobs[0])
            release_breakpoint()
            op.track()

        if hunks:
            with pytest.raises(YtError, match="Error interrupting job"):
                run_map()
        else:
            run_map()

            assert read_table("//tmp/t_out") == rows

    @authors("coteeq")
    def test_preallocate_chunk_lists(self):
        if self.Env.get_component_version("ytserver-controller-agent").abi <= (23, 2):
            pytest.skip()

        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        op = map(
            track=False,
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat && echo stderr > /proc/self/fd/2 && BREAKPOINT"),
        )

        # orchid does not outlive operation, so we need to inspect it in the middle of the operation
        wait_breakpoint()

        assert get(
            op.get_path() + "/controller_orchid/progress/schedule_job_statistics/failed/not_enough_chunk_lists"
        ) == 0

        release_breakpoint()
        op.track()

    @authors("coteeq")
    def test_map_unavailable_chunk_on_job(self):
        create("table", "//tmp/t_in", attributes={
            "schema": [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "int64"},
            ],
            "replication_factor": 1,
        })
        data = [
            {"key": 1, "value": 1},
            {"key": 2, "value": 1},
            {"key": 3, "value": 1},
        ]
        write_table("//tmp/t_in", data)

        # Filter out nodes which host the input table,
        # so we do not accidentally ban nodes with jobs.
        chunk_ids = get("//tmp/t_in/@chunk_ids")
        assert len(chunk_ids) == 1
        table_nodes = [str(node) for node in get("#" + str(chunk_ids[0]) + "/@stored_replicas")]
        assert len(table_nodes) == 1
        all_nodes = get("//sys/cluster_nodes")
        non_table_nodes = [node for node in all_nodes if node not in table_nodes]
        assert len(non_table_nodes) == self.NUM_NODES - 1
        scheduling_filter = " | ".join(non_table_nodes)

        try:
            update_controller_agent_config("chunk_location_throttler/limit", 0.0)

            op = map(
                track=False,
                command=";".join([
                    events_on_fs().wait_event_cmd("run_job"),
                    "cat",
                ]),
                in_=["//tmp/t_in"],
                out=["<create=%true>//tmp/t_out"],
                spec={
                    "testing": {
                        # Need some delay, so we can ban nodes before job started
                        "build_job_spec_proto_delay": 2000,
                    },
                    "scheduling_tag_filter": scheduling_filter,
                    "job_io": {
                        "table_reader": {
                            # Fail fast
                            "retry_count": 1,
                        }
                    },
                }
            )

            # Wait until controller fetches tables.
            wait(lambda: op.get_state() == "running")

            # Lose the chunk. Hopefully we will do this while controller prepares
            # jobspec (thus build_job_spec_proto_delay in operation spec).
            set_nodes_banned(table_nodes, True)
            wait(lambda: get("#{0}/@replication_status/default/lost".format(chunk_ids[0])))

            # Wait for the job to be aborted due to failed chunks.
            wait(lambda: op.get_job_count("aborted") > 0)

            def get_unavailable_chunks_orchid():
                path = "/controller_orchid/unavailable_input_chunks"
                if self.Env.get_component_version("ytserver-controller-agent").abi >= (24, 1):
                    path += "/<local>"
                return path
            assert get(op.get_path() + get_unavailable_chunks_orchid()) == [chunk_ids[0]]

            # Allow to locate chunk
            remove("//sys/controller_agents/config/chunk_location_throttler")

            # Allow jobs to finish
            events_on_fs().notify_event("run_job")
            set_nodes_banned(table_nodes, False)
            op.track()

        finally:
            remove("//sys/controller_agents/config/chunk_location_throttler", force=True)
            set_nodes_banned(table_nodes, False)

        assert sorted_dicts(read_table("//tmp/t_out")) == sorted_dicts(data)


##################################################################


class TestSchedulerMapCommandsPorto(TestSchedulerMapCommands):
    ENABLE_MULTIDAEMON = False  # Use porto.
    USE_PORTO = True


##################################################################


@pytest.mark.enabled_multidaemon
class TestSchedulerMapCommandsMulticell(TestSchedulerMapCommands):
    ENABLE_MULTIDAEMON = True
    NUM_TEST_PARTITIONS = 15
    NUM_SECONDARY_MASTER_CELLS = 2

    @authors("babenko")
    def test_multicell_input_fetch(self):
        create("table", "//tmp/t1", attributes={"external_cell_tag": 11})
        write_table("//tmp/t1", [{"a": 1}])
        create("table", "//tmp/t2", attributes={"external_cell_tag": 12})
        write_table("//tmp/t2", [{"a": 2}])

        create("table", "//tmp/t_in", attributes={"external": False})
        merge(mode="ordered", in_=["//tmp/t1", "//tmp/t2"], out="//tmp/t_in")

        create("table", "//tmp/t_out")
        map(in_="//tmp/t_in", out="//tmp/t_out", command="cat")

        assert_items_equal(read_table("//tmp/t_out"), [{"a": 1}, {"a": 2}])


##################################################################


@pytest.mark.enabled_multidaemon
class TestSchedulerMapCommandsPortal(TestSchedulerMapCommandsMulticell):
    ENABLE_MULTIDAEMON = True
    ENABLE_TMP_PORTAL = True


@pytest.mark.enabled_multidaemon
class TestSchedulerMapCommandsShardedTx(TestSchedulerMapCommandsPortal):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 5
    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["cypress_node_host"]},
        "11": {"roles": ["cypress_node_host"]},
        "12": {"roles": ["chunk_host"]},
        "13": {"roles": ["cypress_node_host"]},
        "14": {"roles": ["transaction_coordinator"]},
        "15": {"roles": ["transaction_coordinator"]},
    }


@pytest.mark.enabled_multidaemon
class TestSchedulerMapCommandsShardedTxCTxS(TestSchedulerMapCommandsShardedTx):
    ENABLE_MULTIDAEMON = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

    DELTA_RPC_PROXY_CONFIG = {
        "cluster_connection": {
            "transaction_manager": {
                "use_cypress_transaction_service": True,
            }
        }
    }


@pytest.mark.enabled_multidaemon
class TestSchedulerMapCommandsMirroredTx(TestSchedulerMapCommandsShardedTxCTxS):
    ENABLE_MULTIDAEMON = True
    USE_SEQUOIA = True
    ENABLE_CYPRESS_TRANSACTIONS_IN_SEQUOIA = True
    ENABLE_TMP_ROOTSTOCK = False
    NUM_TEST_PARTITIONS = 24

    def setup_method(self, method):
        super(TestSchedulerMapCommandsShardedTxCTxS, self).setup_method(method)
        set("//sys/@config/transaction_manager/forbid_transaction_actions_for_cypress_transactions", True)
        update_controller_agent_config(
            "set_committed_attribute_via_transaction_action",
            False,
            wait_for_orchid=False)
        update_controller_agent_config(
            "commit_operation_cypress_node_changes_via_system_transaction",
            True)


##################################################################


@pytest.mark.enabled_multidaemon
class TestWriteBufferEstimation(YTEnvSetup):
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    @authors("don-dron")
    def test_write_buffer_estimation(self):
        columns = []

        for i in range(32):
            columns.append("c{}".format(i))

        schema = []

        for c in columns:
            schema.append({"name": c, "type": "string"})

        create("table", "//tmp/t_input", attributes={"schema": schema, "optimize_for": "scan", "replication_factor": 1})
        create("table", "//tmp/t_output", attributes={"schema": schema, "optimize_for": "scan", "replication_factor": 1})

        for row in range(1):
            values = []

            for c in columns:
                values.append({c: "{}".format(row)})

            write_table("<append=true>//tmp/t_input", values, verbose=False)

        op = map(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command="cat",
            spec={
                "mapper": {"format": "dsv"},
                "resource_limits": {"user_slots": 10},
                "job_count": 10,
                "max_speculative_job_count_per_task": 0,
                "enable_job_splitting": False,
                "auto_merge": {
                    "mode": "disabled"
                }
            },
        )

        op.track()

        statistics = get(op.get_path() + "/controller_orchid/progress/job_statistics_v2")

        old_memory_reserve_statistics = extract_statistic_v2(
            statistics,
            key="job_proxy.memory_reserve",
            summary_type="sum")
        # GetInputIOMemorySize + GetFinalOutputIOMemorySize.
        # base + 10 job, fixed buffer with 16 megabytes.
        print_debug("Memory reserve with fixed wrtie buffers {}".format(old_memory_reserve_statistics))
        assert old_memory_reserve_statistics >= 1751063386 + 16 * 10 * 1024**2

        op = map(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command="cat",
            spec={
                "enable_write_buffer_size_estimation": True,
                "mapper": {"format": "dsv"},
                "resource_limits": {"user_slots": 10},
                "max_speculative_job_count_per_task": 0,
                "enable_job_splitting": False,
                "job_count": 10,
                "auto_merge": {
                    "mode": "disabled"
                }
            },
        )

        op.track()

        statistics = get(op.get_path() + "/controller_orchid/progress/job_statistics_v2")

        memory_reserve_statistics = extract_statistic_v2(
            statistics,
            key="job_proxy.memory_reserve",
            summary_type="sum")
        # GetInputIOMemorySize + GetFinalOutputIOMemorySize.
        # This diff is due to a change in the size of the write buffer
        # base + 10 job, 32 columns, 2 megabytes.
        print_debug("Memory reserve with estimated wrtie buffers {}".format(memory_reserve_statistics))
        assert memory_reserve_statistics >= 1751063386 + 10 * 32 * 2 * 1024**2

        alerts = op.get_alerts()
        assert "write_buffer_memory_overrun" in alerts
        assert len(alerts) == 1

        alert = alerts["write_buffer_memory_overrun"]

        for inner in alert["inner_errors"]:
            estimated = inner["attributes"]["memory_with_estimated_buffers"]
            fixed = inner["attributes"]["memory_with_fixed_buffers"]
            print_debug("Write buffers {} {}".format(estimated, fixed))
            assert estimated - fixed == 32 * 2 * 1024**2 - 16 * 1024**2


##################################################################


class TestJobSizeAdjuster(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {"controller_agent": {"map_operation_options": {"data_size_per_job": 1}}}

    @authors("max42")
    @pytest.mark.skipif("True", reason="YT-8228")
    def test_map_job_size_adjuster_boost(self):
        create("table", "//tmp/t_input")
        original_data = [{"index": "%05d" % i} for i in range(31)]
        for row in original_data:
            write_table("<append=true>//tmp/t_input", row, verbose=False)

        create("table", "//tmp/t_output")

        op = map(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command="echo lines=`wc -l`",
            spec={"mapper": {"format": "dsv"}, "resource_limits": {"user_slots": 1}},
        )

        expected = [{"lines": str(2 ** i)} for i in range(5)]
        actual = read_table("//tmp/t_output")
        assert_items_equal(actual, expected)
        estimated = get(op.get_path() + "/@progress/tasks/0/estimated_input_data_weight_histogram")
        histogram = get(op.get_path() + "/@progress/tasks/0/input_data_weight_histogram")
        assert estimated == histogram
        assert histogram["max"] / histogram["min"] == 16
        assert histogram["count"][0] == 1
        assert sum(histogram["count"]) == 5

    @authors("max42")
    def test_map_job_size_adjuster_max_limit(self):
        create("table", "//tmp/t_input")
        original_data = [{"index": "%05d" % i} for i in range(31)]
        for row in original_data:
            write_table("<append=true>//tmp/t_input", row, verbose=False)
        chunk_id = get_first_chunk_id("//tmp/t_input")
        chunk_size = get("#{0}/@data_weight".format(chunk_id))
        create("table", "//tmp/t_output")

        map(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command="echo lines=`wc -l`",
            spec={
                "mapper": {"format": "dsv"},
                "max_data_size_per_job": chunk_size * 4,
                "resource_limits": {"user_slots": 3},
            },
        )

        for row in read_table("//tmp/t_output"):
            assert int(row["lines"]) < 5

    @authors("ignat")
    def test_map_unavailable_chunk(self):
        create("table", "//tmp/t_input", attributes={"replication_factor": 1})
        original_data = [{"index": "%05d" % i} for i in range(20)]
        write_table("<append=true>//tmp/t_input", original_data[0], verbose=False)
        chunk_id = get_singular_chunk_id("//tmp/t_input")

        chunk_size = get("#{0}/@uncompressed_data_size".format(chunk_id))
        replicas = get("#{0}/@stored_replicas".format(chunk_id))
        assert len(replicas) == 1
        replica_to_ban = str(replicas[0])  # str() is for attribute stripping.

        banned = False
        for node in ls("//sys/cluster_nodes"):
            if node == replica_to_ban:
                set_node_banned(node, True)
                banned = True
        assert banned

        wait(lambda: get("#{0}/@replication_status/default/lost".format(chunk_id)))

        for row in original_data[1:]:
            write_table("<append=true>//tmp/t_input", row, verbose=False)
        chunk_ids = get("//tmp/t_input/@chunk_ids")
        assert len(chunk_ids) == len(original_data)

        create("table", "//tmp/t_output")
        op = map(
            track=False,
            command="sleep $YT_JOB_INDEX; cat",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={"data_size_per_job": chunk_size * 2},
        )

        wait(lambda: op.get_job_count("completed") > 3)

        unbanned = False
        for node in ls("//sys/cluster_nodes"):
            if node == replica_to_ban:
                set_node_banned(node, False)
                unbanned = True
        assert unbanned

        op.track()
        assert op.get_state() == "completed"


##################################################################


@pytest.mark.enabled_multidaemon
class TestInputOutputFormats(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "operations_update_period": 10,
            "running_allocations_update_period": 10,
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
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
        }
    }

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "user_slots": 5,
                "cpu": 5,
                "memory": 5 * 1024 ** 3,
            }
        },
        # COMPAT(arkady-e1ppa)
        "exec_node": {
            "job_controller": {
                "resource_limits": {
                    "user_slots": 5,
                    "cpu": 5,
                    "memory": 5 * 1024 ** 3,
                }
            },
        },
    }

    @authors("ignat")
    @pytest.mark.ignore_in_opensource_ci
    def test_tskv_input_format(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})

        mapper = b"""
import sys
input = sys.stdin.readline().strip('\\n').split('\\t')
assert input == ['tskv', 'foo=bar']
print('{hello=world}')

"""
        create("file", "//tmp/mapper.py")
        write_file("//tmp/mapper.py", mapper)

        create("table", "//tmp/t_out")
        map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="python3 mapper.py",
            file="//tmp/mapper.py",
            spec={"mapper": {"input_format": yson.loads(b"<line_prefix=tskv>dsv")}},
        )

        assert read_table("//tmp/t_out") == [{"hello": "world"}]

    @authors("ignat")
    @pytest.mark.ignore_in_opensource_ci
    def test_tskv_output_format(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})

        mapper = b"""
import sys
input = sys.stdin.readline().strip('\\n')
assert input == '<"table_index"=0;>#;'
input = sys.stdin.readline().strip('\\n')
assert input == '{"foo"="bar";};'
print("tskv" + "\\t" + "hello=world")
"""
        create("file", "//tmp/mapper.py")
        write_file("//tmp/mapper.py", mapper)

        create("table", "//tmp/t_out")
        map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="python3 mapper.py",
            file="//tmp/mapper.py",
            spec={
                "mapper": {
                    "enable_input_table_index": True,
                    "input_format": yson.loads(b"<format=text>yson"),
                    "output_format": yson.loads(b"<line_prefix=tskv>dsv"),
                }
            },
        )

        assert read_table("//tmp/t_out") == [{"hello": "world"}]

    @authors("ignat")
    @pytest.mark.ignore_in_opensource_ci
    def test_yamr_output_format(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})

        mapper = b"""
import sys
input = sys.stdin.readline().strip('\\n')
assert input == '{"foo"="bar";};'
print("key\\tsubkey\\tvalue")

"""
        create("file", "//tmp/mapper.py")
        write_file("//tmp/mapper.py", mapper)

        create("table", "//tmp/t_out")
        map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="python3 mapper.py",
            file="//tmp/mapper.py",
            spec={
                "mapper": {
                    "input_format": yson.loads(b"<format=text>yson"),
                    "output_format": yson.loads(b"<has_subkey=true>yamr"),
                }
            },
        )

        assert read_table("//tmp/t_out") == [{"key": "key", "subkey": "subkey", "value": "value"}]

    @authors("ignat")
    @pytest.mark.ignore_in_opensource_ci
    def test_yamr_input_format(self):
        create("table", "//tmp/t_in")
        write_table(
            "//tmp/t_in",
            {"value": "value", "subkey": "subkey", "key": "key", "a": "another"},
        )

        mapper = b"""
import sys
input = sys.stdin.readline().strip('\\n').split('\\t')
assert input == ['key', 'subkey', 'value']
print('{hello=world}')

"""
        create("file", "//tmp/mapper.py")
        write_file("//tmp/mapper.py", mapper)

        create("table", "//tmp/t_out")
        map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="python3 mapper.py",
            file="//tmp/mapper.py",
            spec={"mapper": {"input_format": yson.loads(b"<has_subkey=true>yamr")}},
        )

        assert read_table("//tmp/t_out") == [{"hello": "world"}]

    @authors("ignat")
    def test_type_conversion(self):
        create("table", "//tmp/s")
        write_table("//tmp/s", {"foo": "42"})

        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": make_schema(
                    [
                        {"name": "int64", "type": "int64", "sort_order": "ascending"},
                        {"name": "uint64", "type": "uint64"},
                        {"name": "boolean", "type": "boolean"},
                        {"name": "double", "type": "double"},
                        {"name": "any", "type": "any"},
                    ],
                    strict=False,
                )
            },
        )

        row = '{int64=3u; uint64=42; boolean="false"; double=18; any={}; extra=qwe}'

        with pytest.raises(YtError):
            map(
                in_="//tmp/s",
                out="//tmp/t",
                command="echo '{0}'".format(row),
                spec={"max_failed_job_count": 1},
            )

        yson_with_type_conversion = yson.loads(b"<enable_type_conversion=%true>yson")
        map(
            in_="//tmp/s",
            out="//tmp/t",
            command="echo '{0}'".format(row),
            format=yson_with_type_conversion,
            spec={
                "max_failed_job_count": 1,
                "mapper": {"output_format": yson_with_type_conversion},
            },
        )

    @authors("max42")
    def test_invalid_row_indices(self):
        create("table", "//tmp/t_in")
        write_table("<append=%true>//tmp/t_in", [{"a": i} for i in range(10)])
        write_table("<append=%true>//tmp/t_in", [{"a": i} for i in range(10, 20)])

        create("table", "//tmp/t_out")

        # None of this operations should fail.

        map(in_="//tmp/t_in[#18:#2]", out="//tmp/t_out", command="cat")

        map(in_="//tmp/t_in[#8:#2]", out="//tmp/t_out", command="cat")

        map(in_="//tmp/t_in[#18:#12]", out="//tmp/t_out", command="cat")

        map(in_="//tmp/t_in[#8:#8]", out="//tmp/t_out", command="cat")

        map(in_="//tmp/t_in[#10:#10]", out="//tmp/t_out", command="cat")

        map(in_="//tmp/t_in[#12:#12]", out="//tmp/t_out", command="cat")

    @pytest.mark.ignore_in_opensource_ci
    @authors("max42")
    def test_ordered_several_ranges(self):
        # YT-11322
        create(
            "table",
            "//tmp/t_in",
            attributes={"schema": [{"name": "key", "type": "int64", "sort_order": "ascending"}]},
        )
        create("table", "//tmp/t_out")
        write_table("//tmp/t_in", [{"key": 0}, {"key": 1}, {"key": 2}])

        script = b"\n".join(
            [
                b"#!/usr/bin/env python3",
                b"import sys",
                b"import base64",
                b"print('{out=\"' + base64.standard_b64encode(sys.stdin.buffer.read()).decode() + '\"}')",
            ]
        )

        create("file", "//tmp/script.py", attributes={"executable": True})
        write_file("//tmp/script.py", script)

        expected_content = [
            {"key": 0, "table_index": 0, "row_index": 0, "range_index": 0},
            {"key": 1, "table_index": 0, "row_index": 1, "range_index": 1},
            {"key": 2, "table_index": 1, "row_index": 2, "range_index": 0},
        ]

        map(
            in_=[
                "<ranges=[{exact={row_index=0}};{exact={row_index=1}}]>//tmp/t_in",
                "<ranges=[{exact={row_index=2}}]>//tmp/t_in",
            ],
            out="//tmp/t_out",
            ordered=True,
            spec={
                "job_count": 1,
                "mapper": {
                    "file_paths": ["//tmp/script.py"],
                    "format": yson.loads(b"<format=text>yson"),
                },
                "max_failed_job_count": 1,
                "job_io": {
                    "control_attributes": {
                        "enable_range_index": True,
                        "enable_row_index": True,
                        "enable_table_index": True,
                    }
                },
            },
            command="./script.py",
        )

        job_input = read_table("//tmp/t_out")[0]["out"]
        job_input = base64.standard_b64decode(job_input)
        rows = yson.loads(job_input, yson_type="list_fragment")
        actual_content = []
        current_attrs = {}
        for row in rows:
            if type(row) is yson.yson_types.YsonEntity:
                for key, value in row.attributes.items():
                    current_attrs[key] = value
            else:
                new_row = dict(row)
                for key, value in current_attrs.items():
                    new_row[key] = value
                actual_content.append(new_row)

        assert actual_content == expected_content


##################################################################


@pytest.mark.enabled_multidaemon
class TestInputOutputFormatsMulticell(TestInputOutputFormats):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 2


##################################################################


@pytest.mark.enabled_multidaemon
class TestNestingLevelLimitOperations(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    YSON_DEPTH_LIMIT = 100

    DELTA_DRIVER_CONFIG = {
        "cypress_write_yson_nesting_level_limit": YSON_DEPTH_LIMIT,
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "operations_update_period": 10,
            "running_allocations_update_period": 10,
        },
        "cluster_connection": {
            "cypress_write_yson_nesting_level_limit": YSON_DEPTH_LIMIT,
        },
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operations_update_period": 10,
            # COMPAT(shakurov): change the default to false and remove
            # this delta once masters are up to date.
            "enable_prerequisites_for_starting_completion_transactions": False,
        },
        "cluster_connection": {
            "cypress_write_yson_nesting_level_limit": YSON_DEPTH_LIMIT,
        },
    }

    @staticmethod
    def _create_deep_object(depth):
        result = {}
        current = result
        for _ in range(depth):
            current["a"] = {}
            current = current["a"]
        return result

    @authors("levysotsky")
    def test_map_operation(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"a": "b"}])
        create("table", "//tmp/t_out")

        good_obj = self._create_deep_object(self.YSON_DEPTH_LIMIT - 5)
        map(in_="//tmp/t_in", out="//tmp/t_out", command="cat", spec={"annotations": good_obj})

        bad_obj = self._create_deep_object(self.YSON_DEPTH_LIMIT + 1)
        with raises_yt_error("Depth limit exceeded"):
            map(in_="//tmp/t_in", out="//tmp/t_out", command="cat", spec={"annotations": bad_obj})


##################################################################


@pytest.mark.enabled_multidaemon
class TestEnvironment(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    HOME_PATH_FROM_CONFIG = "//tmp/other/path"

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operations_update_period": 10,
            "environment": {
                "HOME": HOME_PATH_FROM_CONFIG
            }
        },
    }

    @authors("nadya73")
    def test_map_operation(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b"})

        op = map(
            track=False,
            in_="//tmp/t1",
            out="//tmp/t2",
            command=r'cat; echo "{v1=\"$TMPDIR\"};{v2=\"$HOME\"}"',
        )

        op.track()

        res = read_table("//tmp/t2")
        assert len(res) == 3
        assert res[0] == {"a": "b"}
        assert "v1" in res[1]
        assert res[1]["v1"].startswith("/")

        assert "v2" in res[2]
        assert res[2]["v2"] == TestEnvironment.HOME_PATH_FROM_CONFIG


##################################################################
