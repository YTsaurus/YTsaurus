from yt_env_setup import (YTEnvSetup)

from yt_commands import (
    authors, ls, get, set, create, write_table, wait, run_test_vanilla,
    with_breakpoint, wait_breakpoint, release_breakpoint, vanilla, map,
    disable_scheduler_jobs_on_node, enable_scheduler_jobs_on_node,
    interrupt_job, update_nodes_dynamic_config, abort_job, run_sleeping_vanilla,
    set_node_banned, extract_statistic_v2, get_allocation_id_from_job_id, alter_table,
    write_file, print_debug,
)

from yt_helpers import JobCountProfiler, profiler_factory, is_uring_supported, is_uring_disabled

import pytest


##################################################################


class TestJobs(YTEnvSetup):
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    @authors("pogorelov")
    def test_uninterruptible_jobs(self):
        aborted_job_profiler = JobCountProfiler(
            "aborted", tags={"tree": "default", "job_type": "vanilla", "abort_reason": "interruption_unsupported"})

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=1)

        wait_breakpoint()

        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 1
        disable_scheduler_jobs_on_node(nodes[0], "test")

        wait(lambda: aborted_job_profiler.get_job_count_delta() == 1)

        release_breakpoint()

        enable_scheduler_jobs_on_node(nodes[0])

        op.track()

    @authors("pogorelov")
    def test_interruption_timeout(self):
        aborted_job_profiler = JobCountProfiler(
            "aborted", tags={"tree": "default", "job_type": "vanilla", "abort_reason": "interruption_timeout"})

        command = """(trap "sleep 1000" SIGINT; BREAKPOINT)"""

        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "tasks_a": {
                        "job_count": 1,
                        "command": with_breakpoint(command),
                        "interruption_signal": "SIGINT",
                        "signal_root_process_only": True,
                        "restart_exit_code": 5,
                    }
                },
                "max_failed_job_count": 1,
            },
        )

        (job_id,) = wait_breakpoint()

        interrupt_job(job_id, interrupt_timeout=2000)

        wait(lambda: aborted_job_profiler.get_job_count_delta() == 1)

        release_breakpoint()

        op.track()

    @authors("arkady-e1ppa")
    def test_job_proxy_exit_profiling(self):
        update_nodes_dynamic_config(value=True, path="/exec_node/job_controller/profile_job_proxy_process_exit")
        nodes = ls("//sys/cluster_nodes")
        profilers = [profiler_factory().at_node(node) for node in nodes]
        exit_ok = [profiler.counter("job_controller/job_proxy_process_exit/zero_exit_code") for profiler in profilers]

        op = run_test_vanilla("echo 1> null", job_count=3)

        op.wait_for_state("completed")

        wait(lambda: any([counter.get_delta() > 0 for counter in exit_ok]))

        op.track()


@authors("khlebnikov")
class TestJobsCri(TestJobs):
    JOB_ENVIRONMENT_TYPE = "cri"


class TestJobsDisabled(YTEnvSetup):
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 100,
                "user_slots": 100,
            },
        }
    }

    def _get_op_job(self, op):
        wait(lambda: op.list_jobs())

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        return job_ids[0]

    @authors("pogorelov")
    def test_job_abort_on_fatal_alert(self):
        node_address = ls("//sys/cluster_nodes")[0]
        assert not get("//sys/cluster_nodes/{}/@alerts".format(node_address))

        aborted_job_profiler = JobCountProfiler(
            "aborted", tags={"tree": "default", "job_type": "vanilla", "abort_reason": "node_with_disabled_jobs"})

        update_nodes_dynamic_config({
            "exec_node": {
                "job_controller": {
                    "job_common": {
                        "waiting_for_job_cleanup_timeout": 500,
                    }
                }

            },
        })

        op1 = run_sleeping_vanilla(
            spec={"job_testing_options": {"delay_in_cleanup": 1000}, "sanity_check_delay": 60 * 1000},
        )

        job_id_1 = self._get_op_job(op1)

        op2 = run_sleeping_vanilla()

        abort_job(job_id_1)

        wait(lambda: aborted_job_profiler.get_job_count_delta() >= 1)

        op1.abort()
        op2.abort()


class TestNodeBanned(YTEnvSetup):
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 1,
                "user_slots": 1,
            },
        }
    }

    @authors("pogorelov")
    def test_job_abort_on_node_banned(self):
        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 1
        node = nodes[0]

        path = "job_controller/job_final_state"
        counter = profiler_factory().at_node(node, fixed_tags={"state": "aborted"}).counter(path)

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"))

        wait_breakpoint()

        set_node_banned(node, True)

        wait(lambda: counter.get_delta(verbose=True) != 0)

        op.abort()


class TestJobProxyCallFailed(YTEnvSetup):
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_controller": {
                    "job_proxy": {
                        "testing_config": {
                            "fail_on_job_proxy_spawned_call": True,
                        },
                    },
                },
            },
        }
    }

    @authors("pogorelov")
    def test_on_job_proxy_spawned_call_fail(self):
        aborted_job_profiler = JobCountProfiler(
            "aborted", tags={"tree": "default", "job_type": "vanilla", "abort_reason": "other"})

        op = run_test_vanilla("sleep 1", job_count=1)

        wait(lambda: aborted_job_profiler.get_job_count_delta() >= 1)

        update_nodes_dynamic_config({
            "exec_node": {
                "job_controller": {
                    "job_proxy": {
                        "testing_config": {
                            "fail_on_job_proxy_spawned_call": False,
                        },
                    },
                },
            },
        })

        op.track()


class TestJobStatistics(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    NODE_IO_ENGINE_TYPE = "thread_pool"

    @authors("ngc224")
    def test_io_statistics(self):
        create("table", "//tmp/t_input")
        write_table("//tmp/t_input", {"foo": "bar"})

        create("table", "//tmp/t_output")

        op = map(
            command="cat",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            track=True,
        )

        statistics = op.get_statistics()
        chunk_reader_statistics = statistics["chunk_reader_statistics"]

        assert extract_statistic_v2(chunk_reader_statistics, "session_count") > 0
        assert extract_statistic_v2(chunk_reader_statistics, "retry_count") > 0
        assert extract_statistic_v2(chunk_reader_statistics, "pass_count") > 0

        assert extract_statistic_v2(chunk_reader_statistics, "data_bytes_transmitted") > 0
        assert extract_statistic_v2(chunk_reader_statistics, "data_bytes_read_from_disk") > 0
        assert extract_statistic_v2(chunk_reader_statistics, "data_io_requests") > 0
        assert extract_statistic_v2(chunk_reader_statistics, "meta_bytes_read_from_disk", summary_type="count") > 0
        assert extract_statistic_v2(chunk_reader_statistics, "meta_bytes_transmitted", summary_type="count") > 0
        assert extract_statistic_v2(chunk_reader_statistics, "meta_io_requests", summary_type="count") > 0

        assert extract_statistic_v2(chunk_reader_statistics, "block_count") > 0
        assert extract_statistic_v2(chunk_reader_statistics, "prefetched_block_count", summary_type="count") > 0

    @authors("artemagafonov")
    def test_vanilla_statistics(self):
        op = run_test_vanilla("true", track=True)

        statistics = op.get_statistics()
        pipes_statistic = statistics["user_job"]["pipes"]

        assert "data" not in statistics or ("data" in statistics and "input" not in statistics["data"])
        assert "input" not in pipes_statistic
        assert "output" in pipes_statistic


@authors("ngc224")
@pytest.mark.skipif(not is_uring_supported() or is_uring_disabled(), reason="io_uring is not available on this host")
class TestJobStatisticsUring(YTEnvSetup):
    NUM_MASTERS = TestJobStatistics.NUM_MASTERS
    NUM_NODES = TestJobStatistics.NUM_NODES
    NUM_SCHEDULERS = TestJobStatistics.NUM_SCHEDULERS

    NODE_IO_ENGINE_TYPE = "uring"

    test_io_statistics = TestJobStatistics.test_io_statistics


class TestAllocationWithTwoJobs(YTEnvSetup):
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    USE_PORTO = True

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 1,
                "user_slots": 1,
            },
        },
        "exec_node": {
            "job_proxy": {
                "check_user_job_memory_limit": False,
            },
        },
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_controller": {
                    "allocation": {
                        "enable_multiple_jobs": True,
                    },
                },
            },
        },
    }

    @authors("pogorelov")
    def test_simple(self):
        create("table", "//tmp/t_in", attributes={"replication_factor": 1})
        create("table", "//tmp/t_out", attributes={"replication_factor": 1})

        write_table("//tmp/t_in", [{"foo": "bar"}] * 2)

        op = map(
            wait_for_jobs=True,
            track=False,
            command=with_breakpoint("BREAKPOINT ; cat"),
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"data_size_per_job": 1},
        )

        job_ids = wait_breakpoint()
        assert len(job_ids) == 1

        job_id1 = job_ids[0]

        allocation_id1 = get_allocation_id_from_job_id(job_id1)

        release_breakpoint(job_id=job_id1)

        job_ids = wait_breakpoint()
        assert len(job_ids) == 1

        job_id2 = job_ids[0]

        assert job_id1 != job_id2

        allocation_id2 = get_allocation_id_from_job_id(job_id2)

        assert allocation_id1 == allocation_id2

        release_breakpoint()

        op.track()

    @authors("pogorelov")
    def test_restore_resources(self):
        job_count = 2
        create("table", "//tmp/t_in", attributes={"replication_factor": 1})
        create("table", "//tmp/t_out", attributes={"replication_factor": 1})
        create("file", "//tmp/mapper.py", attributes={"replication_factor": 1})

        write_table("//tmp/t_in", [{"key": i} for i in range(job_count)])

        memory = 500 * 10 ** 6

        script = with_breakpoint(
f"""
import os
import subprocess
import time

from random import randint

def rndstr(n):
    s = ''
    for i in range(100):
        s += chr(randint(ord('a'), ord('z')))
    return s * (n // 100)

cmd = '''BREAKPOINT'''

def breakpoint():
    subprocess.call(cmd, shell=True)

job_index = int(os.environ['YT_JOB_INDEX'])

if job_index != 0:
    breakpoint()

a = list()
while len(a) * 100000 < {memory}:
    a.append(rndstr(100000))

if job_index == 0:
    breakpoint()
""" # noqa
        ).encode("ascii")

        print_debug("Script is ", script)

        write_file("//tmp/mapper.py", script, attributes={"executable": True})

        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="python3 mapper.py",
            job_count=job_count,
            spec={
                "data_weight_per_job": 1,
                "mapper": {
                    "memory_limit": 200 * 10 ** 6,
                    "user_job_memory_digest_default_value": 0.9,
                    "file_paths": ["//tmp/mapper.py"],
                },
            })

        job_id1, = wait_breakpoint(job_count=1)

        allocation_id = get_allocation_id_from_job_id(job_id1)

        print_debug(f"First job is {job_id1}")

        first_job_memory = 0

        allocation_orchid_path = op.get_job_node_orchid_path(job_id1) + f"/exec_node/job_controller/allocations/{allocation_id}"

        def check_overdraft():
            nonlocal first_job_memory

            orchid = get(allocation_orchid_path)

            print_debug("Resource usage: {}, initial resource demand: {}".format(orchid["base_resource_usage"], orchid["initial_resource_demand"]))

            first_job_memory = orchid["base_resource_usage"]["user_memory"]

            return orchid["base_resource_usage"]["user_memory"] > orchid["initial_resource_demand"]["user_memory"]

        wait(check_overdraft)

        release_breakpoint(job_id=job_id1)

        job_id2, = wait_breakpoint(job_count=1)

        print_debug(f"Second job is {job_id2}")

        assert allocation_id == get_allocation_id_from_job_id(job_id2)

        orchid = get(allocation_orchid_path)

        print_debug("Job2 resource usage: {}".format(orchid["base_resource_usage"]))

        assert orchid["base_resource_usage"]["user_memory"] < first_job_memory
        assert orchid["base_resource_usage"]["user_memory"] == orchid["initial_resource_demand"]["user_memory"]

        release_breakpoint()

        op.track()


class TestNodeAddressResolveFailed(YTEnvSetup):
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_controller": {
                    "job_common": {
                        "testing": {
                            "fail_address_resolve": True,
                        },
                    },
                },
            },
        },
    }

    @authors("pogorelov")
    def test_resolve_failed(self):
        aborted_job_profiler = JobCountProfiler(
            "aborted", tags={"tree": "default", "job_type": "vanilla", "abort_reason": "address_resolve_failed"})

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=1)

        wait(lambda: aborted_job_profiler.get_job_count_delta() > 0)

        update_nodes_dynamic_config(value=False, path="exec_node/job_controller/job_common/testing/fail_address_resolve")

        wait_breakpoint(job_count=1)

        release_breakpoint()

        op.track()


class TestGroupOutOfOrderBlocks(YTEnvSetup):
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "chunk_client_dispatcher": {
            "chunk_reader_pool_size": 1,
        },
    }

    @authors("ngc224")
    @pytest.mark.parametrize("group_out_of_order_blocks", [True, False])
    def test_group_out_of_order_blocks(self, group_out_of_order_blocks):
        create("table", "//tmp/t_input")
        set("//tmp/t_input/@optimize_for", b"scan")
        set("//tmp/t_input/@replication_factor", self.NUM_NODES)

        column_count = 10
        row_count = 100

        schema = [
            dict(
                name=f'field_{column_index}',
                type='string',
            ) for column_index in range(column_count)
        ]

        data = [
            {
                f'field_{column_index}': f'value_{row_index}'
                for column_index in range(column_count)
            } for row_index in range(row_count)
        ]

        alter_table("//tmp/t_input", schema=schema)

        write_table("//tmp/t_input", data)

        assert get("//tmp/t_input/@chunk_count") == 1

        create("table", "//tmp/t_output")
        set("//tmp/t_output/@replication_factor", self.NUM_NODES)

        op = map(
            command="cat",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec=dict(
                job_io=dict(
                    table_reader=dict(
                        group_out_of_order_blocks=group_out_of_order_blocks,
                    ),
                ),
                job_count=1,
            ),
            track=True,
        )

        assert get("//tmp/t_output/@row_count") == row_count

        statistics = op.get_statistics()
        chunk_reader_statistics = statistics["chunk_reader_statistics"]
        expected_data_io_requests = 1 if group_out_of_order_blocks else column_count

        assert extract_statistic_v2(chunk_reader_statistics, "data_io_requests") == expected_data_io_requests
        assert extract_statistic_v2(chunk_reader_statistics, "block_count") == column_count
