from yt_env_setup import YTEnvSetup, wait

from yt_helpers import profiler_factory

from yt_commands import (
    authors, wait_breakpoint, release_breakpoint, with_breakpoint,
    exists, get, set, create, ls, write_table, sort,
    run_sleeping_vanilla, run_test_vanilla)

from flaky import flaky
import pytest

import time
import copy

SPEC_WITH_CPU_MONITOR = {
    "job_cpu_monitor": {
        "check_period": 100,
        "increase_coefficient": 1.45,
        "decrease_coefficient": 0.7,
        "smoothing_factor": 0.2,
        "vote_window_size": 5,
        "vote_decision_threshold": 3,
        "min_cpu_limit": 0.1,
        "enable_cpu_reclaim": True,
    }
}


class TestAggregatedCpuMetrics(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    USE_PORTO = True

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "running_allocations_update_period": 10,
            "fair_share_update_period": 100,
            "profiling_update_period": 100,
            "fair_share_profiling_period": 100,
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "job_metrics_report_period": 100,
        }
    }

    @authors("renadeen")
    def test_sleeping(self):
        profiler = profiler_factory().at_scheduler(fixed_tags={"tree": "default", "pool": "root"})
        smoothed_cpu_counter = profiler.counter("scheduler/pools/metrics/aggregated_smoothed_cpu_usage_x100")
        max_cpu_counter = profiler.counter("scheduler/pools/metrics/aggregated_max_cpu_usage_x100")
        preemptible_cpu_counter = profiler.counter("scheduler/pools/metrics/aggregated_preemptible_cpu_x100")

        run_sleeping_vanilla(spec=SPEC_WITH_CPU_MONITOR)

        wait(lambda: preemptible_cpu_counter.get_delta() > 0)
        wait(lambda: smoothed_cpu_counter.get_delta() > 0)
        wait(lambda: smoothed_cpu_counter.get_delta() < max_cpu_counter.get_delta())

    @authors("renadeen")
    @pytest.mark.skip(reason="Works fine locally but fails at tc. Need to observe it a bit.")
    def test_busy(self):
        spec = copy.deepcopy(SPEC_WITH_CPU_MONITOR)
        spec["job_cpu_monitor"]["min_cpu_limit"] = 1

        profiler = profiler_factory().at_scheduler(fixed_tags={"tree": "default", "pool": "root"})
        smoothed_cpu_counter = profiler.counter("scheduler/pools/metrics/aggregated_smoothed_cpu_usage_x100")
        max_cpu_counter = profiler.counter("scheduler/pools/metrics/aggregated_max_cpu_usage_x100")
        preemptible_cpu_counter = profiler.counter("scheduler/pools/metrics/aggregated_preemptible_cpu_x100")

        op = run_test_vanilla(with_breakpoint("BREAKPOINT; while true; do : ; done"), spec)
        wait_breakpoint()
        release_breakpoint()
        time.sleep(0.2)
        op.abort()

        wait(lambda: smoothed_cpu_counter.get_delta() > 0)
        wait(lambda: smoothed_cpu_counter.get_delta() < max_cpu_counter.get_delta())
        wait(lambda: preemptible_cpu_counter.get_delta() == 0)


class TestDynamicCpuReclaim(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "allowed_node_resources_overcommit_duration": 600 * 1000,
        }
    }

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {"cpu": 1.5, "user_slots": 2},
        }
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_controller": {
                    "cpu_overdraft_timeout": 1000,
                    "resource_adjustment_period": 1000,
                }
            }
        },
    }

    USE_PORTO = True

    @authors("renadeen")
    @pytest.mark.skip(reason="Broken after move to Porto")
    def test_dynamic_cpu_statistics_of_sort_operation(self):
        create("table", "//tmp/t_in", attributes={"replication_factor": 1})
        create("table", "//tmp/t_out", attributes={"replication_factor": 1})
        n = 1000000
        write_table("//tmp/t_in", [{"a": (42 * x) % n} for x in range(n)])

        op = sort(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            sort_by="a",
            spec=SPEC_WITH_CPU_MONITOR,
            track=False,
        )
        wait(lambda: len(list(op.get_running_jobs())) > 0, sleep_backoff=0.1)
        stats_path = self.wait_and_get_stats_path(list(op.get_running_jobs())[0])
        wait(lambda: exists(stats_path + "/preemptible_cpu_x100"), sleep_backoff=0.1)
        # Sort is more io bound than CPU bound.
        wait(
            lambda: get(stats_path + "/preemptible_cpu_x100")["max"] > 50,
            sleep_backoff=0.1,
        )

    @authors("renadeen")
    @flaky(max_runs=5)
    def test_dynamic_cpu_statistics(self):
        run_test_vanilla(
            with_breakpoint("BREAKPOINT; while true; do : ; done"),
            SPEC_WITH_CPU_MONITOR,
        )
        job_id = wait_breakpoint()[0]
        stats_path = self.wait_and_get_stats_path(job_id)

        wait(lambda: get(stats_path + "/smoothed_cpu_usage_x100")["max"] <= 15)
        wait(lambda: get(stats_path + "/preemptible_cpu_x100")["max"] >= 70)

        release_breakpoint()

        wait(lambda: get(stats_path + "/smoothed_cpu_usage_x100")["max"] >= 85)
        wait(lambda: get(stats_path + "/preemptible_cpu_x100")["max"] <= 30)

    @authors("renadeen")
    def test_new_jobs_are_scheduled_on_reclaimed_cpu(self):
        # node has 1.5 CPU, min spare CPU to schedule new jobs is 1

        run_test_vanilla(with_breakpoint("BREAKPOINT", "Op1"), spec=SPEC_WITH_CPU_MONITOR)
        job_id1 = wait_breakpoint("Op1")[0]

        stats_path = self.wait_and_get_stats_path(job_id1)
        wait(lambda: exists(stats_path + "/preemptible_cpu_x100"))
        wait(lambda: get(stats_path + "/preemptible_cpu_x100")["max"] > 50)

        run_test_vanilla(with_breakpoint("BREAKPOINT", "Op2"))
        wait_breakpoint("Op2")

    @authors("renadeen")
    def test_node_aborts_job_on_lack_of_cpu(self):
        op1 = run_test_vanilla(
            with_breakpoint("BREAKPOINT; while true; do : ; done", "Op1"),
            spec=SPEC_WITH_CPU_MONITOR,
        )
        wait_breakpoint("Op1")

        op2 = run_test_vanilla("while true; do : ; done")
        wait(lambda: len(op2.get_running_jobs()) == 1)

        release_breakpoint("Op1")
        wait(lambda: len(op2.get_running_jobs()) == 0)
        wait(lambda: get(op2.get_path() + "/@progress/jobs/aborted/scheduled/resource_overdraft") == 1)
        wait(lambda: len(op1.get_running_jobs()) == 1)

    def wait_and_get_stats_path(self, job_id):
        node = ls("//sys/cluster_nodes")[0]
        result = "//sys/cluster_nodes/{0}/orchid/exec_node/job_controller/active_jobs/{1}/statistics/job_proxy".format(
            node, job_id
        )
        wait(lambda: exists(result + "/smoothed_cpu_usage_x100"), sleep_backoff=0.1)
        return result


class TestSchedulerAbortsJobOnLackOfCpu(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    DELTA_NODE_CONFIG = {"job_resource_manager": {"resource_limits": {"cpu": 2.5, "user_slots": 3}}}

    DELTA_SCHEDULER_CONFIG = {"scheduler": {"watchers_update_period": 100}}

    USE_PORTO = True

    @authors("renadeen")
    def test_scheduler_aborts_job_on_lack_of_cpu(self):
        set("//sys/pool_trees/default/@config/non_preemptible_resource_usage_threshold", {"user_slots": 0})
        set("//sys/pool_trees/default/@config/preemptive_scheduling_backoff", 0)
        set("//sys/pool_trees/default/@config/aggressive_preemption_satisfaction_threshold", 0.1)
        set("//sys/pool_trees/default/@config/preemption_satisfaction_threshold", 0.1)
        time.sleep(0.2)

        op1 = run_test_vanilla(
            with_breakpoint("BREAKPOINT; while true; do : ; done", "Op1"),
            spec=SPEC_WITH_CPU_MONITOR,
        )
        wait_breakpoint("Op1")

        op2 = run_test_vanilla("while true; do : ; done", spec={"weight": 0.001}, job_count=2)
        wait(lambda: len(op2.get_running_jobs()) == 2)

        release_breakpoint("Op1")

        wait(lambda: len(op2.get_running_jobs()) == 1)
        wait(lambda: get(op2.get_path() + "/@progress/jobs/aborted/scheduled/preemption") > 0)
        wait(lambda: len(op1.get_running_jobs()) == 1)


class TestNodeAbortsJobOnLackOfMemory(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {"memory": 512 * 1024 * 1024},
        }
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_controller": {
                    "resource_adjustment_period": 1000,
                },
            },
        },
    }

    @authors("renadeen")
    @pytest.mark.skip(reason="Currently broken")
    def test_node_aborts_job_on_lack_of_memory(self):
        memory_consume_command = (
            'python3 -c "import time\ncount = 100*1000*1000\nx = list(range(count))\ntime.sleep(1000)"'
        )
        op1 = run_test_vanilla(
            with_breakpoint("BREAKPOINT; " + memory_consume_command, "Op1"),
            spec={
                "vanilla": {
                    "memory_limit": 400 * 1024 * 1024,
                    "memory_reserve_factor": 0.5,
                }
            },
        )
        wait_breakpoint("Op1")

        op2 = run_test_vanilla(
            memory_consume_command,
            spec={
                "vanilla": {
                    "memory_limit": 400 * 1024 * 1024,
                    "memory_reserve_factor": 1,
                }
            },
        )
        wait(lambda: len(op2.get_running_jobs()) == 1)

        release_breakpoint("Op1")
        wait(lambda: get(op1.get_path() + "/@progress/jobs/aborted/scheduled/resource_overdraft") == 1)
