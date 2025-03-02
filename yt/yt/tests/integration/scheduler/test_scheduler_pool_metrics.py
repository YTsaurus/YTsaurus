from yt_env_setup import (
    YTEnvSetup,
    Restarter,
    CONTROLLER_AGENTS_SERVICE,
)

from yt_commands import (
    authors, get_job, wait, wait_no_assert, wait_breakpoint, release_breakpoint, with_breakpoint,
    create, ls, get, set, exists,
    create_pool, create_pool_tree, write_table, map, run_test_vanilla, run_sleeping_vanilla, vanilla, abort_job,
    get_operation_cypress_path, extract_statistic_v2, update_pool_tree_config, update_scheduler_config,
    update_controller_agent_config
)

from yt_helpers import profiler_factory

from yt.test_helpers import are_almost_equal

from yt_scheduler_helpers import (
    scheduler_orchid_operation_path, scheduler_orchid_pool_path
)

import pytest

import os
import time

##################################################################


def get_cypress_metrics(operation_id, key):
    statistics = get(get_operation_cypress_path(operation_id) + "/@progress/job_statistics_v2")
    return sum(
        filter(
            lambda x: x is not None,
            [
                extract_statistic_v2(statistics, key, job_state=job_state)
                for job_state in ("completed", "failed", "aborted")
            ],
        )
    )

##################################################################


class TestPoolMetrics(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "running_allocations_update_period": 10,
            "fair_share_update_period": 100,
            "profiling_update_period": 100,
            "fair_share_profiling_period": 100,
        },
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 2000,
            "job_metrics_report_period": 100,
            "custom_job_metrics": [
                {
                    "statistics_path": "/user_job/block_io/bytes_written",
                    "profiling_name": "my_metric",
                },
                {
                    "statistics_path": "/user_job/block_io/bytes_written",
                    "profiling_name": "my_metric_failed",
                    "job_state_filter": "failed",
                },
                {
                    "statistics_path": "/user_job/block_io/bytes_written",
                    "profiling_name": "my_metric_completed",
                    "job_state_filter": "completed",
                },
                {
                    "statistics_path": "/custom/value",
                    "profiling_name": "my_custom_metric_sum",
                    "summary_value_type": "sum",
                },
                {
                    "statistics_path": "/custom/value",
                    "profiling_name": "my_custom_metric_max",
                    "summary_value_type": "max",
                },
                {
                    "statistics_path": "/custom/value",
                    "profiling_name": "my_custom_metric_last",
                    "summary_value_type": "last",
                },
            ],
        }
    }

    USE_PORTO = True

    @authors("ignat")
    def test_map(self):
        pytest.skip("this test is broken")

        create_pool("parent")
        create_pool("child1", parent_name="parent")
        create_pool("child2", parent_name="parent")

        create("table", "//t_input")
        create("table", "//t_output")

        # write table of 2 chunks because we want 2 jobs
        write_table("//t_input", [{"key": i} for i in range(0, 100)])
        write_table("<append=%true>//t_input", [{"key": i} for i in range(100, 500)])

        # create directory backed by block device and accessible to job
        os.makedirs(self.default_disk_path)
        os.chmod(self.default_disk_path, 0o777)

        # our command does the following
        # - writes (and syncs) something to disk
        # - works for some time (to ensure that it sends several heartbeats
        # - writes something to stderr because we want to find our jobs in //sys/operations later
        map_cmd = (
            """for i in $(seq 10); do"""
            """    python3 -c "import os; os.write(5, b'{{value=$i}};')";"""
            """    dd if=/dev/urandom of={}/foo$i bs=1M count=1 oflag=direct;"""
            """    sync; sleep 0.5;"""
            """done;"""
            """cat; sleep 10; echo done > /dev/stderr"""
        )
        map_cmd = map_cmd.format(self.default_disk_path)

        metric_name = "user_job_bytes_written"
        statistics_name = "user_job.block_io.bytes_written"

        profiler = profiler_factory().at_scheduler(fixed_tags={"tree": "default"})

        pools = ["parent", "child1", "child2"]
        usual_metric_counter = {
            pool: profiler.counter("scheduler/pools/metrics/" + metric_name, tags={"pool": pool})
            for pool in pools
        }
        custom_metric_counter = {
            pool: profiler.counter("scheduler/pools/metrics/my_metric", tags={"pool": pool})
            for pool in pools
        }
        custom_metric_completed_counter = {
            pool: profiler.counter("scheduler/pools/metrics/my_metric_completed", tags={"pool": pool})
            for pool in pools
        }
        custom_metric_failed_counter = {
            pool: profiler.counter("scheduler/pools/metrics/my_metric_failed", tags={"pool": pool})
            for pool in pools
        }
        custom_metric_max_sensor = profiler.gauge("scheduler/pools/metrics/my_custom_metric_max", tags={"pool": "child2"})
        custom_metric_sum_sensor = profiler.gauge("scheduler/pools/metrics/my_custom_metric_sum", tags={"pool": "child2"})

        op11 = map(
            in_="//t_input",
            out="//t_output",
            command=map_cmd,
            spec={"job_count": 2, "pool": "child1"},
        )
        op12 = map(
            in_="//t_input",
            out="//t_output",
            command=map_cmd,
            spec={"job_count": 2, "pool": "child1"},
        )

        op2 = map(
            in_="//t_input",
            out="//t_output",
            command=map_cmd,
            spec={"job_count": 2, "pool": "child2"},
        )

        for counter in (
            usual_metric_counter,
            custom_metric_counter,
            custom_metric_completed_counter,
        ):
            wait(lambda: counter["parent"].get_delta() > 0)

            op11_writes = get_cypress_metrics(op11.id, statistics_name)
            op12_writes = get_cypress_metrics(op12.id, statistics_name)
            op2_writes = get_cypress_metrics(op2.id, statistics_name)

            wait(lambda: counter["child1"].get_delta() == op11_writes + op12_writes > 0)
            wait(lambda: counter["child2"].get_delta() == op2_writes > 0)
            wait(
                lambda: counter["parent"].get_delta() == op11_writes + op12_writes + op2_writes > 0
            )

        assert custom_metric_failed_counter["child2"].get_delta() == 0

        wait(lambda: custom_metric_max_sensor.get() == 20)
        wait(lambda: custom_metric_sum_sensor.get() == 110)

        jobs_11 = op11.list_jobs()
        assert len(jobs_11) >= 2

    @authors("akulyat")
    def test_scheduled_resources(self):
        update_pool_tree_config(
            "default",
            {
                "preemption_satisfaction_threshold": 0.99,
                "fair_share_starvation_timeout": 1000,
                "non_preemptible_resource_usage_threshold": {"user_slots": 0},
                "preemptive_scheduling_backoff": 0,
            })

        create_pool("parent")
        create_pool("child1", parent_name="parent")
        create_pool("child2", parent_name="parent")

        profiler = profiler_factory().at_scheduler(fixed_tags={"tree": "default"})
        scheduled_cpu_non_preemptive_child1_counter = profiler.counter("scheduler/pools/scheduled_job_resources/cpu", {"pool": "child1", "scheduling_stage": "regular_medium_priority"})
        scheduled_cpu_preemptive_child2_counter = profiler.counter("scheduler/pools/scheduled_job_resources/cpu", {"pool": "child2", "scheduling_stage": "preemptive_normal"})
        scheduled_cpu_non_preemptive_parent_counter = profiler.counter("scheduler/pools/scheduled_job_resources/cpu", {"pool": "parent", "scheduling_stage": "regular_medium_priority"})
        scheduled_cpu_preemptive_parent_counter = profiler.counter("scheduler/pools/scheduled_job_resources/cpu", {"pool": "parent", "scheduling_stage": "preemptive_normal"})
        preempted_cpu_preemption_child1_counter = profiler.counter("scheduler/pools/preempted_job_resources/cpu", {"pool": "child1", "preemption_reason": "preemption"})
        preempted_cpu_preemption_parent_counter = profiler.counter("scheduler/pools/preempted_job_resources/cpu", {"pool": "parent", "preemption_reason": "preemption"})
        preempted_cpu_overcommit_child1_counter = profiler.counter("scheduler/pools/preempted_job_resources/cpu", {"pool": "child1", "preemption_reason": "resource_overcommit"})
        preempted_cpu_time_preemption_child1_counter = profiler.counter("scheduler/pools/preempted_job_resource_times/cpu", {"pool": "child1", "preemption_reason": "preemption"})

        op1 = run_sleeping_vanilla(
            job_count=3,
            task_patch={"cpu_limit": 1.0},
            spec={
                "pool": "child1",
            },
        )
        wait(lambda: exists(scheduler_orchid_operation_path(op1.id)))
        wait(lambda: get(scheduler_orchid_operation_path(op1.id) + "/resource_usage/cpu") == 3.0)

        # To ensure some exec time for op1's jobs.
        time.sleep(3.0)

        op2 = run_sleeping_vanilla(
            job_count=1,
            task_patch={"cpu_limit": 1.0},
            spec={
                "pool": "child2",
            },
        )
        wait(lambda: exists(scheduler_orchid_operation_path(op2.id)))
        wait(lambda: get(scheduler_orchid_operation_path(op2.id) + "/resource_usage/cpu") == 1.0)

        wait(lambda: scheduled_cpu_non_preemptive_child1_counter.get_delta() == 3)
        wait(lambda: preempted_cpu_preemption_child1_counter.get_delta() == 1)
        wait(lambda: scheduled_cpu_preemptive_child2_counter.get_delta() == 1)
        wait(lambda: scheduled_cpu_non_preemptive_parent_counter.get_delta() == 3)
        wait(lambda: preempted_cpu_preemption_parent_counter.get_delta() == 1)
        wait(lambda: scheduled_cpu_preemptive_parent_counter.get_delta() == 1)

        # 1 preempted job * 3 CPU-seconds.
        wait(lambda: preempted_cpu_time_preemption_child1_counter.get_delta() >= 3)

        update_scheduler_config("allowed_node_resources_overcommit_duration", 1000)

        wait(lambda: len(op1.list_jobs()) == 2)
        job = op1.list_jobs()[0]
        node = get_job(op1.id, job)['address']
        set("//sys/cluster_nodes/{}/@resource_limits_overrides/cpu".format(node), 0.2)
        wait(lambda: get(scheduler_orchid_operation_path(op1.id) + "/resource_usage/cpu") == 1.0)

        wait(lambda: preempted_cpu_overcommit_child1_counter.get_delta() == 1)

    @authors("ignat")
    def test_time_metrics(self):
        create_pool("parent")
        create_pool("child", parent_name="parent")

        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")

        write_table("<append=%true>//tmp/t_input", [{"key": i} for i in range(2)])

        profiler = profiler_factory().at_scheduler(fixed_tags={"tree": "default"})
        total_time_completed_parent_counter = profiler.counter("scheduler/pools/metrics/total_time_completed", {"pool": "parent"})
        total_time_completed_child_counter = profiler.counter("scheduler/pools/metrics/total_time_completed", {"pool": "child"})
        total_time_aborted_parent_counter = profiler.counter("scheduler/pools/metrics/total_time_aborted", {"pool": "parent"})
        total_time_aborted_child_counter = profiler.counter("scheduler/pools/metrics/total_time_aborted", {"pool": "child"})

        op = map(
            command=with_breakpoint("cat; BREAKPOINT"),
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={"data_size_per_job": 1, "pool": "child"},
            track=False,
        )

        jobs = wait_breakpoint(job_count=2)
        assert len(jobs) == 2

        release_breakpoint(job_id=jobs[0])

        # Wait until short job is completed.
        wait(lambda: len(op.get_running_jobs()) == 1)

        running_jobs = list(op.get_running_jobs())
        assert len(running_jobs) == 1
        abort_job(running_jobs[0])

        def check_metrics(parent_counter, child_counter):
            parent_delta = parent_counter.get_delta()
            child_delta = child_counter.get_delta()
            return parent_delta != 0 and child_delta != 0 and parent_delta == child_delta

        # NB: profiling is built asynchronously in separate thread and can contain non-consistent information.
        wait(lambda: check_metrics(total_time_completed_parent_counter, total_time_completed_child_counter))
        wait(lambda: check_metrics(total_time_aborted_parent_counter, total_time_aborted_child_counter))

    @authors("eshcherbin")
    def test_total_time_operation_by_state(self):
        create_pool("parent")
        for i in range(3):
            create_pool("child" + str(i + 1), parent_name="parent")

        create("table", "//tmp/t_input")
        for i in range(3):
            create("table", "//tmp/t_output_" + str(i + 1))

        write_table("//tmp/t_input", {"foo": "bar"})

        profiler = profiler_factory().at_scheduler(fixed_tags={"tree": "default"})

        pools = ["parent", "child1", "child2", "child3"]
        total_time_counter = {
            pool: profiler.counter("scheduler/pools/metrics/total_time", tags={"pool": pool})
            for pool in pools
        }
        total_time_operation_completed_counter = {
            pool: profiler.counter("scheduler/pools/metrics/total_time_operation_completed", tags={"pool": pool})
            for pool in pools
        }
        total_time_operation_failed_counter = {
            pool: profiler.counter("scheduler/pools/metrics/total_time_operation_failed", tags={"pool": pool})
            for pool in pools
        }
        total_time_operation_aborted_counter = {
            pool: profiler.counter("scheduler/pools/metrics/total_time_operation_aborted", tags={"pool": pool})
            for pool in pools
        }

        main_resource_consumption_operation_completed_counter = {
            pool: profiler.counter("scheduler/pools/metrics/main_resource_consumption_operation_completed", tags={"pool": pool})
            for pool in pools
        }
        main_resource_consumption_operation_failed_counter = {
            pool: profiler.counter("scheduler/pools/metrics/main_resource_consumption_operation_failed", tags={"pool": pool})
            for pool in pools
        }
        main_resource_consumption_operation_aborted_counter = {
            pool: profiler.counter("scheduler/pools/metrics/main_resource_consumption_operation_aborted", tags={"pool": pool})
            for pool in pools
        }

        op1 = map(
            command=("sleep 5; cat"),
            in_="//tmp/t_input",
            out="//tmp/t_output_1",
            spec={"pool": "child1", "mapper": {"cpu_limit": 0.5}},
            track=False,
        )
        op2 = map(
            command=("sleep 5; cat; exit 1"),
            in_="//tmp/t_input",
            out="//tmp/t_output_2",
            spec={"pool": "child2", "max_failed_job_count": 1},
            track=False,
        )
        op3 = map(
            command=("sleep 100; cat"),
            in_="//tmp/t_input",
            out="//tmp/t_output_3",
            spec={"pool": "child3"},
            track=False,
        )

        # Wait until at least some metrics are reported for op3
        wait(lambda: total_time_counter["child3"].get_delta() > 0)
        op3.abort(wait_until_completed=True)

        op1.track()
        op2.track(raise_on_failed=False)

        def check_metrics(parent_counter, child_counter):
            parent_delta = parent_counter.get_delta()
            child_delta = child_counter.get_delta()
            assert parent_delta != 0
            assert child_delta != 0
            assert parent_delta == child_delta

        # TODO(eshcherbin): This is used for flap diagnostics. Remove when the test is fixed.
        time.sleep(2)
        for pool in pools:
            total_time_counter[pool].get()

        # NB: profiling is built asynchronously in separate thread and can contain non-consistent information.
        wait_no_assert(lambda: check_metrics(total_time_operation_completed_counter["parent"], total_time_operation_completed_counter["child1"]))
        wait_no_assert(lambda: check_metrics(total_time_operation_failed_counter["parent"], total_time_operation_failed_counter["child2"]))
        wait_no_assert(lambda: check_metrics(total_time_operation_aborted_counter["parent"], total_time_operation_aborted_counter["child3"]))
        wait(
            lambda: total_time_counter["parent"].get()
            == total_time_operation_completed_counter["parent"].get()
            + total_time_operation_failed_counter["parent"].get()
            + total_time_operation_aborted_counter["parent"].get()
            > 0
        )

        # We have high relative error due to rounding issues.
        wait(
            lambda: are_almost_equal(
                total_time_counter["parent"].get(),
                2.0 * main_resource_consumption_operation_completed_counter["parent"].get()
                + main_resource_consumption_operation_failed_counter["parent"].get()
                + main_resource_consumption_operation_aborted_counter["parent"].get(),
                relative_error=0.05)
        )

    @authors("eshcherbin")
    def test_total_time_operation_completed_several_jobs(self):
        create_pool("unique_pool")

        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")

        write_table("<append=%true>//tmp/t_input", [{"key": i} for i in range(2)])

        profiler = profiler_factory().at_scheduler(fixed_tags={"tree": "default", "pool": "unique_pool"})
        total_time_completed_counter = profiler.counter("scheduler/pools/metrics/total_time_completed")
        total_time_aborted_counter = profiler.counter("scheduler/pools/metrics/total_time_aborted")
        total_time_operation_completed_counter = profiler.counter("scheduler/pools/metrics/total_time_operation_completed")
        total_time_operation_failed_counter = profiler.counter("scheduler/pools/metrics/total_time_operation_failed")
        total_time_operation_aborted_counter = profiler.counter("scheduler/pools/metrics/total_time_operation_aborted")

        op = map(
            command=with_breakpoint("cat; BREAKPOINT; sleep 3"),
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "data_size_per_job": 1,
                "pool": "unique_pool",
                "max_speculative_job_count_per_task": 0,
            },
            track=False,
        )

        jobs = wait_breakpoint(job_count=2)
        assert len(jobs) == 2

        release_breakpoint(job_id=jobs[0])

        # Wait until short job is completed.
        wait(lambda: len(op.get_running_jobs()) == 1)

        running_jobs = list(op.get_running_jobs())
        assert len(running_jobs) == 1

        job_to_abort = running_jobs[0]
        release_breakpoint(job_id=job_to_abort)
        abort_job(job_to_abort)

        # Wait for restarted job to reach breakpoint.
        jobs = wait_breakpoint()
        assert len(jobs) == 1 and jobs[0] != job_to_abort
        release_breakpoint(job_id=jobs[0])

        op.track()

        wait(
            lambda: total_time_operation_completed_counter.get_delta()
            == total_time_completed_counter.get_delta()
            + total_time_aborted_counter.get_delta()
            > 0
        )
        assert total_time_operation_failed_counter.get_delta() == 0
        assert total_time_operation_aborted_counter.get_delta() == 0

    @authors("eshcherbin")
    def test_total_time_operation_failed_several_jobs(self):
        create_pool("unique_pool")

        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")

        write_table(
            "<append=%true>//tmp/t_input",
            [{"sleep": 2, "exit": 0}, {"sleep": 5, "exit": 1}],
            output_format="json",
        )

        map_cmd = """python3 -c 'import sys; import time; import json; row=json.loads(input()); time.sleep(row["sleep"]); sys.exit(row["exit"])'"""

        profiler = profiler_factory().at_scheduler(fixed_tags={"tree": "default", "pool": "unique_pool"})
        total_time_counter = profiler.counter("scheduler/pools/metrics/total_time")
        total_time_operation_completed_counter = profiler.counter("scheduler/pools/metrics/total_time_operation_completed")
        total_time_operation_failed_counter = profiler.counter("scheduler/pools/metrics/total_time_operation_failed")
        total_time_operation_aborted_counter = profiler.counter("scheduler/pools/metrics/total_time_operation_aborted")

        op = map(
            command=map_cmd,
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "data_size_per_job": 1,
                "max_failed_job_count": 1,
                "pool": "unique_pool",
                "mapper": {"input_format": "json", "check_input_fully_consumed": True},
            },
            track=False,
        )
        op.track(raise_on_failed=False)

        wait(lambda: total_time_counter.get_delta() == total_time_operation_failed_counter.get_delta() > 0)
        assert total_time_operation_completed_counter.get_delta() == 0
        assert total_time_operation_aborted_counter.get_delta() == 0

    @authors("eshcherbin")
    def test_total_time_operation_completed_per_tree(self):
        create("table", "//tmp/t_in")
        for i in range(9):
            write_table("<append=%true>//tmp/t_in", [{"x": i}])
        create("table", "//tmp/t_out")

        # Set up second tree
        node = ls("//sys/cluster_nodes")[0]
        set("//sys/cluster_nodes/" + node + "/@user_tags/end", "other")
        set("//sys/pool_trees/default/@config/nodes_filter", "!other")
        create_pool_tree("other", config={"nodes_filter": "other"})

        time.sleep(1.0)

        profiler = profiler_factory().at_scheduler(fixed_tags={"pool": "<Root>"})
        total_time_default_counter = profiler.counter("scheduler/pools/metrics/total_time", tags={"tree": "default"})
        total_time_other_counter = profiler.counter("scheduler/pools/metrics/total_time", tags={"tree": "other"})
        total_time_operation_completed_default_counter = profiler.counter("scheduler/pools/metrics/total_time_operation_completed", tags={"tree": "default"})
        total_time_operation_completed_other_counter = profiler.counter("scheduler/pools/metrics/total_time_operation_completed", tags={"tree": "other"})

        map(
            command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"data_size_per_job": 1, "pool_trees": ["default", "other"]},
        )

        wait(lambda: total_time_default_counter.get_delta() == total_time_operation_completed_default_counter.get_delta() > 0)
        wait(lambda: total_time_other_counter.get_delta() == total_time_operation_completed_other_counter.get_delta() > 0)

    @authors("eshcherbin")
    def test_revive(self):
        create_pool("unique_pool")

        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("<append=%true>//tmp/t_input", {"foo": "bar"})

        before_breakpoint = """for i in $(seq 10) ; do python3 -c "import os; os.write(5, b'{value=$i};')" ; sleep 0.5 ; done ; sleep 5 ; """
        after_breakpoint = """for i in $(seq 9 -1 5) ; do python3 -c "import os; os.write(5, b'{value=$i};')" ; sleep 0.5 ; done ; cat ; sleep 5 ; echo done > /dev/stderr ; """

        profiler = profiler_factory().at_scheduler(fixed_tags={"tree": "default", "pool": "unique_pool"})
        total_time_counter = profiler.counter("scheduler/pools/metrics/total_time")
        custom_counter_sum = profiler.counter("scheduler/pools/metrics/my_custom_metric_sum")
        custom_counter_last = profiler.counter("scheduler/pools/metrics/my_custom_metric_last")

        op = map(
            command=with_breakpoint(before_breakpoint + "BREAKPOINT ; " + after_breakpoint),
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={"pool": "unique_pool"},
            track=False,
        )

        jobs = wait_breakpoint()
        assert len(jobs) == 1

        total_time_before_restart = total_time_counter.get_delta()
        wait(lambda: custom_counter_sum.get_delta() == 55)
        assert custom_counter_last.get_delta() == 0

        # We need to have the job in the snapshot, so that it is not restarted after operation revival.
        op.wait_for_fresh_snapshot()

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        wait(lambda: get("//sys/scheduler/orchid/scheduler/operations/{}/state".format(op.id)) == "running")
        wait(lambda: op.get_state() == "running")

        release_breakpoint()

        op.track()

        wait(lambda: total_time_counter.get_delta() > total_time_before_restart)
        wait(lambda: custom_counter_sum.get_delta() == 90)
        wait(lambda: custom_counter_last.get_delta() == 5)

    @authors("eshcherbin")
    def test_distributed_resources_profiling(self):
        create_pool("strong", attributes={"strong_guarantee_resources": {"cpu": 1}})
        create_pool("integral", attributes={
            "integral_guarantees": {
                "guarantee_type": "burst",
                "resource_flow": {"cpu": 1},
                "burst_guarantee_resources": {"cpu": 1},
            },
        })

        profiler = profiler_factory().at_scheduler(fixed_tags={"tree": "default"})
        wait(lambda: profiler.gauge("scheduler/distributed_resources/cpu").get() == 2)
        wait(lambda: profiler.gauge("scheduler/distributed_strong_guarantee_resources/cpu").get() == 1)
        wait(lambda: profiler.gauge("scheduler/distributed_resource_flow/cpu").get() == 1)
        wait(lambda: profiler.gauge("scheduler/distributed_resource_flow/cpu").get() == 1)
        wait(lambda: profiler.gauge("scheduler/distributed_burst_guarantee_resources/cpu").get() == 1)
        wait(lambda: profiler.gauge("scheduler/undistributed_resources/cpu").get() == 1)
        wait(lambda: profiler.gauge("scheduler/undistributed_resource_flow/cpu").get() == 0)
        wait(lambda: profiler.gauge("scheduler/undistributed_burst_guarantee_resources/cpu").get() == 0)

    @authors("pogorelov")
    def test_total_time_metric(self):
        create_pool("research")
        op = run_test_vanilla(with_breakpoint("sleep 5; BREAKPOINT"), spec={"pool": "research"}, job_count=1)

        research_profiler = profiler_factory().at_scheduler(
            fixed_tags={"tree": "default", "pool": "research"})
        total_time_counter = research_profiler.counter("scheduler/pools/metrics/total_time")
        exec_time_counter = research_profiler.counter("scheduler/pools/metrics/exec_time")

        wait_breakpoint()

        job_ids = list(op.get_running_jobs())
        assert len(job_ids) == 1
        job_id = job_ids[0]

        abort_job(job_id)

        def get_total_time_delta():
            total_time = total_time_counter.get()
            result = total_time - get_total_time_delta.previous_total_time
            get_total_time_delta.previous_total_time = total_time
            return result

        get_total_time_delta.previous_total_time = 0

        wait(lambda: total_time_counter.get())

        wait(lambda: get_total_time_delta() == 0, sleep_backoff=1)

        # Total and exec times should not differ much.
        assert total_time_counter.get() - exec_time_counter.get() < 3000

    @authors("eshcherbin")
    def test_operation_count_by_preemption_priority(self):
        # For operations with strange tag filters to have non-zero fair share and starve.
        update_scheduler_config("operation_stuck_check", {"period": 1000000000})
        update_controller_agent_config("safe_scheduler_online_time", 1000000000)
        update_pool_tree_config(
            "default",
            {
                "node_reconnection_timeout": 1000000000,
                "preemption_satisfaction_threshold": 0.99,
                "fair_share_starvation_timeout": 1000,
                "fair_share_aggressive_starvation_timeout": 1000,
            })

        profiler = profiler_factory().at_scheduler(fixed_tags={"tree": "default"})
        operation_count_by_preemption_priority_gauge = profiler.gauge("scheduler/operation_count_by_preemption_priority")

        create_pool("pool", wait_for_orchid=False)
        create_pool("subpool", attributes={"enable_aggressive_starvation": True})
        # NB(eshcherbin): We use two tasks, so that this operation is still schedulable.
        op1 = vanilla(
            spec={
                "pool": "pool",
                "tasks": {
                    "possible": {"job_count": 1, "command": "sleep 1000", "cpu_limit": 1.0},
                    "impossible": {"job_count": 1, "command": "sleep 1000", "cpu_limit": 10.0},
                },
            },
            track=False
        )
        op2 = run_sleeping_vanilla(spec={"pool": "pool", "scheduling_tag_filter": "nonexisting_tag"})
        op3 = run_sleeping_vanilla(spec={"pool": "subpool", "scheduling_tag_filter": "nonexisting_tag"})
        wait(lambda: exists(scheduler_orchid_operation_path(op2.id)))
        wait(lambda: get(scheduler_orchid_operation_path(op1.id) + "/resource_usage/cpu") == 1.0)
        wait(lambda: get(scheduler_orchid_operation_path(op1.id) + "/starvation_status") == "non_starving")
        wait(lambda: get(scheduler_orchid_operation_path(op2.id) + "/starvation_status") == "starving")
        wait(lambda: get(scheduler_orchid_pool_path("pool") + "/starvation_status") == "starving")
        wait(lambda: get(scheduler_orchid_operation_path(op3.id) + "/starvation_status") == "aggressively_starving")
        wait(lambda: get(scheduler_orchid_pool_path("subpool") + "/starvation_status") == "aggressively_starving")
        wait(lambda: get(scheduler_orchid_operation_path(op1.id) + "/lowest_starving_ancestor", default=None) is not None)

        expected_counts_per_scope = {
            "operation_only": {
                "none": 1.0,
                "normal": 1.0,
                "aggressive": 1.0,
                "ssd_normal": 0.0,
                "ssd_aggressive": 0.0,
            },
            "operation_and_ancestors": {
                "none": 0.0,
                "normal": 2.0,
                "aggressive": 1.0,
                "ssd_normal": 0.0,
                "ssd_aggressive": 0.0,
            },
        }
        for scope, expected_counts in expected_counts_per_scope.items():
            for priority, count in expected_counts.items():
                for ssd_enabled in ["true", "false"]:
                    wait(lambda: operation_count_by_preemption_priority_gauge.get(tags={
                        "scope": scope,
                        "ssd_priority_preemption_enabled": ssd_enabled,
                        "priority": priority,
                    }) == count)

    @authors("eshcherbin")
    def test_specified_resource_limits(self):
        create_pool("pool")

        profiler = profiler_factory().at_scheduler(fixed_tags={
            "tree": "default",
            "pool": "pool",
        })
        specified_resource_limits_cpu_gauge = profiler.gauge("scheduler/pools/specified_resource_limits/cpu")
        specified_resource_limits_user_slots_gauge = profiler.gauge("scheduler/pools/specified_resource_limits/user_slots")

        time.sleep(3.0)
        wait(lambda: specified_resource_limits_cpu_gauge.get() is None)
        wait(lambda: specified_resource_limits_user_slots_gauge.get() is None)

        set("//sys/pool_trees/default/pool/@resource_limits", {"cpu": 1.0})
        wait(lambda: specified_resource_limits_cpu_gauge.get() == 1.0)
        wait(lambda: specified_resource_limits_user_slots_gauge.get() is None)

        set("//sys/pool_trees/default/pool/@resource_limits", {"user_slots": 2})
        wait(lambda: specified_resource_limits_cpu_gauge.get() is None)
        wait(lambda: specified_resource_limits_user_slots_gauge.get() == 2.0)

        set("//sys/pool_trees/default/pool/@resource_limits", {})
        wait(lambda: specified_resource_limits_cpu_gauge.get() is None)
        wait(lambda: specified_resource_limits_user_slots_gauge.get() is None)


##################################################################


@pytest.mark.enabled_multidaemon
class TestImproperlyPreemptedResources(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "running_allocations_update_period": 10,
            "fair_share_update_period": 100,
            "profiling_update_period": 100,
            "fair_share_profiling_period": 100,
            "operation_stuck_check": {
                "period": 1000000000,
            },
        },
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "safe_scheduler_online_time": 1000000000,
        }
    }

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 6,
                "user_slots": 6,
            },
        },
    }

    @authors("eshcherbin")
    def test_improperly_preempted_resources(self):
        update_pool_tree_config(
            "default",
            {
                "preemption_satisfaction_threshold": 0.99,
                "fair_share_starvation_timeout": 1000,
                "fair_share_aggressive_starvation_timeout": 1100,
                "non_preemptible_resource_usage_threshold": {"user_slots": 0},
                "preemptive_scheduling_backoff": 0,
            })

        create_pool("first", attributes={"strong_guarantee_resources": {"cpu": 3}, "allow_aggressive_preemption": False})
        create_pool("second", attributes={"strong_guarantee_resources": {"cpu": 3}, "enable_aggressive_starvation": True})

        profiler = profiler_factory().at_scheduler(fixed_tags={"tree": "default"})
        preempted_cpu_first_counter = \
            profiler.counter("scheduler/pools/preempted_job_resources/cpu", {"pool": "first", "preemption_reason": "aggressive_preemption"})
        improperly_preempted_cpu_first_counter = \
            profiler.counter("scheduler/pools/improperly_preempted_job_resources/cpu", {"pool": "first", "preemption_reason": "aggressive_preemption"})
        improperly_preempted_cpu_second_counter = \
            profiler.counter("scheduler/pools/improperly_preempted_job_resources/cpu", {"pool": "second"})

        normal_op = run_sleeping_vanilla(job_count=3, spec={"pool": "first"})
        wait(lambda: get(scheduler_orchid_operation_path(normal_op.id) + "/resource_usage/cpu", default=None) == 3.0)

        starving_op = run_sleeping_vanilla(spec={"pool": "second", "scheduling_tag_filter": "nonexistent_tag"})
        wait(lambda: get(scheduler_orchid_operation_path(starving_op.id) + "/starvation_status", default=None) == "aggressively_starving")
        wait(lambda: get(scheduler_orchid_pool_path("second") + "/starvation_status", default=None) == "aggressively_starving")

        greedy_op = run_sleeping_vanilla(job_count=2, spec={"pool": "second"}, task_patch={"cpu_limit": 2.0})
        wait(lambda: are_almost_equal(get(scheduler_orchid_operation_path(greedy_op.id) + "/detailed_fair_share/total/cpu", default=None), 1.0 / 3.0))
        wait(lambda: get(scheduler_orchid_operation_path(greedy_op.id) + "/resource_usage/cpu") == 2.0)
        wait(lambda: get(scheduler_orchid_pool_path("second") + "/starvation_status") == "aggressively_starving")

        set("//sys/pool_trees/default/first/@allow_aggressive_preemption", True)
        wait(lambda: get(scheduler_orchid_operation_path(greedy_op.id) + "/resource_usage/cpu") == 4.0)
        wait(lambda: get(scheduler_orchid_pool_path("second") + "/starvation_status") == "non_starving")

        wait(lambda: improperly_preempted_cpu_first_counter.get_delta() == preempted_cpu_first_counter.get_delta() > 0)
        wait(lambda: improperly_preempted_cpu_second_counter.get_delta() == 0)
