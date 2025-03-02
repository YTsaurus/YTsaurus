from yt_env_setup import (
    YTEnvSetup,
    Restarter,
    SCHEDULERS_SERVICE,
    NODES_SERVICE,
)

from yt_commands import (
    authors, print_debug, update_nodes_dynamic_config, wait, wait_no_assert, wait_breakpoint, release_breakpoint, with_breakpoint, events_on_fs,
    create, ls, get, set, move, remove, exists, create_user, create_pool, create_pool_tree,
    make_ace, check_permission,
    read_table, write_table,
    map, map_reduce, merge,
    vanilla, sort, run_test_vanilla,
    run_sleeping_vanilla, abort_op,
    get_operation, get_first_chunk_id, get_singular_chunk_id, update_op_parameters,
    update_pool_tree_config, update_user_to_default_pool_map,
    enable_op_detailed_logs, set_node_banned, set_all_nodes_banned,
    create_test_tables, PrepareTables, raises_yt_error,
    update_pool_tree_config_option, update_scheduler_config)

from yt_scheduler_helpers import (
    scheduler_orchid_pool_path, scheduler_orchid_default_pool_tree_path,
    scheduler_orchid_operation_path, scheduler_orchid_default_pool_tree_config_path,
    scheduler_orchid_path, scheduler_orchid_pool_tree_config_path,
    scheduler_new_orchid_pool_tree_path)

from yt_helpers import profiler_factory

import yt_error_codes

import yt.yson as yson

from yt.test_helpers import are_almost_equal

from yt.common import YtError

import pytest

import os
import time

import builtins

##################################################################


def get_from_tree_orchid(tree, path, **kwargs):
    return get("//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/{}/{}".format(tree, path), **kwargs)


##################################################################


@pytest.mark.enabled_multidaemon
class TestResourceUsage(YTEnvSetup, PrepareTables):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "connect_retry_backoff_time": 100,
            "fair_share_update_period": 100,
            "fair_share_profiling_period": 100,
            "alerts_update_period": 100,
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operation_time_limit_check_period": 100,
        }
    }

    DELTA_NODE_CONFIG = {
        "resource_limits": {
            "user_jobs": {
                "type": "static",
                "value": 10 ** 9,
            },
        }
    }

    def setup_method(self, method):
        super(TestResourceUsage, self).setup_method(method)
        set("//sys/pool_trees/default/@config/preemptive_scheduling_backoff", 0)
        set("//sys/pool_trees/default/@config/non_preemptible_resource_usage_threshold", {"user_slots": 0})
        time.sleep(0.5)

    def _check_running_jobs(self, op, desired_running_jobs):
        success_iter = 0
        min_success_iteration = 10
        for _ in range(100):
            time.sleep(0.1)

            running_jobs = op.get_running_jobs()
            if not running_jobs:
                continue

            assert len(running_jobs) <= desired_running_jobs
            if len(running_jobs) < desired_running_jobs:
                continue

            success_iter += 1
            if success_iter == min_success_iteration:
                return
        assert False

    @authors("ignat")
    def test_root_pool(self):
        wait(
            lambda: are_almost_equal(
                get(scheduler_orchid_default_pool_tree_path() + "/pools/<Root>/fair_share_ratio"),
                0.0,
            )
        )

    @authors("ignat")
    def test_scheduler_promised_fair_share(self):
        total_resource_limits = get("//sys/scheduler/orchid/scheduler/cluster/resource_limits")

        create_pool(
            "big_pool",
            attributes={"min_share_resources": {"cpu": total_resource_limits["cpu"]}},
        )
        create_pool("subpool_1", parent_name="big_pool", attributes={"weight": 1.0})
        create_pool("subpool_2", parent_name="big_pool", attributes={"weight": 3.0})
        create_pool("small_pool", attributes={"weight": 100.0})
        create_pool("subpool_3", parent_name="small_pool")
        create_pool("subpool_4", parent_name="small_pool")

        # Wait for fair share update.
        time.sleep(1)

        def get_pool_promised_fair_share_resources(pool):
            return get(scheduler_orchid_pool_path(pool) + "/promised_fair_share_resources")

        def get_pool_promised_dominant_fair_share(pool):
            return get(scheduler_orchid_pool_path(pool) + "/promised_dominant_fair_share")

        assert are_almost_equal(get_pool_promised_dominant_fair_share("big_pool"), 1.0)
        assert get_pool_promised_fair_share_resources("big_pool") == total_resource_limits

        assert are_almost_equal(get_pool_promised_dominant_fair_share("small_pool"), 0)
        assert are_almost_equal(get_pool_promised_dominant_fair_share("subpool_3"), 0)
        assert are_almost_equal(get_pool_promised_dominant_fair_share("subpool_4"), 0)

        assert are_almost_equal(get_pool_promised_dominant_fair_share("subpool_1"), 1.0 / 4.0)
        assert are_almost_equal(get_pool_promised_dominant_fair_share("subpool_2"), 3.0 / 4.0)

        self._prepare_tables()

        def get_operation_promised_dominant_fair_share(op):
            return op.get_runtime_progress("scheduling_info_per_pool_tree/default/promised_dominant_fair_share", 0.0)

        op = map(
            track=False,
            command=with_breakpoint("cat ; BREAKPOINT"),
            in_=["//tmp/t_in"],
            out="//tmp/t_out",
            spec={"pool": "big_pool"},
        )
        wait_breakpoint()

        # Wait for fair share update.
        time.sleep(1)

        assert are_almost_equal(get_operation_promised_dominant_fair_share(op), 1.0 / 5.0)
        assert are_almost_equal(get_pool_promised_dominant_fair_share("subpool_1"), 1.0 / 5.0)
        assert are_almost_equal(get_pool_promised_dominant_fair_share("subpool_2"), 3.0 / 5.0)

        release_breakpoint()
        op.track()

    @authors("ignat")
    def test_resource_limits(self):
        resource_limits = {"cpu": 1.0, "memory": 1000 * 1024 * 1024, "network": 10}
        create_pool("test_pool", attributes={"resource_limits": resource_limits})

        # TODO(renadeen): Do better, I know you can.
        @wait_no_assert
        def check_pool_limits():
            pool_resource_limits = get(scheduler_orchid_pool_path("test_pool") + "/specified_resource_limits")
            for resource, limit in resource_limits.items():
                assert are_almost_equal(pool_resource_limits[resource], limit)

        memory_limit = 200 * 1024 * 1024
        testing_options = {"schedule_job_delay": {"duration": 500, "type": "async"}}
        op = run_sleeping_vanilla(
            job_count=3,
            task_patch={
                "memory_limit": memory_limit,
            },
            spec={
                "pool": "test_pool",
                "testing": testing_options,
            },
        )
        self._check_running_jobs(op, 1)
        op.abort()

        op = run_sleeping_vanilla(
            job_count=3,
            task_patch={
                "memory_limit": memory_limit,
            },
            spec={
                "pool": "test_pool",
                "resource_limits": resource_limits,
                "testing": testing_options,
            },
        )
        self._check_running_jobs(op, 1)

        # TODO(renadeen): Do better, I know you can.
        @wait_no_assert
        def check_op_limits():
            op_resource_limits = get(scheduler_orchid_operation_path(op.id) + "/specified_resource_limits")
            for resource, limit in resource_limits.items():
                assert are_almost_equal(op_resource_limits[resource], limit)

    @authors("ignat")
    @pytest.mark.parametrize("limits_target", ["pool", "operation"])
    def test_resource_limits_preemption(self, limits_target):
        create_pool("test_pool")

        op = run_sleeping_vanilla(
            job_count=3,
            spec={
                "pool": "test_pool",
            },
        )
        wait(lambda: len(op.get_running_jobs()) == 3)

        resource_limits = {"cpu": 2}
        if limits_target == "pool":
            set("//sys/pools/test_pool/@resource_limits", resource_limits)
            wait(
                lambda: are_almost_equal(
                    get(scheduler_orchid_default_pool_tree_path() + "/pools/test_pool/resource_limits/cpu"),
                    2,
                )
            )
        else:
            update_op_parameters(op.id, parameters={
                "scheduling_options_per_pool_tree": {
                    "default": {
                        "resource_limits": {"user_slots": 2},
                    },
                }
            })

        wait(lambda: len(op.get_running_jobs()) == 2)

    @authors("ignat")
    def test_resource_limits_runtime(self):
        op = run_sleeping_vanilla(job_count=3, spec={"resource_limits": {"user_slots": 1}})
        self._check_running_jobs(op, 1)

        update_op_parameters(op.id, parameters={
            "scheduling_options_per_pool_tree": {
                "default": {
                    "resource_limits": {"user_slots": 2},
                },
            }
        })
        self._check_running_jobs(op, 2)

    @authors("ignat")
    def test_max_possible_resource_usage(self):
        create_pool("low_cpu_pool", attributes={"resource_limits": {"cpu": 1}})
        create_pool("subpool_1", parent_name="low_cpu_pool")
        create_pool(
            "subpool_2",
            parent_name="low_cpu_pool",
            attributes={"resource_limits": {"cpu": 0}},
        )
        create_pool("high_cpu_pool")

        self._create_table("//tmp/t_in")
        self._create_table("//tmp/t_out_1")
        self._create_table("//tmp/t_out_2")
        self._create_table("//tmp/t_out_3")
        data = [{"foo": i} for i in range(3)]
        write_table("//tmp/t_in", data)

        def get_pool_fair_share(pool, resource):
            return get("{0}/pools/{1}/detailed_fair_share/total/{2}".format(scheduler_orchid_default_pool_tree_path(), pool, resource))

        command_with_breakpoint = with_breakpoint("cat ; BREAKPOINT")
        op1 = map(
            track=False,
            command=command_with_breakpoint,
            in_="//tmp/t_in",
            out="//tmp/t_out_1",
            spec={"job_count": 1, "pool": "subpool_1"},
        )

        op2 = map(
            track=False,
            command=command_with_breakpoint,
            in_="//tmp/t_in",
            out="//tmp/t_out_2",
            spec={"job_count": 2, "pool": "high_cpu_pool"},
        )

        wait_breakpoint()

        for resource in ["cpu", "user_slots"]:
            wait(lambda: are_almost_equal(get_pool_fair_share("subpool_1", resource), 1.0 / 3.0))
            wait(lambda: are_almost_equal(get_pool_fair_share("subpool_2", resource), 0.0))
            wait(lambda: are_almost_equal(get_pool_fair_share("low_cpu_pool", resource), 1.0 / 3.0))
            wait(lambda: are_almost_equal(get_pool_fair_share("high_cpu_pool", resource), 2.0 / 3.0))

        op3 = map(
            track=False,
            command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out_3",
            spec={"job_count": 1, "pool": "subpool_2", "mapper": {"cpu_limit": 0}},
        )

        time.sleep(1)

        wait(lambda: are_almost_equal(get_pool_fair_share("subpool_1", "user_slots"), 1.0 / 4.0))
        wait(lambda: are_almost_equal(get_pool_fair_share("subpool_2", "user_slots"), 1.0 / 4.0))
        wait(lambda: are_almost_equal(get_pool_fair_share("low_cpu_pool", "user_slots"), 1.0 / 2.0))
        wait(lambda: are_almost_equal(get_pool_fair_share("high_cpu_pool", "user_slots"), 1.0 / 2.0))

        wait(lambda: are_almost_equal(get_pool_fair_share("subpool_1", "cpu"), 1.0 / 4.0))
        wait(lambda: are_almost_equal(get_pool_fair_share("subpool_2", "cpu"), 0.0))
        wait(lambda: are_almost_equal(get_pool_fair_share("low_cpu_pool", "cpu"), 1.0 / 4.0))
        wait(lambda: are_almost_equal(get_pool_fair_share("high_cpu_pool", "cpu"), 1.0 / 2.0))

        release_breakpoint()
        op1.track()
        op2.track()
        op3.track()

    @authors("renadeen", "ignat")
    def test_fractional_cpu_usage(self):
        self._create_table("//tmp/t_in")
        self._create_table("//tmp/t_out")
        data = [{"foo": i} for i in range(3)]
        write_table("//tmp/t_in", data)

        op = map(
            track=False,
            command=with_breakpoint("cat ; BREAKPOINT"),
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"job_count": 3, "mapper": {"cpu_limit": 0.87}},
        )
        wait_breakpoint()

        def get_resource_usage(op):
            return op.get_runtime_progress("scheduling_info_per_pool_tree/default/resource_usage")

        wait(lambda: are_almost_equal(get_resource_usage(op)["cpu"], 3 * 0.87))

        release_breakpoint()
        op.track()

    @authors("ignat")
    def test_pool_change_with_resource_limits(self):
        resource_limits = {"cpu": 1, "memory": 1000 * 1024 * 1024, "network": 10}
        create_pool("destination_pool", attributes={"resource_limits": resource_limits})

        create_pool("source_pool")

        self._create_table("//tmp/t_in")
        self._create_table("//tmp/t_out")
        data = [{"foo": i} for i in range(3)]
        write_table("//tmp/t_in", data)

        op = map(
            track=False,
            command=with_breakpoint("cat ; BREAKPOINT"),
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "job_count": 1,
                "pool": "source_pool",
            },
        )
        wait_breakpoint()

        move("//sys/pools/source_pool", "//sys/pools/destination_pool/source_pool")

        release_breakpoint()
        op.track()

    @authors("ignat")
    def test_pool_change_with_resource_limits_and_limits_after_move(self):
        update_scheduler_config("watchers_update_period", 500)
        update_scheduler_config("fair_share_update_period", 500)

        update_pool_tree_config(
            "default",
            {
                "testing_options": {
                    "delay_inside_resource_usage_initialization_in_tree": 2000,
                },
            })

        resource_limits = {"cpu": 2, "memory": 1000 * 1024 * 1024, "network": 10}
        create_pool("destination_pool", attributes={"resource_limits": resource_limits})

        create_pool("source_pool")

        op = run_test_vanilla(
            track=False,
            command="python3 -c 'import random, time; x = random.randint(1, 5) / 10.0; time.sleep(x)'",
            job_count=10,
            pool="source_pool",
            task_patch={
                "cpu_limit": 0.5,
            },
            spec={
                "testing": {
                    "schedule_job_delay_scheduler": {
                        "duration": 1000,
                    },
                },
            },
        )

        wait(lambda: len(op.get_running_jobs()) >= 1)
        time.sleep(0.2)

        move("//sys/pools/source_pool", "//sys/pools/destination_pool/source_pool")
        set("//sys/pools/destination_pool/source_pool/@resource_limits", resource_limits)

        op.track()

    @authors("renadeen")
    def test_move_does_not_lose_resource_usage_in_source_ancestor(self):
        create_pool(
            "pool_with_limits",
            attributes={"resource_limits": {"cpu": 1}})

        create_pool("pool_without_limits", parent_name="pool_with_limits")

        op1 = run_sleeping_vanilla(spec={"pool": "pool_without_limits"})
        wait(lambda: op1.get_running_jobs())

        move(
            "//sys/pool_trees/default/pool_with_limits/pool_without_limits",
            "//sys/pool_trees/default/pool_without_limits")

        op2 = run_sleeping_vanilla(spec={"pool": "pool_with_limits"})
        # Resource usage in parent pool wasn't transferred and new jobs weren't scheduled as resource_limits exceeded.
        wait(lambda: op2.get_running_jobs())

    @authors("eshcherbin")
    def test_apply_specified_resource_limits_to_demand(self):
        update_scheduler_config("operation_stuck_check", {"safe_timeout": 100000000})

        create_pool("pool", attributes={"resource_limits": {"cpu": 0}})

        op = run_sleeping_vanilla(job_count=3, spec={
            "pool": "pool",
            "apply_specified_resource_limits_to_demand": True,
        })
        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/resource_demand/cpu", default=None) == 3.0)
        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/resource_demand/user_slots") == 3)

        update_op_parameters(op.id, parameters={
            "scheduling_options_per_pool_tree": {
                "default": {
                    "resource_limits": {"cpu": 3.0, "user_slots": 2},
                },
            }
        })
        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/resource_demand/cpu") == 3.0)
        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/resource_demand/user_slots") == 2)

        update_op_parameters(op.id, parameters={
            "scheduling_options_per_pool_tree": {
                "default": {
                    "resource_limits": {"cpu": 1.0, "user_slots": 2},
                },
            }
        })
        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/resource_demand/cpu") == 1.0)
        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/resource_demand/user_slots") == 2)

        set("//sys/pool_trees/default/pool/@non_preemptible_resource_usage_threshold", {"user_slots": 3})
        set("//sys/pool_trees/default/pool/@resource_limits", {"cpu": 2.0})

        update_op_parameters(op.id, parameters={
            "scheduling_options_per_pool_tree": {
                "default": {
                    "resource_limits": {"cpu": 3.0, "user_slots": 3},
                },
            }
        })
        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/resource_demand/cpu") == 3.0)
        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/resource_demand/user_slots") == 3)
        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/resource_usage/user_slots") == 2)

        update_op_parameters(op.id, parameters={
            "scheduling_options_per_pool_tree": {
                "default": {
                    "resource_limits": {"cpu": 1.0, "user_slots": 0},
                },
            }
        })
        time.sleep(3.0)
        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/resource_demand/cpu") == 2.0)
        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/resource_demand/user_slots") == 2)
        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/resource_usage/user_slots") == 2)

    @authors("omgronny")
    def test_limited_resource_demand(self):
        create_pool("some_pool", attributes={"resource_limits": {"user_slots": 1}})

        run_sleeping_vanilla(job_count=3, spec={
            "pool": "some_pool",
        })
        wait(lambda: get(scheduler_orchid_pool_path("some_pool", "default") + "/resource_demand/user_slots", default=None) == 3.0)
        wait(lambda: get(scheduler_orchid_pool_path("some_pool", "default") + "/resource_demand/cpu", default=None) == 3.0)
        wait(lambda: get(scheduler_orchid_pool_path("some_pool", "default") + "/limited_resource_demand/user_slots", default=None) == 1.0)
        wait(lambda: get(scheduler_orchid_pool_path("some_pool", "default") + "/limited_resource_demand/cpu", default=None) == 1.0)

    @authors("omgronny")
    def test_limited_resource_demand_hierarchical(self):
        create_pool("parent_pool", attributes={"resource_limits": {"cpu": 4}})
        create_pool("child_pool1", parent_name="parent_pool", attributes={"resource_limits": {"cpu": 1}})
        create_pool("child_pool2", parent_name="parent_pool", attributes={"resource_limits": {"cpu": 2}})

        run_sleeping_vanilla(job_count=3, spec={
            "pool": "child_pool1",
        })
        wait(lambda: get(scheduler_orchid_pool_path("child_pool1", "default") + "/resource_demand/cpu", default=None) == 3.0)
        wait(lambda: get(scheduler_orchid_pool_path("child_pool1", "default") + "/limited_resource_demand/cpu", default=None) == 1.0)

        run_sleeping_vanilla(job_count=5, spec={
            "pool": "child_pool2",
        })
        wait(lambda: get(scheduler_orchid_pool_path("child_pool2", "default") + "/resource_demand/cpu", default=None) == 5.0)
        wait(lambda: get(scheduler_orchid_pool_path("child_pool2", "default") + "/limited_resource_demand/cpu", default=None) == 2.0)

        wait(lambda: get(scheduler_orchid_pool_path("parent_pool", "default") + "/limited_resource_demand/cpu", default=None) == 3.0)


##################################################################


@pytest.mark.enabled_multidaemon
class TestStrategyWithSlowController(YTEnvSetup, PrepareTables):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    CONCURRENT_HEARTBEAT_LIMIT = 2

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 100,
            "node_shard_count": 1,
            "soft_concurrent_heartbeat_limit": CONCURRENT_HEARTBEAT_LIMIT,
            "hard_concurrent_heartbeat_limit": CONCURRENT_HEARTBEAT_LIMIT,
        }
    }

    @authors("renadeen", "ignat")
    @pytest.mark.xfail(run=False, reason="It's too flacky now and not sure that it is correct")
    def test_strategy_with_slow_controller(self):
        update_pool_tree_config(
            "default",
            {
                "allowed_resource_usage_staleness": 0,
            })
        slow_spec = {"testing": {"schedule_job_delay": {"duration": 1000, "type": "async"}}}

        create_pool("pool")
        create_pool(
            "pool_with_guarantees",
            attributes={"strong_guarantee_resources": {"cpu": 5}})

        # Occupy the cluster
        op0 = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=5, pool="pool_with_guarantees")
        wait_breakpoint(job_count=5)

        # Run operations
        op1 = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=10, pool="pool")
        op2 = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=10, pool="pool", spec=slow_spec)

        wait(lambda: op1.get_state() == "running")
        wait(lambda: op2.get_state() == "running")

        enable_op_detailed_logs(op1)
        enable_op_detailed_logs(op2)

        # Give some time to check that op0 is not preemptible.
        time.sleep(2)

        assert op1.get_job_count("running") == 0
        assert op2.get_job_count("running") == 0

        # Free up the cluster
        for j in op0.get_running_jobs():
            release_breakpoint(job_id=j)

        # Check the resulting allocation
        wait(lambda: op1.get_job_count("running") + op2.get_job_count("running") == 5)
        assert abs(op1.get_job_count("running") - op2.get_job_count("running")) <= self.CONCURRENT_HEARTBEAT_LIMIT


@pytest.mark.enabled_multidaemon
class TestUnavailableChunkStrategies(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 2
    NUM_SCHEDULERS = 1

    def _prepare_tables(self):
        create("table", "//tmp/t_in")
        set("//tmp/t_in/@replication_factor", 1)
        write_table("//tmp/t_in", {"foo": "bar"})

        create("table", "//tmp/t_out")
        set("//tmp/t_out/@replication_factor", 1)

    def _get_table_chunk_node(self, table):
        chunk_id = get_first_chunk_id(table)
        replicas = get("#{0}/@stored_replicas".format(chunk_id))
        assert len(replicas) == 1

        return replicas[0]

    @authors("ignat")
    def test_strategies(self):
        v1 = {"key1": "aaa", "key2": "hello"}
        v2 = {"key1": "bb", "key2": "darkness"}
        v3 = {"key1": "bbxx", "key2": "my"}
        v4 = {"key1": "zfoo", "key2": "old"}
        v5 = {"key1": "zzz", "key2": "friend"}

        create("table", "//tmp/t_in")
        set("//tmp/t_in/@replication_factor", 1)
        write_table("//tmp/t_in", [v1, v2, v3, v4, v5])

        create("table", "//tmp/t_out")
        set("//tmp/t_out/@replication_factor", 1)

        node = self._get_table_chunk_node("//tmp/t_in")
        set_node_banned(node, True)

        print_debug("Fail strategy")
        with pytest.raises(YtError):
            map(
                in_="//tmp/t_in{key2}",
                out="//tmp/t_out",
                command="cat",
                spec={"unavailable_chunk_strategy": "fail"},
            )

        print_debug("Skip strategy")
        map(
            in_="//tmp/t_in{key1}",
            out="//tmp/t_out",
            command="cat",
            spec={"unavailable_chunk_strategy": "skip"},
        )
        assert read_table("//tmp/t_out") == []

        print_debug("Wait strategy")
        op = map(
            track=False,
            in_="//tmp/t_in{key2}",
            out="//tmp/t_out",
            command="cat",
            spec={"unavailable_chunk_strategy": "wait"},
        )

        set_node_banned(node, False)
        op.track()

        assert read_table("//tmp/t_out") == [{"key2": val} for val in ("hello", "darkness", "my", "old", "friend")]

    @authors("gepardo")
    def test_unavailable_chunks_orchid(self):
        self._prepare_tables()

        node = self._get_table_chunk_node("//tmp/t_in")
        set_node_banned(node, True)

        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="cat",
            spec={"unavailable_chunk_strategy": "wait"},
        )

        orchid_path = op.get_orchid_path()
        wait(lambda: exists(orchid_path))
        wait(lambda: get(orchid_path + "/unavailable_input_chunks/<local>") == [get_first_chunk_id("//tmp/t_in")])

        set_node_banned(node, False)
        wait(lambda: get(orchid_path + "/unavailable_input_chunks/<local>") == [])

        op.track()

    @authors("ignat")
    def test_strategies_in_sort(self):
        v1 = {"key": "aaa"}
        v2 = {"key": "bb"}
        v3 = {"key": "bbxx"}
        v4 = {"key": "zfoo"}
        v5 = {"key": "zzz"}

        create("table", "//tmp/t_in")
        set("//tmp/t_in/@replication_factor", 1)
        write_table("//tmp/t_in", [v3, v5, v1, v2, v4])  # some random order

        create("table", "//tmp/t_out")
        set("//tmp/t_out/@replication_factor", 1)

        set_all_nodes_banned(True)

        print_debug("Fail strategy")
        with pytest.raises(YtError):
            sort(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                sort_by="key",
                spec={"unavailable_chunk_strategy": "fail"},
            )

        print_debug("Skip strategy")
        sort(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            sort_by="key",
            spec={"unavailable_chunk_strategy": "skip"},
        )
        assert read_table("//tmp/t_out") == []

        print_debug("Wait strategy")
        op = sort(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            sort_by="key",
            spec={"unavailable_chunk_strategy": "wait"},
        )

        # Give a chance to scraper to work
        time.sleep(1.0)
        set_all_nodes_banned(False)
        op.track()

        assert read_table("//tmp/t_out") == [v1, v2, v3, v4, v5]
        assert get("//tmp/t_out/@sorted")
        assert get("//tmp/t_out/@sorted_by") == ["key"]

    @authors("ignat")
    def test_strategies_in_merge(self):
        create("table", "//tmp/t1")
        set("//tmp/t1/@replication_factor", 1)
        write_table("<append=true>//tmp/t1", [{"a": 0}, {"a": 2}], sorted_by="a")
        write_table("<append=true>//tmp/t1", [{"a": 4}, {"a": 6}], sorted_by="a")

        create("table", "//tmp/t2")
        set("//tmp/t2/@replication_factor", 1)
        write_table("<append=true>//tmp/t2", [{"a": 1}, {"a": 3}], sorted_by="a")
        write_table("<append=true>//tmp/t2", [{"a": 5}, {"a": 7}], sorted_by="a")

        create("table", "//tmp/t_out")
        set("//tmp/t_out/@replication_factor", 1)

        set_all_nodes_banned(True)

        print_debug("Fail strategy")
        with pytest.raises(YtError):
            merge(
                mode="sorted",
                in_=["//tmp/t1", "//tmp/t2"],
                out="//tmp/t_out",
                spec={"unavailable_chunk_strategy": "fail"},
            )

        print_debug("Skip strategy")
        merge(
            mode="sorted",
            in_=["//tmp/t1", "//tmp/t2"],
            out="//tmp/t_out",
            spec={"unavailable_chunk_strategy": "skip"},
        )
        assert read_table("//tmp/t_out") == []

        print_debug("Wait strategy")
        op = merge(
            track=False,
            mode="sorted",
            in_=["//tmp/t1", "//tmp/t2"],
            out="//tmp/t_out",
            spec={"unavailable_chunk_strategy": "wait"},
        )

        # Give a chance for scraper to work
        time.sleep(1.0)
        set_all_nodes_banned(False)
        op.track()

        assert read_table("//tmp/t_out") == [{"a": i} for i in range(8)]
        assert get("//tmp/t_out/@sorted")
        assert get("//tmp/t_out/@sorted_by") == ["a"]


##################################################################


class TestSchedulerOperationLimits(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {"scheduler": {"static_orchid_cache_update_period": 100}}

    def setup_method(self, method):
        super(TestSchedulerOperationLimits, self).setup_method(method)
        set("//sys/pool_trees/default/@config/max_running_operation_count_per_pool", 1)
        set("//sys/pool_trees/default/@config/default_parent_pool", "default_pool")
        default_tree_config_path = "//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/default/config"
        wait(lambda: get(default_tree_config_path)["default_parent_pool"] == "default_pool")
        wait(lambda: get(default_tree_config_path)["max_running_operation_count_per_pool"] == 1)

    def _run_operations(self):
        create("table", "//tmp/in")
        create("table", "//tmp/out1")
        create("table", "//tmp/out2")
        create("table", "//tmp/out3")
        write_table("//tmp/in", [{"foo": "bar"}])

        command = with_breakpoint("cat > /dev/null && BREAKPOINT")
        op1 = map(
            track=False,
            command=command,
            in_=["//tmp/in"],
            out="//tmp/out1",
            spec={"pool": "test_pool_1"},
        )

        op2 = map(
            track=False,
            command=command,
            in_=["//tmp/in"],
            out="//tmp/out2",
            spec={"pool": "test_pool_1"},
        )

        op3 = map(
            track=False,
            command=command,
            in_=["//tmp/in"],
            out="//tmp/out3",
            spec={"pool": "test_pool_2"},
        )

        wait_breakpoint(job_count=2)

        # We sleep some time to make sure that op2 will not start.
        time.sleep(1)

        assert op1.get_state() == "running"
        assert op2.get_state() == "pending"
        assert op3.get_state() == "running"

        release_breakpoint()

        op1.track()
        op2.track()
        op3.track()

        assert read_table("//tmp/out1") == []
        assert read_table("//tmp/out2") == []
        assert read_table("//tmp/out3") == []

    @authors("ignat")
    def test_operations_pool_limit(self):
        create_pool("test_pool_1")
        create_pool("test_pool_2")
        self._run_operations()

    @authors("ignat")
    def test_operations_recursive_pool_limit(self):
        create_pool("research")
        set("//sys/pools/research/@max_running_operation_count", 2)
        create_pool("test_pool_1", parent_name="research")
        create_pool("test_pool_2", parent_name="research")
        self._run_operations()

    @authors("asaitgalin")
    def test_operation_count(self):
        create("table", "//tmp/in")
        write_table("//tmp/in", [{"foo": i} for i in range(5)])

        attrs = {"max_running_operation_count": 3}
        create_pool("research", attributes=attrs)
        create_pool("subpool", parent_name="research", attributes=attrs)
        create_pool("other_subpool", parent_name="subpool", attributes=attrs)

        ops = []
        for i in range(3):
            create("table", "//tmp/out_" + str(i))
            op = map(
                command="sleep 1000; cat >/dev/null",
                in_=["//tmp/in"],
                out="//tmp/out_" + str(i),
                spec={"pool": "other_subpool"},
                track=False,
            )
            ops.append(op)

        wait(lambda: get(scheduler_orchid_default_pool_tree_path() + "/pools/research/operation_count") == 3)
        wait(lambda: get(scheduler_orchid_default_pool_tree_path() + "/pools/research/running_operation_count") == 3)

    @authors("ignat")
    def test_pending_operations_after_revive(self):
        create("table", "//tmp/in")
        create("table", "//tmp/out1")
        create("table", "//tmp/out2")
        data = [{"foo": i} for i in range(5)]
        write_table("//tmp/in", data)

        op1 = map(track=False, command="sleep 5.0; cat", in_=["//tmp/in"], out="//tmp/out1")
        op2 = map(track=False, command="cat", in_=["//tmp/in"], out="//tmp/out2")

        time.sleep(1.5)

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        op1.track()
        op2.track()

        assert read_table("//tmp/out1") == data
        assert read_table("//tmp/out2") == data

    @authors("ermolovd", "ignat")
    def test_abort_of_pending_operation(self):
        create("table", "//tmp/in")
        create("table", "//tmp/out1")
        create("table", "//tmp/out2")
        create("table", "//tmp/out3")
        write_table("//tmp/in", [{"foo": i} for i in range(5)])

        command = with_breakpoint("cat > /dev/null ; BREAKPOINT")
        op1 = map(track=False, command=command, in_=["//tmp/in"], out="//tmp/out1")
        op2 = map(track=False, command=command, in_=["//tmp/in"], out="//tmp/out2")
        op3 = map(track=False, command=command, in_=["//tmp/in"], out="//tmp/out3")

        wait_breakpoint()

        # Sleep some time to make sure that op2 and op3 will not start.
        time.sleep(1)
        assert op1.get_state() == "running"
        assert op2.get_state() == "pending"
        assert op3.get_state() == "pending"

        op2.abort()
        release_breakpoint()
        op1.track()
        op3.track()

        assert op1.get_state() == "completed"
        assert op2.get_state() == "aborted"
        assert op3.get_state() == "completed"

    @authors("ignat")
    def test_reconfigured_pools_operations_limit(self):
        create("table", "//tmp/in")
        create("table", "//tmp/out1")
        create("table", "//tmp/out2")
        write_table("//tmp/in", [{"foo": i} for i in range(5)])

        create_pool("test_pool_1")
        create_pool("test_pool_2")

        op1 = map(
            track=False,
            command=with_breakpoint("cat ; BREAKPOINT"),
            in_=["//tmp/in"],
            out="//tmp/out1",
            spec={"pool": "test_pool_1"},
        )
        wait_breakpoint()

        remove("//sys/pools/test_pool_1")
        create_pool("test_pool_1", parent_name="test_pool_2", wait_for_orchid=True)

        op2 = map(
            track=False,
            command="cat",
            in_=["//tmp/in"],
            out="//tmp/out2",
            spec={"pool": "test_pool_2"},
        )

        assert op1.get_state() == "running"
        wait(lambda: op2.get_state() == "pending")

        release_breakpoint()
        op1.track()
        op2.track()

    @authors("ignat")
    def test_total_operations_limit(self):
        create_pool("research")
        create_pool("research_subpool", parent_name="research")
        create_pool("production")
        set("//sys/pools/research/@max_operation_count", 3)

        create("table", "//tmp/in")
        write_table("//tmp/in", [{"foo": "bar"}])
        for i in range(5):
            create("table", "//tmp/out" + str(i))

        ops = []

        def run(index, pool, should_raise):
            def execute(track):
                return map(
                    track=track,
                    command="sleep 1000; cat",
                    in_=["//tmp/in"],
                    out="//tmp/out" + str(index),
                    spec={"pool": pool},
                )

            if should_raise:
                with pytest.raises(YtError):
                    execute(track=True)
            else:
                op = execute(track=False)
                wait(lambda: op.get_state() in ("pending", "running"))
                ops.append(op)

        for i in range(3):
            run(i, "research", False)

        for i in range(3, 5):
            run(i, "research", True)

        for i in range(3, 5):
            run(i, "research_subpool", True)

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        for i in range(3, 5):
            run(i, "research", True)

        for i in range(3, 5):
            run(i, "production", False)

        pools_path = "//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/default/fair_share_info/pools"
        wait(lambda: get(pools_path + "/production/running_operation_count") == 1)
        wait(lambda: get(pools_path + "/production/operation_count") == 2)
        wait(lambda: get(pools_path + "/research/running_operation_count") == 1)
        wait(lambda: get(pools_path + "/research/operation_count") == 3)
        wait(lambda: get(pools_path + "/<Root>/running_operation_count") == 2)
        wait(lambda: get(pools_path + "/<Root>/operation_count") == 5)

        for op in ops:
            op.abort()

    @authors("ignat")
    def test_pool_changes(self):
        create_pool("research")
        create_pool("subpool", parent_name="research")
        create_pool("production")

        create("table", "//tmp/in")
        write_table("//tmp/in", [{"foo": "bar"}])
        for i in range(5):
            create("table", "//tmp/out" + str(i))

        ops = []

        def run(index, pool):
            ops.append(
                map(
                    track=False,
                    command="sleep 1000; cat",
                    in_=["//tmp/in"],
                    out="//tmp/out" + str(index),
                    spec={"pool": pool},
                )
            )

        for i in range(1, 4):
            run(i, "subpool")

        time.sleep(0.5)

        pools_path = scheduler_orchid_default_pool_tree_path() + "/pools"
        wait(lambda: get(pools_path + "/subpool/running_operation_count") == 1)
        wait(lambda: get(pools_path + "/subpool/operation_count") == 3)

        wait(lambda: get(pools_path + "/research/running_operation_count") == 1)
        wait(lambda: get(pools_path + "/research/operation_count") == 3)

        assert get(pools_path + "/production/running_operation_count") == 0
        assert get(pools_path + "/production/operation_count") == 0

        move("//sys/pools/research/subpool", "//sys/pools/production/subpool")

        time.sleep(0.5)

        assert get(pools_path + "/subpool/running_operation_count") == 1
        assert get(pools_path + "/subpool/operation_count") == 3

        wait(lambda: get(pools_path + "/research/running_operation_count") == 0)
        wait(lambda: get(pools_path + "/research/operation_count") == 0)

        wait(lambda: get(pools_path + "/production/running_operation_count") == 1)
        wait(lambda: get(pools_path + "/production/operation_count") == 3)

    def _test_pool_acl_prologue(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        create_user("u")

    def _test_pool_acl_core(self, pool, acl_path):
        def _run_op():
            map(
                command="cat",
                in_="//tmp/t_in",
                out="//tmp/t_out",
                authenticated_user="u",
                spec={"pool": pool},
            )

        _run_op()
        set("//sys/pools{0}/@acl/0/action".format(acl_path), "deny")
        check_permission("u", "use", "//sys/pools" + acl_path)
        with pytest.raises(YtError):
            _run_op()

    @authors("ignat")
    def test_global_pool_acl(self):
        self._test_pool_acl_prologue()
        create_pool(
            "p",
            attributes={"inherit_acl": False, "acl": [make_ace("allow", "u", "use")]},
        )
        self._test_pool_acl_core("p", "/p")

    @authors("ignat")
    def test_inner_pool_acl(self):
        self._test_pool_acl_prologue()
        create_pool(
            "p1",
            attributes={"inherit_acl": False, "acl": [make_ace("allow", "u", "use")]},
        )
        create_pool("p2", parent_name="p1")
        self._test_pool_acl_core("p2", "/p1")

    @authors("ignat")
    def test_forbid_immediate_operations(self):
        self._test_pool_acl_prologue()

        create_pool("p1", attributes={"forbid_immediate_operations": True})
        create_pool("p2", parent_name="p1")
        create_pool("default_pool", attributes={"forbid_immediate_operations": True})

        with pytest.raises(YtError):
            map(
                command="cat",
                in_="//tmp/t_in",
                out="//tmp/t_out",
                user="u",
                spec={"pool": "p1"},
            )

        map(
            command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            user="u",
            spec={"pool": "p2"},
        )

        map(
            command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            user="u",
            spec={"pool": "p3"},
        )

    @authors("ignat")
    def test_rename_pool_with_running_operation(self):
        create_pool("default_pool", attributes={"resource_limits": {"user_slots": 2}})
        create_pool("test_pool", attributes={"resource_limits": {"user_slots": 1}})

        def rename_test_pool(check_usage=False):
            set("//sys/pool_trees/default/test_pool/@name", "renamed_pool")
            wait(lambda: "renamed_pool" in ls(scheduler_new_orchid_pool_tree_path("default") + "/pools"))

            pool_info = get(scheduler_new_orchid_pool_tree_path("default") + "/pools/test_pool")
            assert pool_info["full_path"] == "/default_pool/test_pool"
            assert pool_info["resource_limits"]["user_slots"] == 3

            if check_usage:
                wait(lambda: get(scheduler_new_orchid_pool_tree_path("default") + "/pools/test_pool/resource_usage/user_slots") == 2)

        def restore_test_pool(check_usage=False):
            set("//sys/pool_trees/default/renamed_pool/@name", "test_pool")
            wait(lambda: "renamed_pool" not in ls(scheduler_new_orchid_pool_tree_path("default") + "/pools"))

            pool_info = get(scheduler_new_orchid_pool_tree_path("default") + "/pools/test_pool")
            assert pool_info["full_path"] == "/test_pool"
            assert pool_info["resource_limits"]["user_slots"] == 1

            if check_usage:
                wait(lambda: get(scheduler_new_orchid_pool_tree_path("default") + "/pools/test_pool/resource_usage/user_slots") == 1)

        op = run_test_vanilla(with_breakpoint("BREAKPOINT; sleep 0.5"), job_count=10, pool="test_pool")

        wait_breakpoint()

        rename_test_pool(check_usage=True)
        restore_test_pool(check_usage=True)

        release_breakpoint()

        rename_test_pool()
        time.sleep(2)
        restore_test_pool()
        time.sleep(2)
        rename_test_pool()

        op.track()

        # Give some time to process ephemeral pool deletion.
        time.sleep(1)


@pytest.mark.enabled_multidaemon
class TestLightweightOperations(YTEnvSetup, PrepareTables):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "static_orchid_cache_update_period": 100,
            "pending_by_pool_operation_scan_period": 1000,
        },
    }

    def _create_pools(self):
        create_pool("root", attributes={"max_operation_count": 5, "max_running_operation_count": 2})
        create_pool("pool", parent_name="root")
        create_pool("lightweight_pool", parent_name="root", attributes={"mode": "fifo", "enable_lightweight_operations": True})

    def _run_operations(self, count, pool):
        ops = []
        for _ in range(count):
            op = run_sleeping_vanilla(spec={"pool": pool})
            ops.append(op)
        return ops

    def _wait_for_operation(self, op, state="running"):
        op.wait_for_state(state)
        wait(lambda: exists(scheduler_orchid_operation_path(op.id)))

    def _check_operation_counts(
            self,
            expected_pool_total_count,
            expected_pool_running_count,
            expected_pool_lightweight_count,
            expected_lightweight_pool_total_count,
            expected_lightweight_pool_running_count,
            expected_lightweight_pool_lightweight_count,
            check_root=True):
        wait(lambda: get(scheduler_orchid_pool_path("pool") + "/operation_count") == expected_pool_total_count)
        wait(lambda: get(scheduler_orchid_pool_path("pool") + "/running_operation_count") == expected_pool_running_count)
        wait(lambda: get(scheduler_orchid_pool_path("pool") + "/lightweight_running_operation_count") == expected_pool_lightweight_count)
        wait(lambda: get(scheduler_orchid_pool_path("lightweight_pool") + "/operation_count") == expected_lightweight_pool_total_count)
        wait(lambda: get(scheduler_orchid_pool_path("lightweight_pool") + "/running_operation_count") == expected_lightweight_pool_running_count)
        wait(lambda: get(scheduler_orchid_pool_path("lightweight_pool") + "/lightweight_running_operation_count") == expected_lightweight_pool_lightweight_count)
        if check_root:
            wait(lambda: get(scheduler_orchid_pool_path("root") + "/operation_count") ==
                 expected_pool_total_count + expected_lightweight_pool_total_count)
            wait(lambda: get(scheduler_orchid_pool_path("root") + "/running_operation_count") ==
                 expected_pool_running_count + expected_lightweight_pool_running_count)
            wait(lambda: get(scheduler_orchid_pool_path("root") + "/lightweight_running_operation_count") ==
                 expected_pool_lightweight_count + expected_lightweight_pool_lightweight_count)

    @authors("eshcherbin")
    def test_simple(self):
        self._create_pools()

        wait(lambda: not get(scheduler_orchid_pool_path("pool") + "/lightweight_operations_enabled"))
        wait(lambda: not get(scheduler_orchid_pool_path("pool") + "/effective_lightweight_operations_enabled"))
        wait(lambda: get(scheduler_orchid_pool_path("lightweight_pool") + "/lightweight_operations_enabled"))
        wait(lambda: get(scheduler_orchid_pool_path("lightweight_pool") + "/effective_lightweight_operations_enabled"))

        blocking_op = run_sleeping_vanilla(spec={"pool": "pool"})
        self._wait_for_operation(blocking_op)

        ops = self._run_operations(4, "lightweight_pool")

        for op in ops:
            self._wait_for_operation(op)

        with pytest.raises(YtError):
            run_sleeping_vanilla(spec={"pool": "lightweight_pool"})

        self._check_operation_counts(1, 1, 0, 4, 0, 4)

        wait(lambda: not get(scheduler_orchid_operation_path(blocking_op.id) + "/lightweight"))
        for op in ops:
            wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/lightweight"))

    @authors("eshcherbin, omgronny")
    def test_change_operation_pool(self):
        self._create_pools()

        ops = self._run_operations(3, "lightweight_pool")

        for op in ops:
            self._wait_for_operation(op)
            wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/lightweight"))
            wait(lambda: get_operation(op.id)["scheduling_attributes_per_pool_tree"]["default"]["running_in_lightweight_pool"])

        for op in ops[:2]:
            update_op_parameters(op.id, parameters={"pool": "pool"})

        for op in ops[:2]:
            wait(lambda: not get(scheduler_orchid_operation_path(op.id) + "/lightweight"))
            wait(lambda: not get_operation(op.id)["scheduling_attributes_per_pool_tree"]["default"]["running_in_lightweight_pool"])

        self._check_operation_counts(2, 2, 0, 1, 0, 1)

        with pytest.raises(YtError):
            update_op_parameters(ops[2].id, parameters={"pool": "pool"})

        for op in ops:
            update_op_parameters(op.id, parameters={"pool": "lightweight_pool"})

        for op in ops:
            wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/lightweight"))
            wait(lambda: get_operation(op.id)["scheduling_attributes_per_pool_tree"]["default"]["running_in_lightweight_pool"])

        self._run_operations(2, "pool")

        self._check_operation_counts(2, 2, 0, 3, 0, 3)

    @authors("eshcherbin, omgronny")
    def test_change_pool_config(self):
        self._create_pools()

        ops = self._run_operations(3, "lightweight_pool")
        ops += self._run_operations(2, "pool")

        for op in ops:
            self._wait_for_operation(op)

        for op in ops[:3]:
            wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/lightweight"))
            wait(lambda: get_operation(op.id)["scheduling_attributes_per_pool_tree"]["default"]["running_in_lightweight_pool"])
        for op in ops[3:]:
            wait(lambda: not get(scheduler_orchid_operation_path(op.id) + "/lightweight"))
            wait(lambda: not get_operation(op.id)["scheduling_attributes_per_pool_tree"]["default"]["running_in_lightweight_pool"])

        self._check_operation_counts(2, 2, 0, 3, 0, 3)

        set("//sys/pool_trees/default/root/lightweight_pool/@enable_lightweight_operations", False)

        wait(lambda: not get(scheduler_orchid_pool_path("lightweight_pool") + "/lightweight_operations_enabled"))
        wait(lambda: not get(scheduler_orchid_pool_path("lightweight_pool") + "/effective_lightweight_operations_enabled"))
        self._check_operation_counts(2, 2, 0, 3, 3, 0)
        for op in ops:
            wait(lambda: not get(scheduler_orchid_operation_path(op.id) + "/lightweight"))
            wait(lambda: not get_operation(op.id)["scheduling_attributes_per_pool_tree"]["default"]["running_in_lightweight_pool"])

        set("//sys/pool_trees/default/root/lightweight_pool/@mode", "fair_share")

        wait(lambda: not get(scheduler_orchid_pool_path("lightweight_pool") + "/lightweight_operations_enabled"))
        wait(lambda: not get(scheduler_orchid_pool_path("lightweight_pool") + "/effective_lightweight_operations_enabled"))
        self._check_operation_counts(2, 2, 0, 3, 3, 0)
        for op in ops:
            wait(lambda: not get(scheduler_orchid_operation_path(op.id) + "/lightweight"))
            wait(lambda: not get_operation(op.id)["scheduling_attributes_per_pool_tree"]["default"]["running_in_lightweight_pool"])

        set("//sys/pool_trees/default/root/lightweight_pool/@enable_lightweight_operations", True)

        wait(lambda: get(scheduler_orchid_pool_path("lightweight_pool") + "/lightweight_operations_enabled"))
        wait(lambda: not get(scheduler_orchid_pool_path("lightweight_pool") + "/effective_lightweight_operations_enabled"))
        self._check_operation_counts(2, 2, 0, 3, 3, 0)

        set("//sys/pool_trees/default/root/lightweight_pool/@mode", "fifo")

        wait(lambda: get(scheduler_orchid_pool_path("lightweight_pool") + "/lightweight_operations_enabled"))
        wait(lambda: get(scheduler_orchid_pool_path("lightweight_pool") + "/effective_lightweight_operations_enabled"))
        self._check_operation_counts(2, 2, 0, 3, 0, 3)

        for op in ops[:3]:
            wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/lightweight"))
            wait(lambda: get_operation(op.id)["scheduling_attributes_per_pool_tree"]["default"]["running_in_lightweight_pool"])

    @authors("eshcherbin")
    def test_vanilla_operation_in_fair_share_pool(self):
        self._create_pools()

        set("//sys/pool_trees/default/root/pool/@enable_lightweight_operations", True)
        wait(lambda: get(scheduler_orchid_pool_path("pool") + "/lightweight_operations_enabled"))
        wait(lambda: not get(scheduler_orchid_pool_path("pool") + "/effective_lightweight_operations_enabled"))

        op1 = run_sleeping_vanilla(spec={"pool": "pool"})
        self._check_operation_counts(1, 1, 0, 0, 0, 0)

        op1.abort()
        self._check_operation_counts(0, 0, 0, 0, 0, 0)

        set("//sys/pool_trees/default/root/pool/@enable_lightweight_operations", False)

        op2 = run_sleeping_vanilla(spec={"pool": "pool"})
        self._check_operation_counts(1, 1, 0, 0, 0, 0)

        set("//sys/pool_trees/default/root/pool/@enable_lightweight_operations", True)
        self._check_operation_counts(1, 1, 0, 0, 0, 0)

        op2.abort()
        self._check_operation_counts(0, 0, 0, 0, 0, 0)

    @authors("eshcherbin")
    def test_move_pool(self):
        self._create_pools()

        ops = self._run_operations(3, "lightweight_pool")
        ops += self._run_operations(2, "pool")

        for op in ops:
            self._wait_for_operation(op)

        self._check_operation_counts(2, 2, 0, 3, 0, 3)

        move("//sys/pool_trees/default/root/lightweight_pool", "//sys/pool_trees/default/root/pool/lightweight_pool")

        self._check_operation_counts(5, 2, 3, 3, 0, 3, check_root=False)

    @authors("eshcherbin")
    def test_pending_operation_becomes_lightweight(self):
        self._create_pools()

        running_ops = self._run_operations(2, "pool")
        time.sleep(0.1)

        pending_ops = self._run_operations(2, "pool")

        for op in running_ops:
            self._wait_for_operation(op)
            wait(lambda: not get(scheduler_orchid_operation_path(op.id) + "/lightweight"))
        for op in pending_ops:
            self._wait_for_operation(op, "pending")
            wait(lambda: not get(scheduler_orchid_operation_path(op.id) + "/lightweight"))

        self._check_operation_counts(4, 2, 0, 0, 0, 0)

        update_op_parameters(pending_ops[0].id, parameters={"pool": "lightweight_pool"})

        wait(lambda: get(scheduler_orchid_operation_path(pending_ops[0].id) + "/lightweight"))
        self._check_operation_counts(3, 2, 0, 1, 0, 1)
        self._wait_for_operation(pending_ops[0])

        set("//sys/pool_trees/default/root/pool/@mode", "fifo")
        set("//sys/pool_trees/default/root/pool/@enable_lightweight_operations", True)

        wait(lambda: get(scheduler_orchid_operation_path(pending_ops[1].id) + "/lightweight"))
        self._check_operation_counts(3, 0, 3, 1, 0, 1)
        self._wait_for_operation(pending_ops[1])

    @authors("eshcherbin")
    def test_pending_operation_becomes_running_after_other_running_operation_becomes_lightweight(self):
        self._create_pools()
        set("//sys/pool_trees/default/root/lightweight_pool/@enable_lightweight_operations", False)
        wait(lambda: not get(scheduler_orchid_pool_path("lightweight_pool") + "/effective_lightweight_operations_enabled"))

        blocking_op = run_sleeping_vanilla(spec={"pool": "lightweight_pool"})
        self._wait_for_operation(blocking_op)

        ops = []
        for _ in range(2):
            op = run_sleeping_vanilla(spec={"pool": "pool"})
            ops.append(op)
            time.sleep(0.1)

        self._wait_for_operation(ops[0])
        self._wait_for_operation(ops[1], "pending")

        set("//sys/pool_trees/default/root/lightweight_pool/@enable_lightweight_operations", True)
        wait(lambda: get(scheduler_orchid_pool_path("lightweight_pool") + "/effective_lightweight_operations_enabled"))

        wait(lambda: get(scheduler_orchid_operation_path(blocking_op.id) + "/lightweight"))
        for op in ops:
            wait(lambda: not get(scheduler_orchid_operation_path(op.id) + "/lightweight"))
        self._wait_for_operation(ops[1])

    @authors("eshcherbin")
    def test_operation_eligibility(self):
        self._create_pools()

        self._create_table("//tmp/t_in")
        self._create_table("//tmp/t_out_1")
        self._create_table("//tmp/t_out_2")
        data = [{"foo": i} for i in range(3)]
        write_table("//tmp/t_in", data)

        blocking_op = run_sleeping_vanilla(spec={"pool": "pool"})
        self._wait_for_operation(blocking_op)

        map_ops = []
        for i in range(2):
            op = map(
                track=False,
                command="cat; sleep 1000",
                in_="//tmp/t_in",
                out="//tmp/t_out_{}".format(i + 1),
                spec={"job_count": 1, "pool": "lightweight_pool"},
            )
            map_ops.append(op)

        self._wait_for_operation(map_ops[0])
        self._wait_for_operation(map_ops[1], "pending")
        for op in map_ops:
            wait(lambda: not get(scheduler_orchid_operation_path(op.id) + "/lightweight"))
        self._check_operation_counts(1, 1, 0, 2, 1, 0)

        vanilla_op = run_sleeping_vanilla(spec={"pool": "lightweight_pool"})

        self._wait_for_operation(vanilla_op)
        wait(lambda: get(scheduler_orchid_operation_path(vanilla_op.id) + "/lightweight"))
        self._check_operation_counts(1, 1, 0, 3, 1, 1)

    @authors("eshcherbin")
    def test_only_eligible_pending_operation_becomes_running(self):
        self._create_pools()

        self._create_table("//tmp/t_in")
        self._create_table("//tmp/t_out")
        data = [{"foo": i} for i in range(3)]
        write_table("//tmp/t_in", data)

        blocking_ops = self._run_operations(2, "pool")
        for op in blocking_ops:
            self._wait_for_operation(op)

        set("//sys/pool_trees/default/root/lightweight_pool/@enable_lightweight_operations", False)
        wait(lambda: not get(scheduler_orchid_pool_path("lightweight_pool") + "/effective_lightweight_operations_enabled"))

        vanilla_op = run_sleeping_vanilla(spec={"pool": "lightweight_pool"})
        map_op = map(
            track=False,
            command="cat; sleep 1000",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"job_count": 1, "pool": "lightweight_pool"},
        )

        self._wait_for_operation(vanilla_op, "pending")
        self._wait_for_operation(map_op, "pending")

        set("//sys/pool_trees/default/root/lightweight_pool/@enable_lightweight_operations", True)

        self._wait_for_operation(vanilla_op)
        self._wait_for_operation(map_op, "pending")


##################################################################


@pytest.mark.enabled_multidaemon
class TestInferWeightFromGuarantees(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {"scheduler": {"fair_share_update_period": 100}}

    @authors("ignat")
    def test_infer_weight_from_strong_guarantees(self):
        total_cpu_limit = get("//sys/scheduler/orchid/scheduler/cluster/resource_limits/cpu")
        set("//sys/pool_trees/default/@config/infer_weight_from_min_share_ratio_multiplier", 10)

        create_pool(
            "test_pool1",
            pool_tree="default",
            attributes={"strong_guarantee_resources": {"cpu": 0.3 * total_cpu_limit}},
        )
        create_pool(
            "test_pool2",
            pool_tree="default",
            attributes={"strong_guarantee_resources": {"cpu": 0.4 * total_cpu_limit}},
        )
        create_pool("test_pool3", pool_tree="default")
        create_pool(
            "subpool1",
            pool_tree="default",
            parent_name="test_pool2",
            attributes={"strong_guarantee_resources": {"cpu": 0.4 * 0.3 * total_cpu_limit}},
        )
        create_pool(
            "subpool2",
            pool_tree="default",
            parent_name="test_pool2",
            attributes={"strong_guarantee_resources": {"cpu": 0.4 * 0.4 * total_cpu_limit}},
        )

        def get_pool_weight(pool):
            return get(scheduler_orchid_pool_path(pool) + "/weight")

        wait(lambda: are_almost_equal(get_pool_weight("test_pool1"), 3.0 / 7.0 * 10.0))
        wait(lambda: are_almost_equal(get_pool_weight("test_pool2"), 4.0 / 7.0 * 10.0))
        wait(lambda: are_almost_equal(get_pool_weight("test_pool3"), 1.0))

        wait(lambda: are_almost_equal(get_pool_weight("subpool1"), 3.0))
        wait(lambda: are_almost_equal(get_pool_weight("subpool2"), 4.0))

    @authors("renadeen")
    def test_infer_weight_from_resource_flow(self):
        total_cpu_limit = get("//sys/scheduler/orchid/scheduler/cluster/resource_limits/cpu")
        set("//sys/pool_trees/default/@config/infer_weight_from_min_share_ratio_multiplier", 10)

        create_pool(
            "test_pool1",
            pool_tree="default",
            attributes={"integral_guarantees": {
                "resource_flow": {"cpu": 0.3 * total_cpu_limit},
                "guarantee_type": "relaxed",
            }},
        )
        create_pool(
            "test_pool2",
            pool_tree="default",
            attributes={"integral_guarantees": {
                "resource_flow": {"cpu": 0.4 * total_cpu_limit},
                "guarantee_type": "none",
            }},
        )
        create_pool(
            "subpool1",
            pool_tree="default",
            parent_name="test_pool2",
            attributes={"integral_guarantees": {
                "resource_flow": {"cpu": 0.4 * 0.4 * total_cpu_limit},
                "guarantee_type": "burst",
            }},
        )
        create_pool(
            "subpool2",
            pool_tree="default",
            parent_name="test_pool2",
            attributes={"integral_guarantees": {
                "resource_flow": {"cpu": 0.4 * 0.6 * total_cpu_limit},
                "guarantee_type": "relaxed",
            }},
        )
        create_pool("test_pool3", pool_tree="default")

        def get_pool_weight(pool):
            return get(scheduler_orchid_pool_path(pool) + "/weight")

        wait(lambda: are_almost_equal(get_pool_weight("test_pool1"), 3.0 / 7.0 * 10.0))
        wait(lambda: are_almost_equal(get_pool_weight("test_pool2"), 4.0 / 7.0 * 10.0))
        wait(lambda: are_almost_equal(get_pool_weight("test_pool3"), 1.0))

        wait(lambda: are_almost_equal(get_pool_weight("subpool1"), 4.0))
        wait(lambda: are_almost_equal(get_pool_weight("subpool2"), 6.0))

    @authors("renadeen")
    def test_infer_weight_from_both_guarantees(self):
        total_cpu_limit = get("//sys/scheduler/orchid/scheduler/cluster/resource_limits/cpu")
        set("//sys/pool_trees/default/@config/infer_weight_from_min_share_ratio_multiplier", 10)

        create_pool(
            "test_pool1",
            pool_tree="default",
            attributes={"strong_guarantee_resources": {"cpu": 0.4 * total_cpu_limit}},
        )
        create_pool(
            "test_pool2",
            pool_tree="default",
            attributes={"integral_guarantees": {
                "resource_flow": {"cpu": 0.6 * total_cpu_limit},
                "guarantee_type": "relaxed",
            }},
        )
        create_pool("test_pool3", pool_tree="default")

        def get_pool_weight(pool):
            return get(scheduler_orchid_pool_path(pool) + "/weight")

        wait(lambda: are_almost_equal(get_pool_weight("test_pool1"), 0.4 * 10.0))
        wait(lambda: are_almost_equal(get_pool_weight("test_pool2"), 0.6 * 10.0))
        wait(lambda: are_almost_equal(get_pool_weight("test_pool3"), 1.0))


##################################################################


@pytest.mark.enabled_multidaemon
class TestSchedulerStuckOperations(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 100,
            "operation_stuck_check": {
                "period": 100,
                "safe_timeout": 5000,
                "min_schedule_allocation_attempts": 10,
                "limiting_ancestor_safe_timeout": 5000,
            },
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "banned_exec_nodes_check_period": 100000000,
            "map_reduce_operation_options": {
                "min_uncompressed_block_size": 1,
            },
        }
    }

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        config["job_resource_manager"]["resource_limits"]["cpu"] = 2
        config["job_resource_manager"]["resource_limits"]["user_slots"] = 2

    def setup_method(self, method):
        super(TestSchedulerStuckOperations, self).setup_method(method)
        # TODO(eshcherbin): Remove this after tree config is reset correctly in yt_env_setup.
        set("//sys/pool_trees/default/@config/enable_limiting_ancestor_check", True)
        wait(lambda: get(scheduler_orchid_pool_tree_config_path("default") + "/enable_limiting_ancestor_check"))

    @authors("ignat")
    def test_stuck_operation(self):
        ops = []
        for node in ls("//sys/cluster_nodes"):
            op = run_test_vanilla(
                command="sleep 1000",
                spec={
                    "pool": "fake_pool",
                    "scheduling_tag_filter": node,
                },
            )
            ops.append(op)

        for op in ops:
            wait(lambda: len(op.get_running_jobs()) == 1)

        op = run_test_vanilla(
            command="sleep 1000",
            spec={"pool": "fake_pool"},
            task_patch={"cpu_limit": 1.5},
        )

        wait(lambda: op.get_state() == "failed")

        result = str(get(op.get_path() + "/@result"))
        assert "scheduling is stuck" in result
        assert "no successful scheduled allocations" in result

    @authors("eshcherbin")
    def test_disable_limiting_ancestor_check_for_operation(self):
        create_pool("limiting_pool", attributes={"resource_limits": {"cpu": 1.0}})
        create_pool("subpool", parent_name="limiting_pool")
        wait(lambda: get(scheduler_orchid_pool_path("limiting_pool") + "/resource_limits/cpu") == 1.0)

        # NB(eshcherbin): We set the pool in "scheduling_options_per_pool_tree" to also cover the problem from YT-13761.
        op = run_test_vanilla(
            "sleep 0.5",
            job_count=10,
            spec={
                "enable_limiting_ancestor_check": False,
                "scheduling_options_per_pool_tree": {
                    "default": {
                        "pool": "subpool",
                    },
                },
            },
            task_patch={"cpu_limit": 2.0},
        )

        # Let the operation hang for some time. The limiting ancestor check should not be triggered.
        time.sleep(10)

        remove("//sys/pool_trees/default/limiting_pool/@resource_limits")
        op.track()

    @authors("eshcherbin")
    def test_disable_limiting_ancestor_check_for_tree(self):
        set("//sys/pool_trees/default/@config/enable_limiting_ancestor_check", False)
        wait(lambda: not get(scheduler_orchid_pool_tree_config_path("default") + "/enable_limiting_ancestor_check"))

        create_pool("limiting_pool", attributes={"resource_limits": {"cpu": 1.0}})
        create_pool("subpool", parent_name="limiting_pool")
        wait(lambda: get(scheduler_orchid_pool_path("limiting_pool") + "/resource_limits/cpu") == 1.0)

        op = run_test_vanilla(
            "sleep 0.5",
            job_count=10,
            spec={"pool": "subpool"},
            task_patch={"cpu_limit": 2.0},
        )

        # Let the operation hang for some time. The limiting ancestor check should not be triggered.
        time.sleep(10)

        remove("//sys/pool_trees/default/limiting_pool/@resource_limits")
        op.track()

    @authors("eshcherbin")
    def test_limiting_ancestor(self):
        create_pool("limiting_pool", attributes={"resource_limits": {"cpu": 1.0}})
        create_pool("subpool", parent_name="limiting_pool")
        wait(lambda: get(scheduler_orchid_pool_path("limiting_pool") + "/resource_limits/cpu") == 1.0)

        op = run_sleeping_vanilla(job_count=10, spec={"pool": "subpool"}, task_patch={"cpu_limit": 2.0})

        wait(lambda: op.get_state() == "failed")

        result = str(get(op.get_path() + "/@result"))

        # TODO(eshcherbin): Use error codes.
        assert "scheduling is stuck" in result
        assert "ancestor \"limiting_pool\" whose specified limits for resources [cpu] are too small" in result

    @authors("eshcherbin")
    def test_skip_limiting_ancestor_check_on_node_shortage(self):
        update_scheduler_config("operation_stuck_check", {"safe_timeout": 100000000})

        set("//sys/pool_trees/default/@config/nodes_filter", "!custom")
        create_pool_tree("custom_tree", config={"nodes_filter": "custom"})
        create_pool("research", pool_tree="custom_tree")

        node = ls("//sys/cluster_nodes")[0]
        set("//sys/cluster_nodes/{}/@user_tags".format(node), ["custom"])
        wait(
            lambda: get(
                scheduler_orchid_path() + "/scheduler/scheduling_info_per_pool_tree/custom_tree/resource_limits/cpu"
            )
            > 0.0
        )

        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=2,
            spec={"pool": "subpool", "pool_trees": ["custom_tree"]},
            task_patch={"cpu_limit": 2.0},
        )
        wait_breakpoint()

        # Ensure that the controller has seen the available node and will not fail the operation.
        time.sleep(3.0)

        set("//sys/cluster_nodes/{}/@user_tags".format(node), [])
        wait(
            lambda: get(
                scheduler_orchid_path() + "/scheduler/scheduling_info_per_pool_tree/custom_tree/resource_limits/cpu"
            )
            == 0.0
        )

        release_breakpoint()

        # Let the operation hang with no available nodes in the tree for some time.
        # The limiting ancestor check should not be triggered.
        time.sleep(10)

        set("//sys/cluster_nodes/{}/@user_tags".format(node), ["custom"])
        wait(
            lambda: get(
                scheduler_orchid_path() + "/scheduler/scheduling_info_per_pool_tree/custom_tree/resource_limits/cpu"
            )
            > 0.0
        )

        op.track()

    @authors("eshcherbin")
    def test_no_fail_if_min_needed_resources_are_laggy(self):
        set("//sys/scheduler/config/min_needed_resources_update_period", 1000)
        update_scheduler_config("operation_stuck_check", {"safe_timeout": 100000000})

        data = [{"foo": str(i - (i % 2)), "bar": i} for i in range(8)]
        create("table", "//tmp/in")
        create("table", "//tmp/out")
        write_table("//tmp/in", data)

        op = map_reduce(
            mapper_command=with_breakpoint("BREAKPOINT; cat"),
            reducer_command="sleep 10; cat",
            in_="//tmp/in",
            out="//tmp/out",
            sort_by=["foo"],
            spec={
                "partition_count": 4,
                "pivot_keys": [["0"], ["2"], ["4"], ["6"]],
                "data_size_per_reduce_job": 2,
                "resource_limits": {
                    "cpu": 1
                },
            },
            track=False
        )

        wait_breakpoint()

        time.sleep(2.0)

        set("//sys/scheduler/config/min_needed_resources_update_period", 100000000)
        wait(lambda: get(scheduler_orchid_path() + "/scheduler/config/min_needed_resources_update_period") == 100000000)

        time.sleep(5.0)

        release_breakpoint()

        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/usage_share/cpu", default=0.0) == 0.0)
        set("//sys/scheduler/config/min_needed_resources_update_period", 100)
        wait(lambda: get(scheduler_orchid_path() + "/scheduler/config/min_needed_resources_update_period") == 100)

        # The limiting ancestor check should not be triggered.
        # See: YT-13869.
        op.track()


##################################################################


@pytest.mark.enabled_multidaemon
class TestEphemeralPools(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "fair_share_update_period": 300,
        }
    }

    def teardown_method(self, method):
        update_user_to_default_pool_map({})
        super(TestEphemeralPools, self).teardown_method(method)

    def wait_pool_exists(self, pool):
        wait(lambda: exists(scheduler_orchid_pool_path(pool)), sleep_backoff=0.1)

    def get_pool_parent(self, pool):
        return get(scheduler_orchid_pool_path(pool) + "/parent")

    @authors("ignat")
    def test_default_parent_pool(self):
        create_pool("default_pool")
        update_pool_tree_config(
            "default",
            {"default_parent_pool": "default_pool", "max_ephemeral_pools_per_user": 2})

        command = with_breakpoint("BREAKPOINT")
        op1 = run_test_vanilla(command)
        op2 = run_test_vanilla(command, spec={"pool": "my_pool"})

        # Each operation has one job.
        wait_breakpoint(job_count=2)

        pools_path = scheduler_orchid_default_pool_tree_path() + "/pools"
        pool = get(pools_path + "/root")
        assert pool["parent"] == "default_pool"

        pool = get(pools_path + "/my_pool")
        assert pool["parent"] == "default_pool"

        scheduling_info_per_pool_tree = "//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree"
        assert builtins.set(["root", "my_pool"]) == builtins.set(
            get(scheduling_info_per_pool_tree + "/default/user_to_ephemeral_pools/root")
        )

        remove("//sys/pools/default_pool")

        release_breakpoint()
        for op in [op1, op2]:
            op.track()

    @authors("renadeen")
    def test_ephemeral_pool_is_created_in_user_default_parent_pool(self):
        create_user("u")
        create_pool("default_for_u")
        update_pool_tree_config("default", {"use_user_default_parent_pool_map": True})
        update_user_to_default_pool_map({"u": "default_for_u"})

        op = run_sleeping_vanilla(authenticated_user="u")

        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/pool", default="") == "u")
        wait(lambda: get(scheduler_orchid_pool_path("u") + "/parent", default="") == "default_for_u")

    @authors("renadeen")
    def test_change_in_user_default_parent_pool_map_changes_parent_of_existing_ephemeral_pool(self):
        create_user("u")
        create_pool("default_for_u")
        update_pool_tree_config("default", {"use_user_default_parent_pool_map": True})

        run_sleeping_vanilla(authenticated_user="u")
        wait(lambda: exists(scheduler_orchid_pool_path("u")))
        wait(lambda: get(scheduler_orchid_pool_path("u") + "/parent", default="") == "<Root>")

        update_user_to_default_pool_map({"u": "default_for_u"})

        wait(lambda: get(scheduler_orchid_pool_path("u") + "/parent", default="") == "default_for_u")

    @authors("renadeen")
    def test_nonexistent_pools_are_created_in_default_user_pool(self):
        update_pool_tree_config("default", {"use_user_default_parent_pool_map": True})
        create_user("u")
        create_pool("default_for_u")
        update_user_to_default_pool_map({"u": "default_for_u"})

        op = run_sleeping_vanilla(spec={"pool": "nonexistent"}, authenticated_user="u")

        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/pool", default="") == "nonexistent")
        wait(lambda: get(scheduler_orchid_pool_path("nonexistent") + "/parent", default="") == "default_for_u")

    @authors("renadeen")
    def test_ephemeral_flag(self):
        create_pool("real_pool")
        op = run_sleeping_vanilla(spec={"pool": "ephemeral_pool"})
        op.wait_for_state("running")
        wait(lambda: get(scheduler_orchid_pool_path("ephemeral_pool") + "/is_ephemeral", default=False))
        wait(lambda: not get(scheduler_orchid_pool_path("real_pool") + "/is_ephemeral", default=False))
        wait(lambda: not get(scheduler_orchid_pool_path("<Root>") + "/is_ephemeral", default=False))

    @authors("renadeen")
    def test_ephemeral_pool_in_custom_pool_simple(self):
        create_pool(
            "custom_pool",
            attributes={
                "create_ephemeral_subpools": True,
            })

        op = run_sleeping_vanilla(spec={"pool": "custom_pool"})
        wait(lambda: len(list(op.get_running_jobs())) == 1)

        wait(lambda: exists(scheduler_orchid_default_pool_tree_path() + "/pools/custom_pool$root"))
        pool = get(scheduler_orchid_default_pool_tree_path() + "/pools/custom_pool$root")
        assert pool["parent"] == "custom_pool"
        assert pool["mode"] == "fair_share"
        assert pool["is_ephemeral"]

    @authors("renadeen")
    def test_crash_on_disabling_ephemeral_hub(self):
        # Scenario:
        # 1. there is main pool which is ephemeral hub and has max_running_operation_count=1
        # 2. launch first operation in the main pool - it runs in the ephemeral subpool
        # 3. launch second operation in the main pool - it will be attached to the ephemeral subpool as well
        #    but will become pending as parent reached its max_running_operation_count
        # 4. remove `"create_ephemeral_subpools": True` from the main pool
        # 5. scheduler tries to move all operations from the ephemeral subpool to the main pool in order to remove ephemeral subpool
        #    but it won't pay attention to pending state of operations and will run all operations in main pool
        # 7. running operation count breaks the limit and scheduler crashes

        create_pool(
            "ephemeral_hub",
            attributes={
                "create_ephemeral_subpools": True,
                "max_running_operation_count": 1,
            })

        op = run_sleeping_vanilla(spec={"pool": "ephemeral_hub"})
        wait(lambda: len(list(op.get_running_jobs())) == 1)
        wait(lambda: exists(scheduler_orchid_default_pool_tree_path() + "/pools/ephemeral_hub$root"))

        op = run_sleeping_vanilla(spec={"pool": "ephemeral_hub"})
        wait(lambda: op.get_state() == "pending")

        remove("//sys/pools/ephemeral_hub/@create_ephemeral_subpools")
        # crash
        wait(lambda: not exists(scheduler_orchid_default_pool_tree_path() + "/pools/ephemeral_hub$root"))

    @authors("renadeen")
    def test_custom_ephemeral_pool_persists_after_pool_update(self):
        create_pool(
            "custom_pool",
            attributes={
                "create_ephemeral_subpools": True,
            })

        op = run_sleeping_vanilla(spec={"pool": "custom_pool"})
        wait(lambda: len(list(op.get_running_jobs())) == 1)
        wait(
            lambda: get(
                scheduler_orchid_default_pool_tree_path() + "/pools/custom_pool$root/parent",
                default="",
            )
            == "custom_pool"
        )

        create_pool("trigger_pool_update")
        time.sleep(0.5)  # wait orchid update
        # after pools update all ephemeral pools where mistakenly moved to default pool
        assert get(scheduler_orchid_default_pool_tree_path() + "/pools/custom_pool$root/parent") == "custom_pool"

    @authors("renadeen")
    def test_ephemeral_pool_parent_is_removed_after_operation_complete(self):
        create_pool(
            "custom_pool",
            attributes={
                "create_ephemeral_subpools": True,
            })

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), spec={"pool": "custom_pool"})
        wait_breakpoint()

        remove("//sys/pools/custom_pool")
        wait(lambda: exists(scheduler_orchid_default_pool_tree_path() + "/pools/custom_pool"))
        wait(lambda: exists(scheduler_orchid_default_pool_tree_path() + "/pools/custom_pool$root"))

        release_breakpoint()
        op.track()
        wait(lambda: not exists(scheduler_orchid_default_pool_tree_path() + "/pools/custom_pool"))
        wait(lambda: not exists(scheduler_orchid_default_pool_tree_path() + "/pools/custom_pool$root"))

    @authors("renadeen")
    def test_custom_ephemeral_pool_scheduling_mode(self):
        create_pool(
            "custom_pool_fifo",
            attributes={
                "create_ephemeral_subpools": True,
                "ephemeral_subpool_config": {"mode": "fifo"},
            })

        op = run_sleeping_vanilla(spec={"pool": "custom_pool_fifo"})
        wait(lambda: len(list(op.get_running_jobs())) == 1)

        wait(lambda: exists(scheduler_orchid_default_pool_tree_path() + "/pools/custom_pool_fifo$root"))
        pool_fifo = get(scheduler_orchid_default_pool_tree_path() + "/pools/custom_pool_fifo$root")
        assert pool_fifo["parent"] == "custom_pool_fifo"
        assert pool_fifo["mode"] == "fifo"

    @authors("renadeen")
    def test_custom_ephemeral_pool_max_operation_count(self):
        create_pool(
            "custom_pool",
            attributes={
                "create_ephemeral_subpools": True,
                "ephemeral_subpool_config": {"max_operation_count": 1},
            })

        op = run_sleeping_vanilla(spec={"pool": "custom_pool"})
        wait(lambda: len(list(op.get_running_jobs())) == 1)

        with pytest.raises(YtError):
            run_test_vanilla(command="", spec={"pool": "custom_pool"}, track=True)

    @authors("renadeen")
    def test_custom_ephemeral_pool_resource_limits(self):
        create_pool(
            "custom_pool",
            attributes={
                "create_ephemeral_subpools": True,
                "ephemeral_subpool_config": {"resource_limits": {"cpu": 1}}
            })

        run_sleeping_vanilla(spec={"pool": "custom_pool"})
        wait(lambda: exists(scheduler_orchid_default_pool_tree_path() + "/pools/custom_pool$root"))
        pool_info = get(scheduler_orchid_default_pool_tree_path() + "/pools/custom_pool$root")
        assert pool_info["resource_limits"]["cpu"] == 1.0

    @authors("renadeen")
    def test_ephemeral_to_explicit_pool_transformation(self):
        create_pool("default_pool", wait_for_orchid=False)
        set("//sys/pool_trees/default/@config/default_parent_pool", "default_pool")
        self.wait_pool_exists("default_pool")

        run_sleeping_vanilla(spec={"pool": "test_pool"})
        self.wait_pool_exists("test_pool")

        create_pool("test_pool")

        wait(lambda: self.get_pool_parent("test_pool") == "<Root>")
        ephemeral_pools = scheduler_orchid_path() + "/scheduler/scheduling_info_per_pool_tree/default/user_to_ephemeral_pools/root"

        wait(lambda: get(ephemeral_pools) == [])
        wait(lambda: not get(scheduler_orchid_pool_path("<Root>") + "/is_ephemeral", default=False))

    @authors("renadeen")
    def test_explicit_to_ephemeral_pool_transformation_with_user_default_parent_pool_map(self):
        create_user("u")
        create_pool("u")
        create_pool("default_parent_for_u")

        update_pool_tree_config("default", {"use_user_default_parent_pool_map": True})
        update_user_to_default_pool_map({"u": "default_parent_for_u"})

        op = run_sleeping_vanilla(authenticated_user="u")
        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/pool", default="") == "u")
        wait(lambda: self.get_pool_parent("u") == "<Root>")

        remove("//sys/pools/u")

        wait(lambda: self.get_pool_parent("u") == "default_parent_for_u")
        wait(lambda: get(scheduler_orchid_pool_path("u") + "/is_ephemeral", default=False))

    @authors("renadeen")
    def test_explicit_to_ephemeral_pool_transformation_with_ephemeral_subpool_config(self):
        create_pool("research", attributes={"ephemeral_subpool_config": {"max_operation_count": 3}})
        create_pool("ephemeral")
        update_pool_tree_config("default", {"default_parent_pool": "research"})

        op = run_sleeping_vanilla(spec={"pool": "ephemeral"})
        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/pool", default="") == "ephemeral")
        wait(lambda: self.get_pool_parent("ephemeral") == "<Root>")

        remove("//sys/pools/ephemeral")

        wait(lambda: self.get_pool_parent("ephemeral") == "research")
        wait(lambda: get(scheduler_orchid_pool_path("ephemeral") + "/is_ephemeral", default=False))

        wait(lambda: get(scheduler_orchid_pool_path("ephemeral") + "/max_operation_count", default=False) == 3)

    @authors("renadeen")
    def test_ephemeral_subpool_config_applies_to_existent_ephemeral_subpools(self):
        create_pool("research", attributes={"ephemeral_subpool_config": {"max_operation_count": 3}})
        create_pool("explicit", parent_name="research")
        update_pool_tree_config("default", {"default_parent_pool": "research"})

        op = run_sleeping_vanilla(spec={"pool": "ephemeral"})
        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/pool", default="") == "ephemeral")

        set("//sys/pools/research/@ephemeral_subpool_config", {"max_operation_count": 5})

        wait(lambda: get(scheduler_orchid_pool_path("ephemeral") + "/max_operation_count", default=False) == 5)
        wait(lambda: get(scheduler_orchid_pool_path("explicit") + "/max_operation_count", default=False) != 5)

    @authors("renadeen")
    def test_user_runs_operation_in_ephemeral_pool_under_root(self):
        create_user("u")

        op = run_sleeping_vanilla(authenticated_user="u")
        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/pool", default="") == "u")
        wait(lambda: self.get_pool_parent("u") == "<Root>")

        op2 = run_sleeping_vanilla(authenticated_user="u")
        wait(lambda: get(scheduler_orchid_operation_path(op2.id) + "/pool", default="") == "u")

    @authors("ignat")
    def test_ephemeral_pools_limit(self):
        create("table", "//tmp/t_in")
        set("//tmp/t_in/@replication_factor", 1)
        write_table("//tmp/t_in", {"foo": "bar"})

        for i in range(1, 5):
            output = "//tmp/t_out" + str(i)
            create("table", output)
            set(output + "/@replication_factor", 1)

        create_pool("default_pool")
        set("//sys/pool_trees/default/@config/default_parent_pool", "default_pool")
        set("//sys/pool_trees/default/@config/max_ephemeral_pools_per_user", 3)
        time.sleep(0.2)

        ops = []
        breakpoints = []
        for i in range(1, 4):
            breakpoint_name = "breakpoint{0}".format(i)
            breakpoints.append(breakpoint_name)
            ops.append(
                map(
                    track=False,
                    command="cat ; {breakpoint_cmd}".format(
                        breakpoint_cmd=events_on_fs().breakpoint_cmd(breakpoint_name)
                    ),
                    in_="//tmp/t_in",
                    out="//tmp/t_out" + str(i),
                    spec={"pool": "pool" + str(i)},
                )
            )
            wait_breakpoint(breakpoint_name)

        scheduling_info_per_pool_tree = "//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree"
        assert builtins.set(["pool" + str(i) for i in range(1, 4)]) == builtins.set(
            get(scheduling_info_per_pool_tree + "/default/user_to_ephemeral_pools/root")
        )

        with pytest.raises(YtError):
            map(
                track=False,
                command="cat",
                in_="//tmp/t_in",
                out="//tmp/t_out4",
                spec={"pool": "pool4"},
            )

        remove("//sys/pools/default_pool")
        remove("//sys/pool_trees/default/@config/default_parent_pool")
        remove("//sys/pool_trees/default/@config/max_ephemeral_pools_per_user")

        for breakpoint_name in breakpoints:
            release_breakpoint(breakpoint_name)

        for op in ops:
            op.track()

    @authors("eshcherbin")
    def test_move_last_operation_from_ephemeral_pool(self):
        create_pool("real")

        op = run_sleeping_vanilla(spec={"pool": "ephemeral"})
        wait(lambda: exists(scheduler_orchid_pool_path("ephemeral")))
        op.wait_for_state("running")

        update_op_parameters(op.id, parameters={"pool": "real"})
        wait(lambda: not exists(scheduler_orchid_pool_path("ephemeral")))

    @authors("renadeen")
    def test_move_to_ephemeral_hub_via_common_option(self):
        create_pool("ephemeral_hub", attributes={"create_ephemeral_subpools": True})
        op = run_sleeping_vanilla(spec={"pool": "inexistent"})
        wait(lambda: exists(scheduler_orchid_pool_path("inexistent")))

        update_op_parameters(op.id, parameters={"pool": "ephemeral_hub"})

        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/pool") == "ephemeral_hub$root")

    @authors("renadeen")
    def test_move_to_ephemeral_hub_via_per_pool_tree_option(self):
        create_pool("ephemeral_hub", attributes={"create_ephemeral_subpools": True})
        op = run_sleeping_vanilla(spec={"pool": "inexistent"})
        wait(lambda: exists(scheduler_orchid_pool_path("inexistent")))

        update_op_parameters(op.id, parameters={
            "scheduling_options_per_pool_tree": {
                "default": {"pool": "ephemeral_hub"}
            }})

        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/pool") == "ephemeral_hub$root")

    @authors("renadeen")
    def test_fifo_pool_cannot_create_ephemeral_subpools(self):
        create_pool("fifo_pool", attributes={"mode": "fifo"})
        with pytest.raises(YtError):
            set("//sys/pools/fifo_pool/@create_ephemeral_subpools", True)

        create_pool("ephemeral_hub", attributes={"create_ephemeral_subpools": True})
        with pytest.raises(YtError):
            set("//sys/pools/ephemeral_hub/@mode", "fifo")

    @authors("renadeen")
    def test_destroy_user_ephemeral_pools_on_config_change(self):
        create_pool("ephemeral_hub", attributes={"create_ephemeral_subpools": True})

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), spec={"pool": "ephemeral_hub"})
        wait_breakpoint()

        set("//sys/pools/ephemeral_hub/@create_ephemeral_subpools", False)
        set("//sys/pools/ephemeral_hub/@mode", "fifo")

        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/pool") == "ephemeral_hub")

        release_breakpoint()
        op.track()

    @authors("renadeen")
    def test_default_ephemeral_hub_applies_ephemeral_subpool_config(self):
        update_pool_tree_config("default", {"default_parent_pool": "research"})
        create_pool(
            "research",
            attributes={
                "ephemeral_subpool_config": {
                    "mode": "fifo",
                    "max_operation_count": 1,
                    "resource_limits": {"cpu": 1},
                },
            })

        op = run_sleeping_vanilla(spec={"pool": "ephemeral"})
        wait(lambda: len(list(op.get_running_jobs())) == 1)

        wait(lambda: exists(scheduler_orchid_default_pool_tree_path() + "/pools/ephemeral"))
        pool_info = get(scheduler_orchid_default_pool_tree_path() + "/pools/ephemeral")
        assert pool_info["parent"] == "research"
        assert pool_info["mode"] == "fifo"
        assert pool_info["resource_limits"]["cpu"] == 1.0
        assert pool_info["max_operation_count"] == 1

    @authors("renadeen")
    def test_per_user_default_ephemeral_hub_applies_ephemeral_subpool_config(self):
        create_user("u")
        create_pool(
            "default_for_u",
            attributes={
                "ephemeral_subpool_config": {
                    "mode": "fifo",
                    "max_operation_count": 1,
                    "resource_limits": {"cpu": 1},
                },
            })
        update_user_to_default_pool_map({"u": "default_for_u"})
        update_pool_tree_config("default", {"use_user_default_parent_pool_map": True})

        op = run_sleeping_vanilla(authenticated_user="u")
        wait(lambda: len(list(op.get_running_jobs())) == 1)

        wait(lambda: exists(scheduler_orchid_default_pool_tree_path() + "/pools/u"))
        pool_info = get(scheduler_orchid_default_pool_tree_path() + "/pools/u")
        assert pool_info["parent"] == "default_for_u"
        assert pool_info["mode"] == "fifo"
        assert pool_info["resource_limits"]["cpu"] == 1.0

    @authors("renadeen")
    def test_require_specified_pools_existence_flag(self):
        with raises_yt_error(yt_error_codes.Scheduler.OperationLaunchedInNonexistentPool):
            run_sleeping_vanilla(spec={"pool": "ephemeral", "require_specified_pools_existence": True})

        # It's OK to launch operation in unspecified ephemeral pool.
        run_test_vanilla(":", track=True, spec={"require_specified_pools_existence": True})

        # It's OK to launch operation in ephemeral pool under ephemeral hub.
        create_pool("ephemeral_hub", attributes={"create_ephemeral_subpools": True})
        run_test_vanilla(":", track=True, spec={"pool": "ephemeral_hub", "require_specified_pools_existence": True})

    @authors("renadeen")
    def test_require_specified_pools_existence_global_flag(self):
        update_scheduler_config("require_specified_operation_pools_existence", True)
        with raises_yt_error(yt_error_codes.Scheduler.OperationLaunchedInNonexistentPool):
            run_sleeping_vanilla(spec={"pool": "ephemeral"})

        # It's OK to launch operation in unspecified ephemeral pool.
        run_test_vanilla(":", track=True)

        # It's OK to launch operation in ephemeral pool under ephemeral hub.
        create_pool("ephemeral_hub", attributes={"create_ephemeral_subpools": True})
        run_test_vanilla(":", track=True, spec={"pool": "ephemeral_hub"})


@pytest.mark.enabled_multidaemon
class TestSchedulerPoolsCommon(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "fair_share_update_period": 300,
            "profiling_update_period": 300,
            "fair_share_profiling_period": 300,
            "event_log": {
                "enable": True,
                "flush_period": 300,
                "retry_backoff_time": 300,
            },
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "event_log": {
                "enable": True,
                "flush_period": 300,
                "retry_backoff_time": 300,
            },
        },
    }

    @authors("ignat")
    def test_pools_reconfiguration(self):
        create_test_tables(attributes={"replication_factor": 1})

        testing_options = {"schedule_job_delay": {"duration": 1000}}

        create_pool("test_pool_1")
        create_pool("test_pool_2")

        op = map(
            track=False,
            command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"pool": "test_pool_1", "testing": testing_options},
        )
        wait(lambda: op.get_state() == "running")

        remove("//sys/pools/test_pool_1")
        create_pool("test_pool_1", parent_name="test_pool_2", wait_for_orchid=False)

        op.track()

    @authors("renadeen", "babenko")
    def test_event_log(self):
        create_test_tables(attributes={"replication_factor": 1})

        create_pool("event_log_test_pool", attributes={"min_share_resources": {"cpu": 1}})
        op = map(
            command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"pool": "event_log_test_pool"},
        )

        @wait_no_assert
        def check_events():
            events = []
            for row in read_table("//sys/scheduler/event_log"):
                event_type = row["event_type"]
                if (
                    event_type.startswith("operation_")
                    and event_type != "operation_prepared"
                    and event_type != "operation_materialized"
                    and row["operation_id"] == op.id
                ):
                    events.append(row["event_type"])
                    if event_type == "operation_started":
                        assert row["pool"]
                    if event_type == "operation_completed":
                        assert row["progress"]["job_statistics"]
                        assert row["progress"]["job_statistics_v2"]
            assert events == ["operation_started", "operation_completed"]

        @wait_no_assert
        def check_pools():
            pools_info = [
                row
                for row in read_table("//sys/scheduler/event_log")
                if row["event_type"] == "pools_info" and "event_log_test_pool" in row["pools"]["default"]
            ]
            assert len(pools_info) == 1
            custom_pool_info = pools_info[-1]["pools"]["default"]["event_log_test_pool"]
            assert are_almost_equal(custom_pool_info["strong_guarantee_resources"]["cpu"], 1.0)
            assert custom_pool_info["mode"] == "fair_share"

    @authors("eshcherbin")
    def test_pool_count(self):
        def get_orchid_pool_count():
            return get_from_tree_orchid("default", "fair_share_info/pool_count")

        pool_count_sensor = profiler_factory().at_scheduler(fixed_tags={"tree": "default"}).gauge("scheduler/pools/pool_count")

        def check_pool_count(expected_pool_count):
            wait(lambda: get_orchid_pool_count() == expected_pool_count)
            wait(lambda: pool_count_sensor.get() == expected_pool_count)

        check_pool_count(0)

        create_pool("first_pool")
        create_pool("second_pool")
        check_pool_count(2)

        op = run_sleeping_vanilla(spec={"pool": "first_pool"})
        check_pool_count(2)
        op.abort()

        # Ephemeral pool.
        op = run_sleeping_vanilla()
        check_pool_count(3)
        op.abort()
        check_pool_count(2)

    @authors("renadeen")
    def test_ephemeral_pool_name_validation(self):
        with pytest.raises(YtError):
            run_sleeping_vanilla(spec={"pool": "invalid$name"})

        op = run_sleeping_vanilla(spec={"pool": "valid_name"})
        with pytest.raises(YtError):
            update_op_parameters(op.id, parameters={"pool": "invalid|name"})


@pytest.mark.enabled_multidaemon
class TestSchedulerPoolsReconfiguration(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,  # Update pools configuration period
        }
    }

    orchid_pools = scheduler_orchid_default_pool_tree_path() + "/pools"

    def setup_method(self, method):
        super(TestSchedulerPoolsReconfiguration, self).setup_method(method)
        wait(lambda: len(ls(self.orchid_pools)) == 1, sleep_backoff=0.1)  # <Root> is always in orchid
        wait(lambda: not get("//sys/scheduler/@alerts"), sleep_backoff=0.1)

    @authors("renadeen")
    def test_add_nested_pool(self):
        create_pool("test_parent")
        create_pool("test_pool", parent_name="test_parent")

        assert self.get_pool_parent("test_parent") == "<Root>"
        assert self.get_pool_parent("test_pool") == "test_parent"

    @authors("renadeen")
    def test_move_to_existing_pool(self):
        create_pool("test_parent")
        create_pool("test_pool")
        wait(lambda: self.get_pool_parent("test_pool") == "<Root>")

        move("//sys/pools/test_pool", "//sys/pools/test_parent/test_pool")
        wait(lambda: self.get_pool_parent("test_pool") == "test_parent")

    @authors("renadeen")
    def test_move_to_new_pool(self):
        create_pool("test_pool")

        # We'd like to execute these two commands atomically
        create_pool("new_pool", wair_for_orchid=False)
        move("//sys/pools/test_pool", "//sys/pools/new_pool/test_pool")

        self.wait_pool_exists("new_pool")
        wait(lambda: self.get_pool_parent("test_pool") == "new_pool")

    @authors("renadeen")
    def test_move_to_root_pool(self):
        create_pool("test_parent")
        create_pool("test_pool", parent_name="test_parent")
        wait(lambda: self.get_pool_parent("test_pool") == "test_parent")

        move("//sys/pools/test_parent/test_pool", "//sys/pools/test_pool")

        wait(lambda: self.get_pool_parent("test_pool") == "<Root>")

    @authors("renadeen")
    def test_remove_big_hierarchy(self):
        # Test bug when some pools weren't removed due to wrong removal order and inability to remove nonempty pools
        parent = None
        for i in range(10):
            pool = "pool" + str(i)
            create_pool(pool, parent_name=parent)
            parent = pool

        remove("//sys/pools/pool0")
        wait(lambda: len(ls(self.orchid_pools)) == 1)  # only <Root> must remain

    @authors("renadeen")
    def test_subtle_reconfiguration_crash(self):
        # 1. there are two pools - parent (max_running_operation_count=1) and child (max_running_operation_count=2)
        # 2. launch first operation in child - it runs
        # 3. launch second operation in child - it will wait at parent as parent reached its max_running_operation_count
        # 4. set child's max_running_operation_count to 0
        # 5. first operation completes
        # 6. scheduler wakes up second operation
        # 7. operation is stuck in child - the pool where operation just finished - scheduler crash here

        create_pool("parent", attributes={"max_running_operation_count": 1})
        create_pool("child", parent_name="parent", attributes={"max_running_operation_count": 2})
        running_op = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=1, spec={"pool": "child"})
        wait_breakpoint()
        waiting_op = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=1, spec={"pool": "child"})
        waiting_op.wait_for_state("pending")
        set("//sys/pools/parent/child/@max_running_operation_count", 0)
        release_breakpoint()
        running_op.track()

    def wait_pool_exists(self, pool):
        wait(lambda: exists(self.orchid_pools + "/" + pool), sleep_backoff=0.1)

    def get_pool_parent(self, pool):
        return get(self.orchid_pools + "/" + pool + "/parent")


##################################################################


@pytest.mark.enabled_multidaemon
class TestSchedulerSuspiciousJobs(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1
    USE_PORTO = True

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "job_proxy": {
                "job_proxy_heartbeat_period": 100,  # 100 msec
            },
        },
        "job_resource_manager": {"resource_limits": {"user_slots": 2, "cpu": 2}},
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "running_allocations_update_period": 100,  # 100 msec
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "suspicious_jobs": {
                "inactivity_timeout": 2000,  # 2 sec
                "update_period": 100,  # 100 msec
            },
        }
    }

    @authors("ignat")
    def test_false_suspicious_jobs(self):
        create("table", "//tmp/t", attributes={"replication_factor": 1})
        create("table", "//tmp/t1", attributes={"replication_factor": 1})
        create("table", "//tmp/t2", attributes={"replication_factor": 1})
        write_table("//tmp/t", [{"foo": i} for i in range(10)])

        # Jobs below are not suspicious, they are just stupid.
        op1 = map(
            track=False,
            command='echo -ne "x = 1\nwhile True:\n    x = (x * x + 1) % 424243" | python3',
            in_="//tmp/t",
            out="//tmp/t1",
        )

        op2 = map(track=False, command="sleep 1000", in_="//tmp/t", out="//tmp/t2")

        for i in range(200):
            running_jobs1 = op1.get_running_jobs()
            running_jobs2 = op2.get_running_jobs()
            print_debug(
                "running_jobs1:",
                len(running_jobs1),
                "running_jobs2:",
                len(running_jobs2),
            )
            if not running_jobs1 or not running_jobs2:
                time.sleep(0.1)
            else:
                break

        if not running_jobs1 or not running_jobs2:
            assert False, "Failed to have running jobs in both operations"

        time.sleep(5)

        job1_id = next(iter(running_jobs1.keys()))
        job2_id = next(iter(running_jobs2.keys()))

        time.sleep(1)

        suspicious1 = get(op1.get_path() + "/controller_orchid/running_jobs/{}/suspicious".format(job1_id))
        suspicious2 = get(op2.get_path() + "/controller_orchid/running_jobs/{}/suspicious".format(job2_id))

        if suspicious1 or suspicious2:
            print_debug("Some of jobs considered suspicious, their brief statistics are:")
            for i in range(50):
                if suspicious1 and exists(op1.get_path() + "/controller_orchid/running_jobs/{}/brief_statistics".format(job1_id)):
                    print_debug(
                        "job1 brief statistics:",
                        get(op1.get_path() + "/controller_orchid/running_jobs/{}/brief_statistics".format(job1_id)),
                    )
                if suspicious2 and exists(op2.get_path() + "/controller_orchid/running_jobs/{}/brief_statistics".format(job2_id)):
                    print_debug(
                        "job2 brief statistics:",
                        get(op2.get_path() + "/controller_orchid/running_jobs/{}/brief_statistics".format(job2_id)),
                    )

        assert not suspicious1
        assert not suspicious2

        op1.abort()
        op2.abort()

    @authors("max42")
    def test_true_suspicious_jobs(self):
        create("table", "//tmp/t_in", attributes={"replication_factor": 1})
        create("table", "//tmp/t_out", attributes={"replication_factor": 1})

        for i in range(15):
            write_table("<append=%true>//tmp/t_in", {"a": i})
        op = merge(
            track=False,
            in_=["//tmp/t_in"],
            out="//tmp/t_out",
            spec={
                "force_transform": True,
                "mode": "ordered",
                "job_io": {
                    "testing_options": {"pipe_delay": 4000},
                    "buffer_row_count": 1,
                },
                "enable_job_splitting": False,
            },
        )

        running_jobs = None
        for i in range(200):
            running_jobs = op.get_running_jobs()
            print_debug("Running jobs:", len(running_jobs))
            if not running_jobs:
                time.sleep(0.1)
            else:
                break

        if not running_jobs:
            assert False, "Failed to have running jobs"

        time.sleep(5)

        job_id = next(iter(running_jobs.keys()))

        # Most part of the time we should be suspicious, let's check that
        for i in range(30):
            suspicious = get(op.get_path() + "/controller_orchid/running_jobs/{}/suspicious".format(job_id))
            if suspicious:
                break
            time.sleep(0.1)

        if not suspicious:
            print_debug("Job is not considered suspicious, its brief statistics are:")
            for i in range(50):
                if exists(op.get_path() + "/controller_orchid/running_jobs/{}/brief_statistics".format(job_id)):
                    print_debug(
                        "job brief statistics:",
                        get(op.get_path() + "/controller_orchid/running_jobs/{}/brief_statistics".format(job_id)),
                    )

        assert suspicious

    @authors("max42")
    def DISABLED_test_true_suspicious_jobs_old(self):
        # This test involves dirty hack to make lots of retries for fetching feasible
        # seeds from master making the job suspicious (as it doesn't give the input for the
        # user job for a long time).
        #
        # We create a table consisting of the only chunk, temporarily set cpu = 0 to prevent
        # the map from running via @resource_limits_overrides, then we remove the chunk from
        # the chunk_store via the filesystem and return CPU back to the normal state.

        create("table", "//tmp/t", attributes={"replication_factor": 1})
        create("table", "//tmp/d", attributes={"replication_factor": 1})
        write_table("//tmp/t", {"a": 2})

        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 1
        node = nodes[0]
        set(
            "//sys/cluster_nodes/{0}/@resource_limits_overrides".format(node),
            {"cpu": 0},
        )

        op = map(track=False, command="cat", in_="//tmp/t", out="//tmp/d")

        chunk_id = get_singular_chunk_id("//tmp/t")

        chunk_store_path = self.Env.configs["node"][0]["data_node"]["store_locations"][0]["path"]
        chunk_path = os.path.join(chunk_store_path, chunk_id[-2:], chunk_id)
        os.remove(chunk_path)
        os.remove(chunk_path + ".meta")

        set(
            "//sys/cluster_nodes/{0}/@resource_limits_overrides".format(node),
            {"cpu": 1},
        )

        while True:
            if exists("//sys/scheduler/orchid/scheduler/operations/{0}".format(op.id)):
                running_jobs = op.get_running_jobs()
                if len(running_jobs) > 0:
                    break

            time.sleep(1.0)

        assert len(running_jobs) == 1
        job_id = next(iter(running_jobs.keys()))

        for i in range(20):
            suspicious = get(op.get_path() + "/controller_orchid/running_jobs/{}/suspicious".format(job_id))
            if not suspicious:
                time.sleep(1.0)

            if exists(op.get_path() + "/controller_orchid/running_jobs/{}/brief_statistics".format(job_id)):
                print_debug(get(op.get_path() + "/controller_orchid/running_jobs/{}/brief_statistics".format(job_id)))

        assert suspicious


##################################################################


@pytest.mark.enabled_multidaemon
class TestMinNeededResources(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "min_needed_resources_update_period": 200
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "safe_scheduler_online_time": 500,
        }
    }

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "memory": 10 * 1024 * 1024 * 1024,
                "cpu": 3,
                "user_slots": 2,
            }
        },
        "resource_limits": {"memory": 20 * 1024 * 1024 * 1024},
    }

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "cypress_manager": {
            "default_table_replication_factor": 1
        }
    }

    @authors("ignat")
    def test_min_needed_resources(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"x": 1}])
        create("table", "//tmp/t_out")

        op1 = map(
            command="sleep 100; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "mapper": {
                    "memory_limit": 8 * 1024 * 1024 * 1024,
                    "memory_reserve_factor": 1.0,
                    "cpu_limit": 2,
                }
            },
            track=False,
        )

        op1_path = "//sys/scheduler/orchid/scheduler/operations/" + op1.id
        wait(lambda: exists(op1_path) and get(op1_path + "/state") == "running")

        wait(lambda: get(op1.get_path() + "/controller_orchid/progress/schedule_job_statistics/count") > 0)

        create("table", "//tmp/t2_in")
        write_table("//tmp/t2_in", [{"x": 1}])
        create("table", "//tmp/t2_out")

        op2 = map(
            command="cat",
            in_="//tmp/t2_in",
            out="//tmp/t2_out",
            spec={
                "mapper": {
                    "memory_limit": 3 * 1024 * 1024 * 1024,
                    "memory_reserve_factor": 1.0,
                    "cpu_limit": 2,
                }
            },
            track=False,
        )

        op2_path = "//sys/scheduler/orchid/scheduler/operations/" + op2.id
        wait(lambda: exists(op2_path) and get(op2_path + "/state") == "running")

        time.sleep(3.0)
        assert get(op2.get_path() + "/controller_orchid/progress/schedule_job_statistics/count") == 0

        abort_op(op1.id)

        op2.track()

    @authors("eshcherbin")
    def test_min_needed_resources_unsatisfied_count(self):
        op = run_sleeping_vanilla(task_patch={"cpu_limit": 2}, job_count=2)
        wait(
            lambda: get(
                scheduler_orchid_operation_path(op.id) + "/min_needed_resources_unsatisfied_count/cpu",
                default=0,
            )
            >= 10
        )


##################################################################


@pytest.mark.enabled_multidaemon
class TestSchedulerInferChildrenWeightsFromHistoricUsage(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_CPUS_PER_NODE = 10
    NUM_SLOTS_PER_NODE = 10

    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 100,
            "fair_share_profiling_period": 100,
        }
    }

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "cpu": NUM_CPUS_PER_NODE,
                "user_slots": NUM_SLOTS_PER_NODE,
            }
        }
    }

    def setup_method(self, method):
        super(TestSchedulerInferChildrenWeightsFromHistoricUsage, self).setup_method(method)
        create_pool("parent")
        set(
            "//sys/pools/parent/@historic_usage_aggregation_period",
            1000,  # 1 sec
        )
        time.sleep(0.5)

    def _init_children(self, num_children=2):
        for i in range(num_children):
            create_pool("child" + str(i + 1), parent_name="parent")

    def _get_pool_fair_share_ratio(self, pool):
        try:
            return get(scheduler_orchid_pool_path(pool) + "/dominant_fair_share/total")
        except YtError:
            return 0.0

    def _get_pool_usage_ratio(self, pool):
        try:
            return get(scheduler_orchid_pool_path(pool) + "/dominant_usage_share")
        except YtError:
            return 0.0

    def _test_more_fair_share_for_new_operation_base(self, num_jobs_op1, num_jobs_op2):
        self._init_children()

        op1_tasks_spec = {"task": {"job_count": num_jobs_op1, "command": "sleep 100;"}}
        op1 = vanilla(spec={"pool": "child1", "tasks": op1_tasks_spec}, track=False)

        wait(
            lambda: are_almost_equal(
                self._get_pool_usage_ratio("child1"),
                min(num_jobs_op1 / self.NUM_SLOTS_PER_NODE, 1.0),
            )
        )

        # give some time for historic usage to accumulate
        time.sleep(2)

        op2_tasks_spec = {"task": {"job_count": num_jobs_op2, "command": "sleep 100;"}}

        dominant_fair_share_sensor = profiler_factory().at_scheduler(fixed_tags={"tree": "default", "pool": "child2"})\
            .gauge("scheduler/pools/dominant_fair_share/total")

        op2 = vanilla(spec={"pool": "child2", "tasks": op2_tasks_spec}, track=False)

        # it's hard to estimate historic usage for all children, because run time can vary and jobs
        # can spuriously abort and restart; so we don't set the threshold any greater than 0.5
        wait(lambda: dominant_fair_share_sensor.get() is not None, iter=300, sleep_backoff=0.1)
        wait(lambda: dominant_fair_share_sensor.get() > 0.5, iter=300, sleep_backoff=0.1)

        op1.complete()
        op2.complete()

    @authors("eshcherbin")
    def test_more_fair_share_for_new_operation_equal_demand(self):
        self._test_more_fair_share_for_new_operation_base(10, 10)

    @authors("eshcherbin")
    def test_more_fair_share_for_new_operation_bigger_demand(self):
        self._test_more_fair_share_for_new_operation_base(5, 10)

    @authors("eshcherbin")
    def test_more_fair_share_for_new_operation_smaller_demand(self):
        self._test_more_fair_share_for_new_operation_base(10, 6)

    # NB(eshcherbin): this test works only if new config effectively disables historic usage aggregation
    def _test_equal_fair_share_after_disabling_config_change_base(self, new_config):
        self._init_children()

        op1_tasks_spec = {"task": {"job_count": self.NUM_SLOTS_PER_NODE, "command": "sleep 100;"}}
        op1 = vanilla(spec={"pool": "child1", "tasks": op1_tasks_spec}, track=False)

        wait(lambda: are_almost_equal(self._get_pool_usage_ratio("child1"), 1.0))

        # give some time for historic usage to accumulate
        time.sleep(2)

        # change config and wait for it to be applied
        set(
            "//sys/pools/parent/@historic_usage_aggregation_period",
            new_config["historic_usage_aggregation_period"],
        )
        wait(lambda: get("//sys/pools/parent/@historic_usage_aggregation_period") == new_config["historic_usage_aggregation_period"])
        time.sleep(0.5)

        op2_tasks_spec = {"task": {"job_count": self.NUM_SLOTS_PER_NODE, "command": "sleep 100;"}}

        dominant_fair_share_sensor = profiler_factory().at_scheduler(fixed_tags={"tree": "default", "pool": "child2"}) \
            .gauge("scheduler/pools/dominant_fair_share/total")

        op2 = vanilla(spec={"pool": "child2", "tasks": op2_tasks_spec}, track=False)

        wait(lambda: dominant_fair_share_sensor.get() is not None, iter=300, sleep_backoff=0.1)
        wait(lambda: are_almost_equal(dominant_fair_share_sensor.get(), 0.5), iter=300, sleep_backoff=0.1)

        op1.complete()
        op2.complete()

    @authors("eshcherbin")
    def test_equal_fair_share_after_disabling_historic_usage(
        self,
    ):
        self._test_equal_fair_share_after_disabling_config_change_base(
            {
                "historic_usage_aggregation_period": 1000,  # 1 sec
            }
        )


##################################################################


@authors("renadeen")
class TestIntegralGuarantees(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    FAIR_SHARE_UPDATE_PERIOD = 500

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,  # Update pools configuration period
            "fair_share_update_period": FAIR_SHARE_UPDATE_PERIOD
        }
    }

    DELTA_NODE_CONFIG = {"job_resource_manager": {"resource_limits": {"cpu": 10, "user_slots": 10}}}

    def wait_pool_fair_share(self, pool, strong, integral, weight_proportional):
        path = scheduler_orchid_default_pool_tree_path() + "/pools/" + pool + "/detailed_fair_share"
        wait(lambda: exists(path))

        @wait_no_assert
        def check_pool_fair_share():
            fair_share = get(path, default=-1)
            assert are_almost_equal(fair_share["strong_guarantee"]["cpu"], strong)
            assert are_almost_equal(fair_share["integral_guarantee"]["cpu"], integral)
            assert are_almost_equal(fair_share["weight_proportional"]["cpu"], weight_proportional)

    def setup_method(self, method):
        super(TestIntegralGuarantees, self).setup_method(method)
        set("//sys/pool_trees/default/@config/integral_guarantees", {"smooth_period": self.FAIR_SHARE_UPDATE_PERIOD})

    def test_simple_burst_integral_guarantee(self):
        create_pool(
            "burst_pool",
            attributes={
                "min_share_resources": {"cpu": 2},
                "integral_guarantees": {
                    "guarantee_type": "burst",
                    "resource_flow": {"cpu": 3},
                    "burst_guarantee_resources": {"cpu": 8},
                },
            },
        )

        run_sleeping_vanilla(job_count=10, spec={"pool": "burst_pool"})
        self.wait_pool_fair_share("burst_pool", strong=0.2, integral=0.3, weight_proportional=0.5)

    def test_simple_relaxed_integral_guarantee(self):
        create_pool(
            "relaxed_pool",
            attributes={
                "min_share_resources": {"cpu": 2},
                "integral_guarantees": {
                    "guarantee_type": "relaxed",
                    "resource_flow": {"cpu": 3},
                },
            },
        )

        run_sleeping_vanilla(job_count=10, spec={"pool": "relaxed_pool"})
        self.wait_pool_fair_share("relaxed_pool", strong=0.2, integral=0.3, weight_proportional=0.5)

    def test_min_share_vs_burst(self):
        create_pool(
            "min_share_pool",
            attributes={
                "min_share_resources": {"cpu": 5},
            },
        )

        create_pool(
            "burst_pool",
            attributes={
                "integral_guarantees": {
                    "guarantee_type": "burst",
                    "resource_flow": {"cpu": 5},
                    "burst_guarantee_resources": {"cpu": 5},
                }
            },
        )

        run_sleeping_vanilla(job_count=10, spec={"pool": "min_share_pool"})
        run_sleeping_vanilla(job_count=10, spec={"pool": "burst_pool"})

        self.wait_pool_fair_share("min_share_pool", strong=0.5, integral=0.0, weight_proportional=0.0)
        self.wait_pool_fair_share("burst_pool", strong=0.0, integral=0.5, weight_proportional=0.0)
        self.wait_pool_fair_share("<Root>", strong=0.5, integral=0.5, weight_proportional=0.0)

    def test_min_share_vs_relaxed(self):
        create_pool(
            "min_share_pool",
            attributes={
                "min_share_resources": {"cpu": 5},
            },
        )

        create_pool(
            "relaxed_pool",
            attributes={
                "integral_guarantees": {
                    "guarantee_type": "relaxed",
                    "resource_flow": {"cpu": 5},
                }
            },
        )

        run_sleeping_vanilla(job_count=10, spec={"pool": "min_share_pool"})
        run_sleeping_vanilla(job_count=10, spec={"pool": "relaxed_pool"})

        self.wait_pool_fair_share("min_share_pool", strong=0.5, integral=0.0, weight_proportional=0.0)
        self.wait_pool_fair_share("relaxed_pool", strong=0.0, integral=0.5, weight_proportional=0.0)
        self.wait_pool_fair_share("<Root>", strong=0.5, integral=0.5, weight_proportional=0.0)

    def test_relaxed_vs_empty_min_share(self):
        create_pool(
            "strong_guarantee_pool",
            attributes={
                "min_share_resources": {"cpu": 5},
            },
        )

        create_pool(
            "relaxed_pool",
            attributes={
                "integral_guarantees": {
                    "guarantee_type": "relaxed",
                    "resource_flow": {"cpu": 10},
                }
            },
        )

        run_sleeping_vanilla(job_count=10, spec={"pool": "relaxed_pool"})

        self.wait_pool_fair_share("strong_guarantee_pool", strong=0.0, integral=0.0, weight_proportional=0.0)
        self.wait_pool_fair_share("relaxed_pool", strong=0.0, integral=1.0, weight_proportional=0.0)
        self.wait_pool_fair_share("<Root>", strong=0.0, integral=1.0, weight_proportional=0.0)

    def test_relaxed_vs_no_guarantee(self):
        create_pool(
            "relaxed_pool",
            attributes={
                "integral_guarantees": {
                    "guarantee_type": "relaxed",
                    "resource_flow": {"cpu": 10},
                }
            },
        )

        create_pool("no_guarantee_pool")

        run_sleeping_vanilla(job_count=10, spec={"pool": "relaxed_pool"})
        run_sleeping_vanilla(job_count=10, spec={"pool": "no_guarantee_pool"})

        self.wait_pool_fair_share("relaxed_pool", strong=0.0, integral=1.0, weight_proportional=0.0)
        self.wait_pool_fair_share("no_guarantee_pool", strong=0.0, integral=0.0, weight_proportional=0.0)
        self.wait_pool_fair_share("<Root>", strong=0.0, integral=1.0, weight_proportional=0.0)

    def test_burst_gets_all_relaxed_none(self):
        create_pool(
            "burst_pool",
            attributes={
                "integral_guarantees": {
                    "guarantee_type": "burst",
                    "resource_flow": {"cpu": 10},
                    "burst_guarantee_resources": {"cpu": 10},
                }
            },
        )

        create_pool(
            "relaxed_pool",
            attributes={
                "integral_guarantees": {
                    "guarantee_type": "relaxed",
                    "resource_flow": {"cpu": 5},
                }
            },
        )

        run_sleeping_vanilla(job_count=10, spec={"pool": "burst_pool"})
        run_sleeping_vanilla(job_count=10, spec={"pool": "relaxed_pool"})

        self.wait_pool_fair_share("burst_pool", strong=0.0, integral=1.0, weight_proportional=0.0)
        self.wait_pool_fair_share("relaxed_pool", strong=0.0, integral=0.0, weight_proportional=0.0)
        self.wait_pool_fair_share("<Root>", strong=0.0, integral=1.0, weight_proportional=0.0)

    def test_burst_gets_its_guarantee_relaxed_gets_remaining(self):
        create_pool(
            "burst_pool",
            attributes={
                "integral_guarantees": {
                    "guarantee_type": "burst",
                    "resource_flow": {"cpu": 6},
                    "burst_guarantee_resources": {"cpu": 6},
                }
            },
        )

        create_pool(
            "relaxed_pool",
            attributes={
                "integral_guarantees": {
                    "guarantee_type": "relaxed",
                    "resource_flow": {"cpu": 10},
                }
            },
        )

        run_sleeping_vanilla(job_count=10, spec={"pool": "burst_pool"})
        run_sleeping_vanilla(job_count=10, spec={"pool": "relaxed_pool"})

        self.wait_pool_fair_share("burst_pool", strong=0.0, integral=0.6, weight_proportional=0.0)
        self.wait_pool_fair_share("relaxed_pool", strong=0.0, integral=0.4, weight_proportional=0.0)
        self.wait_pool_fair_share("<Root>", strong=0.0, integral=1.0, weight_proportional=0.0)

    def test_all_kinds_of_pools_weight_proportional_distribution(self):
        create_pool(
            "min_share_pool",
            attributes={"min_share_resources": {"cpu": 1}, "weight": 3},
        )

        create_pool(
            "burst_pool",
            attributes={
                "integral_guarantees": {
                    "guarantee_type": "burst",
                    "resource_flow": {"cpu": 1},
                    "burst_guarantee_resources": {"cpu": 1},
                },
                "weight": 1,
            },
        )

        create_pool(
            "relaxed_pool",
            attributes={
                "integral_guarantees": {
                    "guarantee_type": "relaxed",
                    "resource_flow": {"cpu": 1},
                },
                "weight": 1,
            },
        )

        create_pool("no_guarantee_pool", attributes={"weight": 2})

        run_sleeping_vanilla(job_count=10, spec={"pool": "min_share_pool"})
        run_sleeping_vanilla(job_count=10, spec={"pool": "burst_pool"})
        run_sleeping_vanilla(job_count=10, spec={"pool": "relaxed_pool"})
        run_sleeping_vanilla(job_count=10, spec={"pool": "no_guarantee_pool"})

        self.wait_pool_fair_share("min_share_pool", strong=0.1, integral=0.0, weight_proportional=0.3)
        self.wait_pool_fair_share("burst_pool", strong=0.0, integral=0.1, weight_proportional=0.1)
        self.wait_pool_fair_share("relaxed_pool", strong=0.0, integral=0.1, weight_proportional=0.1)
        self.wait_pool_fair_share("no_guarantee_pool", strong=0.0, integral=0.0, weight_proportional=0.2)
        self.wait_pool_fair_share("<Root>", strong=0.1, integral=0.2, weight_proportional=0.7)

    def test_min_share_and_burst_guarantees_adjustment(self):
        create_pool(
            "min_share_pool",
            attributes={
                "min_share_resources": {"cpu": 6},
            },
        )

        create_pool(
            "burst_pool",
            attributes={
                "integral_guarantees": {
                    "guarantee_type": "burst",
                    "resource_flow": {"cpu": 9},
                    "burst_guarantee_resources": {"cpu": 9},
                }
            },
        )

        run_sleeping_vanilla(job_count=10, spec={"pool": "min_share_pool"})
        run_sleeping_vanilla(job_count=10, spec={"pool": "burst_pool"})

        self.wait_pool_fair_share("min_share_pool", strong=0.4, integral=0.0, weight_proportional=0.0)
        self.wait_pool_fair_share("burst_pool", strong=0.0, integral=0.6, weight_proportional=0.0)
        self.wait_pool_fair_share("<Root>", strong=0.4, integral=0.6, weight_proportional=0.0)

    def test_accumulated_resource_ratio_volume_consumption(self):
        create_pool(
            "test_pool",
            attributes={
                "integral_guarantees": {
                    "guarantee_type": "relaxed",
                    "resource_flow": {"cpu": 5},
                }
            },
        )
        pool_path = scheduler_orchid_default_pool_tree_path() + "/pools/test_pool"
        wait(lambda: exists(pool_path))

        # No operations -> volume is accumulating
        wait(lambda: get(pool_path + "/accumulated_resource_ratio_volume") > 1)
        run_sleeping_vanilla(job_count=10, spec={"pool": "test_pool"})

        # Minimum volume is 0.25 = 0.5 (period) * 0.5 (flow_ratio)
        wait(lambda: get(pool_path + "/accumulated_resource_ratio_volume") < 0.3)

    def test_accumulated_resource_ratio_volume_survives_restart(self):
        create_pool(
            "test_pool",
            attributes={
                "integral_guarantees": {
                    "guarantee_type": "relaxed",
                    "resource_flow": {"cpu": 10},
                }
            },
        )

        wait(lambda: exists(scheduler_orchid_pool_path("test_pool")))
        wait(lambda: get(scheduler_orchid_pool_path("test_pool") + "/accumulated_resource_volume/cpu") > 30)

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        wait(lambda: exists(scheduler_orchid_pool_path("test_pool")), sleep_backoff=0.1)
        # Wait for total resource limits to appear.
        wait(
            lambda: get(scheduler_orchid_pool_path("<Root>") + "/resource_limits/cpu") > 0,
            sleep_backoff=0.1,
        )

        volume = get(scheduler_orchid_pool_path("test_pool") + "/accumulated_resource_volume/cpu")
        assert 30 < volume < 50

    def test_integral_pools_orchid(self):
        create_pool(
            "burst_pool",
            attributes={
                "min_share_resources": {"cpu": 2},
                "integral_guarantees": {
                    "guarantee_type": "burst",
                    "resource_flow": {"cpu": 3},
                    "burst_guarantee_resources": {"cpu": 7},
                },
            },
        )

        create_pool(
            "relaxed_pool",
            attributes={
                "integral_guarantees": {
                    "guarantee_type": "relaxed",
                    "resource_flow": {"cpu": 5},
                }
            },
        )

        root_pool_path = scheduler_orchid_default_pool_tree_path() + "/pools/<Root>"
        burst_pool_path = scheduler_orchid_default_pool_tree_path() + "/pools/burst_pool"
        relaxed_pool_path = scheduler_orchid_default_pool_tree_path() + "/pools/relaxed_pool"
        wait(lambda: exists(relaxed_pool_path))

        assert get(burst_pool_path + "/integral_guarantee_type") == "burst"
        assert get(burst_pool_path + "/specified_burst_ratio") == 0.7
        assert get(burst_pool_path + "/specified_resource_flow_ratio") == 0.3
        assert get(burst_pool_path + "/integral_pool_capacity/cpu") == 3.0 * 86400

        assert get(relaxed_pool_path + "/integral_guarantee_type") == "relaxed"
        assert get(relaxed_pool_path + "/specified_resource_flow_ratio") == 0.5
        assert get(relaxed_pool_path + "/integral_pool_capacity/cpu") == 5.0 * 86400

        assert get(root_pool_path + "/total_resource_flow_ratio") == 0.8
        assert get(root_pool_path + "/total_burst_ratio") == 0.7

    def test_integral_pools_with_parent(self):
        create_pool(
            "limited_parent",
            attributes={
                "integral_guarantees": {
                    "guarantee_type": "none",
                    "resource_flow": {"cpu": 10},
                    "burst_guarantee_resources": {"cpu": 10},
                }
            },
        )

        create_pool(
            "burst_pool",
            parent_name="limited_parent",
            attributes={
                "integral_guarantees": {
                    "guarantee_type": "burst",
                    "resource_flow": {"cpu": 5},
                    "burst_guarantee_resources": {"cpu": 10},
                }
            },
        )

        create_pool(
            "relaxed_pool",
            parent_name="limited_parent",
            attributes={
                "integral_guarantees": {
                    "guarantee_type": "relaxed",
                    "resource_flow": {"cpu": 5},
                }
            },
        )

        run_sleeping_vanilla(job_count=10, spec={"pool": "burst_pool"})
        run_sleeping_vanilla(job_count=10, spec={"pool": "relaxed_pool"})

        self.wait_pool_fair_share("burst_pool", strong=0.0, integral=0.5, weight_proportional=0.0)
        self.wait_pool_fair_share("relaxed_pool", strong=0.0, integral=0.5, weight_proportional=0.0)
        self.wait_pool_fair_share("limited_parent", strong=0.0, integral=1.0, weight_proportional=0.0)
        self.wait_pool_fair_share("<Root>", strong=0.0, integral=1.0, weight_proportional=0.0)

    def test_burst_and_flow_ratio_orchid(self):
        create_pool(
            "parent",
            attributes={
                "integral_guarantees": {
                    "resource_flow": {"cpu": 8},
                    "burst_guarantee_resources": {"cpu": 8}
                }
            })
        create_pool(
            "child",
            parent_name="parent",
            attributes={
                "integral_guarantees": {
                    "guarantee_type": "burst",
                    "resource_flow": {"cpu": 3},
                    "burst_guarantee_resources": {"cpu": 5},
                },
            },
        )

        orchid_prefix = scheduler_orchid_default_pool_tree_path() + "/pools/"
        wait(lambda: exists(orchid_prefix + "child"))

        wait(lambda: get(orchid_prefix + "parent/total_burst_ratio") == 0.5)
        wait(lambda: get(orchid_prefix + "parent/total_resource_flow_ratio") == 0.3)
        wait(lambda: get(orchid_prefix + "child/total_burst_ratio") == 0.5)
        wait(lambda: get(orchid_prefix + "child/total_resource_flow_ratio") == 0.3)

    def test_volume_overflow_distribution_simple(self):
        set("//sys/pool_trees/default/@config", {"should_distribute_free_volume_among_children": True})
        create_pool("ancestor", attributes={
            "integral_guarantees": {
                "guarantee_type": "none",
                "resource_flow": {"cpu": 10**10 + 1},
            },
        })
        create_pool("overflow_pool", parent_name="ancestor", attributes={
            "integral_guarantees": {
                "guarantee_type": "relaxed",
                "resource_flow": {"cpu": 10**10},
            },
        })
        create_pool("acceptable_pool", parent_name="ancestor", attributes={
            "integral_guarantees": {
                "guarantee_type": "relaxed",
                "resource_flow": {"cpu": 1},
            },
        })

        time.sleep(1)  # Wait a bit to accumulate some volume in overflow_pool.

        set("//sys/pools/ancestor/overflow_pool/@integral_guarantees/resource_flow/cpu", 10**5)
        # With 10**5 cpu flow overflow_pool is already full and will fill sibling acceptable_pool in one second.

        volume_path = scheduler_orchid_pool_path("acceptable_pool") + "/accumulated_resource_volume/cpu"
        wait(lambda: get(volume_path) == 86400.)

    def test_volume_overflow_distribution_with_deep_paths(self):
        set("//sys/pool_trees/default/@config", {"should_distribute_free_volume_among_children": True})
        create_pool("ancestor", attributes={
            "integral_guarantees": {
                "guarantee_type": "none",
                "resource_flow": {"cpu": 10**10 + 1},
            },
        })
        create_pool("overflow_ancestor", parent_name="ancestor", attributes={
            "integral_guarantees": {
                "guarantee_type": "none",
                "resource_flow": {"cpu": 10**10},
            },
        })
        create_pool("overflow_pool", parent_name="overflow_ancestor", attributes={
            "integral_guarantees": {
                "guarantee_type": "relaxed",
                "resource_flow": {"cpu": 10**10},
            },
        })
        create_pool("acceptable_ancestor", parent_name="ancestor", attributes={
            "integral_guarantees": {
                "guarantee_type": "none",
                "resource_flow": {"cpu": 1},
            },
        })
        create_pool("acceptable_pool", parent_name="acceptable_ancestor", attributes={
            "integral_guarantees": {
                "guarantee_type": "relaxed",
                "resource_flow": {"cpu": 1},
            },
        })

        time.sleep(1)  # Wait a bit to accumulate some volume in overflow_pool.

        set("//sys/pools/ancestor/overflow_ancestor/overflow_pool/@integral_guarantees/resource_flow/cpu", 10**5)
        # With 10**5 cpu flow overflow_pool is already full and will fill sibling acceptable_pool in one second.

        volume_path = scheduler_orchid_pool_path("acceptable_pool") + "/accumulated_resource_volume/cpu"
        wait(lambda: get(volume_path) == 86400.)

    def test_volume_overflow_distribution_if_pool_does_not_accept_it(self):
        set("//sys/pool_trees/default/@config", {"should_distribute_free_volume_among_children": True})

        create_pool("ancestor", attributes={
            "integral_guarantees": {
                "guarantee_type": "none",
                "resource_flow": {"cpu": 10**10 + 1},
            },
        })
        create_pool("overflow_pool", parent_name="ancestor", attributes={
            "integral_guarantees": {
                "guarantee_type": "relaxed",
                "resource_flow": {"cpu": 10**10},
            },
        })
        create_pool("not_acceptable_pool", parent_name="ancestor", attributes={
            "integral_guarantees": {
                "guarantee_type": "relaxed",
                "resource_flow": {"cpu": 1},
                "can_accept_free_volume": False,
            },
        })

        time.sleep(1)  # Wait a bit to accumulate some volume in overflow_pool.

        set("//sys/pools/ancestor/overflow_pool/@integral_guarantees/resource_flow/cpu", 10**5)
        # With 10**5 cpu flow overflow_pool is full.

        time.sleep(2)
        volume_path = scheduler_orchid_pool_path("not_acceptable_pool") + "/accumulated_resource_volume/cpu"
        assert get(volume_path) < 30.

    def test_zero_relaxed_guarantees_and_scheduler_doesnt_crash(self):
        create_pool(
            "relaxed_pool",
            attributes={
                "integral_guarantees": {
                    "guarantee_type": "relaxed",
                    "resource_flow": {"cpu": 0},
                },
            },
        )

        run_test_vanilla(command=with_breakpoint("BREAKPOINT"), job_count=3, spec={"pool": "relaxed_pool"})
        self.wait_pool_fair_share("relaxed_pool", strong=0, integral=0, weight_proportional=0.3)
        release_breakpoint()


##################################################################


@authors("renadeen")
@pytest.mark.enabled_multidaemon
class TestCrashDuringDistributingFreeVolumeAfterRemovingAllResourceFlow(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    # Scenario:
    # 1. there are burst pool with running operation and relaxed pool which supplies free volume for the first
    # 2. user removes resource flow from burst pool
    # 3. burst pool's capacity become zero but old volume is still there
    # 4. accumulated volume is being consumed by operation
    # 5. due to bug burst pool declares that it can accept more volume
    # 6. free volume distribution algorithm is not prepared for pool with zero resource flow and with non-zero acceptable volume and crashes

    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    FAIR_SHARE_UPDATE_PERIOD = 500

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,  # Update pools configuration period
            "fair_share_update_period": FAIR_SHARE_UPDATE_PERIOD
        }
    }

    def test_crash_during_distributing_free_volume_after_removing_resource_flow(self):
        set("//sys/pool_trees/default/@config", {
            "should_distribute_free_volume_among_children": True,
            "integral_guarantees": {"smooth_period": self.FAIR_SHARE_UPDATE_PERIOD}
            })
        create_pool("ancestor", attributes={
            "integral_guarantees": {
                "guarantee_type": "none",
                "resource_flow": {"cpu": 1000},
                "burst_guarantee_resources": {"cpu": 1000},
            },
        })
        create_pool("overflow_pool", parent_name="ancestor", attributes={
            "integral_guarantees": {
                "guarantee_type": "relaxed",
                "resource_flow": {"cpu": 1000.0},
            },
        })
        time.sleep(1)  # Wait a bit to accumulate some volume in overflow_pool.
        set("//sys/pools/ancestor/overflow_pool/@integral_guarantees/resource_flow/cpu", 0.01)

        create_pool("problem_ancestor", parent_name="ancestor", attributes={
            "integral_guarantees": {
                "guarantee_type": "none",
                "resource_flow": {"cpu": 1},
                "burst_guarantee_resources": {"cpu": 1},
            },
        })
        create_pool("problem_pool", parent_name="problem_ancestor", attributes={
            "integral_guarantees": {
                "guarantee_type": "burst",
                "resource_flow": {"cpu": 1},
                "burst_guarantee_resources": {"cpu": 1},
            },
        })
        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), spec={"pool": "problem_pool"})

        set("//sys/pools/ancestor/problem_ancestor/problem_pool/@integral_guarantees/resource_flow/cpu", 0)
        set("//sys/pools/ancestor/problem_ancestor/@integral_guarantees/resource_flow/cpu", 0)

        # Cannot use 0 since free volume from overflow_pool will always flow in.
        wait(lambda: get(scheduler_orchid_pool_path("problem_pool") + "/accumulated_resource_volume/cpu") < 0.02)

        release_breakpoint()
        op.track()


##################################################################


@pytest.mark.enabled_multidaemon
class TestSatisfactionRatio(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,  # Update pools configuration period
        }
    }

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "gpu_manager": {
                "testing": {
                    "test_resource": True,
                    "test_gpu_count": 8
                },
            },
        },
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 8,
                "user_slots": 8,
            },
        }
    }

    def setup_method(self, method):
        super(TestSatisfactionRatio, self).setup_method(method)
        update_pool_tree_config("default", {
            "non_preemptible_resource_usage_threshold": {
                "user_slots": 10,
            },
        })

    @authors("eshcherbin")
    def test_satisfaction_simple(self):
        create_pool("pool")

        op1 = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            spec={"pool": "pool"},
            job_count=3,
            task_patch={"cpu_limit": 4.0},
        )
        wait_breakpoint(job_count=2)

        op2 = run_sleeping_vanilla(spec={"pool": "pool"}, task_patch={"cpu_limit": 4.0})

        wait(
            lambda: are_almost_equal(
                get(scheduler_orchid_operation_path(op1.id) + "/satisfaction_ratio"),
                2.0,
            )
        )
        wait(
            lambda: are_almost_equal(
                get(scheduler_orchid_operation_path(op2.id) + "/satisfaction_ratio"),
                0.0,
            )
        )
        wait(lambda: are_almost_equal(get(scheduler_orchid_pool_path("pool") + "/satisfaction_ratio"), 0.0))

        release_breakpoint()

        op1.wait_for_state("completed")

        wait(
            lambda: are_almost_equal(
                get(scheduler_orchid_operation_path(op2.id) + "/satisfaction_ratio"),
                1.0,
            )
        )
        wait(lambda: are_almost_equal(get(scheduler_orchid_pool_path("pool") + "/satisfaction_ratio"), 1.0))

    @authors("eshcherbin")
    def test_dont_use_pool_satisfaction_for_scheduling(self):
        create_pool("first", attributes={
            "strong_guarantee_resources": {"cpu": 3.0},
            "resource_limits": {"user_slots": 1},
        })
        create_pool("second", attributes={
            "strong_guarantee_resources": {"cpu": 3.0},
            "resource_limits": {"user_slots": 1},
        })

        op1 = run_sleeping_vanilla(job_count=2, spec={"pool": "first"})
        op2 = run_sleeping_vanilla(job_count=3, spec={"pool": "second"})

        wait(lambda: get(scheduler_orchid_operation_path(op1.id) + "/resource_usage/user_slots", default=None) == 1)
        wait(lambda: get(scheduler_orchid_operation_path(op2.id) + "/resource_usage/user_slots") == 1)

        blocking_op = run_sleeping_vanilla(task_patch={"cpu_limit": 6.0})
        wait(lambda: get(scheduler_orchid_operation_path(blocking_op.id) + "/resource_usage/user_slots", default=None) == 1)

        remove("//sys/pool_trees/default/first/@resource_limits")
        remove("//sys/pool_trees/default/second/@resource_limits")

        op3 = run_sleeping_vanilla(spec={"pool": "first"}, task_patch={"gpu_limit": 1, "enable_gpu_layers": False})

        wait(lambda: are_almost_equal(get(scheduler_orchid_operation_path(op1.id) + "/satisfaction_ratio"), 0.5))
        wait(lambda: are_almost_equal(get(scheduler_orchid_operation_path(op2.id) + "/satisfaction_ratio"), 1.0 / 3.0))
        wait(lambda: are_almost_equal(get(scheduler_orchid_operation_path(op3.id) + "/satisfaction_ratio", default=None), 0.0))
        wait(lambda: are_almost_equal(get(scheduler_orchid_pool_path("first") + "/satisfaction_ratio"), 0.0))
        wait(lambda: are_almost_equal(get(scheduler_orchid_pool_path("second") + "/satisfaction_ratio"), 1.0 / 3.0))

        wait(lambda: are_almost_equal(get(scheduler_orchid_pool_path("first") + "/local_satisfaction_ratio"), 0.0))

        wait(lambda: get(scheduler_orchid_operation_path(op1.id) + "/scheduling_index") == 2)
        wait(lambda: get(scheduler_orchid_operation_path(op2.id) + "/scheduling_index") == 1)
        wait(lambda: get(scheduler_orchid_operation_path(op3.id) + "/scheduling_index") == 0)

    @authors("eshcherbin")
    def test_operation_satisfaction_distribution_profiling(self):
        UPPER_BOUND = 2.0
        PRECISION = 0.001
        QUANTILES = [0.2, 0.25, 0.4, 0.5, 0.6, 0.75, 0.8, 1.0]
        update_pool_tree_config("default", {
            "per_pool_satisfaction_digest": {
                "lower_bound": 0.0,
                "upper_bound": UPPER_BOUND,
                "absolute_precision": PRECISION,
            },
            "per_pool_satisfaction_profiling_quantiles": QUANTILES,
            "non_preemptible_resource_usage_threshold": {
                "user_slots": 10,
            },
        })

        create_pool("pool", attributes={"strong_guarantee_resources": {"cpu": 8.0}})

        OP_USAGE_AND_DEMANDS = [
            (0, 1),
            (1, 2),
            (2, 2),
            (2, 3),
        ]

        ops = []
        blocking_demand = 0
        for usage, demand in OP_USAGE_AND_DEMANDS:
            op = run_sleeping_vanilla(
                job_count=demand,
                spec={
                    "scheduling_options_per_pool_tree": {
                        "default": {
                            "resource_limits": {"user_slots": usage}
                        },
                    },
                    "pool": "pool",
                }
            )
            ops.append(op)

            blocking_demand += demand - usage

        for i, op in enumerate(ops):
            wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/resource_usage/user_slots", default=None) == OP_USAGE_AND_DEMANDS[i][0])
        wait(lambda: are_almost_equal(get(scheduler_orchid_pool_path("pool") + "/demand_share/user_slots"), 1.0))

        blocking_op = run_sleeping_vanilla(job_count=blocking_demand)
        wait(lambda: get(scheduler_orchid_operation_path(blocking_op.id) + "/resource_usage/user_slots", default=None) == blocking_demand)

        for i, op in enumerate(ops):
            update_op_parameters(op.id, parameters={"scheduling_options_per_pool_tree": {
                "default": {"resource_limits": {"user_slots": OP_USAGE_AND_DEMANDS[i][1]}}},
            })

        for i, op in enumerate(ops):
            satisfaction = OP_USAGE_AND_DEMANDS[i][0] / OP_USAGE_AND_DEMANDS[i][1]
            wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/local_satisfaction_ratio") == satisfaction)
        wait(lambda: get(scheduler_orchid_operation_path(blocking_op.id) + "/local_satisfaction_ratio") >= UPPER_BOUND)

        profiler = profiler_factory().at_scheduler(fixed_tags={"tree": "default"})
        root_satisfaction_distribution_sensor = profiler.gauge(
            "scheduler/pools/operation_satisfaction_distribution",
            fixed_tags={"pool": "<Root>"})
        pool_satisfaction_distribution_sensor = profiler.gauge(
            "scheduler/pools/operation_satisfaction_distribution",
            fixed_tags={"pool": "pool"})

        def format_quantile(q):
            if q == 1.0:
                return '1'
            return str(q)

        satisfactions = sorted(usage / demand for usage, demand in OP_USAGE_AND_DEMANDS)
        for i, satisfaction in enumerate(satisfactions):
            quantile = (i + 1) / len(satisfactions)
            wait(lambda: are_almost_equal(
                pool_satisfaction_distribution_sensor.get(tags={"quantile": format_quantile(quantile)}),
                satisfaction,
                absolute_error=PRECISION,
            ))

        satisfactions.append(UPPER_BOUND)
        for i, satisfaction in enumerate(satisfactions):
            quantile = (i + 1) / len(satisfactions)
            wait(lambda: are_almost_equal(
                root_satisfaction_distribution_sensor.get(tags={"quantile": format_quantile(quantile)}),
                satisfaction,
                absolute_error=PRECISION,
            ))


##################################################################


class TestVectorStrongGuarantees(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 10,
                "user_slots": 10,
                "network": 100,
            }
        }
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,  # Update pools configuration period
            "alerts_update_period": 100,
        }
    }

    def _are_almost_equal_vec(self, lhs, rhs):
        for resource in ["cpu", "user_slots", "network"]:
            if not are_almost_equal(lhs.get(resource, 0.0), rhs.get(resource, 0.0)):
                return False
        return True

    def setup_method(self, method):
        super(TestVectorStrongGuarantees, self).setup_method(method)
        set("//sys/pool_trees/default/@config/main_resource", "cpu")

    @authors("eshcherbin")
    def test_explicit_guarantees(self):
        create_pool("pool", attributes={"strong_guarantee_resources": {"cpu": 30, "user_slots": 30}})
        wait(lambda: self._are_almost_equal_vec(
            get(scheduler_orchid_pool_path("pool") + "/strong_guarantee_resources"),
            {"cpu": 30, "user_slots": 30}))
        wait(lambda: self._are_almost_equal_vec(
            get(scheduler_orchid_pool_path("pool") + "/effective_strong_guarantee_resources"),
            {"cpu": 30, "user_slots": 30, "network": 300}))
        wait(lambda: self._are_almost_equal_vec(
            get(scheduler_orchid_pool_path("pool") + "/strong_guarantee_share"),
            {"cpu": 1.0, "user_slots": 1.0, "network": 1.0}))

        create_pool("subpool1", parent_name="pool", attributes={"strong_guarantee_resources": {"cpu": 10, "user_slots": 20}})
        create_pool("subpool2", parent_name="pool", attributes={"strong_guarantee_resources": {"cpu": 20, "user_slots": 10}})

        wait(lambda: self._are_almost_equal_vec(
            get(scheduler_orchid_pool_path("subpool1") + "/effective_strong_guarantee_resources"),
            {"cpu": 10, "user_slots": 20, "network": 100}))
        wait(lambda: self._are_almost_equal_vec(
            get(scheduler_orchid_pool_path("subpool1") + "/strong_guarantee_share"),
            {"cpu": 1.0 / 3.0, "user_slots": 2.0 / 3.0, "network": 1.0 / 3.0}))
        wait(lambda: self._are_almost_equal_vec(
            get(scheduler_orchid_pool_path("subpool2") + "/effective_strong_guarantee_resources"),
            {"cpu": 20, "user_slots": 10, "network": 200}))
        wait(lambda: self._are_almost_equal_vec(
            get(scheduler_orchid_pool_path("subpool2") + "/strong_guarantee_share"),
            {"cpu": 2.0 / 3.0, "user_slots": 1.0 / 3.0, "network": 2.0 / 3.0}))

        run_sleeping_vanilla(job_count=3, spec={"pool": "pool"}, task_patch={"cpu_limit": 10})

        op1 = run_sleeping_vanilla(job_count=20, spec={"pool": "subpool1"}, task_patch={"cpu_limit": 0.5})
        op2 = run_sleeping_vanilla(job_count=10, spec={"pool": "subpool2"}, task_patch={"cpu_limit": 2})
        wait(lambda: exists(scheduler_orchid_operation_path(op2.id)))

        wait(lambda: are_almost_equal(get(scheduler_orchid_operation_path(op1.id) + "/detailed_fair_share/total/cpu"), 1.0 / 3.0))
        wait(lambda: are_almost_equal(get(scheduler_orchid_operation_path(op1.id) + "/detailed_fair_share/total/user_slots"), 2.0 / 3.0))
        wait(lambda: are_almost_equal(get(scheduler_orchid_operation_path(op2.id) + "/detailed_fair_share/total/cpu"), 2.0 / 3.0))
        wait(lambda: are_almost_equal(get(scheduler_orchid_operation_path(op2.id) + "/detailed_fair_share/total/user_slots"), 1.0 / 3.0))

    @authors("eshcherbin")
    def test_partially_explicit_guarantees_without_scaling(self):
        create_pool("pool", attributes={"strong_guarantee_resources": {"cpu": 30, "user_slots": 30}})
        create_pool("subpool1", parent_name="pool", attributes={"strong_guarantee_resources": {"cpu": 10, "user_slots": 15}})
        create_pool("subpool2", parent_name="pool", attributes={"strong_guarantee_resources": {"cpu": 5}})
        create_pool("subpool3", parent_name="pool", attributes={"strong_guarantee_resources": {"cpu": 10}})

        wait(lambda: self._are_almost_equal_vec(
            get(scheduler_orchid_pool_path("subpool1") + "/effective_strong_guarantee_resources"),
            {"cpu": 10, "user_slots": 15, "network": 100}))
        wait(lambda: self._are_almost_equal_vec(
            get(scheduler_orchid_pool_path("subpool1") + "/strong_guarantee_share"),
            {"cpu": 1.0 / 3.0, "user_slots": 1.0 / 2.0, "network": 1.0 / 3.0}))
        wait(lambda: self._are_almost_equal_vec(
            get(scheduler_orchid_pool_path("subpool2") + "/effective_strong_guarantee_resources"),
            {"cpu": 5, "user_slots": 5, "network": 50}))
        wait(lambda: self._are_almost_equal_vec(
            get(scheduler_orchid_pool_path("subpool2") + "/strong_guarantee_share"),
            {"cpu": 1.0 / 6.0, "user_slots": 1.0 / 6.0, "network": 1.0 / 6.0}))
        wait(lambda: self._are_almost_equal_vec(
            get(scheduler_orchid_pool_path("subpool3") + "/effective_strong_guarantee_resources"),
            {"cpu": 10, "user_slots": 10, "network": 100}))
        wait(lambda: self._are_almost_equal_vec(
            get(scheduler_orchid_pool_path("subpool3") + "/strong_guarantee_share"),
            {"cpu": 1.0 / 3.0, "user_slots": 1.0 / 3.0, "network": 1.0 / 3.0}))

    @authors("eshcherbin")
    def test_partially_explicit_guarantees_with_scaling(self):
        create_pool("pool", attributes={"strong_guarantee_resources": {"cpu": 30, "user_slots": 30}})
        create_pool("subpool1", parent_name="pool", attributes={"strong_guarantee_resources": {"cpu": 10, "user_slots": 25}})
        create_pool("subpool2", parent_name="pool", attributes={"strong_guarantee_resources": {"cpu": 5}})
        create_pool("subpool3", parent_name="pool", attributes={"strong_guarantee_resources": {"cpu": 10}})

        wait(lambda: self._are_almost_equal_vec(
            get(scheduler_orchid_pool_path("subpool1") + "/effective_strong_guarantee_resources"),
            {"cpu": 10, "user_slots": 25, "network": 100}))
        wait(lambda: self._are_almost_equal_vec(
            get(scheduler_orchid_pool_path("subpool1") + "/strong_guarantee_share"),
            {"cpu": 1.0 / 3.0, "user_slots": 5.0 / 6.0, "network": 1.0 / 3.0}))
        wait(lambda: self._are_almost_equal_vec(
            get(scheduler_orchid_pool_path("subpool2") + "/effective_strong_guarantee_resources"),
            {"cpu": 5, "user_slots": 1, "network": 50}))
        wait(lambda: self._are_almost_equal_vec(
            get(scheduler_orchid_pool_path("subpool2") + "/strong_guarantee_share"),
            {"cpu": 1.0 / 6.0, "user_slots": 1.0 / 30.0, "network": 1.0 / 6.0}))
        wait(lambda: self._are_almost_equal_vec(
            get(scheduler_orchid_pool_path("subpool3") + "/effective_strong_guarantee_resources"),
            {"cpu": 10, "user_slots": 3, "network": 100}))
        wait(lambda: self._are_almost_equal_vec(
            get(scheduler_orchid_pool_path("subpool3") + "/strong_guarantee_share"),
            {"cpu": 1.0 / 3.0, "user_slots": 1.0 / 10.0, "network": 1.0 / 3.0}))

    def _wait_for_guarantee_overcommit_alert(self):
        wait(lambda: get("//sys/scheduler/@alerts"))
        assert get("//sys/scheduler/@alerts")[0]["inner_errors"][0]["inner_errors"][0]["code"] == \
               yt_error_codes.Scheduler.PoolTreeGuaranteesOvercommit

    @authors("eshcherbin")
    def test_guarantee_overcommit(self):
        create_pool("pool1", attributes={"strong_guarantee_resources": {"cpu": 15, "user_slots": 20}})
        create_pool("pool2", attributes={"strong_guarantee_resources": {"cpu": 20}})

        wait(lambda: self._are_almost_equal_vec(
            get(scheduler_orchid_pool_path("pool1") + "/effective_strong_guarantee_resources"),
            {"cpu": 15, "user_slots": 20, "network": 150}))
        wait(lambda: self._are_almost_equal_vec(
            get(scheduler_orchid_pool_path("pool1") + "/strong_guarantee_share"),
            {"cpu": 3.0 / 8.0, "user_slots": 1.0 / 2.0, "network": 3.0 / 8.0}))
        wait(lambda: self._are_almost_equal_vec(
            get(scheduler_orchid_pool_path("pool2") + "/effective_strong_guarantee_resources"),
            {"cpu": 20, "user_slots": 20, "network": 200}))
        wait(lambda: self._are_almost_equal_vec(
            get(scheduler_orchid_pool_path("pool2") + "/strong_guarantee_share"),
            {"cpu": 1.0 / 2.0, "user_slots": 1.0 / 2.0, "network": 1.0 / 2.0}))

        self._wait_for_guarantee_overcommit_alert()

    @authors("eshcherbin")
    def test_guarantee_overcommit_zero_cluster(self):
        with Restarter(self.Env, NODES_SERVICE):
            create_pool("pool1", attributes={"strong_guarantee_resources": {"cpu": 15, "user_slots": 20}})
            create_pool("pool2", attributes={"strong_guarantee_resources": {"cpu": 20}})

            wait(lambda: self._are_almost_equal_vec(
                get(scheduler_orchid_pool_path("pool1") + "/effective_strong_guarantee_resources"),
                {"cpu": 15, "user_slots": 20, "network": 0}))
            wait(lambda: self._are_almost_equal_vec(
                get(scheduler_orchid_pool_path("pool1") + "/strong_guarantee_share"),
                {"cpu": 0.0, "user_slots": 0.0, "network": 0.0}))
            wait(lambda: self._are_almost_equal_vec(
                get(scheduler_orchid_pool_path("pool2") + "/effective_strong_guarantee_resources"),
                {"cpu": 20, "user_slots": 0, "network": 0}))
            wait(lambda: self._are_almost_equal_vec(
                get(scheduler_orchid_pool_path("pool2") + "/strong_guarantee_share"),
                {"cpu": 0.0, "user_slots": 0.0, "network": 0.0}))

            self._wait_for_guarantee_overcommit_alert()

    @authors("eshcherbin")
    def test_guarantee_overcommit_single_large_pool(self):
        create_pool("pool", attributes={"strong_guarantee_resources": {"cpu": 40}})

        wait(lambda: self._are_almost_equal_vec(
            get(scheduler_orchid_pool_path("pool") + "/effective_strong_guarantee_resources"),
            {"cpu": 40, "user_slots": 40, "network": 400}))
        wait(lambda: self._are_almost_equal_vec(
            get(scheduler_orchid_pool_path("pool") + "/strong_guarantee_share"),
            {"cpu": 1.0, "user_slots": 1.0, "network": 1.0}))

        self._wait_for_guarantee_overcommit_alert()


##################################################################


@pytest.mark.enabled_multidaemon
class TestPriorityStrongGuaranteeAdjustment(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 10,
                "user_slots": 10,
            }
        }
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 100,
            "watchers_update_period": 100,  # Update pools configuration period.
            "alerts_update_period": 100,
            "nodes_attributes_update_period": 100,
        }
    }

    def setup_method(self, method):
        super(TestPriorityStrongGuaranteeAdjustment, self).setup_method(method)
        set("//sys/pool_trees/default/@config/nodes_filter", "!other")
        create_pool_tree("other", config={"nodes_filter": "other"})

    def _check(self, pools, expected_shares):
        for pool, expected_share in zip(pools, expected_shares):
            wait(lambda: are_almost_equal(expected_share, get(scheduler_orchid_pool_path(pool) + "/strong_guarantee_share/cpu")))

    def _remove_node(self, node):
        set(f"//sys/exec_nodes/{node}/@user_tags/end", "other")

    @authors("eshcherbin")
    def test_one_priority_pool(self):
        create_pool("root", attributes={"strong_guarantee_resources": {"cpu": 30.0}})
        pools = [
            "poolA",
            "poolB",
            "poolC",
        ]
        for pool in pools:
            create_pool(pool, parent_name="root", attributes={"strong_guarantee_resources": {"cpu": 10.0}})

        self._check(pools, [1.0 / 3.0, 1.0 / 3.0, 1.0 / 3.0])

        nodes = ls("//sys/exec_nodes")
        self._remove_node(nodes[0])
        wait(lambda: are_almost_equal(get(scheduler_new_orchid_pool_tree_path("default") + "/resource_limits/cpu"), 20.0))

        self._check(pools, [1.0 / 3.0, 1.0 / 3.0, 1.0 / 3.0])

        wait(lambda: not get(scheduler_orchid_pool_path("root") + "/priority_strong_guarantee_adjustment_donorship_enabled"))
        wait(lambda: not get(scheduler_orchid_pool_path("poolA") + "/priority_strong_guarantee_adjustment_enabled"))

        set("//sys/pool_trees/default/root/@enable_priority_strong_guarantee_adjustment_donorship", True)
        set("//sys/pool_trees/default/root/poolA/@enable_priority_strong_guarantee_adjustment", True)

        wait(lambda: get(scheduler_orchid_pool_path("root") + "/priority_strong_guarantee_adjustment_donorship_enabled"))
        wait(lambda: get(scheduler_orchid_pool_path("poolA") + "/priority_strong_guarantee_adjustment_enabled"))
        wait(lambda: not get(scheduler_orchid_pool_path("poolB") + "/priority_strong_guarantee_adjustment_enabled"))

        self._check(pools, [0.5, 0.25, 0.25])

        wait(lambda: are_almost_equal(get(scheduler_orchid_pool_path("root") + "/strong_guarantee_share_by_tier/regular_pools/cpu"), 1.0))
        wait(lambda: are_almost_equal(get(scheduler_orchid_pool_path("root") + "/strong_guarantee_share_by_tier/priority_pools/cpu"), 0.0))
        wait(lambda: are_almost_equal(get(scheduler_orchid_pool_path("poolA") + "/strong_guarantee_share_by_tier/priority_pools/cpu"), 0.5))

        self._remove_node(nodes[1])
        wait(lambda: are_almost_equal(get(scheduler_new_orchid_pool_tree_path("default") + "/resource_limits/cpu"), 10.0))

        self._check(pools, [1.0, 0.0, 0.0])

        wait(lambda: are_almost_equal(get(scheduler_orchid_pool_path("root") + "/strong_guarantee_share_by_tier/regular_pools/cpu"), 1.0))
        wait(lambda: are_almost_equal(get(scheduler_orchid_pool_path("root") + "/strong_guarantee_share_by_tier/priority_pools/cpu"), 0.0))
        wait(lambda: are_almost_equal(get(scheduler_orchid_pool_path("poolA") + "/strong_guarantee_share_by_tier/priority_pools/cpu"), 1.0))

    @authors("eshcherbin")
    def test_two_priority_pools_same_root(self):
        create_pool("root", attributes={
            "strong_guarantee_resources": {"cpu": 30.0},
            "enable_priority_strong_guarantee_adjustment_donorship": True,
        })
        pool_to_guarantee = {
            "poolA": 10.0,
            "poolB": 5.0,
            "poolC": 10.0,
            "poolD": 5.0,
        }
        pools = list(pool_to_guarantee)
        for pool, guarantee in pool_to_guarantee.items():
            create_pool(pool, parent_name="root", attributes={"strong_guarantee_resources": {"cpu": guarantee}})

        self._check(pools, [1.0 / 3.0, 1.0 / 6.0, 1.0 / 3.0, 1.0 / 6.0])

        nodes = ls("//sys/exec_nodes")
        self._remove_node(nodes[0])
        wait(lambda: are_almost_equal(get(scheduler_new_orchid_pool_tree_path("default") + "/resource_limits/cpu"), 20.0))

        self._check(pools, [1.0 / 3.0, 1.0 / 6.0, 1.0 / 3.0, 1.0 / 6.0])

        set("//sys/pool_trees/default/root/poolA/@enable_priority_strong_guarantee_adjustment", True)
        set("//sys/pool_trees/default/root/poolC/@enable_priority_strong_guarantee_adjustment", True)

        self._check(pools, [0.5, 0.0, 0.5, 0.0])

        self._remove_node(nodes[1])
        wait(lambda: are_almost_equal(get(scheduler_new_orchid_pool_tree_path("default") + "/resource_limits/cpu"), 10.0))

        self._check(pools, [0.5, 0.0, 0.5, 0.0])

    @authors("eshcherbin")
    def test_two_priority_pools_different_roots(self):
        create_pool("rootA", attributes={
            "strong_guarantee_resources": {"cpu": 15.0},
            "enable_priority_strong_guarantee_adjustment_donorship": True,
        })
        create_pool("rootB", attributes={
            "strong_guarantee_resources": {"cpu": 15.0},
            "enable_priority_strong_guarantee_adjustment_donorship": True,
        })
        pool_to_parent_and_guarantee = {
            "poolA": ("rootA", 10.0),
            "poolB": ("rootA", 5.0),
            "poolC": ("rootB", 10.0),
            "poolD": ("rootB", 5.0),
        }
        pools = list(pool_to_parent_and_guarantee)
        for pool, (root, guarantee) in pool_to_parent_and_guarantee.items():
            create_pool(pool, parent_name=root, attributes={"strong_guarantee_resources": {"cpu": guarantee}})

        self._check(pools, [1.0 / 3.0, 1.0 / 6.0, 1.0 / 3.0, 1.0 / 6.0])

        nodes = ls("//sys/exec_nodes")
        self._remove_node(nodes[0])
        wait(lambda: are_almost_equal(get(scheduler_new_orchid_pool_tree_path("default") + "/resource_limits/cpu"), 20.0))

        self._check(pools, [1.0 / 3.0, 1.0 / 6.0, 1.0 / 3.0, 1.0 / 6.0])

        set("//sys/pool_trees/default/rootA/poolA/@enable_priority_strong_guarantee_adjustment", True)
        set("//sys/pool_trees/default/rootB/poolC/@enable_priority_strong_guarantee_adjustment", True)

        self._check(pools, [0.5, 0.0, 0.5, 0.0])

        self._remove_node(nodes[1])
        wait(lambda: are_almost_equal(get(scheduler_new_orchid_pool_tree_path("default") + "/resource_limits/cpu"), 10.0))

        self._check(pools, [0.5, 0.0, 0.5, 0.0])

    @authors("eshcherbin")
    def test_nested_donors(self):
        create_pool("root", attributes={
            "strong_guarantee_resources": {"cpu": 30.0},
            "enable_priority_strong_guarantee_adjustment_donorship": True,
        })
        create_pool("subroot", parent_name="root", attributes={
            "strong_guarantee_resources": {"cpu": 15.0},
            "enable_priority_strong_guarantee_adjustment_donorship": True,
        })
        pool_to_parent_and_guarantee = {
            "poolA": ("root", 15.0),
            "poolB": ("subroot", 5.0),
            "poolC": ("subroot", 10.0),
        }
        pools = list(pool_to_parent_and_guarantee)
        for pool, (root, guarantee) in pool_to_parent_and_guarantee.items():
            create_pool(pool, parent_name=root, attributes={"strong_guarantee_resources": {"cpu": guarantee}})

        set("//sys/pool_trees/default/root/poolA/@enable_priority_strong_guarantee_adjustment", True)
        set("//sys/pool_trees/default/root/subroot/poolC/@enable_priority_strong_guarantee_adjustment", True)

        self._check(pools, [0.5, 1.0 / 6.0, 1.0 / 3.0])

        nodes = ls("//sys/exec_nodes")
        self._remove_node(nodes[0])
        wait(lambda: are_almost_equal(get(scheduler_new_orchid_pool_tree_path("default") + "/resource_limits/cpu"), 20.0))

        self._check(pools, [0.75, 0.0, 0.25])

    @authors("eshcherbin")
    def test_priority_pool_is_donor(self):
        create_pool("root", attributes={
            "strong_guarantee_resources": {"cpu": 30.0},
            "enable_priority_strong_guarantee_adjustment_donorship": True,
        })
        create_pool("subroot", parent_name="root", attributes={
            "strong_guarantee_resources": {"cpu": 15.0},
            "enable_priority_strong_guarantee_adjustment_donorship": True,
            "enable_priority_strong_guarantee_adjustment": True,
        })
        create_pool("poolA", parent_name="subroot", attributes={
            "strong_guarantee_resources": {"cpu": 10.0},
            "enable_priority_strong_guarantee_adjustment": True,
        })
        create_pool("poolB", parent_name="subroot", attributes={
            "strong_guarantee_resources": {"cpu": 5.0},
        })

        pools = ["subroot", "poolA", "poolB"]

        self._check(pools, [0.5, 1.0 / 3.0, 1.0 / 6.0])

        nodes = ls("//sys/exec_nodes")
        self._remove_node(nodes[0])
        wait(lambda: are_almost_equal(get(scheduler_new_orchid_pool_tree_path("default") + "/resource_limits/cpu"), 20.0))

        self._check(pools, [0.75, 0.5, 0.25])

        self._remove_node(nodes[1])
        wait(lambda: are_almost_equal(get(scheduler_new_orchid_pool_tree_path("default") + "/resource_limits/cpu"), 10.0))

        self._check(pools, [1.0, 1.0, 0.0])

    def _get_pool_without_donor_error(self):
        for alert in get("//sys/scheduler/@alerts"):
            if "Found pool configuration issues during fair share update" not in alert["message"]:
                continue
            for error in alert["inner_errors"][0]["inner_errors"]:
                if error["code"] == yt_error_codes.Scheduler.PriorityStrongGuaranteeAdjustmentPoolsWithoutDonor:
                    return error
        return None

    @authors("eshcherbin")
    def test_config_validation(self):
        create_pool("root", attributes={
            "strong_guarantee_resources": {"cpu": 30.0},
            "enable_priority_strong_guarantee_adjustment_donorship": True,
        })
        pool_to_guarantee = {
            "poolA": 20.0,
            "poolB": 10.0,
        }
        pools = list(pool_to_guarantee)
        for pool, guarantee in pool_to_guarantee.items():
            create_pool(pool, parent_name="root", attributes={"strong_guarantee_resources": {"cpu": guarantee}})

        self._check(pools, [2.0 / 3.0, 1.0 / 3.0])

        set("//sys/pool_trees/default/root/poolA/@enable_priority_strong_guarantee_adjustment", True)

        nodes = ls("//sys/exec_nodes")
        self._remove_node(nodes[0])
        wait(lambda: are_almost_equal(get(scheduler_new_orchid_pool_tree_path("default") + "/resource_limits/cpu"), 20.0))

        wait(lambda: self._get_pool_without_donor_error() is None)

        self._check(pools, [1.0, 0.0])

        remove("//sys/pool_trees/default/root/@enable_priority_strong_guarantee_adjustment_donorship")

        def check_pool():
            error = self._get_pool_without_donor_error()
            return error is not None and "poolA" in error["attributes"]["priority_pools_without_donors"]
        wait(check_pool)

        self._check(pools, [2.0 / 3.0, 1.0 / 3.0])

        remove("//sys/pool_trees/default/root/poolA/@enable_priority_strong_guarantee_adjustment")

        wait(lambda: self._get_pool_without_donor_error() is None)

        self._check(pools, [2.0 / 3.0, 1.0 / 3.0])


##################################################################


@pytest.mark.enabled_multidaemon
class TestFifoPools(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 100,
            "fair_share_profiling_period": 100,
        }
    }

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "user_slots": 10,
                "cpu": 10
            }
        }
    }

    def setup_method(self, method):
        super(TestFifoPools, self).setup_method(method)
        update_pool_tree_config("default", {"preemptive_scheduling_backoff": 0})

    @authors("eshcherbin")
    @pytest.mark.parametrize("with_gang_operation", [False, True])
    def test_truncate_unsatisfied_child_fair_share_in_fifo_pools(self, with_gang_operation):
        create_pool("fifo", attributes={"mode": "fifo"})
        create_pool("normal")

        blocking_op1 = run_sleeping_vanilla(task_patch={"cpu_limit": 3.0}, spec={"pool": "fifo"})
        blocking_op1.wait_for_state("running")
        wait(lambda: are_almost_equal(get(scheduler_orchid_operation_path(blocking_op1.id) + "/detailed_fair_share/total/cpu"), 0.3))

        blocking_op2 = run_sleeping_vanilla(
            task_patch={"cpu_limit": 3.0},
            spec={
                "pool": "fifo",
                "is_gang": with_gang_operation,
            })
        blocking_op2.wait_for_state("running")
        wait(lambda: are_almost_equal(get(scheduler_orchid_operation_path(blocking_op2.id) + "/detailed_fair_share/total/cpu"), 0.3))

        op = run_sleeping_vanilla(task_patch={"cpu_limit": 5.0}, spec={"pool": "normal"})
        op.wait_for_state("running")

        wait(lambda: are_almost_equal(get(scheduler_orchid_operation_path(op.id) + "/detailed_fair_share/total/cpu"), 0.5))
        wait(lambda: are_almost_equal(get(scheduler_orchid_operation_path(blocking_op1.id) + "/detailed_fair_share/total/cpu"), 0.3))
        wait(lambda: are_almost_equal(get(scheduler_orchid_operation_path(blocking_op2.id) + "/detailed_fair_share/total/cpu"), 0.2))
        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/starvation_status") != "non_starving")

        set("//sys/pool_trees/default/@config/enable_fair_share_truncation_in_fifo_pool", True)
        wait(lambda: get(scheduler_orchid_default_pool_tree_config_path() + "/enable_fair_share_truncation_in_fifo_pool"))
        if with_gang_operation:
            wait(lambda: are_almost_equal(get(scheduler_orchid_operation_path(blocking_op2.id) + "/detailed_fair_share/total/cpu"), 0.0))
            wait(lambda: are_almost_equal(get(scheduler_orchid_operation_path(op.id) + "/usage_share/cpu"), 0.5))
        else:
            time.sleep(1.0)
            wait(lambda: are_almost_equal(get(scheduler_orchid_operation_path(blocking_op2.id) + "/detailed_fair_share/total/cpu"), 0.2))
            wait(lambda: are_almost_equal(get(scheduler_orchid_operation_path(op.id) + "/usage_share/cpu"), 0.0))

        set("//sys/pool_trees/default/@config/enable_fair_share_truncation_in_fifo_pool", False)
        wait(lambda: not get(scheduler_orchid_default_pool_tree_config_path() + "/enable_fair_share_truncation_in_fifo_pool"))
        wait(lambda: are_almost_equal(get(scheduler_orchid_operation_path(blocking_op2.id) + "/detailed_fair_share/total/cpu"), 0.2))

    @authors("eshcherbin", "ignat")
    def test_max_schedulable_element_count_in_fifo_pool(self):
        update_pool_tree_config_option("default", "max_schedulable_element_count_in_fifo_pool", 1)
        create_pool("fifo", attributes={"mode": "fifo"})

        profiler = profiler_factory().at_scheduler(fixed_tags={"tree": "default", "pool": "fifo"})
        schedulable_element_count_sensor = profiler.gauge("scheduler/pools/schedulable_element_count")
        schedulable_pool_count_sensor = profiler.gauge("scheduler/pools/schedulable_pool_count")
        schedulable_operation_count_sensor = profiler.gauge("scheduler/pools/schedulable_operation_count")
        wait(lambda: schedulable_element_count_sensor.get() is not None)
        wait(lambda: schedulable_pool_count_sensor.get() is not None)
        wait(lambda: schedulable_operation_count_sensor.get() is not None)

        ops = []
        for i in range(4):
            job_count = 2
            spec = {"pool": "fifo"}
            if i == 0:
                job_count = 1
                spec["suspend_operation_after_materialization"] = True
            ops.append(run_sleeping_vanilla(job_count=job_count, task_patch={"cpu_limit": 6.0}, spec=spec))
            time.sleep(0.1)
        wait(lambda: get(scheduler_orchid_operation_path(ops[1].id) + "/resource_usage/cpu", default=None) == 6.0)

        def check_reasons(reasons):
            for op, reason in zip(ops, reasons):
                wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/unschedulable_reason", default=None) == reason)

        fifo_reason = "fifo_schedulable_element_count_limit_reached"
        check_reasons(["suspended", yson.YsonEntity(), fifo_reason, fifo_reason])

        wait(lambda: schedulable_element_count_sensor.get() == 2)
        wait(lambda: schedulable_pool_count_sensor.get() == 1)
        wait(lambda: schedulable_operation_count_sensor.get() == 1)

        ops[0].resume()
        check_reasons([yson.YsonEntity(), yson.YsonEntity(), fifo_reason, fifo_reason])

        wait(lambda: schedulable_element_count_sensor.get() == 3)
        wait(lambda: schedulable_pool_count_sensor.get() == 1)
        wait(lambda: schedulable_operation_count_sensor.get() == 2)

    @authors("eshcherbin")
    def test_allow_gang_operations_only_in_fifo_pools(self):
        update_pool_tree_config("default", {"allow_gang_operations_only_in_fifo_pools": True})
        create_pool("fair_share", attributes={"mode": "fair_share"})
        create_pool("fifo", attributes={"mode": "fifo"})

        wait(lambda: get(scheduler_orchid_pool_path("fifo") + "/gang_operations_allowed", default=None))
        wait(lambda: not get(scheduler_orchid_pool_path("fair_share") + "/gang_operations_allowed"))

        run_sleeping_vanilla(spec={"is_gang": True, "pool": "fifo"})

        with raises_yt_error(yt_error_codes.Scheduler.GangOperationsAllowedOnlyInFifoPools):
            run_sleeping_vanilla(spec={"is_gang": True, "pool": "fair_share"})

        set("//sys/pools/fair_share/@always_allow_gang_operations", True)

        wait(lambda: get(scheduler_orchid_pool_path("fair_share") + "/gang_operations_allowed"))

        run_sleeping_vanilla(spec={"is_gang": True, "pool": "fair_share"})

    @authors("eshcherbin")
    def test_allow_gang_operations_only_in_fifo_ephemeral_subpools(self):
        update_pool_tree_config("default", {"allow_gang_operations_only_in_fifo_pools": True})
        create_pool("ephemeral_root", attributes={"create_ephemeral_subpools": True})

        wait(lambda: not get(scheduler_orchid_pool_path("ephemeral_root") + "/gang_operations_allowed"))

        with raises_yt_error(yt_error_codes.Scheduler.GangOperationsAllowedOnlyInFifoPools):
            run_sleeping_vanilla(spec={"is_gang": True, "pool": "ephemeral_root"})

        set("//sys/pools/ephemeral_root/@ephemeral_subpool_config", {"mode": "fifo"})

        wait(lambda: get(scheduler_orchid_pool_path("ephemeral_root") + "/gang_operations_allowed"))

        op = run_sleeping_vanilla(spec={"is_gang": True, "pool": "ephemeral_root"})

        op.wait_for_state("running")
        print_debug(get(scheduler_orchid_operation_path(op.id) + "/pool"))


##################################################################


@pytest.mark.enabled_multidaemon
class TestRaceBetweenOperationUnregistrationAndFairShareUpdate(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 100,
            "allowed_node_resources_overcommit_duration": 0,
        }
    }

    def setup_method(self, method):
        super(TestRaceBetweenOperationUnregistrationAndFairShareUpdate, self).setup_method(method)
        update_pool_tree_config("default", {
            "fair_share_starvation_timeout": 100000,
            "preemptive_scheduling_backoff": 0,
            "testing_options": {}
        })

    @authors("eshcherbin")
    @pytest.mark.parametrize("use_dynamic_config_resource_limits_overrides", [False, True])
    def test_race_between_operation_unregistration_and_fair_share_update(self, use_dynamic_config_resource_limits_overrides):
        # See YT-17137.
        op = run_sleeping_vanilla()
        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/resource_usage/cpu", default=None) == 1.0)
        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/starvation_status") == "non_starving")

        node = ls("//sys/cluster_nodes")[0]
        if use_dynamic_config_resource_limits_overrides:
            update_nodes_dynamic_config({"resource_limits": {"overrides": {"cpu": 0.5}}})
        else:
            set("//sys/cluster_nodes/{}/@resource_limits_overrides".format(node), {"cpu": 0.5})

        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/resource_usage/cpu") == 0.0)
        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/scheduling_status") == "below_fair_share")

        update_pool_tree_config_option("default", "testing_options/delay_inside_fair_share_update", {"duration": 5000, "type": "sync"})
        time.sleep(1.0)
        update_pool_tree_config_option("default", "fair_share_starvation_timeout", 0)
        time.sleep(5.0)

        op.abort()

        @wait_no_assert
        def check():
            path = scheduler_orchid_operation_path(op.id) + "/starvation_status"
            assert not exists(path) or get(path) == "starving"


##################################################################


@pytest.mark.enabled_multidaemon
class TestGuaranteePriorityScheduling(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 100,
        }
    }

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "user_slots": 4,
                "cpu": 4,
            }
        }
    }

    @authors("eshcherbin")
    def test_simple(self):
        update_pool_tree_config("default", {
            "enable_limiting_ancestor_check": False,
            "enable_guarantee_priority_scheduling": True,
        })

        create_pool("root", attributes={
            "resource_limits": {"user_slots": 0},
            "strong_guarantee_resources": {"cpu": 4.0},
            "allow_regular_preemption": False,
        })
        create_pool("prod", parent_name="root", attributes={
            "resource_limits": {"user_slots": 1},
            "strong_guarantee_resources": {"cpu": 3.0},
            "compute_promised_guarantee_fair_share": True,
        })
        create_pool("research", parent_name="root")

        prod_op = run_sleeping_vanilla(job_count=2, task_patch={"cpu_limit": 2.0}, spec={"pool": "prod"})
        research_op = run_sleeping_vanilla(spec={"pool": "research"})

        wait(lambda: exists(scheduler_orchid_operation_path(research_op.id)))

        set("//sys/pool_trees/default/root/@resource_limits/user_slots", 1)

        wait(lambda: get(scheduler_orchid_operation_path(prod_op.id) + "/resource_usage/user_slots") == 1)
        wait(lambda: get(scheduler_orchid_operation_path(research_op.id) + "/resource_usage/user_slots") == 0)

        remove("//sys/pool_trees/default/root/prod/@resource_limits")
        remove("//sys/pool_trees/default/root/@resource_limits")

        wait(lambda: get(scheduler_orchid_operation_path(prod_op.id) + "/resource_usage/user_slots") == 2)
        wait(lambda: get(scheduler_orchid_operation_path(research_op.id) + "/resource_usage/user_slots") == 0)
