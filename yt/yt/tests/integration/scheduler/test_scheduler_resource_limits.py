from yt_env_setup import YTEnvSetup, is_asan_build, is_debug_build, Restarter, NODES_SERVICE

from yt_commands import (
    authors, print_debug, wait, wait_breakpoint, release_breakpoint, with_breakpoint, create,
    ls, get, set, create_pool, write_file, read_table, write_table, map, vanilla, get_job,
    update_nodes_dynamic_config, update_controller_agent_config,
    run_test_vanilla, sync_create_cells)

from yt_scheduler_helpers import scheduler_orchid_pool_path

from yt_helpers import read_structured_log, write_log_barrier

import yt.environment.init_operations_archive as init_operations_archive

from yt.common import YtError, update
import pytest

import string
import socket
import time
import builtins

###############################################################################################

MEMORY_SCRIPT = """#!/usr/bin/env python3
import time

from random import randint
def rndstr(n):
    s = ''
    for i in range(100):
        s += chr(randint(ord('a'), ord('z')))
    return s * (n // 100)

{before_action}

a = list()
while len(a) * 100000 < {memory}:
    a.append(rndstr(100000))

{after_action}
"""


def create_memory_script(memory, before_action=""):
    return MEMORY_SCRIPT.format(before_action=before_action, memory=memory, after_action="time.sleep(5.0)").encode("ascii")


###############################################################################################


@pytest.mark.enabled_multidaemon
class TestSchedulerMemoryLimits(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    # pytest.mark.xfail(run = False, reason = "Set-uid-root before running.")
    @authors("psushin")
    def test_map(self):
        create("table", "//tmp/t_in")
        write_table(
            "//tmp/t_in",
            {"value": "value", "subkey": "subkey", "key": "key", "a": "another"},
        )

        create("table", "//tmp/t_out")

        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="python3 -c 'import time; a=[1]*100000000; time.sleep(10)'",
            spec={"max_failed_job_count": 2, "mapper": {"memory_limit": 512 * 1024 * 1024}},
        )

        # if all jobs failed then operation is also failed
        with pytest.raises(YtError):
            op.track()
        # ToDo: check job error messages.
        import builtins
        for job_id in op.list_jobs():
            inner_error = get_job(op.id, job_id)["error"]["inner_errors"][0]
            assert "Memory limit exceeded" in inner_error["message"]
            attributes = inner_error["attributes"]
            assert "processes" in attributes
            expected_cmdline = ["python3", "-c", "import time; a=[1]*100000000; time.sleep(10)"]
            assert expected_cmdline in builtins.map(lambda x: x["cmdline"], attributes["processes"])

    @authors("max42", "ignat")
    def test_dirty_sandbox(self):
        create("table", "//tmp/t_in")
        write_table(
            "//tmp/t_in",
            {"value": "value", "subkey": "subkey", "key": "key", "a": "another"},
        )

        create("table", "//tmp/t_out")

        command = "cat > /dev/null; mkdir ./tmpxxx; echo 1 > ./tmpxxx/f1; chmod 700 ./tmpxxx;"
        map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command=command,
            spec={"max_failed_job_count": 1},
        )


@pytest.mark.enabled_multidaemon
class TestSchedulerMemoryLimitsPorto(TestSchedulerMemoryLimits):
    ENABLE_MULTIDAEMON = True
    USE_PORTO = True


@pytest.mark.enabled_multidaemon
class TestDisabledMemoryLimit(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "job_proxy": {
                "check_user_job_memory_limit": False,
            },
        }
    }

    @authors("psushin")
    @pytest.mark.ignore_in_opensource_ci
    def test_no_memory_limit(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        write_table("//tmp/t_in", {"cool": "stuff"})

        map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="python3 -c 'import time; a=[1]*1000000; time.sleep(10)'",
            spec={"mapper": {"memory_limit": 1}},
        )


###############################################################################################


class TestMemoryReserveFactor(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # Checks structured log.
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1
    USE_PORTO = True

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "user_job_memory_digest_precision": 0.05,
            # To be sure that 20 jobs is enough to avoid outliers.
            "user_job_memory_reserve_quantile": 0.8,
        }
    }

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "job_proxy": {
                "job_proxy_send_heartbeat_before_abort": True,
                "always_abort_on_memory_reserve_overdraft": True,
            },
        }
    }

    @pytest.mark.skipif(is_asan_build(), reason="This test does not work under ASAN")
    @authors("ignat", "max42")
    @pytest.mark.timeout(300)
    def test_memory_reserve_factor(self):
        job_count = 20

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"key": i} for i in range(job_count)])
        create("table", "//tmp/t_out")

        create("file", "//tmp/mapper.py")
        write_file("//tmp/mapper.py", create_memory_script(memory=200 * 10 ** 6), attributes={"executable": True})

        controller_agent_address = ls("//sys/controller_agents/instances")[0]
        from_barrier = write_log_barrier(controller_agent_address)

        op = map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="python3 mapper.py",
            job_count=job_count,
            spec={
                "resource_limits": {"cpu": 1},
                "data_weight_per_job": 1,
                "mapper": {
                    "memory_limit": 400 * 10 ** 6,
                    "user_job_memory_digest_default_value": 0.9,
                    "file_paths": ["//tmp/mapper.py"],
                },
            })

        time.sleep(1)

        to_barrier = write_log_barrier(controller_agent_address)

        structured_log = read_structured_log(
            self.path_to_run + "/logs/controller-agent-0.json.log",
            from_barrier=from_barrier,
            to_barrier=to_barrier,
            row_filter=lambda e: "event_type" in e)

        last_memory_reserve = None
        for event in structured_log:
            if event["event_type"] == "job_completed" and event["operation_id"] == op.id:
                print_debug(
                    event["job_id"],
                    event["statistics"]["user_job"]["memory_reserve"]["sum"],
                    event["statistics"]["user_job"]["max_memory"]["sum"],
                )
                if event.get("predecessor_type") != "resource_overdraft":
                    last_memory_reserve = int(event["statistics"]["user_job"]["memory_reserve"]["sum"])
        assert last_memory_reserve is not None
        # Sometimes we observe memory usage of ytserver-exec + script that is about 320MB.
        assert 2e8 <= last_memory_reserve <= 3.5e8

###############################################################################################


@pytest.mark.skipif(is_asan_build(), reason="This test does not work under ASAN")
@pytest.mark.skipif(is_debug_build(), reason="This test does not work under Debug build")
class TestCumulativeMemoryStatistics(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # Checks structured logs.
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1
    USE_PORTO = True

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "user_job_memory_digest_precision": 0.05,
        }
    }

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "job_proxy": {
                "job_proxy_send_heartbeat_before_abort": True,
                "always_abort_on_memory_reserve_overdraft": True,
            },
        }
    }

    @authors("arkady-e1ppa")
    def test_cumulative_memory_statistics(self):
        job_count = 2

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"key": i} for i in range(job_count)])
        create("table", "//tmp/t_out")

        create("file", "//tmp/mapper.py")
        write_file("//tmp/mapper.py", create_memory_script(memory=10 * 10 ** 6), attributes={"executable": True})

        controller_agent_address = ls("//sys/controller_agents/instances")[0]
        from_barrier = write_log_barrier(controller_agent_address)

        op = map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="python3 mapper.py",
            job_count=job_count,
            spec={
                "resource_limits": {"cpu": 1},
                "data_weight_per_job": 1,
                "mapper": {
                    "memory_limit": 400 * 10 ** 6,
                    "user_job_memory_digest_default_value": 0.4,
                    "file_paths": ["//tmp/mapper.py"],
                },
            })

        time.sleep(1)

        to_barrier = write_log_barrier(controller_agent_address)

        structured_log = read_structured_log(
            self.path_to_run + "/logs/controller-agent-0.json.log",
            from_barrier=from_barrier,
            to_barrier=to_barrier,
            row_filter=lambda e: "event_type" in e)

        for event in structured_log:
            if event["event_type"] in ("job_completed") and event["operation_id"] == op.id:
                assert "user_job" in event["statistics"]
                print_debug(
                    event["job_id"],
                    event["statistics"]["user_job"]["cumulative_memory_mb_sec"]["last"] * 2 ** 20,
                    event["statistics"]["user_job"]["cumulative_max_memory"]["last"],
                    event["statistics"]["job_proxy"]["cumulative_memory_mb_sec"]["last"] * 2 ** 20,
                    event["statistics"]["job_proxy"]["cumulative_max_memory"]["last"],
                )

                assert event["statistics"]["user_job"]["cumulative_memory_mb_sec"]["last"] * 2 ** 20 < \
                       event["statistics"]["user_job"]["cumulative_max_memory"]["last"]

                assert event["statistics"]["job_proxy"]["cumulative_memory_mb_sec"]["last"] * 2 ** 20 < \
                       event["statistics"]["job_proxy"]["cumulative_max_memory"]["last"]

###############################################################################################


@pytest.mark.skipif(is_asan_build(), reason="This test does not work under ASAN")
@pytest.mark.skipif(is_debug_build(), reason="This test does not work under Debug build")
class TestMemoryReserveMultiplier(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # Checks structured logs.
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1
    USE_PORTO = True

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "user_job_memory_digest_precision": 0.05,
        }
    }

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "job_proxy": {
                "job_proxy_heartbeat_period": 500,
                "job_proxy_send_heartbeat_before_abort": True,
                "always_abort_on_memory_reserve_overdraft": True,
            },
        }
    }

    @authors("ignat")
    def test_user_job_resource_overdraft_memory_multiplier(self):
        update_controller_agent_config("user_job_resource_overdraft_memory_multiplier", 1.5)

        controller_agent_address = ls("//sys/controller_agents/instances")[0]
        from_barrier = write_log_barrier(controller_agent_address)

        create("file", "//tmp/mapper.py", attributes={"replication_factor": 1, "executable": True})
        write_file(
            "//tmp/mapper.py",
            create_memory_script(
                # This sleep is necessary to report statistics from job proxy to node before abort.
                before_action="time.sleep(5)",
                # Higher memory is necessary due to additional ytserver-exec memory footprint.
                memory=250 * 10 ** 6,
            ))

        op = run_test_vanilla(
            track=True,
            command="python3 mapper.py",
            job_count=1,
            spec={
                "resource_limits": {"cpu": 1},
            },
            task_patch={
                "memory_limit": 400 * 10 ** 6,
                "user_job_memory_digest_default_value": 0.4,
                "file_paths": ["//tmp/mapper.py"],
            })

        time.sleep(1)

        to_barrier = write_log_barrier(controller_agent_address)

        structured_log = read_structured_log(
            self.path_to_run + "/logs/controller-agent-0.json.log",
            from_barrier=from_barrier,
            to_barrier=to_barrier,
            row_filter=lambda e: "event_type" in e)

        user_job_memory_reserves = []
        last_job_id = None
        for event in structured_log:
            if event["event_type"] in ("job_aborted", "job_completed") and event["operation_id"] == op.id:
                assert "user_job" in event["statistics"]
                print_debug(
                    event["job_id"],
                    event.get("predecessor_type"),
                    event.get("predecessor_job_id"),
                    event["statistics"]["user_job"]["memory_reserve"]["sum"],
                    event["statistics"]["user_job"]["max_memory"]["sum"],
                )

                if last_job_id is not None:
                    assert last_job_id == event["predecessor_job_id"]
                    assert event["predecessor_type"] == "resource_overdraft"
                last_job_id = event["job_id"]

                user_job_memory_reserves.append(int(event["statistics"]["user_job"]["memory_reserve"]["sum"]))

        assert user_job_memory_reserves[0] == 160 * 10 ** 6
        assert user_job_memory_reserves[1] == 240 * 10 ** 6
        assert user_job_memory_reserves[2] == 400 * 10 ** 6

    @authors("ignat")
    def test_job_proxy_resource_overdraft_memory_multiplier(self):
        footprint = 16 * 1024 * 1024

        # We assume that user job even with alive ytserver-exec process should not consume more than 400MB of RAM.
        user_job_max_memory = 400 * 1024 * 1024

        update_controller_agent_config("job_proxy_resource_overdraft_memory_multiplier", 1.5)
        update_controller_agent_config("footprint_memory", footprint)

        controller_agent_address = ls("//sys/controller_agents/instances")[0]
        from_barrier = write_log_barrier(controller_agent_address)

        create("table", "//tmp/in", attributes={"replication_factor": 1})
        create("table", "//tmp/out", attributes={"replication_factor": 1})
        write_table("//tmp/in", [{"key": string.ascii_letters + str(value)} for value in range(5 * 1000 * 1000)])

        op = map(
            track=True,
            command="sleep 10",
            in_="//tmp/in",
            out="//tmp/out",
            spec={
                "job_count": 1,
                "mapper": {
                    "memory_limit": 500 * 10 ** 6,
                    "user_job_memory_digest_default_value": 0.9,
                    "user_job_memory_digest_lower_bound": 0.8,
                    "job_proxy_memory_digest": {
                        "default_value": 0.1,
                        "lower_bound": 0.1,
                        "upper_bound": 2.0,
                    },
                }
            })

        time.sleep(1)

        to_barrier = write_log_barrier(controller_agent_address)

        structured_log = read_structured_log(
            self.path_to_run + "/logs/controller-agent-0.json.log",
            from_barrier=from_barrier,
            to_barrier=to_barrier,
            row_filter=lambda e: "event_type" in e)

        job_proxy_memory_reserves = []
        user_job_memory_reserves = []
        last_job_id = None
        for event in structured_log:
            if event["event_type"] in ("job_aborted", "job_completed") and event["operation_id"] == op.id:
                assert "user_job" in event["statistics"]
                print_debug(
                    event["job_id"],
                    event.get("predecessor_type"),
                    event.get("predecessor_job_id"),
                    event["statistics"]["user_job"]["memory_reserve"]["sum"],
                    event["statistics"]["user_job"]["max_memory"]["sum"],
                    event["statistics"]["job_proxy"]["memory_reserve"]["sum"],
                    event["statistics"]["job_proxy"]["max_memory"]["sum"],
                )

                if last_job_id is not None:
                    assert last_job_id == event["predecessor_job_id"]
                    assert event["predecessor_type"] == "resource_overdraft"
                last_job_id = event["job_id"]

                assert event["statistics"]["user_job"]["max_memory"]["sum"] < user_job_max_memory

                job_proxy_memory_reserves.append(int(event["statistics"]["job_proxy"]["memory_reserve"]["sum"]) - footprint)
                user_job_memory_reserves.append(int(event["statistics"]["user_job"]["memory_reserve"]["sum"]))

        assert abs(job_proxy_memory_reserves[1] / job_proxy_memory_reserves[0] - 1.5) < 1e-3
        assert abs(job_proxy_memory_reserves[2] / job_proxy_memory_reserves[0] - 20.0) < 1e-3

        assert user_job_memory_reserves[1] <= user_job_memory_reserves[0]
        assert user_job_memory_reserves[2] <= user_job_memory_reserves[0]

###############################################################################################


@pytest.mark.skipif(is_asan_build(), reason="This test does not work under ASAN")
@pytest.mark.skipif(is_debug_build(), reason="This test does not work under Debug build")
@pytest.mark.enabled_multidaemon
class TestResourceOverdraftAbort(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True
    USE_PORTO = True

    DELTA_NODE_CONFIG = {
        "resource_limits": {
            # Each job proxy occupies about 100MB.
            "user_jobs": {
                "type": "static",
                "value": 2000 * 10 ** 6,
            },
        },
        "job_resource_manager": {
            "resource_limits": {
                "user_slots": 2,
                "cpu": 2,
            }
        }
    }

    def setup_method(self, method):
        super(TestResourceOverdraftAbort, self).setup_method(method)
        sync_create_cells(1)
        init_operations_archive.create_tables_latest_version(
            self.Env.create_native_client(), override_tablet_cell_bundle="default"
        )

    # Actual situation when resource overdraft logic has an effect is following:
    # 1. Some job B is within his memory reserver.
    # 2. Other job A overdrafted memory reserve.
    # 3. Job B has memory growth due to memory growth of Job Proxy.
    # This test was an attempt to reproduce this situation.
    @authors("ignat")
    def DISABLED_test_abort_job_with_actual_overdraft(self):
        create("file", "//tmp/script_500.py", attributes={"executable": True})
        write_file(
            "//tmp/script_500.py",
            MEMORY_SCRIPT.format(before_action="time.sleep(1)", memory=500 * 10 ** 6, after_action="time.sleep(1000)").encode("ascii")
        )

        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 3

        node = nodes[0]

        op_a = run_test_vanilla(
            track=False,
            command=with_breakpoint(
                "python3 script_500.py & BREAKPOINT; python3 script_500.py",
                breakpoint_name="a",
            ),
            job_count=1,
            spec={
                "resource_limits": {"cpu": 1},
                "max_failed_job_count": 1,
                "scheduling_tag_filter": node,
            },
            task_patch={
                "memory_limit": 1100 * 10 ** 6,
                "user_job_memory_digest_default_value": 0.5,
                "file_paths": ["//tmp/script_500.py"],
            })

        job_id_a, = wait_breakpoint(breakpoint_name="a")
        print_debug("Job id for operation {}".format(op_a.id), job_id_a)

        statistics = op_a.get_job_statistics(job_id_a)

        max_memory_statistics = statistics["user_job"]["max_memory"]
        assert max_memory_statistics["count"] == 1
        assert max_memory_statistics["sum"] >= 500 * 10 ** 6

        memory_reserve_statistics = statistics["user_job"]["memory_reserve"]
        assert memory_reserve_statistics["count"] == 1
        assert memory_reserve_statistics["sum"] == 550 * 10 ** 6

        op_b = run_test_vanilla(
            track=False,
            command=with_breakpoint(
                "python3 script_500.py & BREAKPOINT; python3 script_500.py",
                breakpoint_name="b",
            ),
            job_count=1,
            spec={
                "resource_limits": {"cpu": 1},
                "max_failed_job_count": 1,
                "scheduling_tag_filter": node,
            },
            task_patch={
                "memory_limit": 1100 * 10 ** 6,
                "user_job_memory_digest_lower_bound": 1.0,
                "file_paths": ["//tmp/script_500.py"],
            })
        job_id_b, = wait_breakpoint(breakpoint_name="b")
        print_debug("Job id for operation {}".format(op_b.id), job_id_b)

        release_breakpoint(breakpoint_name="a")
        wait(lambda: op_a.get_job_statistics(job_id_a)["user_job"]["max_memory"]["sum"] >= 1000 * 10 ** 6)

        release_breakpoint(breakpoint_name="b")
        wait(lambda: get_job(op_a.id, job_id_a)["state"] == "aborted")

        job_info = get_job(op_a.id, job_id_a)
        assert job_info["error"]["attributes"]["abort_reason"] == "resource_overdraft"

        assert get_job(op_b.id, job_id_b)["state"] == "running"


###############################################################################################


class TestContainerCpuProperties(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # Use list_*_subcontainers.
    USE_PORTO = True
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "map_operation_options": {
                "set_container_cpu_limit": True,
                "cpu_limit_overcommit_multiplier": 2,
                "initial_cpu_limit_overcommit": 0.05,
            },
        }
    }

    def get_job_container_property(self, property):
        from porto import Connection

        conn = Connection()

        containers = self.Env.list_node_subcontainers(0)
        assert containers
        user_job_container = [x for x in containers if x.endswith("/uj")][0]
        # Strip last three symbols "/uj"
        slot_container = user_job_container[:-3]

        return conn.GetProperty(slot_container, property)

    @authors("psushin", "max42")
    def test_container_cpu_limit_spec(self):
        create("table", "//tmp/t1", attributes={"replication_factor": 1})
        write_table("//tmp/t1", [{"foo": 1}])

        create("table", "//tmp/t2", attributes={"replication_factor": 1})

        op = map(
            wait_for_jobs=True,
            track=False,
            command=with_breakpoint("BREAKPOINT ; cat"),
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={
                "mapper": {
                    "cpu_limit": 0.1,
                    "set_container_cpu_limit": True,
                }
            },
        )

        wait_breakpoint()
        assert self.get_job_container_property("cpu_limit") == "0.1c"
        release_breakpoint()
        op.track()

    @authors("psushin")
    def test_container_cpu_limit_no_spec(self):
        create("table", "//tmp/t1", attributes={"replication_factor": 1})
        write_table("//tmp/t1", [{"foo": 1}])

        create("table", "//tmp/t2", attributes={"replication_factor": 1})

        op = map(
            wait_for_jobs=True,
            track=False,
            command=with_breakpoint("BREAKPOINT ; cat"),
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"mapper": {"cpu_limit": 0.1}},
        )
        wait_breakpoint()
        # Cpu limit is set due to map_operation_options.
        assert self.get_job_container_property("cpu_limit") == "0.25c"
        release_breakpoint()
        op.track()

    @authors("prime")
    def test_force_idle_cpu_policy(self):
        update_nodes_dynamic_config({
            "exec_node": {
                "job_controller": {
                    "job_proxy": {
                        "force_idle_cpu_policy": True,
                    }
                }
            },
        })

        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "task": {
                        "job_count": 1,
                        "command": with_breakpoint("BREAKPOINT ; exit 0"),
                    }
                }
            })

        wait_breakpoint()
        assert self.get_job_container_property("cpu_policy") == "idle"
        release_breakpoint()
        op.track()


###############################################################################################


class TestDaemonSubcontainer(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # Use get_*_container.
    USE_PORTO = True
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    DELTA_NODE_CONFIG = {
        "resource_limits": {
            "node_dedicated_cpu": 1,
            "node_cpu_weight": 100,
        },
        "exec_node": {
            "slot_manager": {
                "job_environment": {
                    "type": "porto",
                    "use_daemon_subcontainer": True,
                }
            }
        }
    }

    @authors("prime")
    def test_start(self):
        container = self.Env.get_node_container(0)

        assert container.GetProperty("cpu_weight") == '100'
        assert container.GetProperty("cpu_guarantee") == '1c'


###############################################################################################


class TestUpdateInstanceLimits(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    DELTA_NODE_CONFIG = {
        "instance_limits_update_period": 200,
        "resource_limits_update_period": 200,
        "dynamic_config_manager": {
            "enable_unrecognized_options_alert": True,
        },
        "resource_limits": {
            "node_dedicated_cpu": 1,
            "memory_limits": {
                "user_jobs": {
                    "type": "dynamic",
                },
                "tablet_static": {
                    "type": "static",
                    "value": 10 ** 9,
                },
                "tablet_dynamic": {
                    "type": "dynamic",
                },
            },
        },
        "exec_node": {
            "slot_manager": {
                "job_environment": {
                    "type": "porto",
                },
            }
        },
    }

    USE_PORTO = True
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    @authors("psushin", "gritukan")
    def test_update_cpu_limits(self):
        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 1
        node = nodes[0]
        self.Env.set_nodes_cpu_limit(4)
        wait(lambda: int(get("//sys/cluster_nodes/{}/@resource_limits/cpu".format(node))) == 3)
        wait(lambda: int(get("//sys/cluster_nodes/{}/orchid/instance_limits_tracker/cpu_limit".format(node))) == 4)
        self.Env.set_nodes_cpu_limit(3)
        wait(lambda: int(get("//sys/cluster_nodes/{}/@resource_limits/cpu".format(node))) == 2)
        wait(lambda: int(get("//sys/cluster_nodes/{}/orchid/instance_limits_tracker/cpu_limit".format(node))) == 3)
        assert int(get("//sys/cluster_nodes/{}/orchid/instance_limits_tracker/memory_usage".format(node))) > 0

    @authors("gritukan")
    def test_update_memory_limits(self):
        # User jobs memory limit is approximately (total_memory - 10**9) / 2.
        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 1
        node = nodes[0]

        # Restart nodes to decrease footprint.
        with Restarter(self.Env, NODES_SERVICE):
            pass

        precision = 3 * 10 ** 8
        self.Env.set_nodes_memory_limit(3 * 10 ** 9)
        wait(
            lambda: abs(get("//sys/cluster_nodes/{}/@resource_limits/user_memory".format(node)) - 10 ** 9)
            <= precision
        )
        self.Env.set_nodes_memory_limit(2 * 10 ** 9)
        wait(
            lambda: abs(get("//sys/cluster_nodes/{}/@resource_limits/user_memory".format(node)) - 5 * 10 ** 8)
            <= precision
        )
        self.Env.set_nodes_memory_limit(10 ** 9 - 1)
        wait(lambda: int(get("//sys/cluster_nodes/{}/@resource_limits/user_memory".format(node))) == 0)

    @authors("gritukan")
    def test_dynamic_resource_limits_config(self):
        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 1
        node = nodes[0]

        self.Env.set_nodes_memory_limit(15 * 10 ** 8)
        self.Env.set_nodes_cpu_limit(4)
        wait(lambda: int(get("//sys/cluster_nodes/{}/@resource_limits/cpu".format(node))) == 3)

        update_nodes_dynamic_config({
            "node_dedicated_cpu": 2,
            "total_cpu": 5,
            "cpu_per_tablet_slot": 1,
            "free_memory_watermark": 100000,
            "memory_limits": {
                "user_jobs": {
                    "type": "static",
                    "value": 12345678,
                },
                "tablet_static": {
                    "type": "static",
                    "value": 10 ** 9,
                },
                "tablet_dynamic": {
                    "type": "dynamic",
                },
            },
        }, path="resource_limits", replace=True)

        wait(lambda: int(get("//sys/cluster_nodes/{}/@resource_limits/user_memory".format(node))) == 12345678)
        wait(lambda: int(get("//sys/cluster_nodes/{}/@resource_limits/cpu".format(node))) == 2)

        update_nodes_dynamic_config({
            "node_dedicated_cpu": 1,
            "total_cpu": 5,
            "cpu_per_tablet_slot": 1,
            "free_memory_watermark": 100000,
            "memory_limits": {
                "user_jobs": {
                    "type": "dynamic",
                },
                "tablet_static": {
                    "type": "static",
                    "value": 10 ** 9,
                },
                "tablet_dynamic": {
                    "type": "dynamic",
                },
            },
        }, path="resource_limits", replace=True)

        wait(lambda: int(get("//sys/cluster_nodes/{}/@resource_limits/cpu".format(node))) == 3)

###############################################################################################


@pytest.mark.enabled_multidaemon
class TestSchedulerGpu(YTEnvSetup):
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
            config["job_resource_manager"] = update(config.get("job_resource_manager"), {
                "resource_limits": {
                    "user_slots": 4,
                    "cpu": 4,
                },
            })

            config["exec_node"] = update(config.get("exec_node"), {
                "gpu_manager": {
                    "testing": {
                        "test_resource": True,
                        "test_gpu_count": 4,
                    },
                }
            })

    def setup_method(self, method):
        super(TestSchedulerGpu, self).setup_method(method)
        set("//sys/pool_trees/default/@config/main_resource", "gpu")

    @authors("renadeen")
    def test_job_count(self):
        gpu_nodes = [
            node
            for node in ls("//sys/cluster_nodes")
            if get("//sys/cluster_nodes/{}/@resource_limits/gpu".format(node)) > 0
        ]
        assert len(gpu_nodes) == 1
        gpu_node = gpu_nodes[0]

        create("table", "//tmp/in")
        create("table", "//tmp/out")
        write_table("//tmp/in", [{"foo": i} for i in range(3)])

        op = map(
            command=with_breakpoint("cat ; BREAKPOINT"),
            in_="//tmp/in",
            out="//tmp/out",
            spec={
                "mapper": {
                    "gpu_limit": 1,
                    "enable_gpu_layers": False,
                }
            },
            track=False,
        )

        wait_breakpoint()

        jobs = op.get_running_jobs()
        assert len(jobs) == 1
        assert next(iter(jobs.values()))["address"] == gpu_node

    @authors("ignat")
    def test_min_share_resources(self):
        create_pool("gpu_pool", attributes={"min_share_resources": {"gpu": 1}})
        wait(lambda: get(scheduler_orchid_pool_path("gpu_pool") + "/min_share_resources/gpu") == 1)
        wait(lambda: get(scheduler_orchid_pool_path("gpu_pool") + "/min_share_ratio") == 0.25)

    @authors("ignat")
    def test_packing(self):
        def check_gpus(op, gpu_indexes):
            jobs = op.get_running_jobs()
            if len(jobs) != 1:
                return False
            assert next(iter(jobs.values()))["address"] == gpu_node
            job_info = get(
                "//sys/cluster_nodes/{}/orchid/exec_node/job_controller/active_jobs/{}".format(gpu_node, next(iter(jobs.keys())))
            )
            job_gpu_indexes = sorted([device["device_number"] for device in job_info["exec_attributes"]["gpu_devices"]])
            if job_gpu_indexes != sorted(gpu_indexes):
                return False
            return True

        gpu_nodes = [
            node
            for node in ls("//sys/cluster_nodes")
            if get("//sys/cluster_nodes/{}/@resource_limits/gpu".format(node)) > 0
        ]
        assert len(gpu_nodes) == 1
        gpu_node = gpu_nodes[0]

        create("table", "//tmp/in")
        create("table", "//tmp/out1")
        create("table", "//tmp/out2")
        write_table("//tmp/in", [{"foo": i} for i in range(3)])

        op1 = map(
            command=with_breakpoint("cat ; BREAKPOINT"),
            in_="//tmp/in",
            out="//tmp/out1",
            spec={
                "mapper": {
                    "gpu_limit": 1,
                    "enable_gpu_layers": False,
                }
            },
            track=False,
        )

        wait(lambda: get("//sys/cluster_nodes/{}/@resource_usage/gpu".format(gpu_node)) == 1)

        wait(lambda: check_gpus(op1, [0]))

        op2 = map(
            command=with_breakpoint("cat ; BREAKPOINT"),
            in_="//tmp/in",
            out="//tmp/out2",
            spec={
                "mapper": {
                    "gpu_limit": 2,
                    "enable_gpu_layers": False,
                }
            },
            track=False,
        )

        wait(lambda: get("//sys/cluster_nodes/{}/@resource_usage/gpu".format(gpu_node)) == 3)

        wait(lambda: check_gpus(op2, [2, 3]))


###############################################################################################


@pytest.mark.enabled_multidaemon
class TestPorts(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "start_port": 20000,
            "port_count": 3,
            "resource_limits": {"user_slots": 2, "cpu": 2},
        },
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_controller": {
                    "waiting_jobs_timeout": 1000,
                },
            },
        },
    }

    @authors("ignat")
    def test_simple(self):
        create("table", "//tmp/t_in", attributes={"replication_factor": 1})
        write_table("//tmp/t_in", [{"a": 0}])

        create("table", "//tmp/t_out", attributes={"replication_factor": 1})
        create("table", "//tmp/t_out_other", attributes={"replication_factor": 1})

        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command=with_breakpoint(
                "echo $YT_PORT_0 >&2; echo $YT_PORT_1 >&2; "
                "if [ -n \"$YT_PORT_2\" ]; then echo 'FAILED' >&2; fi; cat; BREAKPOINT"
            ),
            spec={
                "mapper": {
                    "port_count": 2,
                }
            },
        )

        jobs = wait_breakpoint()
        assert len(jobs) == 1

        # Not enough ports
        with pytest.raises(YtError):
            map(
                in_="//tmp/t_in",
                out="//tmp/t_out_other",
                command="cat",
                spec={
                    "mapper": {
                        "port_count": 2,
                    },
                    "max_failed_job_count": 1,
                    "fail_on_job_restart": True,
                },
            )

        release_breakpoint()
        op.track()

        stderr = op.read_stderr(jobs[0])
        assert b"FAILED" not in stderr
        ports = list(builtins.map(int, stderr.split()))
        assert len(ports) == 2
        assert ports[0] != ports[1]

        assert all(port >= 20000 and port < 20003 for port in ports)

        map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="echo $YT_PORT_0 >&2; echo $YT_PORT_1 >&2; "
                    "if [ -n \"$YT_PORT_2\" ]; then echo 'FAILED' >&2; fi; cat",
            spec={
                "mapper": {
                    "port_count": 2,
                }
            },
        )

        jobs = op.list_jobs()
        assert len(jobs) == 1

        stderr = op.read_stderr(jobs[0])
        assert b"FAILED" not in stderr
        ports = list(builtins.map(int, stderr.split()))
        assert len(ports) == 2
        assert ports[0] != ports[1]

        assert all(port >= 20000 and port < 20003 for port in ports)

    @authors("max42")
    def test_preliminary_bind(self):
        create("table", "//tmp/t_in", attributes={"replication_factor": 1})
        create("table", "//tmp/t_out", attributes={"replication_factor": 1})
        write_table("//tmp/t_in", [{"a": 1}])

        server_socket = None
        try:
            try:
                server_socket = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
                server_socket.bind(("::1", 20001))
            except Exception as err:
                pytest.skip("Caught following exception while trying to bind to port 20001: {}".format(err))
                return

            # We run test several times to make sure that ports did not stuck inside node.
            for iteration in range(3):
                if iteration in [0, 1]:
                    expected_ports = [{"port": 20000}, {"port": 20002}]
                else:
                    server_socket.close()
                    server_socket = None
                    expected_ports = [{"port": 20000}, {"port": 20001}]

                map(
                    in_="//tmp/t_in",
                    out="//tmp/t_out",
                    command='echo "{port=$YT_PORT_0}; {port=$YT_PORT_1}"',
                    spec={
                        "mapper": {
                            "port_count": 2,
                            "format": "yson",
                        }
                    },
                )

                ports = read_table("//tmp/t_out")
                assert ports == expected_ports
        finally:
            if server_socket is not None:
                server_socket.close()
