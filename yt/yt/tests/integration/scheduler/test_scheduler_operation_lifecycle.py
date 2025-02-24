from yt_env_setup import (
    YTEnvSetup,
    Restarter,
    is_asan_build,
    SCHEDULERS_SERVICE,
    CONTROLLER_AGENTS_SERVICE,
)

from yt_commands import (
    authors, print_debug, wait, wait_no_assert, wait_breakpoint, release_breakpoint, with_breakpoint,
    create, ls, get,
    set, remove, exists, create_account, create_tmpdir, create_user, create_pool, create_pool_tree,
    start_transaction, abort_transaction,
    lookup_rows, read_table, write_table, map, reduce, sort,
    run_test_vanilla, run_sleeping_vanilla,
    abort_job, get_job, get_job_fail_context, list_jobs, list_operations, get_operation, clean_operations,
    abandon_job, sync_create_cells, update_controller_agent_config, update_scheduler_config,
    set_all_nodes_banned, PrepareTables, sorted_dicts)

from yt_scheduler_helpers import (
    scheduler_orchid_operation_path,
    scheduler_orchid_pool_path,
    scheduler_orchid_default_pool_tree_path)

from yt_helpers import JobCountProfiler, get_current_time, parse_yt_time, profiler_factory, get_job_count_profiling

from yt.yson import YsonEntity
from yt.common import uuid_to_parts, YtResponseError, YtError, datetime_to_string
from yt.test_helpers import are_almost_equal
import yt.environment.init_operations_archive as init_operations_archive
from yt.environment import arcadia_interop

import pytest
from flaky import flaky

import shutil
import time
import subprocess
import os.path
from datetime import datetime, timedelta

##################################################################


class TestSchedulerFunctionality(YTEnvSetup, PrepareTables):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_TEST_PARTITIONS = 4
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "connect_retry_backoff_time": 100,
            "fair_share_update_period": 100,
            "profiling_update_period": 100,
            "fair_share_profiling_period": 100,
            "alerts_update_period": 100,
            # Unrecognized alert often interferes with the alerts that
            # are tested in this test suite.
            "enable_unrecognized_alert": False,
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operation_time_limit": 1000000,
            "operation_time_limit_check_period": 100,
            "snapshot_period": 500,
            "snapshot_writer": {
                "upload_replication_factor": 1,
                "min_upload_replication_factor": 1,
            },
        }
    }

    USE_PORTO = True

    @authors("ignat")
    def test_connection_time(self):
        connection_time_attr = parse_yt_time(get("//sys/scheduler/@connection_time"))
        connection_time_orchid = parse_yt_time(get("//sys/scheduler/orchid/scheduler/service/last_connection_time"))
        assert connection_time_orchid - connection_time_attr < timedelta(seconds=2)

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        new_connection_time_attr = parse_yt_time(get("//sys/scheduler/@connection_time"))
        new_connection_time_orchid = parse_yt_time(get("//sys/scheduler/orchid/scheduler/service/last_connection_time"))

        assert new_connection_time_attr > connection_time_attr
        assert new_connection_time_orchid > connection_time_orchid

    @authors("ignat")
    @flaky(max_runs=3)
    def test_revive(self):
        def get_connection_time():
            return parse_yt_time(get("//sys/scheduler/@connection_time"))

        self._prepare_tables()

        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="echo '{foo=bar}'; sleep 4",
        )

        time.sleep(3)

        assert get_current_time() - get_connection_time() > timedelta(seconds=3)

        op.wait_for_fresh_snapshot()

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        assert get_current_time() - get_connection_time() < timedelta(seconds=3)

        op.track()

        # Check clean start in events.
        found_pending = False
        events = get_operation(op.id, attributes=["events"])["events"]
        for event in reversed(events):
            if event["state"] == "pending":
                assert "attributes" in event
                assert "revived_from_snapshot" in event["attributes"]
                assert event["attributes"]["revived_from_snapshot"]
                found_pending = True
                break
        assert found_pending

        assert read_table("//tmp/t_out") == [{"foo": "bar"}]

    @authors("ignat")
    def test_banned_operation(self):
        self._prepare_tables()

        self._create_table("//tmp/t_out1")
        self._create_table("//tmp/t_out2")

        op1 = map(track=False, in_="//tmp/t_in", out="//tmp/t_out1", command="sleep 1000")
        op2 = map(track=False, in_="//tmp/t_in", out="//tmp/t_out2", command="sleep 1000")
        op1.ensure_running()
        op2.ensure_running()

        set(op1.get_path() + "/@banned", True)

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        wait(lambda: ls("//sys/scheduler/orchid/scheduler/operations") == [op2.id])

    @authors("ignat")
    @pytest.mark.timeout(150)
    def test_disconnect_during_revive(self):
        op_count = 10

        self._create_table("//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})
        for i in range(1, op_count + 1):
            self._create_table("//tmp/t_out" + str(i))

        ops = []
        for i in range(1, op_count):
            ops.append(
                map(
                    track=False,
                    # Sleep is necessary since we not support revive for completing operations.
                    command="sleep 3; cat",
                    in_=["//tmp/t_in"],
                    out="//tmp/t_out" + str(i),
                )
            )

        for i in range(10):
            while True:
                scheduler_locks = get("//sys/scheduler/lock/@locks", verbose=False)
                if len(scheduler_locks) > 0:
                    scheduler_transaction = scheduler_locks[0]["transaction_id"]
                    abort_transaction(scheduler_transaction)
                    break
                time.sleep(0.01)

        for op in ops:
            op.track()

        for i in range(1, op_count):
            assert read_table("//tmp/t_out" + str(i)) == [{"foo": "bar"}]

    @authors("ignat")
    def test_user_transaction_abort_when_scheduler_is_down(self):
        self._prepare_tables()

        transaction_id = start_transaction(timeout=300 * 1000)
        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="echo '{foo=bar}'; sleep 50",
            transaction_id=transaction_id,
        )

        wait(lambda: op.get_job_count("running") == 1)

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            abort_transaction(transaction_id)

        with pytest.raises(YtError):
            op.track()

    @authors("ignat")
    def test_scheduler_transaction_abort_when_scheduler_is_down(self):
        self._prepare_tables()

        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="echo '{foo=bar}'; sleep 3",
        )

        time.sleep(2)

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            abort_transaction(get(op.get_path() + "/@input_transaction_id"))
            abort_transaction(get(op.get_path() + "/@output_transaction_id"))

        op.track()

        assert read_table("//tmp/t_out") == [{"foo": "bar"}]

    @authors("ignat")
    def test_scheduler_operation_abort_when_operation_is_orphaned(self):
        self._prepare_tables()

        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="echo '{foo=bar}'; sleep 100",
        )

        wait(lambda: op.get_state() == "running")

        input_tx = get(op.get_path() + "/@input_transaction_id")
        assert exists("#" + input_tx)

        update_scheduler_config("testing_options/handle_orphaned_operations_delay", 5000)

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            assert exists("#" + input_tx)

        wait(lambda: op.get_state() == "orphaned")

        op.abort()

        assert not exists("#" + input_tx)

    @authors("ignat")
    def test_suspend_during_revive(self):
        update_controller_agent_config("enable_snapshot_loading", False)

        self._create_table("//tmp/in")
        self._create_table("//tmp/out")
        write_table("//tmp/in", [{"foo": i} for i in range(5)])

        op = map(track=False, command="sleep 1000", in_=["//tmp/in"], out="//tmp/out")
        op.ensure_running()

        op.suspend()
        wait(lambda: get(op.get_path() + "/@suspended"))

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        op.ensure_running()

        wait(lambda: op.get_job_count("running") == 0)

        assert get(op.get_path() + "/@suspended")

        op.resume()
        wait(lambda: op.get_job_count("running") == 1)

    @authors("ignat")
    def test_operation_time_limit(self):
        self._create_table("//tmp/in")
        self._create_table("//tmp/out")

        write_table("//tmp/in", [{"foo": i} for i in range(5)])

        # Operation specific time limit.
        op2 = map(
            track=False,
            command="sleep 3.0; cat >/dev/null",
            in_=["//tmp/in"],
            out="//tmp/out",
            spec={"time_limit": 1000},
        )

        wait(lambda: op2.get_state() == "failed")

    @authors("ignat")
    def test_operation_suspend_with_account_limit_exceeded(self):
        create_account("limited")
        set("//sys/accounts/limited/@resource_limits/chunk_count", 1)

        self._create_table("//tmp/in")
        self._create_table("//tmp/out")
        set("//tmp/out/@account", "limited")
        write_table("//tmp/in", [{"foo": i} for i in range(3)])

        op = map(
            track=False,
            command="sleep $YT_JOB_INDEX; cat",
            in_=["//tmp/in"],
            out="//tmp/out",
            spec={
                "data_size_per_job": 1,
                "suspend_operation_if_account_limit_exceeded": True,
            },
        )

        wait(lambda: get(op.get_path() + "/@suspended"), iter=100, sleep_backoff=0.6)

        assert op.get_state() == "running"

        wait(lambda: len(get(op.get_path() + "/@alerts")) > 0)
        assert list(get(op.get_path() + "/@alerts")) == ["operation_suspended"]

        set("//sys/accounts/limited/@resource_limits/chunk_count", 10)
        op.resume()
        op.track()

        assert op.get_state() == "completed"
        assert not get(op.get_path() + "/@suspended")
        assert not get(op.get_path() + "/@alerts")

    @authors("max42")
    def test_suspend_operation_after_materialization(self):
        self._create_table("//tmp/in")
        self._create_table("//tmp/out")
        write_table("//tmp/in", [{"foo": 0}])

        op = map(
            track=False,
            command="cat",
            in_="//tmp/in",
            out="//tmp/out",
            spec={
                "data_size_per_job": 1,
                "suspend_operation_after_materialization": True,
            },
        )
        wait(lambda: get(op.get_path() + "/@suspended"))
        op.resume()
        op.track()

    @authors("ignat")
    @flaky(max_runs=3)
    def test_fail_context_saved_on_time_limit(self):
        self._create_table("//tmp/in")
        self._create_table("//tmp/out")

        write_table("//tmp/in", [{"foo": i} for i in range(5)])

        op = map(
            track=False,
            command=with_breakpoint("BREAKPOINT"),
            in_=["//tmp/in"],
            out="//tmp/out",
        )

        wait_breakpoint()
        update_controller_agent_config("operation_time_limit", 100)

        wait(lambda: op.get_state() == "failed")
        wait(lambda: op.list_jobs())

        jobs = list_jobs(op.id)["jobs"]
        for job in jobs:
            if job["state"] == "aborted":
                assert len(get_job_fail_context(op.id, job["id"])) > 0

    # Test is flaky by the next reason: schedule job may fail by some reason (chunk list demand is not met, et.c)
    # and in this case we can successfully schedule job for the next operation in queue.
    @authors("ignat")
    @flaky(max_runs=3)
    def test_fifo_default(self):
        self._create_table("//tmp/in")
        self._create_table("//tmp/out1")
        self._create_table("//tmp/out2")
        self._create_table("//tmp/out3")
        write_table("//tmp/in", [{"foo": i} for i in range(5)])

        create_pool("fifo_pool", ignore_existing=True)
        set("//sys/pools/fifo_pool/@mode", "fifo")

        pools_orchid = scheduler_orchid_default_pool_tree_path() + "/pools"
        wait(lambda: exists(pools_orchid + "/fifo_pool"))
        wait(lambda: get(pools_orchid + "/fifo_pool/mode") == "fifo")

        ops = []
        for i in range(1, 4):
            ops.append(
                map(
                    track=False,
                    command="sleep 3; cat >/dev/null",
                    in_=["//tmp/in"],
                    out="//tmp/out" + str(i),
                    spec={"pool": "fifo_pool"},
                )
            )

        for op in ops:
            op.track()

        finish_times = [get(op.get_path() + "/@finish_time") for op in ops]
        for cur, next in zip(finish_times, finish_times[1:]):
            assert cur < next

    # Test is flaky by the next reason: schedule job may fail by some reason (chunk list demand is not met, et.c)
    # and in this case we can successfully schedule job for the next operation in queue.
    @pytest.mark.timeout(120)
    @authors("ignat")
    @flaky(max_runs=3)
    def test_fifo_by_pending_job_count(self):
        op_count = 3

        for i in range(1, op_count + 1):
            self._create_table("//tmp/in" + str(i))
            self._create_table("//tmp/out" + str(i))
            write_table(
                "//tmp/in" + str(i),
                [{"foo": j} for j in range(op_count * (op_count + 1 - i))],
            )

        create_pool("fifo_pool", ignore_existing=True)
        set("//sys/pools/fifo_pool/@mode", "fifo")
        set("//sys/pools/fifo_pool/@fifo_sort_parameters", ["pending_job_count"])

        # Wait until pools tree would be updated
        time.sleep(0.6)

        ops = []
        for i in range(1, op_count + 1):
            ops.append(
                map(
                    track=False,
                    command="sleep 1.0; cat >/dev/null",
                    in_=["//tmp/in" + str(i)],
                    out="//tmp/out" + str(i),
                    spec={"pool": "fifo_pool", "data_size_per_job": 1},
                )
            )

        time.sleep(1.0)
        for index, op in enumerate(ops):
            assert op.get_runtime_progress("scheduling_info_per_pool_tree/default/fifo_index", default=-1) == 2 - index

        for op in reversed(ops):
            op.track()

        finish_times = [get(op.get_path() + "/@finish_time") for op in ops]
        for cur, next in zip(finish_times, finish_times[1:]):
            assert cur > next

    @authors("ignat")
    def test_preparing_operation_transactions(self):
        self._prepare_tables()

        set_all_nodes_banned(True)
        op = sort(track=False, in_="//tmp/t_in", out="//tmp/t_in", sort_by=["foo"])
        time.sleep(2)

        for tx in ls("//sys/transactions", attributes=["operation_id"]):
            if tx.attributes.get("operation_id", "") == op.id:
                for i in range(10):
                    try:
                        abort_transaction(tx)
                    except YtResponseError as err:
                        if err.is_no_such_transaction():
                            break
                        if i == 9:
                            raise

        with pytest.raises(YtError):
            op.track()

        set_all_nodes_banned(False)

    @authors("ignat")
    def test_abort_custom_error_message(self):
        self._prepare_tables()

        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="echo '{foo=bar}'; sleep 3",
        )
        op.abort(abort_message="Test abort")

        assert op.get_state() == "aborted"
        assert get(op.get_path() + "/@result/error/inner_errors/0/message") == "Test abort"

    @authors("ignat")
    def test_operation_pool_attributes(self):
        self._prepare_tables()

        op = map(in_="//tmp/t_in", out="//tmp/t_out", command="cat")
        assert get(op.get_path() + "/@runtime_parameters/scheduling_options_per_pool_tree/default/pool") == "root"

    @authors("babenko", "gritukan")
    def test_operation_events_attribute(self):
        op = run_test_vanilla(with_breakpoint("BREAKPOINT"))

        op.wait_for_fresh_snapshot()

        wait_breakpoint()

        events = get(op.get_path() + "/@events")
        assert [
            "starting",
            "waiting_for_agent",
            "initializing",
            "preparing",
            "pending",
            "materializing",
            "running",
        ] == [event["state"] for event in events]

        def event_contains_agent_address(event):
            if "attributes" not in event:
                return False
            if "controller_agent_address" not in event["attributes"]:
                return False
            return len(event["attributes"]["controller_agent_address"]) > 0

        assert any(event_contains_agent_address(event) for event in events)

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        wait(lambda: len(get(op.get_path() + "/@events")) == 14)
        events = get(op.get_path() + "/@events")
        assert [
            "starting",
            "waiting_for_agent",
            "initializing",
            "preparing",
            "pending",
            "materializing",
            "running",
            "orphaned",
            "waiting_for_agent",
            "revive_initializing",
            "reviving",
            "pending",
            "reviving_jobs",
            "running",
        ] == [event["state"] for event in events]
        assert sum(event_contains_agent_address(event) for event in events) == 2

        release_breakpoint()
        op.track()

        events = get(op.get_path() + "/@events")
        assert [
            "starting",
            "waiting_for_agent",
            "initializing",
            "preparing",
            "pending",
            "materializing",
            "running",
            "orphaned",
            "waiting_for_agent",
            "revive_initializing",
            "reviving",
            "pending",
            "reviving_jobs",
            "running",
            "completing",
            "completed",
        ] == [event["state"] for event in events]
        assert sum(event_contains_agent_address(event) for event in events) == 2

    @authors("ignat")
    def test_exceed_job_time_limit(self):
        self._prepare_tables()

        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="sleep 10 ; cat",
            spec={"max_failed_job_count": 1, "mapper": {"job_time_limit": 2000}},
        )

        # if all jobs failed then operation is also failed
        with pytest.raises(YtError):
            op.track()

        for job_id in op.list_jobs():
            inner_errors = get_job(op.id, job_id)["error"]["inner_errors"]
            assert "Job time limit exceeded" in inner_errors[0]["message"]

    @authors("ignat")
    @flaky(max_runs=3)
    def test_within_job_time_limit(self):
        self._prepare_tables()
        map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="sleep 1 ; cat",
            spec={"max_failed_job_count": 1, "mapper": {"job_time_limit": 3000}},
        )

    @authors("ignat")
    def test_suspend_resume(self):
        self._create_table("//tmp/t_in")
        self._create_table("//tmp/t_out")
        write_table("//tmp/t_in", [{"foo": i} for i in range(10)])

        op = map(
            track=False,
            command="sleep 1; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"data_size_per_job": 1},
        )

        for i in range(5):
            time.sleep(0.5)
            op.suspend(abort_running_jobs=True)
            time.sleep(0.5)
            op.resume()

        for i in range(5):
            op.suspend()
            op.resume()

        for i in range(5):
            op.suspend(abort_running_jobs=True)
            op.resume()

        op.track()

        assert sorted_dicts(read_table("//tmp/t_out")) == [{"foo": i} for i in range(10)]

    @authors("ignat")
    def test_table_changed_during_operation_prepare(self):
        self._prepare_tables()

        op1 = map(
            track=False,
            in_="//tmp/t_in",
            out="<append=true>//tmp/t_in",
            command="cat",
            spec={
                "testing": {
                    "delay_inside_prepare": 5000,
                }
            },
        )
        wait(lambda: op1.get_state() == "completed")

        assert sorted_dicts(read_table("//tmp/t_in")) == [{"foo": "bar"} for _ in range(2)]

        op2 = map(
            track=False,
            in_="//tmp/t_in",
            out="<append=true>//tmp/t_in",
            command="cat",
            spec={
                "testing": {
                    "delay_inside_prepare": 5000,
                }
            },
        )
        wait(lambda: get("//tmp/t_in/@locks"))
        write_table("<append=true>//tmp/t_in", [{"x": "y"}])
        wait(lambda: op2.get_state() == "failed")

        op3 = map(
            track=False,
            in_="//tmp/t_in",
            out="<append=true>//tmp/t_in",
            command="cat",
            spec={
                "testing": {
                    "delay_inside_prepare": 5000,
                }
            },
        )
        wait(lambda: get("//tmp/t_in/@locks"))
        write_table("//tmp/t_in", [{"x": "y"}])
        wait(lambda: op3.get_state() == "failed")

    @authors("ignat")
    def test_starting_operation(self):
        self._prepare_tables()

        op_response = map(
            track=False,
            in_="//tmp/t_in",
            out="<append=true>//tmp/t_in",
            command="cat",
            spec={
                "testing": {
                    "delay_before_start": 5000,
                }
            },
            return_response=True,
        )

        time.sleep(2)

        scheduler_locks = get("//sys/scheduler/lock/@locks", verbose=False)
        assert len(scheduler_locks) > 0
        scheduler_transaction = scheduler_locks[0]["transaction_id"]
        abort_transaction(scheduler_transaction)

        op_response.wait()
        assert not op_response.is_ok()
        error = YtResponseError(op_response.error())
        assert error.contains_text("Master disconnected")

    @authors("gritukan")
    def test_operation_abort_failed(self):
        self._prepare_tables()

        connection_time = parse_yt_time(get("//sys/scheduler/@connection_time"))

        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="echo '{foo=bar}'; sleep 10000000",
            spec={"testing": {"throw_exception_during_operation_abort": True}},
        )
        wait(lambda: op.get_job_count("running") == 1)

        op.abort()
        wait(lambda: op.get_state() == "aborted")

        # Scheduler should not reconnect.
        new_connection_time = parse_yt_time(get("//sys/scheduler/@connection_time"))
        assert new_connection_time == connection_time

    @authors("eshcherbin")
    def test_invalid_needed_resources_in_materialize(self):
        op = run_sleeping_vanilla(spec={"testing": {"report_invalid_needed_resources_in_materialize": True}})
        op.wait_for_state("failed")

    @authors("eshcherbin")
    def test_invalid_needed_resources_in_revive(self):
        op = run_sleeping_vanilla(spec={"testing": {"report_invalid_needed_resources_in_revive": True}})
        op.wait_for_state("running")
        op.wait_for_fresh_snapshot()

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        op.wait_for_state("failed")


# See: YT-22656.
@pytest.mark.enabled_multidaemon
class TestSuspendStopsSchedulingInCurrentSnapshot(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 100,
            "watchers_update_period": 100,
            "static_orchid_cache_update_period": 100,
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "static_orchid_cache_update_period": 100,
        }
    }

    @authors("escherbin")
    def test_suspend_stops_scheduling_in_current_snapshot(self):
        blocking_op = run_sleeping_vanilla()
        wait(lambda: len(blocking_op.get_running_jobs()) == 1)

        op = run_sleeping_vanilla(job_count=3)
        wait(lambda: len(op.get_running_jobs()) == 2)
        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/schedulable"))

        update_scheduler_config("fair_share_update_period", 60000)

        # Wait 2 times previous update period to make sure there are no more fair share updates in this test.
        time.sleep(0.2)

        op.suspend(abort_running_jobs=False)

        job = list(op.get_running_jobs())[0]
        abort_job(job)

        time.sleep(3.0)

        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/schedulable"))

        wait(lambda: len(op.get_running_jobs()) == 1)

        op.resume()

        wait(lambda: len(op.get_running_jobs()) == 2)


@pytest.mark.enabled_multidaemon
class TestSchedulerProfiling(YTEnvSetup, PrepareTables):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "connect_retry_backoff_time": 100,
            "fair_share_update_period": 100,
            "profiling_update_period": 100,
            "fair_share_profiling_period": 100,
            "alerts_update_period": 100,
            # Unrecognized alert often interferes with the alerts that
            # are tested in this test suite.
            "enable_unrecognized_alert": False,
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operation_time_limit_check_period": 100,
        }
    }

    USE_PORTO = True

    @authors("ignat", "eshcherbin")
    def test_pool_profiling(self):
        create_pool("unique_pool")
        pool_path = "//sys/pools/unique_pool"
        set(pool_path + "/@max_operation_count", 50)
        wait(lambda: get(scheduler_orchid_pool_path("unique_pool") + "/max_operation_count") == 50)
        set(pool_path + "/@max_running_operation_count", 8)
        wait(lambda: get(scheduler_orchid_pool_path("unique_pool") + "/max_running_operation_count") == 8)

        profiler = profiler_factory().at_scheduler(fixed_tags={"tree": "default", "pool": "unique_pool"})

        metric_prefix = "scheduler/pools/"
        dominant_fair_share_sensor = profiler.gauge(metric_prefix + "dominant_fair_share/total")
        dominant_usage_share_sensor = profiler.gauge(metric_prefix + "dominant_usage_share")
        dominant_demand_share_sensor = profiler.gauge(metric_prefix + "dominant_demand_share")
        dominant_promised_fair_share_sensor = profiler.gauge(metric_prefix + "promised_dominant_fair_share")
        weight_sensor = profiler.gauge(metric_prefix + "weight")
        cpu_usage_sensor = profiler.gauge(metric_prefix + "resource_usage/cpu")
        user_slots_usage_sensor = profiler.gauge(metric_prefix + "resource_usage/user_slots")
        cpu_demand_sensor = profiler.gauge(metric_prefix + "resource_demand/cpu")
        user_slots_demand_sensor = profiler.gauge(metric_prefix + "resource_demand/user_slots")
        running_operation_count_sensor = profiler.gauge(metric_prefix + "running_operation_count")
        total_operation_count_sensor = profiler.gauge(metric_prefix + "total_operation_count")
        max_operation_count_sensor = profiler.gauge(metric_prefix + "max_operation_count")
        max_running_operation_count_sensor = profiler.gauge(metric_prefix + "max_running_operation_count")
        finished_operation_count_sensor = profiler.gauge(metric_prefix + "finished_operation_count")
        strong_guarantee_resources_cpu_sensor = profiler.gauge(metric_prefix + "strong_guarantee_resources/cpu")
        strong_guarantee_resources_memory_sensor = profiler.gauge(metric_prefix + "strong_guarantee_resources/user_memory")
        strong_guarantee_resources_user_slots_sensor = profiler.gauge(metric_prefix + "strong_guarantee_resources/user_slots")

        op = run_sleeping_vanilla(spec={"pool": "unique_pool"})

        wait(lambda: dominant_fair_share_sensor.get() == 1.0)
        wait(lambda: dominant_usage_share_sensor.get() == 1.0)
        wait(lambda: dominant_demand_share_sensor.get() == 1.0)
        wait(lambda: dominant_promised_fair_share_sensor.get() == 1.0)
        wait(lambda: weight_sensor.get() == 1.0)
        wait(lambda: cpu_usage_sensor.get() == 1)
        wait(lambda: user_slots_usage_sensor.get() == 1)
        wait(lambda: cpu_demand_sensor.get() == 1)
        wait(lambda: user_slots_demand_sensor.get() == 1)
        wait(lambda: running_operation_count_sensor.get() == 1)
        wait(lambda: total_operation_count_sensor.get() == 1)

        # pool guaranties metrics
        wait(lambda: max_operation_count_sensor.get() == 50)
        wait(lambda: max_running_operation_count_sensor.get() == 8)
        wait(lambda: strong_guarantee_resources_cpu_sensor.get() == 0)
        wait(lambda: strong_guarantee_resources_memory_sensor.get() == 0)
        wait(lambda: strong_guarantee_resources_user_slots_sensor.get() == 0)
        op.complete()

        wait(lambda: finished_operation_count_sensor.get(tags={"state": "completed"}) == 1)
        wait(lambda: finished_operation_count_sensor.get(tags={"state": "failed"}) == 0)
        wait(lambda: finished_operation_count_sensor.get(tags={"state": "aborted"}) == 0)

    @authors("ignat", "eshcherbin")
    def test_operations_by_slot_profiling(self):
        create_pool("some_pool")

        profiler = profiler_factory().at_scheduler(fixed_tags={"tree": "default", "pool": "some_pool"})

        metric_prefix = "scheduler/operations_by_slot/"
        dominant_fair_share_sensor = profiler.gauge(metric_prefix + "dominant_fair_share/total")
        dominant_usage_share_sensor = profiler.gauge(metric_prefix + "dominant_usage_share")
        dominant_demand_share_sensor = profiler.gauge(metric_prefix + "dominant_demand_share")
        dominant_promised_fair_share_sensor = profiler.gauge(metric_prefix + "promised_dominant_fair_share")
        weight_sensor = profiler.gauge(metric_prefix + "weight")
        cpu_usage_sensor = profiler.gauge(metric_prefix + "resource_usage/cpu")
        user_slots_usage_sensor = profiler.gauge(metric_prefix + "resource_usage/user_slots")
        cpu_demand_sensor = profiler.gauge(metric_prefix + "resource_demand/cpu")
        user_slots_demand_sensor = profiler.gauge(metric_prefix + "resource_demand/user_slots")
        scheduling_index_sensor = profiler.gauge(metric_prefix + "scheduling_index")

        tags1 = {"slot_index": "0"}
        tags2 = {"slot_index": "1"}

        accumulated_resource_usage_cpu_sensors = dict()
        accumulated_resource_usage_user_slots_sensors = dict()
        operation_schedule_job_attempt_count_sensor = dict()

        for name, tags in (("tags1", tags1), ("tags2", tags2)):
            accumulated_resource_usage_cpu_sensors[name] = profiler.counter(metric_prefix + "accumulated_resource_usage/cpu", tags=tags)
            accumulated_resource_usage_user_slots_sensors[name] = profiler.counter(metric_prefix + "accumulated_resource_usage/user_slots", tags=tags)
            operation_schedule_job_attempt_count_sensor[name] = profiler.counter(metric_prefix + "schedule_job_attempt_count", tags=tags)

        op1 = run_sleeping_vanilla(spec={"pool": "some_pool"})
        wait(lambda: op1.get_job_count("running") == 1)
        op2 = run_sleeping_vanilla(spec={"pool": "some_pool"})
        op2.wait_for_state("running")

        def get_slot_index(op):
            return op.get_runtime_progress("scheduling_info_per_pool_tree/default/slot_index", default=-1)

        wait(lambda: get_slot_index(op1) == int(tags1["slot_index"]))
        wait(lambda: get_slot_index(op2) == int(tags2["slot_index"]))

        wait(lambda: are_almost_equal(dominant_fair_share_sensor.get(tags=tags1), 0.5))
        wait(lambda: dominant_usage_share_sensor.get(tags=tags1) == 1.0)
        wait(lambda: dominant_demand_share_sensor.get(tags=tags1) == 1.0)
        wait(lambda: are_almost_equal(dominant_promised_fair_share_sensor.get(tags=tags1), 0.5))
        wait(lambda: weight_sensor.get(tags=tags1) == 1.0)
        wait(lambda: cpu_usage_sensor.get(tags=tags1) == 1)
        wait(lambda: user_slots_usage_sensor.get(tags=tags1) == 1)
        wait(lambda: cpu_demand_sensor.get(tags=tags1) == 1)
        wait(lambda: user_slots_demand_sensor.get(tags=tags1) == 1)
        wait(lambda: scheduling_index_sensor.get(tags=tags1) == -1)
        # Some non-trivial lower bound on resource consumption.
        wait(lambda: accumulated_resource_usage_cpu_sensors["tags1"].get_delta() > 2.0)
        wait(lambda: accumulated_resource_usage_user_slots_sensors["tags1"].get_delta() > 2.0)

        wait(lambda: operation_schedule_job_attempt_count_sensor["tags1"].get_delta() >= 1.0)

        wait(lambda: are_almost_equal(dominant_fair_share_sensor.get(tags=tags2), 0.5))
        wait(lambda: dominant_usage_share_sensor.get(tags=tags2) == 0)
        wait(lambda: dominant_demand_share_sensor.get(tags=tags2) == 1.0)
        wait(lambda: are_almost_equal(dominant_promised_fair_share_sensor.get(tags=tags2), 0.5))
        wait(lambda: weight_sensor.get(tags=tags2) == 1.0)
        wait(lambda: cpu_usage_sensor.get(tags=tags2) == 0)
        wait(lambda: user_slots_usage_sensor.get(tags=tags2) == 0)
        wait(lambda: cpu_demand_sensor.get(tags=tags2) == 1)
        wait(lambda: user_slots_demand_sensor.get(tags=tags2) == 1)
        wait(lambda: scheduling_index_sensor.get(tags=tags2) == 0)
        wait(lambda: accumulated_resource_usage_cpu_sensors["tags2"].get_delta() == 0.0)
        wait(lambda: accumulated_resource_usage_user_slots_sensors["tags2"].get_delta() == 0.0)

        op1.abort(wait_until_finished=True)

        wait(lambda: dominant_fair_share_sensor.get(tags=tags2) == 1.0)
        wait(lambda: dominant_usage_share_sensor.get(tags=tags2) == 1.0)
        wait(lambda: dominant_demand_share_sensor.get(tags=tags2) == 1.0)
        wait(lambda: dominant_promised_fair_share_sensor.get(tags=tags2) == 1.0)
        wait(lambda: weight_sensor.get(tags=tags2) == 1.0)

        wait(lambda: operation_schedule_job_attempt_count_sensor["tags2"].get_delta() == 1.0)

    @authors("ignat", "eshcherbin")
    def test_operations_by_user_profiling(self):
        create_user("ignat")
        create_user("egor")

        create_pool("some_pool")
        create_pool("other_pool", attributes={"allowed_profiling_tags": ["hello", "world"]})

        profiler = profiler_factory().at_scheduler(fixed_tags={"tree": "default"})

        metric_prefix = "scheduler/operations_by_user/"
        dominant_fair_share_sensor = profiler.gauge(metric_prefix + "dominant_fair_share/total")
        dominant_usage_share_sensor = profiler.gauge(metric_prefix + "dominant_usage_share")
        dominant_demand_share_sensor = profiler.gauge(metric_prefix + "dominant_demand_share")
        dominant_promised_fair_share_sensor = profiler.gauge(metric_prefix + "promised_dominant_fair_share")
        weight_sensor = profiler.gauge(metric_prefix + "weight")
        cpu_usage_sensor = profiler.gauge(metric_prefix + "resource_usage/cpu")
        user_slots_usage_sensor = profiler.gauge(metric_prefix + "resource_usage/user_slots")
        cpu_demand_sensor = profiler.gauge(metric_prefix + "resource_demand/cpu")
        user_slots_demand_sensor = profiler.gauge(metric_prefix + "resource_demand/user_slots")

        tags = {"pool": "some_pool", "user_name": "ignat"}

        operation_schedule_job_attempt_count_sensor = profiler.counter(metric_prefix + "schedule_job_attempt_count", tags=tags)

        op1 = run_sleeping_vanilla(
            spec={"pool": "some_pool", "custom_profiling_tag": "hello"},
            authenticated_user="ignat",
        )
        wait(lambda: op1.get_job_count("running") == 1)
        op2 = run_sleeping_vanilla(
            spec={"pool": "other_pool", "custom_profiling_tag": "world"},
            authenticated_user="egor",
        )
        wait(lambda: op2.get_state() == "running")
        op3 = run_sleeping_vanilla(
            spec={"pool": "other_pool", "custom_profiling_tag": "hello"},
            authenticated_user="egor",
        )
        wait(lambda: op3.get_state() == "running")
        op4 = run_sleeping_vanilla(
            spec={"pool": "other_pool", "custom_profiling_tag": "hello"},
            authenticated_user="egor",
        )
        wait(lambda: op4.get_state() == "running")

        wait(lambda: are_almost_equal(dominant_fair_share_sensor.get(tags=tags), 0.5))
        wait(lambda: dominant_usage_share_sensor.get(tags=tags) == 1.0)
        wait(lambda: dominant_demand_share_sensor.get(tags=tags) == 1.0)
        wait(lambda: are_almost_equal(dominant_promised_fair_share_sensor.get(tags=tags), 0.5))
        wait(lambda: weight_sensor.get(tags=tags) == 1.0)
        wait(lambda: cpu_usage_sensor.get(tags=tags) == 1)
        wait(lambda: user_slots_usage_sensor.get(tags=tags) == 1)
        wait(lambda: cpu_demand_sensor.get(tags=tags) == 1)
        wait(lambda: user_slots_demand_sensor.get(tags=tags) == 1)
        wait(lambda: operation_schedule_job_attempt_count_sensor.get_delta() == 1.0)

        def get_total_metric_by_users(metric, tags_):
            return sum(metric.get(tags=dict({"user_name": user}, **tags_), default=0) for user in ("ignat", "egor"))

        def get_total_counter_metric_by_users(counter):
            return sum(counter[user].get_delta() for user in ("ignat", "egor"))

        def create_operation_schedule_job_attempt_count_sensor(tags_):
            return {
                "ignat": profiler.counter(metric_prefix + "schedule_job_attempt_count", tags=dict({"user_name": "ignat"}, **tags_)),
                "egor": profiler.counter(metric_prefix + "schedule_job_attempt_count", tags=dict({"user_name": "egor"}, **tags_)),
            }

        tags = {"pool": "other_pool", "custom": "hello"}

        operation_schedule_job_attempt_count_sensor = create_operation_schedule_job_attempt_count_sensor(tags)

        wait(lambda: are_almost_equal(get_total_metric_by_users(dominant_fair_share_sensor, tags), 1.0 / 3.0))
        wait(lambda: get_total_metric_by_users(dominant_usage_share_sensor, tags) == 0)
        wait(lambda: get_total_metric_by_users(dominant_demand_share_sensor, tags) == 2.0)
        wait(lambda: are_almost_equal(get_total_metric_by_users(dominant_promised_fair_share_sensor, tags), 1.0 / 3.0))
        wait(lambda: get_total_metric_by_users(weight_sensor, tags) == 2.0)
        wait(lambda: get_total_metric_by_users(cpu_usage_sensor, tags) == 0)
        wait(lambda: get_total_metric_by_users(user_slots_usage_sensor, tags) == 0)
        wait(lambda: get_total_metric_by_users(cpu_demand_sensor, tags) == 2)
        wait(lambda: get_total_metric_by_users(user_slots_demand_sensor, tags) == 2)
        wait(lambda: get_total_counter_metric_by_users(operation_schedule_job_attempt_count_sensor) >= 0.0)

        tags = {"pool": "other_pool", "custom": "world"}

        operation_schedule_job_attempt_count_sensor = create_operation_schedule_job_attempt_count_sensor(tags)

        wait(lambda: are_almost_equal(get_total_metric_by_users(dominant_fair_share_sensor, tags), 1.0 / 6.0))
        wait(lambda: get_total_metric_by_users(dominant_usage_share_sensor, tags) == 0)
        wait(lambda: get_total_metric_by_users(dominant_demand_share_sensor, tags) == 1.0)
        wait(lambda: are_almost_equal(get_total_metric_by_users(dominant_promised_fair_share_sensor, tags), 1.0 / 6.0))
        wait(lambda: get_total_metric_by_users(weight_sensor, tags) == 1.0)
        wait(lambda: get_total_metric_by_users(cpu_usage_sensor, tags) == 0)
        wait(lambda: get_total_metric_by_users(user_slots_usage_sensor, tags) == 0)
        wait(lambda: get_total_metric_by_users(cpu_demand_sensor, tags) == 1)
        wait(lambda: get_total_metric_by_users(user_slots_demand_sensor, tags) == 1)
        wait(lambda: get_total_counter_metric_by_users(operation_schedule_job_attempt_count_sensor) == 0.0)

        tags = {"pool": "other_pool", "user_name": "egor"}

        operation_schedule_job_attempt_count_sensor = profiler.counter(metric_prefix + "schedule_job_attempt_count", tags=tags)

        wait(lambda: are_almost_equal(dominant_fair_share_sensor.get(tags=tags), 0.5))
        wait(lambda: dominant_usage_share_sensor.get(tags=tags) == 0)
        wait(lambda: dominant_demand_share_sensor.get(tags=tags) == 3.0)
        wait(lambda: are_almost_equal(dominant_promised_fair_share_sensor.get(tags=tags), 0.5))
        wait(lambda: weight_sensor.get(tags=tags) == 3.0)
        wait(lambda: cpu_usage_sensor.get(tags=tags) == 0)
        wait(lambda: user_slots_usage_sensor.get(tags=tags) == 0)
        wait(lambda: cpu_demand_sensor.get(tags=tags) == 3)
        wait(lambda: user_slots_demand_sensor.get(tags=tags) == 3)
        wait(lambda: operation_schedule_job_attempt_count_sensor.get_delta() == 0.0)

        op4.abort(wait_until_finished=True)
        op3.abort(wait_until_finished=True)
        op1.abort(wait_until_finished=True)

        wait(lambda: dominant_fair_share_sensor.get(tags=tags) == 1.0)
        wait(lambda: dominant_usage_share_sensor.get(tags=tags) == 1.0)
        wait(lambda: dominant_demand_share_sensor.get(tags=tags) == 1.0)
        wait(lambda: are_almost_equal(dominant_promised_fair_share_sensor.get(tags=tags), 0.5))
        wait(lambda: weight_sensor.get(tags=tags) == 1.0)
        wait(lambda: operation_schedule_job_attempt_count_sensor.get_delta() == 1.0)

        tags = {"pool": "other_pool", "custom": "world"}

        operation_schedule_job_attempt_count_sensor = create_operation_schedule_job_attempt_count_sensor(tags)

        wait(lambda: get_total_metric_by_users(dominant_fair_share_sensor, tags) == 1.0)
        wait(lambda: get_total_metric_by_users(dominant_usage_share_sensor, tags) == 1.0)
        wait(lambda: get_total_metric_by_users(dominant_demand_share_sensor, tags) == 1.0)
        wait(lambda: are_almost_equal(get_total_metric_by_users(dominant_promised_fair_share_sensor, tags), 0.5))
        wait(lambda: get_total_metric_by_users(weight_sensor, tags) == 1.0)
        wait(lambda: get_total_counter_metric_by_users(operation_schedule_job_attempt_count_sensor) == 0.0)

    @authors("ignat", "eshcherbin")
    def test_job_count_profiling(self):
        self._prepare_tables()

        start_profiling = get_job_count_profiling()

        def get_new_jobs_with_state(state):
            current_profiling = get_job_count_profiling()
            return current_profiling["state"][state] - start_profiling["state"][state]

        op = map(
            track=False,
            command=with_breakpoint("echo '{foo=bar}'; BREAKPOINT"),
            in_=["//tmp/t_in"],
            out="//tmp/t_out",
        )

        wait(lambda: get_new_jobs_with_state("running") == 1)

        for job in op.get_running_jobs():
            abort_job(job)

        wait(lambda: get_new_jobs_with_state("aborted") == 1)

        release_breakpoint()
        op.track()

        wait(lambda: get_new_jobs_with_state("completed") == 1)

        assert op.get_state() == "completed"

    @authors("eshcherbin")
    def test_aborted_job_count_profiling_per_tree(self):
        default_tree_aborted_job_profiler = JobCountProfiler("aborted", tags={"tree": "default"})
        other_tree_aborted_job_profiler = JobCountProfiler("aborted", tags={"tree": "other"})

        op1 = run_sleeping_vanilla()
        wait(lambda: get(scheduler_orchid_operation_path(op1.id) + "/resource_usage/cpu", default=None) == 1.0)
        op1.abort()

        wait(lambda: default_tree_aborted_job_profiler.get_job_count_delta() == 1)

        set("//sys/pool_trees/default/@config/nodes_filter", "!other")
        create_pool_tree("other", config={"nodes_filter": "other"})
        node = ls("//sys/cluster_nodes")[0]
        set("//sys/cluster_nodes/{}/@user_tags".format(node), ["other"])
        wait(lambda: get(scheduler_orchid_pool_path("<Root>", tree="other") + "/resource_limits/cpu", default=None) == 1.0)

        op2 = run_sleeping_vanilla(spec={"pool_trees": ["other"]})
        wait(lambda: get(scheduler_orchid_operation_path(op2.id, tree="other") + "/resource_usage/cpu", default=None) == 1.0)
        op2.abort()

        wait(lambda: other_tree_aborted_job_profiler.get_job_count_delta() == 1)
        wait(lambda: default_tree_aborted_job_profiler.get_job_count_delta() == 1)

    @authors("ignat")
    def test_scheduling_index_profiling(self):
        profiler = profiler_factory().at_scheduler(fixed_tags={"tree": "default"})

        tags = {"scheduling_index": "0", "scheduling_stage": "regular_medium_priority"}
        operation_scheduling_index_attempt_count = profiler.counter(
            "scheduler/operation_scheduling_index_attempt_count",
            tags=tags)
        max_operation_scheduling_index = profiler.counter(
            "scheduler/max_operation_scheduling_index",
            tags=tags)

        run_sleeping_vanilla()

        # NB: We cannot check equality, since operations from previous tests may affect counter
        wait(lambda: operation_scheduling_index_attempt_count.get_delta() >= 1)
        wait(lambda: max_operation_scheduling_index.get_delta() >= 1)

    @authors("eshcherbin")
    def test_started_job_count_profiling(self):
        started_job_profiler = JobCountProfiler("started", tags={"tree": "default"})
        started_vanilla_job_profiler = JobCountProfiler("started", tags={"tree": "default", "job_type": "vanilla"})

        time.sleep(3.0)
        assert started_job_profiler.get_job_count_delta() == started_vanilla_job_profiler.get_job_count_delta() == 0

        run_sleeping_vanilla()

        wait(lambda: started_job_profiler.get_job_count_delta() == started_vanilla_job_profiler.get_job_count_delta() == 1)


##################################################################


@pytest.mark.enabled_multidaemon
class TestSchedulerProfilingOnOperationFinished(YTEnvSetup, PrepareTables):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "connect_retry_backoff_time": 100,
            "fair_share_update_period": 100,
            "profiling_update_period": 100,
            "fair_share_profiling_period": 100,
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operation_time_limit_check_period": 100,
            "operation_job_metrics_push_period": 1000000000,
            "job_metrics_report_period": 100,
            "custom_job_metrics": [
                {
                    "statistics_path": "/custom/value_completed",
                    "profiling_name": "metric_completed",
                    "summary_value_type": "sum",
                },
                {
                    "statistics_path": "/custom/value_failed",
                    "profiling_name": "metric_failed",
                    "summary_value_type": "sum",
                },
            ],
        }
    }

    USE_PORTO = True

    @authors("eshcherbin")
    def test_operation_completed(self):
        create_pool("unique_pool")

        metric_completed_counter = profiler_factory() \
            .at_scheduler(fixed_tags={"tree": "default", "pool": "unique_pool"})\
            .counter("scheduler/pools/metrics/metric_completed")

        cmd = """python -c "import os; os.write(5, '{value_completed=117};')";"""
        run_test_vanilla(cmd, spec={"pool": "unique_pool"})

        wait(lambda: metric_completed_counter.get() == 117)

    @authors("eshcherbin")
    def test_operation_failed(self):
        create_pool("unique_pool")

        metric_failed_counter = profiler_factory() \
            .at_scheduler(fixed_tags={"tree": "default", "pool": "unique_pool"}) \
            .counter("scheduler/pools/metrics/metric_failed")

        cmd = """python -c "import os; os.write(5, '{value_failed=225};')"; exit 1"""
        op = run_test_vanilla(
            command=cmd,
            spec={"max_failed_job_count": 1, "pool": "unique_pool"},
        )
        op.track(raise_on_failed=False)

        wait(lambda: metric_failed_counter.get_delta() == 225)


##################################################################


@pytest.mark.enabled_multidaemon
class TestSchedulerErrorTruncate(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    NUM_SECONDARY_MASTER_CELLS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_controller": {
                    "job_common": {
                        "test_job_error_truncation": True,
                    }
                },
                "job_reporter": {
                    "reporting_period": 10,
                    "min_repeat_delay": 10,
                    "max_repeat_delay": 10,
                },
            }
        }
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "enable_job_reporter": True,
            "enable_job_spec_reporter": True,
            "enable_job_stderr_reporter": True,
        }
    }

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        config["cluster_connection"]["primary_master"]["rpc_timeout"] = 50000
        for connection in config["cluster_connection"]["secondary_masters"]:
            connection["rpc_timeout"] = 50000

    def setup_method(self, method):
        super(TestSchedulerErrorTruncate, self).setup_method(method)
        sync_create_cells(1)
        init_operations_archive.create_tables_latest_version(
            self.Env.create_native_client(), override_tablet_cell_bundle="default"
        )
        self._tmpdir = create_tmpdir("jobids")

    def teardown_method(self, method):
        remove("//sys/operations_archive")
        super(TestSchedulerErrorTruncate, self).teardown_method(method)

    @authors("ignat")
    def test_error_truncate(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        write_table("//tmp/t_in", {"foo": "bar"})

        op = map(
            command=with_breakpoint("BREAKPOINT; echo '{foo=bar}'; exit 1"),
            in_=["//tmp/t_in"],
            out="//tmp/t_out",
            spec={"max_failed_job_count": 1},
            track=False,
        )

        wait(lambda: op.get_running_jobs())
        running_job = next(iter(op.get_running_jobs().keys()))

        release_breakpoint()
        op.track(raise_on_failed=False)

        def find_truncated_errors(error):
            assert len(error.get("inner_errors", [])) <= 2
            if error.get("attributes", {}).get("inner_errors_truncated", False):
                return True
            return any([find_truncated_errors(inner_error) for inner_error in error.get("inner_errors", [])])

        job_error = get_job(job_id=running_job, operation_id=op.id)["error"]
        assert find_truncated_errors(job_error)


##################################################################


class TestSafeAssertionsMode(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # TODO(nadya73): It crashes CA. Fix it.
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "enable_controller_failure_spec_option": True,
        },
    }

    VERY_USEFUL_AND_READABLE_LOGS = [
        "coredumper.log",
        "coredumper_inner.log",
        "coredumper_lister.log",
    ]

    @classmethod
    def setup_class(cls):
        super(TestSafeAssertionsMode, cls).setup_class()

        for log in cls.VERY_USEFUL_AND_READABLE_LOGS:
            open("/tmp/" + log, "w")

    @classmethod
    def teardown_class(cls):
        shutil.rmtree(cls.core_path)

        for log in cls.VERY_USEFUL_AND_READABLE_LOGS:
            if os.path.exists("/tmp/" + log):
                shutil.copyfile("/tmp/" + log, cls.path_to_run + "/" + log)
        super(TestSafeAssertionsMode, cls).teardown_class()

    @classmethod
    def modify_controller_agent_config(cls, config, cluster_index):
        cls.core_path = os.path.join(cls.path_to_run, "_cores")
        os.mkdir(cls.core_path)
        os.chmod(cls.core_path, 0o777)
        config["core_dumper"] = {
            "path": cls.core_path,
            # Pattern starts with the underscore to trick teamcity; we do not want it to
            # pay attention to the created core.
            "pattern": "_core.%CORE_PID.%CORE_SIG.%CORE_THREAD_NAME-%CORE_REASON",
        }

    @authors("max42")
    @pytest.mark.skipif(is_asan_build(), reason="Core dumps + ASAN = no way")
    @pytest.mark.timeout(150)
    @pytest.mark.ignore_in_opensource_ci
    def test_assertion_failure(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})
        create("table", "//tmp/t_out")

        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"testing": {"controller_failure": "assertion_failure_in_prepare"}},
            command="cat",
        )
        with pytest.raises(YtError):
            op.track()

        err = op.get_error()
        print_debug("=== error ===")
        print_debug(err)

        assert err.contains_code(212)  # NScheduler::EErrorCode::OperationControllerCrashed

        # Core path is either attribute of an error itself, or of the only inner error when it is
        # wrapped with 'Operation has failed to prepare' error.
        core_path = err.attributes.get("core_path") or err.inner_errors[0].get("attributes", {}).get("core_path")
        assert core_path != YsonEntity()

        # Wait until core is finished. This may take a really long time under debug :(
        controller_agent_address = get(op.get_path() + "/@controller_agent_address")

        def check_core():
            if not os.path.exists(core_path):
                print_debug("size = n/a")
            else:
                print_debug("size =", os.stat(core_path).st_size)

            active_count = get(
                "//sys/controller_agents/instances/{}/orchid/core_dumper/active_count".format(
                    controller_agent_address
                )
            )
            assert active_count == 0

        wait_no_assert(check_core, wait_args=dict(iter=200, sleep_backoff=5))

        gdb = arcadia_interop.yatest_common.gdb_path()

        assert os.path.exists(core_path)
        child = subprocess.Popen(
            [
                gdb,
                "--batch",
                "-ex",
                "bt",
                os.path.join(self.bin_path, "ytserver-controller-agent"),
                core_path,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = child.communicate()
        print_debug("=== stderr ===")
        print_debug(stderr)
        print_debug("=== stdout ===")
        print_debug(stdout)
        assert child.returncode == 0
        assert b"Prepare" in stdout

    @authors("ignat")
    def test_unexpected_exception(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})
        create("table", "//tmp/t_out")

        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"testing": {"controller_failure": "exception_thrown_in_on_job_completed"}},
            command="cat",
        )
        with pytest.raises(YtError):
            op.track()
        print_debug(op.get_error())
        assert op.get_error().contains_code(213)  # NScheduler::EErrorCode::TestingError


class TestSchedulerAttributes(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "event_log": {"retry_backoff_time": 7, "flush_period": 5000},
            "enable_operation_heavy_attributes_archivation": True,
            "operations_cleaner": {
                "enable": True,
                "analysis_period": 100,
                "archive_batch_timeout": 100,
                "min_archivation_retry_sleep_delay": 100,
                "max_archivation_retry_sleep_delay": 110,
                "clean_delay": 50,
                "fetch_batch_size": 1,
                "max_operation_age": 100,
            },
        },
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operation_options": {"spec_template": {"data_weight_per_job": 1000}},
            "map_operation_options": {
                "spec_template": {
                    "data_weight_per_job": 2000,
                    "max_failed_job_count": 10,
                }
            },
        },
    }

    @authors("ignat")
    def test_specs(self):
        create("table", "//tmp/t_in")
        write_table("<append=true;sorted_by=[foo]>//tmp/t_in", {"foo": "bar"})

        create("table", "//tmp/t_out")

        op = map(command="sleep 1000", in_=["//tmp/t_in"], out="//tmp/t_out", track=False, fail_fast=False)

        full_spec_path = "//sys/scheduler/orchid/scheduler/operations/{0}/full_spec".format(op.id)
        wait(lambda: exists(full_spec_path))

        assert get("{}/data_weight_per_job".format(full_spec_path)) == 2000
        assert get("{}/max_failed_job_count".format(full_spec_path)) == 10

        op.abort()

        op = reduce(
            command="sleep 1000",
            in_=["//tmp/t_in"],
            out="//tmp/t_out",
            reduce_by=["foo"],
            track=False,
            fail_fast=False,
        )
        wait(lambda: op.get_state() == "running")

        full_spec_path = "//sys/scheduler/orchid/scheduler/operations/{0}/full_spec".format(op.id)
        wait(lambda: exists(full_spec_path))

        assert get("{}/data_weight_per_job".format(full_spec_path)) == 1000
        assert get("{}/max_failed_job_count".format(full_spec_path)) == 10

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        op.ensure_running()

        assert get("{}/data_weight_per_job".format(full_spec_path)) == 1000
        assert get("{}/max_failed_job_count".format(full_spec_path)) == 10

        op.abort()

    @authors("ignat")
    def test_unrecognized_spec(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"a": "b"}])
        create("table", "//tmp/t_out")
        op = map(
            command="sleep 1000",
            in_=["//tmp/t_in"],
            out="//tmp/t_out",
            track=False,
            spec={"xxx": "yyy"},
        )

        wait(lambda: exists(op.get_path() + "/@unrecognized_spec"))
        assert get(op.get_path() + "/@unrecognized_spec") == {"xxx": "yyy"}

    @authors("ignat")
    def test_brief_progress(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"a": "b"}])
        create("table", "//tmp/t_out")
        op = map(command="sleep 1000", in_=["//tmp/t_in"], out="//tmp/t_out", track=False)

        wait(lambda: exists(op.get_path() + "/@brief_progress"))
        assert "jobs" in list(get(op.get_path() + "/@brief_progress"))

        assert get(op.get_path() + "/@brief_progress/input_transaction_id") == \
            get_operation(op.id)["brief_progress"]["input_transaction_id"]

        assert get(op.get_path() + "/@brief_progress/output_transaction_id") == \
            get_operation(op.id)["brief_progress"]["output_transaction_id"]

    @authors("omgronny")
    @pytest.mark.parametrize("enable_spec_archivation", [True, False])
    def test_specs_in_archive(self, enable_spec_archivation):
        sync_create_cells(1)
        init_operations_archive.create_tables_latest_version(
            self.Env.create_native_client(), override_tablet_cell_bundle="default",
        )

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        update_scheduler_config("enable_operation_heavy_attributes_archivation", enable_spec_archivation)

        op = run_test_vanilla(
            command="sleep 1000",
            track=False,
            spec={"xxx": "yyy"}
        )

        if enable_spec_archivation:
            wait(lambda: "unrecognized_spec" in get_operation(op.id, attributes=["unrecognized_spec"]))
            assert not exists(op.get_path() + "/@unrecognized_spec")
            assert get_operation(op.id, attributes=["unrecognized_spec"])["unrecognized_spec"]["xxx"] == "yyy"
        else:
            wait(lambda: exists(op.get_path() + "/@unrecognized_spec"))
            assert get(op.get_path() + "/@unrecognized_spec")["xxx"] == "yyy"

    @authors("omgronny")
    def test_brief_spec_in_archive(self):
        sync_create_cells(1)
        init_operations_archive.create_tables_latest_version(
            self.Env.create_native_client(), override_tablet_cell_bundle="default",
        )

        transaction_id = start_transaction(timeout=300 * 1000)
        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), tx=transaction_id)

        wait_breakpoint()

        wait(lambda: exists(op.get_path() + "/@brief_spec"))
        assert get(op.get_path() + "/@brief_spec/user_transaction_id") == transaction_id
        assert get_operation(op.id)["brief_spec"]["user_transaction_id"] == transaction_id

        release_breakpoint()
        op.track()

        clean_operations()

        id_hi, id_lo = uuid_to_parts(op.id)
        rows = lookup_rows("//sys/operations_archive/ordered_by_id", [{"id_hi": id_hi, "id_lo": id_lo}])
        assert rows[0]["brief_spec"]["user_transaction_id"] == transaction_id

        assert get_operation(op.id)["brief_spec"]["user_transaction_id"] == transaction_id
        res = list_operations(
            include_archive=True,
            from_time=datetime_to_string(datetime.utcfromtimestamp(0)),
            to_time=datetime_to_string(datetime.utcnow()),
        )
        assert res["operations"][0]["brief_spec"]["user_transaction_id"] == transaction_id


##################################################################


class TestAsyncControllerActions(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 2  # snapshot upload replication factor is 2; unable to configure
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "heavy_request_immediate_response_timeout": 250,
        },
    }

    # NB(eshcherbin): These first two tests are probably meaningless, but let them be.
    @authors("eshcherbin")
    def test_fast_operation_flow(self):
        op = run_test_vanilla("sleep 1")
        op.track()

    @authors("eshcherbin")
    def test_fast_operation_flow_revive(self):
        op = run_test_vanilla(with_breakpoint("BREAKPOINT"))

        wait_breakpoint()

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        release_breakpoint()

        op.track()

    @authors("eshcherbin")
    def test_slow_initialization(self):
        op = run_test_vanilla("sleep 1", spec={"testing": {"delay_inside_initialize": 500}})
        op.track()

    @authors("eshcherbin")
    def test_slow_initialization_two_operations(self):
        op1 = run_test_vanilla("sleep 1", spec={"testing": {"delay_inside_initialize": 600}})
        op2 = run_test_vanilla("sleep 1", spec={"testing": {"delay_inside_initialize": 500}})
        op1.track()
        op2.track()

    @authors("eshcherbin")
    def test_slow_initialization_revive(self):
        op = run_test_vanilla("sleep 1", spec={"testing": {"delay_inside_initialize": 1500}})
        op.wait_for_state("initializing")

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        op.track()

    @authors("eshcherbin")
    def test_slow_preparation(self):
        op = run_test_vanilla("sleep 1", spec={"testing": {"delay_inside_prepare": 500}})
        op.track()

    @authors("eshcherbin")
    def test_slow_preparation_two_operations(self):
        op1 = run_test_vanilla("sleep 1", spec={"testing": {"delay_inside_prepare": 600}})
        op2 = run_test_vanilla("sleep 1", spec={"testing": {"delay_inside_prepare": 500}})
        op1.track()
        op2.track()

    @authors("eshcherbin")
    def test_slow_materialization(self):
        op = run_test_vanilla("sleep 1", spec={"testing": {"delay_inside_materialize": 500}})
        op.track()

    @authors("eshcherbin")
    def test_slow_materialization_two_operations(self):
        op1 = run_test_vanilla("sleep 1", spec={"testing": {"delay_inside_materialize": 600}})
        op2 = run_test_vanilla("sleep 1", spec={"testing": {"delay_inside_materialize": 500}})
        op1.track()
        op2.track()

    @authors("eshcherbin")
    def test_slow_materialization_revive(self):
        op = run_test_vanilla("sleep 1", spec={"testing": {"delay_inside_materialize": 1500}})
        op.wait_for_state("materializing")

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        op.track()

    @authors("eshcherbin")
    def test_slow_revival_from_scratch(self):
        # Use "delay_inside_prepare" here because revival from scratch is basically preparation.
        op = run_test_vanilla("sleep 1", spec={"testing": {"delay_inside_prepare": 500}})

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        op.track()

    @authors("eshcherbin")
    def test_slow_revival_from_snapshot(self):
        update_controller_agent_config("snapshot_period", 300)

        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            spec={"testing": {"delay_inside_revive": 500}},
        )

        op.wait_for_fresh_snapshot()

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        release_breakpoint()

        op.track()

    @authors("eshcherbin")
    def test_slow_revival_from_scratch_two_operations(self):
        # Use "delay_inside_prepare" here because revival from scratch is basically preparation.
        op1 = run_test_vanilla("sleep 1", spec={"testing": {"delay_inside_prepare": 500}})
        op2 = run_test_vanilla("sleep 1", spec={"testing": {"delay_inside_prepare": 500}})

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        op1.track()
        op2.track()

    @authors("eshcherbin")
    def test_slow_revival_from_snapshot_two_operations(self):
        update_controller_agent_config("snapshot_period", 300)

        op1 = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            spec={"testing": {"delay_inside_revive": 500}},
        )
        op2 = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            spec={"testing": {"delay_inside_revive": 500}},
        )

        op1.wait_for_fresh_snapshot()
        op2.wait_for_fresh_snapshot()

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        release_breakpoint()

        op1.track()
        op2.track()

    @authors("eshcherbin")
    def test_slow_commit(self):
        op = run_test_vanilla(
            "sleep 1",
            spec={
                "testing": {
                    "delay_inside_operation_commit": 500,
                    "delay_inside_operation_commit_stage": "start",
                }
            },
        )
        op.track()

    @authors("eshcherbin")
    def test_slow_commit_two_operations(self):
        op1 = run_test_vanilla(
            "sleep 1",
            spec={
                "testing": {
                    "delay_inside_operation_commit": 600,
                    "delay_inside_operation_commit_stage": "start",
                }
            },
        )
        op2 = run_test_vanilla(
            "sleep 1",
            spec={
                "testing": {
                    "delay_inside_operation_commit": 500,
                    "delay_inside_operation_commit_stage": "start",
                }
            },
        )
        op1.track()
        op2.track()

    @authors("eshcherbin")
    def test_slow_commit_revive(self):
        op = run_test_vanilla(
            "sleep 1",
            spec={
                "testing": {
                    "delay_inside_operation_commit": 1500,
                    "delay_inside_operation_commit_stage": "start",
                }
            },
        )
        op.wait_for_state("completing")

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        op.track()

    @authors("eshcherbin")
    def test_slow_everything(self):
        op = run_test_vanilla(
            "sleep 1",
            spec={
                "testing": {
                    "delay_inside_initialize": 500,
                    "delay_inside_prepare": 500,
                    "delay_inside_materialize": 500,
                    "delay_inside_operation_commit": 500,
                    "delay_inside_operation_commit_stage": "start",
                }
            },
        )
        op.track()

    @authors("eshcherbin")
    def test_slow_everything_two_operations(self):
        op1 = run_test_vanilla(
            "sleep 1",
            spec={
                "testing": {
                    "delay_inside_initialize": 600,
                    "delay_inside_prepare": 600,
                    "delay_inside_materialize": 600,
                    "delay_inside_operation_commit": 600,
                    "delay_inside_operation_commit_stage": "start",
                }
            },
        )
        op2 = run_test_vanilla(
            "sleep 1",
            spec={
                "testing": {
                    "delay_inside_initialize": 500,
                    "delay_inside_prepare": 500,
                    "delay_inside_materialize": 500,
                    "delay_inside_operation_commit": 500,
                    "delay_inside_operation_commit_stage": "start",
                }
            },
        )
        op1.track()
        op2.track()

    @authors("eshcherbin")
    def test_slow_everything_revive(self):
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            spec={
                "testing": {
                    "delay_inside_initialize": 500,
                    "delay_inside_prepare": 500,
                    "delay_inside_materialize": 500,
                    "delay_inside_operation_commit": 500,
                    "delay_inside_operation_commit_stage": "start",
                }
            },
        )

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        release_breakpoint()

        op.track()


@pytest.mark.enabled_multidaemon
class TestControllerAgentPrerequisiteTxError(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 2  # snapshot upload replication factor is 2; unable to configure
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "controller_agent_tracker": {
                "incarnation_transaction_ping_period": 10000,
                "incarnation_transaction_timeout": 30000,
            }
        }
    }
    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operations_update_period": 10000,
        },
    }

    def _abort_controller_agent_incarnation_transaction(self):
        incarnation_tx = None
        for tx in ls("//sys/transactions", attributes=["title"]):
            title = tx.attributes.get("title", "")
            id = str(tx)
            if "Controller agent incarnation" in title:
                incarnation_tx = id
        assert incarnation_tx is not None

        abort_transaction(incarnation_tx)

    @authors("ignat")
    def test_incarnation_transaction_abort(self):
        create("table", "//tmp/test_input")
        create("table", "//tmp/test_output")
        write_table("//tmp/test_input", [{"a": "b"}])
        op = map(
            track=False,
            command="sleep 1",
            in_="//tmp/test_input",
            out="//tmp/test_output",
            spec={"testing": {"delay_inside_initialize": 5000}},
        )
        # This sleep is intentional, it is usually enough to enter initialization, but test still should be correct if it does not happen.
        time.sleep(3)
        self._abort_controller_agent_incarnation_transaction()
        op.track()


@pytest.mark.enabled_multidaemon
class TestControllerAgentDisconnectionDuringUnregistration(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 2  # snapshot upload replication factor is 2; unable to configure
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "controller_agent_tracker": {
                "heavy_rpc_timeout": 12000,
                "light_rpc_timeout": 8000,
            }
        }
    }
    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "testing_options": {
                "delay_in_unregistration": 20000,
            }
        },
    }

    @authors("ignat")
    def test_no_revive_after_unregistration(self):
        create("table", "//tmp/test_input")
        create("table", "//tmp/test_output")
        write_table("//tmp/test_input", [{"a": "b"}])

        transaction_id = start_transaction(timeout=300 * 1000)
        op = map(
            track=False,
            command="sleep 1000",
            in_="//tmp/test_input",
            out="//tmp/test_output",
            transaction_id=transaction_id,
        )

        wait(lambda: op.get_running_jobs())

        # TODO(ignat): avoid this sleep.
        time.sleep(2)

        abandon_job(next(iter(op.get_running_jobs().keys())))

        wait(lambda: op.get_state() == "completed")

        # Wait for delay in unregistration.
        time.sleep(15)

        assert op.get_state() == "completed"


##################################################################


@pytest.mark.enabled_multidaemon
class TestSuccessfulScheduleJobDurationEstimate(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "schedule_job_statistics_log_backoff": 10,
            "schedule_job_statistics_moving_average_window_size": 10,
        },
    }

    @authors("eshcherbin")
    def test_simple(self):
        op = run_sleeping_vanilla(job_count=2, spec={"testing": {"inside_schedule_job_delay": {"duration": 500}}})

        wait(lambda: get(op.get_orchid_path() + "/progress/schedule_job_statistics/successful_duration_estimate_us", default=-1) >= 500000)
