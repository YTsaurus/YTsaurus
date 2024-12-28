from yt_env_setup import YTEnvSetup

from yt_commands import (
    add_member, authors, create_access_control_object, remove,
    make_ace, raises_yt_error, wait, create_user, print_debug, select_rows,
    set, get, insert_rows, generate_uuid, ls)

from yt_error_codes import AuthorizationErrorCode, ResolveErrorCode

from yt_helpers import profiler_factory

from yt_queries import Query, start_query, list_queries, get_query_tracker_info

from yt.common import date_string_to_timestamp_mcs

from yt.test_helpers import assert_items_equal

from collections import Counter
from builtins import set as Set

import pytest


def expect_queries(queries, list_result, incomplete=False):
    try:
        assert Set(q.id for q in queries) == Set(q["id"] for q in list_result["queries"])
        assert list_result["incomplete"] == incomplete
    except AssertionError:
        timestamp = list_result["timestamp"]
        print_debug(f"Assertion failed, dumping content of dynamic tables by timestamp {timestamp}")
        for table in ("active_queries", "finished_queries", "finished_queries_by_start_time", "finished_queries_by_aco_and_start_time", "finished_queries_by_user_and_start_time"):
            print_debug(f"{table}:")
            select_rows(f"* from [//sys/query_tracker/{table}]", timestamp=timestamp)
        raise


class TestMetrics(YTEnvSetup):
    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }

    @authors("mpereskokova")
    def test_active_queries_metrics(self, query_tracker):
        query_tracker = ls("//sys/query_tracker/instances")[0]
        profiler = profiler_factory().at_query_tracker(query_tracker)

        q = start_query("mock", "run_forever")

        active_queries_metric = profiler.gauge("query_tracker/active_queries")
        wait(lambda: active_queries_metric.get({"state": "Running"}) == 1)

        q.abort()
        wait(lambda: active_queries_metric.get({"state": "Running"}) == 0)

    @authors("mpereskokova")
    def test_state_time_metrics(self, query_tracker):
        query_tracker = ls("//sys/query_tracker/instances")[0]
        profiler = profiler_factory().at_query_tracker(query_tracker)
        state_time_metric = profiler.gauge("query_tracker/state_time")

        q1 = start_query("mock", "run_forever")
        wait(lambda: state_time_metric.get({"state": "Pending"}) is not None)
        assert state_time_metric.get({"state": "Running"}) is None
        assert state_time_metric.get({"state": "Aborting"}) is None

        q1.abort()
        wait(lambda: state_time_metric.get({"state": "Running"}) is not None)
        wait(lambda: state_time_metric.get({"state": "Aborting"}) is not None)
        assert state_time_metric.get({"state": "Failing"}) is None

        q2 = start_query("mock", "fail")
        with raises_yt_error("failed"):
            q2.track()
        wait(lambda: state_time_metric.get({"state": "Failing"}) is not None)
        assert state_time_metric.get({"state": "Completing"}) is None

        q3 = start_query("mock", "complete_after", settings={"duration": 0})
        q3.track()
        wait(lambda: state_time_metric.get({"state": "Completing"}) is not None)


class TestQueriesMock(YTEnvSetup):
    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }

    @authors("max42")
    def test_fail(self, query_tracker):
        q = start_query("mock", "fail")
        with raises_yt_error("failed"):
            q.track()
        assert q.get_state() == "failed"

        q = start_query("mock", "fail_by_exception")
        with raises_yt_error("failed"):
            q.track()
        assert q.get_state() == "failed"

        q = start_query("mock", "fail_after", settings={"duration": 3000})
        with raises_yt_error("failed"):
            q.track()
        assert q.get_state() == "failed"

    @authors("max42")
    def test_abort(self, query_tracker):
        q = start_query("mock", "run_forever")
        wait(lambda: q.get_state() == "running")
        q.abort()
        with raises_yt_error("aborted"):
            q.track()
        assert q.get_state() == "aborted"

    @authors("max42")
    def test_complete(self, query_tracker):
        error = {"code": 42, "message": "Mock query execution error", "attributes": {"some_attr": "some_value"}}
        schema = [{"name": "foo", "type": "int64"}, {"name": "bar", "type": "string"}]
        rows = [{"foo": 42, "bar": "abc"}, {"foo": -17, "bar": "def"}, {"foo": 123, "bar": "ghi"}]

        q = start_query("mock", "complete_after", settings={
            "duration": 3000,
            "results": [
                {"error": error},
                {"schema": schema, "rows": rows},
            ]
        })

        q.track()
        query_info = q.get()
        assert query_info["result_count"] == 2

        result_0_info = q.get_result(0)
        assert result_0_info["error"] == error
        with raises_yt_error("Mock query execution error"):
            q.read_result(0)

        result_1_info = q.get_result(1)
        for column in result_1_info["schema"]:
            del column["type_v3"]
            del column["required"]
        result_1_info["schema"] = list(result_1_info["schema"])
        assert result_1_info["data_statistics"]["row_count"] == 3
        assert result_1_info["data_statistics"]["data_weight"] == 36
        assert result_1_info["schema"] == schema
        assert_items_equal(q.read_result(1), rows)
        assert_items_equal(q.read_result(1, lower_row_index=1, upper_row_index=2), rows[1:2])
        assert_items_equal(q.read_result(1, lower_row_index=-1, upper_row_index=5), rows)
        assert_items_equal(q.read_result(1, lower_row_index=2, upper_row_index=1), [])
        assert_items_equal(q.read_result(1, columns=["foo"]), [{"foo": row["foo"]} for row in rows])
        assert q.read_result(1, columns=["bar", "foo", "bar"], output_format="dsv") == \
            b"""bar=abc\tfoo=42\nbar=def\tfoo=-17\nbar=ghi\tfoo=123\n"""

    @authors("max42")
    def test_list(self, query_tracker):
        create_user("u1")
        create_user("u2")
        q0 = start_query("mock", "fail", authenticated_user="u1")
        q1 = start_query("mock", "complete_after", settings={"duration": 16000}, authenticated_user="u2")
        q2 = start_query("mock", "fail_by_exception", authenticated_user="u1")
        q3 = start_query("mock", "blahblah", authenticated_user="u2")
        q4 = start_query("mock", "fail_after", settings={"duration": 8000}, authenticated_user="u2")
        q5 = start_query("mock", "run_forever", authenticated_user="u1")
        q6 = start_query("mock", "run_forever", authenticated_user="u1")

        def collect_batch(attribute):
            queries = list_queries(cursor_direction="future", attributes=["id", attribute])
            result = []
            for q, expected_q in zip(queries["queries"], (q0, q1, q2, q3, q4, q5, q6)):
                assert q["id"] == expected_q.id
                result.append(q[attribute])
            return result

        q_times = collect_batch("start_time")

        wait(lambda: q6.get_state() == "running")
        q6.abort()

        def run_checks():
            expect_queries([q6, q5, q4, q3, q2, q1, q0], list_queries())
            expect_queries([q0, q1, q2, q3, q4, q5, q6], list_queries(cursor_direction="future"))

            expect_queries([q6, q5, q2, q0], list_queries(user="u1"))
            expect_queries([q4, q3, q1], list_queries(user="u2"))

            expect_queries([q4, q2, q0], list_queries(filter="fail"))

            expect_queries([q5, q4],
                           list_queries(from_time=q_times[1], to_time=q_times[5], limit=2),
                           incomplete=True)
            expect_queries([q3, q2],
                           list_queries(from_time=q_times[1], to_time=q_times[5], limit=2, cursor_time=q_times[4]),
                           incomplete=True)
            expect_queries([q1],
                           list_queries(from_time=q_times[1], to_time=q_times[5], limit=2, cursor_time=q_times[2]))
            expect_queries([q1, q2, q3],
                           list_queries(from_time=q_times[1], to_time=q_times[5], limit=3, cursor_direction="future"),
                           incomplete=True)
            expect_queries([q4, q5],
                           list_queries(from_time=q_times[1], to_time=q_times[5], limit=3, cursor_direction="future",
                                        cursor_time=q_times[3]))

        while True:
            run_checks()
            q_states = collect_batch("state")
            print_debug("Query states:", q_states, "counts:", Counter(q_states))
            if q_states == ["failed", "completed", "failed", "failed", "failed", "running", "aborted"]:
                break

        expect_queries([q6], list_queries(state="aborted"))
        expect_queries([q4, q3, q2, q0], list_queries(state="failed"))
        expect_queries([q5], list_queries(state="running"))
        expect_queries([q1], list_queries(state="completed"))

    @authors("max42")
    def test_draft(self, query_tracker):
        q = start_query("mock", "blahblah", draft=True)
        q_info = q.get()
        assert q_info["state"] == "draft"
        assert q_info["query"] == "blahblah"
        assert list_queries()["queries"] == [q_info]

    @authors("max42")
    def test_annotations(self, query_tracker):
        q = start_query("mock", "complete_after", settings={"duration": 5000}, annotations={"foo": "bar"})
        wait(lambda: q.get_state() == "running")
        assert len(list_queries(filter="bar")["queries"]) == 1

        q.alter(annotations={"qux": "quux"})
        q_info = list_queries(filter="qux", attributes=["annotations", "state"])["queries"][0]
        assert q_info["annotations"] == {"qux": "quux"}
        assert q_info["state"] == "running"
        wait(lambda: q.get_state() == "completed")

        assert len(list_queries(filter="qux")["queries"]) > 0

        q.alter(annotations={"qwe": "asd"})
        assert len(list_queries(filter="asd")["queries"]) > 0

    @authors("mpereskokova")
    def test_rows_limit(self, query_tracker):
        schema = [{"name": "a", "type": "int64"}]
        rows = [{"a": 42}, {"a": 43}, {"a": 44}]
        q = start_query("mock", "complete_after", settings={
            "results": [
                {"schema": schema, "rows": rows, "is_truncated": True},
                {"schema": schema, "rows": rows, "is_truncated": False},
            ]
        })

        q.track()
        assert q.get_result(0)["is_truncated"]
        assert not q.get_result(1)["is_truncated"]


class TestQueryTrackerBan(YTEnvSetup):
    NUM_QUERY_TRACKER = 1
    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }
    QUERY_TRACKER_DYNAMIC_CONFIG = {"state_check_period": 2000}

    def _test_query_fails():
        start_query("mock", "run_forever")
        return False

    @authors("mpereskokova")
    def test_query_tracker_ban(self, query_tracker):
        address = query_tracker.query_tracker.addresses[0]
        set(f"//sys/query_tracker/instances/{address}/@banned", True)

        with raises_yt_error() as err:
            wait(TestQueryTrackerBan._test_query_fails)
        assert err[0].contains_text("No alive peers found")
        wait(lambda: get(f"//sys/query_tracker/instances/{address}/orchid/state_checker/banned"), ignore_exceptions=True)

        guid = generate_uuid()
        insert_rows("//sys/query_tracker/active_queries", [{
            "query_id": guid,
            "engine": "mock",
            "user": "root",
            "query": "run_forever",
            "incarnation": 0,
            "start_time": 0,
            "execution_start_time": 0,
            "state": "pending",
            "settings": {}
        }])

        acquisition_iterations = get(f"//sys/query_tracker/instances/{address}/orchid/query_tracker/acquisition_iterations")
        wait(lambda: get(f"//sys/query_tracker/instances/{address}/orchid/query_tracker/acquisition_iterations") - acquisition_iterations >= 3)

        assert list(select_rows(f'* from [//sys/query_tracker/active_queries] WHERE query_id = "{guid}"'))[0]["state"] == "pending"

        set(f"//sys/query_tracker/instances/{address}/@banned", False)
        query = Query(guid)
        wait(lambda: query.get_state() == "running", ignore_exceptions=True)


class TestQueryTrackerQueryRestart(YTEnvSetup):
    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }

    def _insert_query(self, state, query="run_forever", settings={}, error="", is_abort=False):
        guid = generate_uuid()
        query = {
            "query_id": guid,
            "engine": "mock",
            "user": "root",
            "query": query,
            "incarnation": 0,
            "start_time": 0,
            "execution_start_time": 0,
            "progress": {},
            "annotations": {},
            "state": state,
            "settings": settings
        }
        if is_abort:
            query["abort_request"] = {"attributes": {}, "code": 100, "message": error}
        else:
            query["error"] = {"attributes": {}, "code": 100, "message": error}

        insert_rows("//sys/query_tracker/active_queries", [query])
        return guid

    @authors("mpereskokova")
    def test_query_tracker_aborting_query(self, query_tracker):
        error = "test_abort"
        guid = self._insert_query("aborting", error, is_abort=True)

        query = Query(guid)
        with raises_yt_error("aborted"):
            query.track()

    @authors("mpereskokova")
    def test_query_tracker_failing_query(self, query_tracker):
        guid = self._insert_query("failing")

        query = Query(guid)
        with raises_yt_error("failed"):
            query.track()

    @authors("mpereskokova")
    def test_query_tracker_completing_query(self, query_tracker):
        guid = self._insert_query("completing")

        query = Query(guid)
        query.track()

    @authors("mpereskokova")
    def test_query_tracker_running_query(self, query_tracker):
        guid = self._insert_query("running", query="complete_after", settings={"duration": 100})

        query = Query(guid)
        query.track()

    @authors("mpereskokova")
    def test_query_tracker_pending_query(self, query_tracker):
        guid = self._insert_query("pending", query="complete_after", settings={"duration": 100})

        query = Query(guid)
        query.track()


class TestAccessControl(YTEnvSetup):
    NUM_TEST_PARTITIONS = 16

    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }

    @authors("krock21")
    def test_no_acos(self, query_tracker):
        create_user("u1")
        create_user("u2")
        q_u1 = start_query("mock", "u1", authenticated_user="u1")
        q_u2 = start_query("mock", "u2", authenticated_user="u2")
        q_u1.get(authenticated_user="u1")
        q_u2.get(authenticated_user="u2")
        with raises_yt_error(AuthorizationErrorCode):
            q_u1.get(authenticated_user="u2")
        with raises_yt_error(AuthorizationErrorCode):
            q_u2.get(authenticated_user="u1")

    @authors("krock21")
    def test_aco_does_not_exist(self, query_tracker):
        create_user("u1")
        create_user("u2")
        create_user("superuser_u3")
        add_member("superuser_u3", "superusers")
        create_access_control_object(
            "aco1",
            "queries",
            attributes={
                "principal_acl": [
                    make_ace("allow", "u2", "use"),
                ]
            })
        q_u1 = start_query("mock", "run_forever", authenticated_user="u1", access_control_object="aco1")
        with raises_yt_error(ResolveErrorCode):
            start_query("mock", "run_forever", authenticated_user="u2", access_control_object="aco2")
        q_u1.get(authenticated_user="u2")

        # Delete ACO and verify that owner and superusers can still access it.
        remove("//sys/access_control_object_namespaces/queries/aco1")
        q_u1.get(authenticated_user="u1")
        q_u1.get(authenticated_user="superuser_u3")
        with raises_yt_error():
            q_u1.get(authenticated_user="u2")

    @authors("krock21")
    def test_aco_denies_author(self, query_tracker):
        create_user("u1")
        create_access_control_object(
            "aco_denies_author",
            "queries",
            attributes={
                "principal_acl": [
                    make_ace("deny", "u1", "use"),
                ]
            })
        q_u1 = start_query("mock", "run_forever", authenticated_user="u1", access_control_object="aco_denies_author")
        q_u1.get(authenticated_user="u1")

    @authors("krock21")
    def test_get(self, query_tracker):
        create_user("u1")
        create_user("u2")
        create_user("u3")
        create_access_control_object(
            "aco_get",
            "queries",
            attributes={
                "principal_acl": [
                    make_ace("allow", "u2", "use"),
                    make_ace("allow", "u3", "read"),
                    make_ace("allow", "u3", "write"),
                    make_ace("allow", "u3", "administer"),
                ]
            })
        q_u1 = start_query("mock", "run_forever", authenticated_user="u1", access_control_object="aco_get")
        q_u1.get(authenticated_user="u2")
        with raises_yt_error(AuthorizationErrorCode):
            q_u1.get(authenticated_user="u3")

    @authors("krock21")
    def test_get_aco_in_response(self, query_tracker):
        create_user("u1")
        create_access_control_object(
            "aco_get_aco_in_response",
            "queries")
        q_active = start_query("mock", "run_forever", authenticated_user="u1", access_control_object="aco_get_aco_in_response")
        q_finished = start_query("mock", "complete_after", settings={"duration": 100}, authenticated_user="u1", access_control_object="aco_get_aco_in_response")
        q_without_aco_active = start_query("mock", "run_forever", authenticated_user="u1")
        q_without_aco_finished = start_query("mock", "complete_after", settings={"duration": 100}, authenticated_user="u1")

        wait(lambda: q_finished.get_state() == "completed")
        wait(lambda: q_without_aco_finished.get_state() == "completed")

        assert q_active.get()["access_control_object"] == "aco_get_aco_in_response"
        assert "access_control_object" not in q_active.get(attributes=["id"])

        assert q_finished.get()["access_control_object"] == "aco_get_aco_in_response"
        assert "access_control_object" not in q_finished.get(attributes=["id"])

        # We may want to change behaviour here to return "nobody" in this case
        assert "access_control_object" not in q_without_aco_active.get()
        assert "access_control_object" not in q_without_aco_active.get(attributes=["id"])

        assert "access_control_object" not in q_without_aco_finished.get()
        assert "access_control_object" not in q_without_aco_finished.get(attributes=["id"])

    @authors("krock21")
    def test_abort(self, query_tracker):
        create_user("u1")
        create_user("u2")
        create_user("u3")
        create_access_control_object(
            "aco_abort",
            "queries",
            attributes={
                "principal_acl": [
                    make_ace("allow", "u2", "administer"),
                    make_ace("allow", "u3", "read"),
                    make_ace("allow", "u3", "write"),
                    make_ace("allow", "u3", "use"),
                ]
            })
        q_u1 = start_query("mock", "run_forever", authenticated_user="u1", access_control_object="aco_abort")
        wait(lambda: q_u1.get_state() == "running")
        with raises_yt_error(AuthorizationErrorCode):
            q_u1.abort(authenticated_user="u3")
        q_u1.abort(authenticated_user="u2")

    @authors("krock21")
    def test_get_query_result(self, query_tracker):
        create_user("u1")
        create_user("u2")
        create_user("u3")
        create_access_control_object(
            "aco_get_query_result",
            "queries",
            attributes={
                "principal_acl": [
                    make_ace("allow", "u2", "read"),
                    make_ace("allow", "u3", "administer"),
                    make_ace("allow", "u3", "write"),
                    make_ace("allow", "u3", "use"),
                ]
            })
        q_u1 = start_query("mock", "complete_after", authenticated_user="u1", access_control_object="aco_get_query_result", settings={
            "duration": 3000,
            "results": [
                {"schema": [{"name": "foo", "type": "int64"}], "rows": [{"foo": 42}]},
            ]
        })
        q_u1.track()
        q_u1.get_result(0, authenticated_user="u2")
        with raises_yt_error(AuthorizationErrorCode):
            q_u1.get_result(0, authenticated_user="u3")

    @authors("krock21")
    def test_read_query_result(self, query_tracker):
        create_user("u1")
        create_user("u2")
        create_user("u3")
        create_access_control_object(
            "aco_read_query_result",
            "queries",
            attributes={
                "principal_acl": [
                    make_ace("allow", "u2", "read"),
                    make_ace("allow", "u3", "administer"),
                    make_ace("allow", "u3", "write"),
                    make_ace("allow", "u3", "use"),
                ]
            })
        q_u1 = start_query("mock", "complete_after", authenticated_user="u1", access_control_object="aco_read_query_result", settings={
            "duration": 3000,
            "results": [
                {"schema": [{"name": "foo", "type": "int64"}], "rows": [{"foo": 42}]},
            ]
        })
        q_u1.track()
        q_u1.read_result(0, authenticated_user="u2")
        with raises_yt_error(AuthorizationErrorCode):
            q_u1.read_result(0, authenticated_user="u3")

    @authors("krock21")
    def test_alter(self, query_tracker):
        create_user("u1")
        create_user("u2")
        create_user("u3")
        create_access_control_object(
            "aco_alter",
            "queries",
            attributes={
                "principal_acl": [
                    make_ace("allow", "u2", "administer"),
                    make_ace("allow", "u3", "read"),
                    make_ace("allow", "u3", "write"),
                    make_ace("allow", "u3", "use"),
                ]
            })
        q_u1 = start_query("mock", "run_forever", authenticated_user="u1", access_control_object="aco_alter")
        q_u1.alter(authenticated_user="u2", annotations={"qwe": "asd"})
        with raises_yt_error(AuthorizationErrorCode):
            q_u1.alter(authenticated_user="u3", annotations={"qwe2": "asd3"})

    @authors("krock21", "mpereskokova")
    def test_alter_aco(self, query_tracker):
        create_user("u1")
        create_user("u2")
        create_user("u3")
        create_access_control_object(
            "aco_alter_aco",
            "queries",
            attributes={
                "principal_acl": [
                    make_ace("allow", "u2", "administer"),
                    make_ace("allow", "u3", "read"),
                    make_ace("allow", "u3", "write"),
                    make_ace("allow", "u3", "use"),
                ]
            })
        create_access_control_object(
            "aco_alter_aco_u3",
            "queries",
            attributes={
                "principal_acl": [
                    make_ace("allow", "u3", "administer"),
                ]
            })
        q_u1 = start_query("mock", "run_forever", authenticated_user="u1", access_control_object="aco_alter_aco")
        q_u2 = start_query("mock", "complete_after", settings={"duration": 0}, authenticated_user="u1", access_control_object="everyone")
        q_u2.track()

        q_u2.alter(authenticated_user="u1", access_control_object="nobody")
        q_u2.alter(authenticated_user="u1", annotations={"qwe": "asd"}, access_control_object="everyone")

        q_u1.alter(authenticated_user="u1", access_control_object="aco_alter_aco")

        q_u1.alter(authenticated_user="u2", annotations={"qwe": "asd"}, access_control_object="nobody")
        with raises_yt_error(AuthorizationErrorCode):
            q_u1.alter(authenticated_user="u2", annotations={"qwe": "asd"}, access_control_object="aco_alter_aco")

        q_u1.alter(authenticated_user="u1", access_control_object="aco_alter_aco")

        with raises_yt_error(AuthorizationErrorCode):
            q_u1.alter(authenticated_user="u3", access_control_object="nobody")
        with raises_yt_error(AuthorizationErrorCode):
            q_u1.alter(authenticated_user="u3", access_control_object="aco_alter_aco")

        q_u1.alter(authenticated_user="u1", access_control_object="aco_alter_aco_u3")

        q_u1.alter(authenticated_user="u3", access_control_object="aco_alter_aco")
        with raises_yt_error(AuthorizationErrorCode):
            q_u1.alter(authenticated_user="u3", access_control_object="aco_alter_aco_u3")

        with raises_yt_error(ResolveErrorCode):
            q_u1.alter(authenticated_user="u1", access_control_object="nonexistent_aco")

    @authors("aleksandr.gaev")
    def test_get_query_tracker_info(self, query_tracker):
        supported_features = {'access_control': True, 'multiple_aco': True}
        assert get_query_tracker_info() == \
            {'query_tracker_stage': 'production', 'cluster_name': 'primary', 'supported_features': supported_features, 'access_control_objects': ['everyone', 'everyone-share', 'nobody']}

        assert get_query_tracker_info(attributes=[]) == {'query_tracker_stage': 'production', 'cluster_name': '', 'supported_features': {}, 'access_control_objects': []}
        assert get_query_tracker_info(attributes=["cluster_name"]) == {'query_tracker_stage': 'production', 'cluster_name': 'primary', 'supported_features': {}, 'access_control_objects': []}
        assert get_query_tracker_info(attributes=["supported_features"]) == \
            {'query_tracker_stage': 'production', 'cluster_name': '', 'supported_features': supported_features, 'access_control_objects': []}
        assert get_query_tracker_info(attributes=["access_control_objects"]) == \
            {'query_tracker_stage': 'production', 'cluster_name': '', 'supported_features': {}, 'access_control_objects': ['everyone', 'everyone-share', 'nobody']}

        assert get_query_tracker_info(stage='testing') == \
            {'query_tracker_stage': 'testing', 'cluster_name': 'primary', 'supported_features': supported_features, 'access_control_objects': ['everyone', 'everyone-share', 'nobody']}


class TestShare(YTEnvSetup):
    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }

    @authors("mpereskokova")
    def test_share(self, query_tracker):
        create_user("u1")
        create_user("u2")

        q = start_query("mock", "run_forever", authenticated_user="u1", access_control_object="everyone-share")

        q.get(authenticated_user="u2")
        expect_queries([q], list_queries(authenticated_user="u1"))
        expect_queries([], list_queries(authenticated_user="u2"))


class TestIndexTables(YTEnvSetup):
    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }

    @authors("mpereskokova")
    def test_start_query(self, query_tracker):
        create_user("user")

        q = start_query("mock", "complete_after", settings={"duration": 0}, access_control_objects=["everyone", "everyone-share"], authenticated_user="user")
        q.track()
        q_info = q.get()

        queries_by_aco = list(select_rows("* from [//sys/query_tracker/finished_queries_by_aco_and_start_time]"))
        assert len(queries_by_aco) == 2

        assert queries_by_aco[0]["query_id"] == q_info["id"]
        assert queries_by_aco[0]["access_control_object"] == "everyone"
        assert queries_by_aco[0]["minus_start_time"] == -date_string_to_timestamp_mcs(q_info["start_time"])

        assert queries_by_aco[1]["query_id"] == q_info["id"]
        assert queries_by_aco[1]["access_control_object"] == "everyone-share"
        assert queries_by_aco[1]["minus_start_time"] == -date_string_to_timestamp_mcs(q_info["start_time"])

        queries_by_user = list(select_rows("* from [//sys/query_tracker/finished_queries_by_user_and_start_time]"))
        assert len(queries_by_user) == 1

        assert queries_by_user[0]["user"] == "user"
        assert queries_by_user[0]["minus_start_time"] == -date_string_to_timestamp_mcs(q_info["start_time"])

    @authors("mpereskokova")
    def test_list_queries(self, query_tracker):
        create_user("u1")
        create_user("u2")

        q1 = start_query("mock", "complete_after", settings={"duration": 0}, access_control_objects=["everyone"], authenticated_user="u2")
        q2 = start_query("mock", "complete_after", settings={"duration": 0}, access_control_objects=["everyone", "everyone-share"], authenticated_user="u1")
        q1.track()
        q2.track()

        expect_queries([q2, q1], list_queries(authenticated_user="u1", limit=2))


class TestMultipleAccessControl(YTEnvSetup):
    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }

    @authors("mpereskokova")
    def test_start_query(self, query_tracker):
        create_user("u1")

        start_query("mock", "run_forever", authenticated_user="u1", access_control_object="nobody")
        start_query("mock", "run_forever", authenticated_user="u1", access_control_objects=["nobody"])
        with raises_yt_error():
            start_query("mock", "run_forever", authenticated_user="u1", access_control_objects=["nobody"], access_control_object="nobody")

    @authors("mpereskokova")
    def test_alter_aco(self, query_tracker):
        create_user("u1")
        create_user("u2")
        create_access_control_object(
            "aco",
            "queries",
            attributes={
                "principal_acl": [
                    make_ace("allow", "u1", "read"),
                    make_ace("allow", "u1", "use"),
                ]
            })

        q = start_query("mock", "run_forever", authenticated_user="u2", access_control_object="nobody")
        with raises_yt_error(AuthorizationErrorCode):
            q.get(authenticated_user="u1")

        with raises_yt_error(AuthorizationErrorCode):
            q.alter(authenticated_user="u1", access_control_objects=["aco", "nobody"])
        q.alter(authenticated_user="u2", access_control_objects=["aco", "nobody"])

        q.get(authenticated_user="u1")

        q.alter(authenticated_user="u2", access_control_objects=[])
        with raises_yt_error(AuthorizationErrorCode):
            q.get(authenticated_user="u1")

    @authors("mpereskokova")
    def test_get_query(self, query_tracker):
        create_user("u1")
        create_access_control_object("yet_another_nobody", "queries")

        q1 = start_query("mock", "run_forever", authenticated_user="u1", access_control_object="nobody")
        q2 = start_query("mock", "run_forever", authenticated_user="u1", access_control_objects=["nobody", "yet_another_nobody"])

        query = q1.get(authenticated_user="u1")
        assert query["access_control_object"] == "nobody"
        assert query["access_control_objects"] == ["nobody"]

        query = q2.get(authenticated_user="u1")
        assert "access_control_object" not in query
        assert query["access_control_objects"] == ["nobody", "yet_another_nobody"]

    @authors("mpereskokova")
    def test_list_queries(self, query_tracker):
        create_user("u1")
        create_user("u2")
        create_access_control_object("yet_another_nobody", "queries")
        create_access_control_object(
            "aco",
            "queries",
            attributes={
                "principal_acl": [
                    make_ace("allow", "u1", "read"),
                    make_ace("allow", "u1", "use"),
                ]
            })

        q1 = start_query("mock", "run_forever", authenticated_user="u1", access_control_objects=["nobody", "yet_another_nobody"])
        q2 = start_query("mock", "run_forever", authenticated_user="u2", access_control_objects=["aco"])

        expect_queries([q1, q2], list_queries(authenticated_user="u1"))
        expect_queries([q2], list_queries(authenticated_user="u2"))


# Separate list to fit 480 seconds limit for a test class.
class TestAccessControlList(YTEnvSetup):
    NUM_TEST_PARTITIONS = 16

    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }

    @pytest.mark.parametrize(
        "query_type",
        [
            ["mock", "fail", {}],
            ["mock", "complete_after", {"settings": {"duration": 1000}}],
            ["mock", "fail_by_exception", {}],
            ["mock", "blahblah", {}],
            ["mock", "fail_after", {"settings": {"duration": 1000}}],
            ["mock", "run_forever", {}]
        ])
    @authors("krock21")
    def test_list(self, query_tracker, query_type):
        create_user("u1")
        create_user("u2")
        create_user("u3")
        create_user("u4")
        create_user("u5")
        create_user("u6")
        create_user("superuser_u7")
        add_member("superuser_u7", "superusers")
        create_access_control_object(
            "aco_list_u1",
            "queries",
            attributes={
                "principal_acl": [
                    # Allow u2, deny everyone else.
                    make_ace("allow", "u2", "use"),
                    make_ace("allow", "u3", "read"),
                    make_ace("allow", "u4", "write"),
                    make_ace("allow", "u5", "administer"),
                ]
            })
        create_access_control_object(
            "aco_list_u2",
            "queries",
            attributes={
                "principal_acl": [
                    # Allow u3, deny everyone else.
                    make_ace("allow", "u3", "use"),
                    make_ace("allow", "u4", "use"),
                    make_ace("deny", "u4", "use"),
                    make_ace("deny", "u5", "use"),
                ]
            })
        create_access_control_object(
            "aco_list_u3",
            "queries",
            attributes={
                "principal_acl": [
                    # Allow everyone except u6.
                    make_ace("allow", "u1", "use"),
                    make_ace("allow", "u2", "use"),
                    make_ace("allow", "u4", "use"),
                    make_ace("allow", "u5", "use"),
                ]
            })
        create_access_control_object(
            "aco_list_u4",
            "queries",
            attributes={
                "principal_acl": [
                    # Allow everyone except u5.
                    make_ace("allow", "u1", "use"),
                    make_ace("allow", "u2", "use"),
                    make_ace("allow", "u3", "use"),
                ]
            })
        create_access_control_object(
            "aco_list_u5",
            "queries",
            attributes={
                "principal_acl": [
                    # Deny everyone implicitly.
                ]
            })

        create_access_control_object(
            "aco_list_u6",
            "queries",
            attributes={
                "principal_acl": [
                    # Deny everyone explicitly, even u6 themselves.
                    make_ace("deny", "u1", "use"),
                    make_ace("deny", "u2", "use"),
                    make_ace("deny", "u3", "use"),
                    make_ace("deny", "u4", "use"),
                    make_ace("deny", "u5", "use"),
                    make_ace("deny", "u6", "use"),
                ]
            })

        q_u1 = start_query(query_type[0], query_type[1], **query_type[2], authenticated_user="u1", access_control_object="aco_list_u1")
        q_u2 = start_query(query_type[0], query_type[1], **query_type[2], authenticated_user="u2", access_control_object="aco_list_u2")
        q_u3 = start_query(query_type[0], query_type[1], **query_type[2], authenticated_user="u3", access_control_object="aco_list_u3")
        q_u4 = start_query(query_type[0], query_type[1], **query_type[2], authenticated_user="u4", access_control_object="aco_list_u4")
        q_u5 = start_query(query_type[0], query_type[1], **query_type[2], authenticated_user="u5", access_control_object="aco_list_u5")
        q_u6 = start_query(query_type[0], query_type[1], **query_type[2], authenticated_user="u6", access_control_object="aco_list_u6")

        if "settings" in query_type[2] and "duration" in query_type[2]["settings"]:
            q_u1.track(raise_on_unsuccess=False)
            q_u2.track(raise_on_unsuccess=False)
            q_u3.track(raise_on_unsuccess=False)
            q_u4.track(raise_on_unsuccess=False)
            q_u5.track(raise_on_unsuccess=False)
            q_u6.track(raise_on_unsuccess=False)

        expect_queries([q_u1, q_u2, q_u3, q_u4, q_u5, q_u6], list_queries(cursor_direction="future"))  # Root user sees everything.
        expect_queries([q_u1, q_u2, q_u3, q_u4, q_u5, q_u6], list_queries(cursor_direction="future", authenticated_user="superuser_u7"))  # Superuser sees everything.
        expect_queries([q_u1, q_u3, q_u4], list_queries(cursor_direction="future", authenticated_user="u1"))
        expect_queries([q_u1, q_u2, q_u3, q_u4], list_queries(cursor_direction="future", authenticated_user="u2"))
        expect_queries([q_u2, q_u3, q_u4], list_queries(cursor_direction="future", authenticated_user="u3"))
        expect_queries([q_u3, q_u4], list_queries(cursor_direction="future", authenticated_user="u4"))
        expect_queries([q_u3, q_u5], list_queries(cursor_direction="future", authenticated_user="u5"))
        expect_queries([q_u6], list_queries(cursor_direction="future", authenticated_user="u6"))

    @authors("krock21")
    def test_list_by_aco(self, query_tracker):
        create_user("u1")
        create_user("u2")
        create_user("u3")
        create_access_control_object(
            "aco_list_by_aco1",
            "queries",
            attributes={
                "principal_acl": [
                    make_ace("allow", "users", "use"),
                ]
            })
        create_access_control_object(
            "aco_list_by_aco2",
            "queries",
            attributes={
                "principal_acl": [
                    make_ace("allow", "users", "use"),
                ]
            })
        create_access_control_object(
            "aco_list_by_aco3",
            "queries",
            attributes={
                "principal_acl": [
                    make_ace("allow", "users", "use"),
                ]
            })
        q_u1 = start_query("mock", "run_forever", authenticated_user="u1", access_control_object="aco_list_by_aco1")
        q_u2 = start_query("mock", "run_forever", authenticated_user="u2", access_control_object="aco_list_by_aco2")
        q_u3 = start_query("mock", "run_forever", authenticated_user="u3", access_control_object="aco_list_by_aco3")

        expect_queries([q_u1, q_u2, q_u3], list_queries(filter="aco:"))
        expect_queries([q_u1], list_queries(filter="aco_list_by_aco1"))
        expect_queries([q_u1], list_queries(filter="aco:aco_list_by_aco1"))
        expect_queries([q_u2], list_queries(filter="aco_list_by_aco2"))
        expect_queries([q_u2], list_queries(filter="aco:aco_list_by_aco2"))
        expect_queries([q_u3], list_queries(filter="aco_list_by_aco3"))
        expect_queries([q_u3], list_queries(filter="aco:aco_list_by_aco3"))

    @authors("mpereskokova")
    def test_list_sql_injection(self, query_tracker):
        create_user("u1")
        create_user("u2")
        q0 = start_query("mock", "fail", authenticated_user="u1")
        q1 = start_query("mock", "fail", authenticated_user="u2")

        expect_queries([q0], list_queries(cursor_direction="future", attributes=["id"], user="u1"))
        expect_queries([q1], list_queries(cursor_direction="future", attributes=["id"], user="u2"))
        expect_queries([], list_queries(cursor_direction="future", attributes=["id"], user="u1\") OR ([user]=\"u2"))


##################################################################


@authors("apollo1321")
class TestQueriesMockRpcProxy(TestQueriesMock):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    NUM_RPC_PROXIES = 1


@authors("mpereskokova")
class TestQueryTrackerBanRpcProxy(TestQueryTrackerBan):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    NUM_RPC_PROXIES = 1


@authors("apollo1321")
class TestAccessControlRpcProxy(TestAccessControl):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    NUM_RPC_PROXIES = 1


@authors("mpereskokova")
class TestShareRpcProxy(TestShare):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    NUM_RPC_PROXIES = 1


@authors("apollo1321")
class TestAccessControlListRpcProxy(TestAccessControlList):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    NUM_RPC_PROXIES = 1


@authors("mpereskokova")
class TestMultipleAccessControlRpcProxy(TestMultipleAccessControl):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    NUM_RPC_PROXIES = 1
