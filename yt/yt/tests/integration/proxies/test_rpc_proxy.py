from .proxy_format_config import _TestProxyFormatConfigBase

from yt_env_setup import YTEnvSetup, Restarter, RPC_PROXIES_SERVICE, is_asan_build

from yt_commands import (
    authors, wait, wait_breakpoint, release_breakpoint, with_breakpoint, events_on_fs, create, ls,
    get, set,
    copy, remove, exists, create_account, create_user, create_proxy_role, make_ace, start_transaction, commit_transaction, ping_transaction,
    create_access_control_object_namespace, create_access_control_object,
    insert_rows, select_rows,
    lookup_rows, alter_table, explain_query, read_file, write_file,
    read_table, write_table, map,
    map_reduce, sort, dump_job_context, sync_create_cells,
    sync_mount_table, sync_unmount_table, update_op_parameters, set_node_banned,
    set_account_disk_space_limit, create_dynamic_table, execute_command, Operation, raises_yt_error,
    discover_proxies)

from yt_helpers import write_log_barrier, read_structured_log, read_structured_log_single_entry, profiler_factory

from yt_type_helpers import make_schema

from yt.wrapper import JsonFormat, YtClient
from yt.common import YtError, YtResponseError, update_inplace
import yt.yson as yson

from yt_driver_bindings import Driver

from flaky import flaky
import pytest

from copy import deepcopy
from random import shuffle, randint
import builtins
import time
import yt_error_codes

import os.path

from typing import Any

##################################################################


class TestRpcProxy(YTEnvSetup):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

    NUM_RPC_PROXIES = 1

    DELTA_RPC_PROXY_CONFIG = {
        "retry_request_queue_size_limit_exceeded": False,
        "discovery_service": {
            "proxy_update_period": 100
        },
        "access_checker": {
            "enabled": True,
            "cache": {
                "expire_after_access_time": 100,
            },
        },
    }

    @authors("shakurov")
    def test_non_sticky_transactions_dont_stick(self):
        tx = start_transaction(timeout=1000)
        wait(lambda: not exists("//sys/transactions/" + tx))

    @authors("prime")
    def test_dynamic_config(self):
        proxy_name = ls("//sys/rpc_proxies")[0]

        set(
            "//sys/rpc_proxies/@config",
            {"tracing": {"user_sample_rate": {"prime": 1.0}}},
        )

        def config_updated():
            config = get("//sys/rpc_proxies/" + proxy_name + "/orchid/dynamic_config_manager/effective_config")
            return "prime" in config["tracing"]["user_sample_rate"]

        wait(config_updated)

        with Restarter(self.Env, RPC_PROXIES_SERVICE):
            pass

        wait(config_updated, ignore_exceptions=True)


class RpcProxyAccessCheckerTestBase(YTEnvSetup):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

    NUM_RPC_PROXIES = 1

    DELTA_RPC_PROXY_CONFIG = {
        "access_checker": {
            "enabled": True,
            "cache": {
                "expire_after_access_time": 100,
            },
        },
    }

    @authors("gritukan", "verytable")
    def test_access_checker(self):
        rpc_proxies = ls("//sys/rpc_proxies")
        assert len(rpc_proxies) == 1
        proxy_name = rpc_proxies[0]

        def check_access(user):
            try:
                get("//sys/@config", authenticated_user=user)
            except YtError:
                return False
            return True

        self.create_proxy_role_namespace()

        create_user("u")
        self.create_proxy_role("r1")
        self.create_proxy_role("r2")

        self.set_acl("r1", [make_ace("deny", "u", "use")])
        self.set_acl("r2", [make_ace("allow", "u", "use")])

        # "u" is not allowed to use proxies with role "r1".
        set("//sys/rpc_proxies/" + proxy_name + "/@role", "r1")
        wait(lambda: not check_access("u"))

        # "u" is allowed to use proxies with role "r2".
        set("//sys/rpc_proxies/" + proxy_name + "/@role", "r2")
        wait(lambda: check_access("u"))

        # Now "u" is not allowed to use proxies with role "r2".
        self.set_acl("r2", [make_ace("deny", "u", "use")])
        wait(lambda: not check_access("u"))

        # There is no node for proxy role "r3". By default we allow access to
        # proxies with unknown role.
        set("//sys/rpc_proxies/" + proxy_name + "/@role", "r3")
        wait(lambda: check_access("u"))

        # Set proxy role back to "r2". User "u" still can't use it.
        set("//sys/rpc_proxies/" + proxy_name + "/@role", "r2")
        wait(lambda: not check_access("u"))

        # Disable access checker via dynamic config. Now "u" can use proxy.
        set("//sys/rpc_proxies/@config", {"access_checker": {"enabled": False}})
        wait(lambda: check_access("u"))

        # Enable access checker via dynamic config. And "u" is banned again.
        set("//sys/rpc_proxies/@config", {"access_checker": {"enabled": True}})
        wait(lambda: not check_access("u"))


@pytest.mark.enabled_multidaemon
class TestRpcProxyAccessChecker(RpcProxyAccessCheckerTestBase):
    ENABLE_MULTIDAEMON = True

    def create_proxy_role_namespace(self):
        create("rpc_proxy_role_map", "//sys/rpc_proxy_roles")

    def create_proxy_role(self, name):
        create_proxy_role(name, "rpc")

    def set_acl(self, role, acl):
        set(f"//sys/rpc_proxy_roles/{role}/@acl", acl)


@pytest.mark.enabled_multidaemon
class TestRpcProxyAccessCheckerWithAco(RpcProxyAccessCheckerTestBase):
    ENABLE_MULTIDAEMON = True

    @classmethod
    def setup_class(cls):
        cls.DELTA_RPC_PROXY_CONFIG["access_checker"].update({
            "use_access_control_objects": True,
            "path_prefix": "//sys/access_control_object_namespaces/rpc_proxy_roles",
        })
        super().setup_class()

    def create_proxy_role_namespace(self):
        create_access_control_object_namespace("rpc_proxy_roles")

    def create_proxy_role(self, name):
        create_access_control_object(name, "rpc_proxy_roles")

    def set_acl(self, role, acl):
        set(f"//sys/access_control_object_namespaces/rpc_proxy_roles/{role}/principal/@acl", acl)


@pytest.mark.enabled_multidaemon
class TestRpcProxyStructuredLogging(YTEnvSetup):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    USE_DYNAMIC_TABLES = True

    NUM_RPC_PROXIES = 1

    ENABLE_MULTIDAEMON = True

    @classmethod
    def modify_rpc_proxy_config(cls, multidaemon_config, config):
        if "logging" in config[0]:
            config[0]["logging"]["rules"].append(
                {
                    "min_level": "debug",
                    "writers": ["main"],
                    "include_categories": ["RpcProxyStructuredMain", "Barrier"],
                    "message_format": "structured",
                }
            )
            config[0]["logging"]["writers"]["main"] = {
                "type": "file",
                "file_name": os.path.join(cls.path_to_run, "logs/rpc-proxy-0.main.yson.log"),
                "format": "yson",
            }
        multidaemon_config["logging"]["rules"].append(
            {
                "min_level": "debug",
                "writers": ["main-rpc-proxy"],
                "include_categories": ["RpcProxyStructuredMain", "Barrier"],
                "message_format": "structured",
            }
        )
        multidaemon_config["logging"]["writers"]["main-rpc-proxy"] = {
            "type": "file",
            "file_name": os.path.join(cls.path_to_run, "logs/rpc-proxy-0.main.yson.log"),
            "format": "yson",
        }

    def setup_method(self, method):
        super(TestRpcProxyStructuredLogging, self).setup_method(method)
        self.proxy_address = ls("//sys/rpc_proxies")[0]

        native_config = deepcopy(self.Env.configs["driver"])
        native_config["connection_type"] = "native"
        native_config["api_version"] = 3
        self.native_driver = Driver(native_config)

        self.rpc_proxy_log_file = self.path_to_run + "/logs/rpc-proxy-0.main.yson.log"

    def _write_log_barrier(self):
        return write_log_barrier(self.proxy_address, driver=self.native_driver)

    @authors("max42")
    def test_logging_dynamic_config(self):
        def method_path_list(from_barrier=None, to_barrier=None) -> list[tuple[str | None, str | None]]:
            return [
                (line.get("method", None), line.get("path", None))
                for line in read_structured_log(self.rpc_proxy_log_file, from_barrier=from_barrier, to_barrier=to_barrier)
            ]

        path_for_get = "//tmp/path_for_get"
        path_for_list = "//tmp/path_for_list"
        create("map_node", path_for_get)
        create("map_node", path_for_list)

        set("//sys/rpc_proxies/@config", {})
        time.sleep(0.5)

        b1 = self._write_log_barrier()

        get(path_for_get)
        ls(path_for_list)

        b2 = self._write_log_barrier()
        assert ("GetNode", path_for_get) in method_path_list(b1, b2)
        assert ("ListNode", path_for_list) in method_path_list(b1, b2)

        set("//sys/rpc_proxies/@config", {
            "api": {"structured_logging_main_topic": {"suppressed_methods": ["GetNode"]}},
        })
        time.sleep(0.5)

        get(path_for_get)
        ls(path_for_list)

        b3 = self._write_log_barrier()
        assert ("GetNode", path_for_get) not in method_path_list(b2, b3)
        assert ("ListNode", path_for_list) in method_path_list(b2, b3)

        set("//sys/rpc_proxies/@config", {
            "api": {
                "structured_logging_main_topic": {
                    "methods": {"ListNode": {"enable": False}}
                }
            },
        })
        time.sleep(0.5)

        get(path_for_get)
        ls(path_for_list)

        b4 = self._write_log_barrier()
        assert ("GetNode", path_for_get) in method_path_list(b3, b4)
        assert ("ListNode", path_for_list) not in method_path_list(b3, b4)

    @authors("dgolear")
    def test_user_tag_pass(self):
        set("//sys/rpc_proxies/@config", {})
        time.sleep(0.5)

        b1 = self._write_log_barrier()

        query = "* from [//path/to/table]"
        user_tag = "example_user"
        with raises_yt_error(yt_error_codes.ResolveErrorCode):
            select_rows(query, user_tag=user_tag)

        b2 = self._write_log_barrier()

        assert any(builtins.map(
            lambda line: line.get("identity", {}).get("user_tag", "") == user_tag,
            read_structured_log(self.rpc_proxy_log_file, from_barrier=b1, to_barrier=b2)
        ))

    @authors("max42")
    def test_max_request_byte_size(self):
        long_table_name = "//" + "a" * 4096
        query = "* from [" + long_table_name + "]"

        set("//sys/rpc_proxies/@config", {})
        time.sleep(0.5)

        b1 = self._write_log_barrier()

        set("//sys/rpc_proxies/@config", {
            "api": {
                "structured_logging_query_truncation_size": 1024 * 1024 * 128,
            },
        })
        time.sleep(0.5)

        with raises_yt_error(yt_error_codes.ResolveErrorCode):
            select_rows(query)

        b2 = self._write_log_barrier()

        set("//sys/rpc_proxies/@config", {
            "api": {
                "structured_logging_query_truncation_size": 1024 * 1024 * 128,
                "structured_logging_max_request_byte_size": 1024,
            },
        })
        time.sleep(0.5)

        with raises_yt_error(yt_error_codes.ResolveErrorCode):
            select_rows(query)

        b3 = self._write_log_barrier()

        def get_select_entry(from_barrier=None, to_barrier=None):
            for line in read_structured_log(self.rpc_proxy_log_file, from_barrier=from_barrier, to_barrier=to_barrier):
                if line.get("method", None) == "SelectRows":
                    return line
            return None

        assert get_select_entry(from_barrier=b1, to_barrier=b2)["request"]["query"] == query
        assert get_select_entry(from_barrier=b2, to_barrier=b3)["request"] == yson.YsonEntity()

    @authors("ermolovd")
    def test_per_request_max_request_byte_size(self):
        def search_log_by_path(path, from_barrier=None, to_barrier=None):
            for line in read_structured_log(self.rpc_proxy_log_file, from_barrier=from_barrier, to_barrier=to_barrier):
                if line.get("path", None) == path:
                    return line
            return None

        path_for_get = "//tmp/path_for_get"
        path_for_list = "//tmp/path_for_list"

        create("map_node", path_for_get)
        create("map_node", path_for_list)

        set("//sys/rpc_proxies/@config", {})
        time.sleep(0.5)

        b1 = self._write_log_barrier()

        set("//sys/rpc_proxies/@config", {
            "api": {},
        })
        time.sleep(0.5)

        ls(path_for_get)
        get(path_for_list)

        b2 = self._write_log_barrier()
        assert search_log_by_path(path_for_get, from_barrier=b1, to_barrier=b2)["request"]["path"] == path_for_get
        assert search_log_by_path(path_for_list, from_barrier=b1, to_barrier=b2)["request"]["path"] == path_for_list

        set("//sys/rpc_proxies/@config", {
            "api": {
                "structured_logging_main_topic": {
                    "methods": {
                        "ListNode": {
                            "max_request_byte_size": 0,
                        }
                    },
                }
            },
        })
        time.sleep(0.5)

        get(path_for_get)
        ls(path_for_list)

        b3 = self._write_log_barrier()
        assert search_log_by_path(path_for_get, from_barrier=b2, to_barrier=b3)["request"]["path"] == path_for_get
        assert search_log_by_path(path_for_list, from_barrier=b2, to_barrier=b3)["request"] == yson.YsonEntity()

    @authors("ermolovd")
    def test_query_truncation(self):
        query = "* from [//path/to/some/table]"
        b0 = self._write_log_barrier()

        with raises_yt_error(yt_error_codes.ResolveErrorCode):
            select_rows(query)

        b1 = self._write_log_barrier()

        try:
            set("//sys/rpc_proxies/@config", {
                "api": {"structured_logging_query_truncation_size": 10},
            })
            time.sleep(0.5)

            with raises_yt_error(yt_error_codes.ResolveErrorCode):
                select_rows(query)
        finally:
            set("//sys/rpc_proxies/@config", {})

        b2 = self._write_log_barrier()

        def read_entry(method, from_barrier, to_barrier):
            return read_structured_log_single_entry(
                self.rpc_proxy_log_file,
                lambda e: e.get("method", None) == method,
                from_barrier=from_barrier,
                to_barrier=to_barrier)

        assert read_entry("SelectRows", from_barrier=b0, to_barrier=b1)["request"]["query"] == query
        assert read_entry("SelectRows", from_barrier=b1, to_barrier=b2)["request"]["query"] == query[:10]


@pytest.mark.enabled_multidaemon
class TestRpcProxyDiscovery(YTEnvSetup):
    ENABLE_HTTP_PROXY = True
    ENABLE_RPC_PROXY = True

    NUM_RPC_PROXIES = 2

    ENABLE_MULTIDAEMON = True

    def setup_method(self, method):
        super(TestRpcProxyDiscovery, self).setup_method(method)
        driver_config = deepcopy(self.Env.configs["driver"])
        driver_config["api_version"] = 4
        self.driver = Driver(driver_config)

    @authors("verytable")
    def test_addresses(self):
        proxy = ls("//sys/rpc_proxies")[0]

        addresses = get("//sys/rpc_proxies/" + proxy + "/@addresses")
        assert "internal_rpc" in addresses
        assert "default" in addresses["internal_rpc"]
        assert proxy == addresses["internal_rpc"]["default"]

    @authors("verytable")
    def test_discovery(self):
        configured_proxy_addresses = sorted(self.Env.get_rpc_proxy_addresses())
        configured_monitoring_addresses = sorted(self.Env.get_rpc_proxy_monitoring_addresses())

        for test_name, request, expected_addresses in [
            (
                "defaults", {}, configured_proxy_addresses,
            ),
            (
                "explicit_address_type",
                {"address_type": "internal_rpc"},
                configured_proxy_addresses,
            ),
            (
                "explicit_params",
                {"address_type": "internal_rpc", "network_name": "default"},
                configured_proxy_addresses,
            ),
            (
                "monitoring_addresses",
                {"address_type": "monitoring_http", "network_name": "default"},
                configured_monitoring_addresses,
            ),
        ]:
            proxies = discover_proxies(type_="rpc", driver=self.driver, **request)
            assert sorted(proxies) == expected_addresses, test_name

    @authors("verytable")
    def test_invalid_address_type(self):
        with pytest.raises(YtError):
            discover_proxies(type_="rpc", driver=self.driver, address_type="invalid")

    @authors("verytable")
    def test_invalid_network_name(self):
        proxies = discover_proxies(type_="rpc", driver=self.driver, network_name="invalid")
        assert len(proxies) == 0


@pytest.mark.enabled_multidaemon
class TestRpcProxyDiscoveryRoleFromStaticConfig(YTEnvSetup):
    ENABLE_HTTP_PROXY = True
    ENABLE_RPC_PROXY = True

    NUM_RPC_PROXIES = 2

    DELTA_RPC_PROXY_CONFIG = {
        "role": "ab",
    }

    ENABLE_MULTIDAEMON = True

    def setup_method(self, method):
        super(TestRpcProxyDiscoveryRoleFromStaticConfig, self).setup_method(method)
        driver_config = deepcopy(self.Env.configs["driver"])
        driver_config["api_version"] = 4
        self.driver = Driver(driver_config)

    @authors("nadya73")
    def test_role(self):
        proxy = ls("//sys/rpc_proxies")[0]

        role = get("//sys/rpc_proxies/" + proxy + "/@role")
        assert role == "ab"

    @authors("nadya73")
    def test_discovery(self):
        configured_proxy_addresses = sorted(self.Env.get_rpc_proxy_addresses())

        proxies = discover_proxies(type_="rpc", driver=self.driver, **{"address_type": "internal_rpc"})
        assert sorted(proxies) == []

        proxies = discover_proxies(type_="rpc", driver=self.driver, **{"address_type": "internal_rpc", "role": "ab"})
        assert sorted(proxies) == configured_proxy_addresses


@pytest.mark.enabled_multidaemon
class TestRpcProxyDiscoveryBalancers(YTEnvSetup):
    ENABLE_HTTP_PROXY = True
    ENABLE_RPC_PROXY = True

    NUM_RPC_PROXIES = 2

    ENABLE_MULTIDAEMON = True

    def setup_method(self, method):
        super(TestRpcProxyDiscoveryBalancers, self).setup_method(method)
        driver_config = deepcopy(self.Env.configs["driver"])
        driver_config["api_version"] = 4
        driver_config["proxy_discovery_cache"] = {
            "expire_after_access_time": 0,
        }
        self.driver = Driver(driver_config)

    @authors("nadya73")
    def test_discovery(self):
        configured_proxy_addresses = sorted(self.Env.get_rpc_proxy_addresses())
        configured_monitoring_addresses = sorted(self.Env.get_rpc_proxy_monitoring_addresses())

        balancer_first = 'default-balancer.com:9013'
        balancer_second = 'default-balancer-2.com:9013'
        set(
            "//sys/rpc_proxies/@balancers",
            {
                'default': {
                    'internal_rpc': {
                        'default': [balancer_first, balancer_second]
                    }
                }
            },
        )

        proxies = discover_proxies(type_="rpc", driver=self.driver, **{"address_type": "internal_rpc"})
        assert proxies == [balancer_first, balancer_second]

        proxies = discover_proxies(type_="rpc", driver=self.driver, **{"address_type": "internal_rpc", "ignore_balancers": True})
        assert sorted(proxies) == configured_proxy_addresses

        proxies = discover_proxies(type_="rpc", driver=self.driver, **{"address_type": "monitoring_http"})
        assert sorted(proxies) == configured_monitoring_addresses

    @authors("nadya73")
    def test_invalid_address_type(self):
        with pytest.raises(YtError):
            discover_proxies(type_="rpc", driver=self.driver, address_type="invalid")

    @authors("nadya73")
    def test_invalid_network_name(self):
        proxies = discover_proxies(type_="rpc", driver=self.driver, network_name="invalid")
        assert len(proxies) == 0


@pytest.mark.enabled_multidaemon
class TestRpcProxyDiscoveryViaHttp(YTEnvSetup):
    DRIVER_BACKEND = "rpc"
    ENABLE_HTTP_PROXY = True
    ENABLE_RPC_PROXY = True

    NUM_RPC_PROXIES = 2

    ENABLE_MULTIDAEMON = True

    def setup_method(self, method):
        super(TestRpcProxyDiscoveryViaHttp, self).setup_method(method)
        driver_config = deepcopy(self.Env.configs["driver"])
        driver_config["api_version"] = 4
        self.driver = Driver(driver_config)

    def _create_yt_rpc_client_with_http_discovery(self, config=None):
        proxy = self.Env.get_proxy_address()

        default_config = {
            "token": "cypress_token",
            "backend": "rpc",
            "driver_config": {
                "connection_type": "rpc",
                "cluster_url": "http://"+proxy,
            },
        }

        if config is not None:
            update_inplace(default_config, config)

        return YtClient(proxy=None, config=default_config)

    @authors("verytable")
    def test_discovery(self):
        for test_name, config in [
            (
                "defaults", {},
            ),
            (
                "explicit_address_type",
                {"driver_config": {"proxy_address_type": "internal_rpc"}},
            ),
            (
                "explicit_params",
                {"driver_config": {"proxy_address_type": "internal_rpc", "proxy_network_name": "default"}},
            )
        ]:
            yc = self._create_yt_rpc_client_with_http_discovery(config=config)
            assert yc.get("//tmp/@"), test_name

    @authors("verytable")
    def test_discovery_invalid_address_type(self):
        with pytest.raises(RuntimeError) as excinfo:
            yc = self._create_yt_rpc_client_with_http_discovery(config={"driver_config": {"proxy_address_type": "invalid"}})
            yc.get("//tmp/@")

        assert "Error creating driver" in str(excinfo.value)

    @authors("verytable")
    def test_discovery_invalid_network(self):
        with pytest.raises(YtError) as excinfo:
            yc = self._create_yt_rpc_client_with_http_discovery(config={"driver_config": {"proxy_network_name": "invalid"}})
            yc.get("//tmp/@")

        assert excinfo.value.contains_text("Proxy list is empty")


@pytest.mark.enabled_multidaemon
class TestRpcProxyBase(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True
    ENABLE_MULTIDAEMON = True

    _schema_dicts = [
        {"name": "index", "type": "int64"},
        {"name": "str", "type": "string"},
    ]
    _schema = make_schema(_schema_dicts, strict=True)

    _schema_dicts_sorted = [
        {"name": "index", "type": "int64", "sort_order": "ascending"},
        {"name": "str", "type": "string"},
    ]
    _schema_sorted = make_schema(_schema_dicts_sorted, strict=True, unique_keys=True)
    _sample_index = 241
    _sample_text = "sample text"
    _sample_line = {"index": _sample_index, "str": _sample_text}

    def _create_simple_table(self, path, data=[], dynamic=True, sorted=True, **kwargs):
        schema = self._schema_sorted if sorted else self._schema
        create("table", path, attributes={"dynamic": dynamic, "schema": schema}, **kwargs)

        if dynamic:
            sync_create_cells(1)
        if not data:
            return
        if dynamic:
            sync_mount_table(path)
            insert_rows(path, data)
            sync_unmount_table(path)
        else:
            write_table(path, data)

    def _start_simple_operation(self, cmd, **kwargs):
        self._create_simple_table("//tmp/t_in", data=[self._sample_line], sorted=True, dynamic=True)
        self._create_simple_table("//tmp/t_out", dynamic=False, sorted=True)

        return map(in_="//tmp/t_in", out="//tmp/t_out", track=False, mapper_command=cmd, **kwargs)

    def _start_simple_operation_on_fs(self, event_name="barrier", **kwargs):
        return self._start_simple_operation(events_on_fs().wait_event_cmd(event_name), **kwargs)

    def _start_simple_operation_with_breakpoint(self, cmd_with_breakpoint="BREAKPOINT", **kwargs):
        return self._start_simple_operation(events_on_fs().with_breakpoint(cmd_with_breakpoint), **kwargs)

    def _prepare_output_table(self):
        alter_table("//tmp/t_out", dynamic=True, schema=self._schema_sorted)
        sync_mount_table("//tmp/t_out")


##################################################################


@pytest.mark.enabled_multidaemon
class TestWritingRows(TestRpcProxyBase):
    @authors("faucct")
    def test_writing_large_rows(self):
        MB = 1024 ** 2
        data = [{"index": 0, "str": "F" * (17 * MB)}]
        create("table", "//tmp/table", attributes={"dynamic": False, "schema": self._schema})
        write_table("//tmp/table", data, table_writer={"max_row_weight": 20 * MB})
        assert read_table("//tmp/table") == data

    @authors("faucct")
    def test_writing_large_rows_129(self):
        MB = 1024 ** 2
        data = [{"index": 0, "str": "F" * (129 * MB)}]
        create("table", "//tmp/table", attributes={"dynamic": False, "schema": self._schema})
        with raises_yt_error(f'Expected <= {128 * MB}, found {130 * MB}'):
            write_table("//tmp/table", data, table_writer={"max_row_weight": 130 * MB})
        with raises_yt_error() as e:
            write_table("//tmp/table", data, table_writer={"max_row_weight": 128 * MB})
        assert 'Memory limit exceeded while parsing YSON stream: ' in str(e[0]) or 'Row weight is too large' in str(e[0])
        assert read_table("//tmp/table") == []

    @authors("faucct")
    @pytest.mark.parametrize("atomicity", ["none", "full"])
    def test_writing_large_dynamic_rows(self, atomicity):
        MB = 1024 ** 2
        data = [{"index": 0, "str": "F" * (17 * MB)}]
        create("table", "//tmp/dynamic", attributes={"dynamic": True, "schema": self._schema, "atomicity": atomicity})
        sync_create_cells(1)
        sync_mount_table("//tmp/dynamic")
        with raises_yt_error(f'Value of type "string" is too long: length {17 * MB}, limit {16 * MB}'):
            insert_rows("//tmp/dynamic", data, table_writer={"max_row_weight": 20 * MB})
        sync_unmount_table("//tmp/dynamic")
        assert read_table("//tmp/dynamic") == []


##################################################################


@pytest.mark.enabled_multidaemon
class TestRpcProxyClientRetries(TestRpcProxyBase):
    NUM_NODES = 2

    DELTA_RPC_DRIVER_CONFIG = {
        "enable_retries": True,
        "enable_exponential_retry_backoffs": True,
        "retry_backoff": {
            "invocation_count": 5,
            "min_backoff": 100,
        },
        "retry_timeout": 2000,
        "default_total_streaming_timeout": 1000,
        "proxy_list_update_period": 1000,
        "proxy_list_retry_period": 100,
    }

    DELTA_RPC_PROXY_CONFIG = {
        "retry_request_queue_size_limit_exceeded": False,
        "discovery_service": {
            "proxy_update_period": 100
        },
    }

    DELTA_MASTER_CONFIG = {
        "object_service": {
            "sticky_user_error_expire_time": 0
        }
    }

    ENABLE_MULTIDAEMON = True

    @classmethod
    def setup_class(cls):
        super(TestRpcProxyBase, cls).setup_class()
        native_config = deepcopy(cls.Env.configs["driver"])
        native_config["connection_type"] = "native"
        native_config["api_version"] = 3
        cls.native_driver = Driver(native_config)

    @authors("kiselyovp")
    # TODO(kiselyovp): a temporary measure, see YT-13024.
    @flaky
    def test_proxy_banned(self):
        rpc_proxy_addresses = ls("//sys/rpc_proxies")
        try:
            for i in range(5):
                set(
                    "//sys/rpc_proxies/{0}/@banned".format(rpc_proxy_addresses[i % self.NUM_RPC_PROXIES]),
                    True,
                )
                time.sleep(0.1)
                get("//@")
                set(
                    "//sys/rpc_proxies/{0}/@banned".format(rpc_proxy_addresses[i % self.NUM_RPC_PROXIES]),
                    False,
                )
        finally:
            for address in rpc_proxy_addresses:
                set(
                    "//sys/rpc_proxies/{0}/@banned".format(address),
                    False,
                    driver=self.native_driver,
                )

    @authors("kiselyovp")
    # TODO(kiselyovp): a temporary measure, see YT-13024.
    @flaky
    def test_proxy_banned_sticky(self):
        rpc_proxy_addresses = ls("//sys/rpc_proxies")
        try:
            tx = start_transaction(sticky=True)
            fails = 0
            for i, address in enumerate(rpc_proxy_addresses):
                set("//sys/rpc_proxies/{0}/@banned".format(address), True)
                time.sleep(0.2)
                start = time.time()
                try:
                    ping_transaction(tx)
                except YtError:
                    fails += 1
                end = time.time()
                assert end - start < 1.4
                set("//sys/rpc_proxies/{0}/@banned".format(address), False)
            assert fails == 1
        finally:
            for address in rpc_proxy_addresses:
                set(
                    "//sys/rpc_proxies/{0}/@banned".format(address),
                    False,
                    driver=self.native_driver,
                )

    @authors("kiselyovp")
    def test_request_queue_size_limit_exceeded(self):
        create_user("u")
        set("//sys/users/u/@request_queue_size_limit", 0)
        start = time.time()
        with pytest.raises(YtError):
            create("map_node", "//tmp/test", authenticated_user="u")
        end = time.time()
        assert end - start >= 1.4

        rsp = create("map_node", "//tmp/test", authenticated_user="u", return_response=True)
        time.sleep(0.1)
        assert not rsp.is_set()
        set("//sys/users/u/@request_queue_size_limit", 1)
        rsp.wait()
        if not rsp.is_ok():
            raise YtResponseError(rsp.error())
        assert exists("//tmp/test")

    @authors("kiselyovp")
    def test_streaming_without_retries(self):
        create("file", "//tmp/file")
        write_file("//tmp/file", b"abacaba")
        assert read_file("//tmp/file") == b"abacaba"

        create("table", "//tmp/table")
        write_table("//tmp/table", {"a": "b"})
        assert read_table("//tmp/table") == [{"a": "b"}]

        nodes = ls("//sys/cluster_nodes")
        set_node_banned(nodes[0], True)
        try:
            start = time.time()
            with pytest.raises(YtError):
                write_file("//tmp/file", b"dabacaba")
            end = time.time()
            assert end - start < 1.4
        finally:
            set_node_banned(nodes[0], False)


##################################################################


@pytest.mark.enabled_multidaemon
class TestOperationsRpcProxy(TestRpcProxyBase):
    ENABLE_MULTIDAEMON = True

    @authors("kiselyovp")
    def test_map_reduce_simple(self):
        self._create_simple_table("//tmp/t_in", data=[self._sample_line])

        self._create_simple_table("//tmp/t_out", dynamic=False)
        map_reduce(in_="//tmp/t_in", out="//tmp/t_out", sort_by="index", reducer_command="cat")

        self._prepare_output_table()

        assert len(select_rows("* from [//tmp/t_out]")) == 1
        assert len(lookup_rows("//tmp/t_out", [{"index": self._sample_index - 2}])) == 0
        assert len(lookup_rows("//tmp/t_out", [{"index": self._sample_index}])) == 1

    @authors("kiselyovp")
    def test_sort(self):
        size = 10 ** 3
        original_table = [{"index": num, "str": "number " + str(num)} for num in range(size)]
        new_table = deepcopy(original_table)
        shuffle(new_table)

        self._create_simple_table("//tmp/t_in1", data=new_table[:size // 2], sorted=False)
        self._create_simple_table("//tmp/t_in2", data=new_table[size // 2:], sorted=False)
        self._create_simple_table("//tmp/t_out", dynamic=False, sorted=True)
        sort(in_=["//tmp/t_in1", "//tmp/t_in2"], out="//tmp/t_out", sort_by="index")

        self._prepare_output_table()

        assert select_rows("* from [//tmp/t_out] LIMIT " + str(2 * size)) == original_table

    @authors("kiselyovp")
    def test_abort_operation(self):
        op = self._start_simple_operation_with_breakpoint()
        wait(lambda: op.get_state() == "running")

        op.abort()

        wait(lambda: op.get_state() == "aborted")

    @authors("kiselyovp")
    def test_complete_operation(self):
        op = self._start_simple_operation_with_breakpoint()
        wait(lambda: op.get_state() == "running")

        op.complete()

        op.track()
        assert op.get_state() == "completed"

    @authors("kiselyovp")
    def test_suspend_resume_operation(self):
        op = self._start_simple_operation_with_breakpoint()
        wait(lambda: op.get_state() == "running")

        op.suspend(abort_running_jobs=True)
        wait(lambda: get(op.get_path() + "/@suspended"))
        assert op.get_state() == "running"
        events_on_fs().release_breakpoint()
        time.sleep(2)
        assert get(op.get_path() + "/@suspended")
        assert op.get_state() == "running"

        assert op.get_alerts() == {}
        op.resume()
        assert op.get_alerts() == {}
        op.track()
        assert op.get_state() == "completed"

    @authors("faucct")
    def test_suspend_resume_operation_with_reason(self):
        op = self._start_simple_operation_with_breakpoint()
        wait(lambda: op.get_state() == "running")

        op.suspend(abort_running_jobs=True, reason="because")
        wait(lambda: get(op.get_path() + "/@suspended"))
        assert op.get_state() == "running"
        events_on_fs().release_breakpoint()
        time.sleep(2)
        assert get(op.get_path() + "/@suspended")
        assert op.get_state() == "running"

        alert = op.get_alerts()["operation_suspended"]
        assert alert["message"] == "Suspend operation by user request"
        assert alert["attributes"]["reason"] == "because"
        op.resume()
        assert op.get_alerts() == {}
        op.track()
        assert op.get_state() == "completed"

    @authors("kiselyovp")
    def test_update_op_params_check_perms(self):
        op = self._start_simple_operation_with_breakpoint()
        wait(lambda: op.get_state() == "running")

        create_user("u")

        update_op_parameters(op.id, parameters={"acl": [make_ace("allow", "u", ["read", "manage"])]})
        # No exception.
        op.complete(authenticated_user="u")

        events_on_fs().release_breakpoint()
        op.track()


##################################################################


@pytest.mark.enabled_multidaemon
class TestDumpJobContextRpcProxy(TestRpcProxyBase):
    ENABLE_MULTIDAEMON = True

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_reporter": {
                    "reporting_period": 10,
                    "min_repeat_delay": 10,
                    "max_repeat_delay": 10,
                }
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

    @authors("kiselyovp")
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


##################################################################


@pytest.mark.enabled_multidaemon
class TestPessimisticQuotaCheckRpcProxy(TestRpcProxyBase):
    ENABLE_MULTIDAEMON = True

    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 0

    REPLICATOR_REACTION_TIME = 3.5

    DELTA_RPC_PROXY_CONFIG = {
        "api_service": {
            "security_manager": {
                "user_cache": {
                    "expire_after_successful_update_time": 1000,
                    "refresh_time": 100,
                    "expire_after_failed_update_time": 100,
                    "expire_after_access_time": 100,
                }
            }
        }
    }

    def _set_account_chunk_count_limit(self, account, value):
        set("//sys/accounts/{0}/@resource_limits/chunk_count".format(account), value)

    def _set_account_tablet_count_limit(self, account, value):
        set("//sys/accounts/{0}/@resource_limits/tablet_count".format(account), value)

    def _is_account_disk_space_limit_violated(self, account):
        return get("//sys/accounts/{0}/@violated_resource_limits/disk_space".format(account))

    def _is_account_chunk_count_limit_violated(self, account):
        return get("//sys/accounts/{0}/@violated_resource_limits/chunk_count".format(account))

    @authors("kiselyovp")
    def test_chunk_count_limits(self):
        create_account("max")
        self._set_account_tablet_count_limit("max", 100500)

        self._create_simple_table("//tmp/t", [self._sample_line])
        create("map_node", "//tmp/a")
        set("//tmp/a/@account", "max")

        self._set_account_chunk_count_limit("max", 0)
        with pytest.raises(YtError):
            copy("//tmp/t", "//tmp/a/t")
        assert not exists("//tmp/a/t")
        copy("//tmp/t", "//tmp/a/t", pessimistic_quota_check=False)
        assert exists("//tmp/a/t")

    @authors("kiselyovp")
    def test_disk_space_limits(self):
        create_account("max")
        self._set_account_tablet_count_limit("max", 100500)

        self._create_simple_table("//tmp/t", [self._sample_line])
        create("map_node", "//tmp/a")
        set("//tmp/a/@account", "max")

        set_account_disk_space_limit("max", 0)
        with pytest.raises(YtError):
            copy("//tmp/t", "//tmp/a/t")
        assert not exists("//tmp/a/t")
        copy("//tmp/t", "//tmp/a/t", pessimistic_quota_check=False)
        assert exists("//tmp/a/t")

    @authors("savrus")
    def test_user_ban(self):
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")
        create_user("a")
        set("//sys/users/a/@banned", True)
        with pytest.raises(YtError):
            explain_query("1 from [//tmp/t]", authenticated_user="a")
        set("//sys/users/a/@banned", False)
        time.sleep(0.5)
        explain_query("1 from [//tmp/t]", authenticated_user="a")


##################################################################


@pytest.mark.enabled_multidaemon
class TestPessimisticQuotaCheckMulticellRpcProxy(TestPessimisticQuotaCheckRpcProxy):
    NUM_SECONDARY_MASTER_CELLS = 2
    NUM_SCHEDULERS = 1
    ENABLE_MULTIDAEMON = True


##################################################################


@pytest.mark.enabled_multidaemon
class TestModifyRowsRpcProxy(TestRpcProxyBase):
    ENABLE_MULTIDAEMON = True
    BATCH_CAPACITY = 10
    DELTA_RPC_DRIVER_CONFIG = {"modify_rows_batch_capacity": BATCH_CAPACITY}

    def _test_modify_rows_batching(self, request_count, key_count, tx_type="tablet"):
        self._create_simple_table("//tmp/table")
        sync_mount_table("//tmp/table")

        tx = start_transaction(type=tx_type, sticky=True)

        for i in range(request_count):
            insert_rows(
                "//tmp/table",
                [{"index": i % key_count, "str": str(i // key_count)}],
                tx=tx,
            )

        commit_transaction(tx)

        expected_result = [{"index": i, "str": str((request_count - i - 1) // key_count)} for i in range(key_count)]
        assert select_rows("* from [//tmp/table]") == expected_result

        sync_unmount_table("//tmp/table")
        remove("//tmp/table")

    @authors("kiselyovp")
    def test_modify_rows_batching(self):
        self._test_modify_rows_batching(60, 7, "tablet")


##################################################################


@pytest.mark.enabled_multidaemon
class TestRpcProxyWithoutDiscovery(TestRpcProxyBase):
    ENABLE_MULTIDAEMON = True
    NUM_RPC_PROXIES = 1
    ENABLE_HTTP_PROXY = False
    NUM_HTTP_PROXIES = 0
    DELTA_RPC_PROXY_CONFIG = {"discovery_service": {"enable": False}}

    @authors("sashbel")
    def test_proxy_without_discovery(self):
        # check discovery dir is empty
        assert ls("//sys/rpc_proxies") == []

        self._create_simple_table("//tmp/t_in", data=[self._sample_line])
        sync_mount_table("//tmp/t_in")
        assert select_rows("* from [//tmp/t_in]") == [self._sample_line]


##################################################################


@pytest.mark.enabled_multidaemon
class TestCompressionRpcProxy(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True
    USE_DYNAMIC_TABLES = True

    DELTA_DRIVER_CONFIG = {
        "request_codec": "lz4",
        "response_codec": "quick_lz",
    }

    @authors("babenko")
    def test_simple_rpc_calls(self):
        set("//tmp/@foo", 1)
        get("//tmp/@foo")

    @authors("babenko")
    def test_rpc_streaming(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})
        read_table("//tmp/t")

    @authors("babenko")
    def test_rpc_attachments(self):
        sync_create_cells(1)
        create_dynamic_table(
            "//tmp/d",
            schema=[
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ],
        )
        sync_mount_table("//tmp/d")
        insert_rows("//tmp/d", [{"key": 0, "value": "foo"}])
        lookup_rows("//tmp/d", [{"key": 0}])


@pytest.mark.enabled_multidaemon
class TestModernCompressionRpcProxy(TestCompressionRpcProxy):
    ENABLE_MULTIDAEMON = True
    DELTA_DRIVER_CONFIG = {
        "request_codec": "lz4",
        "response_codec": "quick_lz",
        "enable_legacy_rpc_codecs": False,
    }


##################################################################


@pytest.mark.enabled_multidaemon
class TestRpcProxyFormatConfig(TestRpcProxyBase, _TestProxyFormatConfigBase):
    ENABLE_MULTIDAEMON = True

    def setup_method(self, method):
        super(TestRpcProxyFormatConfig, self).setup_method(method)
        proxy_name = ls("//sys/rpc_proxies")[0]

        set("//sys/rpc_proxies/@config", {"api": {"formats": self.FORMAT_CONFIG}})

        def config_updated():
            config = get("//sys/rpc_proxies/" + proxy_name + "/orchid/dynamic_config_manager/effective_config")
            return config \
                .get("api", {}) \
                .get("formats", {}) \
                .get("yamred_dsv", {}) \
                .get("user_overrides", {}) \
                .get("good_user", False)

        wait(config_updated)

    def _do_run_operation(self, op_type, spec, user, use_start_op):
        operation = Operation()
        params = {
            "operation_type": op_type,
            "spec": spec,
            "authenticated_user": user,
        }
        operation.id = yson.loads(execute_command("start_op", params))["operation_id"]
        return operation

    def _test_format_enable(self, format, user, enable):
        self._test_format_enable_operations(format, user, enable)

    def _test_format_defaults(self, format, user, content, expected_format):
        assert str(expected_format) == "yson"
        yson_format = expected_format.attributes.get("format", "text")
        expected_content_operation = yson.dumps(content, yson_format=yson_format, yson_type="list_fragment")
        self._test_format_defaults_operations(format, user, content, expected_content_operation)


##################################################################


@pytest.mark.skipif(is_asan_build(), reason="Memory allocation is not reported under ASAN")
@pytest.mark.enabled_multidaemon
class TestRpcProxyHeapUsageStatisticsBase(TestRpcProxyBase):
    ENABLE_MULTIDAEMON = True
    NUM_RPC_PROXIES = 1

    @classmethod
    def setup_class(cls):
        cls.DELTA_RPC_PROXY_CONFIG.update({
            "heap_profiler": {
                "sampling_rate": 32 << 10,
                "snapshot_update_period": 20,
            },
        })
        super().setup_class()

    def enable_allocation_tags(self, proxy):
        set(f"//sys/{proxy}/@config", {
            "api": {
                "enable_allocation_tags": True,
            },
        })
        wait(lambda: get(f"//sys/{proxy}/@config")["api"]["enable_allocation_tags"])

    def prepare_allocation(self, proxy, user):
        self.enable_allocation_tags(proxy)

        if not exists(f"//sys/users/{user}"):
            create_user(user)

        wait(lambda: exists(f"//sys/users/{user}"))

        self._create_simple_table("//tmp/t_in", data=[self._sample_line], sorted=True, dynamic=True, authenticated_user=user)
        self._create_simple_table("//tmp/t_out", dynamic=False, sorted=True, authenticated_user=user)
        map(in_="//tmp/t_in", out="//tmp/t_out", track=False, mapper_command="cat", authenticated_user=user)

    def check_memory_usage(self, memory_usage, user):
        tags = ["user", "rpc"]
        if not memory_usage or len(memory_usage) < len(tags):
            return False

        for tag in tags:
            assert tag in memory_usage
            if not memory_usage[tag]:
                return False

        if user not in memory_usage["user"].keys():
            return False

        if memory_usage["user"][user] < 1024 ** 2:
            return False

        rpc_total_usage = 0
        for value in memory_usage["rpc"].values():
            rpc_total_usage += value

        assert rpc_total_usage / len(memory_usage["rpc"]) > 1024 ** 2

        user_total_usage = 0
        for value in memory_usage["user"].values():
            user_total_usage += value

        assert user_total_usage == rpc_total_usage

        return True


##################################################################


@pytest.mark.skipif(is_asan_build(), reason="Memory allocation is not reported under ASAN")
class TestRpcProxyHeapUsageStatistics(TestRpcProxyHeapUsageStatisticsBase):
    ENABLE_MULTIDAEMON = False  # Checks profiling.
    DELTA_RPC_PROXY_CONFIG = {
        "api_service": {
            "testing": {
                "heap_profiler": {
                    "allocation_size": 5 * 1024 ** 2,
                    "allocation_release_delay": 30 * 1000,
                },
            },
        },
    }

    def _get_user(self):
        return f"u{randint(100000, 999999)}"

    @authors("ni-stoiko")
    @pytest.mark.timeout(120)
    def test_heap_usage_statistics(self):
        user = self._get_user()
        self.prepare_allocation("rpc_proxies", user)

        rpc_proxies = ls("//sys/rpc_proxies")
        assert len(rpc_proxies) == 1
        rpc_proxy_agent_orchid = f"//sys/rpc_proxies/{rpc_proxies[0]}/orchid/rpc_proxy"

        wait(lambda: self.check_memory_usage(get(rpc_proxy_agent_orchid + "/heap_usage"), user))

    @authors("ni-stoiko", "galtsev")
    @pytest.mark.timeout(120)
    @flaky(max_runs=10)
    def test_heap_usage_gauge(self):
        user = self._get_user()
        self.prepare_allocation("rpc_proxies", user)

        rpc_proxies = ls("//sys/rpc_proxies")
        assert len(rpc_proxies) == 1

        profiler = profiler_factory().at_rpc_proxy(rpc_proxies[0])
        rpc_memory_usage_gauge = profiler.gauge("memory/heap_usage/rpc")
        user_memory_usage_gauge = profiler.gauge("memory/heap_usage/user")

        def check(statistics, tag, memory=0):
            for stat in statistics:
                if stat["tags"] and tag == stat["tags"]:
                    return stat["value"] > memory
            return False

        """ Example for rpc tag.
        [
            {'tags': {'rpc': 'ModifyRows'}, 'value': 16908288},
            {'tags': {'rpc': 'UnmountTable'}, 'value': 15851520},
            {'tags': {'rpc': 'ListNode'}, 'value': 116244480},
            {'tags': {}, 'value': 2759430144},
            {'tags': {'rpc': 'PingTransaction'}, 'value': 12681216},
            {'tags': {'rpc': 'CreateObject'}, 'value': 22192128},
            {'tags': {'rpc': 'StartTransaction'}, 'value': 29589504},
            {'tags': {'rpc': 'StartOperation'}, 'value': 15851520},
            {'tags': {'rpc': 'GetNode'}, 'value': 2420207616},
            {'tags': {'rpc': 'GetTableMountInfo'}, 'value': 28532736},
            {'tags': {'rpc': 'CreateNode'}, 'value': 33816576},
            {'tags': {'rpc': 'CommitTransaction'}, 'value': 22192128},
            {'tags': {'rpc': 'MountTable'}, 'value': 25362432}
        ]
        """
        wait(lambda: check(rpc_memory_usage_gauge.get_all(), {"rpc": "CreateNode"}))
        wait(lambda: check(rpc_memory_usage_gauge.get_all(), {"rpc": "ListNode"}))
        wait(lambda: check(rpc_memory_usage_gauge.get_all(), {"rpc": "MountTable"}))
        wait(lambda: check(rpc_memory_usage_gauge.get_all(), {"rpc": "StartOperation"}))

        """ Example for user tag.
        [
            {'tags': {}, 'value': 883458048},
            {'tags': {'user': 'root'}, 'value': 832733184},
            {'tags': {'user': 'u1'}, 'value': 50724864}
        ]
        """
        wait(lambda: check(user_memory_usage_gauge.get_all(), {"user": user}))

        def get_usage(path, tag):
            profiler = profiler_factory().at_rpc_proxy(rpc_proxies[0])
            statistics = profiler.gauge(path).get_all()

            for stat in statistics:
                if stat["tags"] and tag == stat["tags"]:
                    return stat["value"]
            return -1

        wait(lambda: get_usage("memory/heap_usage/rpc", {"rpc": "StartTransaction"}) > 0)
        wait(lambda: get_usage("memory/heap_usage/user", {"user": user}) > 0)


##################################################################


@pytest.mark.skipif(is_asan_build(), reason="Memory allocation is not reported under ASAN")
@pytest.mark.enabled_multidaemon
class TestRpcProxyNullApiTestingOptions(TestRpcProxyHeapUsageStatisticsBase):
    ENABLE_MULTIDAEMON = True

    @authors("ni-stoiko")
    def test_null_api_testing_options(self):
        self.prepare_allocation("rpc_proxies", "user")


@pytest.mark.enabled_multidaemon
class TestRpcProxySignaturesBase(TestRpcProxyBase):
    ENABLE_MULTIDAEMON = True
    DELTA_RPC_PROXY_CONFIG = {
        "signature_validation": {
            "cypress_key_reader": dict(),
            "validator": dict(),
        },
        "signature_generation": {
            "cypress_key_writer": {
                "owner_id": "test-rpc-proxy",
            },
            "generator": dict(),
        },
    }

    # NB(pavook): to avoid key owner collision.
    NUM_RPC_PROXIES = 1

    OWNERS_PATH = "//sys/public_keys/by_owner"
    KEYS_PATH = f"{OWNERS_PATH}/test-rpc-proxy"


def deep_update(source: dict[Any, Any], overrides: dict[Any, Any]) -> dict[Any, Any]:
    """
    Update a nested dictionary.
    """
    result = source.copy()
    for key, value in overrides.items():
        if isinstance(value, dict) and value:
            returned = deep_update(source.get(key, {}), value)
            result[key] = returned
        else:
            result[key] = value
    return result


@pytest.mark.enabled_multidaemon
class TestRpcProxySignaturesKeyCreation(TestRpcProxySignaturesBase):
    ENABLE_MULTIDAEMON = True
    DELTA_RPC_PROXY_CONFIG = deep_update(TestRpcProxySignaturesBase.DELTA_RPC_PROXY_CONFIG, {
        "signature_generation": {
            "key_rotator": {
                "key_rotation_interval": "2h",
            },
        },
    })

    @authors("pavook")
    @pytest.mark.timeout(60)
    def test_public_key_appears(self):
        wait(lambda: len(ls(self.KEYS_PATH)) == 1)


@pytest.mark.enabled_multidaemon
class TestRpcProxySignaturesKeyRotation(TestRpcProxySignaturesBase):
    ENABLE_MULTIDAEMON = True
    DELTA_RPC_PROXY_CONFIG = deep_update(TestRpcProxySignaturesBase.DELTA_RPC_PROXY_CONFIG, {
        "signature_generation": {
            "key_rotator": {
                "key_rotation_interval": "200ms",
            },
        },
    })

    @authors("pavook")
    @pytest.mark.timeout(60)
    def test_public_key_rotates(self):
        wait(lambda: len(ls(self.KEYS_PATH)) > 1)
