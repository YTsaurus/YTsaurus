from __future__ import print_function

import yt_commands
import yt_sequoia_helpers
import yt_scheduler_helpers

try:
    from yt_tests_settings import has_tvm_service_support
except ImportError:
    from yt_tests_opensource_settings import has_tvm_service_support

from yt.environment import YTInstance, arcadia_interop
from yt.environment.api import LocalYtConfig
from yt.environment.helpers import (
    emergency_exit_within_tests,
    find_cri_endpoint,
)
from yt.environment.porto_helpers import porto_available
from yt.environment.default_config import (
    get_dynamic_cypress_proxy_config,
    get_dynamic_master_config,
)
from yt.environment.helpers import (  # noqa
    Restarter,
    SCHEDULERS_SERVICE,
    CONTROLLER_AGENTS_SERVICE,
    NODES_SERVICE,
    CHAOS_NODES_SERVICE,
    MASTERS_SERVICE,
    MASTER_CACHES_SERVICE,
    QUEUE_AGENTS_SERVICE,
    RPC_PROXIES_SERVICE,
    HTTP_PROXIES_SERVICE,
    KAFKA_PROXIES_SERVICE,
)

from yt.sequoia_tools import DESCRIPTORS

from yt.test_helpers import wait, WaitFailed, get_work_path, get_build_root, get_tests_sandbox
import yt.test_helpers.cleanup as test_cleanup

from yt.common import YtResponseError, format_error, update_inplace
import yt.logger

from yt_commands import print_debug
from yt_driver_bindings import reopen_logs
from yt_helpers import master_exit_read_only_sync

import pytest

import gc
import os
import sys
import logging
import decorator
import functools
import shutil
import hashlib

from time import sleep, time
from threading import Thread

OUTPUT_PATH = None
SANDBOX_ROOTDIR = None

##################################################################


def prepare_yatest_environment(need_suid, artifact_components=None, force_create_environment=False, extra_artifact_components=None):
    yt.logger.LOGGER.setLevel(logging.DEBUG)
    artifact_components = artifact_components or {}

    global OUTPUT_PATH
    global SANDBOX_ROOTDIR

    # This env var is used for determining if we are in Devtools' ytexec environment or not.
    ytrecipe = os.environ.get("YT_OUTPUT") is not None

    bin_paths = []
    for path_suffix, components in (("", artifact_components), ("extra", extra_artifact_components)):
        if components is None:
            bin_paths.append(None)
            continue
        destination = os.path.join(get_work_path(), "build", "suid" if need_suid else "nosuid", path_suffix)

        if "trunk" in artifact_components:
            # Explicitly specifying trunk components is not necessary as we already assume
            # any component to be taken from trunk by default. We still allow doing that for clarity.
            # So just skip such components.
            artifact_components.pop("trunk")

        bin_path = os.path.join(destination, "bin")

        if (force_create_environment or artifact_components) and os.path.exists(destination):
            shutil.rmtree(destination)

        if not os.path.exists(destination):
            os.makedirs(destination)
            path = arcadia_interop.prepare_yt_environment(
                destination,
                binary_root=get_build_root(),
                copy_ytserver_all=not ytrecipe,
                need_suid=need_suid and not ytrecipe,
                artifact_components=components,
            )
            assert path == bin_path

        bin_paths.append(bin_path)

    SANDBOX_ROOTDIR = get_tests_sandbox(arcadia_suffix=None)
    if arcadia_interop.yatest_common is not None:
        OUTPUT_PATH = arcadia_interop.yatest_common.output_path()
    else:
        OUTPUT_PATH = SANDBOX_ROOTDIR

    return bin_paths


def search_binary_path(binary_name):
    return arcadia_interop.search_binary_path(binary_name, binary_root=get_build_root())


def _retry_with_gc_collect(func, driver=None):
    while True:
        try:
            func()
            break
        except YtResponseError:
            yt_commands.gc_collect(driver=driver)


def find_ut_file(file_name):
    import library.python.resource as rs

    with open(file_name, "wb") as bc:
        bc_content = rs.find("/llvm_bc/" + file_name.split(".")[0])
        bc.write(bc_content)
    return file_name


def skip_if_porto(func):
    def wrapper(func, self, *args, **kwargs):
        if hasattr(self, "USE_PORTO") and self.USE_PORTO:
            pytest.skip("This test does not support Porto isolation")
        return func(self, *args, **kwargs)

    return decorator.decorate(func, wrapper)


def get_sanitizer_type():
    if arcadia_interop.yatest_common is None:
        return None

    try:
        return arcadia_interop.yatest_common.context.sanitize
    except NotImplementedError:
        # This method is called from is_{asan,msan}_build which may be
        # called outside of testing runtime. For example, when one test
        # imports class from another test, Arcadia binary python building
        # system collects dependencies; during that process class code is
        # interpreted, and if one of the tests imposes pytest.mark.skipif
        # depending on sanitizer type, error is raised.
        #
        # We do not want error to be raised in this case as it breaks
        # Arcadia's import_test.
        return None


def is_asan_build():
    return get_sanitizer_type() == "address"


def is_msan_build():
    return get_sanitizer_type() == "memory"


def is_debug_build():
    if arcadia_interop.yatest_common is None:
        return False

    return "debug" in arcadia_interop.yatest_common.context.build_type


def skip_if_rpc_driver_backend(func):
    def wrapper(func, self, *args, **kwargs):
        if self.DRIVER_BACKEND == "rpc":
            pytest.skip("This test is not supported with RPC proxy driver backend")
        return func(self, *args, **kwargs)

    return decorator.decorate(func, wrapper)


def parametrize_external(func):
    spec = decorator.getfullargspec(func)
    index = spec.args.index("external")

    def wrapper(func, self, *args, **kwargs):
        if self.NUM_SECONDARY_MASTER_CELLS == 0 and args[index - 1] is True:
            pytest.skip("No secondary cells")
        return func(self, *args, **kwargs)

    return pytest.mark.parametrize("external", [False, True])(decorator.decorate(func, wrapper))


class Checker(Thread):
    def __init__(self, check_function):
        super(Checker, self).__init__()
        self._check_function = check_function
        self._active = None

    def start(self):
        self._active = True
        super(Checker, self).start()

    def run(self):
        while self._active:
            now = time()
            self._check_function()
            delta = time() - now
            if delta > 0.1:
                print("Check took %lf seconds" % delta, file=sys.stderr)
            sleep(1.0)

    def stop(self):
        self._active = False
        self.join()


class YTEnvSetup(object):
    NUM_MASTERS = 3
    NUM_CLOCKS = 0
    NUM_DISCOVERY_SERVERS = 0
    NUM_TIMESTAMP_PROVIDERS = 0
    NUM_NONVOTING_MASTERS = 0
    NUM_SECONDARY_MASTER_CELLS = 0
    DEFER_SECONDARY_CELL_START = False
    ENABLE_SECONDARY_CELLS_CLEANUP = True
    MASTER_CELL_DESCRIPTORS = {}
    NUM_NODES = 5
    DEFER_NODE_START = False
    NUM_CHAOS_NODES = 0
    DEFER_CHAOS_NODE_START = False
    NUM_MASTER_CACHES = 0
    NUM_SCHEDULERS = 0
    DEFER_SCHEDULER_START = False
    NUM_CONTROLLER_AGENTS = None
    DEFER_CONTROLLER_AGENT_START = False
    ENABLE_HTTP_PROXY = False
    ENABLE_CHYT_HTTP_PROXIES = False
    ENABLE_CHYT_HTTPS_PROXIES = False
    NUM_HTTP_PROXIES = 1
    ENABLE_RPC_PROXY = None
    NUM_RPC_PROXIES = 2
    DRIVER_BACKEND = "native"
    NODE_PORT_SET_SIZE = None
    STORE_LOCATION_COUNT = 1
    ARTIFACT_COMPONENTS = {}
    EXTRA_ARTIFACT_COMPONENTS = None
    FORCE_CREATE_ENVIRONMENT = False
    NUM_CELL_BALANCERS = 0
    ENABLE_BUNDLE_CONTROLLER = False
    NUM_QUEUE_AGENTS = 0
    NUM_KAFKA_PROXIES = 0
    NUM_TABLET_BALANCERS = 0
    NUM_CYPRESS_PROXIES = 0
    NUM_REPLICATED_TABLE_TRACKERS = 0
    ENABLE_RESOURCE_TRACKING = False
    ENABLE_TVM_ONLY_PROXIES = False
    ENABLE_DYNAMIC_TABLE_COLUMN_RENAMES = True
    ENABLE_STATIC_DROP_COLUMN = True
    ENABLE_DYNAMIC_DROP_COLUMN = True
    ENABLE_ALLOW_SECONDARY_INDICES = True
    ENABLE_TLS = False

    JOB_PROXY_LOGGING = {"mode": "simple"}

    DELTA_NODE_FLAVORS = []

    DELTA_DRIVER_CONFIG = {
        "table_writer": {
            "enable_large_columnar_statistics": True,
        },
    }
    DELTA_DRIVER_LOGGING_CONFIG = {}
    DELTA_RPC_DRIVER_CONFIG = {}
    DELTA_MASTER_CONFIG = {}
    DELTA_DYNAMIC_MASTER_CONFIG = {}
    DELTA_NODE_CONFIG = {}
    DELTA_DYNAMIC_NODE_CONFIG = {}
    DELTA_CHAOS_NODE_CONFIG = {}
    DELTA_SCHEDULER_CONFIG = {}
    DELTA_CONTROLLER_AGENT_CONFIG = {}
    _DEFAULT_DELTA_CONTROLLER_AGENT_CONFIG = {
        "operation_options": {
            "spec_template": {
                "max_failed_job_count": 1,
            }
        },
        "controller_agent": {
            "enable_table_column_renaming": True,
            "map_operation_options": {
                "spec_template": {
                    "job_io": {
                        "table_writer": {
                            "enable_large_columnar_statistics": True,
                        },
                        "dynamic_table_writer": {
                            "enable_large_columnar_statistics": True,
                        },
                    },
                }
            },
            "map_reduce_operation_options": {
                "spec_template": {
                    "map_job_io": {
                        "table_writer": {
                            "enable_large_columnar_statistics": True,
                        },
                        "dynamic_table_writer": {
                            "enable_large_columnar_statistics": True,
                        },
                    },
                    "sort_job_io": {
                        "table_writer": {
                            "enable_large_columnar_statistics": True,
                        },
                        "dynamic_table_writer": {
                            "enable_large_columnar_statistics": True,
                        },
                    },
                    "reduce_job_io": {
                        "table_writer": {
                            "enable_large_columnar_statistics": True,
                        },
                        "dynamic_table_writer": {
                            "enable_large_columnar_statistics": True,
                        },
                    },
                }
            },
            "sort_operation_options": {
                "spec_template": {
                    "merge_job_io": {
                        "table_writer": {
                            "enable_large_columnar_statistics": True,
                        },
                        "dynamic_table_writer": {
                            "enable_large_columnar_statistics": True,
                        },
                    },
                    "partition_job_io": {
                        "table_writer": {
                            "enable_large_columnar_statistics": True,
                        },
                        "dynamic_table_writer": {
                            "enable_large_columnar_statistics": True,
                        },
                    },
                    "sort_job_io": {
                        "table_writer": {
                            "enable_large_columnar_statistics": True,
                        },
                        "dynamic_table_writer": {
                            "enable_large_columnar_statistics": True,
                        },
                    },
                }
            },
            "sorted_merge_operation_options": {
                "spec_template": {
                    "job_io": {
                        "table_writer": {
                            "enable_large_columnar_statistics": True,
                        },
                        "dynamic_table_writer": {
                            "enable_large_columnar_statistics": True,
                        },
                    },
                },
            },
            "unordered_merge_operation_options": {
                "spec_template": {
                    "job_io": {
                        "table_writer": {
                            "enable_large_columnar_statistics": True,
                        },
                        "dynamic_table_writer": {
                            "enable_large_columnar_statistics": True,
                        },
                    },
                },
            },
            "ordered_merge_operation_options": {
                "spec_template": {
                    "job_io": {
                        "table_writer": {
                            "enable_large_columnar_statistics": True,
                        },
                        "dynamic_table_writer": {
                            "enable_large_columnar_statistics": True,
                        },
                    },
                },
            },
        },
    }
    DELTA_PROXY_CONFIG = {}
    DELTA_RPC_PROXY_CONFIG = {
        "api_service": {
            "enable_large_columnar_statistics": True,
        },
    }
    DELTA_CELL_BALANCER_CONFIG = {}
    DELTA_TABLET_BALANCER_CONFIG = {}
    DELTA_MASTER_CACHE_CONFIG = {}
    DELTA_QUEUE_AGENT_CONFIG = {}
    DELTA_KAFKA_PROXY_CONFIG = {}
    DELTA_CYPRESS_PROXY_CONFIG = {}
    DELTA_CYPRESS_PROXY_DYNAMIC_CONFIG = {}

    USE_PORTO = False  # Enables use_slot_user_id, use_porto_for_servers, jobs_environment_type="porto"
    USE_SLOT_USER_ID = None  # If set explicitly, overrides USE_PORTO.
    JOB_ENVIRONMENT_TYPE = None  # "porto", "cri"
    USE_CUSTOM_ROOTFS = False
    USE_DYNAMIC_TABLES = False
    USE_MASTER_CACHE = False
    USE_PERMISSION_CACHE = True
    # USE_SEQUOIA overrides this setting.
    USE_PRIMARY_CLOCKS = True

    USE_SEQUOIA = False
    ENABLE_CYPRESS_TRANSACTIONS_IN_SEQUOIA = False
    VALIDATE_SEQUOIA_TREE_CONSISTENCY = False

    # Ground cluster should be lean by default.
    NUM_MASTERS_GROUND = 1
    NUM_NODES_GROUND = 1
    NUM_HTTP_PROXIES_GROUND = 0
    NUM_RPC_PROXIES_GROUND = 1
    NUM_SCHEDULERS_GROUND = 0
    NUM_NONVOTING_MASTERS_GROUND = 0
    NUM_SECONDARY_MASTER_CELLS_GROUND = 0
    NUM_CHAOS_NODES_GROUND = 0
    NUM_MASTER_CACHES_GROUND = 0
    NUM_CELL_BALANCERS_GROUND = 0
    NUM_CONTROLLER_AGENTS_GROUND = 0
    NUM_QUEUE_AGENTS_GROUND = 0
    NUM_TABLET_BALANCERS_GROUND = 0
    NUM_CYPRESS_PROXIES_GROUND = 0

    ENABLE_TMP_ROOTSTOCK = False
    ENABLE_BULK_INSERT = False
    ENABLE_HUNKS_REMOTE_COPY = False
    ENABLE_TMP_PORTAL = False
    ENABLE_TABLET_BALANCER = False
    ENABLE_STANDALONE_TABLET_BALANCER = False
    ENABLE_STANDALONE_REPLICATED_TABLE_TRACKER = False

    NUM_REMOTE_CLUSTERS = 0
    NUM_TEST_PARTITIONS = 1
    CLASS_TEST_LIMIT = 8 * 60  # limits all test cases in class duration inside partition (seconds)
    NODE_IO_ENGINE_TYPE = None  # use "thread_pool" or "uring" or "weighted_fair_share_thread_pool"
    NODE_USE_DIRECT_IO_FOR_READS = "never"

    # COMPAT(kvk1920)
    TEST_MAINTENANCE_FLAGS = False

    WAIT_FOR_DYNAMIC_CONFIG = True

    @classmethod
    def is_multicell(cls):
        return cls.NUM_SECONDARY_MASTER_CELLS > 0

    @classmethod
    def get_num_secondary_master_cells(cls):
        return cls.NUM_SECONDARY_MASTER_CELLS

    # To be redefined in successors
    @classmethod
    def modify_master_config(cls, config, tag, peer_index, cluster_index):
        pass

    @classmethod
    def modify_clock_config(cls, config, cluster_index, master_cell_tag):
        pass

    @classmethod
    def modify_scheduler_config(cls, config, cluster_index):
        pass

    @classmethod
    def modify_queue_agent_config(cls, config):
        pass

    @classmethod
    def modify_kafka_proxy_config(cls, config):
        pass

    @classmethod
    def modify_controller_agent_config(cls, config, cluster_index):
        pass

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        pass

    @classmethod
    def modify_chaos_node_config(cls, config, cluster_index):
        pass

    @classmethod
    def modify_proxy_config(cls, config):
        pass

    @classmethod
    def modify_rpc_proxy_config(cls, config):
        pass

    @classmethod
    def modify_driver_config(cls, config):
        pass

    @classmethod
    def modify_cell_balancer_config(cls, config):
        pass

    @classmethod
    def modify_tablet_balancer_config(cls, config):
        pass

    @classmethod
    def modify_cypress_proxy_config(cls, config):
        pass

    @classmethod
    def modify_master_cache_config(cls, config):
        pass

    @classmethod
    def modify_timestamp_providers_configs(cls, timestamp_providers_configs, clock_configs, yt_configs):
        return False

    @classmethod
    def on_masters_started(cls):
        pass

    @classmethod
    def get_ground_index_offset(cls):
        return cls.NUM_REMOTE_CLUSTERS + 1

    @classmethod
    def _is_ground_cluster(cls, cluster_index):
        return cluster_index >= cls.get_ground_index_offset()

    @classmethod
    def _get_param_real_name(cls, name, cluster_index):
        if cluster_index == 0:
            primary_cluster_param_name = f"{name}_PRIMARY"
            if hasattr(cls, primary_cluster_param_name):
                return primary_cluster_param_name
            return name

        if cls._is_ground_cluster(cluster_index):
            non_ground_cluster_index = cluster_index - cls.get_ground_index_offset()
            if non_ground_cluster_index != 0:
                param_name = f"{name}_REMOTE_{non_ground_cluster_index - 1}_GROUND"
                if hasattr(cls, param_name):
                    return param_name
            param_name = f"{name}_GROUND"
            if hasattr(cls, param_name):
                return param_name

        param_name = f"{name}_REMOTE_{cluster_index - 1}"
        if hasattr(cls, param_name):
            return param_name

        return name

    @classmethod
    def find_node_with_flavors(self, required_flavors):
        for node in yt_commands.get("//sys/cluster_nodes"):
            flavors = yt_commands.get("//sys/cluster_nodes/{}/@flavors".format(node))
            found = (sorted(flavors) == sorted(required_flavors))
            if found:
                return node
        return None

    @classmethod
    def get_param(cls, name, cluster_index):
        actual_name = cls._get_param_real_name(name, cluster_index)

        return getattr(cls, actual_name)

    @classmethod
    def partition_items(cls, items):
        if cls.NUM_TEST_PARTITIONS == 1:
            return [items]

        partitions = [[] for _ in range(cls.NUM_TEST_PARTITIONS)]
        for item in items:
            nodeid = item.nodeid.encode("utf-8")
            hexdigest = hashlib.md5(nodeid).hexdigest()
            partitions[int(hexdigest, 16) % len(partitions)].append(item)
        return partitions

    @classmethod
    def modify_driver_logging_config(cls, config):
        update_inplace(config, cls.DELTA_DRIVER_LOGGING_CONFIG)

    @classmethod
    def create_yt_cluster_instance(cls, index, path):
        modify_configs_func = functools.partial(cls.apply_config_patches, cluster_index=index, cluster_path=path)
        modify_dynamic_configs_func = functools.partial(cls.apply_node_dynamic_config_patches, cluster_index=index)
        modify_driver_logging_config_func = cls.modify_driver_logging_config

        yt.logger.info("Creating cluster instance")

        if hasattr(cls, "USE_NATIVE_AUTH"):
            use_native_auth = getattr(cls, "USE_NATIVE_AUTH")
        else:
            use_native_auth = has_tvm_service_support

        secondary_cell_count = 0 if cls._is_ground_cluster(index) else cls.get_param("NUM_SECONDARY_MASTER_CELLS", index)
        cypress_proxy_count = 0 if cls._is_ground_cluster(index) or not cls.get_param("USE_SEQUOIA", index) else cls.get_param("NUM_CYPRESS_PROXIES", index)
        clock_count = 0
        if cls.get_param("USE_SEQUOIA", index):
            if cls._is_ground_cluster(index):
                clock_count = cls.get_param("NUM_CLOCKS", index - cls.get_ground_index_offset())
        elif index == 0 or not cls.get_param("USE_PRIMARY_CLOCKS", index):
            clock_count = cls.get_param("NUM_CLOCKS", index)

        has_ground = cls.get_param("USE_SEQUOIA", index) and not cls._is_ground_cluster(index)
        primary_cell_tag = (index + 1) * 10
        clock_cluster_tag = (index + cls.get_ground_index_offset() + 1) * 10 if has_ground else primary_cell_tag

        if cls.USE_SLOT_USER_ID is None:
            use_slot_user_id = cls.USE_PORTO
        else:
            use_slot_user_id = cls.USE_SLOT_USER_ID

        yt_config = LocalYtConfig(
            use_porto_for_servers=cls.USE_PORTO,
            jobs_environment_type="porto" if cls.USE_PORTO else cls.JOB_ENVIRONMENT_TYPE,
            use_slot_user_id=use_slot_user_id,
            use_native_client=True,
            master_count=cls.get_param("NUM_MASTERS", index),
            nonvoting_master_count=cls.get_param("NUM_NONVOTING_MASTERS", index),
            secondary_cell_count=secondary_cell_count,
            defer_secondary_cell_start=cls.get_param("DEFER_SECONDARY_CELL_START", index),
            clock_count=clock_count,
            timestamp_provider_count=cls.get_param("NUM_TIMESTAMP_PROVIDERS", index),
            cell_balancer_count=cls.get_param("NUM_CELL_BALANCERS", index),
            enable_bundle_controller=cls.get_param("ENABLE_BUNDLE_CONTROLLER", index),
            discovery_server_count=cls.get_param("NUM_DISCOVERY_SERVERS", index),
            queue_agent_count=cls.get_param("NUM_QUEUE_AGENTS", index),
            kafka_proxy_count=cls.get_param("NUM_KAFKA_PROXIES", index),
            node_count=cls.get_param("NUM_NODES", index),
            defer_node_start=cls.get_param("DEFER_NODE_START", index),
            chaos_node_count=cls.get_param("NUM_CHAOS_NODES", index),
            defer_chaos_node_start=cls.get_param("DEFER_CHAOS_NODE_START", index),
            master_cache_count=cls.get_param("NUM_MASTER_CACHES", index),
            scheduler_count=cls.get_param("NUM_SCHEDULERS", index),
            defer_scheduler_start=cls.get_param("DEFER_SCHEDULER_START", index),
            job_proxy_logging=cls.get_param("JOB_PROXY_LOGGING", index),
            controller_agent_count=(
                cls.get_param("NUM_CONTROLLER_AGENTS", index)
                if cls.get_param("NUM_CONTROLLER_AGENTS", index) is not None
                else cls.get_param("NUM_SCHEDULERS", index)),
            tablet_balancer_count=cls.get_param("NUM_TABLET_BALANCERS", index),
            defer_controller_agent_start=cls.get_param("DEFER_CONTROLLER_AGENT_START", index),
            http_proxy_count=(
                cls.get_param("NUM_HTTP_PROXIES", index) if cls.get_param("ENABLE_HTTP_PROXY", index) else 0),
            rpc_proxy_count=(
                cls.get_param("NUM_RPC_PROXIES", index) if cls.get_param("ENABLE_RPC_PROXY", index) else 0),
            cypress_proxy_count=cypress_proxy_count,
            replicated_table_tracker_count=cls.get_param("NUM_REPLICATED_TABLE_TRACKERS", index),
            fqdn="localhost",
            enable_master_cache=cls.get_param("USE_MASTER_CACHE", index),
            enable_permission_cache=cls.get_param("USE_PERMISSION_CACHE", index),
            primary_cell_tag=primary_cell_tag,
            has_ground=has_ground,
            clock_cluster_tag=clock_cluster_tag,
            enable_structured_logging=True,
            enable_log_compression=True,
            log_compression_method="zstd",
            node_port_set_size=cls.get_param("NODE_PORT_SET_SIZE", index),
            store_location_count=cls.get_param("STORE_LOCATION_COUNT", index),
            node_io_engine_type=cls.get_param("NODE_IO_ENGINE_TYPE", index),
            node_use_direct_io_for_reads=cls.get_param("NODE_USE_DIRECT_IO_FOR_READS", index),
            cluster_name=cls.get_cluster_name(index),
            enable_resource_tracking=cls.get_param("ENABLE_RESOURCE_TRACKING", index),
            enable_tvm_only_proxies=cls.get_param("ENABLE_TVM_ONLY_PROXIES", index),
            mock_tvm_id=(1000 + index if use_native_auth else None),
            enable_tls=cls.ENABLE_TLS,
            wait_for_dynamic_config=cls.WAIT_FOR_DYNAMIC_CONFIG,
            enable_chyt_http_proxies=cls.get_param("ENABLE_CHYT_HTTP_PROXIES", index),
            enable_chyt_https_proxies=cls.get_param("ENABLE_CHYT_HTTPS_PROXIES", index),
            delta_global_cluster_connection_config={
                "permission_cache": {
                    "expire_after_successful_update_time": 60000,
                    "refresh_time": 60000,
                    "expire_after_failed_update_time": 1000,
                    "expire_after_access_time": 300000,
                },
            } if cls._is_ground_cluster(index) else None
        )

        if yt_config.jobs_environment_type == "porto" and not porto_available():
            pytest.skip("Porto is not available")

        if yt_config.jobs_environment_type == "cri" and yt_config.cri_endpoint is None:
            yt_config.cri_endpoint = find_cri_endpoint()
            if yt_config.cri_endpoint is None:
                pytest.skip("CRI endpoint not found")

        instance = YTInstance(
            path,
            yt_config,
            watcher_config={"disable_logrotate": True},
            kill_child_processes=True,
            modify_configs_func=modify_configs_func,
            modify_dynamic_configs_func=modify_dynamic_configs_func,
            stderrs_path=os.path.join(
                OUTPUT_PATH,
                "yt_stderrs",
                cls.run_name,
                str(index)),
            external_bin_path=cls.bin_path,
            modify_driver_logging_config_func=modify_driver_logging_config_func,
        )

        instance._cluster_name = cls.get_cluster_name(index)
        setattr(instance, "_default_driver_backend", cls.get_param("DRIVER_BACKEND", index))

        return instance

    @classmethod
    def get_cluster_name(cls, cluster_index):
        if cluster_index == 0:
            return "primary"
        if cluster_index <= cls.NUM_REMOTE_CLUSTERS:
            return "remote_" + str(cluster_index - 1)
        if cluster_index == cls.get_ground_index_offset():
            return "primary_ground"
        return "remote_{}_ground".format(cluster_index - cls.get_ground_index_offset() - 1)

    # NB: Does not return ground clusters.
    @classmethod
    def get_cluster_names(cls):
        return [cls.get_cluster_name(cluster_index) for cluster_index in range(cls.NUM_REMOTE_CLUSTERS + 1)]

    @classmethod
    def setup_class(cls, test_name=None, run_id=None):
        logging.basicConfig(level=logging.INFO)

        cls._start_time = time()

        need_suid = False
        cls.cleanup_root_files = False
        if cls.USE_PORTO:
            need_suid = True
            cls.cleanup_root_files = True

        # Initialize `cls` fields before actual setup to make teardown correct.

        cls.ground_envs = []
        # TODO(ignat): Rename Env to env
        cls.Env = None
        cls.remote_envs = []

        if test_name is None:
            test_name = cls.__name__
        cls.test_name = test_name

        cls.liveness_checkers = []

        log_rotator = Checker(reopen_logs)
        log_rotator.daemon = True
        log_rotator.start()
        cls.liveness_checkers.append(log_rotator)

        # The following line initializes SANDBOX_ROOTDIR.
        cls.bin_path, cls.extra_bin_path = prepare_yatest_environment(
            need_suid=need_suid,
            artifact_components=cls.ARTIFACT_COMPONENTS,
            force_create_environment=cls.FORCE_CREATE_ENVIRONMENT,
            extra_artifact_components=cls.EXTRA_ARTIFACT_COMPONENTS,
        )
        cls.path_to_test = os.path.join(SANDBOX_ROOTDIR, test_name)

        cls.run_id = None
        cls.path_to_run = cls.path_to_test

        cls.run_name = os.path.basename(cls.path_to_run)

        if os.environ.get("YT_OUTPUT") is None:
            disk_path = SANDBOX_ROOTDIR
        else:
            disk_path = os.environ.get("HDD_PATH")

        cls.default_disk_path = os.path.join(disk_path, cls.run_name, "disk_default")
        cls.ssd_disk_path = os.path.join(disk_path, cls.run_name, "disk_ssd")

        cls.fake_default_disk_path = os.path.join(
            OUTPUT_PATH,
            cls.run_name,
            "disk_default")
        cls.fake_ssd_disk_path = os.path.join(
            OUTPUT_PATH,
            cls.run_name,
            "disk_ssd")

        cls.primary_cluster_path = cls.path_to_run
        if cls.NUM_REMOTE_CLUSTERS > 0 or cls.USE_SEQUOIA:
            cls.primary_cluster_path = os.path.join(cls.path_to_run, "primary")

        if cls.USE_SEQUOIA:
            cls.USE_PRIMARY_CLOCKS = False

        if cls.USE_SEQUOIA != cls.VALIDATE_SEQUOIA_TREE_CONSISTENCY:
            cls.VALIDATE_SEQUOIA_TREE_CONSISTENCY = False

        try:
            cls.start_envs()
        except:  # noqa
            cls.teardown_class()
            raise

    @classmethod
    def _setup_cluster_configuration(cls, index, clusters, driver=None):
        cluster_name = cls.get_cluster_name(index)

        driver = driver or yt_commands.get_driver(cluster=cluster_name)
        if driver is None:
            return

        requests = [
            yt_commands.make_batch_request("set", path="//sys/@cluster_name", input=cluster_name),
            yt_commands.make_batch_request("set", path="//sys/clusters", input=clusters),
            yt_commands.make_batch_request("set", path="//sys/@cluster_connection",
                                           input=clusters[cluster_name])
        ]
        responses = yt_commands.execute_batch(requests, driver=driver)
        for response in responses:
            yt_commands.raise_batch_error(response)

    @classmethod
    def _on_sequoia_started(cls, cluster_index, driver=None):
        clusters = {}
        for instance in cls.combined_envs:
            clusters[instance._cluster_name] = instance.get_cluster_configuration()["cluster_connection"]

        cls._setup_cluster_configuration(cluster_index, clusters, driver=driver)
        if cls.get_param("USE_SEQUOIA", cluster_index):
            cls._setup_cluster_configuration(cluster_index + cls.get_ground_index_offset(), clusters)
            cls._setup_sequoia_tables(cluster_index)

    @classmethod
    def start_envs(cls):
        # Ground clusters instantiation.
        for original_cluster_index in range(cls.NUM_REMOTE_CLUSTERS + 1):
            if cls.get_param("USE_SEQUOIA", original_cluster_index):
                cluster_index = original_cluster_index + cls.get_ground_index_offset()
                cluster_path = os.path.join(cls.path_to_run, cls.get_cluster_name(cluster_index))
                cls.ground_envs.append(cls.create_yt_cluster_instance(cluster_index, cluster_path))

        # Primary cluster instantiation.
        cls.Env = cls.create_yt_cluster_instance(0, cls.primary_cluster_path)

        # Remote clusters instantiation.
        for cluster_index in range(1, cls.NUM_REMOTE_CLUSTERS + 1):
            cluster_path = os.path.join(cls.path_to_run, cls.get_cluster_name(cluster_index))
            cls.remote_envs.append(cls.create_yt_cluster_instance(cluster_index, cluster_path))

        # All at once so one can copy alien entries between them
        cluster_envs = [cls.Env] + cls.remote_envs
        timestamp_provider_configs = [
            cluster_env.get_cluster_configuration()["timestamp_provider"] for cluster_env in cluster_envs
        ]
        clock_configs = [
            cluster_env.get_cluster_configuration()["clock"] for cluster_env in cluster_envs
        ]
        yt_configs = [cluster_env.yt_config for cluster_env in cluster_envs]
        if cls.modify_timestamp_providers_configs(timestamp_provider_configs, clock_configs, yt_configs):
            for cluster_env in cluster_envs:
                cluster_env.rewrite_timestamp_provider_configs()

        latest_run_path = os.path.join(cls.path_to_test, "run_latest")
        if os.path.exists(latest_run_path):
            os.remove(latest_run_path)
        os.symlink(cls.path_to_run, latest_run_path)

        yt_commands.is_multicell = cls.is_multicell()
        yt_commands.path_to_run_tests = cls.path_to_run

        cls.combined_envs = cls.ground_envs + cluster_envs
        yt_commands.init_drivers(cls.combined_envs)

        for env in cls.ground_envs:
            env.start()

        cls.Env.start(
            on_sequoia_started_func=lambda driver: cls._on_sequoia_started(cluster_index=0, driver=driver),
            on_masters_started_func=cls.on_masters_started)

        for index, env in enumerate(cls.remote_envs):
            env.start(on_sequoia_started_func=lambda driver: cls._on_sequoia_started(cluster_index=index + 1, driver=driver))

        yt_commands.wait_drivers()

        for env in cls.combined_envs:
            liveness_checker = Checker(lambda: env.check_liveness(callback_func=emergency_exit_within_tests))
            liveness_checker.daemon = True
            liveness_checker.start()
            cls.liveness_checkers.append(liveness_checker)

        # TODO(babenko): wait for cluster sync
        if cls.remote_envs:
            sleep(1.0)

        if yt_commands.is_multicell and not cls.DEFER_SECONDARY_CELL_START:
            yt_commands.remove("//sys/operations")
            yt_commands.create("portal_entrance", "//sys/operations", attributes={"exit_cell_tag": 11})

        if cls.USE_DYNAMIC_TABLES:
            for cluster_index in range(cls.NUM_REMOTE_CLUSTERS + 1):
                driver = yt_commands.get_driver(cluster=cls.get_cluster_name(cluster_index))
                if driver is None:
                    continue
                # Raise dynamic tables limits since they are zero by default.
                yt_commands.set(
                    "//sys/accounts/tmp/@resource_limits/tablet_count",
                    10000,
                    driver=driver,
                )
                yt_commands.set(
                    "//sys/accounts/tmp/@resource_limits/tablet_static_memory",
                    1024 * 1024 * 1024,
                    driver=driver,
                )

                if cls.ENABLE_STANDALONE_TABLET_BALANCER:
                    if not yt_commands.exists("//sys/tablet_balancer/config", driver=driver):
                        yt_commands.create(
                            "document",
                            "//sys/tablet_balancer/config",
                            attributes={"value": {}},
                            force=True,
                            driver=driver)

        if cls.USE_CUSTOM_ROOTFS:
            yt_commands.create("map_node", "//layers")

            yt_commands.create("file", "//layers/exec.tar.gz", attributes={"replication_factor": 1})
            yt_commands.write_file("//layers/exec.tar.gz", open("rootfs/exec.tar.gz", "rb").read())
            yt_commands.create("file", "//layers/rootfs.tar.gz", attributes={"replication_factor": 1})
            yt_commands.write_file("//layers/rootfs.tar.gz", open("rootfs/rootfs.tar.gz", "rb").read())

    @classmethod
    def _setup_sequoia_tables(cls, cluster_index):
        ground_driver = yt_sequoia_helpers.get_ground_driver(cluster=cls.get_cluster_name(cluster_index))
        if ground_driver is None:
            return

        cls._restore_sequoia_bundle_options(cluster_index + cls.get_ground_index_offset())
        # TODO(h0pless): Use values from config for path, account and bundle names.
        yt_commands.sync_create_cells(1, tablet_cell_bundle="sequoia", driver=ground_driver)
        yt_commands.set("//sys/accounts/sequoia/@resource_limits/tablet_count", 10000, driver=ground_driver)
        yt_commands.set("//sys/accounts/sequoia/@resource_limits/tablet_static_memory", 4 * (2**30), driver=ground_driver)

        for descriptor in DESCRIPTORS.as_dict().values():
            table_path = descriptor.get_default_path()
            yt_commands.create(
                "table",
                table_path,
                attributes={
                    "dynamic": True,
                    "schema": descriptor.schema,
                    "tablet_cell_bundle": "sequoia",
                    "account": "sequoia",
                    "enable_shared_write_locks": True,
                    "in_memory_mode": "uncompressed",
                },
                driver=ground_driver)

        unapproved_chunk_replicas_path = DESCRIPTORS.unapproved_chunk_replicas.get_default_path()
        yt_commands.set(f"{unapproved_chunk_replicas_path}/@mount_config/min_data_versions", 0, driver=ground_driver)
        yt_commands.set(f"{unapproved_chunk_replicas_path}/@mount_config/max_data_versions", 1, driver=ground_driver)
        yt_commands.set(f"{unapproved_chunk_replicas_path}/@mount_config/min_data_ttl", 0, driver=ground_driver)
        yt_commands.set(f"{unapproved_chunk_replicas_path}/@mount_config/max_data_ttl", 5000, driver=ground_driver)

        response_keeper_path = DESCRIPTORS.unapproved_chunk_replicas.get_default_path()
        yt_commands.set(f"{response_keeper_path}/@mount_config/min_data_versions", 0, driver=ground_driver)
        yt_commands.set(f"{response_keeper_path}/@mount_config/max_data_versions", 1, driver=ground_driver)
        yt_commands.set(f"{response_keeper_path}/@mount_config/min_data_ttl", 0, driver=ground_driver)
        yt_commands.set(f"{response_keeper_path}/@mount_config/max_data_ttl", 1000, driver=ground_driver)

        for descriptor in DESCRIPTORS.as_dict().values():
            table_path = descriptor.get_default_path()
            yt_commands.sync_mount_table(table_path, driver=ground_driver)

    @classmethod
    def apply_node_dynamic_config_patches(cls, config, ytserver_version, cluster_index):
        delta_node_config = cls.get_param("DELTA_DYNAMIC_NODE_CONFIG", cluster_index)

        update_inplace(config, delta_node_config)

        # COMPAT(pogorelov)
        if "node" in cls.ARTIFACT_COMPONENTS.get("23_2", []):
            config["%true"]["exec_node"]["controller_agent_connector"]["use_job_tracker_service_to_settle_jobs"] = True

        # COMPAT(arkady-e1ppa)
        if any(component in cls.ARTIFACT_COMPONENTS.get("23_2", []) for component in ["scheduler", "controller_agent"]):
            config["%true"]["exec_node"]["job_controller"]["disable_legacy_allocation_preparation"] = False

        if cls._is_ground_cluster(cluster_index):
            config["%true"]["tablet_node"].setdefault("security_manager", {})
            config["%true"]["tablet_node"]["security_manager"].setdefault("resource_limits_cache", {})
            config["%true"]["tablet_node"]["security_manager"]["resource_limits_cache"] = {
                "expire_after_successful_update_time": 45000,
                "refresh_time": 45000,
                "expire_after_failed_update_time": 1000,
                "expire_after_access_time": 120000,
            }

        return config

    @classmethod
    def apply_config_patches(cls, configs, ytserver_version, cluster_index, cluster_path):
        for tag in [configs["master"]["primary_cell_tag"]] + configs["master"]["secondary_cell_tags"]:
            for peer_index, config in enumerate(configs["master"][tag]):
                config = update_inplace(config, cls.get_param("DELTA_MASTER_CONFIG", cluster_index))
                configs["master"][tag][peer_index] = cls.update_timestamp_provider_config(cluster_index, config)
                configs["master"][tag][peer_index] = cls.update_sequoia_connection_config(cluster_index, config)
                configs["master"][tag][peer_index] = cls.update_transaction_supervisor_config(cluster_index, config)
                cls.modify_master_config(configs["master"][tag][peer_index], tag, peer_index, cluster_index)

        for index, config in enumerate(configs["scheduler"]):
            config = update_inplace(config, cls.get_param("DELTA_SCHEDULER_CONFIG", cluster_index))

            # COMPAT(pogorelov)
            if "scheduler" in cls.ARTIFACT_COMPONENTS.get("23_1", []):
                config["scheduler"]["control_unknown_operation_jobs_lifetime"] = False
            if "node" in cls.ARTIFACT_COMPONENTS.get("23_1", []):
                config["scheduler"].pop("always_send_controller_agent_descriptors")
                config["scheduler"].pop("send_full_controller_agent_descriptors_for_jobs")

            configs["scheduler"][index] = cls.update_timestamp_provider_config(cluster_index, config)
            cls.modify_scheduler_config(configs["scheduler"][index], cluster_index)
        for config in configs["clock"][configs["clock"]["cell_tag"]]:
            cls.modify_clock_config(config, cluster_index, configs["master"]["primary_cell_tag"])
        for index, config in enumerate(configs["queue_agent"]):
            config = update_inplace(config, cls.get_param("DELTA_QUEUE_AGENT_CONFIG", cluster_index))
            configs["queue_agent"][index] = cls.update_timestamp_provider_config(cluster_index, config)
            cls.modify_queue_agent_config(configs["queue_agent"][index])
        for index, config in enumerate(configs["kafka_proxy"]):
            config = update_inplace(config, cls.get_param("DELTA_KAFKA_PROXY_CONFIG", cluster_index))
            configs["kafka_proxy"][index] = cls.update_timestamp_provider_config(cluster_index, config)
            cls.modify_kafka_proxy_config(configs["kafka_proxy"][index])
        for index, config in enumerate(configs["cell_balancer"]):
            config = update_inplace(config, cls.get_param("DELTA_CELL_BALANCER_CONFIG", cluster_index))
            configs["cell_balancer"][index] = cls.update_timestamp_provider_config(cluster_index, config)
            cls.modify_cell_balancer_config(configs["cell_balancer"][index])
        for index, config in enumerate(configs["tablet_balancer"]):
            config = update_inplace(config, cls.get_param("DELTA_TABLET_BALANCER_CONFIG", cluster_index))
            configs["tablet_balancer"][index] = cls.update_timestamp_provider_config(cluster_index, config)
            cls.modify_tablet_balancer_config(configs["tablet_balancer"][index])
        for index, config in enumerate(configs["master_cache"]):
            config = update_inplace(config, cls.get_param("DELTA_MASTER_CACHE_CONFIG", cluster_index))
            configs["master_cache"][index] = cls.update_timestamp_provider_config(cluster_index, config)
            cls.modify_master_cache_config(configs["master_cache"][index])
        for index, config in enumerate(configs["controller_agent"]):
            delta_config = cls.get_param("DELTA_CONTROLLER_AGENT_CONFIG", cluster_index)
            config = update_inplace(
                update_inplace(config, YTEnvSetup._DEFAULT_DELTA_CONTROLLER_AGENT_CONFIG),
                delta_config,
            )

            # COMPAT(pogorelov)
            if "controller-agent" in cls.ARTIFACT_COMPONENTS.get("23_1", []):
                config["controller_agent"]["control_job_lifetime_at_scheduler"] = False
                config["controller_agent"]["job_tracker"]["abort_vanished_jobs"] = True

            artifact_components_23_2 = cls.ARTIFACT_COMPONENTS.get("23_2", [])

            # COMPAT(kvk1920)
            if "master" in artifact_components_23_2:
                config["controller_agent"]["commit_operation_cypress_node_changes_via_system_transaction"] = False
                config["controller_agent"]["set_committed_attribute_via_transaction_action"] = True

            # COMPAT(arkady-e1ppa)
            if "nodes" not in artifact_components_23_2:
                config["controller_agent"]["job_tracker"]["enable_graceful_abort"] = True

            if "node" in artifact_components_23_2:
                print_debug("Turning off job_id_unequal_to_allocation_id flag")
                config["controller_agent"]["job_id_unequal_to_allocation_id"] = False

            configs["controller_agent"][index] = cls.update_timestamp_provider_config(cluster_index, config)
            cls.modify_controller_agent_config(configs["controller_agent"][index], cluster_index)

        node_flavors_length = len(cls.DELTA_NODE_FLAVORS)
        assert node_flavors_length == 0 or cls.NUM_NODES == node_flavors_length

        for index, config in enumerate(configs["node"]):
            config = update_inplace(config, cls.get_param("DELTA_NODE_CONFIG", cluster_index))
            if cls.USE_CUSTOM_ROOTFS:
                config = update_inplace(config, get_custom_rootfs_delta_node_config())

            config["ref_counted_tracker_dump_period"] = 5000

            # TODO(khlebnikov) move "breakpoints" out of "tmp" which shouldn't be shared.
            shared_dir = os.path.join(cluster_path, "tmp")
            if not os.path.exists(shared_dir):
                os.mkdir(shared_dir)
            config["exec_node"].setdefault("root_fs_binds", []).append({
                "internal_path": shared_dir,
                "external_path": shared_dir,
                "read_only": False,
            })

            config = cls.update_timestamp_provider_config(cluster_index, config)

            if node_flavors_length != 0:
                config["flavors"] = cls.DELTA_NODE_FLAVORS[index]

            configs["node"][index] = config
            cls.modify_node_config(configs["node"][index], cluster_index)

        for index, config in enumerate(configs["chaos_node"]):
            config = update_inplace(config, cls.get_param("DELTA_CHAOS_NODE_CONFIG", cluster_index))
            configs["chaos_node"][index] = cls.update_timestamp_provider_config(cluster_index, config)
            cls.modify_chaos_node_config(configs["chaos_node"][index], cluster_index)

        for index, config in enumerate(configs["http_proxy"]):
            # COMPAT(pogorelov)
            config["cluster_connection"]["scheduler"]["use_scheduler_job_prober_service"] = False

            delta_config = cls.get_param("DELTA_PROXY_CONFIG", cluster_index)

            config = update_inplace(config, delta_config)
            configs["http_proxy"][index] = cls.update_timestamp_provider_config(cluster_index, config)
            cls.modify_proxy_config(configs["http_proxy"])

        for index, config in enumerate(configs["rpc_proxy"]):
            # COMPAT(pogorelov)
            config["cluster_connection"]["scheduler"]["use_scheduler_job_prober_service"] = False

            config = update_inplace(config, cls.get_param("DELTA_RPC_PROXY_CONFIG", cluster_index))

            configs["rpc_proxy"][index] = cls.update_timestamp_provider_config(cluster_index, config)
            cls.modify_rpc_proxy_config(configs["rpc_proxy"])

        for index, config in enumerate(configs["cypress_proxy"]):
            config = update_inplace(config, cls.get_param("DELTA_CYPRESS_PROXY_CONFIG", cluster_index))
            configs["cypress_proxy"][index] = cls.update_timestamp_provider_config(cluster_index, config)
            configs["cypress_proxy"][index] = cls.update_sequoia_connection_config(cluster_index, config)
            cls.modify_cypress_proxy_config(configs["cypress_proxy"][index])

        for key, config in configs["driver"].items():
            config = update_inplace(config, cls.get_param("DELTA_DRIVER_CONFIG", cluster_index))

            configs["driver"][key] = cls.update_timestamp_provider_config(cluster_index, config)
            cls.modify_driver_config(configs["driver"][key])

        configs["rpc_driver"] = update_inplace(
            configs["rpc_driver"],
            cls.get_param("DELTA_RPC_DRIVER_CONFIG", cluster_index),
        )

    @classmethod
    def update_transaction_supervisor_config(cls, cluster_index, config):
        if not cls.get_param("USE_SEQUOIA", cluster_index) or cls._is_ground_cluster(cluster_index):
            return config
        config.setdefault("transaction_supervisor", {})
        config["transaction_supervisor"]["enable_wait_until_prepared_transactions_finished"] = True
        return config

    @classmethod
    def update_sequoia_connection_config(cls, cluster_index, config):
        if not cls.get_param("USE_SEQUOIA", cluster_index) or cls._is_ground_cluster(cluster_index):
            return config

        ground_cluster_name = cls.get_cluster_name(cluster_index + cls.get_ground_index_offset())
        config["cluster_connection"].setdefault("sequoia_connection", {})
        config["cluster_connection"]["sequoia_connection"]["ground_cluster_name"] = ground_cluster_name
        return config

    @classmethod
    def update_timestamp_provider_config(cls, cluster_index, config):
        if cls.get_param("USE_SEQUOIA", cluster_index):
            if cls._is_ground_cluster(cluster_index):
                return config

            ground_list_index = 0
            for index in range(cluster_index):
                if cls.get_param("USE_SEQUOIA", index):
                    ground_list_index += 1

            ground_timestamp_provider = cls.ground_envs[ground_list_index].configs["master"][0]["cluster_connection"]["timestamp_provider"]
            if "timestamp_provider" in config:
                config["timestamp_provider"] = ground_timestamp_provider
            if "cluster_connection" in config:
                config["cluster_connection"]["timestamp_provider"] = ground_timestamp_provider
            return config

        if cls.get_param("NUM_CLOCKS", cluster_index) == 0 or cluster_index == 0 or not cls.get_param("USE_PRIMARY_CLOCKS", cluster_index):
            return config
        primary_timestamp_provider = cls.Env.configs["chaos_node"][0]["cluster_connection"]["timestamp_provider"]
        if "timestamp_provider" in config:
            config["timestamp_provider"] = primary_timestamp_provider
        if "cluster_connection" in config:
            config["cluster_connection"]["timestamp_provider"] = primary_timestamp_provider
        return config

    @classmethod
    def teardown_class(cls):
        if cls.liveness_checkers:
            map(lambda c: c.stop(), cls.liveness_checkers)

        for env in cls.ground_envs + [cls.Env] + cls.remote_envs:
            if env is None:
                continue
            env.stop()
            env.remove_runtime_data()

        yt_commands.terminate_drivers()
        gc.collect()

        class_duration = time() - cls._start_time
        class_limit = (2 if is_asan_build() else 1) * cls.CLASS_TEST_LIMIT

        if class_duration > class_limit:
            pytest.fail(
                "Execution of class {} took more than {} seconds ({} seconds).\n"
                "Check test stdout for detailed duration report.\n"
                "You can split class into smaller partitions, using NUM_TEST_PARTITIONS option (current value is {})."
                .format(cls.__name__, class_limit, class_duration, cls.NUM_TEST_PARTITIONS)
            )

    def setup_method(self, method):
        for cluster_index in range(self.NUM_REMOTE_CLUSTERS + 1):
            self.setup_cluster(method, cluster_index)

            if self.get_param("USE_SEQUOIA", cluster_index):
                self.setup_cluster(method, cluster_index + self.get_ground_index_offset())

        for env in self.combined_envs:
            env.restore_default_node_dynamic_config()
            env.restore_default_bundle_dynamic_config()

    def setup_cluster(self, method, cluster_index):
        driver = yt_commands.get_driver(cluster=self.get_cluster_name(cluster_index))
        if driver is None:
            return

        node_count = self.get_param("NUM_NODES", cluster_index) + self.get_param("NUM_CHAOS_NODES", cluster_index)
        if node_count > 0:
            wait(lambda: yt_commands.get("//sys/cluster_nodes/@count", driver=driver) == node_count)

        scheduler_count = self.get_param("NUM_SCHEDULERS", cluster_index)
        if scheduler_count > 0:
            scheduler_pool_trees_root = self.Env.configs["scheduler"][0]["scheduler"].get(
                "pool_trees_root", "//sys/pool_trees"
            )
        else:
            scheduler_pool_trees_root = "//sys/pool_trees"
        yt_commands.create("map_node", "//sys/cypress_proxies", ignore_existing=True, driver=driver)
        self._restore_globals(
            cluster_index=cluster_index,
            scheduler_count=scheduler_count,
            scheduler_pool_trees_root=scheduler_pool_trees_root,
            driver=driver,
        )

        yt_commands.gc_collect(driver=driver)

        yt_commands.clear_metadata_caches(driver=driver)

        if self.USE_DYNAMIC_TABLES:
            self._setup_tablet_manager(driver=driver)
            self._clear_ql_pools(driver=driver)
            self._restore_default_bundle_options(cluster_index)
            self._setup_tablet_balancer_dynamic_config(driver=driver)
            self._setup_standalone_replicated_table_tracker_dynamic_config(driver=driver)

        if self._is_ground_cluster(cluster_index):
            yt_commands.ls("//sys/cluster_nodes", attributes=["user_tags"])
            yt_commands.set(
                "//sys/cluster_nodes/@config/%true/tablet_node",
                {
                    "security_manager": {
                        "resource_limits_cache": {
                            "expire_after_successful_update_time": 120000,
                            "refresh_time": 120000,
                            "expire_after_failed_update_time": 2000,
                            "expire_after_access_time": 180000,
                        },
                    },
                },
                driver=driver)

        if not self.get_param("DEFER_SECONDARY_CELL_START", cluster_index):
            yt_commands.wait_for_nodes(driver=driver)
            yt_commands.wait_for_chunk_replicator_enabled(driver=driver)

        if self.get_param("NUM_SCHEDULERS", cluster_index) > 0:
            for response in yt_commands.execute_batch(
                [
                    yt_commands.make_batch_request(
                        "create",
                        path="//sys/controller_agents/config",
                        type="document",
                        attributes={
                            "value": {
                                "enable_bulk_insert_for_everyone": self.ENABLE_BULK_INSERT,
                                "enable_versioned_remote_copy": self.ENABLE_BULK_INSERT or self.ENABLE_HUNKS_REMOTE_COPY,
                                "enable_hunks_remote_copy": self.ENABLE_HUNKS_REMOTE_COPY,
                                "testing_options": {
                                    "rootfs_test_layers": [
                                        "//layers/exec.tar.gz",
                                        "//layers/rootfs.tar.gz",
                                    ]
                                    if self.USE_CUSTOM_ROOTFS
                                    else [],
                                },
                            }
                        },
                        force=True,
                    ),
                    yt_commands.make_batch_request(
                        "create",
                        path="//sys/scheduler/config",
                        type="document",
                        attributes={"value": {}},
                        force=True,
                    ),
                ],
                driver=driver,
            ):
                assert not yt_commands.get_batch_error(response)

            def _get_config_versions(orchids):
                requests = [
                    yt_commands.make_batch_request(
                        "get",
                        path=orchid + "/config_revision",
                        return_only_value=True,
                    )
                    for orchid in orchids
                ]
                responses = yt_commands.execute_batch(requests, driver=driver)
                return list(map(lambda r: yt_commands.get_batch_output(r), responses))

            def _wait_for_configs(orchids):
                old_versions = _get_config_versions(orchids)

                def _wait_func():
                    new_versions = _get_config_versions(orchids)
                    return all(new >= old + 2 for old, new in zip(old_versions, new_versions))

                wait(_wait_func)

            orchids = []
            for instance in yt_commands.ls("//sys/controller_agents/instances", driver=driver):
                orchids.append("//sys/controller_agents/instances/{}/orchid/controller_agent".format(instance))
            orchids.append("//sys/scheduler/orchid/scheduler")
            _wait_for_configs(orchids)

        if self.get_param("ENABLE_TMP_ROOTSTOCK", cluster_index) and not self._is_ground_cluster(cluster_index):
            assert self.get_param("USE_SEQUOIA", cluster_index)
            # NB: Sometimes roles will not be applied to cells yet, which can lead to an error. Just retrying helps.
            wait(lambda: yt_commands.create(
                "rootstock",
                "//tmp",
                force=True,
                driver=driver),
                ignore_exceptions=True)
        elif self.ENABLE_TMP_PORTAL and cluster_index == 0:
            yt_commands.create(
                "portal_entrance",
                "//tmp",
                attributes={
                    "account": "tmp",
                    "exit_cell_tag": 11,
                    "acl": [
                        {
                            "action": "allow",
                            "permissions": ["read", "write", "remove"],
                            "subjects": ["users"],
                        },
                        {
                            "action": "allow",
                            "permissions": ["read"],
                            "subjects": ["everyone"],
                        },
                    ],
                },
                force=True,
                driver=driver,
            )

            yt_commands.create(
                "map_node",
                "//portals",
                attributes={
                    "account": "tmp",
                    "acl": [
                        {
                            "action": "allow",
                            "permissions": ["read", "write", "remove"],
                            "subjects": ["users"],
                        }
                    ],
                    "opaque": True,
                },
                force=True,
                driver=driver,
            )
        else:
            yt_commands.create(
                "map_node",
                "//tmp",
                attributes={
                    "account": "tmp",
                    "acl": [
                        {
                            "action": "allow",
                            "permissions": ["read", "write", "remove"],
                            "subjects": ["users"],
                        }
                    ],
                    "opaque": True,
                },
                force=True,
                driver=driver,
            )

    def teardown_method(self, method, wait_for_nodes=True):
        yt_commands._zombie_responses[:] = []

        for cluster_index, env in enumerate(self.ground_envs + [self.Env] + self.remote_envs):
            env.check_liveness(callback_func=emergency_exit_within_tests)

            # COMPAT(danilalexeev)
            if env.get_component_version("ytserver-master").abi >= (23, 2):
                driver = yt_commands.get_driver(cluster=self.get_cluster_name(cluster_index))
                self._master_exit_read_only_sync(env, driver=driver)

        for cluster_index, env in enumerate([self.Env] + self.remote_envs):
            self.teardown_cluster(method, cluster_index, wait_for_nodes)

            if self.get_param("USE_SEQUOIA", cluster_index):
                self.teardown_cluster(method, cluster_index + self.get_ground_index_offset())

        yt_commands.reset_events_on_fs()

    def teardown_cluster(self, method, cluster_index, wait_for_nodes=True):
        cluster_name = self.get_cluster_name(cluster_index)
        driver = yt_commands.get_driver(cluster=cluster_name)
        if driver is None:
            return

        if self.VALIDATE_SEQUOIA_TREE_CONSISTENCY and not self._is_ground_cluster(cluster_index):
            yt_sequoia_helpers.validate_sequoia_tree_consistency(cluster_name)

        self._reset_nodes(wait_for_nodes, driver=driver)

        if self.get_param("NUM_SCHEDULERS", cluster_index) > 0:
            self._remove_operations(driver=driver)
            self._wait_for_jobs_to_vanish(driver=driver)

        self._abort_transactions(driver=driver)

        if self.get_param("USE_SEQUOIA", cluster_index):
            scions = yt_commands.ls("//sys/scions", driver=driver)
            for scion in scions:
                yt_commands.remove(f"#{scion}", driver=driver)
            yt_commands.gc_collect(driver=driver)

            if self._is_ground_cluster(cluster_index):
                # Links for system stuff will still be present in sequoia tables during teardown.
                wait(lambda: yt_commands.select_rows(f"* from [{DESCRIPTORS.path_to_node_id.get_default_path()}] where not is_substr('//sys', path)", driver=driver) == [])
                wait(lambda: yt_commands.select_rows(f"* from [{DESCRIPTORS.node_id_to_path.get_default_path()}] where not is_substr('//sys', path)", driver=driver) == [])
                wait(lambda: yt_commands.select_rows(f"* from [{DESCRIPTORS.child_node.get_default_path()}]", driver=driver) == [])

                for table in DESCRIPTORS.get_group("transactions"):
                    wait(lambda: yt_commands.select_rows(f"* from [{table.get_default_path()}]", driver=driver) == [])

        # Ground cluster can't have rootstocks or portals.
        # Do not remove tmp if ENABLE_TMP_ROOTSTOCK, since it will be removed with scions.
        if not self.get_param("ENABLE_TMP_ROOTSTOCK", cluster_index) and not self._is_ground_cluster(cluster_index):
            yt_commands.remove("//tmp", driver=driver)
            if self.ENABLE_TMP_PORTAL:
                yt_commands.remove("//portals", driver=driver)
                # XXX(babenko): portals
                wait(lambda: not yt_commands.exists("//tmp&", driver=driver))

        self._remove_objects(
            enable_secondary_cells_cleanup=self.get_param("ENABLE_SECONDARY_CELLS_CLEANUP", cluster_index),
            driver=driver,
        )

        yt_commands.gc_collect(driver=driver)
        yt_commands.clear_metadata_caches(driver=driver)

    def _master_exit_read_only_sync(self, yt_instance, driver=None):
        logger = yt.logger.LOGGER
        old_level = logger.level
        logger.setLevel(logging.ERROR)

        master_exit_read_only_sync(driver=driver)

        logger.setLevel(old_level)

    def _abort_transactions(self, driver=None):
        abort_command = "abort_transaction" if driver.get_config()["api_version"] == 4 else "abort_tx"
        requests = []

        test_cleanup.abort_transactions(
            list_action=lambda *args, **kwargs: yt_commands.ls(*args, driver=driver, **kwargs),
            abort_action=lambda transaction_id: requests.append(
                yt_commands.make_batch_request(abort_command, transaction_id=transaction_id)),
            exists_action=lambda *args, **kwargs: yt_commands.exists(*args, driver=driver, **kwargs),
            get_action=lambda *args, **kwargs: yt_commands.get(*args, driver=driver, **kwargs))

        yt_commands.execute_batch(requests, driver=driver)

    def _reset_nodes(self, wait_for_nodes=True, driver=None):
        use_maintenance_requests = yt_commands.get(
            "//sys/@config/node_tracker/forbid_maintenance_attribute_writes",
            default=False,
            driver=driver)
        if use_maintenance_requests:
            maintenance_attributes = ["maintenance_requests"]
        else:
            maintenance_attributes = ["banned", "decommissioned", "disable_write_sessions", "disable_scheduler_jobs", "disable_tablet_cells"]

        attributes = maintenance_attributes + [
            "maintenance_requests",
            "resource_limits_overrides",
            "user_tags",
        ]
        nodes = yt_commands.ls("//sys/cluster_nodes", attributes=attributes, driver=driver)

        requests = []
        for node in nodes:
            node_name = str(node)
            if node.attributes["resource_limits_overrides"] != {}:
                requests.append(
                    yt_commands.make_batch_request(
                        "set",
                        path="//sys/cluster_nodes/{0}/@resource_limits_overrides".format(node_name),
                        input={},
                    )
                )
            if node.attributes["user_tags"] != []:
                requests.append(
                    yt_commands.make_batch_request(
                        "set",
                        path="//sys/cluster_nodes/{0}/@user_tags".format(node_name),
                        input=[],
                    )
                )
            if use_maintenance_requests:
                if node.attributes["maintenance_requests"]:
                    requests.append(
                        yt_commands.make_batch_request(
                            "remove_maintenance",
                            component="cluster_node",
                            address=node_name,
                            all=True
                        )
                    )
            else:
                for maintenance_flag in maintenance_attributes:
                    if node.attributes[maintenance_flag]:
                        requests.append(
                            yt_commands.make_batch_request(
                                "set",
                                path=f"//sys/cluster_nodes/{node}/@{maintenance_flag}",
                                input="false"
                            ))

        for response in yt_commands.execute_batch(requests, driver=driver):
            assert not yt_commands.get_batch_error(response)

        location_key_lists = [
            ["data_node", "cache_locations"],
            ["data_node", "volume_manager", "layer_locations"],
            ["data_node", "store_locations"],
            ["exec_agent", "slot_manager", "locations"],
            ["exec_node", "slot_manager", "locations"],
        ]

        disabled_paths = []
        for node_config in self.Env.configs["node"]:
            for location_key_list in location_key_lists:
                locations = self._walk_dictionary(node_config, location_key_list)
                if locations:
                    for location in locations:
                        path = location["path"]
                        for root, dirs, files in os.walk(path):
                            if "disabled" in files:
                                yt.logger.error(f"Location {path} is disabled after test execution")
                                disabled_paths.append(path)
        assert not disabled_paths, 'Found disabled locations after test execution:\n' + "\n".join(disabled_paths)

        def nodes_online():
            nodes = yt_commands.ls("//sys/cluster_nodes", attributes=["state"], driver=driver)

            return all(node.attributes["state"] == "online" for node in nodes)

        if wait_for_nodes:
            wait(
                nodes_online,
                ignore_exceptions=True,
                error_message="Nodes weren't online after the test teardown for 60 seconds",
                timeout=60)

    def _walk_dictionary(self, dictionary, key_list):
        node = dictionary
        try:
            for key in key_list:
                node = node[key]
            return node
        except KeyError:
            return None

    def _remove_objects(self, enable_secondary_cells_cleanup, driver=None):
        def list_multiple_action(list_kwargs):
            responses = yt_commands.execute_batch(
                [
                    yt_commands.make_batch_request("list", return_only_value=True, **kwargs)
                    for kwargs in list_kwargs
                ],
                driver=driver)
            # COMPAT(gritukan)
            object_lists = []
            for response in responses:
                objects = []
                try:
                    objects = yt_commands.get_batch_output(response)
                except YtResponseError:
                    pass
                object_lists.append(objects)
            return object_lists

        def remove_multiple_action(remove_kwargs):
            yt_commands.gc_collect(driver=driver)
            yt_commands.execute_batch(
                [
                    yt_commands.make_batch_request("remove", **kwargs)
                    for kwargs in remove_kwargs
                ],
                driver=driver)

        def exists_multiple_action(object_ids):
            responses = yt_commands.execute_batch(
                [
                    yt_commands.make_batch_request("exists", path=object_id, return_only_value=True)
                    for object_id in object_ids
                ],
                driver=driver)
            return [yt_commands.get_batch_output(response)["value"] for response in responses]

        def get_object_ids_to_ignore():
            ids = []

            # COMPAT(gritukan, aleksandra-zh)
            if yt_commands.exists("//sys/tablet_cell_bundles/sequoia", driver=driver):
                ids += yt_commands.get("//sys/tablet_cell_bundles/sequoia/@tablet_cell_ids", driver=driver)

            return ids

        test_cleanup.cleanup_objects(
            list_multiple_action=list_multiple_action,
            remove_multiple_action=remove_multiple_action,
            exists_multiple_action=exists_multiple_action,
            enable_secondary_cells_cleanup=enable_secondary_cells_cleanup,
            object_ids_to_ignore=get_object_ids_to_ignore(),
        )

    def _wait_for_scheduler_state_restored(self, driver=None):
        exec_node_count = len(yt_commands.get_exec_nodes())

        def check():
            responses = yt_commands.execute_batch(
                [
                    yt_commands.make_batch_request(
                        "list",
                        path=yt_scheduler_helpers.scheduler_orchid_path() + "/scheduler/scheduling_info_per_pool_tree",
                        return_only_value=True,
                    ),
                    yt_commands.make_batch_request(
                        "get",
                        path=yt_scheduler_helpers.scheduler_orchid_path() + "/scheduler/default_pool_tree",
                        return_only_value=True,
                    ),
                    yt_commands.make_batch_request(
                        "list",
                        path=yt_scheduler_helpers.scheduler_orchid_default_pool_tree_path() + "/pools",
                        return_only_value=True,
                    ),
                    yt_commands.make_batch_request(
                        "get",
                        path=yt_scheduler_helpers.scheduler_orchid_path()
                        + "/scheduler/scheduling_info_per_pool_tree/default/node_count",
                        return_only_value=True,
                    ),
                ],
                driver=driver,
            )
            return (
                all(yt_commands.get_batch_error(r) is None for r in responses)
                and yt_commands.get_batch_output(responses[0]) == ["default"]
                and yt_commands.get_batch_output(responses[1]) == "default"
                and yt_commands.get_batch_output(responses[2]) == ["<Root>"]
                and yt_commands.get_batch_output(responses[3]) == exec_node_count
            )

        wait(check)

    def _restore_globals(self,
                         cluster_index,
                         scheduler_count,
                         scheduler_pool_trees_root,
                         driver=None):
        dynamic_master_config = self._apply_master_dynamic_config_patches(get_dynamic_master_config(), cluster_index)
        cypres_proxy_dynamic_config = self._apply_cypres_proxy_dynamic_config_patches(get_dynamic_cypress_proxy_config(), cluster_index)

        default_pool_tree_config = {
            "nodes_filter": "",
            "main_resource": "cpu",
            "min_child_heap_size": 3,
            "batch_operation_scheduling": {
                "batch_size": 3,
                "fallback_min_spare_job_resources": {
                    "cpu": 1.5,
                    "user_slots": 1,
                    "memory": 512 * 1024 * 1024,
                },
            },
            # Make default settings suitable for starvation and preemption.
            "preemptive_scheduling_backoff": 500,
            "non_preemptible_resource_usage_threshold": {"user_slots": 0},
            "fair_share_starvation_timeout": 1000,
            "enable_conditional_preemption": True,
            "check_operation_for_liveness_in_preschedule": False,
        }

        for response in yt_commands.execute_batch(
            [
                yt_commands.make_batch_request(
                    "set",
                    path="//sys/tablet_cell_bundles/default/@dynamic_options",
                    input={},
                ),
                yt_commands.make_batch_request(
                    "set",
                    path="//sys/tablet_cell_bundles/default/@tablet_balancer_config",
                    input={},
                ),
                yt_commands.make_batch_request("set", path="//sys/@config", input=dynamic_master_config),
                yt_commands.make_batch_request("set", path="//sys/cypress_proxies/@config", input=cypres_proxy_dynamic_config),
            ],
            driver=driver,
        ):
            assert not yt_commands.get_batch_error(response)

        restore_pool_trees_requests = []
        should_set_default_config = False
        if yt_commands.exists(scheduler_pool_trees_root + "/default", driver=driver):
            restore_pool_trees_requests.append(
                yt_commands.make_batch_request("remove", path=scheduler_pool_trees_root + "/default/*"))
            should_set_default_config = True
        else:
            restore_pool_trees_requests.append(
                yt_commands.make_batch_request(
                    "create",
                    type="scheduler_pool_tree",
                    attributes={
                        "name": "default",
                        "config": default_pool_tree_config,
                    },
                )
            )
        for pool_tree in yt_commands.ls(scheduler_pool_trees_root, driver=driver):
            if pool_tree != "default":
                restore_pool_trees_requests.append(
                    yt_commands.make_batch_request("remove", path=scheduler_pool_trees_root + "/" + pool_tree)
                )
        for response in yt_commands.execute_batch(restore_pool_trees_requests, driver=driver):
            yt_commands.raise_batch_error(response)

        # Could not add this to the batch request because of possible races at scheduler.
        yt_commands.set(scheduler_pool_trees_root + "/@default_tree", "default", driver=driver)

        if not yt_commands.exists("//sys/scheduler/user_to_default_pool", driver=driver):
            yt_commands.create(
                "document",
                "//sys/scheduler/user_to_default_pool",
                attributes={"value": {}},
                force=True,
                driver=driver)

        if should_set_default_config:
            yt_commands.set(scheduler_pool_trees_root + "/default/@config", default_pool_tree_config)

        if scheduler_count > 0:
            self._wait_for_scheduler_state_restored(driver=driver)

    def _apply_cypres_proxy_dynamic_config_patches(self, config, cluster_index):
        delta_cypress_proxy_config = self.get_param("DELTA_CYPRESS_PROXY_DYNAMIC_CONFIG", cluster_index)
        update_inplace(config, delta_cypress_proxy_config)
        return config

    def _apply_master_dynamic_config_patches(self, config, cluster_index):
        master_cell_descriptors = self.get_param("MASTER_CELL_DESCRIPTORS", cluster_index)
        update_inplace(
            config, self.get_param("DELTA_DYNAMIC_MASTER_CONFIG", cluster_index)
        )
        config["multicell_manager"]["cell_descriptors"] = master_cell_descriptors
        if self.Env.get_component_version("ytserver-master").abi >= (20, 4):
            config["enable_descending_sort_order"] = True
            config["enable_descending_sort_order_dynamic"] = True
        if self.Env.get_component_version("ytserver-master").abi >= (22, 1):
            config["enable_table_column_renaming"] = True
        allow_dynamic_renames = \
            self.Env.get_component_version("ytserver-master").abi >= (23, 1) and \
            self.ENABLE_DYNAMIC_TABLE_COLUMN_RENAMES

        if allow_dynamic_renames:
            config["enable_dynamic_table_column_renaming"] = True

        config["enable_static_table_drop_column"] = self.ENABLE_STATIC_DROP_COLUMN
        config["enable_dynamic_table_drop_column"] = self.ENABLE_DYNAMIC_DROP_COLUMN
        config["allow_everyone_create_secondary_indices"] = self.ENABLE_ALLOW_SECONDARY_INDICES

        # COMPAT(kvk1920)
        if self.Env.get_component_version("ytserver-master").abi < (24, 2):
            config["node_tracker"]["full_node_states_gossip_period"] = 6 * 60 * 60 * 1000

        if self.USE_SEQUOIA:
            config["sequoia_manager"]["enable"] = True
            if self.ENABLE_CYPRESS_TRANSACTIONS_IN_SEQUOIA:
                config["sequoia_manager"]["enable_cypress_transactions_in_sequoia"] = True

        # COMPAT(kvk1920)
        if self.Env.get_component_version("ytserver-master").abi >= (24, 2):
            config["transaction_manager"]["alert_transaction_is_not_compatible_with_method"] = True

        if not self.TEST_MAINTENANCE_FLAGS and self.Env.get_component_version("ytserver-master").abi >= (23, 1):
            config["node_tracker"]["forbid_maintenance_attribute_writes"] = True

        config.setdefault("chunk_service", {})
        return config

    def _wait_for_dynamic_config(self, root_path, config, instances, driver=None):
        if not self.WAIT_FOR_DYNAMIC_CONFIG:
            return

        def check():
            responses = yt_commands.execute_batch(
                [
                    yt_commands.make_batch_request(
                        "get",
                        path=f"{root_path}/{instance}/orchid/dynamic_config_manager",
                        return_only_value=True,
                    )
                    for instance in instances
                ],
                driver=driver,
                verbose=False,
            )
            for response in responses:
                output = yt_commands.get_batch_output(response)
                if config != output.get("applied_config"):
                    return False
            return True

        wait(check)

    def _setup_tablet_manager(self, driver=None):
        for response in yt_commands.execute_batch(
            [
                yt_commands.make_batch_request(
                    "set",
                    path="//sys/@config/tablet_manager/tablet_balancer/tablet_balancer_schedule",
                    input="1",
                ),
                yt_commands.make_batch_request(
                    "set",
                    path="//sys/@config/tablet_manager/tablet_cell_balancer/enable_verbose_logging",
                    input=True,
                ),
                yt_commands.make_batch_request(
                    "set",
                    path="//sys/@config/tablet_manager/tablet_balancer/enable_tablet_balancer",
                    input=self.ENABLE_TABLET_BALANCER,
                ),
                yt_commands.make_batch_request(
                    "set",
                    path="//sys/@config/tablet_manager/enable_bulk_insert",
                    input=self.ENABLE_BULK_INSERT,
                ),
                yt_commands.make_batch_request(
                    "set",
                    path="//sys/@config/tablet_manager/enable_backups",
                    input=True,
                ),
                yt_commands.make_batch_request(
                    "set",
                    path="//sys/@config/tablet_manager/enable_tablet_resource_validation",
                    input=True,
                ),
                yt_commands.make_batch_request(
                    "set",
                    path="//sys/@config/tablet_manager/use_avenues",
                    input=True,
                ),
                yt_commands.make_batch_request(
                    "set",
                    path="//sys/@config/tablet_manager/replicated_table_tracker/use_new_replicated_table_tracker",
                    input=self.ENABLE_STANDALONE_REPLICATED_TABLE_TRACKER,
                ),
            ],
            driver=driver,
        ):
            assert not yt_commands.get_batch_error(response)

    def _setup_tablet_balancer_dynamic_config(self, driver=None):
        if self.ENABLE_STANDALONE_TABLET_BALANCER:
            config = {
                "enable": self.ENABLE_STANDALONE_TABLET_BALANCER,
                "enable_everywhere": self.ENABLE_STANDALONE_TABLET_BALANCER,
                "schedule": "1",
                "min_desired_tablet_size": 0,
                "action_manager": {
                    "tablet_action_polling_period": 100,
                }
            }

            yt_commands.set(
                "//sys/tablet_balancer/config",
                config,
                driver=driver)

            instances = yt_commands.ls("//sys/tablet_balancer/instances")

            self._wait_for_dynamic_config("//sys/tablet_balancer/instances", config, instances, driver=driver)

    def _setup_standalone_replicated_table_tracker_dynamic_config(self, driver=None):
        if self.ENABLE_STANDALONE_REPLICATED_TABLE_TRACKER:
            config = yt_commands.get("//sys/@config/tablet_manager/replicated_table_tracker", driver=driver)
            config["use_new_replicated_table_tracker"] = True

            yt_commands.set(
                "//sys/@config/tablet_manager/replicated_table_tracker",
                config,
                driver=driver)

            instances_path = "//sys/replicated_table_tracker/instances"
            instances = yt_commands.ls(instances_path, driver=driver)
            self._wait_for_dynamic_config(instances_path, config, instances, driver=driver)

    def _clear_ql_pools(self, driver=None):
        yt_commands.remove("//sys/ql_pools/*", driver=driver)

    @classmethod
    def _restore_bundle_options(cls, bundle, account, cluster_index):
        driver = yt_commands.get_driver(cluster=cls.get_cluster_name(cluster_index))
        num_nodes = cls.get_param("NUM_NODES", cluster_index)

        def do():
            for response in yt_commands.execute_batch(
                [
                    yt_commands.make_batch_request(
                        "set",
                        path=f"//sys/tablet_cell_bundles/{bundle}/@dynamic_options",
                        input={},
                    ),
                    yt_commands.make_batch_request(
                        "set",
                        path=f"//sys/tablet_cell_bundles/{bundle}/@tablet_balancer_config",
                        input={},
                    ),
                    yt_commands.make_batch_request(
                        "set",
                        path=f"//sys/tablet_cell_bundles/{bundle}/@options",
                        input={
                            "changelog_replication_factor": 1 if num_nodes < 3 else 3,
                            "changelog_read_quorum": 1 if num_nodes < 3 else 2,
                            "changelog_write_quorum": 1 if num_nodes < 3 else 2,
                            "changelog_account": account,
                            "snapshot_replication_factor": 1 if num_nodes < 3 else 3,
                            "snapshot_account": account,
                        },
                    ),
                ],
                driver=driver,
            ):
                assert not yt_commands.get_batch_error(response)

        _retry_with_gc_collect(do, driver=driver)

    @classmethod
    def _restore_default_bundle_options(cls, cluster_index):
        cls._restore_bundle_options("default", "sys", cluster_index)

    @classmethod
    def _restore_sequoia_bundle_options(cls, cluster_index):
        assert cls._is_ground_cluster(cluster_index)
        # TODO(kvk1920): use Sequoia bundle and account from non-ground
        # cluster's config.
        cls._restore_bundle_options("sequoia", "sequoia", cluster_index)

    def _remove_operations(self, driver=None):
        abort_command = "abort_operation" if driver.get_config()["api_version"] == 4 else "abort_op"

        if yt_commands.get("//sys/scheduler/instances/@count", driver=driver) == 0:
            return

        abort_requests = []
        remove_requests = []

        test_cleanup.cleanup_operations(
            list_action=lambda *args, **kwargs: yt_commands.ls(*args, driver=driver, **kwargs),
            abort_action=lambda operation_id: abort_requests.append(
                yt_commands.make_batch_request(abort_command, operation_id=operation_id)),
            remove_action=lambda *args, **kwargs: remove_requests.append(
                yt_commands.make_batch_request("remove", *args, **kwargs)),
        )

        for response in yt_commands.execute_batch(abort_requests, driver=driver):
            err = yt_commands.get_batch_error(response)
            if err is not None:
                print(format_error(err), file=sys.stderr)

        # TODO(ignat): is it actually need to be called here?
        self._abort_transactions(driver=driver)

        for response in yt_commands.execute_batch(remove_requests, driver=driver):
            assert not yt_commands.get_batch_error(response)

    def _wait_for_jobs_to_vanish(self, driver=None):
        nodes_with_flavors = yt_commands.get("//sys/cluster_nodes", driver=driver, attributes=["flavors"])

        exec_nodes = [
            node
            for node, attr in nodes_with_flavors.items() if "exec" in attr.attributes.get("flavors", ["exec"])
        ]

        def check_no_exec_node_jobs():
            requests = [
                yt_commands.make_batch_request(
                    "get",
                    path="//sys/cluster_nodes/{0}/orchid/exec_node/job_controller/active_job_count".format(node),
                    return_only_value=True,
                )
                for node in exec_nodes
            ]

            responses = yt_commands.execute_batch(requests, driver=driver)

            return all([yt_commands.get_batch_output(response) == 0 for response in responses])

        try:
            wait(check_no_exec_node_jobs, iter=300)
        except WaitFailed:
            requests = [
                yt_commands.make_batch_request(
                    "get",
                    path="//sys/cluster_nodes/{0}/orchid/exec_node/job_controller/active_jobs".format(node),
                    return_only_value=True,
                )
                for node in exec_nodes
            ]

            responses = yt_commands.execute_batch(requests, driver=driver)
            print("There are remaining scheduler jobs:", file=sys.stderr)

            for node, response in zip(exec_nodes, responses):
                print("Node {}: {}".format(node, response), file=sys.stderr)


def get_custom_rootfs_delta_node_config():
    return {
        "exec_node": {
            "slot_manager": {
                "do_not_set_user_id": True,
                "job_environment": {
                    "use_exec_from_layer": True,
                },
            },
        }
    }


def get_service_component_name(service):
    return {
        SCHEDULERS_SERVICE: "scheduler",
        CONTROLLER_AGENTS_SERVICE: "controller-agent",
        NODES_SERVICE: "node",
        CHAOS_NODES_SERVICE: "node",
        MASTERS_SERVICE: "master",
        MASTER_CACHES_SERVICE: "master-cache",
        QUEUE_AGENTS_SERVICE: "queue-agent",
        RPC_PROXIES_SERVICE: "proxy",
        HTTP_PROXIES_SERVICE: "http-proxy",
        KAFKA_PROXIES_SERVICE: "kafka-proxy",
    }[service]
