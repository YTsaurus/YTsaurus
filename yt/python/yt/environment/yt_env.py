# -*- coding: utf-8 -*-

from __future__ import print_function

from .configs_provider import _init_logging, build_configs
from .default_config import get_dynamic_master_config, get_dynamic_queue_agent_config
from .helpers import (
    read_config, write_config, is_dead, OpenPortIterator,
    wait_for_removing_file_lock, get_value_from_config, WaitFailed,
    is_port_opened, push_front_env_path, wait_for_dynamic_config_update)
from .porto_helpers import PortoSubprocess, porto_available
from .watcher import ProcessWatcher
from .init_cluster import _initialize_world_for_local_cluster
from .local_cypress import _synchronize_cypress_with_local_dir
from .local_cluster_configuration import modify_cluster_configuration, get_patched_dynamic_node_config, get_patched_dynamic_master_config

from .tls_helpers import create_ca, create_certificate

from yt.common import YtError, remove_file, makedirp, update, get_value, which, to_native_str
from yt.wrapper.common import flatten
from yt.wrapper.constants import FEEDBACK_URL
from yt.wrapper.errors import YtResponseError
from yt.wrapper import YtClient

from yt.test_helpers import wait

import yt.yson as yson
import yt.subprocess_wrapper as subprocess

try:
    from yt.packages.six import itervalues, iteritems
    from yt.packages.six.moves import xrange, map as imap
except ImportError:
    from six import itervalues, iteritems
    from six.moves import xrange, map as imap

import yt.packages.requests as requests
from yt.common import get_fqdn

import logging
import os
import copy
import datetime
import errno
import itertools
import hashlib
import time
import signal
import socket
import shutil
import stat
import sys
import traceback
from collections import defaultdict, namedtuple, OrderedDict
from threading import RLock

logger = logging.getLogger("YtLocal")

# YT-12960: disable debug logging about file locks.
logging.getLogger("library.python.filelock").setLevel(logging.INFO)

BinaryVersion = namedtuple("BinaryVersion", ["abi", "literal"])

BinaryInfo = namedtuple("BinaryInfo", ["name", "path"])

_environment_driver_logging_config = None


DEFAULT_ADMIN_USERNAME = "admin"
DEFAULT_ADMIN_PASSWORD = "password"
DEFAULT_ADMIN_TOKEN = "password"


def set_environment_driver_logging_config(config):
    global _environment_driver_logging_config
    _environment_driver_logging_config = config


class YtEnvRetriableError(YtError):
    pass


def _parse_version(s):
    if "version:" in s:
        # "ytserver  version: 0.17.3-unknown~debug~0+local"
        # "ytserver  version: 18.5.0-local~local"
        literal = s.split(":", 1)[1].strip()
    else:
        # "19.0.0-local~debug~local"
        literal = s.strip()
    parts = list(imap(int, literal.split("-")[0].split(".")[:3]))
    abi = tuple(parts[:2])
    return BinaryVersion(abi, literal)


def _get_yt_binary_path(binary, custom_paths):
    paths = which(binary, custom_paths=custom_paths)
    if paths:
        return paths[0]
    return None


def _do_get_yt_binary_versions(binary_infos):
    result = OrderedDict()

    # It is important to run processes simultaneously to reduce delay when reading output.
    processes = []
    for binary_info in binary_infos:
        assert binary_info.path

        process = subprocess.Popen([binary_info.path, "--version"], stdout=subprocess.PIPE)
        processes.append((binary_info.name, process))

    for name, process in processes:
        stdout, _ = process.communicate()
        assert name not in result
        result[name] = _parse_version(to_native_str(stdout))
        assert process.returncode == 0, f"Failed to get version of {name}, returncode={process.returncode}"

    return result


def get_yt_binary_versions(named_paths):
    binary_infos = [
        BinaryInfo(name=name, path=_get_yt_binary_path(name, custom_paths=[custom_path]))
        for name, custom_path in named_paths
    ]
    return _do_get_yt_binary_versions(binary_infos)


def _get_yt_versions(custom_paths):
    binaries = ["ytserver-master", "ytserver-node", "ytserver-scheduler", "ytserver-controller-agent",
                "ytserver-http-proxy", "ytserver-proxy", "ytserver-job-proxy",
                "ytserver-clock", "ytserver-discovery", "ytserver-cell-balancer",
                "ytserver-exec", "ytserver-tools", "ytserver-timestamp-provider", "ytserver-master-cache",
                "ytserver-tablet-balancer", "ytserver-replicated-table-tracker", "ytserver-kafka-proxy", "ytserver-queue-agent"]

    binary_infos = [BinaryInfo(name=name, path=_get_yt_binary_path(name, custom_paths=custom_paths)) for name in binaries]

    path_dir_to_binaries = defaultdict(list)
    missing_binaries = []
    for name, path in binary_infos:
        if path is None:
            missing_binaries.append(name)
        else:
            path_dir = os.path.dirname(path)
            path_dir_to_binaries[path_dir].append(name)

    if missing_binaries:
        logger.info("Binaries %s are not found, custom_paths=%s", missing_binaries, custom_paths)
    for path_dir, binaries in iteritems(path_dir_to_binaries):
        logger.debug("Binaries %s found at directory %s", binaries, path_dir)

    filtered_binary_infos = [binary_info for binary_info in binary_infos if binary_info.path is not None]

    return _do_get_yt_binary_versions(filtered_binary_infos)


def _configure_logger(path):
    ytrecipe = os.environ.get("YT_OUTPUT") is not None
    if not ytrecipe:
        logger.propagate = False
        if not logger.handlers:
            logger.addHandler(logging.StreamHandler())
        logger.handlers[0].setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    else:
        logger.handlers = [logging.FileHandler(path)]
        logger.handlers[0].setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))


def _get_ports_generator(yt_config):
    if yt_config.port_range_start:
        return itertools.count(yt_config.port_range_start)
    elif yt_config.listen_port_pool is not None:
        return iter(yt_config.listen_port_pool)
    else:
        return OpenPortIterator(
            port_locks_path=os.environ.get("YT_LOCAL_PORT_LOCKS_PATH", None),
            local_port_range=yt_config.local_port_range)


class YTInstance(object):
    def __init__(self, path, yt_config,
                 modify_configs_func=None,
                 modify_dynamic_configs_func=None,
                 kill_child_processes=False,
                 watcher_config=None,
                 run_watcher=True,
                 ytserver_all_path=None,
                 watcher_binary=None,
                 stderrs_path=None,
                 preserve_working_dir=False,
                 external_bin_path=None,
                 tmpfs_path=None,
                 open_port_iterator=None,
                 modify_driver_logging_config_func=None):
        self.path = os.path.realpath(os.path.abspath(path))
        self.yt_config = yt_config
        self.modify_dynamic_configs_func = modify_dynamic_configs_func

        self.id = yt_config.cluster_name

        if yt_config.master_count == 0:
            raise YtError("Can't start local YT without master")
        if yt_config.scheduler_count == 0 and yt_config.controller_agent_count != 0:
            raise YtError("Can't start controller agent without scheduler")
        if yt_config.node_count < len(yt_config.node_flavors):
            raise YtError("Total node count is less than count of nodes with flavors")

        if yt_config.use_porto_for_servers and not porto_available():
            raise YtError("Option use_porto_for_servers is specified but porto is not available")
        self._subprocess_module = PortoSubprocess if yt_config.use_porto_for_servers else subprocess

        self._watcher_binary = watcher_binary

        if external_bin_path is not None:
            self.bin_path = external_bin_path
        else:
            self.bin_path = os.path.abspath(os.path.join(self.path, "bin"))
        self.logs_path = os.path.abspath(os.path.join(self.path, "logs"))
        self.configs_path = os.path.abspath(os.path.join(self.path, "configs"))
        self.runtime_data_path = os.path.abspath(os.path.join(self.path, "runtime_data"))
        self.pids_filename = os.path.join(self.path, "pids.txt")
        self.pids_file = None

        self._load_existing_environment = False
        if os.path.exists(self.path):
            if not preserve_working_dir:
                shutil.rmtree(self.path, ignore_errors=True)
            else:
                self._load_existing_environment = True

        self.ytserver_all_path = ytserver_all_path
        if self.ytserver_all_path is None:
            self.ytserver_all_path = os.environ.get("YTSERVER_ALL_PATH")
        if self.ytserver_all_path is not None:
            # Replace path with realpath in order to support specifying a relative path.
            self.ytserver_all_path = os.path.realpath(self.ytserver_all_path)

        if not self._load_existing_environment:
            if self.ytserver_all_path is not None:
                if not os.path.exists(self.ytserver_all_path):
                    raise YtError("ytserver-all binary is missing at path " + self.ytserver_all_path)
                makedirp(self.bin_path)
                programs = ["master", "clock", "node", "job-proxy", "exec", "cell-balancer",
                            "proxy", "http-proxy", "tools", "scheduler", "discovery",
                            "controller-agent", "timestamp-provider", "master-cache",
                            "tablet-balancer", "replicated-table-tracker", "queue-agent", "kafka-proxy"]
                for program in programs:
                    os.symlink(os.path.abspath(self.ytserver_all_path), os.path.join(self.bin_path, "ytserver-" + program))

        if os.path.exists(self.bin_path):
            self.custom_paths = [self.bin_path]
        else:
            self.custom_paths = None

        with push_front_env_path(self.bin_path):
            self._binary_to_version = _get_yt_versions(custom_paths=self.custom_paths)

        if "ytserver-master" not in self._binary_to_version:
            raise YtError("Failed to find YT binaries; specify ytserver-all path using "
                          "ytserver_all_path keyword in Python/--ytserver-all-path argument of yt_local/$YTSERVER_ALL_PATH, or "
                          "put ytserver-* binaries into $PATH")

        abi_versions = set(imap(lambda v: v.abi, self._binary_to_version.values()))
        self.abi_version = abi_versions.pop()

        self._lock = RLock()

        self.configs = defaultdict(list)
        self.config_paths = defaultdict(list)

        makedirp(self.path)
        makedirp(self.logs_path)
        # This is important to collect ytserver-exec stderrs.
        os.chmod(self.logs_path, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
        makedirp(self.configs_path)
        makedirp(self.runtime_data_path)

        # NB: we should not write logs before this point.
        _configure_logger(os.path.join(self.path, "yt_local.log"))

        self.stderrs_path = stderrs_path or os.path.join(self.path, "stderrs")
        makedirp(self.stderrs_path)
        self._stderr_paths = defaultdict(list)

        self._tmpfs_path = tmpfs_path
        if self._tmpfs_path is not None:
            self._tmpfs_path = os.path.abspath(self._tmpfs_path)

        # Dictionary from service name to the list of processes.
        self._service_processes = defaultdict(list)
        # Dictionary from pid to tuple with Process and arguments
        self._pid_to_process = {}

        self._kill_child_processes = kill_child_processes
        self._started = False
        self._wait_functions = []

        self._run_watcher = run_watcher
        self.watcher_config = watcher_config
        if self.watcher_config is None:
            self.watcher_config = {}

        if not yt_config.enable_log_compression:
            self.watcher_config["logs_rotate_compress"] = True
        else:
            self.watcher_config["disable_logrotate"] = True

        self._default_client_config = {
            "enable_token": self.yt_config.enable_auth,
            "is_local_mode": True,
            "pickling": {
                "enable_local_files_usage_in_job": True,
            },
        }

        if open_port_iterator is not None:
            self._open_port_iterator = open_port_iterator
        else:
            self._open_port_iterator = _get_ports_generator(yt_config)

        self._prepare_builtin_environment(
            self._open_port_iterator,
            modify_configs_func,
            modify_driver_logging_config_func)

    def _prepare_directories(self):
        master_dirs = []
        master_tmpfs_dirs = [] if self._tmpfs_path else None

        for cell_index in xrange(self.yt_config.secondary_cell_count + 1):
            name = self._get_master_name("master", cell_index)
            master_dirs.append([os.path.join(self.runtime_data_path, name, str(i))
                                for i in xrange(self.yt_config.master_count)])
            for dir_ in master_dirs[cell_index]:
                makedirp(dir_)

            if self._tmpfs_path is not None:
                master_tmpfs_dirs.append([os.path.join(self._tmpfs_path, name, str(i))
                                          for i in xrange(self.yt_config.master_count)])

                for dir_ in master_tmpfs_dirs[cell_index]:
                    makedirp(dir_)

        return {"master": master_dirs,
                "master_tmpfs": master_tmpfs_dirs,
                "clock": self._make_service_dirs("clock", self.yt_config.clock_count),
                "clock_tmpfs": self._make_service_dirs("clock", self.yt_config.clock_count, in_tmpfs=True),
                "discovery": self._make_service_dirs("discovery", self.yt_config.discovery_server_count),
                "timestamp_provider": self._make_service_dirs("timestamp_provider",
                                                              self.yt_config.timestamp_provider_count),
                # bundle controller uses same directory,
                "cell_balancer": self._make_service_dirs("cell_balancer", self.yt_config.cell_balancer_count),
                "scheduler": self._make_service_dirs("scheduler", self.yt_config.scheduler_count),
                "controller_agent": self._make_service_dirs("controller_agent", self.yt_config.controller_agent_count),
                "node": self._make_service_dirs("node", self.yt_config.node_count),
                "node_tmpfs": self._make_service_dirs("node", self.yt_config.node_count, in_tmpfs=True),
                "chaos_node": self._make_service_dirs("chaos_node", self.yt_config.chaos_node_count),
                "master_cache": self._make_service_dirs("master_cache", self.yt_config.master_cache_count),
                "http_proxy": self._make_service_dirs("http_proxy", self.yt_config.http_proxy_count),
                "queue_agent": self._make_service_dirs("queue_agent", self.yt_config.queue_agent_count),
                "kafka_proxy": self._make_service_dirs("kafka_proxy", self.yt_config.kafka_proxy_count),
                "rpc_proxy": self._make_service_dirs("rpc_proxy", self.yt_config.rpc_proxy_count),
                "tablet_balancer": self._make_service_dirs("tablet_balancer", self.yt_config.tablet_balancer_count),
                "cypress_proxy": self._make_service_dirs("cypress_proxy", self.yt_config.cypress_proxy_count),
                "replicated_table_tracker": self._make_service_dirs("replicated_table_tracker",
                                                                    self.yt_config.replicated_table_tracker_count),
                }

    def _log_component_line(self, binary, name, count, is_external=False):
        if isinstance(count, int) and count == 0:
            return
        if binary is not None and binary not in self._binary_to_version:
            return

        count = str(count)
        if binary is not None:
            version = " (version: {})".format(self._binary_to_version[binary].literal)
        else:
            version = ""

        if is_external:
            external_marker = " [external]"
        else:
            external_marker = ""

        logger.info("  {} {}{}{}".format(name.ljust(20), count.ljust(15), version, external_marker))

    def get_ytserver_all_path(self):
        return self.ytserver_all_path

    def get_bin_path(self):
        return self.bin_path

    def prepare_external_component(self, binary, lowercase_with_underscores_name, human_readable_name, configs, force_overwrite=False):
        self._log_component_line(binary, human_readable_name, len(configs), is_external=True)

        config_paths = []

        for index, config in enumerate(configs):
            config_name = "{}-{}.yson".format(lowercase_with_underscores_name, index)
            config_path = os.path.join(self.configs_path, config_name)
            if self._load_existing_environment and not force_overwrite:
                config = read_config(config_path)
            else:
                write_config(config, config_path)

            config_paths.append(config_path)
            self._service_processes[lowercase_with_underscores_name].append(None)

        return config_paths

    def _prepare_certificates(self):
        names = [self.yt_config.fqdn, self.yt_config.cluster_name]

        if self.yt_config.ca_cert is None:
            self.yt_config.ca_cert = os.path.join(self.path, "ca.crt")
            self.yt_config.ca_cert_key = os.path.join(self.path, "ca.key")
            create_ca(ca_cert=self.yt_config.ca_cert, ca_cert_key=self.yt_config.ca_cert_key)

        if self.yt_config.rpc_cert is None:
            self.yt_config.rpc_cert = os.path.join(self.path, "rpc.crt")
            self.yt_config.rpc_cert_key = os.path.join(self.path, "rpc.key")
            create_certificate(
                ca_cert=self.yt_config.ca_cert,
                ca_cert_key=self.yt_config.ca_cert_key,
                cert=self.yt_config.rpc_cert,
                cert_key=self.yt_config.rpc_cert_key,
                names=names
            )

        if self.yt_config.https_cert is None and self.yt_config.http_proxy_count > 0:
            self.yt_config.https_cert = os.path.join(self.path, "https.crt")
            self.yt_config.https_cert_key = os.path.join(self.path, "https.key")
            create_certificate(
                ca_cert=self.yt_config.ca_cert,
                ca_cert_key=self.yt_config.ca_cert_key,
                cert=self.yt_config.https_cert,
                cert_key=self.yt_config.https_cert_key,
                names=names
            )

    def _prepare_builtin_environment(self, ports_generator, modify_configs_func, modify_driver_logging_config_func):
        service_infos = [
            ("ytserver-clock", "clocks", self.yt_config.clock_count),
            ("ytserver-discovery", "discovery servers", self.yt_config.discovery_server_count),
            ("ytserver-master", "masters", "{} ({} nonvoting)".format(self.yt_config.master_count, self.yt_config.nonvoting_master_count)),
            ("ytserver-timestamp-provider", "timestamp providers", self.yt_config.timestamp_provider_count),
            ("ytserver-node", "nodes", "{} ({} chaos)".format(self.yt_config.node_count, self.yt_config.chaos_node_count)),
            ("ytserver-master-cache", "master caches", self.yt_config.master_cache_count),
            ("ytserver-scheduler", "schedulers", self.yt_config.scheduler_count),
            ("ytserver-controller-agent", "controller agents", self.yt_config.controller_agent_count),
            ("ytserver-cell-balancer", "cell balancers", self.yt_config.cell_balancer_count),
            (None, "secondary cells", self.yt_config.secondary_cell_count),
            ("ytserver-queue-agent", "queue agents", self.yt_config.queue_agent_count),
            ("ytserver-kafka-proxy", "kafka proxies", self.yt_config.kafka_proxy_count),
            ("ytserver-tablet-balancer", "tablet balancers", self.yt_config.tablet_balancer_count),
            ("ytserver-http-proxy", "HTTP proxies", self.yt_config.http_proxy_count),
            ("ytserver-proxy", "RPC proxies", self.yt_config.rpc_proxy_count),
            ("ytserver-cypres-proxy", "cypress proxies", self.yt_config.cypress_proxy_count),
            ("ytserver-replicated-table-tracker", "replicated table trackers", self.yt_config.replicated_table_tracker_count),
        ]

        logger.info("Start preparing cluster instance as follows:")
        for binary, name, count in service_infos:
            self._log_component_line(binary, name, count, is_external=False)

        logger.info("  {} {}".format("working dir".ljust(20), self.path.ljust(15)))

        if self.yt_config.master_count == 0:
            logger.warning("Master count is zero. Instance is not prepared.")
            return

        logger.info("Preparing directories")
        self._directories = self._prepare_directories()

        if self.yt_config.enable_tls:
            logger.info("Preparing certificates")
            self._prepare_certificates()

        logger.info("Preparing configs")
        cluster_configuration = build_configs(
            self.yt_config, ports_generator, self._directories, self.logs_path, self._binary_to_version)

        modify_cluster_configuration(self.yt_config, cluster_configuration)

        if modify_configs_func:
            modify_configs_func(cluster_configuration, self.abi_version)

        self._cluster_configuration = cluster_configuration

        if self.yt_config.master_count + self.yt_config.secondary_cell_count > 0:
            self._prepare_masters(cluster_configuration["master"])
        if self.yt_config.clock_count > 0:
            self._prepare_clocks(cluster_configuration["clock"])
        if self.yt_config.timestamp_provider_count > 0:
            self._prepare_timestamp_providers(cluster_configuration["timestamp_provider"])
        if self.yt_config.discovery_server_count > 0:
            self._prepare_discovery_servers(cluster_configuration["discovery"])
        if self.yt_config.node_count > 0:
            self._prepare_nodes(cluster_configuration["node"])
        if self.yt_config.chaos_node_count > 0:
            self._prepare_chaos_nodes(cluster_configuration["chaos_node"])
        if self.yt_config.master_cache_count > 0:
            self._prepare_master_caches(cluster_configuration["master_cache"])
        if self.yt_config.scheduler_count > 0:
            self._prepare_schedulers(cluster_configuration["scheduler"])
        if self.yt_config.controller_agent_count > 0:
            self._prepare_controller_agents(cluster_configuration["controller_agent"])
        if self.yt_config.http_proxy_count > 0:
            self._prepare_http_proxies(cluster_configuration["http_proxy"])
        if self.yt_config.rpc_proxy_count > 0:
            self._prepare_rpc_proxies(cluster_configuration["rpc_proxy"], cluster_configuration["rpc_client"])
        if self.yt_config.cell_balancer_count > 0:
            self._prepare_cell_balancers(cluster_configuration["cell_balancer"])
        if self.yt_config.queue_agent_count > 0:
            self._prepare_queue_agents(cluster_configuration["queue_agent"])
        if self.yt_config.kafka_proxy_count > 0:
            self._prepare_kafka_proxies(cluster_configuration["kafka_proxy"])
        if self.yt_config.tablet_balancer_count > 0:
            self._prepare_tablet_balancers(cluster_configuration["tablet_balancer"])
        if self.yt_config.cypress_proxy_count > 0:
            self._prepare_cypress_proxies(cluster_configuration["cypress_proxy"])
        if self.yt_config.replicated_table_tracker_count > 0:
            self._prepare_replicated_table_trackers(cluster_configuration["replicated_table_tracker"])

        self._prepare_drivers(
            cluster_configuration["driver"],
            cluster_configuration["rpc_driver"],
            cluster_configuration["master"],
            cluster_configuration["clock"],
            modify_driver_logging_config_func)

        logger.info("Finished preparing cluster instance")

    def _make_service_dirs(self, service_name, count, in_tmpfs=False):
        if in_tmpfs:
            if self._tmpfs_path is None:
                return None
            else:
                path = self._tmpfs_path
        else:
            path = self.runtime_data_path

        service_dirs = [os.path.join(path, service_name, str(i)) for i in xrange(count)]
        for dir_ in service_dirs:
            makedirp(dir_)
        return service_dirs

    def _wait_or_skip(self, function, sync):
        if sync:
            function()
        else:
            self._wait_functions.append(function)

    def remove_runtime_data(self):
        if os.path.exists(self.runtime_data_path):
            shutil.rmtree(self.runtime_data_path, ignore_errors=True)

    def start(self, on_masters_started_func=None):
        for name, processes in iteritems(self._service_processes):
            for index in xrange(len(processes)):
                processes[index] = None
        self._pid_to_process.clear()

        if self.yt_config.master_count == 0:
            logger.warning("Cannot start YT instance without masters")
            return

        self.pids_file = open(self.pids_filename, "wt")
        try:
            self._configure_driver_logging()
            self._configure_yt_tracing()
            self._configure_yp_service_discovery()

            if self.yt_config.http_proxy_count > 0:
                self.start_http_proxy(sync=False)

            self.start_master_cell(sync=False)

            if self.yt_config.clock_count > 0:
                self.start_clock(sync=False)

            if self.yt_config.discovery_server_count > 0:
                self.start_discovery_server(sync=False)

            if self.yt_config.timestamp_provider_count > 0:
                self.start_timestamp_providers(sync=False)

            if self.yt_config.master_cache_count > 0:
                self.start_master_caches(sync=False)

            if self.yt_config.rpc_proxy_count > 0:
                self.start_rpc_proxy(sync=False)

            if self.yt_config.cell_balancer_count > 0:
                self.start_cell_balancers(sync=False)

            if self.yt_config.queue_agent_count > 0:
                self.start_queue_agents(sync=False)

            if self.yt_config.kafka_proxy_count > 0:
                self.start_kafka_proxy(sync=False)

            if self.yt_config.tablet_balancer_count > 0:
                self.start_tablet_balancers(sync=False)

            if self.yt_config.cypress_proxy_count > 0:
                self.start_cypress_proxies(sync=False)

            if self.yt_config.replicated_table_tracker_count > 0:
                self.start_replicated_table_trackers(sync=False)

            self.synchronize()

            if self.yt_config.create_admin_user:
                self.create_admin_user()

            if not self.yt_config.defer_secondary_cell_start:
                self.start_secondary_master_cells(sync=False)
                self.synchronize()

            client = self._create_cluster_client()

            if self.yt_config.http_proxy_count > 0:
                # NB: it is used to determine proper operation URL in local mode.
                client.set("//sys/@local_mode_proxy_address", self.get_http_proxy_address())
                # NB: it is used for enabling local mode optimizations in C++ API.
                client.set("//sys/@local_mode_fqdn", get_fqdn())

            if on_masters_started_func is not None:
                on_masters_started_func()

            patched_node_config = self._apply_nodes_dynamic_config(client)

            queue_agent_dynamic_config = None
            if self.yt_config.queue_agent_count > 0:
                queue_agent_dynamic_config = self._apply_queue_agent_dynamic_config(client)

            # TODO(nadya73): fill kafka proxy dynamic config.

            if self.yt_config.node_count > 0 and not self.yt_config.defer_node_start:
                self.start_nodes(sync=False)
            if self.yt_config.chaos_node_count > 0 and not self.yt_config.defer_chaos_node_start:
                self.start_chaos_nodes(sync=False)
            if self.yt_config.scheduler_count > 0 and not self.yt_config.defer_scheduler_start:
                self.start_schedulers(sync=False)
            if self.yt_config.controller_agent_count > 0 and not self.yt_config.defer_controller_agent_start:
                self.start_controller_agents(sync=False)

            self.synchronize()

            if self._run_watcher:
                self._start_watcher()
                self._started = True

            if self.yt_config.initialize_world:
                if not self._load_existing_environment:
                    client = self.create_client()

                    # This hack is necessary to correctly run inside docker container.
                    # In this case public proxy port differs from proxy port inside container and
                    # we should use latter.
                    client.config["proxy"]["enable_proxy_discovery"] = False

                    _initialize_world_for_local_cluster(
                        client,
                        self,
                        self.yt_config)

                    if self.yt_config.local_cypress_dir is not None:
                        _synchronize_cypress_with_local_dir(
                            self.yt_config.local_cypress_dir,
                            self.yt_config.meta_files_suffix,
                            client)

            self._wait_for_dynamic_config_update(patched_node_config, client)
            if queue_agent_dynamic_config is not None:
                self._wait_for_dynamic_config_update(queue_agent_dynamic_config, client, instance_type="queue_agents/instances")
            # TODO(nadya73): update kafka proxy dynamic config
            self._write_environment_info_to_file()
            logger.info("Environment started")
        except (YtError, KeyboardInterrupt) as err:
            logger.exception("Failed to start environment")
            self.stop(force=True)
            raise YtError("Failed to start environment", inner_errors=[err])

    def create_admin_user(self):
        client = self.create_native_client()

        client.create("user", attributes={"name": DEFAULT_ADMIN_USERNAME})
        client.add_member(DEFAULT_ADMIN_USERNAME, "superusers")

        token_hash = hashlib.sha256(DEFAULT_ADMIN_TOKEN.encode()).hexdigest()
        token_map_node_path = f"//sys/cypress_tokens/{token_hash}"
        client.create("map_node", token_map_node_path)
        client.set_attribute(token_map_node_path, "user", DEFAULT_ADMIN_USERNAME)

        client.set_user_password(DEFAULT_ADMIN_USERNAME, DEFAULT_ADMIN_PASSWORD)

        wait(lambda: client.get("//sys/users/{}/@life_stage".format(DEFAULT_ADMIN_USERNAME)) == "creation_committed")

    def stop(self, force=False):
        if not self._started and not force:
            return

        with self._lock:
            self.stop_impl()

        self._started = False

    def exit(self):
        if self.pids_file is not None:
            self.pids_file.close()

    def stop_impl(self):
        killed_services = set()

        if self._run_watcher:
            self.kill_service("watcher")
            killed_services.add("watcher")

        for name in ["http_proxy", "node", "chaos_node", "scheduler", "controller_agent", "master",
                     "rpc_proxy", "timestamp_provider", "master_caches", "cell_balancer",
                     "tablet_balancer", "cypress_proxy", "replicated_table_tracker", "queue_agent", "kafka_proxy"]:
            if name in self.configs:
                self.kill_service(name)
                killed_services.add(name)

        for name in self.configs:
            if name not in killed_services:
                self.kill_service(name)

        self.pids_file.close()
        remove_file(self.pids_filename, force=True)

        if self._open_port_iterator is not None:
            if hasattr(self._open_port_iterator, "release"):
                self._open_port_iterator.release()
            self._open_port_iterator = None

        wait_for_removing_file_lock(os.path.join(self.path, "lock_file"))

    def synchronize(self):
        for func in self._wait_functions:
            func()
        self._wait_functions = []

    def rewrite_master_configs(self):
        self._prepare_masters(self._cluster_configuration["master"], force_overwrite=True)

    def rewrite_node_configs(self):
        self._prepare_nodes(self._cluster_configuration["node"], force_overwrite=True)

    def rewrite_chaos_node_configs(self):
        self._prepare_chaos_nodes(self._cluster_configuration["chaos_node"], force_overwrite=True)

    def rewrite_scheduler_configs(self):
        self._prepare_schedulers(self._cluster_configuration["scheduler"], force_overwrite=True)

    def rewrite_controller_agent_configs(self):
        self._prepare_controller_agents(self._cluster_configuration["controller_agent"], force_overwrite=True)

    def rewrite_timestamp_provider_configs(self):
        self._prepare_timestamp_providers(self._cluster_configuration["timestamp_provider"], force_overwrite=True)

    def get_node_address(self, index, with_port=True, config_service="node"):
        node_config = self.configs[config_service][index]
        node_address = node_config["address_resolver"]["localhost_fqdn"]
        if with_port:
            node_address = "{}:{}".format(node_address, node_config["rpc_port"])
        return node_address

    def get_cluster_configuration(self):
        return self._cluster_configuration

    def get_open_port_iterator(self):
        return self._open_port_iterator

    # TODO(max42): remove this method and rename all its usages to get_http_proxy_address.
    def get_proxy_address(self):
        return self.get_http_proxy_address()

    def get_http_proxy_address(self, tvm_only=False, https=False, chyt=False):
        return self.get_http_proxy_addresses(tvm_only=tvm_only, https=https, chyt=chyt)[0]

    def get_http_proxy_url(self):
        return "http://" + self.get_http_proxy_address()

    def get_https_proxy_url(self):
        return "https://" + self.get_http_proxy_address(https=True)

    def get_http_proxy_addresses(self, tvm_only=False, https=False, chyt=False):
        if self.yt_config.http_proxy_count == 0:
            raise YtError("Http proxies are not started")
        if tvm_only and https:
            raise YtError("Request of HTTPS and TVM proxies are not supported simultaneously")
        if tvm_only and chyt:
            raise YtError("Request of CHYT and TVM proxies are not supported simultaneously")
        if https and not self.yt_config.enable_tls:
            raise YtError("Https proxies are not enabled")

        if tvm_only:
            port_path = "tvm_only_http_server/port"
        else:
            if chyt:
                if https:
                    port_path = "chyt_https_server/port"
                else:
                    port_path = "chyt_http_server/port"
            else:
                if https:
                    port_path = "https_server/port"
                else:
                    port_path = "port"

        return ["{0}:{1}".format(self.yt_config.fqdn, get_value_from_config(config, port_path, "http_proxy"))
                for config in self.configs["http_proxy"]]

    def get_kafka_proxy_address(self):
        return self.get_kafka_proxy_addresses()[0]

    def get_kafka_proxy_addresses(self):
        if self.yt_config.kafka_proxy_count == 0:
            raise YtError("Kafka proxies are not started")
        return ["{0}:{1}".format(self.yt_config.fqdn, get_value_from_config(config, "port", "kafkaproxy"))
                for config in self.configs["kafka_proxy"]]

    def get_rpc_proxy_address(self, tvm_only=False):
        return self.get_rpc_proxy_addresses(tvm_only=tvm_only)[0]

    def get_rpc_proxy_addresses(self, tvm_only=False):
        if self.yt_config.rpc_proxy_count == 0:
            raise YtError("Rpc proxies are not started")
        if tvm_only:
            return ["{0}:{1}".format(self.yt_config.fqdn, get_value_from_config(config, "tvm_only_rpc_port", "rpc_proxy"))
                    for config in self.configs["rpc_proxy"]]
        return ["{0}:{1}".format(self.yt_config.fqdn, get_value_from_config(config, "rpc_port", "rpcproxy"))
                for config in self.configs["rpc_proxy"]]

    def get_rpc_proxy_monitoring_addresses(self):
        if self.yt_config.rpc_proxy_count == 0:
            raise YtError("Rpc proxies are not started")
        return ["{0}:{1}".format(self.yt_config.fqdn, get_value_from_config(config, "monitoring_port", "rpcproxy"))
                for config in self.configs["rpc_proxy"]]

    def get_grpc_proxy_address(self):
        if self.yt_config.rpc_proxy_count == 0:
            raise YtError("Rpc proxies are not started")
        addresses = get_value_from_config(self.configs["rpc_proxy"][0], "grpc_server/addresses", "rpc_proxy")
        return addresses[0]["address"]

    def get_timestamp_provider_monitoring_addresses(self):
        if self.yt_config.timestamp_provider_count == 0:
            raise YtError("Timestamp providers are not started")
        return ["{0}:{1}"
                .format(
                    self.yt_config.fqdn,
                    get_value_from_config(config, "monitoring_port", "timestamp_provider"))
                for config in self.configs["timestamp_provider"]]

    def get_clock_monitoring_addresses(self):
        if self.yt_config.clock_count == 0:
            raise YtError("Clocks are not started")
        return ["{0}:{1}"
                .format(
                    self.yt_config.fqdn,
                    get_value_from_config(config, "monitoring_port", "clock"))
                for config in self.configs["clock"]]

    def get_master_cache_monitoring_addresses(self):
        if self.yt_config.master_cache_count == 0:
            raise YtError("Master caches are not started")
        return ["{0}:{1}"
                .format(self.yt_config.fqdn,
                        get_value_from_config(config, "monitoring_port", "master_cache"))
                for config in self.configs["master_cache"]]

    def get_cell_balancer_monitoring_addresses(self):
        if self.yt_config.cell_balancer_count == 0:
            raise YtError("Cell balancers are not started")
        return ["{0}:{1}"
                .format(
                    self.yt_config.fqdn,
                    get_value_from_config(config, "monitoring_port", "cell_balancer"))
                for config in self.configs["cell_balancer"]]

    def kill_schedulers(self, indexes=None):
        self.kill_service("scheduler", indexes=indexes)

    def kill_controller_agents(self, indexes=None):
        self.kill_service("controller_agent", indexes=indexes)

    def _abort_node_transactions_and_wait(self, addresses, wait_offline):
        client = self._create_cluster_client()
        for node in client.list("//sys/cluster_nodes", attributes=["lease_transaction_id"]):
            if str(node) not in addresses:
                continue
            if "lease_transaction_id" in node.attributes:
                client.abort_transaction(node.attributes["lease_transaction_id"])

        if wait_offline:
            wait(
                lambda:
                all([
                    node.attributes["state"] == "offline"
                    for node in client.list("//sys/cluster_nodes", attributes=["state"])
                    if str(node) in addresses
                ])
            )

    def kill_nodes(self, indexes=None, wait_offline=True):
        self.kill_service("node", indexes=indexes)

        addresses = None
        if indexes is None:
            indexes = list(xrange(self.yt_config.node_count))
        addresses = [self.get_node_address(index) for index in indexes]

        self._abort_node_transactions_and_wait(addresses, wait_offline)

    def kill_chaos_nodes(self, indexes=None, wait_offline=True):
        self.kill_service("chaos_node", indexes=indexes)

        addresses = None
        if indexes is None:
            indexes = list(xrange(self.yt_config.chaos_node_count))
        addresses = [self.get_node_address(index, config_service="chaos_node") for index in indexes]

        self._abort_node_transactions_and_wait(addresses, wait_offline)

    def kill_http_proxies(self, indexes=None):
        self.kill_service("proxy", indexes=indexes)

    def kill_rpc_proxies(self, indexes=None):
        self.kill_service("rpc_proxy", indexes=indexes)

    def kill_masters_at_cells(self, indexes=None, cell_indexes=None):
        if cell_indexes is None:
            cell_indexes = [0]
        for cell_index in cell_indexes:
            name = self._get_master_name("master", cell_index)
            self.kill_service(name, indexes=indexes)

    def kill_all_masters(self):
        self.kill_masters_at_cells(indexes=None, cell_indexes=xrange(self.yt_config.secondary_cell_count + 1))

    def kill_master_caches(self, indexes=None):
        self.kill_service("master_cache", indexes=indexes)

    def kill_queue_agents(self, indexes=None):
        self.kill_service("queue_agent", indexes=indexes)

    def kill_kafka_proxies(self, indexes=None):
        self.kill_service("kafka_proxy", indexes=indexes)

    def kill_tablet_balancers(self, indexes=None):
        self.kill_service("tablet_balancer", indexes=indexes)

    def kill_cypress_proxies(self, indexes=None):
        self.kill_service("cypress_proxies", indexes=indexes)

    def kill_replicated_table_trackers(self, indexes=None):
        self.kill_service("replicated_table_tracker", indexes=indexes)

    def kill_service(self, name, indexes=None):
        with self._lock:
            logger.info("Killing %s", name)
            processes = self._service_processes[name]
            for index, process in enumerate(processes):
                if process is None or (indexes is not None and index not in indexes):
                    continue
                self._kill_process(process, name)
                if isinstance(process, PortoSubprocess):
                    process.destroy()
                del self._pid_to_process[process.pid]
                processes[index] = None

    def get_service_pids(self, name, indexes=None):
        result = []
        with self._lock:
            logger.info("Obtaining %s pids", name)
            processes = self._service_processes[name]
            for index, process in enumerate(processes):
                if process is None or (indexes is not None and index not in indexes):
                    continue
                result.append(process.pid)
        return result

    def get_node_container(self, index):
        with self._lock:
            processes = self._service_processes["node"]
            process = processes[index]
            if not isinstance(process, PortoSubprocess):
                raise YtError("Node is not running in container")

            return process._container

    def list_node_subcontainers(self, index):
        with self._lock:
            processes = self._service_processes["node"]
            process = processes[index]
            if not isinstance(process, PortoSubprocess):
                raise YtError("Cannot list subcontainers for non-porto environment")

            return process.list_subcontainers()

    def set_nodes_cpu_limit(self, cpu_limit):
        with self._lock:
            logger.info("Setting cpu limit {0} for nodes".format(cpu_limit))
            processes = self._service_processes["node"]
            for process in processes:
                if not isinstance(process, PortoSubprocess):
                    raise YtError("Cpu limits are not supported for non-porto environment")
                process.set_cpu_limit(cpu_limit)

    def set_nodes_memory_limit(self, memory_limit):
        with self._lock:
            logger.info("Setting memory limit {0} for nodes".format(memory_limit))
            processes = self._service_processes["node"]
            for process in processes:
                if not isinstance(process, PortoSubprocess):
                    raise YtError("Memory limits are not supported for non-porto environment")
                process.set_memory_limit(memory_limit)

    def check_liveness(self, callback_func):
        with self._lock:
            for info in itervalues(self._pid_to_process):
                proc, args = info
                proc.poll()
                if proc.returncode is not None:
                    callback_func(self, proc, args)
                    break

    def get_component_version(self, component):
        """
        Return structure identifying component version.
        Returned object has fields "abi" and "literal", e.g. (20, 3) and "20.3.1234-...".

        :param component: should be binary name, e.g. "ytserver-master", "ytserver-node", etc.
        :type component str
        :return: component version.
        :rtype BinaryVersion
        """
        if component not in self._binary_to_version:
            raise YtError("Component {} not found; available components are {}".format(
                component, self._binary_to_version.keys()))
        return self._binary_to_version[component]

    def _configure_driver_logging(self):
        try:
            import yt_driver_bindings
            yt_driver_bindings.configure_logging(
                _environment_driver_logging_config or self.configs["driver_logging"]
            )
        except ImportError:
            pass

        import yt.wrapper.native_driver as native_driver
        native_driver.logging_configured = True

    def _configure_yt_tracing(self):
        try:
            import yt_tracing
        except ImportError:
            return

        if "JAEGER_COLLECTOR" in os.environ:
            yt_tracing.initialize_tracer({
                "service_name": "python",
                "flush_period": 100,
                "collector_channel_config": {"address": os.environ["JAEGER_COLLECTOR"]},
                "enable_pid_tag": True,
            })

    def _configure_yp_service_discovery(self):
        try:
            import yt_driver_bindings
            yt_driver_bindings.configure_yp_service_discovery({"enable": False})
        except ImportError:
            pass

        import yt.wrapper.native_driver as native_driver
        native_driver.yp_service_discovery_configured = True

    def _write_environment_info_to_file(self):
        info = {}
        if self.yt_config.http_proxy_count > 0:
            info["http_proxies"] = [{"address": address} for address in self.get_http_proxy_addresses()]
            if len(self.get_http_proxy_addresses()) == 1:
                info["proxy"] = {"address": self.get_http_proxy_addresses()[0]}
        with open(os.path.join(self.path, "info.yson"), "wb") as fout:
            yson.dump(info, fout, yson_format="pretty")

    def _kill_process(self, proc, name):
        def print_proc_info(pid):
            try:
                tasks_path = "/proc/{0}/task".format(pid)
                for task_id in os.listdir(tasks_path):
                    for name in ["status", "sched", "wchan"]:
                        try:
                            with open(os.path.join(tasks_path, task_id, name), "r") as fin:
                                logger.error("Process %s task %s %s: %s",
                                             pid, task_id, name, fin.read().replace("\n", "\\n"))
                        except Exception as e:
                            logger.warning("Failed to print process %s task %s %s: %s", pid, task_id, name, e)
            except Exception as e:
                logger.warning("Failed to print process %s info: %s", pid, e)

        proc.poll()
        if proc.returncode is not None:
            if self._started:
                logger.warning("{0} (pid: {1}, working directory: {2}) is already terminated with exit code {3}".format(
                               name, proc.pid, os.path.join(self.path, name), proc.returncode))
                self._process_stderrs(name)
            return

        def safe_kill(kill):
            try:
                kill()
            except OSError as e:
                if e.errno != errno.ESRCH:
                    raise

        logger.info("Sending SIGTERM (pid: {}, current_process_pid: {})".format(proc.pid, os.getpid()))
        safe_kill(lambda: os.kill(proc.pid, signal.SIGTERM))

        # leave 5s for process to finish writing coverage profile.
        for i in range(50):
            if proc.poll() is not None:
                break
            time.sleep(0.1)
        else:
            logger.info("Sending SIGKILL (pid: {}, current_process_pid: {})".format(proc.pid, os.getpid()))
            safe_kill(lambda: os.kill(proc.pid, signal.SIGKILL))

        safe_kill(lambda: os.killpg(proc.pid, signal.SIGKILL))

        for i in range(100):
            verbose = i > 50 and (i + 1) % 10 == 0
            if is_dead(proc.pid, verbose=verbose):
                break
            if verbose:
                print_proc_info(proc.pid)
            time.sleep(0.3)
        else:
            raise WaitFailed("Wait failed")

    def _append_pid(self, pid):
        self.pids_file.write(str(pid) + "\n")
        self.pids_file.flush()

    def _process_stderrs(self, name, number=None):
        has_some_bind_failure = False

        def process_stderr(path, num=None):
            number_suffix = ""
            if num is not None:
                number_suffix = "-" + str(num)

            stderr = open(path).read()
            if stderr:
                sys.stderr.write("{0}{1} stderr:\n{2}"
                                 .format(name.capitalize(), number_suffix, stderr))
                if "Address already in use" in stderr:
                    return True
            return False

        if number is not None:
            has_bind_failure = process_stderr(self._stderr_paths[name][number], number)
            has_some_bind_failure = has_some_bind_failure or has_bind_failure
        else:
            for i, stderr_path in enumerate(self._stderr_paths[name]):
                has_bind_failure = process_stderr(stderr_path, i)
                has_some_bind_failure = has_some_bind_failure or has_bind_failure

        if has_some_bind_failure:
            raise YtEnvRetriableError("Process failed to bind on some of ports")

    def _run(self, args, name, env=None, number=None):
        with self._lock:
            index = number if number is not None else 0
            name_with_number = name
            if number is not None:
                name_with_number = "{0}-{1}".format(name, number)

            if self._service_processes[name][index] is not None:
                logger.debug("Process %s is already running (pid: %s)", name_with_number, self._service_processes[name][index].pid)
                return

            stderr_path = os.path.join(self.stderrs_path, "stderr.{0}".format(name_with_number))
            self._stderr_paths[name].append(stderr_path)

            if self._kill_child_processes:
                args += ["--pdeathsig", str(int(signal.SIGTERM))]
            args += ["--setsid"]

            if env is None:
                env = copy.copy(os.environ)
                if "YT_LOG_LEVEL" in env:
                    del env["YT_LOG_LEVEL"]
            env = update(env, {"YT_ALLOC_CONFIG": "{enable_eager_memory_release=%true}"})

            with open(os.devnull, "w") as stdout, open(stderr_path, "w") as stderr:
                p = self._subprocess_module.Popen(args, shell=False, close_fds=True, cwd=self.runtime_data_path,
                                                  env=env, stdout=stdout, stderr=stderr, start_new_session=True)

            self._validate_process_is_running(p, name, number)

            self._service_processes[name][index] = p
            self._pid_to_process[p.pid] = (p, args)
            self._append_pid(p.pid)

            tags = []
            if self.yt_config.use_porto_for_servers:
                tags.append("name: {}".format(p._portoName))
            tags.append("pid: {}".format(p.pid))
            tags.append("current_process_pid: {}".format(os.getpid()))

            logger.info("Process %s started (%s)", name, ", ".join(tags))

            return p

    def run_yt_component(self, component, config_paths, name=None, config_option=None, custom_paths=None):
        if config_option is None:
            config_option = "--config"
        if name is None:
            name = component
        if custom_paths is None:
            custom_paths = []
        if self.custom_paths is not None:
            custom_paths += self.custom_paths

        logger.info("Starting %s", name)

        pids = []
        for index in xrange(len(config_paths)):
            with push_front_env_path(self.bin_path):
                binary_path = _get_yt_binary_path("ytserver-" + component, custom_paths=custom_paths)
                if binary_path is None:
                    logger.error("Could not start component {}, path env = {}, custom_paths = {}".format(
                        component, os.environ.get("PATH"), custom_paths))
                    raise YtError("Could not start component '{}', make sure it is available in PATH".format(component),
                                  attributes={"path_env": os.environ.get("PATH").split(":")})
                args = [binary_path]
            if self._kill_child_processes:
                args.extend(["--pdeathsig", str(int(signal.SIGKILL))])
            args.extend([config_option, config_paths[index]])

            number = None if len(config_paths) == 1 else index
            with push_front_env_path(self.bin_path):
                run_result = self._run(args, name, number=number)
                if run_result is not None:
                    pids.append(run_result.pid)
        return pids

    def _run_builtin_yt_component(self, component, name=None, config_option=None, custom_paths=None):
        if name is None:
            name = component
        self.run_yt_component(component, self.config_paths[name], name=name, config_option=config_option, custom_paths=custom_paths)

    def _get_master_name(self, master_name, cell_index):
        if cell_index == 0:
            return master_name
        else:
            return master_name + "_secondary_" + str(cell_index - 1)

    def _prepare_masters(self, master_configs, force_overwrite=False):
        for cell_index in xrange(self.yt_config.secondary_cell_count + 1):
            master_name = self._get_master_name("master", cell_index)
            if cell_index == 0:
                cell_tag = master_configs["primary_cell_tag"]
            else:
                cell_tag = master_configs["secondary_cell_tags"][cell_index - 1]

            if force_overwrite:
                self.configs[master_name] = []
                self.config_paths[master_name] = []
                self._service_processes[master_name] = []

            for master_index in xrange(self.yt_config.master_count):
                master_config_name = "master-{0}-{1}.yson".format(cell_index, master_index)
                config_path = os.path.join(self.configs_path, master_config_name)
                if self._load_existing_environment and not force_overwrite:
                    if not os.path.isfile(config_path):
                        raise YtError("Master config {0} not found. It is possible that you requested "
                                      "more masters than configs exist".format(config_path))
                    config = read_config(config_path)
                else:
                    config = master_configs[cell_tag][master_index]
                    write_config(config, config_path)

                self.configs[master_name].append(config)
                self.config_paths[master_name].append(config_path)
                self._service_processes[master_name].append(None)

    def start_master_cell(self, cell_index=0, sync=True, set_config=True):
        master_name = self._get_master_name("master", cell_index)
        secondary = cell_index > 0

        self._run_builtin_yt_component("master", name=master_name)

        def quorum_ready():
            self._validate_processes_are_running(master_name)

            logger = logging.getLogger("Yt")
            old_level = logger.level
            logger.setLevel(logging.ERROR)
            try:
                # XXX(asaitgalin): Once we're able to execute set("//sys/@config") then quorum is ready
                # and the world is initialized. Secondary masters can only be checked with native
                # driver so it is requirement to have driver bindings if secondary cells are started.
                client = None
                if not secondary:
                    client = self._create_cluster_client()
                else:
                    client = self.create_native_client(master_name.replace("master", "driver"))
                client.config["proxy"]["retries"]["enable"] = False

                # `suppress_transaction_coordinator_sync` and `suppress_upstream_sync`
                # are set True due to possibly enabled read-only mode.
                if set_config:
                    client.set("//sys/@config", get_patched_dynamic_master_config(get_dynamic_master_config()),
                               suppress_transaction_coordinator_sync=True,
                               suppress_upstream_sync=True)
                else:
                    client.get("//sys/@config",
                               suppress_transaction_coordinator_sync=True,
                               suppress_upstream_sync=True)

                return True
            except (requests.RequestException, YtError) as err:
                return False, err
            finally:
                logger.setLevel(old_level)

        cell_ready = quorum_ready

        if secondary:
            primary_cell_client = self.create_native_client()
            cell_tag = int(
                self._cluster_configuration["master"]["secondary_cell_tags"][cell_index - 1])

            def quorum_ready_and_cell_registered():
                result = quorum_ready()
                if isinstance(result, tuple) and not result[0]:
                    return result

                return cell_tag in primary_cell_client.get(
                    "//sys/@registered_master_cell_tags",
                    suppress_transaction_coordinator_sync=True)

            cell_ready = quorum_ready_and_cell_registered

        self._wait_or_skip(lambda: self._wait_for(cell_ready, master_name, max_wait_time=30), sync)

    def start_all_masters(self, start_secondary_master_cells, sync=True, set_config=True):
        self.start_master_cell(sync=sync, set_config=set_config)

        if start_secondary_master_cells:
            self.start_secondary_master_cells(sync=sync, set_config=set_config)

    def start_secondary_master_cells(self, sync=True, set_config=True):
        for i in xrange(self.yt_config.secondary_cell_count):
            self.start_master_cell(i + 1, sync=sync, set_config=set_config)

    def _prepare_clocks(self, clock_configs):
        for clock_index in xrange(self.yt_config.clock_count):
            clock_config_name = "clock-{0}.yson".format(clock_index)
            config_path = os.path.join(self.configs_path, clock_config_name)
            if self._load_existing_environment:
                if not os.path.isfile(config_path):
                    raise YtError("Clock config {0} not found. It is possible that you requested "
                                  "more clocks than configs exist".format(config_path))
                config = read_config(config_path)
            else:
                config = clock_configs[clock_configs["cell_tag"]][clock_index]
                write_config(config, config_path)

            self.configs["clock"].append(config)
            self.config_paths["clock"].append(config_path)
            self._service_processes["clock"].append(None)

    def start_clock(self, sync=True):
        self._run_builtin_yt_component("clock")

        def quorum_ready():
            self._validate_processes_are_running("clock")

            clock_statuses_logger = logging.getLogger()
            addresses = self.get_clock_monitoring_addresses()
            clock_statuses_logger.debug(f"Clocks addresses: {addresses}")
            try:
                for address in addresses:
                    resp = requests.get(f"http://{address}/orchid/clock_cell/hydra/active")
                    resp.raise_for_status()
                    clock_statuses_logger.debug(resp.text)
                    return resp.text == "true"
            except (requests.exceptions.RequestException, socket.error):
                return False, traceback.format_exc()

        self._wait_or_skip(lambda: self._wait_for(quorum_ready, "clock", max_wait_time=30), sync)

    def _prepare_discovery_servers(self, discovery_server_configs):
        for discovery_server_index in xrange(self.yt_config.discovery_server_count):
            discovery_server_config_name = "discovery-{0}.yson".format(discovery_server_index)
            config_path = os.path.join(self.configs_path, discovery_server_config_name)
            if self._load_existing_environment:
                if not os.path.isfile(config_path):
                    raise YtError("Discovery server config {0} not found. It is possible that you requested "
                                  "more discovery servers than configs exist".format(config_path))
                config = read_config(config_path)
            else:
                config = discovery_server_configs[discovery_server_index]
                write_config(config, config_path)

            self.configs["discovery"].append(config)
            self.config_paths["discovery"].append(config_path)
            self._service_processes["discovery"].append(None)

    def start_discovery_server(self, sync=True):
        self._run_builtin_yt_component("discovery")

    def _prepare_queue_agents(self, queue_agent_configs):
        for queue_agent_index in xrange(self.yt_config.queue_agent_count):
            queue_agent_config_name = "queue_agent-{0}.yson".format(queue_agent_index)
            config_path = os.path.join(self.configs_path, queue_agent_config_name)
            if self._load_existing_environment:
                if not os.path.isfile(config_path):
                    raise YtError("Queue agent config {0} not found. It is possible that you requested "
                                  "more queue agents than configs exist".format(config_path))
                config = read_config(config_path)
            else:
                config = queue_agent_configs[queue_agent_index]
                write_config(config, config_path)

            self.configs["queue_agent"].append(config)
            self.config_paths["queue_agent"].append(config_path)
            self._service_processes["queue_agent"].append(None)

    def start_queue_agents(self, sync=True):
        self._run_builtin_yt_component("queue-agent", name="queue_agent")

        client = self._create_cluster_client()

        def queue_agents_ready():
            self._validate_processes_are_running("queue_agent")

            try:
                if not client.exists("//sys/queue_agents/instances"):
                    return False
                instances = client.list("//sys/queue_agents/instances")
                if len(instances) != self.yt_config.queue_agent_count:
                    return False
                for instance in instances:
                    if not client.exists("//sys/queue_agents/instances/" + instance + "/orchid/queue_agent"):
                        return False
            except YtError:
                logger.exception("Error while waiting for queue agents")
                return False

            return True

        self._wait_or_skip(
            lambda: self._wait_for(queue_agents_ready, "queue_agent", max_wait_time=20),
            sync)

    def _prepare_timestamp_providers(self, timestamp_provider_configs, force_overwrite=False):
        if force_overwrite:
            self.configs["timestamp_provider"] = []
            self.config_paths["timestamp_provider"] = []
            self._service_processes["timestamp_provider"] = []

        for timestamp_provider_index in xrange(self.yt_config.timestamp_provider_count):
            timestamp_provider_config_name = "timestamp_provider-{0}.yson".format(timestamp_provider_index)
            config_path = os.path.join(self.configs_path, timestamp_provider_config_name)
            if self._load_existing_environment and not force_overwrite:
                if not os.path.isfile(config_path):
                    raise YtError("Timestamp provider config {0} not found. It is possible that you requested "
                                  "more timestamp providers than configs exist".format(config_path))
                config = read_config(config_path)
            else:
                config = timestamp_provider_configs[timestamp_provider_index]
                write_config(config, config_path)

            self.configs["timestamp_provider"].append(config)
            self.config_paths["timestamp_provider"].append(config_path)
            self._service_processes["timestamp_provider"].append(None)

    def start_timestamp_providers(self, sync=True):
        self._run_builtin_yt_component("timestamp-provider", name="timestamp_provider")

        def timestamp_providers_ready():
            self._validate_processes_are_running("timestamp_provider")

            addresses = self.get_timestamp_provider_monitoring_addresses()
            try:
                for address in addresses:
                    resp = requests.get("http://{0}/orchid".format(address))
                    resp.raise_for_status()
            except (requests.exceptions.RequestException, socket.error):
                return False, traceback.format_exc()

            return True

        self._wait_or_skip(
            lambda: self._wait_for(timestamp_providers_ready, "timestamp_provider", max_wait_time=20),
            sync)

    def _prepare_cell_balancers(self, cell_balancer_configs):
        for cell_balancer_index in xrange(self.yt_config.cell_balancer_count):
            cell_balancer_config_name = "cell_balancer-{0}.yson".format(cell_balancer_index)
            config_path = os.path.join(self.configs_path, cell_balancer_config_name)
            if self._load_existing_environment:
                if not os.path.isfile(config_path):
                    raise YtError("Cell balancer config {0} not found. It is possible that you requested "
                                  "more cell balancers than configs exist".format(config_path))
                config = read_config(config_path)
            else:
                config = cell_balancer_configs[cell_balancer_index]
                write_config(config, config_path)

            self.configs["cell_balancer"].append(config)
            self.config_paths["cell_balancer"].append(config_path)
            self._service_processes["cell_balancer"].append(None)

    def start_cell_balancers(self, sync=True):
        self._run_builtin_yt_component("cell-balancer", name="cell_balancer")

        client = self._create_cluster_client()

        def cell_balancers_ready():
            self._validate_processes_are_running("cell_balancer")

            instances = client.list("//sys/cell_balancers/instances")
            if len(instances) != self.yt_config.cell_balancer_count:
                return False
            try:
                active_cell_balancer_orchid_path = None
                for instance in instances:
                    # bundle_controller uses this path
                    orchid_path = f"//sys/cell_balancers/instances/{instance}/orchid"
                    try:
                        res = client.get(orchid_path + "/cell_balancer/service/connected")
                        if res:
                            active_cell_balancer_orchid_path = orchid_path
                    except YtResponseError as error:
                        if not error.is_resolve_error():
                            raise

                if active_cell_balancer_orchid_path is None:
                    return False, "No active cell_balancer found"

                if self.yt_config.enable_bundle_controller:
                    if not client.exists("//sys/bundle_controller/orchid"):
                        return False, "Bundle controller did not elect leader"

            except YtResponseError as err:
                # Orchid connection refused
                if not err.contains_code(105) and not err.contains_code(100):
                    raise
                return False, err
            return True

        self._wait_or_skip(
            lambda: self._wait_for(cell_balancers_ready, "cell_balancer", max_wait_time=20),
            sync)

    def _list_nodes(self, pick_chaos=False):
        client = self._create_cluster_client()
        nodes = client.list("//sys/cluster_nodes", attributes=["state", "flavors"])

        # COMPAT(savrus): drop default flavors list when 20.3 is deprecated
        nodes = [n for n in nodes if ("chaos" in n.attributes.get("flavors", [])) == pick_chaos]
        return nodes

    def _prepare_nodes(self, node_configs, force_overwrite=False):
        if force_overwrite:
            self.configs["node"] = []
            self.config_paths["node"] = []
            self._service_processes["node"] = []

        for node_index in xrange(self.yt_config.node_count):
            node_config_name = "node-" + str(node_index) + ".yson"
            config_path = os.path.join(self.configs_path, node_config_name)
            if self._load_existing_environment and not force_overwrite:
                if not os.path.isfile(config_path):
                    raise YtError("Node config {0} not found. It is possible that you requested "
                                  "more nodes than configs exist".format(config_path))
                config = read_config(config_path)
            else:
                config = node_configs[node_index]
                write_config(config, config_path)

            self.configs["node"].append(config)
            self.config_paths["node"].append(config_path)
            self._service_processes["node"].append(None)

    def start_nodes(self, sync=True):
        self._run_builtin_yt_component("node")

        def nodes_ready():
            self._validate_processes_are_running("node")

            nodes = self._list_nodes(pick_chaos=False)
            # "mixed" is for the first start, "online" is for potential later restarts.
            target_states = ("mixed", "online") if self.yt_config.defer_secondary_cell_start else ("online")
            return len(nodes) == self.yt_config.node_count and \
                all(node.attributes["state"] in target_states for node in nodes)

        self._wait_or_skip(
            lambda:
                self._wait_for(nodes_ready, "node", max_wait_time=max(self.yt_config.node_count * 6.0, 60)),
            sync)

    def _prepare_chaos_nodes(self, chaos_node_configs, force_overwrite=False):
        if force_overwrite:
            self.configs["chaos_node"] = []
            self.config_paths["chaos_node"] = []
            self._service_processes["chaos_node"] = []

        for chaos_node_index in xrange(self.yt_config.chaos_node_count):
            chaos_node_config_name = "chaos-node-" + str(chaos_node_index) + ".yson"
            config_path = os.path.join(self.configs_path, chaos_node_config_name)
            if self._load_existing_environment and not force_overwrite:
                if not os.path.isfile(config_path):
                    raise YtError("Chaos node config {0} not found. It is possible that you requested "
                                  "more chaos nodes than configs exist".format(config_path))
                config = read_config(config_path)
            else:
                config = chaos_node_configs[chaos_node_index]
                write_config(config, config_path)

            self.configs["chaos_node"].append(config)
            self.config_paths["chaos_node"].append(config_path)
            self._service_processes["chaos_node"].append(None)

    def start_chaos_nodes(self, sync=True):
        self._run_builtin_yt_component("node", name="chaos_node")

        def chaos_nodes_ready():
            self._validate_processes_are_running("node")

            nodes = self._list_nodes(pick_chaos=True)
            return len(nodes) == self.yt_config.chaos_node_count and all(node.attributes["state"] == "online" for node in nodes)

        wait_function = lambda: self._wait_for(chaos_nodes_ready, "chaos_node", max_wait_time=max(self.yt_config.chaos_node_count * 6.0, 60))  # noqa
        self._wait_or_skip(wait_function, sync)

    def _prepare_master_caches(self, master_cache_configs):
        for master_cache_index in xrange(self.yt_config.master_cache_count):
            master_cache_config_name = "master_cache-{0}.yson".format(master_cache_index)
            config_path = os.path.join(self.configs_path, master_cache_config_name)
            if self._load_existing_environment:
                if not os.path.isfile(config_path):
                    raise YtError("Master cache config {0} not found. It is possible that you requested "
                                  "more timestamp providers than configs exist".format(config_path))
                config = read_config(config_path)
            else:
                config = master_cache_configs[master_cache_index]
                write_config(config, config_path)

            self.configs["master_cache"].append(config)
            self.config_paths["master_cache"].append(config_path)
            self._service_processes["master_cache"].append(None)

    def start_master_caches(self, sync=True):
        self._run_builtin_yt_component("master-cache", name="master_cache")

        def master_caches_ready():
            self._validate_processes_are_running("master_cache")

            addresses = self.get_master_cache_monitoring_addresses()
            try:
                for address in addresses:
                    resp = requests.get("http://{0}/orchid".format(address))
                    resp.raise_for_status()
            except (requests.exceptions.RequestException, socket.error):
                return False, traceback.format_exc()

            return True

        self._wait_or_skip(lambda: self._wait_for(master_caches_ready, "master_cache", max_wait_time=20), sync)

    def _prepare_schedulers(self, scheduler_configs, force_overwrite=False):
        if force_overwrite:
            self.configs["scheduler"] = []
            self.config_paths["scheduler"] = []
            self._service_processes["scheduler"] = []

        for scheduler_index in xrange(self.yt_config.scheduler_count):
            scheduler_config_name = "scheduler-" + str(scheduler_index) + ".yson"
            config_path = os.path.join(self.configs_path, scheduler_config_name)
            if self._load_existing_environment and not force_overwrite:
                if not os.path.isfile(config_path):
                    raise YtError("Scheduler config {0} not found. It is possible that you requested "
                                  "more schedulers than configs exist".format(config_path))
                config = read_config(config_path)
            else:
                config = scheduler_configs[scheduler_index]
                write_config(config, config_path)

            self.configs["scheduler"].append(config)
            self.config_paths["scheduler"].append(config_path)
            self._service_processes["scheduler"].append(None)

    def _prepare_controller_agents(self, controller_agent_configs, force_overwrite=False):
        if force_overwrite:
            self.configs["controller_agent"] = []
            self.config_paths["controller_agent"] = []
            self._service_processes["controller_agent"] = []

        for controller_agent_index in xrange(self.yt_config.controller_agent_count):
            controller_agent_config_name = "controller_agent-" + str(controller_agent_index) + ".yson"
            config_path = os.path.join(self.configs_path, controller_agent_config_name)
            if self._load_existing_environment and not force_overwrite:
                if not os.path.isfile(config_path):
                    raise YtError("Controller agent config {0} not found. It is possible that you requested "
                                  "more controller agents than configs exist".format(config_path))
                config = read_config(config_path)
            else:
                config = controller_agent_configs[controller_agent_index]
                write_config(config, config_path)

            self.configs["controller_agent"].append(config)
            self.config_paths["controller_agent"].append(config_path)
            self._service_processes["controller_agent"].append(None)

    def start_schedulers(self, sync=True, custom_paths=None):
        self._remove_scheduler_lock()

        client = self._create_cluster_client()

        # COMPAT(max42): drop this when 20.2 is deprecated.
        is_pool_tree_config_present = self.get_component_version("ytserver-master").abi >= (20, 3)
        if client.get("//sys/pool_trees/@type") == "map_node":
            if is_pool_tree_config_present:
                client.create("map_node", "//sys/pool_trees/default", attributes={"config": {}},
                              ignore_existing=True, recursive=True)
            else:
                client.create("map_node", "//sys/pool_trees/default", ignore_existing=True, recursive=True)
        else:
            client.create("scheduler_pool_tree", attributes={"name": "default"}, ignore_existing=True)
        client.set("//sys/pool_trees/@default_tree", "default")

        if is_pool_tree_config_present:
            client.set("//sys/pool_trees/default/@config/max_ephemeral_pools_per_user", 5)
        else:
            client.set("//sys/pool_trees/default/@max_ephemeral_pools_per_user", 5)

        if not client.exists("//sys/pools"):
            client.link("//sys/pool_trees/default", "//sys/pools", ignore_existing=True)

        self._run_builtin_yt_component("scheduler", custom_paths=custom_paths)

        def schedulers_ready():
            def check_node_state(node):
                if "state" in node:
                    return node["state"] == "online"
                return node["scheduler_state"] == "online" and node["master_state"] == "online"

            self._validate_processes_are_running("scheduler")

            instances = client.list("//sys/scheduler/instances")
            if len(instances) != self.yt_config.scheduler_count:
                return False

            try:
                active_scheduler_orchid_path = None
                for instance in instances:
                    orchid_path = "//sys/scheduler/instances/{0}/orchid".format(instance)
                    try:
                        client.set(orchid_path + "/@retry_backoff_time", 100)
                        if client.get(orchid_path + "/scheduler/service/connected"):
                            active_scheduler_orchid_path = orchid_path
                    except YtResponseError as error:
                        if not error.is_resolve_error():
                            raise

                if active_scheduler_orchid_path is None:
                    return False, "No active scheduler found"

                try:
                    # Don't run the check below if the config is not exposed in the scheduler's Orchid
                    # (e.g. due to expose_config_in_orchid = %false in scheduler's config).
                    orchid_keys = client.list(active_scheduler_orchid_path)
                    if "config" in orchid_keys:
                        master_cell_id = client.get("//sys/@cell_id")
                        scheduler_cell_id = client.get(
                            active_scheduler_orchid_path + "/config/cluster_connection/primary_master/cell_id")
                        if master_cell_id != scheduler_cell_id:
                            return False, "Incorrect scheduler connected, its cell_id {0} does not match master cell {1}"\
                                          .format(scheduler_cell_id, master_cell_id)
                except YtResponseError as error:
                    if error.is_resolve_error():
                        return False, "Failed to request primary master cell id from master and scheduler" + str(error)
                    else:
                        raise

                nodes = client.list("//sys/cluster_nodes", attributes=["flavors"])

                def check_node(node):
                    # COMPAT(gritukan)
                    if "flavors" not in node.attributes:
                        return True
                    return "exec" in node.attributes["flavors"]

                exec_nodes = list(filter(check_node, nodes))

                if not self.yt_config.defer_node_start:
                    nodes = list(itervalues(client.get(active_scheduler_orchid_path + "/scheduler/nodes")))
                    return len(nodes) == len(exec_nodes) and all(check_node_state(node) for node in nodes)

                return True

            except YtResponseError as err:
                # Orchid connection refused
                if not err.contains_code(105) and not err.contains_code(100):
                    raise
                return False, err

        self._wait_or_skip(lambda: self._wait_for(schedulers_ready, "scheduler"), sync)

    def start_controller_agents(self, sync=True, custom_paths=None):
        self._run_builtin_yt_component("controller-agent", name="controller_agent", custom_paths=custom_paths)

        def controller_agents_ready():
            self._validate_processes_are_running("controller_agent")

            client = self._create_cluster_client()
            instances = client.list("//sys/controller_agents/instances")
            if len(instances) != self.yt_config.controller_agent_count:
                return False, "Only {0} agents are registered in cypress".format(len(instances))

            try:
                active_agents_count = 0
                for instance in instances:
                    orchid_path = "//sys/controller_agents/instances/{0}/orchid".format(instance)
                    path = orchid_path + "/controller_agent"
                    try:
                        client.set(orchid_path + "/@retry_backoff_time", 100)
                        active_agents_count += client.get(path + "/service/connected")
                    except YtResponseError as error:
                        if not error.is_resolve_error():
                            raise

                if active_agents_count < self.yt_config.controller_agent_count:
                    return False, "Only {0} agents are active".format(active_agents_count)

                return True
            except YtResponseError as err:
                # Orchid connection refused
                if not err.contains_code(105) and not err.contains_code(100):
                    raise
                return False, err

        self._wait_or_skip(lambda: self._wait_for(controller_agents_ready, "controller_agent"), sync)

    def create_client(self):
        if self.yt_config.http_proxy_count > 0:
            token = DEFAULT_ADMIN_TOKEN if self.yt_config.enable_auth else None
            return YtClient(token=token, proxy=self.get_http_proxy_address(), config=self._default_client_config)
        return self.create_native_client()

    def create_native_client(self, driver_name="driver"):
        driver_config_path = self.config_paths[driver_name]

        with open(driver_config_path, "rb") as f:
            driver_config = yson.load(f)

        driver_config["connection_type"] = "native"

        config = update(
            self._default_client_config,
            {
                "backend": "native",
                "driver_config": driver_config,
            }
        )

        if self.yt_config.address_resolver_config:
            config["driver_address_resolver_config"] = self.yt_config.address_resolver_config

        return YtClient(config=config)

    def _create_cluster_client(self):
        if self.yt_config.native_client_supported:
            return self.create_native_client()
        else:
            return self.create_client()

    def _remove_scheduler_lock(self):
        client = self._create_cluster_client()
        try:
            tx_id = client.get("//sys/scheduler/lock/@locks/0/transaction_id")
            if tx_id:
                client.abort_transaction(tx_id)
                logger.info("Previous scheduler transaction was aborted")
        except YtResponseError as error:
            if not error.is_resolve_error():
                raise

    def _prepare_drivers(self,
                         driver_configs,
                         rpc_driver_config,
                         master_configs,
                         clock_configs,
                         modify_driver_logging_config_func):
        for cell_index in xrange(self.yt_config.secondary_cell_count + 1):
            if cell_index == 0:
                tag = master_configs["primary_cell_tag"]
                name = "driver"
            else:
                tag = master_configs["secondary_cell_tags"][cell_index - 1]
                name = "driver_secondary_" + str(cell_index - 1)

            config_path = os.path.join(self.configs_path, "driver-{}.yson".format(cell_index))

            if self._load_existing_environment:
                if not os.path.isfile(config_path):
                    raise YtError("Driver config {0} not found".format(config_path))
                config = read_config(config_path)
            else:
                config = driver_configs[tag]
                write_config(config, config_path)

            self.configs[name] = config
            self.config_paths[name] = config_path

        if self.yt_config.clock_count > 0:
            name = "clock_driver"
            config_path = os.path.join(self.configs_path, "clock-driver.yson")

            if self._load_existing_environment:
                if not os.path.isfile(config_path):
                    raise YtError("Driver config {0} not found".format(config_path))
                config = read_config(config_path)
            else:
                config = driver_configs[tag]
                write_config(config, config_path)

            self.configs[name] = config
            self.config_paths[name] = config_path

        if self.yt_config.rpc_proxy_count > 0:
            name = "rpc_driver"
            config_path = os.path.join(self.configs_path, "rpc-driver.yson")

            if self._load_existing_environment:
                if not os.path.isfile(config_path):
                    raise YtError("Driver config {0} not found".format(config_path))
                config = read_config(config_path)
            else:
                config = rpc_driver_config
                write_config(config, config_path)

            self.configs[name] = config
            self.config_paths[name] = config_path

        config_path = os.path.join(self.configs_path, "driver-logging.yson")

        config = _init_logging(self.logs_path, "driver", self.yt_config)
        if modify_driver_logging_config_func:
            modify_driver_logging_config_func(config)
        write_config(config, config_path)

        self.configs["driver_logging"] = config
        self.config_paths["driver_logging"] = config_path

    def _prepare_kafka_proxies(self, kafka_proxy_configs, force_overwrite=False):
        self.configs["kafka_proxy"] = []
        self.config_paths["kafka_proxy"] = []

        for i in xrange(self.yt_config.kafka_proxy_count):
            config_path = os.path.join(self.configs_path, "kafka-proxy-{}.yson".format(i))
            if self._load_existing_environment and not force_overwrite:
                if not os.path.isfile(config_path):
                    raise YtError("Kafka proxy config {0} not found".format(config_path))
                config = read_config(config_path)
            else:
                config = kafka_proxy_configs[i]
                write_config(config, config_path)

            self.configs["kafka_proxy"].append(config)
            self.config_paths["kafka_proxy"].append(config_path)
            self._service_processes["kafka_proxy"].append(None)

    def _prepare_http_proxies(self, proxy_configs, force_overwrite=False):
        self.configs["http_proxy"] = []
        self.config_paths["http_proxy"] = []

        for i in xrange(self.yt_config.http_proxy_count):
            config_path = os.path.join(self.configs_path, "http-proxy-{}.yson".format(i))
            if self._load_existing_environment and not force_overwrite:
                if not os.path.isfile(config_path):
                    raise YtError("Http proxy config {0} not found".format(config_path))
                config = read_config(config_path)
            else:
                config = proxy_configs[i]
                write_config(config, config_path)

            if self.yt_config.https_cert is not None:
                cred = config["https_server"]["credentials"]
                shutil.copy(self.yt_config.https_cert, cred["cert_chain"]["file_name"])
                shutil.copy(self.yt_config.https_cert_key, cred["private_key"]["file_name"])

            self.configs["http_proxy"].append(config)
            self.config_paths["http_proxy"].append(config_path)
            self._service_processes["http_proxy"].append(None)

    def _prepare_rpc_proxies(self, rpc_proxy_configs, rpc_client_config):
        self.configs["rpc_proxy"] = []
        self.config_paths["rpc_proxy"] = []

        for i in xrange(self.yt_config.rpc_proxy_count):
            config_path = os.path.join(self.configs_path, "rpc-proxy-{}.yson".format(i))
            if self._load_existing_environment:
                if not os.path.isfile(config_path):
                    raise YtError("Rpc proxy config {0} not found".format(config_path))
                config = read_config(config_path)
            else:
                config = rpc_proxy_configs[i]
                write_config(config, config_path)

            self.configs["rpc_proxy"].append(config)
            self.config_paths["rpc_proxy"].append(config_path)
            self._service_processes["rpc_proxy"].append(None)

            rpc_client_config_path = os.path.join(self.configs_path, "rpc-client.yson")
            if self._load_existing_environment:
                if not os.path.isfile(rpc_client_config_path):
                    raise YtError("Rpc client config {0} not found".format(rpc_client_config_path))
            else:
                write_config(rpc_client_config, rpc_client_config_path)

    def start_http_proxy(self, sync=True):
        self._run_builtin_yt_component("http-proxy", name="http_proxy")

        def proxy_ready():
            self._validate_processes_are_running("http_proxy")

            try:
                for http_proxy_address in self.get_http_proxy_addresses():
                    address = "127.0.0.1:{0}".format(http_proxy_address.split(":")[1])
                    resp = requests.get("http://{0}/api".format(address))
                    resp.raise_for_status()
            except (requests.exceptions.RequestException, socket.error):
                return False, traceback.format_exc()

            return True

        self._wait_or_skip(lambda: self._wait_for(proxy_ready, "http_proxy"), sync)

    def start_kafka_proxy(self, sync=True):
        self._run_builtin_yt_component("kafka-proxy", name="kafka_proxy")

        def proxy_ready():
            self._validate_processes_are_running("kafka_proxy")
            # TODO(nadya73): add wait condition.
            return True

        self._wait_or_skip(lambda: self._wait_for(proxy_ready, "kafka_proxy"), sync)

    def start_rpc_proxy(self, sync=True):
        self._run_builtin_yt_component("proxy", name="rpc_proxy")

        client = self._create_cluster_client()

        proxies_with_discovery_count = 0
        proxies_ports = []
        for proxy in self.configs["rpc_proxy"]:
            proxies_ports.append(proxy["rpc_port"])
            if "tvm_only_rpc_port" in proxy:
                proxies_ports.append(proxy["tvm_only_rpc_port"])

            discovery_service_config = get_value(proxy.get("discovery_service"), {})
            discovery_service_enabled = get_value(discovery_service_config.get("enable"), True)
            if discovery_service_enabled:
                proxies_with_discovery_count += 1

        def rpc_proxy_ready():
            self._validate_processes_are_running("rpc_proxy")

            proxies_from_discovery = client.get("//sys/rpc_proxies")
            proxies_discovery_ready = len(proxies_from_discovery) == proxies_with_discovery_count and \
                all("alive" in proxy for proxy in proxies_from_discovery.values())

            proxies_ports_ready = all(is_port_opened(port) for port in proxies_ports)

            return proxies_discovery_ready and proxies_ports_ready

        self._wait_or_skip(lambda: self._wait_for(rpc_proxy_ready, "rpc_proxy"), sync)

    def _prepare_tablet_balancers(self, tablet_balancer_configs):
        for tablet_balancer_index in xrange(self.yt_config.tablet_balancer_count):
            tablet_balancer_config_name = "tablet_balancer-{0}.yson".format(tablet_balancer_index)
            config_path = os.path.join(self.configs_path, tablet_balancer_config_name)
            if self._load_existing_environment:
                if not os.path.isfile(config_path):
                    raise YtError("Tablet balancer config {0} not found. It is possible that you requested "
                                  "more tablet balancers than configs exist".format(config_path))
                config = read_config(config_path)
            else:
                config = tablet_balancer_configs[tablet_balancer_index]
                write_config(config, config_path)

            self.configs["tablet_balancer"].append(config)
            self.config_paths["tablet_balancer"].append(config_path)
            self._service_processes["tablet_balancer"].append(None)

    def start_tablet_balancers(self, sync=True):
        self._run_builtin_yt_component("tablet-balancer", name="tablet_balancer")

        def tablet_balancer_ready():
            self._validate_processes_are_running("tablet_balancer")
            return True

        self._wait_or_skip(
            lambda: self._wait_for(tablet_balancer_ready, "tablet_balancer"),
            sync)

    def _prepare_cypress_proxies(self, cypress_proxy_configs):
        for cypress_proxy_index in xrange(self.yt_config.cypress_proxy_count):
            cypress_proxy_config_name = "cypress_proxy-{0}.yson".format(cypress_proxy_index)
            config_path = os.path.join(self.configs_path, cypress_proxy_config_name)
            if self._load_existing_environment:
                if not os.path.isfile(config_path):
                    raise YtError("Cypress proxy config {0} not found. It is possible that you requested "
                                  "more cypress proxies than configs exist".format(config_path))
                config = read_config(config_path)
            else:
                config = cypress_proxy_configs[cypress_proxy_index]
                write_config(config, config_path)

            self.configs["cypress_proxy"].append(config)
            self.config_paths["cypress_proxy"].append(config_path)
            self._service_processes["cypress_proxy"].append(None)

    def start_cypress_proxies(self, sync=True):
        self._run_builtin_yt_component("cypress-proxy", name="cypress_proxy")

        def cypress_proxy_ready():
            self._validate_processes_are_running("cypress_proxy")
            return True

        self._wait_or_skip(
            lambda: self._wait_for(cypress_proxy_ready, "cypress_proxy"),
            sync)

    def _prepare_replicated_table_trackers(self, replicated_table_tracker_configs):
        for replicated_table_tracker_index in xrange(self.yt_config.replicated_table_tracker_count):
            replicated_table_tracker_config_name = "replicated_table_tracker-{0}.yson".format(replicated_table_tracker_index)
            config_path = os.path.join(self.configs_path, replicated_table_tracker_config_name)
            if self._load_existing_environment:
                if not os.path.isfile(config_path):
                    raise YtError("Replicated table tracker config {0} not found. It is possible that you requested "
                                  "more replicated table trackers than configs exist".format(config_path))
                config = read_config(config_path)
            else:
                config = replicated_table_tracker_configs[replicated_table_tracker_index]
                write_config(config, config_path)

            self.configs["replicated_table_tracker"].append(config)
            self.config_paths["replicated_table_tracker"].append(config_path)
            self._service_processes["replicated_table_tracker"].append(None)

    def start_replicated_table_trackers(self, sync=True):
        self._run_builtin_yt_component("replicated-table-tracker", name="replicated_table_tracker")

        def replicated_table_tracker_ready():
            self._validate_processes_are_running("replicated_table_tracker")
            return True

        self._wait_or_skip(
            lambda: self._wait_for(replicated_table_tracker_ready, "replicated_table_tracker"),
            sync)

    def _validate_process_is_running(self, process, name, number=None):
        if number is not None:
            name_with_number = "{0}-{1}".format(name, number)
        else:
            name_with_number = name

        if process.poll() is not None:
            self._process_stderrs(name, number)
            raise YtError("Process {0} unexpectedly terminated with error code {1}. "
                          "If the problem is reproducible please report to {2}"
                          .format(name_with_number, process.returncode, FEEDBACK_URL))

    def _validate_processes_are_running(self, name):
        for index, process in enumerate(self._service_processes[name]):
            if process is None:
                continue
            self._validate_process_is_running(process, name, index)

    def _wait_for(self, condition, name, max_wait_time=40, sleep_quantum=0.1):
        condition_error = None
        start_time = datetime.datetime.now()
        logger.info("Waiting for %s...", name)
        while datetime.datetime.now() - start_time < datetime.timedelta(seconds=max_wait_time):
            result = condition()
            if isinstance(result, tuple):
                ok, condition_error = result
            else:
                ok = result

            if ok:
                logger.info("%s ready", name.capitalize())
                return
            time.sleep(sleep_quantum)

        self._process_stderrs(name)

        error = YtError("{0} still not ready after {1} seconds. See logs in working dir for details."
                        .format(name.capitalize(), max_wait_time))
        if condition_error is not None:
            if isinstance(condition_error, str):
                condition_error = YtError(condition_error)
            error.inner_errors = [condition_error]

        raise error

    def _start_watcher(self):
        logger.info("Starting watcher")

        log_paths = []
        if self.yt_config.enable_debug_logging:
            def extract_debug_log_paths(service, config, result):
                log_config_path = "logging/writers/debug/file_name"
                result.append(get_value_from_config(config, log_config_path, service))
                if service == "node":
                    job_proxy_log_config_path = "exec_node/job_proxy/job_proxy_logging/log_manager_template/writers/debug/file_name"
                    result.append(get_value_from_config(config, job_proxy_log_config_path, service))

            extract_debug_log_paths("driver", {"logging": self.configs["driver_logging"]}, log_paths)

            for service, configs in list(iteritems(self.configs)):
                for config in flatten(configs):
                    if service.startswith("driver") \
                            or service.startswith("rpc_driver") \
                            or service.startswith("clock_driver"):
                        continue

                    extract_debug_log_paths(service, config, log_paths)

        self._service_processes["watcher"].append(None)

        with push_front_env_path(self.bin_path):
            self._watcher = ProcessWatcher(
                self._watcher_binary,
                self._pid_to_process.keys(),
                log_paths,
                lock_path=os.path.join(self.path, "lock_file"),
                config_dir=self.configs_path,
                logs_dir=self.logs_path,
                runtime_dir=self.runtime_data_path,
                process_runner=lambda args, env=None: self._run(args, "watcher", env=env),
                config=self.watcher_config)

        try:
            self._watcher.start()
        except YtError:
            logger.warning("Watcher stderr: %s", open(self._stderr_paths["watcher"][0]).read())
            raise

        logger.info("Watcher started")

    def _apply_queue_agent_dynamic_config(self, client):
        dyn_queue_agent_config = get_dynamic_queue_agent_config(self.yt_config)
        client.create("map_node", "//sys/queue_agents", ignore_existing=True)
        client.create("document", "//sys/queue_agents/config", ignore_existing=True, attributes={"value": dyn_queue_agent_config})
        return dyn_queue_agent_config

    def _apply_nodes_dynamic_config(self, client):
        patched_dynamic_node_config = get_patched_dynamic_node_config(self.yt_config)

        if self.modify_dynamic_configs_func is not None:
            self.modify_dynamic_configs_func(patched_dynamic_node_config, self.abi_version)

        client.set("//sys/cluster_nodes/@config", patched_dynamic_node_config)

        return patched_dynamic_node_config["%true"]

    def restore_default_node_dynamic_config(self):
        client = self._create_cluster_client()

        patched_config = self._apply_nodes_dynamic_config(client)

        self._wait_for_dynamic_config_update(patched_config, client)

    def restore_default_bundle_dynamic_config(self):
        client = self._create_cluster_client()

        client.set("//sys/tablet_cell_bundles/@config", {})

        self._wait_for_dynamic_config_update({}, client, config_node_name="bundle_dynamic_config_manager")

    def _wait_for_dynamic_config_update(self, expected_config, client, instance_type="cluster_nodes", config_node_name="dynamic_config_manager"):
        if not self.yt_config.wait_for_dynamic_config:
            return

        return wait_for_dynamic_config_update(client, expected_config, instances_path="//sys/{0}".format(instance_type), config_node_name=config_node_name)
