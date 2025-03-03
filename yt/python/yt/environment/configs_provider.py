from . import default_config
from .helpers import canonize_uuid

from yt.wrapper.common import MB, GB
from yt.common import update, update_inplace

from yt.yson import to_yson_type

try:
    from yt.packages.six.moves import xrange
except ImportError:
    from six.moves import xrange

import random
import os
from copy import deepcopy


DEFAULT_TRANSACTION_PING_PERIOD = 500


def _get_timestamp_provider_addresses(yt_config,
                                      master_connection_configs,
                                      clock_connection_config,
                                      timestamp_provider_addresses):
    if yt_config.timestamp_provider_count > 0 and timestamp_provider_addresses is not None:
        return timestamp_provider_addresses
    elif yt_config.clock_count > 0:
        return clock_connection_config["addresses"]
    else:
        return master_connection_configs[master_connection_configs["primary_cell_tag"]]["addresses"]


def _get_timestamp_provider_peer_configs(yt_config,
                                         master_connection_configs,
                                         clock_connection_config,
                                         timestamp_provider_addresses):
    addresses = _get_timestamp_provider_addresses(yt_config, master_connection_configs, clock_connection_config, timestamp_provider_addresses)
    return [{"address" : address} for address in addresses]


def build_configs(yt_config, ports_generator, dirs, logs_dir, binary_to_version):
    multidaemon_config = {
        "daemons": {},
    }
    init_singletons(multidaemon_config, yt_config)
    init_jaeger_collector(multidaemon_config, "multi", process_tags={})

    _init_logging(logs_dir,
                  "multi",
                  multidaemon_config.setdefault("logging", {}),
                  yt_config,
                  log_errors_to_stderr=True,
                  has_structured_logs=True)

    discovery_configs = _build_discovery_server_configs(
        yt_config,
        multidaemon_config,
        ports_generator,
        logs_dir)

    clock_configs, clock_connection_config = _build_clock_configs(
        yt_config,
        multidaemon_config,
        dirs["clock"],
        dirs["clock_tmpfs"],
        ports_generator,
        logs_dir)

    # Note that queue agent config depends on master rpc ports and master config depends on queue agent rpc ports.
    # That's why we prepare queue agent rpc ports separately before both configs.
    queue_agent_rpc_ports = _allocate_queue_agent_rpc_ports(yt_config, ports_generator)
    # Cypress proxies are involved in a similar cycle.
    cypress_proxy_rpc_ports = _allocate_cypress_proxy_rpc_ports(yt_config, ports_generator)

    master_configs, master_connection_configs = _build_master_configs(
        yt_config,
        multidaemon_config,
        dirs["master"],
        dirs["master_tmpfs"],
        clock_connection_config,
        discovery_configs,
        queue_agent_rpc_ports,
        cypress_proxy_rpc_ports,
        ports_generator,
        logs_dir)

    timestamp_provider_configs, timestamp_provider_addresses = _build_timestamp_provider_configs(
        yt_config,
        multidaemon_config,
        deepcopy(master_connection_configs),
        deepcopy(clock_connection_config),
        ports_generator,
        logs_dir)

    master_cache_configs, master_cache_addresses = _build_master_cache_configs(
        yt_config,
        multidaemon_config,
        master_connection_configs,
        clock_connection_config,
        discovery_configs,
        timestamp_provider_addresses,
        cypress_proxy_rpc_ports,
        ports_generator,
        logs_dir)

    cell_balancer_configs, cell_balancer_addresses = _build_cell_balancer_configs(
        yt_config,
        multidaemon_config,
        deepcopy(master_connection_configs),
        deepcopy(clock_connection_config),
        discovery_configs,
        timestamp_provider_addresses,
        master_cache_addresses,
        cypress_proxy_rpc_ports,
        ports_generator,
        logs_dir,
    )

    node_configs, node_addresses = _build_node_configs(
        multidaemon_config,
        dirs["node"],
        dirs["node_tmpfs"],
        deepcopy(master_connection_configs),
        deepcopy(clock_connection_config),
        discovery_configs,
        timestamp_provider_addresses,
        master_cache_addresses,
        cypress_proxy_rpc_ports,
        ports_generator,
        logs_dir,
        yt_config)

    chaos_node_configs = _build_chaos_node_configs(
        multidaemon_config,
        dirs["chaos_node"],
        deepcopy(master_connection_configs),
        deepcopy(clock_connection_config),
        discovery_configs,
        timestamp_provider_addresses,
        master_cache_addresses,
        cypress_proxy_rpc_ports,
        ports_generator,
        logs_dir,
        yt_config)

    queue_agent_configs = _build_queue_agent_configs(
        multidaemon_config,
        deepcopy(master_connection_configs),
        deepcopy(clock_connection_config),
        discovery_configs,
        timestamp_provider_addresses,
        master_cache_addresses,
        cypress_proxy_rpc_ports,
        queue_agent_rpc_ports,
        ports_generator,
        logs_dir,
        yt_config)

    kafka_proxy_configs = _build_kafka_proxy_configs(
        multidaemon_config,
        deepcopy(master_connection_configs),
        deepcopy(clock_connection_config),
        discovery_configs,
        timestamp_provider_addresses,
        master_cache_addresses,
        cypress_proxy_rpc_ports,
        ports_generator,
        logs_dir,
        yt_config)

    scheduler_configs = _build_scheduler_configs(
        multidaemon_config,
        dirs["scheduler"],
        deepcopy(master_connection_configs),
        deepcopy(clock_connection_config),
        discovery_configs,
        timestamp_provider_addresses,
        master_cache_addresses,
        cypress_proxy_rpc_ports,
        ports_generator,
        logs_dir,
        yt_config)

    controller_agent_configs = _build_controller_agent_configs(
        multidaemon_config,
        dirs["controller_agent"],
        deepcopy(master_connection_configs),
        deepcopy(clock_connection_config),
        discovery_configs,
        timestamp_provider_addresses,
        master_cache_addresses,
        cypress_proxy_rpc_ports,
        ports_generator,
        logs_dir,
        yt_config)

    http_proxy_configs = _build_http_proxy_config(
        multidaemon_config,
        dirs["http_proxy"],
        deepcopy(master_connection_configs),
        deepcopy(clock_connection_config),
        discovery_configs,
        timestamp_provider_addresses,
        master_cache_addresses,
        cypress_proxy_rpc_ports,
        queue_agent_rpc_ports,
        ports_generator,
        logs_dir,
        yt_config=yt_config,
        version=binary_to_version["ytserver-http-proxy"])

    http_proxy_url = None
    if yt_config.http_proxy_count > 0:
        http_proxy_url = "{0}:{1}".format(yt_config.fqdn, http_proxy_configs[0]["port"])

    rpc_proxy_configs = _build_rpc_proxy_configs(
        multidaemon_config,
        logs_dir,
        deepcopy(master_connection_configs),
        deepcopy(clock_connection_config),
        discovery_configs,
        timestamp_provider_addresses,
        master_cache_addresses,
        cypress_proxy_rpc_ports,
        ports_generator,
        yt_config)

    rpc_client_config = None
    rpc_proxy_addresses = None
    if yt_config.rpc_proxy_count > 0:
        rpc_proxy_addresses = [
            "{0}:{1}".format(yt_config.fqdn, rpc_proxy_config["rpc_port"])
            for rpc_proxy_config in rpc_proxy_configs
        ]
        rpc_client_config = {
            "connection_type": "rpc",
            "addresses": rpc_proxy_addresses
        }

    driver_configs = _build_native_driver_configs(
        deepcopy(master_connection_configs),
        deepcopy(clock_connection_config),
        discovery_configs,
        timestamp_provider_addresses,
        master_cache_addresses,
        cypress_proxy_rpc_ports,
        queue_agent_rpc_ports,
        yt_config=yt_config)

    rpc_driver_config = _build_rpc_driver_config(rpc_proxy_addresses, http_proxy_url)

    tablet_balancer_configs, tablet_balancer_addresses = _build_tablet_balancer_configs(
        yt_config,
        multidaemon_config,
        deepcopy(master_connection_configs),
        deepcopy(clock_connection_config),
        discovery_configs,
        timestamp_provider_addresses,
        master_cache_addresses,
        cypress_proxy_rpc_ports,
        ports_generator,
        logs_dir,
    )

    cypress_proxy_configs = _build_cypress_proxy_configs(
        yt_config,
        multidaemon_config,
        deepcopy(master_connection_configs),
        deepcopy(clock_connection_config),
        discovery_configs,
        timestamp_provider_addresses,
        master_cache_addresses,
        cypress_proxy_rpc_ports,
        ports_generator,
        logs_dir,
    )

    replicated_table_tracker_configs = _build_replicated_table_tracker_configs(
        yt_config,
        multidaemon_config,
        deepcopy(master_connection_configs),
        deepcopy(clock_connection_config),
        discovery_configs,
        timestamp_provider_addresses,
        master_cache_addresses,
        cypress_proxy_rpc_ports,
        ports_generator,
        logs_dir,
    )

    cluster_configuration = {
        "master": master_configs,
        "clock": clock_configs,
        "discovery": discovery_configs,
        "queue_agent": queue_agent_configs,
        "kafka_proxy": kafka_proxy_configs,
        "timestamp_provider": timestamp_provider_configs,
        "cell_balancer": cell_balancer_configs,
        "driver": driver_configs,
        "rpc_driver": rpc_driver_config,
        "scheduler": scheduler_configs,
        "controller_agent": controller_agent_configs,
        "node": node_configs,
        "chaos_node": chaos_node_configs,
        "master_cache": master_cache_configs,
        "http_proxy": http_proxy_configs,
        "rpc_proxy": rpc_proxy_configs,
        "rpc_client": rpc_client_config,
        "tablet_balancer": tablet_balancer_configs,
        "cypress_proxy": cypress_proxy_configs,
        "replicated_table_tracker": replicated_table_tracker_configs,
        "cluster_connection": _build_cluster_connection_config(
            yt_config,
            master_connection_configs,
            clock_connection_config,
            discovery_configs,
            timestamp_provider_addresses,
            master_cache_addresses,
            cypress_proxy_rpc_ports,
            queue_agent_rpc_ports),
        "multi": multidaemon_config,
    }

    return cluster_configuration


def _build_master_configs(yt_config,
                          multidaemon_config_output,
                          master_dirs,
                          master_tmpfs_dirs,
                          clock_connection_config,
                          discovery_configs,
                          queue_agent_rpc_ports,
                          cypress_proxy_rpc_ports,
                          ports_generator,
                          logs_dir):
    ports = []

    cell_tags = [str(yt_config.primary_cell_tag + index)
                 for index in xrange(yt_config.secondary_cell_count + 1)]
    random_part = random.randint(0, 2 ** 32 - 1)
    cell_ids = [canonize_uuid("%x-ffffffff-%x0259-ffffffff" % (random_part, int(tag)))
                for tag in cell_tags]

    nonvoting_master_count = yt_config.nonvoting_master_count

    connection_configs = {}
    for cell_index in xrange(yt_config.secondary_cell_count + 1):
        cell_ports = []
        cell_addresses = []
        peer_configs = []

        for i in xrange(yt_config.master_count):
            rpc_port, monitoring_port = next(ports_generator), next(ports_generator)
            address = "{0}:{1}".format(yt_config.fqdn, rpc_port)
            peer_config = {}
            peer_config["address"] = address
            if i >= yt_config.master_count - nonvoting_master_count:
                peer_config["voting"] = False
            peer_configs.append(peer_config)
            cell_addresses.append(address)
            cell_ports.append((rpc_port, monitoring_port))

        ports.append(cell_ports)

        connection_config = {
            "peers": peer_configs,
            # COMPAT(aleksandra-zh)
            "addresses": cell_addresses,
            "cell_id": cell_ids[cell_index]
        }
        connection_configs[cell_tags[cell_index]] = connection_config

    connection_configs["primary_cell_tag"] = cell_tags[0]
    connection_configs["secondary_cell_tags"] = cell_tags[1:]

    cluster_connection_config = \
        _build_cluster_connection_config(
            yt_config,
            connection_configs,
            clock_connection_config,
            discovery_configs,
            timestamp_provider_addresses=[],
            master_cache_addresses=[],
            cypress_proxy_rpc_ports=cypress_proxy_rpc_ports,
            queue_agent_rpc_ports=queue_agent_rpc_ports)

    configs = {}
    for cell_index in xrange(yt_config.secondary_cell_count + 1):
        cell_configs = []

        for master_index in xrange(yt_config.master_count):
            config = default_config.get_master_config()

            singletons_config = multidaemon_config_output if yt_config.enable_multidaemon else config
            if not yt_config.enable_multidaemon:
                init_singletons(singletons_config, yt_config)
                _init_logging(logs_dir,
                              "master-{0}-{1}".format(cell_index, master_index),
                              singletons_config.setdefault("logging", {}),
                              yt_config,
                              log_errors_to_stderr=True,
                              has_structured_logs=True)

            init_jaeger_collector(singletons_config, "master", {
                "cell_role": "primary" if cell_index == 0 else "secondary",
                "master_index": str(master_index),
            })

            init_cypress_annotations(config, master_index)

            set_at(config, "hydra_manager", _get_hydra_manager_config(), merge=True)

            config["rpc_port"], config["monitoring_port"] = ports[cell_index][master_index]

            config["primary_master"] = connection_configs[cell_tags[0]]
            config["secondary_masters"] = [connection_configs[tag]
                                           for tag in connection_configs["secondary_cell_tags"]]

            config["enable_timestamp_manager"] = ((yt_config.clock_count == 0) and not yt_config.has_ground)

            if yt_config.has_ground:
                config["clock_cluster_tag"] = yt_config.clock_cluster_tag

            if yt_config.discovery_server_count > 0:
                discovery_server_config = {}
                discovery_server_config["addresses"] = discovery_configs[0]["discovery_server"]["server_addresses"]
                config["discovery_server"] = discovery_server_config

            # COMPAT(aleksandra-zh)
            set_at(config, "timestamp_provider/addresses",
                   _get_timestamp_provider_addresses(yt_config, connection_configs, clock_connection_config, None))
            set_at(config, "timestamp_provider/peers",
                   _get_timestamp_provider_peer_configs(yt_config, connection_configs, clock_connection_config, None))

            set_at(config, "snapshots/path",
                   os.path.join(master_dirs[cell_index][master_index], "snapshots"))

            if master_tmpfs_dirs is None:
                set_at(config, "changelogs/path",
                       os.path.join(master_dirs[cell_index][master_index], "changelogs"))
            else:
                set_at(config, "changelogs/path",
                       os.path.join(master_tmpfs_dirs[cell_index][master_index], "changelogs"))

            config["cluster_connection"] = cluster_connection_config

            multidaemon_config_output["daemons"][f"master_{cell_index}_{master_index}"] = {
                "type": "master",
                "config": config,
            }
            cell_configs.append(config)

        configs[cell_tags[cell_index]] = cell_configs

    configs["primary_cell_tag"] = cell_tags[0]
    configs["secondary_cell_tags"] = cell_tags[1:]

    return configs, connection_configs


def _allocate_queue_agent_rpc_ports(yt_config, ports_generator):
    rpc_ports = []

    for i in xrange(yt_config.queue_agent_count):
        rpc_port = next(ports_generator)
        rpc_ports.append(rpc_port)

    return rpc_ports


def _build_clock_configs(yt_config, multidaemon_config_output, clock_dirs, clock_tmpfs_dirs, ports_generator, logs_dir):
    cell_tag = 1000
    random_part = random.randint(0, 2 ** 32 - 1)
    cell_id = canonize_uuid("%x-ffffffff-%x0259-ffffffff" % (random_part, int(cell_tag)))

    ports = []
    cell_addresses = []
    peer_configs = []

    for i in xrange(yt_config.clock_count):
        rpc_port, monitoring_port = next(ports_generator), next(ports_generator)
        address = to_yson_type("{0}:{1}".format(yt_config.fqdn, rpc_port))
        cell_addresses.append(address)
        peer_config = {}
        peer_config["address"] = address
        peer_configs.append(peer_config)
        ports.append((rpc_port, monitoring_port))

    connection_config = {
        # COMPAT(aleksandra-zh)
        "addresses": cell_addresses,
        "peers": peer_configs,
        "cell_id": cell_id
    }

    configs = {}
    instance_configs = []

    for clock_index in xrange(yt_config.clock_count):
        config = default_config.get_clock_config()

        singletons_config = multidaemon_config_output if yt_config.enable_multidaemon else config
        if not yt_config.enable_multidaemon:
            init_singletons(singletons_config, yt_config)

            _init_logging(logs_dir,
                          "clock-{0}".format(clock_index),
                          singletons_config.setdefault("logging", {}),
                          yt_config,
                          log_errors_to_stderr=True)

        init_jaeger_collector(singletons_config, "clock", {
            "clock_index": str(clock_index)
        })

        init_cypress_annotations(config, clock_index)

        set_at(config, "hydra_manager", _get_hydra_manager_config(), merge=True)

        config["rpc_port"], config["monitoring_port"] = ports[clock_index]

        config["clock_cell"] = connection_config

        # COMPAT(aleksandra-zh)
        set_at(config, "timestamp_provider/addresses", connection_config["addresses"])
        set_at(config, "snapshots/path",
               os.path.join(clock_dirs[clock_index], "snapshots"))

        if clock_tmpfs_dirs is None:
            set_at(config, "changelogs/path",
                   os.path.join(clock_dirs[clock_index], "changelogs"))
        else:
            set_at(config, "changelogs/path",
                   os.path.join(clock_tmpfs_dirs[clock_index], "changelogs"))

        multidaemon_config_output["daemons"][f"clock_{clock_index}"] = {
            "type": "clock",
            "config": config,
        }

        instance_configs.append(config)

    configs[cell_tag] = instance_configs
    configs["cell_tag"] = cell_tag

    return configs, connection_config


def _build_discovery_server_configs(yt_config, multidaemon_config_output, ports_generator, logs_dir):
    server_addresses = []
    ports = []

    for i in xrange(yt_config.discovery_server_count):
        rpc_port = yt_config.discovery_server_ports[i] if yt_config.discovery_server_ports else next(ports_generator)
        monitoring_port = next(ports_generator)
        address = to_yson_type("{0}:{1}".format(yt_config.fqdn, rpc_port))
        server_addresses.append(address)
        ports.append((rpc_port, monitoring_port))

    configs = []
    for index in xrange(yt_config.discovery_server_count):
        discovery_server_config = {}
        discovery_server_config["server_addresses"] = server_addresses

        config = {}
        config["discovery_server"] = discovery_server_config

        singletons_config = multidaemon_config_output if yt_config.enable_multidaemon else config
        if not yt_config.enable_multidaemon:
            init_singletons(singletons_config, yt_config)

            _init_logging(logs_dir,
                          "discovery-" + str(i),
                          singletons_config.setdefault("logging", {}),
                          yt_config,
                          log_errors_to_stderr=True)

        init_jaeger_collector(singletons_config, "discovery", {
            "discovery_index": str(i)
        })

        config["rpc_port"], config["monitoring_port"] = ports[index]

        multidaemon_config_output["daemons"][f"discovery_{index}"] = {
            "type": "discovery_server",
            "config": config,
        }
        configs.append(config)

    return configs


def _build_queue_agent_configs(multidaemon_config_output,
                               master_connection_configs,
                               clock_connection_config,
                               discovery_configs,
                               timestamp_provider_addresses,
                               master_cache_addresses,
                               cypress_proxy_rpc_ports,
                               rpc_ports,
                               ports_generator,
                               logs_dir,
                               yt_config):
    configs = []
    for index in xrange(yt_config.queue_agent_count):
        config = default_config.get_queue_agent_config()

        singletons_config = multidaemon_config_output if yt_config.enable_multidaemon else config
        if not yt_config.enable_multidaemon:
            init_singletons(singletons_config, yt_config)
            _init_logging(logs_dir,
                          "queue-agent-" + str(index),
                          singletons_config.setdefault("logging", {}),
                          yt_config)

        init_jaeger_collector(singletons_config, "queue_agent", {
            "queue_agent_index": str(index)
        })

        init_cypress_annotations(config, index)

        config["cluster_connection"] = \
            _build_cluster_connection_config(
                yt_config,
                master_connection_configs,
                clock_connection_config,
                discovery_configs,
                timestamp_provider_addresses,
                master_cache_addresses,
                cypress_proxy_rpc_ports)

        config["rpc_port"] = rpc_ports[index]
        config["monitoring_port"] = next(ports_generator)

        set_at(config, "queue_agent/stage", "production")

        multidaemon_config_output["daemons"][f"queue_agent_{index}"] = {
            "type": "queue_agent",
            "config": config,
        }
        configs.append(config)

    return configs


def _build_kafka_proxy_configs(multidaemon_config_output,
                               master_connection_configs,
                               clock_connection_config,
                               discovery_configs,
                               timestamp_provider_addresses,
                               master_cache_addresses,
                               cypress_proxy_rpc_ports,
                               ports_generator,
                               logs_dir,
                               yt_config):
    configs = []
    for index in xrange(yt_config.kafka_proxy_count):
        config = default_config.get_kafka_proxy_config()

        config["port"] = next(ports_generator)
        config["rpc_port"] = next(ports_generator)

        singletons_config = multidaemon_config_output if yt_config.enable_multidaemon else config
        if not yt_config.enable_multidaemon:
            init_singletons(singletons_config, yt_config)

            _init_logging(logs_dir,
                          "kafka-proxy-" + str(index),
                          singletons_config.setdefault("logging", {}),
                          yt_config)

        init_jaeger_collector(singletons_config, "kafka_proxy", {
            "kafka_proxy_index": str(index)
        })

        init_cypress_annotations(config, index)

        config["cluster_connection"] = \
            _build_cluster_connection_config(
                yt_config,
                master_connection_configs,
                clock_connection_config,
                discovery_configs,
                timestamp_provider_addresses,
                master_cache_addresses,
                cypress_proxy_rpc_ports)

        config["monitoring_port"] = next(ports_generator)

        multidaemon_config_output["daemons"][f"kafka_proxy_{index}"] = {
            "type": "kafka_proxy",
            "config": config,
        }
        configs.append(config)

    return configs


def _build_timestamp_provider_configs(yt_config,
                                      multidaemon_config_output,
                                      master_connection_configs,
                                      clock_connection_config,
                                      ports_generator,
                                      logs_dir):
    configs = []
    addresses = []

    for index in xrange(yt_config.timestamp_provider_count):
        config = default_config.get_timestamp_provider_config()

        singletons_config = multidaemon_config_output if yt_config.enable_multidaemon else config
        if not yt_config.enable_multidaemon:
            init_singletons(singletons_config, yt_config)

            _init_logging(logs_dir,
                          "timestamp-provider-" + str(index),
                          singletons_config.setdefault("logging", {}),
                          yt_config,
                          log_errors_to_stderr=True)

        init_jaeger_collector(singletons_config, "timestamp_provider", {
            "timestamp_provider_index": str(index)
        })

        init_cypress_annotations(config, index)

        # COMPAT(aleksandra-zh)
        set_at(config, "timestamp_provider/addresses",
               _get_timestamp_provider_addresses(yt_config, master_connection_configs, clock_connection_config, None))
        set_at(config, "timestamp_provider/peers",
               _get_timestamp_provider_peer_configs(yt_config, master_connection_configs, clock_connection_config, None))

        config["rpc_port"] = next(ports_generator)
        config["monitoring_port"] = next(ports_generator)

        multidaemon_config_output["daemons"][f"timestamp_provider_{index}"] = {
            "type": "timestamp_provider",
            "config": config,
        }
        configs.append(config)
        addresses.append("{}:{}".format(yt_config.fqdn, config["rpc_port"]))

    return configs, addresses


def _build_cell_balancer_configs(yt_config,
                                 multidaemon_config_output,
                                 master_connection_configs,
                                 clock_connection_config,
                                 discovery_configs,
                                 timestamp_provider_addresses,
                                 master_cache_addresses,
                                 cypress_proxy_rpc_ports,
                                 ports_generator,
                                 logs_dir):
    configs = []
    addresses = []

    for index in xrange(yt_config.cell_balancer_count):
        config = default_config.get_cell_balancer_config()

        singletons_config = multidaemon_config_output if yt_config.enable_multidaemon else config
        if not yt_config.enable_multidaemon:
            init_singletons(singletons_config, yt_config)

            _init_logging(logs_dir,
                          "cell-balancer-" + str(index),
                          singletons_config.setdefault("logging", {}),
                          yt_config,
                          log_errors_to_stderr=True)

        init_jaeger_collector(singletons_config, "cell_balancer", {
            "cell_balancer_index": str(index)
        })

        init_cypress_annotations(config, index)

        config["cluster_connection"] = \
            _build_cluster_connection_config(
                yt_config,
                master_connection_configs,
                clock_connection_config,
                discovery_configs,
                timestamp_provider_addresses,
                master_cache_addresses,
                cypress_proxy_rpc_ports,
                config_template=config["cluster_connection"])

        config["rpc_port"] = next(ports_generator)
        config["monitoring_port"] = next(ports_generator)

        config["enable_bundle_controller"] = yt_config.enable_bundle_controller

        if yt_config.enable_bundle_controller:
            # Consistent with prod
            config["enable_cell_balancer"] = False
            config["bundle_controller"] = {
                "cluster" : "local",
                "root_path" : "//sys/bundle_controller/controller",
                "has_instance_allocator_service": False,
                "hulk_allocations_path" : "//sys/hulk/allocation_requests",
                "hulk_allocations_history_path" : "//sys/hulk/allocation_requests_history",
                "hulk_deallocations_path" : "//sys/hulk/deallocation_requests",
                "hulk_deallocations_history_path" : "//sys/hulk/deallocation_requests_history",
                "decommission_released_nodes": False,
                "bundle_scan_period" : "100ms",
            }

        multidaemon_config_output["daemons"][f"cell_balancer_{index}"] = {
            "type": "cell_balancer",
            "config": config,
        }
        configs.append(config)
        addresses.append("{}:{}".format(yt_config.fqdn, config["rpc_port"]))

    return configs, addresses


def _build_master_cache_configs(yt_config,
                                multidaemon_config_output,
                                master_connection_configs,
                                clock_connection_config,
                                discovery_configs,
                                timestamp_provider_addresses,
                                cypress_proxy_rpc_ports,
                                ports_generator,
                                logs_dir):
    configs = []
    addresses = []

    for index in xrange(yt_config.master_cache_count):
        config = default_config.get_master_cache_config()

        singletons_config = multidaemon_config_output if yt_config.enable_multidaemon else config
        if not yt_config.enable_multidaemon:
            init_singletons(singletons_config, yt_config)

            _init_logging(logs_dir,
                          "master-cache-" + str(index),
                          singletons_config.setdefault("logging", {}),
                          yt_config,
                          has_structured_logs=True)

        init_jaeger_collector(singletons_config, "master_cache", {"master_cache_index": str(index)})

        init_cypress_annotations(config, index)

        config["cluster_connection"] = \
            _build_cluster_connection_config(
                yt_config,
                master_connection_configs,
                clock_connection_config,
                discovery_configs,
                timestamp_provider_addresses,
                [],  # master cache addresses
                cypress_proxy_rpc_ports,
                config_template=config["cluster_connection"])

        config["rpc_port"] = next(ports_generator)
        config["monitoring_port"] = next(ports_generator)

        multidaemon_config_output["daemons"][f"master_cache_{index}"] = {
            "type": "master_cache",
            "config": config,
        }
        configs.append(config)
        addresses.append("{}:{}".format(yt_config.fqdn, config["rpc_port"]))

    return configs, addresses


def _build_scheduler_configs(multidaemon_config_output,
                             scheduler_dirs,
                             master_connection_configs,
                             clock_connection_config,
                             discovery_configs,
                             timestamp_provider_addresses,
                             master_cache_addresses,
                             cypress_proxy_rpc_ports,
                             ports_generator,
                             logs_dir,
                             yt_config):
    configs = []

    for index in xrange(yt_config.scheduler_count):
        config = default_config.get_scheduler_config()

        singletons_config = multidaemon_config_output if yt_config.enable_multidaemon else config
        if not yt_config.enable_multidaemon:
            init_singletons(singletons_config, yt_config)

            _init_logging(logs_dir,
                          "scheduler-" + str(index),
                          singletons_config.setdefault("logging", {}),
                          yt_config,
                          has_structured_logs=True)

        init_jaeger_collector(singletons_config, "scheduler", {"scheduler_index": str(index)})

        init_cypress_annotations(config, index)

        config["cluster_connection"] = \
            _build_cluster_connection_config(
                yt_config,
                master_connection_configs,
                clock_connection_config,
                discovery_configs,
                timestamp_provider_addresses,
                master_cache_addresses,
                cypress_proxy_rpc_ports,
                config_template=config["cluster_connection"])

        config["rpc_port"] = next(ports_generator)
        config["monitoring_port"] = next(ports_generator)

        multidaemon_config_output["daemons"][f"scheduler_{index}"] = {
            "type": "scheduler",
            "config": config,
        }
        configs.append(config)

    return configs


def _build_controller_agent_configs(multidaemon_config_output,
                                    controller_agent_dirs,
                                    master_connection_configs,
                                    clock_connection_config,
                                    discovery_configs,
                                    timestamp_provider_addresses,
                                    master_cache_addresses,
                                    cypress_proxy_rpc_ports,
                                    ports_generator,
                                    logs_dir,
                                    yt_config):
    configs = []

    for index in xrange(yt_config.controller_agent_count):
        config = default_config.get_controller_agent_config()

        singletons_config = multidaemon_config_output if yt_config.enable_multidaemon else config
        if not yt_config.enable_multidaemon:
            init_singletons(singletons_config, yt_config)

            _init_logging(logs_dir,
                          "controller-agent-" + str(index),
                          singletons_config.setdefault("logging", {}),
                          yt_config,
                          has_structured_logs=True)

        init_jaeger_collector(singletons_config, "controller_agent", {"controller_agent_index": str(index)})

        init_cypress_annotations(config, index)

        config["cluster_connection"] = \
            _build_cluster_connection_config(
                yt_config,
                master_connection_configs,
                clock_connection_config,
                discovery_configs,
                timestamp_provider_addresses,
                master_cache_addresses,
                cypress_proxy_rpc_ports,
                config_template=config["cluster_connection"])

        config["rpc_port"] = next(ports_generator)
        config["monitoring_port"] = next(ports_generator)

        multidaemon_config_output["daemons"][f"controller_agent_{index}"] = {
            "type": "controller_agent",
            "config": config,
        }
        configs.append(config)

    return configs


def _build_node_configs(multidaemon_config_output,
                        node_dirs,
                        node_tmpfs_dirs,
                        master_connection_configs,
                        clock_connection_config,
                        discovery_configs,
                        timestamp_provider_addresses,
                        master_cache_addresses,
                        cypress_proxy_rpc_ports,
                        ports_generator,
                        logs_dir,
                        yt_config):
    configs = []
    addresses = []

    for index in xrange(yt_config.node_count):
        config = default_config.get_node_config()

        singletons_config = multidaemon_config_output if yt_config.enable_multidaemon else config
        if not yt_config.enable_multidaemon:
            init_singletons(singletons_config, yt_config)

            _init_logging(
                logs_dir,
                "node-{0}".format(index),
                singletons_config.setdefault("logging", {}),
                yt_config,
                has_structured_logs=True)

        jaeger_process_tags = {"node_index": str(index)}
        init_jaeger_collector(singletons_config, "node", jaeger_process_tags)

        init_job_proxy_jaeger_collector(config, jaeger_process_tags)

        init_cypress_annotations(config, index)

        config["addresses"] = [
            ("interconnect", yt_config.fqdn),
            ("default", yt_config.fqdn)
        ]

        config["rpc_port"] = next(ports_generator)
        config["monitoring_port"] = next(ports_generator)
        config["skynet_http_port"] = next(ports_generator)

        addresses.append("{0}:{1}".format(yt_config.fqdn, config["rpc_port"]))

        config["cluster_connection"] = \
            _build_cluster_connection_config(
                yt_config,
                master_connection_configs,
                clock_connection_config,
                discovery_configs,
                timestamp_provider_addresses,
                master_cache_addresses,
                cypress_proxy_rpc_ports,
                config_template=config["cluster_connection"])

        cache_location_config = {
            "quota": 256 * MB,
            "io_config": {
                "enable_sync": False,
            },
        }

        if node_tmpfs_dirs is not None and yt_config.allow_chunk_storage_in_tmpfs:
            cache_location_config["path"] = os.path.join(node_tmpfs_dirs[index], "chunk_cache")
        else:
            cache_location_config["path"] = os.path.join(node_dirs[index], "chunk_cache")

        set_at(config, "data_node/cache_locations", [cache_location_config])

        if yt_config.jobs_environment_type is not None:
            set_at(
                config,
                "exec_node/slot_manager/job_environment",
                _get_node_job_environment_config(yt_config, index, logs_dir)
            )

        if yt_config.jobs_environment_type == "cri":
            # Forward variables set in docker image into user job environment.
            set_at(config, "exec_node/job_proxy/forward_all_environment_variables", True)

            # Job proxy needs CA certificate to validate incoming connections.
            if yt_config.ca_cert is not None:
                get_at(config, "exec_node/slot_manager/job_environment/job_proxy_bind_mounts").append({
                    "internal_path": yt_config.ca_cert,
                    "external_path": yt_config.ca_cert,
                    "read_only": True,
                })

        if yt_config.use_slot_user_id:
            start_uid = 10000 + config["rpc_port"]
            set_at(config, "exec_node/slot_manager/job_environment/start_uid", start_uid)
        else:
            set_at(config, "exec_node/slot_manager/do_not_set_user_id", True)

        set_at(config, "exec_node/slot_manager/locations", [
            {"path": os.path.join(node_dirs[index], "slots"), "disk_usage_watermark": 0}
        ])

        changelog_config = {
            "preallocate_size": 2 ** 20,
        }

        store_location_configs = []

        for location_index in range(yt_config.store_location_count):
            store_location_config = {
                "low_watermark": 0,
                "high_watermark": 0,
                "disable_writes_watermark": 0,
                "io_config": {
                    "enable_sync": False,
                },
                "use_direct_io_for_reads" : yt_config.node_use_direct_io_for_reads,
                "multiplexed_changelog": changelog_config,
                "high_latency_split_changelog": changelog_config,
                "low_latency_split_changelog": changelog_config,
            }

            if yt_config.node_io_engine_type:
                store_location_config["io_engine_type"] = yt_config.node_io_engine_type

            if yt_config.node_chunk_store_quota is not None:
                store_location_config["quota"] = yt_config.node_chunk_store_quota

            if node_tmpfs_dirs is not None and yt_config.allow_chunk_storage_in_tmpfs:
                store_location_config["path"] = os.path.join(node_tmpfs_dirs[index], "chunk_store/{0}".format(location_index))
            else:
                store_location_config["path"] = os.path.join(node_dirs[index], "chunk_store/{0}".format(location_index))

            store_location_configs.append(store_location_config)

        set_at(config, "data_node/store_locations", store_location_configs)
        set_at(config, "data_node/use_disable_send_blocks", True)

        layer_location_config = {
            "low_watermark": 1,
            "location_is_absolute": False,
        }

        if node_tmpfs_dirs is not None and yt_config.allow_chunk_storage_in_tmpfs:
            layer_location_config["path"] = os.path.join(node_tmpfs_dirs[index], "layers")
        else:
            layer_location_config["path"] = os.path.join(node_dirs[index], "layers")

        set_at(config, "data_node/volume_manager/layer_locations", [layer_location_config])

        # COMPAT
        log_name = "job_proxy-{0}-slot-%slot_index%".format(index) if yt_config.enable_legacy_logging_scheme else "job_proxy-{0}".format(index)

        set_at(
            config,
            "exec_node/job_proxy/job_proxy_logging/mode",
            yt_config.job_proxy_logging["mode"]
        )
        set_at(
            config,
            "exec_node/job_proxy/job_proxy_logging/log_manager_template",
            {}
        )

        _init_logging(
            logs_dir,
            log_name,
            config["exec_node"]["job_proxy"]["job_proxy_logging"]["log_manager_template"],
            yt_config,
            # TODO(pogorelov): Support compression in DumpJobProxyLog.
            enable_log_compression=(yt_config.job_proxy_logging["mode"] == "simple"),
            use_name_in_writer_name=False,
        )

        # COMPAT
        for key in config["exec_node"]["job_proxy"]["job_proxy_logging"]["log_manager_template"]:
            config["exec_node"]["job_proxy"]["job_proxy_logging"][key] = config["exec_node"]["job_proxy"]["job_proxy_logging"]["log_manager_template"][key]

        set_at(
            config,
            "exec_node/job_proxy/job_proxy_logging/job_proxy_stderr_path",
            # COMPAT
            os.path.join(logs_dir, "job_proxy-{0}-stderr-slot-%slot_index%".format(index))
            if yt_config.enable_legacy_logging_scheme
            else os.path.join(logs_dir, "job_proxy-{0}-stderr".format(index)),
        )
        set_at(
            config,
            "exec_node/job_proxy/job_proxy_logging/executor_stderr_path",
            # COMPAT
            os.path.join(logs_dir, "ytserver_exec-{0}-stderr-slot-%slot_index%".format(index))
            if yt_config.enable_legacy_logging_scheme
            else os.path.join(logs_dir, "ytserver_exec-{0}-stderr".format(index))
        )

        set_at(
            config,
            "exec_node/job_proxy_log_manager/directory",
            os.path.join(logs_dir, "job_proxy-{0}".format(index))
        )
        set_at(
            config,
            "exec_node/job_proxy_log_manager/sharding_key_length",
            yt_config.job_proxy_log_manager["sharding_key_length"]
        )
        set_at(
            config,
            "exec_node/job_proxy_log_manager/logs_storage_period",
            yt_config.job_proxy_log_manager["logs_storage_period"]
        )
        if yt_config.job_proxy_log_manager["directory_traversal_concurrency"] is not None:
            set_at(
                config,
                "exec_node/job_proxy_log_manager/directory_traversal_concurrency",
                yt_config.job_proxy_log_manager["directory_traversal_concurrency"]
            )
        if yt_config.job_proxy_log_manager["log_dump"] is not None:
            set_at(
                config,
                "exec_node/job_proxy_log_manager/log_dump",
                yt_config.job_proxy_log_manager["log_dump"]
            )

        set_at(config, "tablet_node/hydra_manager", _get_hydra_manager_config(), merge=True)
        set_at(config, "tablet_node/hydra_manager/restart_backoff_time", 100)
        set_at(config, "job_resource_manager/resource_limits", yt_config.jobs_resource_limits, merge=True)
        set_at(config, "resource_limits", _get_node_resource_limits_config(yt_config), merge=True)

        configs.append(config)

    if hasattr(ports_generator, "local_port_range"):
        USER_PORT_START = ports_generator.local_port_range[1]
        USER_PORT_END = min(USER_PORT_START + 10000, 2 ** 16)
        assert USER_PORT_START < USER_PORT_END
    else:
        # Legacy constants.
        USER_PORT_START = 20000
        USER_PORT_END = 30000

    node_count = len(configs)
    for index, config in enumerate(configs):
        port_start = USER_PORT_START + (index * (USER_PORT_END - USER_PORT_START)) // node_count
        port_end = USER_PORT_START + ((index + 1) * (USER_PORT_END - USER_PORT_START)) // node_count

        if yt_config.node_port_set_size is None:
            set_at(config, "job_resource_manager/start_port", port_start)
            set_at(config, "job_resource_manager/port_count", port_end - port_start)
        else:
            ports = [next(ports_generator) for _ in xrange(yt_config.node_port_set_size)]
            set_at(config, "job_resource_manager/port_set", ports)

        multidaemon_config_output["daemons"][f"node_{index}"] = {
            "type": "node",
            "config": config,
        }

    return configs, addresses


def _build_chaos_node_configs(multidaemon_config_output,
                              chaos_node_dirs,
                              master_connection_configs,
                              clock_connection_config,
                              discovery_configs,
                              timestamp_provider_addresses,
                              master_cache_addresses,
                              cypress_proxy_rpc_ports,
                              ports_generator,
                              logs_dir,
                              yt_config):
    configs = []

    for index in xrange(yt_config.chaos_node_count):
        config = default_config.get_chaos_node_config()

        singletons_config = multidaemon_config_output if yt_config.enable_multidaemon else config
        if not yt_config.enable_multidaemon:
            init_singletons(singletons_config, yt_config)

            _init_logging(
                logs_dir,
                "chaos-node-{0}".format(index),
                singletons_config.setdefault("logging", {}),
                yt_config)

        init_jaeger_collector(singletons_config, "chaos_node", {"chaos_node_index": str(index)})

        init_cypress_annotations(config, index)

        config["addresses"] = [
            ("interconnect", yt_config.fqdn),
            ("default", yt_config.fqdn)
        ]
        config["rpc_port"] = next(ports_generator)
        config["monitoring_port"] = next(ports_generator)
        config["skynet_http_port"] = next(ports_generator)

        config["cluster_connection"] = \
            _build_cluster_connection_config(
                yt_config,
                master_connection_configs,
                clock_connection_config,
                discovery_configs,
                timestamp_provider_addresses,
                master_cache_addresses,
                cypress_proxy_rpc_ports,
                config_template=config["cluster_connection"])

        cache_location_config = {
            "quota": 0,
            "io_config": {
                "enable_sync": False,
            },
            "path": os.path.join(chaos_node_dirs[index], "chunk_cache"),
        }
        set_at(config, "data_node/cache_locations", [cache_location_config])

        set_at(config, "cellar_node/cellar_manager/cellars/chaos/occupant/hydra_manager", _get_hydra_manager_config(), merge=True)
        set_at(config, "cellar_node/cellar_manager/cellars/chaos/occupant/hydra_manager/restart_backoff_time", 100)
        set_at(config, "cellar_node/cellar_manager/cellars/chaos/occupant/response_keeper", _get_response_keeper_config())

        multidaemon_config_output["daemons"][f"chaos_node_{index}"] = {
            "type": "node",
            "config": config,
        }
        configs.append(config)

    return configs


def _build_http_proxy_config(multidaemon_config_output,
                             proxy_dir,
                             master_connection_configs,
                             clock_connection_config,
                             discovery_configs,
                             timestamp_provider_addresses,
                             master_cache_addresses,
                             cypress_proxy_rpc_ports,
                             queue_agent_rpc_ports,
                             ports_generator,
                             logs_dir,
                             yt_config,
                             version):
    driver_config = default_config.get_driver_config()

    cluster_connection = _build_cluster_connection_config(
        yt_config,
        master_connection_configs,
        clock_connection_config,
        discovery_configs,
        timestamp_provider_addresses,
        master_cache_addresses,
        cypress_proxy_rpc_ports,
        queue_agent_rpc_ports)

    proxy_configs = []

    for index in xrange(yt_config.http_proxy_count):
        config = default_config.get_proxy_config()
        config["port"] = \
            yt_config.http_proxy_ports[index] if yt_config.http_proxy_ports else next(ports_generator)
        if yt_config.enable_tvm_only_proxies:
            config["tvm_only_http_server"] = {"port": next(ports_generator)}
        config["monitoring_port"] = next(ports_generator)
        config["rpc_port"] = next(ports_generator)

        if yt_config.enable_chyt_http_proxies:
            config["chyt_http_server"] = {"port": next(ports_generator)}

        fqdn = "{0}:{1}".format(yt_config.fqdn, config["port"])
        set_at(config, "coordinator/public_fqdn", fqdn)

        singletons_config = multidaemon_config_output if yt_config.enable_multidaemon else config
        if not yt_config.enable_multidaemon:
            init_singletons(singletons_config, yt_config)

            _init_logging(
                logs_dir,
                "http-proxy-{}".format(index),
                singletons_config.setdefault("logging", {}),
                yt_config,
                has_structured_logs=True)

        init_jaeger_collector(config, "http_proxy", {"http_proxy_index": str(index)})

        init_cypress_annotations(config, index)

        config["driver"] = deepcopy(driver_config)
        config["cluster_connection"] = deepcopy(cluster_connection)

        config["zookeeper_proxy"] = {
            "server": {
                "port": next(ports_generator),
            },
        }

        if yt_config.https_cert is not None:
            set_at(config, "https_server", {
                "port": yt_config.https_proxy_ports[index] if yt_config.https_proxy_ports else next(ports_generator),
                "credentials": {
                    "cert_chain": {
                        "file_name": os.path.join(proxy_dir[index], 'https.crt'),
                    },
                    "private_key": {
                        "file_name": os.path.join(proxy_dir[index], 'https.key'),
                    },
                },
            })

        if yt_config.enable_chyt_https_proxies:
            set_at(config, "chyt_https_server", {
                "port": next(ports_generator),
                "credentials": {
                    "cert_chain": {
                        "file_name": os.path.join(proxy_dir[index], 'https.crt'),
                    },
                    "private_key": {
                        "file_name": os.path.join(proxy_dir[index], 'https.key'),
                    },
                },
            })

        if yt_config.enable_auth:
            set_at(config, "auth/enable_authentication", True)
            set_at(config, "auth/require_authentication", True)

        multidaemon_config_output["daemons"][f"http_proxy_{index}"] = {
            "type": "http_proxy",
            "config": config,
        }

        proxy_configs.append(config)

    return proxy_configs


def _build_native_driver_configs(master_connection_configs,
                                 clock_connection_config,
                                 discovery_configs,
                                 timestamp_provider_addresses,
                                 master_cache_addresses,
                                 cypress_proxy_rpc_ports,
                                 queue_agent_rpc_ports,
                                 yt_config):
    secondary_cell_tags = master_connection_configs["secondary_cell_tags"]
    primary_cell_tag = master_connection_configs["primary_cell_tag"]

    configs = {}
    for cell_index in xrange(yt_config.secondary_cell_count + 1):
        config = default_config.get_driver_config()
        init_chunk_client_dispatcher(config)
        config["start_queue_consumer_registration_manager"] = True

        if cell_index == 0:
            tag = primary_cell_tag
            update_inplace(config, _build_cluster_connection_config(
                yt_config,
                master_connection_configs,
                clock_connection_config,
                discovery_configs,
                timestamp_provider_addresses,
                master_cache_addresses,
                cypress_proxy_rpc_ports,
                queue_agent_rpc_ports))
        else:
            tag = secondary_cell_tags[cell_index - 1]
            cell_connection_config = {
                "primary_master": master_connection_configs[secondary_cell_tags[cell_index - 1]],
                "master_cell_directory_synchronizer": {
                    "sync_period": None
                },
                "timestamp_provider": {
                    # COMPAT(aleksandra-zh)
                    "addresses": _get_timestamp_provider_addresses(
                        yt_config,
                        master_connection_configs,
                        clock_connection_config,
                        timestamp_provider_addresses),
                    "peers": _get_timestamp_provider_peer_configs(
                        yt_config,
                        master_connection_configs,
                        clock_connection_config,
                        timestamp_provider_addresses),
                },
                "transaction_manager": {
                    "default_ping_period": DEFAULT_TRANSACTION_PING_PERIOD,
                }
            }

            if yt_config.mock_tvm_id is not None:
                cell_connection_config["tvm_id"] = yt_config.mock_tvm_id

            discovery_server_addresses = master_connection_configs[primary_cell_tag]["addresses"]
            if yt_config.discovery_server_count > 0:
                discovery_server_addresses = discovery_configs[0]["discovery_server"]["server_addresses"]

            discovery_connection_config = {}
            discovery_connection_config["addresses"] = discovery_server_addresses
            cell_connection_config["discovery_connection"] = discovery_connection_config

            update_inplace(cell_connection_config["primary_master"], _get_retrying_channel_config())
            update_inplace(cell_connection_config["primary_master"], _get_rpc_config())

            update_inplace(config, cell_connection_config)

        if yt_config.mock_tvm_id is not None:
            config["tvm_service"] = {
                "enable_mock": True,
                "client_self_id": yt_config.mock_tvm_id,
                "client_enable_service_ticket_fetching": True,
                "client_enable_service_ticket_checking": True,
                "client_dst_map": {
                    "self": yt_config.mock_tvm_id,
                },
                "client_self_secret": "TestSecret-" + str(yt_config.mock_tvm_id),
            }

        configs[tag] = config

    if yt_config.clock_count > 0:
        config = deepcopy(configs[primary_cell_tag])
        update_inplace(config["timestamp_provider"], clock_connection_config)
        configs[primary_cell_tag] = config

    return configs


def _build_rpc_driver_config(rpc_proxy_addresses, http_proxy_url):
    config = default_config.get_driver_config()

    config["connection_type"] = "rpc"

    config["dynamic_channel_pool"] = {
        "soft_backoff_time": 100,
        "hard_backoff_time": 100
    }

    if http_proxy_url is not None:
        config["cluster_url"] = http_proxy_url
    else:
        config["proxy_addresses"] = rpc_proxy_addresses

    return config


def _build_rpc_proxy_configs(multidaemon_config_output,
                             logs_dir,
                             master_connection_configs,
                             clock_connection_config,
                             discovery_configs,
                             timestamp_provider_addresses,
                             master_cache_addresses,
                             cypress_proxy_rpc_ports,
                             ports_generator,
                             yt_config):
    configs = []

    for index in xrange(yt_config.rpc_proxy_count):
        grpc_server_config = {
            "addresses": [
                {
                    "address": "{}:{}".format(yt_config.fqdn, next(ports_generator))
                }
            ]
        }

        config = {
            "cluster_connection": master_connection_configs,
            "discovery_service": {
                "liveness_update_period": 500,
                "proxy_update_period": 500
            },
            "grpc_server": grpc_server_config,
            "monitoring_port": next(ports_generator),
            "yt_alloc_dump_period": 15000,
            "ref_counted_tracker_dump_period": 15000,
            "enable_authentication": False,
            "api_service": {
                "security_manager": {
                    "user_cache": {
                        "expire_after_successful_update_time": 0,
                        "refresh_time": 0,
                        "expire_after_failed_update_time": 0,
                        "expire_after_access_time": 0,
                    }
                }
            },
            "dynamic_config_manager": {
                "update_period": 100,
            },
            "rpc_server": {
                "tracing_mode": "force",
            }
        }

        singletons_config = multidaemon_config_output if yt_config.enable_multidaemon else config
        if not yt_config.enable_multidaemon:
            init_singletons(singletons_config, yt_config)
            _init_logging(
                logs_dir,
                "rpc-proxy-{}".format(index),
                singletons_config.setdefault("logging", {}),
                yt_config)

        init_jaeger_collector(singletons_config, "rpc_proxy", {"rpc_proxy_index": str(index)})

        init_cypress_annotations(config, index)
        config["cluster_connection"] = _build_cluster_connection_config(
            yt_config,
            master_connection_configs,
            clock_connection_config,
            discovery_configs,
            timestamp_provider_addresses,
            master_cache_addresses,
            cypress_proxy_rpc_ports)

        config["rpc_port"] = \
            yt_config.rpc_proxy_ports[index] if yt_config.rpc_proxy_ports else next(ports_generator)
        if yt_config.enable_tvm_only_proxies:
            config["tvm_only_rpc_port"] = next(ports_generator)

        multidaemon_config_output["daemons"][f"rpc_proxy_{index}"] = {
            "type": "rpc_proxy",
            "config": config,
        }
        configs.append(config)

    return configs


def _build_cluster_connection_config(yt_config,
                                     master_connection_configs,
                                     clock_connection_config,
                                     discovery_configs,
                                     timestamp_provider_addresses,
                                     master_cache_addresses,
                                     cypress_proxy_rpc_ports,
                                     queue_agent_rpc_ports=None,
                                     config_template=None):
    queue_agent_rpc_ports = queue_agent_rpc_ports or []
    primary_cell_tag = master_connection_configs["primary_cell_tag"]
    secondary_cell_tags = master_connection_configs["secondary_cell_tags"]

    cluster_connection = {
        "cluster_name": yt_config.cluster_name,
        "cell_directory": _get_balancing_channel_config(),
        "primary_master": master_connection_configs[primary_cell_tag],
        "transaction_manager": {
            "default_ping_period": DEFAULT_TRANSACTION_PING_PERIOD,
        },
        "timestamp_provider": {
            # COMPAT(aleksandra-zh)
            "addresses": _get_timestamp_provider_addresses(yt_config, master_connection_configs,
                                                           clock_connection_config, timestamp_provider_addresses),
            "peers": _get_timestamp_provider_peer_configs(yt_config, master_connection_configs,
                                                          clock_connection_config, timestamp_provider_addresses),
            "update_period": 500,
            "soft_backoff_time": 100,
            "hard_backoff_time": 100,
        },
        "cell_directory_synchronizer": {
            "sync_period": 500,
            "sync_period_splay": 100,
        },
        "chaos_cell_directory_synchronizer": {
            "sync_period": 500,
            "sync_period_splay": 100,
            "sync_all_chaos_cells": True,
        },
        "cluster_directory_synchronizer": {
            "sync_period": 500,
            "expire_after_successful_update_time": 500,
            "expire_after_failed_update_time": 500,
        },
        "table_mount_cache": {
            "expire_after_successful_update_time": 0,
            "expire_after_failed_update_time": 0,
            "expire_after_access_time": 0,
            "refresh_time": 0,
        },
        "sync_replica_cache": {
            "expire_after_successful_update_time": 0,
            "expire_after_failed_update_time": 0,
            "expire_after_access_time": 0,
            "refresh_time": 0,
        },
        "queue_agent": {
            "stages": {
                "production": {"addresses": ["{}:{}".format(yt_config.fqdn, port) for port in queue_agent_rpc_ports]},
            },
            "queue_consumer_registration_manager": {
                "bypass_caching": True,
                "cache_refresh_period": 3000,
                "configuration_refresh_period": 500,
                "resolve_symlinks": True,
                "resolve_replicas": True,
            },
        },
        "permission_cache": {
            "expire_after_successful_update_time": 0,
            "expire_after_failed_update_time": 0,
            "expire_after_access_time": 0,
            "refresh_time": 0,
        },
        "group_attribute_cache": {
            "expire_after_successful_update_time": 0,
            "expire_after_failed_update_time": 0,
            "expire_after_access_time": 0,
            "refresh_time": 0,
        },
        "master_cell_directory_synchronizer": {
            "sync_period": 500,
            "expire_after_successful_update_time": 500,
            "expire_after_failed_update_time": 500,
        },
        "job_node_descriptor_cache": {
            "expire_after_successful_update_time": 0,
            "expire_after_failed_update_time": 0,
            "expire_after_access_time": 0,
            "refresh_time": 0,
        },
        "scheduler": {
            "enable_exponential_retry_backoffs": True,
            "retry_backoff": {
                "min_backoff": 10,
            },
            # COMPAT(pogorelov)
            "use_scheduler_job_prober_service": False,
        },
        "node_directory_synchronizer": {
            "sync_period": 500,
            "expire_after_successful_update_time": 500,
            "expire_after_failed_update_time": 500,
        },
        "chaos_residency_cache": {
            "expire_after_access_time": 72000000,  # 20h
            "expire_after_successful_update_time": 72000000,  # 20h
            "expire_after_failed_update_time": 100,
            "refresh_time": 0,
            "is_client_mode_active" : True,
        },
        "upload_transaction_timeout": 5000,
    }

    if len(cypress_proxy_rpc_ports) > 0:
        cypress_proxy_addresses = ["{}:{}".format(yt_config.fqdn, rpc_port) for rpc_port in cypress_proxy_rpc_ports]
        cluster_connection["cypress_proxy"] = {}
        update_inplace(cluster_connection["cypress_proxy"], _get_retrying_channel_config())
        update_inplace(cluster_connection["cypress_proxy"], _get_balancing_channel_config())
        cluster_connection["cypress_proxy"]["addresses"] = cypress_proxy_addresses

    if yt_config.mock_tvm_id is not None:
        cluster_connection["tvm_id"] = yt_config.mock_tvm_id

    update_inplace(cluster_connection["primary_master"], _get_retrying_channel_config())
    update_inplace(cluster_connection["primary_master"], _get_balancing_channel_config())
    update_inplace(cluster_connection["primary_master"], _get_rpc_config())

    cluster_connection["secondary_masters"] = []
    for tag in secondary_cell_tags:
        config = master_connection_configs[tag]
        update_inplace(config, _get_retrying_channel_config())
        update_inplace(config, _get_balancing_channel_config())
        update_inplace(config, _get_rpc_config())
        cluster_connection["secondary_masters"].append(config)

    if config_template is not None:
        cluster_connection = update_inplace(config_template, cluster_connection)

    discovery_server_addresses = cluster_connection["primary_master"]["addresses"]
    if yt_config.discovery_server_count > 0:
        discovery_server_addresses = discovery_configs[0]["discovery_server"]["server_addresses"]

    discovery_connection_config = {}
    discovery_connection_config["addresses"] = discovery_server_addresses
    cluster_connection["discovery_connection"] = discovery_connection_config

    if yt_config.enable_master_cache:
        cluster_connection["master_cache"] = {
            "enable_master_cache_discovery": len(master_cache_addresses) == 0,
            "master_cache_discovery_period": 100,
            "soft_backoff_time": 100,
            "hard_backoff_time": 100,
            "rpc_timeout": 25000,
            # Decrease the number of retry attempts to overcome the following issue:
            # when a master cache node becomes banned it takes this node a significant amount of time
            # to re-register again (once it becomes unbanned) since the node must wait
            # for metadata synchronizer iterations to fail before falling back to
            # direct master communication.
            "retry_attempts": 3,
            "addresses": master_cache_addresses,
            "cell_id": master_connection_configs[primary_cell_tag]["cell_id"],
        }

    if yt_config.clock_count > 0:
        cluster_connection["clock_servers"] = clock_connection_config

    if yt_config.chaos_node_count > 0:
        cluster_connection["replication_card_cache"] = {
            "expire_after_successful_update_time": 100,
            "expire_after_failed_update_time": 100,
            "expire_after_access_time": 100,
            "refresh_time": 50,
            "soft_backoff_time": 100,
            "hard_backoff_time": 100,
            "addresses": master_cache_addresses,
        }
        cluster_connection["chaos_cell_channel"] = {
            "enable_exponential_retry_backoffs": True,
            "retry_backoff": {
                "invocation_count": 10,
                "min_backoff": 50,
            },
            "rpc_acknowledge_timeout": 100,
        }
        cluster_connection["chaos_residency_cache"] = {
            "expire_after_successful_update_time": 100,
            "expire_after_failed_update_time": 100,
            "expire_after_access_time": 100,
            "refresh_time": 50,
        }

    if not yt_config.enable_permission_cache:
        cluster_connection["permission_cache"] = {
            "expire_after_successful_update_time": 0,
            "expire_after_failed_update_time": 0,
            "expire_after_access_time": 0,
            "refresh_time": 0
        }

    if yt_config.ca_cert is not None:
        set_at(cluster_connection, "bus_client", {
            "ca": {
                "file_name": yt_config.ca_cert,
            },
            "encryption_mode": "required",
            "verification_mode": "full",
            "peer_alternative_host_name": yt_config.cluster_name,
        })

    if yt_config.delta_global_cluster_connection_config:
        cluster_connection = update(cluster_connection, yt_config.delta_global_cluster_connection_config)

    return cluster_connection


def _build_tablet_balancer_configs(yt_config,
                                   multidaemon_config_output,
                                   master_connection_configs,
                                   clock_connection_config,
                                   discovery_configs,
                                   timestamp_provider_addresses,
                                   master_cache_addresses,
                                   cypress_proxy_rpc_ports,
                                   ports_generator,
                                   logs_dir):
    configs = []
    addresses = []

    for index in xrange(yt_config.tablet_balancer_count):
        config = default_config.get_tablet_balancer_config()

        singletons_config = multidaemon_config_output if yt_config.enable_multidaemon else config
        if not yt_config.enable_multidaemon:
            init_singletons(singletons_config, yt_config)
            _init_logging(logs_dir,
                          "tablet-balancer-" + str(index),
                          singletons_config.setdefault("logging", {}),
                          yt_config,
                          has_structured_logs=True)

        init_jaeger_collector(singletons_config, "tablet_balancer", {"tablet_balancer_index": str(index)})

        init_cypress_annotations(config, index)

        config["cluster_connection"] = \
            _build_cluster_connection_config(
                yt_config,
                master_connection_configs,
                clock_connection_config,
                discovery_configs,
                timestamp_provider_addresses,
                master_cache_addresses,
                cypress_proxy_rpc_ports)

        config["rpc_port"] = next(ports_generator)
        config["monitoring_port"] = next(ports_generator)

        multidaemon_config_output["daemons"][f"tablet_balancer_{index}"] = {
            "type": "tablet_balancer",
            "config": config,
        }
        configs.append(config)
        addresses.append("{}:{}".format(yt_config.fqdn, config["rpc_port"]))

    return configs, addresses


def _build_replicated_table_tracker_configs(yt_config,
                                            multidaemon_config_output,
                                            master_connection_configs,
                                            clock_connection_config,
                                            discovery_configs,
                                            timestamp_provider_addresses,
                                            master_cache_addresses,
                                            cypress_proxy_rpc_ports,
                                            ports_generator,
                                            logs_dir):
    configs = []

    for index in xrange(yt_config.replicated_table_tracker_count):
        config = default_config.get_replicated_table_tracker_config()

        singletons_config = multidaemon_config_output if yt_config.enable_multidaemon else config
        if not yt_config.enable_multidaemon:
            init_singletons(singletons_config, yt_config)

            _init_logging(logs_dir,
                          "replicated-table-tracker-" + str(index),
                          singletons_config.setdefault("logging", {}),
                          yt_config,
                          has_structured_logs=True)

        init_jaeger_collector(singletons_config, "replicated_table_tracker", {"replicated_table_tracker_index": str(index)})

        init_cypress_annotations(config, index)

        config["cluster_connection"] = \
            _build_cluster_connection_config(
                yt_config,
                master_connection_configs,
                clock_connection_config,
                discovery_configs,
                timestamp_provider_addresses,
                master_cache_addresses,
                cypress_proxy_rpc_ports)

        config["rpc_port"] = next(ports_generator)
        config["monitoring_port"] = next(ports_generator)

        multidaemon_config_output["daemons"][f"replicated_table_tracker_{index}"] = {
            "type": "replicated_table_tracker",
            "config": config,
        }
        configs.append(config)

    return configs


def _allocate_cypress_proxy_rpc_ports(yt_config, ports_generator):
    rpc_ports = []

    for i in xrange(yt_config.cypress_proxy_count):
        rpc_port = next(ports_generator)
        rpc_ports.append(rpc_port)

    return rpc_ports


def _build_cypress_proxy_configs(yt_config,
                                 multidaemon_config_output,
                                 master_connection_configs,
                                 clock_connection_config,
                                 discovery_configs,
                                 timestamp_provider_addresses,
                                 master_cache_addresses,
                                 cypress_proxy_rpc_ports,
                                 ports_generator,
                                 logs_dir):
    configs = []

    for index in xrange(yt_config.cypress_proxy_count):
        config = default_config.get_cypress_proxy_config()

        singletons_config = multidaemon_config_output if yt_config.enable_multidaemon else config
        if not yt_config.enable_multidaemon:
            init_singletons(singletons_config, yt_config)

            _init_logging(logs_dir,
                          "cypress-proxy-" + str(index),
                          singletons_config.setdefault("logging", {}),
                          yt_config,
                          has_structured_logs=True)

        init_jaeger_collector(singletons_config, "cypress_proxy", {"cypress_proxy_index": str(index)})

        init_cypress_annotations(config, index)

        config["cluster_connection"] = \
            _build_cluster_connection_config(
                yt_config,
                master_connection_configs,
                clock_connection_config,
                discovery_configs,
                timestamp_provider_addresses,
                master_cache_addresses,
                cypress_proxy_rpc_ports=[])

        config["rpc_port"] = cypress_proxy_rpc_ports[index]
        config["monitoring_port"] = next(ports_generator)

        multidaemon_config_output["daemons"][f"cypress_proxy_{index}"] = {
            "type": "cypress_proxy",
            "config": config,
        }
        configs.append(config)

    return configs


def _init_logging(path, name, logging_config, yt_config,
                  log_errors_to_stderr=False,
                  has_structured_logs=False,
                  enable_log_compression=None,
                  use_name_in_writer_name=True):
    if enable_log_compression is None:
        enable_log_compression = yt_config.enable_log_compression
    return init_logging(
        path,
        name,
        logging_config,
        enable_debug_logging=yt_config.enable_debug_logging,
        enable_log_compression=enable_log_compression,
        log_compression_method=yt_config.log_compression_method,
        enable_structured_logging=yt_config.enable_structured_logging and has_structured_logs,
        log_errors_to_stderr=log_errors_to_stderr,
        use_name_in_writer_name=use_name_in_writer_name)


def init_logging(path, name,
                 logging_config=None,
                 enable_debug_logging=False,
                 enable_log_compression=False,
                 log_compression_method="gzip",
                 enable_structured_logging=False,
                 abort_on_alert=None,
                 log_errors_to_stderr=False,
                 use_name_in_writer_name=True):
    def _get_writer_name(writer_name):
        if use_name_in_writer_name:
            return f"{writer_name}-{name}"
        return writer_name

    if logging_config is None:
        logging_config = {}

    if enable_log_compression and log_compression_method == "zstd":
        suffix = ".zst"
        compression_options = {
            "enable_compression": True,
            "compression_method": "zstd",
            "compression_level": 1,
        }
    elif enable_log_compression:
        suffix = ".gz"
        compression_options = {
            "enable_compression": True,
        }
    else:
        suffix = ""
        compression_options = {}

    if abort_on_alert is None:
        abort_on_alert = True

    if "abort_on_alert" not in logging_config:
        logging_config["abort_on_alert"] = abort_on_alert

    if "compression_thread_count" not in logging_config:
        logging_config["compression_thread_count"] = 4

    # Info logs.
    writer_name = _get_writer_name("info")
    logging_config.setdefault("rules", []).append({
        "min_level": "info",
        "writers": [writer_name],
        "family": "plain_text",
    })
    logging_config.setdefault("writers", {})[writer_name] = {
        "type": "file",
        "file_name": "{path}/{name}.log".format(path=path, name=name) + suffix,
    }
    logging_config["writers"][writer_name].update(compression_options)

    # Stderr.
    if log_errors_to_stderr:
        writer_name = _get_writer_name("stderr")
        logging_config["rules"].append(
            {
                "min_level": "error",
                "writers": [writer_name],
                "family": "plain_text",
            }
        )
        logging_config["writers"][writer_name] = {
            "type": "stderr",
        }

    # Debug logs.
    if enable_debug_logging:
        writer_name = _get_writer_name("debug")
        logging_config["rules"].append({
            "min_level": "debug",
            "family": "plain_text",
            "exclude_categories": ["Bus", "Concurrency"],
            "writers": [writer_name],
        })
        logging_config["writers"][writer_name] = {
            "type": "file",
            "file_name": "{path}/{name}.debug.log".format(path=path, name=name) + suffix,
        }
        logging_config["writers"][writer_name].update(compression_options)

    if "YT_ENABLE_TRACE_LOGGING" in os.environ:
        writer_name = _get_writer_name("trace")
        logging_config["rules"].append({
            "min_level": "trace",
            "family": "plain_text",
            "exclude_categories": ["Bus"],
            "writers": [writer_name],
        })
        logging_config["writers"][writer_name] = {
            "type": "file",
            "file_name": "{path}/{name}.trace.log".format(path=path, name=name) + suffix,
        }
        logging_config["writers"][writer_name].update(compression_options)

    if enable_structured_logging:
        writer_name = _get_writer_name("json")
        logging_config["rules"].append({
            "min_level": "debug",
            "writers": [writer_name],
            "family": "structured",
        })
        logging_config["writers"][writer_name] = {
            "type": "file",
            "file_name": "{path}/{name}.json.log".format(path=path, name=name),
            "accepted_message_format": "structured",
        }

    return logging_config


def set_at(config, path, value, merge=False):
    """Sets value in config by path creating intermediate dict nodes."""
    parts = path.split("/")
    for index, part in enumerate(parts):
        if index != len(parts) - 1:
            config = config.setdefault(part, {})
        else:
            if merge:
                config[part] = update(config.get(part, {}), value)
            else:
                config[part] = value


def get_at(config, path, default_value=None):
    for part in path.split("/"):
        if not isinstance(config, dict):
            raise ValueError("Path should not contain non-dict intermediate values")
        if part not in config:
            return default_value
        config = config[part]
    return config


def init_chunk_client_dispatcher(config):
    set_at(config, "chunk_client_dispatcher", {
        "chunk_reader_pool_size": 1,
    })


def init_cypress_annotations(config, index):
    set_at(config, "cypress_annotations/yt_env_index", index)


def init_singletons(config, yt_config):
    set_at(config, "stockpile", {
        "thread_count": 0,
    })
    set_at(config, "yp_service_discovery", {
        "enable": False,
    })
    init_chunk_client_dispatcher(config)
    set_at(config, "rpc_dispatcher", {
        "compression_pool_size": 1,
        "heavy_pool_size": 1,
        "alert_on_missing_request_info": True,
    })
    set_at(config, "chunk_client_dispatcher", {
        "chunk_reader_pool_size": 1,
    })
    set_at(config, "tcp_dispatcher", {
        "thread_pool_size": 2,
        "enable_local_bypass": True,
    })
    set_at(config, "address_resolver/localhost_fqdn", yt_config.fqdn)
    set_at(config, "solomon_exporter/grid_step", 1000)
    set_at(config, "enable_ref_counted_tracker_profiling", yt_config.enable_resource_tracking)
    # COMPAT(babenko): drop in 25.1
    set_at(config, "enable_resource_tracker", yt_config.enable_resource_tracking)
    set_at(config, "resource_tracker/enable", yt_config.enable_resource_tracking)
    if yt_config.mock_tvm_id is not None:
        set_at(config, "native_authentication_manager", {
            "tvm_service": {
                "enable_mock": True,
                "client_self_id": yt_config.mock_tvm_id,
                "client_enable_service_ticket_fetching": True,
                "client_enable_service_ticket_checking": True,
                "client_self_secret": "TestSecret-" + str(yt_config.mock_tvm_id),
            },
            "enable_validation": True,
        })
    if yt_config.rpc_cert is not None:
        set_at(config, "bus_server", {
            "encryption_mode": "optional",
            "cert_chain": {
                "file_name": yt_config.rpc_cert,
            },
            "private_key": {
                "file_name": yt_config.rpc_cert_key,
            },
        })


def init_jaeger_collector(config, name, process_tags):
    if "JAEGER_COLLECTOR" in os.environ:
        if "jaeger" not in config:
            set_at(config, "jaeger", {
                "service_name": name,
                "flush_period": 100,
                "collector_channel_config": {"address": os.environ["JAEGER_COLLECTOR"]},
                "enable_pid_tag": True,
                "process_tags": process_tags,
            })
        else:
            set_at(config, "jaeger/process_tags", process_tags)


def init_job_proxy_jaeger_collector(config, process_tags):
    if "JAEGER_COLLECTOR" in os.environ:
        set_at(config, "exec_node/job_proxy/job_proxy_jaeger", {
            "service_name": "job_proxy",
            "flush_period": 100,
            "collector_channel_config": {"address": os.environ["JAEGER_COLLECTOR"]},
            "enable_pid_tag": True,
            "process_tags": process_tags,
        })


def _get_hydra_manager_config():
    return {
        "leader_lease_check_period": 100,
        "leader_lease_timeout": 20000,
        "disable_leader_lease_grace_delay": True,
        "invariants_check_probability": 0.005,
        "response_keeper": _get_response_keeper_config()
    }


def _get_response_keeper_config():
    return {
        "enable_warmup": False,
        "expiration_time": 25000,
        "warmup_time": 30000,
    }


def _get_balancing_channel_config():
    return {
        "soft_backoff_time": 100,
        "hard_backoff_time": 100,
        "enable_peer_polling": True,
        "peer_polling_period": 5000,
        "peer_polling_period_splay": 100,
        "peer_polling_request_timeout": 100,
        "rediscover_period": 5000,
        "rediscover_splay": 500,
    }


def _get_retrying_channel_config():
    return {
        "enable_exponential_retry_backoffs": True,
        "retry_backoff": {
            "min_backoff": 100,
        },
    }


def _get_rpc_config():
    return {
        "rpc_timeout": 25000
    }


def _get_node_resource_limits_config(yt_config):
    FOOTPRINT_MEMORY = 1 * GB
    CHUNK_META_CACHE_MEMORY = 1 * GB
    BLOB_SESSIONS_MEMORY = 2 * GB

    memory = 0
    memory += yt_config.jobs_resource_limits.get("memory", 0)
    if yt_config.node_memory_limit_addition is not None:
        memory += yt_config.node_memory_limit_addition

    memory += FOOTPRINT_MEMORY
    memory += CHUNK_META_CACHE_MEMORY
    memory += BLOB_SESSIONS_MEMORY

    return {"memory": memory}


def _get_node_base_cgroup(cluster_name, index):
    if os.path.isdir("/run/systemd/system"):
        return "/yt.slice/yt-{}_node_{}.slice".format(cluster_name.replace('-', '_'), index)
    return "/yt/{}_node_{}".format(cluster_name, index)


def _get_node_job_environment_config(yt_config, index, logs_dir):
    return {
        "cri": {
            "type": "cri",
            "cri_executor": {
                "runtime_endpoint": yt_config.cri_endpoint,
                "image_endpoint": yt_config.cri_endpoint,
                "base_cgroup": _get_node_base_cgroup(yt_config.cluster_name, index),
                "namespace": "yt--{}-node-{}".format(yt_config.cluster_name, index),
                "verbose_logging": True,
            },
            "job_proxy_image": yt_config.default_docker_image,
            "use_job_proxy_from_image": False,
            "job_proxy_bind_mounts": [
                {
                    "internal_path":  logs_dir,
                    "external_path": logs_dir,
                    "read_only": False,
                },
            ],
        },
        "porto": {
            "type": "porto",
            "use_short_container_names": True,
        },
        "simple": {
            "type": "simple",
        },
    }[yt_config.jobs_environment_type]
