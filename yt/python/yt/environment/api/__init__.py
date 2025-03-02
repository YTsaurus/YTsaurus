try:
    import yt.packages.attr as attr
except ImportError:
    import attr


@attr.s
class LocalYtConfig(object):
    """High level local YT configuration"""
    path = attr.ib(None)
    tmpfs_path = attr.ib(None)
    fqdn = attr.ib("localhost")
    use_porto_for_servers = attr.ib(False)
    native_client_supported = attr.ib(False)
    kill_child_processes = attr.ib(False)
    run_watcher = attr.ib(False)
    optimize_config = attr.ib(False)
    address_resolver_config = attr.ib(None)
    enable_multidaemon = attr.ib(False)

    """Cluster initialization options"""
    initialize_world = attr.ib(False)
    wait_tablet_cell_initialization = attr.ib(False)
    init_operations_archive = attr.ib(False)
    local_cypress_dir = attr.ib(None)
    meta_files_suffix = attr.ib(".meta")
    cluster_name = attr.ib(None)
    wait_for_dynamic_config = attr.ib(True)

    """High level master configuration"""
    primary_cell_tag = attr.ib(1)
    has_ground = attr.ib(False)
    clock_cluster_tag = attr.ib(1)

    """High level node settings"""
    jobs_environment_type = attr.ib(None)
    jobs_resource_limits = attr.ib(factory=lambda: {
        "cpu": 1,
        "memory": 4 * (2 ** 30),
        "user_slots": 1,
    })
    node_memory_limit_addition = attr.ib(0)
    node_chunk_store_quota = attr.ib(None)
    allow_chunk_storage_in_tmpfs = attr.ib(False)
    node_io_engine_type = attr.ib(None)
    node_use_direct_io_for_reads = attr.ib("never")
    store_location_count = attr.ib(1)
    use_slot_user_id = attr.ib(True)
    cri_endpoint = attr.ib(None)
    # COMPAT(pogorelov)
    job_proxy_logging = attr.ib(factory=lambda: {
        "mode": "per_job_directory",
    })
    job_proxy_log_manager = attr.ib(factory=lambda: {
        "sharding_key_length": 1,
        "logs_storage_period": "7d",
        "directory_traversal_concurrency": None,
        "log_dump": {
            "log_writer_name": "debug",
        },
    })
    default_docker_image = "docker.io/library/python:3.8-slim"

    """Feature flags"""
    enable_master_cache = attr.ib(False)
    enable_permission_cache = attr.ib(False)
    enable_rpc_driver_proxy_discovery = attr.ib(False)
    enable_resource_tracking = attr.ib(False)
    enable_tvm_only_proxies = attr.ib(False)
    enable_chyt_http_proxies = attr.ib(False)
    enable_chyt_https_proxies = attr.ib(False)
    enable_auth = attr.ib(False)
    create_admin_user = attr.ib(False)

    """TLS settings"""
    enable_tls = attr.ib(False)
    ca_cert = attr.ib(None)
    ca_cert_key = attr.ib(None)
    rpc_cert = attr.ib(None)
    rpc_cert_key = attr.ib(None)
    https_cert = attr.ib(None)
    https_cert_key = attr.ib(None)

    """Native authentication settings"""
    mock_tvm_id = attr.ib(None)

    """Logging configuration"""
    enable_log_compression = attr.ib(False)
    enable_debug_logging = attr.ib(True)
    enable_structured_logging = attr.ib(False)
    log_compression_method = attr.ib("gzip")

    """Port settings"""
    http_proxy_ports = attr.ib(factory=list)
    https_proxy_ports = attr.ib(factory=list)
    rpc_proxy_ports = attr.ib(factory=list)
    discovery_server_ports = attr.ib(factory=list)

    port_locks_path = attr.ib(None)
    local_port_range = attr.ib(None)
    port_range_start = attr.ib(None)
    node_port_set_size = attr.ib(None)
    listen_port_pool = attr.ib(None)

    """Cluster shape"""
    master_count = attr.ib(1)
    nonvoting_master_count = attr.ib(0)
    clock_count = attr.ib(0)
    discovery_server_count = attr.ib(0)
    queue_agent_count = attr.ib(0)
    kafka_proxy_count = attr.ib(0)
    timestamp_provider_count = attr.ib(0)
    secondary_cell_count = attr.ib(0)
    scheduler_count = attr.ib(1)
    controller_agent_count = attr.ib(1)
    node_count = attr.ib(1)
    chaos_node_count = attr.ib(0)
    http_proxy_count = attr.ib(1)
    rpc_proxy_count = attr.ib(1)
    master_cache_count = attr.ib(0)
    remote_cluster_count = attr.ib(0)
    cell_balancer_count = attr.ib(0)
    enable_bundle_controller = attr.ib(False)
    tablet_balancer_count = attr.ib(0)
    cypress_proxy_count = attr.ib(0)
    replicated_table_tracker_count = attr.ib(0)

    """Start options"""
    defer_node_start = attr.ib(False)
    defer_chaos_node_start = attr.ib(False)
    defer_scheduler_start = attr.ib(False)
    defer_controller_agent_start = attr.ib(False)
    defer_secondary_cell_start = attr.ib(False)

    """Config patches"""
    delta_master_config = attr.ib(None)
    delta_clock_config = attr.ib(None)
    delta_scheduler_config = attr.ib(None)
    delta_queue_agent_config = attr.ib(None)
    delta_kafka_proxy_config = attr.ib(None)
    delta_controller_agent_config = attr.ib(None)
    delta_node_config = attr.ib(None)
    delta_http_proxy_config = attr.ib(None)
    delta_rpc_proxy_config = attr.ib(None)
    delta_driver_config = attr.ib(None)
    delta_master_cache_config = attr.ib(None)
    delta_global_cluster_connection_config = attr.ib(None)

    # COMPAT
    enable_legacy_logging_scheme = attr.ib(False)

    @enable_auth.validator
    def check_native_yson_available_for_auth_configuration(self, attribute, value):
        if value and not self.native_client_supported:
            raise ValueError("Native client support must be enabled to use authentication")

    @create_admin_user.validator
    def check_auth_enabled(self, attribute, value):
        if value and not self.enable_auth:
            raise ValueError("Auth must be enabled to create admin user")


__all__ = ["LocalYtConfig"]
