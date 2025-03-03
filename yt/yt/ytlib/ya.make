LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

INCLUDE(../RpcProxyProtocolVersion.txt)
INCLUDE(../ya_check_dependencies.inc)

PROTO_NAMESPACE(yt)

SRCS(
    admin/proto/admin_service.proto
    admin/proto/restart_service.proto

    api/connection.cpp
    api/native/backup_session.cpp
    api/native/bundle_controller_client_impl.cpp
    api/native/cell_commit_session.cpp
    api/native/chaos_helpers.cpp
    api/native/chaos_replicated_table_type_handler.cpp
    api/native/chaos_table_replica_type_handler.cpp
    api/native/client_admin_impl.cpp
    api/native/client_authentication_impl.cpp
    api/native/client_cache.cpp
    api/native/client_cypress_impl.cpp
    api/native/client_distributed_table_impl.cpp
    api/native/client_dynamic_tables_impl.cpp
    api/native/client_file_cache_impl.cpp
    api/native/client_files_impl.cpp
    api/native/client_impl.cpp
    api/native/client_internal_impl.cpp
    api/native/client_flow_impl.cpp
    api/native/client_job_info_impl.cpp
    api/native/client_jobs_impl.cpp
    api/native/client_journals_impl.cpp
    api/native/client_operation_info_impl.cpp
    api/native/client_operations_impl.cpp
    api/native/client_pools_impl.cpp
    api/native/client_queries_impl.cpp
    api/native/client_replicated_tables_impl.cpp
    api/native/client_security_impl.cpp
    api/native/client_shuffle_impl.cpp
    api/native/client_static_tables_impl.cpp
    api/native/client_transactions_impl.cpp
    api/native/config.cpp
    api/native/connection.cpp
    api/native/queue_consumer_type_handler.cpp
    api/native/default_type_handler.cpp
    api/native/file_reader.cpp
    api/native/file_writer.cpp
    api/native/helpers.cpp
    api/native/journal_reader.cpp
    api/native/journal_writer.cpp
    api/native/list_operations.cpp
    api/native/partition_tables.cpp
    api/native/pick_replica_session.cpp
    api/native/pipeline_type_handler.cpp
    api/native/queue_producer_type_handler.cpp
    api/native/replicated_table_replica_type_handler.cpp
    api/native/replication_card_collocation_type_handler.cpp
    api/native/replication_card_type_handler.cpp
    api/native/rpc_helpers.cpp
    api/native/skynet.cpp
    api/native/secondary_index_type_handler.cpp
    api/native/secondary_index_modification.cpp
    api/native/sticky_mount_cache.cpp
    api/native/sync_replica_cache.cpp
    api/native/table_collocation_type_handler.cpp
    api/native/table_reader.cpp
    api/native/table_replica_synchronicity_cache.cpp
    api/native/tablet_action_type_handler.cpp
    api/native/tablet_commit_session.cpp
    api/native/tablet_helpers.cpp
    api/native/tablet_request_batcher.cpp
    api/native/tablet_sync_replica_cache.cpp
    api/native/transaction_helpers.cpp
    api/native/transaction_participant.cpp
    api/native/transaction.cpp
    api/native/type_handler_detail.cpp
    api/native/ypath_helpers.cpp

    api/native/proto/transaction_actions.proto  # COMPAT(kvk1920)

    auth/config.cpp
    GLOBAL auth/configure_native_authentication_manager.cpp
    auth/native_authenticating_channel.cpp
    auth/native_authentication_manager.cpp
    auth/native_authenticator.cpp
    auth/tvm_bridge_service.cpp
    auth/tvm_bridge.cpp

    auth/proto/tvm_bridge_service.proto

    bundle_controller/config.cpp
    bundle_controller/bundle_controller_channel.cpp

    cell_balancer/proto/cell_tracker_service.proto

    cell_master_client/cell_directory_synchronizer.cpp
    cell_master_client/cell_directory.cpp
    cell_master_client/config.cpp
    cell_master_client/protobuf_helpers.cpp

    cellar_client/proto/tablet_cell_service.proto

    cellar_node_tracker_client/proto/cellar_node_tracker_service.proto
    cellar_node_tracker_client/proto/heartbeat.proto

    chaos_client/alien_cell.cpp
    chaos_client/banned_replica_tracker.cpp
    chaos_client/chaos_cell_channel_factory.cpp
    chaos_client/chaos_cell_directory_synchronizer.cpp
    chaos_client/chaos_residency_cache.cpp
    chaos_client/config.cpp
    chaos_client/master_cache_channel.cpp
    chaos_client/native_replication_card_cache_detail.cpp
    chaos_client/replication_card_channel_factory.cpp
    chaos_client/replication_cards_watcher.cpp
    chaos_client/replication_cards_watcher_client.cpp

    chaos_client/proto/alien_cell.proto
    chaos_client/proto/chaos_master_service.proto
    chaos_client/proto/chaos_node_service.proto
    chaos_client/proto/coordinator_service.proto

    chunk_client/block_cache.cpp
    chunk_client/block_fetcher.cpp
    chunk_client/block_id.cpp
    chunk_client/block_reorderer.cpp
    chunk_client/block_tracking_chunk_reader.cpp
    chunk_client/block.cpp
    chunk_client/cache_reader.cpp
    chunk_client/chunk_fragment_read_controller.cpp
    chunk_client/chunk_fragment_reader.cpp
    chunk_client/chunk_meta_cache.cpp
    GLOBAL chunk_client/chunk_meta_extensions.cpp
    chunk_client/chunk_meta_fetcher.cpp
    chunk_client/chunk_reader_host.cpp
    chunk_client/chunk_reader_memory_manager.cpp
    chunk_client/chunk_reader_statistics.cpp
    chunk_client/chunk_replica_cache.cpp
    chunk_client/chunk_scraper.cpp
    chunk_client/chunk_spec_fetcher.cpp
    chunk_client/chunk_spec.cpp
    chunk_client/chunk_teleporter.cpp
    chunk_client/chunk_writer_statistics.cpp
    chunk_client/client_block_cache.cpp
    chunk_client/combine_data_slices.cpp
    chunk_client/config.cpp
    GLOBAL chunk_client/configure_dispatcher.cpp
    chunk_client/confirming_writer.cpp
    chunk_client/data_sink.cpp
    chunk_client/data_slice_descriptor.cpp
    chunk_client/data_source.cpp
    chunk_client/deferred_chunk_meta.cpp
    chunk_client/dispatcher.cpp
    chunk_client/encoding_chunk_writer.cpp
    chunk_client/encoding_writer.cpp
    chunk_client/erasure_adaptive_repair.cpp
    chunk_client/erasure_helpers.cpp
    chunk_client/erasure_part_reader.cpp
    chunk_client/erasure_part_writer.cpp
    chunk_client/erasure_reader.cpp
    chunk_client/erasure_repair.cpp
    chunk_client/erasure_writer.cpp
    chunk_client/fetcher.cpp
    chunk_client/format.cpp
    chunk_client/helpers.cpp
    chunk_client/input_chunk_slice.cpp
    chunk_client/input_chunk.cpp
    GLOBAL chunk_client/job_spec_extensions.cpp
    chunk_client/legacy_data_slice.cpp
    chunk_client/medium_directory_synchronizer.cpp
    chunk_client/medium_directory.cpp
    chunk_client/memory_reader.cpp
    chunk_client/memory_tracked_deferred_chunk_meta.cpp
    chunk_client/memory_writer.cpp
    chunk_client/meta_aggregating_writer.cpp
    chunk_client/multi_chunk_writer_base.cpp
    chunk_client/multi_reader_manager_base.cpp
    chunk_client/parallel_multi_reader_manager.cpp
    chunk_client/parallel_reader_memory_manager.cpp
    chunk_client/preloaded_block_cache.cpp
    chunk_client/public.cpp
    chunk_client/reader_factory.cpp
    chunk_client/replication_reader.cpp
    chunk_client/replication_writer.cpp
    chunk_client/sequential_multi_reader_manager.cpp
    chunk_client/session_id.cpp
    chunk_client/striped_erasure_reader.cpp
    chunk_client/striped_erasure_writer.cpp
    chunk_client/throttler_manager.cpp
    chunk_client/traffic_meter.cpp

    chunk_client/proto/block_id.proto
    chunk_client/proto/chunk_info.proto
    chunk_client/proto/chunk_owner_ypath.proto
    chunk_client/proto/chunk_reader_statistics.proto
    chunk_client/proto/chunk_service.proto
    chunk_client/proto/chunk_slice.proto
    chunk_client/proto/chunk_writer_statistics.proto
    chunk_client/proto/data_node_service.proto
    chunk_client/proto/data_sink.proto
    chunk_client/proto/data_source.proto
    chunk_client/proto/heartbeat.proto
    chunk_client/proto/medium_directory.proto
    chunk_client/proto/session_id.proto

    chunk_pools/chunk_pool_factory.cpp
    chunk_pools/chunk_stripe_key.cpp
    chunk_pools/chunk_stripe.cpp
    chunk_pools/output_order.cpp

    controller_agent/helpers.cpp
    controller_agent/persistence.cpp
    controller_agent/public.cpp
    controller_agent/serialize.cpp

    controller_agent/proto/controller_agent_descriptor.proto
    controller_agent/proto/controller_agent_service.proto
    controller_agent/proto/job_prober_service.proto
    controller_agent/proto/job.proto
    controller_agent/proto/output_result.proto

    coverage/proto/coverage.proto
    coverage/coverage_service.cpp
    coverage/helpers.cpp

    cypress_client/batch_attribute_fetcher.cpp
    cypress_client/public.cpp
    cypress_client/rpc_helpers.cpp

    cypress_client/proto/cypress_ypath.proto
    cypress_client/proto/rpc.proto

    cypress_server/proto/sequoia_actions.proto

    cypress_transaction_client/proto/cypress_transaction_service.proto

    data_node_tracker_client/location_directory.cpp

    data_node_tracker_client/proto/data_node_tracker_service.proto

    distributed_chunk_session_client/config.cpp
    distributed_chunk_session_client/distributed_chunk_writer.cpp
    distributed_chunk_session_client/distributed_chunk_session_controller.cpp

    distributed_chunk_session_client/proto/distributed_chunk_session_service.proto

    driver/config.cpp

    election/cell_manager.cpp
    election/config.cpp
    election/public.cpp

    election/proto/election_service.proto

    event_log/config.cpp
    event_log/event_log.cpp

    exec_node_admin/proto/exec_node_admin_service.proto
    exec_node_tracker_client/proto/exec_node_tracker_service.proto

    file_client/file_chunk_output.cpp
    file_client/file_chunk_reader.cpp
    file_client/file_chunk_writer.cpp

    file_client/proto/file_chunk_meta.proto

    hive/cell_directory_synchronizer.cpp
    hive/cell_directory.cpp
    hive/cluster_directory_synchronizer.cpp
    hive/cluster_directory.cpp
    hive/config.cpp
    hive/downed_cell_tracker.cpp
    hive/public.cpp

    hive/proto/cell_directory.proto
    hive/proto/hive_service.proto

    hydra/config.cpp
    hydra/helpers.cpp
    hydra/peer_channel.cpp
    hydra/peer_discovery.cpp

    hydra/proto/hydra_manager.proto
    hydra/proto/hydra_service.proto

    incumbent_client/incumbent_descriptor.cpp
    incumbent_client/public.cpp

    job_prober_client/job_shell_descriptor_cache.cpp

    job_prober_client/proto/job_prober_service.proto

    job_proxy/any_to_composite_converter.cpp
    job_proxy/config.cpp
    job_proxy/helpers.cpp
    job_proxy/job_spec_helper.cpp
    job_proxy/private.cpp
    job_proxy/user_job_io_factory.cpp
    job_proxy/user_job_read_controller.cpp

    journal_client/chunk_reader.cpp
    journal_client/config.cpp
    journal_client/erasure_parts_reader.cpp
    journal_client/erasure_repair.cpp
    journal_client/helpers.cpp
    journal_client/journal_chunk_writer.cpp
    journal_client/journal_hunk_chunk_writer.cpp
    journal_client/public.cpp

    journal_client/proto/format.proto
    journal_client/proto/journal_ypath.proto

    lease_client/proto/lease_service.proto

    misc/memory_usage_tracker.cpp
    misc/synchronizer_detail.cpp

    columnar_chunk_format/column_block_manager.cpp
    columnar_chunk_format/memory_helpers.cpp
    columnar_chunk_format/prepared_meta.cpp
    columnar_chunk_format/rowset_builder.cpp
    columnar_chunk_format/segment_readers.cpp
    columnar_chunk_format/versioned_chunk_reader.cpp

    node_tracker_client/channel.cpp
    node_tracker_client/config.cpp
    node_tracker_client/helpers.cpp
    node_tracker_client/node_addresses_provider.cpp
    node_tracker_client/node_directory_builder.cpp
    node_tracker_client/node_directory_synchronizer.cpp
    node_tracker_client/node_statistics.cpp
    node_tracker_client/node_status_directory.cpp
    node_tracker_client/public.cpp

    node_tracker_client/proto/node_tracker_service.proto

    object_client/caching_object_service.cpp
    object_client/config.cpp
    object_client/helpers.cpp
    object_client/master_ypath_proxy.cpp
    object_client/object_attribute_cache.cpp
    object_client/object_service_cache.cpp
    object_client/object_service_proxy.cpp
    object_client/public.cpp

    object_client/proto/master_ypath.proto
    object_client/proto/object_service.proto
    object_client/proto/object_ypath.proto
    object_client/proto/user_directory.proto

    orchid/orchid_service.cpp
    orchid/orchid_ypath_service.cpp

    orchid/proto/orchid_service.proto

    query_client/executor.cpp
    query_client/explain.cpp
    query_client/functions_cache.cpp
    query_client/session_coordinator.cpp
    query_client/tracked_memory_chunk_provider.cpp

    queue_client/config.cpp
    queue_client/queue_consumer_init.cpp
    queue_client/dynamic_state.cpp
    queue_client/helpers.cpp
    queue_client/producer_init.cpp
    queue_client/registration_manager.cpp

    replicated_table_tracker_client/proto/replicated_table_tracker_client.proto

    scheduler/cluster_name.cpp
    scheduler/config.cpp
    scheduler/disk_resources.cpp
    scheduler/helpers.cpp
    scheduler/job_resources_helpers.cpp
    scheduler/job_resources_with_quota.cpp
    scheduler/public.cpp
    scheduler/scheduler_channel.cpp

    scheduler/proto/allocation.proto
    scheduler/proto/resources.proto
    scheduler/proto/rich_transaction_id.proto
    scheduler/proto/pool_ypath.proto
    scheduler/proto/scheduler_service.proto

    security_client/config.cpp
    security_client/helpers.cpp
    security_client/permission_cache.cpp
    security_client/public.cpp
    security_client/user_attribute_cache.cpp

    security_client/proto/account_ypath.proto
    security_client/proto/group_ypath.proto
    security_client/proto/user_ypath.proto

    sequoia_client/client.cpp
    sequoia_client/helpers.cpp
    sequoia_client/public.cpp
    sequoia_client/record_helpers.cpp
    sequoia_client/table_descriptor.cpp
    sequoia_client/transaction.cpp
    sequoia_client/write_set.cpp

    sequoia_client/proto/transaction_client.proto

    table_chunk_format/boolean_column_reader.cpp
    table_chunk_format/boolean_column_writer.cpp
    table_chunk_format/column_reader_detail.cpp
    table_chunk_format/column_reader.cpp
    table_chunk_format/column_writer_detail.cpp
    table_chunk_format/column_writer.cpp
    table_chunk_format/column_meta.cpp
    table_chunk_format/data_block_writer.cpp
    table_chunk_format/floating_point_column_reader.cpp
    table_chunk_format/floating_point_column_writer.cpp
    table_chunk_format/integer_column_reader.cpp
    table_chunk_format/integer_column_writer.cpp
    table_chunk_format/null_column_reader.cpp
    table_chunk_format/null_column_writer.cpp
    table_chunk_format/reader_helpers.cpp
    table_chunk_format/schemaless_column_reader.cpp
    table_chunk_format/schemaless_column_writer.cpp
    table_chunk_format/slim_versioned_block_reader.cpp
    table_chunk_format/slim_versioned_block_writer.cpp
    table_chunk_format/string_column_reader.cpp
    table_chunk_format/string_column_writer.cpp
    table_chunk_format/timestamp_reader.cpp
    table_chunk_format/timestamp_writer.cpp

    table_client/blob_table_writer.cpp
    table_client/cache_based_versioned_chunk_reader.cpp
    table_client/cached_versioned_chunk_meta.cpp
    table_client/chunk_column_mapping.cpp
    table_client/chunk_index_builder.cpp
    table_client/chunk_index_read_controller.cpp
    table_client/chunk_index.cpp
    table_client/chunk_lookup_hash_table.cpp
    GLOBAL table_client/chunk_meta_extensions.cpp
    table_client/chunk_reader_base.cpp
    table_client/chunk_slice_fetcher.cpp
    table_client/chunk_slice_size_fetcher.cpp
    table_client/chunk_slice.cpp
    table_client/column_filter_dictionary.cpp
    table_client/columnar_chunk_meta.cpp
    table_client/columnar_chunk_reader_base.cpp
    table_client/columnar_statistics_fetcher.cpp
    table_client/config.cpp
    table_client/dictionary_compression_session.cpp
    table_client/granule_min_max_filter.cpp
    table_client/helpers.cpp
    table_client/hunks.cpp
    table_client/indexed_versioned_chunk_reader.cpp
    table_client/key_filter.cpp
    table_client/key_set.cpp
    table_client/nested_row_merger.cpp
    table_client/overlapping_reader.cpp
    table_client/partition_chunk_reader.cpp
    table_client/partition_sort_reader.cpp
    table_client/partitioner.cpp
    table_client/performance_counters.cpp
    table_client/remote_dynamic_store_reader.cpp
    table_client/row_merger.cpp
    table_client/samples_fetcher.cpp
    table_client/schema_dictionary.cpp
    table_client/schema_inferer.cpp
    table_client/schema.cpp
    table_client/schemaful_chunk_reader.cpp
    table_client/schemaful_concatenating_reader.cpp
    table_client/schemaful_reader_adapter.cpp
    table_client/schemaless_block_reader.cpp
    table_client/schemaless_block_writer.cpp
    table_client/schemaless_buffered_table_writer.cpp
    table_client/schemaless_chunk_reader.cpp
    table_client/schemaless_chunk_writer.cpp
    table_client/schemaless_multi_chunk_reader.cpp
    table_client/skynet_column_evaluator.cpp
    table_client/slice_boundary_key.cpp
    table_client/sorted_merging_reader.cpp
    table_client/sorting_reader.cpp
    table_client/table_columnar_statistics_cache.cpp
    table_client/table_read_spec.cpp
    table_client/timing_reader.cpp
    table_client/timing_statistics.cpp
    table_client/versioned_block_reader.cpp
    table_client/versioned_block_writer.cpp
    table_client/versioned_chunk_reader.cpp
    table_client/versioned_chunk_writer.cpp
    table_client/versioned_offloading_reader.cpp
    table_client/versioned_reader_adapter.cpp
    table_client/versioned_row_digest.cpp
    table_client/versioned_row_merger.cpp
    table_client/virtual_value_directory.cpp

    table_client/proto/table_ypath.proto
    table_client/proto/virtual_value_directory.proto

    tablet_client/backup.cpp
    tablet_client/config.cpp
    tablet_client/helpers.cpp
    tablet_client/native_table_mount_cache.cpp
    tablet_client/pivot_keys_builder.cpp
    tablet_client/pivot_keys_picker.cpp
    tablet_client/public.cpp

    tablet_client/proto/backup.proto
    tablet_client/proto/heartbeat.proto
    tablet_client/proto/master_tablet_service.proto
    tablet_client/proto/table_replica_ypath.proto
    tablet_client/proto/tablet_cell_bundle_ypath.proto
    tablet_client/proto/tablet_ypath.proto
    tablet_client/proto/tablet_service.proto

    tablet_node_tracker_client/proto/tablet_node_tracker_service.proto

    transaction_client/action.cpp
    transaction_client/clock_manager.cpp
    transaction_client/config.cpp
    transaction_client/helpers.cpp
    transaction_client/remote_cluster_timestamp_provider.cpp
    transaction_client/transaction_listener.cpp
    transaction_client/transaction_manager.cpp

    transaction_client/proto/action.proto
    transaction_client/proto/transaction_service.proto

    transaction_supervisor/proto/transaction_participant_service.proto
    transaction_supervisor/proto/transaction_supervisor_service.proto

    yql_client/config.cpp

    yql_client/proto/yql_service.proto

    shuffle_client/proto/shuffle_service.proto
)

GENERATE_YT_RECORD(
    sequoia_client/records/chunk_replicas.yaml
    OUTPUT_INCLUDES
        yt/yt/ytlib/sequoia_client/public.h
        yt/yt/client/node_tracker_client/public.h
        library/cpp/yt/yson_string/string.h
)

GENERATE_YT_RECORD(
    sequoia_client/records/location_replicas.yaml
    OUTPUT_INCLUDES
        yt/yt/ytlib/sequoia_client/public.h
        yt/yt/client/node_tracker_client/public.h
        yt/yt/client/object_client/public.h
)

GENERATE_YT_RECORD(
    sequoia_client/records/unapproved_chunk_replicas.yaml
    OUTPUT_INCLUDES
        yt/yt/ytlib/sequoia_client/public.h
        yt/yt/client/node_tracker_client/public.h
        library/cpp/yt/yson_string/string.h
)

GENERATE_YT_RECORD(
    sequoia_client/records/chunk_refresh_queue.yaml
    OUTPUT_INCLUDES
        yt/yt/ytlib/sequoia_client/public.h
)

GENERATE_YT_RECORD(
    sequoia_client/records/child_node.yaml
    OUTPUT_INCLUDES
        yt/yt/ytlib/sequoia_client/public.h
)

GENERATE_YT_RECORD(
    sequoia_client/records/node_id_to_path.yaml
    OUTPUT_INCLUDES
        yt/yt/ytlib/sequoia_client/public.h
)

GENERATE_YT_RECORD(
    sequoia_client/records/path_to_node_id.yaml
    OUTPUT_INCLUDES
        yt/yt/ytlib/sequoia_client/public.h
)

GENERATE_YT_RECORD(
    sequoia_client/records/transactions.yaml
    OUTPUT_INCLUDES
        yt/yt/ytlib/sequoia_client/public.h
        yt/yt/core/ytree/attributes.h
)

GENERATE_YT_RECORD(
    sequoia_client/records/transaction_descendants.yaml
    OUTPUT_INCLUDES
        yt/yt/ytlib/sequoia_client/public.h
)

GENERATE_YT_RECORD(
    sequoia_client/records/transaction_replicas.yaml
    OUTPUT_INCLUDES
        yt/yt/ytlib/sequoia_client/public.h
)

GENERATE_YT_RECORD(
    sequoia_client/records/dependent_transactions.yaml
    OUTPUT_INCLUDES
        yt/yt/ytlib/sequoia_client/public.h
)

GENERATE_YT_RECORD(
    sequoia_client/records/node_forks.yaml
    OUTPUT_INCLUDES
        yt/yt/ytlib/sequoia_client/public.h
)

GENERATE_YT_RECORD(
    sequoia_client/records/node_snapshots.yaml
    OUTPUT_INCLUDES
        yt/yt/ytlib/sequoia_client/public.h
)

GENERATE_YT_RECORD(
    sequoia_client/records/path_forks.yaml
    OUTPUT_INCLUDES
        yt/yt/ytlib/sequoia_client/public.h
)

GENERATE_YT_RECORD(
    sequoia_client/records/child_forks.yaml
    OUTPUT_INCLUDES
        yt/yt/ytlib/sequoia_client/public.h
)

GENERATE_YT_RECORD(
    sequoia_client/records/response_keeper.yaml
    OUTPUT_INCLUDES
        yt/yt/ytlib/sequoia_client/public.h
)

GENERATE_YT_RECORD(
    scheduler/records/operation_alias.yaml
)

GENERATE_YT_RECORD(
    scheduler/records/job_fail_context.yaml
)

GENERATE_YT_RECORD(
    scheduler/records/operation_id.yaml
)

GENERATE_YT_RECORD(
    scheduler/records/ordered_by_start_time.yaml
    OUTPUT_INCLUDES
        yt/yt/core/yson/string.h
)

GENERATE_YT_RECORD(
    scheduler/records/job.yaml
    OUTPUT_INCLUDES
        yt/yt/core/yson/string.h
)

GENERATE_YT_RECORD(
    scheduler/records/ordered_by_id.yaml
    OUTPUT_INCLUDES
        yt/yt/core/yson/string.h
)

GENERATE_YT_RECORD(
    scheduler/records/job_trace_event.yaml
)

GENERATE_YT_RECORD(
    scheduler/records/job_profile.yaml
)

GENERATE_YT_RECORD(
    scheduler/records/job_stderr.yaml
)

GENERATE_YT_RECORD(
    scheduler/records/job_spec.yaml
)

GENERATE_YT_RECORD(
    queue_client/records/queue_producer_session.yaml
    OUTPUT_INCLUDES
        yt/yt/client/queue_client/public.h
        yt/yt/core/yson/string.h
)

GENERATE_YT_RECORD(
    queue_client/records/queue_object.yaml
    OUTPUT_INCLUDES
        yt/yt/core/yson/string.h
        yt/yt/client/object_client/public.h
        yt/yt/ytlib/queue_client/public.h
)

GENERATE_YT_RECORD(
    queue_client/records/consumer_object.yaml
    OUTPUT_INCLUDES
        yt/yt/core/yson/string.h
        yt/yt/ytlib/queue_client/public.h
)

GENERATE_YT_RECORD(
    queue_client/records/consumer_registration.yaml
    OUTPUT_INCLUDES
        yt/yt/core/yson/string.h
)

GENERATE_YT_RECORD(
    queue_client/records/queue_agent_object_mapping.yaml
    OUTPUT_INCLUDES
        yt/yt/core/yson/string.h
)

GENERATE_YT_RECORD(
    queue_client/records/replicated_table_mapping.yaml
    OUTPUT_INCLUDES
        yt/yt/core/yson/string.h
        yt/yt/ytlib/queue_client/public.h
)

ADDINCL(
    contrib/libs/sparsehash/src
)

PEERDIR(
    contrib/libs/re2
    contrib/libs/protobuf
    contrib/libs/yajl
    library/cpp/erasure
    library/cpp/iterator
    library/cpp/yt/backtrace/symbolizers/dwarf
    yt/yt/library/erasure/impl
    yt/yt/library/containers
    yt/yt/library/disk_manager
    yt/yt/library/process
    yt/yt/library/random
    yt/yt/core
    yt/yt/core/http
    yt/yt/library/auth_server
    yt/yt/library/numeric
    yt/yt/library/heavy_schema_validation
    yt/yt/library/quantile_digest
    yt/yt/library/tvm/service
    yt/yt/library/xor_filter
    yt/yt/client
    yt/yt/client/logging
    yt/yt/library/formats
    yt/yt/library/query/engine_api
    yt/yt/library/query/row_comparer_api
    yt/yt/library/query/secondary_index
    yt/yt/library/web_assembly/api
    yt/yt/library/profiling/solomon
    yt/yt/library/program
    yt/yt/library/vector_hdrf
    yt/yt/ytlib/discovery_client
    yt/yt/ytlib/query_tracker_client
    yt/yt_proto/yt/client
    yt/yt/flow/lib/client
    yt/yt/flow/lib/native_client
)

END()

RECURSE(
    discovery_client
    distributed_throttler
)

IF (NOT OPENSOURCE)
    RECURSE(
        benchmarks
    )
ENDIF()

RECURSE_FOR_TESTS(
    unittests
    misc/unittests
    table_client/unittests
    tablet_client/unittests
    queue_client/unittests
    table_chunk_format/unittests
)
