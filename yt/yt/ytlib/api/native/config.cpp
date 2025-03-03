#include "config.h"

#include <yt/yt/ytlib/hive/config.h>

#include <yt/yt/ytlib/node_tracker_client/config.h>

#include <yt/yt/ytlib/bundle_controller/config.h>

#include <yt/yt/ytlib/discovery_client/config.h>

#include <yt/yt/ytlib/scheduler/config.h>

#include <yt/yt/ytlib/table_client/config.h>

#include <yt/yt/ytlib/chaos_client/config.h>

#include <yt/yt/ytlib/security_client/config.h>

#include <yt/yt/ytlib/object_client/config.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/ytlib/queue_client/config.h>

#include <yt/yt/ytlib/query_tracker_client/config.h>

#include <yt/yt/ytlib/yql_client/config.h>

#include <yt/yt/ytlib/transaction_client/config.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/transaction_client/config.h>

#include <yt/yt/library/query/engine_api/config.h>

#include <yt/yt/core/bus/tcp/config.h>

namespace NYT::NApi::NNative {

using namespace NObjectClient;
using namespace NTransactionClient;
using namespace NDiscoveryClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void TMasterConnectionConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("rpc_timeout", &TThis::RpcTimeout)
        .Default(TDuration::Seconds(30));

    registrar.Preprocessor([] (TThis* config) {
        config->RetryAttempts = 100;
        config->RetryTimeout = TDuration::Minutes(3);
    });
}

////////////////////////////////////////////////////////////////////////////////

void TMasterCacheConnectionConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_master_cache_discovery", &TThis::EnableMasterCacheDiscovery)
        .Default(true);
    registrar.Parameter("master_cache_discovery_period", &TThis::MasterCacheDiscoveryPeriod)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("master_cache_discovery_period_splay", &TThis::MasterCacheDiscoveryPeriodSplay)
        .Default(TDuration::Seconds(10));

    registrar.Postprocessor([] (TThis* config) {
        if (config->EnableMasterCacheDiscovery && config->Endpoints) {
            THROW_ERROR_EXCEPTION("Cannot specify \"endpoints\" when master cache discovery is enabled");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TClockServersConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("rpc_timeout", &TThis::RpcTimeout)
        .Default(TDuration::Seconds(30));
}

////////////////////////////////////////////////////////////////////////////////

void TCypressProxyConnectionConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("rpc_timeout", &TThis::RpcTimeout)
        .Default(TDuration::Seconds(30));
}

////////////////////////////////////////////////////////////////////////////////

void TSequoiaConnectionConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("ground_cluster_name", &TThis::GroundClusterName)
        .Default();

    registrar.Parameter("sequoia_root_path", &TThis::SequoiaRootPath)
        .Default("//sys/sequoia");

    registrar.Parameter("sequoia_transaction_timeout", &TThis::SequoiaTransactionTimeout)
        .Default(TDuration::Minutes(1));
}

////////////////////////////////////////////////////////////////////////////////

void TConnectionStaticConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("networks", &TThis::Networks)
        .Default();
    registrar.Parameter("timestamp_provider", &TThis::TimestampProvider)
        .Default();
    registrar.Parameter("clock_servers", &TThis::ClockServers)
        .Default();
    registrar.Parameter("cypress_proxy", &TThis::CypressProxy)
        .Default();
    registrar.Parameter("master_cell_directory_synchronizer", &TThis::MasterCellDirectorySynchronizer)
        .DefaultNew();
    registrar.Parameter("chaos_cell_directory_synchronizer", &TThis::ChaosCellDirectorySynchronizer)
        .DefaultNew();
    registrar.Parameter("clock_manager", &TThis::ClockManager)
        .DefaultNew();
    registrar.Parameter("sync_replica_cache", &TThis::SyncReplicaCache)
        .DefaultNew();
    registrar.Parameter("connection_name", &TThis::ConnectionName)
        .Alias("name")
        .Default("default");
    registrar.Parameter("banned_replica_tracker_cache", &TThis::BannedReplicaTrackerCache)
        .DefaultNew();
}

void TConnectionStaticConfig::OverrideMasterAddresses(const std::vector<std::string>& addresses)
{
    auto patchMasterConnectionConfig = [&] (const TMasterConnectionConfigPtr& config) {
        config->Addresses = addresses;
        config->Endpoints = nullptr;
        if (config->RetryTimeout && *config->RetryTimeout > config->RpcTimeout) {
            config->RpcTimeout = *config->RetryTimeout;
        }
        config->RetryTimeout = std::nullopt;
        config->RetryAttempts = 1;
        config->IgnorePeerState = true;
    };

    patchMasterConnectionConfig(PrimaryMaster);
    for (const auto& config : SecondaryMasters) {
        patchMasterConnectionConfig(config);
    }
    if (!MasterCache) {
        MasterCache = New<TMasterCacheConnectionConfig>();
        MasterCache->Load(ConvertToNode(PrimaryMaster));
    }
    patchMasterConnectionConfig(MasterCache);
    MasterCache->EnableMasterCacheDiscovery = false;

    MasterCellDirectorySynchronizer->RetryPeriod = std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

void TConnectionDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cell_directory", &TThis::CellDirectory)
        .DefaultNew();
    registrar.Parameter("cell_directory_synchronizer", &TThis::CellDirectorySynchronizer)
        .DefaultNew();
    registrar.Parameter("downed_cell_tracker", &TThis::DownedCellTracker)
        .DefaultNew();
    registrar.Parameter("scheduler", &TThis::Scheduler)
        .DefaultNew();
    registrar.Parameter("bundle_controller", &TThis::BundleController)
        .DefaultNew();
    registrar.Parameter("queue_agent", &TThis::QueueAgent)
        .DefaultNew();
    registrar.Parameter("query_tracker", &TThis::QueryTracker)
        .DefaultNew();
    registrar.Parameter("yql_agent", &TThis::YqlAgent)
        .DefaultNew();
    registrar.Parameter("transaction_manager", &TThis::TransactionManager)
        .DefaultNew();
    registrar.Parameter("block_cache", &TThis::BlockCache)
        .DefaultNew();
    registrar.Parameter("chunk_meta_cache", &TThis::ChunkMetaCache)
        .DefaultNew();
    registrar.Parameter("chunk_replica_cache", &TThis::ChunkReplicaCache)
        .DefaultNew();
    registrar.Parameter("replication_card_cache", &TThis::ReplicationCardCache)
        .Optional();
    registrar.Parameter("cluster_directory_synchronizer", &TThis::ClusterDirectorySynchronizer)
        .DefaultNew();
    registrar.Parameter("medium_directory_synchronizer", &TThis::MediumDirectorySynchronizer)
        .DefaultNew();
    registrar.Parameter("node_directory_synchronizer", &TThis::NodeDirectorySynchronizer)
        .DefaultNew();
    registrar.Parameter("chunk_slice_fetcher", &TThis::ChunkSliceFetcher)
        .DefaultNew();
    registrar.Parameter("discovery_connection", &TThis::DiscoveryConnection)
        .Optional();

    registrar.Parameter("query_evaluator", &TThis::QueryEvaluator)
        .DefaultNew();
    registrar.Parameter("default_select_rows_timeout", &TThis::DefaultSelectRowsTimeout)
        // COMPAT(babenko)
        .Alias("query_timeout")
        .Default(TDuration::Seconds(60));
    registrar.Parameter("select_rows_response_codec", &TThis::SelectRowsResponseCodec)
        // COMPAT(babenko)
        .Alias("query_response_codec")
        .Default(NCompression::ECodec::Lz4);
    registrar.Parameter("default_input_row_limit", &TThis::DefaultInputRowLimit)
        .GreaterThan(0)
        .Default(1'000'000);
    registrar.Parameter("default_output_row_limit", &TThis::DefaultOutputRowLimit)
        .GreaterThan(0)
        .Default(1'000'000);

    registrar.Parameter("distributed_query_session_ping_period", &TThis::DistributedQuerySessionPingPeriod)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("distributed_query_session_retention_time", &TThis::DistributedQuerySessionRetentionTime)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("distributed_query_session_control_rpc_timeout", &TThis::DistributedQuerySessionControlRpcTimeout)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("column_evaluator_cache", &TThis::ColumnEvaluatorCache)
        .DefaultNew();

    registrar.Parameter("expression_evaluator_cache", &TThis::ExpressionEvaluatorCache)
        .DefaultNew();

    registrar.Parameter("write_rows_timeout", &TThis::WriteRowsTimeout)
        // COMPAT(babenko)
        .Alias("write_timeout")
        .Default(TDuration::Seconds(60));
    registrar.Parameter("write_rows_request_codec", &TThis::WriteRowsRequestCodec)
        // COMPAT(babenko)
        .Alias("write_request_codec")
        .Default(NCompression::ECodec::Lz4);
    registrar.Parameter("max_rows_per_write_request", &TThis::MaxRowsPerWriteRequest)
        .GreaterThan(0)
        .Default(1'000);
    registrar.Parameter("max_data_weight_per_write_request", &TThis::MaxDataWeightPerWriteRequest)
        .GreaterThan(0)
        .Default(64_MB);
    registrar.Parameter("max_rows_per_transaction", &TThis::MaxRowsPerTransaction)
        .GreaterThan(0)
        .Default(100'000);

    registrar.Parameter("default_lookup_rows_timeout", &TThis::DefaultLookupRowsTimeout)
        // COMPAT(babenko)
        .Alias("lookup_timeout")
        .Default(TDuration::Seconds(60));
    registrar.Parameter("lookup_rows_request_codec", &TThis::LookupRowsRequestCodec)
        .Alias("lookup_request_codec")
        .Default(NCompression::ECodec::Lz4);
    registrar.Parameter("lookup_rows_response_codec", &TThis::LookupRowsResponseCodec)
        .Alias("lookup_response_codec")
        .Default(NCompression::ECodec::Lz4);
    registrar.Parameter("max_rows_per_lookup_request", &TThis::MaxRowsPerLookupRequest)
        .Alias("max_rows_per_read_request")
        .GreaterThan(0)
        .Default(1000);
    registrar.Parameter("lookup_rows_request_timeout_slack", &TThis::LookupRowsRequestTimeoutSlack)
        .Default(TDuration::Zero());
    registrar.Parameter("lookup_rows_in_memory_logging_suppression_timeout", &TThis::LookupRowsInMemoryLoggingSuppressionTimeout)
        .Optional();
    registrar.Parameter("lookup_rows_ext_memory_logging_suppression_timeout", &TThis::LookupRowsExtMemoryLoggingSuppressionTimeout)
        .Optional();

    registrar.Parameter("default_get_tablet_errors_limit", &TThis::DefaultGetTabletErrorsLimit)
        .Default(5)
        .GreaterThan(0);

    registrar.Parameter("udf_registry_path", &TThis::UdfRegistryPath)
        .Default("//tmp/udfs");
    registrar.Parameter("function_registry_cache", &TThis::FunctionRegistryCache)
        .DefaultNew();
    registrar.Parameter("function_impl_cache", &TThis::FunctionImplCache)
        .DefaultNew();

    registrar.Parameter("thread_pool_size", &TThis::ThreadPoolSize)
        .Default(4);

    registrar.Parameter("bus_client", &TThis::BusClient)
        .DefaultNew();
    registrar.Parameter("idle_channel_ttl", &TThis::IdleChannelTtl)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("default_get_in_sync_replicas_timeout", &TThis::DefaultGetInSyncReplicasTimeout)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("default_get_tablet_infos_timeout", &TThis::DefaultGetTabletInfosTimeout)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("default_trim_table_timeout", &TThis::DefaultTrimTableTimeout)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("default_get_operation_retry_interval", &TThis::DefaultGetOperationRetryInterval)
        .Default(TDuration::Seconds(3));
    registrar.Parameter("default_get_operation_timeout", &TThis::DefaultGetOperationTimeout)
        .Default(TDuration::Minutes(5));
    registrar.Parameter("default_list_jobs_timeout", &TThis::DefaultListJobsTimeout)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("default_get_job_timeout", &TThis::DefaultGetJobTimeout)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("default_list_operations_timeout", &TThis::DefaultListOperationsTimeout)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("default_pull_rows_timeout", &TThis::DefaultPullRowsTimeout)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("default_sync_alien_cells_timeout", &TThis::DefaultSyncAlienCellsTimeout)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("default_chaos_node_service_timeout", &TThis::DefaultChaosNodeServiceTimeout)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("default_chaos_watcher_client_request_timeout",
        &TThis::DefaultChaosWatcherClientRequestTimeout)
        .Default(TDuration::Minutes(30));
    registrar.Parameter("default_chaos_replicated_table_get_tablet_count_timeout",
        &TThis::DefaulChaosReplicatedTableGetTabletCountTimeout)
        .Default(TDuration::Seconds(15));

    registrar.Parameter("default_fetch_table_rows_timeout", &TThis::DefaultFetchTableRowsTimeout)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("default_register_transaction_actions_timeout", &TThis::DefaultRegisterTransactionActionsTimeout)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("default_get_ordered_tablet_safe_trim_row_count_timeout", &TThis::DefaultGetOrderedTabletSafeTrimRowCountTimeout)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("cypress_write_yson_nesting_level_limit", &TThis::CypressWriteYsonNestingLevelLimit)
        .Default(NYson::OriginalNestingLevelLimit)
        .LessThanOrEqual(NYson::NewNestingLevelLimit);

    registrar.Parameter("job_prober_rpc_timeout", &TThis::JobProberRpcTimeout)
        .Default(TDuration::Seconds(45));

    registrar.Parameter("default_cache_sticky_group_size", &TThis::DefaultCacheStickyGroupSize)
        .Alias("cache_sticky_group_size_override")
        .Default(1);
    registrar.Parameter("enable_dynamic_cache_sticky_group_size", &TThis::EnableDynamicCacheStickyGroupSize)
        .Default(true);
    registrar.Parameter("sticky_group_size_cache_expiration_timeout", &TThis::StickyGroupSizeCacheExpirationTimeout)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("max_request_window_size", &TThis::MaxRequestWindowSize)
        .GreaterThan(0)
        .Default(65'536);

    registrar.Parameter("upload_transaction_timeout", &TThis::UploadTransactionTimeout)
        .Default(TDuration::Seconds(15));
    // NB: its default value is |UploadTransactionTimeout| / 2. See postprocessor.
    registrar.Parameter("upload_transaction_ping_period", &TThis::UploadTransactionPingPeriod)
        .Optional();
    registrar.Parameter("hive_sync_rpc_timeout", &TThis::HiveSyncRpcTimeout)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("permission_cache", &TThis::PermissionCache)
        .DefaultNew();
    registrar.Parameter("user_attribute_cache", &TThis::UserAttributeCache)
        .DefaultNew();

    registrar.Parameter("job_shell_descriptor_cache", &TThis::JobShellDescriptorCache)
        .Alias("job_node_descriptor_cache")
        .DefaultNew();

    registrar.Parameter("max_chunks_per_fetch", &TThis::MaxChunksPerFetch)
        .Default(100'000)
        .GreaterThan(0);

    registrar.Parameter("max_chunks_per_locate_request", &TThis::MaxChunksPerLocateRequest)
        .Default(10'000)
        .GreaterThan(0);

    registrar.Parameter("nested_input_transaction_timeout", &TThis::NestedInputTransactionTimeout)
        .Default(TDuration::Minutes(10));
    registrar.Parameter("nested_input_transaction_ping_period", &TThis::NestedInputTransactionPingPeriod)
        .Default(TDuration::Minutes(1));

    registrar.Parameter("cluster_liveness_check_timeout", &TThis::ClusterLivenessCheckTimeout)
        .Default(TDuration::Seconds(15));

    registrar.Parameter("chunk_fetch_retries", &TThis::ChunkFetchRetries)
        .DefaultNew();

    registrar.Parameter("banned_replica_tracker_cache", &TThis::BannedReplicaTrackerCache)
        .DefaultNew();

    registrar.Parameter("chaos_cell_channel", &TThis::ChaosCellChannel)
        .DefaultNew();

    registrar.Parameter("hydra_admin_channel", &TThis::HydraAdminChannel)
        .DefaultNew();

    registrar.Parameter("sequoia_connection", &TThis::SequoiaConnection)
        .DefaultNew();

    registrar.Parameter("use_followers_for_write_targets_allocation", &TThis::UseFollowersForWriteTargetsAllocation)
        .Default(true);

    registrar.Parameter("tvm_id", &TThis::TvmId)
        .Default();

    registrar.Parameter("chaos_residency_cache", &TThis::ChaosResidencyCache)
        // COMPAT(gryzlov-ad)
        .Alias("replication_card_residency_cache")
        .DefaultNew();

    registrar.Parameter("object_life_stage_check_period", &TThis::ObjectLifeStageCheckPeriod)
        .Default(TDuration::MilliSeconds(100));
    registrar.Parameter("object_life_stage_check_retry_count", &TThis::ObjectLifeStageCheckRetryCount)
        .GreaterThan(0)
        .Default(100);
    registrar.Parameter("object_life_stage_check_timeout", &TThis::ObjectLifeStageCheckTimeout)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("read_operations_archive_state_from", &TThis::ReadOperationsArchiveStateFrom)
        .Alias("read_archive_state_from")
        .Default(EMasterChannelKind::Follower);

    registrar.Preprocessor([] (TThis* config) {
        config->FunctionImplCache->Capacity = 100;

        config->JobShellDescriptorCache->ExpireAfterAccessTime = TDuration::Minutes(5);
        config->JobShellDescriptorCache->ExpireAfterSuccessfulUpdateTime = TDuration::Minutes(5);
        config->JobShellDescriptorCache->RefreshTime = TDuration::Minutes(1);

        config->SyncReplicaCache->ExpireAfterSuccessfulUpdateTime = TDuration::Minutes(5);
        config->SyncReplicaCache->RefreshTime = TDuration::Seconds(5);

        config->BannedReplicaTrackerCache->Capacity = 1000;
    });

    registrar.Parameter("sync_replica_cache", &TThis::SyncReplicaCache)
        .DefaultNew();
    registrar.Parameter("clock_manager", &TThis::ClockManager)
        .DefaultNew();
    registrar.Parameter("replica_fallback_retry_count", &TThis::ReplicaFallbackRetryCount)
        .GreaterThanOrEqual(0)
        .Default(3);

    registrar.Parameter("disable_new_range_inference", &TThis::DisableNewRangeInference)
        .Default(false);

    registrar.Parameter("use_web_assembly", &TThis::UseWebAssembly)
        .Default(false);

    registrar.Parameter("group_by_with_limit_is_unordered", &TThis::GroupByWithLimitIsUnordered)
        .Default(false);

    registrar.Parameter("flow_pipeline_controller_rpc_timeout", &TThis::FlowPipelineControllerRpcTimeout)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("enable_distributed_replication_collocation_attachment", &TThis::EnableDistributedReplicationCollocationAttachment)
        .Default(false);

    registrar.Postprocessor([] (TConnectionDynamicConfig* config) {
        if (!config->UploadTransactionPingPeriod.has_value()) {
            config->UploadTransactionPingPeriod = config->UploadTransactionTimeout / 2;
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

TConnectionCompoundConfig::TConnectionCompoundConfig(const INodePtr& node)
    : Static(ConvertTo<TConnectionStaticConfigPtr>(node))
    , Dynamic(ConvertTo<TConnectionDynamicConfigPtr>(node))
{ }

TConnectionCompoundConfig::TConnectionCompoundConfig(
    TConnectionStaticConfigPtr staticConfig,
    TConnectionDynamicConfigPtr dynamicConfig)
    : Static(std::move(staticConfig))
    , Dynamic(std::move(dynamicConfig))
{ }

TConnectionCompoundConfigPtr TConnectionCompoundConfig::Clone() const
{
    return New<TConnectionCompoundConfig>(CloneYsonStruct(Static), CloneYsonStruct(Dynamic));
}

void Serialize(const TConnectionCompoundConfigPtr& connectionConfig, NYson::IYsonConsumer* consumer)
{
    if (!connectionConfig) {
        BuildYsonFluently(consumer)
            .BeginMap()
            .EndMap();
        return;
    }
    auto node = ConvertToNode(connectionConfig->Static);
    node = PatchNode(node, ConvertToNode(connectionConfig->Dynamic));
    Serialize(node, consumer);
}

void Deserialize(TConnectionCompoundConfigPtr& connectionConfig, const INodePtr& node)
{
    connectionConfig = New<TConnectionCompoundConfig>();
    connectionConfig->Static = ConvertTo<TConnectionStaticConfigPtr>(node);
    connectionConfig->Dynamic = ConvertTo<TConnectionDynamicConfigPtr>(node);
}

void Deserialize(TConnectionCompoundConfigPtr& connectionConfig, TYsonPullParserCursor* cursor)
{
    Deserialize(connectionConfig, ExtractTo<INodePtr>(cursor));
}

////////////////////////////////////////////////////////////////////////////////

TRemoteTimestampProviderConfigPtr CreateRemoteTimestampProviderConfig(TMasterConnectionConfigPtr config)
{
    auto timestampProviderConfig = New<TRemoteTimestampProviderConfig>();

    // Use masters for timestamp generation.
    timestampProviderConfig->Addresses = config->Addresses;
    timestampProviderConfig->RpcTimeout = config->RpcTimeout;

    // TRetryingChannelConfig
    timestampProviderConfig->RetryBackoffTime = config->RetryBackoffTime;
    timestampProviderConfig->RetryAttempts = config->RetryAttempts;
    timestampProviderConfig->RetryTimeout = config->RetryTimeout;

    return timestampProviderConfig;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
