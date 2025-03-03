#pragma once

#include "public.h"

#include <yt/yt/client/api/config.h>

#include <yt/yt/ytlib/cell_master_client/config.h>

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/ytlib/hydra/config.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/library/query/engine_api/public.h>

#include <yt/yt/ytlib/bundle_controller/public.h>

#include <yt/yt/ytlib/discovery_client/public.h>

#include <yt/yt/ytlib/scheduler/public.h>

#include <yt/yt/ytlib/queue_client/public.h>

#include <yt/yt/ytlib/query_tracker_client/public.h>

#include <yt/yt/ytlib/yql_client/public.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/security_client/public.h>

#include <yt/yt/ytlib/chunk_client/config.h>

#include <yt/yt/ytlib/chaos_client/public.h>

#include <yt/yt/library/auth_server/public.h>

#include <yt/yt/core/bus/tcp/public.h>

#include <yt/yt/core/compression/public.h>

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/rpc/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

struct TMasterConnectionConfig
    : public NHydra::TPeerConnectionConfig
    , public NRpc::TRetryingChannelConfig
{
    //! Timeout for RPC requests to masters.
    TDuration RpcTimeout;

    REGISTER_YSON_STRUCT(TMasterConnectionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMasterConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

struct TMasterCacheConnectionConfig
    : public TMasterConnectionConfig
{
    bool EnableMasterCacheDiscovery;
    TDuration MasterCacheDiscoveryPeriod;
    TDuration MasterCacheDiscoveryPeriodSplay;

    REGISTER_YSON_STRUCT(TMasterCacheConnectionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMasterCacheConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

struct TClockServersConfig
    : public NHydra::TPeerConnectionConfig
    , public NRpc::TRetryingChannelConfig
{
    //! Timeout for RPC requests to clock servers.
    TDuration RpcTimeout;

    REGISTER_YSON_STRUCT(TClockServersConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TClockServersConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCypressProxyConnectionConfig
    : public NRpc::TBalancingChannelConfig
    , public NRpc::TRetryingChannelConfig
{
    //! Timeout for RPC requests to Cypress proxies.
    TDuration RpcTimeout;

    REGISTER_YSON_STRUCT(TCypressProxyConnectionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCypressProxyConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

// Consider adding inheritance from TRetryingChannelConfig and/or TBalancingChannelConfig.
struct TSequoiaConnectionConfig
    : public virtual NYTree::TYsonStruct
{
    //! If |nullopt|, Sequoia tables are handled on the local cluster.
    std::optional<TString> GroundClusterName;

    NYTree::TYPath SequoiaRootPath;

    TDuration SequoiaTransactionTimeout;

    REGISTER_YSON_STRUCT(TSequoiaConnectionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSequoiaConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

//! A static cluster connection configuration.
/*!
 *  For primary cluster connections (i.e. those created in bootstraps of native servers)
 *  these options are taken from the static configuration file.
 *
 *  For secondary cluster connections (i.e. those taken from cluster directory or other
 *  dynamic sources) these options are taken from the configuration node as well as dynamic one.
 *
 *  NB: We try to keep the size of a statically generated cluster connection (by ytcfgen, YTInstance
 *  or k8s operator) as small as possible, so do not add new fields here unless it is absolutely necessary.
 *  A good reason for an option to be here is when it is required for fetching //sys/@cluster_connection.
 *  In other situations prefer adding fields only to dynamic config.
 */
struct TConnectionStaticConfig
    : public NApi::TConnectionConfig
    , public NCellMasterClient::TCellDirectoryConfig
{
    std::optional<NNodeTrackerClient::TNetworkPreferenceList> Networks;

    NTransactionClient::TRemoteTimestampProviderConfigPtr TimestampProvider;
    TClockServersConfigPtr ClockServers;

    //! If |nullptr|, requests are passed directly to masters.
    TCypressProxyConnectionConfigPtr CypressProxy;

    NCellMasterClient::TCellDirectorySynchronizerConfigPtr MasterCellDirectorySynchronizer;

    NChaosClient::TChaosCellDirectorySynchronizerConfigPtr ChaosCellDirectorySynchronizer;

    NTransactionClient::TClockManagerConfigPtr ClockManager;

    TAsyncExpiringCacheConfigPtr SyncReplicaCache;

    //! Visible in profiling as tag `connection_name`.
    TString ConnectionName;

    TSlruCacheConfigPtr BannedReplicaTrackerCache;

    //! Replaces all master addresses with given master cache addresses.
    //! Used to proxy all job requests through cluster nodes.
    void OverrideMasterAddresses(const std::vector<std::string>& addresses);

    REGISTER_YSON_STRUCT(TConnectionStaticConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TConnectionStaticConfig)

////////////////////////////////////////////////////////////////////////////////

//! A dynamic cluster connection configuration which is designed to be taken from //sys/@cluster_connection.
/*!
 *  NB: The word "dynamic" represents the origin of this config rather than the ability
 *  to reconfigure on the fly. Change of some of the options here will not take effect
 *  for an already existing connection; this may be fixed by writing reconfiguration code in
 *  #TConnection::Reconfigure method.
 *
 *  NB: During the transition period in order to keep the old behavior some of the components may take
 *  the dynamic config from the static configuration.
 *
 */
struct TConnectionDynamicConfig
    : public NApi::TConnectionDynamicConfig
    , public NChunkClient::TChunkTeleporterConfig
{
    // TODO(max42): distribute these options into two groups: dynamically reconfigurable and not.

    NHiveClient::TCellDirectoryConfigPtr CellDirectory;
    NHiveClient::TCellDirectorySynchronizerConfigPtr CellDirectorySynchronizer;
    NHiveClient::TDownedCellTrackerConfigPtr DownedCellTracker;
    NDiscoveryClient::TDiscoveryConnectionConfigPtr DiscoveryConnection;
    NQueueClient::TQueueAgentConnectionConfigPtr QueueAgent;
    NQueryTrackerClient::TQueryTrackerConnectionConfigPtr QueryTracker;
    NYqlClient::TYqlAgentConnectionConfigPtr YqlAgent;
    NScheduler::TSchedulerConnectionConfigPtr Scheduler;
    NBundleController::TBundleControllerChannelConfigPtr BundleController;
    NTransactionClient::TTransactionManagerConfigPtr TransactionManager;
    NChunkClient::TBlockCacheConfigPtr BlockCache;
    NChunkClient::TClientChunkMetaCacheConfigPtr ChunkMetaCache;
    NChunkClient::TChunkReplicaCacheConfigPtr ChunkReplicaCache;
    NChaosClient::TReplicationCardCacheDynamicConfigPtr ReplicationCardCache;
    NHiveClient::TClusterDirectorySynchronizerConfigPtr ClusterDirectorySynchronizer;
    NChunkClient::TMediumDirectorySynchronizerConfigPtr MediumDirectorySynchronizer;
    NNodeTrackerClient::TNodeDirectorySynchronizerConfigPtr NodeDirectorySynchronizer;
    NChunkClient::TChunkSliceFetcherConfigPtr ChunkSliceFetcher;

    NQueryClient::TExecutorConfigPtr QueryEvaluator;
    NQueryClient::TColumnEvaluatorCacheConfigPtr ColumnEvaluatorCache;
    NQueryClient::TExpressionEvaluatorCacheConfigPtr ExpressionEvaluatorCache;
    TDuration DefaultSelectRowsTimeout;
    NCompression::ECodec SelectRowsResponseCodec;
    i64 DefaultInputRowLimit;
    i64 DefaultOutputRowLimit;

    TDuration DistributedQuerySessionPingPeriod;
    TDuration DistributedQuerySessionRetentionTime;
    TDuration DistributedQuerySessionControlRpcTimeout;

    TDuration WriteRowsTimeout;
    NCompression::ECodec WriteRowsRequestCodec;
    int MaxRowsPerWriteRequest;
    i64 MaxDataWeightPerWriteRequest;
    int MaxRowsPerTransaction;

    TDuration DefaultLookupRowsTimeout;
    NCompression::ECodec LookupRowsRequestCodec;
    NCompression::ECodec LookupRowsResponseCodec;
    int MaxRowsPerLookupRequest;
    //! Slack between client-side timeout and timeout of internal subrequests.
    //! Is used if |EnablePartialResult| is set for a lookup request so it
    //! will not completely timeout because of slow subrequests.
    TDuration LookupRowsRequestTimeoutSlack;
    std::optional<TDuration> LookupRowsInMemoryLoggingSuppressionTimeout;
    std::optional<TDuration> LookupRowsExtMemoryLoggingSuppressionTimeout;

    int DefaultGetTabletErrorsLimit;

    NYPath::TYPath UdfRegistryPath;
    TAsyncExpiringCacheConfigPtr FunctionRegistryCache;
    TSlruCacheConfigPtr FunctionImplCache;

    int ThreadPoolSize;

    NBus::TBusConfigPtr BusClient;
    TDuration IdleChannelTtl;

    TDuration DefaultGetInSyncReplicasTimeout;
    TDuration DefaultGetTabletInfosTimeout;
    TDuration DefaultTrimTableTimeout;
    TDuration DefaultGetOperationTimeout;
    TDuration DefaultGetOperationRetryInterval;
    TDuration DefaultListJobsTimeout;
    TDuration DefaultGetJobTimeout;
    TDuration DefaultListOperationsTimeout;
    TDuration DefaultPullRowsTimeout;
    TDuration DefaultSyncAlienCellsTimeout;
    TDuration DefaultChaosNodeServiceTimeout;
    TDuration DefaultChaosWatcherClientRequestTimeout;
    TDuration DefaultFetchTableRowsTimeout;
    TDuration DefaultRegisterTransactionActionsTimeout;
    TDuration DefaultGetOrderedTabletSafeTrimRowCountTimeout;
    TDuration DefaulChaosReplicatedTableGetTabletCountTimeout;

    int CypressWriteYsonNestingLevelLimit;

    TDuration JobProberRpcTimeout;

    int DefaultCacheStickyGroupSize;
    bool EnableDynamicCacheStickyGroupSize;
    TDuration StickyGroupSizeCacheExpirationTimeout;

    ssize_t MaxRequestWindowSize;

    TDuration UploadTransactionTimeout;
    std::optional<TDuration> UploadTransactionPingPeriod;
    TDuration HiveSyncRpcTimeout;

    TAsyncExpiringCacheConfigPtr JobShellDescriptorCache;

    NSecurityClient::TPermissionCacheConfigPtr PermissionCache;
    NSecurityClient::TUserAttributeCacheConfigPtr UserAttributeCache;

    int MaxChunksPerFetch;
    int MaxChunksPerLocateRequest;

    TDuration NestedInputTransactionTimeout;
    TDuration NestedInputTransactionPingPeriod;

    TDuration ClusterLivenessCheckTimeout;

    NObjectClient::TReqExecuteBatchWithRetriesConfigPtr ChunkFetchRetries;

    TSlruCacheDynamicConfigPtr BannedReplicaTrackerCache;

    NChaosClient::TReplicationCardChannelConfigPtr ChaosCellChannel;

    NRpc::TRetryingChannelConfigPtr HydraAdminChannel;

    TSequoiaConnectionConfigPtr SequoiaConnection;

    bool UseFollowersForWriteTargetsAllocation;

    //! TVM application id corresponding to the cluster.
    //! If set, should (and hopefully will) be used for authentication in all native protocol RPC requests.
    std::optional<NAuth::TTvmId> TvmId;

    NChaosClient::TChaosResidencyCacheConfigPtr ChaosResidencyCache;

    TDuration ObjectLifeStageCheckPeriod;
    int ObjectLifeStageCheckRetryCount;
    TDuration ObjectLifeStageCheckTimeout;

    TAsyncExpiringCacheDynamicConfigPtr SyncReplicaCache;

    NTransactionClient::TDynamicClockManagerConfigPtr ClockManager;

    int ReplicaFallbackRetryCount;

    bool DisableNewRangeInference;

    bool UseWebAssembly;

    // COMPAT(sabdenovch)
    bool GroupByWithLimitIsUnordered;

    TDuration FlowPipelineControllerRpcTimeout;

    EMasterChannelKind ReadOperationsArchiveStateFrom;

    bool EnableDistributedReplicationCollocationAttachment;

    REGISTER_YSON_STRUCT(TConnectionDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TConnectionDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

//! A helper over INodePtr to provide access both to static and dynamic parts of cluster connection config.
//! Used in scenarios when cluster config is deserialized from a single node; in particular
//! Note that we cannot simply inherit from both TConnectionStaticConfig and TConnectionDynamicConfig
//! since they share some fields with different types (e.g. SyncReplicaCache).
struct TConnectionCompoundConfig
    : public TRefCounted
{
    TConnectionStaticConfigPtr Static;
    TConnectionDynamicConfigPtr Dynamic;

    TConnectionCompoundConfig() = default;
    TConnectionCompoundConfig(
        TConnectionStaticConfigPtr staticConfig,
        TConnectionDynamicConfigPtr dynamicConfig);

    explicit TConnectionCompoundConfig(const NYTree::INodePtr& node);

    TConnectionCompoundConfigPtr Clone() const;
};

DEFINE_REFCOUNTED_TYPE(TConnectionCompoundConfig)

void Serialize(const TConnectionCompoundConfigPtr& connectionConfig, NYson::IYsonConsumer* consumer);
void Deserialize(TConnectionCompoundConfigPtr& connectionConfig, const NYTree::INodePtr& node);
void Deserialize(TConnectionCompoundConfigPtr& connectionConfig, NYson::TYsonPullParserCursor* cursor);

////////////////////////////////////////////////////////////////////////////////

NTransactionClient::TRemoteTimestampProviderConfigPtr CreateRemoteTimestampProviderConfig(TMasterConnectionConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative

