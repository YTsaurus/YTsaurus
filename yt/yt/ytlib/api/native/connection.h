#pragma once

#include "public.h"

#include <yt/yt/client/api/connection.h>

#include <yt/yt/ytlib/cell_master_client/public.h>

#include <yt/yt/ytlib/chaos_client/public.h>

#include <yt/yt/ytlib/discovery_client/public.h>

#include <yt/yt/library/query/base/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/job_prober_client/public.h>

#include <yt/yt/ytlib/queue_client/public.h>

#include <yt/yt/ytlib/security_client/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/library/auth_server/public.h>

#include <yt/yt/client/cell_master_client/public.h>

#include <yt/yt/client/chaos_client/public.h>

#include <yt/yt/core/logging/public.h>

#include <yt/yt/core/misc/sync_expiring_cache.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

struct IConnection
    : public NApi::IConnection
{
    virtual const TConnectionStaticConfigPtr& GetStaticConfig() const = 0;
    virtual TConnectionDynamicConfigPtr GetConfig() const = 0;
    virtual TConnectionCompoundConfigPtr GetCompoundConfig() const = 0;

    virtual const NNodeTrackerClient::TNetworkPreferenceList& GetNetworks() const = 0;

    virtual NObjectClient::TCellId GetPrimaryMasterCellId() const = 0;
    virtual NObjectClient::TCellTag GetPrimaryMasterCellTag() const = 0;
    virtual NObjectClient::TCellTagList GetSecondaryMasterCellTags() const = 0;
    virtual NObjectClient::TCellTag GetRandomMasterCellTagWithRoleOrThrow(
        NCellMasterClient::EMasterCellRole role) const = 0;
    virtual NObjectClient::TCellId GetMasterCellId(NObjectClient::TCellTag cellTag) const = 0;

    virtual const NQueryClient::IEvaluatorPtr& GetQueryEvaluator() = 0;
    virtual const NQueryClient::IColumnEvaluatorCachePtr& GetColumnEvaluatorCache() = 0;
    virtual const NQueryClient::IExpressionEvaluatorCachePtr& GetExpressionEvaluatorCache() = 0;
    virtual const NChunkClient::IBlockCachePtr& GetBlockCache() = 0;
    virtual const NChunkClient::IClientChunkMetaCachePtr& GetChunkMetaCache() = 0;

    virtual const NCellMasterClient::ICellDirectoryPtr& GetMasterCellDirectory() = 0;
    virtual const NCellMasterClient::ICellDirectorySynchronizerPtr& GetMasterCellDirectorySynchronizer() = 0;

    virtual const NHiveClient::ICellDirectoryPtr& GetCellDirectory() = 0;
    virtual const NHiveClient::ICellDirectorySynchronizerPtr& GetCellDirectorySynchronizer() = 0;
    virtual const NChaosClient::IChaosCellDirectorySynchronizerPtr& GetChaosCellDirectorySynchronizer() = 0;

    virtual NHiveClient::TClusterDirectoryPtr GetClusterDirectory() const = 0;
    virtual const NHiveClient::IClusterDirectorySynchronizerPtr& GetClusterDirectorySynchronizer() = 0;

    virtual const NChunkClient::TMediumDirectoryPtr& GetMediumDirectory() = 0;
    virtual const NChunkClient::TMediumDirectorySynchronizerPtr& GetMediumDirectorySynchronizer() = 0;

    virtual const NNodeTrackerClient::TNodeDirectoryPtr& GetNodeDirectory() = 0;
    virtual const NNodeTrackerClient::INodeDirectorySynchronizerPtr& GetNodeDirectorySynchronizer() = 0;

    virtual const NChunkClient::IChunkReplicaCachePtr& GetChunkReplicaCache() = 0;

    virtual std::pair<IClientPtr, NYPath::TYPath> GetQueryTrackerStage(const TString& stage) = 0;
    virtual NRpc::IChannelPtr GetQueryTrackerChannelOrThrow(const TString& stage) = 0;

    virtual const NHiveClient::TDownedCellTrackerPtr& GetDownedCellTracker() = 0;

    virtual NRpc::IChannelPtr GetMasterChannelOrThrow(
        EMasterChannelKind kind,
        NObjectClient::TCellTag cellTag = NObjectClient::PrimaryMasterCellTagSentinel) = 0;
    virtual NRpc::IChannelPtr GetMasterChannelOrThrow(
        EMasterChannelKind kind,
        NObjectClient::TCellId cellId) = 0;

    //! Returns channel for communicating with Cypress.
    //! If Cypress Proxies are configured, returns channel to them,
    //! otherwise returns channel directly to the master.
    //! Throws if cell tag is unknown.
    virtual NRpc::IChannelPtr GetCypressChannelOrThrow(
        EMasterChannelKind kind,
        NObjectClient::TCellTag cellTag = NObjectClient::PrimaryMasterCellTagSentinel) = 0;

    virtual const NRpc::IChannelPtr& GetSchedulerChannel() = 0;
    virtual const NRpc::IChannelPtr& GetBundleControllerChannel() = 0;
    virtual const NRpc::IChannelFactoryPtr& GetChannelFactory() = 0;

    virtual NRpc::IChannelPtr GetChaosChannelByCellId(
        NObjectClient::TCellId cellId,
        NHydra::EPeerKind peerKind = NHydra::EPeerKind::Leader) = 0;
    virtual NRpc::IChannelPtr GetChaosChannelByCellTag(
        NObjectClient::TCellTag cellTag,
        NHydra::EPeerKind peerKind = NHydra::EPeerKind::Leader) = 0;
    virtual NRpc::IChannelPtr GetChaosChannelByCardId(
        NChaosClient::TReplicationCardId replicationCardId,
        NHydra::EPeerKind peerKind = NHydra::EPeerKind::Leader) = 0;

    virtual NRpc::IChannelPtr FindQueueAgentChannel(TStringBuf stage) const = 0;
    virtual const NQueueClient::TQueueConsumerRegistrationManagerPtr& GetQueueConsumerRegistrationManager() const = 0;

    virtual NRpc::IRoamingChannelProviderPtr GetYqlAgentChannelProviderOrThrow(const TString& stage) const = 0;

    virtual const NTabletClient::ITableMountCachePtr& GetTableMountCache() = 0;
    virtual const NChaosClient::IReplicationCardCachePtr& GetReplicationCardCache() = 0;
    virtual const NTransactionClient::ITimestampProviderPtr& GetTimestampProvider() = 0;
    virtual const NTransactionClient::IClockManagerPtr& GetClockManager() = 0;

    virtual const NJobProberClient::TJobShellDescriptorCachePtr& GetJobShellDescriptorCache() = 0;

    virtual const NSecurityClient::TPermissionCachePtr& GetPermissionCache() = 0;
    virtual const NSecurityClient::TUserAttributeCachePtr& GetUserAttributeCache() = 0;

    virtual const TStickyGroupSizeCachePtr& GetStickyGroupSizeCache() = 0;

    virtual const TSyncReplicaCachePtr& GetSyncReplicaCache() = 0;
    virtual const TTabletSyncReplicaCachePtr& GetTabletSyncReplicaCache() = 0;
    virtual const NChaosClient::IBannedReplicaTrackerCachePtr& GetBannedReplicaTrackerCache() = 0;
    virtual const NChaosClient::IChaosResidencyCachePtr& GetChaosResidencyCache() = 0;
    virtual const ITableReplicaSynchronicityCachePtr& GetTableReplicaSynchronicityCache() = 0;

    virtual IClientPtr CreateNativeClient(const TClientOptions& options) = 0;

    virtual std::vector<std::string> GetDiscoveryServerAddresses() const = 0;
    virtual NDiscoveryClient::IDiscoveryClientPtr CreateDiscoveryClient(
        NDiscoveryClient::TDiscoveryClientConfigPtr config,
        NRpc::IChannelFactoryPtr channelFactory) = 0;
    virtual NDiscoveryClient::IMemberClientPtr CreateMemberClient(
        NDiscoveryClient::TMemberClientConfigPtr config,
        NRpc::IChannelFactoryPtr channelFactory,
        IInvokerPtr invoker,
        NDiscoveryClient::TMemberId id,
        NDiscoveryClient::TGroupId groupId) = 0;

    virtual NYTree::IYPathServicePtr GetOrchidService() = 0;

    void Terminate() override = 0;

    virtual void InitializeDiscoveryServerAddressPool() = 0;

    virtual TFuture<void> SyncHiveCellWithOthers(
        const std::vector<NElection::TCellId>& srcCellIds,
        NElection::TCellId dstCellId) = 0;

    virtual const NLogging::TLogger& GetLogger() = 0;

    virtual void Reconfigure(const TConnectionDynamicConfigPtr& dynamicConfig) = 0;

    virtual void RegisterShuffleService(const std::string& address) = 0;

    virtual NRpc::IChannelPtr GetShuffleServiceChannelOrThrow() = 0;

    virtual NRpc::IChannelPtr CreateChannelByAddress(const std::string& address) = 0;

    using TReconfiguredSignature = void(const TConnectionDynamicConfigPtr& newConfig);
    DECLARE_INTERFACE_SIGNAL(TReconfiguredSignature, Reconfigured);
};

DEFINE_REFCOUNTED_TYPE(IConnection)

////////////////////////////////////////////////////////////////////////////////

class TStickyGroupSizeCache
    : public TRefCounted
{
public:
    struct TKey
    {
        std::optional<std::string> Key;
        TSharedRefArray Message;

        operator size_t() const;
        bool operator == (const TKey& other) const;
    };

    explicit TStickyGroupSizeCache(TDuration expirationTimeout);

    std::optional<int> UpdateAdvisedStickyGroupSize(const TKey& key, int stickyGroupSize);
    std::optional<int> GetAdvisedStickyGroupSize(const TKey& key);

private:
    const TIntrusivePtr<TSyncExpiringCache<TKey, std::optional<int>>> AdvisedStickyGroupSize_;
};

DEFINE_REFCOUNTED_TYPE(TStickyGroupSizeCache)

////////////////////////////////////////////////////////////////////////////////

struct TConnectionOptions
    : public NApi::TConnectionOptions
{
    bool RetryRequestQueueSizeLimitExceeded = false;

    //! If non-null, provides an externally-controlled block cache.
    NChunkClient::IBlockCachePtr BlockCache;

    //! If non-null, provides an externally-controlled chunk meta cache.
    NChunkClient::IClientChunkMetaCachePtr ChunkMetaCache;

    //! If non-null, provides a TVM service for authentication.
    NAuth::IDynamicTvmServicePtr TvmService;

    EChaosResidencyCacheType ChaosResidencyCacheMode = EChaosResidencyCacheType::Client;

    explicit TConnectionOptions(IInvokerPtr connectionInvoker);
    TConnectionOptions() = default;
};

//! Native connection talks directly to the cluster via internal
//! (and typically not stable) RPC protocols.
IConnectionPtr CreateConnection(
    TConnectionStaticConfigPtr staticConfig,
    TConnectionDynamicConfigPtr dynamicConfig,
    TConnectionOptions options = {},
    TWeakPtr<NHiveClient::TClusterDirectory> clusterDirectoryOverride = nullptr,
    INodeMemoryTrackerPtr memoryTracker = nullptr);

//! A method for creating a connection from a compound config which
//! contains both static and dynamic connection configurations.
//! Used in various cases: in components that are not expected to update
//! their cluster connection from Cypress (e.g. job proxies) or
//! in legacy components during the transition period.
IConnectionPtr CreateConnection(
    TConnectionCompoundConfigPtr compoundConfig,
    TConnectionOptions options = {},
    TWeakPtr<NHiveClient::TClusterDirectory> clusterDirectoryOverride = nullptr,
    INodeMemoryTrackerPtr memoryTracker = nullptr);

////////////////////////////////////////////////////////////////////////////////

IConnectionPtr FindRemoteConnection(
    const IConnectionPtr& connection,
    const std::string& clusterName);

//! Returns native connection to cluster `clusterName`.
//! Falls back to the provided connection if `clusterName` is null or the cluster is not present in the connection's cluster directory.
IConnectionPtr FindRemoteConnection(
    const IConnectionPtr& connection,
    const std::optional<std::string>& clusterName);

IConnectionPtr GetRemoteConnectionOrThrow(
    const NApi::NNative::IConnectionPtr& connection,
    const std::string& clusterName,
    bool syncOnFailure = false);

IConnectionPtr FindRemoteConnection(
    const NApi::NNative::IConnectionPtr& connection,
    NObjectClient::TCellTag cellTag);

IConnectionPtr GetRemoteConnectionOrThrow(
    const NApi::NNative::IConnectionPtr& connection,
    NObjectClient::TCellTag cellTag);

////////////////////////////////////////////////////////////////////////////////

//! Fetches table mount info for object on a potentially-remote cluster, which may be specified in the rich YPath.
TFuture<NTabletClient::TTableMountInfoPtr> GetTableMountInfo(
    const NYPath::TRichYPath& objectPath,
    const IConnectionPtr& connection);

////////////////////////////////////////////////////////////////////////////////

//! Uses user attribute cache to check if the user is a superuser.
TFuture<bool> IsSuperuser(const NApi::NNative::IConnectionPtr& connection, const std::string& user);

//! Uses user attribute cache to check if the user is banned.
TFuture<bool> IsUserBanned(const NApi::NNative::IConnectionPtr& connection, const std::string& user);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
