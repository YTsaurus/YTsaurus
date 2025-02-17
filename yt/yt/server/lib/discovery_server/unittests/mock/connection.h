#include <yt/yt/ytlib/discovery_client/public.h>
#include <yt/yt/ytlib/discovery_client/helpers.h>

#include <yt/yt/ytlib/api/native/connection.h>

namespace NYT::NDiscoveryServer {

////////////////////////////////////////////////////////////////////////////////

class TMockDistributedThrottlerConnection
    : public NApi::NNative::IConnection
{
public:
    void SubscribeReconfigured(const TCallback<TReconfiguredSignature>&) override
    {
        YT_UNIMPLEMENTED();
    }
    void UnsubscribeReconfigured(const TCallback<TReconfiguredSignature>&) override
    {
        YT_UNIMPLEMENTED();
    }

public:
    explicit TMockDistributedThrottlerConnection(NDiscoveryClient::TDiscoveryConnectionConfigPtr config);

    const NApi::NNative::TConnectionStaticConfigPtr& GetStaticConfig() const override
    {
        YT_UNIMPLEMENTED();
    }
    NApi::NNative::TConnectionDynamicConfigPtr GetConfig() const override
    {
        YT_UNIMPLEMENTED();
    }
    NApi::NNative::TConnectionCompoundConfigPtr GetCompoundConfig() const override
    {
        YT_UNIMPLEMENTED();
    }

    const NNodeTrackerClient::TNetworkPreferenceList& GetNetworks() const override
    {
        YT_UNIMPLEMENTED();
    }

    NElection::TCellId GetPrimaryMasterCellId() const override
    {
        YT_UNIMPLEMENTED();
    }
    NObjectClient::TCellTag GetPrimaryMasterCellTag() const override
    {
        YT_UNIMPLEMENTED();
    }
    NObjectClient::TCellTagList GetSecondaryMasterCellTags() const override
    {
        YT_UNIMPLEMENTED();
    }
    NObjectClient::TCellTag GetRandomMasterCellTagWithRoleOrThrow(NCellMasterClient::EMasterCellRole /*role*/) const override
    {
        YT_UNIMPLEMENTED();
    }
    NObjectClient::TCellId GetMasterCellId(NObjectClient::TCellTag /*cellTag*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    const NQueryClient::IEvaluatorPtr& GetQueryEvaluator() override
    {
        YT_UNIMPLEMENTED();
    }
    const NQueryClient::IColumnEvaluatorCachePtr& GetColumnEvaluatorCache() override
    {
        YT_UNIMPLEMENTED();
    }
    const NQueryClient::IExpressionEvaluatorCachePtr& GetExpressionEvaluatorCache() override
    {
        YT_UNIMPLEMENTED();
    }
    const NChunkClient::IBlockCachePtr& GetBlockCache() override
    {
        YT_UNIMPLEMENTED();
    }
    const NChunkClient::IClientChunkMetaCachePtr& GetChunkMetaCache() override
    {
        YT_UNIMPLEMENTED();
    }

    const NCellMasterClient::ICellDirectoryPtr& GetMasterCellDirectory() override
    {
        YT_UNIMPLEMENTED();
    }
    const NCellMasterClient::ICellDirectorySynchronizerPtr& GetMasterCellDirectorySynchronizer() override
    {
        YT_UNIMPLEMENTED();
    }

    const NHiveClient::ICellDirectoryPtr& GetCellDirectory() override
    {
        YT_UNIMPLEMENTED();
    }
    const NHiveClient::ICellDirectorySynchronizerPtr& GetCellDirectorySynchronizer() override
    {
        YT_UNIMPLEMENTED();
    }
    const NChaosClient::IChaosCellDirectorySynchronizerPtr& GetChaosCellDirectorySynchronizer() override
    {
        YT_UNIMPLEMENTED();
    }

    NHiveClient::TClusterDirectoryPtr GetClusterDirectory() const override
    {
        YT_UNIMPLEMENTED();
    }
    const NHiveClient::IClusterDirectorySynchronizerPtr& GetClusterDirectorySynchronizer() override
    {
        YT_UNIMPLEMENTED();
    }

    const NChunkClient::TMediumDirectoryPtr& GetMediumDirectory() override
    {
        YT_UNIMPLEMENTED();
    }
    const NChunkClient::TMediumDirectorySynchronizerPtr& GetMediumDirectorySynchronizer() override
    {
        YT_UNIMPLEMENTED();
    }

    const NNodeTrackerClient::TNodeDirectoryPtr& GetNodeDirectory() override
    {
        YT_UNIMPLEMENTED();
    }
    const NNodeTrackerClient::INodeDirectorySynchronizerPtr& GetNodeDirectorySynchronizer() override
    {
        YT_UNIMPLEMENTED();
    }

    const NChunkClient::IChunkReplicaCachePtr& GetChunkReplicaCache() override
    {
        YT_UNIMPLEMENTED();
    }

    const NHiveClient::TDownedCellTrackerPtr& GetDownedCellTracker() override
    {
        YT_UNIMPLEMENTED();
    }

    NRpc::IChannelPtr GetMasterChannelOrThrow(
        NApi::EMasterChannelKind /*kind*/,
        NObjectClient::TCellTag /*cellTag*/) override
    {
        YT_UNIMPLEMENTED();
    }
    NRpc::IChannelPtr GetMasterChannelOrThrow(
        NApi::EMasterChannelKind /*kind*/,
        NObjectClient::TCellId /*cellId*/) override
    {
        YT_UNIMPLEMENTED();
    }
    NRpc::IChannelPtr GetCypressChannelOrThrow(
        NApi::EMasterChannelKind /*kind*/,
        NObjectClient::TCellTag /*cellTag*/) override
    {
        YT_UNIMPLEMENTED();
    }
    const NRpc::IChannelPtr& GetSchedulerChannel() override
    {
        YT_UNIMPLEMENTED();
    }
    NRpc::IChannelPtr GetChaosChannelByCellId(
        NObjectClient::TCellId /*cellId*/,
        NHydra::EPeerKind /*peerKind*/) override
    {
        YT_UNIMPLEMENTED();
    }
    NRpc::IChannelPtr GetChaosChannelByCellTag(
        NObjectClient::TCellTag /*cellTag*/,
        NHydra::EPeerKind /*peerKind*/) override
    {
        YT_UNIMPLEMENTED();
    }
    NRpc::IChannelPtr GetChaosChannelByCardId(
        NChaosClient::TReplicationCardId /*replicationCardId*/,
        NHydra::EPeerKind /*peerKind*/) override
    {
        YT_UNIMPLEMENTED();
    }

    const NRpc::IChannelFactoryPtr& GetChannelFactory() override
    {
        YT_UNIMPLEMENTED();
    }

    NRpc::IChannelPtr FindQueueAgentChannel(TStringBuf /*stage*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    const NQueueClient::TQueueConsumerRegistrationManagerPtr& GetQueueConsumerRegistrationManager() const override
    {
        YT_UNIMPLEMENTED();
    }

    NRpc::IRoamingChannelProviderPtr GetYqlAgentChannelProviderOrThrow(const TString& /*stage*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    std::pair<NApi::NNative::IClientPtr, TString> GetQueryTrackerStage(const TString& /*stage*/) override
    {
        YT_UNIMPLEMENTED();
    }

    NRpc::IChannelPtr GetQueryTrackerChannelOrThrow(const TString& /*stage*/) override
    {
        YT_UNIMPLEMENTED();
    }

    const NTabletClient::ITableMountCachePtr& GetTableMountCache() override
    {
        YT_UNIMPLEMENTED();
    }
    const NChaosClient::IReplicationCardCachePtr& GetReplicationCardCache() override
    {
        YT_UNIMPLEMENTED();
    }
    const NChaosClient::IChaosResidencyCachePtr& GetChaosResidencyCache() override
    {
        YT_UNIMPLEMENTED();
    }
    const NTransactionClient::ITimestampProviderPtr& GetTimestampProvider() override
    {
        YT_UNIMPLEMENTED();
    }
    const NTransactionClient::IClockManagerPtr& GetClockManager() override
    {
        YT_UNIMPLEMENTED();
    }

    const NJobProberClient::TJobShellDescriptorCachePtr& GetJobShellDescriptorCache() override
    {
        YT_UNIMPLEMENTED();
    }

    const NSecurityClient::TPermissionCachePtr& GetPermissionCache() override
    {
        YT_UNIMPLEMENTED();
    }

    const NSecurityClient::TUserAttributeCachePtr& GetUserAttributeCache() override
    {
        YT_UNIMPLEMENTED();
    }

    const NApi::NNative::TStickyGroupSizeCachePtr& GetStickyGroupSizeCache() override
    {
        YT_UNIMPLEMENTED();
    }

    const NApi::NNative::TSyncReplicaCachePtr& GetSyncReplicaCache() override
    {
        YT_UNIMPLEMENTED();
    }
    const NApi::NNative::TTabletSyncReplicaCachePtr& GetTabletSyncReplicaCache() override
    {
        YT_UNIMPLEMENTED();
    }
    const NChaosClient::IBannedReplicaTrackerCachePtr& GetBannedReplicaTrackerCache() override
    {
        YT_UNIMPLEMENTED();
    }
    const NApi::NNative::ITableReplicaSynchronicityCachePtr& GetTableReplicaSynchronicityCache() override
    {
        YT_UNIMPLEMENTED();
    }

    NApi::NNative::IClientPtr CreateNativeClient(const NApi::TClientOptions& /*options*/) override
    {
        YT_UNIMPLEMENTED();
    }

    std::vector<std::string> GetDiscoveryServerAddresses() const override
    {
        YT_UNIMPLEMENTED();
    }

    NApi::TClusterTag GetClusterTag() const override
    {
        YT_UNIMPLEMENTED();
    }

    const std::string& GetLoggingTag() const override
    {
        YT_UNIMPLEMENTED();
    }

    const TString& GetClusterId() const override
    {
        YT_UNIMPLEMENTED();
    }

    const std::optional<std::string>& GetClusterName() const override
    {
        YT_UNIMPLEMENTED();
    }

    IInvokerPtr GetInvoker() override
    {
        YT_UNIMPLEMENTED();
    }

    bool IsSameCluster(const NApi::IConnectionPtr& /*other*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    NApi::IClientPtr CreateClient(const NApi::TClientOptions& /*options*/) override
    {
        YT_UNIMPLEMENTED();
    }

    NHiveClient::ITransactionParticipantPtr CreateTransactionParticipant(
        NObjectClient::TCellId /*cellId*/,
        const NApi::TTransactionParticipantOptions& /*options*/) override
    {
        YT_UNIMPLEMENTED();
    }

    void ClearMetadataCaches() override
    {
        YT_UNIMPLEMENTED();
    }


    NDiscoveryClient::IDiscoveryClientPtr CreateDiscoveryClient(
        NDiscoveryClient::TDiscoveryClientConfigPtr config,
        NRpc::IChannelFactoryPtr channelFactory) override;
    NDiscoveryClient::IMemberClientPtr CreateMemberClient(
        NDiscoveryClient::TMemberClientConfigPtr config,
        NRpc::IChannelFactoryPtr channelFactory,
        IInvokerPtr invoker,
        NDiscoveryClient::TMemberId id,
        NDiscoveryClient::TGroupId groupId) override;

    NYTree::IYPathServicePtr GetOrchidService() override
    {
        YT_UNIMPLEMENTED();
    }

    void Terminate() override
    {
        YT_UNIMPLEMENTED();
    }
    bool IsTerminated() const override
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<void> SyncHiveCellWithOthers(
        const std::vector<NElection::TCellId>& /*srcCellIds*/,
        NElection::TCellId /*dstCellId*/) override
    {
        YT_UNIMPLEMENTED();
    }

    const NLogging::TLogger& GetLogger() override
    {
        YT_UNIMPLEMENTED();
    }

    void Reconfigure(const NApi::NNative::TConnectionDynamicConfigPtr& /*dynamicConfig*/) override
    {
        YT_UNIMPLEMENTED();
    }

    void InitializeDiscoveryServerAddressPool() override
    {
        YT_UNIMPLEMENTED();
    }

    NYson::TYsonString GetConfigYson() const override
    {
        YT_UNIMPLEMENTED();
    }

    const NRpc::IChannelPtr& GetBundleControllerChannel() override
    {
        YT_UNIMPLEMENTED();
    }

    void RegisterShuffleService(const std::string& /*address*/) override
    {
        YT_UNIMPLEMENTED();
    }

    NRpc::IChannelPtr GetShuffleServiceChannelOrThrow() override
    {
        YT_UNIMPLEMENTED();
    }

    NRpc::IChannelPtr CreateChannelByAddress(const std::string& /*address*/) override
    {
        YT_UNIMPLEMENTED();
    }

private:
    const NDiscoveryClient::TDiscoveryConnectionConfigPtr Config_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryServer
