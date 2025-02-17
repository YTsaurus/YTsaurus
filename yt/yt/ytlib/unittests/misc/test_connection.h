#pragma once

#include <yt/yt/ytlib/auth/native_authentication_manager.h>
#include <yt/yt/ytlib/auth/native_authenticating_channel.h>

#include <yt/yt/ytlib/chaos_client/banned_replica_tracker.h>
#include <yt/yt/ytlib/chaos_client/chaos_cell_directory_synchronizer.h>
#include <yt/yt/ytlib/chaos_client/chaos_cell_channel_factory.h>
#include <yt/yt/ytlib/chaos_client/config.h>
#include <yt/yt/ytlib/chaos_client/native_replication_card_cache_detail.h>
#include <yt/yt/ytlib/chaos_client/replication_card_channel_factory.h>
#include <yt/yt/ytlib/chaos_client/chaos_residency_cache.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory.h>
#include <yt/yt/ytlib/cell_master_client/cell_directory_synchronizer.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_cache.h>
#include <yt/yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/yt/ytlib/chunk_client/medium_directory.h>
#include <yt/yt/ytlib/chunk_client/medium_directory_synchronizer.h>
#include <yt/yt/ytlib/chunk_client/chunk_replica_cache.h>

#include <yt/yt/ytlib/hive/config.h>
#include <yt/yt/ytlib/hive/cell_directory.h>
#include <yt/yt/ytlib/hive/cell_directory_synchronizer.h>
#include <yt/yt/ytlib/hive/cluster_directory.h>
#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>
#include <yt/yt/ytlib/hive/downed_cell_tracker.h>
#include <yt/yt/ytlib/hive/hive_service_proxy.h>

#include <yt/yt/ytlib/hydra/peer_channel.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>
#include <yt/yt/library/query/engine_api/evaluator.h>
#include <yt/yt/library/query/engine_api/expression_evaluator.h>

#include <yt/yt/ytlib/query_client/functions_cache.h>

#include <yt/yt/ytlib/queue_client/config.h>
#include <yt/yt/ytlib/queue_client/registration_manager.h>

#include <yt/yt/ytlib/query_tracker_client/config.h>

#include <yt/yt/ytlib/yql_client/config.h>

#include <yt/yt/ytlib/discovery_client/discovery_client.h>
#include <yt/yt/ytlib/discovery_client/member_client.h>
#include <yt/yt/ytlib/discovery_client/request_session.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>
#include <yt/yt/ytlib/node_tracker_client/node_addresses_provider.h>
#include <yt/yt/ytlib/node_tracker_client/node_directory_synchronizer.h>

#include <yt/yt/ytlib/job_prober_client/job_shell_descriptor_cache.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/scheduler/config.h>
#include <yt/yt/ytlib/scheduler/scheduler_channel.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/bundle_controller/bundle_controller_channel.h>

#include <yt/yt/ytlib/security_client/permission_cache.h>
#include <yt/yt/ytlib/security_client/user_attribute_cache.h>

#include <yt/yt/ytlib/tablet_client/native_table_mount_cache.h>

#include <yt/yt/ytlib/transaction_client/config.h>
#include <yt/yt/ytlib/transaction_client/clock_manager.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/tablet_sync_replica_cache.h>
#include <yt/yt/ytlib/api/native/table_replica_synchronicity_cache.h>
#include <yt/yt/ytlib/api/native/client_impl.h>
#include <yt/yt/ytlib/api/native/sync_replica_cache.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/hive/transaction_participant.h>

#include <yt/yt/client/api/sticky_transaction_pool.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/sequoia_client/public.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/internal_client.h>
#include <yt/yt/client/api/transaction_client.h>

#include <yt/yt/client/transaction_client/config.h>
#include <yt/yt/client/transaction_client/noop_timestamp_provider.h>
#include <yt/yt/client/transaction_client/remote_timestamp_provider.h>

#include <yt/yt/library/tvm/service/tvm_service.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/lease_manager.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/bus/public.h>

#include <yt/yt/core/rpc/bus/channel.h>

#include <yt/yt/core/rpc/public.h>
#include <yt/yt/core/rpc/channel.h>
#include <yt/yt/core/rpc/service_detail.h>
#include <yt/yt/core/rpc/roaming_channel.h>
#include <yt/yt/core/rpc/caching_channel_factory.h>
#include <yt/yt/core/rpc/balancing_channel.h>
#include <yt/yt/core/rpc/retrying_channel.h>
#include <yt/yt/core/rpc/helpers.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/attributes.h>

#include <yt/yt/core/misc/checksum.h>
#include <yt/yt/core/misc/lazy_ptr.h>
#include <yt/yt/core/misc/memory_usage_tracker.h>

#include <library/cpp/yt/threading/atomic_object.h>

#include <library/cpp/testing/gtest_extensions/gtest_extensions.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

class TTestConnection
    : public IConnection
{
public:
    MOCK_METHOD(void, SubscribeReconfigured, (const TCallback<TReconfiguredSignature>& newConfig), (override));
    MOCK_METHOD(void, UnsubscribeReconfigured, (const TCallback<TReconfiguredSignature>& newConfig), (override));

public:
    explicit TTestConnection(
        NRpc::IChannelFactoryPtr channelFactory,
        NNodeTrackerClient::TNetworkPreferenceList networkPreferenceList,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
        IInvokerPtr invoker,
        INodeMemoryTrackerPtr nodeMemoryTracker);

    MOCK_METHOD(TConnectionCompoundConfigPtr, GetCompoundConfig, (), (const, override));
    MOCK_METHOD(NObjectClient::TCellId, GetPrimaryMasterCellId, (), (const, override));
    MOCK_METHOD(NObjectClient::TCellTag, GetPrimaryMasterCellTag, (), (const, override));
    MOCK_METHOD(NObjectClient::TCellTagList, GetSecondaryMasterCellTags, (), (const, override));
    MOCK_METHOD(NObjectClient::TCellTag, GetRandomMasterCellTagWithRoleOrThrow, (NCellMasterClient::EMasterCellRole), (const, override));
    MOCK_METHOD(NObjectClient::TCellId, GetMasterCellId, (NObjectClient::TCellTag), (const, override));
    MOCK_METHOD(const NQueryClient::IEvaluatorPtr&, GetQueryEvaluator, (), (override));
    MOCK_METHOD(const NQueryClient::IColumnEvaluatorCachePtr&, GetColumnEvaluatorCache, (), (override));
    MOCK_METHOD(const NQueryClient::IExpressionEvaluatorCachePtr&, GetExpressionEvaluatorCache, (), (override));
    MOCK_METHOD(const NChunkClient::IBlockCachePtr&, GetBlockCache, (), (override));
    MOCK_METHOD(const NChunkClient::IClientChunkMetaCachePtr&, GetChunkMetaCache, (), (override));
    MOCK_METHOD(const NCellMasterClient::ICellDirectoryPtr&, GetMasterCellDirectory, (), (override));
    MOCK_METHOD(const NCellMasterClient::ICellDirectorySynchronizerPtr&, GetMasterCellDirectorySynchronizer, (), (override));
    MOCK_METHOD(const NHiveClient::ICellDirectorySynchronizerPtr&, GetCellDirectorySynchronizer, (), (override));
    MOCK_METHOD(const NChaosClient::IChaosCellDirectorySynchronizerPtr&, GetChaosCellDirectorySynchronizer, (), (override));
    MOCK_METHOD(NHiveClient::TClusterDirectoryPtr, GetClusterDirectory, (), (const, override));
    MOCK_METHOD(const NHiveClient::IClusterDirectorySynchronizerPtr&, GetClusterDirectorySynchronizer, (), (override));
    MOCK_METHOD(const NChunkClient::TMediumDirectorySynchronizerPtr&, GetMediumDirectorySynchronizer, (), (override));
    MOCK_METHOD(const NNodeTrackerClient::INodeDirectorySynchronizerPtr&, GetNodeDirectorySynchronizer, (), (override));
    MOCK_METHOD(const NChunkClient::IChunkReplicaCachePtr&, GetChunkReplicaCache, (), (override));
    MOCK_METHOD((std::pair<IClientPtr, NYPath::TYPath>), GetQueryTrackerStage, (const TString&), (override));
    MOCK_METHOD(NRpc::IChannelPtr, GetQueryTrackerChannelOrThrow, (const TString&), (override));
    MOCK_METHOD(NRpc::IChannelPtr, GetChaosChannelByCellId, (NObjectClient::TCellId, NHydra::EPeerKind), (override));
    MOCK_METHOD(NRpc::IChannelPtr, GetChaosChannelByCellTag, (NObjectClient::TCellTag, NHydra::EPeerKind), (override));
    MOCK_METHOD(NRpc::IChannelPtr, GetChaosChannelByCardId, (NChaosClient::TReplicationCardId, NHydra::EPeerKind), (override));
    MOCK_METHOD(NRpc::IChannelPtr, FindQueueAgentChannel, (TStringBuf), (const, override));
    MOCK_METHOD(const NQueueClient::TQueueConsumerRegistrationManagerPtr&, GetQueueConsumerRegistrationManager, (), (const, override));
    MOCK_METHOD(NRpc::IRoamingChannelProviderPtr, GetYqlAgentChannelProviderOrThrow, (const TString&), (const, override));
    MOCK_METHOD(const NTabletClient::ITableMountCachePtr&, GetTableMountCache, (), (override));
    MOCK_METHOD(const NChaosClient::IReplicationCardCachePtr&, GetReplicationCardCache, (), (override));
    MOCK_METHOD(const NChaosClient::IChaosResidencyCachePtr&, GetChaosResidencyCache, (), (override));
    MOCK_METHOD(const NTransactionClient::ITimestampProviderPtr&, GetTimestampProvider, (), (override));
    MOCK_METHOD(const NJobProberClient::TJobShellDescriptorCachePtr&, GetJobShellDescriptorCache, (), (override));
    MOCK_METHOD(const NSecurityClient::TPermissionCachePtr&, GetPermissionCache, (), (override));
    MOCK_METHOD(const NSecurityClient::TUserAttributeCachePtr&, GetUserAttributeCache, (), (override));
    MOCK_METHOD(const TStickyGroupSizeCachePtr&, GetStickyGroupSizeCache, (), (override));
    MOCK_METHOD(const TSyncReplicaCachePtr&, GetSyncReplicaCache, (), (override));
    MOCK_METHOD(const TTabletSyncReplicaCachePtr&, GetTabletSyncReplicaCache, (), (override));
    MOCK_METHOD(const NChaosClient::IBannedReplicaTrackerCachePtr&, GetBannedReplicaTrackerCache, (), (override));
    MOCK_METHOD(const ITableReplicaSynchronicityCachePtr&, GetTableReplicaSynchronicityCache, (), (override));
    MOCK_METHOD(std::vector<std::string>, GetDiscoveryServerAddresses, (), (const, override));
    MOCK_METHOD(NDiscoveryClient::IDiscoveryClientPtr, CreateDiscoveryClient, (NDiscoveryClient::TDiscoveryClientConfigPtr, NRpc::IChannelFactoryPtr), (override));
    MOCK_METHOD(NDiscoveryClient::IMemberClientPtr, CreateMemberClient, (NDiscoveryClient::TMemberClientConfigPtr, NRpc::IChannelFactoryPtr, IInvokerPtr, NDiscoveryClient::TMemberId, NDiscoveryClient::TGroupId), (override));
    MOCK_METHOD(NYTree::IYPathServicePtr, GetOrchidService, (), (override));
    MOCK_METHOD(void, Terminate, (), (override));
    MOCK_METHOD(void, InitializeDiscoveryServerAddressPool, (), (override));
    MOCK_METHOD(bool, IsTerminated, (), (const, override));
    MOCK_METHOD(TFuture<void>, SyncHiveCellWithOthers, (const std::vector<NElection::TCellId>&, NElection::TCellId), (override));
    MOCK_METHOD(const NLogging::TLogger&, GetLogger, (), (override));
    MOCK_METHOD(NRpc::IChannelPtr, CreateChannel, (bool), ());
    MOCK_METHOD(const TConnectionConfigPtr&, GetConfig, (), ());
    MOCK_METHOD(TClusterTag, GetClusterTag, (), (const, override));
    MOCK_METHOD(const std::string&, GetLoggingTag, (), (const, override));
    MOCK_METHOD(const TString&, GetClusterId, (), (const, override));
    MOCK_METHOD(const std::optional<std::string>&, GetClusterName, (), (const, override));
    MOCK_METHOD(bool, IsSameCluster, (const TIntrusivePtr<NApi::IConnection>&), (const, override));
    MOCK_METHOD(NHiveClient::ITransactionParticipantPtr, CreateTransactionParticipant, (NHiveClient::TCellId, const NApi::TTransactionParticipantOptions&), (override));
    MOCK_METHOD(void, ClearMetadataCaches, (), (override));
    MOCK_METHOD(NYson::TYsonString, GetConfigYson, (), (const, override));
    MOCK_METHOD(NApi::IClientPtr, CreateClient, (const NApi::TClientOptions&), (override));
    MOCK_METHOD(void, Reconfigure, (const TConnectionDynamicConfigPtr&), (override));
    MOCK_METHOD(NRpc::IChannelPtr, GetShuffleServiceChannelOrThrow, (), (override));
    MOCK_METHOD(void, RegisterShuffleService, (const std::string&), (override));

    const TConnectionStaticConfigPtr& GetStaticConfig() const override;
    const NNodeTrackerClient::TNetworkPreferenceList& GetNetworks() const override;
    TConnectionDynamicConfigPtr GetConfig() const override;
    NRpc::IChannelPtr CreateChannelByAddress(const std::string& address) override;
    IClientPtr CreateNativeClient(const TClientOptions& options) override;
    const NRpc::IChannelFactoryPtr& GetChannelFactory() override;
    const NNodeTrackerClient::TNodeDirectoryPtr& GetNodeDirectory() override;

    const NRpc::IChannelPtr& GetSchedulerChannel() override;
    const NRpc::IChannelPtr& GetBundleControllerChannel() override;

    const NTransactionClient::IClockManagerPtr& GetClockManager() override;
    const NHiveClient::ICellDirectoryPtr& GetCellDirectory() override;

    const NHiveClient::TDownedCellTrackerPtr& GetDownedCellTracker() override;
    const NChunkClient::TMediumDirectoryPtr& GetMediumDirectory() override;

    NRpc::IChannelPtr GetMasterChannelOrThrow(
        EMasterChannelKind kind,
        NObjectClient::TCellTag cellTag = NObjectClient::PrimaryMasterCellTagSentinel) override;
    NRpc::IChannelPtr GetMasterChannelOrThrow(
        EMasterChannelKind kind,
        NObjectClient::TCellId cellId) override;
    NRpc::IChannelPtr GetCypressChannelOrThrow(
        EMasterChannelKind kind,
        NObjectClient::TCellTag cellTag = NObjectClient::PrimaryMasterCellTagSentinel) override;

    IInvokerPtr GetInvoker() override;

private:
    const NRpc::IChannelFactoryPtr ChannelFactory_;
    const NNodeTrackerClient::TNetworkPreferenceList NetworkPreferenceList_;
    const TConnectionStaticConfigPtr Config_;
    const TConnectionDynamicConfigPtr DynamicConfig_;
    const IInvokerPtr Invoker_;
    const INodeMemoryTrackerPtr NodeMemoryTracker_;
    const NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;
    const NRpc::IChannelPtr SchedulerChannel_;
    const NRpc::IChannelPtr BundleControllerChannel_;
    const NTransactionClient::IClockManagerPtr ClockManager_;
    const NHiveClient::ICellDirectoryPtr CellDirectory_;
    const NHiveClient::TDownedCellTrackerPtr DownedCellTracker_;
    const NChunkClient::TMediumDirectoryPtr MediumDirectory_;
};

DECLARE_REFCOUNTED_CLASS(TTestConnection)
DEFINE_REFCOUNTED_TYPE(TTestConnection)

////////////////////////////////////////////////////////////////////////////////

NApi::NNative::IConnectionPtr CreateConnection(
    NRpc::IChannelFactoryPtr channelFactory,
    NNodeTrackerClient::TNetworkPreferenceList networkPreferenceList,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    IInvokerPtr invoker,
    INodeMemoryTrackerPtr nodeMemoryTracker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
