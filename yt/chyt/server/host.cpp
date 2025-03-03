#include "host.h"

#include "clickhouse_invoker.h"
#include "clickhouse_service_proxy.h"
#include "config.h"
#include "custom_data_types.h"
#include "dictionary_source.h"
#include "health_checker.h"
#include "invoker_liveness_checker.h"
#include "memory_watchdog.h"
#include "query_context.h"
#include "query_registry.h"
#include "storage_distributor.h"
#include "storage_system_clique.h"
#include "storage_system_log_table_exporter.h"
#include "table_functions.h"
#include "user_defined_sql_objects_storage.h"
#include "yt_database.h"
#include "yt_directory_database.h"

#include <yt/yt/server/lib/misc/address_helpers.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/node_tracker_client/node_directory_synchronizer.h>

#include <yt/yt/ytlib/table_client/table_columnar_statistics_cache.h>

#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>
#include <yt/yt/ytlib/chunk_client/parallel_reader_memory_manager.h>

#include <yt/yt/ytlib/security_client/permission_cache.h>

#include <yt/yt/ytlib/object_client/object_attribute_cache.h>

#include <yt/yt/library/clickhouse_discovery/discovery_v1.h>
#include <yt/yt/library/clickhouse_discovery/discovery_v2.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/misc/crash_handler.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/caching_channel_factory.h>

#include <yt/yt/core/bus/tcp/config.h>

#include <Common/DateLUT.h>
#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <Interpreters/ProcessList.h>
#include <IO/HTTPCommon.h>


#include <util/system/env.h>

#include <atomic>
#include <memory>
#include <vector>

namespace NYT::NClickHouseServer {

using namespace NApi::NNative;
using namespace NProfiling;
using namespace NYTree;
using namespace NRpc::NBus;
using namespace NProto;
using namespace NSecurityClient;
using namespace NObjectClient;
using namespace NTracing;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NYPath;
using namespace NServer;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = ClickHouseYtLogger;

static const std::vector<std::string> DiscoveryAttributes{
    "host",
    "rpc_port",
    "monitoring_port",
    "tcp_port",
    "http_port",
    "pid",
    "job_cookie",
    "start_time",
    "clique_id",
    "clique_incarnation",
};

static const TString SysClickHouse = "//sys/clickhouse";

////////////////////////////////////////////////////////////////////////////////

class THost::TImpl
    : public TRefCounted, protected DB::WithMutableContext
{
public:
    TImpl(
        THost* owner,
        IInvokerPtr controlInvoker,
        TYtConfigPtr config,
        TConnectionCompoundConfigPtr connectionConfig,
        TPorts ports)
        : Owner_(owner)
        , ControlInvoker_(std::move(controlInvoker))
        , Config_(std::move(config))
        , Ports_(ports)
        , ConnectionConfig_(std::move(connectionConfig))
        , GossipExecutor_(New<TPeriodicExecutor>(
            ControlInvoker_,
            BIND(&TImpl::MakeGossip, MakeWeak(this)),
            Config_->Gossip->Period))
        , ControlInvokerChecker_(New<TInvokerLivenessChecker>(
            ControlInvoker_,
            Config_->ControlInvokerChecker,
            "Control"))
        , WorkerThreadPool_(CreateThreadPool(Config_->WorkerThreadCount, "Worker"))
        , WorkerInvoker_(WorkerThreadPool_->GetInvoker())
        , ClickHouseWorkerInvoker_(CreateClickHouseInvoker(WorkerInvoker_))
        , FetcherThreadPool_(CreateThreadPool(Config_->FetcherThreadCount, "Fetcher"))
        , FetcherInvoker_(FetcherThreadPool_->GetInvoker())
        , ClickHouseFetcherInvoker_(CreateClickHouseInvoker(FetcherInvoker_))
        , SystemLogTableExporterActionQueue_(New<TActionQueue>("SystemLogTableExporter"))
        , InstanceCookie_(std::stoi(GetEnv("YT_JOB_COOKIE", /*default =*/ "0")))
    {
        InitializeClients();
        InitializeCaches();
        InitializeReaderMemoryManager();
        RegisterFactories();

        // Configure clique's directory.
        Config_->Discovery->Directory += "/" + ToString(Config_->CliqueId);
        switch (Config_->Discovery->Version) {
            case 1: {
                Discovery_ = CreateDiscoveryV1(
                    Config_->Discovery,
                    RootClient_,
                    ControlInvoker_,
                    DiscoveryAttributes,
                    Logger());
                break;
            }
            case 2: {
                auto groupId = (Config_->CliqueAlias.empty()) ? ToString(Config_->CliqueId) : Config_->CliqueAlias;
                Config_->Discovery->GroupId = "/chyt/" + groupId;
                Discovery_ = CreateDiscoveryV2(
                    Config_->Discovery,
                    Connection_,
                    ChannelFactory_,
                    ControlInvoker_,
                    DiscoveryAttributes,
                    Logger(),
                    ClickHouseYtProfiler().WithPrefix("/discovery"));
                break;
            }
            default:
                YT_ABORT();
        }

        ClickHouseYtProfiler().AddFuncGauge(
            "/clique_instance_count",
            MakeStrong(this),
            [this] {
                return Config_->CliqueInstanceCount;
            });

        if (Config_->CpuLimit) {
            ClickHouseYtProfiler().AddFuncGauge(
                "/cpu_limit",
                MakeStrong(this),
                [this] {
                    return *Config_->CpuLimit;
                });
        }

        ClickHouseYtProfiler().AddFuncGauge(
            "/memory_limit/watchdog",
            MakeStrong(this),
            [this] {
                return Config_->MemoryWatchdog->MemoryLimit - Config_->MemoryWatchdog->CodicilWatermark;
            });

        ClickHouseYtProfiler().AddFuncGauge(
            "/memory_limit/oom",
            MakeStrong(this),
            [this] {
                return Config_->MemoryWatchdog->MemoryLimit;
            });
    }

    void SetContext(DB::ContextMutablePtr context_)
    {
        YT_VERIFY(context_ && context.expired());
        context = context_;
    }

    void InitQueryRegistry()
    {
        QueryRegistry_ = New<TQueryRegistry>(
            ControlInvoker_,
            getContext(),
            Config_->QueryRegistry);
    }

    void InitSingletones()
    {
        MemoryWatchdog_ = New<TMemoryWatchdog>(
            Config_->MemoryWatchdog,
            BIND(&TQueryRegistry::WriteStateToStderr, QueryRegistry_),
            BIND([] { raise(SIGINT); }));
        HealthChecker_ = New<THealthChecker>(
            Config_->HealthChecker,
            Config_->User,
            getContext(),
            Owner_);
    }

    DB::ContextMutablePtr GetContext() const
    {
        return getContext();
    }

    void Start()
    {
        YT_ASSERT_INVOKER_AFFINITY(GetControlInvoker());

        YT_VERIFY(getContext());

        if (Config_->ControlInvokerChecker->Enabled) {
            ControlInvokerChecker_->Start();
        }

        QueryRegistry_->Start();
        MemoryWatchdog_->Start();

        GossipExecutor_->Start();
        HealthChecker_->Start();

        CreateOrchidNode();
        StartDiscovery();

        WriteToStderr("*** Serving started ***\n");
    }

    void HandleIncomingGossip(const TString& instanceId, EInstanceState state)
    {
        ControlInvoker_->Invoke(BIND(&TImpl::DoHandleIncomingGossip, MakeWeak(this), instanceId, state));
    }

    TFuture<void> StopDiscovery()
    {
        GossipExecutor_->ScheduleOutOfBand();
        return Discovery_->Leave();
    }

    void ValidateCliquePermission(const TString& user, EPermission permission) const
    {
        auto key = TPermissionKey{
            .Object = Format("//sys/access_control_object_namespaces/chyt/%v/principal", ToYPathLiteral(Config_->CliqueAlias)),
            .User = user,
            .Permission = permission,
        };
        WaitFor(PermissionCache_->Get(key))
            .ThrowOnError();
    }

    void ValidateTableReadPermissions(
        const std::vector<NYPath::TRichYPath>& paths,
        const TString& user)
    {
        std::vector<TPermissionKey> permissionCacheKeys;
        permissionCacheKeys.reserve(paths.size());
        for (const auto& path : paths) {
            permissionCacheKeys.push_back(TPermissionKey{
                .Object = path.GetPath(),
                .User = user,
                .Permission = EPermission::Read,
                .Columns = path.GetColumns(),
            });
        }
        auto validationResults = WaitFor(PermissionCache_->GetMany(permissionCacheKeys))
            .ValueOrThrow();

        std::vector<TError> errors;
        for (size_t index = 0; index < validationResults.size(); ++index) {
            const auto& validationResult = validationResults[index];
            PermissionCache_->Set(permissionCacheKeys[index], validationResult);

            if (!validationResult.IsOK()) {
                errors.push_back(validationResult
                    << TErrorAttribute("path", paths[index])
                    << TErrorAttribute("permission", "read")
                    << TErrorAttribute("columns", paths[index].GetColumns()));
            }
        }
        if (!errors.empty()) {
            constexpr int MaxInnerErrorCount = 10;
            if (errors.size() > MaxInnerErrorCount) {
                errors.resize(MaxInnerErrorCount);
            }

            THROW_ERROR_EXCEPTION("Error validating permissions for user %Qv", user) << errors;
        }
    }

    std::vector<TErrorOr<NYTree::IAttributeDictionaryPtr>> GetObjectAttributes(
        const std::vector<TYPath>& paths,
        const IClientPtr& client)
    {
        if (paths.empty()) {
            return {};
        }

        const auto& user = client->GetOptions().GetAuthenticatedUser();
        auto cachedAttributes = TableAttributeCache_->FindMany(paths);
        std::vector<TYPath> missedPaths;
        for (int index = 0; index < (int)paths.size(); ++index) {
            if (!cachedAttributes[index].has_value()) {
                missedPaths.push_back(paths[index]);
            }
        }

        YT_LOG_DEBUG("Getting object attributes (HitCount: %v, MissedCount: %v, User: %v)",
            paths.size() - missedPaths.size(),
            missedPaths.size(),
            user);

        std::reverse(missedPaths.begin(), missedPaths.end());

        // TODO(max42): eliminate this.
        auto attributesForMissedPaths = WaitFor(TableAttributeCache_->GetFromClient(
            missedPaths,
            client))
            .ValueOrThrow();

        std::vector<TErrorOr<NYTree::IAttributeDictionaryPtr>> attributes;
        attributes.reserve(paths.size());

        for (auto& cachedAttributeOrError : cachedAttributes) {
            if (cachedAttributeOrError.has_value()) {
                attributes.emplace_back(std::move(*cachedAttributeOrError));
            } else {
                TableAttributeCache_->Set(missedPaths.back(), attributesForMissedPaths.back());
                attributes.emplace_back(std::move(attributesForMissedPaths.back()));
                attributesForMissedPaths.pop_back();
                missedPaths.pop_back();
            }
        }

        return attributes;
    }

    void InvalidateCachedObjectAttributes(const std::vector<TYPath>& paths)
    {
        YT_LOG_DEBUG("Invalidating locally cached table attributes (PathCount: %v)", paths.size());

        for (const auto& path : paths) {
            TableAttributeCache_->InvalidateActive(path);
        }
    }

    void InvalidateCachedObjectAttributesGlobally(const std::vector<TYPath>& paths, EInvalidateCacheMode mode, TDuration timeout)
    {
        YT_LOG_DEBUG("Invalidating cached table attributes in clique (PathCount: %v, Mode: %v, Timeout: %v)",
            paths.size(),
            mode,
            timeout);

        if (mode == EInvalidateCacheMode::None) {
            return;
        }

        InvalidateCachedObjectAttributes(paths);

        if (mode == EInvalidateCacheMode::Local) {
            return;
        }

        auto instances = Discovery_->List();

        using TResponse = NRpc::TTypedClientResponse<TRspInvalidateCachedObjectAttributes>::TResult;
        std::vector<TFuture<TResponse>> futures;
        futures.reserve(instances.size());

        for (auto [instanceId, attributes] : instances) {
            if (instanceId == ToString(Config_->InstanceId)) {
                // We have already invalidated attributes locally.
                continue;
            }

            auto channel = ChannelFactory_->CreateChannel(
                Format("%v:%v", attributes->Get<TString>("host"), attributes->Get<int>("rpc_port")));
            TClickHouseServiceProxy proxy(channel);

            auto req = proxy.InvalidateCachedObjectAttributes();
            req->SetTimeout(timeout);
            NYT::ToProto(req->mutable_table_paths(), paths);

            futures.push_back(req->Invoke());
        }

        if (mode == EInvalidateCacheMode::Sync) {
            WaitFor(AllSet(futures))
                .ThrowOnError();
        }
    }

    const TObjectAttributeCachePtr& GetObjectAttributeCache() const
    {
        return TableAttributeCache_;
    }

    TClusterNodes GetNodes(bool alwaysIncludeLocal) const
    {
        auto nodeList = FilterNodesByCliqueId(Discovery_->List());
        TClusterNodes result;
        result.reserve(nodeList.size());

        bool resultIncludesLocal = false;

        for (const auto& [_, attributes] : nodeList) {
            auto host = attributes->Get<TString>("host");
            auto tcpPort = attributes->Get<i64>("tcp_port");
            auto cookie = attributes->Get<i64>("job_cookie");
            bool isLocal = (host == Config_->Address) && (tcpPort == Ports_.Tcp);

            result.push_back(CreateClusterNode(
                TClusterNodeName{host, tcpPort},
                cookie,
                getContext()->getSettingsRef(),
                isLocal));

            resultIncludesLocal |= isLocal;
        }

        if (!resultIncludesLocal && alwaysIncludeLocal) {
            result.push_back(GetLocalNode());
        }

        return result;
    }

    IClusterNodePtr GetLocalNode() const
    {
        return CreateClusterNode(
            TClusterNodeName{*Config_->Address, Ports_.Tcp},
            GetInstanceCookie(),
            getContext()->getSettingsRef(),
            /*isLocal*/ true);
    }

    int GetInstanceCookie() const
    {
        return InstanceCookie_;
    }

    const IInvokerPtr& GetControlInvoker() const
    {
        return ControlInvoker_;
    }

    const IInvokerPtr& GetWorkerInvoker() const
    {
        return WorkerInvoker_;
    }

    const IInvokerPtr& GetClickHouseWorkerInvoker() const
    {
        return ClickHouseWorkerInvoker_;
    }

    const IInvokerPtr& GetFetcherInvoker() const
    {
        return FetcherInvoker_;
    }

    const IInvokerPtr& GetClickHouseFetcherInvoker() const
    {
        return ClickHouseFetcherInvoker_;
    }

    const IMultiReaderMemoryManagerPtr& GetMultiReaderMemoryManager() const
    {
        return ParallelReaderMemoryManager_;
    }

    void HandleCrashSignal() const
    {
        QueryRegistry_->WriteStateToStderr();
        WriteToStderr("*** Current query id (possible reason of failure): ");
        const auto& queryId = DB::CurrentThread::getQueryId();
        WriteToStderr(queryId.data(), queryId.size());
        WriteToStderr(" ***\n");

        if (DB::CurrentThread::isInitialized()) {
            auto& status = DB::CurrentThread::get();
            const auto& context = status.getQueryContext();
            if (context) {
                const auto* queryContext = GetQueryContext(context);
                WriteToStderr("*** Current user: ");
                WriteToStderr(queryContext->User.data(), queryContext->User.size());
                WriteToStderr(" ***\n");

                if (queryContext->InitialQuery) {
                    WriteToStderr("*** Begin of the initial query ***\n");
                    WriteToStderr(queryContext->InitialQuery->data(), queryContext->InitialQuery->size());
                    WriteToStderr("\n*** End of the initial query ***\n");
                } else {
                    WriteToStderr("*** Initial query is missing ***\n");
                }

                if (auto status = context->getProcessListElement()) {
                    const auto& info = status->getInfo();
                    WriteToStderr("*** Begin of the context query ***\n");
                    WriteToStderr(info.query.data(), info.query.size());
                    WriteToStderr("\n*** End of the context query ***\n");
                } else {
                    WriteToStderr("*** Query is not in the process list ***\n");
                }
            } else {
                WriteToStderr("*** Query context is unavailable ***\n");
            }
        } else {
            WriteToStderr("*** Current thread is not initialized ***\n");
        }
    }

    TFuture<void> GetIdleFuture() const
    {
        return QueryRegistry_->GetIdleFuture();
    }

    NApi::NNative::IClientPtr GetRootClient() const
    {
        return RootClient_;
    }

    NApi::NNative::IClientPtr CreateClient(const TString& user) const
    {
        auto identity = NRpc::TAuthenticationIdentity(user);
        auto options = NApi::TClientOptions::FromAuthenticationIdentity(identity);
        return ClientCache_->Get(identity, options);
    }

    void HandleSigint()
    {
        ++SigintCounter_;
    }

    TQueryRegistryPtr GetQueryRegistry() const
    {
        return QueryRegistry_;
    }

    TYtConfigPtr GetConfig() const
    {
        return Config_;
    }

    EInstanceState GetInstanceState() const
    {
        return SigintCounter_ == 0 ? EInstanceState::Active : EInstanceState::Stopped;
    }

    void PopulateSystemDatabase(DB::IDatabase* systemDatabase) const
    {
        systemDatabase->attachTable(
            getContext(),
            "clique",
            CreateStorageSystemClique(Discovery_, Config_->InstanceId));
    }

    DB::DatabasePtr CreateYTDatabase() const
    {
        return NYT::NClickHouseServer::CreateYTDatabase();
    }

    std::vector<DB::DatabasePtr> CreateUserDefinedDatabases() const
    {
        std::vector<DB::DatabasePtr> result;
        result.reserve(Config_->DatabaseDirectories.size());
        for (const auto& [databaseName, root] : Config_->DatabaseDirectories) {
            result.push_back(NYT::NClickHouseServer::CreateDirectoryDatabase(databaseName, root));
        }
        return result;
    }

    std::vector<TString> GetUserDefinedDatabaseNames() const
    {
        std::vector<TString> result;
        result.reserve(Config_->DatabaseDirectories.size());
        for (const auto& [databaseName, _] : Config_->DatabaseDirectories) {
            result.push_back(databaseName);
        }
        return result;
    }

    NTableClient::TTableColumnarStatisticsCachePtr GetTableColumnarStatisticsCache() const
    {
        return TableColumnarStatisticsCache_;
    }

    bool HasUserDefinedSqlObjectStorage() const
    {
        return Config_->UserDefinedSqlObjectsStorage->Enabled;
    }

    IUserDefinedSqlObjectsYTStorage* GetUserDefinedSqlObjectStorage()
    {
        return dynamic_cast<IUserDefinedSqlObjectsYTStorage*>(
            &getContext()->getUserDefinedSQLObjectsStorage());
    }

    void SetSqlObjectOnOtherInstances(const TString& objectName, const NClickHouseServer::TSqlObjectInfo& info) const
    {
        YT_LOG_DEBUG("Setting SQL object on other instances (ObjectName: %v)", objectName);

        auto instances = Discovery_->List();

        using TResponse = NRpc::TTypedClientResponse<TRspSetSqlObject>::TResult;
        std::vector<TFuture<TResponse>> futures;
        futures.reserve(instances.size());

        for (auto [instanceId, attributes] : instances) {
            if (instanceId == ToString(Config_->InstanceId)) {
                // We don't want to set object on the current instance.
                continue;
            }

            auto channel = ChannelFactory_->CreateChannel(
                Format("%v:%v", attributes->Get<TString>("host"), attributes->Get<int>("rpc_port")));
            TClickHouseServiceProxy proxy(channel);

            auto req = proxy.SetSqlObject();
            req->set_object_name(objectName);
            ToProto(req->mutable_object_info(), info);

            futures.push_back(req->Invoke());
        }

        WaitFor(AllSet(futures))
            .ThrowOnError();
    }

    void RemoveSqlObjectOnOtherInstances(const TString& objectName, NHydra::TRevision revision) const
    {
        YT_LOG_DEBUG("Removing SQL object on other instances (ObjectName: %v)", objectName);

        auto instances = Discovery_->List();

        using TResponse = NRpc::TTypedClientResponse<TRspRemoveSqlObject>::TResult;
        std::vector<TFuture<TResponse>> futures;
        futures.reserve(instances.size());

        for (auto [instanceId, attributes] : instances) {
            if (instanceId == ToString(Config_->InstanceId)) {
                // We don't want to remove object on the current instance.
                continue;
            }

            auto channel = ChannelFactory_->CreateChannel(
                Format("%v:%v", attributes->Get<TString>("host"), attributes->Get<int>("rpc_port")));
            TClickHouseServiceProxy proxy(channel);

            auto req = proxy.RemoveSqlObject();
            req->set_object_name(objectName);
            req->set_revision(ToProto(revision));

            futures.push_back(req->Invoke());
        }

        WaitFor(AllSet(futures))
            .ThrowOnError();
    }

private:
    THost* const Owner_;
    const IInvokerPtr ControlInvoker_;
    const TYtConfigPtr Config_;
    TPorts Ports_;
    const TConnectionCompoundConfigPtr ConnectionConfig_;
    THealthCheckerPtr HealthChecker_;
    TMemoryWatchdogPtr MemoryWatchdog_;
    TQueryRegistryPtr QueryRegistry_;
    TPeriodicExecutorPtr GossipExecutor_;
    TInvokerLivenessCheckerPtr ControlInvokerChecker_;
    IThreadPoolPtr WorkerThreadPool_;
    IInvokerPtr WorkerInvoker_;
    IInvokerPtr ClickHouseWorkerInvoker_;
    IThreadPoolPtr FetcherThreadPool_;
    IInvokerPtr FetcherInvoker_;
    IInvokerPtr ClickHouseFetcherInvoker_;
    TActionQueuePtr SystemLogTableExporterActionQueue_;

    NApi::NNative::IClientPtr RootClient_;
    NApi::NNative::IClientPtr CacheClient_;
    NApi::NNative::IConnectionPtr Connection_;
    NApi::NNative::TClientCachePtr ClientCache_;

    TPermissionCachePtr PermissionCache_;
    TObjectAttributeCachePtr TableAttributeCache_;
    NTableClient::TTableColumnarStatisticsCachePtr TableColumnarStatisticsCache_;

    IDiscoveryPtr Discovery_;
    int InstanceCookie_;

    NRpc::IChannelFactoryPtr ChannelFactory_;

    THashSet<TString> KnownInstances_;
    THashMap<TString, int> UnknownInstancePingCounter_;

    IMultiReaderMemoryManagerPtr ParallelReaderMemoryManager_;

    std::atomic<int> SigintCounter_ = {0};

    void InitializeClients()
    {
        NApi::NNative::TConnectionOptions connectionOptions;
        connectionOptions.RetryRequestQueueSizeLimitExceeded = true;

        ChannelFactory_ = CreateCachingChannelFactory(CreateTcpBusChannelFactory(New<NBus::TBusConfig>()));

        Connection_ = NApi::NNative::CreateConnection(
            ConnectionConfig_,
            connectionOptions);

        // Kick-start node directory synchronizing; otherwise it will start only with first query.
        Connection_->GetNodeDirectorySynchronizer()->Start();

        ClientCache_ = New<NApi::NNative::TClientCache>(Config_->ClientCache, Connection_);

        auto getClientForUser = [&] (const TString& user) {
            auto identity = NRpc::TAuthenticationIdentity(user);
            auto options = NApi::TClientOptions::FromAuthenticationIdentity(identity);
            return ClientCache_->Get(identity, options);
        };
        RootClient_ = getClientForUser(Config_->User);
        CacheClient_ = getClientForUser(CacheUserName);
    }

    void InitializeCaches()
    {
        PermissionCache_ = New<TPermissionCache>(
            Config_->PermissionCache,
            Connection_,
            ClickHouseYtProfiler().WithPrefix("/permission_cache"));

        TableAttributeCache_ = New<NObjectClient::TObjectAttributeCache>(
            Config_->TableAttributeCache,
            TableAttributesToFetch,
            CacheClient_,
            ControlInvoker_,
            Logger(),
            ClickHouseYtProfiler().WithPrefix("/object_attribute_cache"));

        TableColumnarStatisticsCache_ = New<NTableClient::TTableColumnarStatisticsCache>(
            Config_->TableColumnarStatisticsCache,
            CacheClient_,
            FetcherInvoker_,
            Logger(),
            ClickHouseYtProfiler().WithPrefix("/table_columnar_statistics_cache"));
    }

    void InitializeReaderMemoryManager()
    {
        TParallelReaderMemoryManagerOptions parallelReaderMemoryManagerOptions{
            .TotalReservedMemorySize = Config_->TotalReaderMemoryLimit,
            .MaxInitialReaderReservedMemory = Config_->TotalReaderMemoryLimit,
            .EnableProfiling = true
        };
        ParallelReaderMemoryManager_ = CreateParallelReaderMemoryManager(
            parallelReaderMemoryManagerOptions,
            NChunkClient::TDispatcher::Get()->GetReaderMemoryManagerInvoker());
    }

    void StartDiscovery()
    {
        if (Discovery_->Version() == 1) {
            NApi::TCreateNodeOptions createCliqueNodeOptions;
            createCliqueNodeOptions.IgnoreExisting = true;
            createCliqueNodeOptions.Recursive = true;
            createCliqueNodeOptions.Attributes = ConvertToAttributes(THashMap<TString, i64>{{"discovery_version", Discovery_->Version()}});
            WaitFor(RootClient_->CreateNode(
                Config_->Discovery->Directory,
                NObjectClient::EObjectType::MapNode,
                createCliqueNodeOptions))
                .ThrowOnError();
        }

        YT_UNUSED_FUTURE(Discovery_->StartPolling());

        auto attributes = ConvertToAttributes(THashMap<TString, INodePtr>{
            {"host", ConvertToNode(Config_->Address)},
            {"rpc_port", ConvertToNode(Ports_.Rpc)},
            {"monitoring_port", ConvertToNode(Ports_.Monitoring)},
            {"tcp_port", ConvertToNode(Ports_.Tcp)},
            {"http_port", ConvertToNode(Ports_.Http)},
            {"pid", ConvertToNode(getpid())},
            {"job_cookie", ConvertToNode(InstanceCookie_)},
            {"start_time", ConvertToNode(TInstant::Now())},
            {"clique_id", ConvertToNode(Config_->CliqueId)},
            {"clique_incarnation", ConvertToNode(Config_->CliqueIncarnation)},
        });

        if (Config_->QueryStickyGroupSize) {
            attributes->Set("query_sticky_group_size", *Config_->QueryStickyGroupSize);
        }

        WaitFor(Discovery_->Enter(ToString(Config_->InstanceId), attributes))
            .ThrowOnError();

        // Update after entering the group guarantees that we will notify all
        // alive instances via gossip about new one.
        YT_UNUSED_FUTURE(Discovery_->UpdateList());
    }

    THashMap<TString, NYTree::IAttributeDictionaryPtr> FilterNodesByCliqueId(const THashMap<TString, NYTree::IAttributeDictionaryPtr>& nodes) const
    {
        THashMap<TString, NYTree::IAttributeDictionaryPtr> result;
        for (const auto& [key, attributes] : nodes) {
            if (!attributes || !attributes->Contains("clique_id")) {
                continue;
            }
            auto cliqueId = attributes->Find<TGuid>("clique_id");
            if (cliqueId == Config_->CliqueId) {
                result.emplace(key, attributes);
            }
        }
        return result;
    }

    void MakeGossip()
    {
        YT_LOG_DEBUG("Gossip started");

        // Instances can be banned because of transient errors (e.g. network errors).
        // Pinging banned instances can help to restore clique faster after such errors.
        auto nodes = FilterNodesByCliqueId(Discovery_->List(/*includeBanned*/ Config_->Gossip->PingBanned));
        std::vector<TFuture<NRpc::TTypedClientResponse<TRspProcessGossip>::TResult>> futures;
        futures.reserve(nodes.size());
        auto selfState = GetInstanceState();

        for (auto [_, attributes] : nodes) {
            auto channel = ChannelFactory_->CreateChannel(
                attributes->Get<TString>("host") + ":" + ToString(attributes->Get<ui64>("rpc_port")));
            TClickHouseServiceProxy proxy(channel);
            auto req = proxy.ProcessGossip();
            req->SetTimeout(Config_->Gossip->Timeout);
            req->set_instance_id(ToString(Config_->InstanceId));
            req->set_instance_state(ToProto(selfState));
            futures.push_back(req->Invoke());
        }
        auto responses = WaitFor(AllSet(futures))
            .ValueOrThrow();

        // TODO(max42): better logging.

        std::vector<TString> alive;
        std::vector<TString> dead;

        alive.reserve(nodes.size());

        auto responseIt = responses.begin();
        for (auto [name, attributes] : nodes) {
            if (!responseIt->IsOK() || responseIt->Value()->instance_id() != name ||
                FromProto<EInstanceState>(responseIt->Value()->instance_state()) == EInstanceState::Stopped)
            {
                YT_LOG_WARNING("Banning instance (Address: %v, HttpPort: %v, TcpPort: %v, RpcPort: %v, JobId: %v, State: %v)",
                    attributes->Get<TString>("host"),
                    attributes->Get<ui64>("http_port"),
                    attributes->Get<ui64>("tcp_port"),
                    attributes->Get<ui64>("rpc_port"),
                    name,
                    (responseIt->IsOK() ? Format("%v", FromProto<EInstanceState>(responseIt->Value()->instance_state())) : "Request failed"));
                dead.push_back(name);
            } else {
                alive.push_back(name);
            }
            ++responseIt;
        }

        if (Config_->Gossip->AllowUnban) {
            Discovery_->Unban(alive);
        }
        Discovery_->Ban(dead);

        YT_LOG_DEBUG("Gossip completed (Alive: %v, Dead: %v)", alive.size(), dead.size());
    }

    void DoHandleIncomingGossip(const TString& instanceId, EInstanceState state)
    {
        if (state != EInstanceState::Active) {
            YT_LOG_DEBUG("Received gossip from non-active instance (InstanceId: %v, State: %v)",
                instanceId,
                state);
            Discovery_->Ban(instanceId);
            return;
        }

        if (Config_->Gossip->AllowUnban) {
            Discovery_->Unban(instanceId);
        }

        if (KnownInstances_.contains(instanceId)) {
            return;
        }

        auto& counter = UnknownInstancePingCounter_[instanceId];
        ++counter;

        YT_LOG_DEBUG("Received gossip from unknown instance (InstanceId: %v, State: %v, Counter: %v)",
            instanceId,
            state,
            counter);

        if (counter >= Config_->Gossip->UnknownInstancePingLimit) {
            return;
        }

        for (const auto& [name, _] : Discovery_->List(/*eraseBanned*/ false)) {
            if (KnownInstances_.insert(name).second) {
                UnknownInstancePingCounter_.erase(name);
            }
        }

        if (KnownInstances_.contains(instanceId))  {
            return;
        }

        YT_UNUSED_FUTURE(Discovery_->UpdateList(Config_->Gossip->UnknownInstanceAgeThreshold));
    }

    void CreateOrchidNode()
    {
        const auto& host = NNet::GetLocalHostName();
        NApi::TCreateNodeOptions options;
        options.Recursive = true;
        options.Force = true;
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("remote_addresses", GetLocalAddresses({{"default", host}}, Ports_.Rpc));
        options.Attributes = std::move(attributes);

        auto path = SysClickHouse + "/orchids/" + ToString(Config_->CliqueId) + "/" + ToString(InstanceCookie_);

        WaitFor(RootClient_->CreateNode(path, EObjectType::Orchid, options))
            .ThrowOnError();

        YT_LOG_INFO("Initialized orchid node (Host: %v, Port: %v, OrchidNodePath: %v)",
            host,
            Ports_.Rpc,
            path);
    }

    void RegisterFactories()
    {
        RegisterTableFunctions();
        RegisterTableDictionarySource(Owner_);
        RegisterStorageDistributor();
        RegisterStorageSystemLogTableExporter(
            Owner_,
            GetRootClient(),
            SystemLogTableExporterActionQueue_->GetInvoker());
        RegisterDataTypeBoolean();
        RegisterDataTypeTimestamp();
    }
};

////////////////////////////////////////////////////////////////////////////////

THost::THost(
    IInvokerPtr controlInvoker,
    TPorts ports,
    TYtConfigPtr config,
    TConnectionCompoundConfigPtr connectionConfig)
    : Impl_(New<TImpl>(
        this,
        std::move(controlInvoker),
        std::move(config),
        std::move(connectionConfig),
        ports))
{ }

void THost::Start()
{
    Impl_->Start();
}

void THost::HandleIncomingGossip(const TString& instanceId, EInstanceState state)
{
    Impl_->HandleIncomingGossip(instanceId, state);
}

TFuture<void> THost::StopDiscovery()
{
    return Impl_->StopDiscovery();
}

void THost::ValidateCliquePermission(const TString& user, EPermission permission) const
{
    return Impl_->ValidateCliquePermission(user, permission);
}

void THost::ValidateTableReadPermissions(
    const std::vector<NYPath::TRichYPath>& paths,
    const TString& user)
{
    return Impl_->ValidateTableReadPermissions(paths, user);
}

std::vector<TErrorOr<NYTree::IAttributeDictionaryPtr>> THost::GetObjectAttributes(
    const std::vector<NYPath::TYPath>& paths,
    const IClientPtr& client)
{
    return Impl_->GetObjectAttributes(paths, client);
}

void THost::InvalidateCachedObjectAttributes(const std::vector<NYPath::TYPath>& paths)
{
    Impl_->InvalidateCachedObjectAttributes(paths);
}

void THost::InvalidateCachedObjectAttributesGlobally(
    const std::vector<NYPath::TYPath>& paths,
    EInvalidateCacheMode mode,
    TDuration timeout)
{
    Impl_->InvalidateCachedObjectAttributesGlobally(paths, mode, timeout);
}

const TObjectAttributeCachePtr&     THost::GetObjectAttributeCache() const
{
    return Impl_->GetObjectAttributeCache();
}

const IInvokerPtr& THost::GetControlInvoker() const
{
    return Impl_->GetControlInvoker();
}

const IInvokerPtr& THost::GetWorkerInvoker() const
{
    return Impl_->GetWorkerInvoker();
}

const IInvokerPtr& THost::GetClickHouseWorkerInvoker() const
{
    return Impl_->GetClickHouseWorkerInvoker();
}

const IInvokerPtr& THost::GetFetcherInvoker() const
{
    return Impl_->GetFetcherInvoker();
}

const IInvokerPtr& THost::GetClickHouseFetcherInvoker() const
{
    return Impl_->GetClickHouseFetcherInvoker();
}

TClusterNodes THost::GetNodes(bool alwaysIncludeLocal) const
{
    return Impl_->GetNodes(alwaysIncludeLocal);
}

IClusterNodePtr THost::GetLocalNode() const
{
    return Impl_->GetLocalNode();
}

int THost::GetInstanceCookie() const
{
    return Impl_->GetInstanceCookie();
}

void THost::HandleCrashSignal() const
{
    return Impl_->HandleCrashSignal();
}

void THost::HandleSigint()
{
    return Impl_->HandleSigint();
}

const IMultiReaderMemoryManagerPtr& THost::GetMultiReaderMemoryManager() const
{
    return Impl_->GetMultiReaderMemoryManager();
}

NApi::NNative::IClientPtr THost::GetRootClient() const
{
    return Impl_->GetRootClient();
}

NApi::NNative::IClientPtr THost::CreateClient(const TString& user) const
{
    return Impl_->CreateClient(user);
}

TFuture<void> THost::GetIdleFuture() const
{
    return Impl_->GetIdleFuture();
}

TQueryRegistryPtr THost::GetQueryRegistry() const
{
    return Impl_->GetQueryRegistry();
}

TYtConfigPtr THost::GetConfig() const
{
    return Impl_->GetConfig();
}

EInstanceState THost::GetInstanceState() const
{
    return Impl_->GetInstanceState();
}

void THost::PopulateSystemDatabase(DB::IDatabase* systemDatabase) const
{
    return Impl_->PopulateSystemDatabase(systemDatabase);
}

std::shared_ptr<DB::IDatabase> THost::CreateYTDatabase() const
{
    return Impl_->CreateYTDatabase();
}

std::vector<DB::DatabasePtr> THost::CreateUserDefinedDatabases() const
{
    return Impl_->CreateUserDefinedDatabases();
}

std::vector<TString> THost::GetUserDefinedDatabaseNames() const
{
    return Impl_->GetUserDefinedDatabaseNames();
}

void THost::SetContext(DB::ContextMutablePtr context)
{
    Impl_->SetContext(context);
}

DB::ContextMutablePtr THost::GetContext() const
{
    return Impl_->GetContext();
}

void THost::InitQueryRegistry()
{
    Impl_->InitQueryRegistry();
}

void THost::InitSingletones()
{
    return Impl_->InitSingletones();
}

NTableClient::TTableColumnarStatisticsCachePtr THost::GetTableColumnarStatisticsCache() const
{
    return Impl_->GetTableColumnarStatisticsCache();
}

THost::~THost() = default;


bool THost::HasUserDefinedSqlObjectStorage() const
{
    return Impl_->HasUserDefinedSqlObjectStorage();
}

IUserDefinedSqlObjectsYTStorage* THost::GetUserDefinedSqlObjectStorage()
{
    return Impl_->GetUserDefinedSqlObjectStorage();
}

void THost::SetSqlObjectOnOtherInstances(const TString& objectName, const NClickHouseServer::TSqlObjectInfo& info) const
{
    Impl_->SetSqlObjectOnOtherInstances(objectName, info);
}

void THost::RemoveSqlObjectOnOtherInstances(const TString& objectName, NHydra::TRevision revision) const
{
    Impl_->RemoveSqlObjectOnOtherInstances(objectName, revision);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
