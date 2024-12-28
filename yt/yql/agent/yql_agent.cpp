#include "yql_agent.h"

#include "config.h"
#include "interop.h"

#include <library/cpp/yt/logging/backends/arcadia/backend.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/hive/cluster_directory.h>
#include <yt/yt/ytlib/yql_client/public.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/logging/config.h>

#include <yt/yt/core/net/config.h>

#include <yt/yql/plugin/bridge/plugin.h>

namespace NYT::NYqlAgent {

using namespace NConcurrency;
using namespace NYTree;
using namespace NHiveClient;
using namespace NYqlClient;
using namespace NYqlClient::NProto;
using namespace NYson;
using namespace NHiveClient;
using namespace NSecurityClient;
using namespace NLogging;

static constexpr auto& Logger = YqlAgentLogger;

////////////////////////////////////////////////////////////////////////////////

class TActiveQueriesGuard
{
public:
    TActiveQueriesGuard() = delete;

    TActiveQueriesGuard(int maxSimultaneousQueries, std::atomic<int>* activeQueries)
        : ActiveQueries_(activeQueries)
    {
        IsTaken_ = true;
        auto queries = ActiveQueries_->load();
        do {
            if (queries >= maxSimultaneousQueries) {
                IsTaken_ = false;
                return;
            }
        } while (!ActiveQueries_->compare_exchange_weak(queries, queries + 1));
    }

    ~TActiveQueriesGuard()
    {
        if (IsTaken_) {
            ActiveQueries_->fetch_add(-1);
        }
    }

    bool IsTaken()
    {
        return IsTaken_;
    }

private:
    std::atomic<int>* const ActiveQueries_;
    bool IsTaken_;
};

class TActiveQueriesGuardFactory
{
public:
    explicit TActiveQueriesGuardFactory(int maxSimultaneousQueries)
        : MaxSimultaneousQueries_(maxSimultaneousQueries)
    { }

    void Update(int maxSimultaneousQueries)
    {
        MaxSimultaneousQueries_ = maxSimultaneousQueries;
    }

    TActiveQueriesGuard CreateGuard()
    {
        return TActiveQueriesGuard(MaxSimultaneousQueries_, &ActiveQueries_);
    }

    int GetGuardedValue() const
    {
        return ActiveQueries_.load();
    }

private:
    int MaxSimultaneousQueries_;
    std::atomic<int> ActiveQueries_;
};

////////////////////////////////////////////////////////////////////////////////

static std::optional<TString> TryIssueToken(const TQueryId queryId, const TString& user, const std::vector<TString>& clusters, THashMap<TString, NApi::NNative::IClientPtr>& queryClients, const TDuration& expirationTimeout)
{
    TString token;
    if (clusters.empty()) {
        return token;
    }

    auto options = NApi::TIssueTemporaryTokenOptions{ .ExpirationTimeout = expirationTimeout };
    auto attributes = CreateEphemeralAttributes();
    attributes->Set("query_id", queryId);
    attributes->Set("responsible", "query_tracker");

    for (auto& cluster : clusters) {
        YT_LOG_DEBUG("Requesting token (User: %v, Cluster: %v)", user, cluster);
        auto rspOrError = token.empty()
            ? WaitFor(queryClients[cluster]->IssueTemporaryToken(user, attributes, options))
            : WaitFor(queryClients[cluster]->IssueSpecificTemporaryToken(user, token, attributes, options));

        if (!rspOrError.IsOK()) {
            YT_LOG_WARNING("Token request failed (User: %v, Cluster: %v)", user, cluster);
            if (rspOrError.FindMatching(NYTree::EErrorCode::AlreadyExists)) {
                YT_LOG_WARNING("Requested token already exists in the cluster (User: %v, Cluster: %v)", user, cluster);
                return std::nullopt;
            }
            rspOrError.ThrowOnError();
        }

        if (token.empty()) {
            token = rspOrError.ValueOrThrow().Token;
        }
        YT_LOG_DEBUG("Token received (User: %v, Cluster: %v)", user, cluster);
    }

    return token;
}

static TString IssueToken(const TQueryId queryId, const TString& user, const std::vector<TString>& clusters, THashMap<TString, NApi::NNative::IClientPtr>& queryClients, const TDuration& expirationTimeout, const int attempts)
{
    for (int attempt = 0; attempt < attempts; attempt++) {
        auto tokenOrErr = TryIssueToken(queryId, user, clusters, queryClients, expirationTimeout);
        if (!tokenOrErr) {
            // The selected token already exists on one of the clusters. We need to try to issue token again.
            continue;
        }

        return *tokenOrErr;
    }

    THROW_ERROR_EXCEPTION("Token cannot be issued, all attempts failed");
}

static void RefreshToken(const TString& user, const TString& token, const THashMap<TString, NApi::NNative::IClientPtr>& queryClients)
{
    for (auto& [cluster, client] : queryClients) {
        YT_LOG_DEBUG("Refreshing token (User: %v, Cluster: %v)", user, cluster);
        auto rspOrError = WaitFor(client->RefreshTemporaryToken(user, token, {}));
        if (!rspOrError.IsOK()) {
            YT_LOG_WARNING("Token refreshing failed (User: %v, Cluster: %v)", user, cluster);
        } else {
            YT_LOG_DEBUG("Token refreshed (User: %v, Cluster: %v)", user, cluster);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

class TYqlAgent
    : public IYqlAgent
{
public:
    TYqlAgent(
        TSingletonsConfigPtr singletonsConfig,
        TYqlAgentConfigPtr yqlAgentConfig,
        TYqlAgentDynamicConfigPtr dynamicConfig,
        TClusterDirectoryPtr clusterDirectory,
        TClientDirectoryPtr clientDirectory,
        IInvokerPtr controlInvoker,
        TString agentId)
        : SingletonsConfig_(std::move(singletonsConfig))
        , Config_(std::move(yqlAgentConfig))
        , ClusterDirectory_(std::move(clusterDirectory))
        , ClientDirectory_(std::move(clientDirectory))
        , ControlInvoker_(std::move(controlInvoker))
        , AgentId_(std::move(agentId))
        , DynamicConfig_(std::move(dynamicConfig))
        , ThreadPool_(CreateThreadPool(Config_->YqlThreadCount, "Yql"))
        , ActiveQueriesGuardFactory_(TActiveQueriesGuardFactory(DynamicConfig_->MaxSimultaneousQueries))
    {
        YqlAgentProfiler().AddFuncGauge("/active_queries", MakeStrong(this), [this] {
            return ActiveQueriesGuardFactory_.GetGuardedValue();
        });

        static const TYsonString EmptyMap = TYsonString(TString("{}"));

        auto clustersConfig = Config_->GatewayConfig->AsMap()->GetChildOrThrow("cluster_mapping")->AsList();

        auto singletonsConfigDefaultLogging = CloneYsonStruct(SingletonsConfig_);
        // Compressed logs are broken if plugin tries to open and write to them.
        singletonsConfigDefaultLogging->SetSingletonConfig(TLogManagerConfig::CreateDefault());

        auto singletonsConfigString = singletonsConfigDefaultLogging
            ? ConvertToYsonString(singletonsConfigDefaultLogging)
            : EmptyMap;

        if (!Config_->DqManagerConfig->AddressResolver) {
            Config_->DqManagerConfig->AddressResolver = SingletonsConfig_->GetSingletonConfig<NNet::TAddressResolverConfig>();
        }

        THashSet<TString> presentClusters;
        for (const auto& cluster : clustersConfig->GetChildren()) {
            presentClusters.insert(cluster->AsMap()->GetChildOrThrow("name")->GetValue<TString>());
        }

        for (const auto& clusterName : ClusterDirectory_->GetClusterNames()) {
            if (presentClusters.contains(clusterName)) {
                continue;
            }

            auto cluster = NYTree::BuildYsonNodeFluently()
                .BeginMap()
                    .Item("name").Value(clusterName)
                    .Item("cluster").Value(clusterName)
                .EndMap();
            auto settings = TYqlPluginConfig::MergeClusterDefaultSettings(GetEphemeralNodeFactory()->CreateList());
            cluster->AsMap()->AddChild("settings", std::move(settings));
            clustersConfig->AddChild(std::move(cluster));
        }

        NYqlPlugin::TYqlPluginOptions options{
            .SingletonsConfig = singletonsConfigString,
            .GatewayConfig = ConvertToYsonString(Config_->GatewayConfig),
            .DqGatewayConfig = Config_->EnableDq ? ConvertToYsonString(Config_->DqGatewayConfig) : TYsonString(),
            .DqManagerConfig = Config_->EnableDq ? ConvertToYsonString(Config_->DqManagerConfig) : TYsonString(),
            .FileStorageConfig = ConvertToYsonString(Config_->FileStorageConfig),
            .OperationAttributes = ConvertToYsonString(Config_->OperationAttributes),
            .Libraries = ConvertToYsonString(Config_->Libraries),
            .YTTokenPath = Config_->YTTokenPath,
            .LogBackend = NYT::NLogging::CreateArcadiaLogBackend(TLogger("YqlPlugin")),
            .YqlPluginSharedLibrary = Config_->YqlPluginSharedLibrary,
        };
        YqlPlugin_ = NYqlPlugin::CreateYqlPlugin(std::move(options));
    }

    void Start() override
    {
        YqlPlugin_->Start();
    }

    void Stop() override
    { }

    NYTree::IMapNodePtr GetOrchidNode() const override
    {
        return GetEphemeralNodeFactory()->CreateMap();
    }

    void OnDynamicConfigChanged(
        const TYqlAgentDynamicConfigPtr& /*oldConfig*/,
        const TYqlAgentDynamicConfigPtr& newConfig) override
    {
        DynamicConfig_ = newConfig;
        if (DynamicConfig_->MaxSimultaneousQueries >= Config_->YqlThreadCount) {
            YT_LOG_ERROR("Decreased \"max_simultaneous_queries\"; it should be less than \"yql_thread_count\" (MaxSimultaneousQueries: %v, YqlThreadCount: %v)",
                DynamicConfig_->MaxSimultaneousQueries,
                Config_->YqlThreadCount);

            DynamicConfig_->MaxSimultaneousQueries = Config_->YqlThreadCount - 1;
        }
        ActiveQueriesGuardFactory_.Update(DynamicConfig_->MaxSimultaneousQueries);
    }

    TFuture<std::pair<TRspStartQuery, std::vector<TSharedRef>>> StartQuery(TQueryId queryId, const TString& user, const TReqStartQuery& request) override
    {
        YT_LOG_INFO("Starting query (QueryId: %v, User: %v)", queryId, user);

        return BIND(&TYqlAgent::DoStartQuery, MakeStrong(this), queryId, user, request)
            .AsyncVia(ThreadPool_->GetInvoker())
            .Run();
    }

    TFuture<void> AbortQuery(TQueryId queryId) override
    {
        return BIND(&TYqlAgent::DoAbortQuery, MakeStrong(this), queryId)
            .AsyncVia(ThreadPool_->GetInvoker())
            .Run();
    }

    TRspGetQueryProgress GetQueryProgress(TQueryId queryId) override
    {
        YT_LOG_DEBUG("Getting query progress (QueryId: %v)", queryId);

        TRspGetQueryProgress response;

        YT_LOG_DEBUG("Getting progress from YQL plugin");

        try {
            auto result = YqlPlugin_->GetProgress(queryId);
            if (result.YsonError) {
                auto error = ConvertTo<TError>(TYsonString(*result.YsonError));
                THROW_ERROR error;
            }
            YT_LOG_DEBUG("YQL plugin progress call completed");

            if (result.Plan || result.Progress) {
                TYqlResponse yqlResponse;
                ValidateAndFillYqlResponseField(yqlResponse, result.Plan, &TYqlResponse::mutable_plan);
                ValidateAndFillYqlResponseField(yqlResponse, result.Progress, &TYqlResponse::mutable_progress);
                response.mutable_yql_response()->Swap(&yqlResponse);
            }
            return response;
        } catch (const std::exception& ex) {
            auto error = TError("YQL plugin call failed") << TError(ex);
            YT_LOG_INFO(error, "YQL plugin call failed");
            THROW_ERROR error;
        }
    }

private:
    const TSingletonsConfigPtr SingletonsConfig_;
    const TYqlAgentConfigPtr Config_;
    const TClusterDirectoryPtr ClusterDirectory_;
    const TClientDirectoryPtr ClientDirectory_;
    const IInvokerPtr ControlInvoker_;
    const TString AgentId_;

    TYqlAgentDynamicConfigPtr DynamicConfig_;

    std::unique_ptr<NYqlPlugin::IYqlPlugin> YqlPlugin_;

    IThreadPoolPtr ThreadPool_;
    TActiveQueriesGuardFactory ActiveQueriesGuardFactory_;

    std::pair<TRspStartQuery, std::vector<TSharedRef>> DoStartQuery(TQueryId queryId, const TString& user, const TReqStartQuery& request)
    {
        auto guard = ActiveQueriesGuardFactory_.CreateGuard();

        if (!guard.IsTaken()) {
            YT_LOG_INFO(
                "Query was throttled (QueryId: %v, User: %v)",
                queryId,
                user);
            THROW_ERROR_EXCEPTION(NYqlClient::EErrorCode::RequestThrottled, "Query was throttled");
        }

        static const auto EmptyMap = TYsonString(TString("{}"));

        const auto& Logger = YqlAgentLogger().WithTag("QueryId: %v", queryId);

        const auto& yqlRequest = request.yql_request();

        TRspStartQuery response;

        YT_LOG_INFO("Invoking YQL embedded");

        std::vector<TSharedRef> wireRowsets;
        try {
            auto query = Format("pragma yt.UseNativeYtTypes; pragma ResultRowsLimit=\"%v\";\n%v", request.row_count_limit(), yqlRequest.query());
            auto settings = yqlRequest.has_settings() ? TYsonString(yqlRequest.settings()) : EmptyMap;

            std::vector<NYqlPlugin::TQueryFile> files;
            files.reserve(yqlRequest.files_size());
            for (const auto& file : yqlRequest.files()) {
                files.push_back(NYqlPlugin::TQueryFile{
                    .Name = file.name(),
                    .Content = file.content(),
                    .Type = static_cast<EQueryFileContentType>(file.type()),
                });
            }

            auto clustersResult = YqlPlugin_->GetUsedClusters(query, settings, files);
            if (clustersResult.YsonError) {
                auto error = ConvertTo<TError>(TYsonString(*clustersResult.YsonError));
                THROW_ERROR error;
            }

            THashMap<TString, NApi::NNative::IClientPtr> queryClients;
            for (const auto& clusterName : clustersResult.Clusters) {
                queryClients[clusterName] = ClusterDirectory_->GetConnectionOrThrow(clusterName)->CreateNativeClient(NApi::TClientOptions{ .User = user });
            }

            auto token = IssueToken(queryId, user, clustersResult.Clusters, queryClients, Config_->TokenExpirationTimeout, Config_->IssueTokenAttempts);

            auto refreshTokenExecutor = New<TPeriodicExecutor>(ControlInvoker_, BIND(&RefreshToken, user, token, queryClients), Config_->RefreshTokenPeriod);
            refreshTokenExecutor->Start();

            // This is a long blocking call.
            auto result = YqlPlugin_->Run(queryId, user, token, query, settings, files, yqlRequest.mode());
            WaitFor(refreshTokenExecutor->Stop()).ThrowOnError();

            if (result.YsonError) {
                auto error = ConvertTo<TError>(TYsonString(*result.YsonError));
                THROW_ERROR error;
            }

            YT_LOG_INFO("YQL plugin call completed");

            TYqlResponse yqlResponse;
            ValidateAndFillYqlResponseField(yqlResponse, result.YsonResult, &TYqlResponse::mutable_result);
            ValidateAndFillYqlResponseField(yqlResponse, result.Plan, &TYqlResponse::mutable_plan);
            ValidateAndFillYqlResponseField(yqlResponse, result.Statistics, &TYqlResponse::mutable_statistics);
            ValidateAndFillYqlResponseField(yqlResponse, result.Progress, &TYqlResponse::mutable_progress);
            ValidateAndFillYqlResponseField(yqlResponse, result.TaskInfo, &TYqlResponse::mutable_task_info);
            if (request.build_rowsets() && result.YsonResult) {
                auto rowsets = BuildRowsets(ClientDirectory_, *result.YsonResult, request.row_count_limit());

                for (const auto& rowset : rowsets) {
                    if (rowset.Error.IsOK()) {
                        wireRowsets.push_back(rowset.WireRowset);
                        response.add_rowset_errors();
                        response.add_incomplete(rowset.Incomplete);
                    } else {
                        wireRowsets.push_back(TSharedRef());
                        ToProto(response.add_rowset_errors(), rowset.Error);
                        response.add_incomplete(false);
                    }
                }
            }
            response.mutable_yql_response()->Swap(&yqlResponse);
            return {response, wireRowsets};
        } catch (const std::exception& ex) {
            auto error = TError("YQL plugin call failed") << TError(ex);
            YT_LOG_INFO(error, "YQL plugin call failed");
            THROW_ERROR error;
        }
    }

    void DoAbortQuery(TQueryId queryId)
    {
        YT_LOG_INFO("Aborting query (QueryId: %v)", queryId);

        TError error;

        try {
            auto abortResult = YqlPlugin_->Abort(queryId);
            if (auto ysonError = abortResult.YsonError) {
                error = ConvertTo<TError>(TYsonString(*ysonError));
            }
        } catch (const std::exception& ex) {
            auto error = TError("YQL plugin call failed") << TError(ex);
            YT_LOG_INFO(error, "YQL plugin call failed");
            THROW_ERROR error;
        }

        error.ThrowOnError();
    }

    void ValidateAndFillYqlResponseField(TYqlResponse& yqlResponse, const std::optional<TString>& rawField, TString* (TYqlResponse::*mutableProtoFieldAccessor)())
    {
        if (!rawField) {
            return;
        }
        // TODO(max42): original YSON tends to unnecessary pretty.
        *((&yqlResponse)->*mutableProtoFieldAccessor)() = *rawField;
    };
};

////////////////////////////////////////////////////////////////////////////////

IYqlAgentPtr CreateYqlAgent(
    TSingletonsConfigPtr singletonsConfig,
    TYqlAgentConfigPtr config,
    TYqlAgentDynamicConfigPtr dynamicConfig,
    TClusterDirectoryPtr clusterDirectory,
    TClientDirectoryPtr clientDirectory,
    IInvokerPtr controlInvoker,
    TString agentId)
{
    return New<TYqlAgent>(
        std::move(singletonsConfig),
        std::move(config),
        std::move(dynamicConfig),
        std::move(clusterDirectory),
        std::move(clientDirectory),
        std::move(controlInvoker),
        std::move(agentId));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
