#include "spyt_engine.h"

#include "config.h"
#include "handler_base.h"
#include "spyt_discovery.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/discovery_client/config.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/core/http/client.h>
#include <yt/yt/core/http/helpers.h>
#include <yt/yt/core/http/http.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/dispatcher.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/json/json_writer.h>
#include <yt/yt/core/json/json_parser.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/caching_channel_factory.h>

#include <library/cpp/string_utils/base64/base64.h>

#include <util/string/split.h>

namespace NYT::NQueryTracker {

using namespace NQueryTrackerClient;
using namespace NApi;
using namespace NYPath;
using namespace NRpc;
using namespace NRpc::NBus;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TSpytSettings
    : public TYsonStruct
{
public:
    std::optional<TString> Cluster;

    std::optional<TYPath> DiscoveryPath;

    std::optional<TString> DiscoveryGroup;

    std::optional<TString> SparkConf;

    THashMap<TString, TString> Params;

    bool SessionReuse;

    REGISTER_YSON_STRUCT(TSpytSettings);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("cluster", &TThis::Cluster)
            .Default();
        registrar.Parameter("discovery_path", &TThis::DiscoveryPath)
            .Default();
        registrar.Parameter("discovery_group", &TThis::DiscoveryGroup)
            .Default();
        registrar.Parameter("spark_conf", &TThis::SparkConf)
            .Default();
        registrar.Parameter("params", &TThis::Params)
            .Default();
        registrar.Parameter("session_reuse", &TThis::SessionReuse)
            .Default(true);
    }
};

DEFINE_REFCOUNTED_TYPE(TSpytSettings)
DECLARE_REFCOUNTED_CLASS(TSpytSettings)

////////////////////////////////////////////////////////////////////////////////

class TSpytVersion
{
public:
    TSpytVersion(TString rawVersion)
    {
        auto dashPos = rawVersion.find_first_of('-');
        // Strip SNAPSHOT version suffix.
        if (dashPos != TString::npos) {
            rawVersion = rawVersion.substr(0, dashPos);
        }
        if (!StringSplitter(rawVersion).Split('.').TryCollectInto(&Major_, &Minor_, &Patch_)) {
            THROW_ERROR_EXCEPTION(
                "Version %Qv cannot be parsed",
                rawVersion);
        }
    }

    bool operator<(const TSpytVersion& rhs) const
    {
        return Major_ < rhs.Major_ || Major_ == rhs.Major_ && (Minor_ < rhs.Minor_ || Minor_ == rhs.Minor_ && Patch_ < rhs.Patch_);
    }

private:
    int Major_;
    int Minor_;
    int Patch_;
};

////////////////////////////////////////////////////////////////////////////////

struct SpytQueryResult {
    const bool IsTruncated;
    const TSharedRef WireData;
};

////////////////////////////////////////////////////////////////////////////////

class TSpytQueryHandler
    : public TQueryHandlerBase
{
public:
    TSpytQueryHandler(
        const NApi::IClientPtr& stateClient,
        const NYPath::TYPath& stateRoot,
        const TSpytEngineConfigPtr& config,
        const IChannelFactoryPtr& channelFactory,
        const NQueryTrackerClient::NRecords::TActiveQuery& activeQuery,
        const NHiveClient::TClusterDirectoryPtr& clusterDirectory,
        const IInvokerPtr& controlInvoker)
        : TQueryHandlerBase(stateClient, stateRoot, controlInvoker, config, activeQuery)
        , Settings_(ConvertTo<TSpytSettingsPtr>(SettingsNode_))
        , Config_(config)
        , Cluster_(Settings_->Cluster.value_or(Config_->DefaultCluster))
        , NativeConnection_(clusterDirectory->GetConnectionOrThrow(Cluster_))
        , QueryClient_(NativeConnection_->CreateNativeClient(TClientOptions{.User = activeQuery.User}))
        , HttpClient_(CreateClient(Config_->HttpClient, NYT::NBus::TTcpDispatcher::Get()->GetXferPoller()))
        , Headers_(New<NHttp::THeaders>())
        , RefreshTokenExecutor_(New<TPeriodicExecutor>(GetCurrentInvoker(), BIND(&TSpytQueryHandler::RefreshToken, MakeWeak(this)), Config_->RefreshTokenPeriod))
        , SessionReuse_(Settings_->SessionReuse)
    {
        if (Cluster_.empty()) {
            THROW_ERROR_EXCEPTION("'cluster' setting is not specified");
        }
        auto discoveryPath = Settings_->DiscoveryPath.value_or(Config_->DefaultDiscoveryPath);
        auto discoveryGroup = Settings_->DiscoveryGroup.value_or(Config_->DefaultDiscoveryGroup);
        if (!discoveryPath.empty()) {
            YT_LOG_DEBUG("Discovery path will be used (Path: %v)", discoveryPath);
            Discovery_ = CreateDiscoveryV1(QueryClient_, discoveryPath);
        } else if (!discoveryGroup.empty()) {
            YT_LOG_DEBUG("Discovery group will be used (Group: %v)", discoveryGroup);
            Discovery_ = CreateDiscoveryV2(NativeConnection_, discoveryGroup, channelFactory, Logger);
        } else {
            THROW_ERROR_EXCEPTION("Either 'discovery_path' or 'discovery_group' setting must be specified");
        }
        Headers_->Add("Content-Type", "application/json");
    }

    void Start() override
    {
        YT_LOG_DEBUG("Starting SPYT query");
        OnQueryStarted();
        StartProgressWriter();
        AsyncQueryResult_ = BIND(&TSpytQueryHandler::Execute, MakeStrong(this))
            .AsyncVia(GetCurrentInvoker())
            .Run();
        AsyncQueryResult_.Subscribe(BIND(&TSpytQueryHandler::OnSpytResponse, MakeWeak(this)).Via(GetCurrentInvoker()));
    }

    void Abort() override
    {
        YT_LOG_DEBUG("Aborting SPYT query (SessionUrl: %v)", SessionUrl_);
        AsyncQueryResult_.Cancel(TError("Query aborted"));
        // After Abort() call there is Detach() call always. But double closing request is not the error.
        FreeResources();
        StopBackgroundExecutors();
    }

    void Detach() override
    {
        YT_LOG_DEBUG("Detaching SPYT query (SessionUrl: %v)", SessionUrl_);
        AsyncQueryResult_.Cancel(TError("Query detached"));
        FreeResources();
        StopBackgroundExecutors();
    }

private:
    const TSpytSettingsPtr Settings_;
    const TSpytEngineConfigPtr Config_;
    const TString Cluster_;
    const NApi::NNative::IConnectionPtr NativeConnection_;
    const NApi::NNative::IClientPtr QueryClient_;
    const NHttp::IClientPtr HttpClient_;
    const NHttp::THeadersPtr Headers_;
    const TPeriodicExecutorPtr RefreshTokenExecutor_;
    const bool SessionReuse_;
    ISpytDiscoveryPtr Discovery_;
    TFuture<SpytQueryResult> AsyncQueryResult_;
    TString SessionUrl_;
    TString StatementUrl_;
    std::optional<TString> Token_;
    // Not exists for YT scheduled jobs
    std::optional<TString> MasterWebUI_;

    void SetProgress(double progressValue, const std::optional<INodePtr>& plan)
    {
        YT_LOG_DEBUG("Reporting progress (Progress: %v)", progressValue);
        auto progress = BuildYsonStringFluently()
            .BeginMap()
                .OptionalItem("webui", MasterWebUI_)
                .Item("spyt_progress").Value(progressValue)
                .OptionalItem("spyt_plan", plan)
            .EndMap();
        OnProgress(std::move(progress));
    }

    TString GetLocation(const NHttp::IResponsePtr& response) const
    {
        return response->GetHeaders()->GetOrThrow("Location");
    }

    void ValidateStatusCode(const NHttp::IResponsePtr& response, const NHttp::EStatusCode& expected) const
    {
        if (response->GetStatusCode() != expected) {
            THROW_ERROR_EXCEPTION(
                "Unexpected Livy status code: expected %Qv, actual %Qv",
                expected,
                response->GetStatusCode())
                    << TErrorAttribute("response_body", response->ReadAll().ToStringBuf());
        }
    }

    INodePtr ParseJson(const TSharedRef& data) const
    {
        TMemoryInput stream(data.Begin(), data.Size());
        auto factory = NYTree::CreateEphemeralNodeFactory();
        auto builder = NYTree::CreateBuilderFromFactory(factory.get());
        auto config = New<NJson::TJsonFormatConfig>();
        NJson::ParseJson(&stream, builder.get(), config);
        return builder->EndTree();
    }

    INodePtr ExecuteGetQuery(const TString& url) const
    {
        YT_LOG_DEBUG("Executing HTTP GET request (Url: %v)", url);
        auto rsp = WaitFor(HttpClient_->Get(url))
            .ValueOrThrow();
        YT_LOG_DEBUG("HTTP GET request executed (StatusCode: %v)", rsp->GetStatusCode());
        ValidateStatusCode(rsp, NHttp::EStatusCode::OK);
        auto jsonRoot = ParseJson(rsp->ReadAll());
        return jsonRoot;
    }

    TString WaitSessionStatusChange(const TString& url, const TString& defaultState)
    {
        auto state = defaultState;
        while (state == defaultState) {
            TDelayedExecutor::WaitForDuration(Config_->StatusPollPeriod);
            state = ExecuteGetQuery(url)->AsMap()->GetChildValueOrThrow<TString>("state");
        }
        return state;
    }

    TString WaitStatementStatusChange(const TString& url, const TString& defaultState)
    {
        auto state = defaultState;
        while (state == defaultState) {
            TDelayedExecutor::WaitForDuration(Config_->StatusPollPeriod);
            auto jsonRoot = ExecuteGetQuery(url)->AsMap();
            state = jsonRoot->GetChildValueOrThrow<TString>("state");
            auto progressValue = jsonRoot->GetChildValueOrThrow<double>("progress");
            auto rawPlan = jsonRoot->FindChild("plan");
            auto plan = rawPlan ? std::make_optional(rawPlan) : std::nullopt;
            SetProgress(progressValue, plan);
        }
        return state;
    }

    TString GetReleaseMode(const TString& version) const
    {
        if (version.Contains("SNAPSHOT")) {
            return "snapshots";
        } else {
            return "releases";
        }
    }

    TString GetSpytFile(const TString& version, const TString& filename) const
    {
        return Format("yt:/%v/spyt/%v/%v/%v", Config_->SpytHome, GetReleaseMode(version), version, filename);
    }

    TString SerializeYsonToJson(const INodePtr& ysonNode) const
    {
        TString result;
        TStringOutput resultOutput(result);
        auto jsonWriter = NJson::CreateJsonConsumer(&resultOutput);
        Serialize(ysonNode, jsonWriter.get());
        jsonWriter->Flush();
        return result;
    }

    THashMap<TString, TString> GetSparkConf() const
    {
        THashMap<TString, TString> sparkConf;
        if (Settings_->SparkConf) {
            sparkConf = ConvertTo<THashMap<TString, TString>>(TYsonString(Settings_->SparkConf.value()));
            if (sparkConf.contains("spark.hadoop.yt.user") || sparkConf.contains("spark.hadoop.yt.token")) {
                THROW_ERROR_EXCEPTION("Providing credentials is forbidden");
            }
        }
        // COMPAT(alex-shishkin): Cluster >= 1.78.0 doesn't require client files.
        auto clusterVersion = Discovery_->GetVersion();
        if (clusterVersion && TSpytVersion(*clusterVersion) < TSpytVersion("1.78.0")) {
            sparkConf["spark.yt.version"] = *clusterVersion;
            auto extraJar = GetSpytFile(*clusterVersion, "spark-yt-data-source.jar");
            auto jarsInsert = sparkConf.emplace("spark.yt.jars", extraJar).second;
            if (!jarsInsert) {
                THROW_ERROR_EXCEPTION("Configuration of 'spark.yt.jars' is forbidden");
            }
            auto pyFilesInsert = sparkConf.emplace("spark.yt.pyFiles", GetSpytFile(*clusterVersion, "spyt.zip")).second;
            if (!pyFilesInsert) {
                THROW_ERROR_EXCEPTION("Configuration of 'spark.yt.pyFiles' is forbidden");
            }
        }
        // Dirty hack for absent scheme.
        sparkConf.emplace("spark.hadoop.fs.null.impl", "tech.ytsaurus.spyt.fs.YtTableFileSystem");
        sparkConf.emplace("spark.yt.log.enabled", "false");
        YT_LOG_DEBUG("Session spark conf prepared (Data: %v)", sparkConf);
        // Token insertion after data logging.
        if (Token_) {
            sparkConf.emplace("spark.hadoop.yt.user", User_);
            sparkConf.emplace("spark.hadoop.yt.token", *Token_);
            YT_LOG_DEBUG("Authetication information for user was inserted (User: %v)", User_);
        }
        return sparkConf;
    }

    TString MakeSessionStartQueryData() const
    {
        auto dataNode = BuildYsonNodeFluently()
            .BeginMap()
                .Item("kind").Value("spark")
                .Item("proxyUser").Value(User_)
                .Item("conf").Value(GetSparkConf())
            .EndMap();
        return SerializeYsonToJson(dataNode);
    }

    void StartSession(const TString& rootUrl)
    {
        auto data = MakeSessionStartQueryData();
        auto body = TSharedRef::FromString(data);
        auto rsp = WaitFor(HttpClient_->Post(rootUrl + "/sessions", body, Headers_))
            .ValueOrThrow();
        ValidateStatusCode(rsp, NHttp::EStatusCode::Created);
        YT_LOG_DEBUG("Session creation response received (Headers: %v)", rsp->GetHeaders()->Dump());
        SessionUrl_ = rootUrl + GetLocation(rsp);
        YT_LOG_DEBUG("Session creation response parsed (Url: %v)", SessionUrl_);
    }

    void WaitSessionReady()
    {
        auto state = WaitSessionStatusChange(SessionUrl_, "starting");
        if (!(state == "idle" || (state == "busy" && SessionReuse_))) {
            THROW_ERROR_EXCEPTION(
                "Unexpected Livy session state: expected \"idle\" or \"busy\", found %Qv",
                state);
        }
    }

    void CancelStatement() const
    {
        if (!StatementUrl_.empty()) {
            YT_LOG_DEBUG("Canceling statement");
            auto data = TSharedRef::FromString("{}");
            auto rsp = WaitFor(HttpClient_->Post(StatementUrl_ + "/cancel", data, Headers_))
                .ValueOrThrow();
            YT_LOG_DEBUG("Statement cancelation response received (Code: %v)", rsp->GetStatusCode());
        }
    }

    void CloseSession() const
    {
        if (!SessionUrl_.empty()) {
            YT_LOG_DEBUG("Closing session");
            auto rsp = WaitFor(HttpClient_->Delete(SessionUrl_, Headers_))
                .ValueOrThrow();
            YT_LOG_DEBUG("Session closing response received (Code: %v)", rsp->GetStatusCode());
        }
    }

    TString ConcatChunks(const std::vector<TString>& chunks) const
    {
        size_t summarySize = 0;
        for (const auto& chunk : chunks) {
            summarySize += chunk.size();
        }
        TString result;
        result.reserve(summarySize);
        for (const auto& chunk : chunks) {
            result.append(chunk);
        }
        return result;
    }

    SpytQueryResult ExtractTableBytes(const TString& queryResult) const
    {
        auto encodedChunks = StringSplitter(queryResult).Split('\n').ToList<TString>();
        YT_LOG_DEBUG("Raw result received (LineCount: %v)", encodedChunks.size());

        bool IsTruncated = encodedChunks[0] == "T";
        std::vector<TString> tableChunks;
        for (size_t i = 1; i < encodedChunks.size(); i++) {
            tableChunks.push_back(Base64StrictDecode(encodedChunks[i]));
        }

        return {IsTruncated, TSharedRef::FromString(ConcatChunks(tableChunks))};
    }

    TString ParseQueryOutput(const IMapNodePtr& outputNode) const
    {
        auto status = outputNode->GetChildOrThrow("status")->AsString()->GetValue();
        if (status == "ok") {
            auto dataNode = outputNode->GetChildOrThrow("data")->AsMap();
            return dataNode->GetChildValueOrThrow<TString>("text/plain");
        } else if (status == "error") {
            auto error = outputNode->GetChildValueOrThrow<TString>("evalue");
            auto traceback = outputNode->GetChildValueOrThrow<std::vector<TString>>("traceback");
            THROW_ERROR_EXCEPTION("Livy error: %v", error)
                << TErrorAttribute("traceback", traceback);
        } else {
            THROW_ERROR_EXCEPTION("Unknown Livy query status: %v", status);
        }
    }

    TString MakeStatementSubmitQueryData(const TString& sqlQuery) const
    {
        TString paramsSetting;
        for (const auto &[name, value] : Settings_->Params) {
            paramsSetting.append(Format("spark.sql(\"set %v=%v\");", name, value));
        }
        auto code = Format(
            "{"
            " import tech.ytsaurus.spyt.serializers.GenericRowSerializer;"
            " import java.util.Base64; import java.nio.charset.StandardCharsets.UTF_8;"
            " %v"
            " val query = new String(Base64.getDecoder().decode(\"%v\"), UTF_8);"
            " val rowCountLimit = %v;"
            " val df = spark.sql(query).limit(rowCountLimit + 1);"
            " println(GenericRowSerializer.dfToYTFormatWithBase64(df, rowCountLimit).mkString(\"\\n\"))"
            "}",
            paramsSetting,
            Base64Encode(sqlQuery),
            Config_->RowCountLimit);
        auto dataNode = BuildYsonNodeFluently()
            .BeginMap()
                .Item("code").Value(code)
            .EndMap();
        return SerializeYsonToJson(dataNode);
    }

    void SubmitStatement(const TString& rootUrl, const TString& sqlQuery)
    {
        auto data = MakeStatementSubmitQueryData(sqlQuery);
        YT_LOG_DEBUG("Statement data prepared (Data: %v)", data);
        auto body = TSharedRef::FromString(data);
        auto rsp = WaitFor(HttpClient_->Post(SessionUrl_ + "/statements", body, Headers_))
            .ValueOrThrow();
        ValidateStatusCode(rsp, NHttp::EStatusCode::Created);
        YT_LOG_DEBUG("Statement submission response received (Headers: %v)", rsp->GetHeaders()->Dump());
        StatementUrl_ = rootUrl + GetLocation(rsp);
        YT_LOG_DEBUG("Statement submission response parsed (Url: %v)", StatementUrl_);
    }

    TString WaitStatementFinished()
    {
        // Query may be in queue.
        auto state = WaitStatementStatusChange(StatementUrl_, "waiting");
        state = WaitStatementStatusChange(StatementUrl_, "running");
        if (state != "available") {
            THROW_ERROR_EXCEPTION(
                "Unexpected Livy result state: expected \"available\", found %Qv",
                state);
        }
        auto queryResult = ExecuteGetQuery(StatementUrl_);
        auto outputNode = queryResult->AsMap()->GetChildOrThrow("output")->AsMap();
        return ParseQueryOutput(outputNode);
    }

    TString GetLivyServerUrl() const
    {
        YT_LOG_DEBUG("Listing livy discovery");
        auto livyUrlOptional = Discovery_->GetLivyUrl();
        if (!livyUrlOptional) {
            THROW_ERROR_EXCEPTION(
                "Livy address was not found in discovery. SPYT cluster must be started with '--enable-livy' option");
        }
        auto livyUrl = "http://" + *livyUrlOptional;
        YT_LOG_DEBUG("Livy server url received (Url: %v)", livyUrl);
        return livyUrl;
    }

    std::optional<TString> IssueToken() const
    {
        auto options = TIssueTemporaryTokenOptions{ .ExpirationTimeout = Config_->TokenExpirationTimeout };
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("query_id", QueryId_);
        attributes->Set("responsible", "query_tracker");
        YT_LOG_DEBUG("Requesting token (User: %v)", User_);
        auto rspOrError = WaitFor(QueryClient_->IssueTemporaryToken(User_, attributes, options));
        if (!rspOrError.IsOK()) {
            YT_LOG_DEBUG("Token request failed (User: %v)", User_);
            return std::nullopt;
        }
        auto token = rspOrError.Value().Token;
        YT_LOG_DEBUG("Token received (User: %v)", User_);
        return token;
    }

    void RefreshToken() const
    {
        YT_VERIFY(Token_);
        YT_LOG_DEBUG("Refreshing token (User: %v)", User_);
        auto rspOrError = WaitFor(QueryClient_->RefreshTemporaryToken(User_, *Token_, {}));
        if (!rspOrError.IsOK()) {
            YT_LOG_WARNING("Token refreshing failed (User: %v)", User_);
        } else {
            YT_LOG_DEBUG("Token refreshed (User: %v)", User_);
        }
    }

    void UpdateMasterWebUIUrl()
    {
        YT_LOG_DEBUG("Listing webui discovery");
        auto url = Discovery_->GetMasterWebUIUrl();
        YT_LOG_DEBUG("Master webui url received (Url: %v)", url);
        if (url) {
            MasterWebUI_ = "http://" + *url;
        }
    }

    // Try to initialize SessionUrl_ and Token_ by existing session.
    bool TryConnectToExistingSession(const TString& rootUrl)
    {
        auto dataNode = BuildYsonNodeFluently().BeginMap().Item("proxyUser").Value(User_).EndMap();
        auto body = TSharedRef::FromString(SerializeYsonToJson(dataNode));
        auto rsp = WaitFor(HttpClient_->Post(rootUrl + "/sessions/find-by-user", body, Headers_))
            .ValueOrThrow();
        YT_LOG_DEBUG("Session search response received (Headers: %v)", rsp->GetHeaders()->Dump());
        if (rsp->GetStatusCode() == NHttp::EStatusCode::OK) {
            auto sessionUrl = rootUrl + GetLocation(rsp);
            // NB(alex-shishkin): Session data contains sensetive data.
            auto jsonRoot = ParseJson(rsp->ReadAll())->AsMap();
            auto token = jsonRoot->GetChildOrThrow("conf")->AsMap()->FindChildValue<TString>("spark.hadoop.yt.token");
            SessionUrl_ = sessionUrl;
            Token_ = token;
            YT_LOG_DEBUG("Existing session parsed (Url: %v)", SessionUrl_);
            return true;
        } else if (rsp->GetStatusCode() == NHttp::EStatusCode::NotFound) {
            YT_LOG_DEBUG("Existing session was not found");
            return false;
        } else if (rsp->GetStatusCode() == NHttp::EStatusCode::MethodNotAllowed) {
            YT_LOG_DEBUG("Livy server doesn't support 'find-by-user' endpoint");
            return false;
        } else {
            THROW_ERROR_EXCEPTION(
                "Unexpected session search status code: %Qv",
                rsp->GetStatusCode())
                    << TErrorAttribute("response_body", rsp->ReadAll().ToStringBuf());
        }
    }

    void PrepareSession(const TString& rootUrl)
    {
        if (SessionReuse_ && TryConnectToExistingSession(rootUrl)) {
            YT_LOG_DEBUG("Reusing old session");
        } else {
            try {
                YT_LOG_DEBUG("Starting session");
                Token_ = IssueToken();
                StartSession(rootUrl);  // Session URL stores to the class field
                WaitSessionReady();
            } catch (const std::exception& ex) {
                YT_LOG_DEBUG(ex, "Caught error while preparing session");
                CloseSession();
                throw;
            }
        }
        if (Token_) {
            RefreshTokenExecutor_->Start();
        }
    }

    void FreeResources()
    {
        if (SessionReuse_) {
            CancelStatement();
        } else {
            CloseSession();
        }
    }

    void StopBackgroundExecutors()
    {
        StopProgressWriter();
        YT_UNUSED_FUTURE(RefreshTokenExecutor_->Stop());
    }

    SpytQueryResult Execute()
    {
        UpdateMasterWebUIUrl();
        SetProgress(0.0, std::nullopt);
        auto rootUrl = GetLivyServerUrl();
        try {
            PrepareSession(rootUrl);
            YT_LOG_DEBUG("Session is ready, submitting statement (SessionUrl: %v)", SessionUrl_);
            SubmitStatement(rootUrl, Query_);
            auto queryResult = WaitStatementFinished();
            YT_LOG_DEBUG("Query finished");
            FreeResources();
            return ExtractTableBytes(queryResult);
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Caught error while executing query");
            FreeResources();
            throw;
        }
    }

    void OnSpytResponse(const TErrorOr<SpytQueryResult>& queryResultOrError)
    {
        StopBackgroundExecutors();
        if (queryResultOrError.FindMatching(NYT::EErrorCode::Canceled)) {
            return;
        }
        if (!queryResultOrError.IsOK()) {
            OnQueryFailed(queryResultOrError);
            return;
        }
        auto result = queryResultOrError.Value();
        OnQueryCompletedWire({TWireRowset{
            .Rowset = result.WireData,
            .IsTruncated = result.IsTruncated
        }});
    }
};

class TSpytEngine
    : public IQueryEngine
{
public:
    TSpytEngine(IClientPtr stateClient, TYPath stateRoot)
        : StateClient_(std::move(stateClient))
        , StateRoot_(std::move(stateRoot))
        , ControlQueue_(New<TActionQueue>("SpytEngineControl"))
        , ClusterDirectory_(DynamicPointerCast<NNative::IConnection>(StateClient_->GetConnection())->GetClusterDirectory())
        , ChannelFactory_(CreateCachingChannelFactory(CreateTcpBusChannelFactory(New<NYT::NBus::TBusConfig>())))
    { }

    IQueryHandlerPtr StartOrAttachQuery(NRecords::TActiveQuery activeQuery) override
    {
        return New<TSpytQueryHandler>(
            StateClient_,
            StateRoot_,
            Config_,
            ChannelFactory_,
            activeQuery,
            ClusterDirectory_,
            ControlQueue_->GetInvoker());
    }

    void Reconfigure(const TEngineConfigBasePtr& config) override
    {
        Config_ = DynamicPointerCast<TSpytEngineConfig>(config);
    }

private:
    const IClientPtr StateClient_;
    const TYPath StateRoot_;
    const TActionQueuePtr ControlQueue_;
    TSpytEngineConfigPtr Config_;
    const NHiveClient::TClusterDirectoryPtr ClusterDirectory_;
    const IChannelFactoryPtr ChannelFactory_;
};

IQueryEnginePtr CreateSpytEngine(IClientPtr stateClient, TYPath stateRoot)
{
    return New<TSpytEngine>(
        std::move(stateClient),
        std::move(stateRoot));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
