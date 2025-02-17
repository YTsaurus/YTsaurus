#include "api_service.h"

#include "access_checker.h"
#include "config.h"
#include "format_row_stream.h"
#include "helpers.h"
#include "private.h"
#include "proxy_coordinator.h"
#include "query_corpus_reporter.h"
#include "security_manager.h"

#include <yt/yt/server/lib/misc/format_manager.h>
#include <yt/yt/server/lib/misc/profiling_helpers.h>

#include <yt/yt/server/lib/transaction_server/helpers.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/client_cache.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/transaction_client/clock_manager.h>

#include <yt/yt/ytlib/query_tracker_client/query_tracker_service_proxy.h>

#include <yt/yt/library/tracing/jaeger/sampler.h>

#include <yt/yt/library/auth_server/helpers.h>

#include <yt/yt/client/arrow/arrow_row_stream_decoder.h>
#include <yt/yt/client/arrow/arrow_row_stream_encoder.h>

#include <yt/yt/client/formats/config.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/config.h>
#include <yt/yt/client/api/distributed_table_session.h>
#include <yt/yt/client/api/file_reader.h>
#include <yt/yt/client/api/file_writer.h>
#include <yt/yt/client/api/helpers.h>
#include <yt/yt/client/api/journal_reader.h>
#include <yt/yt/client/api/journal_writer.h>
#include <yt/yt/client/api/query_tracker_client.h>
#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/api/sticky_transaction_pool.h>
#include <yt/yt/client/api/table_reader.h>
#include <yt/yt/client/api/table_writer.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/api/rpc_proxy/helpers.h>
#include <yt/yt/client/api/rpc_proxy/protocol_version.h>
#include <yt/yt/client/api/rpc_proxy/row_stream.h>
#include <yt/yt/client/api/rpc_proxy/wire_row_stream.h>

#include <yt/yt/client/security_client/helpers.h>

#include <yt/yt/client/chunk_client/config.h>

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/client/signature/generator.h>
#include <yt/yt/client/signature/signature.h>
#include <yt/yt/client/signature/validator.h>

#include <yt/yt/client/table_client/config.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/transaction_client/helpers.h>
#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/client/scheduler/operation_id_or_alias.h>
#include <yt/yt/client/scheduler/spec_patch.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/logging/fluent_log.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/rpc/service_detail.h>
#include <yt/yt/core/rpc/stream.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>
#include <library/cpp/yt/memory/non_null_ptr.h>

#include <library/cpp/yt/misc/cast.h>

#include <algorithm>
#include <ranges>

namespace NYT::NRpcProxy {

using namespace NApi::NRpcProxy;
using namespace NApi;
using namespace NArrow;
using namespace NAuth;
using namespace NChaosClient;
using namespace NChunkClient;
using namespace NCodegen;
using namespace NCompression;
using namespace NConcurrency;
using namespace NLogging;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NQueryTrackerClient;
using namespace NQueueClient;
using namespace NRpc;
using namespace NScheduler;
using namespace NSecurityClient;
using namespace NSignature;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTracing;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NServer;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

struct TApiServiceBufferTag
{ };

namespace {

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

TError MakeCanceledError()
{
    return TError("RPC request canceled");
}

void SetTimeoutOptions(
    TTimeoutOptions* options,
    const IServiceContext* context)
{
    options->Timeout = context->GetTimeout();
}

void FromProto(
    TPrerequisiteOptions* options,
    const NApi::NRpcProxy::NProto::TPrerequisiteOptions& proto)
{
    options->PrerequisiteTransactionIds.resize(proto.transactions_size());
    for (int i = 0; i < proto.transactions_size(); ++i) {
        const auto& protoItem = proto.transactions(i);
        auto& item = options->PrerequisiteTransactionIds[i];
        FromProto(&item, protoItem.transaction_id());
    }
    options->PrerequisiteRevisions.resize(proto.revisions_size());
    for (int i = 0; i < proto.revisions_size(); ++i) {
        const auto& protoItem = proto.revisions(i);
        options->PrerequisiteRevisions[i] = New<TPrerequisiteRevisionConfig>();
        auto& item = *options->PrerequisiteRevisions[i];
        item.Revision = FromProto<NHydra::TRevision>(protoItem.revision());
        item.Path = protoItem.path();
    }
}

void FromProto(
    TMasterReadOptions* options,
    const NApi::NRpcProxy::NProto::TMasterReadOptions& proto)
{
    using NYT::FromProto;
    if (proto.has_read_from()) {
        FromProto(&options->ReadFrom, proto.read_from());
    }
    if (proto.has_expire_after_successful_update_time()) {
        FromProto(&options->ExpireAfterSuccessfulUpdateTime, proto.expire_after_successful_update_time());
    }
    if (proto.has_expire_after_failed_update_time()) {
        FromProto(&options->ExpireAfterFailedUpdateTime, proto.expire_after_failed_update_time());
    }
    if (proto.has_success_staleness_bound()) {
        FromProto(&options->SuccessStalenessBound, proto.success_staleness_bound());
    }
    if (proto.has_cache_sticky_group_size()) {
        options->CacheStickyGroupSize = proto.cache_sticky_group_size();
    }
}

void FromProto(
    TMutatingOptions* options,
    const NApi::NRpcProxy::NProto::TMutatingOptions& proto)
{
    if (proto.has_mutation_id()) {
        FromProto(&options->MutationId, proto.mutation_id());
    }
    if (proto.has_retry()) {
        options->Retry = proto.retry();
    }
}

void FromProto(
    TSuppressableAccessTrackingOptions* options,
    const NApi::NRpcProxy::NProto::TSuppressableAccessTrackingOptions& proto)
{
    if (proto.has_suppress_access_tracking()) {
        options->SuppressAccessTracking = proto.suppress_access_tracking();
    }
    if (proto.has_suppress_modification_tracking()) {
        options->SuppressModificationTracking = proto.suppress_modification_tracking();
    }
    if (proto.has_suppress_expiration_timeout_renewal()) {
        options->SuppressExpirationTimeoutRenewal = proto.suppress_expiration_timeout_renewal();
    }
}

void FromProto(
    TTabletRangeOptions* options,
    const NApi::NRpcProxy::NProto::TTabletRangeOptions& proto)
{
    if (proto.has_first_tablet_index()) {
        options->FirstTabletIndex = proto.first_tablet_index();
    }
    if (proto.has_last_tablet_index()) {
        options->LastTabletIndex = proto.last_tablet_index();
    }
}

void FromProto(
    TTabletReadOptionsBase* options,
    const NApi::NRpcProxy::NProto::TTabletReadOptions& proto)
{
    if (proto.has_read_from()) {
        options->ReadFrom = FromProto<NHydra::EPeerKind>(proto.read_from());
    }
    if (proto.has_cached_sync_replicas_timeout()) {
        options->CachedSyncReplicasTimeout = FromProto<TDuration>(proto.cached_sync_replicas_timeout());
    }
}

template <class TRequest>
void SetMutatingOptions(
    TMutatingOptions* options,
    const TRequest* request,
    const IServiceContext* context)
{
    if (request->has_mutating_options()) {
        FromProto(options, request->mutating_options());
    }
    const auto& header = context->RequestHeader();
    if (header.retry()) {
        options->Retry = true;
    }
}

TServiceDescriptor GetServiceDescriptor()
{
    return TServiceDescriptor(NApi::NRpcProxy::ApiServiceName)
        .SetProtocolVersion({
            YTRpcProxyProtocolVersionMajor,
            YTRpcProxyServerProtocolVersionMinor
        });
}

[[noreturn]] void ThrowUnsupportedRowsetFormat(NApi::NRpcProxy::NProto::ERowsetFormat rowsetFormat)
{
    THROW_ERROR_EXCEPTION(
        "Unsupported rowset format %Qv",
        NApi::NRpcProxy::NProto::ERowsetFormat_Name(rowsetFormat));
}

IRowStreamEncoderPtr CreateRowStreamEncoder(
    NApi::NRpcProxy::NProto::ERowsetFormat rowsetFormat,
    NApi::NRpcProxy::NProto::ERowsetFormat arrowFallbackRowsetFormat,
    TTableSchemaPtr schema,
    std::optional<std::vector<std::string>> columns,
    TNameTablePtr nameTable,
    NFormats::TControlAttributesConfigPtr controlAttributesConfig,
    std::optional<NFormats::TFormat> format)
{
    auto createNonArrowEncoder = [&] (NApi::NRpcProxy::NProto::ERowsetFormat rowsetFormat) -> IRowStreamEncoderPtr {
        switch (rowsetFormat) {
            case NApi::NRpcProxy::NProto::RF_YT_WIRE:
                return CreateWireRowStreamEncoder(nameTable);
            case NApi::NRpcProxy::NProto::RF_FORMAT:
                if (!format) {
                    THROW_ERROR_EXCEPTION("No format for %Qv", NApi::NRpcProxy::NProto::ERowsetFormat_Name(rowsetFormat));
                }
                return CreateFormatRowStreamEncoder(
                    nameTable,
                    *format,
                    schema,
                    columns,
                    controlAttributesConfig);
            case NApi::NRpcProxy::NProto::RF_ARROW:
                YT_ABORT();
            default:
                ThrowUnsupportedRowsetFormat(rowsetFormat);
        };
    };

    switch (rowsetFormat) {
        case NApi::NRpcProxy::NProto::RF_YT_WIRE:
        case NApi::NRpcProxy::NProto::RF_FORMAT:
            return createNonArrowEncoder(rowsetFormat);
        case NApi::NRpcProxy::NProto::RF_ARROW: {
            if (arrowFallbackRowsetFormat == NApi::NRpcProxy::NProto::RF_ARROW) {
                THROW_ERROR_EXCEPTION("Arrow fallback rowset format must be different from arrow");
            }

            auto fallbackEncoder = createNonArrowEncoder(arrowFallbackRowsetFormat);
            return CreateArrowRowStreamEncoder(schema, std::move(columns), nameTable, fallbackEncoder, controlAttributesConfig);
        }
        default:
            ThrowUnsupportedRowsetFormat(rowsetFormat);
    }
}

IRowStreamDecoderPtr CreateRowStreamDecoder(
    NApi::NRpcProxy::NProto::ERowsetFormat rowsetFormat,
    TTableSchemaPtr schema,
    TNameTablePtr nameTable,
    std::optional<NFormats::TFormat> format)
{
    switch (rowsetFormat) {
        case NApi::NRpcProxy::NProto::RF_YT_WIRE:
            return CreateWireRowStreamDecoder(std::move(nameTable), CreateUnlimitedWireProtocolOptions());

        case NApi::NRpcProxy::NProto::RF_ARROW:
            return CreateArrowRowStreamDecoder(std::move(schema), std::move(nameTable));

        case NApi::NRpcProxy::NProto::RF_FORMAT:
            if (!format) {
                THROW_ERROR_EXCEPTION("No format for %Qv", NApi::NRpcProxy::NProto::ERowsetFormat_Name(rowsetFormat));
            }
            return CreateFormatRowStreamDecoder(std::move(nameTable), std::move(*format), std::move(schema));

        default:
            THROW_ERROR_EXCEPTION("Unsupported rowset format %Qv",
                NApi::NRpcProxy::NProto::ERowsetFormat_Name(rowsetFormat));
    }
}

bool IsColumnarRowsetFormat(NApi::NRpcProxy::NProto::ERowsetFormat format)
{
    return format == NApi::NRpcProxy::NProto::RF_ARROW;
}

DECLARE_REFCOUNTED_CLASS(TDetailedProfilingCounters)

class TDetailedProfilingCounters
    : public TRefCounted
{
public:
    explicit TDetailedProfilingCounters(TProfiler profiler)
        : Profiler_(std::move(profiler))
        , LookupDuration_(Profiler_.TimeHistogram(
            "/lookup_duration",
            TDuration::MicroSeconds(1),
            TDuration::Seconds(10)))
        , SelectDuration_(Profiler_.TimeHistogram(
            "/select_duration",
            TDuration::MicroSeconds(1),
            TDuration::Seconds(10)))
        , LookupMountCacheWaitTime_(Profiler_.Timer("/lookup_mount_cache_wait_time"))
        , SelectMountCacheWaitTime_(Profiler_.Timer("/select_mount_cache_wait_time"))
        , LookupPermissionCacheWaitTime_(Profiler_.Timer("/lookup_permission_cache_wait_time"))
        , SelectPermissionCacheWaitTime_(Profiler_.Timer("/select_permission_cache_wait_time"))
        , WastedLookupSubrequestCount_(Profiler_.Counter("/wasted_lookup_subrequest_count"))
    { }

    const TEventTimer& LookupDurationTimer() const
    {
        return LookupDuration_;
    }

    const TEventTimer& SelectDurationTimer() const
    {
        return SelectDuration_;
    }

    const TEventTimer& LookupMountCacheWaitTimer() const
    {
        return LookupMountCacheWaitTime_;
    }

    const TEventTimer& SelectMountCacheWaitTimer() const
    {
        return SelectMountCacheWaitTime_;
    }

    const TEventTimer& LookupPermissionCacheWaitTimer() const
    {
        return LookupPermissionCacheWaitTime_;
    }

    const TEventTimer& SelectPermissionCacheWaitTimer() const
    {
        return SelectPermissionCacheWaitTime_;
    }

    const TCounter& WastedLookupSubrequestCount() const
    {
        return WastedLookupSubrequestCount_;
    }

    TCounter* GetRetryCounterByReason(TErrorCode reason)
    {
        return RetryCounters_.FindOrInsert(
            reason,
            [&] {
                return Profiler_
                    .WithTag("reason", ToString(reason))
                    .Counter("/retry_count");
            })
            .first;
    }

private:
    const TProfiler Profiler_;

    //! Histograms.
    TEventTimer LookupDuration_;
    TEventTimer SelectDuration_;

    //! Timers.
    TEventTimer LookupMountCacheWaitTime_;
    TEventTimer SelectMountCacheWaitTime_;
    TEventTimer LookupPermissionCacheWaitTime_;
    TEventTimer SelectPermissionCacheWaitTime_;

    TCounter WastedLookupSubrequestCount_;

    //! Retryable error code to counter map.
    NConcurrency::TSyncMap<TErrorCode, TCounter> RetryCounters_;
};

DEFINE_REFCOUNTED_TYPE(TDetailedProfilingCounters)

//! This context extends standard typed service context. By this moment it is used for structured
//! logging reasons.
template <class TRequestMessage, class TResponseMessage>
class TApiServiceContext
    : public TTypedServiceContext<TRequestMessage, TResponseMessage>
{
public:
    // For most cases the most important request field is "path". If it is present in request message,
    // we want to see it in the structured log.
    DEFINE_BYVAL_RW_PROPERTY(std::optional<TString>, RequestPath);

    TLogger Logger;

public:
    using TTypedServiceContext<TRequestMessage, TResponseMessage>::TTypedServiceContext;

    void SetupMainMessage(TYsonString requestYson)
    {
        EmitMain_ = true;
        RequestYson_ = std::move(requestYson);
    }

    void SetupErrorMessage()
    {
        EmitError_ = true;
    }

    void LogStructuredError(const TError& /*error*/) const
    {
        LogStructured();
    }

    void LogStructured() const
    {
        // Throwing an exception here leads to a double reply, so wrap with try-catch.
        try {
            if (EmitMain_) {
                DoEmitMain();
            }
            if (EmitError_) {
                DoEmitError();
            }
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(TError(ex), "Error while logging structured event");
        }
    }

private:
    //! True if message should be emitted to main topic.
    bool EmitMain_ = false;
    // YSON-serialized request body. This field may be really heavy.
    std::optional<TYsonString> RequestYson_;

    //! True if message should be emitted to error topic provided that request indeed resulted in error.
    bool EmitError_ = false;

    TError GetFinalError() const
    {
        return this->IsCanceled() ? MakeCanceledError() : this->GetError();
    }

    void DoEmitMain() const
    {
        // This topic is a complete verbose structured logging, similar to HTTP proxy structured logging.
        // Note that message contains full request body encoded in YSON, so messages may be really heavy.
        // At production clusters, messages from this topic should not be emitted for the most frequently
        // invoked methods like LookupRows or ModifyRows.

        const auto& header = this->GetRequestHeader();
        const auto& credentialsExt = header.GetExtension(NRpc::NProto::TCredentialsExt::credentials_ext);
        auto hashedCredentials = NAuth::HashCredentials(credentialsExt);
        const auto& traceContext = this->GetTraceContext();

        const auto& error = GetFinalError();
        auto errorCodeSet = error.GetDistinctNonTrivialErrorCodes();
        std::vector<TErrorCode> errorCodes(errorCodeSet.begin(), errorCodeSet.end());
        std::sort(errorCodes.begin(), errorCodes.end());

        LogStructuredEventFluently(RpcProxyStructuredLoggerMain(), ELogLevel::Info)
            .Item("request_id").Value(this->GetRequestId())
            .Item("endpoint").Value(this->GetEndpointAttributes())
            .Item("method").Value(this->GetMethod())
            .OptionalItem("path", RequestPath_)
            .Item("request").Value(RequestYson_)
            .Item("identity").Value(this->GetAuthenticationIdentity())
            .Item("credentials").Value(hashedCredentials)
            .Item("is_retry").Value(this->IsRetry())
            .OptionalItem("mutation_id", this->GetMutationId())
            .OptionalItem("realm_id", this->GetRealmId())
            // Zero-value corresponds to default tos level, so OptionalItem works reasonably,
            // producing item only if tos level is different from default one.
            .OptionalItem("tos_level", header.tos_level())
            .DoIf(static_cast<bool>(traceContext), [&] (auto fluent) {
                fluent
                    .Item("trace_id").Value(traceContext->GetTraceId());
            })
            .Item("error").Value(error)
            .Item("error_skeleton").Value(error.GetSkeleton())
            .Item("error_codes").Value(errorCodes)
            .OptionalItem("user_agent", YT_PROTO_OPTIONAL(header, user_agent))
            .OptionalItem("request_body_size", GetMessageBodySize(this->GetRequestMessage()))
            .OptionalItem("request_attachment_total_size", GetTotalMessageAttachmentSize(this->GetRequestMessage()))
            .DoIf(!this->IsCanceled(), [&] (auto fluent) {
                fluent
                    .OptionalItem("response_body_size", GetMessageBodySize(this->GetResponseMessage()))
                    .OptionalItem("response_attachment_total_size", GetTotalMessageAttachmentSize(this->GetResponseMessage()));
            })
            .OptionalItem("client_start_time", this->GetStartTime())
            .OptionalItem("timeout", this->GetTimeout())
            .Item("arrive_instant").Value(this->GetArriveInstant())
            .Item("wait_time").Value(this->GetWaitDuration())
            .Item("execution_time").Value(this->GetExecutionDuration())
            .Item("finish_instant").Value(this->GetFinishInstant())
            .OptionalItem("cpu_time", this->GetTraceContextTime());
    }

    void DoEmitError() const
    {
        // This topic is designated for (primarily dyntable) error analytics reasons.
        // It is expected to be significantly lighter than main topic, so it is enabled
        // for all methods by default. Messages are emitted only for requests resulted
        // in errors.

        const auto& error = GetFinalError();
        if (error.IsOK()) {
            return;
        }

        auto errorCodeSet = error.GetDistinctNonTrivialErrorCodes();
        std::vector<TErrorCode> errorCodes(errorCodeSet.begin(), errorCodeSet.end());
        std::sort(errorCodes.begin(), errorCodes.end());

        LogStructuredEventFluently(RpcProxyStructuredLoggerError(), ELogLevel::Info)
            .Item("request_id").Value(this->GetRequestId())
            .Item("endpoint").Value(this->GetEndpointAttributes())
            .Item("method").Value(this->GetMethod())
            .OptionalItem("path", RequestPath_)
            .Item("identity").Value(this->GetAuthenticationIdentity())
            .Item("error").Value(error)
            .Item("error_skeleton").Value(error.GetSkeleton())
            .Item("error_codes").Value(errorCodes);
            // TODO(max42): YT-15042. Add error skeleton.
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TApiService)

class TApiService
    : public TServiceBase
    , public IApiService
{
public:
    template <class TRequestMessage, class TResponseMessage>
    using TTypedServiceContextImpl = TApiServiceContext<TRequestMessage, TResponseMessage>;

    TApiService(
        TApiServiceConfigPtr config,
        IInvokerPtr controlInvoker,
        IInvokerPtr workerInvoker,
        NApi::NNative::IConnectionPtr connection,
        NRpc::IAuthenticatorPtr authenticator,
        IProxyCoordinatorPtr proxyCoordinator,
        IAccessCheckerPtr accessChecker,
        ISecurityManagerPtr securityManager,
        NTracing::TSamplerPtr traceSampler,
        NLogging::TLogger logger,
        TProfiler profiler,
        INodeMemoryTrackerPtr memoryTracker,
        IStickyTransactionPoolPtr stickyTransactionPool,
        ISignatureValidatorPtr signatureValidator,
        ISignatureGeneratorPtr signatureGenerator,
        IQueryCorpusReporterPtr queryCorpusReporter)
        : TServiceBase(
            std::move(workerInvoker),
            GetServiceDescriptor(),
            std::move(logger),
            TServiceOptions{
                .MemoryUsageTracker = WithCategory(memoryTracker, EMemoryCategory::Rpc),
                .Authenticator = std::move(authenticator),
            })
        , ApiServiceConfig_(config)
        , Profiler_(std::move(profiler))
        , Connection_(std::move(connection))
        , ProxyCoordinator_(std::move(proxyCoordinator))
        , AccessChecker_(std::move(accessChecker))
        , SecurityManager_(std::move(securityManager))
        , TraceSampler_(std::move(traceSampler))
        , StickyTransactionPool_(stickyTransactionPool
            ? stickyTransactionPool
            : CreateStickyTransactionPool(Logger))
        , AuthenticatedClientCache_(New<NApi::NNative::TClientCache>(
            config->ClientCache,
            Connection_))
        , ControlInvoker_(std::move(controlInvoker))
        , HeapProfilerTestingOptions_(
            config->TestingOptions
            ? config->TestingOptions->HeapProfiler
            : nullptr)
        , HeavyRequestMemoryUsageTracker_(WithCategory(memoryTracker, EMemoryCategory::HeavyRequest))
        , SignatureValidator_(std::move(signatureValidator))
        , SignatureGenerator_(std::move(signatureGenerator))
        , QueryCorpusReporter_(std::move(queryCorpusReporter))
        , SelectConsumeDataWeight_(Profiler_.Counter("/select_consume/data_weight"))
        , SelectConsumeRowCount_(Profiler_.Counter("/select_consume/row_count"))
        , SelectOutputDataWeight_(Profiler_.Counter("/select_output/data_weight"))
        , SelectOutputRowCount_(Profiler_.Counter("/select_output/row_count"))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GenerateTimestamps));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartTransaction));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PingTransaction));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AbortTransaction));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CommitTransaction));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FlushTransaction));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AttachTransaction));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(DetachTransaction));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(ExistsNode));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetNode));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ListNode));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CreateNode));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RemoveNode));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SetNode));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(MultisetAttributesNode));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(LockNode));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(UnlockNode));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CopyNode));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(MoveNode));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(LinkNode));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ConcatenateNodes));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(MountTable));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(UnmountTable));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RemountTable));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FreezeTable));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(UnfreezeTable));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ReshardTable));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ReshardTableAutomatic));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(TrimTable));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AlterTable));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AlterTableReplica));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AlterReplicationCard));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(BalanceTabletCells));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CreateTableBackup));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RestoreTableBackup));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartOperation));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AbortOperation));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SuspendOperation));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ResumeOperation));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CompleteOperation));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(UpdateOperationParameters));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PatchOperationSpec));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetOperation));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ListOperations));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(ListJobs));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(DumpJobContext));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetJobInput)
            .SetStreamingEnabled(true)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetJobInputPaths));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetJobSpec));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetJobStderr));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetJobTrace));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetJobFailContext));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetJob));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AbandonJob));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PollJobShell));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AbortJob));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(DumpJobProxyLog));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(LookupRows));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(VersionedLookupRows));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(MultiLookup)
            .SetConcurrencyLimit(1'000));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SelectRows)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ExplainQuery));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PullRows)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetInSyncReplicas));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetTabletInfos));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetTabletErrors));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(AdvanceConsumer));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AdvanceQueueConsumer));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PullQueue));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PullConsumer));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PullQueueConsumer));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RegisterQueueConsumer));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(UnregisterQueueConsumer));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ListQueueConsumerRegistrations));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CreateQueueProducerSession));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RemoveQueueProducerSession));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PushQueueProducer));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(ModifyRows));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(BatchModifyRows));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(BuildSnapshot));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ExitReadOnly));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(MasterExitReadOnly));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(DiscombobulateNonvotingPeers));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GCCollect));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SuspendCoordinator));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ResumeCoordinator));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(MigrateReplicationCards));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SuspendChaosCells));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ResumeChaosCells));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SuspendTabletCells));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ResumeTabletCells));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AddMaintenance));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RemoveMaintenance));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(DisableChunkLocations));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(DestroyChunkLocations));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ResurrectChunkLocations));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RequestRestart));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(CreateObject));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetTableMountInfo));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetTablePivotKeys));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(AddMember));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RemoveMember));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CheckPermission));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CheckPermissionByAcl));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(TransferAccountResources));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(ReadFile)
            .SetStreamingEnabled(true)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(WriteFile)
            .SetStreamingEnabled(true)
            .SetCancelable(true));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(ReadJournal)
            .SetStreamingEnabled(true)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(WriteJournal)
            .SetStreamingEnabled(true)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(TruncateJournal));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(ReadTable)
            .SetStreamingEnabled(true)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(WriteTable)
            .SetStreamingEnabled(true)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetColumnarStatistics));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PartitionTables));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetFileFromCache));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PutFileToCache));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetPipelineSpec));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SetPipelineSpec));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetPipelineDynamicSpec));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SetPipelineDynamicSpec));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartPipeline));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(StopPipeline));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PausePipeline));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetPipelineState));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetFlowView));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartQuery));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AbortQuery));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetQueryResult));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ReadQueryResult));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetQuery));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ListQueries));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AlterQuery));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetQueryTrackerInfo));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartDistributedWriteSession)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FinishDistributedWriteSession));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(WriteTableFragment)
            .SetStreamingEnabled(true)
            .SetCancelable(true));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartShuffle));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(WriteShuffleData)
            .SetStreamingEnabled(true)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ReadShuffleData)
            .SetStreamingEnabled(true)
            .SetCancelable(true));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(CheckClusterLiveness));

        DeclareServerFeature(ERpcProxyFeature::GetInSyncWithoutKeys);
        DeclareServerFeature(ERpcProxyFeature::WideLocks);
    }

    void OnDynamicConfigChanged(const TApiServiceDynamicConfigPtr& config) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto oldConfig = Config_.Acquire();

        YT_LOG_DEBUG(
            "Updating API service config (OldConfig: %v, NewConfig: %v)",
            ConvertToYsonString(oldConfig, EYsonFormat::Text),
            ConvertToYsonString(config, EYsonFormat::Text));

        AuthenticatedClientCache_->Reconfigure(config->ClientCache);

        SecurityManager_->Reconfigure(config->SecurityManager);

        Config_.Store(config);
    }

    IYPathServicePtr CreateOrchidService() override
    {
        return IYPathService::FromProducer(BIND_NO_PROPAGATE(&TApiService::BuildOrchid, MakeStrong(this)))
            ->Via(ControlInvoker_);
    }

private:
    TApiServiceConfigPtr ApiServiceConfig_;
    const TProfiler Profiler_;
    TAtomicIntrusivePtr<TApiServiceDynamicConfig> Config_{New<TApiServiceDynamicConfig>()};
    const NApi::NNative::IConnectionPtr Connection_;
    const IProxyCoordinatorPtr ProxyCoordinator_;
    const IAccessCheckerPtr AccessChecker_;
    const ISecurityManagerPtr SecurityManager_;
    const NTracing::TSamplerPtr TraceSampler_;
    const IStickyTransactionPoolPtr StickyTransactionPool_;
    const NNative::TClientCachePtr AuthenticatedClientCache_;
    const IInvokerPtr ControlInvoker_;
    const THeapProfilerTestingOptionsPtr HeapProfilerTestingOptions_;
    const IMemoryUsageTrackerPtr HeavyRequestMemoryUsageTracker_;
    const ISignatureValidatorPtr SignatureValidator_;
    const ISignatureGeneratorPtr SignatureGenerator_;

    const IQueryCorpusReporterPtr QueryCorpusReporter_;

    static const TStructuredLoggingMethodDynamicConfigPtr DefaultMethodConfig;

    TCounter SelectConsumeDataWeight_;
    TCounter SelectConsumeRowCount_;

    TCounter SelectOutputDataWeight_;
    TCounter SelectOutputRowCount_;

    struct TDetailedProfilingCountersKey
    {
        std::optional<std::string> UserTag;
        std::optional<TYPath> TablePath;

        operator size_t() const
        {
            return MultiHash(
                UserTag,
                TablePath);
        }
    };

    using TDetailedProfilingCountersMap = NConcurrency::TSyncMap<
        TDetailedProfilingCountersKey,
        TDetailedProfilingCountersPtr
    >;
    TDetailedProfilingCountersMap DetailedProfilingCountersMap_;

    std::atomic<i64> NextSequenceNumberSourceId_ = 0;

    NNative::IClientPtr GetOrCreateClient(const TAuthenticationIdentity& identity)
    {
        auto options = TClientOptions::FromAuthenticationIdentity(identity);
        return AuthenticatedClientCache_->Get(identity, options);
    }

    void AllocateTestData(const TTraceContextPtr& traceContext)
    {
        if (!HeapProfilerTestingOptions_ || !traceContext) {
            return;
        }

        if (HeapProfilerTestingOptions_->AllocationSize) {
            auto guard = TCurrentTraceContextGuard(traceContext);

            auto size = HeapProfilerTestingOptions_->AllocationSize.value();
            auto delay = HeapProfilerTestingOptions_->AllocationReleaseDelay.value_or(TDuration::Zero());

            MakeTestHeapAllocation(size, delay);

            YT_LOG_DEBUG("Test heap allocation is finished (AllocationSize: %v, AllocationReleaseDelay: %v)",
                size,
                delay);
        }
    }

    void BuildOrchid(IYsonConsumer* consumer)
    {
        BuildYsonFluently(consumer)
            .DoMap([] (TFluentMap fluent) {
                DumpGlobalMemoryUsageSnapshot(
                    fluent.GetConsumer(),
                    {
                        RpcProxyUserAllocationTagKey,
                        RpcProxyMethodAllocationTagKey
                    });
            });
    }

    void SetupTracing(const IServiceContextPtr& context)
    {
        auto* traceContext = NTracing::TryGetCurrentTraceContext();
        if (!traceContext) {
            return;
        }

        const auto& config = Config_.Acquire();

        if (config->ForceTracing) {
            traceContext->SetSampled();
        }

        const auto& identity = context->GetAuthenticationIdentity();
        TraceSampler_->SampleTraceContext(identity.User, traceContext);

        if (traceContext->IsRecorded()) {
            traceContext->AddTag("user", identity.User);
            if (identity.UserTag != identity.User) {
                traceContext->AddTag("user_tag", identity.UserTag);
            }
        }

        if (config->EnableAllocationTags) {
            traceContext->SetAllocationTags({
                {RpcProxyUserAllocationTagKey, identity.User},
                {RpcProxyRequestIdAllocationTagKey, ToString(context->GetRequestId())},
                {RpcProxyMethodAllocationTagKey, context->RequestHeader().method()},
            });

            AllocateTestData(traceContext);
        }
    }

    template <class TRequestMessage, class TResponseMessage>
    void InitContext(TApiServiceContext<TRequestMessage, TResponseMessage>* context)
    {
        using TContext = NYT::NRpcProxy::TApiServiceContext<TRequestMessage, TResponseMessage>;

        context->Logger = Logger
            .WithTag("RequestId: %v", context->GetRequestId());

        // First, recover request path from the typed request context using the incredible power of C++20 concepts.
        std::optional<TString> requestPath;
        if constexpr (requires { context->Request().path(); }) {
            requestPath.emplace(context->Request().path());
        }

        context->SetRequestPath(std::move(requestPath));

        // Then, connect it to the typed context using subscriptions for reply and cancel signals.
        context->SubscribeReplied(BIND(&TContext::LogStructured, MakeWeak(context)));
        context->SubscribeCanceled(BIND(&TContext::LogStructuredError, MakeWeak(context)));

        // Finally, setup structured logging messages to be emitted.

        auto shouldEmit = [method = context->GetMethod()] (const TStructuredLoggingTopicDynamicConfigPtr& config) {
            return config->Enable &&
                !config->SuppressedMethods.contains(method) &&
                config->Methods.Value(method, DefaultMethodConfig)->Enable;
        };

        const auto& config = Config_.Acquire();

        // NB: We try to do heavy work only if we are actually going to omit corresponding message. Conserve priceless CPU time.
        if (shouldEmit(config->StructuredLoggingMainTopic)) {
            TString requestYson;
            TStringOutput requestOutput(requestYson);
            TYsonWriter requestYsonWriter(&requestOutput, EYsonFormat::Text);
            TProtobufParserOptions parserOptions{
                .SkipUnknownFields = true,
                .Utf8Check = EUtf8Check::Disable,
            };

            const TRequestMessage* request = &context->Request();
            TRequestMessage copy;

            if constexpr (std::is_same_v<TRequestMessage, NApi::NRpcProxy::NProto::TReqSelectRows>) {
                if (std::ssize(request->query()) > config->StructuredLoggingQueryTruncationSize) {
                    copy = *request;
                    copy.mutable_query()->resize(config->StructuredLoggingQueryTruncationSize);
                    request = &copy;
                }
            }

            const auto& methodConfig =  config->StructuredLoggingMainTopic->Methods.Value(context->GetMethod(), DefaultMethodConfig);
            const auto maxRequestSize = methodConfig->MaxRequestByteSize.value_or(config->StructuredLoggingMaxRequestByteSize);

            if (static_cast<i64>(request->ByteSizeLong()) <= maxRequestSize) {
                WriteProtobufMessage(&requestYsonWriter, *request, parserOptions);
            } else {
                requestYsonWriter.OnEntity();
            }

            context->SetupMainMessage(TYsonString(std::move(requestYson)));
        }

        if (shouldEmit(config->StructuredLoggingErrorTopic)) {
            context->SetupErrorMessage();
        }
    }

    NNative::IClientPtr GetAuthenticatedClientOrThrow(
        const IServiceContextPtr& context,
        const google::protobuf::Message* request)
    {
        SetupTracing(context);

        const auto& identity = context->GetAuthenticationIdentity();

        THROW_ERROR_EXCEPTION_IF_FAILED(AccessChecker_->CheckAccess(identity.User));

        SecurityManager_->ValidateUser(identity.User);

        ProxyCoordinator_->ValidateOperable();

        const auto& config = Config_.Acquire();

        // Pretty-printing Protobuf requires a bunch of effort, so we make it conditional.
        if (config->VerboseLogging) {
            YT_LOG_DEBUG("RequestId: %v, RequestBody: %v",
                context->GetRequestId(),
                request->ShortDebugString());
        }

        auto client = GetOrCreateClient(identity);
        if (!client) {
            THROW_ERROR_EXCEPTION("No client found for identity %Qv", identity);
        }

        return client;
    }

    ITransactionPtr FindTransaction(
        const IServiceContextPtr& context,
        const google::protobuf::Message* request,
        TTransactionId transactionId,
        const std::optional<TTransactionAttachOptions>& options,
        bool searchInPool = true)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        ITransactionPtr transaction;
        if (searchInPool) {
            transaction = StickyTransactionPool_->FindTransactionAndRenewLease(transactionId);
        }
        // Attachment to a tablet transaction works via sticky transaction pool.
        // Native client AttachTransaction is only supported for master transactions.
        if (!transaction && options && IsMasterTransactionId(transactionId)) {
            transaction = client->AttachTransaction(transactionId, *options);
        }

        return transaction;
    }

    ITransactionPtr GetTransactionOrThrow(
        const IServiceContextPtr& context,
        const google::protobuf::Message* request,
        TTransactionId transactionId,
        const std::optional<TTransactionAttachOptions>& options,
        bool searchInPool = true)
    {
        auto transaction = FindTransaction(
            context,
            request,
            transactionId,
            options,
            searchInPool);
        if (!transaction) {
            NTransactionServer::ThrowNoSuchTransaction(transactionId);
        }
        return transaction;
    }

    template <class TContext, class TExecutor, class TResultHandler>
    class TExecuteCallSession
        : public TRefCounted
    {
    public:
        TExecuteCallSession(
            TApiServicePtr apiService,
            TIntrusivePtr<TContext> context,
            TExecutor&& executor,
            TResultHandler&& resultHandler)
            : ApiService_(std::move(apiService))
            , Context_(std::move(context))
            , Executor_(std::move(executor))
            , ResultHandler_(std::move(resultHandler))
        { }

        void Run()
        {
            auto future = Executor_();

            using TResult = typename decltype(future)::TValueType;

            future.Subscribe(
                BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<TResult>& resultOrError) {
                    if (!resultOrError.IsOK()) {
                        HandleError(resultOrError);
                        return;
                    }

                    try {
                        if constexpr(std::is_same_v<TResult, void>) {
                            ResultHandler_(Context_);
                        } else {
                            ResultHandler_(Context_, resultOrError.Value());
                        }
                        Context_->Reply();
                    } catch (const std::exception& ex) {
                        HandleError(ex);
                    } catch (const TFiberCanceledException&) {
                        // Intentionally do nothing.
                    }
                }));

            Context_->SubscribeCanceled(BIND([future = std::move(future)] (const TError& error) {
                future.Cancel(error);
            }));
        }

    private:
        const TApiServicePtr ApiService_;
        const TIntrusivePtr<TContext> Context_;
        const TExecutor Executor_;
        const TResultHandler ResultHandler_;

        void HandleError(const TError& error)
        {
            auto wrappedError = TError(error.GetCode(), "Internal RPC call failed")
                << error;
            // If request contains path (e.g. GetNode), enrich error with it.
            if constexpr (requires { Context_->Request().path(); }) {
                wrappedError = wrappedError
                    << TErrorAttribute("path", Context_->Request().path());
            }
            Context_->Reply(std::move(wrappedError));
        }
    };

    template <class TContext, class TExecutor, class TResultHandler>
    void ExecuteCall(
        TIntrusivePtr<TContext> context,
        TExecutor&& executor,
        TResultHandler&& resultHandler)
    {
        New<TExecuteCallSession<TContext, TExecutor, TResultHandler>>(
            this,
            std::move(context),
            std::move(executor),
            std::move(resultHandler))
            ->Run();
    }

    template <class TContext, class TExecutor>
    void ExecuteCall(
        const TIntrusivePtr<TContext>& context,
        TExecutor&& executor)
    {
        ExecuteCall(
            context,
            std::move(executor),
            [] (const TIntrusivePtr<TContext>& /*context*/) { });
    }


    TDetailedProfilingCountersPtr GetOrCreateDetailedProfilingCounters(
        const TDetailedProfilingCountersKey& key)
    {
        return *DetailedProfilingCountersMap_.FindOrInsert(
            key,
            [&] {
                auto profiler = Profiler_
                    .WithPrefix("/detailed_table_statistics")
                    .WithSparse();
                if (key.TablePath) {
                    profiler = profiler
                        .WithTag("table_path", *key.TablePath);
                }
                if (key.UserTag) {
                    profiler = profiler
                        // TODO(babenko): switch to std::string
                        .WithTag("user", ToString(*key.UserTag));
                }
                return New<TDetailedProfilingCounters>(std::move(profiler));
            })
            .first;
    }


    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GenerateTimestamps)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto count = request->count();
        auto clockClusterTag = request->has_clock_cluster_tag()
            ? FromProto<TCellTag>(request->clock_cluster_tag())
            : InvalidCellTag;

        context->SetRequestInfo("Count: %v, ClockClusterTag: %v",
            count,
            clockClusterTag);

        if (clockClusterTag == InvalidCellTag) {
            Connection_->GetClockManager()->ValidateDefaultClock("Unable to generate timestamps");
        }

        const auto& timestampProvider = Connection_->GetTimestampProvider();

        ExecuteCall(
            context,
            [=, Logger = Logger, connection = Connection_] {
                return timestampProvider->GenerateTimestamps(count, clockClusterTag).ApplyUnique(
                    BIND([connection, clockClusterTag, count, Logger] (TErrorOr<TTimestamp>&& providerResult) {
                        if (providerResult.IsOK() ||
                            !(providerResult.FindMatching(NTransactionClient::EErrorCode::UnknownClockClusterTag) ||
                                providerResult.FindMatching(NTransactionClient::EErrorCode::ClockClusterTagMismatch) ||
                                providerResult.FindMatching(NRpc::EErrorCode::UnsupportedServerFeature)))
                        {
                            return MakeFuture(std::move(providerResult));
                        }

                        YT_LOG_WARNING(
                            providerResult,
                            "Wrong clock cluster tag %v, trying to generate timestamps via direct call",
                            clockClusterTag);

                        auto alienClient = connection->GetClockManager()->GetTimestampProviderOrThrow(clockClusterTag);
                        return alienClient->GenerateTimestamps(count);
                    }));
            },
            [clockClusterTag] (const auto& context, const TTimestamp& timestamp) {
                auto* response = &context->Response();
                response->set_timestamp(timestamp);

                context->SetResponseInfo("Timestamp: %v@%v",
                    timestamp,
                    clockClusterTag);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, StartTransaction)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        if (!request->sticky() && request->type() == NApi::NRpcProxy::NProto::ETransactionType::TT_TABLET) {
            THROW_ERROR_EXCEPTION("Tablet transactions must be sticky");
        }

        auto transactionType = FromProto<NTransactionClient::ETransactionType>(request->type());

        TTransactionStartOptions options;
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_timeout()) {
            options.Timeout = FromProto<TDuration>(request->timeout());
        }
        if (request->has_deadline()) {
            options.Deadline = FromProto<TInstant>(request->deadline());
        }
        if (request->has_id()) {
            FromProto(&options.Id, request->id());
        }
        if (request->has_parent_id()) {
            FromProto(&options.ParentId, request->parent_id());
        }
        if (request->has_replicate_to_master_cell_tags()) {
            options.ReplicateToMasterCellTags =
                FromProto<TCellTagList>(request->replicate_to_master_cell_tags().cell_tags());
        }
        options.AutoAbort = false;
        options.Sticky = request->sticky();
        options.Ping = request->ping();
        options.PingAncestors = request->ping_ancestors();
        options.Atomicity = FromProto<NTransactionClient::EAtomicity>(request->atomicity());
        options.Durability = FromProto<NTransactionClient::EDurability>(request->durability());
        if (request->has_attributes()) {
            options.Attributes = NYTree::FromProto(request->attributes());
        }
        options.PrerequisiteTransactionIds = FromProto<std::vector<TTransactionId>>(request->prerequisite_transaction_ids());
        if (request->has_start_timestamp()) {
            options.StartTimestamp = request->start_timestamp();
        }

        context->SetRequestInfo("TransactionType: %v, TransactionId: %v, ParentId: %v, PrerequisiteTransactionIds: %v, "
            "Timeout: %v, Deadline: %v, AutoAbort: %v, "
            "Sticky: %v, Ping: %v, PingAncestors: %v, Atomicity: %v, Durability: %v, StartTimestamp: %v",
            transactionType,
            options.Id,
            options.ParentId,
            options.PrerequisiteTransactionIds,
            options.Timeout,
            options.Deadline,
            options.AutoAbort,
            options.Sticky,
            options.Ping,
            options.PingAncestors,
            options.Atomicity,
            options.Durability,
            options.StartTimestamp);

        ExecuteCall(
            context,
            [=] {
                return client->StartTransaction(transactionType, options);
            },
            [=, this, this_ = MakeStrong(this)] (const auto& context, const auto& transaction) {
                auto* response = &context->Response();
                ToProto(response->mutable_id(), transaction->GetId());
                response->set_start_timestamp(transaction->GetStartTimestamp());
                if (transactionType == ETransactionType::Tablet) {
                    response->set_sequence_number_source_id(NextSequenceNumberSourceId_++);
                }

                if (options.Sticky) {
                    StickyTransactionPool_->RegisterTransaction(transaction);
                }

                context->SetResponseInfo("TransactionId: %v, StartTimestamp: %v",
                    transaction->GetId(),
                    transaction->GetStartTimestamp());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, PingTransaction)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        TTransactionAttachOptions attachOptions;
        attachOptions.Ping = false;
        attachOptions.PingAncestors = request->ping_ancestors();

        context->SetRequestInfo("TransactionId: %v",
            transactionId);

        auto transaction = GetTransactionOrThrow(
            context,
            request,
            transactionId,
            attachOptions);

        ExecuteCall(
            context,
            [=] {
                return transaction->Ping();
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, CommitTransaction)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        TTransactionCommitOptions options;
        SetMutatingOptions(&options, request, context.Get());
        options.AdditionalParticipantCellIds = FromProto<std::vector<TCellId>>(request->additional_participant_cell_ids());
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }

        context->SetRequestInfo("TransactionId: %v, AdditionalParticipantCellIds: %v, PrerequisiteTransactionIds: %v",
            transactionId,
            options.PrerequisiteTransactionIds,
            options.AdditionalParticipantCellIds);

        auto transaction = GetTransactionOrThrow(
            context,
            request,
            transactionId,
            TTransactionAttachOptions{
                .Ping = false,
                .PingAncestors = false
            });

        ExecuteCall(
            context,
            [=] {
                return transaction->Commit(options);
            },
            [] (const auto& context, const TTransactionCommitResult& result) {
                auto* response = &context->Response();
                ToProto(response->mutable_commit_timestamps(), result.CommitTimestamps);
                response->set_primary_commit_timestamp(result.PrimaryCommitTimestamp);

                context->SetResponseInfo("PrimaryCommitTimestamp: %v, CommitTimestamps: %v",
                    result.PrimaryCommitTimestamp, result.CommitTimestamps);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, FlushTransaction)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        context->SetRequestInfo("TransactionId: %v",
            transactionId);

        auto transaction = GetTransactionOrThrow(
            context,
            request,
            transactionId,
            TTransactionAttachOptions{
                .Ping = false,
                .PingAncestors = false
            });

        ExecuteCall(
            context,
            [=] {
                return transaction->Flush();
            },
            [&] (const auto& context, const TTransactionFlushResult& result) {
                auto* response = &context->Response();
                ToProto(response->mutable_participant_cell_ids(), result.ParticipantCellIds);

                context->SetResponseInfo("ParticipantCellIds: %v",
                    result.ParticipantCellIds);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, AbortTransaction)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        TTransactionAbortOptions options;
        SetMutatingOptions(&options, request, context.Get());

        context->SetRequestInfo("TransactionId: %v",
            transactionId);

        auto transaction = GetTransactionOrThrow(
            context,
            request,
            transactionId,
            TTransactionAttachOptions{
                .Ping = false,
                .PingAncestors = false
            });

        ExecuteCall(
            context,
            [=] {
                return transaction->Abort(options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, AttachTransaction)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        TTransactionAttachOptions options;
        if (request->has_ping_period()) {
            options.PingPeriod = TDuration::FromValue(request->ping_period());
        }
        if (request->has_ping()) {
            options.Ping = request->ping();
        }
        if (request->has_ping_ancestors()) {
            options.PingAncestors = request->ping_ancestors();
        }

        context->SetRequestInfo("TransactionId: %v",
            transactionId);

        auto transaction = GetTransactionOrThrow(
            context,
            request,
            transactionId,
            options,
            /*searchInPool*/ true);

        response->set_type(static_cast<NApi::NRpcProxy::NProto::ETransactionType>(transaction->GetType()));
        response->set_start_timestamp(transaction->GetStartTimestamp());
        response->set_atomicity(static_cast<NApi::NRpcProxy::NProto::EAtomicity>(transaction->GetAtomicity()));
        response->set_durability(static_cast<NApi::NRpcProxy::NProto::EDurability>(transaction->GetDurability()));
        response->set_timeout(ToProto(transaction->GetTimeout()));
        if (transaction->GetType() == ETransactionType::Tablet) {
            response->set_sequence_number_source_id(NextSequenceNumberSourceId_++);
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, DetachTransaction)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        context->SetRequestInfo("TransactionId: %v",
            transactionId);

        StickyTransactionPool_->UnregisterTransaction(transactionId);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, CreateObject)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto type = FromProto<EObjectType>(request->type());
        TCreateObjectOptions options;
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_ignore_existing()) {
            options.IgnoreExisting = request->ignore_existing();
        }
        if (request->has_attributes()) {
            options.Attributes = NYTree::FromProto(request->attributes());
        }

        context->SetRequestInfo("Type: %v, IgnoreExisting: %v",
            type,
            options.IgnoreExisting);

        ExecuteCall(
            context,
            [=] {
                return client->CreateObject(type, options);
            },
            [] (const auto& context, NObjectClient::TObjectId objectId) {
                auto* response = &context->Response();
                ToProto(response->mutable_object_id(), objectId);

                context->SetResponseInfo("ObjectId: %v", objectId);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetTableMountInfo)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto path = FromProto<TYPath>(request->path());

        context->SetRequestInfo("Path: %v", path);

        const auto& tableMountCache = client->GetTableMountCache();
        ExecuteCall(
            context,
            [=] {
                return tableMountCache->GetTableInfo(path);
            },
            [] (const auto& context, const TTableMountInfoPtr& tableMountInfo) {
                auto* response = &context->Response();

                ToProto(response->mutable_table_id(), tableMountInfo->TableId);
                const auto& primarySchema = tableMountInfo->Schemas[ETableSchemaKind::Primary];
                ToProto(response->mutable_schema(), primarySchema);
                for (const auto& tabletInfoPtr : tableMountInfo->Tablets) {
                    ToProto(response->add_tablets(), *tabletInfoPtr);
                }

                if (tableMountInfo->IsChaosReplicated() && tableMountInfo->UpperCapBound.GetCount() != 0) {
                    response->set_tablet_count(tableMountInfo->UpperCapBound[0].Data.Int64);
                }

                response->set_dynamic(tableMountInfo->Dynamic);
                ToProto(response->mutable_upstream_replica_id(), tableMountInfo->UpstreamReplicaId);
                for (const auto& replica : tableMountInfo->Replicas) {
                    auto* protoReplica = response->add_replicas();
                    ToProto(protoReplica->mutable_replica_id(), replica->ReplicaId);
                    protoReplica->set_cluster_name(replica->ClusterName);
                    protoReplica->set_replica_path(replica->ReplicaPath);
                    protoReplica->set_mode(ToProto(replica->Mode));
                }
                response->set_physical_path(tableMountInfo->PhysicalPath);

                for (const auto& indexInfo : tableMountInfo->Indices) {
                    auto* protoIndexInfo = response->add_indices();
                    ToProto(protoIndexInfo->mutable_index_table_id(), indexInfo.TableId);
                    protoIndexInfo->set_index_kind(ToProto(indexInfo.Kind));
                    if (const auto& predicate = indexInfo.Predicate) {
                        ToProto(protoIndexInfo->mutable_predicate(), *predicate);
                    }
                    if (const auto& unfoldedColumn = indexInfo.UnfoldedColumn) {
                        ToProto(protoIndexInfo->mutable_unfolded_column(), *unfoldedColumn);
                    }
                    protoIndexInfo->set_index_correspondence(ToProto(indexInfo.Correspondence));
                }

                context->SetResponseInfo("Dynamic: %v, TabletCount: %v, ReplicaCount: %v",
                    tableMountInfo->Dynamic,
                    tableMountInfo->Tablets.size(),
                    tableMountInfo->Replicas.size());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetTablePivotKeys)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto path = FromProto<TYPath>(request->path());

        context->SetRequestInfo("Path: %v", path);
        TGetTablePivotKeysOptions options;
        options.RepresentKeyAsList = request->represent_key_as_list();

        ExecuteCall(
            context,
            [=] {
                return client->GetTablePivotKeys(path, options);
            },
            [] (const auto& context, const TYsonString& result) {
                auto* response = &context->Response();
                response->set_value(result.ToString());
            });
    }

    ////////////////////////////////////////////////////////////////////////////////
    // CYPRESS
    ////////////////////////////////////////////////////////////////////////////////

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ExistsNode)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TNodeExistsOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }
        if (request->has_master_read_options()) {
            FromProto(&options, request->master_read_options());
        }
        if (request->has_suppressable_access_tracking_options()) {
            FromProto(&options, request->suppressable_access_tracking_options());
        }

        context->SetRequestInfo("Path: %v",
            path);

        ExecuteCall(
            context,
            [=] {
                return client->NodeExists(path, options);
            },
            [] (const auto& context, const bool& result) {
                auto* response = &context->Response();
                response->set_exists(result);

                context->SetResponseInfo("Exists: %v",
                    result);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetNode)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TGetNodeOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_attributes()) {
            FromProto(&options.Attributes, request->attributes());
        } else if (request->has_legacy_attributes() && !request->legacy_attributes().all()) {
            // COMPAT(max42): remove when no clients older than Aug22 are there.
            options.Attributes = TAttributeFilter(FromProto<std::vector<std::string>>(request->legacy_attributes().keys()));
        }
        if (request->has_max_size()) {
            options.MaxSize = request->max_size();
        }
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }
        if (request->has_master_read_options()) {
            FromProto(&options, request->master_read_options());
        }
        if (request->has_suppressable_access_tracking_options()) {
            FromProto(&options, request->suppressable_access_tracking_options());
        }
        if (request->has_complexity_limits()) {
            FromProto(&options.ComplexityLimits, request->complexity_limits());
        }
        if (request->has_options()) {
            options.Options = NYTree::FromProto(request->options());
        }

        context->SetRequestInfo("Path: %v, AttributeFilter: %v",
            path,
            options.Attributes);

        ExecuteCall(
            context,
            [=] {
                return client->GetNode(path, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->set_value(result.ToString());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ListNode)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TListNodeOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_attributes()) {
            FromProto(&options.Attributes, request->attributes());
        } else if (request->has_legacy_attributes() && !request->legacy_attributes().all()) {
            // COMPAT(max42): remove when no clients older than Aug22 are there.
            options.Attributes = TAttributeFilter(FromProto<std::vector<std::string>>(request->legacy_attributes().keys()));
        }
        if (request->has_max_size()) {
            options.MaxSize = request->max_size();
        }
        if (request->has_complexity_limits()) {
            FromProto(&options.ComplexityLimits, request->complexity_limits());
        }
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }
        if (request->has_master_read_options()) {
            FromProto(&options, request->master_read_options());
        }
        if (request->has_suppressable_access_tracking_options()) {
            FromProto(&options, request->suppressable_access_tracking_options());
        }

        context->SetRequestInfo("Path: %v, AttributeFilter: %v",
            path,
            options.Attributes);

        ExecuteCall(
            context,
            [=] {
                return client->ListNode(path, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->set_value(result.ToString());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, CreateNode)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();
        auto type = FromProto<NObjectClient::EObjectType>(request->type());

        TCreateNodeOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_attributes()) {
            options.Attributes = NYTree::FromProto(request->attributes());
        }
        if (request->has_recursive()) {
            options.Recursive = request->recursive();
        }
        if (request->has_force()) {
            options.Force = request->force();
        }
        if (request->has_ignore_existing()) {
            options.IgnoreExisting = request->ignore_existing();
        }
        if (request->has_lock_existing()) {
            options.LockExisting = request->lock_existing();
        }
        if (request->has_ignore_type_mismatch()) {
            options.IgnoreTypeMismatch = request->ignore_type_mismatch();
        }
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }

        context->SetRequestInfo("Path: %v, Type: %v",
            path,
            type);

        ExecuteCall(
            context,
            [=] {
                return client->CreateNode(path, type, options);
            },
            [] (const auto& context, NCypressClient::TNodeId nodeId) {
                auto* response = &context->Response();
                ToProto(response->mutable_node_id(), nodeId);

                context->SetResponseInfo("NodeId: %v",
                    nodeId);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, RemoveNode)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TRemoveNodeOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_recursive()) {
            options.Recursive = request->recursive();
        }
        if (request->has_force()) {
            options.Force = request->force();
        }
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }

        context->SetRequestInfo("Path: %v",
            path);

        ExecuteCall(
            context,
            [=] {
                return client->RemoveNode(path, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, SetNode)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();
        auto value = TYsonString(request->value());

        TSetNodeOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_recursive()) {
            options.Recursive = request->recursive();
        }
        if (request->has_force()) {
            options.Force = request->force();
        }
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }
        if (request->has_suppressable_access_tracking_options()) {
            FromProto(&options, request->suppressable_access_tracking_options());
        }

        context->SetRequestInfo("Path: %v",
            path);

        ExecuteCall(
            context,
            [=] {
                return client->SetNode(path, value, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, MultisetAttributesNode)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        auto attributes = GetEphemeralNodeFactory()->CreateMap();
        for (const auto& protoSubrequest : request->subrequests()) {
            attributes->AddChild(
                protoSubrequest.attribute(),
                ConvertToNode(TYsonString(protoSubrequest.value())));
        }

        TMultisetAttributesNodeOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_force()) {
            options.Force = request->force();
        }
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }
        if (request->has_suppressable_access_tracking_options()) {
            FromProto(&options, request->suppressable_access_tracking_options());
        }

        context->SetRequestInfo("Path: %v, Attributes: %v",
            path,
            attributes->GetKeys());

        ExecuteCall(
            context,
            [=] {
                return client->MultisetAttributesNode(path, attributes, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, LockNode)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();
        auto mode = FromProto<NCypressClient::ELockMode>(request->mode());

        TLockNodeOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_waitable()) {
            options.Waitable = request->waitable();
        }
        if (request->has_child_key()) {
            options.ChildKey = request->child_key();
        }
        if (request->has_attribute_key()) {
            options.AttributeKey = request->attribute_key();
        }
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }

        context->SetRequestInfo("Path: %v, Mode: %v",
            path,
            mode);

        ExecuteCall(
            context,
            [=] {
                return client->LockNode(path, mode, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                ToProto(response->mutable_node_id(), result.NodeId);
                ToProto(response->mutable_lock_id(), result.LockId);
                response->set_revision(ToProto(result.Revision));

                context->SetResponseInfo("NodeId: %v, LockId: %v, Revision: %x",
                    result.NodeId,
                    result.LockId,
                    result.Revision);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, UnlockNode)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TUnlockNodeOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }

        context->SetRequestInfo("Path: %v", path);

        ExecuteCall(
            context,
            [=] {
                return client->UnlockNode(path, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, CopyNode)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& srcPath = request->src_path();
        const auto& dstPath = request->dst_path();

        TCopyNodeOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_recursive()) {
            options.Recursive = request->recursive();
        }
        if (request->has_ignore_existing()) {
            options.IgnoreExisting = request->ignore_existing();
        }
        if (request->has_lock_existing()) {
            options.LockExisting = request->lock_existing();
        }
        if (request->has_force()) {
            options.Force = request->force();
        }
        if (request->has_preserve_account()) {
            options.PreserveAccount = request->preserve_account();
        }
        if (request->has_preserve_creation_time()) {
            options.PreserveCreationTime = request->preserve_creation_time();
        }
        if (request->has_preserve_modification_time()) {
            options.PreserveModificationTime = request->preserve_modification_time();
        }
        if (request->has_preserve_expiration_time()) {
            options.PreserveExpirationTime = request->preserve_expiration_time();
        }
        if (request->has_preserve_expiration_timeout()) {
            options.PreserveExpirationTimeout = request->preserve_expiration_timeout();
        }
        if (request->has_preserve_owner()) {
            options.PreserveOwner = request->preserve_owner();
        }
        if (request->has_preserve_acl()) {
            options.PreserveAcl = request->preserve_acl();
        }
        if (request->has_pessimistic_quota_check()) {
            options.PessimisticQuotaCheck = request->pessimistic_quota_check();
        }
        if (request->has_enable_cross_cell_copying()) {
            options.EnableCrossCellCopying = request->enable_cross_cell_copying();
        }
        if (request->has_allow_secondary_index_abandonment()) {
            options.AllowSecondaryIndexAbandonment = request->allow_secondary_index_abandonment();
        }
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }

        context->SetRequestInfo("SrcPath: %v, DstPath: %v",
            srcPath,
            dstPath);

        ExecuteCall(
            context,
            [=] {
                return client->CopyNode(srcPath, dstPath, options);
            },
            [] (const auto& context, NCypressClient::TNodeId nodeId) {
                auto* response = &context->Response();
                ToProto(response->mutable_node_id(), nodeId);

                context->SetResponseInfo("NodeId: %v",
                    nodeId);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, MoveNode)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& srcPath = request->src_path();
        const auto& dstPath = request->dst_path();

        TMoveNodeOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_recursive()) {
            options.Recursive = request->recursive();
        }
        if (request->has_force()) {
            options.Force = request->force();
        }
        if (request->has_preserve_account()) {
            options.PreserveAccount = request->preserve_account();
        }
        if (request->has_preserve_creation_time()) {
            options.PreserveCreationTime = request->preserve_creation_time();
        }
        if (request->has_preserve_modification_time()) {
            options.PreserveModificationTime = request->preserve_modification_time();
        }
        if (request->has_preserve_expiration_time()) {
            options.PreserveExpirationTime = request->preserve_expiration_time();
        }
        if (request->has_preserve_expiration_timeout()) {
            options.PreserveExpirationTimeout = request->preserve_expiration_timeout();
        }
        if (request->has_preserve_owner()) {
            options.PreserveOwner = request->preserve_owner();
        }
        if (request->has_preserve_acl()) {
            options.PreserveAcl = request->preserve_acl();
        }
        if (request->has_pessimistic_quota_check()) {
            options.PessimisticQuotaCheck = request->pessimistic_quota_check();
        }
        if (request->has_enable_cross_cell_copying()) {
            options.EnableCrossCellCopying = request->enable_cross_cell_copying();
        }
        if (request->has_allow_secondary_index_abandonment()) {
            options.AllowSecondaryIndexAbandonment = request->allow_secondary_index_abandonment();
        }
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }

        context->SetRequestInfo("SrcPath: %v, DstPath: %v",
            srcPath,
            dstPath);

        ExecuteCall(
            context,
            [=] {
                return client->MoveNode(srcPath, dstPath, options);
            },
            [] (const auto& context, const auto& nodeId) {
                auto* response = &context->Response();
                ToProto(response->mutable_node_id(), nodeId);

                context->SetResponseInfo("NodeId: %v",
                    nodeId);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, LinkNode)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& srcPath = request->src_path();
        const auto& dstPath = request->dst_path();

        TLinkNodeOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_recursive()) {
            options.Recursive = request->recursive();
        }
        if (request->has_force()) {
            options.Force = request->force();
        }
        if (request->has_ignore_existing()) {
            options.IgnoreExisting = request->ignore_existing();
        }
        if (request->has_lock_existing()) {
            options.LockExisting = request->lock_existing();
        }
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }

        context->SetRequestInfo("SrcPath: %v, DstPath: %v",
            srcPath,
            dstPath);

        ExecuteCall(
            context,
            [=] {
                return client->LinkNode(
                srcPath,
                dstPath,
                options);
            },
            [] (const auto& context, const auto& nodeId) {
                auto* response = &context->Response();
                ToProto(response->mutable_node_id(), nodeId);

                context->SetResponseInfo("NodeId: %v",
                    nodeId);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ConcatenateNodes)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto srcPaths = FromProto<std::vector<TRichYPath>>(request->src_paths());
        auto dstPath = FromProto<TRichYPath>(request->dst_path());

        TConcatenateNodesOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }

        options.ChunkMetaFetcherConfig = New<NChunkClient::TFetcherConfig>();

        context->SetRequestInfo("SrcPaths: %v, DstPath: %v",
            srcPaths,
            dstPath);

        if (request->has_fetcher()) {
            options.ChunkMetaFetcherConfig->NodeRpcTimeout = FromProto<TDuration>(request->fetcher().node_rpc_timeout());
        }

        ExecuteCall(
            context,
            [=] {
                return client->ConcatenateNodes(srcPaths, dstPath, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ExternalizeNode)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();
        auto cellTag = FromProto<TCellTag>(request->cell_tag());

        TExternalizeNodeOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }

        context->SetRequestInfo("Path: %v, CellTag: %v",
            path,
            cellTag);

        ExecuteCall(
            context,
            [=] {
                return client->ExternalizeNode(path, cellTag, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, InternalizeNode)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TInternalizeNodeOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }

        context->SetRequestInfo("Path: %v",
            path);

        ExecuteCall(
            context,
            [=] {
                return client->InternalizeNode(path, options);
            });
    }

    ////////////////////////////////////////////////////////////////////////////////
    // TABLES (NON-TRANSACTIONAL)
    ////////////////////////////////////////////////////////////////////////////////

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, MountTable)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TMountTableOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_cell_id()) {
            FromProto(&options.CellId, request->cell_id());
        }
        FromProto(&options.TargetCellIds, request->target_cell_ids());
        if (request->has_freeze()) {
            options.Freeze = request->freeze();
        }

        if (request->has_tablet_range_options()) {
            FromProto(&options, request->tablet_range_options());
        }

        context->SetRequestInfo("Path: %v",
            path);

        ExecuteCall(
            context,
            [=] {
                return client->MountTable(path, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, UnmountTable)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TUnmountTableOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_force()) {
            options.Force = request->force();
        }
        if (request->has_tablet_range_options()) {
            FromProto(&options, request->tablet_range_options());
        }

        context->SetRequestInfo("Path: %v",
            path);

        ExecuteCall(
            context,
            [=] {
                return client->UnmountTable(path, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, RemountTable)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TRemountTableOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_tablet_range_options()) {
            FromProto(&options, request->tablet_range_options());
        }

        context->SetRequestInfo("Path: %v",
            path);

        ExecuteCall(
            context,
            [=] {
                return client->RemountTable(path, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, FreezeTable)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TFreezeTableOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_tablet_range_options()) {
            FromProto(&options, request->tablet_range_options());
        }

        context->SetRequestInfo("Path: %v",
            path);

        ExecuteCall(
            context,
            [=] {
                return client->FreezeTable(path, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, UnfreezeTable)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TUnfreezeTableOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_tablet_range_options()) {
            FromProto(&options, request->tablet_range_options());
        }

        context->SetRequestInfo("Path: %v",
            path);

        ExecuteCall(
            context,
            [=] {
                return client->UnfreezeTable(path, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ReshardTable)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TReshardTableOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_tablet_range_options()) {
            FromProto(&options, request->tablet_range_options());
        }
        if (request->has_uniform()) {
            options.Uniform = request->uniform();
        }
        if (request->has_enable_slicing()) {
            options.EnableSlicing = request->enable_slicing();
        }
        if (request->has_slicing_accuracy()) {
            options.SlicingAccuracy = request->slicing_accuracy();
        }

        TFuture<void> result;
        if (request->has_tablet_count()) {
            auto tabletCount = request->tablet_count();

            context->SetRequestInfo("Path: %v, TabletCount: %v",
                path,
                tabletCount);

            ExecuteCall(
                context,
                [=] {
                    return client->ReshardTable(path, tabletCount, options);
                });
        } else {
            auto reader = CreateWireProtocolReader(MergeRefsToRef<TApiServiceBufferTag>(request->Attachments()));
            auto keyRange = reader->ReadUnversionedRowset(false);
            std::vector<TLegacyOwningKey> keys;
            keys.reserve(keyRange.Size());
            for (const auto& key : keyRange) {
                keys.emplace_back(key);
            }

            context->SetRequestInfo("Path: %v, Keys: %v",
                path,
                keys);

            ExecuteCall(
                context,
                [=] {
                    return client->ReshardTable(path, keys, options);
                });
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ReshardTableAutomatic)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();
        auto keepActions = request->keep_actions();

        context->SetRequestInfo("Path: %v, KeepActions: %v",
            path,
            keepActions);

        TReshardTableAutomaticOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_tablet_range_options()) {
            FromProto(&options, request->tablet_range_options());
        }
        options.KeepActions = keepActions;

        ExecuteCall(
            context,
            [=] {
                return client->ReshardTableAutomatic(path, options);
            },
            [] (const auto& context, const auto& tabletActions) {
                auto* response = &context->Response();
                ToProto(response->mutable_tablet_actions(), tabletActions);
                context->SetResponseInfo("TabletActionIds: %v",
                    tabletActions);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, TrimTable)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();
        auto tabletIndex = request->tablet_index();
        auto trimmedRowCount = request->trimmed_row_count();

        TTrimTableOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRequestInfo("Path: %v, TabletIndex: %v, TrimmedRowCount: %v",
            path,
            tabletIndex,
            trimmedRowCount);

        ExecuteCall(
            context,
            [=] {
                return client->TrimTable(
                    path,
                    tabletIndex,
                    trimmedRowCount,
                    options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, AlterTable)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TAlterTableOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_schema()) {
            options.Schema = ConvertTo<TTableSchema>(TYsonString(request->schema()));
        }
        if (request->has_schema_id()) {
            options.SchemaId = FromProto<TMasterTableSchemaId>(request->schema_id());
        }
        if (request->has_dynamic()) {
            options.Dynamic = request->dynamic();
        }
        if (request->has_upstream_replica_id()) {
            options.UpstreamReplicaId = FromProto<TTableReplicaId>(request->upstream_replica_id());
        }
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_schema_modification()) {
            options.SchemaModification = FromProto<ETableSchemaModification>(request->schema_modification());
        }
        if (request->has_replication_progress()) {
            options.ReplicationProgress = FromProto<TReplicationProgress>(request->replication_progress());
        }

        context->SetRequestInfo("Path: %v",
            path);

        ExecuteCall(
            context,
            [=] {
                return client->AlterTable(path, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, AlterTableReplica)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto replicaId = FromProto<TTableReplicaId>(request->replica_id());

        TAlterTableReplicaOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_enabled()) {
            options.Enabled = request->enabled();
        }

        if (request->has_mode()) {
            options.Mode = FromProto<ETableReplicaMode>(request->mode());
        }

        if (request->has_preserve_timestamps()) {
            options.PreserveTimestamps = request->preserve_timestamps();
        }

        if (request->has_atomicity()) {
            options.Atomicity = FromProto<EAtomicity>(request->atomicity());
        }

        if (request->has_enable_replicated_table_tracker()) {
            options.EnableReplicatedTableTracker = request->enable_replicated_table_tracker();
        }

        if (request->has_replica_path()) {
            options.ReplicaPath = request->replica_path();
        }

        context->SetRequestInfo("ReplicaId: %v, Enabled: %v, Mode: %v, Atomicity: %v, PreserveTimestamps: %v, "
            "EnableReplicatedTableTracker: %v, ReplicaPath: %v",
            replicaId,
            options.Enabled,
            options.Mode,
            options.Atomicity,
            options.PreserveTimestamps,
            options.EnableReplicatedTableTracker,
            options.ReplicaPath);

        ExecuteCall(
            context,
            [=] {
                return client->AlterTableReplica(replicaId, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, AlterReplicationCard)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto replicationCardId = FromProto<TReplicationCardId>(request->replication_card_id());

        TAlterReplicationCardOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_replicated_table_options()) {
            options.ReplicatedTableOptions = ConvertTo<TReplicatedTableOptionsPtr>(TYsonString(request->replicated_table_options()));
        }
        if (request->has_enable_replicated_table_tracker()) {
            options.EnableReplicatedTableTracker = request->enable_replicated_table_tracker();
        }
        if (request->has_replication_card_collocation_id()) {
            options.ReplicationCardCollocationId = FromProto<TReplicationCardCollocationId>(request->replication_card_collocation_id());
        }
        if (request->has_collocation_options()) {
            options.CollocationOptions = ConvertTo<TReplicationCollocationOptionsPtr>(TYsonString(request->collocation_options()));
        }

        context->SetRequestInfo("ReplicationCardId: %v",
            replicationCardId);

        ExecuteCall(
            context,
            [=] {
                return client->AlterReplicationCard(replicationCardId, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, BalanceTabletCells)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& bundle = request->bundle();
        auto tables = FromProto<std::vector<NYPath::TYPath>>(request->movable_tables());
        bool keepActions = request->keep_actions();

        context->SetRequestInfo("Bundle: %v, TablePaths: %v, KeepActions: %v",
            bundle,
            tables,
            keepActions);

        TBalanceTabletCellsOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        options.KeepActions = keepActions;

        ExecuteCall(
            context,
            [=] {
                return client->BalanceTabletCells(bundle, tables, options);
            },
            [] (const auto& context, const auto& tabletActions) {
                auto* response = &context->Response();
                ToProto(response->mutable_tablet_actions(), tabletActions);
                context->SetResponseInfo("TabletActionIds: %v",
                    tabletActions);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, CreateTableBackup)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto manifest = New<TBackupManifest>();
        FromProto(manifest.Get(), request->manifest());

        TCreateTableBackupOptions options;
        SetTimeoutOptions(&options, context.Get());
        options.CheckpointTimestampDelay = FromProto<TDuration>(request->checkpoint_timestamp_delay());
        options.CheckpointCheckPeriod = FromProto<TDuration>(request->checkpoint_check_period());
        options.CheckpointCheckTimeout = FromProto<TDuration>(request->checkpoint_check_timeout());
        options.Force = request->force();
        options.PreserveAccount = request->preserve_account();

        context->SetRequestInfo(
            "ClusterCount: %v, CheckpointTimestampDelay: %v, CheckpointCheckPeriod: %v, "
            "CheckpointCheckTimeout: %v, Force: %v, PreserveAccount: %v",
            manifest->Clusters.size(),
            options.CheckpointTimestampDelay,
            options.CheckpointCheckPeriod,
            options.CheckpointCheckTimeout,
            options.Force,
            options.PreserveAccount);

        ExecuteCall(
            context,
            [=] {
                return client->CreateTableBackup(manifest, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, RestoreTableBackup)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto manifest = New<TBackupManifest>();
        FromProto(manifest.Get(), request->manifest());

        TRestoreTableBackupOptions options;
        SetTimeoutOptions(&options, context.Get());
        options.Force = request->force();
        options.Mount = request->mount();
        options.EnableReplicas = request->enable_replicas();
        options.PreserveAccount = request->preserve_account();

        context->SetRequestInfo(
            "ClusterCount: %v, Force: %v, Mount: %v, EnableReplicas: %v, "
            "PreserveAccount: %v",
            manifest->Clusters.size(),
            options.Force,
            options.Mount,
            options.EnableReplicas,
            options.PreserveAccount);

        ExecuteCall(
            context,
            [=] {
                return client->RestoreTableBackup(manifest, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, StartOperation)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto type = NYT::NApi::NRpcProxy::NProto::ConvertOperationTypeFromProto(request->type());
        auto specYson = TYsonString(request->spec());

        {
            auto user = context->GetAuthenticationIdentity().User;
            const auto& config = Config_.Acquire();
            const auto& formatConfigs = config->Formats;
            TFormatManager formatManager(formatConfigs, user);
            auto specNode = ConvertToNode(specYson);
            formatManager.ValidateAndPatchOperationSpec(specNode, type);
            specYson = ConvertToYsonString(specNode);
        }

        TStartOperationOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());

        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }

        context->SetRequestInfo("OperationType: %v, Spec: %v",
            type,
            specYson);

        ExecuteCall(
            context,
            [=] {
                return client->StartOperation(type, specYson, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                context->SetResponseInfo("OperationId: %v", result);
                ToProto(response->mutable_operation_id(), result);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, AbortOperation)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);

        TAbortOperationOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_abort_message()) {
            options.AbortMessage = request->abort_message();
        }

        context->SetRequestInfo("OperationId: %v, AbortMessage: %v",
            operationIdOrAlias,
            options.AbortMessage);

        ExecuteCall(
            context,
            [=] {
                return client->AbortOperation(operationIdOrAlias, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, SuspendOperation)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);

        TSuspendOperationOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_abort_running_jobs()) {
            options.AbortRunningJobs = request->abort_running_jobs();
        }
        if (request->has_reason()) {
            options.Reason = request->reason();
        }

        context->SetRequestInfo("OperationId: %v, AbortRunningJobs: %v",
            operationIdOrAlias,
            options.AbortRunningJobs);

        ExecuteCall(
            context,
            [=] {
                return client->SuspendOperation(operationIdOrAlias, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ResumeOperation)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);

        TResumeOperationOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRequestInfo("OperationId: %v",
            operationIdOrAlias);

        ExecuteCall(
            context,
            [=] {
                return client->ResumeOperation(operationIdOrAlias, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, CompleteOperation)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);

        TCompleteOperationOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRequestInfo("OperationId: %v",
            operationIdOrAlias);

        ExecuteCall(
            context,
            [=] {
                return client->CompleteOperation(operationIdOrAlias, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, UpdateOperationParameters)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);

        auto parameters = TYsonString(request->parameters());

        TUpdateOperationParametersOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRequestInfo("OperationId: %v, Parameters: %v",
            operationIdOrAlias,
            parameters);

        ExecuteCall(
            context,
            [=] {
                return client->UpdateOperationParameters(
                    operationIdOrAlias,
                    parameters,
                    options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, PatchOperationSpec)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);

        TSpecPatchList patches;
        for (const auto& patch : request->patches()) {
            patches.emplace_back(New<TSpecPatch>());
            NScheduler::FromProto(patches.back(), &patch);
        }

        TPatchOperationSpecOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRequestInfo("OperationId: %v, Patches: %v",
            operationIdOrAlias,
            MakeFormattableView(patches, TDefaultFormatter()));

        ExecuteCall(
            context,
            [=] {
                return client->PatchOperationSpec(
                    operationIdOrAlias,
                    patches,
                    options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetOperation)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);

        TGetOperationOptions options;
        SetTimeoutOptions(&options, context.Get());

        if (request->has_master_read_options()) {
            FromProto(&options, request->master_read_options());
        }
        if (request->has_attributes()) {
            options.Attributes.emplace();
            NYT::CheckedHashSetFromProto(&(*options.Attributes), request->attributes().keys());
        } else if (request->legacy_attributes_size() != 0) {
            // COMPAT(max42): remove when no clients older than Aug22 are there.
            options.Attributes.emplace();
            NYT::CheckedHashSetFromProto(&(*options.Attributes), request->legacy_attributes());
        }
        options.IncludeRuntime = request->include_runtime();
        if (request->has_maximum_cypress_progress_age()) {
            options.MaximumCypressProgressAge = FromProto<TDuration>(request->maximum_cypress_progress_age());
        }

        context->SetRequestInfo("OperationId: %v, IncludeRuntime: %v, Attributes: %v",
            operationIdOrAlias,
            options.IncludeRuntime,
            options.Attributes);

        ExecuteCall(
            context,
            [=] {
                return client->GetOperation(operationIdOrAlias, options);
            },
            [] (const auto& context, const auto& operation) {
                auto* response = &context->Response();
                response->set_meta(ConvertToYsonString(operation).ToString());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ListOperations)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        TListOperationsOptions options;
        SetTimeoutOptions(&options, context.Get());

        if (request->has_master_read_options()) {
            FromProto(&options, request->master_read_options());
        }

        if (request->has_from_time()) {
            options.FromTime = FromProto<TInstant>(request->from_time());
        }
        if (request->has_to_time()) {
            options.ToTime = FromProto<TInstant>(request->to_time());
        }
        if (request->has_cursor_time()) {
            options.CursorTime = FromProto<TInstant>(request->cursor_time());
        }
        options.CursorDirection = FromProto<EOperationSortDirection>(request->cursor_direction());
        if (request->has_user_filter()) {
            options.UserFilter = request->user_filter();
        }

        if (request->has_access_filter()) {
            options.AccessFilter = ConvertTo<TListOperationsAccessFilterPtr>(TYsonString(request->access_filter()));
        }

        if (request->has_state_filter()) {
            options.StateFilter = NYT::NApi::NRpcProxy::NProto::ConvertOperationStateFromProto(
                request->state_filter());
        }
        if (request->has_type_filter()) {
            options.TypeFilter = NYT::NApi::NRpcProxy::NProto::ConvertOperationTypeFromProto(
                request->type_filter());
        }
        if (request->has_substr_filter()) {
            options.SubstrFilter = request->substr_filter();
        }
        if (request->has_pool()) {
            options.Pool = request->pool();
        }
        if (request->has_pool_tree()) {
            options.PoolTree = request->pool_tree();
        }
        if (request->has_with_failed_jobs()) {
            options.WithFailedJobs = request->with_failed_jobs();
        }

        options.ArchiveFetchingTimeout = FromProto<TDuration>(request->archive_fetching_timeout());

        options.IncludeArchive = request->include_archive();
        options.IncludeCounters = request->include_counters();
        options.Limit = request->limit();

        if (request->has_attributes()) {
            options.Attributes.emplace();
            FromProto(&(*options.Attributes), request->attributes().keys());
        } else if (request->has_legacy_attributes() && !request->legacy_attributes().all()) {
            // COMPAT(max42): remove when no clients older than Aug22 are there.
            options.Attributes.emplace();
            FromProto(&(*options.Attributes), request->legacy_attributes().keys());
        }

        options.EnableUIMode = request->enable_ui_mode();

        context->SetRequestInfo("IncludeArchive: %v, FromTime: %v, ToTime: %v, CursorTime: %v, UserFilter: %v, "
            "AccessFilter: %v, StateFilter: %v, TypeFilter: %v, SubstrFilter: %v, Attributes: %v",
            options.IncludeArchive,
            options.FromTime,
            options.ToTime,
            options.CursorTime,
            options.UserFilter,
            ConvertToYsonString(options.AccessFilter, EYsonFormat::Text),
            options.StateFilter,
            options.TypeFilter,
            options.SubstrFilter,
            options.Attributes);

        ExecuteCall(
            context,
            [=] {
                return client->ListOperations(options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                ToProto(response->mutable_result(), result);

                context->SetResponseInfo("OperationsCount: %v, FailedJobsCount: %v, Incomplete: %v",
                    result.Operations.size(),
                    result.FailedJobsCount,
                    result.Incomplete);
            });
    }

    ////////////////////////////////////////////////////////////////////////////////
    // JOBS
    ////////////////////////////////////////////////////////////////////////////////

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ListJobs)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);

        TListJobsOptions options;
        SetTimeoutOptions(&options, context.Get());

        if (request->has_master_read_options()) {
            FromProto(&options, request->master_read_options());
        }

        if (request->has_type()) {
            options.Type = NYT::NApi::NRpcProxy::NProto::ConvertJobTypeFromProto(request->type());
        }
        if (request->has_state()) {
            options.State = ConvertJobStateFromProto(request->state());
        }
        if (request->has_address()) {
            options.Address = request->address();
        }
        if (request->has_with_stderr()) {
            options.WithStderr = request->with_stderr();
        }
        if (request->has_with_fail_context()) {
            options.WithFailContext = request->with_fail_context();
        }
        if (request->has_with_spec()) {
            options.WithSpec = request->with_spec();
        }
        if (request->has_with_competitors()) {
            options.WithCompetitors = request->with_competitors();
        }
        if (request->has_job_competition_id()) {
            options.JobCompetitionId = FromProto<TJobId>(request->job_competition_id());
        }
        if (request->has_with_monitoring_descriptor()) {
            options.WithMonitoringDescriptor = request->with_monitoring_descriptor();
        }
        if (request->has_task_name()) {
            options.TaskName = request->task_name();
        }
        if (request->has_operation_incarnation()) {
            options.OperationIncarnation = request->operation_incarnation();
        }
        if (request->has_from_time()) {
            options.FromTime = FromProto<TInstant>(request->from_time());
        }
        if (request->has_to_time()) {
            options.ToTime = FromProto<TInstant>(request->to_time());
        }
        if (request->has_continuation_token()) {
            options.ContinuationToken = request->continuation_token();
        }

        options.SortField = FromProto<EJobSortField>(request->sort_field());
        options.SortOrder = FromProto<EJobSortDirection>(request->sort_order());

        options.Limit = request->limit();
        options.Offset = request->offset();

        options.IncludeCypress = request->include_cypress();
        options.IncludeControllerAgent = request->include_controller_agent();
        options.IncludeArchive = request->include_archive();

        options.DataSource = FromProto<EDataSource>(request->data_source());
        options.RunningJobsLookbehindPeriod = FromProto<TDuration>(request->running_jobs_lookbehind_period());

        context->SetRequestInfo(
            "OperationIdOrAlias: %v, Type: %v, State: %v, Address: %v, IncludeCypress: %v, "
            "IncludeControllerAgent: %v, IncludeArchive: %v, JobCompetitionId: %v, WithCompetitors: %v, WithMonitoringDescriptor: %v",
            operationIdOrAlias,
            options.Type,
            options.State,
            options.Address,
            options.IncludeCypress,
            options.IncludeControllerAgent,
            options.IncludeArchive,
            options.JobCompetitionId,
            options.WithCompetitors,
            options.WithMonitoringDescriptor);

        ExecuteCall(
            context,
            [=] {
                return client->ListJobs(operationIdOrAlias, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                ToProto(response->mutable_result(), result);

                context->SetResponseInfo(
                    "CypressJobCount: %v, ControllerAgentJobCount: %v, ArchiveJobCount: %v",
                    result.CypressJobCount,
                    result.ControllerAgentJobCount,
                    result.ArchiveJobCount);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, DumpJobContext)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto jobId = FromProto<TJobId>(request->job_id());
        auto path = request->path();

        TDumpJobContextOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRequestInfo("JobId: %v, Path: %v",
            jobId,
            path);

        ExecuteCall(
            context,
            [=] {
                return client->DumpJobContext(jobId, path, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetJobInput)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto jobId = FromProto<TJobId>(request->job_id());

        TGetJobInputOptions options;
        SetTimeoutOptions(&options, context.Get());

        options.JobSpecSource = FromProto<EJobSpecSource>(request->job_spec_source());

        context->SetRequestInfo("JobId: %v", jobId);

        auto jobInputReader = WaitFor(client->GetJobInput(jobId, options))
            .ValueOrThrow();
        HandleInputStreamingRequest(context, jobInputReader);
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetJobInputPaths)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto jobId = FromProto<TJobId>(request->job_id());

        TGetJobInputPathsOptions options;
        SetTimeoutOptions(&options, context.Get());

        options.JobSpecSource = FromProto<EJobSpecSource>(request->job_spec_source());

        context->SetRequestInfo("JobId: %v", jobId);

        ExecuteCall(
            context,
            [=] {
                return client->GetJobInputPaths(jobId, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->set_paths(result.ToString());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetJobSpec)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto jobId = FromProto<TJobId>(request->job_id());

        TGetJobSpecOptions options;
        SetTimeoutOptions(&options, context.Get());

        options.OmitNodeDirectory = request->omit_node_directory();
        options.OmitInputTableSpecs = request->omit_input_table_specs();
        options.OmitOutputTableSpecs = request->omit_output_table_specs();
        options.JobSpecSource = FromProto<EJobSpecSource>(request->job_spec_source());

        context->SetRequestInfo("JobId: %v, OmitNodeDirectory: %v, OmitInputTableSpecs: %v, OmitOutputTableSpecs: %v",
            jobId,
            options.OmitNodeDirectory,
            options.OmitInputTableSpecs,
            options.OmitOutputTableSpecs);

        ExecuteCall(
            context,
            [=] {
                return client->GetJobSpec(jobId, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->set_job_spec(result.ToString());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetJobStderr)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);
        auto jobId = FromProto<TJobId>(request->job_id());

        TGetJobStderrOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRequestInfo("OperationIdOrAlias: %v, JobId: %v, Limit: %v, Offset: %v",
            operationIdOrAlias,
            jobId,
            options.Limit,
            options.Offset);

        ExecuteCall(
            context,
            [=] {
                return client->GetJobStderr(operationIdOrAlias, jobId, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                context->SetResponseInfo("Size: %v, TotalSize: %v, EndOffset: %v", result.Data.size(), result.TotalSize, result.EndOffset);
                response->set_total_size(result.TotalSize);
                response->set_end_offset(result.EndOffset);
                response->Attachments().push_back(result.Data);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetJobTrace)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);

        TGetJobTraceOptions options;
        SetTimeoutOptions(&options, context.Get());

        if (request->has_job_id()) {
            options.JobId = FromProto<TJobId>(request->job_id());
        }
        if (request->has_trace_id()) {
            options.TraceId = FromProto<NScheduler::TJobTraceId>(request->trace_id());
        }
        if (request->has_from_event_index()) {
            options.FromEventIndex = request->from_event_index();
        }
        if (request->has_to_event_index()) {
            options.ToEventIndex = request->to_event_index();
        }
        if (request->has_from_time()) {
            options.FromTime = request->from_time();
        }
        if (request->has_to_event_index()) {
            options.ToTime = request->to_time();
        }

        context->SetRequestInfo("OperationIdOrAlias: %v", operationIdOrAlias);

        ExecuteCall(
            context,
            [=] {
                return client->GetJobTrace(operationIdOrAlias, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                ToProto(response->mutable_events(), result);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetJobFailContext)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);
        auto jobId = FromProto<TJobId>(request->job_id());

        TGetJobFailContextOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRequestInfo("OperationIdOrAlias: %v, JobId: %v",
            operationIdOrAlias,
            jobId);

        ExecuteCall(
            context,
            [=] {
                return client->GetJobFailContext(operationIdOrAlias, jobId, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->Attachments().push_back(std::move(result));
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetJob)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);
        auto jobId = FromProto<TJobId>(request->job_id());

        TGetJobOptions options;
        SetTimeoutOptions(&options, context.Get());

        if (request->has_attributes()) {
            options.Attributes.emplace();
            FromProto(&(*options.Attributes), request->attributes().keys());
        } else if (request->has_legacy_attributes() && !request->legacy_attributes().all()) {
            // COMPAT(max42): remove when no clients older than Aug22 are there.
            options.Attributes.emplace();
            FromProto(&(*options.Attributes), request->legacy_attributes().keys());
        }

        context->SetRequestInfo("OperationIdOrAlias: %v, JobId: %v, Attributes: %v",
            operationIdOrAlias,
            jobId,
            options.Attributes);

        ExecuteCall(
            context,
            [=] {
                return client->GetJob(operationIdOrAlias, jobId, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->set_info(result.ToString());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, AbandonJob)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto jobId = FromProto<TJobId>(request->job_id());
        TAbandonJobOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRequestInfo("JobId: %v", jobId);

        ExecuteCall(
            context,
            [=] {
                return client->AbandonJob(jobId, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, PollJobShell)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto jobId = FromProto<TJobId>(request->job_id());
        auto parameters = TYsonString(request->parameters());
        auto shellName = request->has_shell_name()
            ? std::make_optional(request->shell_name())
            : std::nullopt;

        TPollJobShellOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRequestInfo("JobId: %v, Parameters: %v, ShellName: %v",
            jobId,
            parameters,
            shellName);

        ExecuteCall(
            context,
            [=] {
                return client->PollJobShell(jobId, shellName, parameters, options);
            },
            [] (const auto& context, const auto& pollJobShellResponse) {
                auto* response = &context->Response();
                response->set_result(pollJobShellResponse.Result.ToString());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, AbortJob)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto jobId = FromProto<TJobId>(request->job_id());

        TAbortJobOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_interrupt_timeout()) {
            options.InterruptTimeout = FromProto<TDuration>(request->interrupt_timeout());
        }

        context->SetRequestInfo("JobId: %v, InterruptTimeout: %v",
            jobId,
            options.InterruptTimeout);

        ExecuteCall(
            context,
            [=] {
                return client->AbortJob(jobId, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, DumpJobProxyLog)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto jobId = FromProto<TJobId>(request->job_id());
        auto operationId = FromProto<TOperationId>(request->operation_id());
        auto path = FromProto<TYPath>(request->path());

        TDumpJobProxyLogOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRequestInfo("JobId: %v, Path: %v", jobId, path);

        ExecuteCall(
            context,
            [=] {
                return client->DumpJobProxyLog(jobId, operationId, path, options);
            });
    }

    ////////////////////////////////////////////////////////////////////////////////
    // TABLES (TRANSACTIONAL)
    ////////////////////////////////////////////////////////////////////////////////

    template <class TContext, class TRequest, class TOptions>
    static void LookupRowsPrelude(
        const TIntrusivePtr<TContext>& context,
        const TRequest* request,
        TOptions* options)
    {
        if (request->has_tablet_read_options()) {
            FromProto(options, request->tablet_read_options());
        }
        if (request->has_replica_consistency()) {
            options->ReplicaConsistency = FromProto<EReplicaConsistency>(request->replica_consistency());
        }

        SetTimeoutOptions(options, context.Get());

        options->Timestamp = request->timestamp();

        if constexpr (requires { request->retention_timestamp(); }) {
            options->RetentionTimestamp = request->retention_timestamp();
        }

        if (request->has_multiplexing_band()) {
            options->MultiplexingBand = FromProto<EMultiplexingBand>(request->multiplexing_band());
        }
    }

    template <class TContext, class TRequest>
    static void LookupRowsPrologue(
        const TIntrusivePtr<TContext>& /*context*/,
        const TRequest* request,
        TNameTablePtr* nameTable,
        TSharedRange<TUnversionedRow>* keys,
        TLookupRequestOptions* options,
        const std::vector<TSharedRef>& attachments,
        const IMemoryUsageTrackerPtr& memoryTracker)
    {
        if (attachments.empty()) {
            THROW_ERROR_EXCEPTION("Request is missing rowset in attachments");
        }

        struct TDeserializedRowsetTag { };
        auto rowBuffer = New<TRowBuffer>(TDeserializedRowsetTag());

        auto rowset = NApi::NRpcProxy::DeserializeRowset<TUnversionedRow>(
            request->rowset_descriptor(),
            MergeRefsToRef<TApiServiceBufferTag>(attachments),
            rowBuffer);

        auto guard = TMemoryUsageTrackerGuard::TryAcquire(memoryTracker, rowBuffer->GetCapacity())
            .ValueOrThrow();

        *nameTable = rowset->GetNameTable();
        *keys = MakeSharedRange(rowset->GetRows(), MakeSharedRangeHolder(rowset, std::move(guard)));

        TColumnFilter::TIndexes columnFilterIndexes;
        for (int i = 0; i < request->columns_size(); ++i) {
            columnFilterIndexes.push_back((*nameTable)->GetIdOrRegisterName(request->columns(i)));
        }
        options->ColumnFilter = request->columns_size() == 0
            ? TColumnFilter()
            : TColumnFilter(std::move(columnFilterIndexes));
        options->KeepMissingRows = request->keep_missing_rows();
        options->EnablePartialResult = request->enable_partial_result();
        if (request->has_use_lookup_cache()) {
            options->UseLookupCache = request->use_lookup_cache();
        }
        if constexpr (requires {request->has_versioned_read_options();}) {
            FromProto(&options->VersionedReadOptions, request->versioned_read_options());
        }
    }

    void ProcessLookupRowsDetailedProfilingInfo(
        TWallTimer timer,
        const std::string& userTag,
        const TDetailedProfilingInfoPtr& detailedProfilingInfo)
    {
        TDetailedProfilingCountersPtr counters;
        if (detailedProfilingInfo->EnableDetailedTableProfiling) {
            counters = GetOrCreateDetailedProfilingCounters({
                .UserTag = userTag,
                .TablePath = detailedProfilingInfo->TablePath,
            });
            counters->LookupDurationTimer().Record(timer.GetElapsedTime());
            counters->LookupMountCacheWaitTimer().Record(detailedProfilingInfo->MountCacheWaitTime);
            counters->LookupPermissionCacheWaitTimer().Record(detailedProfilingInfo->PermissionCacheWaitTime);
        } else if (!detailedProfilingInfo->RetryReasons.empty() ||
            detailedProfilingInfo->WastedSubrequestCount > 0)
        {
            counters = GetOrCreateDetailedProfilingCounters({});
        }

        if (detailedProfilingInfo->WastedSubrequestCount > 0) {
            counters->WastedLookupSubrequestCount().Increment(detailedProfilingInfo->WastedSubrequestCount);
        }

        for (const auto& reason : detailedProfilingInfo->RetryReasons) {
            counters->GetRetryCounterByReason(reason)->Increment();
        }
    }

    void ProcessSelectRowsDetailedProfilingInfo(
        TWallTimer timer,
        const std::string& userTag,
        const TDetailedProfilingInfoPtr& detailedProfilingInfo)
    {
        TDetailedProfilingCountersPtr counters;
        if (detailedProfilingInfo->EnableDetailedTableProfiling) {
            counters = GetOrCreateDetailedProfilingCounters({
                .UserTag = userTag,
                .TablePath = detailedProfilingInfo->TablePath,
            });
            counters->SelectDurationTimer().Record(timer.GetElapsedTime());
            counters->SelectMountCacheWaitTimer().Record(detailedProfilingInfo->MountCacheWaitTime);
            counters->SelectPermissionCacheWaitTimer().Record(detailedProfilingInfo->PermissionCacheWaitTime);
        } else if (!detailedProfilingInfo->RetryReasons.empty()) {
            counters = GetOrCreateDetailedProfilingCounters({});
        }

        for (const auto& reason : detailedProfilingInfo->RetryReasons) {
            counters->GetRetryCounterByReason(reason)->Increment();
        }
    }

    template <class TResponse, class TRow>
    static std::vector<TSharedRef> PrepareRowsetForAttachment(
        TResponse* response,
        const TIntrusivePtr<IRowset<TRow>>& rowset)
    {
        return NApi::NRpcProxy::SerializeRowset(
            *rowset->GetSchema(),
            rowset->GetRows(),
            response->mutable_rowset_descriptor());
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, LookupRows)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        TWallTimer timer;

        const auto& path = request->path();

        TNameTablePtr nameTable;
        TSharedRange<TUnversionedRow> keys;

        TLookupRowsOptions options;
        auto detailedProfilingInfo = New<TDetailedProfilingInfo>();
        options.DetailedProfilingInfo = detailedProfilingInfo;

        LookupRowsPrelude(
            context,
            request,
            &options);
        LookupRowsPrologue(
            context,
            request,
            &nameTable,
            &keys,
            &options,
            request->Attachments(),
            HeavyRequestMemoryUsageTracker_);

        context->SetRequestInfo("Path: %v, RowCount: %v, Timestamp: %v, ReplicaConsistency: %v",
            request->path(),
            keys.Size(),
            options.Timestamp,
            options.ReplicaConsistency);
        NTracing::AnnotateTraceContext([&] (const auto& traceContext) {
            traceContext->AddTag("yt.table_path", path);
        });

        ExecuteCall(
            context,
            [=] {
                return client->LookupRows(
                    path,
                    std::move(nameTable),
                    std::move(keys),
                    options);
            },
            [=, this, this_ = MakeStrong(this), detailedProfilingInfo = std::move(detailedProfilingInfo)]
            (const auto& context, const auto& result) {
                const auto& rowset = result.Rowset;

                auto* response = &context->Response();
                ToProto(response->mutable_unavailable_key_indexes(), result.UnavailableKeyIndexes);
                response->Attachments() = PrepareRowsetForAttachment(response, rowset);

                ProcessLookupRowsDetailedProfilingInfo(
                    timer,
                    context->GetAuthenticationIdentity().UserTag,
                    detailedProfilingInfo);

                context->SetResponseInfo("RowCount: %v, DetailedTableProfilingEnabled: %v",
                    rowset->GetRows().Size(),
                    detailedProfilingInfo->EnableDetailedTableProfiling);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, VersionedLookupRows)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        TWallTimer timer;

        const auto& path = request->path();

        TNameTablePtr nameTable;
        TSharedRange<TUnversionedRow> keys;

        TVersionedLookupRowsOptions options;
        auto detailedProfilingInfo = New<TDetailedProfilingInfo>();
        options.DetailedProfilingInfo = detailedProfilingInfo;

        LookupRowsPrelude(
            context,
            request,
            &options);
        LookupRowsPrologue(
            context,
            request,
            &nameTable,
            &keys,
            &options,
            request->Attachments(),
            HeavyRequestMemoryUsageTracker_);

        context->SetRequestInfo("Path: %v, RowCount: %v, Timestamp: %v, ReplicaConsistency: %v",
            request->path(),
            keys.Size(),
            options.Timestamp,
            options.ReplicaConsistency);
        NTracing::AnnotateTraceContext([&] (const auto& traceContext) {
            traceContext->AddTag("yt.table_path", path);
        });

        if (request->has_retention_config()) {
            options.RetentionConfig = New<TRetentionConfig>();
            FromProto(options.RetentionConfig.Get(), request->retention_config());
        }

        ExecuteCall(
            context,
            [=] {
                return client->VersionedLookupRows(
                    path,
                    std::move(nameTable),
                    std::move(keys),
                    options);
            },
            [=, this, this_ = MakeStrong(this), detailedProfilingInfo = std::move(detailedProfilingInfo)]
            (const auto& context, const auto& result) {
                const auto& rowset = result.Rowset;

                auto* response = &context->Response();
                ToProto(response->mutable_unavailable_key_indexes(), result.UnavailableKeyIndexes);
                response->Attachments() = PrepareRowsetForAttachment(response, rowset);

                ProcessLookupRowsDetailedProfilingInfo(
                    timer,
                    context->GetAuthenticationIdentity().UserTag,
                    detailedProfilingInfo);

                context->SetResponseInfo("RowCount: %v, EnableDetailedTableProfiling: %v",
                    rowset->GetRows().Size(),
                    detailedProfilingInfo->EnableDetailedTableProfiling);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, MultiLookup)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        TWallTimer timer;

        int subrequestCount = request->subrequests_size();

        TMultiLookupOptions options;
        LookupRowsPrelude(
            context,
            request,
            &options);

        std::vector<TMultiLookupSubrequest> subrequests;
        std::vector<TDetailedProfilingInfoPtr> profilingInfos;
        subrequests.reserve(subrequestCount);
        profilingInfos.reserve(subrequestCount);

        int beginAttachmentIndex = 0;
        for (int i = 0; i < subrequestCount; ++i) {
            const auto& protoSubrequest = request->subrequests(i);

            auto& subrequest = subrequests.emplace_back();
            subrequest.Path = protoSubrequest.path();

            profilingInfos.push_back(New<TDetailedProfilingInfo>());
            subrequest.Options.DetailedProfilingInfo = profilingInfos.back();

            int endAttachmentIndex = beginAttachmentIndex + protoSubrequest.attachment_count();
            if (endAttachmentIndex > std::ssize(request->Attachments())) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::ProtocolError,
                    "Subrequest %v refers to non-existing attachment %v (out of %v)",
                    i,
                    endAttachmentIndex,
                    request->Attachments().size());
            }
            std::vector<TSharedRef> attachments{
                request->Attachments().begin() + beginAttachmentIndex,
                request->Attachments().begin() + endAttachmentIndex};

            LookupRowsPrologue(
                context,
                &protoSubrequest,
                &subrequest.NameTable,
                &subrequest.Keys,
                &subrequest.Options,
                attachments,
                HeavyRequestMemoryUsageTracker_);

            beginAttachmentIndex = endAttachmentIndex;
        }
        if (beginAttachmentIndex != std::ssize(request->Attachments())) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::ProtocolError,
                "Total number of attachments is too large: expected %v, actual %v",
                beginAttachmentIndex,
                request->Attachments().size());
        }

        context->SetRequestInfo("Timestamp: %v, ReplicaConsistency: %v, Subrequests: %v",
            options.Timestamp,
            options.ReplicaConsistency,
            MakeFormattableView(
                subrequests,
                [&] (auto* builder, const TMultiLookupSubrequest& request) {
                    builder->AppendFormat("{Path: %v, RowCount: %v}",
                        request.Path,
                        request.Keys.Size());
                }));
        NTracing::AnnotateTraceContext([&] (const auto& traceContext) {
            for (const auto& subrequest : subrequests) {
                traceContext->AddTag("yt.table_path", subrequest.Path);
            }
        });

        ExecuteCall(
            context,
            [=] {
                return client->MultiLookupRows(
                    std::move(subrequests),
                    std::move(options));
            },
            [=, this, this_ = MakeStrong(this), profilingInfos = std::move(profilingInfos)]
            (const auto& context, const auto& results) {
                auto* response = &context->Response();

                YT_VERIFY(subrequestCount == std::ssize(results));

                std::vector<int> rowCounts;
                rowCounts.reserve(subrequestCount);
                for (const auto& result : results) {
                    const auto& rowset = result.Rowset;
                    auto* subresponse = response->add_subresponses();
                    auto attachments = PrepareRowsetForAttachment(subresponse, rowset);
                    subresponse->set_attachment_count(attachments.size());
                    ToProto(subresponse->mutable_unavailable_key_indexes(), result.UnavailableKeyIndexes);
                    response->Attachments().insert(
                        response->Attachments().end(),
                        attachments.begin(),
                        attachments.end());
                    rowCounts.push_back(rowset->GetRows().Size());
                }

                for (const auto& detailedProfilingInfo : profilingInfos) {
                    ProcessLookupRowsDetailedProfilingInfo(
                        timer,
                        context->GetAuthenticationIdentity().UserTag,
                        detailedProfilingInfo);
                }

                context->SetResponseInfo("RowCounts: %v",
                    rowCounts);
            });
    }

    template <class TRequest>
    static void FillSelectRowsOptionsBaseFromRequest(const TRequest request, TSelectRowsOptionsBase* options)
    {
        if (request->has_timestamp()) {
            options->Timestamp = request->timestamp();
        }
        if (request->has_udf_registry_path()) {
            options->UdfRegistryPath = request->udf_registry_path();
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, SelectRows)
    {
        TWallTimer timer;

        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& query = request->query();

        TSelectRowsOptions options;
        SetTimeoutOptions(&options, context.Get());
        FillSelectRowsOptionsBaseFromRequest(request, &options);

        if (request->has_input_row_limit()) {
            options.InputRowLimit = request->input_row_limit();
        }
        if (request->has_output_row_limit()) {
            options.OutputRowLimit = request->output_row_limit();
        }
        if (request->has_range_expansion_limit()) {
            options.RangeExpansionLimit = request->range_expansion_limit();
        }
        if (request->has_max_subqueries()) {
            options.MaxSubqueries = request->max_subqueries();
        }
        if (request->has_allow_full_scan()) {
            options.AllowFullScan = request->allow_full_scan();
        }
        if (request->has_allow_join_without_index()) {
            options.AllowJoinWithoutIndex = request->allow_join_without_index();
        }
        if (request->has_execution_pool()) {
            options.ExecutionPool = request->execution_pool();
        }
        if (request->has_fail_on_incomplete_result()) {
            options.FailOnIncompleteResult = request->fail_on_incomplete_result();
        }
        if (request->has_verbose_logging()) {
            options.VerboseLogging = request->verbose_logging();
        }
        if (request->has_new_range_inference()) {
            options.NewRangeInference = request->new_range_inference();
        }
        if (request->has_enable_code_cache()) {
            options.EnableCodeCache = request->enable_code_cache();
        }
        if (request->has_retention_timestamp()) {
            options.RetentionTimestamp = request->retention_timestamp();
        }
        // TODO: Support WorkloadDescriptor
        if (request->has_memory_limit_per_node()) {
            options.MemoryLimitPerNode = request->memory_limit_per_node();
        }
        // TODO(lukyan): Move to FillSelectRowsOptionsBaseFromRequest
        if (request->has_suppressable_access_tracking_options()) {
            FromProto(&options, request->suppressable_access_tracking_options());
        }
        if (request->has_replica_consistency()) {
            options.ReplicaConsistency = FromProto<EReplicaConsistency>(request->replica_consistency());
        }
        if (request->has_placeholder_values()) {
            options.PlaceholderValues = NYson::TYsonString(request->placeholder_values());
        }
        if (request->has_use_canonical_null_relations()) {
            options.UseCanonicalNullRelations = request->use_canonical_null_relations();
        }
        if (request->has_merge_versioned_rows()) {
            options.MergeVersionedRows = request->merge_versioned_rows();
        }
        if (request->has_syntax_version()) {
            options.SyntaxVersion = request->syntax_version();
        }
        if (request->has_execution_backend()) {
            options.ExecutionBackend = CheckedEnumCast<EExecutionBackend>(request->execution_backend());
        }
        if (request->has_versioned_read_options()) {
            FromProto(&options.VersionedReadOptions, request->versioned_read_options());
        }
        if (request->has_use_lookup_cache()) {
            options.UseLookupCache = request->use_lookup_cache();
        }
        if (request->has_min_row_count_per_subquery()) {
            options.MinRowCountPerSubquery = request->min_row_count_per_subquery();
        }

        auto detailedProfilingInfo = New<TDetailedProfilingInfo>();
        options.DetailedProfilingInfo = detailedProfilingInfo;
        if (options.PlaceholderValues) {
            context->SetRequestInfo("Query: %v, Timestamp: %v, PlaceholderValues: %v",
                query,
                options.Timestamp,
                options.PlaceholderValues);
        } else {
            context->SetRequestInfo("Query: %v, Timestamp: %v",
                query,
                options.Timestamp);
        }

        ExecuteCall(
            context,
            [=] {
                return client->SelectRows(query, options);
            },
            [=, this, this_ = MakeStrong(this), detailedProfilingInfo = std::move(detailedProfilingInfo)]
            (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->Attachments() = PrepareRowsetForAttachment(response, result.Rowset);
                ToProto(response->mutable_statistics(), result.Statistics);

                ProcessSelectRowsDetailedProfilingInfo(
                    timer,
                    context->GetAuthenticationIdentity().UserTag,
                    detailedProfilingInfo);

                auto rows = result.Rowset->GetRows();

                context->SetResponseInfo("RowCount: %v", rows.Size());

                SelectConsumeDataWeight_.Increment(result.Statistics.DataWeightRead);
                SelectConsumeRowCount_.Increment(result.Statistics.RowsRead);
                SelectOutputDataWeight_.Increment(GetDataWeight(rows));
                SelectOutputRowCount_.Increment(rows.Size());

                if (QueryCorpusReporter_) {
                    QueryCorpusReporter_->AddQuery(query);
                }
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, PullRows)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);
        const auto& path = request->path();

        TPullRowsOptions options;
        FromProto(&options.UpstreamReplicaId, request->upstream_replica_id());
        options.TabletRowsPerRead = request->tablet_rows_per_read();
        options.OrderRowsByTimestamp = request->order_rows_by_timestamp();
        FromProto(&options.ReplicationProgress, request->replication_progress());
        if (request->has_upper_timestamp()) {
            options.UpperTimestamp = request->upper_timestamp();
        }
        for (auto protoReplicationRowIndex : request->start_replication_row_indexes()){
            auto tabletId = FromProto<TTabletId>(protoReplicationRowIndex.tablet_id());
            int rowIndex = protoReplicationRowIndex.row_index();
            if (options.StartReplicationRowIndexes.contains(tabletId)) {
                THROW_ERROR_EXCEPTION("Duplicate tablet id in start replication row indexes")
                    << TErrorAttribute("tablet_id", tabletId);
            }
            InsertOrCrash(options.StartReplicationRowIndexes, std::pair(tabletId, rowIndex));
        }

        context->SetRequestInfo("ReplicationProgress: %v, OrderRowsByTimestamp: %v, UpperTimestamp: %v",
            options.ReplicationProgress,
            options.OrderRowsByTimestamp,
            options.UpperTimestamp);

        ExecuteCall(
            context,
            [=] {
                return client->PullRows(path, options);
            },
            [=] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->set_row_count(result.RowCount);
                response->set_data_weight(result.DataWeight);
                response->set_versioned(result.Versioned);
                ToProto(response->mutable_replication_progress(), result.ReplicationProgress);

                for (auto [tabletId, rowIndex] : result.EndReplicationRowIndexes) {
                    auto* protoReplicationRowIndex = response->add_end_replication_row_indexes();
                    ToProto(protoReplicationRowIndex->mutable_tablet_id(), tabletId);
                    protoReplicationRowIndex->set_row_index(rowIndex);
                }

                response->Attachments() = NApi::NRpcProxy::SerializeRowset(
                    *result.Rowset->GetSchema(),
                    result.Rowset->GetRows(),
                    response->mutable_rowset_descriptor(),
                    result.Versioned);

                context->SetResponseInfo("RowCount: %v",
                    result.RowCount);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ExplainQuery)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& query = request->query();

        TExplainQueryOptions options;
        SetTimeoutOptions(&options, context.Get());
        FillSelectRowsOptionsBaseFromRequest(request, &options);

        if (request->has_new_range_inference()) {
            options.NewRangeInference = request->new_range_inference();
        }

        if (request->has_syntax_version()) {
            options.SyntaxVersion = request->syntax_version();
        }

        context->SetRequestInfo("Query: %v, Timestamp: %v",
            query,
            options.Timestamp);

        ExecuteCall(
            context,
            [=] {
                return client->ExplainQuery(query, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->set_value(result.ToString());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetInSyncReplicas)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TGetInSyncReplicasOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_timestamp()) {
            options.Timestamp = request->timestamp();
        }

        if (request->has_cached_sync_replicas_timeout()) {
            options.CachedSyncReplicasTimeout = FromProto<TDuration>(request->cached_sync_replicas_timeout());
        }

        auto rowset = request->has_rowset_descriptor()
            ? NApi::NRpcProxy::DeserializeRowset<TUnversionedRow>(
                request->rowset_descriptor(),
                MergeRefsToRef<TApiServiceBufferTag>(request->Attachments()))
            : nullptr;
        auto keyCount = rowset
            ? std::make_optional(rowset->GetRows().Size())
            : std::nullopt;

        context->SetRequestInfo("Path: %v, Timestamp: %v, KeyCount: %v",
            path,
            options.Timestamp,
            keyCount);

        ExecuteCall(
            context,
            [=] {
                return rowset
                    ? client->GetInSyncReplicas(
                        path,
                        rowset->GetNameTable(),
                        MakeSharedRange(rowset->GetRows(), rowset),
                        options)
                    : client->GetInSyncReplicas(
                        path,
                        options);
            },
            [] (const auto& context, const std::vector<TTableReplicaId>& replicaIds) {
                auto* response = &context->Response();
                ToProto(response->mutable_replica_ids(), replicaIds);

                context->SetResponseInfo("ReplicaIds: %v",
                    replicaIds);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetTabletInfos)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();
        auto tabletIndexes = FromProto<std::vector<int>>(request->tablet_indexes());

        context->SetRequestInfo("Path: %v, TabletIndexes: %v",
            path,
            tabletIndexes);

        TGetTabletInfosOptions options;
        SetTimeoutOptions(&options, context.Get());
        options.RequestErrors = request->request_errors();

        ExecuteCall(
            context,
            [=] {
                return client->GetTabletInfos(
                    path,
                    tabletIndexes,
                    options);
            },
            [] (const auto& context, const auto& tabletInfos) {
                auto* response = &context->Response();
                for (const auto& tabletInfo : tabletInfos) {
                    auto* protoTabletInfo = response->add_tablets();
                    protoTabletInfo->set_total_row_count(tabletInfo.TotalRowCount);
                    protoTabletInfo->set_trimmed_row_count(tabletInfo.TrimmedRowCount);
                    protoTabletInfo->set_delayed_lockless_row_count(tabletInfo.DelayedLocklessRowCount);
                    protoTabletInfo->set_barrier_timestamp(tabletInfo.BarrierTimestamp);
                    protoTabletInfo->set_last_write_timestamp(tabletInfo.LastWriteTimestamp);
                    ToProto(protoTabletInfo->mutable_tablet_errors(), tabletInfo.TabletErrors);

                    if (tabletInfo.TableReplicaInfos) {
                        for (const auto& replicaInfo : *tabletInfo.TableReplicaInfos) {
                            auto* protoReplicaInfo = protoTabletInfo->add_replicas();
                            ToProto(protoReplicaInfo->mutable_replica_id(), replicaInfo.ReplicaId);
                            protoReplicaInfo->set_last_replication_timestamp(replicaInfo.LastReplicationTimestamp);
                            protoReplicaInfo->set_mode(static_cast<NApi::NRpcProxy::NProto::ETableReplicaMode>(replicaInfo.Mode));
                            protoReplicaInfo->set_current_replication_row_index(replicaInfo.CurrentReplicationRowIndex);
                            protoReplicaInfo->set_committed_replication_row_index(replicaInfo.CommittedReplicationRowIndex);
                            ToProto(protoReplicaInfo->mutable_replication_error(), replicaInfo.ReplicationError);
                        }
                    }
                }
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetTabletErrors)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();
        context->SetRequestInfo("Path: %v", path);

        TGetTabletErrorsOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_limit()) {
            options.Limit = request->limit();
        }

        ExecuteCall(
            context,
            [=] {
                return client->GetTabletErrors(
                    path,
                    options);
            },
            [] (const auto& context, const auto& tabletErrors) {
                auto* response = &context->Response();
                for (const auto& [tabletId, errors] : tabletErrors.TabletErrors) {
                    ToProto(response->add_tablet_ids(), tabletId);
                    ToProto(response->add_tablet_errors()->mutable_errors(), errors);
                }

                for (const auto& [replicaId, errors] : tabletErrors.ReplicationErrors) {
                    ToProto(response->add_replica_ids(), replicaId);
                    ToProto(response->add_replication_errors()->mutable_errors(), errors);
                }
                if (tabletErrors.Incomplete) {
                    response->set_incomplete(tabletErrors.Incomplete);
                }
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, PushQueueProducer)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        auto producerPath = FromProto<TRichYPath>(request->producer_path());
        auto queuePath = FromProto<TRichYPath>(request->queue_path());

        auto sessionId = FromProto<TQueueProducerSessionId>(request->session_id());

        TPushQueueProducerOptions options;
        SetTimeoutOptions(&options, context.Get());
        options.SequenceNumber = YT_PROTO_OPTIONAL(*request, sequence_number, TQueueProducerSequenceNumber);
        if (request->has_require_sync_replica()) {
            options.RequireSyncReplica = request->require_sync_replica();
        }

        if (request->has_user_meta()) {
            options.UserMeta = ConvertToNode(TYsonStringBuf(request->user_meta()));
        }

        context->SetRequestInfo(
            "ProducerPath: %v, QueuePath: %v, SessionId: %v, "
            "Epoch: %v, RequireSyncReplica: %v, TransactionId: %v",
            producerPath,
            queuePath,
            sessionId,
            request->epoch(),
            options.RequireSyncReplica,
            transactionId);

        auto transaction = GetTransactionOrThrow(
            context,
            request,
            transactionId,
            /*options*/ std::nullopt,
            /*searchInPool*/ true);

        auto rowset = NApi::NRpcProxy::DeserializeRowset<TUnversionedRow>(
            request->rowset_descriptor(),
            MergeRefsToRef<TApiServiceBufferTag>(request->Attachments()));

        ExecuteCall(
            context,
            [
                =,
                producerPath = std::move(producerPath),
                queuePath = std::move(queuePath),
                sessionId = std::move(sessionId),
                rowset = std::move(rowset),
                options = std::move(options)
            ] {
                auto rowsetRows = rowset->GetRows();

                return transaction->PushQueueProducer(
                    producerPath,
                    queuePath,
                    sessionId,
                    FromProto<TQueueProducerEpoch>(request->epoch()),
                    rowset->GetNameTable(),
                    rowsetRows,
                    options);
            },
            [] (const auto& context, const auto& pushQueueProducerResult) {
                auto* response = &context->Response();
                response->set_last_sequence_number(pushQueueProducerResult.LastSequenceNumber.Underlying());
                response->set_skipped_row_count(pushQueueProducerResult.SkippedRowCount);

                context->SetResponseInfo(
                    "LastSequenceNumber: %v, SkippedRowCount: %v",
                    pushQueueProducerResult.LastSequenceNumber.Underlying(),
                    pushQueueProducerResult.SkippedRowCount);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, AdvanceQueueConsumer)
    {
        AdvanceQueueConsumerImpl(request, response, context);
    }

    DECLARE_RPC_SERVICE_METHOD_VIA_MESSAGES(
        NApi::NRpcProxy::NProto::TReqAdvanceQueueConsumer,
        NApi::NRpcProxy::NProto::TRspAdvanceQueueConsumer,
        AdvanceConsumer)
    {
        AdvanceQueueConsumerImpl(request, response, context);
    }

    void AdvanceQueueConsumerImpl(
        NApi::NRpcProxy::NProto::TReqAdvanceQueueConsumer* request,
        NApi::NRpcProxy::NProto::TRspAdvanceQueueConsumer* /*response*/,
        const TCtxAdvanceQueueConsumerPtr& context)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        auto consumerPath = FromProto<TRichYPath>(request->consumer_path());
        auto queuePath = FromProto<TRichYPath>(request->queue_path());

        TAdvanceQueueConsumerOptions options;
        SetTimeoutOptions(&options, context.Get());

        auto oldOffset = YT_PROTO_OPTIONAL(*request, old_offset);
        context->SetRequestInfo(
            "ConsumerPath: %v, QueuePath: %v, PartitionIndex: %v, "
            "OldOffset: %v, NewOffset: %v, TransactionId: %v",
            consumerPath,
            queuePath,
            request->partition_index(),
            oldOffset,
            request->new_offset(),
            transactionId);

        auto transaction = GetTransactionOrThrow(
            context,
            request,
            transactionId,
            /*options*/ std::nullopt,
            /*searchInPool*/ true);

        ExecuteCall(
            context,
            [=, consumerPath = std::move(consumerPath), queuePath = std::move(queuePath)] {
                return transaction->AdvanceQueueConsumer(
                    consumerPath,
                    queuePath,
                    request->partition_index(),
                    oldOffset,
                    request->new_offset(),
                    options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, PullQueue)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto queuePath = FromProto<TRichYPath>(request->queue_path());

        TPullQueueOptions options;
        SetTimeoutOptions(&options, context.Get());

        auto rowBatchReadOptions = FromProto<NQueueClient::TQueueRowBatchReadOptions>(request->row_batch_read_options());

        context->SetRequestInfo(
            "QueuePath: %v, Offset: %v, PartitionIndex: %v, "
            "MaxRowCount: %v, MaxDataWeight: %v, DataWeightPerRowHint: %v",
            queuePath,
            request->offset(),
            request->partition_index(),
            rowBatchReadOptions.MaxRowCount,
            rowBatchReadOptions.MaxDataWeight,
            rowBatchReadOptions.DataWeightPerRowHint);

        // TODO(achulkov2): Support WorkloadDescriptor.
        options.UseNativeTabletNodeApi = request->use_native_tablet_node_api();
        if (request->has_replica_consistency()) {
            options.ReplicaConsistency = FromProto<EReplicaConsistency>(request->replica_consistency());
        }

        ExecuteCall(
            context,
            [=] {
                return client->PullQueue(
                    queuePath,
                    request->offset(),
                    request->partition_index(),
                    rowBatchReadOptions,
                    options);
            },
            [=] (const auto& context, const auto& queueRowset) {
                auto* response = &context->Response();
                response->Attachments() = PrepareRowsetForAttachment(response, static_cast<IUnversionedRowsetPtr>(queueRowset));
                response->set_start_offset(queueRowset->GetStartOffset());

                context->SetResponseInfo(
                    "RowCount: %v, StartOffset: %v",
                    queueRowset->GetRows().size(),
                    queueRowset->GetStartOffset());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, PullQueueConsumer)
    {
        PullQueueConsumerImpl(request, response, context);
    }

    DECLARE_RPC_SERVICE_METHOD_VIA_MESSAGES(
        NApi::NRpcProxy::NProto::TReqPullQueueConsumer,
        NApi::NRpcProxy::NProto::TRspPullQueueConsumer,
        PullConsumer)
    {
        PullQueueConsumerImpl(request, response, context);
    }

    void PullQueueConsumerImpl(
        NApi::NRpcProxy::NProto::TReqPullQueueConsumer* request,
        NApi::NRpcProxy::NProto::TRspPullQueueConsumer* /*response*/,
        const TCtxPullQueueConsumerPtr& context)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto consumerPath = FromProto<TRichYPath>(request->consumer_path());
        auto queuePath = FromProto<TRichYPath>(request->queue_path());

        TPullQueueConsumerOptions options;
        SetTimeoutOptions(&options, context.Get());

        auto rowBatchReadOptions = FromProto<NQueueClient::TQueueRowBatchReadOptions>(request->row_batch_read_options());

        std::optional<i64> offset = YT_PROTO_OPTIONAL(*request, offset);

        context->SetRequestInfo(
            "ConsumerPath: %v, QueuePath: %v, Offset: %v, PartitionIndex: %v, "
            "MaxRowCount: %v, MaxDataWeight: %v, DataWeightPerRowHint: %v",
            request->consumer_path(),
            request->queue_path(),
            offset,
            request->partition_index(),
            rowBatchReadOptions.MaxRowCount,
            rowBatchReadOptions.MaxDataWeight,
            rowBatchReadOptions.DataWeightPerRowHint);

        // TODO(achulkov2): Support WorkloadDescriptor.
        if (request->has_replica_consistency()) {
            options.ReplicaConsistency = FromProto<EReplicaConsistency>(request->replica_consistency());
        }

        ExecuteCall(
            context,
            [=] {
                return client->PullQueueConsumer(
                    consumerPath,
                    queuePath,
                    offset,
                    request->partition_index(),
                    rowBatchReadOptions,
                    options);
            },
            [=] (const auto& context, const auto& queueRowset) {
                auto* response = &context->Response();
                response->Attachments() = PrepareRowsetForAttachment(response, static_cast<IUnversionedRowsetPtr>(queueRowset));
                response->set_start_offset(queueRowset->GetStartOffset());

                context->SetResponseInfo(
                    "RowCount: %v, StartOffset: %v",
                    queueRowset->GetRows().size(),
                    queueRowset->GetStartOffset());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, RegisterQueueConsumer)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto queuePath = FromProto<TRichYPath>(request->queue_path());
        auto consumerPath = FromProto<TRichYPath>(request->consumer_path());
        bool vital = request->vital();

        TRegisterQueueConsumerOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_partitions()) {
            options.Partitions = FromProto<std::vector<int>>(request->partitions().items());
        }

        context->SetRequestInfo(
            "QueuePath: %v, ConsumerPath: %v, Vital: %v, Partitions: %v",
            queuePath,
            consumerPath,
            vital,
            options.Partitions);

        ExecuteCall(
            context,
            [=] {
                return client->RegisterQueueConsumer(
                    queuePath,
                    consumerPath,
                    vital,
                    options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, UnregisterQueueConsumer)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto queuePath = FromProto<TRichYPath>(request->queue_path());
        auto consumerPath = FromProto<TRichYPath>(request->consumer_path());

        TUnregisterQueueConsumerOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRequestInfo(
            "QueuePath: %v, ConsumerPath: %v",
            queuePath,
            consumerPath);

        ExecuteCall(
            context,
            [=] {
                return client->UnregisterQueueConsumer(
                    queuePath,
                    consumerPath,
                    options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ListQueueConsumerRegistrations)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        std::optional<TRichYPath> queuePath;
        if (request->has_queue_path()) {
            queuePath = FromProto<TRichYPath>(request->queue_path());
        }
        std::optional<TRichYPath> consumerPath;
        if (request->has_consumer_path()) {
            consumerPath = FromProto<TRichYPath>(request->consumer_path());
        }

        TListQueueConsumerRegistrationsOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRequestInfo(
            "QueuePath: %v, ConsumerPath: %v",
            queuePath,
            consumerPath);

        ExecuteCall(
            context,
            [=] {
                return client->ListQueueConsumerRegistrations(
                    queuePath,
                    consumerPath,
                    options);
            },
            [=] (const auto& context, const std::vector<TListQueueConsumerRegistrationsResult>& registrations) {
                auto* response = &context->Response();
                for (const auto& registration : registrations) {
                    auto* protoRegistration = response->add_registrations();
                    ToProto(protoRegistration->mutable_queue_path(), registration.QueuePath);
                    ToProto(protoRegistration->mutable_consumer_path(), registration.ConsumerPath);
                    protoRegistration->set_vital(registration.Vital);
                    if (registration.Partitions) {
                        ToProto(protoRegistration->mutable_partitions()->mutable_items(), *registration.Partitions);
                    }
                }

                context->SetResponseInfo("Registrations: %v", registrations.size());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, CreateQueueProducerSession)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto producerPath = FromProto<TRichYPath>(request->producer_path());
        auto queuePath = FromProto<TRichYPath>(request->queue_path());
        auto sessionId = FromProto<TQueueProducerSessionId>(request->session_id());

        TCreateQueueProducerSessionOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_user_meta()) {
            options.UserMeta = ConvertToNode(TYsonStringBuf(request->user_meta()));
        }

        context->SetRequestInfo(
            "ProducerPath: %v, QueuePath: %v, SessionId: %v",
            producerPath,
            queuePath,
            sessionId);

        ExecuteCall(
            context,
            [=] {
                return client->CreateQueueProducerSession(
                    producerPath,
                    queuePath,
                    sessionId,
                    options);
            },
            [=] (const auto& context, const TCreateQueueProducerSessionResult& result) {
                auto* response = &context->Response();

                response->set_sequence_number(result.SequenceNumber.Underlying());
                response->set_epoch(result.Epoch.Underlying());
                if (result.UserMeta) {
                    ToProto(response->mutable_user_meta(), ConvertToYsonString(result.UserMeta).ToString());
                }

                context->SetResponseInfo(
                    "SequenceNumber: %v, Epoch: %v",
                    result.SequenceNumber,
                    result.Epoch);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, RemoveQueueProducerSession)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto producerPath = FromProto<TRichYPath>(request->producer_path());
        auto queuePath = FromProto<TRichYPath>(request->queue_path());
        auto sessionId = FromProto<TQueueProducerSessionId>(request->session_id());

        TRemoveQueueProducerSessionOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRequestInfo(
            "ProducerPath: %v, QueuePath: %v, SessionId: %v",
            producerPath,
            queuePath,
            sessionId);

        ExecuteCall(
            context,
            [=] {
                return client->RemoveQueueProducerSession(
                    producerPath,
                    queuePath,
                    sessionId,
                    options);
            });
    }

    void DoModifyRows(
        const NApi::NRpcProxy::NProto::TReqModifyRows& request,
        const std::vector<TSharedRef>& attachments,
        const ITransactionPtr& transaction)
    {
        const auto& path = request.path();

        auto rowset = NApi::NRpcProxy::DeserializeRowset<TUnversionedRow>(
            request.rowset_descriptor(),
            MergeRefsToRef<TApiServiceBufferTag>(attachments));

        auto rowsetRows = rowset->GetRows();
        auto rowsetSize = std::ssize(rowset->GetRows());

        if (rowsetSize != request.row_modification_types_size()) {
            THROW_ERROR_EXCEPTION("Row count mismatch")
                << TErrorAttribute("rowset_size", rowsetSize)
                << TErrorAttribute("row_modification_types_size", request.row_modification_types_size());
        }

        std::vector<TRowModification> modifications;
        modifications.reserve(rowsetSize);
        for (ssize_t index = 0; index < rowsetSize; ++index) {
            TLockMask lockMask;
            if (index < request.row_legacy_read_locks_size()) {
                TLegacyLockBitmap readLockMask = request.row_legacy_read_locks(index);
                for (int index = 0; index < TLegacyLockMask::MaxCount; ++index) {
                    if (readLockMask & (1u << index)) {
                        lockMask.Set(index, ELockType::SharedWeak);
                    }
                }
            } else if (index < request.row_legacy_locks_size()) {
                auto legacyLocks = TLegacyLockMask(request.row_legacy_locks(index));
                for (int index = 0; index < TLegacyLockMask::MaxCount; ++index) {
                    lockMask.Set(index, legacyLocks.Get(index));
                }
            } else if (index < request.row_locks_size()) {
                FromProto(&lockMask, request.row_locks(index));
            }

            modifications.push_back({
                FromProto<ERowModificationType>(request.row_modification_types(index)),
                rowsetRows[index].ToTypeErasedRow(),
                lockMask,
            });
        }

        TModifyRowsOptions options;
        if (request.has_require_sync_replica()) {
            options.RequireSyncReplica = request.require_sync_replica();
        }
        if (request.has_upstream_replica_id()) {
            FromProto(&options.UpstreamReplicaId, request.upstream_replica_id());
        }
        if (request.has_allow_missing_key_columns()) {
            options.AllowMissingKeyColumns = request.allow_missing_key_columns();
        }

        if (Config_.Acquire()->EnableModifyRowsRequestReordering &&
            request.has_sequence_number())
        {
            options.SequenceNumber = request.sequence_number();
            options.SequenceNumberSourceId = request.sequence_number_source_id();
        }

        transaction->ModifyRows(
            path,
            rowset->GetNameTable(),
            MakeSharedRange(std::move(modifications), rowset),
            options);
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ModifyRows)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        context->SetRequestInfo(
            "TransactionId: %v, Path: %v, ModificationCount: %v",
            transactionId,
            request->path(),
            request->row_modification_types_size());

        auto transaction = GetTransactionOrThrow(
            context,
            request,
            transactionId,
            /*options*/ std::nullopt,
            /*searchInPool*/ true);

        DoModifyRows(*request, request->Attachments(), transaction);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, BatchModifyRows)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        context->SetRequestInfo("TransactionId: %v, BatchSize: %v",
            transactionId,
            request->part_counts_size());

        i64 attachmentCount = request->Attachments().size();
        i64 expectedAttachmentCount = 0;
        for (int partCount : request->part_counts()) {
            if (partCount < 0) {
                THROW_ERROR_EXCEPTION("Received a negative part count")
                    << TErrorAttribute("part_count", partCount);
            }
            if (partCount >= attachmentCount) {
                THROW_ERROR_EXCEPTION("Part count is too large")
                    << TErrorAttribute("part_count", partCount);
            }
            expectedAttachmentCount += partCount + 1;
        }
        if (attachmentCount != expectedAttachmentCount) {
            THROW_ERROR_EXCEPTION("Attachment count mismatch")
                << TErrorAttribute("actual_attachment_count", attachmentCount)
                << TErrorAttribute("expected_attachment_count", expectedAttachmentCount);
        }

        auto transaction = GetTransactionOrThrow(
            context,
            request,
            FromProto<TTransactionId>(request->transaction_id()),
            /*options*/ std::nullopt,
            /*searchInPool*/ true);

        int attachmentIndex = 0;
        for (int partCount : request->part_counts()) {
            NApi::NRpcProxy::NProto::TReqModifyRows subrequest;
            if (!TryDeserializeProto(&subrequest, request->Attachments()[attachmentIndex])) {
                THROW_ERROR_EXCEPTION(NRpc::EErrorCode::ProtocolError, "Error deserializing subrequest");
            }
            ++attachmentIndex;
            std::vector<TSharedRef> attachments(
                request->Attachments().begin() + attachmentIndex,
                request->Attachments().begin() + attachmentIndex + partCount);
            DoModifyRows(subrequest, attachments, transaction);
            attachmentIndex += partCount;
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, BuildSnapshot)
    {
        TBuildSnapshotOptions options;
        SetTimeoutOptions(&options, context.Get());
        options.CellId = FromProto<TCellId>(request->cell_id());
        options.SetReadOnly = request->set_read_only();
        options.WaitForSnapshotCompletion = request->wait_for_snapshot_completion();

        context->SetRequestInfo("CellId: %v, SetReadOnly: %v, WaitForSnapshotCompletion: %v",
            options.CellId,
            options.SetReadOnly,
            options.WaitForSnapshotCompletion);

        auto client = GetAuthenticatedClientOrThrow(context, request);
        ExecuteCall(
            context,
            [=] {
                return client->BuildSnapshot(options);
            },
            [] (const auto& context, int snapshotId) {
                auto* response = &context->Response();
                response->set_snapshot_id(snapshotId);
                context->SetResponseInfo("SnapshotId: %v", snapshotId);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ExitReadOnly)
    {
        TExitReadOnlyOptions options;
        SetTimeoutOptions(&options, context.Get());

        auto cellId = FromProto<TCellId>(request->cell_id());

        context->SetRequestInfo("CellId: %v", cellId);

        auto client = GetAuthenticatedClientOrThrow(context, request);
        ExecuteCall(
            context,
            [=] {
                return client->ExitReadOnly(cellId, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, MasterExitReadOnly)
    {
        TMasterExitReadOnlyOptions options;
        SetTimeoutOptions(&options, context.Get());
        options.Retry = request->retry();

        context->SetRequestInfo("Retry: %v", options.Retry);

        auto client = GetAuthenticatedClientOrThrow(context, request);
        ExecuteCall(
            context,
            [=] {
                return client->MasterExitReadOnly(options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, DiscombobulateNonvotingPeers)
    {
        TDiscombobulateNonvotingPeersOptions options;
        SetTimeoutOptions(&options, context.Get());

        auto cellId = FromProto<TCellId>(request->cell_id());

        context->SetRequestInfo("CellId: %v", cellId);

        auto client = GetAuthenticatedClientOrThrow(context, request);
        ExecuteCall(
            context,
            [=] {
                return client->DiscombobulateNonvotingPeers(cellId, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GCCollect)
    {
        TGCCollectOptions options;
        SetTimeoutOptions(&options, context.Get());
        options.CellId = FromProto<TCellId>(request->cell_id());

        context->SetRequestInfo("CellId: %v", options.CellId);

        auto client = GetAuthenticatedClientOrThrow(context, request);
        ExecuteCall(
            context,
            [=] {
                return client->GCCollect(options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, SuspendCoordinator)
    {
        TSuspendCoordinatorOptions options;
        SetTimeoutOptions(&options, context.Get());

        auto coordinatorCellId = FromProto<TCellId>(request->coordinator_cell_id());

        context->SetRequestInfo("CoordinatorCellId: %v",
            coordinatorCellId);

        auto client = GetAuthenticatedClientOrThrow(context, request);
        ExecuteCall(
            context,
            [=] {
                return client->SuspendCoordinator(coordinatorCellId, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ResumeCoordinator)
    {
        TResumeCoordinatorOptions options;
        SetTimeoutOptions(&options, context.Get());

        auto coordinatorCellId = FromProto<TCellId>(request->coordinator_cell_id());

        context->SetRequestInfo("CoordinatorCellId: %v",
            coordinatorCellId);

        auto client = GetAuthenticatedClientOrThrow(context, request);
        ExecuteCall(
            context,
            [=] {
                return client->ResumeCoordinator(coordinatorCellId, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, MigrateReplicationCards)
    {
        TMigrateReplicationCardsOptions options;
        SetTimeoutOptions(&options, context.Get());

        auto chaosCellId = FromProto<TCellId>(request->chaos_cell_id());
        FromProto(&options.ReplicationCardIds, request->replication_card_ids());
        if (request->has_destination_cell_id()) {
            options.DestinationCellId = FromProto<TCellId>(request->destination_cell_id());
        }

        context->SetRequestInfo("ChaosCellId: %v, DestinationCellId: %v, ReplicationCardIds: %v",
            chaosCellId,
            options.DestinationCellId,
            options.ReplicationCardIds);

        auto client = GetAuthenticatedClientOrThrow(context, request);
        ExecuteCall(
            context,
            [=] {
                return client->MigrateReplicationCards(chaosCellId, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, SuspendChaosCells)
    {
        TSuspendChaosCellsOptions options;
        SetTimeoutOptions(&options, context.Get());

        auto cellIds = FromProto<std::vector<TCellId>>(request->cell_ids());

        context->SetRequestInfo("ChaosCellIds: %v",
            cellIds);

        auto client = GetAuthenticatedClientOrThrow(context, request);
        ExecuteCall(
            context,
            [=] {
                return client->SuspendChaosCells(cellIds, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ResumeChaosCells)
    {
        TResumeChaosCellsOptions options;
        SetTimeoutOptions(&options, context.Get());

        auto cellIds = FromProto<std::vector<TCellId>>(request->cell_ids());

        context->SetRequestInfo("ChaosCellIds: %v",
            cellIds);

        auto client = GetAuthenticatedClientOrThrow(context, request);
        ExecuteCall(
            context,
            [=] {
                return client->ResumeChaosCells(cellIds, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, SuspendTabletCells)
    {
        TSuspendTabletCellsOptions options;
        SetTimeoutOptions(&options, context.Get());

        auto cellIds = FromProto<std::vector<TCellId>>(request->cell_ids());

        context->SetRequestInfo("TabletCellIds: %v", cellIds);

        auto client = GetAuthenticatedClientOrThrow(context, request);
        ExecuteCall(
            context,
            [=] {
                return client->SuspendTabletCells(cellIds, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ResumeTabletCells)
    {
        TResumeTabletCellsOptions options;
        SetTimeoutOptions(&options, context.Get());

        auto cellIds = FromProto<std::vector<TCellId>>(request->cell_ids());

        context->SetRequestInfo("TabletCellIds: %v", cellIds);

        auto client = GetAuthenticatedClientOrThrow(context, request);
        ExecuteCall(
            context,
            [=] {
                return client->ResumeTabletCells(cellIds, options);
            });
    }

    static EMaintenanceComponent MaintenanceComponentFromProto(
        NApi::NRpcProxy::NProto::EMaintenanceComponent component)
    {
        using EProtoMaintenanceComponent = NApi::NRpcProxy::NProto::EMaintenanceComponent;

        switch (component) {
            case EProtoMaintenanceComponent::MC_CLUSTER_NODE:
                return EMaintenanceComponent::ClusterNode;
            case EProtoMaintenanceComponent::MC_HTTP_PROXY:
                return EMaintenanceComponent::HttpProxy;
            case EProtoMaintenanceComponent::MC_RPC_PROXY:
                return EMaintenanceComponent::RpcProxy;
            case EProtoMaintenanceComponent::MC_HOST:
                return EMaintenanceComponent::Host;
            default:
                THROW_ERROR_EXCEPTION("Invalid maintenance component %v",
                    static_cast<int>(component));
        }
    }

    static EMaintenanceType MaintenanceTypeFromProto(
        NApi::NRpcProxy::NProto::EMaintenanceType type)
    {
        using EProtoMaintenanceType = NApi::NRpcProxy::NProto::EMaintenanceType;

        switch (type) {
            case NApi::NRpcProxy::NProto::MT_BAN:
                return EMaintenanceType::Ban;
            case EProtoMaintenanceType::MT_DECOMMISSION:
                return EMaintenanceType::Decommission;
            case EProtoMaintenanceType::MT_DISABLE_WRITE_SESSIONS:
                return EMaintenanceType::DisableWriteSessions;
            case EProtoMaintenanceType::MT_DISABLE_TABLET_CELLS:
                return EMaintenanceType::DisableTabletCells;
            case EProtoMaintenanceType::MT_DISABLE_SCHEDULER_JOBS:
                return EMaintenanceType::DisableSchedulerJobs;
            case EProtoMaintenanceType::MT_PENDING_RESTART:
                return EMaintenanceType::PendingRestart;
            default:
                THROW_ERROR_EXCEPTION("Invalid maintenance type %v",
                    static_cast<int>(type));
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, AddMaintenance)
    {
        auto component = MaintenanceComponentFromProto(request->component());
        auto address = request->address();
        auto type = MaintenanceTypeFromProto(request->type());
        auto comment = request->comment();
        ValidateMaintenanceComment(comment);

        // COMPAT(kvk1920): For compatibility with pre-24.2 clients.
        auto supportsPerTargetResponse = request->supports_per_target_response();

        TAddMaintenanceOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRequestInfo(
            "Component: %v, Address: %v, Type: %v, Comment: %v, SupportsPerTargetResponse: %v",
            component,
            address,
            type,
            comment,
            supportsPerTargetResponse);

        auto client = GetAuthenticatedClientOrThrow(context, request);

        ExecuteCall(
            context,
            [=] {
                return client->AddMaintenance(component, address, type, comment, options);
            },
            [=] (const auto& context, const TMaintenanceIdPerTarget& result) {
                auto* response = &context->Response();
                // COMPAT(kvk1920): Compatibility with pre-24.2 RPC clients.
                if (!supportsPerTargetResponse) {
                    ToProto(
                        response->mutable_id(),
                        result.size() == 1 ? result.begin()->second : TMaintenanceId{});
                    return;
                }

                for (const auto& [target, id] : result) {
                    ToProto(
                        &(response->mutable_id_per_target()->operator[](target)),
                        id);
                }
            });
    }


    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, RemoveMaintenance)
    {
        auto component = MaintenanceComponentFromProto(request->component());
        auto address = request->address();

        TStringBuilder requestInfo;
        requestInfo.AppendFormat("Component: %v, Address: %v",
            component,
            address);

        if (request->mine() && request->has_user()) {
            THROW_ERROR_EXCEPTION("Cannot specify both \"user\" and \"mine\"");
        }

        TMaintenanceFilter filter;
        filter.Ids = FromProto<std::vector<TMaintenanceId>>(request->ids());

        if (request->has_type()) {
            filter.Type = MaintenanceTypeFromProto(request->type());
            requestInfo.AppendFormat(", Type: %v", filter.Type);
        }

        using TByUser = TMaintenanceFilter::TByUser;
        if (request->has_user()) {
            auto user = request->user();
            requestInfo.AppendFormat(", User: ", user);
            filter.User = user;
        } else if (request->mine()) {
            filter.User = TByUser::TMine{};
            requestInfo.AppendString(", Mine: true");
        } else {
            filter.User = TByUser::TAll{};
        }

        // COMPAT(kvk1920): For compatibility with pre-24.2 RPC clients.
        auto supportsPerTargetResponse = request->supports_per_target_response();
        requestInfo.AppendFormat(
            ", SupportsPerTargetResponse: %v",
            supportsPerTargetResponse);

        TRemoveMaintenanceOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRawRequestInfo(requestInfo.Flush(), /*incremental*/ false);

        auto client = GetAuthenticatedClientOrThrow(context, request);

        ExecuteCall(
            context,
            [=] {
                return client->RemoveMaintenance(component, address, filter);
            },
            [=] (const auto& context, const TMaintenanceCountsPerTarget& result) {
                auto& response = context->Response();

                auto fillMaintenanceCounts = [] (auto* protoMap, const TMaintenanceCounts& counts) {
                    using namespace NApi::NRpcProxy::NProto;
                    constexpr NApi::NRpcProxy::NProto::EMaintenanceType Types[] =  {
                        MT_BAN,
                        MT_DECOMMISSION,
                        MT_DISABLE_SCHEDULER_JOBS,
                        MT_DISABLE_WRITE_SESSIONS,
                        MT_DISABLE_TABLET_CELLS,
                        MT_PENDING_RESTART
                    };

                    for (auto type : Types) {
                        protoMap->insert({
                            type,
                            counts[MaintenanceTypeFromProto(type)]});
                    }
                };

                // COMPAT(kvk1920): For compatibility with pre-24.2 RPC clients.
                if (!supportsPerTargetResponse) {
                    TMaintenanceCounts totalCounts;
                    for (const auto& [target, targetCounts] : result) {
                        for (auto type : TEnumTraits<EMaintenanceType>::GetDomainValues()) {
                            totalCounts[type] += targetCounts[type];
                        }
                    }

                    // COMPAT(kvk1920): For compatibility with pre-23.2 RPC clients.
                    response.set_ban(totalCounts[EMaintenanceType::Ban]);
                    response.set_decommission(totalCounts[EMaintenanceType::Decommission]);
                    response.set_disable_scheduler_jobs(totalCounts[EMaintenanceType::DisableSchedulerJobs]);
                    response.set_disable_write_sessions(totalCounts[EMaintenanceType::DisableWriteSessions]);
                    response.set_disable_tablet_cells(totalCounts[EMaintenanceType::DisableTabletCells]);
                    response.set_pending_restart(totalCounts[EMaintenanceType::PendingRestart]);

                    response.set_use_map_instead_of_fields(true);

                    fillMaintenanceCounts(response.mutable_removed_maintenance_counts(), totalCounts);

                    return;
                }

                response.set_supports_per_target_response(true);

                auto* protoMap = response.mutable_removed_maintenance_counts_per_target();
                for (const auto& [target, counts] : result) {
                    fillMaintenanceCounts(
                        protoMap->operator[](target).mutable_counts(),
                        counts);
                }
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, DisableChunkLocations)
    {
        auto nodeAddress = request->node_address();
        auto locationUuids = request->location_uuids();

        TDisableChunkLocationsOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRequestInfo("NodeAddress: %v, LocationUuids: %v", nodeAddress, locationUuids);

        auto client = GetAuthenticatedClientOrThrow(context, request);

        ExecuteCall(
            context,
            [=] {
                return client->DisableChunkLocations(
                    nodeAddress,
                    FromProto<std::vector<TGuid>>(request->location_uuids()),
                    options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                ToProto(response->mutable_location_uuids(), result.LocationUuids);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, DestroyChunkLocations)
    {
        auto nodeAddress = request->node_address();
        auto locationUuids = request->location_uuids();
        auto recoverUnlinkedDisks = request->recover_unlinked_disks();

        TDestroyChunkLocationsOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRequestInfo("NodeAddress: %v, RecoverUnlinkedDisks: %v, LocationUuids: %v",
            nodeAddress,
            recoverUnlinkedDisks,
            locationUuids);

        auto client = GetAuthenticatedClientOrThrow(context, request);

        ExecuteCall(
            context,
            [=] {
                return client->DestroyChunkLocations(
                    nodeAddress,
                    recoverUnlinkedDisks,
                    FromProto<std::vector<TGuid>>(request->location_uuids()),
                    options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                ToProto(response->mutable_location_uuids(), result.LocationUuids);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ResurrectChunkLocations)
    {
        auto nodeAddress = request->node_address();
        auto locationUuids = request->location_uuids();

        TResurrectChunkLocationsOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRequestInfo("NodeAddress: %v, LocationUuids: %v", nodeAddress, locationUuids);

        auto client = GetAuthenticatedClientOrThrow(context, request);

        ExecuteCall(
            context,
            [=] {
                return client->ResurrectChunkLocations(
                    nodeAddress,
                    FromProto<std::vector<TGuid>>(request->location_uuids()),
                    options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                ToProto(response->mutable_location_uuids(), result.LocationUuids);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, RequestRestart)
    {
        auto nodeAddress = request->node_address();

        TRequestRestartOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRequestInfo("NodeAddress: %v", nodeAddress);

        auto client = GetAuthenticatedClientOrThrow(context, request);

        ExecuteCall(
            context,
            [=] {
                return client->RequestRestart(
                    nodeAddress,
                    options);
            },
            [] (const auto& /*context*/, const auto& /*result*/) {
                // do nothing.
            });
    }

    ////////////////////////////////////////////////////////////////////////////////
    // SECURITY
    ////////////////////////////////////////////////////////////////////////////////

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, AddMember)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto group = request->group();
        auto member = request->member();

        TAddMemberOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }

        context->SetRequestInfo("Group: %v, Member: %v, MutationId: %v, Retry: %v",
            group,
            member,
            options.MutationId,
            options.Retry);

        ExecuteCall(
            context,
            [=] {
                return client->AddMember(group, member, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, RemoveMember)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto group = request->group();
        auto member = request->member();

        TRemoveMemberOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }

        context->SetRequestInfo("Group: %v, Member: %v, MutationId: %v, Retry: %v",
            group,
            member,
            options.MutationId,
            options.Retry);

        ExecuteCall(
            context,
            [=] {
                return client->RemoveMember(group, member, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, CheckPermission)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& user = request->user();
        const auto& path = request->path();
        auto permission = FromProto<EPermission>(request->permission());

        TCheckPermissionOptions options;
        if (request->has_columns()) {
            options.Columns = FromProto<std::vector<std::string>>(request->columns().items());
        }
        if (request->has_vital()) {
            options.Vital = request->vital();
        }
        SetTimeoutOptions(&options, context.Get());
        if (request->has_master_read_options()) {
            FromProto(&options, request->master_read_options());
        }
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }

        context->SetRequestInfo("User: %v, Path: %v, Permission: %v",
            user,
            path,
            FormatPermissions(permission));

        ExecuteCall(
            context,
            [=] {
                return client->CheckPermission(user, path, permission, options);
            },
            [] (const auto& context, const auto& checkResponse) {
                auto* response = &context->Response();
                ToProto(response->mutable_result(), checkResponse);
                if (checkResponse.Columns) {
                    ToProto(response->mutable_columns()->mutable_items(), *checkResponse.Columns);
                }
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, CheckPermissionByAcl)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        std::optional<TString> user;
        if (request->has_user()) {
            user = request->user();
        }
        auto permission = FromProto<EPermission>(request->permission());
        auto acl = ConvertToNode(TYsonString(request->acl()));

        TCheckPermissionByAclOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_master_read_options()) {
            FromProto(&options, request->master_read_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }

        options.IgnoreMissingSubjects = request->ignore_missing_subjects();

        context->SetRequestInfo("User: %v, Permission: %v",
            user,
            FormatPermissions(permission));

        ExecuteCall(
            context,
            [=] {
                return client->CheckPermissionByAcl(user, permission, acl, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                ToProto(response->mutable_result(), result);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, TransferAccountResources)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto srcAccount = request->src_account();
        auto dstAccount = request->dst_account();
        auto resourceDelta = ConvertToNode(TYsonString(request->resource_delta()));

        TTransferAccountResourcesOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());

        context->SetRequestInfo("SrcAccount: %v, DstAccount: %v",
            srcAccount,
            dstAccount);

        ExecuteCall(
            context,
            [=] {
                return client->TransferAccountResources(srcAccount, dstAccount, resourceDelta, options);
            });
    }

    ////////////////////////////////////////////////////////////////////////////////
    // FILES
    ////////////////////////////////////////////////////////////////////////////////

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ReadFile)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TFileReaderOptions options;
        if (request->has_offset()) {
            options.Offset = request->offset();
        }
        if (request->has_length()) {
            options.Length = request->length();
        }
        if (request->has_config()) {
            options.Config = ConvertTo<TFileReaderConfigPtr>(TYsonString(request->config()));
        }

        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_suppressable_access_tracking_options()) {
            FromProto(&options, request->suppressable_access_tracking_options());
        }

        context->SetRequestInfo("Path: %v, Offset: %v, Length: %v",
            path,
            options.Offset,
            options.Length);

        PutMethodInfoInTraceContext("read_file");

        auto fileReader = WaitFor(client->CreateFileReader(path, options))
            .ValueOrThrow();

        auto outputStream = context->GetResponseAttachmentsStream();

        NApi::NRpcProxy::NProto::TReadFileMeta meta;
        ToProto(meta.mutable_id(), fileReader->GetId());
        meta.set_revision(ToProto(fileReader->GetRevision()));

        auto metaRef = SerializeProtoToRef(meta);
        WaitFor(outputStream->Write(metaRef))
            .ThrowOnError();

        HandleInputStreamingRequest(context, fileReader);
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, WriteFile)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto path = FromProto<NYPath::TRichYPath>(request->path());

        TFileWriterOptions options;
        options.ComputeMD5 = request->compute_md5();
        if (request->has_config()) {
            options.Config = ConvertTo<TFileWriterConfigPtr>(TYsonString(request->config()));
        }

        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }

        context->SetRequestInfo(
            "Path: %v, ComputeMD5: %v",
            path,
            options.ComputeMD5);

        PutMethodInfoInTraceContext("write_file");

        auto fileWriter = client->CreateFileWriter(path, options);
        WaitFor(fileWriter->Open())
            .ThrowOnError();

        HandleOutputStreamingRequest(
            context,
            [&] (TSharedRef block) {
                WaitFor(fileWriter->Write(std::move(block)))
                    .ThrowOnError();
            },
            [&] {
                WaitFor(fileWriter->Close())
                    .ThrowOnError();
            },
            false /*feedbackEnabled*/);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // JOURNALS
    ////////////////////////////////////////////////////////////////////////////////

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ReadJournal)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TJournalReaderOptions options;
        if (request->has_first_row_index()) {
            options.FirstRowIndex = request->first_row_index();
        }
        if (request->has_row_count()) {
            options.RowCount = request->row_count();
        }
        if (request->has_config()) {
            options.Config = ConvertTo<TJournalReaderConfigPtr>(TYsonString(request->config()));
        }

        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_suppressable_access_tracking_options()) {
            FromProto(&options, request->suppressable_access_tracking_options());
        }

        context->SetRequestInfo("Path: %v, FirstRowIndex: %v, RowCount: %v",
            path,
            options.FirstRowIndex,
            options.RowCount);

        auto journalReader = client->CreateJournalReader(path, options);
        WaitFor(journalReader->Open())
            .ThrowOnError();

        HandleInputStreamingRequest(
            context,
            [&] {
                auto rows = WaitFor(journalReader->Read())
                    .ValueOrThrow();

                if (rows.empty()) {
                    return TSharedRef();
                }

                return PackRefs(rows);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, WriteJournal)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TJournalWriterOptions options;
        if (request->has_config()) {
            options.Config = ConvertTo<TJournalWriterConfigPtr>(TYsonString(request->config()));
        }
        options.EnableMultiplexing = request->enable_multiplexing();
        options.EnableChunkPreallocation = request->enable_chunk_preallocation();
        options.ReplicaLagLimit = request->replica_lag_limit();

        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }

        context->SetRequestInfo(
            "Path: %v, EnableMultiplexing: %v, EnableChunkPreallocation: %v, ReplicaLagLimit: %v",
            path,
            options.EnableMultiplexing,
            options.EnableChunkPreallocation,
            options.ReplicaLagLimit);

        auto journalWriter = client->CreateJournalWriter(path, options);
        WaitFor(journalWriter->Open())
            .ThrowOnError();

        HandleOutputStreamingRequest(
            context,
            [&] (const TSharedRef& packedRows) {
                auto rows = UnpackRefs(packedRows);
                WaitFor(journalWriter->Write(rows))
                    .ThrowOnError();
            },
            [&] {
                WaitFor(journalWriter->Close())
                    .ThrowOnError();
            },
            true /*feedbackEnabled*/);
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, TruncateJournal)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();
        auto rowCount = request->row_count();

        TTruncateJournalOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }

        context->SetRequestInfo("Path: %v, RowCount: %v",
            path,
            rowCount);

        ExecuteCall(
            context,
            [=] {
                return client->TruncateJournal(
                    path,
                    rowCount,
                    options);
            });
    }

    ////////////////////////////////////////////////////////////////////////////////
    // TABLES
    ////////////////////////////////////////////////////////////////////////////////

    void ValidateFormat(const std::string& user, const INodePtr& formatNode)
    {
        const auto& config = Config_.Acquire();
        const auto& formatConfigs = config->Formats;
        TFormatManager formatManager(formatConfigs, user);
        formatManager.ValidateAndPatchFormatNode(formatNode, "format");
    }

    std::optional<NFormats::TFormat> GetFormat(
        const auto& context,
        const auto& request)
    {
        std::optional<NFormats::TFormat> format;
        if (request->has_format()) {
            auto stringFormat = TYsonStringBuf(request->format());
            ValidateFormat(context->GetAuthenticationIdentity().User, ConvertToNode(stringFormat));
            format = ConvertTo<NFormats::TFormat>(stringFormat);
        }

        return format;
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ReadTable)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto path = FromProto<NYPath::TRichYPath>(request->path());

        auto format = GetFormat(context, request);

        NApi::TTableReaderOptions options;
        options.Unordered = request->unordered();
        options.OmitInaccessibleColumns = request->omit_inaccessible_columns();
        options.EnableTableIndex = request->enable_table_index();
        options.EnableRowIndex = request->enable_row_index();
        options.EnableRangeIndex = request->enable_range_index();
        if (request->has_config()) {
            options.Config = ConvertTo<TTableReaderConfigPtr>(TYsonString(request->config()));
        }

        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }

        if (request->has_suppressable_access_tracking_options()) {
            FromProto(&options, request->suppressable_access_tracking_options());
        }

        auto desiredRowsetFormat = request->desired_rowset_format();
        auto arrowFallbackRowsetFormat = request->arrow_fallback_rowset_format();

        context->SetRequestInfo(
            "Path: %v, Unordered: %v, OmitInaccessibleColumns: %v, DesiredRowsetFormat: %v, ArrowFallbackRowsetFormat: %v",
            path,
            options.Unordered,
            options.OmitInaccessibleColumns,
            NApi::NRpcProxy::NProto::ERowsetFormat_Name(desiredRowsetFormat),
            NApi::NRpcProxy::NProto::ERowsetFormat_Name(arrowFallbackRowsetFormat));

        PutMethodInfoInTraceContext("read_table");

        auto tableReader = WaitFor(client->CreateTableReader(path, options))
            .ValueOrThrow();

        auto controlAttributesConfig = New<NFormats::TControlAttributesConfig>();
        controlAttributesConfig->EnableRowIndex = request->enable_row_index();
        controlAttributesConfig->EnableTableIndex = request->enable_table_index();
        controlAttributesConfig->EnableRangeIndex = request->enable_range_index();

        auto encoder = CreateRowStreamEncoder(
            desiredRowsetFormat,
            arrowFallbackRowsetFormat,
            tableReader->GetTableSchema(),
            path.GetColumns(),
            tableReader->GetNameTable(),
            controlAttributesConfig,
            std::move(format));

        auto outputStream = context->GetResponseAttachmentsStream();
        NApi::NRpcProxy::NProto::TRspReadTableMeta meta;
        meta.set_start_row_index(tableReader->GetStartRowIndex());
        ToProto(meta.mutable_omitted_inaccessible_columns(), tableReader->GetOmittedInaccessibleColumns());
        ToProto(meta.mutable_schema(), tableReader->GetTableSchema());
        meta.mutable_statistics()->set_total_row_count(tableReader->GetTotalRowCount());
        ToProto(meta.mutable_statistics()->mutable_data_statistics(), tableReader->GetDataStatistics());

        auto metaRef = SerializeProtoToRef(meta);
        WaitFor(outputStream->Write(metaRef))
            .ThrowOnError();

        bool finished = false;

        const auto& config = Config_.Acquire();

        HandleInputStreamingRequest(
            context,
            [&] {
                if (finished) {
                    return TSharedRef();
                }

                TRowBatchReadOptions options{
                    .MaxRowsPerRead = config->ReadBufferRowCount,
                    .MaxDataWeightPerRead = config->ReadBufferDataWeight,
                    .Columnar = IsColumnarRowsetFormat(request->desired_rowset_format())
                };
                auto batch = ReadRowBatch(tableReader, options);
                if (!batch) {
                    finished = true;
                }

                NApi::NRpcProxy::NProto::TRowsetStatistics statistics;
                statistics.set_total_row_count(tableReader->GetTotalRowCount());
                ToProto(statistics.mutable_data_statistics(), tableReader->GetDataStatistics());

                return encoder->Encode(
                    batch ? batch : CreateEmptyUnversionedRowBatch(),
                    &statistics);
            });
    }

    void WriteTableImpl(
        const auto& context,
        const auto& request,
        ITableWriterPtr tableWriter,
        const auto& finalizer)
    {
        auto format = GetFormat(context, request);

        THashMap<NApi::NRpcProxy::NProto::ERowsetFormat, IRowStreamDecoderPtr> parserMap;
        auto getOrCreateDecoder = [&] (NApi::NRpcProxy::NProto::ERowsetFormat rowsetFormat) {
            auto it =  parserMap.find(rowsetFormat);
            if (it == parserMap.end()) {
                auto parser = CreateRowStreamDecoder(
                    rowsetFormat,
                    tableWriter->GetSchema(),
                    tableWriter->GetNameTable(),
                    format);
                it = parserMap.emplace(rowsetFormat, std::move(parser)).first;
            }
            return it->second;
        };

        auto outputStream = context->GetResponseAttachmentsStream();

        NApi::NRpcProxy::NProto::TWriteTableMeta meta;
        ToProto(meta.mutable_schema(), tableWriter->GetSchema());
        auto metaRef = SerializeProtoToRef(meta);
        WaitFor(outputStream->Write(metaRef))
            .ThrowOnError();

        HandleOutputStreamingRequest(
            context,
            [&] (const TSharedRef& block) {
                NApi::NRpcProxy::NProto::TRowsetDescriptor descriptor;
                auto payloadRef = DeserializeRowStreamBlockEnvelope(block, &descriptor, nullptr);

                ValidateRowsetDescriptor(
                    descriptor,
                    NApi::NRpcProxy::CurrentWireFormatVersion,
                    NApi::NRpcProxy::NProto::RK_UNVERSIONED,
                    descriptor.rowset_format());

                auto decoder = getOrCreateDecoder(descriptor.rowset_format());

                auto batch = decoder->Decode(payloadRef, descriptor);

                auto rows = batch->MaterializeRows();

                tableWriter->Write(rows);

                WaitFor(tableWriter->GetReadyEvent())
                    .ThrowOnError();
            },
            [&] {
                WaitFor(tableWriter->Close())
                    .ThrowOnError();
                finalizer();
            },
            false /*feedbackEnabled*/);
    }

    void PatchTableWriterOptions(TNonNullPtr<NApi::TTableWriterOptions> options)
    {
        if (ApiServiceConfig_->EnableLargeColumnarStatistics) {
            options->Config->EnableLargeColumnarStatistics = true;
        }

        // NB: Input comes directly from user and thus requires additional validation.
        options->ValidateAnyIsValidYson = true;
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, WriteTable)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        PutMethodInfoInTraceContext("write_table");

        auto path = FromProto<TRichYPath>(request->path());
        context->SetRequestInfo(
            "Path: %v",
            path);

        NApi::TTableWriterOptions options;
        TString tableWriterConfig("{}");
        if (request->has_config()) {
            tableWriterConfig = request->config();
        }

        options.Config = ConvertTo<TTableWriterConfigPtr>(TYsonString(tableWriterConfig));

        PatchTableWriterOptions(&options);

        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }

        auto tableWriter = WaitFor(client->CreateTableWriter(path, options))
            .ValueOrThrow();

        WriteTableImpl(
            context,
            request,
            std::move(tableWriter),
            [] {});
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetColumnarStatistics)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        std::vector<NYPath::TRichYPath> paths;
        for (const auto& protoSubPath : request->paths()) {
            paths.emplace_back(ConvertTo<NYPath::TRichYPath>(TYsonString(protoSubPath)));
        }

        TGetColumnarStatisticsOptions options;
        SetTimeoutOptions(&options, context.Get());

        options.FetchChunkSpecConfig = New<TFetchChunkSpecConfig>();
        if (request->has_fetch_chunk_spec_config()) {
            FromProto(options.FetchChunkSpecConfig, request->fetch_chunk_spec_config());
        }

        options.FetcherConfig = New<TFetcherConfig>();
        if (request->has_fetcher_config()) {
            FromProto(options.FetcherConfig, request->fetcher_config());
        }

        options.FetcherMode = FromProto<NTableClient::EColumnarStatisticsFetcherMode>(request->fetcher_mode());

        options.EnableEarlyFinish = request->enable_early_finish();

        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }

        context->SetRequestInfo("Paths: %v", paths);

        ExecuteCall(
            context,
            [=] {
                return client->GetColumnarStatistics(paths, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                ToProto(response->mutable_statistics(), result);

                context->SetResponseInfo("StatisticsCount: %v", result.size());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, PartitionTables)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        std::vector<TRichYPath> paths;
        for (const auto& path : request->paths()) {
            paths.emplace_back(NYPath::TRichYPath::Parse(path));
        }

        TPartitionTablesOptions options;
        SetTimeoutOptions(&options, context.Get());

        options.FetchChunkSpecConfig = New<TFetchChunkSpecConfig>();
        if (request->has_fetch_chunk_spec_config()) {
            FromProto(options.FetchChunkSpecConfig, request->fetch_chunk_spec_config());
        }

        options.FetcherConfig = New<TFetcherConfig>();
        if (request->has_fetcher_config()) {
            FromProto(options.FetcherConfig, request->fetcher_config());
        }

        options.ChunkSliceFetcherConfig = New<TChunkSliceFetcherConfig>();
        if (request->has_chunk_slice_fetcher_config() && request->chunk_slice_fetcher_config().has_max_slices_per_fetch()) {
            options.ChunkSliceFetcherConfig->MaxSlicesPerFetch = request->chunk_slice_fetcher_config().max_slices_per_fetch();
        }

        options.PartitionMode = FromProto<NTableClient::ETablePartitionMode>(request->partition_mode());

        options.DataWeightPerPartition = request->data_weight_per_partition();

        if (request->has_max_partition_count()) {
            options.MaxPartitionCount = request->max_partition_count();
        }

        options.AdjustDataWeightPerPartition = request->adjust_data_weight_per_partition();

        options.EnableKeyGuarantee = request->enable_key_guarantee();

        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }

        context->SetRequestInfo("Paths: %v, PartitionMode: %v, KeyGuarantee: %v, DataWeightPerPartition: %v, AdjustDataWeightPerPartition: %v",
            paths,
            options.PartitionMode,
            options.EnableKeyGuarantee,
            options.DataWeightPerPartition,
            options.AdjustDataWeightPerPartition);

        ExecuteCall(
            context,
            [=] {
                return client->PartitionTables(paths, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                ToProto(response->mutable_partitions(), result.Partitions);

                context->SetResponseInfo("PartitionCount: %v", result.Partitions.size());
            });
    }

    ////////////////////////////////////////////////////////////////////////////////
    // DISTRIBUTED TABLE CLIENT
    ////////////////////////////////////////////////////////////////////////////////

    // Signature helpers.
    template <std::constructible_from<TSignaturePtr> TFinal>
    TFinal GenerateSignature(TSignaturePtr&& emptySignature) const
    {
        SignatureGenerator_->Sign(emptySignature);
        return TFinal(std::move(emptySignature));
    }

    TFuture<bool> ValidateSignature(const TSignaturePtr& signedValue) const
    {
        return SignatureValidator_->Validate(signedValue);
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, StartDistributedWriteSession)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        TRichYPath path;
        TDistributedWriteSessionStartOptions options;
        ParseRequest(&path, &options, *request);

        context->SetRequestInfo(
            "Path: %v",
            path);

        auto signSessionAndCookies = [&, this_ = MakeStrong(this)] (TDistributedWriteSessionWithCookies&& sessionAndCookies) {
            sessionAndCookies.Session = GenerateSignature<TSignedDistributedWriteSessionPtr>(std::move(sessionAndCookies.Session.Underlying()));

            for (auto& cookie : sessionAndCookies.Cookies) {
                cookie = GenerateSignature<TSignedWriteFragmentCookiePtr>(std::move(cookie.Underlying()));
            }

            return std::move(sessionAndCookies);
        };

        ExecuteCall(
            context,
            [=] {
                return client->StartDistributedWriteSession(path, options)
                    .ApplyUnique(BIND(std::move(signSessionAndCookies)));
            },
            [] (const auto& context, const auto& result) {
                context->Response().set_signed_session(ConvertToYsonString(result.Session).ToString());
                for (const auto& cookie : result.Cookies) {
                    context->Response().add_signed_cookies(ConvertToYsonString(cookie).ToString());
                }
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, FinishDistributedWriteSession)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        TDistributedWriteSessionWithResults sessionWithResults;

        TDistributedWriteSessionFinishOptions options;
        ParseRequest(&sessionWithResults, &options, *request);

        auto session = ConvertTo<TDistributedWriteSession>(sessionWithResults.Session.Underlying()->Payload());

        context->SetRequestInfo(
            "TableId: %v",
            session.PatchInfo.ObjectId);

        ExecuteCall(
            context,
            [=, this, options = std::move(options), sessionWithResults = std::move(sessionWithResults), sessionId = session.RootChunkListId] {
                std::vector<TFuture<bool>> validation;
                validation.reserve(1 + std::ssize(sessionWithResults.Results));
                validation.push_back(ValidateSignature(sessionWithResults.Session.Underlying()));
                for (const auto& signedResult : sessionWithResults.Results) {
                    auto result = ConvertTo<TWriteFragmentResult>(signedResult.Underlying()->Payload());
                    if (sessionId != result.SessionId) {
                        THROW_ERROR_EXCEPTION(
                            "Found write results with a different session id")
                            << TErrorAttribute("excepted_session_id", sessionId)
                            << TErrorAttribute("found_session_id", result.SessionId)
                            << TErrorAttribute("cookie_id", result.CookieId);
                    }
                    validation.push_back(ValidateSignature(signedResult.Underlying()));
                }

                return AllSucceeded(std::move(validation))
                    .ApplyUnique(BIND([=, options = std::move(options), sessionWithResults = std::move(sessionWithResults)] (std::vector<bool>&& results) {
                        auto allValid = std::ranges::all_of(results, [] (bool value) {
                            return value;
                        });
                        THROW_ERROR_EXCEPTION_UNLESS(
                            allValid,
                            "Signature validation failed for distributed write session finish");
                        return client->FinishDistributedWriteSession(std::move(sessionWithResults), options);
                    }));
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, WriteTableFragment)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        PutMethodInfoInTraceContext("write_table_fragment");

        TSignedWriteFragmentCookiePtr cookie;

        TTableFragmentWriterOptions options;
        ParseRequest(&cookie, &options, *request);

        PatchTableWriterOptions(&options);

        auto concreteCookie = ConvertTo<TWriteFragmentCookie>(cookie.Underlying()->Payload());

        context->SetRequestInfo(
            "TableId: %v, Main transaction id: %v",
            concreteCookie.PatchInfo.ObjectId,
            concreteCookie.MainTransactionId);

        auto isValid = WaitFor(ValidateSignature(cookie.Underlying()))
            .ValueOrThrow();

        if (!isValid) {
            THROW_ERROR_EXCEPTION(
                "Signature validation failed for write table fragment")
                    << TErrorAttribute("session_id", concreteCookie.SessionId)
                    << TErrorAttribute("cookie_id", concreteCookie.CookieId);
        }

        auto tableWriter = WaitFor(client->CreateTableFragmentWriter(cookie, options))
            .ValueOrThrow();

        WriteTableImpl(
            context,
            request,
            tableWriter,
            [&, tableWriter] {
                auto signedWriteResult = GenerateSignature<TSignedWriteFragmentResultPtr>(tableWriter->GetWriteFragmentResult().Underlying());
                response->set_signed_write_result(ConvertToYsonString(signedWriteResult).ToString());
            });
    }

    ////////////////////////////////////////////////////////////////////////////////
    // FILE CACHING
    ////////////////////////////////////////////////////////////////////////////////

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetFileFromCache)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto md5 = request->md5();

        TGetFileFromCacheOptions options;
        SetTimeoutOptions(&options, context.Get());

        options.CachePath = request->cache_path();
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_master_read_options()) {
            FromProto(&options, request->master_read_options());
        }

        context->SetRequestInfo("MD5: %v, CachePath: %v",
            md5,
            options.CachePath);

        ExecuteCall(
            context,
            [=] {
                return client->GetFileFromCache(md5, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                ToProto(response->mutable_result(), result);

                context->SetResponseInfo("Path: %v",
                    result.Path);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, PutFileToCache)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto path = request->path();
        auto md5 = request->md5();

        TPutFileToCacheOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());

        options.CachePath = request->cache_path();
        if (request->has_preserve_expiration_timeout()) {
            options.PreserveExpirationTimeout = request->preserve_expiration_timeout();
        }
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }
        if (request->has_master_read_options()) {
            FromProto(&options, request->master_read_options());
        }

        context->SetRequestInfo("Path: %v, MD5: %v, CachePath: %v",
            path,
            md5,
            options.CachePath);

        ExecuteCall(
            context,
            [=] {
                return client->PutFileToCache(path, md5, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                ToProto(response->mutable_result(), result);

                context->SetResponseInfo("Path: %v",
                    result.Path);
            });
    }

    ////////////////////////////////////////////////////////////////////////////////
    // FLOW
    ////////////////////////////////////////////////////////////////////////////////

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetPipelineSpec)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        TGetPipelineSpecOptions options;
        SetTimeoutOptions(&options, context.Get());

        auto pipelinePath = FromProto<TYPath>(request->pipeline_path());
        context->SetRequestInfo("PipelinePath: %v", pipelinePath);

        ExecuteCall(
            context,
            [=] {
                return client->GetPipelineSpec(pipelinePath, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->set_version(ToProto(result.Version));
                response->set_spec(result.Spec.ToString());

                context->SetResponseInfo("Version: %v",
                    result.Version);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, SetPipelineSpec)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        TSetPipelineSpecOptions options;
        SetTimeoutOptions(&options, context.Get());

        auto pipelinePath = FromProto<TYPath>(request->pipeline_path());

        auto spec = TYsonString(request->spec());

        options.Force = request->force();

        options.ExpectedVersion = request->has_expected_version()
            ? std::make_optional<NFlow::TVersion>(request->expected_version())
            : std::nullopt;

        context->SetRequestInfo("PipelinePath: %v, Force: %v, ExpectedVersion: %v",
            pipelinePath,
            options.Force,
            options.ExpectedVersion);

        ExecuteCall(
            context,
            [=] {
                return client->SetPipelineSpec(pipelinePath, spec, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->set_version(ToProto(result.Version));

                context->SetResponseInfo("Version: %v",
                    result.Version);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetPipelineDynamicSpec)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        TGetPipelineDynamicSpecOptions options;
        SetTimeoutOptions(&options, context.Get());

        auto pipelinePath = FromProto<TYPath>(request->pipeline_path());
        context->SetRequestInfo("PipelinePath: %v", pipelinePath);

        ExecuteCall(
            context,
            [=] {
                return client->GetPipelineDynamicSpec(pipelinePath, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->set_version(ToProto(result.Version));
                response->set_spec(result.Spec.ToString());

                context->SetResponseInfo("Version: %v",
                    result.Version);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, SetPipelineDynamicSpec)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        TSetPipelineDynamicSpecOptions options;
        SetTimeoutOptions(&options, context.Get());

        auto pipelinePath = FromProto<TYPath>(request->pipeline_path());

        auto spec = TYsonString(request->spec());

        options.ExpectedVersion = request->has_expected_version()
            ? std::make_optional<NFlow::TVersion>(request->expected_version())
            : std::nullopt;

        context->SetRequestInfo("PipelinePath: %v, ExpectedVersion: %v",
            pipelinePath,
            options.ExpectedVersion);

        ExecuteCall(
            context,
            [=] {
                return client->SetPipelineDynamicSpec(pipelinePath, spec, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->set_version(ToProto(result.Version));

                context->SetResponseInfo("Version: %v",
                    result.Version);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, StartPipeline)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        TStartPipelineOptions options;
        SetTimeoutOptions(&options, context.Get());

        auto pipelinePath = FromProto<TYPath>(request->pipeline_path());
        context->SetRequestInfo("PipelinePath: %v", pipelinePath);

        ExecuteCall(
            context,
            [=] {
                return client->StartPipeline(pipelinePath, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, StopPipeline)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        TStopPipelineOptions options;
        SetTimeoutOptions(&options, context.Get());

        auto pipelinePath = FromProto<TYPath>(request->pipeline_path());
        context->SetRequestInfo("PipelinePath: %v", pipelinePath);

        ExecuteCall(
            context,
            [=] {
                return client->StopPipeline(pipelinePath, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, PausePipeline)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        TPausePipelineOptions options;
        SetTimeoutOptions(&options, context.Get());

        auto pipelinePath = FromProto<TYPath>(request->pipeline_path());
        context->SetRequestInfo("PipelinePath: %v", pipelinePath);

        ExecuteCall(
            context,
            [=] {
                return client->PausePipeline(pipelinePath, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetPipelineState)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        TGetPipelineStateOptions options;
        SetTimeoutOptions(&options, context.Get());

        auto pipelinePath = FromProto<TYPath>(request->pipeline_path());
        context->SetRequestInfo("PipelinePath: %v", pipelinePath);

        ExecuteCall(
            context,
            [=] {
                return client->GetPipelineState(pipelinePath, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->set_state(ToProto(result.State));

                context->SetResponseInfo("State: %v",
                    result.State);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetFlowView)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        TGetFlowViewOptions options;
        SetTimeoutOptions(&options, context.Get());
        options.Cache = request->cache();

        auto pipelinePath = FromProto<TYPath>(request->pipeline_path());
        auto viewPath = FromProto<TYPath>(request->view_path());
        context->SetRequestInfo("PipelinePath: %v, ViewPath: %v",
            pipelinePath,
            viewPath);

        ExecuteCall(
            context,
            [=] {
                return client->GetFlowView(pipelinePath, viewPath, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->set_flow_view_part(result.FlowViewPart.ToString());
            });
    }

    ////////////////////////////////////////////////////////////////////////////////
    // QUERY TRACKER
    ////////////////////////////////////////////////////////////////////////////////

    template <class TRequest>
    TQueryTrackerServiceProxy GetQueryTrackerProxy(
        const IServiceContextPtr& context,
        const TRequest* request)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);
        return TQueryTrackerServiceProxy(
            client->GetNativeConnection()->GetQueryTrackerChannelOrThrow(request->query_tracker_stage()));
    }

    template <class TProxyRequest, class TQTRequestPtr>
    void FillQueryTrackerRequest(
        const IServiceContextPtr& context,
        const TProxyRequest* proxyRequest,
        const TQTRequestPtr qtRequest)
    {
        qtRequest->SetTimeout(context->GetTimeout());
        qtRequest->SetUser(context->GetAuthenticationIdentity().User);
        qtRequest->mutable_rpc_proxy_request()->MergeFrom(*proxyRequest);
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, StartQuery)
    {
        auto proxy = GetQueryTrackerProxy(context, request);

        auto req = proxy.StartQuery();
        FillQueryTrackerRequest(context, request, req);

        context->SetRequestInfo("Stage: %v, Engine: %v",
            request->query_tracker_stage(),
            ConvertQueryEngineFromProto(request->engine()));

        ExecuteCall(
            context,
            [=] {
                return req->Invoke();
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->MergeFrom(result->rpc_proxy_response());

                context->SetResponseInfo("QueryId: %v", response->query_id());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, AbortQuery)
    {
        auto proxy = GetQueryTrackerProxy(context, request);

        auto req = proxy.AbortQuery();
        FillQueryTrackerRequest(context, request, req);

        context->SetRequestInfo("Stage: %v, QueryId: %v",
            request->query_tracker_stage(),
            FromProto<NQueryTrackerClient::TQueryId>(request->query_id()));

        ExecuteCall(
            context,
            [=] {
                return req->Invoke();
            },
            [] (const auto& /*context*/, const auto& /*result*/) {
                // do nothing.
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetQueryResult)
    {
        auto proxy = GetQueryTrackerProxy(context, request);

        auto req = proxy.GetQueryResult();
        FillQueryTrackerRequest(context, request, req);

        context->SetRequestInfo("Stage: %v, QueryId: %v, ResultIndex: %v",
            request->query_tracker_stage(),
            FromProto<NQueryTrackerClient::TQueryId>(request->query_id()),
            request->result_index());

        ExecuteCall(
            context,
            [=] {
                return req->Invoke();
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->MergeFrom(result->rpc_proxy_response());

                context->SetResponseInfo("QueryId: %v", response->query_id());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ReadQueryResult)
    {
        auto proxy = GetQueryTrackerProxy(context, request);

        auto req = proxy.ReadQueryResult();
        FillQueryTrackerRequest(context, request, req);

        context->SetRequestInfo("Stage: %v, QueryId: %v, ResultIndex: %v",
            request->query_tracker_stage(),
            FromProto<NQueryTrackerClient::TQueryId>(request->query_id()),
            request->result_index());

        ExecuteCall(
            context,
            [=] {
                return req->Invoke();
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->MergeFrom(result->rpc_proxy_response());
                response->Attachments() = result->Attachments();
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetQuery)
    {
        auto proxy = GetQueryTrackerProxy(context, request);

        auto req = proxy.GetQuery();
        FillQueryTrackerRequest(context, request, req);

        context->SetRequestInfo("Stage: %v, QueryId: %v, StartTimestamp: %v",
            request->query_tracker_stage(),
            FromProto<NQueryTrackerClient::TQueryId>(request->query_id()),
            request->timestamp());

        ExecuteCall(
            context,
            [=] {
                return req->Invoke();
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->MergeFrom(result->rpc_proxy_response());

                context->SetResponseInfo("QueryId: %v", response->query().id());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ListQueries)
    {
        auto proxy = GetQueryTrackerProxy(context, request);

        auto req = proxy.ListQueries();
        FillQueryTrackerRequest(context, request, req);

        context->SetRequestInfo(
            "Stage: %v, Limit: %v",
            request->query_tracker_stage(),
            request->limit());

        ExecuteCall(
            context,
            [=] {
                return req->Invoke();
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->MergeFrom(result->rpc_proxy_response());

                context->SetResponseInfo("QueryCount: %v, Incomplete: %v, Timestamp: %v",
                    response->queries_size(),
                    response->incomplete(),
                    response->timestamp());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, AlterQuery)
    {
        auto proxy = GetQueryTrackerProxy(context, request);

        auto req = proxy.AlterQuery();
        FillQueryTrackerRequest(context, request, req);

        context->SetRequestInfo("Stage: %v, QueryId: %v",
            request->query_tracker_stage(),
            FromProto<NQueryTrackerClient::TQueryId>(request->query_id()));

        ExecuteCall(
            context,
            [=] {
                return req->Invoke();
            },
            [] (const auto& /*context*/, const auto& /*result*/) {
                // do nothing.
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetQueryTrackerInfo)
    {
        auto proxy = GetQueryTrackerProxy(context, request);

        auto req = proxy.GetQueryTrackerInfo();
        FillQueryTrackerRequest(context, request, req);

        context->SetRequestInfo("Stage: %v", request->query_tracker_stage());

        ExecuteCall(
            context,
            [=] {
                return req->Invoke();
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->MergeFrom(result->rpc_proxy_response());

                context->SetResponseInfo("ClusterName: %v", response->cluster_name());
            });
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Shuffle Service
    ////////////////////////////////////////////////////////////////////////////////

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, StartShuffle)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto parentTransactionId = FromProto<TTransactionId>(request->parent_transaction_id());

        context->SetRequestInfo(
            "PartitionCount: %v, Account: %v, ParentTransactionId: %v",
            request->partition_count(),
            request->account(),
            parentTransactionId);

        auto user = context->GetAuthenticationIdentity().User;

        auto checkResult = WaitFor(client->CheckPermission(
            user,
            GetAccountPath(request->account()),
            EPermission::Use))
            .ValueOrThrow();

        if (checkResult.Action == ESecurityAction::Deny) {
            THROW_ERROR_EXCEPTION("User %Qv has been denied %Qlv access to account %Qv",
                user,
                EPermission::Use,
                request->account());
        }

        ExecuteCall(
            context,
            [client = std::move(client), request, parentTransactionId] () {
                TStartShuffleOptions options;
                if (request->has_medium()) {
                    options.Medium = request->medium();
                }
                if (request->has_replication_factor()) {
                    options.ReplicationFactor = request->replication_factor();
                }
                return client->StartShuffle(
                    request->account(),
                    request->partition_count(),
                    parentTransactionId,
                    std::move(options));
            },
            [] (const auto& context, const auto& shuffleHandle) {
                auto* response = &context->Response();
                response->set_shuffle_handle(ConvertToYsonString(shuffleHandle).ToString());
                context->SetResponseInfo("TransactionId: %v", shuffleHandle->TransactionId);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ReadShuffleData)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto shuffleHandle = ConvertTo<TShuffleHandlePtr>(TYsonString(request->shuffle_handle()));

        context->SetRequestInfo(
            "TransactionId: %v, CoordinatorAddress: %v, Account: %v, PartitionCount: %v, PartitionIndex: %v",
            shuffleHandle->TransactionId,
            shuffleHandle->CoordinatorAddress,
            shuffleHandle->Account,
            shuffleHandle->PartitionCount,
            request->partition_index());

        auto readerConfig = ConvertTo<NTableClient::TTableReaderConfigPtr>(TYsonString(request->reader_config()));

        auto reader = WaitFor(client->CreateShuffleReader(
            shuffleHandle,
            request->partition_index(),
            readerConfig))
            .ValueOrThrow();

        auto encoder = CreateWireRowStreamEncoder(reader->GetNameTable());

        auto config = Config_.Acquire();

        bool finished = false;

        HandleInputStreamingRequest(
            context,
            [&] {
                if (finished) {
                    return TSharedRef();
                }

                TRowBatchReadOptions options{
                    .MaxRowsPerRead = config->ReadBufferRowCount,
                    .MaxDataWeightPerRead = config->ReadBufferDataWeight,
                };
                auto batch = ReadRowBatch(reader, options);
                if (!batch) {
                    finished = true;
                }

                return encoder->Encode(
                    batch ? batch : CreateEmptyUnversionedRowBatch(),
                    nullptr);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, WriteShuffleData)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto writerConfig = ConvertTo<NTableClient::TTableWriterConfigPtr>(TYsonString(request->writer_config()));

        auto shuffleHandle = ConvertTo<TShuffleHandlePtr>(TYsonString(request->shuffle_handle()));

        auto partitionColumn = request->partition_column();

        context->SetRequestInfo(
            "TransactionId: %v, CoordinatorAddress: %v, Account: %v, PartitionCount: %v, PartitionColumn: %v",
            shuffleHandle->TransactionId,
            shuffleHandle->CoordinatorAddress,
            shuffleHandle->Account,
            shuffleHandle->PartitionCount,
            partitionColumn);

        auto writer = WaitFor(
            client->CreateShuffleWriter(shuffleHandle, partitionColumn, writerConfig))
            .ValueOrThrow();

        auto decoder = CreateWireRowStreamDecoder(writer->GetNameTable());

        HandleOutputStreamingRequest(
            context,
            [&] (const TSharedRef& block) {
                NApi::NRpcProxy::NProto::TRowsetDescriptor descriptor;
                auto payloadRef = DeserializeRowStreamBlockEnvelope(block, &descriptor, nullptr);

                auto batch = decoder->Decode(payloadRef, descriptor);

                auto rows = batch->MaterializeRows();

                if (!writer->Write(rows)) {
                    WaitFor(writer->GetReadyEvent())
                        .ThrowOnError();
                }
            },
            [&] {
                WaitFor(writer->Close())
                    .ThrowOnError();
            });
    }

    ////////////////////////////////////////////////////////////////////////////////
    // MISC
    ////////////////////////////////////////////////////////////////////////////////

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, CheckClusterLiveness)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        TCheckClusterLivenessOptions options;
        SetTimeoutOptions(&options, context.Get());

        options.CheckCypressRoot = request->check_cypress_root();
        options.CheckSecondaryMasterCells = request->check_secondary_master_cells();
        if (request->has_check_tablet_cell_bundle()) {
            options.CheckTabletCellBundle = request->check_tablet_cell_bundle();
        }

        context->SetRequestInfo("CheckCypressRoot: %v, CheckSecondaryMasterCells: %v, CheckTabletCellBundle: %v",
            options.CheckCypressRoot,
            options.CheckSecondaryMasterCells,
            options.CheckTabletCellBundle);

        ExecuteCall(
            context,
            [=] {
                return client->CheckClusterLiveness(options);
            });
    }

    ////////////////////////////////////////////////////////////////////////////////

    bool IsUp(const TCtxDiscoverPtr& /*context*/) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return ProxyCoordinator_->GetOperableState();
    }
};

DEFINE_REFCOUNTED_TYPE(TApiService)

const TStructuredLoggingMethodDynamicConfigPtr TApiService::DefaultMethodConfig = New<TStructuredLoggingMethodDynamicConfig>();

////////////////////////////////////////////////////////////////////////////////

IApiServicePtr CreateApiService(
    TApiServiceConfigPtr config,
    IInvokerPtr controlInvoker,
    IInvokerPtr workerInvoker,
    NApi::NNative::IConnectionPtr connection,
    NRpc::IAuthenticatorPtr authenticator,
    IProxyCoordinatorPtr proxyCoordinator,
    IAccessCheckerPtr accessChecker,
    ISecurityManagerPtr securityManager,
    NTracing::TSamplerPtr traceSampler,
    NLogging::TLogger logger,
    TProfiler profiler,
    ISignatureValidatorPtr signatureValidator,
    ISignatureGeneratorPtr signatureGenerator,
    INodeMemoryTrackerPtr memoryUsageTracker,
    IStickyTransactionPoolPtr stickyTransactionPool,
    IQueryCorpusReporterPtr queryCorpusReporter)
{
    YT_VERIFY(signatureValidator && signatureGenerator);
    return New<TApiService>(
        std::move(config),
        std::move(controlInvoker),
        std::move(workerInvoker),
        std::move(connection),
        std::move(authenticator),
        std::move(proxyCoordinator),
        std::move(accessChecker),
        std::move(securityManager),
        std::move(traceSampler),
        std::move(logger),
        std::move(profiler),
        std::move(memoryUsageTracker),
        std::move(stickyTransactionPool),
        std::move(signatureValidator),
        std::move(signatureGenerator),
        std::move(queryCorpusReporter));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
