#include "data_node_service.h"

#include "bootstrap.h"
#include "private.h"
#include "ally_replica_manager.h"
#include "chunk.h"
#include "chunk_registry.h"
#include "chunk_store.h"
#include "chunk_meta_manager.h"
#include "config.h"
#include "location.h"
#include "location_manager.h"
#include "network_statistics.h"
#include "p2p.h"
#include "session.h"
#include "session_manager.h"
#include "master_connector.h"
#include "offloaded_chunk_read_session.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/node/tablet_node/sorted_dynamic_comparer.h>
#include <yt/yt/server/node/tablet_node/versioned_chunk_meta_manager.h>

#include <yt/yt/server/lib/io/io_engine.h>
#include <yt/yt/server/lib/io/chunk_file_reader.h>
#include <yt/yt/server/lib/io/chunk_fragment.h>

#include <yt/yt/server/lib/rpc/per_workload_category_request_queue_provider.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/proto/data_node_service.pb.h>
#include <yt/yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/table_client/cached_versioned_chunk_meta.h>
#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/chunk_slice.h>
#include <yt/yt/ytlib/table_client/chunk_state.h>
#include <yt/yt/ytlib/table_client/key_set.h>
#include <yt/yt/ytlib/table_client/helpers.h>
#include <yt/yt/ytlib/table_client/samples_fetcher.h>
#include <yt/yt/ytlib/table_client/versioned_chunk_reader.h>

#include <yt/yt/client/api/rpc_proxy/helpers.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/versioned_reader.h>
#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/client/rpc/helpers.h>

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/client/misc/workload.h>
#include <yt/yt/client/misc/io_tags.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_spec.pb.h>

#include <yt/yt/core/bus/bus.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/random.h>
#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/utilex/random.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <cmath>
#include <optional>

namespace NYT::NDataNode {

using namespace NRpc;
using namespace NIO;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NClusterNode;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NProfiling;
using namespace NTracing;

using NChunkClient::TChunkReaderStatistics;
using NYT::ToProto;
using NYT::FromProto;

using TRefCountedColumnarStatisticsSubresponse = TRefCountedProto<TRspGetColumnarStatistics::TSubresponse>;
using TRefCountedColumnarStatisticsSubresponsePtr = TIntrusivePtr<TRefCountedColumnarStatisticsSubresponse>;

////////////////////////////////////////////////////////////////////////////////

namespace {

THashMap<TString, TString> MakeWriteIOTags(
    TString method,
    const ISessionPtr& session,
    const IServiceContextPtr& context)
{
    auto& location = session->GetStoreLocation();
    return {
        {FormatIOTag(EAggregateIOTag::DataNodeMethod), std::move(method)},
        {FormatIOTag(ERawIOTag::WriteSessionId), ToString(session->GetId())},
        {FormatIOTag(ERawIOTag::LocationId), ToString(location->GetId())},
        {FormatIOTag(EAggregateIOTag::LocationType), FormatEnum(location->GetType())},
        {FormatIOTag(EAggregateIOTag::Medium), location->GetMediumName()},
        {FormatIOTag(EAggregateIOTag::DiskFamily), location->GetDiskFamily()},
        // TODO(babenko): switch to std::string
        {FormatIOTag(EAggregateIOTag::User), TString(context->GetAuthenticationIdentity().User)},
        {FormatIOTag(EAggregateIOTag::Direction), "write"},
        {FormatIOTag(ERawIOTag::ChunkId), ToString(DecodeChunkId(session->GetChunkId()).Id)},
    };
}

THashMap<TString, TString> MakeReadIOTags(
    TString method,
    const TChunkLocationPtr& location,
    const IServiceContextPtr& context,
    const TChunkId& chunkId,
    TGuid readSessionId = TGuid())
{
    THashMap<TString, TString> result{
        {FormatIOTag(EAggregateIOTag::DataNodeMethod), std::move(method)},
        {FormatIOTag(ERawIOTag::LocationId), ToString(location->GetId())},
        {FormatIOTag(EAggregateIOTag::LocationType), FormatEnum(location->GetType())},
        {FormatIOTag(EAggregateIOTag::Medium), location->GetMediumName()},
        {FormatIOTag(EAggregateIOTag::DiskFamily), location->GetDiskFamily()},
        // TODO(babenko): switch to std::string
        {FormatIOTag(EAggregateIOTag::User), TString(context->GetAuthenticationIdentity().User)},
        {FormatIOTag(EAggregateIOTag::Direction), "read"},
    };
    if (chunkId) {
        result[FormatIOTag(ERawIOTag::ChunkId)] = ToString(DecodeChunkId(chunkId).Id);
    }
    if (readSessionId) {
        result[FormatIOTag(ERawIOTag::ReadSessionId)] = ToString(readSessionId);
    }
    return result;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TDataNodeService
    : public TServiceBase
{
public:
    TDataNodeService(
        TDataNodeConfigPtr config,
        IBootstrap* bootstrap)
        : TServiceBase(
            bootstrap->GetStorageLightInvoker(),
            TDataNodeServiceProxy::GetDescriptor(),
            DataNodeLogger(),
            TServiceOptions{
                .Authenticator = bootstrap->GetNativeAuthenticator(),
            })
        , Config_(std::move(config))
        , DynamicConfigManager_(bootstrap->GetDynamicConfigManager())
        , Bootstrap_(bootstrap)
    {
        YT_VERIFY(Config_);
        YT_VERIFY(Bootstrap_);

        // TODO(prime): disable RPC attachment checksums for methods receiving/returning blocks
        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartChunk));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FinishChunk)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CancelChunk));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PutBlocks)
            .SetQueueSizeLimit(100)
            .SetConcurrencyLimit(100)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SendBlocks)
            .SetQueueSizeLimit(5'000)
            .SetConcurrencyLimit(5'000)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(UpdateP2PBlocks)
            .SetQueueSizeLimit(5'000)
            .SetConcurrencyLimit(5'000));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FlushBlocks)
            .SetQueueSizeLimit(5'000)
            .SetConcurrencyLimit(5'000)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PingSession));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ProbeChunkSet)
            .SetInvoker(Bootstrap_->GetStorageLookupInvoker())
            .SetQueueSizeLimit(5'000)
            .SetConcurrencyLimit(5'000));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ProbeBlockSet)
            .SetQueueSizeLimit(5'000)
            .SetConcurrencyLimit(5'000));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetBlockSet)
            .SetCancelable(true)
            .SetQueueSizeLimit(1'000)
            .SetConcurrencyLimit(1'000)
            .SetRequestQueueProvider(GetBlockSetQueue_));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetBlockRange)
            .SetCancelable(true)
            .SetQueueSizeLimit(1'000)
            .SetConcurrencyLimit(1'000)
            .SetRequestQueueProvider(GetBlockRangeQueue_));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChunkFragmentSet)
            .SetInvoker(Bootstrap_->GetStorageLookupInvoker())
            .SetQueueSizeLimit(100'000)
            .SetConcurrencyLimit(100'000)
            .SetRequestQueueProvider(GetChunkFragmentSetQueue_));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(LookupRows)
            .SetInvoker(Bootstrap_->GetStorageLookupInvoker())
            .SetCancelable(true)
            .SetQueueSizeLimit(5'000)
            .SetConcurrencyLimit(5'000));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChunkMeta)
            .SetCancelable(true)
            .SetQueueSizeLimit(5'000)
            .SetConcurrencyLimit(5'000)
            .SetHeavy(true)
            .SetRequestQueueProvider(GetChunkMetaQueue_));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChunkSliceDataWeights)
            .SetCancelable(true)
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(UpdatePeer));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetTableSamples)
            .SetCancelable(true)
            .SetHeavy(true)
            .SetResponseCodec(NCompression::ECodec::Lz4));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChunkSlices)
            .SetCancelable(true)
            .SetHeavy(true)
            .SetResponseCodec(NCompression::ECodec::Lz4));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetColumnarStatistics)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(DisableChunkLocations)
            .SetInvoker(Bootstrap_->GetControlInvoker()));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(DestroyChunkLocations)
            .SetInvoker(Bootstrap_->GetControlInvoker()));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ResurrectChunkLocations)
            .SetInvoker(Bootstrap_->GetControlInvoker()));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AnnounceChunkReplicas)
            .SetInvoker(Bootstrap_->GetStorageLightInvoker()));
    }

private:
    struct TGetBlockResponseTemplate
    {
        IChunkPtr Chunk;
        TWorkloadDescriptor WorkloadDescriptor;
        bool HasCompleteChunk;
        bool NetThrottling;
        long NetQueueSize;
        bool DiskThrottling;
        long DiskQueueSize;
        bool ThrottledLargeBlock;
        TChunkReaderStatisticsPtr ChunkReaderStatistics;
    };

    const TDataNodeConfigPtr Config_;
    const TClusterNodeDynamicConfigManagerPtr DynamicConfigManager_;
    IBootstrap* const Bootstrap_;

    IRequestQueueProviderPtr GetBlockSetQueue_ = CreatePerWorkloadCategoryRequestQueueProvider();
    IRequestQueueProviderPtr GetBlockRangeQueue_ = CreatePerWorkloadCategoryRequestQueueProvider();
    IRequestQueueProviderPtr GetChunkFragmentSetQueue_ = CreatePerWorkloadCategoryRequestQueueProvider();
    IRequestQueueProviderPtr GetChunkMetaQueue_ = CreatePerWorkloadCategoryRequestQueueProvider();

    TDataNodeDynamicConfigPtr GetDynamicConfig() const
    {
        return DynamicConfigManager_->GetConfig()->DataNode;
    }

    std::optional<double> GetFallbackTimeoutFraction() const
    {
        return GetDynamicConfig()->FallbackTimeoutFraction;
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, StartChunk)
    {
        auto sessionId = FromProto<TSessionId>(request->session_id());

        TSessionOptions options;
        options.WorkloadDescriptor = GetRequestWorkloadDescriptor(context);
        options.SyncOnClose = request->sync_on_close();
        options.EnableMultiplexing = request->enable_multiplexing();
        options.PlacementId = FromProto<TPlacementId>(request->placement_id());
        options.DisableSendBlocks = GetDynamicConfig()->UseDisableSendBlocks && request->disable_send_blocks();

        context->SetRequestInfo("ChunkId: %v, Workload: %v, SyncOnClose: %v, EnableMultiplexing: %v, PlacementId: %v, DisableSendBlocks: %v",
            sessionId,
            options.WorkloadDescriptor,
            options.SyncOnClose,
            options.EnableMultiplexing,
            options.PlacementId,
            options.DisableSendBlocks);

        ValidateOnline();

        const auto& sessionManager = Bootstrap_->GetSessionManager();
        auto session = sessionManager->StartSession(sessionId, options);
        ToProto(response->mutable_location_uuid(), session->GetStoreLocation()->GetUuid());
        context->ReplyFrom(session->Start());
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, FinishChunk)
    {
        auto sessionId = FromProto<TSessionId>(request->session_id());
        auto blockCount = request->has_block_count() ? std::make_optional(request->block_count()) : std::nullopt;
        bool ignoreMissingSession = request->ignore_missing_session();

        // COMPAT(babenko): This flag is only used for quorum journal session abort.
        // Starting from 24.2, master will be sending AllMediaIndex by itself.
        // Starting from 25.1, this compat may be removed.
        if (ignoreMissingSession) {
            sessionId.MediumIndex = AllMediaIndex;
        }

        context->SetRequestInfo("ChunkId: %v, BlockCount: %v, IgnoreMissingSession: %v",
            sessionId,
            blockCount,
            ignoreMissingSession);

        ValidateOnline();

        const auto& sessionManager = Bootstrap_->GetSessionManager();
        auto session = ignoreMissingSession
            ? sessionManager->FindSession(sessionId)
            : sessionManager->GetSessionOrThrow(sessionId);
        if (!session) {
            YT_VERIFY(ignoreMissingSession);
            YT_LOG_DEBUG("Session is missing (SessionId: %v)",
                sessionId);
            context->Reply();
            return;
        }

        auto meta = request->has_chunk_meta()
            ? New<TRefCountedChunkMeta>(std::move(*request->mutable_chunk_meta()))
            : nullptr;
        session->Finish(meta, blockCount)
            .Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<TChunkInfo>& chunkInfoOrError) {
                if (!chunkInfoOrError.IsOK()) {
                    context->Reply(chunkInfoOrError);
                    return;
                }
                const auto& chunkInfo = chunkInfoOrError.Value();

                // Log IO events for blob chunks only. We don't enable logging for journal chunks here, since
                // they flush the data to disk in FlushBlocks(), not in FinishChunk().
                const auto& ioTracker = Bootstrap_->GetIOTracker();
                bool isBlobChunk = IsBlobChunkId(DecodeChunkId(session->GetChunkId()).Id);
                if (isBlobChunk && chunkInfo.disk_space() > 0 && ioTracker->IsEnabled()) {
                    ioTracker->Enqueue(
                        TIOCounters{.Bytes = chunkInfo.disk_space(), .IORequests = 1},
                        MakeWriteIOTags("FinishChunk", session, context));
                }

                *response->mutable_chunk_info() = chunkInfo;
                context->Reply();
            }));
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, CancelChunk)
    {
        const auto& config = GetDynamicConfig()->TestingOptions;
        if (config->ChunkCancellationDelay) {
            NConcurrency::TDelayedExecutor::WaitForDuration(config->ChunkCancellationDelay.value());
        }

        auto sessionId = FromProto<TSessionId>(request->session_id());
        bool waitForCancelation = request->wait_for_cancelation();

        context->SetRequestInfo("ChunkId: %v",
            sessionId);

        const auto& sessionManager = Bootstrap_->GetSessionManager();
        auto session = sessionManager->FindSession(sessionId);
        if (!session) {
            context->Reply();
            return;
        }

        session->Cancel(TError("Canceled by client request"));

        if (waitForCancelation) {
            context->ReplyFrom(session->GetUnregisteredEvent());
        } else {
            context->Reply();
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, PingSession)
    {
        auto sessionId = FromProto<TSessionId>(request->session_id());
        context->SetRequestInfo("ChunkId: %v",
            sessionId);

        const auto& sessionManager = Bootstrap_->GetSessionManager();
        auto session = sessionManager->GetSessionOrThrow(sessionId);
        const auto& location = session->GetStoreLocation();

        session->Ping();

        response->set_close_demanded(IsCloseDemanded(location));

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, PutBlocks)
    {
        auto sessionId = FromProto<TSessionId>(request->session_id());
        int firstBlockIndex = request->first_block_index();
        int blockCount = static_cast<int>(request->Attachments().size());
        int lastBlockIndex = firstBlockIndex + blockCount - 1;
        bool populateCache = request->populate_cache();
        bool flushBlocks = request->flush_blocks();
        i64 cumulativeBlockSize = request->cumulative_block_size();

        ValidateOnline();

        const auto& sessionManager = Bootstrap_->GetSessionManager();
        auto session = sessionManager->GetSessionOrThrow(sessionId);
        const auto& location = session->GetStoreLocation();
        auto options = session->GetSessionOptions();
        auto blocksWindowShifted = cumulativeBlockSize == 0 || cumulativeBlockSize != 0 && firstBlockIndex >= session->GetWindowSize();

        context->SetRequestInfo(
            "BlockIds: %v:%v-%v, PopulateCache: %v, "
            "FlushBlocks: %v, Medium: %v, "
            "DisableSendBlocks: %v, CumulativeBlockSize: %v, BlocksWindowShifted: %v",
            sessionId,
            firstBlockIndex,
            lastBlockIndex,
            populateCache,
            flushBlocks,
            location->GetMediumName(),
            options.DisableSendBlocks,
            cumulativeBlockSize,
            blocksWindowShifted);

        auto throttlingResult = location->CheckWriteThrottling(
            session->GetId(),
            session->GetWorkloadDescriptor(),
            blocksWindowShifted);
        throttlingResult.Error.ThrowOnError();

        TWallTimer timer;

        response->set_close_demanded(IsCloseDemanded(location));

        // NB: block checksums are validated before writing to disk.
        auto result = session->PutBlocks(
            firstBlockIndex,
            GetRpcAttachedBlocks(request, /*validateChecksums*/ false),
            cumulativeBlockSize,
            populateCache);

        // Flush blocks if needed.
        if (flushBlocks) {
            result = result
                .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TIOCounters& putCounters) mutable {
                    response->mutable_statistics()->set_data_bytes_written_to_medium(putCounters.Bytes);
                    response->mutable_statistics()->set_io_requests(putCounters.IORequests);

                    auto result = session->FlushBlocks(lastBlockIndex);
                    result.Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<TIOCounters>& result) {
                        if (!result.IsOK()) {
                            return;
                        }
                        // Log IO events for journal chunks only. We don't enable logging for blob chunks here, since they flush
                        // the data to disk in FinishChunk().
                        const auto& ioTracker = Bootstrap_->GetIOTracker();
                        const auto& counters = result.ValueOrThrow();
                        bool isJournalChunk = IsJournalChunkId(DecodeChunkId(session->GetChunkId()).Id);
                        if (isJournalChunk && counters.Bytes > 0 && ioTracker->IsEnabled()) {
                            ioTracker->Enqueue(
                                counters,
                                MakeWriteIOTags("FlushBlocks", session, context));
                        }
                    }));
                    return result;
                }));
        } else {
            // Remains to wait on throttlers, hence mark as complete.
            context->SetComplete();
        }

        result.Subscribe(BIND([=] (const TError& error) {
            if (!error.IsOK()) {
                return;
            }
            location->GetPerformanceCounters().PutBlocksWallTime.Record(timer.GetElapsedTime());
        }));

        context->ReplyFrom(result.AsVoid(), Bootstrap_->GetStorageLightInvoker());
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, SendBlocks)
    {
        auto sessionId = FromProto<TSessionId>(request->session_id());
        int firstBlockIndex = request->first_block_index();
        int blockCount = request->block_count();
        int lastBlockIndex = firstBlockIndex + blockCount - 1;
        i64 cumulativeBlockSize = request->cumulative_block_size();
        auto targetDescriptor = FromProto<TNodeDescriptor>(request->target_descriptor());

        context->SetRequestInfo("BlockIds: %v:%v-%v, CumulativeBlockSize: %v, Target: %v",
            sessionId,
            firstBlockIndex,
            lastBlockIndex,
            cumulativeBlockSize,
            targetDescriptor);

        ValidateOnline();

        const auto& sessionManager = Bootstrap_->GetSessionManager();
        auto session = sessionManager->GetSessionOrThrow(sessionId);
        session->SendBlocks(firstBlockIndex, blockCount, cumulativeBlockSize, targetDescriptor)
            .Subscribe(BIND([=] (const TDataNodeServiceProxy::TErrorOrRspPutBlocksPtr& errorOrRsp) {
                if (errorOrRsp.IsOK()) {
                    response->set_close_demanded(errorOrRsp.Value()->close_demanded());
                    context->Reply();
                } else {
                    context->Reply(TError(
                        NChunkClient::EErrorCode::SendBlocksFailed,
                        "Error putting blocks to %v",
                        targetDescriptor.GetDefaultAddress())
                        << errorOrRsp);
                }
            }));
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, FlushBlocks)
    {
        auto sessionId = FromProto<TSessionId>(request->session_id());
        int blockIndex = request->block_index();

        context->SetRequestInfo("BlockId: %v:%v",
            sessionId,
            blockIndex);

        ValidateOnline();

        const auto& sessionManager = Bootstrap_->GetSessionManager();
        auto session = sessionManager->GetSessionOrThrow(sessionId);
        const auto& location = session->GetStoreLocation();
        auto result = session->FlushBlocks(blockIndex);

        response->set_close_demanded(location->IsSick() || sessionManager->GetDisableWriteSessions());
        result.Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<TIOCounters>& result) {
            if (result.IsOK()) {
                // Log IO events for journal chunks only. We don't enable logging for blob chunks here, since they flush
                // the data to disk in FinishChunk(), not in FlushBlocks().
                const auto& ioTracker = Bootstrap_->GetIOTracker();
                const auto& counters = result.Value();
                if (IsJournalChunkId(DecodeChunkId(session->GetChunkId()).Id) && counters.Bytes > 0 && ioTracker->IsEnabled()) {
                    ioTracker->Enqueue(
                        counters,
                        MakeWriteIOTags("FlushBlocks", session, context));
                }
            }
        }));
        context->ReplyFrom(result.AsVoid());
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, UpdateP2PBlocks)
    {
        auto sessionId = FromProto<TGuid>(request->session_id());
        context->SetRequestInfo("SessionId: %v, Iteration: %v, ReceivedBlockCount: %v",
            sessionId,
            request->iteration(),
            request->block_indexes_size());

        ValidateOnline();

        const auto& blockCache = Bootstrap_->GetP2PBlockCache();

        auto blocks = GetRpcAttachedBlocks(request, true /*validateChecksums*/);
        if (std::ssize(blocks) != request->block_indexes_size()) {
            THROW_ERROR_EXCEPTION("Number of attached blocks is different from blocks field length")
                << TErrorAttribute("attached_block_count", blocks.size())
                << TErrorAttribute("blocks_length", request->block_indexes_size());
        }

        if (request->chunk_ids_size() != request->chunk_block_count_size()) {
            THROW_ERROR_EXCEPTION("Invalid block count")
                << TErrorAttribute("chunk_count", request->chunk_ids_size())
                << TErrorAttribute("block_count", request->chunk_block_count_size());
        }

        int j = 0;
        for (int i = 0; i < request->chunk_ids_size(); i++) {
            auto chunkId = FromProto<TChunkId>(request->chunk_ids(i));

            auto blockCount = request->chunk_block_count(i);

            std::vector<int> blockIndexes;
            std::vector<NChunkClient::TBlock> chunkBlocks;

            for (int k = 0; k < blockCount; k++) {
                if (j + k >= std::ssize(blocks)) {
                    THROW_ERROR_EXCEPTION("Invalid chunk block count");
                }

                blockIndexes.push_back(request->block_indexes(j + k));
                chunkBlocks.push_back(blocks[j + k]);
            }

            blockCache->HoldBlocks(
                chunkId,
                blockIndexes,
                chunkBlocks);

            j += blockCount;
        }

        blockCache->FinishSessionIteration(sessionId, request->iteration());

        context->Reply();
    }

    template <class TContext>
    void SuggestAllyReplicas(const TContext& context)
    {
        const auto& allyReplicaManager = Bootstrap_->GetAllyReplicaManager();

        const auto* request = &context->Request();
        auto* response = &context->Response();
        auto chunkId = FromProto<TChunkId>(request->chunk_id());

        if (auto allyReplicas = allyReplicaManager->GetAllyReplicas(chunkId)) {
            if (allyReplicas.Revision > request->ally_replicas_revision()) {
                ToProto(response->mutable_ally_replicas(), allyReplicas);
                YT_LOG_DEBUG("Ally replicas suggested "
                    "(ChunkId: %v, AllyReplicas: %v, ClientAllyReplicasRevision: %v)",
                    chunkId,
                    allyReplicas,
                    request->ally_replicas_revision());
            }
        }
    }

    TFuture<void> WaitP2PBarriers(const TReqGetBlockSet* request)
    {
        const auto& p2pBlockCache = Bootstrap_->GetP2PBlockCache();
        std::vector<TFuture<void>> barrierFutures;

        for (const auto& barrier : request->wait_barriers()) {
            if (static_cast<TNodeId>(barrier.if_node_id()) != Bootstrap_->GetNodeId()) {
                continue;
            }

            auto p2pSessionId = FromProto<TGuid>(barrier.session_id());
            auto barrierFuture = p2pBlockCache->WaitSessionIteration(p2pSessionId, barrier.iteration());

            if (!barrierFuture.IsSet()) {
                YT_LOG_DEBUG("Waiting for P2P barrier (SessionId: %v, Iteration: %v)",
                    p2pSessionId,
                    barrier.iteration());
                barrierFutures.push_back(barrierFuture);
            }
        }

        return AllSucceeded(barrierFutures);
    }

    void AddBlockPeers(
        google::protobuf::RepeatedPtrField<TPeerDescriptor>* peers,
        const std::vector<TP2PSuggestion>& blockPeers)
    {
        for (const auto& suggestion : blockPeers) {
            auto peerDescriptor = peers->Add();

            peerDescriptor->set_block_index(suggestion.BlockIndex);
            ToProto(peerDescriptor->mutable_node_ids(), suggestion.Peers);

            auto barrier = peerDescriptor->mutable_delivery_barier();

            barrier->set_iteration(suggestion.P2PIteration);
            ToProto(barrier->mutable_session_id(), suggestion.P2PSessionId);
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, ProbeChunkSet)
    {
        auto workloadDescriptor = GetRequestWorkloadDescriptor(context);

        auto chunkCount = request->chunk_ids_size();
        context->SetRequestInfo("ChunkCount: %v, Workload: %v",
            chunkCount,
            workloadDescriptor);

        ValidateOnline();

        YT_VERIFY(
            request->ally_replicas_revisions_size() == 0 ||
            request->ally_replicas_revisions_size() == request->chunk_ids_size());

        const auto& chunkRegistry = Bootstrap_->GetChunkRegistry();

        int completeChunkCount = 0;
        for (int chunkIndex = 0; chunkIndex < request->chunk_ids_size(); ++chunkIndex) {
            auto chunkId = FromProto<TChunkId>(request->chunk_ids(chunkIndex));

            auto* subresponse = response->add_subresponses();

            auto chunk = chunkRegistry->FindChunk(chunkId);

            bool hasCompleteChunk = chunk.operator bool();
            subresponse->set_has_complete_chunk(hasCompleteChunk);
            if (hasCompleteChunk) {
                ++completeChunkCount;
            }

            auto diskThrottling = chunk
                ? chunk->GetLocation()->CheckReadThrottling(workloadDescriptor, /*incrementCounter*/ false)
                : TChunkLocation::TDiskThrottlingResult{.Enabled = false, .QueueSize = 0};
            subresponse->set_disk_throttling(diskThrottling.Enabled);
            subresponse->set_disk_queue_size(diskThrottling.QueueSize);

            YT_LOG_DEBUG_UNLESS(diskThrottling.Error.IsOK(), diskThrottling.Error);

            const auto& allyReplicaManager = Bootstrap_->GetAllyReplicaManager();
            if (auto allyReplicas = allyReplicaManager->GetAllyReplicas(chunkId)) {
                // COMPAT(akozhikhov): Empty revision list.
                if (request->ally_replicas_revisions_size() == 0 ||
                    request->ally_replicas_revisions(chunkIndex) < allyReplicas.Revision)
                {
                    auto clientAllyReplicasRevision = request->ally_replicas_revisions_size() == 0
                        ? std::nullopt
                        : std::make_optional(request->ally_replicas_revisions(chunkIndex));

                    ToProto(subresponse->mutable_ally_replicas(), allyReplicas);
                    YT_LOG_DEBUG("Ally replicas suggested "
                        "(ChunkId: %v, AllyReplicas: %v, ClientAllyReplicasRevision: %v)",
                        chunkId,
                        allyReplicas,
                        clientAllyReplicasRevision);
                }
            }
        }

        auto netThrottling = CheckNetOutThrottling(
            context,
            workloadDescriptor,
            /*incrementCounter*/ false);
        response->set_net_throttling(netThrottling.Enabled);
        response->set_net_queue_size(netThrottling.QueueSize);

        context->SetResponseInfo(
            "ChunkCount: %v, CompleteChunkCount: %v, "
            "NetThrottling: %v, NetQueueSize: %v",
            chunkCount,
            completeChunkCount,
            netThrottling.Enabled,
            netThrottling.QueueSize);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, ProbeBlockSet)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        auto blockIndexes = FromProto<std::vector<int>>(request->block_indexes());
        auto workloadDescriptor = GetRequestWorkloadDescriptor(context);

        context->SetRequestInfo("BlockIds: %v:%v, Workload: %v",
            chunkId,
            MakeShrunkFormattableView(blockIndexes, TDefaultFormatter(), 3),
            workloadDescriptor);

        ValidateOnline();

        const auto& chunkRegistry = Bootstrap_->GetChunkRegistry();
        auto chunk = chunkRegistry->FindChunk(chunkId);

        bool hasCompleteChunk = chunk.operator bool();
        response->set_has_complete_chunk(hasCompleteChunk);

        auto diskThrottling = chunk
            ? chunk->GetLocation()->CheckReadThrottling(workloadDescriptor, /*incrementCounter*/ false)
            : TChunkLocation::TDiskThrottlingResult{.Enabled = false, .QueueSize = 0};
        response->set_disk_throttling(diskThrottling.Enabled);
        response->set_disk_queue_size(diskThrottling.QueueSize);

        YT_LOG_DEBUG_UNLESS(diskThrottling.Error.IsOK(), diskThrottling.Error);

        auto netThrottling = CheckNetOutThrottling(
            context,
            workloadDescriptor,
            /*incrementCounter*/ false);
        if (GetDynamicConfig()->TestingOptions->SimulateNetworkThrottlingForGetBlockSet) {
            YT_LOG_WARNING("Simulating network throttling for ProbeBlockSet (ChunkId: %v)",
                chunkId);
            netThrottling.Enabled = true;
        }
        response->set_net_throttling(netThrottling.Enabled);
        response->set_net_queue_size(netThrottling.QueueSize);

        SuggestAllyReplicas(context);

        const auto& p2pSnooper = Bootstrap_->GetP2PSnooper();
        AddBlockPeers(response->mutable_peer_descriptors(), p2pSnooper->OnBlockProbe(chunkId, blockIndexes));

        context->SetResponseInfo(
            "HasCompleteChunk: %v, "
            "NetThrottling: %v, NetQueueSize: %v, "
            "DiskThrottling: %v, DiskQueueSize: %v, "
            "PeerDescriptorCount: %v",
            hasCompleteChunk,
            netThrottling.Enabled,
            netThrottling.QueueSize,
            diskThrottling.Enabled,
            diskThrottling.QueueSize,
            response->peer_descriptors_size());

        context->Reply();
    }

    template <class TContext, class TRequest>
    TChunkReadOptions PrepareChunkReadOptions(
        const TIntrusivePtr<TContext>& context,
        const TRequest* request,
        bool fetchFromCache,
        bool fetchFromDisk,
        const TChunkReaderStatisticsPtr& chunkReaderStatistics)
    {
        TChunkReadOptions options;
        options.WorkloadDescriptor = GetRequestWorkloadDescriptor(context);
        options.PopulateCache = request->populate_cache();
        options.BlockCache = Bootstrap_->GetBlockCache();
        options.FetchFromCache = fetchFromCache;
        options.FetchFromDisk = fetchFromDisk;
        options.ChunkReaderStatistics = chunkReaderStatistics;
        options.ReadSessionId = FromProto<TReadSessionId>(request->read_session_id());
        options.MemoryUsageTracker = Bootstrap_->GetReadBlockMemoryUsageTracker();

        if (context->GetTimeout() && context->GetStartTime()) {
            options.ReadBlocksDeadline =
                *context->GetStartTime() +
                *context->GetTimeout() * GetDynamicConfig()->TestingOptions->BlockReadTimeoutFraction;
        }

        auto readMetaTimeoutFraction = GetDynamicConfig()->ReadMetaTimeoutFraction;

        if (context->GetTimeout() && context->GetStartTime() && readMetaTimeoutFraction) {
            options.ReadMetaDeadLine =
                *context->GetStartTime() +
                *context->GetTimeout() * readMetaTimeoutFraction.value();
        }

        return options;
    }

    template <class TContext, class TResponse>
    TFuture<void> PrepareGetBlocksResponse(
        const TIntrusivePtr<TContext>& context,
        TResponse* response,
        TGetBlockResponseTemplate responseTemplate,
        const std::vector<NChunkClient::TBlock>& blocks) const
    {
        auto netThrottling = responseTemplate.NetThrottling;
        auto hasCompleteChunk = responseTemplate.HasCompleteChunk;

        if (!netThrottling) {
            SetRpcAttachedBlocks(response, blocks);
        }

        auto blocksSize = GetByteSize(response->Attachments());
        if (blocksSize == 0 && responseTemplate.ThrottledLargeBlock) {
            netThrottling = true;
        }

        response->set_net_throttling(netThrottling);

        auto chunkReaderStatistics = responseTemplate.ChunkReaderStatistics;
        auto bytesReadFromDisk =
            chunkReaderStatistics->DataBytesReadFromDisk.load(std::memory_order::relaxed) +
            chunkReaderStatistics->MetaBytesReadFromDisk.load(std::memory_order::relaxed);
        auto chunk = responseTemplate.Chunk;
        const auto& ioTracker = Bootstrap_->GetIOTracker();

        if (bytesReadFromDisk > 0 && ioTracker->IsEnabled()) {
            ioTracker->Enqueue(
                TIOCounters{.Bytes = bytesReadFromDisk, .IORequests = 1},
                MakeReadIOTags(context->GetMethod(), chunk->GetLocation(), context, chunk->GetId()));
        }

        ToProto(response->mutable_chunk_reader_statistics(), chunkReaderStatistics);

        int blocksWithData = std::count_if(
            response->Attachments().begin(),
            response->Attachments().end(),
            [] (const auto& block) -> bool { return static_cast<bool>(block); });

        context->SetResponseInfo(
            "HasCompleteChunk: %v, "
            "NetThrottling: %v, NetQueueSize: %v, "
            "DiskThrottling: %v, DiskQueueSize: %v, "
            "ThrottledLargeBlock: %v, "
            "BlocksWithData: %v, BlocksSize: %v",
            hasCompleteChunk,
            netThrottling,
            responseTemplate.NetQueueSize,
            responseTemplate.DiskThrottling,
            responseTemplate.DiskQueueSize,
            responseTemplate.ThrottledLargeBlock,
            blocksWithData,
            blocksSize);

        if (blocksSize == 0) {
            return VoidFuture;
        }

        // NB: We throttle only heavy responses that contain a non-empty attachment
        // as we want responses containing the information about disk/net throttling
        // to be delivered immediately.

        context->SetComplete();

        const auto& netThrottler = Bootstrap_->GetOutThrottler(responseTemplate.WorkloadDescriptor);
        auto resultFuture = netThrottler->Throttle(blocksSize);

        auto timeoutFraction = GetFallbackTimeoutFraction();

        if (!(context->GetTimeout() && context->GetStartTime() && timeoutFraction)) {
            return resultFuture;
        }

        auto deadline = *context->GetStartTime() + *context->GetTimeout() * timeoutFraction.value();

        auto throttleFuture = resultFuture
            .Apply(BIND([] { return false; }));
        auto fallbackFuture = TDelayedExecutor::MakeDelayed(deadline - TInstant::Now())
            .Apply(BIND([] { return true; }));
        return AnySet<bool>({std::move(throttleFuture), std::move(fallbackFuture)})
            .Apply(BIND([=] (bool isThrottled) {
                if (isThrottled) {
                    response->set_net_throttling(true);
                    response->clear_block_checksums();
                    response->Attachments().clear();

                    // Override response info.
                    context->SetResponseInfo(
                        "HasCompleteChunk: %v,"
                        "NetThrottling: %v",
                        hasCompleteChunk,
                        true);
                }

                // Directly hold current request context.
                Y_UNUSED(context);
            }));
    }

    template <class T, class TContext>
    TFuture<T> WrapWithTimeout(
        TFuture<T> future,
        const TIntrusivePtr<TContext>& context,
        TCallback<T()> fallbackValue)
    {
        auto timeoutFraction = GetFallbackTimeoutFraction();

        if (!(context->GetTimeout() && context->GetStartTime() && timeoutFraction)) {
            return future;
        }

        auto deadline = *context->GetStartTime() + *context->GetTimeout() * timeoutFraction.value();

        return AnySet<T>({
            future
                .ApplyUnique(BIND([fallbackValue] (TErrorOr<T>&& resultOrError) {
                    if (!resultOrError.IsOK() && resultOrError.FindMatching(NChunkClient::EErrorCode::ReadMetaTimeout)) {
                        return fallbackValue();
                    } else if (!resultOrError.IsOK()) {
                        THROW_ERROR_EXCEPTION(resultOrError);
                    } else {
                        return resultOrError.Value();
                    }
                })),
            TDelayedExecutor::MakeDelayed(deadline - TInstant::Now())
                .Apply(fallbackValue)
        });
    }

    template <class TContext>
    TChunkReadOptions BuildReadMetaOption(const TIntrusivePtr<TContext>& context)
    {
        TChunkReadOptions options;
        options.WorkloadDescriptor = GetRequestWorkloadDescriptor(context);
        options.ChunkReaderStatistics = New<TChunkReaderStatistics>();

        auto readMetaTimeoutFraction = GetDynamicConfig()->ReadMetaTimeoutFraction;

        if (context->GetTimeout() && context->GetStartTime() && readMetaTimeoutFraction) {
            options.ReadMetaDeadLine =
                *context->GetStartTime() +
                *context->GetTimeout() * readMetaTimeoutFraction.value();
        }

        return options;
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetBlockSet)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        auto blockIndexes = FromProto<std::vector<int>>(request->block_indexes());
        auto workloadDescriptor = GetRequestWorkloadDescriptor(context);
        bool populateCache = request->populate_cache();
        bool fetchFromCache = request->fetch_from_cache();
        bool fetchFromDisk = request->fetch_from_disk();

        context->SetRequestInfo("BlockIds: %v:%v, PopulateCache: %v, FetchFromCache: %v, "
            "FetchFromDisk: %v, Workload: %v",
            chunkId,
            MakeShrunkFormattableView(blockIndexes, TDefaultFormatter(), 3),
            populateCache,
            fetchFromCache,
            fetchFromDisk,
            workloadDescriptor);

        ValidateOnline();

        const auto& chunkRegistry = Bootstrap_->GetChunkRegistry();
        auto chunk = chunkRegistry->FindChunk(chunkId);

        bool hasCompleteChunk = chunk.operator bool();
        response->set_has_complete_chunk(hasCompleteChunk);

        auto diskThrottling = chunk
            ? chunk->GetLocation()->CheckReadThrottling(workloadDescriptor)
            : TChunkLocation::TDiskThrottlingResult{.Enabled = false, .QueueSize = 0};
        response->set_disk_throttling(diskThrottling.Enabled);
        response->set_disk_queue_size(diskThrottling.QueueSize);

        YT_LOG_DEBUG_UNLESS(diskThrottling.Error.IsOK(), diskThrottling.Error);

        auto netThrottling = CheckNetOutThrottling(context, workloadDescriptor);
        if (GetDynamicConfig()->TestingOptions->SimulateNetworkThrottlingForGetBlockSet) {
            YT_LOG_WARNING("Simulating network throttling for GetBlockSet (ChunkId: %v)",
                chunkId);
            netThrottling.Enabled = true;
        }
        response->set_net_queue_size(netThrottling.QueueSize);

        SuggestAllyReplicas(context);

        auto chunkReaderStatistics = New<TChunkReaderStatistics>();

        struct TReadBlocksResult
        {
            bool ReadFromP2P = false;
            std::vector<TBlock> Blocks;
        };

        auto readBlocks = BIND([=, this, this_ = MakeStrong(this)] {
            bool readFromP2P = false;
            TFuture<std::vector<TBlock>> blocksFuture;

            if (fetchFromCache || fetchFromDisk) {
                auto options = PrepareChunkReadOptions(
                    context,
                    request,
                    fetchFromCache && !netThrottling.Enabled,
                    fetchFromDisk && !netThrottling.Enabled && !diskThrottling.Enabled,
                    chunkReaderStatistics);

                if (!chunk && options.FetchFromCache) {
                    readFromP2P = true;

                    auto blocks = Bootstrap_->GetP2PBlockCache()->LookupBlocks(chunkId, blockIndexes);
                    chunkReaderStatistics->DataBytesReadFromCache.fetch_add(
                        GetByteSize(blocks),
                        std::memory_order::relaxed);
                    blocksFuture = MakeFuture(blocks);
                } else if (chunk) {
                    blocksFuture = chunk->ReadBlockSet(
                        blockIndexes,
                        options);
                } else {
                    blocksFuture = MakeFuture(std::vector<TBlock>());
                }
            } else {
                blocksFuture = MakeFuture(std::vector<TBlock>());
            }

            return blocksFuture
                .ApplyUnique(BIND([=] (std::vector<TBlock>&& blocks) {
                    return TReadBlocksResult{
                        .ReadFromP2P = readFromP2P,
                        .Blocks = std::move(blocks),
                    };
                }));
        });

        auto blocksFuture = WrapWithTimeout(
            WaitP2PBarriers(request)
                .Apply(readBlocks),
            context,
            BIND([] { return TReadBlocksResult(); }));

        // Usually, when subscribing, there are more free fibers than when calling wait for,
        // and the lack of free fibers during a forced context switch leads to the creation
        // of a new fiber and an increase in the number of stacks.
        auto onBlocksRead = BIND([=, this, this_ = MakeStrong(this)] (TReadBlocksResult&& result) {
            auto readFromP2P = result.ReadFromP2P;
            auto& blocks = result.Blocks;

            // NB: P2P manager might steal blocks and assign null values.
            const auto& p2pSnooper = Bootstrap_->GetP2PSnooper();
            bool throttledLargeBlock = false;
            auto blockPeers = p2pSnooper->OnBlockRead(
                chunkId,
                blockIndexes,
                &blocks,
                &throttledLargeBlock,
                readFromP2P);
            AddBlockPeers(response->mutable_peer_descriptors(), blockPeers);

            return PrepareGetBlocksResponse(
                context,
                response,
                TGetBlockResponseTemplate{
                    .Chunk = chunk,
                    .WorkloadDescriptor = workloadDescriptor,
                    .HasCompleteChunk = hasCompleteChunk,
                    .NetThrottling = netThrottling.Enabled,
                    .NetQueueSize = netThrottling.QueueSize,
                    .DiskThrottling = diskThrottling.Enabled,
                    .DiskQueueSize = diskThrottling.QueueSize,
                    .ThrottledLargeBlock = throttledLargeBlock,
                    .ChunkReaderStatistics = chunkReaderStatistics,
                },
                std::move(result.Blocks));
        });

        context->ReplyFrom(blocksFuture.ApplyUnique(onBlocksRead));
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetBlockRange)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        auto workloadDescriptor = GetRequestWorkloadDescriptor(context);
        int firstBlockIndex = request->first_block_index();
        int blockCount = request->block_count();
        bool populateCache = request->populate_cache();

        context->SetRequestInfo(
            "BlockIds: %v:%v-%v, PopulateCache: %v, Workload: %v",
            chunkId,
            firstBlockIndex,
            firstBlockIndex + blockCount - 1,
            populateCache,
            workloadDescriptor);

        ValidateOnline();

        const auto& chunkRegistry = Bootstrap_->GetChunkRegistry();
        auto chunk = chunkRegistry->FindChunk(chunkId);

        bool hasCompleteChunk = chunk.operator bool();
        response->set_has_complete_chunk(hasCompleteChunk);

        auto diskThrottling = chunk
            ? chunk->GetLocation()->CheckReadThrottling(workloadDescriptor)
            : TChunkLocation::TDiskThrottlingResult{.Enabled = false, .QueueSize = 0};
        response->set_disk_throttling(diskThrottling.Enabled);
        response->set_disk_queue_size(diskThrottling.QueueSize);

        YT_LOG_DEBUG_UNLESS(diskThrottling.Error.IsOK(), diskThrottling.Error);

        auto netThrottling = CheckNetOutThrottling(context, workloadDescriptor);
        response->set_net_throttling(netThrottling.Enabled);
        response->set_net_queue_size(netThrottling.QueueSize);

        auto chunkReaderStatistics = New<TChunkReaderStatistics>();
        auto options = PrepareChunkReadOptions(
            context,
            request,
            !netThrottling.Enabled,
            !netThrottling.Enabled && !diskThrottling.Enabled,
            chunkReaderStatistics);

        auto blocksFuture = chunk
            ? WrapWithTimeout(
                chunk->ReadBlockRange(
                    firstBlockIndex,
                    blockCount,
                    options),
                context,
                BIND([] { return std::vector<TBlock>(); }))
            : MakeFuture(std::vector<TBlock>());

        // Usually, when subscribing, there are more free fibers than when calling wait for,
        // and the lack of free fibers during a forced context switch leads to the creation
        // of a new fiber and an increase in the number of stacks.
        auto onBlocksRead = BIND([=, this, this_ = MakeStrong(this)] (const std::vector<TBlock>& blocks) {
            return PrepareGetBlocksResponse(
                context,
                response,
                TGetBlockResponseTemplate{
                    .Chunk = chunk,
                    .WorkloadDescriptor = workloadDescriptor,
                    .HasCompleteChunk = hasCompleteChunk,
                    .NetThrottling = netThrottling.Enabled,
                    .NetQueueSize = netThrottling.QueueSize,
                    .DiskThrottling = diskThrottling.Enabled,
                    .DiskQueueSize = diskThrottling.QueueSize,
                    .ThrottledLargeBlock = false,
                    .ChunkReaderStatistics = chunkReaderStatistics,
                },
                blocks);
        });

        context->ReplyFrom(blocksFuture.Apply(onBlocksRead));
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetChunkFragmentSet)
    {
        auto readSessionId = FromProto<TReadSessionId>(request->read_session_id());
        auto workloadDescriptor = GetRequestWorkloadDescriptor(context);
        auto useDirectIO = request->use_direct_io();

        int totalFragmentCount = 0;
        i64 totalFragmentSize = 0;
        for (const auto& subrequest : request->subrequests()) {
            for (const auto& fragment : subrequest.fragments()) {
                totalFragmentCount += 1;
                if (fragment.length() != WholeBlockFragmentRequestLength) {
                    totalFragmentSize += fragment.length();
                }
            }
        }

        context->SetRequestInfo("ReadSessionId: %v, Workload: %v, SubrequestCount: %v, FragmentsSize: %v/%v",
            readSessionId,
            workloadDescriptor,
            request->subrequests_size(),
            totalFragmentSize,
            totalFragmentCount);

        ValidateOnline();

        auto netThrottling = CheckNetOutThrottling(context, workloadDescriptor);
        response->set_net_throttling(netThrottling.Enabled);

        auto enableThrottling = GetDynamicConfig()->EnableThrottlingForGetChunkFragmentSet;
        if (enableThrottling && netThrottling.Enabled) {
            context->SetResponseInfo("NetThrottling: %v", netThrottling.Enabled);
            context->Reply();
            return;
        }

        THashMap<TChunkLocation*, int> locationToLocationIndex;
        std::vector<std::pair<TChunkLocation*, int>> requestedLocations;

        struct TChunkRequestInfo
        {
            TChunkReadGuard Guard;
            int LocationIndex;
        };
        std::vector<TChunkRequestInfo> chunkRequestInfos;
        chunkRequestInfos.reserve(request->subrequests_size());

        std::vector<TFuture<void>> prepareReaderFutures;

        const auto& chunkRegistry = Bootstrap_->GetChunkRegistry();
        {
            for (const auto& subrequest : request->subrequests()) {
                auto chunkId = FromProto<TChunkId>(subrequest.chunk_id());
                auto chunk = chunkRegistry->FindChunk(chunkId);

                auto* subresponse = response->add_subresponses();

                const auto& allyReplicaManager = Bootstrap_->GetAllyReplicaManager();
                if (auto allyReplicas = allyReplicaManager->GetAllyReplicas(chunkId)) {
                    if (allyReplicas.Revision > subrequest.ally_replicas_revision()) {
                        ToProto(subresponse->mutable_ally_replicas(), allyReplicas);
                        YT_LOG_DEBUG("Ally replicas suggested "
                            "(ChunkId: %v, AllyReplicas: %v, ClientAllyReplicasRevision: %v)",
                            chunkId,
                            allyReplicas,
                            subrequest.ally_replicas_revision());
                    }
                }

                bool chunkAvailable = false;
                if (chunk) {
                    auto diskThrottling = chunk
                        ? chunk->GetLocation()->CheckReadThrottling(workloadDescriptor)
                        : TChunkLocation::TDiskThrottlingResult{.Enabled = false, .QueueSize = 0};
                    auto diskThrottlingEnabled = enableThrottling && diskThrottling.Enabled;
                    subresponse->set_disk_throttling(diskThrottlingEnabled);

                    YT_LOG_DEBUG_UNLESS(diskThrottling.Error.IsOK(), diskThrottling.Error);

                    if (auto guard = TChunkReadGuard::TryAcquire(std::move(chunk));
                        guard && !diskThrottlingEnabled)
                    {
                        auto* location = guard.GetChunk()->GetLocation().Get();
                        auto [it, emplaced] = locationToLocationIndex.try_emplace(
                            location,
                            std::ssize(locationToLocationIndex));
                        if (emplaced) {
                            requestedLocations.emplace_back(location, 0);
                        }
                        requestedLocations[it->second].second += subrequest.fragments_size();

                        TClientChunkReadOptions options{
                            .WorkloadDescriptor = workloadDescriptor,
                            .ReadSessionId = readSessionId,
                            .MemoryUsageTracker = Bootstrap_->GetReadBlockMemoryUsageTracker()
                        };

                        if (auto future = guard.GetChunk()->PrepareToReadChunkFragments(options, useDirectIO)) {
                            YT_LOG_DEBUG("Will wait for chunk reader to become prepared (ChunkId: %v)",
                                guard.GetChunk()->GetId());
                            prepareReaderFutures.push_back(std::move(future));
                        }
                        chunkRequestInfos.push_back({
                            .Guard = std::move(guard),
                            .LocationIndex = it->second
                        });
                        chunkAvailable = true;
                    }
                }

                if (!chunkAvailable) {
                    chunkRequestInfos.emplace_back();
                }

                subresponse->set_has_complete_chunk(chunkAvailable);
            }
        }

        auto enableMemoryTracking = GetDynamicConfig()->EnableMemoryTrackingForGetChunkFragmentSet;
        auto memoryTracker = Bootstrap_->GetReadBlockMemoryUsageTracker();

        auto afterReadersPrepared =
            [
                =,
                this,
                this_ = MakeStrong(this),
                requestedLocations = std::move(requestedLocations),
                chunkRequestInfos = std::move(chunkRequestInfos)
            ] (const TError& error) mutable {
                if (!error.IsOK()) {
                    context->Reply(error);
                    return;
                }

                std::vector<std::vector<int>> locationFragmentIndices(requestedLocations.size());
                std::vector<std::vector<IIOEngine::TReadRequest>> locationRequests(requestedLocations.size());
                std::vector<TMemoryUsageTrackerGuard> locationMemoryGuards(requestedLocations.size());

                for (auto [_, locationRequestCount] : requestedLocations) {
                    locationFragmentIndices.reserve(locationRequestCount);
                    locationRequests.reserve(locationRequestCount);
                }

                int fragmentIndex = 0;
                try {
                    for (int subrequestIndex = 0; subrequestIndex < request->subrequests_size(); ++subrequestIndex) {
                        const auto& subrequest = request->subrequests(subrequestIndex);
                        const auto& chunkRequestInfo = chunkRequestInfos[subrequestIndex];
                        const auto& chunk = chunkRequestInfo.Guard.GetChunk();
                        if (!chunk) {
                            fragmentIndex += subrequest.fragments().size();
                            continue;
                        }

                        auto locationIndex = chunkRequestInfo.LocationIndex;
                        i64 totalSize = 0L;
                        for (const auto& fragment : subrequest.fragments()) {
                            auto readRequest = chunk->MakeChunkFragmentReadRequest(
                                TChunkFragmentDescriptor{
                                    .Length = fragment.length(),
                                    .BlockIndex = fragment.block_index(),
                                    .BlockOffset = fragment.block_offset()
                                },
                                useDirectIO);
                            totalSize += readRequest.Size;
                            locationRequests[locationIndex].push_back(std::move(readRequest));
                            locationFragmentIndices[locationIndex].push_back(fragmentIndex);
                            ++fragmentIndex;
                        }

                        TMemoryUsageTrackerGuard memoryGuard = {};
                        if (enableMemoryTracking) {
                            memoryGuard = TMemoryUsageTrackerGuard::TryAcquire(
                                memoryTracker,
                                totalSize)
                                .ValueOrThrow();
                        }

                        locationMemoryGuards[locationIndex] = std::move(memoryGuard);
                    }
                } catch (const std::exception& ex) {
                    context->Reply(ex);
                    return;
                }

                response->Attachments().resize(fragmentIndex);

                std::vector<TFuture<IIOEngine::TReadResponse>> readFutures;
                readFutures.reserve(requestedLocations.size());

                for (int index = 0; index < std::ssize(requestedLocations); ++index) {
                    auto [location, locationRequestCount] = requestedLocations[index];
                    YT_VERIFY(locationRequestCount == std::ssize(locationRequests[index]));
                    YT_LOG_DEBUG("Reading block fragments (LocationId: %v, FragmentCount: %v)",
                        location->GetId(),
                        locationRequestCount);
                    const auto& ioEngine = location->GetIOEngine();

                    struct TChunkFragmentBuffer
                    { };
                    readFutures.push_back(ioEngine->Read(
                        std::move(locationRequests[index]),
                        workloadDescriptor,
                        GetRefCountedTypeCookie<TChunkFragmentBuffer>(),
                        readSessionId)
                        .ApplyUnique(BIND([
                            =,
                            memoryGuard = std::move(locationMemoryGuards[index]),
                            resultIndex = index] (IIOEngine::TReadResponse&& result) mutable {
                            if (!enableMemoryTracking) {
                                return result;
                            }

                            const auto& fragmentIndices = locationFragmentIndices[resultIndex];
                            YT_VERIFY(result.OutputBuffers.size() == fragmentIndices.size());

                            for (int index = 0; index < std::ssize(fragmentIndices); ++index) {
                                result.OutputBuffers[index] = TrackMemory(memoryTracker, std::move(result.OutputBuffers[index]));
                            }

                            memoryGuard.Release();

                            return result;
                        })));
                }

                AllSucceeded(std::move(readFutures))
                    .SubscribeUnique(BIND(
                        [
                            =,
                            this,
                            this_ = MakeStrong(this),
                            chunkRequestInfos = std::move(chunkRequestInfos),
                            locationFragmentIndices = std::move(locationFragmentIndices),
                            requestedLocations = std::move(requestedLocations)
                        ] (TErrorOr<std::vector<IIOEngine::TReadResponse>>&& resultsOrError) {
                            if (!resultsOrError.IsOK()) {
                                context->Reply(resultsOrError);
                                return;
                            }

                            auto& results = resultsOrError.Value();
                            YT_VERIFY(results.size() == locationFragmentIndices.size());

                            i64 dataBytesReadFromDisk = 0;
                            i64 dataIORequests = 0;
                            for (int resultIndex = 0; resultIndex < std::ssize(results); ++resultIndex) {
                                auto& result = results[resultIndex];
                                const auto& fragmentIndices = locationFragmentIndices[resultIndex];
                                YT_VERIFY(result.OutputBuffers.size() == fragmentIndices.size());
                                for (int index = 0; index < std::ssize(fragmentIndices); ++index) {
                                    response->Attachments()[fragmentIndices[index]] = std::move(result.OutputBuffers[index]);
                                }

                                const auto& ioTracker = Bootstrap_->GetIOTracker();
                                if (result.PaddedBytes > 0 && ioTracker->IsEnabled()) {
                                    ioTracker->Enqueue(
                                        TIOCounters{
                                            .Bytes = result.PaddedBytes,
                                            .IORequests = result.IORequests
                                        },
                                        // NB: Now we do not track chunk id for this method.
                                        MakeReadIOTags(
                                            "GetChunkFragmentSet",
                                            requestedLocations[resultIndex].first,
                                            context,
                                            NullChunkId,
                                            readSessionId));
                                }

                                dataBytesReadFromDisk += result.PaddedBytes;
                                dataIORequests += result.IORequests;
                            }

                            response->mutable_chunk_reader_statistics()->set_data_bytes_read_from_disk(dataBytesReadFromDisk);
                            response->mutable_chunk_reader_statistics()->set_data_io_requests(dataIORequests);
                            if (const auto* traceContext = TryGetCurrentTraceContext()) {
                                FlushCurrentTraceContextElapsedTime();
                                response->mutable_chunk_reader_statistics()->set_remote_cpu_time(
                                    traceContext->GetElapsedTime().GetValue());
                            }

                            auto totalFragmentSize = GetByteSize(response->Attachments());
                            const auto& netThrottler = Bootstrap_->GetOutThrottler(workloadDescriptor);

                            if (netThrottler->IsOverdraft()) {
                                context->SetComplete();
                                context->ReplyFrom(netThrottler->Throttle(totalFragmentSize));
                            } else {
                                netThrottler->Acquire(totalFragmentSize);
                                context->Reply();
                            }
                        }));
            };

        if (prepareReaderFutures.empty()) {
            afterReadersPrepared(TError());
        } else {
            AllSucceeded(std::move(prepareReaderFutures))
                .Subscribe(BIND(std::move(afterReadersPrepared))
                    .Via(Bootstrap_->GetStorageLookupInvoker()));
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, LookupRows)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        auto readSessionId = FromProto<TReadSessionId>(request->read_session_id());
        auto workloadDescriptor = GetRequestWorkloadDescriptor(context);
        auto populateCache = request->populate_cache();
        auto enableHashChunkIndex = request->enable_hash_chunk_index();
        const auto& schemaData = request->schema_data();

        context->SetRequestInfo("ChunkId: %v, ReadSessionId: %v, Workload: %v, "
            "PopulateCache: %v, EnableHashChunkIndex: %v, ContainsSchema: %v",
            chunkId,
            readSessionId,
            workloadDescriptor,
            populateCache,
            enableHashChunkIndex,
            schemaData.has_schema());

        ValidateOnline();

        const auto& chunkRegistry = Bootstrap_->GetChunkRegistry();
        auto chunk = chunkRegistry->GetChunkOrThrow(chunkId);

        // NB: Heating table schema caches up is of higher priority than advisory throttling.
        // COMPAT(akozhikhov): Get rid of schemaRequested field and related logic.
        auto [tableSchema, schemaRequested] = FindTableSchemaForOffloadedReadSession(
            chunkId,
            readSessionId,
            schemaData,
            Bootstrap_->GetTableSchemaCache());
        if (!tableSchema) {
            response->set_fetched_rows(false);
            response->set_request_schema(true);
            context->SetResponseInfo(
                "ChunkId: %v, ReadSessionId: %v, Workload: %v, SchemaRequested: %v",
                chunkId,
                readSessionId,
                workloadDescriptor,
                schemaRequested);
            context->Reply();
            return;
        }
        YT_VERIFY(!schemaRequested);

        auto diskThrottling = chunk
            ? chunk->GetLocation()->CheckReadThrottling(workloadDescriptor)
            : TChunkLocation::TDiskThrottlingResult{.Enabled = false, .QueueSize = 0};
        // COMPAT(akozhikhov): For YT-18378. Drop this after all tablet nodes are updated.
        if (diskThrottling.Enabled) {
            ++diskThrottling.QueueSize;
        }
        response->set_disk_throttling(diskThrottling.Enabled);
        response->set_disk_queue_size(diskThrottling.QueueSize);

        YT_LOG_DEBUG_UNLESS(diskThrottling.Error.IsOK(), diskThrottling.Error);

        auto netThrottling = CheckNetOutThrottling(context, workloadDescriptor);
        response->set_net_throttling(netThrottling.Enabled);
        response->set_net_queue_size(netThrottling.QueueSize);

        auto timestamp = request->timestamp();
        auto columnFilter = FromProto<NTableClient::TColumnFilter>(request->column_filter());
        auto codecId = CheckedEnumCast<NCompression::ECodec>(request->compression_codec());
        auto produceAllVersions = FromProto<bool>(request->produce_all_versions());
        auto overrideTimestamp = request->has_override_timestamp() ? request->override_timestamp() : NullTimestamp;
        auto useDirectIO = request->use_direct_io();

        auto chunkReadSession = CreateOffloadedChunkReadSession(
            Bootstrap_,
            std::move(chunk),
            readSessionId,
            workloadDescriptor,
            std::move(columnFilter),
            timestamp,
            produceAllVersions,
            std::move(tableSchema),
            codecId,
            overrideTimestamp,
            populateCache,
            enableHashChunkIndex,
            useDirectIO);

        context->SetResponseInfo(
            "ChunkId: %v, ReadSessionId: %v, Workload: %v, "
            "DiskThrottling: %v, DiskQueueSize: %v, NetThrottling: %v, NetQueueSize: %v",
            chunkId,
            readSessionId,
            workloadDescriptor,
            diskThrottling.Enabled,
            diskThrottling.QueueSize,
            netThrottling.Enabled,
            netThrottling.QueueSize);

        context->ReplyFrom(chunkReadSession->Lookup(request->Attachments())
            .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TSharedRef& result) {
                response->Attachments().push_back(result);
                response->set_fetched_rows(true);

                const auto& chunkReaderStatistics = chunkReadSession->GetChunkReaderStatistics();
                if (const auto* traceContext = TryGetCurrentTraceContext()) {
                    FlushCurrentTraceContextElapsedTime();
                    chunkReaderStatistics->RemoteCpuTime.fetch_add(
                        traceContext->GetElapsedTime().GetValue(),
                        std::memory_order::relaxed);
                }
                ToProto(response->mutable_chunk_reader_statistics(), chunkReaderStatistics);

                context->SetComplete();

                const auto& netThrottler = Bootstrap_->GetOutThrottler(workloadDescriptor);
                return netThrottler->Throttle(GetByteSize(response->Attachments()));
            })));
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetChunkMeta)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        auto partitionTag = request->has_partition_tag()
            ? std::make_optional(request->partition_tag())
            : std::nullopt;
        auto extensionTags = request->all_extension_tags()
            ? std::nullopt
            : std::make_optional(FromProto<std::vector<int>>(request->extension_tags()));
        auto workloadDescriptor = GetRequestWorkloadDescriptor(context);
        bool enableThrottling = request->enable_throttling();

        context->SetRequestInfo("ChunkId: %v, ExtensionTags: %v, PartitionTag: %v, Workload: %v, EnableThrottling: %v",
            chunkId,
            extensionTags,
            partitionTag,
            workloadDescriptor,
            enableThrottling);

        ValidateOnline();

        auto netThrottling = CheckNetOutThrottling(context, workloadDescriptor);
        response->set_net_throttling(netThrottling.Enabled);

        context->SetResponseInfo("NetThrottling: %v",
            netThrottling.Enabled);

        if (enableThrottling && netThrottling.Enabled) {
            context->Reply();
            return;
        }

        const auto& chunkRegistry = Bootstrap_->GetChunkRegistry();
        auto chunk = chunkRegistry->GetChunkOrThrow(chunkId, AllMediaIndex);

        auto options = BuildReadMetaOption(context);

        struct TReadMetaResult
        {
            TRefCountedChunkMetaPtr Meta;
            bool Throttled = false;
        };

        auto result = chunk->ReadMeta(options, extensionTags)
            .ApplyUnique(BIND([] (TRefCountedChunkMetaPtr&& meta) {
                return TReadMetaResult{
                    .Meta = std::move(meta),
                    .Throttled = false,
                };
            }));
        auto chunkMetaFuture = WrapWithTimeout(
            result,
            context,
            BIND([] { return TReadMetaResult{.Throttled = true}; }));

        context->ReplyFrom(chunkMetaFuture.Apply(BIND([=, this, this_ = MakeStrong(this)] (const TReadMetaResult& readMetaResult) {
            if (context->IsCanceled()) {
                throw TFiberCanceledException();
            }

            if (readMetaResult.Throttled) {
                response->set_net_throttling(true);
                context->SetResponseInfo("NetThrottling: %v", true);
                return;
            }

            const auto& meta = readMetaResult.Meta;

            YT_VERIFY(meta);

            {
                NChunkClient::EChunkFeatures chunkFeatures = FromProto<NChunkClient::EChunkFeatures>(meta->features());
                NChunkClient::EChunkFeatures supportedChunkFeatures = FromProto<NChunkClient::EChunkFeatures>(request->supported_chunk_features());
                ValidateChunkFeatures(chunkId, chunkFeatures, supportedChunkFeatures);
            }

            if (partitionTag) {
                const auto& blockMetaCache = Bootstrap_->GetChunkMetaManager()->GetBlockMetaCache();
                auto cachedBlockMeta = blockMetaCache->Find(chunkId);
                if (!cachedBlockMeta) {
                    auto blockMetaExt = GetProtoExtension<TDataBlockMetaExt>(meta->extensions());
                    cachedBlockMeta = New<TCachedBlockMeta>(chunkId, std::move(blockMetaExt));
                    blockMetaCache->TryInsert(cachedBlockMeta);
                }

                *response->mutable_chunk_meta() = FilterChunkMetaByPartitionTag(*meta, cachedBlockMeta, *partitionTag);
            } else {
                *response->mutable_chunk_meta() = static_cast<TChunkMeta>(*meta);
            }

            ToProto(response->mutable_chunk_reader_statistics(), options.ChunkReaderStatistics);
            ToProto(response->mutable_location_uuid(), chunk->GetLocation()->GetUuid());
        }).AsyncVia(Bootstrap_->GetStorageHeavyInvoker())));
    }

    template <class TContext, class TRequests>
    TFuture<std::vector<TErrorOr<TRefCountedChunkMetaPtr>>> GetChunkMetasForRequests(
        const TIntrusivePtr<TContext>& context,
        const TRequests& requests)
    {
        auto options = BuildReadMetaOption(context);

        std::vector<TFuture<TRefCountedChunkMetaPtr>> chunkMetaFutures;
        for (const auto& request : requests) {
            auto chunkId = FromProto<TChunkId>(request.chunk_id());
            auto chunk = Bootstrap_->GetChunkStore()->FindChunk(chunkId);
            if (chunk) {
                chunkMetaFutures.push_back(chunk->ReadMeta(options));
            } else {
                chunkMetaFutures.push_back(MakeFuture<TRefCountedChunkMetaPtr>(TError(
                    NChunkClient::EErrorCode::NoSuchChunk,
                    "No such chunk %v",
                    chunkId)));
            }
        }
        return AllSet(chunkMetaFutures);
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetChunkSliceDataWeights)
    {
        auto requestCount = request->chunk_requests_size();
        auto workloadDescriptor = GetRequestWorkloadDescriptor(context);

        context->SetRequestInfo("RequestCount: %v, Workload: %v",
            requestCount,
            workloadDescriptor);

        ValidateOnline();

        GetChunkMetasForRequests(context, request->chunk_requests())
            .Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<std::vector<TErrorOr<NChunkClient::TRefCountedChunkMetaPtr>>>& resultsError) {
                if (!resultsError.IsOK()) {
                    context->Reply(resultsError);
                    return;
                }

                if (context->IsCanceled()) {
                    return;
                }

                TNameTablePtr nameTable;
                if (request->has_name_table()) {
                    nameTable = FromProto<TNameTablePtr>(request->name_table());
                }

                const auto& results = resultsError.Value();
                YT_VERIFY(std::ssize(results) == requestCount);

                for (int requestIndex = 0; requestIndex < requestCount; ++requestIndex) {
                    const auto& chunkRequest = request->chunk_requests(requestIndex);

                    std::optional<std::vector<TColumnStableName>> columnStableNames;

                    if (chunkRequest.has_column_filter()) {
                        YT_VERIFY(nameTable);

                        columnStableNames.emplace();
                        columnStableNames->reserve(chunkRequest.column_filter().indexes_size());

                        for (auto columnIndex : chunkRequest.column_filter().indexes()) {
                            columnStableNames->push_back(TColumnStableName(TString{nameTable->GetName(columnIndex)}));
                        }
                    }

                    ProcessSliceSize(
                        chunkRequest,
                        response->add_chunk_responses(),
                        results[requestIndex],
                        columnStableNames);
                }

                context->Reply();
        }).Via(Bootstrap_->GetStorageHeavyInvoker()));
    }

    void ProcessSliceSize(
        const TReqGetChunkSliceDataWeights::TChunkSlice& weightedChunkRequest,
        TRspGetChunkSliceDataWeights::TWeightedChunk* weightedChunkResponse,
        const TErrorOr<TRefCountedChunkMetaPtr>& metaOrError,
        const std::optional<std::vector<TColumnStableName>>& columnStableNames)
    {
        auto chunkId = FromProto<TChunkId>(weightedChunkRequest.chunk_id());
        try {
            THROW_ERROR_EXCEPTION_IF_FAILED(metaOrError, "Error getting meta of chunk %v",
                chunkId);

            const auto& chunkMeta = metaOrError.Value();
            auto type = CheckedEnumCast<EChunkType>(chunkMeta->type());
            if (type != EChunkType::Table) {
                THROW_ERROR_EXCEPTION("Invalid type of chunk %v: expected %Qlv, actual %Qlv",
                    chunkId,
                    EChunkType::Table,
                    type);
            }

            auto miscExt = GetProtoExtension<TMiscExt>(chunkMeta->extensions());
            if (!miscExt.sorted()) {
                THROW_ERROR_EXCEPTION("Chunk %v is not sorted", chunkId);
            }

            auto dataWeight = GetChunkSliceDataWeight(
                weightedChunkRequest,
                *chunkMeta,
                columnStableNames);

            weightedChunkResponse->set_data_weight(dataWeight);
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            YT_LOG_WARNING(error, "Error building chunk slices (ChunkId: %v)",
                chunkId);
            ToProto(weightedChunkResponse->mutable_error(), error);
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetChunkSlices)
    {
        auto requestCount = request->slice_requests_size();
        auto workloadDescriptor = GetRequestWorkloadDescriptor(context);

        context->SetRequestInfo(
            "RequestCount: %v, Workload: %v",
            requestCount,
            workloadDescriptor);

        ValidateOnline();

        GetChunkMetasForRequests(context, request->slice_requests())
            .Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<std::vector<TErrorOr<NChunkClient::TRefCountedChunkMetaPtr>>>& resultsError) {
                if (!resultsError.IsOK()) {
                    context->Reply(resultsError);
                    return;
                }

                if (context->IsCanceled()) {
                    return;
                }

                const auto& results = resultsError.Value();
                YT_VERIFY(std::ssize(results) == requestCount);

                auto keysWriter = New<TKeySetWriter>();
                auto keyBoundsWriter = New<TKeySetWriter>();

                for (int requestIndex = 0; requestIndex < requestCount; ++requestIndex) {
                    ProcessSlice(
                        request->slice_requests(requestIndex),
                        response->add_slice_responses(),
                        keysWriter,
                        keyBoundsWriter,
                        results[requestIndex]);
                }

                response->Attachments().push_back(keysWriter->Finish());
                response->Attachments().push_back(keyBoundsWriter->Finish());
                context->Reply();
            }).Via(Bootstrap_->GetStorageHeavyInvoker()));
    }

    void ProcessSlice(
        const TSliceRequest& sliceRequest,
        TRspGetChunkSlices::TSliceResponse* sliceResponse,
        const TKeySetWriterPtr& keysWriter,
        const TKeySetWriterPtr& keyBoundsWriter,
        const TErrorOr<TRefCountedChunkMetaPtr>& metaOrError)
    {
        auto chunkId = FromProto<TChunkId>(sliceRequest.chunk_id());
        try {
            THROW_ERROR_EXCEPTION_IF_FAILED(metaOrError, "Error getting meta of chunk %v",
                chunkId);

            const auto& chunkMeta = metaOrError.Value();
            auto type = CheckedEnumCast<EChunkType>(chunkMeta->type());
            if (type != EChunkType::Table) {
                THROW_ERROR_EXCEPTION("Invalid type of chunk %v: expected %Qlv, actual %Qlv",
                    chunkId,
                    EChunkType::Table,
                    type);
            }

            auto miscExt = GetProtoExtension<TMiscExt>(chunkMeta->extensions());
            if (!miscExt.sorted()) {
                THROW_ERROR_EXCEPTION("Chunk %v is not sorted", chunkId);
            }

            auto slices = SliceChunk(
                sliceRequest,
                *chunkMeta);

            for (const auto& slice : slices) {
                ToProto(keysWriter, keyBoundsWriter, sliceResponse->add_chunk_slices(), slice);
            }
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            YT_LOG_WARNING(error, "Error building chunk slices (ChunkId: %v)",
                chunkId);
            ToProto(sliceResponse->mutable_error(), error);
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetTableSamples)
    {
        auto samplingPolicy = ESamplingPolicy(request->sampling_policy());
        auto keyColumns = FromProto<TKeyColumns>(request->key_columns());
        auto requestCount = request->sample_requests_size();
        auto maxSampleSize = request->max_sample_size();
        auto workloadDescriptor = GetRequestWorkloadDescriptor(context);

        context->SetRequestInfo("SamplingPolicy: %v, KeyColumns: %v, RequestCount: %v, Workload: %v",
            samplingPolicy,
            keyColumns,
            requestCount,
            workloadDescriptor);

        ValidateOnline();

        GetChunkMetasForRequests(context, request->sample_requests())
            .Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<std::vector<TErrorOr<NChunkClient::TRefCountedChunkMetaPtr>>>& resultsError) {
                if (!resultsError.IsOK()) {
                    context->Reply(resultsError);
                    return;
                }

                if (context->IsCanceled()) {
                    return;
                }

                const auto& results = resultsError.Value();
                YT_VERIFY(std::ssize(results) == requestCount);

                auto keySetWriter = New<TKeySetWriter>();

                for (int requestIndex = 0; requestIndex < requestCount; ++requestIndex) {
                    ProcessChunkSamples(
                        request->sample_requests(requestIndex),
                        response->add_sample_responses(),
                        samplingPolicy,
                        keyColumns,
                        maxSampleSize,
                        keySetWriter,
                        results[requestIndex]);
                }

                response->Attachments().push_back(keySetWriter->Finish());
                context->Reply();
            }).Via(Bootstrap_->GetStorageHeavyInvoker()));
    }

    void ProcessChunkSamples(
        const TReqGetTableSamples::TSampleRequest& sampleRequest,
        TRspGetTableSamples::TChunkSamples* sampleResponse,
        ESamplingPolicy samplingPolicy,
        const TKeyColumns& keyColumns,
        i32 maxSampleSize,
        const TKeySetWriterPtr& keySetWriter,
        const TErrorOr<TRefCountedChunkMetaPtr>& metaOrError)
    {
        auto chunkId = FromProto<TChunkId>(sampleRequest.chunk_id());
        try {
            THROW_ERROR_EXCEPTION_IF_FAILED(metaOrError, "Error getting meta of chunk %v",
                chunkId);
            const auto& meta = *metaOrError.Value();

            auto type = CheckedEnumCast<EChunkType>(meta.type());
            if (type != EChunkType::Table) {
                THROW_ERROR_EXCEPTION("Invalid type of chunk %v: expected %Qlv, actual %Qlv",
                    chunkId,
                    EChunkType::Table,
                    type);
            }

            switch (samplingPolicy) {
                case ESamplingPolicy::Sorting:
                    ProcessSortingSamples(sampleRequest, sampleResponse, keyColumns, maxSampleSize, keySetWriter, meta);
                    break;

                case ESamplingPolicy::Partitioning:
                    ProcessPartitioningSamples(sampleRequest, sampleResponse, keyColumns, keySetWriter, meta);
                    break;

                default:
                    YT_ABORT();
            }

        } catch (const std::exception& ex) {
            auto error = TError(ex);
            YT_LOG_WARNING(error, "Error building chunk samples (ChunkId: %v)",
                chunkId);
            ToProto(sampleResponse->mutable_error(), error);
        }
    }

    static void SerializeSample(
        TRspGetTableSamples::TSample* protoSample,
        TUnversionedValueRange values,
        i32 maxSampleSize,
        i64 weight,
        const TKeySetWriterPtr& keySetWriter,
        const TRowBufferPtr& truncatedSampleValueBuffer)
    {
        auto truncatedValues = TruncateUnversionedValues(values, truncatedSampleValueBuffer, {.ClipAfterOverflow = true, .MaxTotalSize = maxSampleSize});

        protoSample->set_key_index(keySetWriter->WriteValueRange(truncatedValues.Values));
        protoSample->set_incomplete(truncatedValues.Clipped);
        protoSample->set_weight(weight);
    }


    void ProcessPartitioningSamples(
        const TReqGetTableSamples::TSampleRequest& sampleRequest,
        TRspGetTableSamples::TChunkSamples* chunkSamples,
        const TKeyColumns& keyColumns,
        const TKeySetWriterPtr& keySetWriter,
        const TChunkMeta& chunkMeta)
    {
        auto chunkId = FromProto<TChunkId>(sampleRequest.chunk_id());

        // COMPAT(psushin)
        TKeyColumns chunkKeyColumns;
        auto optionalKeyColumnsExt = FindProtoExtension<TKeyColumnsExt>(chunkMeta.extensions());
        if (optionalKeyColumnsExt) {
            chunkKeyColumns = FromProto<TKeyColumns>(*optionalKeyColumnsExt);
        } else {
            auto schemaExt = GetProtoExtension<TTableSchemaExt>(chunkMeta.extensions());
            chunkKeyColumns = FromProto<TTableSchema>(schemaExt).GetKeyColumns();
        }

        bool isCompatibleKeyColumns =
            keyColumns.size() >= chunkKeyColumns.size() &&
            std::equal(
                chunkKeyColumns.begin(),
                chunkKeyColumns.end(),
                keyColumns.begin());

        // Requested key can be wider than stored.
        if (!isCompatibleKeyColumns) {
            auto error = TError("Incompatible key columns in chunk %v: requested key columns %v, chunk key columns %v",
                chunkId,
                keyColumns,
                chunkKeyColumns);
            YT_LOG_WARNING(error);
            ToProto(chunkSamples->mutable_error(), error);
            return;
        }

        auto lowerKey = sampleRequest.has_lower_key()
            ? FromProto<TLegacyOwningKey>(sampleRequest.lower_key())
            : MinKey();

        auto upperKey = sampleRequest.has_upper_key()
            ? FromProto<TLegacyOwningKey>(sampleRequest.upper_key())
            : MaxKey();

        auto blockMeta = GetProtoExtension<TDataBlockMetaExt>(chunkMeta.extensions());

        std::vector<TLegacyOwningKey> samples;
        for (const auto& block : blockMeta.data_blocks()) {
            YT_VERIFY(block.has_last_key());
            auto key = FromProto<TLegacyOwningKey>(block.last_key());
            if (key >= lowerKey && key < upperKey) {
                samples.push_back(WidenKey(key, keyColumns.size()));
            }
        }

        // Don't return more than requested.
        std::random_shuffle(samples.begin(), samples.end());
        auto count = std::min(
            static_cast<int>(samples.size()),
            sampleRequest.sample_count());
        samples.erase(samples.begin() + count, samples.end());

        for (const auto& sample : samples) {
            auto* protoSample = chunkSamples->add_samples();
            protoSample->set_key_index(keySetWriter->WriteKey(sample));
            protoSample->set_incomplete(false);
            protoSample->set_weight(1);
        }
    }

    void ProcessSortingSamples(
        const TReqGetTableSamples::TSampleRequest& sampleRequest,
        TRspGetTableSamples::TChunkSamples* chunkSamples,
        const TKeyColumns& keyColumns,
        i32 maxSampleSize,
        const TKeySetWriterPtr& keySetWriter,
        const TChunkMeta& chunkMeta)
    {
        TNameTablePtr nameTable;
        std::vector<int> keyIds;

        try {
            auto nameTableExt = FindProtoExtension<TNameTableExt>(chunkMeta.extensions());
            if (nameTableExt) {
                nameTable = FromProto<TNameTablePtr>(*nameTableExt);
            } else {
                auto schemaExt = GetProtoExtension<TTableSchemaExt>(chunkMeta.extensions());
                nameTable = TNameTable::FromSchema(FromProto<TTableSchema>(schemaExt));
            }

            for (const auto& column : keyColumns) {
                keyIds.push_back(nameTable->GetIdOrRegisterName(column));
            }
        } catch (const std::exception& ex) {
            auto chunkId = FromProto<TChunkId>(sampleRequest.chunk_id());
            YT_LOG_WARNING(ex, "Failed to gather chunk samples (ChunkId: %v)",
                chunkId);

            // We failed to deserialize name table, so we don't return any samples.
            return;
        }

        std::vector<int> idToKeyIndex(nameTable->GetSize(), -1);
        for (int i = 0; i < std::ssize(keyIds); ++i) {
            idToKeyIndex[keyIds[i]] = i;
        }

        auto samplesExt = GetProtoExtension<TSamplesExt>(chunkMeta.extensions());

        auto truncatedSampleValueBuffer = New<TRowBuffer>();

        // TODO(psushin): respect sampleRequest lower_limit and upper_limit.
        // Old chunks do not store samples weights.
        bool hasWeights = samplesExt.weights_size() > 0;
        for (int index = 0;
            index < samplesExt.entries_size() && chunkSamples->samples_size() < sampleRequest.sample_count();
            ++index)
        {
            int remaining = samplesExt.entries_size() - index;
            if (std::rand() % remaining >= sampleRequest.sample_count() - chunkSamples->samples_size()) {
                continue;
            }

            auto row = FromProto<TUnversionedOwningRow>(samplesExt.entries(index));
            std::vector<TUnversionedValue> values(
                keyColumns.size(),
                MakeUnversionedSentinelValue(EValueType::Null));

            for (const auto& value : row) {
                int keyIndex = idToKeyIndex[value.Id];
                if (keyIndex < 0) {
                    continue;
                }
                values[keyIndex] = value;
            }

            SerializeSample(
                chunkSamples->add_samples(),
                values,
                maxSampleSize,
                hasWeights ? samplesExt.weights(index) : samplesExt.entries(index).length(),
                keySetWriter,
                truncatedSampleValueBuffer);
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetColumnarStatistics)
    {
        auto workloadDescriptor = GetRequestWorkloadDescriptor(context);

        std::optional<TDuration> earlyFinishTimeout;
        if (request->enable_early_finish() && context->GetTimeout()) {
            earlyFinishTimeout = *context->GetTimeout() * GetDynamicConfig()->TestingOptions->ColumnarStatisticsReadTimeoutFraction;
        }

        context->SetRequestInfo(
            "SubrequestCount: %v, Workload: %v, EarlyFinishTimeout: %v",
            request->subrequests_size(),
            workloadDescriptor,
            earlyFinishTimeout);

        ValidateOnline();

        const auto& chunkStore = Bootstrap_->GetChunkStore();

        std::vector<TFuture<TRefCountedColumnarStatisticsSubresponsePtr>> futures;

        auto nameTable = FromProto<TNameTablePtr>(request->name_table());

        std::vector<int> subresponseIndices;
        subresponseIndices.reserve(request->subrequests_size());
        response->mutable_subresponses()->Reserve(request->subrequests_size());
        for (int index = 0; index < request->subrequests_size(); ++index) {
            response->add_subresponses();
        }

        const auto& heavyInvoker = Bootstrap_->GetStorageHeavyInvoker();
        for (int index = 0; index < request->subrequests_size(); ++index) {
            const auto& subrequest = request->subrequests(index);
            auto chunkId = FromProto<TChunkId>(subrequest.chunk_id());

            auto chunk = chunkStore->FindChunk(chunkId);
            if (!chunk) {
                auto error = TError(
                    NChunkClient::EErrorCode::NoSuchChunk,
                    "No such chunk %v",
                    chunkId);
                YT_LOG_WARNING(error);
                ToProto(response->mutable_subresponses(index)->mutable_error(), error);
                continue;
            }

            auto columnIds = FromProto<std::vector<int>>(subrequest.column_ids());
            std::vector<TColumnStableName> columnStableNames;
            columnStableNames.reserve(columnIds.size());
            for (auto id : columnIds) {
                columnStableNames.emplace_back(TString(nameTable->GetNameOrThrow(id)));
            }

            auto options = BuildReadMetaOption(context);

            auto chunkMetaFuture = chunk->ReadMeta(options)
                .Apply(BIND(
                    &TDataNodeService::ExtractColumnarStatisticsFromChunkMeta,
                    MakeStrong(this),
                    Passed(std::move(columnStableNames)),
                    chunkId)
                .AsyncVia(heavyInvoker));
            // Fault-injection for tests.
            if (auto optionalDelay = GetDynamicConfig()->TestingOptions->ColumnarStatisticsChunkMetaFetchMaxDelay) {
                chunkMetaFuture = chunkMetaFuture
                    .Apply(BIND([=, Logger = Logger, config = Config_] (const TRefCountedColumnarStatisticsSubresponsePtr& result) {
                        auto delay = index * *optionalDelay / request->subrequests_size();
                        YT_LOG_DEBUG("Injected a random delay after a chunk meta fetch (Delay: %v)", delay);
                        TDelayedExecutor::WaitForDuration(delay);
                        return result;
                    })
                    .AsyncVia(heavyInvoker));
            }
            futures.push_back(chunkMetaFuture);
            subresponseIndices.push_back(index);
        }

        YT_LOG_DEBUG("Awaiting asynchronous part of columnar statistics fetching");

        auto combinedResult = (earlyFinishTimeout ? AllSetWithTimeout(futures, *earlyFinishTimeout) : AllSet(futures));
        context->SubscribeCanceled(BIND([Logger = Logger, combinedResult = combinedResult] (const TError& error) {
            YT_LOG_DEBUG("Columnar statistics fetch cancelled, propagating cancellation to chunk meta reading");
            combinedResult.Cancel(error);
        }));
        context->ReplyFrom(combinedResult.Apply(BIND(
            &TDataNodeService::CombineColumnarStatisticsSubresponses,
            MakeWeak(this),
            context,
            subresponseIndices)));
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, DisableChunkLocations)
    {
        auto locationManager = Bootstrap_->GetLocationManager();
        auto locationUuids = FromProto<std::vector<TGuid>>(request->location_uuids());

        context->SetRequestInfo("LocationUuids: %v", locationUuids);

        context->ReplyFrom(locationManager->DisableChunkLocations({locationUuids.begin(), locationUuids.end()})
            .Apply(BIND([=] (const std::vector<TGuid>& locationUuids) {
                context->SetResponseInfo("LocationUuids: %v", locationUuids);

                ToProto(response->mutable_location_uuids(), locationUuids);
            })));
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, DestroyChunkLocations)
    {
        auto locationManager = Bootstrap_->GetLocationManager();
        auto recoverUnlinkedDisks = request->recover_unlinked_disks();
        auto locationUuids = FromProto<std::vector<TGuid>>(request->location_uuids());

        context->SetRequestInfo("RecoverUnlinkedDisks: %v, LocationUuids: %v",
            recoverUnlinkedDisks,
            locationUuids);

        context->ReplyFrom(locationManager->DestroyChunkLocations(
            recoverUnlinkedDisks,
            {locationUuids.begin(), locationUuids.end()})
            .Apply(BIND([=] (const std::vector<TGuid>& locationUuids) {
                context->SetResponseInfo("LocationUuids: %v", locationUuids);

                ToProto(response->mutable_location_uuids(), locationUuids);
            })));
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, ResurrectChunkLocations)
    {
        auto locationManager = Bootstrap_->GetLocationManager();
        auto locationUuids = FromProto<std::vector<TGuid>>(request->location_uuids());

        context->SetRequestInfo("LocationUuids: %v", locationUuids);

        context->ReplyFrom(locationManager->ResurrectChunkLocations({locationUuids.begin(), locationUuids.end()})
            .Apply(BIND([=] (const std::vector<TGuid>& locationUuids) {
                context->SetResponseInfo("LocationUuids: %v", locationUuids);

                ToProto(response->mutable_location_uuids(), locationUuids);
            })));
    }

    void CombineColumnarStatisticsSubresponses(
        const TCtxGetColumnarStatisticsPtr& context,
        const std::vector<int>& subresponseIndices,
        const std::vector<TErrorOr<TRefCountedColumnarStatisticsSubresponsePtr>>& subresponsesOrErrors)
    {
        YT_LOG_DEBUG("Combining columnar statistics subresponses (SubresponseCount: %v)", subresponsesOrErrors.size());

        int successCount = 0;
        int timeoutCount = 0;
        int errorCount = 0;

        auto& response = context->Response();
        for (int index = 0; index < std::ssize(subresponsesOrErrors); ++index) {
            if (subresponsesOrErrors[index].IsOK()) {
                response.mutable_subresponses(subresponseIndices[index])->Swap(subresponsesOrErrors[index].Value().Get());

                if (subresponsesOrErrors[index].Value()->has_error()) {
                    ++errorCount;
                } else {
                    ++successCount;
                }
            } else {
                ToProto(
                    response.mutable_subresponses(subresponseIndices[index])->mutable_error(),
                    subresponsesOrErrors[index]);
                ++timeoutCount;
            }
        }

        context->SetResponseInfo(
            "SuccessCount: %v, TimeoutCount: %v, ErrorCount: %v",
            successCount,
            timeoutCount,
            errorCount);
    }

    TRefCountedColumnarStatisticsSubresponsePtr ExtractColumnarStatisticsFromChunkMeta(
        const std::vector<TColumnStableName>& columnStableNames,
        TChunkId chunkId,
        const TErrorOr<TRefCountedChunkMetaPtr>& metaOrError)
    {
        YT_LOG_DEBUG("Extracting columnar statistics from chunk meta (ChunkId: %v)", chunkId);

        auto subresponse = New<TRefCountedColumnarStatisticsSubresponse>();
        try {
            THROW_ERROR_EXCEPTION_IF_FAILED(metaOrError, "Error getting meta");
            const auto& meta = *metaOrError.Value();

            auto type = EChunkType(meta.type());
            if (type != EChunkType::Table) {
                THROW_ERROR_EXCEPTION(
                    "Invalid type: expected %Qlv, actual %Qlv",
                    EChunkType::Table,
                    type);
            }

            auto optionalColumnarStatisticsExt = FindProtoExtension<TColumnarStatisticsExt>(meta.extensions());
            if (!optionalColumnarStatisticsExt) {
                THROW_ERROR_EXCEPTION(
                    NChunkClient::EErrorCode::MissingExtension,
                    "Columnar statistics chunk meta extension missing");
            }
            const auto& columnarStatisticsExt = *optionalColumnarStatisticsExt;
            auto largeColumnarStatisticsExt = FindProtoExtension<TLargeColumnarStatisticsExt>(meta.extensions());

            auto nameTableExt = FindProtoExtension<TNameTableExt>(meta.extensions());
            TNameTablePtr nameTable;
            if (nameTableExt) {
                nameTable = FromProto<TNameTablePtr>(*nameTableExt);
            } else {
                auto schemaExt = GetProtoExtension<TTableSchemaExt>(meta.extensions());
                nameTable = TNameTable::FromSchemaStable(FromProto<TTableSchema>(schemaExt));
            }

            TColumnarStatistics columnarStatistics;
            i64 chunkRowCount = GetProtoExtension<TMiscExt>(meta.extensions()).row_count();

            FromProto(
                &columnarStatistics,
                columnarStatisticsExt,
                largeColumnarStatisticsExt ? &*largeColumnarStatisticsExt : nullptr,
                chunkRowCount);
            auto selectedStatistics = columnarStatistics.SelectByColumnNames(nameTable, columnStableNames);
            ToProto(subresponse->mutable_columnar_statistics(),
                selectedStatistics);

            const auto& largeStatistics = selectedStatistics.LargeStatistics;
            if (!largeStatistics.Empty()) {
                ToProto(subresponse->mutable_large_columnar_statistics(), largeStatistics);
            }

            // COMPAT(denvid): legacy response fields. Remove once all clusters are 23.2+.

            subresponse->mutable_column_data_weights()->Reserve(columnStableNames.size());
            for (const auto& columnName : columnStableNames) {
                auto id = nameTable->FindId(columnName.Underlying());
                if (id && *id < columnarStatisticsExt.column_data_weights().size()) {
                    subresponse->add_column_data_weights(columnarStatisticsExt.column_data_weights(*id));
                } else {
                    subresponse->add_column_data_weights(0);
                }
            }
            if (columnarStatisticsExt.has_timestamp_total_weight()) {
                subresponse->set_timestamp_total_weight(columnarStatisticsExt.timestamp_total_weight());
            }

            YT_LOG_DEBUG("Columnar statistics extracted from chunk meta (ChunkId: %v)", chunkId);
        } catch (const std::exception& ex) {
            auto error = TError("Error fetching columnar statistics for chunk %v", chunkId) << ex;
            YT_LOG_WARNING(error);
            ToProto(subresponse->mutable_error(), error);
        }

        return subresponse;
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, UpdatePeer)
    {
        // NB: this method is no longer used, but noop stub is kept here for smooth rolling update.
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, AnnounceChunkReplicas)
    {
        context->SetRequestInfo("SubrequestCount: %v, SourceNodeId: %v",
            request->announcements_size(),
            request->source_node_id());

        const auto& allyReplicaManager = Bootstrap_->GetAllyReplicaManager();
        allyReplicaManager->OnAnnouncementsReceived(
            TRange(request->announcements()),
            FromProto<TNodeId>(request->source_node_id()));

        context->Reply();
    }

    bool IsCloseDemanded(const TStoreLocationPtr& location)
    {
        const auto& sessionManager = Bootstrap_->GetSessionManager();
        return location->IsSick() || sessionManager->GetDisableWriteSessions();
    }

    void ValidateOnline()
    {
        if (!Bootstrap_->GetMasterConnector()->IsOnline()) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::MasterNotConnected,
                "Master is not connected");
        }
    }

    struct TNetThrottlingResult
    {
        bool Enabled;
        i64 QueueSize;
    };

    template <class TContextPtr>
    TNetThrottlingResult CheckNetOutThrottling(
        const TContextPtr& context,
        const TWorkloadDescriptor& workloadDescriptor,
        bool incrementCounter = true)
    {
        const auto& netThrottler = Bootstrap_->GetOutThrottler(workloadDescriptor);
        auto netQueueSize =
            netThrottler->GetQueueTotalAmount() +
            context->GetBusNetworkStatistics().PendingOutBytes;
        auto netQueueLimit = GetDynamicConfig()->NetOutThrottlingLimit.value_or(
            Config_->NetOutThrottlingLimit);
        bool throttle = netQueueSize > netQueueLimit;
        if (throttle && incrementCounter) {
            Bootstrap_->GetNetworkStatistics().IncrementReadThrottlingCounter(
                context->GetEndpointAttributes().Get("network", DefaultNetworkName));
        }
        return TNetThrottlingResult{.Enabled = throttle, .QueueSize = netQueueSize};
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateDataNodeService(
    TDataNodeConfigPtr config,
    IBootstrap* bootstrap)
{
    return New<TDataNodeService>(
        config,
        bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
