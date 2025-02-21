#include "job.h"

#include "bootstrap.h"
#include "private.h"
#include "chunk.h"
#include "chunk_store.h"
#include "config.h"
#include "journal_chunk.h"
#include "journal_dispatcher.h"
#include "location.h"
#include "master_connector.h"

#include <yt/yt/server/lib/chunk_server/proto/job.pb.h>

#include <yt/yt/server/lib/io/io_tracker.h>

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/lib/hydra_common/file_changelog.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_memory_manager.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/chunk_writer.h>
#include <yt/yt/ytlib/chunk_client/confirming_writer.h>
#include <yt/yt/ytlib/chunk_client/meta_aggregating_writer.h>
#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>
#include <yt/yt/ytlib/chunk_client/erasure_adaptive_repair.h>
#include <yt/yt/ytlib/chunk_client/erasure_repair.h>
#include <yt/yt/ytlib/chunk_client/erasure_part_writer.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/parallel_reader_memory_manager.h>
#include <yt/yt/ytlib/chunk_client/replication_reader.h>
#include <yt/yt/ytlib/chunk_client/replication_writer.h>
#include <yt/yt/ytlib/chunk_client/striped_erasure_reader.h>

#include <yt/yt/ytlib/journal_client/erasure_repair.h>
#include <yt/yt/ytlib/journal_client/chunk_reader.h>
#include <yt/yt/ytlib/journal_client/helpers.h>

#include <yt/yt/ytlib/journal_client/proto/format.pb.h>

#include <yt/yt/ytlib/misc/public.h>
#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/node_tracker_client/helpers.h>
#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/ytlib/object_client/helpers.h>

#include <yt/yt/ytlib/table_client/cached_versioned_chunk_meta.h>

#include <yt/yt/ytlib/misc/public.h>
#include <yt/yt/ytlib/misc/memory_usage_tracker.h>
#include <yt/yt/ytlib/misc/memory_reference_tracker.h>

#include <yt/yt/ytlib/table_client/chunk_state.h>
#include <yt/yt/ytlib/table_client/columnar_chunk_meta.h>
#include <yt/yt/ytlib/table_client/schemaless_chunk_reader.h>
#include <yt/yt/ytlib/table_client/schemaless_multi_chunk_reader.h>
#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>
#include <yt/yt/ytlib/table_client/versioned_chunk_reader.h>
#include <yt/yt/ytlib/table_client/versioned_chunk_writer.h>

#include <yt/yt/library/erasure/impl/codec.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/client/misc/io_tags.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/client/rpc/helpers.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/core/actions/cancelable_context.h>
#include <yt/yt/core/actions/new_with_offloaded_dtor.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <util/generic/algorithm.h>

namespace NYT::NDataNode {

using namespace NApi;
using namespace NObjectClient;
using namespace NNodeTrackerClient;
using namespace NJobAgent;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NChunkServer;
using namespace NChunkServer::NProto;
using namespace NClusterNode;
using namespace NNodeTrackerClient::NProto;
using namespace NConcurrency;
using namespace NYson;
using namespace NCoreDump;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NIO;
using namespace NTracing;
using namespace NJournalClient;
using namespace NYTree;

using NChunkClient::TChunkReaderStatistics;
using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TMasterJobBase::TMasterJobBase(
    NChunkServer::TJobId jobId,
    const NChunkServer::NProto::TJobSpec& jobSpec,
    TString jobTrackerAddress,
    const TJobResources& resourceLimits,
    const TJobResourceAttributes& resourceAttributes,
    IBootstrap* bootstrap)
    : TResourceHolder(
        bootstrap->GetJobResourceManager().Get(),
        EResourcesConsumerType::MasterJob,
        DataNodeLogger.WithTag(
            "JobId: %v, JobType: %v",
            jobId,
            CheckedEnumCast<EJobType>(jobSpec.type())),
        resourceLimits,
        resourceAttributes,
        0)
    , Bootstrap_(bootstrap)
    , Config_(Bootstrap_->GetConfig()->DataNode)
    , JobId_(jobId)
    , JobSpec_(jobSpec)
    , JobTrackerAddress_(std::move(jobTrackerAddress))
    , NodeDirectory_(Bootstrap_->GetNodeDirectory())
    , StartTime_(TInstant::Now())
{
    VERIFY_THREAD_AFFINITY(JobThread);
}

void TMasterJobBase::Start()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    YT_VERIFY(!std::exchange(Started_, true));

    JobState_ = EJobState::Running;
    JobFuture_ = BIND(&TMasterJobBase::GuardedRun, MakeStrong(this))
        .AsyncVia(Bootstrap_->GetJobInvoker())
        .Run();

    YT_VERIFY(GetPorts().empty());
}

bool TMasterJobBase::IsStarted() const noexcept
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return Started_;
}

void TMasterJobBase::OnResourcesAcquired() noexcept
{
    Start();
}

TFuture<void> TMasterJobBase::ReleaseCumulativeResources()
{
    return VoidFuture;
}

void TMasterJobBase::Abort(const TError& error)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    switch (JobState_) {
        case EJobState::Waiting:
            SetAborted(error);
            return;

        case EJobState::Running:
            JobFuture_.Cancel(error);
            SetAborted(error);
            return;

        default:
            return;
    }
}

NChunkServer::TJobId TMasterJobBase::GetId() const noexcept
{
    VERIFY_THREAD_AFFINITY_ANY();

    return JobId_;
}

TGuid TMasterJobBase::GetIdAsGuid() const noexcept
{
    VERIFY_THREAD_AFFINITY_ANY();

    return JobId_.Underlying();
}

EJobType TMasterJobBase::GetType() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return CheckedEnumCast<EJobType>(JobSpec_.type());
}

bool TMasterJobBase::IsUrgent() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return JobSpec_.urgent();
}

const TString& TMasterJobBase::GetJobTrackerAddress() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return JobTrackerAddress_;
}

EJobState TMasterJobBase::GetState() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return JobState_;
}

TJobResources TMasterJobBase::GetResourceUsage() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return TResourceHolder::GetResourceUsage();
}

TJobResult TMasterJobBase::GetResult() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return Result_;
}

TInstant TMasterJobBase::GetStartTime() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return StartTime_;
}


TBriefJobInfo TMasterJobBase::GetBriefInfo() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto [
        baseResourceUsage,
        additionalResourceUsage
    ] = TResourceHolder::GetDetailedResourceUsage();

    return TBriefJobInfo(
        JobId_,
        JobState_,
        GetType(),
        GetJobTrackerAddress(),
        GetStartTime(),
        /*jobDuration=*/ TInstant::Now() - GetStartTime(),
        baseResourceUsage,
        additionalResourceUsage,
        TResourceHolder::GetPorts());
}

void TMasterJobBase::GuardedRun()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto context = TTraceContext::NewRoot(Format("%vJob.Run", GetType()));
    TCurrentTraceContextGuard guard(context);
    auto baggage = context->UnpackOrCreateBaggage();
    AddTagToBaggage(baggage, ERawIOTag::JobId, ToString(GetId()));
    AddTagToBaggage(baggage, EAggregateIOTag::JobType, FormatEnum(GetType()));
    context->PackBaggage(std::move(baggage));

    try {
        JobPrepared_.Fire();
        WaitFor(BIND(&TMasterJobBase::DoRun, MakeStrong(this))
            .AsyncVia(Bootstrap_->GetMasterJobInvoker())
            .Run())
            .ThrowOnError();
    } catch (const std::exception& ex) {
        SetFailed(ex);
        return;
    }
    SetCompleted();
}

void TMasterJobBase::SetCompleted()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    YT_LOG_INFO("Job completed");
    DoSetFinished(EJobState::Completed, TError());
}

void TMasterJobBase::SetFailed(const TError& error)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    YT_LOG_ERROR(error, "Job failed");
    DoSetFinished(EJobState::Failed, error);
}

void TMasterJobBase::SetAborted(const TError& error)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    YT_LOG_INFO(error, "Job aborted");
    DoSetFinished(EJobState::Aborted, error);
}

IChunkPtr TMasterJobBase::FindLocalChunk(TChunkId chunkId, int mediumIndex)
{
    VERIFY_THREAD_AFFINITY_ANY();

    const auto& chunkStore = Bootstrap_->GetChunkStore();
    return chunkStore->FindChunk(chunkId, mediumIndex);
}

IChunkPtr TMasterJobBase::GetLocalChunkOrThrow(TChunkId chunkId, int mediumIndex)
{
    VERIFY_THREAD_AFFINITY_ANY();

    const auto& chunkStore = Bootstrap_->GetChunkStore();
    return chunkStore->GetChunkOrThrow(chunkId, mediumIndex);
}

void TMasterJobBase::DoSetFinished(
    EJobState finalState,
    const TError& error)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobState_ != EJobState::Running && JobState_ != EJobState::Waiting) {
        return;
    }

    // Job resources lifetime steps:
    // 1. Resources are allocated for job for the entire lifetime of job.
    // 2. Job started.
    // 3. Resources are allocated for a job by request of the job (while ResourceState == Acquired).
    // 4. Job finished.
    // 5. Resources are released for a job by request of the job (while ResourceState == Acquired).
    // 6. Resources allocated for the entire lifetime of the job are released (allocated resources are reset to zero).

    // 4th step.
    JobState_ = finalState;
    ToProto(Result_.mutable_error(), error);

    JobFinished_.Fire();

    if (auto jobFuture = JobFuture_) {
        jobFuture.Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TError& error) {
            YT_LOG_DEBUG_IF(
                !error.IsOK(),
                error,
                "Master job finished with error");

            // 5th step.
            ReleaseCumulativeResources()
                // 6th step.
                .Apply(BIND(&TMasterJobBase::ReleaseResources, MakeStrong(this))
                .AsyncVia(Bootstrap_->GetJobInvoker()))
                .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TError& error) {
                    YT_LOG_FATAL_IF(
                        !error.IsOK(),
                        error,
                        "Failed to release master job resources");
                })
                .Via(Bootstrap_->GetJobInvoker()));
        }));

        JobFuture_.Reset();
    }
}

////////////////////////////////////////////////////////////////////////////////

class TChunkRemovalJob
    : public TMasterJobBase
{
public:
    TChunkRemovalJob(
        NChunkServer::TJobId jobId,
        const TJobSpec& jobSpec,
        TString jobTrackerAddress,
        const TJobResources& resourceLimits,
        const TJobResourceAttributes& resourceAttributes,
        IBootstrap* bootstrap)
        : TMasterJobBase(
            jobId,
            std::move(jobSpec),
            std::move(jobTrackerAddress),
            resourceLimits,
            resourceAttributes,
            bootstrap)
        , JobSpecExt_(JobSpec_.GetExtension(TRemoveChunkJobSpecExt::remove_chunk_job_spec_ext))
        , ChunkId_(FromProto<TChunkId>(JobSpecExt_.chunk_id()))
        , DynamicConfig_(Bootstrap_->GetDynamicConfigManager()->GetConfig()->DataNode->RemoveChunkJob)
    {
        Logger.AddTag("ChunkId: %v", ChunkId_);
    }

private:
    const TRemoveChunkJobSpecExt JobSpecExt_;

    const TChunkId ChunkId_;

    const TRemoveChunkJobDynamicConfigPtr DynamicConfig_;

    void DoRun() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        int mediumIndex = JobSpecExt_.medium_index();
        auto replicas = FromProto<TChunkReplicaList>(JobSpecExt_.replicas());
        auto replicasExpirationDeadline = FromProto<TInstant>(JobSpecExt_.replicas_expiration_deadline());
        auto chunkIsDead = JobSpecExt_.chunk_is_dead();

        YT_LOG_INFO("Chunk removal job started (MediumIndex: %v, Replicas: %v, ReplicasExpirationDeadline: %v, ChunkIsDead: %v, DelayBeforeStartRemoveChunk: %v)",
            mediumIndex,
            replicas,
            replicasExpirationDeadline,
            chunkIsDead,
            DynamicConfig_->DelayBeforeStartRemoveChunk);

        // TODO(ifsmirnov, akozhikhov): Consider DRT here.

        auto chunk = chunkIsDead
            ? FindLocalChunk(ChunkId_, mediumIndex)
            : GetLocalChunkOrThrow(ChunkId_, mediumIndex);

        if (!chunk) {
            YT_VERIFY(chunkIsDead);
            YT_LOG_INFO("Dead chunk is missing, reporting success");
            return;
        }

        const auto& chunkStore = Bootstrap_->GetChunkStore();
        WaitFor(chunkStore->RemoveChunk(chunk, DynamicConfig_->DelayBeforeStartRemoveChunk))
            .ThrowOnError();

        // Wait for the removal notification to be delivered to master.
        // Cf. YT-6532.
        // Once we switch from push replication to pull, this code is likely
        // to appear in TReplicateChunkJob as well.
        YT_LOG_INFO("Waiting for heartbeat barrier");
        const auto& masterConnector = Bootstrap_->GetMasterConnector();
        WaitFor(masterConnector->GetHeartbeatBarrier(CellTagFromId(ChunkId_)))
            .ThrowOnError();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TChunkReplicationJob
    : public TMasterJobBase
{
public:
    TChunkReplicationJob(
        NChunkServer::TJobId jobId,
        const TJobSpec& jobSpec,
        TString jobTrackerAddress,
        const TJobResources& resourceLimits,
        const TJobResourceAttributes& resourceAttributes,
        IBootstrap* bootstrap)
        : TMasterJobBase(
            jobId,
            std::move(jobSpec),
            std::move(jobTrackerAddress),
            resourceLimits,
            resourceAttributes,
            bootstrap)
        , JobSpecExt_(JobSpec_.GetExtension(TReplicateChunkJobSpecExt::replicate_chunk_job_spec_ext))
        , ChunkId_(FromProto<TChunkId>(JobSpecExt_.chunk_id()))
        , DynamicConfig_(Bootstrap_->GetDynamicConfigManager()->GetConfig()->DataNode->ReplicateChunkJob)
    {
        Logger.AddTag("ChunkId: %v", ChunkId_);
    }

private:
    const TReplicateChunkJobSpecExt JobSpecExt_;
    const TChunkId ChunkId_;
    const TReplicateChunkJobDynamicConfigPtr DynamicConfig_;

    void DoRun() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        int sourceMediumIndex = JobSpecExt_.source_medium_index();
        auto targetReplicas = FromProto<TChunkReplicaWithMediumList>(JobSpecExt_.target_replicas());
        auto isPullReplicationJob = JobSpecExt_.is_pull_replication_job();
        NodeDirectory_->MergeFrom(JobSpecExt_.node_directory());

        // Compute target medium index.
        if (targetReplicas.empty()) {
            THROW_ERROR_EXCEPTION("No target replicas");
        }
        int targetMediumIndex = targetReplicas[0].GetMediumIndex();
        auto sessionId = TSessionId(ChunkId_, targetMediumIndex);

        YT_LOG_INFO("Chunk replication job started (SourceMediumIndex: %v, TargetReplicas: %v, IsPullReplicationJob: %v)",
            sourceMediumIndex,
            MakeFormattableView(targetReplicas, TChunkReplicaAddressFormatter(NodeDirectory_)),
            isPullReplicationJob);

        TWorkloadDescriptor workloadDescriptor;
        workloadDescriptor.Category = EWorkloadCategory::SystemReplication;
        workloadDescriptor.Annotations.push_back(Format("Replication of chunk %v%v",
            ChunkId_,
            isPullReplicationJob ? " as a virtual pull" : ""));
        workloadDescriptor.DiskFairShareBucketTag = ToString(JobId_);

        auto chunk = GetLocalChunkOrThrow(ChunkId_, sourceMediumIndex);

        auto trackSystemJobsMemory = Bootstrap_
            ->GetDataNodeBootstrap()
            ->GetDynamicConfigManager()
            ->GetConfig()
            ->DataNode
            ->TrackSystemJobsMemory;
        auto tracker = Bootstrap_
            ->GetNodeMemoryReferenceTracker()
            ->WithCategory(EMemoryCategory::SystemJobs);

        TChunkReadOptions chunkReadOptions;
        chunkReadOptions.WorkloadDescriptor = workloadDescriptor;
        chunkReadOptions.BlockCache = Bootstrap_->GetBlockCache();
        chunkReadOptions.ChunkReaderStatistics = New<TChunkReaderStatistics>();
        chunkReadOptions.MemoryReferenceTracker = trackSystemJobsMemory ? tracker : nullptr;
        chunkReadOptions.TrackMemoryAfterSessionCompletion = Bootstrap_
            ->GetDataNodeBootstrap()
            ->GetDynamicConfigManager()
            ->GetConfig()
            ->DataNode
            ->TrackMemoryAfterSessionCompletion;

        TRefCountedChunkMetaPtr meta;
        {
            YT_LOG_DEBUG("Fetching chunk meta");

            auto asyncMeta = chunk->ReadMeta(chunkReadOptions);
            meta = WaitFor(asyncMeta)
                .ValueOrThrow();

            YT_LOG_DEBUG("Chunk meta fetched");
        }

        auto options = New<TRemoteWriterOptions>();
        options->AllowAllocatingNewTargetNodes = false;

        auto writer = CreateReplicationWriter(
            DynamicConfig_->Writer,
            options,
            sessionId,
            std::move(targetReplicas),
            Bootstrap_->GetClient(),
            Bootstrap_->GetLocalHostName(),
            GetNullBlockCache(),
            /*trafficMeter*/ nullptr,
            Bootstrap_->GetThrottler(EDataNodeThrottlerKind::ReplicationOut));

        {
            YT_LOG_DEBUG("Started opening writer");

            WaitFor(writer->Open())
                .ThrowOnError();

            YT_LOG_DEBUG("Writer opened");
        }

        int currentBlockIndex = 0;
        int blockCount = GetBlockCount(ChunkId_, *meta);
        while (currentBlockIndex < blockCount) {
            auto asyncReadBlocks = chunk->ReadBlockRange(
                currentBlockIndex,
                blockCount - currentBlockIndex,
                chunkReadOptions);

            auto readBlocks = WaitFor(asyncReadBlocks)
                .ValueOrThrow();

            i64 totalBlockSize = 0;
            for (const auto& block : readBlocks) {
                if (block) {
                    totalBlockSize += block.Size();
                }
            }
            if (totalBlockSize > 0 && Bootstrap_->GetIOTracker()->IsEnabled()) {
                const auto& location = chunk->GetLocation();

                Bootstrap_->GetIOTracker()->Enqueue(
                    TIOCounters{
                        .Bytes = totalBlockSize,
                        .IORequests = 1
                    },
                    /*tags*/ {
                        {FormatIOTag(ERawIOTag::LocationId), ToString(location->GetId())},
                        {FormatIOTag(EAggregateIOTag::LocationType), FormatEnum(location->GetType())},
                        {FormatIOTag(EAggregateIOTag::Medium), location->GetMediumName()},
                        {FormatIOTag(EAggregateIOTag::DiskFamily), location->GetDiskFamily()},
                        {FormatIOTag(EAggregateIOTag::Direction), "read"},
                        {FormatIOTag(ERawIOTag::ChunkId), ToString(DecodeChunkId(ChunkId_).Id)},
                        {FormatIOTag(EAggregateIOTag::User), "root"},
                    });
            }

            std::vector<TBlock> writeBlocks;
            for (const auto& block : readBlocks) {
                if (!block)
                    break;
                writeBlocks.push_back(block);
            }

            YT_LOG_DEBUG("Enqueuing blocks for replication (Blocks: %v-%v)",
                currentBlockIndex,
                currentBlockIndex + static_cast<int>(writeBlocks.size()) - 1);

            auto writeResult = writer->WriteBlocks(workloadDescriptor, writeBlocks);
            if (!writeResult) {
                WaitFor(writer->GetReadyEvent())
                    .ThrowOnError();
            }

            currentBlockIndex += writeBlocks.size();
        }

        YT_LOG_DEBUG("All blocks are enqueued for replication");

        {
            YT_LOG_DEBUG("Started closing writer");

            auto deferredMeta = New<TDeferredChunkMeta>();
            deferredMeta->MergeFrom(*meta);

            WaitFor(writer->Close(workloadDescriptor, deferredMeta))
                .ThrowOnError();

            YT_LOG_DEBUG("Writer closed");
        }
    }

    static int GetBlockCount(TChunkId chunkId, const TChunkMeta& meta)
    {
        switch (TypeFromId(DecodeChunkId(chunkId).Id)) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk: {
                auto blocksExt = GetProtoExtension<NChunkClient::NProto::TBlocksExt>(meta.extensions());
                return blocksExt.blocks_size();
            }

            case EObjectType::JournalChunk:
            case EObjectType::ErasureJournalChunk: {
                auto miscExt = GetProtoExtension<TMiscExt>(meta.extensions());
                if (!miscExt.sealed()) {
                    THROW_ERROR_EXCEPTION("Cannot replicate an unsealed chunk %v",
                        chunkId);
                }
                return miscExt.row_count();
            }

            default:
                YT_ABORT();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TChunkRepairJob
    : public TMasterJobBase
{
public:
    TChunkRepairJob(
        NChunkServer::TJobId jobId,
        const TJobSpec& jobSpec,
        TString jobTrackerAddress,
        const TJobResources& resourceLimits,
        const TJobResourceAttributes& resourceAttributes,
        IBootstrap* bootstrap,
        TMasterJobSensors sensors)
        : TMasterJobBase(
            jobId,
            std::move(jobSpec),
            std::move(jobTrackerAddress),
            resourceLimits,
            resourceAttributes,
            bootstrap)
        , JobSpecExt_(JobSpec_.GetExtension(TRepairChunkJobSpecExt::repair_chunk_job_spec_ext))
        , ChunkId_(FromProto<TChunkId>(JobSpecExt_.chunk_id()))
        , SourceReplicas_(ParseSourceReplicas(JobSpecExt_))
        , TargetReplicas_(FromProto<TChunkReplicaWithMediumList>(JobSpecExt_.target_replicas()))
        , Sensors_(std::move(sensors))
        , DynamicConfig_(Bootstrap_->GetDynamicConfigManager()->GetConfig()->DataNode->RepairChunkJob)
    {
        Logger.AddTag("ChunkId: %v", ChunkId_);
    }

private:
    const TRepairChunkJobSpecExt JobSpecExt_;
    const TChunkId ChunkId_;
    const TChunkReplicaWithMediumList SourceReplicas_;
    const TChunkReplicaWithMediumList TargetReplicas_;
    const TMasterJobSensors Sensors_;
    const TRepairChunkJobDynamicConfigPtr DynamicConfig_;

    // COMPAT(babenko)
    static TChunkReplicaWithMediumList ParseSourceReplicas(const TRepairChunkJobSpecExt& jobSpecExt)
    {
        if (jobSpecExt.source_replicas_size() == 0) {
            TChunkReplicaWithMediumList result;
            for (auto replica : FromProto<TChunkReplicaList>(jobSpecExt.legacy_source_replicas())) {
                result.emplace_back(replica);
            }
            return result;
        } else {
            return FromProto<TChunkReplicaWithMediumList>(jobSpecExt.source_replicas());
        }
    }

    IChunkReaderAllowingRepairPtr CreateReader(int partIndex)
    {
        TChunkReplicaWithMediumList partReplicas;
        for (auto replica : SourceReplicas_) {
            if (replica.GetReplicaIndex() == partIndex) {
                partReplicas.push_back(replica);
            }
        }

        auto partChunkId = ErasurePartIdFromChunkId(ChunkId_, partIndex);
        if (partReplicas.empty()) {
            return CreateUnavailablePartReader(partChunkId);
        }

        auto options = New<TRemoteReaderOptions>();
        options->AllowFetchingSeedsFromMaster = false;

        auto chunkReaderHost = New<TChunkReaderHost>(
            Bootstrap_->GetClient(),
            Bootstrap_->GetLocalDescriptor(),
            Bootstrap_->GetBlockCache(),
            /*chunkMetaCache*/ nullptr,
            /*nodeStatusDirectory*/ nullptr,
            /*bandwidthThrottler*/ Bootstrap_->GetThrottler(EDataNodeThrottlerKind::RepairIn),
            /*rpsThrottler*/ GetUnlimitedThrottler(),
            /*trafficMeter*/ nullptr);

        return CreateReplicationReader(
            DynamicConfig_->Reader,
            options,
            std::move(chunkReaderHost),
            partChunkId,
            partReplicas);
    }

    IChunkWriterPtr CreateWriter(int partIndex)
    {
        auto targetReplica = [&] {
            for (auto replica : TargetReplicas_) {
                if (replica.GetReplicaIndex() == partIndex) {
                    return replica;
                }
            }
            YT_ABORT();
        }();
        auto partChunkId = ErasurePartIdFromChunkId(ChunkId_, partIndex);
        auto partSessionId = TSessionId(partChunkId, targetReplica.GetMediumIndex());

        auto options = New<TRemoteWriterOptions>();
        options->AllowAllocatingNewTargetNodes = false;

        return CreateReplicationWriter(
            DynamicConfig_->Writer,
            options,
            partSessionId,
            TChunkReplicaWithMediumList(1, targetReplica),
            Bootstrap_->GetClient(),
            Bootstrap_->GetLocalHostName(),
            GetNullBlockCache(),
            /*trafficMeter*/ nullptr,
            Bootstrap_->GetThrottler(EDataNodeThrottlerKind::RepairOut));
    }

    TFuture<void> StartChunkRepairJob(
        NErasure::ICodec* codec,
        const NErasure::TPartIndexList& erasedPartIndexes,
        const IChunkReader::TReadBlocksOptions& readBlocksOptions,
        const std::vector<IChunkWriterPtr>& writers)
    {
        auto readerConfig = DynamicConfig_->Reader;
        auto stripedErasure = JobSpecExt_.striped_erasure_chunk();

        // TODO(gritukan): Implement adaptive repair for striped erasure.
        if (readerConfig->EnableAutoRepair && !stripedErasure) {
            YT_LOG_INFO("Executing adaptive chunk repair (ReplicationReaderSpeedLimitPerSec: %v, "
                "SlowReaderExpirationTimeout: %v, ReplicationReaderTimeout: %v, ReplicationReaderFailureTimeout: %v)",
                readerConfig->ReplicationReaderSpeedLimitPerSec,
                readerConfig->SlowReaderExpirationTimeout,
                readerConfig->ReplicationReaderTimeout,
                readerConfig->ReplicationReaderFailureTimeout);

            std::vector<IChunkReaderAllowingRepairPtr> readers;
            for (int partIndex = 0; partIndex < codec->GetTotalPartCount(); ++partIndex) {
                readers.push_back(CreateReader(partIndex));
            }
            auto future = AdaptiveRepairErasedParts(
                ChunkId_,
                codec,
                readerConfig,
                erasedPartIndexes,
                readers,
                BIND(&TChunkRepairJob::CreateWriter, MakeStrong(this)),
                readBlocksOptions,
                Logger,
                Sensors_.AdaptivelyRepairedChunksCounter);

            future.Subscribe(BIND([this, this_ = MakeStrong(this)] (const TErrorOr<void>& handler) {
                if (handler.IsOK()) {
                    Sensors_.TotalRepairedChunksCounter.Increment();
                } else {
                    Sensors_.FailedRepairChunksCounter.Increment();
                }
            }));

            return future;
        }

        // Legacy: make single repair attempt.
        auto repairPartIndexes = codec->GetRepairIndices(erasedPartIndexes);
        if (!repairPartIndexes) {
            THROW_ERROR_EXCEPTION("Codec is unable to repair the chunk");
        }

        NErasure::TPartIndexSet availableReplicas;
        for (auto replica : SourceReplicas_) {
            availableReplicas.set(replica.GetReplicaIndex());
        }

        std::vector<IChunkReaderAllowingRepairPtr> readers;
        for (int partIndex : *repairPartIndexes) {
            if (!availableReplicas.test(partIndex)) {
                THROW_ERROR_EXCEPTION("No source replicas for part %v", partIndex);
            }
            readers.push_back(CreateReader(partIndex));
        }

        if (stripedErasure) {
            auto windowSize = DynamicConfig_->WindowSize;
            auto memoryManager = New<TChunkReaderMemoryManager>(TChunkReaderMemoryManagerOptions(
                windowSize,
                {},
                false));

            return RepairErasedPartsStriped(
                readerConfig,
                codec,
                std::move(readers),
                std::move(writers),
                std::move(memoryManager),
                readBlocksOptions);
        } else {
            return RepairErasedParts(
                codec,
                erasedPartIndexes,
                readers,
                writers,
                readBlocksOptions);
        }
    }

    void DoRun() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto codecId = CheckedEnumCast<NErasure::ECodec>(JobSpecExt_.erasure_codec());
        auto* codec = NErasure::GetCodec(codecId);
        auto decommission = JobSpecExt_.decommission();
        auto rowCount = JobSpecExt_.has_row_count() ? std::make_optional<i64>(JobSpecExt_.row_count()) : std::nullopt;

        NodeDirectory_->MergeFrom(JobSpecExt_.node_directory());

        YT_LOG_INFO("Chunk repair job started (Codec: %v, "
            "SourceReplicas: %v, TargetReplicas: %v, Decommission: %v, RowCount: %v)",
            codecId,
            MakeFormattableView(SourceReplicas_, TChunkReplicaAddressFormatter(NodeDirectory_)),
            MakeFormattableView(TargetReplicas_, TChunkReplicaAddressFormatter(NodeDirectory_)),
            decommission,
            rowCount);

        TWorkloadDescriptor workloadDescriptor;
        workloadDescriptor.Category = decommission ? EWorkloadCategory::SystemReplication : EWorkloadCategory::SystemRepair;
        workloadDescriptor.Annotations.push_back(Format("%v of chunk %v",
            decommission ? "Decommission via repair" : "Repair",
            ChunkId_));
        workloadDescriptor.DiskFairShareBucketTag = ToString(JobId_);

        auto trackSystemJobsMemory = Bootstrap_
            ->GetDataNodeBootstrap()
            ->GetDynamicConfigManager()
            ->GetConfig()
            ->DataNode
            ->TrackSystemJobsMemory;
        auto tracker = Bootstrap_
            ->GetNodeMemoryReferenceTracker()
            ->WithCategory(EMemoryCategory::SystemJobs);

        // TODO(savrus): profile chunk reader statistics.
        IChunkReader::TReadBlocksOptions readBlocksOptions{
            .ClientOptions = TClientChunkReadOptions{
                .WorkloadDescriptor = workloadDescriptor,
                .TrackMemoryAfterSessionCompletion = Bootstrap_
                    ->GetDataNodeBootstrap()
                    ->GetDynamicConfigManager()
                    ->GetConfig()
                    ->DataNode
                    ->TrackMemoryAfterSessionCompletion,
                .MemoryReferenceTracker = trackSystemJobsMemory ? tracker : nullptr
            },
        };

        NErasure::TPartIndexList sourcePartIndexes;
        for (auto replica : SourceReplicas_) {
            sourcePartIndexes.push_back(replica.GetReplicaIndex());
        }
        SortUnique(sourcePartIndexes);

        NErasure::TPartIndexList erasedPartIndexes;
        for (auto replica : TargetReplicas_) {
            erasedPartIndexes.push_back(replica.GetReplicaIndex());
        }
        SortUnique(erasedPartIndexes);

        std::vector<IChunkWriterPtr> writers;
        for (int partIndex : erasedPartIndexes) {
            writers.push_back(CreateWriter(partIndex));
        }

        {
            TFuture<void> future;
            auto chunkType = TypeFromId(ChunkId_);
            switch (chunkType) {
                case EObjectType::ErasureChunk: {
                    future = StartChunkRepairJob(
                        codec,
                        erasedPartIndexes,
                        readBlocksOptions,
                        writers);
                    break;
                }

                case EObjectType::ErasureJournalChunk: {
                    std::vector<IChunkReaderPtr> readers;
                    for (int partIndex : sourcePartIndexes) {
                        readers.push_back(CreateReader(partIndex));
                    }

                    future = NJournalClient::RepairErasedParts(
                        DynamicConfig_->Reader,
                        codec,
                        *rowCount,
                        erasedPartIndexes,
                        readers,
                        writers,
                        readBlocksOptions.ClientOptions,
                        Logger);
                    break;
                }

                default:
                    THROW_ERROR_EXCEPTION("Unsupported chunk type %Qlv",
                        chunkType);
            }

            WaitFor(future)
                .ThrowOnError();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSealChunkJob
    : public TMasterJobBase
{
public:
    TSealChunkJob(
        NChunkServer::TJobId jobId,
        TJobSpec&& jobSpec,
        TString jobTrackerAddress,
        const TJobResources& resourceLimits,
        const TJobResourceAttributes& resourceAttributes,
        IBootstrap* bootstrap)
        : TMasterJobBase(
            jobId,
            std::move(jobSpec),
            std::move(jobTrackerAddress),
            resourceLimits,
            resourceAttributes,
            bootstrap)
        , JobSpecExt_(JobSpec_.GetExtension(TSealChunkJobSpecExt::seal_chunk_job_spec_ext))
        , ChunkId_(FromProto<TChunkId>(JobSpecExt_.chunk_id()))
        , DynamicConfig_(Bootstrap_->GetDynamicConfigManager()->GetConfig()->DataNode->SealChunkJob)
    {
        Logger.AddTag("ChunkId: %v", ChunkId_);
    }

private:
    const TSealChunkJobSpecExt JobSpecExt_;
    const TChunkId ChunkId_;
    const TSealChunkJobDynamicConfigPtr DynamicConfig_;

    // COMPAT(babenko)
    static TChunkReplicaWithMediumList ParseSourceReplicas(const TSealChunkJobSpecExt& jobSpecExt)
    {
        if (jobSpecExt.source_replicas_size() == 0) {
            TChunkReplicaWithMediumList result;
            for (auto replica : FromProto<TChunkReplicaList>(jobSpecExt.legacy_source_replicas())) {
                result.emplace_back(replica);
            }
            return result;
        } else {
            return FromProto<TChunkReplicaWithMediumList>(jobSpecExt.source_replicas());
        }
    }

    void DoRun() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto codecId = CheckedEnumCast<NErasure::ECodec>(JobSpecExt_.codec_id());
        int mediumIndex = JobSpecExt_.medium_index();
        auto sourceReplicas = ParseSourceReplicas(JobSpecExt_);
        i64 sealRowCount = JobSpecExt_.row_count();

        NodeDirectory_->MergeFrom(JobSpecExt_.node_directory());

        YT_LOG_INFO("Chunk seal job started (MediumIndex: %v, Codec: %v, SourceReplicas: %v, RowCount: %v)",
            mediumIndex,
            codecId,
            MakeFormattableView(sourceReplicas, TChunkReplicaAddressFormatter(NodeDirectory_)),
            sealRowCount);

        auto chunk = GetLocalChunkOrThrow(ChunkId_, mediumIndex);
        if (!chunk->IsJournalChunk()) {
            THROW_ERROR_EXCEPTION("Cannot seal a non-journal chunk %v",
                ChunkId_);
        }

        auto journalChunk = chunk->AsJournalChunk();
        if (journalChunk->IsSealed()) {
            YT_LOG_INFO("Chunk is already sealed");
            return;
        }

        TWorkloadDescriptor workloadDescriptor;
        workloadDescriptor.Category = EWorkloadCategory::SystemTabletLogging;
        workloadDescriptor.Annotations.push_back(Format("Seal of chunk %v",
            ChunkId_));
        workloadDescriptor.DiskFairShareBucketTag = ToString(JobId_);

        auto updateGuard = TChunkUpdateGuard::Acquire(chunk);

        const auto& journalDispatcher = Bootstrap_->GetJournalDispatcher();
        const auto& location = journalChunk->GetStoreLocation();
        auto changelog = WaitFor(journalDispatcher->OpenJournal(location, ChunkId_))
            .ValueOrThrow();

        i64 currentRowCount = changelog->GetRecordCount();
        if (currentRowCount < sealRowCount) {
            YT_LOG_DEBUG("Job will read missing journal chunk rows (Rows: %v-%v)",
                currentRowCount,
                sealRowCount - 1);

            auto chunkReaderHost = New<TChunkReaderHost>(
                Bootstrap_->GetClient(),
                Bootstrap_->GetLocalDescriptor(),
                Bootstrap_->GetBlockCache(),
                /*chunkMetaCache*/ nullptr,
                /*nodeStatusDirectory*/ nullptr,
                Bootstrap_->GetThrottler(EDataNodeThrottlerKind::ReplicationIn),
                /*rpsThrottler*/ GetUnlimitedThrottler(),
                /*trafficMeter*/ nullptr);
            auto reader = NJournalClient::CreateChunkReader(
                DynamicConfig_->Reader,
                std::move(chunkReaderHost),
                ChunkId_,
                codecId,
                sourceReplicas);

            auto trackSystemJobsMemory = Bootstrap_
                ->GetDataNodeBootstrap()
                ->GetDynamicConfigManager()
                ->GetConfig()
                ->DataNode
                ->TrackSystemJobsMemory;
            auto tracker = Bootstrap_
                ->GetNodeMemoryReferenceTracker()
                ->WithCategory(EMemoryCategory::SystemJobs);

            // TODO(savrus): profile chunk reader statistics.
            IChunkReader::TReadBlocksOptions readBlocksOptions{
                .ClientOptions = TClientChunkReadOptions{
                    .WorkloadDescriptor = workloadDescriptor,
                    .TrackMemoryAfterSessionCompletion = Bootstrap_
                        ->GetDataNodeBootstrap()
                        ->GetDynamicConfigManager()
                        ->GetConfig()
                        ->DataNode
                        ->TrackMemoryAfterSessionCompletion,
                    .MemoryReferenceTracker = trackSystemJobsMemory ? tracker : nullptr
                },
            };

            while (currentRowCount < sealRowCount) {
                YT_LOG_DEBUG("Reading rows (Rows: %v-%v)",
                    currentRowCount,
                    sealRowCount - 1);

                auto asyncBlocks = reader->ReadBlocks(
                    readBlocksOptions,
                    currentRowCount,
                    sealRowCount - currentRowCount);
                auto blocks = WaitFor(asyncBlocks)
                    .ValueOrThrow();

                int blockCount = blocks.size();
                if (blockCount == 0) {
                    THROW_ERROR_EXCEPTION("Rows %v-%v are missing but needed to seal chunk %v",
                        currentRowCount,
                        sealRowCount - 1,
                        ChunkId_);
                }

                YT_LOG_DEBUG("Rows received (Rows: %v-%v)",
                    currentRowCount,
                    currentRowCount + blockCount - 1);

                std::vector<TSharedRef> records;
                records.reserve(blocks.size());
                for (const auto& block : blocks) {
                    records.push_back(block.Data);
                }
                changelog->Append(records);

                i64 totalRecordsSize = 0;
                for (const auto& block : blocks) {
                    totalRecordsSize += block.Size();
                }
                if (totalRecordsSize > 0 && Bootstrap_->GetIOTracker()->IsEnabled()) {
                    Bootstrap_->GetIOTracker()->Enqueue(
                        TIOCounters{
                            .Bytes = totalRecordsSize,
                            .IORequests = 1
                        },
                        /*tags*/ {
                            {FormatIOTag(ERawIOTag::LocationId), ToString(location->GetId())},
                            {FormatIOTag(EAggregateIOTag::LocationType), FormatEnum(location->GetType())},
                            {FormatIOTag(EAggregateIOTag::Medium), location->GetMediumName()},
                            {FormatIOTag(EAggregateIOTag::DiskFamily), location->GetDiskFamily()},
                            {FormatIOTag(EAggregateIOTag::Direction), "write"},
                            {FormatIOTag(ERawIOTag::ChunkId), ToString(DecodeChunkId(ChunkId_).Id)},
                            {FormatIOTag(EAggregateIOTag::User), "root"},
                        });
                }

                currentRowCount += blockCount;
            }

            WaitFor(changelog->Flush())
                .ThrowOnError();

            YT_LOG_DEBUG("Finished downloading missing journal chunk rows");
        }

        YT_LOG_DEBUG("Started sealing journal chunk (RowCount: %v)",
            sealRowCount);

        WaitFor(journalChunk->Seal())
            .ThrowOnError();

        YT_LOG_DEBUG("Finished sealing journal chunk");

        journalChunk->UpdateFlushedRowCount(changelog->GetRecordCount());
        journalChunk->UpdateDataSize(changelog->GetDataSize());

        const auto& chunkStore = Bootstrap_->GetChunkStore();
        chunkStore->UpdateExistingChunk(chunk);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TJobSystemMemoryUsageTracker
    : public ITypedNodeMemoryTracker
{
public:
    TJobSystemMemoryUsageTracker(
        TWeakPtr<TResourceHolder> resourceHolder)
        : ResourceHolder_(std::move(resourceHolder))
    { }

    bool Acquire(i64 size) override
    {
        TJobResources resources;
        resources.SystemMemory = size;
        return ResourceHolder_.Lock()->UpdateAdditionalResourceUsage(resources);
    }

    TError TryAcquire(i64 /*size*/) override
    {
        YT_UNIMPLEMENTED();
    }

    TError TryChange(i64 /*size*/) override
    {
        // Job cannot set self memory.
        YT_UNIMPLEMENTED();
    }

    void Release(i64 size) override
    {
        TJobResources resources;
        resources.SystemMemory = -size;
        ResourceHolder_.Lock()->UpdateAdditionalResourceUsage(resources);
    }

    i64 GetFree() const override
    {
        return GetLimit() - GetUsed();
    }

    void SetLimit(i64 /*size*/) override
    {
        // Job cannot set self limits.
        YT_UNIMPLEMENTED();
    }

    i64 GetLimit() const override
    {
        auto resource = ResourceHolder_.Lock()->GetResourceLimits();
        return resource.SystemMemory ? resource.SystemMemory : std::numeric_limits<i64>::max();
    }

    i64 GetUsed() const override
    {
        auto holder = ResourceHolder_.Lock();
        auto resource = holder->GetResourceUsage();
        return resource.SystemMemory;
    }

    bool IsExceeded() const override
    {
        return GetFree() <= 0;
    }

private:
    const TWeakPtr<TResourceHolder> ResourceHolder_;
};

DEFINE_REFCOUNTED_TYPE(TJobSystemMemoryUsageTracker);

////////////////////////////////////////////////////////////////////////////////

class TChunkMergeJob
    : public TMasterJobBase
{
public:
    TChunkMergeJob(
        NChunkServer::TJobId jobId,
        const TJobSpec& jobSpec,
        TString jobTrackerAddress,
        const TJobResources& resourceLimits,
        const TJobResourceAttributes& resourceAttributes,
        IBootstrap* bootstrap,
        i64 readMemoryLimit)
        : TMasterJobBase(
            jobId,
            std::move(jobSpec),
            std::move(jobTrackerAddress),
            resourceLimits,
            resourceAttributes,
            bootstrap)
        , JobSpecExt_(JobSpec_.GetExtension(TMergeChunksJobSpecExt::merge_chunks_job_spec_ext))
        , CellTag_(FromProto<TCellTag>(JobSpecExt_.cell_tag()))
        , MemoryUsageTracker_(New<TJobSystemMemoryUsageTracker>(
            MakeWeak(this)))
        , ReadMemoryLimit_(readMemoryLimit)
        , DynamicConfig_(Bootstrap_->GetDynamicConfigManager()->GetConfig()->DataNode->MergeChunksJob)
    {
        YT_VERIFY(readMemoryLimit > 0);

        // TODO(don-dron): Return MemoryUsageTracker usage for reader.
        TParallelReaderMemoryManagerOptions parallelReaderMemoryManagerOptions{
            .TotalReservedMemorySize = ReadMemoryLimit_,
            .MaxInitialReaderReservedMemory = ReadMemoryLimit_
        };

        MultiReaderMemoryManager_ = CreateParallelReaderMemoryManager(
            parallelReaderMemoryManagerOptions,
            NChunkClient::TDispatcher::Get()->GetReaderMemoryManagerInvoker());
    }

private:
    const TMergeChunksJobSpecExt JobSpecExt_;
    const TCellTag CellTag_;
    const ITypedNodeMemoryTrackerPtr MemoryUsageTracker_;
    const i64 ReadMemoryLimit_;
    const TMergeChunksJobDynamicConfigPtr DynamicConfig_;

    bool DeepMergeFallbackOccurred_ = false;
    TError ShallowMergeValidationError_;
    EChunkMergerMode MergeMode_;
    IMultiReaderMemoryManagerPtr MultiReaderMemoryManager_;

    TTableSchemaPtr Schema_;
    TMasterTableSchemaId SchemaId_;
    NCompression::ECodec CompressionCodec_;
    NErasure::ECodec ErasureCodec_;
    std::optional<EOptimizeFor> OptimizeFor_;
    std::optional<EChunkFormat> ChunkFormat_;
    std::optional<bool> EnableSkynetSharing_;
    int MaxHeavyColumns_;
    std::optional<i64> MaxBlockCount_;

    struct TChunkReadContext
    {
        IChunkReaderPtr Reader;
        TDeferredChunkMetaPtr Meta;
        TChunkId ChunkId;
        std::vector<i64> BlockSizes;
        IChunkReader::TReadBlocksOptions Options;
        TMergeChunkInfo MergeChunkInfo;
    };
    std::vector<TChunkReadContext> InputChunkReadContexts_;

    class TMergeChunkReader
    {
    public:
        TMergeChunkReader(
            NTableClient::TChunkReaderConfigPtr readerConfig,
            std::vector<TChunkReadContext> contexts,
            TTableSchemaPtr schema,
            TNameTablePtr nameTable,
            bool permuteRows,
            IBlockCachePtr blockCache,
            IMultiReaderMemoryManagerPtr memoryManager)
            : ReaderConfig_(std::move(readerConfig))
            , Contexts_(std::move(contexts))
            , Schema_(std::move(schema))
            , NameTable_(std::move(nameTable))
            , PermuteRows_(permuteRows)
            , BlockCache_(std::move(blockCache))
            , MemoryManager_(std::move(memoryManager))
        {
            OpenReader();
        }

        std::optional<std::vector<TUnversionedRow>> ReadRows(const TRowBufferPtr& rowBuffer)
        {
            IUnversionedRowBatchPtr batch;

            while (!batch) {
                batch = ReadRowBatch(ChunkReader_);
                if (!batch) {
                    ++CurrentChunkIndex_;
                    if (CurrentChunkIndex_ == std::ssize(Contexts_)) {
                        break;
                    }
                    OpenReader();
                }
            }

            if (!batch) {
                YT_VERIFY(CurrentChunkIndex_ = std::ssize(Contexts_));
                return std::nullopt;
            }

            auto rows = batch->MaterializeRows();
            if (!PermuteRows_) {
                // Fast path.
                return std::vector<TUnversionedRow>(rows.begin(), rows.end());
            }

            const auto& readerNameTable = ChunkReader_->GetNameTable();
            auto readerTableSize = readerNameTable->GetSize();
            TNameTableToSchemaIdMapping idMapping(readerTableSize);
            const auto& names = readerNameTable->GetNames();
            for (auto index = 0; index < readerTableSize; ++index) {
                idMapping[index] = NameTable_->GetIdOrRegisterName(names[index]);
            }

            std::vector<TUnversionedRow> permutedRows;
            permutedRows.reserve(rows.size());
            for (auto row : rows) {
                auto permutedRow = rowBuffer->CaptureAndPermuteRow(
                    row,
                    *Schema_,
                    Schema_->GetColumnCount(),
                    idMapping,
                    nullptr);
                permutedRows.push_back(permutedRow);
            }

            return permutedRows;
        }

    private:
        const NTableClient::TChunkReaderConfigPtr ReaderConfig_;
        const std::vector<TChunkReadContext> Contexts_;
        const TTableSchemaPtr Schema_;
        const TNameTablePtr NameTable_;
        const bool PermuteRows_;
        const IBlockCachePtr BlockCache_;
        const IMultiReaderMemoryManagerPtr MemoryManager_;

        int CurrentChunkIndex_ = 0;
        ISchemalessChunkReaderPtr ChunkReader_;

        void OpenReader()
        {
            const auto& context = Contexts_[CurrentChunkIndex_];
            auto chunkState = New<TChunkState>(TChunkState{
                .BlockCache = BlockCache_,
                .ChunkSpec = GetChunkSpec(context.MergeChunkInfo),
                .TableSchema = Schema_,
            });

            ChunkReader_ = CreateSchemalessRangeChunkReader(
                std::move(chunkState),
                New<TColumnarChunkMeta>(*context.Meta),
                ReaderConfig_,
                TChunkReaderOptions::GetDefault(),
                context.Reader,
                New<TNameTable>(),
                context.Options.ClientOptions,
                /*keyColumns*/ {},
                /*omittedInaccessibleColumns*/ {},
                TColumnFilter(),
                NChunkClient::TReadRange(),
                /*partitionTag*/ std::nullopt,
                MemoryManager_->CreateChunkReaderMemoryManager());
        }
    };

    void SetMergeJobResult()
    {
        auto* jobResultExt = Result_.MutableExtension(TMergeChunksJobResultExt::merge_chunks_job_result_ext);
        if (MergeMode_ == EChunkMergerMode::Auto) {
            jobResultExt->set_deep_merge_fallback_occurred(DeepMergeFallbackOccurred_);
        }
        ToProto(jobResultExt->mutable_shallow_merge_validation_error(), ShallowMergeValidationError_);

        if (!ShallowMergeValidationError_.IsOK()) {
            YT_LOG_ALERT(ShallowMergeValidationError_, "Shallow merge validation failed");
            THROW_ERROR ShallowMergeValidationError_;
        }
    }

    TFuture<void> ReleaseCumulativeResources() override
    {
        return MultiReaderMemoryManager_->Finalize();
    }

    void DoRun() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        NodeDirectory_->MergeFrom(JobSpecExt_.node_directory());

        const auto& chunkMergerWriterOptions = JobSpecExt_.chunk_merger_writer_options();
        Schema_ = New<TTableSchema>(FromProto<TTableSchema>(chunkMergerWriterOptions.schema()));
        SchemaId_ = FromProto<TMasterTableSchemaId>(chunkMergerWriterOptions.schema_id());
        CompressionCodec_ = CheckedEnumCast<NCompression::ECodec>(chunkMergerWriterOptions.compression_codec());
        ErasureCodec_ = CheckedEnumCast<NErasure::ECodec>(chunkMergerWriterOptions.erasure_codec());
        if (chunkMergerWriterOptions.has_optimize_for()) {
            OptimizeFor_ = CheckedEnumCast<EOptimizeFor>(chunkMergerWriterOptions.optimize_for());
        }
        if (chunkMergerWriterOptions.has_chunk_format()) {
            ChunkFormat_ = CheckedEnumCast<EChunkFormat>(chunkMergerWriterOptions.chunk_format());
        }
        if (chunkMergerWriterOptions.has_enable_skynet_sharing()) {
            EnableSkynetSharing_ = chunkMergerWriterOptions.enable_skynet_sharing();
        }
        MaxHeavyColumns_ = chunkMergerWriterOptions.max_heavy_columns();
        if (chunkMergerWriterOptions.has_max_block_count()) {
            MaxBlockCount_ = chunkMergerWriterOptions.max_block_count();
        }

        MergeMode_ = CheckedEnumCast<EChunkMergerMode>(chunkMergerWriterOptions.merge_mode());
        YT_LOG_INFO("Chunk merge job started (Mode: %v)", MergeMode_);

        PrepareInputChunkReadContexts();
        switch (MergeMode_) {
            case EChunkMergerMode::Shallow:
                MergeShallow();
                break;
            case EChunkMergerMode::Deep:
                MergeDeep();
                break;
            case EChunkMergerMode::Auto:
                try {
                    MergeShallow();
                } catch (const TErrorException& ex) {
                    if (ex.Error().GetCode() != NChunkClient::EErrorCode::IncompatibleChunkMetas) {
                        throw;
                    }
                    YT_LOG_DEBUG(ex, "Unable to merge chunks using shallow mode, falling back to deep merge");
                    DeepMergeFallbackOccurred_ = true;
                    MergeDeep();
                }
                break;
            default:
                THROW_ERROR_EXCEPTION("Cannot merge chunks in %Qlv mode", MergeMode_);
        }
        SetMergeJobResult();
    }

    TChunkReadContext GetChunkReadContext(const TMergeChunkInfo& chunk)
    {
        auto reader = CreateReader(chunk);
        auto chunkId = FromProto<TChunkId>(chunk.id());

        TWorkloadDescriptor workloadDescriptor;
        workloadDescriptor.Category = EWorkloadCategory::SystemMerge;
        workloadDescriptor.Annotations.push_back(Format("Merge chunk %v", chunkId));
        workloadDescriptor.DiskFairShareBucketTag = ToString(JobId_);

        IChunkReader::TReadBlocksOptions options;
        options.ClientOptions.WorkloadDescriptor = workloadDescriptor;

        auto chunkMeta = GetChunkMeta(reader, options.ClientOptions);
        auto blockMetaExt = GetProtoExtension<NTableClient::NProto::TDataBlockMetaExt>(chunkMeta->extensions());

        std::vector<i64> blockSizes;
        blockSizes.reserve(blockMetaExt.data_blocks_size());

        for (const auto& block : blockMetaExt.data_blocks()) {
            blockSizes.push_back(block.uncompressed_size());
        }

        return TChunkReadContext{
            .Reader = std::move(reader),
            .Meta = std::move(chunkMeta),
            .ChunkId = chunkId,
            .BlockSizes = std::move(blockSizes),
            .Options = options,
            .MergeChunkInfo = chunk
        };
    }

    TChunkReadContext GetChunkReadContext(const IMetaAggregatingWriterPtr& writer)
    {
        auto chunkMeta = writer->GetChunkMeta();
        auto miscExt = GetProtoExtension<TMiscExt>(chunkMeta->extensions());

        TMergeChunkInfo chunkInfo;
        ToProto(chunkInfo.mutable_id(), writer->GetChunkId());

        for (auto replica : writer->GetWrittenChunkReplicas()) {
            chunkInfo.add_legacy_source_replicas(ToProto<ui32>(replica.ToChunkReplica()));
            chunkInfo.add_source_replicas(ToProto<ui64>(replica));
        }

        chunkInfo.set_row_count(miscExt.row_count());
        chunkInfo.set_erasure_codec(miscExt.erasure_codec());

        return GetChunkReadContext(chunkInfo);
    }

    void PrepareInputChunkReadContexts()
    {
        InputChunkReadContexts_.clear();
        InputChunkReadContexts_.reserve(JobSpecExt_.input_chunks_size());
        for (const auto& chunk : JobSpecExt_.input_chunks()) {
            InputChunkReadContexts_.push_back(GetChunkReadContext(chunk));
        }
    }

    void MergeShallow()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto confirmingWriter = CreateWriter();

        auto options = New<TMetaAggregatingWriterOptions>();
        options->TableSchema = Schema_;
        options->CompressionCodec = CompressionCodec_;
        options->ErasureCodec = ErasureCodec_;
        if (EnableSkynetSharing_) {
            options->EnableSkynetSharing = *EnableSkynetSharing_;
        }
        options->MaxHeavyColumns = MaxHeavyColumns_;
        options->MaxBlockCount = MaxBlockCount_;

        auto writer = CreateMetaAggregatingWriter(
            confirmingWriter,
            options);
        WaitFor(writer->Open())
            .ThrowOnError();

        for (const auto& chunkReadContext : InputChunkReadContexts_) {
            writer->AbsorbMeta(chunkReadContext.Meta, chunkReadContext.ChunkId);
        }

        for (const auto& chunkReadContext : InputChunkReadContexts_) {
            int currentBlockCount = 0;
            auto blockSizes = chunkReadContext.BlockSizes;
            auto inputChunkBlockCount = std::ssize(blockSizes);

            while (currentBlockCount < inputChunkBlockCount) {
                i64 currentSummarySize = 0;
                int start = currentBlockCount;
                int end = currentBlockCount;

                while (end < inputChunkBlockCount) {
                    currentSummarySize += blockSizes[end++];

                    if (currentSummarySize >= ReadMemoryLimit_) {
                        break;
                    }
                }

                std::vector<int> blockIndices(end - start);
                std::iota(blockIndices.begin(), blockIndices.end(), start);

                auto memoryGuard = TMemoryUsageTrackerGuard::Acquire(MemoryUsageTracker_, currentSummarySize);

                auto asyncResult = chunkReadContext.Reader->ReadBlocks(
                    chunkReadContext.Options,
                    blockIndices);

                auto readResult = WaitFor(asyncResult);
                THROW_ERROR_EXCEPTION_IF_FAILED(readResult, "Error reading blocks");
                auto blocks = readResult.Value();
                if (!writer->WriteBlocks(chunkReadContext.Options.ClientOptions.WorkloadDescriptor, blocks)) {
                    auto writeResult = WaitFor(writer->GetReadyEvent());
                    THROW_ERROR_EXCEPTION_IF_FAILED(writeResult, "Error writing block");
                }
                currentBlockCount += ssize(blocks);
            }
        }

        WaitFor(writer->Close())
            .ThrowOnError();

        if (JobSpecExt_.validate_shallow_merge()) {
            YT_VERIFY(MultiReaderMemoryManager_);

            ShallowMergeValidationError_ = ValidateShallowMerge(writer, MultiReaderMemoryManager_);
        }
    }

    void MergeDeep()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto confirmingWriter = CreateWriter();

        auto chunkWriterOptions = New<TChunkWriterOptions>();
        chunkWriterOptions->CompressionCodec = CompressionCodec_;
        if (OptimizeFor_) {
            chunkWriterOptions->OptimizeFor = *OptimizeFor_;
        }
        if (ChunkFormat_) {
            chunkWriterOptions->ChunkFormat = *ChunkFormat_;
        }
        if (EnableSkynetSharing_) {
            chunkWriterOptions->EnableSkynetSharing = *EnableSkynetSharing_;
        }
        chunkWriterOptions->Postprocess();

        auto minTs = NullTimestamp;
        auto maxTs = NullTimestamp;
        for (const auto& context : InputChunkReadContexts_) {
            auto miscExt = GetProtoExtension<TMiscExt>(context.Meta->extensions());
            if (miscExt.has_min_timestamp()) {
                auto currentMinTs = miscExt.min_timestamp();
                minTs = minTs == NullTimestamp ? currentMinTs : std::min(minTs, currentMinTs);
            }
            if (miscExt.has_max_timestamp()) {
                auto currentMaxTs = miscExt.max_timestamp();
                maxTs = maxTs == NullTimestamp ? currentMaxTs : std::max(maxTs, currentMaxTs);
            }
        }

        auto writer = CreateSchemalessChunkWriter(
            DynamicConfig_->Writer,
            chunkWriterOptions,
            Schema_,
            /*nameTable*/ nullptr,
            confirmingWriter,
            /*dataSink*/ std::nullopt,
            {minTs, maxTs});

        auto rowBuffer = New<TRowBuffer>();
        auto writerNameTable = writer->GetNameTable();

        {
            YT_VERIFY(MultiReaderMemoryManager_);

            TMergeChunkReader chunkReader(
                DynamicConfig_->Reader,
                InputChunkReadContexts_,
                Schema_,
                writerNameTable,
                /*permuteRows*/ true,
                Bootstrap_->GetBlockCache(),
                MultiReaderMemoryManager_);

            while (auto rows = chunkReader.ReadRows(rowBuffer)) {
                if (!writer->Write(MakeRange(*rows))) {
                    WaitFor(writer->GetReadyEvent())
                        .ThrowOnError();
                }
                rowBuffer->Clear();
            }

            WaitFor(writer->Close())
                .ThrowOnError();
        }
    }

    IChunkWriterPtr CreateWriter()
    {
        auto outputChunkId = FromProto<TChunkId>(JobSpecExt_.output_chunk_id());
        int mediumIndex = JobSpecExt_.medium_index();
        auto sessionId = TSessionId(outputChunkId, mediumIndex);
        auto targetReplicas = FromProto<TChunkReplicaWithMediumList>(JobSpecExt_.target_replicas());

        auto options = New<TMultiChunkWriterOptions>();
        options->TableSchema = Schema_;
        options->CompressionCodec = CompressionCodec_;
        options->ErasureCodec = ErasureCodec_;

        return CreateConfirmingWriter(
            DynamicConfig_->Writer,
            options,
            CellTag_,
            NullTransactionId,
            SchemaId_,
            NullChunkListId,
            Bootstrap_->GetClient(),
            Bootstrap_->GetLocalHostName(),
            Bootstrap_->GetBlockCache(),
            /*trafficMeter*/ nullptr,
            Bootstrap_->GetThrottler(EDataNodeThrottlerKind::MergeOut),
            sessionId,
            std::move(targetReplicas));
    }

    static TChunkSpec GetChunkSpec(const NChunkClient::NProto::TMergeChunkInfo& chunkInfo)
    {
        TChunkSpec chunkSpec;
        chunkSpec.set_row_count_override(chunkInfo.row_count());
        chunkSpec.set_erasure_codec(chunkInfo.erasure_codec()),
        *chunkSpec.mutable_chunk_id() = chunkInfo.id();
        *chunkSpec.mutable_legacy_replicas() = chunkInfo.legacy_source_replicas();
        *chunkSpec.mutable_replicas() = chunkInfo.source_replicas();
        return chunkSpec;
    }

    IChunkReaderPtr CreateReader(const NChunkClient::NProto::TMergeChunkInfo& chunk)
    {
        auto inputChunkId = FromProto<TChunkId>(chunk.id());
        YT_LOG_INFO("Reading input chunk (ChunkId: %v)",
            inputChunkId);

        auto erasureReaderConfig = New<TErasureReaderConfig>();
        erasureReaderConfig->EnableAutoRepair = false;

        auto chunkReaderHost = New<TChunkReaderHost>(
            Bootstrap_->GetClient(),
            Bootstrap_->GetLocalDescriptor(),
            Bootstrap_->GetBlockCache(),
            /*chunkMetaCache*/ nullptr,
            /*nodeStatusDirectory*/ nullptr,
            Bootstrap_->GetThrottler(EDataNodeThrottlerKind::MergeIn),
            /*rpsThrottler*/ GetUnlimitedThrottler(),
            /*trafficMeter*/ nullptr);
        return CreateRemoteReader(
            GetChunkSpec(chunk),
            erasureReaderConfig,
            New<TRemoteReaderOptions>(),
            std::move(chunkReaderHost));
    }

    TDeferredChunkMetaPtr GetChunkMeta(IChunkReaderPtr reader, const TClientChunkReadOptions& options)
    {
        auto result = WaitFor(reader->GetMeta(options));
        THROW_ERROR_EXCEPTION_IF_FAILED(result, "Merge job failed");

        auto deferredChunkMeta = New<TDeferredChunkMeta>();
        deferredChunkMeta->CopyFrom(*result.Value());
        return deferredChunkMeta;
    }

    TError ValidateShallowMerge(
        const IMetaAggregatingWriterPtr& writer,
        const IMultiReaderMemoryManagerPtr& multiReaderMemoryManager)
    {
        YT_LOG_INFO("Validating shallow merge result");

        if (DynamicConfig_->FailShallowMergeValidation) {
            return TError("Testing error");
        }

        auto nameTable = TNameTable::FromSchema(*Schema_);

        auto inputChunksRowBuffer = New<TRowBuffer>();
        auto outputChunkRowBuffer = New<TRowBuffer>();

        TRingQueue<TUnversionedRow> inputChunksRows;
        TRingQueue<TUnversionedRow> outputChunkRows;

        i64 inputChunksRowsRead = 0;
        i64 outputChunkRowsRead = 0;

        auto outputChunkReadContext = GetChunkReadContext(writer);

        auto readerConfig = DynamicConfig_->Reader;

        TMergeChunkReader outputChunkReader(
            readerConfig,
            {outputChunkReadContext},
            Schema_,
            nameTable,
            /*permuteRows*/ false,
            Bootstrap_->GetBlockCache(),
            multiReaderMemoryManager);

        // NB: #InputChunkReadContexts_ could be already used during merge.
        PrepareInputChunkReadContexts();
        TMergeChunkReader inputChunksReader(
            readerConfig,
            InputChunkReadContexts_,
            Schema_,
            nameTable,
            /*permuteRows*/ true,
            Bootstrap_->GetBlockCache(),
            multiReaderMemoryManager);

        i64 outputChunkRowCount = outputChunkReadContext.MergeChunkInfo.row_count();
        i64 inputChunksRowCount = 0;
        for (const auto& inputChunk : InputChunkReadContexts_) {
            inputChunksRowCount += inputChunk.MergeChunkInfo.row_count();
        }

        if (inputChunksRowCount != outputChunkRowCount) {
            return TError("Total number of rows in input chunks differs from number of rows in output chunk")
                << TErrorAttribute("input_chunks_row_count", inputChunksRowCount)
                << TErrorAttribute("output_chunk_row_count", outputChunkRowCount);
        }

        i64 rowIndex = 0;
        while (true) {
            auto fillBuffer = [&] (
                TMergeChunkReader* reader,
                TRingQueue<TUnversionedRow>* buffer,
                const TRowBufferPtr& rowBuffer,
                i64* rowCounter)
            {
                YT_VERIFY(buffer->empty());
                rowBuffer->Clear();
                auto rows = reader->ReadRows(rowBuffer);
                if (!rows) {
                    return true;
                }

                for (auto row : *rows) {
                    buffer->push(row);
                }
                *rowCounter += rows->size();

                return false;
            };

            auto inputExhausted = false;
            if (inputChunksRows.empty()) {
                inputExhausted = fillBuffer(
                    &inputChunksReader,
                    &inputChunksRows,
                    inputChunksRowBuffer,
                    &inputChunksRowsRead);
            }

            auto outputExhausted = false;
            if (outputChunkRows.empty()) {
                outputExhausted = fillBuffer(
                    &outputChunkReader,
                    &outputChunkRows,
                    outputChunkRowBuffer,
                    &outputChunkRowsRead);
            }

            if (inputChunksRowsRead > inputChunksRowCount) {
                return TError("Actual number of rows in input chunks is greater than expected")
                    << TErrorAttribute("rows_read", inputChunksRowsRead)
                    << TErrorAttribute("expected_rows", inputChunksRowCount);
            } else if (inputExhausted && inputChunksRowsRead < inputChunksRowCount) {
                return TError("Actual number of rows in input chunks is less than expected")
                    << TErrorAttribute("rows_read", inputChunksRowsRead)
                    << TErrorAttribute("expected_rows", inputChunksRowCount);
            }

            if (outputChunkRowsRead > outputChunkRowCount) {
                return TError("Actual number of rows in output chunk is greater than expected")
                    << TErrorAttribute("rows_read", outputChunkRowsRead)
                    << TErrorAttribute("expected_rows", outputChunkRowCount);
            } else if (outputExhausted && outputChunkRowsRead < outputChunkRowCount) {
                return TError("Actual number of rows in output chunk is less than expected")
                    << TErrorAttribute("rows_read", outputChunkRowsRead)
                    << TErrorAttribute("expected_rows", outputChunkRowCount);
            }

            if (inputExhausted && outputExhausted) {
                YT_VERIFY(inputChunksRowsRead == inputChunksRowCount);
                YT_VERIFY(outputChunkRowsRead == outputChunkRowCount);
                YT_VERIFY(inputChunksRowCount == outputChunkRowCount);

                YT_LOG_DEBUG("Shallow merge result validated (RowCount: %v)",
                    inputChunksRowsRead);

                return TError();
            }

            while (!inputChunksRows.empty() && !outputChunkRows.empty()) {
                auto inputRow = inputChunksRows.front();
                inputChunksRows.pop();

                auto outputRow = outputChunkRows.front();
                outputChunkRows.pop();

                if (inputRow != outputRow) {
                    return TError("Row differs in input and output chunks")
                        << TErrorAttribute("row_index", rowIndex)
                        << TErrorAttribute("input_row", inputRow)
                        << TErrorAttribute("output_row", outputRow);
                }

                ++rowIndex;
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TChunkReincarnationJob
    : public TMasterJobBase
{
public:
    TChunkReincarnationJob(
        NChunkServer::TJobId jobId,
        const TJobSpec& jobSpec,
        TString jobTrackerAddress,
        const TJobResources& resourceLimits,
        const TJobResourceAttributes& resourceAttributes,
        IBootstrap* bootstrap)
        : TMasterJobBase(
            jobId,
            std::move(jobSpec),
            std::move(jobTrackerAddress),
            resourceLimits,
            resourceAttributes,
            bootstrap)
        , JobSpecExt_(
            JobSpec_.GetExtension(TReincarnateChunkJobSpecExt::reincarnate_chunk_job_spec_ext))
        , OldChunkId_(FromProto<TChunkId>(JobSpecExt_.old_chunk_id()))
        , NewChunkId_(FromProto<TChunkId>(JobSpecExt_.new_chunk_id()))
        , CellTag_(CellTagFromId(OldChunkId_))
        , TargetReplicas_(FromProto<TChunkReplicaWithMediumList>(JobSpecExt_.target_replicas()))
        , ErasureCodec_(CheckedEnumCast<NErasure::ECodec>(JobSpecExt_.erasure_codec()))
        , CompressionCodec_(CheckedEnumCast<NCompression::ECodec>(JobSpecExt_.compression_codec()))
        , MediumIndex_(JobSpecExt_.medium_index())
        , EnableSkynetSharing_(JobSpecExt_.has_enable_skynet_sharing()
            ? std::optional(JobSpecExt_.enable_skynet_sharing())
            : std::nullopt)
        , NodeDirectory_(New<NNodeTrackerClient::TNodeDirectory>())
        , DynamicConfig_(Bootstrap_->GetDynamicConfigManager()->GetConfig()->DataNode->ReincarnateChunkJob)
    {
        NodeDirectory_->MergeFrom(JobSpecExt_.node_directory());
    }

private:
    const TReincarnateChunkJobSpecExt JobSpecExt_;
    const TChunkId OldChunkId_;
    const TChunkId NewChunkId_;
    const TCellTag CellTag_;
    const TChunkReplicaWithMediumList TargetReplicas_;
    const NErasure::ECodec ErasureCodec_;
    const NCompression::ECodec CompressionCodec_;
    const int MediumIndex_;
    const std::optional<bool> EnableSkynetSharing_;
    const TNodeDirectoryPtr NodeDirectory_;
    const TReincarnateChunkJobDynamicConfigPtr DynamicConfig_;

    static void CopyMeta(const TDeferredChunkMetaPtr& src, const TDeferredChunkMetaPtr& dst)
    {
        #define COPY_EXTENSION(NClient, TExt) \
            do { \
                if (HasProtoExtension<NClient::NProto::TExt>(src->extensions())) { \
                    SetProtoExtension( \
                        dst->mutable_extensions(), \
                        GetProtoExtension<NClient::NProto::TExt>(src->extensions())); \
                }; \
            } while (false)

        COPY_EXTENSION(NChunkClient, TBlocksExt);
        COPY_EXTENSION(NChunkClient, TMiscExt);
        COPY_EXTENSION(NChunkClient, TErasurePlacementExt);
        COPY_EXTENSION(NChunkClient, TStripedErasurePlacementExt);

        COPY_EXTENSION(NTableClient, TTableSchemaExt);
        COPY_EXTENSION(NTableClient, TNameTableExt);
        COPY_EXTENSION(NTableClient, TDataBlockMetaExt);
        COPY_EXTENSION(NTableClient, TSystemBlockMetaExt);
        COPY_EXTENSION(NTableClient, TBoundaryKeysExt);
        COPY_EXTENSION(NTableClient, TSamplesExt);
        COPY_EXTENSION(NTableClient, TPartitionsExt);
        COPY_EXTENSION(NTableClient, TKeyColumnsExt);

        // NB: TProtoExtensionTag is not specialized for TSortColumnsExt.
        // Probably, this extension isn't used at all.

        COPY_EXTENSION(NTableClient, TColumnMetaExt);
        COPY_EXTENSION(NTableClient, TColumnarStatisticsExt);
        COPY_EXTENSION(NTableClient, THeavyColumnStatisticsExt);
        COPY_EXTENSION(NTableClient, THunkChunkRefsExt);
        COPY_EXTENSION(NTableClient, THunkChunkMetasExt);
        COPY_EXTENSION(NTableClient, THunkChunkMiscExt);
        COPY_EXTENSION(NTableClient, TVersionedRowDigestExt);

        #undef COPY_EXTENSION
    }

    void DoRun() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (IsTestingFailureNeeded()) {
            THROW_ERROR_EXCEPTION("Testing failure");
        }

        TChunkSpec oldChunkSpec;
        ToProto(oldChunkSpec.mutable_chunk_id(), OldChunkId_);
        oldChunkSpec.set_erasure_codec(ToProto<int>(ErasureCodec_));
        *oldChunkSpec.mutable_legacy_replicas() = JobSpecExt_.legacy_source_replicas();
        *oldChunkSpec.mutable_replicas() = JobSpecExt_.source_replicas();

        auto readerConfig = DynamicConfig_->Reader;
        auto writerConfig = DynamicConfig_->Writer;

        auto chunkReaderHost = New<TChunkReaderHost>(
            Bootstrap_->GetClient(),
            Bootstrap_->GetLocalDescriptor(),
            Bootstrap_->GetBlockCache(),
            /*chunkMetaCache*/ nullptr,
            /*nodeStatusDirectory*/ nullptr,
            Bootstrap_->GetThrottler(EDataNodeThrottlerKind::ReincarnationIn),
            /*rpsThrottler*/ GetUnlimitedThrottler(),
            /*trafficMeter*/ nullptr);
        auto remoteReader = CreateRemoteReader(
            oldChunkSpec,
            readerConfig,
            New<TRemoteReaderOptions>(),
            std::move(chunkReaderHost));

        auto trackSystemJobsMemory = Bootstrap_
            ->GetDataNodeBootstrap()
            ->GetDynamicConfigManager()
            ->GetConfig()
            ->DataNode
            ->TrackSystemJobsMemory;
        auto tracker = Bootstrap_
            ->GetNodeMemoryReferenceTracker()
            ->WithCategory(EMemoryCategory::SystemJobs);

        TClientChunkReadOptions readerOptions{
            .WorkloadDescriptor = TWorkloadDescriptor(
                EWorkloadCategory::SystemReincarnation,
                /*band*/ 0,
                /*instant*/ {},
                {Format("Reincarnate chunk %v", OldChunkId_)},
                /*compressionFairShareTag*/ {},
                ToString(JobId_),
                /*diskFairShareBucketWeight=*/ {}),
            .TrackMemoryAfterSessionCompletion = Bootstrap_
                ->GetDataNodeBootstrap()
                ->GetDynamicConfigManager()
                ->GetConfig()
                ->DataNode
                ->TrackMemoryAfterSessionCompletion,
            .MemoryReferenceTracker = trackSystemJobsMemory ? tracker : nullptr
        };

        auto oldChunkMeta = New<TDeferredChunkMeta>();
        {
            auto result = WaitFor(remoteReader->GetMeta(readerOptions));
            THROW_ERROR_EXCEPTION_IF_FAILED(result, "Reincarnation job failed");

            oldChunkMeta->CopyFrom(*result.Value());
        }

        auto oldChunkFormat = CheckedEnumCast<EChunkFormat>(oldChunkMeta->format());
        YT_VERIFY(IsValidTableChunkFormat(oldChunkFormat));

        auto columnarMeta = New<TColumnarChunkMeta>(*oldChunkMeta);

        auto oldChunkState = New<TChunkState>(TChunkState{
            .BlockCache = Bootstrap_->GetBlockCache(),
            .ChunkSpec = oldChunkSpec,
            .ChunkMeta = TCachedVersionedChunkMeta::Create(
                /*preparedColumnarMeta*/ false,
                /*memoryTracker*/ nullptr,
                oldChunkMeta),
            .OverrideTimestamp = NullTimestamp,
            .LookupHashTable = nullptr,
            .KeyComparer = {},
            .VirtualValueDirectory = nullptr,
            .TableSchema = columnarMeta->ChunkSchema(),
            .DataSource = std::nullopt,
            .ChunkColumnMapping = nullptr,
        });

        auto confirmingWriterOptions = New<TMultiChunkWriterOptions>();
        confirmingWriterOptions->TableSchema = oldChunkState->TableSchema;
        confirmingWriterOptions->CompressionCodec = CompressionCodec_;
        confirmingWriterOptions->ErasureCodec = ErasureCodec_;
        if (auto miscExt = GetChunkMiscExt(oldChunkMeta); miscExt && miscExt->has_eden()) {
            confirmingWriterOptions->ChunksEden = miscExt->eden();
        }
        confirmingWriterOptions->Postprocess();

        auto confirmingWriter = CreateConfirmingWriter(
            writerConfig,
            confirmingWriterOptions,
            CellTag_,
            NullTransactionId,
            TMasterTableSchemaId(), // TODO(h0pless): Deduce chunk schemaId on master and send it here.
            NullChunkListId,
            Bootstrap_->GetClient(),
            Bootstrap_->GetLocalHostName(),
            Bootstrap_->GetBlockCache(),
            /*trafficMeter*/ nullptr,
            Bootstrap_->GetThrottler(EDataNodeThrottlerKind::ReincarnationOut),
            TSessionId(NewChunkId_, MediumIndex_),
            TargetReplicas_);

        if (IsTableChunkFormatVersioned(oldChunkFormat)) {
            ReincarnateVersionedChunk(
                std::move(oldChunkMeta),
                std::move(readerOptions),
                std::move(remoteReader),
                std::move(confirmingWriter),
                std::move(oldChunkState));
        } else {
            ReincarnateUnversionedChunk(
                std::move(oldChunkMeta),
                std::move(remoteReader),
                std::move(readerOptions),
                std::move(confirmingWriter),
                std::move(columnarMeta),
                std::move(oldChunkState));
        }
    }

    std::optional<NChunkClient::NProto::TMiscExt> GetChunkMiscExt(
        const TDeferredChunkMetaPtr& meta)
    {
        return FindProtoExtension<NChunkClient::NProto::TMiscExt>(meta->extensions());
    }

    void ReincarnateVersionedChunk(
        TDeferredChunkMetaPtr oldChunkMeta,
        TClientChunkReadOptions readerOptions,
        IChunkReaderPtr remoteReader,
        IChunkWriterPtr confirmingWriter,
        TChunkStatePtr oldChunkState)
    {
        auto reader = CreateVersionedChunkReader(
            NTableClient::TChunkReaderConfig::GetDefault(),
            std::move(remoteReader),
            oldChunkState,
            TCachedVersionedChunkMeta::Create(false, nullptr, oldChunkMeta),
            readerOptions,
            MakeSingletonRowRange(MinKey(), MaxKey()),
            /*columnFilter*/ {},
            AsyncLastCommittedTimestamp,
            /*produceAllVersions*/ true);

        auto writer = CreateVersionedChunkWriter(
            New<TChunkWriterConfig>(),
            CreateChunkWriterOptions(oldChunkMeta),
            oldChunkState->TableSchema,
            confirmingWriter);

        while (auto batch = ReadRowBatch(reader)) {
            YT_LOG_DEBUG("Versioned reincarnation: read %v rows", batch->GetRowCount());
            writer->Write(batch->MaterializeRows());
        }

        CopyMeta(writer->GetMeta(), oldChunkMeta);

        WaitFor(writer->Close())
            .ThrowOnError();
    }

    void ReincarnateUnversionedChunk(
        TDeferredChunkMetaPtr oldChunkMeta,
        IChunkReaderPtr remoteReader,
        TClientChunkReadOptions readerOptions,
        IChunkWriterPtr confirmingWriter,
        TColumnarChunkMetaPtr columnarMeta,
        TChunkStatePtr oldChunkState)
    {
        auto reader = CreateSchemalessRangeChunkReader(
            oldChunkState,
            New<TColumnarChunkMeta>(*oldChunkMeta),
            NTableClient::TChunkReaderConfig::GetDefault(),
            TChunkReaderOptions::GetDefault(),
            remoteReader,
            New<TNameTable>(),
            readerOptions,
            /*keyColumns*/ {},
            /*omittedInaccessibleColumns*/ {},
            NTableClient::TColumnFilter(),
            NChunkClient::TReadRange());

        TChunkTimestamps chunkTimestamps;
        if (auto misc = GetChunkMiscExt(oldChunkMeta)) {
            if (misc->has_min_timestamp()) {
                chunkTimestamps.MinTimestamp = misc->min_timestamp();
            }
            if (misc->has_max_timestamp()) {
                chunkTimestamps.MaxTimestamp = misc->max_timestamp();
            }
        }

        auto writer = CreateSchemalessChunkWriter(
            New<TChunkWriterConfig>(),
            CreateChunkWriterOptions(oldChunkMeta),
            columnarMeta->ChunkSchema(),
            columnarMeta->ChunkNameTable(),
            confirmingWriter,
            /*dataSink*/ std::nullopt,
            chunkTimestamps);

        while (auto batch = ReadRowBatch(reader)) {
            writer->Write(batch->MaterializeRows());
        }

        CopyMeta(
            writer->GetMeta(),
            oldChunkMeta);

        WaitFor(writer->Close())
            .ThrowOnError();
    }

    TChunkWriterOptionsPtr CreateChunkWriterOptions(const TDeferredChunkMetaPtr& oldChunkMeta)
    {
        auto chunkWriterOptions = New<TChunkWriterOptions>();
        chunkWriterOptions->CompressionCodec = CompressionCodec_;
        chunkWriterOptions->EnableSkynetSharing = EnableSkynetSharing_.value_or(false);
        chunkWriterOptions->OptimizeFor = OptimizeForFromFormat(
            CheckedEnumCast<EChunkFormat>(oldChunkMeta->format()));
        if (auto miscExt = GetChunkMiscExt(oldChunkMeta); miscExt && miscExt->has_eden()) {
            chunkWriterOptions->ChunksEden = miscExt->eden();
        }
        chunkWriterOptions->Postprocess();
        return chunkWriterOptions;
    }

    bool IsTestingFailureNeeded()
    {
        return Bootstrap_
            ->GetDynamicConfigManager()
            ->GetConfig()
            ->DataNode
            ->TestingOptions
            ->FailReincarnationJobs;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TAutotomizeChunkJob
    : public TMasterJobBase
{
public:
    TAutotomizeChunkJob(
        NChunkServer::TJobId jobId,
        const TJobSpec& jobSpec,
        TString jobTrackerAddress,
        const TJobResources& resourceLimits,
        const TJobResourceAttributes& resourceAttributes,
        IBootstrap* bootstrap)
        : TMasterJobBase(
            jobId,
            std::move(jobSpec),
            std::move(jobTrackerAddress),
            resourceLimits,
            resourceAttributes,
            bootstrap)
        , JobSpecExt_(JobSpec_.GetExtension(TAutotomizeChunkJobSpecExt::autotomize_chunk_job_spec_ext))
        , BodyChunkId_(FromProto<TChunkId>(JobSpecExt_.body_chunk_id()))
        , TailChunkId_(FromProto<TChunkId>(JobSpecExt_.tail_chunk_id()))
        , Overlayed_(JobSpecExt_.overlayed())
        , ReplicationFactor_(JobSpecExt_.replication_factor())
        , ReadQuorum_(JobSpecExt_.read_quorum())
        , WriteQuorum_(JobSpecExt_.write_quorum())
        , MediumIndex_(JobSpecExt_.medium_index())
        , ErasureCodecId_(CheckedEnumCast<NErasure::ECodec>(JobSpecExt_.erasure_codec()))
        , DynamicConfig_(Bootstrap_->GetDynamicConfigManager()->GetConfig()->DataNode->AutotomizeChunkJob)
    {
        NodeDirectory_->MergeFrom(JobSpecExt_.node_directory());
    }

private:
    const TAutotomizeChunkJobSpecExt JobSpecExt_;

    // Some commonly used fields from the job spec.
    const TChunkId BodyChunkId_;
    const TChunkId TailChunkId_;

    const bool Overlayed_;

    const int ReplicationFactor_;

    const int ReadQuorum_;
    const int WriteQuorum_;

    const int MediumIndex_;

    const NErasure::ECodec ErasureCodecId_;

    const TAutotomizeChunkJobDynamicConfigPtr DynamicConfig_;


    struct TChunkWriterWithIndex
    {
        const IChunkWriterPtr ChunkWriter;
        const int Index;
    };

    virtual void DoRun() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (DynamicConfig_->FailJobs) {
            THROW_ERROR_EXCEPTION("Testing failure");
        }
        if (DynamicConfig_->SleepInJobs) {
            YT_LOG_WARNING("Sleeping forever");
            TDelayedExecutor::WaitForDuration(TDuration::Max());
        }

        auto abortedBodyChunkReplicas = AbortBodyChunkSessions();

        i64 totalRowCount;
        auto bodyChunkSealInfo = ComputeBodyChunkSealInfo(
            abortedBodyChunkReplicas,
            &totalRowCount);

        i64 tailChunkFirstRowIndex = bodyChunkSealInfo.row_count();
        i64 tailChunkLastRowIndex = totalRowCount;
        if (Overlayed_) {
            // Account header row.
            ++tailChunkFirstRowIndex;
            ++tailChunkLastRowIndex;
        }

        auto tailChunkRows = ReadBodyChunkRows(
            /*firstRowIndex*/ tailChunkFirstRowIndex,
            /*lastRowIndex*/ tailChunkLastRowIndex);
        i64 tailLogicalRowCount = std::ssize(tailChunkRows);

        if (Overlayed_) {
            // Add header row.
            auto headerRow = CreateTailChunkHeaderRow(bodyChunkSealInfo);
            tailChunkRows.insert(tailChunkRows.begin(), headerRow);
        }

        auto tailChunkParts = PrepareParts(tailChunkRows);
        auto tailChunkWriters = CreateWriters();
        auto succeededWriters = WriteTailChunk(tailChunkParts, tailChunkWriters);
        ConfirmTailChunk(succeededWriters);

        SetJobResult(bodyChunkSealInfo, tailLogicalRowCount);
    }

    std::vector<TChunkReplicaDescriptor> AbortBodyChunkSessions()
    {
        YT_LOG_DEBUG("Aborting body chunk sessions (BodyChunkId: %v)",
            BodyChunkId_);

        auto bodyChunkReplicas = FromProto<TChunkReplicaWithMediumList>(JobSpecExt_.body_chunk_replicas());

        std::vector<TChunkReplicaDescriptor> bodyChunkReplicaDescriptors;
        bodyChunkReplicaDescriptors.reserve(bodyChunkReplicas.size());
        for (const auto& bodyChunkReplica : bodyChunkReplicas) {
            const auto& nodeDescriptor = NodeDirectory_->GetDescriptor(bodyChunkReplica.GetNodeId());
            TChunkReplicaDescriptor bodyChunkReplicaDescriptor{
                nodeDescriptor,
                bodyChunkReplica.GetReplicaIndex(),
                bodyChunkReplica.GetMediumIndex()
            };
            bodyChunkReplicaDescriptors.push_back(bodyChunkReplicaDescriptor);
        }

        auto future = AbortSessionsQuorum(
            BodyChunkId_,
            bodyChunkReplicaDescriptors,
            DynamicConfig_->RpcTimeout,
            /*quorumSessionDelay*/ TDuration::Zero(),
            ReadQuorum_,
            GetNodeChannelFactory());
        auto abortedBodyChunkReplicas = WaitFor(future)
            .ValueOrThrow();

        YT_LOG_DEBUG("Body chunk replicas aborted (BodyChunkId: %v, AbortedReplicas: %v)",
            BodyChunkId_,
            abortedBodyChunkReplicas);

        return abortedBodyChunkReplicas;
    }

    TChunkSealInfo ComputeBodyChunkSealInfo(
        const std::vector<TChunkReplicaDescriptor>& abortedBodyChunkReplicas,
        i64* totalRowCount)
    {
        YT_LOG_DEBUG("Computing body chunk row count (BodyChunkId: %v)",
            BodyChunkId_);

        auto nodeChannelFactory = GetNodeChannelFactory();

        std::vector<TFuture<TDataNodeServiceProxy::TRspGetChunkMetaPtr>> metaFutures;
        metaFutures.reserve(abortedBodyChunkReplicas.size());
        for (const auto& bodyChunkReplica : abortedBodyChunkReplicas) {
            auto channel = nodeChannelFactory->CreateChannel(bodyChunkReplica.NodeDescriptor);
            TDataNodeServiceProxy proxy(channel);

            auto chunkIdWithIndex = TChunkIdWithIndex(BodyChunkId_, bodyChunkReplica.ReplicaIndex);
            auto partChunkId = EncodeChunkId(chunkIdWithIndex);

            auto req = proxy.GetChunkMeta();
            TWorkloadDescriptor workloadDescriptor;
            workloadDescriptor.Category = EWorkloadCategory::SystemTabletRecovery;
            workloadDescriptor.DiskFairShareBucketTag = ToString(JobId_);
            SetRequestWorkloadDescriptor(req, workloadDescriptor);
            req->SetTimeout(DynamicConfig_->RpcTimeout);
            ToProto(req->mutable_chunk_id(), partChunkId);
            req->add_extension_tags(TProtoExtensionTag<TMiscExt>::Value);
            req->set_supported_chunk_features(ToUnderlying(GetSupportedChunkFeatures()));

            metaFutures.push_back(req->Invoke());
        }

        THashMap<TChunkLocationUuid, TString> locationUuidToAddress;

        auto rspOrErrors = WaitFor(AllSet(metaFutures))
            .ValueOrThrow();
        YT_VERIFY(rspOrErrors.size() == abortedBodyChunkReplicas.size());

        std::vector<TChunkSealInfo> replicaInfos;
        replicaInfos.reserve(rspOrErrors.size());
        for (int index = 0; index < std::ssize(rspOrErrors); ++index) {
            const auto& replica = abortedBodyChunkReplicas[index];
            const auto& rspOrError = rspOrErrors[index];
            const auto& address = replica.NodeDescriptor.GetDefaultAddress();
            if (rspOrError.IsOK()) {
                const auto& rsp = rspOrError.Value();
                auto locationUuid = FromProto<TChunkLocationUuid>(rsp->location_uuid());
                const auto& miscExt = GetProtoExtension<TMiscExt>(rsp->chunk_meta().extensions());

                TChunkSealInfo chunkSealInfo;
                chunkSealInfo.set_row_count(GetLogicalChunkRowCount(miscExt.row_count(), Overlayed_));
                chunkSealInfo.set_compressed_data_size(miscExt.compressed_data_size());
                chunkSealInfo.set_uncompressed_data_size(miscExt.uncompressed_data_size());
                replicaInfos.push_back(chunkSealInfo);

                YT_LOG_DEBUG("Body chunk replica info received "
                    "(BodyChunkId: %v, Address: %v, LogicalRowCount: %v, PhysicalRowCount: %v, LocationUuid: %v)",
                    BodyChunkId_,
                    address,
                    chunkSealInfo.row_count(),
                    miscExt.row_count(),
                    locationUuid);

                if (locationUuidToAddress.contains(locationUuid)) {
                    THROW_ERROR_EXCEPTION("Coinciding location uuid %v reported by nodes %v and %v",
                        locationUuid,
                        address,
                        locationUuidToAddress[locationUuid]);
                } else {
                    YT_VERIFY(locationUuidToAddress.emplace(locationUuid, address).second);
                }
            } else {
                YT_LOG_DEBUG(rspOrError, "Failed to get body chunk replica info (BodyChunkId: %v, Address: %v)",
                    BodyChunkId_,
                    address);
            }
        }

        if (std::ssize(replicaInfos) < ReadQuorum_) {
            THROW_ERROR_EXCEPTION("Unable to compute quorum info for body chunk %v: too few replicas known, %v given, %v needed",
                BodyChunkId_,
                std::ssize(replicaInfos),
                ReadQuorum_);
        }

        SortBy(replicaInfos, [&] (const auto& info) {
            return info.row_count();
        });

        auto bodyChunkSealInfo = replicaInfos.back();
        auto bodyChunkReplicaLagLimit = JobSpecExt_.body_chunk_replica_lag_limit();
        auto bodyChunkLogicalRowCount = std::max<i64>(bodyChunkSealInfo.row_count() - bodyChunkReplicaLagLimit, 0);
        bodyChunkSealInfo.set_first_overlayed_row_index(JobSpecExt_.body_chunk_first_overlayed_row_index());
        bodyChunkSealInfo.set_row_count(bodyChunkLogicalRowCount);

        auto readQuorumInfoIndex = IsErasure()
            ? ReadQuorum_ - NErasure::GetCodec(ErasureCodecId_)->GetGuaranteedRepairablePartCount()
            : ReadQuorum_ - 1;
        *totalRowCount = replicaInfos[readQuorumInfoIndex].row_count();

        YT_LOG_DEBUG("Body chunk seal info computed "
            "(BodyChunkId: %v, ReadQuorum: %v, BodyChunkLogicalRowCount: %v, BodyChunkPhysicalRowCount: %v, TotalRowCount: %v)",
            BodyChunkId_,
            ReadQuorum_,
            bodyChunkSealInfo.row_count(),
            GetPhysicalChunkRowCount(bodyChunkLogicalRowCount, Overlayed_),
            *totalRowCount);

        return bodyChunkSealInfo;
    }

    std::vector<TSharedRef> ReadBodyChunkRows(i64 firstRowIndex, i64 lastRowIndex)
    {
        YT_LOG_DEBUG("Reading body chunk rows (BodyChunkId: %v, Rows: %v-%v)",
            BodyChunkId_,
            firstRowIndex,
            lastRowIndex - 1);

        if (firstRowIndex >= lastRowIndex) {
            return {};
        }

        auto bodyChunkReplicas = FromProto<TChunkReplicaWithMediumList>(JobSpecExt_.body_chunk_replicas());

        auto chunkReaderHost = New<TChunkReaderHost>(
            Bootstrap_->GetClient(),
            Bootstrap_->GetLocalDescriptor(),
            Bootstrap_->GetBlockCache(),
            /*chunkMetaCache*/ nullptr,
            /*nodeStatusDirectory*/ nullptr,
            Bootstrap_->GetThrottler(EDataNodeThrottlerKind::AutotomyIn),
            /*rpsThrottler*/ GetUnlimitedThrottler(),
            /*trafficMeter*/ nullptr);

        auto reader = NJournalClient::CreateChunkReader(
            DynamicConfig_->Reader,
            std::move(chunkReaderHost),
            BodyChunkId_,
            ErasureCodecId_,
            bodyChunkReplicas);

        auto trackSystemJobsMemory = Bootstrap_
            ->GetDataNodeBootstrap()
            ->GetDynamicConfigManager()
            ->GetConfig()
            ->DataNode
            ->TrackSystemJobsMemory;
        auto tracker = Bootstrap_
            ->GetNodeMemoryReferenceTracker()
            ->WithCategory(EMemoryCategory::SystemJobs);

        IChunkReader::TReadBlocksOptions readBlocksOptions;
        readBlocksOptions.ClientOptions.TrackMemoryAfterSessionCompletion = Bootstrap_
            ->GetDataNodeBootstrap()
            ->GetDynamicConfigManager()
            ->GetConfig()
            ->DataNode
            ->TrackMemoryAfterSessionCompletion;
        readBlocksOptions.ClientOptions.MemoryReferenceTracker = trackSystemJobsMemory ? tracker : nullptr;
        auto& workloadDescriptor = readBlocksOptions.ClientOptions.WorkloadDescriptor;
        workloadDescriptor.Category = EWorkloadCategory::SystemTabletRecovery;
        workloadDescriptor.Annotations = {Format("Autotomy of chunk %v", BodyChunkId_)};
        workloadDescriptor.DiskFairShareBucketTag = ToString(JobId_);

        std::vector<TSharedRef> rows;
        rows.reserve(lastRowIndex - firstRowIndex);
        while (firstRowIndex < lastRowIndex) {
            YT_LOG_DEBUG("Reading rows (Rows: %v-%v)",
                firstRowIndex,
                lastRowIndex - 1);

            auto asyncBlocks = reader->ReadBlocks(
                readBlocksOptions,
                firstRowIndex,
                lastRowIndex - firstRowIndex);
            auto blocks = WaitFor(asyncBlocks)
                .ValueOrThrow();

            int blockCount = blocks.size();
            if (blockCount == 0) {
                THROW_ERROR_EXCEPTION("Rows %v-%v are missing but needed to autotomize body chunk %v",
                    firstRowIndex,
                    lastRowIndex - 1,
                    BodyChunkId_);
            }

            YT_LOG_DEBUG("Rows received (Rows: %v-%v)",
                firstRowIndex,
                firstRowIndex + blockCount - 1);

            for (const auto& block : blocks) {
                const auto& row = block.Data;
                rows.push_back(row);
            }

            firstRowIndex += blockCount;
        }

        YT_LOG_DEBUG("Body chunk reading completed");

        return rows;
    }

    TSharedRef CreateTailChunkHeaderRow(const TChunkSealInfo& bodyChunkSealInfo)
    {
        i64 tailFirstRowIndex = bodyChunkSealInfo.first_overlayed_row_index() + bodyChunkSealInfo.row_count();
        NJournalClient::NProto::TOverlayedJournalChunkHeader header;
        header.set_first_row_index(tailFirstRowIndex);

        YT_LOG_DEBUG("Created tail chunk header row (TailFirstRowIndex: %v)",
            tailFirstRowIndex);

        return SerializeProtoToRef(header);
    }

    std::vector<std::vector<TSharedRef>> PrepareParts(const std::vector<TSharedRef>& rows)
    {
        if (IsErasure()) {
            auto* codec = NErasure::GetCodec(ErasureCodecId_);
            return EncodeErasureJournalRows(codec, rows);
        } else {
            return std::vector<std::vector<TSharedRef>>(ReplicationFactor_, rows);
        }
    }

    std::vector<TChunkWriterWithIndex> CreateWriters()
    {
        auto writeSessionId = TSessionId(TailChunkId_, MediumIndex_);

        YT_LOG_DEBUG("Creating tail chunk writers (TailChunkId: %v, SessionId: %v)",
            TailChunkId_,
            writeSessionId);

        if (IsErasure()) {
            auto* erasureCodec = NErasure::GetCodec(ErasureCodecId_);
            auto erasurePartWriters = CreateAllErasurePartWriters(
                DynamicConfig_->Writer,
                New<TRemoteWriterOptions>(),
                writeSessionId,
                erasureCodec,
                Bootstrap_->GetClient(),
                /*trafficMeter*/ nullptr,
                Bootstrap_->GetThrottler(EDataNodeThrottlerKind::AutotomyOut),
                GetNullBlockCache());

            std::vector<TChunkWriterWithIndex> writers;
            writers.reserve(erasurePartWriters.size());
            for (int index = 0; index < std::ssize(erasurePartWriters); ++index) {
                writers.push_back(TChunkWriterWithIndex{
                    .ChunkWriter = std::move(erasurePartWriters[index]),
                    .Index = index
                });
            }

            return writers;
        } else {
            // Journals do not support SendBlocks, so we create
            // #ReplicationFactor replication writers instead of one.

            auto writeTargets = AllocateWriteTargets(
                Bootstrap_->GetClient(),
                writeSessionId,
                /*desiredTargetCount*/ ReplicationFactor_,
                /*minTargetCount*/ ReplicationFactor_,
                /*replicationFactorOverride*/ std::nullopt,
                /*localHostName*/ Bootstrap_->GetLocalHostName(),
                /*forbiddenAddresses*/ {},
                /*allocatedAddresses*/ {},
                Logger);
            YT_VERIFY(std::ssize(writeTargets) == ReplicationFactor_);

            // Each writer uploads exactly one replica.
            auto writerConfig = CloneYsonStruct(DynamicConfig_->Writer);
            writerConfig->UploadReplicationFactor = 1;
            writerConfig->MinUploadReplicationFactor = 1;

            auto writerOptions = New<TRemoteWriterOptions>();
            writerOptions->AllowAllocatingNewTargetNodes = false;

            std::vector<TChunkWriterWithIndex> writers;
            writers.reserve(ReplicationFactor_);
            for (int index = 0; index < ReplicationFactor_; ++index) {
                auto writer = CreateReplicationWriter(
                    writerConfig,
                    writerOptions,
                    writeSessionId,
                    {writeTargets[index]},
                    Bootstrap_->GetClient(),
                    Bootstrap_->GetLocalHostName(),
                    GetNullBlockCache(),
                    /*trafficMeter*/ nullptr,
                    Bootstrap_->GetThrottler(EDataNodeThrottlerKind::AutotomyOut));
                writers.push_back(TChunkWriterWithIndex{
                    .ChunkWriter = std::move(writer),
                    .Index = index
                });
            }

            return writers;
        }
    }

    //! Returns the list of the writers that wrote replica successfully.
    std::vector<TChunkWriterWithIndex> WriteTailChunk(
        const std::vector<std::vector<TSharedRef>>& parts,
        const std::vector<TChunkWriterWithIndex>& writers)
    {
        YT_LOG_DEBUG("Started tail chunk write (TailChunkId: %v, RowCount: %v)",
            TailChunkId_,
            parts[0].size());

        YT_VERIFY(parts.size() == writers.size());

        std::vector<TFuture<void>> replicaFutures;
        replicaFutures.reserve(writers.size());

        TWorkloadDescriptor workloadDescriptor;
        workloadDescriptor.Category = EWorkloadCategory::SystemTabletRecovery;
        workloadDescriptor.DiskFairShareBucketTag = ToString(JobId_);

        for (int index = 0; index < std::ssize(parts); ++index) {
            const auto& part = parts[index];
            const auto& writer = writers[index];

            auto future = BIND([&, index, jobLogger = Logger] {
                auto Logger = jobLogger
                    .WithTag("TailChunkId: %v, WriterIndex: %v",
                        TailChunkId_,
                        index);

                auto& chunkWriter = writer.ChunkWriter;

                YT_LOG_DEBUG("Opening writer");

                WaitFor(chunkWriter->Open())
                    .ThrowOnError();

                YT_LOG_DEBUG("Writing rows");

                std::vector<TBlock> blocks;
                blocks.reserve(part.size());
                for (const auto& row : part) {
                    blocks.push_back(TBlock(row));
                }
                chunkWriter->WriteBlocks(workloadDescriptor, blocks);

                YT_LOG_DEBUG("Closing writer");

                WaitFor(chunkWriter->Close(workloadDescriptor))
                    .ThrowOnError();

                YT_LOG_DEBUG("Writer closed");
            })
                .AsyncVia(GetCurrentInvoker())
                .Run();
            replicaFutures.push_back(std::move(future));
        }

        auto replicaOrErrors = WaitFor(AllSet(std::move(replicaFutures)))
            .ValueOrThrow();
        YT_VERIFY(replicaOrErrors.size() == writers.size());

        std::vector<TChunkWriterWithIndex> succeededWriters;
        succeededWriters.reserve(writers.size());

        std::vector<TError> writerErrors;
        for (int index = 0; index < std::ssize(replicaOrErrors); ++index) {
            const auto& replicaOrError = replicaOrErrors[index];
            if (replicaOrError.IsOK()) {
                succeededWriters.push_back(writers[index]);
            } else {
                auto error = TError("Tail replica writer failed")
                    << TErrorAttribute("tail_chunk_id", TailChunkId_)
                    << TErrorAttribute("writer_index", index)
                    << replicaOrError;
                YT_LOG_WARNING(error);
                writerErrors.push_back(std::move(error));
            }
        }

        if (std::ssize(succeededWriters) < WriteQuorum_) {
            THROW_ERROR_EXCEPTION("Too few tail chunk writers finished successfully: %v completed, %v needed",
                succeededWriters.size(),
                WriteQuorum_)
                << writerErrors;
        }

        return succeededWriters;
    }

    void ConfirmTailChunk(const std::vector<TChunkWriterWithIndex>& succeededWriters)
    {
        YT_LOG_DEBUG("Confirming tail chunk (ChunkId: %v)",
            TailChunkId_);

        TChunkReplicaWithLocationList writtenReplicas;
        for (const auto& writer : succeededWriters) {
            auto replicas = writer.ChunkWriter->GetWrittenChunkReplicas();
            YT_VERIFY(replicas.size() == 1);
            const auto& replica = replicas[0];
            int replicaIndex = IsErasure()
                ? writer.Index
                : GenericChunkReplicaIndex;
            writtenReplicas.emplace_back(
                replica.GetNodeId(),
                replicaIndex,
                replica.GetMediumIndex(),
                replica.GetChunkLocationUuid());
        }

        const auto& client = Bootstrap_->GetClient();
        auto cellTag = CellTagFromId(TailChunkId_);
        auto channel = client->GetMasterChannelOrThrow(NApi::EMasterChannelKind::Leader, cellTag);

        TChunkServiceProxy proxy(channel);
        auto batchReq = proxy.ExecuteBatch();
        GenerateMutationId(batchReq);
        SetSuppressUpstreamSync(&batchReq->Header(), true);
        // COMPAT(shakurov): prefer proto ext (above).
        batchReq->set_suppress_upstream_sync(true);

        auto* req = batchReq->add_confirm_chunk_subrequests();
        ToProto(req->mutable_chunk_id(), TailChunkId_);
        req->mutable_chunk_info();
        ToProto(req->mutable_legacy_replicas(), writtenReplicas);
        auto* meta = req->mutable_chunk_meta();
        meta->set_type(ToProto<int>(EChunkType::Journal));
        meta->set_format(ToProto<int>(EChunkFormat::JournalDefault));
        TMiscExt miscExt;
        SetProtoExtension(meta->mutable_extensions(), miscExt);

        req->set_location_uuids_supported(true);

        bool useLocationUuids = std::all_of(writtenReplicas.begin(), writtenReplicas.end(), [](const auto& replica) {
            return replica.GetChunkLocationUuid() != InvalidChunkLocationUuid;
        });

        if (useLocationUuids) {
            for (const auto& replica : writtenReplicas) {
                auto* replicaInfo = req->add_replicas();
                replicaInfo->set_replica(ToProto<ui64>(replica));
                ToProto(replicaInfo->mutable_location_uuid(), replica.GetChunkLocationUuid());
            }
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            GetCumulativeError(batchRspOrError),
            "Error confirming tail chunk %v",
            TailChunkId_);

        YT_LOG_DEBUG("Tail chunk confirmed (ChunkId: %v)",
            TailChunkId_);
    }

    void SetJobResult(
        const TChunkSealInfo& bodyChunkSealInfo,
        i64 tailRowCount)
    {
        auto* jobResultExt = Result_.MutableExtension(TAutotomizeChunkJobResultExt::autotomize_chunk_job_result_ext);
        ToProto(jobResultExt->mutable_body_chunk_id(), BodyChunkId_);
        *jobResultExt->mutable_body_chunk_seal_info() = bodyChunkSealInfo;
        ToProto(jobResultExt->mutable_tail_chunk_id(), TailChunkId_);

        i64 tailFirstRowIndex = bodyChunkSealInfo.first_overlayed_row_index() + bodyChunkSealInfo.row_count();
        jobResultExt->mutable_tail_chunk_seal_info()->set_first_overlayed_row_index(tailFirstRowIndex);
        jobResultExt->mutable_tail_chunk_seal_info()->set_row_count(tailRowCount);
        jobResultExt->mutable_tail_chunk_seal_info()->set_uncompressed_data_size(1);
        jobResultExt->mutable_tail_chunk_seal_info()->set_compressed_data_size(1);
    }

    bool IsErasure() const
    {
        return ErasureCodecId_ != NErasure::ECodec::None;
    }

    INodeChannelFactoryPtr GetNodeChannelFactory() const
    {
        return Bootstrap_
            ->GetClient()
            ->GetNativeConnection()
            ->CreateNativeClient({.User = NSecurityClient::RootUserName})
            ->GetChannelFactory();
    }
};

////////////////////////////////////////////////////////////////////////////////

TMasterJobBasePtr CreateJob(
    NChunkServer::TJobId jobId,
    TJobSpec&& jobSpec,
    TString jobTrackerAddress,
    const TJobResources& resourceLimits,
    const TJobResourceAttributes& resourceAttributes,
    IBootstrap* bootstrap,
    const TMasterJobSensors& sensors)
{
    auto type = CheckedEnumCast<EJobType>(jobSpec.type());
    switch (type) {
        case EJobType::ReplicateChunk:
            return NewWithOffloadedDtor<TChunkReplicationJob>(
                bootstrap->GetJobInvoker(),
                jobId,
                std::move(jobSpec),
                std::move(jobTrackerAddress),
                resourceLimits,
                resourceAttributes,
                bootstrap);

        case EJobType::RemoveChunk:
            return NewWithOffloadedDtor<TChunkRemovalJob>(
                bootstrap->GetJobInvoker(),
                jobId,
                std::move(jobSpec),
                std::move(jobTrackerAddress),
                resourceLimits,
                resourceAttributes,
                bootstrap);

        case EJobType::RepairChunk:
            return NewWithOffloadedDtor<TChunkRepairJob>(
                bootstrap->GetJobInvoker(),
                jobId,
                std::move(jobSpec),
                std::move(jobTrackerAddress),
                resourceLimits,
                resourceAttributes,
                bootstrap,
                sensors);

        case EJobType::SealChunk:
            return NewWithOffloadedDtor<TSealChunkJob>(
                bootstrap->GetJobInvoker(),
                jobId,
                std::move(jobSpec),
                std::move(jobTrackerAddress),
                resourceLimits,
                resourceAttributes,
                bootstrap);

        case EJobType::MergeChunks: {
            auto totalMergeJobMemoryLimit = bootstrap
                ->GetDynamicConfigManager()
                ->GetConfig()
                ->DataNode
                ->MergeChunksJob
                ->ReadMemoryLimit;
            auto mergeSlots = bootstrap
                ->GetJobResourceManager()
                ->GetResourceLimits()
                .MergeSlots;
            auto readMemoryLimit = totalMergeJobMemoryLimit / mergeSlots;

            return NewWithOffloadedDtor<TChunkMergeJob>(
                bootstrap->GetJobInvoker(),
                jobId,
                std::move(jobSpec),
                std::move(jobTrackerAddress),
                resourceLimits,
                resourceAttributes,
                bootstrap,
                readMemoryLimit);
        }

        case EJobType::AutotomizeChunk:
            return NewWithOffloadedDtor<TAutotomizeChunkJob>(
                bootstrap->GetJobInvoker(),
                jobId,
                std::move(jobSpec),
                std::move(jobTrackerAddress),
                resourceLimits,
                resourceAttributes,
                bootstrap);

        case EJobType::ReincarnateChunk:
            return NewWithOffloadedDtor<TChunkReincarnationJob>(
                bootstrap->GetJobInvoker(),
                jobId,
                std::move(jobSpec),
                std::move(jobTrackerAddress),
                resourceLimits,
                resourceAttributes,
                bootstrap);

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode

