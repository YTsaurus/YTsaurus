#include "job.h"

#include "allocation.h"
#include "bootstrap.h"
#include "chunk_cache.h"
#include "controller_agent_connector.h"
#include "job_controller.h"
#include "job_gpu_checker.h"
#include "job_proxy_log_manager.h"
#include "job_workspace_builder.h"
#include "job_input_cache.h"
#include "gpu_manager.h"
#include "private.h"
#include "slot.h"
#include "slot_manager.h"
#include "throttler_manager.h"
#include "volume_manager.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/node/data_node/artifact.h>
#include <yt/yt/server/node/data_node/bootstrap.h>
#include <yt/yt/server/node/data_node/chunk.h>
#include <yt/yt/server/node/data_node/location.h>
#include <yt/yt/server/node/job_agent/job_resource_manager.h>

#include <yt/yt/library/containers/public.h>

#include <yt/yt/library/containers/cri/config.h>

#include <yt/yt/server/lib/controller_agent/helpers.h>
#include <yt/yt/server/lib/controller_agent/statistics.h>

#include <yt/yt/server/lib/io/io_tracker.h>

#include <yt/yt/server/lib/exec_node/config.h>
#include <yt/yt/server/lib/exec_node/helpers.h>

#include <yt/yt/server/lib/misc/cluster_throttlers_config.h>

#include <yt/yt/server/lib/scheduler/helpers.h>

#include <yt/yt/server/lib/job_agent/structs.h>

#include <yt/yt/server/lib/job_proxy/job_probe.h>

#include <yt/yt/server/lib/misc/job_reporter.h>

#include <yt/yt/server/lib/nbd/profiler.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/traffic_meter.h>
#include <yt/yt/ytlib/chunk_client/job_spec_extensions.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/orchid/orchid_ypath_service.h>
#include <yt/yt/ytlib/scheduler/config.h>

#include <yt/yt/ytlib/table_client/helpers.h>

#include <yt/yt/ytlib/job_prober_client/public.h>

#include <yt/yt/ytlib/job_proxy/public.h>

#include <yt/yt/ytlib/security_client/public.h>

#include <yt/yt/ytlib/job_proxy/config.h>

#include <yt/yt/ytlib/node_tracker_client/node_directory_builder.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/misc/io_tags.h>

#include <yt/yt/client/chunk_client/helpers.h>

#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/library/profiling/producer.h>

#include <yt/yt/library/tcmalloc/config.h>

#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/actions/cancelable_context.h>
#include <yt/yt/core/actions/new_with_offloaded_dtor.h>

#include <yt/yt/core/bus/tcp/client.h>

#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/misc/statistics.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/rpc/bus/channel.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/core/ytree/service_combiner.h>
#include <yt/yt/core/ytree/virtual.h>

#include <yt/yt/core/logging/config.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_spec.pb.h>

#include <library/cpp/yt/error/error_helpers.h>

#include <library/cpp/yt/system/handle_eintr.h>

#include <util/system/env.h>

namespace NYT::NExecNode {

using namespace NRpc;
using namespace NJobProxy;
using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTableClient;
using namespace NFileClient;
using namespace NClusterNode;
using namespace NDataNode;
using namespace NClusterNode;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NIO;
using namespace NJobAgent;
using namespace NJobProberClient;
using namespace NChunkServer;
using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NControllerAgent;
using namespace NControllerAgent::NProto;
using namespace NConcurrency;
using namespace NApi;
using namespace NCoreDump;
using namespace NNet;
using namespace NProfiling;
using namespace NContainers;
using namespace NTracing;
using namespace NTransactionClient;
using namespace NObjectClient;
using namespace NStatisticPath;
using namespace NNbd;
using namespace NSquashFS;
using namespace NServer;

using NNodeTrackerClient::TNodeDirectory;
using NChunkClient::TDataSliceDescriptor;

using NObjectClient::TObjectId;
using NCypressClient::EObjectType;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto DisableSandboxCleanupEnv = "YT_DISABLE_SANDBOX_CLEANUP";

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

TGuid MakeNbdExportId(TJobId jobId, int nbdExportIndex)
{
    auto nbdExportId = jobId.Underlying();
    nbdExportId.Parts32[0] = nbdExportIndex;
    return nbdExportId;
}

////////////////////////////////////////////////////////////////////////////////

static const TString GpuUtilizationGpuSensorName = "gpu/utilization_gpu";
static const TString GpuUtilizationMemorySensorName = "gpu/utilization_memory";
static const TString GpuUtilizationPowerSensorName = "gpu/utilization_power";
static const TString GpuSMUtilizationSensorName = "gpu/sm_utilization";
static const TString GpuSMOccupancySensorName = "gpu/sm_occupancy";
static const TString GpuMemorySensorName = "gpu/memory";
static const TString GpuPowerSensorName = "gpu/power";
static const TString GpuNvlinkRxBytesSensorName = "gpu/nvlink/rx_bytes";
static const TString GpuNvlinkTxBytesSensorName = "gpu/nvlink/tx_bytes";
static const TString GpuPcieRxBytesSensorName = "gpu/pcie/rx_bytes";
static const TString GpuPcieTxBytesSensorName = "gpu/pcie/tx_bytes";
static const TString GpuStuckSensorName = "gpu/stuck";
static const TString GpuRdmaRxBytesSensorName = "gpu/rdma/rx_bytes";
static const TString GpuRdmaTxBytesSensorName = "gpu/rdma/tx_bytes";
static const TString GpuTensorActivitySensorName = "gpu/tensor_activity";
static const TString GpuDramActivitySensorName = "gpu/dram_activity";

const THashMap<TString, TUserJobSensorPtr>& GetSupportedGpuMonitoringSensors()
{
    static const auto SupportedGpuMonitoringSensors = ConvertTo<THashMap<TString, TUserJobSensorPtr>>(BuildYsonStringFluently()
        .BeginMap()
            .Item(GpuUtilizationGpuSensorName).BeginMap()
                .Item("type").Value("gauge")
                .Item("profiling_name").Value("/user_job/gpu/utilization_gpu")
            .EndMap()
            .Item(GpuUtilizationMemorySensorName).BeginMap()
                .Item("type").Value("gauge")
                .Item("profiling_name").Value("/user_job/gpu/utilization_memory")
            .EndMap()
            .Item(GpuUtilizationPowerSensorName).BeginMap()
                .Item("type").Value("gauge")
                .Item("profiling_name").Value("/user_job/gpu/utilization_power")
            .EndMap()
            .Item(GpuSMUtilizationSensorName).BeginMap()
                .Item("type").Value("gauge")
                .Item("profiling_name").Value("/user_job/gpu/sm_utilization")
            .EndMap()
            .Item(GpuSMOccupancySensorName).BeginMap()
                .Item("type").Value("gauge")
                .Item("profiling_name").Value("/user_job/gpu/sm_occupancy")
            .EndMap()
            .Item(GpuMemorySensorName).BeginMap()
                .Item("type").Value("gauge")
                .Item("profiling_name").Value("/user_job/gpu/memory")
            .EndMap()
            .Item(GpuPowerSensorName).BeginMap()
                .Item("type").Value("gauge")
                .Item("profiling_name").Value("/user_job/gpu/power")
            .EndMap()
            .Item(GpuNvlinkRxBytesSensorName).BeginMap()
                .Item("type").Value("gauge")
                .Item("profiling_name").Value("/user_job/gpu/nvlink/rx_bytes/rate")
            .EndMap()
            .Item(GpuNvlinkTxBytesSensorName).BeginMap()
                .Item("type").Value("gauge")
                .Item("profiling_name").Value("/user_job/gpu/nvlink/tx_bytes/rate")
            .EndMap()
            .Item(GpuPcieRxBytesSensorName).BeginMap()
                .Item("type").Value("gauge")
                .Item("profiling_name").Value("/user_job/gpu/pcie/rx_bytes/rate")
            .EndMap()
            .Item(GpuPcieTxBytesSensorName).BeginMap()
                .Item("type").Value("gauge")
                .Item("profiling_name").Value("/user_job/gpu/pcie/tx_bytes/rate")
            .EndMap()
            .Item(GpuStuckSensorName).BeginMap()
                .Item("type").Value("gauge")
                .Item("profiling_name").Value("/user_job/gpu/stuck")
            .EndMap()
            .Item(GpuRdmaRxBytesSensorName).BeginMap()
                .Item("type").Value("gauge")
                .Item("profiling_name").Value("/user_job/gpu/rdma/rx_bytes/rate")
            .EndMap()
            .Item(GpuRdmaTxBytesSensorName).BeginMap()
                .Item("type").Value("gauge")
                .Item("profiling_name").Value("/user_job/gpu/rdma/tx_bytes/rate")
            .EndMap()
            .Item(GpuTensorActivitySensorName).BeginMap()
                .Item("type").Value("gauge")
                .Item("profiling_name").Value("/user_job/gpu/tensor_activity")
            .EndMap()
            .Item(GpuDramActivitySensorName).BeginMap()
                .Item("type").Value("gauge")
                .Item("profiling_name").Value("/user_job/gpu/dram_activity")
            .EndMap()

            // COMPAT(eshcherbin): These sensors are no longer produced, however we cannot remove them
            // because user jobs will fail otherwise.
            .Item("gpu/utilization_clock_sm").BeginMap()
                .Item("type").Value("gauge")
                .Item("profiling_name").Value("/user_job/gpu/utilization_clock_sm")
            .EndMap()
            .Item("gpu/clock_sm").BeginMap()
                .Item("type").Value("gauge")
                .Item("profiling_name").Value("/user_job/gpu/clock_sm")
            .EndMap()
        .EndMap());

    return SupportedGpuMonitoringSensors;
}

////////////////////////////////////////////////////////////////////////////////

void ProfileSensor(ISensorWriter* writer, const TUserJobSensorPtr& sensor, auto value)
{
    switch (sensor->Type) {
        case EMetricType::Counter:
            writer->AddCounter(sensor->ProfilingName, std::max<i64>(0, static_cast<i64>(value)));
            return;
        case EMetricType::Gauge:
            writer->AddGauge(sensor->ProfilingName, static_cast<double>(value));
            return;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TJob::TJob(
    TJobId jobId,
    TOperationId operationId,
    TAllocationPtr allocation,
    TJobSpec&& jobSpec,
    TControllerAgentDescriptor agentDescriptor,
    IBootstrap* bootstrap,
    const TJobCommonConfigPtr& commonConfig)
    : Id_(jobId)
    , OperationId_(operationId)
    , Bootstrap_(bootstrap)
    , JobType_(FromProto<EJobType>(jobSpec.type()))
    , Logger(ExecNodeLogger().WithTag(
        "JobId: %v, OperationId: %v, JobType: %v",
        jobId,
        operationId,
        JobType_))
    , Allocation_(std::move(allocation))
    , ResourceHolder_(Allocation_->GetResourceHolder())
    , InitialResourceDemand_(ResourceHolder_->GetInitialResourceDemand())
    , ControllerAgentDescriptor_(std::move(agentDescriptor))
    , ControllerAgentConnector_(
        Bootstrap_->GetControllerAgentConnectorPool()->GetControllerAgentConnector(ControllerAgentDescriptor_))
    , CommonConfig_(commonConfig)
    , Invoker_(Bootstrap_->GetJobInvoker())
    , CreationTime_(TInstant::Now())
    , TrafficMeter_(New<TTrafficMeter>(
        Bootstrap_->GetLocalDescriptor().GetDataCenter()))
    , GuardedJobSpec_(std::move(jobSpec))
    , JobSpec_(GuardedJobSpec_.Read([] (const TJobSpec& spec) -> const TJobSpec& {
        return spec;
    }))
    , JobSpecExt_(JobSpec_.GetExtension(TJobSpecExt::job_spec_ext))
    , UserJobSpec_(JobSpecExt_.has_user_job_spec() ? &JobSpecExt_.user_job_spec() : nullptr)
    , JobTestingOptions_(JobSpecExt_.has_testing_options()
        ? ConvertTo<TJobTestingOptionsPtr>(TYsonString(JobSpecExt_.testing_options()))
        : New<TJobTestingOptions>())
    , Interruptible_(JobSpecExt_.interruptible())
    , AbortJobIfAccountLimitExceeded_(JobSpecExt_.abort_job_if_account_limit_exceeded())
    , RootVolumeDiskQuotaEnabled_(JobSpecExt_.enable_root_volume_disk_quota())
    , HasUserJobSpec_(UserJobSpec_ != nullptr)
    , TmpfsVolumeInfos_(ParseTmpfsVolumeInfos(UserJobSpec_))
    , IsGpuRequested_(Allocation_->GetRequestedGpu() > 0)
    , TraceContext_(TTraceContext::NewRoot("Job"))
    , FinishGuard_(TraceContext_)
    , JobInputCache_(Bootstrap_->GetExecNodeBootstrap()->GetJobInputCache())
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);
    YT_VERIFY(JobInputCache_);

    YT_LOG_DEBUG("Creating job");

    PackBaggageFromJobSpec(TraceContext_, JobSpec_, OperationId_, Id_, JobType_);

    TrafficMeter_->Start();

    AddJobEvent(JobState_, JobPhase_);

    HandleJobReport(MakeDefaultJobReport()
        .TreeId(JobSpecExt_.tree_id()));
}

TJob::~TJob()
{
    YT_LOG_DEBUG("Destroying job");

    auto jobSpec = GuardedJobSpec_.Transform(
        [] (TJobSpec& spec) -> TJobSpec {
            return std::move(spec);
        });

    // Offload job spec destruction to a large thread pool.
    NRpc::TDispatcher::Get()->GetCompressionPoolInvoker()->Invoke(
        BIND_NO_PROPAGATE([jobSpec = std::move(jobSpec)]{ }));
}

TYsonString TJob::BuildArchiveFeatures() const
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return BuildYsonStringFluently()
        .BeginMap()
            .Item("has_trace").Value(HasJobTrace_)
        .EndMap();
}

void TJob::DoStart(TErrorOr<std::vector<TNameWithAddress>>&& resolvedNodeAddresses)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    GuardedAction(
        "DoStart",
        [&] {
            auto now = TInstant::Now();
            PreparationStartTime_ = now;

            if (!resolvedNodeAddresses.IsOK() || (CommonConfig_->Testing && CommonConfig_->Testing->FailAddressResolve)) {
                THROW_ERROR TError("Failed to resolve node addresses")
                    << TErrorAttribute("abort_reason", EAbortReason::AddressResolveFailed)
                    << std::move(resolvedNodeAddresses);
            }

            ResolvedNodeAddresses_ = std::move(resolvedNodeAddresses.Value());

            StartUserJobMonitoring();

            InitializeArtifacts();

            if (UserJobSpec_) {
                if (UserJobSpec_->has_prepare_time_limit()) {
                    auto prepareTimeLimit = FromProto<TDuration>(UserJobSpec_->prepare_time_limit());
                    TDelayedExecutor::Submit(
                        BIND(&TJob::OnJobPreparationTimeout, MakeWeak(this), prepareTimeLimit, /*fatal*/ false)
                            .Via(Invoker_),
                        prepareTimeLimit);
                }

                if (auto prepareTimeLimit = CommonConfig_->JobPrepareTimeLimit) {
                    TDelayedExecutor::Submit(
                        BIND(&TJob::OnJobPreparationTimeout, MakeWeak(this), *prepareTimeLimit, /*fatal*/ true)
                            .Via(Invoker_),
                        *prepareTimeLimit);
                }

                if (UserJobSpec_->has_network_project_id()) {
                    NetworkProjectId_ = UserJobSpec_->network_project_id();
                }
            }

            if (NeedGpu()) {
                GpuStatistics_.resize(std::size(GetGpuSlots()));
            }

            SetJobPhase(EJobPhase::PreparingNodeDirectory);

            // This is a heavy part of preparation, offload it to compression invoker.
            // TODO(babenko): get rid of MakeWeak
            BIND([this, weakThis = MakeWeak(this)] {
                auto this_ = weakThis.Lock();
                if (!this_) {
                    return std::unique_ptr<NNodeTrackerClient::NProto::TNodeDirectory>();
                }
                return PrepareNodeDirectory();
            })
                .AsyncVia(NRpc::TDispatcher::Get()->GetCompressionPoolInvoker())
                .Run()
                .SubscribeUnique(
                    BIND(&TJob::OnNodeDirectoryPrepared, MakeWeak(this))
                        .Via(Invoker_));
        });
}

bool TJob::IsStarted() const
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return Started_;
}

void TJob::OnResourcesAcquired() noexcept
{
    // Resources can not be acquired for job, we acquire resources for allocation and we can transfer it to job.
    YT_ABORT();
}

void TJob::Start() noexcept
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    TCurrentTraceContextGuard guard(TraceContext_);

    // Job may be aborted concurrently with allocation scheduled.
    if (JobPhase_ != EJobPhase::Created) {
        YT_LOG_INFO(
            "Job was not started since it is not in initial state (JobState: %v, JobPhase: %v)",
            JobState_,
            JobPhase_);
        return;
    }

    YT_VERIFY(!std::exchange(Started_, true));

    ResourcesAcquiredTime_ = TInstant::Now();

    YT_LOG_INFO("Starting job");

    SetJobState(EJobState::Running);

    GetUserSlot()->SetAllocationId(GetAllocationId());

    TFuture<std::vector<TNameWithAddress>> resolveFuture;

    if (UserJobSpec_ && UserJobSpec_->has_network_project_id()) {
        std::vector<TFuture<TNameWithAddress>> nodeAddressFutures;

        auto addresses = Bootstrap_->GetConfig()->Addresses;
        ResolvedNodeAddresses_.reserve(std::size(addresses));

        auto* resolver = TAddressResolver::Get();

        for (auto& [addressName, address] : addresses) {
            nodeAddressFutures.push_back(
                resolver->Resolve(address)
                    .Apply(BIND(
                        [
                            this,
                            this_ = MakeStrong(this),
                            address,
                            addressName = std::move(addressName)
                        ] (const TErrorOr<TNetworkAddress>& resolvedAddressOrError) mutable {
                            if (!resolvedAddressOrError.IsOK()) {
                                YT_LOG_WARNING(
                                    resolvedAddressOrError,
                                    "Failed to resolve node address (AddressName: %v, Address: %v)",
                                    addressName,
                                    address);
                                THROW_ERROR resolvedAddressOrError;
                            }

                            const auto& resolvedAddress = resolvedAddressOrError.Value();
                            YT_VERIFY(resolvedAddress.IsIP6());

                            return TNameWithAddress{
                                .Name = std::move(addressName),
                                .Address = resolvedAddress.ToIP6Address(),
                            };
                        })));
        }

        resolveFuture = AllSucceeded(std::move(nodeAddressFutures));
    } else {
        resolveFuture = MakeFuture(std::vector<TNameWithAddress>());
    }

    resolveFuture.SubscribeUnique(
        BIND(&TJob::DoStart, MakeStrong(this))
            .Via(Bootstrap_->GetJobInvoker()));
}

void TJob::Abort(TError error, bool graceful)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    YT_LOG_INFO(
        error,
        "Job abort requested (Phase: %v, State: %v)",
        JobPhase_,
        JobState_);

    if (graceful) {
        RequestGracefulAbort(std::move(error));
        return;
    }

    Terminate(EJobState::Aborted, std::move(error));
}

void TJob::OnJobProxySpawned()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    GuardedAction(
        "OnJobProxySpawned",
        [&] {
            YT_LOG_INFO("Job proxy spawned");

            ValidateJobPhase(EJobPhase::SpawningJobProxy);
            SetJobPhase(EJobPhase::PreparingArtifacts);

            if (!Bootstrap_->GetJobController()->IsJobProxyProfilingDisabled() && UserJobSpec_ && UserJobSpec_->monitoring_config().enable()) {
                Bootstrap_->GetJobProxySolomonExporter()->AttachRemoteProcess(BIND(&TJob::DumpSensors, MakeStrong(this)));
            }
        });
}

void TJob::PrepareArtifact(
    const TString& artifactName,
    const TString& pipePath)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    GuardedAction(
        "PrepareArtifact",
        [&] {
            YT_LOG_DEBUG(
                "Prepare job artifact (ArtifactName: %v, PipePath: %v)",
                artifactName,
                pipePath);

            // NB: Open pipe for writing before reply.
            auto pipeFd = HandleEintr(::open, pipePath.c_str(), O_WRONLY | O_NONBLOCK | O_CLOEXEC);
            TFile pipe(pipeFd);

            auto fcntlResult = HandleEintr(::fcntl, pipeFd, F_SETFL, O_WRONLY | O_CLOEXEC);
            if (fcntlResult < 0) {
                THROW_ERROR_EXCEPTION("Failed to disable O_NONBLOCK for artifact pipe")
                    << TError::FromSystem();
            }

            ValidateJobPhase(EJobPhase::PreparingArtifacts);

            int artifactIndex = GetOrCrash(UserArtifactNameToIndex_, artifactName);
            const auto& artifact = Artifacts_[artifactIndex];

            YT_VERIFY(artifact.BypassArtifactCache || artifact.CopyFile);

            auto traceContext = CreateTraceContextFromCurrent("ArtifactPrepare");
            TTraceContextGuard guard(traceContext);
            auto baggage = traceContext->UnpackOrCreateBaggage();
            const char* jobIOKind = artifact.BypassArtifactCache ? "artifact_bypass_cache" : "artifact_copy";
            AddTagToBaggage(baggage, EAggregateIOTag::JobIoKind, jobIOKind);
            AddTagsFromDataSource(baggage, FromProto<NChunkClient::TDataSource>(artifact.Key.data_source()));
            traceContext->PackBaggage(std::move(baggage));

            if (artifact.BypassArtifactCache) {
                YT_LOG_INFO(
                    "Download artifact with cache bypass (FileName: %v, Executable: %v, SandboxKind: %v, CompressedDataSize: %v)",
                    artifact.Name,
                    artifact.Executable,
                    artifact.SandboxKind,
                    artifact.Key.GetCompressedDataSize());

                const auto& chunkCache = Bootstrap_->GetChunkCache();
                auto downloadOptions = MakeArtifactDownloadOptions();
                auto producer = chunkCache->MakeArtifactDownloadProducer(artifact.Key, downloadOptions);

                ArtifactPrepareFutures_.push_back(
                    GetUserSlot()->MakeFile(
                        Id_,
                        artifact.Name,
                        artifact.SandboxKind,
                        producer,
                        pipe));
            } else if (artifact.CopyFile) {
                YT_VERIFY(artifact.Chunk);

                YT_LOG_INFO(
                    "Copy artifact (FileName: %v, Executable: %v, SandboxKind: %v, CompressedDataSize: %v)",
                    artifact.Name,
                    artifact.Executable,
                    artifact.SandboxKind,
                    artifact.Key.GetCompressedDataSize());

                ArtifactPrepareFutures_.push_back(
                    GetUserSlot()->MakeCopy(
                        Id_,
                        artifact.Name,
                        artifact.SandboxKind,
                        artifact.Chunk->GetFileName(),
                        pipe,
                        artifact.Chunk->GetLocation()));
            }
        });
}

void TJob::OnArtifactPreparationFailed(
    const TString& artifactName,
    const TString& artifactPath,
    const TError& error)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    Y_UNUSED(artifactName);

    GuardedAction(
        "OnArtifactPreparationFailed",
        [&] {
            ValidateJobPhase(EJobPhase::PreparingArtifacts);

            GetUserSlot()->OnArtifactPreparationFailed(
                Id_,
                artifactName,
                ESandboxKind::User,
                artifactPath,
                error);
    });
}

void TJob::OnArtifactsPrepared()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    // Wait for possible errors during node-side artifact preparation.
    WaitFor(AllSucceeded(ArtifactPrepareFutures_))
        .ThrowOnError();

    GuardedAction(
        "OnArtifactsPrepared",
        [&] {
            YT_LOG_INFO("Artifacts prepared");

            ValidateJobPhase(EJobPhase::PreparingArtifacts);
            SetJobPhase(EJobPhase::PreparingJob);
        });
}

void TJob::OnJobPrepared()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    GuardedAction(
        "OnJobPrepared",
        [&] {
            JobPrepared_.Fire(MakeStrong(this));

            YT_LOG_INFO("Job prepared");

            ValidateJobPhase(EJobPhase::PreparingJob);
            SetJobPhase(EJobPhase::Running);
        });
}

void TJob::Terminate(EJobState finalState, TError error)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    auto doTerminate = [&] {
        auto timeout = CommonConfig_->WaitingForJobCleanupTimeout;

        SetJobPhase(EJobPhase::WaitingForCleanup);
        Finalize(finalState, std::move(error));

        YT_LOG_INFO("Waiting for job cleanup (Timeout: %v)", timeout);
        TDelayedExecutor::Submit(
            BIND(&TJob::OnWaitingForCleanupTimeout, MakeStrong(this))
                .Via(Invoker_),
            timeout);

        ArtifactsFuture_.Cancel(TError("Job terminated"));
        WorkspaceBuildingFuture_.Cancel(TError("Job preparation canceled"));

        if (const auto& slot = GetUserSlot()) {
            slot->CancelPreparation();
        }
    };

    switch (JobPhase_) {
        case EJobPhase::Created:
            doTerminate();
            // NB(arkady-e1ppa): We can have
            // job controller methods forbidding
            // context switch in the callstack.
            // Thus we do cleanup asynchronously.
            GetInvoker()
                ->Invoke(BIND([this, this_ = MakeStrong(this)] {
                    // NB(arkady-e1ppa): We don't do plain cleanup
                    // here because due to async nature of this call
                    // some could beat us to it.
                    HandleFinishingPhase();
                }));

            break;

        case EJobPhase::PreparingNodeDirectory:
        case EJobPhase::DownloadingArtifacts:
        case EJobPhase::PreparingRootVolume:
        case EJobPhase::PreparingSandboxDirectories:
        case EJobPhase::RunningSetupCommands:
        case EJobPhase::RunningGpuCheckCommand:
        case EJobPhase::SpawningJobProxy:
        case EJobPhase::PreparingArtifacts:
        case EJobPhase::PreparingJob:
        case EJobPhase::Running:
        case EJobPhase::RunningExtraGpuCheckCommand:
            doTerminate();
            YT_UNUSED_FUTURE(StopJobProxy());
            break;

        case EJobPhase::FinalizingJobProxy:
            YT_LOG_INFO(
                "Cannot terminate job (JobState: %v, JobPhase: %v, JobError: %v)",
                JobState_,
                JobPhase_,
                Error_);
            break;

        default:
            YT_LOG_INFO(
                "Cannot terminate job (JobState: %v, JobPhase: %v)",
                JobState_,
                JobPhase_);

            YT_VERIFY(IsFinished());
            break;
    }
}

void TJob::Finalize(TError error)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    TForbidContextSwitchGuard guard;

    Finalize(
        /*finalJobState*/ std::nullopt,
        std::move(error),
        /*jobResultExtension*/ std::nullopt,
        /*byJobProxyCompletion*/ false);

    YT_VERIFY(Error_);
    auto& currentError = *Error_;

    // NB: We should disable slot here to give scheduler information about job failure.
    if (currentError.FindMatching(NExecNode::EErrorCode::GpuCheckCommandFailed) &&
        !currentError.FindMatching(NExecNode::EErrorCode::GpuCheckCommandIncorrect))
    {
        Bootstrap_->GetSlotManager()->OnGpuCheckCommandFailed(currentError);
    }
}

bool TJob::Finalize(
    std::optional<EJobState> finalJobState,
    TError error,
    std::optional<NControllerAgent::NProto::TJobResultExt> jobResultExtension,
    bool byJobProxyCompletion)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    TForbidContextSwitchGuard guard;

    if (IsFinished()) {
        YT_LOG_DEBUG("Job already finalized");
        return false;
    }

    YT_LOG_INFO("Finalizing job (FinalState: %v)", finalJobState);

    DoSetResult(std::move(error), std::move(jobResultExtension), byJobProxyCompletion);

    YT_VERIFY(Error_);
    auto& currentError = *Error_;

    if (!finalJobState) {
        DeduceAndSetFinishedJobState();
    } else {
        if (*finalJobState == EJobState::Aborted) {
            if (auto deducedAbortReason = DeduceAbortReason()) {
                currentError <<= TErrorAttribute("abort_reason", deducedAbortReason);

                YT_LOG_DEBUG(
                    "Deduced abort reason set to error (AbortReason: %v, Error: %v)",
                    deducedAbortReason,
                    currentError);
            }
        }

        SetJobState(*finalJobState);
    }

    OnJobFinalized();

    return true;
}

void TJob::Finalize(EJobState finalState, TError error)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    YT_VERIFY(finalState == EJobState::Aborted || finalState == EJobState::Failed);

    Finalize(
        finalState,
        std::move(error),
        /*jobResultExtension*/ std::nullopt,
        /*byJobProxyCompletion*/ false);
}

void TJob::OnJobFinalized()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    YT_VERIFY(Error_);
    auto& currentError = *Error_;

    YT_LOG_INFO(
        currentError,
        "Job finalized (JobState: %v, ResourceUsage: %v)",
        GetState(),
        GetResourceUsage());

    YT_VERIFY(IsFinished());

    FinishTime_ = TInstant::Now();
    // Copy info from traffic meter to statistics.
    auto statistics = ConvertTo<TStatistics>(StatisticsYson_);
    FillTrafficStatistics("exec_agent"_L, statistics, TrafficMeter_);
    StatisticsYson_ = ConvertToYsonString(statistics);

    // NB(eshcherbin): We need to destroy this producer, otherwise it will continue
    // to send metrics for some time after the job is finished.
    UserJobSensorProducer_.Reset();

    JobFinished_.Fire(MakeStrong(this));

    if (!currentError.IsOK()) {
        // NB: It is required to report error that occurred in some place different
        // from OnJobFinished method.
        HandleJobReport(MakeDefaultJobReport().Error(currentError));
    }
}

void TJob::DeduceAndSetFinishedJobState()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    YT_VERIFY(Error_);
    auto& currentError = *Error_;

    if (currentError.IsOK()) {
        SetJobState(EJobState::Completed);
    } else if (IsFatalError(currentError)) {
        currentError <<= TErrorAttribute("fatal", true);
        SetJobState(EJobState::Failed);
    } else {
        auto deducedAbortReason = DeduceAbortReason();
        if (deducedAbortReason) {
            currentError <<= TErrorAttribute("abort_reason", deducedAbortReason);
            SetJobState(EJobState::Aborted);
        } else {
            SetJobState(EJobState::Failed);
        }
    }
}

void TJob::OnResultReceived(TJobResult jobResult)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    GuardedAction(
        "OnResultReceived",
        [&] {
            ResultReceivedTime_ = TInstant::Now();

            SetJobPhase(EJobPhase::FinalizingJobProxy);

            if (jobResult.has_exit_code()) {
                ExitCode_.emplace(jobResult.exit_code());
            }

            std::optional<NControllerAgent::NProto::TJobResultExt> jobResultExtension;
            if (jobResult.HasExtension(NControllerAgent::NProto::TJobResultExt::job_result_ext)) {
                jobResultExtension = std::move(
                    *jobResult.ReleaseExtension(NControllerAgent::NProto::TJobResultExt::job_result_ext));
            }

            if (auto error = FromProto<TError>(jobResult.error());
                error.IsOK() || !NeedsGpuCheck())
            {
                Finalize(
                    /*finalJobState*/ std::nullopt,
                    std::move(error),
                    std::move(jobResultExtension),
                    /*byJobProxyCompletion*/ true);
            } else {
                DoSetResult(
                    std::move(error),
                    std::move(jobResultExtension),
                    /*receivedFromJobProxy*/ true);
            }
        });
}

TJobId TJob::GetId() const noexcept
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return Id_;
}

TAllocationId TJob::GetAllocationId() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return AllocationIdFromJobId(Id_);
}

TOperationId TJob::GetOperationId() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return OperationId_;
}

IInvokerPtr TJob::GetInvoker() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return Invoker_;
}

const TControllerAgentDescriptor& TJob::GetControllerAgentDescriptor() const
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return ControllerAgentDescriptor_;
}

void TJob::UpdateControllerAgentDescriptor(TControllerAgentDescriptor descriptor)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    if (ControllerAgentDescriptor_ == descriptor) {
        return;
    }

    YT_LOG_DEBUG(
        "Update controller agent (ControllerAgentAddress: %v -> %v, ControllerAgentIncarnationId: %v)",
        ControllerAgentDescriptor_.Address,
        descriptor.Address,
        descriptor.IncarnationId);

    ControllerAgentDescriptor_ = std::move(descriptor);

    ControllerAgentConnector_ = Bootstrap_
            ->GetControllerAgentConnectorPool()
            ->GetControllerAgentConnector(ControllerAgentDescriptor_);
}

EJobType TJob::GetType() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return JobType_;
}

TJobSpec TJob::GetSpec() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return GuardedJobSpec_.Load();
}

EJobState TJob::GetState() const
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return JobState_;
}

TInstant TJob::GetCreationTime() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return CreationTime_;
}

NJobAgent::TTimeStatistics TJob::GetTimeStatistics() const
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    auto getWaitForResourcesDuration = [&] () -> std::optional<TDuration> {
        if (!ResourcesAcquiredTime_) {
            return TInstant::Now() - CreationTime_;
        }

        return *ResourcesAcquiredTime_ - CreationTime_;
    };

    auto getPrepareDuration = [&] () -> std::optional<TDuration> {
        // TODO(arkady-e1ppa): Fix PrepareStartTime semantics.
        if (!StartTime_) {
            return std::nullopt;
        }
        if (!PreparationStartTime_) {
            return std::nullopt;
        } else if (!ExecStartTime_) {
            return TInstant::Now() - *PreparationStartTime_;
        } else {
            return *ExecStartTime_ - *PreparationStartTime_;
        }
    };
    auto getPrepareRootFSDuration = [&] () -> std::optional<TDuration> {
        if (!StartPrepareVolumeTime_) {
            return std::nullopt;
        } else if (!FinishPrepareVolumeTime_) {
            return TInstant::Now() - *StartPrepareVolumeTime_;
        } else {
            return *FinishPrepareVolumeTime_ - *StartPrepareVolumeTime_;
        }
    };
    auto getArtifactsDownloadDuration = [&] () -> std::optional<TDuration> {
        if (!PreparationStartTime_) {
            return std::nullopt;
        } else if (!CopyFinishTime_) {
            return TInstant::Now() - *PreparationStartTime_;
        } else {
            return *CopyFinishTime_ - *PreparationStartTime_;
        }
    };
    auto getExecDuration = [&] () -> std::optional<TDuration> {
        if (!ExecStartTime_) {
            return std::nullopt;
        } else if (!FinishTime_) {
            return TInstant::Now() - *ExecStartTime_;
        } else {
            return *FinishTime_ - *ExecStartTime_;
        }
    };
    auto getPreliminaryGpuCheckDuration = [&] () -> std::optional<TDuration> {
        if (!PreliminaryGpuCheckStartTime_) {
            return std::nullopt;
        } else if (!PreliminaryGpuCheckFinishTime_) {
            return TInstant::Now() - *PreliminaryGpuCheckStartTime_;
        } else {
            return *PreliminaryGpuCheckFinishTime_ - *PreliminaryGpuCheckStartTime_;
        }
    };
    auto getExtraGpuCheckDuration = [&] () -> std::optional<TDuration> {
        if (!ExtraGpuCheckStartTime_) {
            return std::nullopt;
        } else if (!ExtraGpuCheckFinishTime_) {
            return TInstant::Now() - *ExtraGpuCheckStartTime_;
        } else {
            return *ExtraGpuCheckFinishTime_ - *ExtraGpuCheckStartTime_;
        }
    };
    auto sumOptionals = [&] (std::optional<TDuration> lhs, std::optional<TDuration> rhs) -> std::optional<TDuration> {
        if (!lhs && !rhs) {
            return std::nullopt;
        } else if (lhs && !rhs) {
            return lhs;
        } else if (rhs && !lhs) {
            return rhs;
        } else {
            return *lhs + *rhs;
        }
    };

    return {
        .WaitingForResourcesDuration = getWaitForResourcesDuration(),
        .PrepareDuration = getPrepareDuration(),
        .ArtifactsDownloadDuration = getArtifactsDownloadDuration(),
        .PrepareRootFSDuration = getPrepareRootFSDuration(),
        .ExecDuration = getExecDuration(),
        .GpuCheckDuration = sumOptionals(getPreliminaryGpuCheckDuration(), getExtraGpuCheckDuration())
    };
}

std::optional<TInstant> TJob::GetStartTime() const
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return StartTime_;
}

EJobPhase TJob::GetPhase() const
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return JobPhase_;
}

int TJob::GetSlotIndex() const
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    auto slot = GetUserSlot();
    if (!slot) {
        return -1;
    }

    return slot->GetSlotIndex();
}

NClusterNode::TJobResources TJob::GetResourceUsage() const
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    if (!ResourceHolder_) {
        THROW_ERROR_EXCEPTION("Job has already released resources");
    }

    return ResourceHolder_->GetResourceUsage();
}

bool TJob::IsGpuRequested() const
{
    return IsGpuRequested_;
}

const std::vector<int>& TJob::GetPorts() const
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return ResourceHolder_->GetPorts();
}

const TError& TJob::GetJobError() const
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    YT_VERIFY(Error_);

    return *Error_;
}

TJobResult TJob::GetResult() const
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    YT_VERIFY(Error_);

    TJobResult result;
    ToProto(result.mutable_error(), *Error_);

    if (ExitCode_.has_value()) {
        result.set_exit_code(*ExitCode_);
    }

    if (JobResultExtension_) {
        *result.MutableExtension(
            NControllerAgent::NProto::TJobResultExt::job_result_ext) = *JobResultExtension_;
    }

    return result;
}

double TJob::GetProgress() const
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return Progress_;
}

void TJob::SetResourceUsage(const NClusterNode::TJobResources& newUsage)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    if (JobPhase_ == EJobPhase::Running) {
        ResourceHolder_->SetBaseResourceUsage(newUsage);
    }
}

void TJob::SetProgress(double progress)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    if (JobPhase_ == EJobPhase::Running) {
        Progress_ = progress;
    }
}

i64 TJob::GetStderrSize() const
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return StderrSize_;
}

void TJob::SetStderrSize(i64 value)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    if (StderrSize_ != value) {
        StderrSize_ = value;
        HandleJobReport(MakeDefaultJobReport()
            .StderrSize(StderrSize_));
    }
}

void TJob::SetStderr(const TString& value)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    Stderr_ = value;
}

void TJob::SetFailContext(const TString& value)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    FailContext_ = value;
}

void TJob::SetHasJobTrace(bool hasJobTrace)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    if (hasJobTrace == HasJobTrace_) {
        return;
    }

    HasJobTrace_ = hasJobTrace;

    HandleJobReport(MakeDefaultJobReport()
        .ArchiveFeatures(BuildArchiveFeatures()));
}

void TJob::AddProfile(TJobProfile value)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    Profiles_.push_back(std::move(value));
}

void TJob::SetCoreInfos(TCoreInfos value)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    CoreInfos_ = std::move(value);
}

const TChunkCacheStatistics& TJob::GetChunkCacheStatistics() const
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return ChunkCacheStatistics_;
}

TYsonString TJob::GetStatistics() const
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return StatisticsYson_;
}

TDataStatistics TJob::GetTotalInputDataStatistics() const
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return TotalInputDataStatistics_;
}

std::vector<TDataStatistics> TJob::GetOutputDataStatistics() const
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return OutputDataStatistics_;
}

TInstant TJob::GetStatisticsLastSendTime() const
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return StatisticsLastSendTime_;
}

void TJob::ResetStatisticsLastSendTime()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    StatisticsLastSendTime_ = TInstant::Now();
}

void TJob::SetStatistics(const TYsonString& statisticsYson)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    if (JobPhase_ != EJobPhase::Running && JobPhase_ != EJobPhase::FinalizingJobProxy) {
        return;
    }

    auto statistics = ConvertTo<TStatistics>(statisticsYson);
    GetTimeStatistics().AddSamplesTo(&statistics);

    if (auto gpuSlots = GetGpuSlots(); !std::empty(gpuSlots)) {
        EnrichStatisticsWithGpuInfo(&statistics, gpuSlots);

        if (IsFullHostGpuJob()) {
            EnrichStatisticsWithRdmaDeviceInfo(&statistics);
        }
    }

    EnrichStatisticsWithDiskInfo(&statistics);

    EnrichStatisticsWithArtifactsInfo(&statistics);

    UpdateIOStatistics(statistics);

    StatisticsYson_ = ConvertToYsonString(statistics);

    HandleJobReport(MakeDefaultJobReport()
        .Statistics(StatisticsYson_));

    UpdateUserJobMonitoring();
}

void TJob::UpdateUserJobMonitoring()
{
    if (!UserJobSensorProducer_) {
        return;
    }

    TSensorBuffer userJobSensors;
    CollectSensorsFromStatistics(&userJobSensors);
    CollectSensorsFromGpuAndRdmaDeviceInfo(&userJobSensors);
    UserJobSensorProducer_->Update(std::move(userJobSensors));
}

void TJob::SetTotalInputDataStatistics(TDataStatistics datastatistics)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    TotalInputDataStatistics_ = std::move(datastatistics);
}

void TJob::SetOutputDataStatistics(std::vector<TDataStatistics> dataStatistics)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    OutputDataStatistics_ = std::move(dataStatistics);
}

TBriefJobInfo TJob::GetBriefInfo() const
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    NClusterNode::TJobResources baseResourceUsage{};
    NClusterNode::TJobResources additionalResourceUsage{};
    std::vector<int> jobPorts;

    if (ResourceHolder_) {
        std::tie(baseResourceUsage, additionalResourceUsage) = ResourceHolder_->GetDetailedResourceUsage();
        jobPorts = GetPorts();
    }

    auto tryGetMonitoringDescriptor = [&] () -> std::optional<std::string> {
        const auto& monitoringConfig = UserJobSpec_->monitoring_config();
        if (!monitoringConfig.enable()) {
            return std::nullopt;
        }

        YT_VERIFY(monitoringConfig.has_job_descriptor());
        return monitoringConfig.job_descriptor();
    };

    return TBriefJobInfo(
        GetId(),
        GetState(),
        GetPhase(),
        GetType(),
        GetStored(),
        IsInterrupted(),
        GetSlotIndex(),
        GetCreationTime(),
        /*jobDuration*/ TInstant::Now() - GetCreationTime(),
        GetStatistics(),
        GetOperationId(),
        baseResourceUsage,
        additionalResourceUsage,
        InitialResourceDemand_,
        std::move(jobPorts),
        JobEvents_,
        CoreInfos_,
        ExecAttributes_,
        tryGetMonitoringDescriptor());
}

NYTree::IYPathServicePtr TJob::CreateStaticOrchidService()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    auto producer = BIND_NO_PROPAGATE([this, this_ = MakeStrong(this)] (IYsonConsumer* consumer) {
        auto jobInfoOrError = WaitFor(BIND_NO_PROPAGATE(
            &TJob::GetBriefInfo,
            MakeStrong(this))
                .AsyncVia(Invoker_)
                .Run());

        YT_LOG_FATAL_UNLESS(
            jobInfoOrError.IsOK(),
            jobInfoOrError,
            "Failed to get brief job info for static orchid");

        BuildYsonFluently(consumer).BeginMap()
            .Do(std::bind(
                &TBriefJobInfo::BuildOrchid,
                std::move(jobInfoOrError).Value(),
                std::placeholders::_1))
        .EndMap();
    });

    return NYTree::IYPathService::FromProducer(std::move(producer));
}

NYTree::IYPathServicePtr TJob::CreateJobProxyOrchidService()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    try {
        ValidateJobRunning();

        if (!JobProxyChannel_) {
            auto client = CreateBusClient(GetUserSlot()->GetBusClientConfig());
            JobProxyChannel_ = NRpc::NBus::CreateBusChannel(std::move(client));
        }
    } catch (const std::exception& ex) {
        YT_LOG_DEBUG(ex, "Failed to create job proxy orchid service");
        return nullptr;
    }

    return NOrchid::CreateOrchidYPathService({
        .Channel = JobProxyChannel_,
        .RemoteRoot = "//job_proxy",
    });
}

NYTree::IYPathServicePtr TJob::CreateDynamicOrchidService()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return New<NYTree::TCompositeMapService>()
        ->AddChild("job_proxy", CreateJobProxyOrchidService());
}

IYPathServicePtr TJob::GetOrchidService()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return New<TServiceCombiner>(
        std::vector{
            CreateStaticOrchidService(),
            CreateDynamicOrchidService()
        });
}

std::vector<TChunkId> TJob::DumpInputContext(TTransactionId transactionId)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);
    ValidateJobRunning();

    try {
        return GetJobProbeOrThrow()->DumpInputContext(transactionId);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error requesting input contexts dump from job proxy")
            << ex;
    }
}

std::optional<TGetJobStderrResponse> TJob::GetStderr(const TGetJobStderrOptions& options)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    if (Stderr_) {
        return TGetJobStderrResponse::MakeJobStderr(TSharedRef::FromString(*Stderr_), options);
    }

    if (!UserJobSpec_) {
        return std::nullopt;
    }

    if (JobPhase_ == EJobPhase::Running) {
        try {
            return GetJobProbeOrThrow()->GetStderr(options);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error requesting stderr from job proxy")
                << ex;
        }
    }

    if (JobPhase_ < EJobPhase::Running ||
        JobState_ == EJobState::Aborted ||
        JobState_ == EJobState::Failed)
    {
        return std::nullopt;
    }

    // Cleanup is not atomic, so in case of job proxy failure we might see job in cleanup phase and running state.
    if (JobPhase_ == EJobPhase::Cleanup) {
        YT_VERIFY(Error_);
        if (Error_->FindMatching(NExecNode::EErrorCode::JobProxyFailed) ||
            Error_->FindMatching(NExecNode::EErrorCode::JobProxyPreparationTimeout))
        {
            return std::nullopt;
        }
    }

    // When job proxy finished with completed or failed state, Stderr_ must not be unset.
    YT_LOG_ALERT(
        "Stderr is unset for job (JobState: %v, JobPhase: %v)",
        JobState_,
        JobPhase_);

    return std::nullopt;
}

std::optional<TString> TJob::GetFailContext()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return FailContext_;
}

const TCoreInfos& TJob::GetCoreInfos()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return CoreInfos_;
}

TPollJobShellResponse TJob::PollJobShell(
    const TJobShellDescriptor& jobShellDescriptor,
    const TYsonString& parameters)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    try {
        return GetJobProbeOrThrow()->PollJobShell(jobShellDescriptor, parameters);
    } catch (const TErrorException& ex) {
        // The following code changes error code for more user-friendly
        // diagnostics in interactive shell.
        if (ex.Error().FindMatching(NRpc::EErrorCode::TransportError)) {
            THROW_ERROR_EXCEPTION(NExecNode::EErrorCode::JobProxyConnectionFailed,
                "No connection to job proxy")
                << ex;
        }
        THROW_ERROR_EXCEPTION("Error polling job shell")
            << ex;
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error polling job shell")
            << ex;
    }
}

void TJob::HandleJobReport(TNodeJobReport&& jobReport)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    Bootstrap_->GetJobReporter()->HandleJobReport(
        jobReport
            .OperationId(GetOperationId())
            .JobId(GetId())
            .Address(Bootstrap_->GetLocalDescriptor().GetDefaultAddress()));
}

void TJob::ReportSpec()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    HandleJobReport(MakeDefaultJobReport()
        .Spec(JobSpec_));
}

void TJob::ReportStderr()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    auto maybeStderr = GetStderr({});
    if (!maybeStderr) {
        return;
    }
    HandleJobReport(TNodeJobReport()
        .Stderr({maybeStderr->Data.data(), maybeStderr->Data.size()}));
}

void TJob::ReportFailContext()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    if (auto failContext = GetFailContext()) {
        HandleJobReport(TNodeJobReport()
            .FailContext(*failContext));
    }
}

void TJob::ReportProfile()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    for (const auto& profile : Profiles_) {
        HandleJobReport(TNodeJobReport()
            .Profile(std::move(profile)));
    }
}

void TJob::DoInterrupt(
    TDuration timeout,
    EInterruptionReason interruptionReason,
    std::optional<TString> preemptionReason,
    const std::optional<NScheduler::TPreemptedFor>& preemptedFor)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    YT_VERIFY(interruptionReason != EInterruptionReason::None);

    auto now = TInstant::Now();
    if (InterruptionDeadline_ && InterruptionDeadline_ < now + timeout) {
        YT_LOG_DEBUG(
            "Job interruption with earlier deadline is already requested, ignore (InterruptionReason: %v, PreemptedFor: %v, CurrentError: %v, CurrentDeadline: %v)",
            InterruptionReason_,
            PreemptedFor_,
            Error_,
            InterruptionDeadline_);
        return;
    }

    YT_LOG_DEBUG(
        "Job interruption requested (Timeout: %v, InterruptionReason: %v, Preempted: %v, PreemptionReason: %v, PreemptedFor: %v)",
        timeout,
        interruptionReason,
        preemptionReason.has_value(),
        preemptionReason,
        preemptedFor);

    if (JobPhase_ > EJobPhase::Running) {
        // We're done with this job, no need to interrupt.
        YT_LOG_DEBUG("Job is already not running, do nothing (JobPhase: %v)", JobPhase_);
        return;
    }

    InterruptionReason_ = interruptionReason;
    PreemptedFor_ = preemptedFor;

    if (!IsInterruptible()) {
        YT_LOG_DEBUG("Job is not interruptible and will be aborted");

        auto error = TError(NJobProxy::EErrorCode::InterruptionUnsupported, "Uninterruptible job aborted")
            << TErrorAttribute("interruption_reason", InterruptionReason_)
            << TErrorAttribute("abort_reason", EAbortReason::InterruptionUnsupported);

        if (interruptionReason == EInterruptionReason::Preemption) {
            error = TError("Job preempted") << error;
            error = error
                << TErrorAttribute("preemption_reason", preemptionReason)
                << TErrorAttribute("abort_reason", EAbortReason::Preemption);
        }

        if (interruptionReason == EInterruptionReason::JobsDisabledOnNode) {
            error = TError("Jobs disabled on node")
                << error;
        }

        ReportJobInterruptionInfo(now, timeout, interruptionReason, preemptionReason, preemptedFor);

        Abort(error);
        return;
    }

    if (JobPhase_ < EJobPhase::Running) {
        auto error = TError(NJobProxy::EErrorCode::InterruptionFailed, "Interrupting job that has not started yet")
            << TErrorAttribute("interruption_reason", InterruptionReason_);

        if (interruptionReason == EInterruptionReason::Preemption) {
            error = TError("Job preempted") << error;
            error = error
                << TErrorAttribute("preemption_reason", preemptionReason)
                << TErrorAttribute("abort_reason", EAbortReason::Preemption);
        }

        ReportJobInterruptionInfo(now, timeout, interruptionReason, preemptionReason, preemptedFor);

        Abort(error);
        return;
    }

    try {
        if (!InterruptionRequested_) {
            AddJobEvent(interruptionReason);
            GetJobProbeOrThrow()->Interrupt();
        }

        InterruptionRequested_ = true;

        if (timeout) {
            TDelayedExecutor::CancelAndClear(InterruptionTimeoutCookie_);
            InterruptionTimeoutCookie_ = TDelayedExecutor::Submit(
                BIND(
                    &TJob::OnJobInterruptionTimeout,
                    MakeWeak(this),
                    InterruptionReason_,
                    preemptionReason),
                timeout,
                Bootstrap_->GetJobInvoker());
            InterruptionDeadline_ = now + timeout;
        }

        ReportJobInterruptionInfo(now, timeout, interruptionReason, preemptionReason, preemptedFor);
    } catch (const std::exception& ex) {
        auto error = TError("Error interrupting job on job proxy")
            << TErrorAttribute("interruption_reason", InterruptionReason_)
            << ex;

        if (error.FindMatching(NJobProxy::EErrorCode::InterruptionFailed)) {
            Abort(error);
        } else {
            THROW_ERROR error;
        }
    }
}

void TJob::Fail(TError error)
{
    YT_LOG_INFO("Fail job (Error: %v)", error);

    try {
        DoFail(std::move(error));
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Error failing job");
    }
}

void TJob::DoFail(TError error)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    if (JobPhase_ != EJobPhase::Running) {
        error = TError("Failing job that is not running") << std::move(error);

        Terminate(EJobState::Failed, std::move(error));

        return;
    }

    try {
        GetJobProbeOrThrow()->Fail(error);
    } catch (const std::exception& ex) {
        auto abortionError = TError("Error failing job on job proxy")
            << ex
            << std::move(error);

        Abort(std::move(abortionError));
    }
}

void TJob::RequestGracefulAbort(TError error)
{
    YT_LOG_INFO("Requesting job graceful abort (Error: %v)", error);
    if (GracefulAbortRequested_) {
        YT_LOG_INFO("Repeating job graceful abort request; ignored");
        return;
    }

    try {
        DoRequestGracefulAbort(std::move(error));
        GracefulAbortRequested_ = true;
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Failed to request job graceful abort");
    }
}

void TJob::DoRequestGracefulAbort(TError error)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    if (JobPhase_ != EJobPhase::Running) {
        Terminate(EJobState::Aborted, std::move(error));
        return;
    }

    try {
        GetJobProbeOrThrow()->GracefulAbort(error);
    } catch (const std::exception& ex) {
        auto abortionError = TError("Error failing job on job proxy")
            << ex;
        abortionError <<= std::move(error);
        Abort(std::move(abortionError));
    }
}

bool TJob::GetStored() const
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return Stored_;
}

void TJob::SetStored()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    YT_LOG_DEBUG("Requested to store job");

    Stored_ = true;
    LastStoredTime_ = TInstant::Now();
    StoredEvent_.TrySet();
}

bool TJob::IsGrowingStale(TDuration maxDelay) const
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    YT_VERIFY(Stored_);

    return LastStoredTime_ + maxDelay <= TInstant::Now();
}

TFuture<void> TJob::GetStoredEvent() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return StoredEvent_;
}

void TJob::OnEvictedFromAllocation() noexcept
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    Allocation_.Reset();
    PrepareResourcesRelease();
}

void TJob::PrepareResourcesRelease() noexcept
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    // NB(arkady-e1ppa): In some cases job can
    // be finished (and also enter cleanup phase)
    // before it is evicted from the allocation.
    // In such a case ResourceHolder_ would be
    // already reset.
    if (ResourceHolder_) {
        ResourceHolder_->PrepareResourcesRelease();
    }
}

bool TJob::IsJobProxyCompleted() const noexcept
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return JobProxyCompleted_;
}

bool TJob::IsInterruptible() const noexcept
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return Interruptible_;
}

void TJob::OnJobInterruptionTimeout(
    EInterruptionReason interruptionReason,
    const std::optional<TString>& preemptionReason)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    auto error = TError(NJobProxy::EErrorCode::InterruptionTimeout, "Interruption is timed out")
        << TErrorAttribute("interruption_reason", InterruptionReason_)
        << TErrorAttribute("abort_reason", EAbortReason::InterruptionTimeout);

    if (interruptionReason == EInterruptionReason::Preemption) {
        error = TError("Job preempted") << error;
        error = error
            << TErrorAttribute("preemption_reason", preemptionReason)
            << TErrorAttribute("abort_reason", EAbortReason::Preemption);
    }

    Abort(std::move(error));
}

TControllerAgentConnectorPool::TControllerAgentConnectorPtr TJob::GetControllerAgentConnector() const noexcept
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return ControllerAgentConnector_.Lock();
}

void TJob::Interrupt(
    TDuration timeout,
    EInterruptionReason interruptionReason,
    std::optional<TString> preemptionReason,
    const std::optional<NScheduler::TPreemptedFor>& preemptedFor)
{
    YT_LOG_INFO(
        "Interrupting job (InterruptionReason: %v, PreemptionReason: %v, PreemptedFor: %v, Timeout: %v)",
        interruptionReason,
        preemptionReason,
        preemptedFor,
        timeout);

    try {
        DoInterrupt(timeout, interruptionReason, preemptionReason, preemptedFor);
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Failed to interrupt job");
    }
}

NScheduler::EInterruptionReason TJob::GetInterruptionReason() const noexcept
{
    return InterruptionReason_;
}

bool TJob::IsInterrupted() const noexcept
{
    return InterruptionReason_ != EInterruptionReason::None;
}

const std::optional<NScheduler::TPreemptedFor>& TJob::GetPreemptedFor() const noexcept
{
    return PreemptedFor_;
}

bool TJob::IsFinished() const noexcept
{
    switch (JobState_) {
        case EJobState::Aborted:
        case EJobState::Completed:
        case EJobState::Failed:
            return true;
        default:
            return false;
    }
}

// Helpers.

void TJob::SetJobState(EJobState state)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    YT_LOG_DEBUG(
        "Setting new job state (Previous: %v, New: %v)",
        JobState_,
        state);

    JobState_ = state;
    AddJobEvent(state);
}

void TJob::SetJobPhase(EJobPhase phase)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    YT_LOG_DEBUG(
        "Setting new job phase (Previous: %v, New: %v)",
        JobPhase_,
        phase);

    JobPhase_ = phase;
    AddJobEvent(phase);
}

void TJob::ValidateJobRunning() const
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    if (JobPhase_ != EJobPhase::Running) {
        YT_LOG_DEBUG(
            "Unexpected job phase (Actual: %v, Expected: %v)",
            JobPhase_,
            EJobPhase::Running);

        THROW_ERROR_EXCEPTION(NJobProberClient::EErrorCode::JobIsNotRunning, "Job %v is not running", Id_)
            << TErrorAttribute("job_state", JobState_)
            << TErrorAttribute("job_phase", JobPhase_);
    }
}

void TJob::StartUserJobMonitoring()
{
    if (!UserJobSpec_) {
        return;
    }

    const auto& monitoringConfig = UserJobSpec_->monitoring_config();
    if (!monitoringConfig.enable()) {
        return;
    }

    RequestedMonitoringSensors_ = FromProto<THashSet<TString>>(monitoringConfig.sensor_names());

    const auto& supportedStatisticSensors = CommonConfig_->UserJobMonitoring->StatisticSensors;
    const auto& supportedGpuSensors = GetSupportedGpuMonitoringSensors();
    for (const auto& sensorName : RequestedMonitoringSensors_) {
        if (!supportedStatisticSensors.contains(sensorName) && !supportedGpuSensors.contains(sensorName)) {
            THROW_ERROR_EXCEPTION("Unknown user job sensor %Qv", sensorName);
        }
    }

    if (monitoringConfig.request_gpu_monitoring()) {
        for (const auto& [sensorName, _] : supportedGpuSensors) {
            RequestedMonitoringSensors_.insert(sensorName);
        }
    }

    UserJobSensorProducer_ = New<TBufferedProducer>();
    TProfiler("")
        .WithGlobal()
        .WithRequiredTag("job_descriptor", monitoringConfig.job_descriptor())
        .AddProducer("", UserJobSensorProducer_);
    HandleJobReport(TNodeJobReport()
        .MonitoringDescriptor(monitoringConfig.job_descriptor()));

    UpdateUserJobMonitoring();
}

void TJob::ReportJobInterruptionInfo(
    TInstant time,
    TDuration timeout,
    NScheduler::EInterruptionReason interruptionReason,
    const std::optional<TString>& preemptionReason,
    const std::optional<NScheduler::TPreemptedFor>& preemptedFor)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    TJobInterruptionInfo interruptionInfo{
        .Time = time,
        .InterruptionReason = interruptionReason,
        .InterruptionTimeout = timeout ? std::optional(timeout) : std::nullopt,
        .PreemptionReason = preemptionReason,
    };

    if (preemptedFor) {
        interruptionInfo.PreemptedFor = TJobInterruptionInfo::TPreemptedFor{
            .AllocationId = preemptedFor->AllocationId,
            .OperationId = preemptedFor->OperationId,
        };
    }

    HandleJobReport(TNodeJobReport()
        .OperationId(OperationId_)
        .InterruptionInfo(std::move(interruptionInfo)));
}

void TJob::DoSetResult(TError error)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    DoSetResult(std::move(error), /*jobResultExtension*/ std::nullopt, /*receivedFromJobProxy*/ false);
}

void TJob::DoSetResult(
    TError error,
    std::optional<NControllerAgent::NProto::TJobResultExt> jobResultExtension,
    bool receivedFromJobProxy)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    if (Error_ && !Error_->IsOK()) {
        YT_LOG_DEBUG(
            "Job error is already set, do not overwrite (CurrentError: %v, Error: %v)",
            Error_,
            error);
        return;
    }

    YT_VERIFY(!error.IsOK() || jobResultExtension);

    YT_LOG_DEBUG("Set job result (Error: %v)", error);

    if (CommonConfig_->TestJobErrorTruncation) {
        if (!error.IsOK()) {
            for (int index = 0; index < 10; ++index) {
                error
                    <<= TError("Test error %v", index);
            }
            YT_LOG_DEBUG(error, "TestJobErrorTruncation");
        }
    }

    JobResultExtension_ = std::nullopt;
    if (jobResultExtension) {
        JobResultExtension_ = std::move(jobResultExtension);
    }

    Error_ = std::move(error).Truncate(
        2,
        16_KB,
        {
            "abort_reason",
            "graceful_abort",
        });

    JobProxyCompleted_ = receivedFromJobProxy;

    FinishTime_ = TInstant::Now();
}

bool TJob::HandleFinishingPhase()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    switch (JobPhase_) {
        case EJobPhase::WaitingForCleanup:
            Cleanup();
            return true;

        case EJobPhase::Cleanup:
        case EJobPhase::Finished:
            return true;

        default:
            return false;
    }
}

void TJob::ValidateJobPhase(EJobPhase expectedPhase) const
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    if (JobPhase_ != expectedPhase) {
        YT_LOG_DEBUG(
            "Unexpected job phase (Actual: %v, Expected: %v)",
            JobPhase_,
            expectedPhase);

        THROW_ERROR_EXCEPTION("Unexpected job phase")
            << TErrorAttribute("expected_phase", expectedPhase)
            << TErrorAttribute("actual_phase", JobPhase_)
            << TErrorAttribute("abort_reason", EAbortReason::UnexpectedNodeJobPhase);
    }
}

// Event handlers.
void TJob::OnNodeDirectoryPrepared(TErrorOr<std::unique_ptr<NNodeTrackerClient::NProto::TNodeDirectory>>&& protoNodeDirectoryOrError)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    StartTime_ = TInstant::Now();

    if (auto delay = JobTestingOptions_->DelayAfterNodeDirectoryPrepared) {
        YT_LOG_DEBUG("Simulate delay after node directory prepared");
        TDelayedExecutor::WaitForDuration(*delay);
    }

    GuardedAction(
        "OnNodeDirectoryPrepared",
        [&] {
            ValidateJobPhase(EJobPhase::PreparingNodeDirectory);
            THROW_ERROR_EXCEPTION_IF_FAILED(protoNodeDirectoryOrError,
                NExecNode::EErrorCode::NodeDirectoryPreparationFailed,
                "Failed to prepare job node directory");

            if (auto& protoNodeDirectory = protoNodeDirectoryOrError.Value()) {
                GuardedJobSpec_.Transform([&](TJobSpec& jobSpec) {
                    jobSpec.MutableExtension(TJobSpecExt::job_spec_ext)
                        ->mutable_input_node_directory()
                        ->Swap(protoNodeDirectory.get());
                });
            }

            SetJobPhase(EJobPhase::DownloadingArtifacts);

            auto artifactsFuture = DownloadArtifacts();
            artifactsFuture.Subscribe(
                BIND(&TJob::OnArtifactsDownloaded, MakeWeak(this))
                    .Via(Invoker_));
            ArtifactsFuture_ = artifactsFuture.As<void>();
        });
}

std::vector<TDevice> TJob::GetGpuDevices()
{
    auto gpuSlots = GetGpuSlots();

    std::vector<TDevice> devices;
    for (const auto& deviceName : Bootstrap_->GetGpuManager()->GetGpuDevices()) {
        bool deviceFound = false;
        for (const auto& gpuSlot : gpuSlots) {
            if (gpuSlot->GetDeviceName() == deviceName) {
                deviceFound = true;
                break;
            }
        }

        // We should not explicitly exclude test device that does not actually exists.
        if (!deviceFound && !Bootstrap_->GetGpuManager()->ShouldTestResource()) {
            // Exclude device explicitly.
            devices.emplace_back(TDevice{
                .DeviceName = deviceName,
                .Access = "-"
            });
        }
    }

    return devices;
}

bool TJob::IsFullHostGpuJob() const
{
    const auto& gpuSlots = GetGpuSlots();
    return !gpuSlots.empty() && gpuSlots.size() == Bootstrap_->GetGpuManager()->GetGpuDevices().size();
}

void TJob::OnArtifactsDownloaded(const TErrorOr<std::vector<NDataNode::IChunkPtr>>& errorOrArtifacts)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    GuardedAction(
        "OnArtifactsDownloaded",
        [&] {
            ValidateJobPhase(EJobPhase::DownloadingArtifacts);
            THROW_ERROR_EXCEPTION_IF_FAILED(errorOrArtifacts, "Failed to download artifacts");

            YT_LOG_INFO("Artifacts downloaded");

            const auto& chunks = errorOrArtifacts.Value();
            for (size_t index = 0; index < Artifacts_.size(); ++index) {
                Artifacts_[index].Chunk = chunks[index];
            }

            CopyFinishTime_ = TInstant::Now();
            RunWithWorkspaceBuilder();
        });
}

void TJob::RunWithWorkspaceBuilder()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);
    std::vector<TDevice> devices = GetGpuDevices();

    std::vector<TBind> binds;
    auto rootFsBinds = GetRootFsBinds();
    binds.reserve(rootFsBinds.size());

    for (const auto& bind : rootFsBinds) {
        binds.push_back(TBind{
            .SourcePath = bind->ExternalPath,
            .TargetPath = bind->InternalPath,
            .ReadOnly = bind->ReadOnly
        });
    }

    if (VirtualSandboxData_) {
        BuildVirtualSandbox();
    }

    TUserSandboxOptions options = BuildUserSandboxOptions();

    TJobWorkspaceBuildingContext context{
        .Logger = Logger,
        .UserSandboxOptions = options,
        .Slot = GetUserSlot(),
        .Job = MakeStrong(this),
        .CommandUser = CommonConfig_->SetupCommandUser,

        .ArtifactDownloadOptions = MakeArtifactDownloadOptions(),

        .Artifacts = Artifacts_,
        .Binds = binds,
        .LayerArtifactKeys = LayerArtifactKeys_,
        .SetupCommands = GetSetupCommands(),
        .DockerImage = DockerImage_,
        .DockerAuth = BuildDockerAuthConfig(),

        .NeedGpuCheck = NeedsGpuCheck(),
        .GpuCheckBinaryPath = UserJobSpec_
            ? std::make_optional(UserJobSpec_->gpu_check_binary_path())
            : std::nullopt,
        .GpuCheckBinaryArgs = UserJobSpec_
            ? std::make_optional(FromProto<std::vector<TString>>(UserJobSpec_->gpu_check_binary_args()))
            : std::optional<std::vector<TString>>(),
        .GpuCheckType = EGpuCheckType::Preliminary,
        .GpuDevices = devices
    };

    auto workspaceBuilder = GetUserSlot()->CreateJobWorkspaceBuilder(
        Invoker_,
        std::move(context));

    workspaceBuilder->SubscribeUpdateArtifactStatistics(
        BIND_NO_PROPAGATE(&TJob::UpdateArtifactStatistics, MakeWeak(this)));

    // TODO(pogorelov): Refactor it. Phase should be changed in callback, not in signal handler.
    // We intentionally subscribe here without Via(Invoker_) to prevent data race.
    workspaceBuilder->SubscribeUpdateBuilderPhase(
        BIND_NO_PROPAGATE(&TJob::SetJobPhase, MakeWeak(this)));

    // TODO(pogorelov): Do not pass TJobWorkspaceBuilderPtr, define structure.
    workspaceBuilder->SubscribeUpdateTimers(
        BIND_NO_PROPAGATE([this, weakThis = MakeWeak(this)] (const TJobWorkspaceBuilderPtr& workspace) {
            auto this_ = weakThis.Lock();
            if (!this_) {
                return;
            }

            PreliminaryGpuCheckStartTime_ = workspace->GetGpuCheckStartTime();
            PreliminaryGpuCheckFinishTime_ = workspace->GetGpuCheckFinishTime();

            StartPrepareVolumeTime_ = workspace->GetVolumePrepareStartTime();
            FinishPrepareVolumeTime_ = workspace->GetVolumePrepareFinishTime();
        })
            .Via(Invoker_));

    auto workspaceFuture = workspaceBuilder->Run();
    workspaceFuture.Subscribe(
        BIND(&TJob::OnWorkspacePreparationFinished, MakeStrong(this))
            .Via(Invoker_));
    WorkspaceBuildingFuture_ = workspaceFuture.AsVoid();
}

void TJob::OnWorkspacePreparationFinished(const TErrorOr<TJobWorkspaceBuildingResult>& resultOrError)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    if (auto delay = JobTestingOptions_->DelayBeforeSpawningJobProxy) {
        YT_LOG_DEBUG("Simulate delay before spawning job proxy");
        TDelayedExecutor::WaitForDuration(*delay);
    }

    GuardedAction(
        "OnWorkspacePreparationFinished",
        [&] {
            // There may be a possible cancellation, but this is not happening now.
            YT_VERIFY(resultOrError.IsOK());

            auto& result = resultOrError.Value();
            TmpfsPaths_ = result.TmpfsPaths;
            RootVolume_ = result.RootVolume;
            // Workspace builder may add or replace docker image.
            DockerImage_ = result.DockerImage;
            DockerImageId_ = result.DockerImageId;
            SetupCommandCount_ = result.SetupCommandCount;

            THROW_ERROR_EXCEPTION_IF_FAILED(
                result.LastBuildError,
                "Job preparation failed");

            RunJobProxy();
        });
}

void TJob::OnExtraGpuCheckCommandFinished(const TError& error)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    ExtraGpuCheckFinishTime_ = TInstant::Now();

    if (HandleFinishingPhase()) {
        return;
    }

    ValidateJobPhase(EJobPhase::RunningExtraGpuCheckCommand);

    YT_LOG_FATAL_IF(
        !Error_ || Error_->IsOK(),
        "Job error is not set during running extra GPU check (Error: %v)",
        Error_);

    auto initialError = std::move(*Error_);

    if (!error.IsOK()) {
        // Reset Error_ to set it with checkError
        Error_ = {};
        JobResultExtension_.reset();

        auto checkError = TError(NExecNode::EErrorCode::GpuCheckCommandFailed, "Extra GPU check command failed")
            << error
            << initialError;

        YT_LOG_WARNING(checkError, "Extra GPU check command executed after job failure is also failed");
        Finalize(std::move(checkError));
    } else {
        YT_LOG_DEBUG("Extra GPU check command finished");

        // NB: manually set error back to avoid reset of JobResultExtension.
        Error_ = std::move(initialError);
        Finalize(TError());
    }

    Cleanup();
}

void TJob::RunJobProxy()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    if (JobPhase_ != EJobPhase::RunningSetupCommands &&
        JobPhase_ != EJobPhase::RunningGpuCheckCommand) {
        YT_LOG_ALERT("Unexpected phase before run job proxy (ActualPhase: %v)", JobPhase_);
    }

    ExecStartTime_ = TInstant::Now();

    SetJobPhase(EJobPhase::SpawningJobProxy);
    InitializeJobProbe();

    auto eligibleChunks = GetKeys(ProxiableChunks_.Load());
    auto hotChunks = JobInputCache_->FilterHotChunkIds(eligibleChunks);

    GuardedJobSpec_.Transform([&] (TJobSpec& jobSpec) {
        PrepareProxiedChunkReading(
            Bootstrap_->GetNodeId(),
            hotChunks,
            THashSet<TChunkId>(eligibleChunks.begin(), eligibleChunks.end()),
            jobSpec.MutableExtension(TJobSpecExt::job_spec_ext));
    });

    BIND(
        &IUserSlot::RunJobProxy,
        GetUserSlot(),
        CreateConfig(),
        Id_,
        OperationId_)
        .AsyncVia(Invoker_)
        .Run()
        .Subscribe(BIND(
            &TJob::OnJobProxyFinished,
            MakeWeak(this))
        .Via(Invoker_));

    TDelayedExecutor::Submit(
        BIND(
            &TJob::OnJobProxyPreparationTimeout,
            MakeWeak(this))
        .Via(Invoker_),
        CommonConfig_->JobProxyPreparationTimeout);
}

void TJob::OnJobProxyPreparationTimeout()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    YT_VERIFY(JobPhase_ >= EJobPhase::SpawningJobProxy);

    if (JobPhase_ == EJobPhase::PreparingJob) {
        YT_LOG_INFO("Job proxy preparation timeout");

        Abort(TError(
            NExecNode::EErrorCode::JobProxyPreparationTimeout,
            "Failed to prepare job proxy within timeout, aborting job"));
    }
}

void TJob::OnJobPreparationTimeout(TDuration prepareTimeLimit, bool fatal)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    if (JobPhase_ < EJobPhase::Running) {
        auto error = TError(
            fatal ? NExecNode::EErrorCode::FatalJobPreparationTimeout : NExecNode::EErrorCode::JobPreparationTimeout,
            "Failed to prepare job within timeout")
            << TErrorAttribute("prepare_time_limit", prepareTimeLimit)
            << TErrorAttribute("job_creation_time", CreationTime_)
            << TErrorAttribute("job_phase", JobPhase_);

        if (fatal) {
            Fail(std::move(error));
        } else {
            Abort(std::move(error));
        }
    }
}

void TJob::OnWaitingForCleanupTimeout()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    if (JobPhase_ == EJobPhase::WaitingForCleanup) {
        auto timeout = CommonConfig_->WaitingForJobCleanupTimeout;

        auto error = TError(NExecNode::EErrorCode::JobCleanupTimeout, "Failed to wait for job cleanup within timeout")
            << TErrorAttribute("job_id", Id_)
            << TErrorAttribute("operation_id", OperationId_)
            << TErrorAttribute("waiting_for_job_cleanup_timeout", timeout);
        Bootstrap_->GetSlotManager()->OnWaitingForJobCleanupTimeout(std::move(error));
    }
}

IUserSlotPtr TJob::GetUserSlot() const
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    if (!ResourceHolder_) {
        return nullptr;
    }

    const auto& userSlot = ResourceHolder_->GetUserSlot();

    return StaticPointerCast<IUserSlot>(userSlot);
}

std::vector<TGpuSlotPtr> TJob::GetGpuSlots() const
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    const auto& gpuSlots = ResourceHolder_->GetGpuSlots();

    std::vector<TGpuSlotPtr> result;
    result.reserve(std::size(gpuSlots));

    for (const auto& gpuSlot : gpuSlots) {
        result.push_back(StaticPointerCast<TGpuSlot>(gpuSlot));
    }

    return result;
}

void TJob::OnJobProxyFinished(const TError& error)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    YT_LOG_INFO(error, "Job proxy finished");

    ResetJobProbe();

    ReportJobProxyProcessFinish(error);

    if (HandleFinishingPhase()) {
        return;
    }

    const auto& currentError = Error_
        ? *Error_
        : TError();
    if (!currentError.IsOK() && NeedsGpuCheck()) {
        SetJobPhase(EJobPhase::RunningExtraGpuCheckCommand);

        TJobGpuCheckerContext context {
            .Slot = GetUserSlot(),
            .Job = MakeStrong(this),
            .RootFS = MakeWritableRootFS(),
            .CommandUser = CommonConfig_->SetupCommandUser,

            .GpuCheckBinaryPath = UserJobSpec_->gpu_check_binary_path(),
            .GpuCheckBinaryArgs = FromProto<std::vector<TString>>(UserJobSpec_->gpu_check_binary_args()),
            .GpuCheckType = EGpuCheckType::Extra,
            .CurrentStartIndex = SetupCommandCount_,
            .TestExtraGpuCheckCommandFailure = Bootstrap_->GetGpuManager()->ShouldTestExtraGpuCheckCommandFailure(),
            .GpuDevices = GetGpuDevices()
        };

        auto checker = New<TJobGpuChecker>(std::move(context), Logger);
        checker->SubscribeRunCheck(BIND_NO_PROPAGATE([this, this_ = MakeStrong(this)] {
            ExtraGpuCheckStartTime_ = TInstant::Now();
        })
            .Via(Invoker_));
        checker->SubscribeFinishCheck(BIND_NO_PROPAGATE([this, this_ = MakeStrong(this)] {
            ExtraGpuCheckFinishTime_ = TInstant::Now();
        })
            .Via(Invoker_));

        YT_LOG_DEBUG("Running extra GPU check");

        BIND(&TJobGpuChecker::RunGpuCheck, checker)
            .AsyncVia(Invoker_)
            .Run()
            .Subscribe(BIND(
                &TJob::OnExtraGpuCheckCommandFinished,
                MakeWeak(this))
                .Via(Invoker_));
    } else {
        if (!error.IsOK()) {
            Finalize(BuildJobProxyError(error));
        } else {
            YT_VERIFY(IsFinished());
        }

        Cleanup();
    }
}

template <class TSourceTag, class TCallback>
void TJob::GuardedAction(const TSourceTag& sourceTag, const TCallback& action)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    YT_LOG_DEBUG(
        "Run guarded action (State: %v, Phase: %v, Source: %v)",
        JobState_,
        JobPhase_,
        sourceTag);

    if (HandleFinishingPhase()) {
        return;
    }

    try {
        TForbidContextSwitchGuard contextSwitchGuard;
        action();
    } catch (const std::exception& ex) {
        // TODO(pogorelov): This method is called not only in preparation states, do something with log message.
        YT_LOG_WARNING(ex, "Error preparing scheduler job");
        Finalize(ex);
        Cleanup();
    }
}

TFuture<void> TJob::StopJobProxy()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    const auto& slot = GetUserSlot();

    YT_LOG_DEBUG("Clean processes (SlotIndex: %v)", slot->GetSlotIndex());

    return slot->CleanProcesses();
}

void TJob::Cleanup()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    YT_VERIFY(IsFinished());

    YT_LOG_FATAL_IF(
        JobPhase_ == EJobPhase::Cleanup || JobPhase_ == EJobPhase::Finished,
        "Job cleanup should be called only once");

    if (auto delay = JobTestingOptions_->DelayInCleanup) {
        YT_LOG_DEBUG("Simulate delay in cleanup");

        TDelayedExecutor::WaitForDuration(*delay);

        if (JobPhase_ >= EJobPhase::Cleanup) {
            return;
        }
    }

    YT_LOG_INFO("Clean up after scheduler job");

    TDelayedExecutor::Cancel(InterruptionTimeoutCookie_);

    SetJobPhase(EJobPhase::Cleanup);

    if (const auto& slot = GetUserSlot()) {
        try {
            WaitFor(StopJobProxy())
                .ThrowOnError();
            // TODO(pogorelov): Maybe we should wait until the process is actually stopped?
        } catch (const std::exception& ex) {
            // Errors during cleanup phase do not affect job outcome.
            YT_LOG_ERROR(ex, "Failed to clean processes (SlotIndex: %v)", slot->GetSlotIndex());
        }
    }

    // NodeDirectory can be really huge, we better offload its cleanup.
    // NB: Do this after slot cleanup.
    {
        auto* inputNodeDirectory = GuardedJobSpec_.Transform([] (TJobSpec& jobSpec) {
            return jobSpec.MutableExtension(TJobSpecExt::job_spec_ext)
                ->release_input_node_directory();
        });
        NRpc::TDispatcher::Get()->GetCompressionPoolInvoker()->Invoke(BIND([inputNodeDirectory] {
            delete inputNodeDirectory;
        }));
    }

    // Release resources.
    GpuStatistics_.clear();

    if (IsStarted() && IsEvicted()) {
        ResourceHolder_->ReleaseNonSlotResources();
    }

    if (RootVolume_) {
        auto removeResult = WaitFor(RootVolume_->Remove());
        YT_LOG_ERROR_IF(
            !removeResult.IsOK(),
            removeResult,
            "Volume remove failed (VolumePath: %v)",
            RootVolume_->GetPath());
        RootVolume_.Reset();

        // Clean up NBD exports only if root volume has been created.
        // In this case there is no race between volume destruction and clean up.
        CleanupNbdExports();
    }

    if (const auto& slot = GetUserSlot()) {
        if (ShouldCleanSandboxes()) {
            try {
                YT_LOG_DEBUG("Clean sandbox (SlotIndex: %v)", slot->GetSlotIndex());
                slot->CleanSandbox();
            } catch (const std::exception& ex) {
                // Errors during cleanup phase do not affect job outcome.
                YT_LOG_ERROR(ex, "Failed to clean sandbox (SlotIndex: %v)", slot->GetSlotIndex());
            }
        } else {
            YT_LOG_WARNING(
                "Sandbox cleanup is disabled by environment variable %v; should be used for testing purposes only",
                DisableSandboxCleanupEnv);
        }
    }

    if (IsEvicted()) {
        ResourceHolder_->ReleaseBaseResources();
    }

    ResourceHolder_.Reset();

    if (UseJobInputCache_.load()) {
        JobInputCache_->UnregisterJobChunks(Id_);
    }

    SetJobPhase(EJobPhase::Finished);

    CleanupFinished_.Set();

    YT_VERIFY(FinishTime_);
    Bootstrap_->GetJobController()->OnJobCleanupFinished(TInstant::Now() - *FinishTime_);

    YT_LOG_INFO("Job finished (JobState: %v)", GetState());
}

//! Make sure NBD exports are unregistered in case of volume creation failures.
void TJob::CleanupNbdExports()
{
    if (auto nbdServer = Bootstrap_->GetNbdServer()) {
        for (const auto& artifactKey : LayerArtifactKeys_) {
            if (!artifactKey.has_nbd_export_id()) {
                continue;
            }

            if (nbdServer->IsDeviceRegistered(artifactKey.nbd_export_id())) {
                TNbdProfilerCounters::Get()->GetCounter(
                    TNbdProfilerCounters::MakeTagSet(artifactKey.data_source().path()),
                    "/device/unregistered_unexpected").Increment(1);

                YT_LOG_ERROR(
                    "NBD export is still registered, unregister it (ExportId: %v, Path: %v)",
                    artifactKey.nbd_export_id(),
                    artifactKey.data_source().path());
                nbdServer->TryUnregisterDevice(artifactKey.nbd_export_id());
            }
        }

        if (VirtualSandboxData_ && nbdServer->IsDeviceRegistered(VirtualSandboxData_->NbdExportId)) {
            YT_LOG_ERROR(
                "NBD virtual layer is still registered, unregister it (ExportId: %v)",
                VirtualSandboxData_->NbdExportId);
            nbdServer->TryUnregisterDevice(VirtualSandboxData_->NbdExportId);
        }
    }
}

TFuture<void> TJob::GetCleanupFinishedEvent()
{
    return CleanupFinished_
        .ToFuture()
        .ToUncancelable();
}

const TAllocationPtr& TJob::GetAllocation() const noexcept
{
    return Allocation_;
}

bool TJob::IsEvicted() const
{
    return !GetAllocation();
}

// Preparation.
std::unique_ptr<NNodeTrackerClient::NProto::TNodeDirectory> TJob::PrepareNodeDirectory()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    const auto& jobSpecExt = JobSpec_.GetExtension(TJobSpecExt::job_spec_ext);
    if (jobSpecExt.has_input_node_directory()) {
        YT_LOG_INFO("Node directory is provided by scheduler");
        return nullptr;
    }

    YT_LOG_INFO("Start preparing node directory");

    const auto& nodeDirectory = Bootstrap_->GetNodeDirectory();

    for (int attempt = 1;; ++attempt) {
        if (JobPhase_ != EJobPhase::PreparingNodeDirectory) {
            break;
        }

        std::optional<TNodeId> unresolvedNodeId;

        auto validateNodeIds = [&] (
            const ::google::protobuf::RepeatedPtrField<NChunkClient::NProto::TChunkSpec>& chunkSpecs,
            const TNodeDirectoryPtr& nodeDirectory)
        {
            for (const auto& chunkSpec : chunkSpecs) {
                auto replicas = GetReplicasFromChunkSpec(chunkSpec);
                for (auto replica : replicas) {
                    auto nodeId = replica.GetNodeId();
                    const auto* descriptor = nodeDirectory->FindDescriptor(nodeId);
                    if (!descriptor) {
                        unresolvedNodeId = nodeId;
                        return;
                    }
                }
            }
        };

        auto validateTableSpecs = [&] (const ::google::protobuf::RepeatedPtrField<TTableInputSpec>& tableSpecs) {
            for (const auto& tableSpec : tableSpecs) {
                validateNodeIds(tableSpec.chunk_specs(), nodeDirectory);
            }
        };

        validateTableSpecs(jobSpecExt.input_table_specs());
        validateTableSpecs(jobSpecExt.foreign_input_table_specs());

        // NB: No need to add these descriptors to the input node directory.
        for (const auto& artifact : Artifacts_) {
            validateNodeIds(artifact.Key.chunk_specs(), nodeDirectory);
        }

        for (const auto& artifactKey : LayerArtifactKeys_) {
            validateNodeIds(artifactKey.chunk_specs(), nodeDirectory);
        }

        if (!unresolvedNodeId) {
            break;
        }

        if (attempt >= CommonConfig_->NodeDirectoryPrepareRetryCount) {
            YT_LOG_WARNING(
                "Some node ids were not resolved, skipping corresponding replicas (UnresolvedNodeId: %v)",
                *unresolvedNodeId);
            break;
        }

        YT_LOG_INFO(
            "Unresolved node id found in job spec; backing off and retrying (NodeId: %v, Attempt: %v)",
            *unresolvedNodeId,
            attempt);
        TDelayedExecutor::WaitForDuration(CommonConfig_->NodeDirectoryPrepareBackoffTime);
    }

    auto protoNodeDirectory = std::make_unique<NNodeTrackerClient::NProto::TNodeDirectory>();
    nodeDirectory->DumpTo(protoNodeDirectory.get());

    if (JobInputCache_->IsEnabled()) {
        UseJobInputCache_.store(true);
        auto chunkSpecs = GetProxiableChunkSpecs(jobSpecExt, GetType());
        ProxiableChunks_.Store(chunkSpecs);
        JobInputCache_->RegisterJobChunks(
            Id_,
            std::move(chunkSpecs));
    }

    YT_LOG_INFO("Finish preparing node directory");

    return protoNodeDirectory;
}

std::vector<NJobProxy::TBindConfigPtr> TJob::GetRootFsBinds()
{
    return Bootstrap_->GetConfig()->ExecNode->RootFSBinds;
}

TJobProxyInternalConfigPtr TJob::CreateConfig()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    auto proxyInternalConfig = CloneYsonStruct(Bootstrap_->GetJobProxyConfigTemplate());
    auto localDescriptor = Bootstrap_->GetLocalDescriptor();
    proxyInternalConfig->DataCenter = localDescriptor.GetDataCenter();
    proxyInternalConfig->Rack = localDescriptor.GetRack();
    proxyInternalConfig->Addresses = localDescriptor.Addresses();

    proxyInternalConfig->LocalHostName = Bootstrap_->GetLocalHostName();

    proxyInternalConfig->BusServer = GetUserSlot()->GetBusServerConfig();

    proxyInternalConfig->TmpfsManager = New<TTmpfsManagerConfig>();
    proxyInternalConfig->TmpfsManager->TmpfsPaths = TmpfsPaths_;

    proxyInternalConfig->MemoryTracker = New<TMemoryTrackerConfig>();
    if (UserJobSpec_) {
        proxyInternalConfig->MemoryTracker->IncludeMemoryMappedFiles = UserJobSpec_->include_memory_mapped_files();
        proxyInternalConfig->MemoryTracker->UseSMapsMemoryTracker = UserJobSpec_->use_smaps_memory_tracker();
    } else {
        proxyInternalConfig->MemoryTracker->IncludeMemoryMappedFiles = true;
        proxyInternalConfig->MemoryTracker->UseSMapsMemoryTracker = false;
    }

    if (UserJobSpec_ && UserJobSpec_->set_container_cpu_limit()) {
        proxyInternalConfig->ContainerCpuLimit = UserJobSpec_->container_cpu_limit();
        if (*proxyInternalConfig->ContainerCpuLimit <= 0) {
            proxyInternalConfig->ContainerCpuLimit = Allocation_->GetRequestedCpu();
        }
    }

    if (UserJobSpec_ && UserJobSpec_->slot_container_memory_limit()) {
        proxyInternalConfig->SlotContainerMemoryLimit = UserJobSpec_->slot_container_memory_limit();
    }

    proxyInternalConfig->MemoryTracker->MemoryStatisticsCachePeriod = proxyInternalConfig->MemoryTracker->UseSMapsMemoryTracker
        ? CommonConfig_->SMapsMemoryTrackerCachePeriod
        : CommonConfig_->MemoryTrackerCachePeriod;

    proxyInternalConfig->JobTestingOptions = JobTestingOptions_;
    proxyInternalConfig->SlotIndex = GetUserSlot()->GetSlotIndex();
    proxyInternalConfig->SlotPath = GetUserSlot()->GetSlotPath();

    if (RootVolume_) {
        proxyInternalConfig->RootPath = RootVolume_->GetPath();
    } else {
        // Pass docker image if root volume is not materialized yet.
        proxyInternalConfig->DockerImage = DockerImage_;
        proxyInternalConfig->DockerImageId = DockerImageId_;
    }

    if (RootVolume_ || DockerImage_) {
        proxyInternalConfig->Binds = GetRootFsBinds();

        for (const auto& artifact : Artifacts_) {
            // Artifact is passed into the job via bind.
            if (artifact.AccessedViaBind) {
                YT_VERIFY(artifact.Chunk);

                YT_LOG_INFO(
                    "Make bind for artifact (FileName: %v, Executable: %v, "
                    "SandboxKind: %v, CompressedDataSize: %v)",
                    artifact.Name,
                    artifact.Executable,
                    artifact.SandboxKind,
                    artifact.Key.GetCompressedDataSize());

                auto sandboxPath = NFS::CombinePaths("/slot", GetSandboxRelPath(artifact.SandboxKind));
                auto targetPath = NFS::CombinePaths(sandboxPath, artifact.Name);

                auto bind = New<TBindConfig>();
                bind->ExternalPath = artifact.Chunk->GetFileName();
                bind->InternalPath = targetPath;
                bind->ReadOnly = true;

                proxyInternalConfig->Binds.push_back(std::move(bind));
            }
        }
    }

    const auto& proxyConfig = Bootstrap_->GetConfig()->ExecNode->JobProxy;

    auto logManagerConfig = proxyInternalConfig->GetSingletonConfig<NLogging::TLogManagerConfig>();
    logManagerConfig->UpdateWriters([&] (const IMapNodePtr& writerConfigNode) {
        auto writerConfig = ConvertTo<NLogging::TLogWriterConfigPtr>(writerConfigNode);
        if (writerConfig->Type != NLogging::TFileLogWriterConfig::WriterType) {
            return writerConfigNode;
        }

        auto fileLogWriterConfig = ConvertTo<NLogging::TFileLogWriterConfigPtr>(writerConfigNode);

        if (proxyConfig->JobProxyLogging->Mode == EJobProxyLoggingMode::PerJobDirectory) {
            const auto& jobProxyLogManager = Bootstrap_->GetJobProxyLogManager();
            YT_VERIFY(jobProxyLogManager);

            fileLogWriterConfig->FileName = jobProxyLogManager->AdjustLogPath(Id_, fileLogWriterConfig->FileName);
        }

        return ConvertTo<IMapNodePtr>(fileLogWriterConfig);
    });

    if (proxyInternalConfig->StderrPath) {
        if (proxyConfig->JobProxyLogging->Mode == EJobProxyLoggingMode::PerJobDirectory) {
            const auto& jobProxyLogManager = Bootstrap_->GetJobProxyLogManager();
            YT_VERIFY(jobProxyLogManager);

            *proxyInternalConfig->StderrPath = jobProxyLogManager->AdjustLogPath(Id_, *proxyInternalConfig->StderrPath);
            YT_LOG_DEBUG("Job proxy stderr path replaced (NewPath: %v)", *proxyInternalConfig->StderrPath);
        }
    }

    if (proxyInternalConfig->ExecutorStderrPath) {
        if (proxyConfig->JobProxyLogging->Mode == EJobProxyLoggingMode::PerJobDirectory) {
            const auto& jobProxyLogManager = Bootstrap_->GetJobProxyLogManager();
            YT_VERIFY(jobProxyLogManager);

            *proxyInternalConfig->ExecutorStderrPath = jobProxyLogManager->AdjustLogPath(Id_, *proxyInternalConfig->ExecutorStderrPath);
            YT_LOG_DEBUG("Executor stderr path replaced (NewPath: %v)", *proxyInternalConfig->ExecutorStderrPath);

        }
    }

    for (const auto& slot : GetGpuSlots()) {
        proxyInternalConfig->GpuIndexes.push_back(slot->GetDeviceIndex());
    }

    // COMPAT(artemagafonov): RootFS is always writable, so the flag should be removed after the update of all nodes.
    proxyInternalConfig->MakeRootFSWritable = true;

    // TODO(ignat): add option to disable fuse within exec node.
    proxyInternalConfig->EnableFuse = UserJobSpec_ && UserJobSpec_->enable_fuse();

    std::vector<TIP6Address> ipAddresses;
    ipAddresses.reserve(ResolvedNodeAddresses_.size());

    if (NetworkProjectId_) {
        for (const auto& [addressName, address] : ResolvedNodeAddresses_) {
            auto networkAddress = New<TUserJobNetworkAddress>();
            networkAddress->Address = TMtnAddress{address}
                .SetProjectId(*NetworkProjectId_)
                .SetHost(GetUserSlot()->GetSlotIndex())
                .ToIP6Address();
            networkAddress->Name = addressName;

            proxyInternalConfig->NetworkAddresses.push_back(networkAddress);
            ipAddresses.push_back(networkAddress->Address);
        }

        if (proxyInternalConfig->NetworkAddresses.empty()) {
            THROW_ERROR_EXCEPTION("No IPv6 node addresses were resolved");
        }

        if (UserJobSpec_ && UserJobSpec_->has_enable_nat64()) {
            proxyInternalConfig->EnableNat64 = UserJobSpec_->enable_nat64();
        }

        if (UserJobSpec_ && UserJobSpec_->has_disable_network()) {
            proxyInternalConfig->DisableNetwork = UserJobSpec_->disable_network();
        }

        proxyInternalConfig->HostName = Format("slot-%v.%v",
            GetUserSlot()->GetSlotIndex(),
            Bootstrap_->GetConfig()->Addresses[0].second);
    } else {
        for (const auto& [addressName, address] : ResolvedNodeAddresses_) {
            ipAddresses.push_back(address);
        }
    }

    {
        auto userSlot = GetUserSlot();
        ExecAttributes_.SlotIndex = userSlot->GetSlotIndex();
        ExecAttributes_.SandboxPath = userSlot->GetSandboxPath(ESandboxKind::User);
        ExecAttributes_.MediumName = userSlot->GetMediumName();

        ExecAttributes_.JobProxySocketPath = userSlot->GetJobProxyUnixDomainSocketPath();

        ExecAttributes_.IPAddresses.reserve(ipAddresses.size());
        for (const auto& address : ipAddresses) {
            ExecAttributes_.IPAddresses.push_back(ToString(address));
        }

        auto gpuSlots = GetGpuSlots();
        ExecAttributes_.GpuDevices.reserve(gpuSlots.size());
        for (const auto& slot : gpuSlots) {
            auto& gpuDevice = ExecAttributes_.GpuDevices.emplace_back(New<TGpuDevice>());
            gpuDevice->DeviceNumber = slot->GetDeviceIndex();
            gpuDevice->DeviceName = slot->GetDeviceName();
        }
    }

    if (auto proxyDynamicConfig = Bootstrap_->GetJobController()->GetJobProxyDynamicConfig()) {
        if (auto jaegerConfig = proxyInternalConfig->TryGetSingletonConfig<NTracing::TJaegerTracerConfig>()) {
            proxyInternalConfig->SetSingletonConfig(jaegerConfig->ApplyDynamic(proxyDynamicConfig->Jaeger));
        }
        proxyInternalConfig->EnableJobShellSeccopm = proxyDynamicConfig->EnableJobShellSeccopm;
        proxyInternalConfig->UsePortoKillForSignalling = proxyDynamicConfig->UsePortoKillForSignalling;
        proxyInternalConfig->ForceIdleCpuPolicy = proxyDynamicConfig->ForceIdleCpuPolicy;
        proxyInternalConfig->AbortOnUncaughtException = proxyDynamicConfig->AbortOnUncaughtException;
        proxyInternalConfig->EnableStderrAndCoreLivePreview = proxyDynamicConfig->EnableStderrAndCoreLivePreview;
        proxyInternalConfig->CheckUserJobOomKill = proxyDynamicConfig->CheckUserJobOomKill;
        if (proxyDynamicConfig->JobEnvironment) {
            proxyInternalConfig->JobEnvironment.MergeWith(proxyDynamicConfig->JobEnvironment);
        }

        proxyInternalConfig->TestingConfig = proxyDynamicConfig->TestingConfig;

        proxyInternalConfig->UseRetryingChannels = proxyDynamicConfig->UseRetryingChannels;
        proxyInternalConfig->RetryingChannel = proxyDynamicConfig->RetryingChannel;
        proxyInternalConfig->PipeReaderTimeoutThreshold = proxyDynamicConfig->PipeReaderTimeoutThreshold;
        proxyInternalConfig->AdaptiveRowCountUpperBound = proxyDynamicConfig->AdaptiveRowCountUpperBound;

        proxyInternalConfig->EnableCudaProfileEventStreaming = proxyDynamicConfig->EnableCudaProfileEventStreaming;
        proxyInternalConfig->JobTraceEventProcessor = proxyDynamicConfig->JobTraceEventProcessor;

        if (proxyDynamicConfig->MemoryProfileDumpPath) {
            auto tcmallocConfig = proxyInternalConfig->GetSingletonConfig<NTCMalloc::TTCMallocConfig>();
            tcmallocConfig->HeapSizeLimit->MemoryProfileDumpPath = *proxyDynamicConfig->MemoryProfileDumpPath;
            tcmallocConfig->HeapSizeLimit->MemoryProfileDumpFilenameSuffix = ToString(GetId());
            tcmallocConfig->HeapSizeLimit->DumpMemoryProfileOnViolation = true;
        }
    }

    proxyInternalConfig->JobThrottler = CloneYsonStruct(CommonConfig_->JobThrottler);
    YT_LOG_DEBUG(
        "Initialize prefetching job throttler (DynamicConfigEnable: %v, JobSpecEnable: %v, PrefetchEnable: %v)",
        CommonConfig_->JobThrottler->BandwidthPrefetch->Enable,
        JobSpecExt_.enable_prefetching_job_throttler(),
        proxyInternalConfig->JobThrottler->BandwidthPrefetch->Enable);

    proxyInternalConfig->StatisticsOutputTableCountLimit = CommonConfig_->StatisticsOutputTableCountLimit;

    proxyInternalConfig->OperationsArchiveVersion = Bootstrap_->GetJobController()->GetOperationsArchiveVersion();

    proxyInternalConfig->EnableRootVolumeDiskQuota = RootVolumeDiskQuotaEnabled_;

    return proxyInternalConfig;
}

NCri::TCriAuthConfigPtr TJob::BuildDockerAuthConfig()
{
    if (UserJobSpec_ && UserJobSpec_->environment_size()) {
        TString prefix = Format("%s_%s=", SecureVaultEnvPrefix, DockerAuthEnv);
        for (const auto& var : UserJobSpec_->environment()) {
            if (var.StartsWith(prefix)) {
                auto ysonConfig = TYsonString(var.substr(prefix.length()));
                return ConvertTo<NCri::TCriAuthConfigPtr>(ysonConfig);
            }
        }
    }

    return nullptr;
}

void TJob::BuildVirtualSandbox()
{
    auto nbdServer = Bootstrap_->GetNbdServer();
    if (!nbdServer) {
        THROW_ERROR_EXCEPTION("NBD server is not present")
            << TErrorAttribute("export_id", VirtualSandboxData_->NbdExportId);
    }

    auto readerHost = nbdServer->GetFileReaderHost();
    auto inThrottler = Bootstrap_->GetDefaultInThrottler();
    auto outRpsThrottler = Bootstrap_->GetReadRpsOutThrottler();
    auto logger = nbdServer->GetLogger();
    auto invoker = nbdServer->GetInvoker();

    std::vector<TArtifactMountOptions> options;

    for (const auto& artifact : Artifacts_) {
        if (!artifact.AccessedViaVirtualSandbox) {
            continue;
        }

        auto sandboxPath = NFS::CombinePaths("/slot", GetSandboxRelPath(artifact.SandboxKind));
        auto filePath = NFS::CombinePaths(sandboxPath, artifact.Name);

        std::vector<NChunkClient::NProto::TChunkSpec> chunkSpecs(
            artifact.Key.chunk_specs().begin(),
            artifact.Key.chunk_specs().end());
        auto reader = CreateRandomAccessFileReader(
            std::move(chunkSpecs),
            filePath,
            readerHost,
            inThrottler,
            outRpsThrottler,
            invoker,
            logger);

        options.push_back({
            .Path = std::move(filePath),
            .Permissions = static_cast<ui16>(0444 + (artifact.Executable ? 0111 : 0000)),
            .Reader = std::move(reader)
        });
    }

    auto squashFsOptions = TSquashFSLayoutBuilderOptions({
        .BlockSize = static_cast<ui32>(CommonConfig_->VirtualSandboxSquashFSBlockSize)
    });

    VirtualSandboxData_->Reader = CreateVirtualSquashFSImageReader(
        std::move(options),
        std::move(squashFsOptions),
        std::move(invoker),
        std::move(logger));
}

TUserSandboxOptions TJob::BuildUserSandboxOptions()
{
    TUserSandboxOptions options;
    // NB: This eventually results in job graceful abort.
    options.DiskOverdraftCallback = BIND(&TJob::Fail, MakeWeak(this))
        .Via(Invoker_);
    // TODO(khlebnikov): Move into volume manager.
    options.EnableRootVolumeDiskQuota = RootVolumeDiskQuotaEnabled_;
    options.UserId = GetUserSlot()->GetUserId();

    if (UserJobSpec_) {
        for (const auto& tmpfsVolumeProto : UserJobSpec_->tmpfs_volumes()) {
            TTmpfsVolume tmpfsVolume;
            tmpfsVolume.Size = tmpfsVolumeProto.size();
            tmpfsVolume.Path = tmpfsVolumeProto.path();
            options.TmpfsVolumes.push_back(tmpfsVolume);
        }

        // COMPAT(ignat).
        if (UserJobSpec_->has_disk_space_limit()) {
            options.DiskSpaceLimit = UserJobSpec_->disk_space_limit();
        }

        // COMPAT(ignat).
        if (UserJobSpec_->has_inode_limit()) {
            options.InodeLimit = UserJobSpec_->inode_limit();
        }

        if (UserJobSpec_->has_disk_request()) {
            if (UserJobSpec_->disk_request().has_disk_space()) {
                options.DiskSpaceLimit = UserJobSpec_->disk_request().disk_space();
            }
            if (UserJobSpec_->disk_request().has_inode_count()) {
                options.InodeLimit = UserJobSpec_->disk_request().inode_count();
            }
        }

        if (options.DiskSpaceLimit.has_value() && options.DiskSpaceLimit.value() <= 0) {
            THROW_ERROR_EXCEPTION(NExecNode::EErrorCode::QuotaSettingFailed, "Set disk space limit must be greater than 0")
                << TErrorAttribute("disk_space_limit", options.DiskSpaceLimit.value());
        }

        if (options.InodeLimit.has_value() && options.InodeLimit.value() <= 0) {
            THROW_ERROR_EXCEPTION(NExecNode::EErrorCode::QuotaSettingFailed, "Set inode limit must be greater than 0")
                << TErrorAttribute("inode_limit", options.InodeLimit.value());
        }
    }

    options.VirtualSandboxData = VirtualSandboxData_;

    return options;
}

bool TJob::CanBeAccessedViaBind(const TArtifact& artifact) const
{
    return !artifact.AccessedViaVirtualSandbox &&
        !artifact.BypassArtifactCache &&
        !artifact.CopyFile;
}

bool TJob::CanBeAccessedViaVirtualSandbox(const TArtifact& artifact) const
{
    if (artifact.BypassArtifactCache ||
        artifact.CopyFile ||
        artifact.Key.data_source().type() != ToProto(NChunkClient::EDataSourceType::File))
    {
        return false;
    }

    // Check if artifact may be inside tmpfs
    if (!Bootstrap_->GetConfig()->ExecNode->SlotManager->EnableTmpfs) {
        return true;
    }

    for (const auto& tmpfsVolume : UserJobSpec_->tmpfs_volumes()) {
        if (tmpfsVolume.path() == "." || artifact.Name.StartsWith(tmpfsVolume.path())) {
            return false;
        }
    }

    return true;
}

// Build artifacts.
void TJob::InitializeArtifacts()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    if (UserJobSpec_) {
        for (const auto& descriptor : UserJobSpec_->files()) {
            Artifacts_.push_back(TArtifact{
                .SandboxKind = ESandboxKind::User,
                .Name = descriptor.file_name(),
                .Executable = descriptor.executable(),
                .BypassArtifactCache = descriptor.bypass_artifact_cache(),
                .CopyFile = descriptor.copy_file(),
                .Key = TArtifactKey(descriptor),
                .Chunk = nullptr
            });
            YT_VERIFY(UserArtifactNameToIndex_.emplace(descriptor.file_name(), Artifacts_.size() - 1).second);
        }

        bool needGpuLayers = NeedGpuLayers() || Bootstrap_->GetGpuManager()->ShouldTestLayers();

        if (needGpuLayers && UserJobSpec_->enable_gpu_layers()) {
            if (UserJobSpec_->layers().empty()) {
                THROW_ERROR_EXCEPTION(NExecNode::EErrorCode::GpuJobWithoutLayers,
                    "No layers specified for GPU job; at least a base layer is required to use GPU");
            }

            for (auto&& layerKey : Bootstrap_->GetGpuManager()->GetToppingLayers()) {
                LayerArtifactKeys_.push_back(std::move(layerKey));
            }
        }

        for (const auto& layerKey : UserJobSpec_->layers()) {
            LayerArtifactKeys_.emplace_back(layerKey);
        }

        if (UserJobSpec_->has_docker_image()) {
            DockerImage_ = UserJobSpec_->docker_image();
        }
    }

    if (JobSpecExt_.has_input_query_spec()) {
        const auto& querySpec = JobSpecExt_.input_query_spec();
        for (const auto& function : querySpec.external_functions()) {
            TArtifactKey key;
            key.mutable_data_source()->set_type(ToProto(EDataSourceType::File));

            for (const auto& chunkSpec : function.chunk_specs()) {
                *key.add_chunk_specs() = chunkSpec;
            }

            Artifacts_.push_back(TArtifact{
                .SandboxKind = ESandboxKind::Udf,
                .Name = function.name(),
                .Executable = false,
                .BypassArtifactCache = false,
                .CopyFile = false,
                .Key = key,
                .Chunk = nullptr
            });
        }
    }

    // Mark NBD layers with NBD export ids.
    auto nbdExportCount = 0;
    for (auto& layer : LayerArtifactKeys_) {
        if (FromProto<ELayerAccessMethod>(layer.access_method()) == ELayerAccessMethod::Nbd) {
            auto nbdExportId = MakeNbdExportId(Id_, nbdExportCount);
            layer.set_nbd_export_id(ToString(nbdExportId));
            ++nbdExportCount;
        }
    }

    if (!LayerArtifactKeys_.empty() &&
        Bootstrap_->GetNbdServer() &&
        RootVolumeDiskQuotaEnabled_ &&
        JobSpecExt_.enable_virtual_sandbox())
    {
        // Mark artifacts that will be accessed via virtual layer.
        int virtualSandboxArtifactCount = 0;
        for (auto& artifact : Artifacts_) {
            if (CanBeAccessedViaVirtualSandbox(artifact)) {
                artifact.AccessedViaVirtualSandbox = true;
                ++virtualSandboxArtifactCount;
            }
        }

        if (virtualSandboxArtifactCount != 0) {
            // The virtual layer can be used. Make nbd export id.
            auto nbdExportId = MakeNbdExportId(Id_, nbdExportCount);
            VirtualSandboxData_ = TVirtualSandboxData({
                .NbdExportId = ToString(nbdExportId)
            });
            ++nbdExportCount;
        }
    }

    // Mark artifacts that will be accessed via bind.
    for (auto& artifact : Artifacts_) {
        artifact.AccessedViaBind = CanBeAccessedViaBind(artifact);
    }
}

TArtifactDownloadOptions TJob::MakeArtifactDownloadOptions() const
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    std::vector<TString> workloadDescriptorAnnotations = {
        Format("OperationId: %v", OperationId_),
        Format("JobId: %v", Id_),
        Format("AuthenticatedUser: %v", JobSpecExt_.authenticated_user()),
    };

    auto options = TArtifactDownloadOptions{
        .TrafficMeter = TrafficMeter_,
    };

    return options;
}

// Start async artifacts download.
TFuture<std::vector<NDataNode::IChunkPtr>> TJob::DownloadArtifacts()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    const auto& chunkCache = Bootstrap_->GetChunkCache();

    std::vector<TFuture<IChunkPtr>> asyncChunks;
    for (const auto& artifact : Artifacts_) {
        i64 artifactSize = artifact.Key.GetCompressedDataSize();
        if (artifact.BypassArtifactCache) {
            ChunkCacheStatistics_.CacheBypassedArtifactsSize += artifactSize;
            asyncChunks.push_back(MakeFuture<IChunkPtr>(nullptr));
            continue;
        }

        if (artifact.AccessedViaVirtualSandbox) {
            asyncChunks.push_back(MakeFuture<IChunkPtr>(nullptr));
            continue;
        }

        YT_LOG_INFO(
            "Download user file (FileName: %v, SandboxKind: %v, CompressedDataSize: %v)",
            artifact.Name,
            artifact.SandboxKind,
            artifact.Key.GetCompressedDataSize());

        auto downloadOptions = MakeArtifactDownloadOptions();
        bool fetchedFromCache = false;
        auto asyncChunk = chunkCache->DownloadArtifact(artifact.Key, downloadOptions, &fetchedFromCache)
            .Apply(BIND([fileName = artifact.Name, this, this_ = MakeStrong(this)] (const TErrorOr<IChunkPtr>& chunkOrError) {
                THROW_ERROR_EXCEPTION_IF_FAILED(chunkOrError,
                    NExecNode::EErrorCode::ArtifactDownloadFailed,
                    "Failed to prepare user file %Qv",
                    fileName);

                const auto& chunk = chunkOrError.Value();
                YT_LOG_INFO(
                    "Artifact chunk ready (FileName: %v, LocationId: %v, ChunkId: %v)",
                    fileName,
                    chunk->GetLocation()->GetId(),
                    chunk->GetId());
                return chunk;
            }));

        asyncChunks.push_back(asyncChunk);

        UpdateArtifactStatistics(artifactSize, fetchedFromCache);
    }

    return AllSucceeded(asyncChunks)
        .ToImmediatelyCancelable();
}

TError TJob::BuildJobProxyError(const TError& spawnError)
{
    // Analyse results.
    if (spawnError.IsOK()) {
        return TError();
    }

    auto jobProxyError = TError(
        NExecNode::EErrorCode::JobProxyFailed,
        "Job proxy failed")
        << spawnError;

    if (spawnError.GetCode() == EProcessErrorCode::NonZeroExitCode) {
        // Try to translate the numeric exit code into some human readable reason.
        auto reason = EJobProxyExitCode(spawnError.Attributes().Get<int>("exit_code"));
        const auto& validReasons = TEnumTraits<EJobProxyExitCode>::GetDomainValues();
        if (std::find(validReasons.begin(), validReasons.end(), reason) != validReasons.end()) {
            jobProxyError <<= TErrorAttribute("reason", reason);
        }
    }

    return jobProxyError;
}

std::optional<EAbortReason> TJob::DeduceAbortReason()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    YT_VERIFY(Error_);

    const auto& resultError = *Error_;

    static const THashSet<TErrorCode> layerErrorCodes = {
        NExecNode::EErrorCode::LayerUnpackingFailed,
        NExecNode::EErrorCode::DockerImagePullingFailed,
        NExecNode::EErrorCode::InvalidImage,
    };

    static const THashSet<TErrorCode> ignoredFailedChunksErrorCodes = {
        NNet::EErrorCode::ResolveTimedOut,
        NChunkClient::EErrorCode::ReaderThrottlingFailed,
        NTableClient::EErrorCode::NameTableUpdateFailed,
    };

    static const THashSet<TErrorCode> otherAbortErrorCodes = {
        NChunkClient::EErrorCode::AllTargetNodesFailed,
        NChunkClient::EErrorCode::ReaderThrottlingFailed,
        NChunkClient::EErrorCode::MasterCommunicationFailed,
        NChunkClient::EErrorCode::MasterNotConnected,
        NChunkClient::EErrorCode::ReaderTimeout,
        NChunkClient::EErrorCode::ChunkBlockFetchFailed,
        NChunkClient::EErrorCode::ChunkMetaFetchFailed,
        NChunkClient::EErrorCode::AutoRepairFailed,
        NExecNode::EErrorCode::ConfigCreationFailed,
        NExecNode::EErrorCode::SlotNotFound,
        NExecNode::EErrorCode::JobEnvironmentDisabled,
        NExecNode::EErrorCode::ArtifactCopyingFailed,
        NExecNode::EErrorCode::ArtifactDownloadFailed,
        NExecNode::EErrorCode::NodeDirectoryPreparationFailed,
        NExecNode::EErrorCode::SlotLocationDisabled,
        NExecNode::EErrorCode::NoLayerLocationAvailable,
        NExecNode::EErrorCode::NotEnoughDiskSpace,
        NJobProxy::EErrorCode::MemoryCheckFailed,
        NContainers::EErrorCode::FailedToStartContainer,
        EProcessErrorCode::CannotResolveBinary,
        NNet::EErrorCode::ResolveTimedOut,
        NExecNode::EErrorCode::JobProxyPreparationTimeout,
        NExecNode::EErrorCode::JobPreparationTimeout,
        NExecNode::EErrorCode::GpuCheckCommandFailed,
        NExecNode::EErrorCode::GpuLayerNotFetched,
        NJobProxy::EErrorCode::JobNotRunning,
        NExecNode::EErrorCode::ArtifactFetchFailed,
    };

    if (JobResultExtension_) {
        const auto& schedulerResultExt = *JobResultExtension_;

        if (!resultError.FindMatching(ignoredFailedChunksErrorCodes) &&
            schedulerResultExt.failed_chunk_ids_size() > 0)
        {
            return EAbortReason::FailedChunks;
        }
    }

    // This is most probably user error, still we don't want to make it fatal.
    if (resultError.FindMatching(layerErrorCodes))
    {
        return std::nullopt;
    }

    if (resultError.GetCode() == NJobProxy::EErrorCode::UserJobFailed) {
        if (auto innerError = resultError.FindMatching(NExecNode::EErrorCode::AbortByControllerAgent)) {
            return innerError->Attributes().Find<EAbortReason>("abort_reason");
        }
    }

    if (auto abortReason = FindAttributeRecursive<EAbortReason>(resultError, "abort_reason")) {
        return *abortReason;
    }

    if (AbortJobIfAccountLimitExceeded_ &&
        resultError.FindMatching(NSecurityClient::EErrorCode::AccountLimitExceeded))
    {
        return EAbortReason::AccountLimitExceeded;
    }

    if (resultError.FindMatching(NExecNode::EErrorCode::ResourceOverdraft)) {
        return EAbortReason::ResourceOverdraft;
    }

    if (resultError.FindMatching(NExecNode::EErrorCode::NodeResourceOvercommit)) {
        return EAbortReason::NodeResourceOvercommit;
    }

    if (resultError.FindMatching(NExecNode::EErrorCode::WaitingJobTimeout)) {
        return EAbortReason::WaitingTimeout;
    }

    if (resultError.FindMatching(NExecNode::EErrorCode::AbortByScheduler) ||
        resultError.FindMatching(NJobProxy::EErrorCode::JobNotPrepared))
    {
        return EAbortReason::Scheduler;
    }

    if (resultError.FindMatching(NJobProxy::EErrorCode::ShallowMergeFailed)) {
        return EAbortReason::ShallowMergeFailed;
    }

    if (resultError.FindMatching(NJobProxy::EErrorCode::InterruptionFailed)) {
        return EAbortReason::InterruptionFailed;
    }

    if (resultError.FindMatching(NJobProxy::EErrorCode::InterruptionTimeout)) {
        return EAbortReason::InterruptionTimeout;
    }

    if (resultError.FindMatching(NJobProxy::EErrorCode::InterruptionUnsupported)) {
        return EAbortReason::InterruptionUnsupported;
    }

    if (resultError.FindMatching(NExecNode::EErrorCode::RootVolumePreparationFailed)) {
        return EAbortReason::RootVolumePreparationFailed;
    }

    if (resultError.FindMatching(NJobProxy::EErrorCode::UserJobPortoApiError)) {
        return EAbortReason::Other;
    }

    if (resultError.FindMatching(otherAbortErrorCodes)) {
        return EAbortReason::Other;
    }

    if (auto jobProxyFailedError = resultError.FindMatching(NExecNode::EErrorCode::JobProxyFailed)) {
        if (resultError.FindMatching(EProcessErrorCode::CannotStartProcess)) {
            return EAbortReason::Other;
        }
        if (auto processError = resultError.FindMatching(EProcessErrorCode::NonZeroExitCode)) {
            auto exitCode = NExecNode::EJobProxyExitCode(processError->Attributes().Get<int>("exit_code"));
            switch (exitCode) {
                case EJobProxyExitCode::SupervisorCommunicationFailed:
                case EJobProxyExitCode::ResultReportFailed:
                case EJobProxyExitCode::ResourcesUpdateFailed:
                case EJobProxyExitCode::GetJobSpecFailed:
                case EJobProxyExitCode::InvalidSpecVersion:
                case EJobProxyExitCode::PortoManagementFailed:
                    return EAbortReason::Other;

                case EJobProxyExitCode::ResourceOverdraft:
                    return EAbortReason::ResourceOverdraft;

                default: {
                    if (CommonConfig_->TreatJobProxyFailureAsAbort) {
                        return EAbortReason::JobProxyFailed;
                    }
                    break;
                }
            }
        }
        if (auto processSignal = resultError.FindMatching(EProcessErrorCode::Signal)) {
            if (processSignal->Attributes().Find<bool>("oom_killed").value_or(false)) {
                return EAbortReason::ResourceOverdraft;
            }
        }
    }

    return std::nullopt;
}

bool TJob::IsFatalError(const TError& error)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return
        error.FindMatching(NTableClient::EErrorCode::SortOrderViolation) ||
        error.FindMatching(NSecurityClient::EErrorCode::AuthenticationError) ||
        error.FindMatching(NSecurityClient::EErrorCode::AuthorizationError) ||
        (
            error.FindMatching(NSecurityClient::EErrorCode::AccountLimitExceeded) &&
            !AbortJobIfAccountLimitExceeded_) ||
        error.FindMatching(NSecurityClient::EErrorCode::NoSuchAccount) ||
        error.FindMatching(NChunkClient::EErrorCode::NoSuchMedium) ||
        error.FindMatching(NChunkClient::EErrorCode::ForbiddenErasureCodec) ||
        error.FindMatching(NNodeTrackerClient::EErrorCode::NoSuchNetwork) ||
        error.FindMatching(NTableClient::EErrorCode::InvalidDoubleValue) ||
        error.FindMatching(NTableClient::EErrorCode::IncomparableTypes) ||
        error.FindMatching(NTableClient::EErrorCode::UnhashableType) ||
        error.FindMatching(NTableClient::EErrorCode::CorruptedNameTable) ||
        error.FindMatching(NTableClient::EErrorCode::RowWeightLimitExceeded) ||
        error.FindMatching(NTableClient::EErrorCode::InvalidColumnFilter) ||
        error.FindMatching(NTableClient::EErrorCode::InvalidColumnRenaming) ||
        error.FindMatching(NTableClient::EErrorCode::FormatCannotRepresentRow) ||
        error.FindMatching(NExecNode::EErrorCode::SetupCommandFailed) ||
        error.FindMatching(NExecNode::EErrorCode::GpuJobWithoutLayers) ||
        error.FindMatching(NExecNode::EErrorCode::GpuCheckCommandIncorrect) ||
        error.FindMatching(NExecNode::EErrorCode::TmpfsOverflow) ||
        error.FindMatching(NExecNode::EErrorCode::FatalJobPreparationTimeout) ||
        error.FindMatching(NFormats::EErrorCode::InvalidFormat);
}

void TJob::EnrichStatisticsWithGpuInfo(TStatistics* statistics, const std::vector<TGpuSlotPtr>& gpuSlots)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    TGpuStatistics aggregatedGpuStatistics;
    i64 totalGpuMemory = 0;

    auto gpuInfoMap = Bootstrap_->GetGpuManager()->GetGpuInfoMap();
    for (int index = 0; index < std::ssize(gpuSlots); ++index) {
        const auto& slot = gpuSlots[index];

        auto& [slotStatistics, slotStatisticsLastUpdateTime] = GpuStatistics_[index];

        NGpu::TGpuInfo gpuInfo;
        {
            auto it = gpuInfoMap.find(slot->GetDeviceIndex());
            if (it == gpuInfoMap.end()) {
                continue;
            }
            gpuInfo = it->second;
        }

        if (!slotStatisticsLastUpdateTime) {
            YT_VERIFY(ExecStartTime_);

            slotStatisticsLastUpdateTime = ExecStartTime_;
        }

        auto period = gpuInfo.UpdateTime - *slotStatisticsLastUpdateTime;
        slotStatistics.CumulativeUtilizationGpu += period.MilliSeconds() * gpuInfo.UtilizationGpuRate;
        if (gpuInfo.UtilizationGpuRate > 0) {
            slotStatistics.CumulativeLoad += period.MilliSeconds();
        }
        slotStatistics.CumulativeUtilizationMemory += period.MilliSeconds() * gpuInfo.UtilizationMemoryRate;
        slotStatistics.CumulativeMemory += period.MilliSeconds() * gpuInfo.MemoryUsed;
        slotStatistics.CumulativeMemoryMBSec += static_cast<i64>(period.SecondsFloat() * gpuInfo.MemoryUsed / 1_MB);
        slotStatistics.CumulativeUtilizationPower += period.MilliSeconds() *
            (gpuInfo.PowerLimit > 0
                ? gpuInfo.PowerDraw / gpuInfo.PowerLimit
                : 0.0);
        slotStatistics.CumulativePower += period.MilliSeconds() * gpuInfo.PowerDraw;
        slotStatistics.CumulativeUtilizationClocksSM += period.MilliSeconds() *
            (gpuInfo.ClocksMaxSM > 0
                ? static_cast<double>(gpuInfo.ClocksSM) / gpuInfo.ClocksMaxSM
                : 0.0);
        slotStatistics.CumulativeSMUtilization += period.MilliSeconds() * gpuInfo.SMUtilizationRate;
        slotStatistics.CumulativeSMOccupancy += period.MilliSeconds() * gpuInfo.SMOccupancyRate;
        slotStatistics.NvlinkRxBytes += static_cast<i64>(period.SecondsFloat() * gpuInfo.NvlinkRxByteRate);
        slotStatistics.NvlinkTxBytes += static_cast<i64>(period.SecondsFloat() * gpuInfo.NvlinkTxByteRate);
        slotStatistics.PcieRxBytes += static_cast<i64>(period.SecondsFloat() * gpuInfo.PcieRxByteRate);
        slotStatistics.PcieTxBytes += static_cast<i64>(period.SecondsFloat() * gpuInfo.PcieTxByteRate);
        slotStatistics.MaxMemoryUsed = std::max(slotStatistics.MaxMemoryUsed, gpuInfo.MemoryUsed);
        if (gpuInfo.Stuck.Status && gpuInfo.Stuck.LastTransitionTime) {
            slotStatistics.MaxStuckDuration = std::max(
                slotStatistics.MaxStuckDuration,
                static_cast<i64>((gpuInfo.UpdateTime - *gpuInfo.Stuck.LastTransitionTime).MilliSeconds()));
        }
        slotStatistics.CumulativeTensorActivity += period.MilliSeconds() * gpuInfo.TensorActivityRate;
        slotStatistics.CumulativeDramActivity += period.MilliSeconds() * gpuInfo.DramActivityRate;

        YT_LOG_DEBUG(
            "Updated job GPU slot statistics "
            "(GpuInfo: %v, SlotStatistics: %v, SlotStatisticsLastUpdateTime: %v, Period: %v)",
            gpuInfo,
            slotStatistics,
            *slotStatisticsLastUpdateTime,
            period);

        slotStatisticsLastUpdateTime = gpuInfo.UpdateTime;

        aggregatedGpuStatistics.CumulativeUtilizationGpu += slotStatistics.CumulativeUtilizationGpu;
        aggregatedGpuStatistics.CumulativeUtilizationMemory += slotStatistics.CumulativeUtilizationMemory;
        aggregatedGpuStatistics.CumulativeMemory += slotStatistics.CumulativeMemory;
        aggregatedGpuStatistics.CumulativeMemoryMBSec += slotStatistics.CumulativeMemoryMBSec;
        aggregatedGpuStatistics.CumulativeLoad += slotStatistics.CumulativeLoad;
        aggregatedGpuStatistics.CumulativeUtilizationPower += slotStatistics.CumulativeUtilizationPower;
        aggregatedGpuStatistics.CumulativePower += slotStatistics.CumulativePower;
        aggregatedGpuStatistics.CumulativeSMUtilization += slotStatistics.CumulativeSMUtilization;
        aggregatedGpuStatistics.CumulativeSMOccupancy += slotStatistics.CumulativeSMOccupancy;
        aggregatedGpuStatistics.NvlinkRxBytes += slotStatistics.NvlinkRxBytes;
        aggregatedGpuStatistics.NvlinkTxBytes += slotStatistics.NvlinkTxBytes;
        aggregatedGpuStatistics.PcieRxBytes += slotStatistics.PcieRxBytes;
        aggregatedGpuStatistics.PcieTxBytes += slotStatistics.PcieTxBytes;
        aggregatedGpuStatistics.MaxMemoryUsed += slotStatistics.MaxMemoryUsed;
        aggregatedGpuStatistics.MaxStuckDuration = std::max(aggregatedGpuStatistics.MaxStuckDuration, slotStatistics.MaxStuckDuration);
        aggregatedGpuStatistics.CumulativeTensorActivity += slotStatistics.CumulativeTensorActivity;
        aggregatedGpuStatistics.CumulativeDramActivity += slotStatistics.CumulativeDramActivity;
        totalGpuMemory += gpuInfo.MemoryTotal;
    }

    YT_LOG_DEBUG(
        "Updated job aggregate GPU statistics (AggregateGpuStatistics: %v, TotalGpuMemory: %v)",
        aggregatedGpuStatistics,
        totalGpuMemory);

    statistics->AddSample("/user_job/gpu/cumulative_utilization_gpu"_SP, aggregatedGpuStatistics.CumulativeUtilizationGpu);
    statistics->AddSample("/user_job/gpu/cumulative_utilization_memory"_SP, aggregatedGpuStatistics.CumulativeUtilizationMemory);
    statistics->AddSample("/user_job/gpu/cumulative_utilization_power"_SP, aggregatedGpuStatistics.CumulativeUtilizationPower);
    statistics->AddSample("/user_job/gpu/cumulative_memory_mb_sec"_SP, aggregatedGpuStatistics.CumulativeMemoryMBSec);
    statistics->AddSample("/user_job/gpu/cumulative_power"_SP, aggregatedGpuStatistics.CumulativePower);
    statistics->AddSample("/user_job/gpu/cumulative_load"_SP, aggregatedGpuStatistics.CumulativeLoad);
    statistics->AddSample("/user_job/gpu/max_memory_used"_SP, aggregatedGpuStatistics.MaxMemoryUsed);
    statistics->AddSample("/user_job/gpu/cumulative_sm_utilization"_SP, aggregatedGpuStatistics.CumulativeSMUtilization);
    statistics->AddSample("/user_job/gpu/cumulative_sm_occupancy"_SP, aggregatedGpuStatistics.CumulativeSMOccupancy);
    statistics->AddSample("/user_job/gpu/nvlink/rx_bytes"_SP, aggregatedGpuStatistics.NvlinkRxBytes);
    statistics->AddSample("/user_job/gpu/nvlink/tx_bytes"_SP, aggregatedGpuStatistics.NvlinkTxBytes);
    statistics->AddSample("/user_job/gpu/pcie/rx_bytes"_SP, aggregatedGpuStatistics.PcieRxBytes);
    statistics->AddSample("/user_job/gpu/pcie/tx_bytes"_SP, aggregatedGpuStatistics.PcieTxBytes);
    statistics->AddSample("/user_job/gpu/max_stuck_duration"_SP, aggregatedGpuStatistics.MaxStuckDuration);
    statistics->AddSample("/user_job/gpu/cumulative_tensor_activity"_SP, aggregatedGpuStatistics.CumulativeTensorActivity);
    statistics->AddSample("/user_job/gpu/cumulative_dram_activity"_SP, aggregatedGpuStatistics.CumulativeDramActivity);
    statistics->AddSample("/user_job/gpu/memory_total"_SP, totalGpuMemory);
}

void TJob::EnrichStatisticsWithRdmaDeviceInfo(TStatistics* statistics)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    TRdmaStatistics aggregatedRdmaStatistics;
    auto rdmaDevices = Bootstrap_->GetGpuManager()->GetRdmaDevices();
    for (const auto& rdmaDevice : rdmaDevices) {
        aggregatedRdmaStatistics.RxByteRate += rdmaDevice.RxByteRate;
        aggregatedRdmaStatistics.TxByteRate += rdmaDevice.TxByteRate;
    }

    statistics->AddSample("/user_job/gpu/rdma/rx_bytes"_SP, aggregatedRdmaStatistics.RxByteRate);
    statistics->AddSample("/user_job/gpu/rdma/tx_bytes"_SP, aggregatedRdmaStatistics.TxByteRate);
}

void TJob::EnrichStatisticsWithDiskInfo(TStatistics* statistics)
{
    auto diskStatistics = GetUserSlot()->GetDiskStatistics();
    MaxDiskUsage_ = std::max(MaxDiskUsage_, diskStatistics.Usage);
    statistics->AddSample("/user_job/disk/usage"_SP, diskStatistics.Usage);
    statistics->AddSample("/user_job/disk/max_usage"_SP, MaxDiskUsage_);
    if (diskStatistics.Limit) {
        statistics->AddSample("/user_job/disk/limit"_SP, *diskStatistics.Limit);
    }
}

void TJob::EnrichStatisticsWithArtifactsInfo(TStatistics* statistics)
{
    statistics->AddSample("/exec_agent/artifacts/cache_hit_artifacts_size"_SP, ChunkCacheStatistics_.CacheHitArtifactsSize);
    statistics->AddSample("/exec_agent/artifacts/cache_miss_artifacts_size"_SP, ChunkCacheStatistics_.CacheMissArtifactsSize);
    statistics->AddSample("/exec_agent/artifacts/cache_bypassed_artifacts_size"_SP, ChunkCacheStatistics_.CacheBypassedArtifactsSize);
}

void TJob::UpdateIOStatistics(const TStatistics& statistics)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    auto getStat = [&] (i64 oldValue, const TStatisticPath& path) {
        auto iter = statistics.Data().find(path);
        i64 newValue = iter == statistics.Data().end() ? 0 : iter->second.GetSum();
        if (newValue < oldValue) {
            YT_LOG_WARNING("Job I/O statistic decreased over time (Name: %v, OldValue: %v, NewValue: %v)", path.Path(), oldValue, newValue);
            newValue = oldValue;
        }
        return newValue;
    };

    auto newBytesRead = getStat(BytesRead_, "/user_job/block_io/bytes_read"_SP);
    auto newBytesWritten = getStat(BytesWritten_, "/user_job/block_io/bytes_written"_SP);

    // NB(gepardo): Porto currently calculates only io_total, without making any difference between read and
    // write IO requests. So, we use io_total to estimate both. This place must be corrected when Porto will
    // export read and write IO requests separately (see PORTO-1011 for details).
    auto newIORequestsRead = getStat(IORequestsRead_, "/user_job/block_io/io_total"_SP);
    auto newIORequestsWritten = getStat(IORequestsWritten_, "/user_job/block_io/io_total"_SP);

    if (Bootstrap_->GetIOTracker()->IsEnabled()) {
        auto processDirection = [&] (const char *direction, i64 byteDelta, i64 ioRequestDelta) {
            if (byteDelta > 0 || ioRequestDelta > 0) {
                Bootstrap_->GetIOTracker()->Enqueue(
                    TIOCounters{
                        .Bytes = byteDelta,
                        .IORequests = ioRequestDelta,
                    },
                    /*tags*/ {
                        {FormatIOTag(EAggregateIOTag::Direction), direction},
                        // TODO(babenko): switch to std::string
                        {FormatIOTag(EAggregateIOTag::User), ToString(GetCurrentAuthenticationIdentity().User)},
                        {FormatIOTag(EAggregateIOTag::JobIoKind), "user_job"},
                    });
            }
        };

        processDirection("read", newBytesRead - BytesRead_, newIORequestsRead - IORequestsRead_);
        processDirection("write", newBytesWritten - BytesWritten_, newIORequestsWritten - IORequestsWritten_);
    }

    BytesRead_ = newBytesRead;
    BytesWritten_ = newBytesWritten;
    IORequestsRead_ = newIORequestsRead;
    IORequestsWritten_ = newIORequestsWritten;
}

void TJob::UpdateArtifactStatistics(i64 compressedDataSize, bool cacheHit)
{
    if (cacheHit) {
        ChunkCacheStatistics_.CacheHitArtifactsSize += compressedDataSize;
    } else {
        ChunkCacheStatistics_.CacheMissArtifactsSize += compressedDataSize;
    }
}

std::vector<TShellCommandConfigPtr> TJob::GetSetupCommands()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    std::vector<TShellCommandConfigPtr> result;

    auto addIfPresent = [&] (const std::optional<TShellCommandConfigPtr>& command) {
        if (command) {
            result.push_back(*command);
        }
    };

    addIfPresent(CommonConfig_->JobSetupCommand);

    bool needGpu = NeedGpuLayers() || Bootstrap_->GetGpuManager()->ShouldTestSetupCommands();
    if (needGpu) {
        auto gpu_commands = Bootstrap_->GetGpuManager()->GetSetupCommands();
        result.insert(result.end(), gpu_commands.begin(), gpu_commands.end());
    }

    return result;
}

NContainers::TRootFS TJob::MakeWritableRootFS()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);
    YT_VERIFY(RootVolume_);

    NContainers::TRootFS rootFS;
    auto rootFsBinds = GetRootFsBinds();

    rootFS.RootPath = RootVolume_->GetPath();
    rootFS.IsRootReadOnly = false;
    rootFS.Binds.reserve(rootFsBinds.size());

    for (const auto& bind : rootFsBinds) {
        rootFS.Binds.push_back(TBind{
            .SourcePath = bind->ExternalPath,
            .TargetPath = bind->InternalPath,
            .ReadOnly = bind->ReadOnly
        });
    }

    return rootFS;
}

TNodeJobReport TJob::MakeDefaultJobReport()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    auto report = TNodeJobReport()
        .Type(GetType())
        .State(GetState())
        .StartTime(GetCreationTime())
        .SpecVersion(0) // TODO: fill correct spec version.
        .CoreInfos(CoreInfos_)
        .ExecAttributes(ConvertToYsonString(ExecAttributes_));
    if (FinishTime_) {
        report.SetFinishTime(*FinishTime_);
    }
    if (JobSpecExt_.has_job_competition_id()) {
        report.SetJobCompetitionId(FromProto<TJobId>(JobSpecExt_.job_competition_id()));
    }
    if (JobSpecExt_.has_probing_job_competition_id()) {
        report.SetProbingJobCompetitionId(FromProto<TJobId>(JobSpecExt_.probing_job_competition_id()));
    }
    if (JobSpecExt_.has_task_name()) {
        report.SetTaskName(JobSpecExt_.task_name());
    }
    if (UserJobSpec_ &&
        UserJobSpec_->has_archive_ttl())
    {
        report.SetTtl(FromProto<TDuration>(UserJobSpec_->archive_ttl()));
    }

    return report;
}


void TJob::InitializeJobProbe()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto probe = CreateJobProbe(GetUserSlot()->GetBusClientConfig());
    {
        auto guard = Guard(JobProbeLock_);
        std::swap(JobProbe_, probe);
    }
}

void TJob::ResetJobProbe()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    IJobProbePtr probe;
    {
        auto guard = Guard(JobProbeLock_);
        std::swap(JobProbe_, probe);
    }
}

IJobProbePtr TJob::GetJobProbeOrThrow()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto guard = Guard(JobProbeLock_);
    if (!JobProbe_) {
        THROW_ERROR_EXCEPTION("Job probe is not available");
    }
    return JobProbe_;
}

void TJob::ReportJobProxyProcessFinish(const TError& error)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    std::optional<TDuration> delay;
    if (ResultReceivedTime_) {
        delay = TInstant::Now() - *ResultReceivedTime_;
    }

    Bootstrap_->GetJobController()->OnJobProxyProcessFinished(error, delay);
}

bool TJob::ShouldCleanSandboxes()
{
    return GetEnv(DisableSandboxCleanupEnv) != "1";
}

bool TJob::NeedGpuLayers()
{
    auto jobEnvironmentType = Bootstrap_->GetJobEnvironmentType();
    if (jobEnvironmentType != EJobEnvironmentType::Porto) {
        return false;
    }

    if (JobSpecExt_.has_user_job_spec()) {
        const auto& userJobSpec = JobSpecExt_.user_job_spec();
        if (userJobSpec.has_cuda_toolkit_version()) {
            return true;
        }
    }

    return NeedGpu();
}

bool TJob::NeedGpu()
{
    return GetResourceUsage().Gpu > 0;
}

// TODO(eshcherbin, ???): Move this monitoring to job proxy and use profiling library directly instead of statistics.
void TJob::CollectSensorsFromStatistics(ISensorWriter* writer)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    IMapNodePtr statisticsNode;
    try {
        statisticsNode = ConvertTo<IMapNodePtr>(StatisticsYson_);
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(
            TError(ex),
            "Failed to convert statistics to map node (JobId: %v, OperationId: %v)",
            GetId(),
            GetOperationId());
        return;
    }

    const auto& supportedStatisticSensors = CommonConfig_->UserJobMonitoring->StatisticSensors;
    for (const auto& [sensorName, sensor] : supportedStatisticSensors) {
        if (!RequestedMonitoringSensors_.contains(sensorName)) {
            continue;
        }

        // NB(omgronny): We write zeros in case the user job hasn't started yet, e.g. the job is preparing.
        if (statisticsNode->GetChildCount() == 0) {
            ProfileSensor(writer, sensor, 0);
            continue;
        }

        INodePtr node;
        try {
            node = FindNodeByYPath(statisticsNode, YPathJoin(sensor->Path, "last"));
            if (!node) {
                YT_LOG_DEBUG("Statistics node not found (SensorName: %v, Path: %v)", sensorName, sensor->Path);
                continue;
            }
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(
                TError(ex),
                "Error looking for statistics node (SensorName: %v, Path: %v)",
                sensorName,
                sensor->Path);
            continue;
        }

        if (node->GetType() != ENodeType::Int64) {
            YT_LOG_DEBUG(
                "Wrong type of sensor (SensorName: %v, ExpectedType: %v, ActualType: %v)",
                sensorName,
                ENodeType::Int64,
                node->GetType());
            continue;
        }

        ProfileSensor(writer, sensor, node->AsInt64()->GetValue());
    }
}

// NB(eshcherbin): When adding new sensors, do not forget to update |GetSupportedGpuMonitoringSensorNames| above.
void TJob::CollectSensorsFromGpuAndRdmaDeviceInfo(ISensorWriter* writer)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    auto gpuSlots = GetGpuSlots();
    if (gpuSlots.empty()) {
        return;
    }

    auto profileSensorIfNeeded = [&] (const TString& name, auto value) {
        if (RequestedMonitoringSensors_.contains(name)) {
            const auto& sensor = GetOrCrash(GetSupportedGpuMonitoringSensors(), name);
            ProfileSensor(writer, sensor, value);
        }
    };

    auto gpuInfoMap = Bootstrap_->GetGpuManager()->GetGpuInfoMap();
    for (int index = 0; index < std::ssize(gpuSlots); ++index) {
        auto slot = gpuSlots[index];

        auto gpuInfo = GetOrDefault(gpuInfoMap, slot->GetDeviceIndex(), NGpu::TGpuInfo{});

        TWithTagGuard tagGuard(writer, "gpu_slot", ToString(index));

        profileSensorIfNeeded(GpuUtilizationGpuSensorName, gpuInfo.UtilizationGpuRate);
        profileSensorIfNeeded(GpuUtilizationMemorySensorName, gpuInfo.UtilizationMemoryRate);
        profileSensorIfNeeded(GpuMemorySensorName, gpuInfo.MemoryUsed);
        profileSensorIfNeeded(
            GpuUtilizationPowerSensorName,
            gpuInfo.PowerLimit == 0.0
                ? 0.0
                : gpuInfo.PowerDraw / gpuInfo.PowerLimit);
        profileSensorIfNeeded(GpuPowerSensorName, gpuInfo.PowerDraw);
        profileSensorIfNeeded(GpuSMUtilizationSensorName, gpuInfo.SMUtilizationRate);
        profileSensorIfNeeded(GpuSMOccupancySensorName, gpuInfo.SMOccupancyRate);
        profileSensorIfNeeded(GpuNvlinkRxBytesSensorName, gpuInfo.NvlinkRxByteRate);
        profileSensorIfNeeded(GpuNvlinkTxBytesSensorName, gpuInfo.NvlinkTxByteRate);
        profileSensorIfNeeded(GpuPcieRxBytesSensorName, gpuInfo.PcieRxByteRate);
        profileSensorIfNeeded(GpuPcieTxBytesSensorName, gpuInfo.PcieTxByteRate);
        profileSensorIfNeeded(GpuStuckSensorName, static_cast<double>(gpuInfo.Stuck.Status));
        profileSensorIfNeeded(GpuTensorActivitySensorName, gpuInfo.TensorActivityRate);
        profileSensorIfNeeded(GpuDramActivitySensorName, gpuInfo.DramActivityRate);
    }


    if (IsFullHostGpuJob()) {
        auto rdmaDevices = Bootstrap_->GetGpuManager()->GetRdmaDevices();
        for (const auto& rdmaDevice : rdmaDevices) {
            TWithTagGuard tagGuard(writer, "rdma_device", rdmaDevice.Name);

            profileSensorIfNeeded(GpuRdmaRxBytesSensorName, rdmaDevice.RxByteRate);
            profileSensorIfNeeded(GpuRdmaTxBytesSensorName, rdmaDevice.TxByteRate);
        }
    }
}

TFuture<TSharedRef> TJob::DumpSensors()
{
    auto jobProbe = GetJobProbeOrThrow();

    return BIND([jobProbe] {
        return jobProbe->DumpSensors();
    })
        .AsyncVia(Invoker_)
        .Run()
        .WithTimeout(CommonConfig_->SensorDumpTimeout);
}

bool TJob::NeedsGpuCheck() const
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return UserJobSpec_ && UserJobSpec_->has_gpu_check_binary_path();
}

i64 TJob::GetJobProxyHeartbeatEpoch() const
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return JobProxyHearbeatEpoch_;
}

bool TJob::UpdateJobProxyHearbeatEpoch(i64 epoch)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    YT_VERIFY(epoch >= 0);
    if (JobProxyHearbeatEpoch_ >= epoch) {
        return false;
    }
    JobProxyHearbeatEpoch_ = epoch;
    return true;
}

const std::vector<NScheduler::TTmpfsVolumeConfigPtr>& TJob::GetTmpfsVolumeInfos() const noexcept
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return TmpfsVolumeInfos_;
}

bool TJob::HasUserJobSpec() const noexcept
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return HasUserJobSpec_;
}

std::vector<NScheduler::TTmpfsVolumeConfigPtr> TJob::ParseTmpfsVolumeInfos(
    const NControllerAgent::NProto::TUserJobSpec* maybeUserJobSpec)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    if (!maybeUserJobSpec) {
        return {};
    }

    std::vector<TTmpfsVolumeConfigPtr> result;
    using std::size;
    result.reserve(size(maybeUserJobSpec->tmpfs_volumes()));

    for (const auto& volume : maybeUserJobSpec->tmpfs_volumes()) {
        result.push_back(New<TTmpfsVolumeConfig>());

        FromProto(result.back().Get(), volume);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

TJobPtr CreateJob(
    TJobId jobId,
    TOperationId operationId,
    TAllocationPtr allocation,
    TJobSpec&& jobSpec,
    TControllerAgentDescriptor agentDescriptor,
    IBootstrap* bootstrap,
    const TJobCommonConfigPtr& commonConfig)
{
    return NewWithOffloadedDtor<TJob>(
        bootstrap->GetJobInvoker(),
        jobId,
        operationId,
        std::move(allocation),
        std::move(jobSpec),
        std::move(agentDescriptor),
        bootstrap,
        commonConfig);
}

////////////////////////////////////////////////////////////////////////////////

void FillJobStatus(NControllerAgent::NProto::TJobStatus* status, const TJobPtr& job)
{
    using NYT::ToProto;

    ToProto(status->mutable_job_id(), job->GetId());
    ToProto(status->mutable_operation_id(), job->GetOperationId());

    status->set_job_type(ToProto(job->GetType()));
    status->set_state(ToProto(job->GetState()));
    status->set_phase(ToProto(job->GetPhase()));
    status->set_job_execution_completed(job->IsJobProxyCompleted());
    status->set_interruption_reason(ToProto(job->GetInterruptionReason()));
    status->set_progress(job->GetProgress());
    *status->mutable_total_input_data_statistics() = job->GetTotalInputDataStatistics();
    ToProto(status->mutable_time_statistics(), job->GetTimeStatistics());

    ToProto(status->mutable_output_data_statistics(), job->GetOutputDataStatistics());

    if (auto stderrSize = job->GetStderrSize(); stderrSize > 0) {
        status->set_stderr_size(stderrSize);
    }
    if (const auto& preemptedFor = job->GetPreemptedFor()) {
        ToProto(status->mutable_preempted_for(), *preemptedFor);
    }

    if (auto startTime = job->GetStartTime()) {
        status->set_start_time(startTime->GetValue());
    }

    status->set_status_timestamp(ToProto(TInstant::Now()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
