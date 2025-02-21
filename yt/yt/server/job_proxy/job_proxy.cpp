#include "job_proxy.h"
#include "cpu_monitor.h"
#include "merge_job.h"
#include "partition_job.h"
#include "partition_sort_job.h"
#include "remote_copy_job.h"
#include "simple_sort_job.h"
#include "sorted_merge_job.h"
#include "user_job.h"
#include "user_job_write_controller.h"
#include "job_prober_service.h"
#include "job_throttler.h"
#include "shallow_merge_job.h"

#include <yt/yt/server/lib/exec_node/helpers.h>

#include <yt/yt/server/lib/controller_agent/helpers.h>
#include <yt/yt/server/lib/controller_agent/statistics.h>

#include <yt/yt/server/lib/exec_node/config.h>
#include <yt/yt/server/lib/exec_node/proto/supervisor_service.pb.h>

#include <yt/yt/server/lib/rpc_proxy/access_checker.h>
#include <yt/yt/server/lib/rpc_proxy/api_service.h>
#include <yt/yt/server/lib/rpc_proxy/proxy_coordinator.h>
#include <yt/yt/server/lib/rpc_proxy/security_manager.h>

#include <yt/yt/server/exec/user_job_synchronizer.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/auth/native_authentication_manager.h>
#include <yt/yt/ytlib/auth/tvm_bridge.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/yt/ytlib/chunk_client/config.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/job_spec_extensions.h>
#include <yt/yt/ytlib/chunk_client/traffic_meter.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>

#include <yt/yt/ytlib/program/helpers.h>

#include <yt/yt/ytlib/controller_agent/helpers.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/job_proxy/config.h>
#include <yt/yt/ytlib/job_proxy/job_spec_helper.h>

#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/ytlib/node_tracker_client/helpers.h>
#include <yt/yt/ytlib/node_tracker_client/node_directory_synchronizer.h>

#include <yt/yt/ytlib/queue_client/registration_manager.h>

#include <yt/yt/library/auth/credentials_injecting_channel.h>

#include <yt/yt/library/auth_server/authentication_manager.h>
#include <yt/yt/library/auth_server/public.h>

#include <yt/yt/library/containers/public.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/library/program/program.h>

#include <yt/yt/library/tracing/jaeger/sampler.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/table_client/config.h>
#include <yt/yt/client/table_client/column_rename_descriptor.h>

#include <yt/yt/core/bus/tcp/client.h>
#include <yt/yt/core/bus/tcp/dispatcher.h>
#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>
#include <yt/yt/core/concurrency/thread_pool_poller.h>

#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/memory_usage_tracker.h>
#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/ref_counted_tracker.h>

#include <yt/yt/core/rpc/authenticator.h>
#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/bus/server.h>
#include <yt/yt/core/rpc/helpers.h>
#include <yt/yt/core/rpc/retrying_channel.h>
#include <yt/yt/core/rpc/server.h>

#include <yt/yt/core/ytree/public.h>
#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/ypath/token.h>

#include <yt/yt/library/dns_over_rpc/client/dns_over_rpc_resolver.h>

#include <util/system/fs.h>
#include <util/system/execpath.h>

#include <util/folder/dirut.h>

#include <sys/resource.h>
#include <sys/time.h>

namespace NYT::NJobProxy {

using namespace NScheduler;
using namespace NExecNode;
using namespace NExecNode::NProto;
using namespace NBus;
using namespace NRpc;
using namespace NRpcProxy;
using namespace NApi;
using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NChunkClient;
using namespace NJobProber;
using namespace NJobProberClient;
using namespace NJobProxy;
using namespace NControllerAgent;
using namespace NControllerAgent::NProto;
using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NContainers;
using namespace NProfiling;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

void FillStatistics(auto& req, const IJobPtr& job, const TStatistics& enrichedStatistics)
{
    auto extendedStatistics = job->GetStatistics();
    req->set_statistics(ConvertToYsonString(enrichedStatistics).ToString());
    *req->mutable_total_input_data_statistics() = std::move(extendedStatistics.TotalInputStatistics.DataStatistics);
    for (auto& statistics : extendedStatistics.OutputStatistics) {
        *req->add_output_data_statistics() = std::move(statistics.DataStatistics);
    }
}

////////////////////////////////////////////////////////////////////////////////

}

////////////////////////////////////////////////////////////////////////////////

TJobProxy::TJobProxy(
    TJobProxyConfigPtr config,
    TOperationId operationId,
    TJobId jobId)
    : Config_(std::move(config))
    , OperationId_(operationId)
    , JobId_(jobId)
    , JobThread_(New<TActionQueue>("JobMain"))
    , ControlThread_(New<TActionQueue>("Control"))
    , Logger(JobProxyLogger.WithTag("OperationId: %v, JobId: %v",
        OperationId_,
        JobId_))
{
    if (Config_->AbortOnUnrecognizedOptions) {
        AbortOnUnrecognizedOptions(Logger, Config_);
    } else {
        WarnForUnrecognizedOptions(Logger, Config_);
    }
}

TString TJobProxy::GetPreparationPath() const
{
    return NFs::CurrentWorkingDirectory();
}

TString TJobProxy::GetSlotPath() const
{
    if ((!Config_->RootPath && !Config_->DockerImage) || Config_->TestRootFS) {
        return NFs::CurrentWorkingDirectory();
    }

    return "/slot";
}

TString TJobProxy::GetJobProxyUnixDomainSocketPath() const
{
    return AdjustPath(*Config_->BusServer->UnixDomainSocketPath);
}

std::vector<NChunkClient::TChunkId> TJobProxy::DumpInputContext()
{
    auto job = GetJobOrThrow();
    return job->DumpInputContext();
}

TString TJobProxy::GetStderr()
{
    auto job = GetJobOrThrow();
    return job->GetStderr();
}

TPollJobShellResponse TJobProxy::PollJobShell(
    const TJobShellDescriptor& jobShellDescriptor,
    const TYsonString& parameters)
{
    auto job = GetJobOrThrow();
    return job->PollJobShell(jobShellDescriptor, parameters);
}

void TJobProxy::Interrupt()
{
    auto job = GetJobOrThrow();
    job->Interrupt();
}

void TJobProxy::Fail()
{
    auto job = GetJobOrThrow();
    job->Fail();
}

TSharedRef TJobProxy::DumpSensors()
{
    return SolomonExporter_->DumpSensors();
}

IServerPtr TJobProxy::GetRpcServer() const
{
    return RpcServer_;
}

TTrafficMeterPtr TJobProxy::GetTrafficMeter() const
{
    return TrafficMeter_;
}

IThroughputThrottlerPtr TJobProxy::GetInBandwidthThrottler() const
{
    return InBandwidthThrottler_;
}

IThroughputThrottlerPtr TJobProxy::GetOutBandwidthThrottler() const
{
    return OutBandwidthThrottler_;
}

IThroughputThrottlerPtr TJobProxy::GetOutRpsThrottler() const
{
    return OutRpsThrottler_;
}

IThroughputThrottlerPtr TJobProxy::GetUserJobContainerCreationThrottler() const
{
    return UserJobContainerCreationThrottler_;
}

void TJobProxy::SendHeartbeat()
{
    auto job = FindJob();
    if (!job) {
        return;
    }

    YT_LOG_DEBUG("Reporting heartbeat to supervisor");

    auto req = SupervisorProxy_->OnJobProgress();
    ToProto(req->mutable_job_id(), JobId_);
    req->set_progress(job->GetProgress());
    FillStatistics(req, job, GetEnrichedStatistics());
    req->set_stderr_size(job->GetStderrSize());

    req->Invoke().Subscribe(BIND(&TJobProxy::OnHeartbeatResponse, MakeWeak(this)));
}

void TJobProxy::OnHeartbeatResponse(const TError& error)
{
    if (!error.IsOK()) {
        // NB: user process is not killed here.
        // Good user processes are supposed to die themselves
        // when io pipes are closed.
        // Bad processes will die at container shutdown.
        YT_LOG_ERROR(error, "Error sending heartbeat to supervisor");
        Abort(EJobProxyExitCode::SupervisorCommunicationFailed);
    }

    YT_LOG_DEBUG("Successfully reported heartbeat to supervisor");
}

void TJobProxy::LogJobSpec(TJobSpec jobSpec)
{
    // We patch copy of job spec for better reading experience.
    if (jobSpec.HasExtension(TJobSpecExt::job_spec_ext)) {
        auto* jobSpecExt = jobSpec.MutableExtension(TJobSpecExt::job_spec_ext);
        // Some fields are serialized in binary YSON, let's make them human readable.
        #define TRANSFORM_TO_PRETTY_YSON(object, field) \
            if (object->has_ ## field() && !object->field().empty()) { \
                object->set_ ## field(ConvertToYsonString(TYsonString(object->field()), EYsonFormat::Pretty).ToString()); \
            }
        TRANSFORM_TO_PRETTY_YSON(jobSpecExt, io_config);
        TRANSFORM_TO_PRETTY_YSON(jobSpecExt, table_reader_options);
        TRANSFORM_TO_PRETTY_YSON(jobSpecExt, acl);
        TRANSFORM_TO_PRETTY_YSON(jobSpecExt, job_cpu_monitor_config);
        TRANSFORM_TO_PRETTY_YSON(jobSpecExt, testing_options);

        // Input table specs may be large, so we print them separately.
        NProtoBuf::RepeatedPtrField<TTableInputSpec> primaryInputTableSpecs;
        jobSpecExt->mutable_input_table_specs()->Swap(&primaryInputTableSpecs);

        NProtoBuf::RepeatedPtrField<TTableInputSpec> foreignInputTableSpecs;
        jobSpecExt->mutable_foreign_input_table_specs()->Swap(&foreignInputTableSpecs);

        for (const auto& [name, inputTableSpecs] : {
            std::make_pair("primary", &primaryInputTableSpecs),
            std::make_pair("foreign", &foreignInputTableSpecs)
        })
        {
            TStringBuilder builder;
            for (const auto& tableSpec : *inputTableSpecs) {
                builder.AppendString(tableSpec.DebugString());
                builder.AppendChar('\n');
            }
            YT_LOG_DEBUG("Job spec %v input table specs:\n%v",
                name,
                builder.Flush());
        }

        // Node directory is huge and useless when debugging.
        jobSpecExt->clear_input_node_directory();
    }

    NControllerAgent::SanitizeJobSpec(&jobSpec);

    YT_LOG_DEBUG("Job spec:\n%v", jobSpec.DebugString());
}

// COMPAT(levysotsky): This function is to be removed after both CA and nodes are updated.
// See YT-16507
static NTableClient::TTableSchemaPtr SetStableNames(
    const NTableClient::TTableSchemaPtr& schema,
    const NTableClient::TColumnRenameDescriptors& renameDescriptors)
{
    THashMap<TString, TString> nameToStableName;
    for (const auto& renameDescriptor : renameDescriptors) {
        nameToStableName.emplace(renameDescriptor.NewName, renameDescriptor.OriginalName);
    }

    std::vector<NTableClient::TColumnSchema> columns;
    for (const auto& originalColumn : schema->Columns()) {
        auto& column = columns.emplace_back(originalColumn);
        YT_VERIFY(!column.IsRenamed());
        if (auto it = nameToStableName.find(column.Name())) {
            column.SetStableName(NTableClient::TStableName(it->second));
        }
    }
    return New<NTableClient::TTableSchema>(
        std::move(columns),
        schema->GetStrict(),
        schema->GetUniqueKeys(),
        schema->GetSchemaModification(),
        schema->DeletedColumns());
}

// COMPAT(levysotsky): We need to distinguish between two cases:
// 1) New CA has sent already renamed schema, we check it and do nothing
// 2) Old CA has sent not-renamed schema, we need to perform the renaming
//    according to rename descriptors.
// This function is to be removed after both CA and nodes are updated. See YT-16507
static IJobSpecHelperPtr MaybePatchDataSourceDirectory(
    const TJobSpec& jobSpecProto)
{
    auto jobSpecExt = jobSpecProto.GetExtension(TJobSpecExt::job_spec_ext);

    if (!HasProtoExtension<NChunkClient::NProto::TDataSourceDirectoryExt>(jobSpecExt.extensions())) {
        return CreateJobSpecHelper(jobSpecProto);
    }
    const auto dataSourceDirectoryExt = GetProtoExtension<NChunkClient::NProto::TDataSourceDirectoryExt>(
        jobSpecExt.extensions());

    auto dataSourceDirectory = FromProto<TDataSourceDirectoryPtr>(dataSourceDirectoryExt);

    for (auto& dataSource : dataSourceDirectory->DataSources()) {
        if (dataSource.Schema() && dataSource.Schema()->HasRenamedColumns()) {
            return CreateJobSpecHelper(jobSpecProto);
        }
    }

    for (auto& dataSource : dataSourceDirectory->DataSources()) {
        if (!dataSource.Schema()) {
            dataSource.Schema() = New<NTableClient::TTableSchema>();
        } else {
            dataSource.Schema() = SetStableNames(
                dataSource.Schema(),
                dataSource.ColumnRenameDescriptors());
        }
    }

    NChunkClient::NProto::TDataSourceDirectoryExt newExt;
    ToProto(&newExt, dataSourceDirectory);
    SetProtoExtension<NChunkClient::NProto::TDataSourceDirectoryExt>(
        jobSpecExt.mutable_extensions(),
        std::move(newExt));

    auto jobSpecProtoCopy = jobSpecProto;
    auto* mutableExt = jobSpecProtoCopy.MutableExtension(TJobSpecExt::job_spec_ext);
    *mutableExt = std::move(jobSpecExt);
    return CreateJobSpecHelper(jobSpecProtoCopy);
}

void TJobProxy::RetrieveJobSpec()
{
    YT_LOG_INFO("Requesting job spec");

    auto req = SupervisorProxy_->GetJobSpec();
    ToProto(req->mutable_job_id(), JobId_);

    auto rspOrError = WaitFor(req->Invoke());
    if (!rspOrError.IsOK()) {
        YT_LOG_ERROR(rspOrError, "Failed to get job spec");
        Abort(EJobProxyExitCode::GetJobSpecFailed);
    }

    const auto& rsp = rspOrError.Value();

    if (rsp->job_spec().version() != GetJobSpecVersion()) {
        YT_LOG_WARNING("Invalid job spec version (Expected: %v, Actual: %v)",
            GetJobSpecVersion(),
            rsp->job_spec().version());
        Abort(EJobProxyExitCode::InvalidSpecVersion);
    }

    JobSpecHelper_ = MaybePatchDataSourceDirectory(rsp->job_spec());

    const auto& resourceUsage = rsp->resource_usage();

    Ports_ = FromProto<std::vector<int>>(rsp->ports());

    auto authenticatedUser = GetJobSpecHelper()->GetJobSpecExt().authenticated_user();
    YT_LOG_INFO("Job spec received (JobType: %v, AuthenticatedUser: %v, ResourceLimits: {Cpu: %v, Memory: %v, Network: %v})",
        NScheduler::EJobType(rsp->job_spec().type()),
        authenticatedUser,
        resourceUsage.cpu(),
        resourceUsage.memory(),
        resourceUsage.network());

    // Job spec is copied intentionally.
    LogJobSpec(rsp->job_spec());

    auto totalMemoryReserve = resourceUsage.memory();
    CpuGuarantee_ = resourceUsage.cpu();
    NetworkUsage_ = resourceUsage.network();

    // We never report to node less memory usage, than was initially reserved.
    TotalMaxMemoryUsage_ = totalMemoryReserve - Config_->AheadMemoryReserve;
    ApprovedMemoryReserve_ = totalMemoryReserve;
    RequestedMemoryReserve_ = totalMemoryReserve;

    const auto& jobSpecExt = JobSpecHelper_->GetJobSpecExt();
    if (jobSpecExt.has_user_job_spec()) {
        const auto& userJobSpec = jobSpecExt.user_job_spec();
        JobProxyMemoryReserve_ = totalMemoryReserve - userJobSpec.memory_reserve();
        YT_LOG_DEBUG("Adjusting job proxy memory limit (JobProxyMemoryReserve: %v, UserJobMemoryReserve: %v)",
            JobProxyMemoryReserve_,
            userJobSpec.memory_reserve());
    } else {
        JobProxyMemoryReserve_ = totalMemoryReserve;
    }

    std::vector<TString> annotations{
        Format("Type: SchedulerJob"),
        Format("OperationId: %v", OperationId_),
        Format("JobId: %v", JobId_),
        Format("JobType: %v", GetJobSpecHelper()->GetJobType()),
        Format("AuthenticatedUser: %v", authenticatedUser),
    };

    for (auto* descriptor : {
        &GetJobSpecHelper()->GetJobIOConfig()->TableReader->WorkloadDescriptor,
        &GetJobSpecHelper()->GetJobIOConfig()->TableWriter->WorkloadDescriptor,
        &GetJobSpecHelper()->GetJobIOConfig()->ErrorFileWriter->WorkloadDescriptor
    })
    {
        descriptor->Annotations.insert(
            descriptor->Annotations.end(),
            annotations.begin(),
            annotations.end());
        descriptor->DiskFairShareBucketTag = ToString(JobId_);
        descriptor->DiskFairShareBucketWeight = CpuGuarantee_;
    }

    {
        const auto& blockCacheConfig = GetJobSpecHelper()->GetJobIOConfig()->BlockCache;
        blockCacheConfig->CompressedData->ShardCount = 1;
        blockCacheConfig->UncompressedData->ShardCount = 1;

        ReaderBlockCache_ = CreateClientBlockCache(
            GetJobSpecHelper()->GetJobIOConfig()->BlockCache,
            EBlockType::CompressedData | EBlockType::UncompressedData);
    }
}

void TJobProxy::DoRun()
{
    LastMemoryMeasureTime_ = Now();
    auto startTime = Now();
    auto resultOrError = WaitFor(BIND(&TJobProxy::RunJob, MakeStrong(this))
        .AsyncVia(JobThread_->GetInvoker())
        .Run());
    auto finishTime = Now();

    TJobResult result;
    if (!resultOrError.IsOK()) {
        YT_LOG_ERROR(resultOrError, "Job failed");
        ToProto(result.mutable_error(), resultOrError);
    } else {
        result = resultOrError.Value();
    }

    // Reliably terminate all async calls before reporting result.
    if (HeartbeatExecutor_) {
        YT_LOG_INFO("Stopping heartbeat executor");
        WaitFor(HeartbeatExecutor_->Stop())
            .ThrowOnError();
        YT_LOG_INFO("Heartbeat executor stopped");
    }

    if (MemoryWatchdogExecutor_) {
        YT_LOG_INFO("Stopping memory watchdog executor");
        WaitFor(MemoryWatchdogExecutor_->Stop())
            .ThrowOnError();
        YT_LOG_INFO("Memory watchdog executor stopped");
    }

    if (CpuMonitor_) {
        YT_LOG_INFO("Stopping CPU monitor");
        WaitFor(CpuMonitor_->Stop())
            .ThrowOnError();
        YT_LOG_INFO("CPU monitor stopped");
    }

    {
        auto error = WaitFor(RpcServer_->Stop()
            .WithTimeout(RpcServerShutdownTimeout));
        YT_LOG_ERROR_UNLESS(error.IsOK(), error, "Error stopping RPC server");
    }

    FillJobResult(&result);
    FillStderrResult(&result);
    ReportResult(
        result,
        startTime,
        finishTime);

    SetJob(nullptr);

    if (auto tracer = GetGlobalTracer()) {
        tracer->Stop();
    }

    LogSystemStats();
}

void TJobProxy::Run()
{
    try {
        DoRun();
    } catch (const std::exception& ex) {
        if (Config_->AbortOnUncaughtException) {
            YT_LOG_FATAL(ex, "Abort on uncaught exception");
        } else {
            throw;
        }
    }
}

IJobPtr TJobProxy::CreateBuiltinJob()
{
    auto jobType = GetJobSpecHelper()->GetJobType();
    switch (jobType) {
        case NScheduler::EJobType::OrderedMerge:
            return CreateOrderedMergeJob(this);

        case NScheduler::EJobType::UnorderedMerge:
            return CreateUnorderedMergeJob(this);

        case NScheduler::EJobType::SortedMerge:
            return CreateSortedMergeJob(this);

        case NScheduler::EJobType::FinalSort:
        case NScheduler::EJobType::IntermediateSort:
            return CreatePartitionSortJob(this);

        case NScheduler::EJobType::SimpleSort:
            return CreateSimpleSortJob(this);

        case NScheduler::EJobType::Partition:
            return CreatePartitionJob(this);

        case NScheduler::EJobType::RemoteCopy:
            return CreateRemoteCopyJob(this);

        case NScheduler::EJobType::ShallowMerge:
            return CreateShallowMergeJob(this);

        default:
            YT_ABORT();
    }
}

TString TJobProxy::AdjustPath(const TString& path) const
{
    YT_VERIFY(path.StartsWith(GetPreparationPath()));
    auto pathSuffix = path.substr(GetPreparationPath().size() + 1);
    auto adjustedPath = NFS::CombinePaths(GetSlotPath(), pathSuffix);
    return adjustedPath;
}

void TJobProxy::UpdateCumulativeMemoryUsage(i64 memoryUsage)
{
    auto now = Now();

    if (now <= LastMemoryMeasureTime_) {
        return;
    }

    CumulativeMemoryUsageMBSec_ += memoryUsage * (now - LastMemoryMeasureTime_).SecondsFloat() / 1_MB;
    LastMemoryMeasureTime_ = now;
}

void TJobProxy::SetJob(IJobPtr job)
{
    Job_.Store(std::move(job));
}

IJobPtr TJobProxy::FindJob() const
{
    return Job_.Acquire();
}

IJobPtr TJobProxy::GetJobOrThrow()
{
    auto job = FindJob();
    if (!job) {
        THROW_ERROR_EXCEPTION("Job is not initialized yet");
    }
    return job;
}

void TJobProxy::SetJobProxyEnvironment(IJobProxyEnvironmentPtr environment)
{
    JobProxyEnvironment_.Store(std::move(environment));
}

void TJobProxy::EnableRpcProxyInJobProxy(int rpcProxyWorkerThreadPoolSize)
{
    YT_VERIFY(Config_->OriginalClusterConnection);
    NLogging::TLogger proxyLogger("RpcProxy");
    auto connection = CreateNativeConnection(Config_->OriginalClusterConnection);
    connection->GetClusterDirectorySynchronizer()->Start();
    connection->GetNodeDirectorySynchronizer()->Start();
    connection->GetQueueConsumerRegistrationManager()->StartSync();
    auto rootClient = connection->CreateNativeClient(TClientOptions::FromUser(NSecurityClient::RootUserName));

    auto proxyCoordinator = CreateProxyCoordinator();
    proxyCoordinator->SetAvailableState(true);
    auto securityManager = CreateSecurityManager(Config_->ApiService->SecurityManager, connection, proxyLogger);
    auto authenticationManager = NAuth::CreateAuthenticationManager(
        Config_->AuthenticationManager,
        NYT::NBus::TTcpDispatcher::Get()->GetXferPoller(),
        rootClient);
    ApiServiceThreadPool_ = CreateThreadPool(rpcProxyWorkerThreadPoolSize, "RpcProxy");
    auto ApiService_ = CreateApiService(
        Config_->ApiService,
        GetControlInvoker(),
        ApiServiceThreadPool_->GetInvoker(),
        connection,
        authenticationManager->GetRpcAuthenticator(),
        proxyCoordinator,
        CreateNoopAccessChecker(),
        securityManager,
        New<TSampler>(),
        proxyLogger,
        TProfiler());
    GetRpcServer()->RegisterService(ApiService_);
    YT_LOG_INFO("RPC proxy API service registered (ThreadCount: %v)", rpcProxyWorkerThreadPoolSize);
}

IJobProxyEnvironmentPtr TJobProxy::FindJobProxyEnvironment() const
{
    return JobProxyEnvironment_.Acquire();
}

TJobResult TJobProxy::RunJob()
{
    RootSpan_ = TTraceContext::NewRoot("Job");
    RootSpan_->SetRecorded();
    RootSpan_->AddTag("yt.job_id", ToString(JobId_));
    TTraceContextGuard guard(RootSpan_);

    IJobPtr job;

    try {
        if (Config_->TvmBridge && Config_->TvmBridgeConnection) {
            auto tvmBridgeClient = CreateBusClient(Config_->TvmBridgeConnection);
            auto tvmBridgeChannel = NRpc::NBus::CreateBusChannel(tvmBridgeClient);

            TvmBridge_ = NAuth::CreateTvmBridge(GetControlInvoker(), tvmBridgeChannel, Config_->TvmBridge);
            NAuth::TNativeAuthenticationManager::Get()->SetTvmService(TvmBridge_);
        }

        SolomonExporter_ = New<TSolomonExporter>(Config_->SolomonExporter);

        auto environment = CreateJobProxyEnvironment(Config_->JobEnvironment);
        SetJobProxyEnvironment(environment);

        LocalDescriptor_ = NNodeTrackerClient::TNodeDescriptor(Config_->Addresses, Config_->LocalHostName, Config_->Rack, Config_->DataCenter);

        TrafficMeter_ = New<TTrafficMeter>(LocalDescriptor_.GetDataCenter());
        TrafficMeter_->Start();

        YT_VERIFY(Config_->BusServer->UnixDomainSocketPath);

        RpcServer_ = NRpc::NBus::CreateBusServer(CreateBusServer(Config_->BusServer));
        RpcServer_->RegisterService(CreateJobProberService(this, GetControlInvoker()));
        RpcServer_->Start();

        if (TvmBridge_) {
            YT_LOG_DEBUG("Ensuring destination service id (ServiceId: %v)", TvmBridge_->GetSelfTvmId());

            WaitFor(TvmBridge_->EnsureDestinationServiceIds({TvmBridge_->GetSelfTvmId()}))
                .ThrowOnError();

            YT_LOG_DEBUG("Destination service id is ready");
        }

        auto supervisorClient = CreateBusClient(Config_->SupervisorConnection);
        auto supervisorChannel = NRpc::NBus::CreateBusChannel(supervisorClient);
        if (TvmBridge_) {
            auto serviceTicketAuth = CreateServiceTicketAuth(TvmBridge_, TvmBridge_->GetSelfTvmId());
            supervisorChannel = CreateServiceTicketInjectingChannel(
                std::move(supervisorChannel),
                NAuth::TAuthenticationOptions::FromServiceTicketAuth(serviceTicketAuth));
        }

        if (Config_->DnsOverRpcResolver) {
            YT_LOG_INFO("Installing DNS-over-RPC resolver");
            auto dnsResolver = NDns::CreateDnsOverRpcResolver(Config_->DnsOverRpcResolver, supervisorChannel);
            NNet::TAddressResolver::Get()->SetDnsResolver(std::move(dnsResolver));
            // This is to enable testing the feature.
            NNet::TAddressResolver::Get()->PurgeCache();
        }

        SupervisorProxy_ = std::make_unique<TSupervisorServiceProxy>(supervisorChannel);
        SupervisorProxy_->SetDefaultTimeout(Config_->SupervisorRpcTimeout);

        RetrieveJobSpec();

        auto clusterConnection = CreateNativeConnection(Config_->ClusterConnection);
        Client_ = clusterConnection->CreateNativeClient(TClientOptions::FromUser(GetAuthenticatedUser()));

        PackBaggageFromJobSpec(RootSpan_, JobSpecHelper_->GetJobSpec(), OperationId_, JobId_);

        auto cpuMonitorConfig = ConvertTo<TJobCpuMonitorConfigPtr>(TYsonString(JobSpecHelper_->GetJobSpecExt().job_cpu_monitor_config()));
        CpuMonitor_ = New<TCpuMonitor>(std::move(cpuMonitorConfig), JobThread_->GetInvoker(), this, CpuGuarantee_);

        if (Config_->JobThrottler) {
            YT_LOG_INFO("Job throttling enabled");

            InBandwidthThrottler_ = CreateInJobBandwidthThrottler(
                Config_->JobThrottler,
                supervisorChannel,
                GetJobSpecHelper()->GetJobIOConfig()->TableReader->WorkloadDescriptor,
                JobId_,
                Logger);

            OutBandwidthThrottler_ = CreateOutJobBandwidthThrottler(
                Config_->JobThrottler,
                supervisorChannel,
                GetJobSpecHelper()->GetJobIOConfig()->TableWriter->WorkloadDescriptor,
                JobId_,
                Logger);

            OutRpsThrottler_ = CreateOutJobRpsThrottler(
                Config_->JobThrottler,
                supervisorChannel,
                GetJobSpecHelper()->GetJobIOConfig()->TableWriter->WorkloadDescriptor,
                JobId_,
                Logger);

            UserJobContainerCreationThrottler_ = CreateUserJobContainerCreationThrottler(
                Config_->JobThrottler,
                supervisorChannel,
                GetJobSpecHelper()->GetJobIOConfig()->TableWriter->WorkloadDescriptor,
                JobId_);
        } else {
            YT_LOG_INFO("Job throttling disabled");

            InBandwidthThrottler_ = GetUnlimitedThrottler();
            OutBandwidthThrottler_ = GetUnlimitedThrottler();
            OutRpsThrottler_ = GetUnlimitedThrottler();
            UserJobContainerCreationThrottler_ = GetUnlimitedThrottler();
        }

        const auto& jobSpecExt = GetJobSpecHelper()->GetJobSpecExt();
        if (jobSpecExt.is_traced()) {
            RootSpan_->SetSampled();
        }
        if (jobSpecExt.has_user_job_spec() && jobSpecExt.user_job_spec().has_monitoring_config()) {
            TString jobProxyDescriptor = jobSpecExt.user_job_spec().monitoring_config().job_descriptor();
            NProfiling::TSolomonRegistry::Get()->SetDynamicTags({
                NProfiling::TTag{"job_descriptor", jobProxyDescriptor},
                NProfiling::TTag{"slot_index", ToString(Config_->SlotIndex)}});
        }
        if (jobSpecExt.has_user_job_spec() && jobSpecExt.user_job_spec().enable_rpc_proxy_in_job_proxy()) {
            EnableRpcProxyInJobProxy(jobSpecExt.user_job_spec().rpc_proxy_worker_thread_pool_size());
        }

        JobProxyMemoryOvercommitLimit_ = jobSpecExt.has_job_proxy_memory_overcommit_limit()
            ? std::make_optional(jobSpecExt.job_proxy_memory_overcommit_limit())
            : std::nullopt;

        RefCountedTrackerLogPeriod_ = FromProto<TDuration>(jobSpecExt.job_proxy_ref_counted_tracker_log_period());

        if (environment) {
            environment->SetCpuGuarantee(CpuGuarantee_);
            if (jobSpecExt.has_user_job_spec() &&
                jobSpecExt.user_job_spec().set_container_cpu_limit())
            {
                auto limit = jobSpecExt.user_job_spec().container_cpu_limit() > 0
                    ? jobSpecExt.user_job_spec().container_cpu_limit()
                    : CpuGuarantee_.load();
                environment->SetCpuLimit(limit);
            }

            if (Config_->ForceIdleCpuPolicy) {
                environment->SetCpuPolicy("idle");
            }
        }

        HeartbeatExecutor_ = New<TPeriodicExecutor>(
            JobThread_->GetInvoker(),
            BIND(&TJobProxy::SendHeartbeat, MakeWeak(this)),
            Config_->HeartbeatPeriod);

        auto jobEnvironmentConfig = ConvertTo<TJobEnvironmentConfigPtr>(Config_->JobEnvironment);
        MemoryWatchdogExecutor_ = New<TPeriodicExecutor>(
            JobThread_->GetInvoker(),
            BIND(&TJobProxy::CheckMemoryUsage, MakeWeak(this)),
            jobEnvironmentConfig->MemoryWatchdogPeriod);

        if (jobSpecExt.has_user_job_spec()) {
            job = CreateUserJob(
                this,
                jobSpecExt.user_job_spec(),
                JobId_,
                Ports_,
                std::make_unique<TUserJobWriteController>(this));
        } else {
            job = CreateBuiltinJob();
        }

        SetJob(job);
        job->Initialize();

        OnSpawned();
    } catch (const std::exception& ex) {
        auto isSupervisorProxyTimeoutError = [] (const TError& error) {
            auto serviceAttribute = error.Attributes().Find<std::optional<TString>>("service");
            return error.GetCode() == NYT::EErrorCode::Timeout &&
                serviceAttribute == TSupervisorServiceProxy::GetDescriptor().ServiceName;
        };

        YT_LOG_ERROR(ex, "Failed to prepare job proxy");
        auto error = TError(ex);
        if (error.FindMatching(isSupervisorProxyTimeoutError)) {
            Abort(EJobProxyExitCode::SupervisorCommunicationFailed);
        }
        Abort(EJobProxyExitCode::JobProxyPrepareFailed);
    }

    job->PrepareArtifacts();
    OnArtifactsPrepared();

    if (GetJobSpecHelper()->GetJobTestingOptions()->FailBeforeJobStart) {
        THROW_ERROR_EXCEPTION("Fail before job started");
    }

    MemoryWatchdogExecutor_->Start();
    HeartbeatExecutor_->Start();
    CpuMonitor_->Start();

    return job->Run();
}

NApi::NNative::IConnectionPtr TJobProxy::CreateNativeConnection(NApi::NNative::TConnectionCompoundConfigPtr config)
{
    if (TvmBridge_ && config->Dynamic->TvmId) {
        YT_LOG_DEBUG("Ensuring destination service id (ServiceId: %v)", *config->Dynamic->TvmId);

        WaitFor(TvmBridge_->EnsureDestinationServiceIds({*config->Dynamic->TvmId}))
            .ThrowOnError();

        YT_LOG_DEBUG("Destination service id is ready");
    }

    return NApi::NNative::CreateConnection(std::move(config));
}

void TJobProxy::ReportResult(
    const TJobResult& result,
    TInstant startTime,
    TInstant finishTime)
{
    TTraceContextGuard guard(RootSpan_);

    YT_LOG_INFO("Reporting job result");

    if (!SupervisorProxy_) {
        YT_LOG_ERROR("Supervisor channel is not available");
        Abort(EJobProxyExitCode::ResultReportFailed);
    }

    auto req = SupervisorProxy_->OnJobFinished();
    ToProto(req->mutable_job_id(), JobId_);
    *req->mutable_result() = result;
    req->set_start_time(ToProto<i64>(startTime));
    req->set_finish_time(ToProto<i64>(finishTime));
    auto job = FindJob();
    if (job) {
        FillStatistics(req, job, GetEnrichedStatistics());
    }
    if (job && GetJobSpecHelper()->GetJobSpecExt().has_user_job_spec()) {
        ToProto(req->mutable_core_infos(), job->GetCoreInfos());

        try {
            auto failContext = job->GetFailContext();
            if (failContext) {
                req->set_fail_context(*failContext);
            }
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Failed to get job fail context on teardown");
        }

        try {
            auto stderr = GetStderr();
            if (!std::empty(stderr)) {
                const auto& jobResultExt = result.GetExtension(TJobResultExt::job_result_ext);
                YT_VERIFY(jobResultExt.has_stderr());
            }
            req->set_job_stderr(std::move(stderr));
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Failed to get job stderr on teardown");
        }
    }

    if (auto job = FindJob()) {
        try {
            for (const auto& profile : job->GetProfiles()) {
                auto* protoProfile = req->add_profiles();
                protoProfile->set_type(profile.Type);
                protoProfile->set_blob(profile.Blob);
                protoProfile->set_profiling_probability(profile.ProfilingProbability);
            }
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Failed to get job profile on teardown");
        }
    }

    auto rspOrError = WaitFor(req->Invoke());
    if (!rspOrError.IsOK()) {
        YT_LOG_ERROR(rspOrError, "Failed to report job result");
        Abort(EJobProxyExitCode::ResultReportFailed);
    }

    YT_LOG_INFO("Job result reported");
}

TStatistics TJobProxy::GetEnrichedStatistics() const
{
    TStatistics statistics;

    auto statisticsOutputTableCountLimit = Config_->StatisticsOutputTableCountLimit.value_or(std::numeric_limits<int>::max());

    if (auto job = FindJob()) {
        auto extendedStatistics = job->GetStatistics();
        statistics = std::move(extendedStatistics.Statstics);

        statistics.AddSample("/data/input", extendedStatistics.TotalInputStatistics.DataStatistics);
        DumpCodecStatistics(extendedStatistics.TotalInputStatistics.CodecStatistics, "/codec/cpu/decode", &statistics);
        for (int index = 0; index < std::min<int>(statisticsOutputTableCountLimit, extendedStatistics.OutputStatistics.size()); ++index) {
            auto ypathIndex = ToYPathLiteral(index);
            statistics.AddSample("/data/output/" + ypathIndex, extendedStatistics.OutputStatistics[index].DataStatistics);
            DumpCodecStatistics(extendedStatistics.OutputStatistics[index].CodecStatistics, "/codec/cpu/encode/" + ypathIndex, &statistics);
        }

        DumpChunkReaderStatistics(&statistics, "/chunk_reader_statistics", extendedStatistics.ChunkReaderStatistics);
        DumpTimingStatistics(&statistics, "/chunk_reader_statistics", extendedStatistics.TimingStatistics);

        if (const auto& pipeStatistics = extendedStatistics.PipeStatistics) {
            auto dumpPipeStatistics = [&] (const TYPath& path, const IJob::TStatistics::TPipeStatistics& pipeStatistics) {
                statistics.AddSample(path + "/idle_time", pipeStatistics.ConnectionStatistics.IdleDuration);
                statistics.AddSample(path + "/busy_time", pipeStatistics.ConnectionStatistics.BusyDuration);
                statistics.AddSample(path + "/bytes", pipeStatistics.Bytes);
            };

            dumpPipeStatistics("/user_job/pipes/input", pipeStatistics->InputPipeStatistics);
            dumpPipeStatistics("/user_job/pipes/output/total", pipeStatistics->TotalOutputPipeStatistics);
            for (int index = 0; index < std::min<int>(statisticsOutputTableCountLimit, pipeStatistics->OutputPipeStatistics.size()); ++index) {
                dumpPipeStatistics("/user_job/pipes/output/" + ToYPathLiteral(index), pipeStatistics->OutputPipeStatistics[index]);
            }
        }
    }

    if (auto environment = FindJobProxyEnvironment()) {
        try {
            auto cpuStatistics = environment->GetCpuStatistics()
                .ValueOrThrow();
            statistics.AddSample("/job_proxy/cpu", cpuStatistics);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Unable to get CPU statistics from resource controller");
        }

        try {
            auto blockIOStatistics = environment->GetBlockIOStatistics()
                .ValueOrThrow();
            statistics.AddSample("/job_proxy/block_io", blockIOStatistics);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Unable to get block IO statistics from resource controller");
        }

        // NB(arkady-e1ppa): GetXx methods are noexcept
        // AddSample methods can only throw if path is incorrect.
        // This is impossible for a hardcoded path.
        // If you happen to change path, don't forget to
        // change it here as well and make it compatible.
        RunNoExcept([&] {
            auto jobCpuStatistics = environment->GetJobCpuStatistics();
            if (jobCpuStatistics) {
                statistics.AddSample("/job/cpu", *jobCpuStatistics);
            }

            auto jobMemoryStatistics = environment->GetJobMemoryStatistics();
            if (jobMemoryStatistics) {
                statistics.AddSample("/job/memory", *jobMemoryStatistics);
            }

            auto jobBlockIOStatistics = environment->GetJobBlockIOStatistics();
            if (jobBlockIOStatistics) {
                statistics.AddSample("/job/block_io", *jobBlockIOStatistics);
            }
        });
    }

    if (JobProxyMaxMemoryUsage_ > 0) {
        statistics.AddSample("/job_proxy/max_memory", JobProxyMaxMemoryUsage_);
    }

    if (JobProxyMemoryReserve_ > 0) {
        statistics.AddSample("/job_proxy/memory_reserve", JobProxyMemoryReserve_);
    }

    if (CumulativeMemoryUsageMBSec_ > 0) {
        statistics.AddSample("/job_proxy/cumulative_memory_mb_sec", CumulativeMemoryUsageMBSec_);
    }

    FillTrafficStatistics(JobProxyTrafficStatisticsPrefix, statistics, TrafficMeter_);

    CpuMonitor_->FillStatistics(statistics);

    statistics.SetTimestamp(TInstant::Now());

    return statistics;
}

IUserJobEnvironmentPtr TJobProxy::CreateUserJobEnvironment(const TJobSpecEnvironmentOptions& options) const
{
    auto environment = FindJobProxyEnvironment();
    YT_VERIFY(environment);

    auto createRootFS = [&] () -> std::optional<TRootFS> {
        if (!Config_->RootPath) {
            YT_LOG_INFO("Job is not using custom rootfs");
            return std::nullopt;
        }

        if (Config_->TestRootFS) {
            YT_LOG_INFO("Job is running in testing rootfs mode");
            return std::nullopt;
        }

        YT_LOG_DEBUG("Job is using custom rootfs (Path: %v)", Config_->RootPath);

        TRootFS rootFS {
            .RootPath = *Config_->RootPath,
            .IsRootReadOnly = !Config_->MakeRootFSWritable,
        };

        // Please observe the hierarchy of binds for correct mounting!
        // TODO(don-dron): Make topological sorting.
        rootFS.Binds.push_back(TBind{
            .SourcePath = GetPreparationPath(),
            .TargetPath = GetSlotPath(),
            .ReadOnly = false
        });

        for (const auto& tmpfsPath : Config_->TmpfsManager->TmpfsPaths) {
            rootFS.Binds.push_back(TBind{
                .SourcePath = tmpfsPath,
                .TargetPath = AdjustPath(tmpfsPath),
                .ReadOnly= false
            });
        }

        // Temporary workaround for nirvana - make tmp directories writable.
        auto tmpPath = NFS::CombinePaths(NFs::CurrentWorkingDirectory(), GetSandboxRelPath(ESandboxKind::Tmp));

        rootFS.Binds.push_back(TBind{
            .SourcePath = tmpPath,
            .TargetPath = "/tmp",
            .ReadOnly = false
        });

        rootFS.Binds.push_back(TBind{
            .SourcePath = tmpPath,
            .TargetPath = "/var/tmp",
            .ReadOnly = false
        });

        for (const auto& bind : Config_->Binds) {
            rootFS.Binds.push_back(TBind{
                .SourcePath = bind->ExternalPath,
                .TargetPath = bind->InternalPath,
                .ReadOnly = bind->ReadOnly
            });
        }

        return rootFS;
    };

    TUserJobEnvironmentOptions environmentOptions {
        .RootFS = createRootFS(),
        .GpuIndexes = Config_->GpuIndexes,
        .HostName = Config_->HostName,
        .NetworkAddresses = Config_->NetworkAddresses,
        .EnableNat64 = Config_->EnableNat64,
        .DisableNetwork = Config_->DisableNetwork,
        .EnableCudaGpuCoreDump = options.EnableGpuCoreDumps,
        .EnablePortoMemoryTracking = options.EnablePortoMemoryTracking,
        .EnablePorto = options.EnablePorto,
        .ThreadLimit = options.ThreadLimit
    };

    if (options.EnableCoreDumps) {
        environmentOptions.SlotCoreWatcherDirectory = NFS::CombinePaths({GetSlotPath(), "cores"});
        environmentOptions.CoreWatcherDirectory = NFS::CombinePaths({GetPreparationPath(), "cores"});
    }

    return environment->CreateUserJobEnvironment(
        JobId_,
        environmentOptions);
}

TJobProxyConfigPtr TJobProxy::GetConfig() const
{
    return Config_;
}

TOperationId TJobProxy::GetOperationId() const
{
    return OperationId_;
}

TJobId TJobProxy::GetJobId() const
{
    return JobId_;
}

TString TJobProxy::GetAuthenticatedUser() const
{
    return JobSpecHelper_->GetJobSpecExt().authenticated_user();
}

TString TJobProxy::GetLocalHostName() const
{
    return Config_->LocalHostName;
}

const IJobSpecHelperPtr& TJobProxy::GetJobSpecHelper() const
{
    YT_VERIFY(JobSpecHelper_);
    return JobSpecHelper_;
}

void TJobProxy::UpdateResourceUsage()
{
    // Fire-and-forget.
    auto req = SupervisorProxy_->UpdateResourceUsage();
    ToProto(req->mutable_job_id(), JobId_);
    auto* resourceUsage = req->mutable_resource_usage();
    resourceUsage->set_cpu(CpuGuarantee_);
    resourceUsage->set_network(NetworkUsage_);
    resourceUsage->set_memory(RequestedMemoryReserve_);
    req->Invoke().Subscribe(BIND(&TJobProxy::OnResourcesUpdated, MakeWeak(this), RequestedMemoryReserve_.load()));
}

void TJobProxy::OnSpawned()
{
    if (Config_->TestingConfig->FailOnJobProxySpawnedCall) {
        YT_LOG_ERROR("Fail OnJobProxySpawned call for testing purposes");
        Abort(EJobProxyExitCode::SupervisorCommunicationFailed);
    }

    auto req = SupervisorProxy_->OnJobProxySpawned();
    ToProto(req->mutable_job_id(), JobId_);
    auto error = WaitFor(req->Invoke());
    if (!error.IsOK()) {
        YT_LOG_ERROR(error, "Failed to notify supervisor about job proxy spawned");
        Abort(EJobProxyExitCode::SupervisorCommunicationFailed);
    }
}

void TJobProxy::OnArtifactsPrepared()
{
    auto req = SupervisorProxy_->OnArtifactsPrepared();
    ToProto(req->mutable_job_id(), JobId_);
    auto error = WaitFor(req->Invoke());
    if (!error.IsOK()) {
        YT_LOG_ERROR(error, "Failed to notify supervisor about artifacts preparation");
        Abort(EJobProxyExitCode::SupervisorCommunicationFailed);
    }
}

void TJobProxy::SetUserJobMemoryUsage(i64 memoryUsage)
{
    UserJobCurrentMemoryUsage_ = memoryUsage;
}

void TJobProxy::OnResourcesUpdated(i64 memoryReserve, const TError& error)
{
    if (!error.IsOK()) {
        YT_LOG_ERROR(error, "Failed to update resource usage");
        Abort(EJobProxyExitCode::ResourcesUpdateFailed);
    }

    if (ApprovedMemoryReserve_ < memoryReserve) {
        YT_LOG_DEBUG("Successfully updated resource usage (MemoryReserve: %v)", memoryReserve);
        ApprovedMemoryReserve_ = memoryReserve;
    }
}

void TJobProxy::ReleaseNetwork()
{
    YT_LOG_INFO("Releasing network");
    NetworkUsage_ = 0;
    UpdateResourceUsage();
}

void TJobProxy::OnPrepared()
{
    YT_LOG_INFO("Job prepared");

    auto req = SupervisorProxy_->OnJobPrepared();
    ToProto(req->mutable_job_id(), JobId_);
    YT_UNUSED_FUTURE(req->Invoke().Apply(BIND(
        [this, this_ = MakeStrong(this)] (const TSupervisorServiceProxy::TErrorOrRspOnJobPreparedPtr& /*rspOrError*/) {
            Prepared_.store(true);
        })));
}

void TJobProxy::PrepareArtifact(
    const TString& artifactName,
    const TString& pipePath)
{
    YT_LOG_INFO("Requesting node to prepare artifact (ArtifactName: %v, PipePath: %v)",
        artifactName,
        pipePath);

    auto req = SupervisorProxy_->PrepareArtifact();
    ToProto(req->mutable_job_id(), JobId_);
    req->set_artifact_name(artifactName);
    req->set_pipe_path(pipePath);

    WaitFor(req->Invoke())
        .ThrowOnError();
}

void TJobProxy::OnArtifactPreparationFailed(
    const TString& artifactName,
    const TString& artifactPath,
    const TError& error)
{
    YT_LOG_ERROR(error, "Artifact preparation failed (ArtifactName: %v, ArtifactPath: %v)",
        artifactName,
        artifactPath);

    auto req = SupervisorProxy_->OnArtifactPreparationFailed();
    ToProto(req->mutable_job_id(), JobId_);
    req->set_artifact_name(artifactName);
    req->set_artifact_path(artifactPath);
    ToProto(req->mutable_error(), error);

    WaitFor(req->Invoke())
        .ThrowOnError();
}

void TJobProxy::OnJobMemoryThrashing()
{
    auto req = SupervisorProxy_->OnJobMemoryThrashing();
    ToProto(req->mutable_job_id(), JobId_);

    WaitFor(req->Invoke())
        .ThrowOnError();
}

NApi::NNative::IClientPtr TJobProxy::GetClient() const
{
    return Client_;
}

TChunkReaderHostPtr TJobProxy::GetChunkReaderHost() const
{
    return New<TChunkReaderHost>(
        Client_,
        LocalDescriptor_,
        ReaderBlockCache_,
        /*chunkMetaCache*/ nullptr,
        /*nodeStatusDirectory*/ nullptr,
        GetInBandwidthThrottler(),
        GetOutRpsThrottler(),
        GetTrafficMeter());
}

IBlockCachePtr TJobProxy::GetReaderBlockCache() const
{
    return ReaderBlockCache_;
}

IBlockCachePtr TJobProxy::GetWriterBlockCache() const
{
    return GetNullBlockCache();
}

const NNodeTrackerClient::TNodeDescriptor& TJobProxy::LocalDescriptor() const
{
    return LocalDescriptor_;
}

void TJobProxy::CheckMemoryUsage()
{
    i64 jobProxyMemoryUsage = 0;
    try {
        auto memoryUsage = GetProcessMemoryUsage();
        // RSS from /proc/pid/statm includes all pages resident to current process,
        // including memory-mapped files and shared memory.
        // Since there typically are multiple instances of job proxy on the host sharing the same binary
        // we don't want to account shared pages overhead on every job. Moreover, this memory is not accounted by
        // controller agent during memory consumption estimation.
        jobProxyMemoryUsage = memoryUsage.Rss - memoryUsage.Shared;
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Failed to get process memory usage");
        return;
    }

    JobProxyMaxMemoryUsage_ = std::max(JobProxyMaxMemoryUsage_.load(), jobProxyMemoryUsage);
    UpdateCumulativeMemoryUsage(jobProxyMemoryUsage);

    YT_LOG_DEBUG("Job proxy memory check (JobProxyMemoryUsage: %v, JobProxyMaxMemoryUsage: %v, JobProxyMemoryReserve: %v, UserJobCurrentMemoryUsage: %v)",
        jobProxyMemoryUsage,
        JobProxyMaxMemoryUsage_.load(),
        JobProxyMemoryReserve_,
        UserJobCurrentMemoryUsage_.load());

    constexpr double JobProxyMaxMemoryUsageLoggingExponentialFactor = 1.2;

    auto usage = JobProxyMaxMemoryUsage_.load();
    if (usage > JobProxyMemoryReserve_ &&
        usage > LastLoggedJobProxyMaxMemoryUsage_ * JobProxyMaxMemoryUsageLoggingExponentialFactor)
    {
        if (TInstant::Now() - LastRefCountedTrackerLogTime_ > RefCountedTrackerLogPeriod_) {
            YT_LOG_WARNING("Job proxy used more memory than estimated "
                "(JobProxyMaxMemoryUsage: %v, JobProxyMemoryReserve: %v, RefCountedTracker: %v)",
                usage,
                JobProxyMemoryReserve_,
                TRefCountedTracker::Get()->GetDebugInfo(2 /* sortByColumn */));
            LastRefCountedTrackerLogTime_ = TInstant::Now();
            LastLoggedJobProxyMaxMemoryUsage_ = usage;
        }
    }

    if (JobProxyMemoryOvercommitLimit_ && jobProxyMemoryUsage > JobProxyMemoryReserve_ + *JobProxyMemoryOvercommitLimit_) {
        YT_LOG_FATAL("Job proxy exceeded the memory overcommit limit "
            "(JobProxyMemoryUsage: %v, JobProxyMemoryReserve: %v, MemoryOvercommitLimit: %v, RefCountedTracker: %v)",
            jobProxyMemoryUsage,
            JobProxyMemoryReserve_,
            JobProxyMemoryOvercommitLimit_,
            TRefCountedTracker::Get()->GetDebugInfo(2 /* sortByColumn */));
    }

    i64 totalMemoryUsage = UserJobCurrentMemoryUsage_ + jobProxyMemoryUsage;

    const auto& jobSpecExt = JobSpecHelper_->GetJobSpecExt();
    if (jobSpecExt.has_user_job_spec()) {
        const auto& userJobSpec = jobSpecExt.user_job_spec();
        // NB: if job proxy is not prepared yet we cannot report actual statistics by heartbeat in abort.
        if (Config_->AlwaysAbortOnMemoryReserveOverdraft && Prepared_) {
            bool overdraft = false;
            if (UserJobCurrentMemoryUsage_ > userJobSpec.memory_reserve()) {
                YT_LOG_INFO("User job memory usage exceeded memory reserve (MemoryUsage: %v, MemoryReserve: %v)",
                    UserJobCurrentMemoryUsage_.load(),
                    userJobSpec.memory_reserve());
                overdraft = true;
            }
            if (jobProxyMemoryUsage > userJobSpec.job_proxy_memory_reserve()) {
                YT_LOG_INFO("Job proxy memory usage exceeded memory reserve (MemoryUsage: %v, MemoryReserve: %v)",
                    jobProxyMemoryUsage,
                    userJobSpec.job_proxy_memory_reserve());
                overdraft = true;
            }
            if (overdraft) {
                Abort(EJobProxyExitCode::ResourceOverdraft);
            }
        }
    }

    if (TotalMaxMemoryUsage_ < totalMemoryUsage) {
        YT_LOG_DEBUG("Total memory usage increased (OldTotalMaxMemoryUsage: %v, NewTotalMaxMemoryUsage: %v)",
            TotalMaxMemoryUsage_,
            totalMemoryUsage);
        TotalMaxMemoryUsage_ = totalMemoryUsage;
        if (TotalMaxMemoryUsage_ > ApprovedMemoryReserve_) {
            YT_LOG_ERROR("Total memory usage exceeded the limit approved by the node "
                "(TotalMaxMemoryUsage: %v, ApprovedMemoryReserve: %v, AheadMemoryReserve: %v)",
                TotalMaxMemoryUsage_,
                ApprovedMemoryReserve_.load(),
                Config_->AheadMemoryReserve);
            // TODO(psushin): first improve memory estimates with data weights.
            // Exit(EJobProxyExitCode::ResourceOverdraft);
        }
    }
    i64 memoryReserve = TotalMaxMemoryUsage_ + Config_->AheadMemoryReserve;
    if (RequestedMemoryReserve_ < memoryReserve) {
        YT_LOG_DEBUG("Requesting node for memory usage increase (OldMemoryReserve: %v, NewMemoryReserve: %v)",
            RequestedMemoryReserve_.load(),
            memoryReserve);
        RequestedMemoryReserve_ = memoryReserve;
        UpdateResourceUsage();
    }
}

void TJobProxy::FillJobResult(TJobResult* jobResult)
{
    auto job = FindJob();
    if (!job) {
        return;
    }

    auto failedChunkIds = job->GetFailedChunkIds();
    if (!failedChunkIds.empty()) {
        YT_LOG_INFO("Failed chunks found (ChunkIds: %v)",
            failedChunkIds);
    }

    // For erasure chunks, replace part id with whole chunk id.
    auto* jobResultExt = jobResult->MutableExtension(TJobResultExt::job_result_ext);
    for (auto chunkId : failedChunkIds) {
        auto actualChunkId = IsErasureChunkPartId(chunkId)
            ? ErasureChunkIdFromPartId(chunkId)
            : chunkId;
        ToProto(jobResultExt->add_failed_chunk_ids(), actualChunkId);
    }

    auto interruptDescriptor = job->GetInterruptDescriptor();

    if (!interruptDescriptor.UnreadDataSliceDescriptors.empty()) {
        auto inputStatistics = job->GetStatistics().TotalInputStatistics.DataStatistics;
        if (inputStatistics.row_count() > 0) {
            // NB(psushin): although we definitely have read some of the rows, the job may have made no progress,
            // since all of these row are from foreign tables, and therefor the ReadDataSliceDescriptors is empty.
            // Still we would like to treat such a job as interrupted, otherwise it may lead to an infinite sequence
            // of jobs being aborted by splitter instead of interrupts.

            ToProto(
                jobResultExt->mutable_unread_chunk_specs(),
                jobResultExt->mutable_chunk_spec_count_per_unread_data_slice(),
                jobResultExt->mutable_virtual_row_index_per_unread_data_slice(),
                interruptDescriptor.UnreadDataSliceDescriptors);
            ToProto(
                jobResultExt->mutable_read_chunk_specs(),
                jobResultExt->mutable_chunk_spec_count_per_read_data_slice(),
                jobResultExt->mutable_virtual_row_index_per_read_data_slice(),
                interruptDescriptor.ReadDataSliceDescriptors);

            jobResultExt->set_restart_needed(true);

            YT_LOG_INFO(
                "Interrupt descriptor found (UnreadDescriptorCount: %v, ReadDescriptorCount: %v, SchedulerResultExt: %v)",
                interruptDescriptor.UnreadDataSliceDescriptors.size(),
                interruptDescriptor.ReadDataSliceDescriptors.size(),
                jobResultExt->ShortDebugString());
        } else {
            if (jobResult->error().code() == 0) {
                ToProto(
                    jobResult->mutable_error(),
                    TError(EErrorCode::JobNotPrepared, "Job did not read anything"));
            }
        }
    }

    const auto& jobSpecExt = GetJobSpecHelper()->GetJobSpecExt();
    if (jobSpecExt.has_user_job_spec()) {
        const auto& userJobSpec = jobSpecExt.user_job_spec();
        if (userJobSpec.has_restart_exit_code()) {
            auto error = FromProto<TError>(jobResult->error());
            if (auto userJobFailedError = error.FindMatching(EErrorCode::UserJobFailed)) {
                auto processFailedError = userJobFailedError->FindMatching(EProcessErrorCode::NonZeroExitCode);
                if (processFailedError && processFailedError->Attributes().Get<int>("exit_code", 0) == userJobSpec.restart_exit_code()) {
                    YT_LOG_INFO("Job exited with code that indicates job restart (ExitCode: %v)",
                        userJobSpec.restart_exit_code());
                    jobResultExt->set_restart_needed(true);
                    ToProto(jobResult->mutable_error(), TError());
                }
            }
        }
    }
}

void TJobProxy::FillStderrResult(TJobResult* jobResult)
{
    const auto& jobSpecExt = GetJobSpecHelper()->GetJobSpecExt();
    const auto& userJobSpec = jobSpecExt.user_job_spec();

    auto* jobResultExt = jobResult->MutableExtension(TJobResultExt::job_result_ext);

    // If we were provided with stderr_table_spec we are expected to write stderr and provide some results.
    if (userJobSpec.has_stderr_table_spec() && !jobResultExt->has_stderr_result()) {
        // If error occurred during user job initialization, stderr blob table writer may not have been created at all.
        YT_LOG_WARNING("Stderr table boundary keys are absent");
        auto* stderrBoundaryKeys = jobResultExt->mutable_stderr_result();
        stderrBoundaryKeys->set_sorted(true);
        stderrBoundaryKeys->set_unique_keys(true);
        stderrBoundaryKeys->set_empty(true);
    }
}

void TJobProxy::Abort(EJobProxyExitCode exitCode)
{
    if (Config_->SendHeartbeatBeforeAbort) {
        auto error = WaitFor(HeartbeatExecutor_->GetExecutedEvent());
        YT_LOG_ERROR_UNLESS(error.IsOK(), error, "Failed to send heartbeat before abort");
    }

    if (auto job = FindJob()) {
        job->Cleanup();
    }

    TProgram::Abort(static_cast<int>(exitCode));
}

bool TJobProxy::TrySetCpuGuarantee(double cpuGuarantee)
{
    if (auto environment = FindJobProxyEnvironment()) {
        try {
            environment->SetCpuGuarantee(cpuGuarantee);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Failed to set CPU share (OldCpuShare: %v, NewCpuShare: %v)",
                CpuGuarantee_.load(),
                cpuGuarantee);
            return false;
        }
        YT_LOG_INFO("Changed CPU share (OldCpuShare: %v, NewCpuShare: %v)",
            CpuGuarantee_.load(),
            cpuGuarantee);
        CpuGuarantee_ = cpuGuarantee;
        UpdateResourceUsage();
        return true;
    } else {
        YT_LOG_INFO("Unable to change CPU share: environment is not set");
        return false;
    }
}

TDuration TJobProxy::GetSpentCpuTime() const
{
    auto result = TDuration::Zero();

    auto validCpuStatistics = [] (const auto& cpuStats) {
        return
            cpuStats &&
            cpuStats->SystemUsageTime.has_value() &&
            cpuStats->UserUsageTime.has_value();
    };

    if (auto job = FindJob()) {
        auto userJobCpu = job->GetUserJobCpuStatistics();
        if (validCpuStatistics(userJobCpu)) {
            result += userJobCpu->SystemUsageTime.value() +
                userJobCpu->UserUsageTime.value();
        }
    }

    if (auto environment = FindJobProxyEnvironment()) {

        auto jobProxyCpu = environment->GetCpuStatistics()
            .ValueOrThrow();

        if (validCpuStatistics(jobProxyCpu)) {
            result += jobProxyCpu->SystemUsageTime.value() +
                jobProxyCpu->UserUsageTime.value();
        } else {
            YT_LOG_WARNING("Cannot get cpu statistics from job environment");
        }
    }

    return result;
}

NLogging::TLogger TJobProxy::GetLogger() const
{
    return Logger;
}

IInvokerPtr TJobProxy::GetControlInvoker() const
{
    return ControlThread_->GetInvoker();
}

void TJobProxy::LogSystemStats() const
{
    struct rusage usageSelf, usageChildren;

    getrusage(RUSAGE_SELF, &usageSelf);
    getrusage(RUSAGE_CHILDREN, &usageChildren);

    YT_LOG_INFO("Get processes CPU usage (JobProxyUserCpu: %v, JobProxySystemCpu: %v, "
        "ChildrenUserCpu: %v, ChildrenSystemCpu: %v)",
        TDuration(usageSelf.ru_utime),
        TDuration(usageSelf.ru_stime),
        TDuration(usageChildren.ru_utime),
        TDuration(usageChildren.ru_stime));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
