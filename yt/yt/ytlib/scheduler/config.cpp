#include "config.h"
#include "job_resources.h"

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/controller_agent/helpers.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/object_client/config.h>

#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/ytlib/table_client/config.h>

#include <yt/yt/client/formats/config.h>

#include <yt/yt/client/security_client/acl.h>
#include <yt/yt/client/security_client/helpers.h>

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/misc/adjusted_exponential_moving_average.h>
#include <yt/yt/core/misc/config.h>
#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/phoenix/type_def.h>

#include <util/string/split.h>

#include <util/folder/path.h>

namespace NYT::NScheduler {

using namespace NChunkClient;
using namespace NSecurityClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NYson;

using NVectorHdrf::EIntegralGuaranteeType;

extern const TString OperationAliasPrefix;

////////////////////////////////////////////////////////////////////////////////

static void ValidateOperationAcl(const TSerializableAccessControlList& acl)
{
    for (const auto& ace : acl.Entries) {
        if (ace.Action != ESecurityAction::Allow && ace.Action != ESecurityAction::Deny) {
            THROW_ERROR_EXCEPTION("Action %Qlv is forbidden to specify in operation ACE",
                ace.Action)
                << TErrorAttribute("ace", ace);
        }
        if (Any(ace.Permissions & ~(EPermission::Read | EPermission::Manage | EPermission::Administer))) {
            THROW_ERROR_EXCEPTION("Only %Qlv, %Qlv and %Qlv permissions are allowed in operation ACL, got %v",
                EPermission::Read,
                EPermission::Manage,
                EPermission::Administer,
                ConvertToYsonString(ace.Permissions, EYsonFormat::Text))
                << TErrorAttribute("ace", ace);
        }
    }
}

namespace {

void ProcessAclAndOwnersParameters(TSerializableAccessControlList* acl, std::vector<std::string>* owners)
{
    if (!acl->Entries.empty() && !owners->empty()) {
        // COMPAT(levysotsky): Priority is given to |acl| currently.
        // An error should be thrown here when all the clusters are updated.
    } else if (!owners->empty()) {
        acl->Entries.emplace_back(
            ESecurityAction::Allow,
            *owners,
            EPermissionSet(EPermission::Read | EPermission::Manage));
        owners->clear();
    }
}

void ValidateNoOutputStreams(const TUserJobSpecPtr& spec, EOperationType operationType)
{
    if (!spec->OutputStreams.empty()) {
        THROW_ERROR_EXCEPTION("\"output_streams\" are currently not allowed in %Qlv operations",
            operationType);
    }
}

void ValidateProfilers(const std::vector<TJobProfilerSpecPtr>& profilers)
{
    double totalProbability = 0.0;
    for (const auto& profiler : profilers) {
        totalProbability += profiler->ProfilingProbability;
    }

    if (totalProbability > 1.0) {
        THROW_ERROR_EXCEPTION("Total probability of enabled profilers is too large")
            << TErrorAttribute("total_probability", totalProbability);
    }
}

void ValidateOutputTablePaths(std::vector<NYPath::TRichYPath> paths)
{
    SortBy(paths, [] (const auto& path) { return path.GetPath(); });
    if (auto duplicatePath = AdjacentFind(paths); duplicatePath != paths.end()) {
        THROW_ERROR_EXCEPTION("Duplicate entries in output_table_paths are not allowed")
            << TErrorAttribute("non_unique_output_table", *duplicatePath);
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

static constexpr int MaxAllowedProfilingTagCount = 200;

TPoolName::TPoolName(TString pool, std::optional<TString> parent)
{
    if (parent) {
        Pool = *parent +  Delimiter + pool;
        ParentPool = std::move(parent);
    } else {
        Pool = std::move(pool);
    }
}

const char TPoolName::Delimiter = '$';

const TString& TPoolName::GetPool() const
{
    return Pool;
}

const std::optional<TString>& TPoolName::GetParentPool() const
{
    return ParentPool;
}

const TString& TPoolName::GetSpecifiedPoolName() const
{
    return ParentPool ? *ParentPool : Pool;
}

TPoolName TPoolName::FromString(const TString& value)
{
    std::vector<TString> parts;
    StringSplitter(value).Split(Delimiter).AddTo(&parts);
    switch (parts.size()) {
        case 1:
            return TPoolName(value, std::nullopt);
        case 2:
            return TPoolName(parts[1], parts[0]);
        default:
            THROW_ERROR_EXCEPTION("Malformed pool name: delimiter %Qv is found more than once in %Qv",
                TPoolName::Delimiter,
                value);
    }
}

TString TPoolName::ToString() const
{
    return Pool;
}

void Deserialize(TPoolName& value, INodePtr node)
{
    value = TPoolName::FromString(node->AsString()->GetValue());
}

void Deserialize(TPoolName& value, TYsonPullParserCursor* cursor)
{
    MaybeSkipAttributes(cursor);
    EnsureYsonToken("TPoolName", *cursor, EYsonItemType::StringValue);
    value = TPoolName::FromString(ExtractTo<TString>(cursor));
}

void Serialize(const TPoolName& value, IYsonConsumer* consumer)
{
    consumer->OnStringScalar(value.ToString());
}

////////////////////////////////////////////////////////////////////////////////

void TJobIOConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("table_reader", &TThis::TableReader)
        .DefaultNew();
    registrar.Parameter("table_writer", &TThis::TableWriter)
        .DefaultNew();
    registrar.Parameter("dynamic_table_writer", &TThis::DynamicTableWriter)
        .DefaultNew();

    registrar.Parameter("control_attributes", &TThis::ControlAttributes)
        .DefaultNew();

    registrar.Parameter("error_file_writer", &TThis::ErrorFileWriter)
        .DefaultNew();

    registrar.Parameter("buffer_row_count", &TThis::BufferRowCount)
        .Default(10'000)
        .GreaterThan(0);
    registrar.Parameter("use_adaptive_buffer_row_count", &TThis::UseAdaptiveRowCount)
        .Default();
    registrar.Parameter("adaptive_row_count_upper_bound", &TThis::AdaptiveRowCountUpperBound)
        .Default(40'000)
        .GreaterThan(0);

    registrar.Parameter("pipe_capacity", &TThis::PipeCapacity)
        .Default()
        .GreaterThan(0)
        // NB(arkady-e1ppa): This is default pipe capacity. Without updating process perms
        // one cannot change capacity to be greater than this number.
        // TODO(arkady-e1ppa): Consider supporting pipe capacity increases.
        .LessThanOrEqual(16 * 4096);

    registrar.Parameter("use_delivery_fenced_pipe_writer", &TThis::UseDeliveryFencedPipeWriter)
        .Default(false);

    registrar.Parameter("pipe_io_pool_size", &TThis::PipeIOPoolSize)
        .Default(1)
        .GreaterThan(0);

    registrar.Parameter("block_cache", &TThis::BlockCache)
        .DefaultNew();

    registrar.Parameter("testing_options", &TThis::Testing)
        .DefaultNew();

    registrar.Preprocessor([] (TJobIOConfig* config) {
        config->ErrorFileWriter->UploadReplicationFactor = 1;

        config->DynamicTableWriter->DesiredChunkSize = 100_MB;
        config->DynamicTableWriter->BlockSize = 256_KB;
    });
}

void TJobIOConfig::TTestingOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("pipe_delay", &TThis::PipeDelay)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TDelayConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("duration", &TThis::Duration)
        .Default();
    registrar.Parameter("type", &TThis::Type)
        .Default(EDelayType::Sync);
}

////////////////////////////////////////////////////////////////////////////////

void TPatchSpecProtocolTestingOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("delay_before_cypress_flush", &TThis::DelayBeforeCypressFlush)
        .Default();
    registrar.Parameter("delay_before_apply", &TThis::DelayBeforeApply)
        .Default();
    registrar.Parameter("fail_validate", &TThis::FailValidate)
        .Default();
    registrar.Parameter("fail_apply", &TThis::FailApply)
        .Default();
    registrar.Parameter("fail_revive", &TThis::FailRevive)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

// TODO(eshcherbin): Change all delays to TDelayConfigPtr.
void TTestingOperationOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("schedule_allocation_delay", &TThis::ScheduleAllocationDelay)
        .Alias("schedule_job_delay")
        .Default();
    registrar.Parameter("inside_schedule_allocation_delay", &TThis::InsideScheduleAllocationDelay)
        .Alias("inside_schedule_job_delay")
        .Default();
    registrar.Parameter("delay_inside_revive", &TThis::DelayInsideRevive)
        .Default();
    registrar.Parameter("delay_inside_initialize", &TThis::DelayInsideInitialize)
        .Default();
    registrar.Parameter("delay_inside_prepare", &TThis::DelayInsidePrepare)
        .Default();
    registrar.Parameter("delay_inside_suspend", &TThis::DelayInsideSuspend)
        .Default();
    registrar.Parameter("delay_inside_materialize", &TThis::DelayInsideMaterialize)
        .Default();
    registrar.Parameter("report_invalid_needed_resources_in_materialize", &TThis::ReportInvalidNeededResourcesInMaterialize)
        .Default(false);
    registrar.Parameter("report_invalid_needed_resources_in_revive", &TThis::ReportInvalidNeededResourcesInRevive)
        .Default(false);
    registrar.Parameter("delay_inside_operation_commit", &TThis::DelayInsideOperationCommit)
        .Default();
    registrar.Parameter("schedule_allocation_delay_scheduler", &TThis::ScheduleAllocationDelayScheduler)
        .Alias("schedule_job_delay_scheduler")
        .Default();
    registrar.Parameter("delay_inside_materialize_scheduler", &TThis::DelayInsideMaterializeScheduler)
        .Default();
    registrar.Parameter("delay_inside_abort", &TThis::DelayInsideAbort)
        .Default();
    registrar.Parameter("delay_inside_register_allocations_from_revived_operation", &TThis::DelayInsideRegisterAllocationsFromRevivedOperation)
        .Alias("delay_inside_register_jobs_from_revived_operation")
        .Default();
    registrar.Parameter("delay_inside_validate_runtime_parameters", &TThis::DelayInsideValidateRuntimeParameters)
        .Default();
    registrar.Parameter("delay_before_start", &TThis::DelayBeforeStart)
        .Default();
    registrar.Parameter("delay_inside_operation_commit_stage", &TThis::DelayInsideOperationCommitStage)
        .Default();
    registrar.Parameter("no_delay_on_second_entrance_to_commit", &TThis::NoDelayOnSecondEntranceToCommit)
        .Default(false);
    registrar.Parameter("controller_failure", &TThis::ControllerFailure)
        .Default();
    registrar.Parameter("settle_job_delay", &TThis::SettleJobDelay)
        .Default();
    registrar.Parameter("fail_settle_job_requests", &TThis::FailSettleJobRequests)
        .Default(false);
    registrar.Parameter("testing_speculative_launch_mode", &TThis::TestingSpeculativeLaunchMode)
        .Default(ETestingSpeculativeLaunchMode::None);
    registrar.Parameter("allocation_size", &TThis::AllocationSize)
        .GreaterThanOrEqual(0)
        .LessThanOrEqual(100_GB)
        .Default();
    registrar.Parameter("allocation_release_delay", &TThis::AllocationReleaseDelay)
        .Default();
    registrar.Parameter("cancellation_stage", &TThis::CancelationStage)
        .Default();
    registrar.Parameter("build_job_spec_proto_delay", &TThis::BuildJobSpecProtoDelay)
        .Default();
    registrar.Parameter("fail_operation_delay", &TThis::FailOperationDelay)
        .Default();
    registrar.Parameter("test_job_speculation_timeout", &TThis::TestJobSpeculationTimeout)
        .Default(false);
    registrar.Parameter("crash_controller_agent", &TThis::CrashControllerAgent)
        .Default(false);
    registrar.Parameter("throw_exception_during_operation_abort", &TThis::ThrowExceptionDuringOperationAbort)
        .Default(false);
    registrar.Parameter("patch_spec_protocol", &TThis::PatchSpecProtocol)
        .Default();

    registrar.Postprocessor([] (TTestingOperationOptions* config) {
        if (const auto& delay = config->InsideScheduleAllocationDelay;
            delay && delay->Type != EDelayType::Sync)
        {
            THROW_ERROR_EXCEPTION("\"inside_schedule_job_delay\" must be sync, use \"schedule_job_delay\" instead");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TJobSplitterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_job_splitting", &TThis::EnableJobSplitting)
        .Default(true);

    registrar.Parameter("enable_job_speculation", &TThis::EnableJobSpeculation)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

void TAutoMergeConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("job_io", &TThis::JobIO)
        .DefaultNew();
    registrar.Parameter("max_intermediate_chunk_count", &TThis::MaxIntermediateChunkCount)
        .Default()
        .GreaterThanOrEqual(1);
    registrar.Parameter("chunk_count_per_merge_job", &TThis::ChunkCountPerMergeJob)
        .Default()
        .GreaterThanOrEqual(1);
    registrar.Parameter("chunk_size_threshold", &TThis::ChunkSizeThreshold)
        .Default(128_MB)
        .GreaterThanOrEqual(1);
    registrar.Parameter("mode", &TThis::Mode)
        .Default(EAutoMergeMode::Disabled);
    registrar.Parameter("enable_shallow_merge", &TThis::EnableShallowMerge)
        .Default(false);
    registrar.Parameter("allow_unknown_extensions",  &TAutoMergeConfig::AllowUnknownExtensions)
        .Default(false);
    registrar.Parameter("max_block_count", &TThis::MaxBlockCount)
        .Default();

    registrar.Parameter("use_intermediate_data_account", &TThis::UseIntermediateDataAccount)
        .Default(false);
    registrar.Parameter("shallow_merge_min_data_weight_per_chunk", &TThis::ShallowMergeMinDataWeightPerChunk)
        .Default(64_KB);
    registrar.Parameter("single_chunk_teleport_strategy", &TThis::SingleChunkTeleportStrategy)
        .Default(ESingleChunkTeleportStrategy::Enabled);

    registrar.Preprocessor([] (TAutoMergeConfig* config) {
        config->JobIO->TableWriter->DesiredChunkWeight = 8_GB;
    });

    registrar.Postprocessor([] (TAutoMergeConfig* config) {
        if (config->JobIO->TableWriter->DesiredChunkWeight < config->ChunkSizeThreshold) {
            THROW_ERROR_EXCEPTION("Desired chunk weight cannot be less than chunk size threshold")
                << TErrorAttribute("chunk_size_threshold", config->ChunkSizeThreshold)
                << TErrorAttribute("desired_chunk_weight", config->JobIO->TableWriter->DesiredChunkWeight);
        }

        if (config->Mode == EAutoMergeMode::Manual) {
            if (!config->MaxIntermediateChunkCount || !config->ChunkCountPerMergeJob) {
                THROW_ERROR_EXCEPTION(
                    "Maximum intermediate chunk count and chunk count per merge job "
                    "should both be present when using manual mode of auto merge");
            }
            if (*config->MaxIntermediateChunkCount < *config->ChunkCountPerMergeJob) {
                THROW_ERROR_EXCEPTION("Maximum intermediate chunk count cannot be less than chunk count per merge job")
                    << TErrorAttribute("max_intermediate_chunk_count", *config->MaxIntermediateChunkCount)
                    << TErrorAttribute("chunk_count_per_merge_job", *config->ChunkCountPerMergeJob);
            }
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TTentativeTreeEligibilityConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("sample_job_count", &TThis::SampleJobCount)
        .Default(10)
        .GreaterThan(0);

    registrar.Parameter("max_tentative_job_duration_ratio", &TThis::MaxTentativeJobDurationRatio)
        .Default(10.0)
        .GreaterThan(0.0);

    registrar.Parameter("min_job_duration", &TThis::MinJobDuration)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("ignore_missing_pool_trees", &TThis::IgnoreMissingPoolTrees)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TSamplingConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("sampling_rate", &TThis::SamplingRate)
        .Default();

    registrar.Parameter("max_total_slice_count", &TThis::MaxTotalSliceCount)
        .Default();

    registrar.Parameter("io_block_size", &TThis::IOBlockSize)
        .Default(16_MB);

    registrar.Postprocessor([] (TSamplingConfig* config) {
        if (config->SamplingRate && (*config->SamplingRate < 0.0 || *config->SamplingRate > 1.0)) {
            THROW_ERROR_EXCEPTION("Sampling rate should be in range [0.0, 1.0]")
                << TErrorAttribute("sampling_rate", config->SamplingRate);
        }
        if (config->MaxTotalSliceCount && *config->MaxTotalSliceCount <= 0) {
            THROW_ERROR_EXCEPTION("max_total_slice_count should be positive")
                << TErrorAttribute("max_total_slice_count", config->MaxTotalSliceCount);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TTmpfsVolumeConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("size", &TThis::Size);
    registrar.Parameter("path", &TThis::Path);
}

void ToProto(NControllerAgent::NProto::TTmpfsVolume* protoTmpfsVolume, const TTmpfsVolumeConfig& tmpfsVolumeConfig)
{
    protoTmpfsVolume->set_size(tmpfsVolumeConfig.Size);
    protoTmpfsVolume->set_path(tmpfsVolumeConfig.Path);
}

void FromProto(TTmpfsVolumeConfig* tmpfsVolumeConfig, const NControllerAgent::NProto::TTmpfsVolume& protoTmpfsVolume)
{
    tmpfsVolumeConfig->Size = protoTmpfsVolume.size();
    tmpfsVolumeConfig->Path = protoTmpfsVolume.path();
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRequestConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("disk_space", &TThis::DiskSpace);
    registrar.Parameter("inode_count", &TThis::InodeCount)
        .Default();
    registrar.Parameter("medium_name", &TThis::MediumName)
        .NonEmpty()
        .Default();
    registrar.Parameter("account", &TThis::Account)
        .NonEmpty()
        .Default();

    registrar.Postprocessor([&] (TDiskRequestConfig* config) {
        if (config->Account && !config->MediumName) {
            THROW_ERROR_EXCEPTION("\"medium_name\" is required in disk request if account is specified");
        }
    });
}

void ToProto(
    NProto::TDiskRequest* protoDiskRequest,
    const TDiskRequestConfig& diskRequestConfig)
{
    protoDiskRequest->set_disk_space(diskRequestConfig.DiskSpace);
    if (diskRequestConfig.InodeCount) {
        protoDiskRequest->set_inode_count(*diskRequestConfig.InodeCount);
    }
    if (diskRequestConfig.MediumName) {
        YT_VERIFY(diskRequestConfig.MediumIndex);
        protoDiskRequest->set_medium_index(*diskRequestConfig.MediumIndex);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TJobShell::Register(TRegistrar registrar)
{
    registrar.Parameter("name", &TThis::Name);

    registrar.Parameter("subcontainer", &TThis::Subcontainer)
        .Default();

    registrar.Parameter("owners", &TThis::Owners)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TUserJobMonitoringConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(false);

    registrar.Parameter("sensor_names", &TThis::SensorNames)
        .Default(GetDefaultSensorNames());

    registrar.Parameter("request_gpu_monitoring", &TThis::RequestGpuMonitoring)
        .Default(false);
}

const std::vector<TString>& TUserJobMonitoringConfig::GetDefaultSensorNames()
{
    static const std::vector<TString> DefaultSensorNames = {
        "cpu/burst",
        "cpu/user",
        "cpu/system",
        "cpu/wait",
        "cpu/throttled",
        "cpu/cfs_throttled",
        "cpu/context_switches",
        "current_memory/rss",
        "current_memory/mapped_file",
        "current_memory/major_page_faults",
        "tmpfs_size",
        "disk/usage",
        "disk/limit",
    };

    return DefaultSensorNames;
}

////////////////////////////////////////////////////////////////////////////////

void TJobProfilerSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("binary", &TThis::Binary);
    registrar.Parameter("type", &TThis::Type);

    registrar.Parameter("profiling_probability", &TThis::ProfilingProbability)
        .InRange(0.0, 1.0);

    registrar.Parameter("sampling_frequency", &TThis::SamplingFrequency)
        .Default(100);

    registrar.Parameter("run_external_symbolizer", &TThis::RunExternalSymbolizer)
        .Default(false);
}

void ToProto(NControllerAgent::NProto::TJobProfilerSpec* protoJobProfilerSpec, const TJobProfilerSpec& jobProfilerSpec)
{
    protoJobProfilerSpec->set_binary(::NYT::ToProto(jobProfilerSpec.Binary));
    protoJobProfilerSpec->set_type(::NYT::ToProto(jobProfilerSpec.Type));
    protoJobProfilerSpec->set_profiling_probability(jobProfilerSpec.ProfilingProbability);
    protoJobProfilerSpec->set_sampling_frequency(jobProfilerSpec.SamplingFrequency);
    protoJobProfilerSpec->set_run_external_symbolizer(jobProfilerSpec.RunExternalSymbolizer);
}

void FromProto(TJobProfilerSpec* jobProfilerSpec, const NControllerAgent::NProto::TJobProfilerSpec& protoJobProfilerSpec)
{
    jobProfilerSpec->Binary = static_cast<EProfilingBinary>(protoJobProfilerSpec.binary());
    jobProfilerSpec->Type = static_cast<EProfilerType>(protoJobProfilerSpec.type());
    jobProfilerSpec->ProfilingProbability = protoJobProfilerSpec.profiling_probability();
    jobProfilerSpec->SamplingFrequency = protoJobProfilerSpec.sampling_frequency();
    jobProfilerSpec->RunExternalSymbolizer = protoJobProfilerSpec.run_external_symbolizer();
}

////////////////////////////////////////////////////////////////////////////////

void TColumnarStatisticsConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enabled", &TThis::Enabled)
        .Default();

    registrar.Parameter("mode", &TThis::Mode)
        .Default(EColumnarStatisticsFetcherMode::Fallback);
}

void TCudaProfilerEnvironment::Register(TRegistrar registrar)
{
    registrar.Parameter("path_environment_variable_name", &TThis::PathEnvironmentVariableName)
        .NonEmpty();

    registrar.Parameter("path_environment_variable_value", &TThis::PathEnvironmentVariableValue)
        .NonEmpty();
}

////////////////////////////////////////////////////////////////////////////////

void TJobFailsTolerance::Register(TRegistrar registrar)
{
    registrar.Parameter("max_fails_unknown_exit_code", &TThis::MaxFailsUnknownExitCode)
        .GreaterThanOrEqual(0)
        .Default(1);

    registrar.Parameter("max_fails_no_exit_code", &TThis::MaxFailsNoExitCode)
        .GreaterThanOrEqual(0)
        .Default(std::numeric_limits<int>::max());

    registrar.Parameter("max_fails_per_known_exit_code", &TThis::MaxFailsPerKnownExitCode)
        .CheckThat([] (const auto& field) {
            for (const auto& [key, value] : field) {
                THROW_ERROR_EXCEPTION_UNLESS(
                    value >= 0,
                    "Maximum value of fails for each exit code must be non-negative! (ExitCode: %v, MaxFailsCount: %v)",
                    key,
                    value);
            }
        })
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TFastIntermediateMediumTableWriterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("upload_replication_factor", &TThis::UploadReplicationFactor)
        .Default(DefaultIntermediateDataReplicationFactor);
    registrar.Parameter("min_upload_replication_factor", &TThis::MinUploadReplicationFactor)
        .Default(1);
    registrar.Parameter("erasure_codec", &TThis::ErasureCodec)
        .Default(NErasure::ECodec::None);
    registrar.Parameter("enable_striped_erasure", &TThis::EnableStripedErasure)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TOperationSpecBase::Register(TRegistrar registrar)
{
    registrar.UnrecognizedStrategy(NYTree::EUnrecognizedStrategy::KeepRecursive);

    registrar.Parameter("intermediate_data_account", &TThis::IntermediateDataAccount)
        .NonEmpty()
        .Default(IntermediateAccountName);
    registrar.Parameter("intermediate_compression_codec", &TThis::IntermediateCompressionCodec)
        .Default(NCompression::ECodec::Lz4);
    registrar.Parameter("intermediate_data_replication_factor", &TThis::IntermediateDataReplicationFactor)
        .Default(DefaultIntermediateDataReplicationFactor);
    registrar.Parameter("min_intermediate_data_replication_factor", &TThis::MinIntermediateDataReplicationFactor)
        .Alias("intermediate_min_data_replication_factor")
        .Default(1);
    registrar.Parameter("intermediate_data_sync_on_close", &TThis::IntermediateDataSyncOnClose)
        .Default(false);
    registrar.Parameter("intermediate_data_medium", &TThis::IntermediateDataMediumName)
        .NonEmpty()
        .Default(NChunkClient::DefaultStoreMediumName);
    registrar.Parameter("fast_intermediate_medium_table_writer_config", &TThis::FastIntermediateMediumTableWriterConfig)
        .Default();
    registrar.Parameter("fast_intermediate_medium_limit", &TThis::FastIntermediateMediumLimit)
        .Default();

    registrar.Parameter("debug_artifacts_account", &TThis::DebugArtifactsAccount)
        .Alias("job_node_account")
        .NonEmpty()
        .Default(NSecurityClient::TmpAccountName);

    registrar.Parameter("unavailable_chunk_strategy", &TThis::UnavailableChunkStrategy)
        .Default(EUnavailableChunkAction::Wait);
    registrar.Parameter("unavailable_chunk_tactics", &TThis::UnavailableChunkTactics)
        .Default(EUnavailableChunkAction::Wait);

    registrar.Parameter("max_data_weight_per_job", &TThis::MaxDataWeightPerJob)
        .Alias("max_data_size_per_job")
        .Default(200_GB)
        .GreaterThan(0);
    registrar.Parameter("max_primary_data_weight_per_job", &TThis::MaxPrimaryDataWeightPerJob)
        .Default(std::numeric_limits<i64>::max())
        .GreaterThan(0);

    registrar.Parameter("max_failed_job_count", &TThis::MaxFailedJobCount)
        .Default(10)
        .GreaterThanOrEqual(0)
        .LessThanOrEqual(10000);
    registrar.Parameter("job_fails_tolerance", &TThis::JobFailsTolerance)
        .Default();
    registrar.Parameter("max_stderr_count", &TThis::MaxStderrCount)
        .Default(10)
        .GreaterThanOrEqual(0)
        .LessThanOrEqual(150);
    registrar.Parameter("max_core_info_count", &TThis::MaxCoreInfoCount)
        .Default(10)
        .GreaterThanOrEqual(0)
        .LessThanOrEqual(150);

    registrar.Parameter("job_proxy_memory_overcommit_limit", &TThis::JobProxyMemoryOvercommitLimit)
        .Default()
        .GreaterThanOrEqual(0);

    registrar.Parameter("job_proxy_ref_counted_tracker_log_period", &TThis::JobProxyRefCountedTrackerLogPeriod)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("title", &TThis::Title)
        .Default();

    registrar.Parameter("time_limit", &TThis::TimeLimit)
        .Default();

    registrar.Parameter("time_limit_job_fail_timeout", &TThis::TimeLimitJobFailTimeout)
        .Default(TDuration::Minutes(2));

    registrar.Parameter("owners", &TThis::Owners)
        .Default();

    registrar.Parameter("acl", &TThis::Acl)
        .Default();

    registrar.Parameter("aco_name", &TThis::AcoName)
        .Default();

    registrar.Parameter("add_authenticated_user_to_acl", &TThis::AddAuthenticatedUserToAcl)
        .Default();

    registrar.Parameter("secure_vault", &TThis::SecureVault)
        .Default();

    registrar.Parameter("enable_secure_vault_variables_in_job_shell", &TThis::EnableSecureVaultVariablesInJobShell)
        .Default(true);

    registrar.Parameter("issue_temporary_token", &TThis::IssueTemporaryToken)
        .Default(false);

    registrar.Parameter("temporary_token_environment_variable_name", &TThis::TemporaryTokenEnvironmentVariableName)
        .Default("YT_TOKEN");

    registrar.Parameter("suspend_operation_if_account_limit_exceeded", &TThis::SuspendOperationIfAccountLimitExceeded)
        .Default(false);

    registrar.Parameter("suspend_operation_after_materialization", &TThis::SuspendOperationAfterMaterialization)
        .Default(false);

    registrar.Parameter("min_locality_input_data_weight", &TThis::MinLocalityInputDataWeight)
        .GreaterThanOrEqual(0)
        .Default(1_GB);

    registrar.Parameter("auto_merge", &TThis::AutoMerge)
        .DefaultNew();

    registrar.Parameter("job_proxy_memory_digest", &TThis::JobProxyMemoryDigest)
        .DefaultCtor([] {
            auto config = New<TLogDigestConfig>();
            config->LowerBound = 0.5;
            config->UpperBound = 2.0;
            config->DefaultValue = 1.0;
            return config;
        });

    registrar.Parameter("job_proxy_resource_overdraft_memory_multiplier", &TThis::JobProxyResourceOverdraftMemoryMultiplier)
        .InRange(1.0, 10.0)
        .Default(std::nullopt);

    registrar.Parameter("fail_on_job_restart", &TThis::FailOnJobRestart)
        .Default(false);

    registrar.Parameter("enable_job_splitting", &TThis::EnableJobSplitting)
        .Default(true);

    registrar.Parameter("slice_erasure_chunks_by_parts", &TThis::SliceErasureChunksByParts)
        .Default(false);

    registrar.Parameter("enable_legacy_live_preview", &TThis::EnableLegacyLivePreview)
        .Default();

    registrar.Parameter("started_by", &TThis::StartedBy)
        .Default();
    registrar.Parameter("annotations", &TThis::Annotations)
        .Default();

    // COMPAT(gritukan): Drop it in favor of `Annotations["description"]'.
    registrar.Parameter("description", &TThis::Description)
        .Default();

    registrar.Parameter("use_columnar_statistics", &TThis::UseColumnarStatistics)
        .Default(false);
    registrar.Parameter("use_chunk_slice_statistics", &TThis::UseChunkSliceStatistics)
        .Default(false);
    registrar.Parameter("enable_read_size_estimation", &TThis::EnableReadSizeEstimation)
        .Default(true);

    registrar.Parameter("ban_nodes_with_failed_jobs", &TThis::BanNodesWithFailedJobs)
        .Default(false);
    registrar.Parameter("ignore_job_failures_at_banned_nodes", &TThis::IgnoreJobFailuresAtBannedNodes)
        .Default(false);
    registrar.Parameter("fail_on_all_nodes_banned", &TThis::FailOnAllNodesBanned)
        .Default(true);

    registrar.Parameter("sampling", &TThis::Sampling)
        .DefaultNew();

    registrar.Parameter("alias", &TThis::Alias)
        .Default();

    registrar.Parameter("omit_inaccessible_columns", &TThis::OmitInaccessibleColumns)
        .Default(false);

    registrar.Parameter("additional_security_tags", &TThis::AdditionalSecurityTags)
        .Default();

    registrar.Parameter("waiting_job_timeout", &TThis::WaitingJobTimeout)
        .Default(std::nullopt)
        .GreaterThanOrEqual(TDuration::Seconds(10))
        .LessThanOrEqual(TDuration::Minutes(10));

    registrar.Parameter("job_speculation_timeout", &TThis::JobSpeculationTimeout)
        .Default()
        .GreaterThan(TDuration::Zero());

    registrar.Parameter("atomicity", &TThis::Atomicity)
        .Default(EAtomicity::Full);

    registrar.Parameter("lock_output_dynamic_tables", &TThis::LockOutputDynamicTables)
        .Default();

    registrar.Parameter("allow_output_dynamic_tables", &TThis::AllowOutputDynamicTables)
        .Default();

    registrar.Parameter("enable_write_buffer_size_estimation", &TThis::EnableWriteBufferSizeEstimation)
        .Default(false);

    registrar.Parameter("job_cpu_monitor", &TThis::JobCpuMonitor)
        .DefaultNew();

    registrar.Parameter("enable_dynamic_store_read", &TThis::EnableDynamicStoreRead)
        .Default();

    registrar.Parameter("controller_agent_tag", &TThis::ControllerAgentTag)
        .Default("default");

    registrar.Parameter("job_shells", &TThis::JobShells)
        .Default();

    registrar.Parameter("job_splitter", &TThis::JobSplitter)
        .DefaultNew();

    registrar.Parameter("experiment_overrides", &TThis::ExperimentOverrides)
        .Default();

    registrar.Parameter("enable_trace_logging", &TThis::EnableTraceLogging)
        .Default(false);

    registrar.Parameter("input_table_columnar_statistics", &TThis::InputTableColumnarStatistics)
        .DefaultNew();
    registrar.Parameter("user_file_columnar_statistics", &TThis::UserFileColumnarStatistics)
        .DefaultNew();

    registrar.Parameter("force_job_proxy_tracing", &TThis::ForceJobProxyTracing)
        .Default(false);
    registrar.Parameter("suspend_on_job_failure", &TThis::SuspendOnJobFailure)
        .Default(false);

    registrar.Parameter("job_testing_options", &TThis::JobTestingOptions)
        .Default();

    registrar.Parameter("read_via_exec_node", &TThis::ReadViaExecNode)
        .Default(false);

    registrar.Parameter("enable_codegen_comparator", &TThis::EnableCodegenComparator)
        .Default(true);

    registrar.Parameter("disable_rename_columns_compatibility_code", &TThis::DisableRenameColumnsCompatibilityCode)
        .Default(false);

    registrar.Parameter("enable_virtual_sandbox", &TThis::EnableVirtualSandbox)
        .Default(false);

    registrar.Parameter("enable_root_volume_disk_quota", &TThis::EnableRootVolumeDiskQuota)
        .Default(false);

    registrar.Parameter("chunk_availability_policy", &TThis::ChunkAvailabilityPolicy)
        .Default(NChunkClient::EChunkAvailabilityPolicy::DataPartsAvailable);

    registrar.Parameter("sanity_check_delay", &TThis::SanityCheckDelay)
        .Default();

    registrar.Parameter("profilers", &TThis::Profilers)
        .Default();

    registrar.Parameter("default_base_layer_path", &TThis::DefaultBaseLayerPath)
        .Default();

    registrar.Parameter("job_experiment", &TThis::JobExperiment)
        .DefaultNew();

    registrar.Parameter("adjust_dynamic_table_data_slices", &TThis::AdjustDynamicTableDataSlices)
        .Default(false);

    registrar.Parameter("batch_row_count", &TThis::BatchRowCount)
        .Default()
        .GreaterThan(0);

    registrar.Parameter("bypass_hunk_remote_copy_prohibition", &TThis::BypassHunkRemoteCopyProhibition)
        .Default();

    registrar.Parameter("cuda_profiler_layer_path", &TThis::CudaProfilerLayerPath)
        .Default();

    registrar.Parameter("cuda_profiler_environment_variables", &TThis::CudaProfilerEnvironmentVariables)
        .Default();

    registrar.Parameter("cuda_profiler_environment", &TThis::CudaProfilerEnvironment)
        .Default();

    registrar.Parameter("ignore_yt_variables_in_shell_environment", &TThis::IgnoreYTVariablesInShellEnvironment)
        .Default(false);

    registrar.Parameter("allow_offloading", &TThis::AllowOffloading)
        .Default(true);

    registrar.Parameter("require_specified_pools_existence", &TThis::RequireSpecifiedPoolsExistence)
        .Default();

    registrar.Parameter("use_cluster_throttlers", &TThis::UseClusterThrottlers)
        .Default(false);

    registrar.Postprocessor([] (TOperationSpecBase* spec) {
        if (spec->UnavailableChunkStrategy == EUnavailableChunkAction::Wait &&
            spec->UnavailableChunkTactics == EUnavailableChunkAction::Skip)
        {
            THROW_ERROR_EXCEPTION("Your tactics conflicts with your strategy, Luke!");
        }

        if (spec->SecureVault) {
            for (const auto& name : spec->SecureVault->GetKeys()) {
                NControllerAgent::ValidateEnvironmentVariableName(name);
            }
        }

        if (spec->IssueTemporaryToken) {
            NControllerAgent::ValidateEnvironmentVariableName(spec->TemporaryTokenEnvironmentVariableName);
        }

        if (spec->Alias && !spec->Alias->StartsWith(OperationAliasPrefix)) {
            THROW_ERROR_EXCEPTION("Operation alias should start with %Qv", OperationAliasPrefix)
                << TErrorAttribute("operation_alias", spec->Alias);
        }

        constexpr int MaxAnnotationsYsonTextLength = 10_KB;
        if (ConvertToYsonString(spec->Annotations, EYsonFormat::Text).AsStringBuf().size() > MaxAnnotationsYsonTextLength) {
            THROW_ERROR_EXCEPTION("Length of annotations YSON text representation should not exceed %v",
                MaxAnnotationsYsonTextLength);
        }

        if (spec->AcoName) {
            if (spec->AddAuthenticatedUserToAcl) {
                THROW_ERROR_EXCEPTION("Cannot use \"add_authenticated_user_to_acl\" with aco");
            }
            if (spec->Acl) {
                THROW_ERROR_EXCEPTION(NScheduler::EErrorCode::CannotUseBothAclAndAco, "Cannot use both ACL and ACO name");
            }
            if (spec->Owners) {
                THROW_ERROR_EXCEPTION(NScheduler::EErrorCode::CannotUseBothAclAndAco, "Cannot use both ACL and ACO name");
            }
        }

        if (spec->Acl) {
            ValidateOperationAcl(*spec->Acl);
        }
        if (spec->Owners) {
            if (!spec->Acl) {
                spec->Acl = TSerializableAccessControlList();
            }
            ProcessAclAndOwnersParameters(&(*spec->Acl), &(*spec->Owners));
        }

        ValidateSecurityTags(spec->AdditionalSecurityTags);
        ValidateProfilers(spec->Profilers);

        {
            THashSet<TString> jobShellNames;
            for (const auto& jobShell : spec->JobShells) {
                if (!jobShellNames.emplace(jobShell->Name).second) {
                    THROW_ERROR_EXCEPTION("Job shell names should be distinct")
                        << TErrorAttribute("duplicate_shell_name", jobShell->Name);
                }
            }
        }

        if (spec->UseColumnarStatistics) {
            spec->InputTableColumnarStatistics->Enabled = true;
            spec->UserFileColumnarStatistics->Enabled = true;
        }

        if (spec->BatchRowCount && spec->Sampling && spec->Sampling->SamplingRate) {
            THROW_ERROR_EXCEPTION("Option \"batch_row_count\" cannot be used with input sampling");
        }

        // COMPAT(omgronny)
        if (spec->CudaProfilerEnvironmentVariables.empty() && spec->CudaProfilerEnvironment) {
            EmplaceOrCrash(
                spec->CudaProfilerEnvironmentVariables,
                spec->CudaProfilerEnvironment->PathEnvironmentVariableName,
                spec->CudaProfilerEnvironment->PathEnvironmentVariableValue);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TTaskOutputStreamConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("schema", &TThis::Schema)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TJobExperimentConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_failed_treatment_jobs", &TThis::MaxFailedTreatmentJobs)
        .Default(10);
    registrar.Parameter("switch_on_experiment_success", &TThis::SwitchOnExperimentSuccess)
        .Default(true);
    registrar.Parameter("alert_on_any_treatment_failure", &TThis::AlertOnAnyTreatmentFailure)
        .Default(false);

    registrar.Parameter("base_layer_path", &TThis::BaseLayerPath)
        .NonEmpty()
        .Default();

    registrar.Parameter("network_project", &TThis::NetworkProject)
        .NonEmpty()
        .Default();

    registrar.Postprocessor([] (TJobExperimentConfig* config) {
        if (config->BaseLayerPath && config->NetworkProject) {
            THROW_ERROR_EXCEPTION(
                "Options \"base_layer_path\" and \"network_project\" cannot be specified simultaneously")
                << TErrorAttribute("base_layer_path", config->BaseLayerPath)
                << TErrorAttribute("network_project", config->NetworkProject);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TUserJobSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("task_title", &TThis::TaskTitle)
        .Default();
    registrar.Parameter("file_paths", &TThis::FilePaths)
        .Default();
    registrar.Parameter("layer_paths", &TThis::LayerPaths)
        .Default();
    registrar.Parameter("format", &TThis::Format)
        .Default();
    registrar.Parameter("input_format", &TThis::InputFormat)
        .Default();
    registrar.Parameter("output_format", &TThis::OutputFormat)
        .Default();
    registrar.Parameter("output_streams", &TThis::OutputStreams)
        .Default();
    registrar.Parameter("enable_input_table_index", &TThis::EnableInputTableIndex)
        .Default();
    registrar.Parameter("environment", &TThis::Environment)
        .Default();
    registrar.Parameter("cpu_limit", &TThis::CpuLimit)
        .Default(1)
        .GreaterThanOrEqual(0);
    registrar.Parameter("gpu_limit", &TThis::GpuLimit)
        .Default(0)
        .GreaterThanOrEqual(0);
    registrar.Parameter("port_count", &TThis::PortCount)
        .Default(0)
        .GreaterThanOrEqual(0)
        .LessThanOrEqual(50);
    registrar.Parameter("job_time_limit", &TThis::JobTimeLimit)
        .Alias("exec_time_limit")
        .Default()
        .GreaterThanOrEqual(TDuration::Seconds(1));
    registrar.Parameter("prepare_time_limit", &TThis::PrepareTimeLimit)
        .Default(TDuration::Minutes(45))
        .GreaterThanOrEqual(TDuration::Minutes(1));
    registrar.Parameter("memory_limit", &TThis::MemoryLimit)
        .Default(512_MB)
        .GreaterThan(0)
        .LessThanOrEqual(16_TB);
    registrar.Parameter("memory_reserve_factor", &TThis::MemoryReserveFactor)
        .Default();
    registrar.Parameter("user_job_memory_digest_default_value", &TThis::UserJobMemoryDigestDefaultValue)
        .Default(0.5)
        .GreaterThan(0.)
        .LessThanOrEqual(1.);
    registrar.Parameter("user_job_memory_digest_lower_bound", &TThis::UserJobMemoryDigestLowerBound)
        .Default(0.05)
        .GreaterThan(0.)
        .LessThanOrEqual(1.);
    registrar.Parameter("ignore_memory_reserve_factor_less_than_one", &TThis::IgnoreMemoryReserveFactorLessThanOne)
        .Default(false);
    registrar.Parameter("user_job_resource_overdraft_memory_multiplier", &TThis::UserJobResourceOverdraftMemoryMultiplier)
        .Alias("resource_overdraft_memory_reserve_multiplier")
        .InRange(1.0, 10.0)
        .Default(std::nullopt);
    registrar.Parameter("job_proxy_memory_digest", &TThis::JobProxyMemoryDigest)
        .DefaultCtor([] {
            auto config = New<TLogDigestConfig>();
            config->LowerBound = 0.5;
            config->UpperBound = 2.0;
            config->DefaultValue = 1.0;
            return config;
        });
    registrar.Parameter("job_proxy_resource_overdraft_memory_multiplier", &TThis::JobProxyResourceOverdraftMemoryMultiplier)
        .InRange(1.0, 10.0)
        .Default(std::nullopt);
    registrar.Parameter("include_memory_mapped_files", &TThis::IncludeMemoryMappedFiles)
        .Default(true);
    registrar.Parameter("use_yamr_descriptors", &TThis::UseYamrDescriptors)
        .Default(false);
    registrar.Parameter("check_input_fully_consumed", &TThis::CheckInputFullyConsumed)
        .Default(false);
    registrar.Parameter("max_stderr_size", &TThis::MaxStderrSize)
        .Default(5_MB)
        .GreaterThan(0)
        .LessThanOrEqual(1_GB);

    registrar.Parameter("custom_statistics_count_limit", &TThis::CustomStatisticsCountLimit)
        .Default(128)
        .GreaterThan(0)
        .LessThanOrEqual(1024);
    registrar.Parameter("tmpfs_size", &TThis::TmpfsSize)
        .Default()
        .GreaterThan(0);
    registrar.Parameter("tmpfs_path", &TThis::TmpfsPath)
        .Default();
    registrar.Parameter("tmpfs_volumes", &TThis::TmpfsVolumes)
        .Default();
    registrar.Parameter("disk_space_limit", &TThis::DiskSpaceLimit)
        .Default()
        .GreaterThanOrEqual(0);
    registrar.Parameter("inode_limit", &TThis::InodeLimit)
        .Default()
        .GreaterThanOrEqual(0);
    registrar.Parameter("disk_request", &TThis::DiskRequest)
        .Default();
    registrar.Parameter("copy_files", &TThis::CopyFiles)
        .Default(false);
    registrar.Parameter("deterministic", &TThis::Deterministic)
        .Default(false);
    registrar.Parameter("use_porto_memory_tracking", &TThis::UsePortoMemoryTracking)
        .Default(false);
    registrar.Parameter("set_container_cpu_limit", &TThis::SetContainerCpuLimit)
        .Default(false);
    registrar.Parameter("force_core_dump", &TThis::ForceCoreDump)
        .Default(false);
    registrar.Parameter("interruption_signal", &TThis::InterruptionSignal)
        .Default();
    registrar.Parameter("signal_root_process_only", &TThis::SignalRootProcessOnly)
        .Default(false);
    registrar.Parameter("restart_exit_code", &TThis::RestartExitCode)
        .Default();
    registrar.Parameter("enable_gpu_layers", &TThis::EnableGpuLayers)
        .Default(true);
    registrar.Parameter("cuda_toolkit_version", &TThis::CudaToolkitVersion)
        .Default();
    registrar.Parameter("gpu_check_layer_name", &TThis::GpuCheckLayerName)
        .Default();
    registrar.Parameter("gpu_check_binary_path", &TThis::GpuCheckBinaryPath)
        .Default();
    registrar.Parameter("gpu_check_binary_args", &TThis::GpuCheckBinaryArgs)
        .Default();
    registrar.Parameter("job_speculation_timeout", &TThis::JobSpeculationTimeout)
        .Default()
        .GreaterThan(TDuration::Zero());
    registrar.Parameter("network_project", &TThis::NetworkProject)
        .NonEmpty()
        .Default();
    registrar.Parameter("enable_porto", &TThis::EnablePorto)
        .Default();
    registrar.Parameter("fail_job_on_core_dump", &TThis::FailJobOnCoreDump)
        .Default(true);
    // COMPAT(artemagafonov): RootFS is always writable, so the flag should be removed after the update of all nodes.
    registrar.Parameter("make_rootfs_writable", &TThis::MakeRootFSWritable)
        .Default(false);
    registrar.Parameter("enable_fuse", &TThis::EnableFuse)
        .Default(false);
    registrar.Parameter("use_smaps_memory_tracker", &TThis::UseSMapsMemoryTracker)
        .Default(false);
    registrar.Parameter("monitoring", &TThis::Monitoring)
        .DefaultNew();

    registrar.Parameter("system_layer_path", &TThis::SystemLayerPath)
        .Default();

    registrar.Parameter("docker_image", &TThis::DockerImage)
        .Default();

    registrar.Parameter("redirect_stdout_to_stderr", &TThis::RedirectStdoutToStderr)
        .Default(false);

    registrar.Parameter("profilers", &TThis::Profilers)
        .Default();

    registrar.Parameter("enable_rpc_proxy_in_job_proxy", &TThis::EnableRpcProxyInJobProxy)
        .Default(false);
    registrar.Parameter("rpc_proxy_worker_thread_pool_size", &TThis::RpcProxyWorkerThreadPoolSize)
        .Default(1)
        .GreaterThan(0);

    registrar.Parameter("fail_on_job_restart", &TThis::FailOnJobRestart)
        .Default(false);

    registrar.Parameter("extra_environment", &TThis::ExtraEnvironment)
        .Default();

    registrar.Parameter("archive_ttl", &TThis::ArchiveTtl)
        .Default();

    registrar.Postprocessor([] (TUserJobSpec* spec) {
        if ((spec->TmpfsSize || spec->TmpfsPath) && !spec->TmpfsVolumes.empty()) {
            THROW_ERROR_EXCEPTION(
                "Options \"tmpfs_size\" and \"tmpfs_path\" cannot be specified "
                "simultaneously with \"tmpfs_volumes\"")
                << TErrorAttribute("tmpfs_size", spec->TmpfsSize)
                << TErrorAttribute("tmpfs_path", spec->TmpfsPath)
                << TErrorAttribute("tmpfs_volumes", spec->TmpfsVolumes);
        }

        if (spec->TmpfsPath) {
            auto volume = New<TTmpfsVolumeConfig>();
            volume->Size = spec->TmpfsSize ? *spec->TmpfsSize : spec->MemoryLimit;
            volume->Path = *spec->TmpfsPath;
            spec->TmpfsVolumes.push_back(volume);
            spec->TmpfsPath = std::nullopt;
            spec->TmpfsSize = std::nullopt;
        }

        i64 totalTmpfsSize = 0;
        for (const auto& volume : spec->TmpfsVolumes) {
            if (!NFS::IsPathRelativeAndInvolvesNoTraversal(volume->Path)) {
                THROW_ERROR_EXCEPTION("Tmpfs path %v does not point inside the sandbox directory",
                    volume->Path);
            }
            totalTmpfsSize += volume->Size;
        }

        // Memory reserve should greater than or equal to tmpfs_size (see YT-5518 for more details).
        if (totalTmpfsSize > spec->MemoryLimit) {
            THROW_ERROR_EXCEPTION("Total size of tmpfs volumes must be less than or equal to memory limit")
                << TErrorAttribute("tmpfs_size", totalTmpfsSize)
                << TErrorAttribute("memory_limit", spec->MemoryLimit);
        }

        for (int i = 0; i < std::ssize(spec->TmpfsVolumes); ++i) {
            for (int j = 0; j < std::ssize(spec->TmpfsVolumes); ++j) {
                if (i == j) {
                    continue;
                }

                auto lhsFsPath = TFsPath(spec->TmpfsVolumes[i]->Path);
                auto rhsFsPath = TFsPath(spec->TmpfsVolumes[j]->Path);
                if (lhsFsPath.IsSubpathOf(rhsFsPath)) {
                    THROW_ERROR_EXCEPTION("Path of tmpfs volume %Qv is prefix of other tmpfs volume %Qv",
                        spec->TmpfsVolumes[i]->Path,
                        spec->TmpfsVolumes[j]->Path);
                }
            }
        }

        if (spec->MemoryReserveFactor &&
            (*spec->MemoryReserveFactor == 1.0 || !spec->IgnoreMemoryReserveFactorLessThanOne))
        {
            spec->UserJobMemoryDigestLowerBound = spec->UserJobMemoryDigestDefaultValue = *spec->MemoryReserveFactor;
        }

        auto memoryDigestLowerLimit = static_cast<double>(totalTmpfsSize) / spec->MemoryLimit;
        spec->UserJobMemoryDigestDefaultValue = std::min(
            1.0,
            std::max(spec->UserJobMemoryDigestDefaultValue, memoryDigestLowerLimit));
        spec->UserJobMemoryDigestLowerBound = std::min(
            1.0,
            std::max(spec->UserJobMemoryDigestLowerBound, memoryDigestLowerLimit));
        spec->UserJobMemoryDigestDefaultValue = std::max(spec->UserJobMemoryDigestLowerBound, spec->UserJobMemoryDigestDefaultValue);

        for (const auto& [variableName, _] : spec->Environment) {
            NControllerAgent::ValidateEnvironmentVariableName(variableName);
        }

        if (!spec->DiskSpaceLimit && spec->InodeLimit) {
            THROW_ERROR_EXCEPTION("Option \"inode_limit\" can be specified only with \"disk_space_limit\"");
        }

        if (spec->DiskSpaceLimit && spec->DiskRequest) {
            THROW_ERROR_EXCEPTION(
                "Options \"disk_space_limit\" and \"inode_limit\" cannot be specified "
                "together with \"disk_request\"")
                << TErrorAttribute("disk_space_limit", spec->DiskSpaceLimit)
                << TErrorAttribute("inode_limit", spec->InodeLimit)
                << TErrorAttribute("disk_request", spec->DiskRequest);
        }

        if (spec->DiskSpaceLimit) {
            spec->DiskRequest = New<TDiskRequestConfig>();
            spec->DiskRequest->DiskSpace = *spec->DiskSpaceLimit;
            spec->DiskRequest->InodeCount = spec->InodeLimit;
            spec->DiskSpaceLimit = std::nullopt;
            spec->InodeLimit = std::nullopt;
        }

        // COMPAT(artemagafonov): RootFS is always writable, so the flag should be removed after the update of all nodes.
        if (spec->MakeRootFSWritable && spec->LayerPaths.empty()) {
            THROW_ERROR_EXCEPTION("Option \"make_rootfs_writable\" cannot be set without specifying \"layer_paths\"");
        }

        if (spec->Profilers) {
            ValidateProfilers(*spec->Profilers);
        }

        if (spec->UseYamrDescriptors && spec->RedirectStdoutToStderr) {
            THROW_ERROR_EXCEPTION("Uncompatible options \"use_yamr_descriptors\" and \"redirect_stdout_to_stderr\" are both set");
        }
    });
}

void TUserJobSpec::InitEnableInputTableIndex(int inputTableCount, TJobIOConfigPtr jobIOConfig)
{
    if (!EnableInputTableIndex) {
        EnableInputTableIndex = (inputTableCount != 1);
    }

    jobIOConfig->ControlAttributes->EnableTableIndex = *EnableInputTableIndex;
}

////////////////////////////////////////////////////////////////////////////////

void TOptionalUserJobSpec::Register(TRegistrar registrar)
{
    registrar.BaseClassParameter("command", &TUserJobSpec::Command)
        .Default();
}

bool TOptionalUserJobSpec::IsNontrivial() const
{
    return Command != TString();
}

////////////////////////////////////////////////////////////////////////////////

void TMandatoryUserJobSpec::Register(TRegistrar registrar)
{
    registrar.BaseClassParameter("command", &TUserJobSpec::Command);
}

////////////////////////////////////////////////////////////////////////////////

void TGangManagerConfig::Register(TRegistrar /*registrar*/)
{ }

void TVanillaTaskSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("job_count", &TThis::JobCount)
        .GreaterThanOrEqual(1);
    registrar.Parameter("job_io", &TThis::JobIO)
        .DefaultNew();
    registrar.Parameter("output_table_paths", &TThis::OutputTablePaths)
        .Alias("output_paths")
        .Default();
    registrar.Parameter("restart_completed_jobs", &TThis::RestartCompletedJobs)
        .Default(false);
    registrar.Parameter("gang_manager", &TThis::GangManager)
        .Default();

    registrar.Postprocessor([] (TVanillaTaskSpec* spec) {
        if (spec->GangManager && spec->RestartCompletedJobs) {
            THROW_ERROR_EXCEPTION(
                "\"gang_manager\" and \"restart_completed_jobs\" can not be turned on both");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TQueryFilterOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_chunk_filter", &TThis::EnableChunkFilter)
        .Default(false);
    registrar.Parameter("enable_row_filter", &TThis::EnableRowFilter)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

void TInputQueryOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("use_system_columns", &TThis::UseSystemColumns)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TInputlyQueryableSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("input_query", &TThis::InputQuery)
        .Default();
    registrar.Parameter("input_schema", &TThis::InputSchema)
        .Default();
    registrar.Parameter("input_query_filter_options", &TThis::InputQueryFilterOptions)
        .DefaultNew();
    registrar.Parameter("input_query_options", &TThis::InputQueryOptions)
        .DefaultNew();

    registrar.Postprocessor([] (TInputlyQueryableSpec* spec) {
        if (spec->InputSchema && !spec->InputQuery) {
            THROW_ERROR_EXCEPTION("Found \"input_schema\" without \"input_query\" in operation spec");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TOperationWithUserJobSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("stderr_table_path", &TThis::StderrTablePath)
        .Default();
    registrar.Parameter("stderr_table_writer", &TThis::StderrTableWriter)
        // TODO(babenko): deprecate this
        .Alias("stderr_table_writer_config")
        .DefaultNew();

    registrar.Parameter("core_table_path", &TThis::CoreTablePath)
        .Default();
    registrar.Parameter("core_table_writer", &TThis::CoreTableWriter)
        // TODO(babenko): deprecate this
        .Alias("core_table_writer_config")
        .DefaultNew();

    registrar.Parameter("enable_cuda_gpu_core_dump", &TThis::EnableCudaGpuCoreDump)
        .Default(false);

    registrar.Postprocessor([] (TOperationWithUserJobSpec* spec) {
        if (spec->StderrTablePath) {
            *spec->StderrTablePath = spec->StderrTablePath->Normalize();
        }

        if (spec->CoreTablePath) {
            *spec->CoreTablePath = spec->CoreTablePath->Normalize();
        }

        if (spec->EnableCudaGpuCoreDump && !spec->CoreTablePath) {
            THROW_ERROR_EXCEPTION("\"enable_cuda_gpu_core_dump\" option requires \"core_table_path\" options to be set");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TSimpleOperationSpecBase::Register(TRegistrar registrar)
{
    registrar.Parameter("data_weight_per_job", &TThis::DataWeightPerJob)
        .Alias("data_size_per_job")
        .Default()
        .GreaterThan(0);
    registrar.Parameter("job_count", &TThis::JobCount)
        .Default()
        .GreaterThan(0);
    registrar.Parameter("max_job_count", &TThis::MaxJobCount)
        .Default()
        .GreaterThan(0);
    registrar.Parameter("max_data_slices_per_job", &TThis::MaxDataSlicesPerJob)
        .Default()
        .GreaterThan(0);
    registrar.Parameter("force_job_size_adjuster", &TThis::ForceJobSizeAdjuster)
        .Default(false);
    registrar.Parameter("force_allow_job_interruption", &TThis::ForceAllowJobInterruption)
        .Default(false);
    registrar.Parameter("locality_timeout", &TThis::LocalityTimeout)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("job_io", &TThis::JobIO)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TOperationWithInputSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("input_table_paths", &TThis::InputTablePaths)
        .Alias("input_paths")
        .NonEmpty();
}

////////////////////////////////////////////////////////////////////////////////

void TUnorderedOperationSpecBase::Register(TRegistrar registrar)
{
    registrar.Preprocessor([] (TUnorderedOperationSpecBase* spec) {
        spec->JobIO->TableReader->MaxBufferSize = 256_MB;
    });
}

////////////////////////////////////////////////////////////////////////////////

void TMapOperationSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("mapper", &TThis::Mapper)
        .DefaultNew();
    registrar.Parameter("output_table_paths", &TThis::OutputTablePaths)
        .Alias("output_paths")
        .Default();
    registrar.Parameter("ordered", &TThis::Ordered)
        .Default(false);

    registrar.Postprocessor([] (TMapOperationSpec* spec) {
        spec->Mapper->InitEnableInputTableIndex(spec->InputTablePaths.size(), spec->JobIO);
        spec->Mapper->TaskTitle = "Mapper";

        ValidateNoOutputStreams(spec->Mapper, EOperationType::Map);

        ValidateOutputTablePaths(spec->OutputTablePaths);
    });
}

////////////////////////////////////////////////////////////////////////////////

void TMergeOperationSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("output_table_path", &TThis::OutputTablePath)
        .Alias("output_path");
    registrar.Parameter("mode", &TThis::Mode)
        .Default(EMergeMode::Unordered);
    registrar.Parameter("combine_chunks", &TThis::CombineChunks)
        .Default(false);
    registrar.Parameter("force_transform", &TThis::ForceTransform)
        .Default(false);
    registrar.Parameter("schema_inference_mode", &TThis::SchemaInferenceMode)
        .Default(ESchemaInferenceMode::Auto);
}

////////////////////////////////////////////////////////////////////////////////

void TUnorderedMergeOperationSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("single_chunk_teleport_strategy", &TThis::SingleChunkTeleportStrategy)
        .Default(ESingleChunkTeleportStrategy::Enabled);
}

////////////////////////////////////////////////////////////////////////////////

void TSortedMergeOperationSpec::Register(TRegistrar registrar)
{
    registrar.Postprocessor([] (TSortedMergeOperationSpec* spec) {
        auto unrecognized = spec->GetRecursiveUnrecognized();
        THROW_ERROR_EXCEPTION_IF(
            unrecognized->FindChild("input_query") != nullptr,
            "\"input_query\" is not supported in a sorted merge operation; consider using ordered merge instead");
    });
}

////////////////////////////////////////////////////////////////////////////////

void TEraseOperationSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("table_path", &TThis::TablePath);
    registrar.Parameter("combine_chunks", &TThis::CombineChunks)
        .Default(false);
    registrar.Parameter("schema_inference_mode", &TThis::SchemaInferenceMode)
        .Default(ESchemaInferenceMode::Auto);
}

////////////////////////////////////////////////////////////////////////////////

void TSortedOperationSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("use_new_sorted_pool", &TThis::UseNewSortedPool)
        .Default(false);
    registrar.Parameter("merge_by", &TThis::MergeBy)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TReduceOperationSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("reducer", &TThis::Reducer)
        .DefaultNew();
    registrar.Parameter("input_table_paths", &TThis::InputTablePaths)
        .Alias("input_paths")
        .NonEmpty();
    registrar.Parameter("output_table_paths", &TThis::OutputTablePaths)
        .Alias("output_paths")
        .Default();

    registrar.Parameter("reduce_by", &TThis::ReduceBy)
        .Default();
    registrar.Parameter("sort_by", &TThis::SortBy)
        .Default();
    registrar.Parameter("join_by", &TThis::JoinBy)
        .Default();

    registrar.Parameter("enable_key_guarantee", &TThis::EnableKeyGuarantee)
        .Default();

    registrar.Parameter("pivot_keys", &TThis::PivotKeys)
        .Default();

    registrar.Parameter("validate_key_column_types", &TThis::ValidateKeyColumnTypes)
        .Default(true);

    registrar.Parameter("consider_only_primary_size", &TThis::ConsiderOnlyPrimarySize)
        .Default(false);

    registrar.Parameter("slice_foreign_chunks", &TThis::SliceForeignChunks)
        .Default(false);

    registrar.Parameter("foreign_table_lookup_keys_threshold", &TThis::ForeignTableLookupKeysThreshold)
        .Default();

    registrar.Parameter("foreign_table_lookup_data_weight_threshold", &TThis::ForeignTableLookupDataWeightThreshold)
        .Default();

    registrar.Postprocessor([] (TReduceOperationSpec* spec) {
        NTableClient::ValidateSortColumns(spec->JoinBy);
        NTableClient::ValidateSortColumns(spec->ReduceBy);
        NTableClient::ValidateSortColumns(spec->SortBy);

        bool hasPrimary = false;
        for (const auto& path : spec->InputTablePaths) {
            hasPrimary |= path.GetPrimary();
        }
        if (hasPrimary) {
            for (auto& path : spec->InputTablePaths) {
                path.Attributes().Set("foreign", !path.GetPrimary());
                path.Attributes().Remove("primary");
            }
        }

        spec->Reducer->InitEnableInputTableIndex(spec->InputTablePaths.size(), spec->JobIO);
        spec->Reducer->TaskTitle = "Reducer";

        ValidateNoOutputStreams(spec->Reducer, EOperationType::Reduce);

        ValidateOutputTablePaths(spec->OutputTablePaths);
    });
}

////////////////////////////////////////////////////////////////////////////////

void TSortOperationSpecBase::Register(TRegistrar registrar)
{
    registrar.Parameter("input_table_paths", &TThis::InputTablePaths)
        .Alias("input_paths")
        .NonEmpty();
    registrar.Parameter("partition_count", &TThis::PartitionCount)
        .Default()
        .GreaterThan(0);
    registrar.Parameter("max_partition_factor", &TThis::MaxPartitionFactor)
        .Default()
        .GreaterThan(1);
    registrar.Parameter("partition_data_weight", &TThis::PartitionDataWeight)
        .Alias("partition_data_size")
        .Default()
        .GreaterThan(0);
    registrar.Parameter("data_weight_per_sort_job", &TThis::DataWeightPerShuffleJob)
        .Alias("data_size_per_sort_job")
        .Default(2_GB)
        .GreaterThan(0);
    registrar.Parameter("data_weight_per_intermediate_partition_job", &TThis::DataWeightPerIntermediatePartitionJob)
        .Default(2_GB)
        .GreaterThan(0);
    registrar.Parameter("max_chunk_slice_per_shuffle_job", &TThis::MaxChunkSlicePerShuffleJob)
        .Default(8000)
        .GreaterThan(0);
    registrar.Parameter("max_chunk_slice_per_intermediate_partition_job", &TThis::MaxChunkSlicePerIntermediatePartitionJob)
        .Default(8000)
        .GreaterThan(0);
    registrar.Parameter("shuffle_start_threshold", &TThis::ShuffleStartThreshold)
        .Default(0.75)
        .InRange(0.0, 1.0);
    registrar.Parameter("merge_start_threshold", &TThis::MergeStartThreshold)
        .Default(0.9)
        .InRange(0.0, 1.0);
    registrar.Parameter("sort_locality_timeout", &TThis::SortLocalityTimeout)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("shuffle_network_limit", &TThis::ShuffleNetworkLimit)
        .Default(0);
    registrar.Parameter("max_shuffle_data_slice_count", &TThis::MaxShuffleDataSliceCount)
        .GreaterThan(0)
        .Default(10'000'000);
    registrar.Parameter("max_shuffle_job_count", &TThis::MaxShuffleJobCount)
        .GreaterThan(0)
        .Default(200'000);
    registrar.Parameter("max_merge_data_slice_count", &TThis::MaxMergeDataSliceCount)
        .GreaterThan(0)
        .Default(10'000'000);
    registrar.Parameter("sort_by", &TThis::SortBy)
        .Default();
    registrar.Parameter("enable_partitioned_data_balancing", &TThis::EnablePartitionedDataBalancing)
        .Default(false);
    registrar.Parameter("enable_intermediate_output_recalculation", &TThis::EnableIntermediateOutputRecalculation)
        .Default(true);
    registrar.Parameter("pivot_keys", &TThis::PivotKeys)
        .Default();
    registrar.Parameter("use_new_partitions_heuristic", &TThis::UseNewPartitionsHeuristic)
        .Default(false);
    registrar.Parameter("partition_size_factor", &TThis::PartitionSizeFactor)
        .GreaterThan(0)
        .LessThanOrEqual(1)
        .Default(0.7);
    registrar.Parameter("use_new_sorted_pool", &TThis::UseNewSortedPool)
        .Default(false);

    registrar.Parameter("samples_per_partition", &TThis::SamplesPerPartition)
        .Default(1000)
        .GreaterThan(1);

    registrar.Postprocessor([] (TSortOperationSpecBase* spec) {
        NTableClient::ValidateSortColumns(spec->SortBy);

        // Validate pivot_keys.
        for (const auto& pivotKey : spec->PivotKeys) {
            try {
                for (const auto& value : pivotKey) {
                    ValidateDataValueType(value.Type);
                }
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION(TError("Pivot keys are invalid") << ex);
            }

            if (pivotKey.GetCount() > std::ssize(spec->SortBy)) {
                THROW_ERROR_EXCEPTION("Pivot key cannot be longer than sort_by")
                    << TErrorAttribute("key", pivotKey)
                    << TErrorAttribute("sort_by", spec->SortBy);
            }
        }

        auto sortComparator = GetComparator(spec->SortBy);
        for (int index = 0; index + 1 < std::ssize(spec->PivotKeys); ++index) {
            const auto& upperBound = TKeyBound::FromRow() < spec->PivotKeys[index];
            const auto& nextUpperBound = TKeyBound::FromRow() < spec->PivotKeys[index + 1];
            if (sortComparator.CompareKeyBounds(upperBound, nextUpperBound) >= 0) {
                THROW_ERROR_EXCEPTION("Pivot keys should form a strictly increasing sequence")
                    << TErrorAttribute("pivot_key", spec->PivotKeys[index])
                    << TErrorAttribute("next_pivot_key", spec->PivotKeys[index + 1])
                    << TErrorAttribute("comparator", sortComparator);
            }
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TSortOperationSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("output_table_path", &TThis::OutputTablePath)
        .Alias("output_path");
    registrar.BaseClassParameter("partition_job_io", &TSortOperationSpec::PartitionJobIO)
        .DefaultNew();
    registrar.BaseClassParameter("sort_job_io", &TSortOperationSpec::SortJobIO)
        .DefaultNew();
    registrar.BaseClassParameter("merge_job_io", &TSortOperationSpec::MergeJobIO)
        .DefaultNew();

    // Provide custom names for shared settings.
    registrar.BaseClassParameter("partition_job_count", &TSortOperationSpec::PartitionJobCount)
        .Default()
        .GreaterThan(0);
    registrar.BaseClassParameter("data_weight_per_partition_job", &TSortOperationSpec::DataWeightPerPartitionJob)
        .Alias("data_size_per_partition_job")
        .Default()
        .GreaterThan(0);
    registrar.BaseClassParameter("simple_sort_locality_timeout", &TSortOperationSpec::SimpleSortLocalityTimeout)
        .Default(TDuration::Seconds(5));
    registrar.BaseClassParameter("simple_merge_locality_timeout", &TSortOperationSpec::SimpleMergeLocalityTimeout)
        .Default(TDuration::Seconds(5));
    registrar.BaseClassParameter("partition_locality_timeout", &TSortOperationSpec::PartitionLocalityTimeout)
        .Default(TDuration::Seconds(5));
    registrar.BaseClassParameter("merge_locality_timeout", &TSortOperationSpec::MergeLocalityTimeout)
        .Default(TDuration::Minutes(1));

    registrar.Parameter("max_input_data_weight", &TThis::MaxInputDataWeight)
        .GreaterThan(0)
        .Default(5_PB);

    registrar.Parameter("schema_inference_mode", &TThis::SchemaInferenceMode)
        .Default(ESchemaInferenceMode::Auto);

    registrar.BaseClassParameter("data_weight_per_sorted_merge_job", &TSortOperationSpec::DataWeightPerSortedJob)
        .Alias("data_size_per_sorted_merge_job")
        .Default();

    registrar.Preprocessor([] (TSortOperationSpec* spec) {
        spec->PartitionJobIO->TableReader->MaxBufferSize = 1_GB;
        spec->PartitionJobIO->TableWriter->MaxBufferSize = 2_GB;

        spec->SortJobIO->TableReader->MaxBufferSize = 1_GB;
        spec->SortJobIO->TableReader->RetryCount = 3;
        spec->SortJobIO->TableReader->PassCount = 50;

        // Output slices must be small enough to make reasonable jobs in sorted chunk pool.
        spec->SortJobIO->TableWriter->DesiredChunkWeight = 256_MB;
        spec->MergeJobIO->TableReader->RetryCount = 3;
        spec->MergeJobIO->TableReader->PassCount = 50;

        spec->MapSelectivityFactor = 1.0;
    });

    registrar.Postprocessor([&] (TSortOperationSpec* spec) {
        if (spec->SortBy.empty()) {
            THROW_ERROR_EXCEPTION("\"sort_by\" option should be set in Sort operations");
        }

        if (spec->Sampling && spec->Sampling->SamplingRate) {
            THROW_ERROR_EXCEPTION("Sampling in sort operation is not supported");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TMapReduceOperationSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("output_table_paths", &TThis::OutputTablePaths)
        .Alias("output_paths")
        .NonEmpty();
    registrar.Parameter("reduce_by", &TThis::ReduceBy)
        .Default();
    // Mapper can be absent -- leave it null by default.
    registrar.Parameter("mapper", &TThis::Mapper)
        .Default();
    // ReduceCombiner can be absent -- leave it null by default.
    registrar.Parameter("reduce_combiner", &TThis::ReduceCombiner)
        .Default();
    registrar.Parameter("reducer", &TThis::Reducer)
        .DefaultNew();
    registrar.BaseClassParameter("map_job_io", &TMapReduceOperationSpec::PartitionJobIO)
        .DefaultNew();
    registrar.BaseClassParameter("sort_job_io", &TMapReduceOperationSpec::SortJobIO)
        .DefaultNew();
    registrar.BaseClassParameter("reduce_job_io", &TMapReduceOperationSpec::MergeJobIO)
        .DefaultNew();

    registrar.Parameter("mapper_output_table_count", &TThis::MapperOutputTableCount)
        .Default(0)
        .GreaterThanOrEqual(0);

    // Provide custom names for shared settings.
    registrar.BaseClassParameter("map_job_count", &TMapReduceOperationSpec::PartitionJobCount)
        .Default()
        .GreaterThan(0);
    registrar.BaseClassParameter("data_weight_per_map_job", &TMapReduceOperationSpec::DataWeightPerPartitionJob)
        .Alias("data_size_per_map_job")
        .Default()
        .GreaterThan(0);
    registrar.BaseClassParameter("map_locality_timeout", &TMapReduceOperationSpec::PartitionLocalityTimeout)
        .Default(TDuration::Seconds(5));
    registrar.BaseClassParameter("reduce_locality_timeout", &TMapReduceOperationSpec::MergeLocalityTimeout)
        .Default(TDuration::Minutes(1));
    registrar.BaseClassParameter("map_selectivity_factor", &TMapReduceOperationSpec::MapSelectivityFactor)
        .Default(1.0)
        .GreaterThan(0);

    registrar.BaseClassParameter("data_weight_per_reduce_job", &TMapReduceOperationSpec::DataWeightPerSortedJob)
        .Alias("data_size_per_reduce_job")
        .Default();

    registrar.Parameter("force_reduce_combiners", &TThis::ForceReduceCombiners)
        .Default(false);

    registrar.Parameter("ordered", &TThis::Ordered)
        .Default(false);

    registrar.Parameter("enable_table_index_if_has_trivial_mapper", &TThis::EnableTableIndexIfHasTrivialMapper)
        .Default(false);

    registrar.Parameter("disable_sorted_input_in_reducer", &TThis::DisableSortedInputInReducer)
        .Default(false);

    registrar.Parameter("compute_pivot_keys_from_samples", &TThis::ComputePivotKeysFromSamples)
        .Default(false);

    // The following settings are inherited from base but make no sense for map-reduce:
    //   SimpleSortLocalityTimeout
    //   SimpleMergeLocalityTimeout
    //   MapSelectivityFactor

    registrar.Preprocessor([] (TMapReduceOperationSpec* spec) {
        spec->PartitionJobIO->TableReader->MaxBufferSize = 256_MB;
        spec->PartitionJobIO->TableWriter->MaxBufferSize = 2_GB;

        spec->SortJobIO->TableReader->MaxBufferSize = 1_GB;
        // Output slices must be small enough to make reasonable jobs in sorted chunk pool.
        spec->SortJobIO->TableWriter->DesiredChunkWeight = 256_MB;

        spec->SortJobIO->TableReader->RetryCount = 3;
        spec->SortJobIO->TableReader->PassCount = 50;

        spec->MergeJobIO->TableReader->RetryCount = 3;
        spec->MergeJobIO->TableReader->PassCount = 50 ;
    });

    registrar.Postprocessor([] (TMapReduceOperationSpec* spec) {
        auto throwError = [] (NTableClient::EControlAttribute attribute, const TString& jobType) {
            THROW_ERROR_EXCEPTION(
                "%Qlv control attribute is not supported by %Qlv jobs in map-reduce operation",
                attribute,
                jobType);
        };
        auto validateControlAttributes = [&] (const NFormats::TControlAttributesConfigPtr& attributes, const TString& jobType) {
            if (attributes->EnableRowIndex) {
                throwError(NTableClient::EControlAttribute::RowIndex, jobType);
            }
            if (attributes->EnableRangeIndex) {
                throwError(NTableClient::EControlAttribute::RangeIndex, jobType);
            }
            if (attributes->EnableTabletIndex) {
                throwError(NTableClient::EControlAttribute::TabletIndex, jobType);
            }
        };
        if (spec->ForceReduceCombiners && !spec->HasNontrivialReduceCombiner()) {
            THROW_ERROR_EXCEPTION("Found \"force_reduce_combiners\" without nontrivial \"reduce_combiner\" in operation spec");
        }
        validateControlAttributes(spec->MergeJobIO->ControlAttributes, "reduce");
        validateControlAttributes(spec->SortJobIO->ControlAttributes, "reduce_combiner");

        if (spec->HasNontrivialReduceCombiner()) {
            if (spec->MergeJobIO->ControlAttributes->EnableTableIndex != spec->SortJobIO->ControlAttributes->EnableTableIndex) {
                THROW_ERROR_EXCEPTION("%Qlv control attribute must be the same for \"reduce\" and \"reduce_combiner\" jobs",
                    NTableClient::EControlAttribute::TableIndex);
            }
        }

        if (!spec->ReduceBy.empty()) {
            NTableClient::ValidateKeyColumns(spec->ReduceBy);
        }

        if (spec->ReduceBy.empty()) {
            spec->ReduceBy = GetColumnNames(spec->SortBy);
        }

        auto generateSortColumnsFromReduceColumns = [&] {
            TSortColumns columns;
            for (const auto& reduceColumn : spec->ReduceBy) {
                columns.push_back(TColumnSortSchema{
                    .Name = reduceColumn,
                    .SortOrder = ESortOrder::Ascending
                });
            }
            return columns;
        };

        if (spec->SortBy.empty()) {
            spec->SortBy = generateSortColumnsFromReduceColumns();
        } else if (spec->DisableSortedInputInReducer) {
            // Checking if sort_by is specified at the beginning of Postprocess works correctly
            // the first time the config is parsed. However, when the specification is persisted
            // in CA and loaded again, it will fail. This is because SortBy is always set in
            // Postprocess, even if the user did not specify it explicitly.
            THROW_ERROR_EXCEPTION_IF(
                spec->SortBy != generateSortColumnsFromReduceColumns(),
                "\"sort_by\" cannot be specified when \"disable_sorted_input_in_reducer\" is set"
            );
        }

        if (spec->ReduceBy.empty() && spec->SortBy.empty()) {
            THROW_ERROR_EXCEPTION("At least one of the \"sort_by\" or \"reduce_by\" fields should be specified");
        }

        if (spec->HasNontrivialMapper()) {
            for (int i = 0; i < std::ssize(spec->Mapper->OutputStreams) - spec->MapperOutputTableCount; ++i) {
                const auto& stream = spec->Mapper->OutputStreams[i];
                if (stream->Schema->GetSortColumns() != spec->SortBy) {
                    THROW_ERROR_EXCEPTION("Schemas of mapper output streams should have exactly the same "
                        "\"sort_by\" sort column prefix")
                        << TErrorAttribute("violating_schema", stream->Schema)
                        << TErrorAttribute("sort_by", spec->SortBy);
                }
                auto sortColumnNames = GetColumnNames(spec->SortBy);
                const auto& firstStream = spec->Mapper->OutputStreams.front();
                if (*stream->Schema->Filter(sortColumnNames) != *firstStream->Schema->Filter(sortColumnNames)) {
                    THROW_ERROR_EXCEPTION("Key columns of mapper output streams should have the same names and types")
                        << TErrorAttribute("lhs_schema", firstStream->Schema)
                        << TErrorAttribute("rhs_schema", stream->Schema);
                }
            }
        }

        if (!spec->HasNontrivialMapper() && spec->EnableTableIndexIfHasTrivialMapper) {
            spec->Reducer->EnableInputTableIndex = true;
            if (spec->HasNontrivialReduceCombiner()) {
                spec->ReduceCombiner->EnableInputTableIndex = true;
            }
        }

        if (spec->HasNontrivialMapper()) {
            spec->Mapper->InitEnableInputTableIndex(spec->InputTablePaths.size(), spec->PartitionJobIO);
            spec->Mapper->TaskTitle = "Mapper";
        }

        auto intermediateStreamCount = 1;
        if (spec->HasNontrivialMapper()) {
            if (!spec->Mapper->OutputStreams.empty()) {
                intermediateStreamCount = spec->Mapper->OutputStreams.size();
            }
        } else {
            if (spec->Reducer->EnableInputTableIndex.value_or(false) ||
                spec->MergeJobIO->ControlAttributes->EnableTableIndex)
            {
                intermediateStreamCount = spec->InputTablePaths.size();
            }
        }

        if (spec->HasNontrivialReduceCombiner()) {
            if (intermediateStreamCount > 1) {
                THROW_ERROR_EXCEPTION("Nontrivial reduce combiner is not allowed with several intermediate streams");
            }
            spec->ReduceCombiner->InitEnableInputTableIndex(intermediateStreamCount, spec->SortJobIO);
            spec->ReduceCombiner->TaskTitle = "Reduce combiner";
        }

        if (spec->HasNontrivialMapper() && spec->ComputePivotKeysFromSamples) {
            THROW_ERROR_EXCEPTION("Pivot keys computation is not allowed with nontrivial mapper");
        }

        spec->Reducer->InitEnableInputTableIndex(intermediateStreamCount, spec->MergeJobIO);
        spec->Reducer->TaskTitle = "Reducer";

        if (intermediateStreamCount > 1) {
            if (!spec->HasNontrivialMapper()) {
                spec->PartitionJobIO->ControlAttributes->EnableTableIndex = true;
            }
            spec->MergeJobIO->ControlAttributes->EnableTableIndex = true;
            spec->SortJobIO->ControlAttributes->EnableTableIndex = true;
        }

        if (spec->Sampling && spec->Sampling->SamplingRate) {
            spec->MapSelectivityFactor *= *spec->Sampling->SamplingRate;
        }

        if (spec->MapperOutputTableCount >= std::ssize(spec->OutputTablePaths) ||
            (spec->HasNontrivialMapper() && !spec->Mapper->OutputStreams.empty() && spec->MapperOutputTableCount >= std::ssize(spec->Mapper->OutputStreams)))
        {
            auto error = TError("There should be at least one non-mapper output table; maybe you need \"map\" operation instead?")
                << TErrorAttribute("mapper_output_table_count", spec->MapperOutputTableCount)
                << TErrorAttribute("output_table_count", spec->OutputTablePaths.size());
            if (spec->HasNontrivialMapper()) {
                error = error << TErrorAttribute("mapper_output_stream_count", spec->Mapper->OutputStreams.size());
            }
            THROW_ERROR error;
        }

        ValidateOutputTablePaths(spec->OutputTablePaths);
    });
}

bool TMapReduceOperationSpec::HasNontrivialMapper() const
{
    return Mapper && Mapper->IsNontrivial();
}

bool TMapReduceOperationSpec::HasNontrivialReduceCombiner() const
{
    return ReduceCombiner && ReduceCombiner->IsNontrivial();
}

bool TMapReduceOperationSpec::HasSchemafulIntermediateStreams() const
{
    if (HasNontrivialMapper()) {
        return !Mapper->OutputStreams.empty();
    }
    return MergeJobIO->ControlAttributes->EnableTableIndex;
}

////////////////////////////////////////////////////////////////////////////////

void TRemoteCopyOperationSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("cluster_name", &TThis::ClusterName)
        .NonEmpty()
        .Default();
    registrar.Parameter("input_table_paths", &TThis::InputTablePaths)
        .Alias("input_paths")
        .NonEmpty();
    registrar.Parameter("output_table_path", &TThis::OutputTablePath)
        .Alias("output_path");
    registrar.Parameter("network_name", &TThis::NetworkName)
        .NonEmpty()
        .Default();
    registrar.Parameter("networks", &TThis::Networks)
        .Alias("network_names")
        .Default();
    registrar.Parameter("cluster_connection", &TThis::ClusterConnection)
        .Default();
    registrar.Parameter("copy_attributes", &TThis::CopyAttributes)
        .Default(false);
    registrar.Parameter("attribute_keys", &TThis::AttributeKeys)
        .Default();
    registrar.Parameter("force_copy_system_attributes", &TThis::ForceCopySystemAttributes)
        .Default(false);
    registrar.Parameter("concurrency", &TThis::Concurrency)
        .Default(4);
    registrar.Parameter("block_buffer_size", &TThis::BlockBufferSize)
        .Default(64_MB);
    registrar.Parameter("schema_inference_mode", &TThis::SchemaInferenceMode)
        .Default(ESchemaInferenceMode::Auto);
    registrar.Parameter("delay_in_copy_chunk", &TThis::DelayInCopyChunk)
        .Default(TDuration::Zero());
    registrar.Parameter("erasure_chunk_repair_delay", &TThis::ErasureChunkRepairDelay)
        .Default(TDuration::Minutes(15));
    registrar.Parameter("repair_erasure_chunks", &TThis::RepairErasureChunks)
        .Default(false);
    registrar.Parameter("use_remote_master_caches", &TThis::UseRemoteMasterCaches)
        .Default(false);
    registrar.Parameter("allow_cluster_connection", &TThis::AllowClusterConnection)
        .Default(true);

    registrar.Preprocessor([] (TRemoteCopyOperationSpec* spec) {
        // NB: In remote copy operation chunks are never decompressed,
        // so the data weight does not affect anything.
        spec->MaxDataWeightPerJob = std::numeric_limits<i64>::max();
    });
    registrar.Postprocessor([] (TRemoteCopyOperationSpec* spec) {
        if (spec->InputTablePaths.size() > 1) {
            THROW_ERROR_EXCEPTION("Multiple tables in remote copy are not supported");
        }

        if (spec->NetworkName) {
            if (spec->Networks) {
                THROW_ERROR_EXCEPTION("Options \"network_name\" and \"networks\"/\"network_names\" cannot be specified simultaneously");
            }
            spec->Networks = {*spec->NetworkName};
            spec->NetworkName.reset();
        }

        if (!spec->ClusterName && !spec->ClusterConnection) {
            THROW_ERROR_EXCEPTION("Neither cluster name nor cluster connection specified");
        }

        if (spec->Sampling && spec->Sampling->SamplingRate) {
            THROW_ERROR_EXCEPTION("You do not want sampling in remote copy operation :)");
        }

        if (spec->RepairErasureChunks) {
            // If we are OK with repairing chunks, we are OK with repairing chunks from parity parts.
            spec->ChunkAvailabilityPolicy = NChunkClient::EChunkAvailabilityPolicy::Repairable;
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TVanillaOperationSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("tasks", &TThis::Tasks)
        .NonEmpty();

    registrar.Postprocessor([] (TVanillaOperationSpec* spec) {
        TStringBuf taskWithGangManagerName;
        TStringBuf taskWithFailOnJobRestartName;
        TStringBuf taskWithOutputTableName;
        for (const auto& [taskName, taskSpec] : spec->Tasks) {
            if (taskName.empty()) {
                THROW_ERROR_EXCEPTION("Empty task names are not allowed");
            }

            taskSpec->TaskTitle = taskName;

            ValidateNoOutputStreams(taskSpec, EOperationType::Vanilla);

            ValidateOutputTablePaths(taskSpec->OutputTablePaths);

            if (taskSpec->GangManager) {
                taskWithGangManagerName = taskName;
            }
            if (taskSpec->FailOnJobRestart) {
                taskWithFailOnJobRestartName = taskName;
            }
            if (!empty(taskSpec->OutputTablePaths)) {
                taskWithOutputTableName = taskName;
            }
        }

        if (taskWithGangManagerName && spec->FailOnJobRestart) {
            THROW_ERROR_EXCEPTION(
                "Operation with \"fail_on_job_restart\" enabled can not have tasks with configured \"gang_manager\"")
                << TErrorAttribute("task_with_gang_manager_name", taskWithGangManagerName);
        }

        if (taskWithGangManagerName && taskWithFailOnJobRestartName) {
            THROW_ERROR_EXCEPTION(
                "Operation can not have both task with \"gang_manager\" and task with \"fail_on_job_restart\"")
                << TErrorAttribute("task_with_gang_manager_name", taskWithGangManagerName)
                << TErrorAttribute("task_with_fail_on_job_restart_name", taskWithFailOnJobRestartName);
        }

        if (taskWithOutputTableName && taskWithGangManagerName) {
            THROW_ERROR_EXCEPTION(
                "Gang operations having output tables are not currently supported")
                << TErrorAttribute("task_with_output_table_name", taskWithOutputTableName)
                << TErrorAttribute("task_with_gang_manager_name", taskWithGangManagerName);
        }

        if (spec->Sampling && spec->Sampling->SamplingRate) {
            THROW_ERROR_EXCEPTION("You do not want sampling in vanilla operation :)");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TJobResourcesConfig::Register(TRegistrar registrar)
{
    registrar.BaseClassParameter("user_slots", &TJobResourcesConfig::UserSlots)
        .Default()
        .GreaterThanOrEqual(0);
    registrar.BaseClassParameter("cpu", &TJobResourcesConfig::Cpu)
        .Default()
        .GreaterThanOrEqual(0);
    registrar.BaseClassParameter("network", &TJobResourcesConfig::Network)
        .Default()
        .GreaterThanOrEqual(0);
    registrar.BaseClassParameter("memory", &TJobResourcesConfig::Memory)
        .Default()
        .GreaterThanOrEqual(0);
    registrar.BaseClassParameter("gpu", &TJobResourcesConfig::Gpu)
        .Default()
        .GreaterThanOrEqual(0);
}

TJobResourcesConfigPtr TJobResourcesConfig::Clone()
{
    auto result = New<TJobResourcesConfig>();
    ForEachResource([&result, this] (auto NVectorHdrf::TJobResourcesConfig::* resourceDataMember, EJobResourceType /*resourceType*/) {
        result.Get()->*resourceDataMember = this->*resourceDataMember;
    });
    return result;
}

TJobResourcesConfigPtr TJobResourcesConfig::operator-()
{
    auto result = New<TJobResourcesConfig>();
    ForEachResource([&result, this] (auto NVectorHdrf::TJobResourcesConfig::* resourceDataMember, EJobResourceType /*resourceType*/) {
        if ((this->*resourceDataMember).has_value()) {
            result.Get()->*resourceDataMember = -*(this->*resourceDataMember);
        }
    });
    return result;
}

////////////////////////////////////////////////////////////////////////////////

void TCommonPreemptionConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("fair_share_starvation_timeout", &TThis::FairShareStarvationTimeout)
        .Alias("fair_share_preemption_timeout")
        .Default();
    registrar.Parameter("fair_share_starvation_tolerance", &TThis::FairShareStarvationTolerance)
        .InRange(0.0, 1.0)
        .Default();

    registrar.Parameter("non_preemptible_resource_usage_threshold", &TThis::NonPreemptibleResourceUsageThreshold)
        .Default();
}

void TPoolPreemptionConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_aggressive_starvation", &TThis::EnableAggressiveStarvation)
        .Default();
    registrar.Parameter("allow_aggressive_preemption", &TThis::AllowAggressivePreemption)
        .Alias("allow_aggressive_starvation_preemption")
        .Default();

    registrar.Parameter("allow_normal_preemption", &TThis::AllowNormalPreemption)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

void TPoolPresetConfig::Register(TRegistrar registrar)
{
    registrar.UnrecognizedStrategy(EUnrecognizedStrategy::KeepRecursive);

    registrar.Parameter("allow_regular_allocations_on_ssd_nodes", &TThis::AllowRegularAllocationsOnSsdNodes)
        .Alias("allow_regular_jobs_on_ssd_nodes")
        .Default(true);

    registrar.Parameter("enable_lightweight_operations", &TThis::EnableLightweightOperations)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TSchedulableConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("weight", &TThis::Weight)
        .Default()
        .InRange(MinSchedulableWeight, MaxSchedulableWeight);

    registrar.Parameter("max_share_ratio", &TThis::MaxShareRatio)
        .Default()
        .InRange(0.0, 1.0);
    registrar.Parameter("resource_limits", &TThis::ResourceLimits)
        .DefaultNew();

    registrar.Parameter("strong_guarantee_resources", &TThis::StrongGuaranteeResources)
        .Alias("min_share_resources")
        .DefaultNew();
    registrar.Parameter("scheduling_tag_filter", &TThis::SchedulingTagFilter)
        .Alias("scheduling_tag")
        .Default();

    registrar.Postprocessor([&] (TSchedulableConfig* config) {
        if (config->SchedulingTagFilter.Size() > MaxSchedulingTagRuleCount) {
            THROW_ERROR_EXCEPTION("Specifying more than %v tokens in scheduling tag filter is not allowed",
                MaxSchedulingTagRuleCount);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TExtendedSchedulableConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("pool", &TThis::Pool)
        .NonEmpty()
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TEphemeralSubpoolConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("mode", &TThis::Mode)
        .Default(ESchedulingMode::FairShare);

    registrar.Parameter("max_running_operation_count", &TThis::MaxRunningOperationCount)
        .Default();

    registrar.Parameter("max_operation_count", &TThis::MaxOperationCount)
        .Default();

    registrar.Parameter("resource_limits", &TThis::ResourceLimits)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TPoolIntegralGuaranteesConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("guarantee_type", &TThis::GuaranteeType)
        .Default(EIntegralGuaranteeType::None);

    registrar.Parameter("burst_guarantee_resources", &TThis::BurstGuaranteeResources)
        .DefaultNew();

    registrar.Parameter("resource_flow", &TThis::ResourceFlow)
        .DefaultNew();

    registrar.Parameter("relaxed_share_multiplier_limit", &TThis::RelaxedShareMultiplierLimit)
        .InRange(1, 10)
        .Default();

    registrar.Parameter("can_accept_free_volume", &TThis::CanAcceptFreeVolume)
        .Default(true);

    registrar.Parameter("should_distribute_free_volume_among_children", &TThis::ShouldDistributeFreeVolumeAmongChildren)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TPoolConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("mode", &TThis::Mode)
        .Default(ESchedulingMode::FairShare);

    registrar.Parameter("max_running_operation_count", &TThis::MaxRunningOperationCount)
        .Default()
        .GreaterThanOrEqual(0);

    registrar.Parameter("max_operation_count", &TThis::MaxOperationCount)
        .Default()
        .GreaterThanOrEqual(0);

    registrar.Parameter("fifo_sort_parameters", &TThis::FifoSortParameters)
        .Default({EFifoSortParameter::Weight, EFifoSortParameter::StartTime})
        .NonEmpty();

    registrar.Parameter("forbid_immediate_operations", &TThis::ForbidImmediateOperations)
        .Default(false);

    registrar.Parameter("create_ephemeral_subpools", &TThis::CreateEphemeralSubpools)
        .Default(false);

    registrar.Parameter("ephemeral_subpool_config", &TThis::EphemeralSubpoolConfig)
        .Default();

    registrar.Parameter("historic_usage_aggregation_period", &TThis::HistoricUsageAggregationPeriod)
        .Default();

    registrar.Parameter("allowed_profiling_tags", &TThis::AllowedProfilingTags)
        .Default();

    registrar.Parameter("enable_by_user_profiling", &TThis::EnableByUserProfiling)
        .Default();

    registrar.Parameter("abc", &TThis::Abc)
        .Default();

    registrar.Parameter("integral_guarantees", &TThis::IntegralGuarantees)
        .DefaultNew();

    registrar.Parameter("enable_detailed_logs", &TThis::EnableDetailedLogs)
        .Default(false);

    registrar.Parameter("config_presets", &TThis::ConfigPresets)
        .Default();

    registrar.Parameter("config_preset", &TThis::ConfigPreset)
        .Default();

    registrar.Parameter("enable_fair_share_truncation_in_fifo_pool", &TThis::EnableFairShareTruncationInFifoPool)
        .Alias("truncate_fifo_pool_unsatisfied_child_fair_share")
        .Default();

    registrar.Parameter("metering_tags", &TThis::MeteringTags)
        .Default();

    registrar.Parameter("offloading_settings", &TThis::OffloadingSettings)
        .Default();

    registrar.Parameter("allow_idle_cpu_policy", &TThis::AllowIdleCpuPolicy)
        .Default();

    registrar.Parameter("compute_promised_guarantee_fair_share", &TThis::ComputePromisedGuaranteeFairShare)
        .Default(false);

    registrar.Parameter("enable_priority_scheduling_segment_module_assignment", &TThis::EnablePrioritySchedulingSegmentModuleAssignment)
        .Default();

    registrar.Parameter("redirect_to_cluster", &TThis::RedirectToCluster)
        .Default();

    registrar.Parameter("enable_priority_strong_guarantee_adjustment", &TThis::EnablePriorityStrongGuaranteeAdjustment)
        .Default(false);

    registrar.Parameter("enable_priority_strong_guarantee_adjustment_donorship", &TThis::EnablePriorityStrongGuaranteeAdjustmentDonorship)
        .Default(false);

    registrar.Parameter("always_allow_gang_operations", &TThis::AlwaysAllowGangOperations)
        .Default(false);

    registrar.Postprocessor([] (TThis* config) {
        // COMPAT(omgronny)
        if (config->ConfigPreset && !config->ConfigPresets.empty()) {
            THROW_ERROR_EXCEPTION("Cannot specify both %Qv and %Qv at the same time",
                "config_preset",
                "config_presets");
        }
    });
}

void TPoolConfig::Validate(const TString& poolName)
{
    Postprocess();

    if (MaxOperationCount && MaxRunningOperationCount && *MaxOperationCount < *MaxRunningOperationCount) {
        THROW_ERROR_EXCEPTION("%Qv must be greater than or equal to %Qv, but %v < %v",
            "max_operation_count",
            "max_running_operation_count",
            *MaxOperationCount,
            *MaxRunningOperationCount)
            << TErrorAttribute("pool_name", poolName);
    }
    if (AllowedProfilingTags.size() > MaxAllowedProfilingTagCount) {
        THROW_ERROR_EXCEPTION("Limit for the number of allowed profiling tags exceeded")
            << TErrorAttribute("allowed_profiling_tag_count", AllowedProfilingTags.size())
            << TErrorAttribute("max_allowed_profiling_tag_count", MaxAllowedProfilingTagCount)
            << TErrorAttribute("pool_name", poolName);
    }
    if (IntegralGuarantees->BurstGuaranteeResources->IsNonTrivial() &&
        IntegralGuarantees->GuaranteeType == EIntegralGuaranteeType::Relaxed)
    {
        THROW_ERROR_EXCEPTION("Burst guarantees cannot be specified for integral guarantee type \"relaxed\"")
            << TErrorAttribute("integral_guarantee_type", IntegralGuarantees->GuaranteeType)
            << TErrorAttribute("burst_guarantee_resources", IntegralGuarantees->BurstGuaranteeResources)
            << TErrorAttribute("pool_name", poolName);
    }
    if (!MeteringTags.empty() && !Abc) {
        THROW_ERROR_EXCEPTION("Metering tags can be specified only for pool with specified abc attribute")
            << TErrorAttribute("pool_name", poolName);
    }

    if (Mode == ESchedulingMode::Fifo && CreateEphemeralSubpools) {
        THROW_ERROR_EXCEPTION("Fifo pool cannot create ephemeral subpools");
    }
}

////////////////////////////////////////////////////////////////////////////////

void TFairShareStrategyPackingConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(false);

    registrar.Parameter("metric", &TThis::Metric)
        .Default(EPackingMetricType::AngleLength);

    registrar.Parameter("max_better_past_snapshots", &TThis::MaxBetterPastSnapshots)
        .Default(2);
    registrar.Parameter("absolute_metric_value_tolerance", &TThis::AbsoluteMetricValueTolerance)
        .Default(0.05)
        .GreaterThanOrEqual(0.0);
    registrar.Parameter("relative_metric_value_tolerance", &TThis::RelativeMetricValueTolerance)
        .Default(1.5)
        .GreaterThanOrEqual(1.0);
    registrar.Parameter("min_window_size_for_schedule", &TThis::MinWindowSizeForSchedule)
        .Default(0)
        .GreaterThanOrEqual(0);
    registrar.Parameter("max_heartbeat_window_size", &TThis::MaxHeartbeatWindowSize)
        .Default(10);
    registrar.Parameter("max_heartbeat_age", &TThis::MaxHeartbeatAge)
        .Default(TDuration::Hours(1));
}

////////////////////////////////////////////////////////////////////////////////

static constexpr int MaxSchedulingSegmentModuleCount = 8;

void TStrategyOperationSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("pool", &TThis::Pool)
        .NonEmpty()
        .Default();
    registrar.Parameter("scheduling_options_per_pool_tree", &TThis::SchedulingOptionsPerPoolTree)
        .Alias("fair_share_options_per_pool_tree")
        .Default();
    registrar.Parameter("pool_trees", &TThis::PoolTrees)
        .Default();
    registrar.Parameter("max_concurrent_schedule_allocation_calls", &TThis::MaxConcurrentControllerScheduleAllocationCalls)
        .Alias("max_concurrent_schedule_job_calls")
        .Alias("max_concurrent_controller_schedule_job_calls")
        .Default();
    registrar.Parameter("schedule_in_single_tree", &TThis::ScheduleInSingleTree)
        .Default(false);
    registrar.Parameter("consider_guarantees_for_single_tree", &TThis::ConsiderGuaranteesForSingleTree)
        .Default(false);
    registrar.Parameter("tentative_pool_trees", &TThis::TentativePoolTrees)
        .Default();
    registrar.Parameter("use_default_tentative_pool_trees", &TThis::UseDefaultTentativePoolTrees)
        .Default(false);
    registrar.Parameter("tentative_tree_eligibility", &TThis::TentativeTreeEligibility)
        .DefaultNew();
    registrar.Parameter("update_preemptible_allocations_list_logging_period", &TThis::UpdatePreemptibleAllocationsListLoggingPeriod)
        .Alias("update_preemptible_jobs_list_logging_period")
        .Default(1000);
    registrar.Parameter("custom_profiling_tag", &TThis::CustomProfilingTag)
        .Default();
    registrar.Parameter("max_unpreemptible_allocation_count", &TThis::MaxUnpreemptibleRunningAllocationCount)
        .Alias("max_unpreemptible_job_count")
        .Default();
    registrar.Parameter("try_avoid_duplicating_jobs", &TThis::TryAvoidDuplicatingJobs)
        .Default();
    registrar.Parameter("max_speculative_job_count_per_task", &TThis::MaxSpeculativeJobCountPerTask)
        .Default(10);
    registrar.Parameter("max_probing_job_count_per_task", &TThis::MaxProbingJobCountPerTask)
        .Default(10);
    registrar.Parameter("probing_ratio", &TThis::ProbingRatio)
        .InRange(0, 1)
        .Default();
    registrar.Parameter("probing_pool_tree", &TThis::ProbingPoolTree)
        .Default();

    // TODO(ignat): move it to preemption settings.
    registrar.Parameter("preemption_mode", &TThis::PreemptionMode)
        .Default(EPreemptionMode::Normal);
    registrar.Parameter("scheduling_segment", &TThis::SchedulingSegment)
        .Default();
    registrar.Parameter("scheduling_segment_modules", &TThis::SchedulingSegmentModules)
        .Alias("scheduling_segment_data_centers")
        .NonEmpty()
        .Default();
    registrar.Parameter("enable_limiting_ancestor_check", &TThis::EnableLimitingAncestorCheck)
        .Default(true);
    registrar.Parameter("is_gang", &TThis::IsGang)
        .Default(false);
    registrar.Parameter("testing", &TThis::TestingOperationOptions)
        .DefaultNew();
    registrar.Parameter("erase_trees_with_pool_limit_violations", &TThis::EraseTreesWithPoolLimitViolations)
        .Default(false);
    registrar.Parameter("apply_specified_resource_limits_to_demand", &TThis::ApplySpecifiedResourceLimitsToDemand)
        .Default(false);
    registrar.Parameter("allow_idle_cpu_policy", &TThis::AllowIdleCpuPolicy)
        .Default();

    registrar.Postprocessor([] (TStrategyOperationSpec* spec) {
        if (spec->SchedulingSegmentModules && spec->SchedulingSegmentModules->size() >= MaxSchedulingSegmentModuleCount) {
            THROW_ERROR_EXCEPTION("%Qv size must be not greater than %v",
                "scheduling_segment_modules",
                MaxSchedulingSegmentModuleCount);
        }
        if (spec->ScheduleInSingleTree && (spec->TentativePoolTrees || spec->UseDefaultTentativePoolTrees)) {
            THROW_ERROR_EXCEPTION("%Qv option cannot be used simultaneously with tentative pool trees (check %Qv and %Qv)",
                "schedule_in_single_tree",
                "tentative_pool_trees",
                "use_default_tentative_pool_trees");
        }
        if (!spec->ScheduleInSingleTree && spec->ConsiderGuaranteesForSingleTree) {
            THROW_ERROR_EXCEPTION("%Qv option cannot be used without %Qv",
                "consider_guarantees_for_single_tree",
                "schedule_in_single_tree");
        }

        // COMPAT(eshcherbin)
        if (spec->MaxUnpreemptibleRunningAllocationCount) {
            if (*spec->MaxUnpreemptibleRunningAllocationCount != 0) {
                THROW_ERROR_EXCEPTION("%Qv or %Qv cannot be set to a non-zero value, use %Qv instead",
                    "max_unpreemptible_allocation_count",
                    "max_unpreemptible_job_count",
                    "non_preemptible_resource_usage_threshold");
            }

            if (!spec->NonPreemptibleResourceUsageThreshold) {
                spec->NonPreemptibleResourceUsageThreshold = New<TJobResourcesConfig>();
            }
            if (!spec->NonPreemptibleResourceUsageThreshold->UserSlots) {
                spec->NonPreemptibleResourceUsageThreshold->UserSlots = *spec->MaxUnpreemptibleRunningAllocationCount;
            }
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TOperationJobShellRuntimeParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("owners", &TThis::Owners)
        .Default();
}

void TOperationFairShareTreeRuntimeParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("weight", &TThis::Weight)
        .Optional()
        .InRange(MinSchedulableWeight, MaxSchedulableWeight);
    registrar.Parameter("pool", &TThis::Pool);
    registrar.Parameter("resource_limits", &TThis::ResourceLimits)
        .DefaultNew();
    registrar.Parameter("enable_detailed_logs", &TThis::EnableDetailedLogs)
        .Default(false);
    registrar.Parameter("tentative", &TThis::Tentative)
        .Default(false);
    registrar.Parameter("probing", &TThis::Probing)
        .Default(false);
    registrar.Parameter("offloading", &TThis::Offloading)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void SerializeHeavyRuntimeParameters(NYTree::TFluentMap fluent, const TOperationRuntimeParameters& parameters)
{
    fluent
        .OptionalItem("annotations", parameters.Annotations);
}

void Serialize(const TOperationRuntimeParameters& parameters, IYsonConsumer* consumer, bool serializeHeavy)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("owners").Value(parameters.Owners)
            .DoIf(!parameters.Acl.Entries.empty(), [&] (auto fluent) {
                fluent.Item("acl").Value(parameters.Acl);
            })
            .OptionalItem("aco_name", parameters.AcoName)
            .Item("scheduling_options_per_pool_tree").Value(parameters.SchedulingOptionsPerPoolTree)
            .Item("options_per_job_shell").Value(parameters.OptionsPerJobShell)
            .DoIf(serializeHeavy, [&] (auto fluent) {
                SerializeHeavyRuntimeParameters(fluent, parameters);
            })
            .Item("erased_trees").Value(parameters.ErasedTrees)
            .Item("controller_agent_tag").Value(parameters.ControllerAgentTag)
        .EndMap();
}

void Serialize(const TOperationRuntimeParametersPtr& parameters, IYsonConsumer* consumer, bool serializeHeavy)
{
    if (!parameters) {
        consumer->OnEntity();
    } else {
        Serialize(*parameters, consumer, serializeHeavy);
    }
}

void Deserialize(TOperationRuntimeParameters& parameters, INodePtr node)
{
    auto mapNode = node->AsMap();
    if (auto owners = mapNode->FindChild("owners")) {
        Deserialize(parameters.Owners, owners);
    }
    if (auto acoName = mapNode->FindChild("aco_name")) {
        Deserialize(parameters.AcoName, acoName);
    }
    if (auto acl = mapNode->FindChild("acl")) {
        Deserialize(parameters.Acl, acl);
    }
    parameters.SchedulingOptionsPerPoolTree = ConvertTo<THashMap<TString, TOperationFairShareTreeRuntimeParametersPtr>>(
        mapNode->GetChildOrThrow("scheduling_options_per_pool_tree"));
    if (auto optionsPerJobShell = mapNode->FindChild("options_per_job_shell")) {
        parameters.OptionsPerJobShell = ConvertTo<THashMap<TString, TOperationJobShellRuntimeParametersPtr>>(optionsPerJobShell);
    }
    if (auto annotations = mapNode->FindChild("annotations")) {
        Deserialize(parameters.Annotations, annotations);
    }
    if (auto erasedTrees = mapNode->FindChild("erased_trees")) {
        Deserialize(parameters.ErasedTrees, erasedTrees);
    }
    if (auto controllerAgentTag = mapNode->FindChild("controller_agent_tag")) {
        Deserialize(parameters.ControllerAgentTag, controllerAgentTag);
    } else {
        parameters.ControllerAgentTag = "default";
    }

    if (parameters.AcoName) {
        if (!parameters.Acl.Entries.empty()) {
            THROW_ERROR_EXCEPTION(NScheduler::EErrorCode::CannotUseBothAclAndAco, "Cannot use both ACL and ACO name");
        }
        if (!parameters.Owners.empty()) {
            THROW_ERROR_EXCEPTION(NScheduler::EErrorCode::CannotUseBothAclAndAco, "Cannot use both ACL and ACO name");
        }
    }
    ValidateOperationAcl(parameters.Acl);
    ProcessAclAndOwnersParameters(&parameters.Acl, &parameters.Owners);
}

void Deserialize(TOperationRuntimeParameters& parameters, TYsonPullParserCursor* cursor)
{
    Deserialize(parameters, ExtractTo<INodePtr>(cursor));
}

////////////////////////////////////////////////////////////////////////////////

void TOperationFairShareTreeRuntimeParametersUpdate::Register(TRegistrar registrar)
{
    registrar.Parameter("weight", &TThis::Weight)
        .Optional()
        .InRange(MinSchedulableWeight, MaxSchedulableWeight);
    registrar.Parameter("pool", &TThis::Pool)
        .NonEmpty()
        .Optional();
    registrar.Parameter("resource_limits", &TThis::ResourceLimits)
        .Default();
    registrar.Parameter("enable_detailed_logs", &TThis::EnableDetailedLogs)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

void TOperationRuntimeParametersUpdate::Register(TRegistrar registrar)
{
    registrar.Parameter("pool", &TThis::Pool)
        .Optional();
    registrar.Parameter("weight", &TThis::Weight)
        .Optional()
        .InRange(MinSchedulableWeight, MaxSchedulableWeight);
    registrar.Parameter("acl", &TThis::Acl)
        .Optional();
    registrar.Parameter("aco_name", &TThis::AcoName)
        .Optional();
    registrar.Parameter("scheduling_options_per_pool_tree", &TThis::SchedulingOptionsPerPoolTree)
        .Default();
    registrar.Parameter("options_per_job_shell", &TThis::OptionsPerJobShell)
        .Default();
    registrar.Parameter("annotations", &TThis::Annotations)
        .Optional();
    registrar.Parameter("controller_agent_tag", &TThis::ControllerAgentTag)
        .Optional();

    registrar.Postprocessor([] (TOperationRuntimeParametersUpdate* update) {
        if (update->Acl) {
            if (update->AcoName) {
                THROW_ERROR_EXCEPTION(NScheduler::EErrorCode::CannotUseBothAclAndAco, "Cannot use both ACL and ACO name");
            }
            ValidateOperationAcl(*update->Acl);
        }
    });
}

bool TOperationRuntimeParametersUpdate::ContainsPool() const
{
    auto result = Pool.has_value();
    for (const auto& [_, treeOptions] : SchedulingOptionsPerPoolTree) {
        result |= treeOptions->Pool.has_value();
    }
    return result;
}

EPermissionSet TOperationRuntimeParametersUpdate::GetRequiredPermissions() const
{
    auto requiredPermissions = EPermissionSet(EPermission::Manage);

    for (const auto& [poolTree, treeParams] : SchedulingOptionsPerPoolTree) {
        if (treeParams->EnableDetailedLogs.has_value()) {
            requiredPermissions |= EPermission::Administer;
            break;
        }
    }

    return requiredPermissions;
}

////////////////////////////////////////////////////////////////////////////////

TOperationFairShareTreeRuntimeParametersPtr UpdateFairShareTreeRuntimeParameters(
    const TOperationFairShareTreeRuntimeParametersPtr& origin,
    const TOperationFairShareTreeRuntimeParametersUpdatePtr& update)
{
    try {
        if (!origin) {
            return NYTree::ConvertTo<TOperationFairShareTreeRuntimeParametersPtr>(ConvertToNode(update));
        }
        return UpdateYsonStruct(origin, ConvertToNode(update));
    } catch (const std::exception& exception) {
        THROW_ERROR_EXCEPTION("Error updating operation fair share tree runtime parameters")
            << exception;
    }
}

void TSchedulerConnectionConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("rpc_timeout", &TThis::RpcTimeout)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("rpc_acknowledgement_timeout", &TThis::RpcAcknowledgementTimeout)
        .Default(TDuration::Seconds(15));
}

////////////////////////////////////////////////////////////////////////////////

void TJobCpuMonitorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_cpu_reclaim", &TThis::EnableCpuReclaim)
        .Default(false);

    registrar.Parameter("check_period", &TThis::CheckPeriod)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("start_delay", &TThis::StartDelay)
        .Default(TDuration::Zero());

    registrar.Parameter("smoothing_factor", &TThis::SmoothingFactor)
        .InRange(0, 1)
        .Default(0.1);

    registrar.Parameter("relative_upper_bound", &TThis::RelativeUpperBound)
        .InRange(0, 1)
        .Default(0.9);

    registrar.Parameter("relative_lower_bound", &TThis::RelativeLowerBound)
        .InRange(0, 1)
        .Default(0.6);

    registrar.Parameter("increase_coefficient", &TThis::IncreaseCoefficient)
        .InRange(1, 2)
        .Default(1.45);

    registrar.Parameter("decrease_coefficient", &TThis::DecreaseCoefficient)
        .InRange(0, 1)
        .Default(0.97);

    registrar.Parameter("vote_window_size", &TThis::VoteWindowSize)
        .GreaterThan(0)
        .Default(5);

    registrar.Parameter("vote_decision_threshold", &TThis::VoteDecisionThreshold)
        .GreaterThan(0)
        .Default(3);

    registrar.Parameter("min_cpu_limit", &TThis::MinCpuLimit)
        .InRange(0, 1)
        .Default(1);
}

////////////////////////////////////////////////////////////////////////////////

void TOffloadingPoolSettingsConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("pool", &TThis::Pool)
        .Default();

    registrar.Parameter("weight", &TThis::Weight)
        .Optional()
        .InRange(MinSchedulableWeight, MaxSchedulableWeight);

    registrar.Parameter("tentative", &TThis::Tentative)
        .Default(false);

    registrar.Parameter("resource_limits", &TThis::ResourceLimits)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

PHOENIX_DEFINE_OPAQUE_TYPE(TEraseOperationSpec);
PHOENIX_DEFINE_OPAQUE_TYPE(TMapOperationSpec);
PHOENIX_DEFINE_OPAQUE_TYPE(TMapReduceOperationSpec);
PHOENIX_DEFINE_OPAQUE_TYPE(TMergeOperationSpec);
PHOENIX_DEFINE_OPAQUE_TYPE(TSortedOperationSpec);
PHOENIX_DEFINE_OPAQUE_TYPE(TReduceOperationSpec);
PHOENIX_DEFINE_OPAQUE_TYPE(TOperationSpecBase);
PHOENIX_DEFINE_OPAQUE_TYPE(TOrderedMergeOperationSpec);
PHOENIX_DEFINE_OPAQUE_TYPE(TRemoteCopyOperationSpec);
PHOENIX_DEFINE_OPAQUE_TYPE(TSimpleOperationSpecBase);
PHOENIX_DEFINE_OPAQUE_TYPE(TSortedMergeOperationSpec);
PHOENIX_DEFINE_OPAQUE_TYPE(TSortOperationSpec);
PHOENIX_DEFINE_OPAQUE_TYPE(TSortOperationSpecBase);
PHOENIX_DEFINE_OPAQUE_TYPE(TStrategyOperationSpec);
PHOENIX_DEFINE_OPAQUE_TYPE(TUnorderedMergeOperationSpec);
PHOENIX_DEFINE_OPAQUE_TYPE(TUnorderedOperationSpecBase);
PHOENIX_DEFINE_OPAQUE_TYPE(TVanillaOperationSpec);
PHOENIX_DEFINE_OPAQUE_TYPE(TUserJobSpec);
PHOENIX_DEFINE_OPAQUE_TYPE(TMandatoryUserJobSpec);
PHOENIX_DEFINE_OPAQUE_TYPE(TOptionalUserJobSpec);
PHOENIX_DEFINE_OPAQUE_TYPE(TVanillaTaskSpec);

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void ToProto(
    NControllerAgent::NProto::TQueryFilterOptions* protoQueryFilterOptions,
    const TQueryFilterOptionsPtr& queryFilterOptions)
{
    protoQueryFilterOptions->set_enable_chunk_filter(queryFilterOptions->EnableChunkFilter);
    protoQueryFilterOptions->set_enable_row_filter(queryFilterOptions->EnableRowFilter);
}

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
