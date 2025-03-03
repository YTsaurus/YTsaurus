#include "config.h"

#include <yt/yt/server/lib/chunk_pools/config.h>

#include <yt/yt/server/lib/job_agent/config.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/ytlib/chunk_client/config.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/event_log/config.h>

#include <yt/yt/ytlib/node_tracker_client/config.h>

#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>

#include <yt/yt/core/concurrency/config.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/library/re2/re2.h>

#include <yt/yt/library/program/config.h>

namespace NYT::NControllerAgent {

using namespace NStatisticPath;

////////////////////////////////////////////////////////////////////////////////

void TIntermediateChunkScraperConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("restart_timeout", &TThis::RestartTimeout)
        .Default(TDuration::Seconds(10));
}

////////////////////////////////////////////////////////////////////////////////

void TTestingOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_snapshot_cycle_after_materialization", &TThis::EnableSnapshotCycleAfterMaterialization)
        .Default(false);

    registrar.Parameter("rootfs_test_layers", &TThis::RootfsTestLayers)
        .Default();

    registrar.Parameter("delay_in_unregistration", &TThis::DelayInUnregistration)
        .Default();

    registrar.Parameter("delay_in_handshake", &TThis::DelayInHandshake)
        .Default();

    registrar.Parameter(
        "abort_output_transaction_after_completion_transaction_commit",
        &TThis::AbortOutputTransactionAfterCompletionTransactionCommit)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TLowGpuPowerUsageOnWindowConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("window_size", &TThis::WindowSize)
        .Default(TDuration::Minutes(60));

    registrar.Parameter("record_period", &TThis::RecordPeriod)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("threshold", &TThis::Threshold)
        .Default(100.0);
}

////////////////////////////////////////////////////////////////////////////////

void TAlertManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("period", &TThis::Period)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("tmpfs_alert_max_unused_space_ratio", &TThis::TmpfsAlertMaxUnusedSpaceRatio)
        .InRange(0.0, 1.0)
        .Default(0.2);

    registrar.Parameter("tmpfs_alert_min_unused_space_threshold", &TThis::TmpfsAlertMinUnusedSpaceThreshold)
        .Default(512_MB)
        .GreaterThan(0);

    registrar.Parameter("tmpfs_alert_memory_usage_mute_ratio", &TThis::TmpfsAlertMemoryUsageMuteRatio)
        .InRange(0.0, 1.0)
        .Default(0.8);

    registrar.Parameter("memory_usage_alert_max_unused_size", &TThis::MemoryUsageAlertMaxUnusedSize)
        .Default(8_GB)
        .GreaterThan(0);

    registrar.Parameter("memory_usage_alert_max_unused_ratio", &TThis::MemoryUsageAlertMaxUnusedRatio)
        .InRange(0.0, 1.0)
        .Default(0.2);

    registrar.Parameter("memory_usage_alert_max_job_count", &TThis::MemoryUsageAlertMaxJobCount)
        .Default()
        .GreaterThan(0);

    registrar.Parameter("memory_reserve_factor_alert_max_unused_ratio", &TThis::MemoryReserveFactorAlertMaxUnusedRatio)
        .InRange(0.0, 1.0)
        .Default(0.8);

    registrar.Parameter("aborted_jobs_alert_max_aborted_time", &TThis::AbortedJobsAlertMaxAbortedTime)
        .Default((i64) 10 * 60 * 1'000)
        .GreaterThan(0);

    registrar.Parameter("aborted_jobs_alert_max_aborted_time_ratio", &TThis::AbortedJobsAlertMaxAbortedTimeRatio)
        .InRange(0.0, 1.0)
        .Default(0.25);

    registrar.Parameter("short_jobs_alert_min_job_duration", &TThis::ShortJobsAlertMinJobDuration)
        .Default(TDuration::Minutes(1));

    registrar.Parameter("short_jobs_alert_min_job_count", &TThis::ShortJobsAlertMinJobCount)
        .Default(1'000);

    registrar.Parameter("short_jobs_alert_min_allowed_operation_duration_to_max_job_duration_ratio", &TThis::ShortJobsAlertMinAllowedOperationDurationToMaxJobDurationRatio)
        .Default(2.0);

    registrar.Parameter("intermediate_data_skew_alert_min_partition_size", &TThis::IntermediateDataSkewAlertMinPartitionSize)
        .Default(10_GB)
        .GreaterThan(0);

    registrar.Parameter("intermediate_data_skew_alert_min_interquartile_range", &TThis::IntermediateDataSkewAlertMinInterquartileRange)
        .Default(1_GB)
        .GreaterThan(0);

    registrar.Parameter("job_spec_throttling_alert_activation_count_threshold", &TThis::JobSpecThrottlingAlertActivationCountThreshold)
        .Default(1'000)
        .GreaterThan(0);

    registrar.Parameter("low_cpu_usage_alert_min_execution_time", &TThis::LowCpuUsageAlertMinExecTime)
        .Default(TDuration::Minutes(10));

    registrar.Parameter("low_cpu_usage_alert_min_average_job_time", &TThis::LowCpuUsageAlertMinAverageJobTime)
        .Default(TDuration::Minutes(1));

    registrar.Parameter("low_cpu_usage_alert_cpu_usage_threshold", &TThis::LowCpuUsageAlertCpuUsageThreshold)
        .Default(0.5)
        .GreaterThan(0);

    registrar.Parameter("low_cpu_usage_alert_statistics", &TThis::LowCpuUsageAlertStatistics)
        .Default({
            "/job_proxy/cpu/system"_SP,
            "/job_proxy/cpu/user"_SP,
            "/user_job/cpu/system"_SP,
            "/user_job/cpu/user"_SP,
        });

    registrar.Parameter("low_cpu_usage_alert_job_states", &TThis::LowCpuUsageAlertJobStates)
        .Default({
            EJobState::Completed
        });

    registrar.Parameter("high_cpu_wait_alert_min_average_job_time", &TThis::HighCpuWaitAlertMinAverageJobTime)
        .Default(TDuration::Minutes(10));

    registrar.Parameter("high_cpu_wait_alert_threshold", &TThis::HighCpuWaitAlertThreshold)
        .Default(0.5)
        .GreaterThan(0);

    registrar.Parameter("high_cpu_wait_alert_statistics", &TThis::HighCpuWaitAlertStatistics)
        .Default({
            "/user_job/cpu/wait"_SP,
        });

    registrar.Parameter("high_cpu_wait_alert_job_states", &TThis::HighCpuWaitAlertJobStates)
        .Default({
            EJobState::Completed,
            EJobState::Running
        });


    registrar.Parameter("operation_too_long_alert_min_wall_time", &TThis::OperationTooLongAlertMinWallTime)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("operation_too_long_alert_estimate_duration_threshold", &TThis::OperationTooLongAlertEstimateDurationThreshold)
        .Default(TDuration::Days(7));

    registrar.Parameter("low_gpu_usage_alert_min_total_gpu_duration", &TThis::LowGpuUsageAlertMinTotalGpuDuration)
        .Default(TDuration::Hours(100));

    registrar.Parameter("low_gpu_usage_alert_min_duration", &TThis::LowGpuUsageAlertMinDuration)
        .Default(TDuration::Minutes(30));

    registrar.Parameter("low_gpu_usage_alert_gpu_usage_threshold", &TThis::LowGpuUsageAlertGpuUsageThreshold)
        .Default(0.5)
        .InRange(0.0, 1.0);

    registrar.Parameter("low_gpu_usage_alert_gpu_utilization_power_threshold", &TThis::LowGpuUsageAlertGpuUtilizationPowerThreshold)
        .Default(0.3)
        .InRange(0.0, 1.0);

    registrar.Parameter("low_gpu_usage_alert_gpu_utilization_sm_threshold", &TThis::LowGpuUsageAlertGpuUtilizationSMThreshold)
        .Default(0.3)
        .InRange(0.0, 1.0);

    registrar.Parameter("low_gpu_usage_alert_statistics", &TThis::LowGpuUsageAlertStatistics)
        .Default({
            "/user_job/gpu/cumulative_utilization_gpu"_SP,
        });

    registrar.Parameter("low_gpu_usage_alert_job_states", &TThis::LowGpuUsageAlertJobStates)
        .Default({
            EJobState::Completed,
            EJobState::Running,
        });

    registrar.Parameter("low_gpu_power_usage_on_window", &TThis::LowGpuPowerUsageOnWindow)
        .DefaultNew();

    registrar.Parameter("queue_total_time_estimate_threshold", &TThis::QueueTotalTimeEstimateThreshold)
        .Alias("queue_average_wait_time_threshold")
        .Default(TDuration::Minutes(1));

    registrar.Parameter("task_unavailable_network_bandwidth_time_ratio_alert_threshold", &TThis::TaskUnavailableNetworkBandwidthTimeRatioAlertThreshold)
        .Default(0.3)
        .InRange(0.0, 1.0);
}

////////////////////////////////////////////////////////////////////////////////

void TJobSplitterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("min_job_time", &TThis::MinJobTime)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("exec_to_prepare_time_ratio", &TThis::ExecToPrepareTimeRatio)
        .Default(20.0);

    registrar.Parameter("no_progress_job_time_to_average_prepare_time_ratio", &TThis::NoProgressJobTimeToAveragePrepareTimeRatio)
        .Default(20.0);

    registrar.Parameter("min_total_data_weight", &TThis::MinTotalDataWeight)
        .Alias("min_total_data_size")
        .Default(1_GB);

    registrar.Parameter("update_period", &TThis::UpdatePeriod)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("candidate_percentile", &TThis::CandidatePercentile)
        .GreaterThanOrEqual(0.5)
        .LessThanOrEqual(1.0)
        .Default(0.8);

    registrar.Parameter("late_jobs_percentile", &TThis::LateJobsPercentile)
        .GreaterThanOrEqual(0.5)
        .LessThanOrEqual(1.0)
        .Default(0.95);

    registrar.Parameter("residual_job_factor", &TThis::ResidualJobFactor)
        .GreaterThan(0)
        .LessThanOrEqual(1.0)
        .Default(0.8);

    registrar.Parameter("residual_job_count_min_threshold", &TThis::ResidualJobCountMinThreshold)
        .GreaterThan(0)
        .Default(10);

    registrar.Parameter("max_jobs_per_split", &TThis::MaxJobsPerSplit)
        .GreaterThan(0)
        .Default(5);

    registrar.Parameter("max_input_table_count", &TThis::MaxInputTableCount)
        .GreaterThan(0)
        .Default(100);

    registrar.Parameter("split_timeout_before_speculate", &TThis::SplitTimeoutBeforeSpeculate)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("job_logging_period", &TThis::JobLoggingPeriod)
        .Default(TDuration::Minutes(3));

    registrar.Parameter("enable_job_splitting", &TThis::EnableJobSplitting)
        .Default(true);

    registrar.Parameter("enable_job_speculation", &TThis::EnableJobSpeculation)
        .Default(true);

    registrar.Parameter("show_running_jobs_in_progress", &TThis::ShowRunningJobsInProgress)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TSuspiciousJobsOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("inactivity_timeout", &TThis::InactivityTimeout)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("cpu_usage_threshold", &TThis::CpuUsageThreshold)
        .Default(300);
    registrar.Parameter("input_pipe_time_idle_fraction", &TThis::InputPipeIdleTimeFraction)
        .Default(0.95);
    registrar.Parameter("output_pipe_time_idle_fraction", &TThis::OutputPipeIdleTimeFraction)
        .Default(0.95);
    registrar.Parameter("update_period", &TThis::UpdatePeriod)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("max_orchid_entry_count_per_type", &TThis::MaxOrchidEntryCountPerType)
        .Default(100);
}

////////////////////////////////////////////////////////////////////////////////

void TDataBalancerOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("logging_min_consecutive_violation_count", &TThis::LoggingMinConsecutiveViolationCount)
        .Default(1'000);
    registrar.Parameter("logging_period", &TThis::LoggingPeriod)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("tolerance", &TThis::Tolerance)
        .Default(2.0);
    registrar.Parameter("use_node_io_weight", &TThis::UseNodeIOWeight)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

void TUserJobOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("thread_limit_multiplier", &TThis::ThreadLimitMultiplier)
        .Default(10'000);
    registrar.Parameter("initial_thread_limit", &TThis::InitialThreadLimit)
        .Default(10'000);
}

////////////////////////////////////////////////////////////////////////////////

void TOperationOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("spec_template", &TThis::SpecTemplate)
        .Default();

    registrar.Parameter("slice_data_weight_multiplier", &TThis::SliceDataWeightMultiplier)
        .Alias("slice_data_size_multiplier")
        .Default(0.51)
        .GreaterThan(0.0);

    registrar.Parameter("max_data_slices_per_job", &TThis::MaxDataSlicesPerJob)
        // This is a reasonable default for jobs with user code.
        // Defaults for system jobs are in Initializer.
        .Default(1'000)
        .GreaterThan(0);
    registrar.Parameter("max_data_slices_per_job_limit", &TThis::MaxDataSlicesPerJobLimit)
        .Default(10'000)
        .GreaterThan(0);

    registrar.Parameter("max_slice_data_weight", &TThis::MaxSliceDataWeight)
        .Alias("max_slice_data_size")
        .Default(1_GB)
        .GreaterThan(0);

    registrar.Parameter("min_slice_data_weight", &TThis::MinSliceDataWeight)
        .Alias("min_slice_data_size")
        .Default(1_MB)
        .GreaterThan(0);

    registrar.Parameter("max_input_table_count", &TThis::MaxInputTableCount)
        .Default(3'000)
        .GreaterThan(0);

    registrar.Parameter("max_output_tables_times_jobs_count", &TThis::MaxOutputTablesTimesJobsCount)
        .Default(20 * 100'000)
        .GreaterThanOrEqual(100'000);

    registrar.Parameter("job_splitter", &TThis::JobSplitter)
        .DefaultNew();

    registrar.Parameter("max_build_retry_count", &TThis::MaxBuildRetryCount)
        .Default(5)
        .GreaterThanOrEqual(0);

    registrar.Parameter("data_weight_per_job_retry_factor", &TThis::DataWeightPerJobRetryFactor)
        .Default(2.0)
        .GreaterThan(1.0);

    registrar.Parameter("initial_cpu_limit_overcommit", &TThis::InitialCpuLimitOvercommit)
        .Default(2.0)
        .GreaterThanOrEqual(0);

    registrar.Parameter("cpu_limit_overcommit_multiplier", &TThis::CpuLimitOvercommitMultiplier)
        .Default(1.0)
        .GreaterThanOrEqual(1.0);

    registrar.Parameter("set_container_cpu_limit", &TThis::SetContainerCpuLimit)
        .Default(false);

    registrar.Parameter("set_slot_container_memory_limit", &TThis::SetSlotContainerMemoryLimit)
        .Default(false);

    registrar.Parameter("slot_container_memory_overhead", &TThis::SlotContainerMemoryOverhead)
        .Default(0)
        .GreaterThanOrEqual(0);

    // NB: Defaults for these values are actually in preprocessor of TControllerAgentConfig::OperationOptions.
    registrar.Parameter("controller_building_job_spec_count_limit", &TThis::ControllerBuildingJobSpecCountLimit)
        .Default();
    registrar.Parameter("controller_total_building_job_spec_slice_count_limit", &TThis::ControllerTotalBuildingJobSpecSliceCountLimit)
        .Default();

    registrar.Parameter("custom_statistics_count_limit", &TThis::CustomStatisticsCountLimit)
        .Default(1024);

    registrar.Parameter("user_job_options", &TThis::UserJobOptions)
        .DefaultNew();

    registrar.Postprocessor([&] (TOperationOptions* options) {
        if (options->MaxSliceDataWeight < options->MinSliceDataWeight) {
            THROW_ERROR_EXCEPTION("Minimum slice data weight must be less than or equal to maximum slice data size")
                << TErrorAttribute("min_slice_data_weight", options->MinSliceDataWeight)
                << TErrorAttribute("max_slice_data_weight", options->MaxSliceDataWeight);
        }

        if (options->MaxDataSlicesPerJobLimit < options->MaxDataSlicesPerJob) {
            THROW_ERROR_EXCEPTION("Default maximum count of data slices per job must be less than or equal to the limit of the maximum count of data slices per job")
                << TErrorAttribute("max_data_slices_per_job", options->MaxDataSlicesPerJob)
                << TErrorAttribute("max_data_slices_per_job_limit", options->MaxDataSlicesPerJobLimit);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TSimpleOperationOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("max_job_count", &TThis::MaxJobCount)
        .Default(100'000);

    registrar.Parameter("data_weight_per_job", &TThis::DataWeightPerJob)
        .Alias("data_size_per_job")
        .Default(256_MB)
        .GreaterThan(0);
}

////////////////////////////////////////////////////////////////////////////////

void TMapOperationOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("job_size_adjuster", &TThis::JobSizeAdjuster)
        .DefaultNew();

    registrar.Preprocessor([&] (TMapOperationOptions* options) {
        options->DataWeightPerJob = 128_MB;
    });
}

////////////////////////////////////////////////////////////////////////////////

void TUnorderedMergeOperationOptions::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TOrderedMergeOperationOptions::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TSortedMergeOperationOptions::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TReduceOperationOptions::Register(TRegistrar registrar)
{
    registrar.Preprocessor([&] (TReduceOperationOptions* options) {
        options->DataWeightPerJob = 128_MB;
    });
}

////////////////////////////////////////////////////////////////////////////////

void TEraseOperationOptions::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TSortOperationOptionsBase::Register(TRegistrar registrar)
{
    registrar.Parameter("max_partition_job_count", &TThis::MaxPartitionJobCount)
        .Default(500'000)
        .GreaterThan(0);

    registrar.Parameter("max_partition_count", &TThis::MaxPartitionCount)
        .Default(200'000)
        .GreaterThan(0);

    registrar.Parameter("max_new_partition_count", &TThis::MaxNewPartitionCount)
        .Default(2'000'000)
        .GreaterThan(0);

    registrar.Parameter("max_partition_factor", &TThis::MaxPartitionFactor)
        .Default(500)
        .GreaterThan(1);

    registrar.Parameter("max_sample_size", &TThis::MaxSampleSize)
        .Default(10_KB)
        .GreaterThanOrEqual(1_KB)
            // NB(psushin): removing this validator may lead to weird errors in sorting.
        .LessThanOrEqual(NTableClient::MaxSampleSize);

    registrar.Parameter("compressed_block_size", &TThis::CompressedBlockSize)
        .Default(1_MB)
        .GreaterThanOrEqual(1_KB);

    registrar.Parameter("min_partition_weight", &TThis::MinPartitionWeight)
        .Alias("min_partition_size")
        .Default(256_MB)
        .GreaterThanOrEqual(1);

    // Minimum is 1 for tests.
    registrar.Parameter("min_uncompressed_block_size", &TThis::MinUncompressedBlockSize)
        .Default(100_KB)
        .GreaterThanOrEqual(1);

    registrar.Parameter("max_value_count_per_simple_sort_job", &TThis::MaxValueCountPerSimpleSortJob)
        .Default(10'000'000)
        .GreaterThanOrEqual(1);

    registrar.Parameter("partition_job_size_adjuster", &TThis::PartitionJobSizeAdjuster)
        .DefaultNew();

    registrar.Parameter("data_balancer", &TThis::DataBalancer)
        .DefaultNew();

    registrar.Parameter("critical_new_partition_difference_ratio", &TThis::CriticalNewPartitionDifferenceRatio)
        .Default(10.0)
        .GreaterThan(0.0);
}

////////////////////////////////////////////////////////////////////////////////

void TSortOperationOptions::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TMapReduceOperationOptions::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TRemoteCopyOperationOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("cpu_limit", &TThis::CpuLimit)
        .Default(NScheduler::TCpuResource(0.1));
    registrar.Parameter("networks", &TThis::Networks)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TGangManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enabled", &TThis::Enabled)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

void TVanillaOperationOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("max_task_count", &TThis::MaxTaskCount)
        .Default(100);
    registrar.Parameter("max_total_job_count", &TThis::MaxTotalJobCount)
        .Default(100 * 1000);
    registrar.Parameter("gang_manager", &TThis::GangManager)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TZombieOperationOrchidsConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("limit", &TThis::Limit)
        .Default(10'000)
        .GreaterThanOrEqual(0);

    registrar.Parameter("clean_period", &TThis::CleanPeriod)
        .Default(TDuration::Minutes(1));

    registrar.Parameter("enable", &TThis::Enable)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

void TUserJobMonitoringConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("default_max_monitored_user_jobs_per_operation", &TThis::DefaultMaxMonitoredUserJobsPerOperation)
        .Default(5)
        .GreaterThanOrEqual(0);

    registrar.Parameter("extended_max_monitored_user_jobs_per_operation", &TThis::ExtendedMaxMonitoredUserJobsPerOperation)
        .Alias("max_monitored_user_jobs_per_operation")
        .Default(200)
        .GreaterThanOrEqual(0);

    registrar.Parameter("enable_extended_max_monitored_user_jobs_per_operation", &TThis::EnableExtendedMaxMonitoredUserJobsPerOperation)
        .Default({
            {EOperationType::Vanilla, true},
        })
        .ResetOnLoad();

    registrar.Parameter("max_monitored_user_jobs_per_agent", &TThis::MaxMonitoredUserJobsPerAgent)
        .Default(1'000)
        .GreaterThanOrEqual(0);
}

////////////////////////////////////////////////////////////////////////////////

void TMemoryWatchdogConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("total_controller_memory_limit", &TThis::TotalControllerMemoryLimit)
        .Default();

    registrar.Parameter("operation_controller_memory_limit", &TThis::OperationControllerMemoryLimit)
        .Default(50_GB);
    registrar.Parameter("operation_controller_memory_overconsumption_threshold", &TThis::OperationControllerMemoryOverconsumptionThreshold)
        .Default(30_GB);

    registrar.Parameter("memory_usage_check_period", &TThis::MemoryUsageCheckPeriod)
        .Default(TDuration::Seconds(5));
}

////////////////////////////////////////////////////////////////////////////////

void TUserFileLimitsConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_size", &TThis::MaxSize)
        .Default(10_GB);
    registrar.Parameter("max_table_data_weight", &TThis::MaxTableDataWeight)
        .Default(10_GB);
    registrar.Parameter("max_chunk_count", &TThis::MaxChunkCount)
        .Default(1'000);
}

////////////////////////////////////////////////////////////////////////////////

void TUserFileLimitsPatchConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_size", &TThis::MaxSize)
        .Default();
    registrar.Parameter("max_table_data_weight", &TThis::MaxTableDataWeight)
        .Default();
    registrar.Parameter("max_chunk_count", &TThis::MaxChunkCount)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TJobTrackerTestingOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("delay_in_settle_job", &TThis::DelayInSettleJob)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TJobTrackerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("node_disconnection_timeout", &TThis::NodeDisconnectionTimeout)
        .Default(TDuration::Seconds(120))
        .GreaterThan(TDuration::Zero());
    registrar.Parameter("job_confirmation_timeout", &TThis::JobConfirmationTimeout)
        .Default(TDuration::Seconds(240))
        .GreaterThan(TDuration::Zero());
    registrar.Parameter("logging_job_sample_size", &TThis::LoggingJobSampleSize)
        .Default(3)
        .GreaterThanOrEqual(0);
    registrar.Parameter(
        "duration_before_job_considered_disappeared_from_node",
        &TThis::DurationBeforeJobConsideredDisappearedFromNode)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("check_node_heartbeat_sequence_number", &TThis::CheckNodeHeartbeatSequenceNumber)
        .Default(false);
    registrar.Parameter("heavy_invoker_thread_count", &TThis::HeavyInvokerThreadCount)
        .Default(4)
        .GreaterThan(0);
    registrar.Parameter("testing_options", &TThis::TestingOptions)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TDockerRegistryConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("internal_registry_address", &TThis::InternalRegistryAddress)
        .Default();
    registrar.Parameter("use_yt_token_for_internal_registry", &TThis::UseYtTokenForInternalRegistry)
        .Default(false);
    registrar.Parameter("forward_internal_images_to_job_specs", &TThis::ForwardInternalImagesToJobSpecs)
        .Default(false);
    registrar.Parameter("translate_internal_images_into_layers", &TThis::TranslateInternalImagesIntoLayers)
        .Default(true);
    registrar.Postprocessor([&] (TDockerRegistryConfig* options) {
        if (!options->TranslateInternalImagesIntoLayers && !options->ForwardInternalImagesToJobSpecs) {
            THROW_ERROR_EXCEPTION("At least one of forward_internal_images_to_job_specs or translate_internal_images_into_layers must be enabled");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TDisallowRemoteOperationsConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("allowed_users", &TThis::AllowedUsers)
        .Default();
    registrar.Parameter("allowed_clusters", &TThis::AllowedClusters)
        .Default();
    registrar.Parameter("allowed_for_everyone_clusters", &TThis::AllowedForEveryoneClusters)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TControllerAgentConfig::Register(TRegistrar registrar)
{
    registrar.UnrecognizedStrategy(NYTree::EUnrecognizedStrategy::KeepRecursive);

    registrar.Parameter("chunk_list_preallocation_count", &TThis::ChunkListPreallocationCount)
        .Default(128)
        .GreaterThanOrEqual(0);
    registrar.Parameter("max_chunk_list_allocation_count", &TThis::MaxChunkListAllocationCount)
        .Default(16 << 10)
        .GreaterThanOrEqual(0);
    registrar.Parameter("chunk_list_watermark_count", &TThis::ChunkListWatermarkCount)
        .Default(50)
        .GreaterThanOrEqual(0);
    registrar.Parameter("chunk_list_allocation_multiplier", &TThis::ChunkListAllocationMultiplier)
        .Default(2.0)
        .GreaterThan(1.0);
    registrar.Parameter("desired_chunk_lists_per_release", &TThis::DesiredChunkListsPerRelease)
        .Default(10'000);
    registrar.Parameter("intermediate_output_master_cell_count", &TThis::IntermediateOutputMasterCellCount)
        .GreaterThanOrEqual(1)
        .Default(4);

    registrar.Parameter("enable_snapshot_building", &TThis::EnableSnapshotBuilding)
        .Default(true);
    registrar.Parameter("enable_snapshot_building_disabled_alert", &TThis::EnableSnapshotBuildingDisabledAlert)
        .Default(true);
    registrar.Parameter("snapshot_period", &TThis::SnapshotPeriod)
        .Default(TDuration::Seconds(300));
    registrar.Parameter("snapshot_timeout", &TThis::SnapshotTimeout)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("snapshot_fork_timeout", &TThis::SnapshotForkTimeout)
        .Default(TDuration::Minutes(2));
    registrar.Parameter("operation_controller_suspend_timeout", &TThis::OperationControllerSuspendTimeout)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("parallel_snapshot_builder_count", &TThis::ParallelSnapshotBuilderCount)
        .Default(4)
        .GreaterThan(0);
    registrar.Parameter("snapshot_writer", &TThis::SnapshotWriter)
        .DefaultNew();

    registrar.Parameter("enable_snapshot_loading", &TThis::EnableSnapshotLoading)
        .Default(true);
    registrar.Parameter("enable_snapshot_loading_disabled_alert", &TThis::EnableSnapshotLoadingDisabledAlert)
        .Default(true);
    registrar.Parameter("snapshot_reader", &TThis::SnapshotReader)
        .DefaultNew();

    registrar.Parameter("transactions_refresh_period", &TThis::TransactionsRefreshPeriod)
        .Default(TDuration::Seconds(3));
    registrar.Parameter("operations_update_period", &TThis::OperationsUpdatePeriod)
        .Default(TDuration::Seconds(3));
    registrar.Parameter("intermediate_account_usage_update_period", &TThis::IntermediateMediumUsageUpdatePeriod)
        .Default(TDuration::Seconds(3));
    registrar.Parameter("chunk_unstage_period", &TThis::ChunkUnstagePeriod)
        .Default(TDuration::MilliSeconds(100));

    registrar.Parameter("enable_unrecognized_alert", &TThis::EnableUnrecognizedAlert)
        .Default(true);

    registrar.Parameter("max_children_per_attach_request", &TThis::MaxChildrenPerAttachRequest)
        .Default(10'000)
        .GreaterThan(0);

    registrar.Parameter("chunk_location_throttler", &TThis::ChunkLocationThrottler)
        .DefaultCtor([] {
            // NB(coteeq): Controller will flood itself with
            // OnChunkLocated calls if throttler is unlimited.
            auto config = New<NConcurrency::TThroughputThrottlerConfig>();
            config->Limit = 5'000;
            return config;
        });

    registrar.Parameter("event_log", &TThis::EventLog)
        .DefaultCtor([] {
            auto config = New<NEventLog::TEventLogManagerConfig>();
            config->Enable = false;
            config->MaxRowWeight = 128_MB;
            config->Path = "//sys/scheduler/event_log";
            return config;
        });

    registrar.Parameter("scheduler_handshake_rpc_timeout", &TThis::SchedulerHandshakeRpcTimeout)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("scheduler_handshake_failure_backoff", &TThis::SchedulerHandshakeFailureBackoff)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("scheduler_heartbeat_rpc_timeout", &TThis::SchedulerHeartbeatRpcTimeout)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("scheduler_heartbeat_failure_backoff", &TThis::SchedulerHeartbeatFailureBackoff)
        .Default(TDuration::MilliSeconds(100));

    registrar.Parameter("scheduler_heartbeat_period", &TThis::SchedulerHeartbeatPeriod)
        .Default(TDuration::MilliSeconds(100));

    registrar.Parameter("schedule_allocation_heartbeat_period", &TThis::ScheduleAllocationHeartbeatPeriod)
        .Alias("schedule_job_heartbeat_period")
        .Default(TDuration::MilliSeconds(10));

    registrar.Parameter("config_update_period", &TThis::ConfigUpdatePeriod)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("exec_nodes_update_period", &TThis::ExecNodesUpdatePeriod)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("operations_push_period", &TThis::OperationsPushPeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("operation_job_metrics_push_period", &TThis::OperationJobMetricsPushPeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("operation_alerts_push_period", &TThis::OperationAlertsPushPeriod)
        .Default(TDuration::Seconds(3));
    registrar.Parameter("suspicious_jobs_push_period", &TThis::SuspiciousJobsPushPeriod)
        .Default(TDuration::Seconds(3));

    registrar.Parameter("controller_thread_count", &TThis::ControllerThreadCount)
        .Default(16)
        .GreaterThan(0);

    registrar.Parameter("chunk_scraper_thread_count", &TThis::ChunkScraperThreadCount)
        .Default(4)
        .GreaterThan(0);

    registrar.Parameter("job_spec_build_thread_count", &TThis::JobSpecBuildThreadCount)
        .Default(16)
        .GreaterThan(0);

    registrar.Parameter("statistics_offload_thread_count", &TThis::StatisticsOffloadThreadCount)
        .Default(16)
        .GreaterThan(0);

    registrar.Parameter("controller_static_orchid_update_period", &TThis::ControllerStaticOrchidUpdatePeriod)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("controller_orchid_keys_update_period", &TThis::ControllerOrchidKeysUpdatePeriod)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("max_concurrent_safe_core_dumps", &TThis::MaxConcurrentSafeCoreDumps)
        .Default(1)
        .GreaterThanOrEqual(0);

    registrar.Parameter("scheduling_tag_filter_expire_timeout", &TThis::SchedulingTagFilterExpireTimeout)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("running_job_statistics_update_period", &TThis::RunningJobStatisticsUpdatePeriod)
        .Default(TDuration::Minutes(1));

    registrar.Parameter("operation_time_limit", &TThis::OperationTimeLimit)
        .Default();
    registrar.Parameter("operation_time_limit_check_period", &TThis::OperationTimeLimitCheckPeriod)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("resource_demand_sanity_check_period", &TThis::ResourceDemandSanityCheckPeriod)
        .Default(TDuration::Seconds(15));

    registrar.Parameter("operation_initialization_timeout", &TThis::OperationInitializationTimeout)
        .Default(TDuration::Minutes(10));
    registrar.Parameter("operation_transaction_timeout", &TThis::OperationTransactionTimeout)
        .Default(TDuration::Minutes(300));
    registrar.Parameter("operation_transaction_ping_period", &TThis::OperationTransactionPingPeriod)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("operation_progress_log_backoff", &TThis::OperationLogProgressBackoff)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("task_update_period", &TThis::TaskUpdatePeriod)
        .Default(TDuration::Seconds(3));

    registrar.Parameter("available_exec_nodes_check_period", &TThis::AvailableExecNodesCheckPeriod)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("banned_exec_nodes_check_period", &TThis::BannedExecNodesCheckPeriod)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("operation_build_progress_period", &TThis::OperationBuildProgressPeriod)
        .Default(TDuration::Seconds(3));

    registrar.Parameter("check_tentative_tree_eligibility_period", &TThis::CheckTentativeTreeEligibilityPeriod)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("update_account_resource_usage_leases_period", &TThis::UpdateAccountResourceUsageLeasesPeriod)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("zombie_operation_orchids", &TThis::ZombieOperationOrchids)
        .DefaultNew();

    registrar.Parameter("max_retained_jobs_per_operation", &TThis::MaxRetainedJobsPerOperation)
        .Alias("max_job_nodes_per_operation")
        .Default(200)
        .GreaterThanOrEqual(0)
        .LessThanOrEqual(1'000);

    registrar.Parameter("max_archived_job_spec_count_per_operation", &TThis::MaxArchivedJobSpecCountPerOperation)
        .Default(500)
        .GreaterThanOrEqual(0);

    registrar.Parameter("guaranteed_archived_job_spec_count_per_operation", &TThis::GuaranteedArchivedJobSpecCountPerOperation)
        .Default(10)
        .GreaterThanOrEqual(0);

    registrar.Parameter("min_job_duration_to_archive_job_spec", &TThis::MinJobDurationToArchiveJobSpec)
        .Default(TDuration::Minutes(30));

    registrar.Parameter("max_chunks_per_fetch", &TThis::MaxChunksPerFetch)
        .Default(100'000)
        .GreaterThan(0);

    registrar.Parameter("user_file_limits", &TThis::UserFileLimits)
        .DefaultNew();

    registrar.Parameter("user_file_limits_per_tree", &TThis::UserFileLimitsPerTree)
        .Default();

    registrar.Parameter("max_user_file_count", &TThis::MaxUserFileCount)
        .Default(1'000)
        .GreaterThan(0);

    registrar.Parameter("max_input_table_count", &TThis::MaxInputTableCount)
        .Default(1'000)
        .GreaterThan(0);

    registrar.Parameter("max_output_table_count", &TThis::MaxOutputTableCount)
        .Default(1'000)
        .GreaterThan(0);

    registrar.Parameter("max_ranges_on_table", &TThis::MaxRangesOnTable)
        .Default(1'000)
        .GreaterThan(0);

    registrar.Parameter("safe_online_node_count", &TThis::SafeOnlineNodeCount)
        .GreaterThanOrEqual(0)
        .Default(1);

    registrar.Parameter("safe_scheduler_online_time", &TThis::SafeSchedulerOnlineTime)
        .Default(TDuration::Minutes(10));

    registrar.Parameter("controller_exec_node_info_update_period", &TThis::ControllerExecNodeInfoUpdatePeriod)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("max_chunks_per_locate_request", &TThis::MaxChunksPerLocateRequest)
        .GreaterThan(0)
        .Default(10'000);

    registrar.Parameter("operation_options", &TThis::OperationOptions)
        .Default(NYTree::GetEphemeralNodeFactory()->CreateMap());

    registrar.Parameter("map_operation_options", &TThis::MapOperationOptions)
        .DefaultNew();
    registrar.Parameter("reduce_operation_options", &TThis::ReduceOperationOptions)
        .DefaultNew();
    registrar.Parameter("join_reduce_operation_options", &TThis::JoinReduceOperationOptions)
        .DefaultNew();
    registrar.Parameter("erase_operation_options", &TThis::EraseOperationOptions)
        .DefaultNew();
    registrar.Parameter("ordered_merge_operation_options", &TThis::OrderedMergeOperationOptions)
        .DefaultNew();
    registrar.Parameter("unordered_merge_operation_options", &TThis::UnorderedMergeOperationOptions)
        .DefaultNew();
    registrar.Parameter("sorted_merge_operation_options", &TThis::SortedMergeOperationOptions)
        .DefaultNew();
    registrar.Parameter("map_reduce_operation_options", &TThis::MapReduceOperationOptions)
        .DefaultNew();
    registrar.Parameter("sort_operation_options", &TThis::SortOperationOptions)
        .DefaultNew();
    registrar.Parameter("remote_copy_operation_options", &TThis::RemoteCopyOperationOptions)
        .DefaultNew();
    registrar.Parameter("vanilla_operation_options", &TThis::VanillaOperationOptions)
        .DefaultNew();

    registrar.Parameter("environment", &TThis::Environment)
        .Default({
            {"HOME", "$(SandboxPath)"},
            {"TMPDIR", "$(SandboxPath)"},
        });

    registrar.Parameter("enable_controller_failure_spec_option", &TThis::EnableControllerFailureSpecOption)
        .Default(false);

    registrar.Parameter("enable_job_revival", &TThis::EnableJobRevival)
        .Default(true);

    registrar.Parameter("enable_locality", &TThis::EnableLocality)
        .Default(true);

    registrar.Parameter("fetcher", &TThis::Fetcher)
        .DefaultNew();

    registrar.Parameter("chunk_slice_fetcher", &TThis::ChunkSliceFetcher)
        .DefaultNew();

    registrar.Parameter("udf_registry_path", &TThis::UdfRegistryPath)
        .Default();

    registrar.Parameter("enable_tmpfs", &TThis::EnableTmpfs)
        .Default(true);
    registrar.Parameter("enable_map_job_size_adjustment", &TThis::EnableMapJobSizeAdjustment)
        .Default(true);
    registrar.Parameter("enable_job_splitting", &TThis::EnableJobSplitting)
        .Default(true);
    registrar.Parameter("enable_job_interrupts", &TThis::EnableJobInterrupts)
        .Default(true);

    registrar.Parameter("use_columnar_statistics_default", &TThis::UseColumnarStatisticsDefault)
        .Default(false);

    registrar.Parameter("lock_non_atomic_output_dynamic_tables", &TThis::LockNonAtomicOutputDynamicTables)
        .Default(false);

    registrar.Parameter("heavy_job_spec_slice_count_threshold", &TThis::HeavyJobSpecSliceCountThreshold)
        .Default(1'000)
        .GreaterThan(0);

    //! By default we disable job size adjustment for partition maps,
    //! since it may lead to partition data skew between nodes.
    registrar.Parameter("enable_partition_map_job_size_adjustment", &TThis::EnablePartitionMapJobSizeAdjustment)
        .Default(false);

    registrar.Parameter("user_job_memory_digest_precision", &TThis::UserJobMemoryDigestPrecision)
        .Default(0.01)
        .GreaterThan(0);
    registrar.Parameter("user_job_memory_reserve_quantile", &TThis::UserJobMemoryReserveQuantile)
        .InRange(0.0, 1.0)
        .Default(0.95);
    registrar.Parameter("job_proxy_memory_reserve_quantile", &TThis::JobProxyMemoryReserveQuantile)
        .InRange(0.0, 1.0)
        .Default(0.95);
    registrar.Parameter("memory_digest_resource_overdraft_factor", &TThis::MemoryDigestResourceOverdraftFactor)
        .InRange(1.0, 10.0)
        .Default(1.1);
    registrar.Parameter("user_job_resource_overdraft_memory_multiplier", &TThis::UserJobResourceOverdraftMemoryMultiplier)
        .Alias("resource_overdraft_memory_reserve_multiplier")
        .InRange(1.0, 10.0)
        .Default(std::nullopt);
    registrar.Parameter("job_proxy_resource_overdraft_memory_multiplier", &TThis::JobProxyResourceOverdraftMemoryMultiplier)
        .InRange(1.0, 10.0)
        .Default(std::nullopt);
    registrar.Parameter("use_resource_overdraft_memory_multiplier_from_spec", &TThis::UseResourceOverdraftMemoryMultiplierFromSpec)
        .Alias("use_resource_overdraft_memory_reserve_multiplier_from_spec")
        .Default(false);

    registrar.Parameter("iops_threshold", &TThis::IopsThreshold)
        .Default();
    registrar.Parameter("iops_throttler_limit", &TThis::IopsThrottlerLimit)
        .Default();

    registrar.Parameter("chunk_scraper", &TThis::ChunkScraper)
        .DefaultNew();

    registrar.Parameter("max_total_slice_count", &TThis::MaxTotalSliceCount)
        .Default((i64) 10'000'000)
        .GreaterThan(0);

    registrar.Parameter("alert_manager", &TThis::AlertManager)
        .Alias("operation_alerts")
        .DefaultNew();

    registrar.Parameter("controller_row_buffer_chunk_size", &TThis::ControllerRowBufferChunkSize)
        .Default(64_KB)
        .GreaterThan(0);

    registrar.Parameter("testing_options", &TThis::TestingOptions)
        .DefaultNew();

    registrar.Parameter("suspicious_jobs", &TThis::SuspiciousJobs)
        .DefaultNew();

    registrar.Parameter("job_spec_codec", &TThis::JobSpecCodec)
        .Default(NCompression::ECodec::Lz4);

    registrar.Parameter("job_metrics_report_period", &TThis::JobMetricsReportPeriod)
        .Default(TDuration::Seconds(15));

    registrar.Parameter("system_layer_path", &TThis::SystemLayerPath)
        .Default();

    registrar.Parameter("default_layer_path", &TThis::DefaultLayerPath)
        .Default();

    registrar.Parameter("cuda_toolkit_layer_directory_path", &TThis::CudaToolkitLayerDirectoryPath)
        .Default();

    registrar.Parameter("gpu_check_layer_directory_path", &TThis::GpuCheckLayerDirectoryPath)
        .Default();

    registrar.Parameter("docker_registry", &TThis::DockerRegistry)
        .DefaultNew();

    registrar.Parameter("schedule_allocation_statistics_log_backoff", &TThis::ScheduleAllocationStatisticsLogBackoff)
        .Alias("schedule_job_statistics_log_backoff")
        .Default(TDuration::Seconds(1));

    registrar.Parameter("schedule_allocation_statistics_moving_average_window_size", &TThis::ScheduleAllocationStatisticsMovingAverageWindowSize)
        .Alias("schedule_job_statistics_moving_average_window_size")
        .Default(50)
        .GreaterThanOrEqual(0);

    registrar.Parameter("controller_throttling_log_backoff", &TThis::ControllerThrottlingLogBackoff)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("job_spec_slice_throttler", &TThis::JobSpecSliceThrottler)
        .DefaultCtor([] { return NConcurrency::TThroughputThrottlerConfig::Create(500'000); });

    registrar.Parameter("static_orchid_cache_update_period", &TThis::StaticOrchidCacheUpdatePeriod)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("cached_running_jobs_update_period", &TThis::CachedRunningJobsUpdatePeriod)
        .Default();

    registrar.Parameter("alerts_update_period", &TThis::AlertsUpdatePeriod)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("total_controller_memory_limit", &TThis::TotalControllerMemoryLimit)
        .Default();

    registrar.Parameter("schedule_allocation_controller_queue", &TThis::ScheduleAllocationControllerQueue)
        .Alias("schedule_job_controller_queue")
        .Default(EOperationControllerQueue::Default);

    registrar.Parameter("job_events_controller_queue", &TThis::JobEventsControllerQueue)
        .Default(EOperationControllerQueue::Default);

    registrar.Parameter("invoker_pool_total_time_aggregation_period", &TThis::InvokerPoolTotalTimeAggregationPeriod)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("schedule_allocation_total_time_threshold", &TThis::ScheduleAllocationTotalTimeThreshold)
        .Alias("schedule_job_wait_time_threshold")
        .Default(TDuration::Seconds(5));
    registrar.Parameter("job_events_total_time_threshold", &TThis::JobEventsTotalTimeThreshold)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("allow_users_group_read_intermediate_data", &TThis::AllowUsersGroupReadIntermediateData)
        .Default(false);

    registrar.Parameter("custom_job_metrics", &TThis::CustomJobMetrics)
        .Default();

    registrar.Parameter("dynamic_table_lock_checking_attempt_count_limit", &TThis::DynamicTableLockCheckingAttemptCountLimit)
        .Default(10);
    registrar.Parameter("dynamic_table_lock_checking_interval_scale", &TThis::DynamicTableLockCheckingIntervalScale)
        .Default(1.5);
    registrar.Parameter("dynamic_table_lock_checking_interval_duration_min", &TThis::DynamicTableLockCheckingIntervalDurationMin)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("dynamic_table_lock_checking_interval_duration_max", &TThis::DynamicTableLockCheckingIntervalDurationMax)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("desired_block_size", &TThis::DesiredBlockSize)
        .Default(2_MB);

    registrar.Parameter("max_estimated_write_buffer_size", &TThis::MaxEstimatedWriteBufferSize)
        .Default(128_MB);
    registrar.Parameter("write_buffer_memory_overrun_alert_factor", &TThis::WriteBufferMemoryOverrunAlertFactor)
        .GreaterThan(0.0)
        .Default(0.1);

    registrar.Parameter("enable_operation_progress_archivation", &TThis::EnableOperationProgressArchivation)
        .Default(true);
    registrar.Parameter("operation_progress_archivation_timeout", &TThis::OperationProgressArchivationTimeout)
        .Default(TDuration::Seconds(3));

    registrar.Parameter("enable_controller_features_archivation", &TThis::EnableControllerFeaturesArchivation)
        .Default(true);

    registrar.Parameter("legacy_live_preview_user_blacklist", &TThis::LegacyLivePreviewUserBlacklist)
        .DefaultNew("robot-.*");

    registrar.Parameter("enable_bulk_insert_for_everyone", &TThis::EnableBulkInsertForEveryone)
        .Default(false);
    registrar.Parameter("enable_versioned_remote_copy", &TThis::EnableVersionedRemoteCopy)
        .Default(true);
    registrar.Parameter("enable_hunks_remote_copy", &TThis::EnableHunksRemoteCopy)
        .Default(true);

    registrar.Parameter("default_enable_porto", &TThis::DefaultEnablePorto)
        .Default(NScheduler::EEnablePorto::None);

    registrar.Parameter("job_reporter", &TThis::JobReporter)
        .DefaultNew();

    registrar.Parameter("heavy_request_immediate_response_timeout", &TThis::HeavyRequestImmediateResponseTimeout)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("memory_usage_profiling_period", &TThis::MemoryUsageProfilingPeriod)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("enable_bypass_artifact_cache", &TThis::EnableBypassArtifactCache)
        .Default(true);

    // COMPAT(gritukan): This default is quite dangerous, change it when all controller agents will have fresh configs.
    registrar.Parameter("tags", &TThis::Tags)
        .Default(std::vector<TString>({"default"}));

    registrar.Parameter("user_job_monitoring", &TThis::UserJobMonitoring)
        .DefaultNew();

    registrar.Parameter("obligatory_account_media", &TThis::ObligatoryAccountMedia)
        .Alias("obligatory_account_mediums")
        .Default();

    registrar.Parameter("deprecated_media", &TThis::DeprecatedMedia)
        .Alias("deprecated_mediums")
        .Default();

    registrar.Parameter("enable_master_resource_usage_accounting", &TThis::EnableMasterResourceUsageAccounting)
        .Default(true);

    registrar.Parameter("memory_watchdog", &TThis::MemoryWatchdog)
        .DefaultNew();

    registrar.Parameter("secure_vault_length_limit", &TThis::SecureVaultLengthLimit)
        .Default(64_MB);

    registrar.Parameter("chunk_teleporter", &TThis::ChunkTeleporter)
        .DefaultNew();

    registrar.Parameter("enable_columnar_statistics_early_finish", &TThis::EnableColumnarStatisticsEarlyFinish)
        .Default(true);
    registrar.Parameter("enable_table_column_renaming", &TThis::EnableTableColumnRenaming)
        .Default(false);

    registrar.Parameter("footprint_memory", &TThis::FootprintMemory)
        .Default();

    registrar.Parameter("enable_job_profiling", &TThis::EnableJobProfiling)
        .Default();

    registrar.Parameter("cuda_profiler_layer_path", &TThis::CudaProfilerLayerPath)
        .Default();

    registrar.Parameter("cuda_profiler_environment_variables", &TThis::CudaProfilerEnvironmentVariables)
        .Default();

    registrar.Parameter("cuda_profiler_environment", &TThis::CudaProfilerEnvironment)
        .Default();

    registrar.Parameter("max_running_job_statistics_update_count_per_heartbeat", &TThis::MaxRunningJobStatisticsUpdateCountPerHeartbeat)
        .Default(std::numeric_limits<int>::max());

    registrar.Parameter("running_allocation_time_statistics_updates_send_period", &TThis::RunningAllocationTimeStatisticsUpdatesSendPeriod)
        .Alias("running_job_time_statistics_updates_send_period")
        .Default(TDuration::Seconds(2));

    registrar.Parameter("job_tracker", &TThis::JobTracker)
        .DefaultNew();

    registrar.Parameter("fast_intermediate_medium", &TThis::FastIntermediateMedium)
        .Default("ssd_blobs");
    registrar.Parameter("fast_intermediate_medium_limit", &TThis::FastIntermediateMediumLimit)
        .Default(0);

    registrar.Parameter("release_failed_job_on_exception", &TThis::ReleaseFailedJobOnException)
        .Default(true);

    registrar.Parameter("network_projects_allowed_for_offloading", &TThis::NetworkProjectsAllowedForOffloading)
        .Default();

    registrar.Parameter("set_committed_attribute_via_transaction_action", &TThis::SetCommittedAttributeViaTransactionAction)
        .Default(false);

    registrar.Parameter("enable_network_in_operation_demand", &TThis::EnableNetworkInOperationDemand)
        .Default(true);

    registrar.Parameter("commit_operation_cypress_node_changes_via_system_transaction", &TThis::CommitOperationCypressNodeChangesViaSystemTransaction)
        .Default(false);

    registrar.Parameter("rpc_server", &TThis::RpcServer)
        .DefaultNew();

    registrar.Parameter("max_job_aborts_until_operation_failure", &TThis::MaxJobAbortsUntilOperationFailure)
        .Default(THashMap<EAbortReason, int>({{EAbortReason::RootVolumePreparationFailed, 1000}}));

    registrar.Parameter("job_id_unequal_to_allocation_id", &TThis::JobIdUnequalToAllocationId)
        .Default(false);

    registrar.Parameter("disallow_remote_operations", &TThis::DisallowRemoteOperations)
        .DefaultNew();

    registrar.Parameter("enable_merge_schemas_during_schema_infer", &TThis::EnableMergeSchemasDuringSchemaInfer)
        .Default(false);

    registrar.Parameter("enable_job_archive_ttl", &TThis::EnableJobArchiveTtl)
        .Default(false);

    registrar.Parameter("fail_operation_on_errors_in_live_preview", &TThis::FailOperationOnErrorsInLivePreview)
        .Default(false);

    registrar.Parameter("enable_job_fails_tolerance", &TThis::EnableJobFailsTolerance)
        .Default(false);

    registrar.Parameter("allocation_job_count_limit", &TThis::AllocationJobCountLimit)
        .Default()
        .GreaterThan(0);

    registrar.Preprocessor([&] (TControllerAgentConfig* config) {
        config->ChunkLocationThrottler->Limit = 10'000;

        // Value in options is an upper bound hint on uncompressed data size for merge jobs.
        config->OrderedMergeOperationOptions->DataWeightPerJob = 20_GB;
        config->OrderedMergeOperationOptions->MaxDataSlicesPerJob = 10'000;

        config->SortedMergeOperationOptions->DataWeightPerJob = 20_GB;
        config->SortedMergeOperationOptions->MaxDataSlicesPerJob = 10'000;

        config->UnorderedMergeOperationOptions->DataWeightPerJob = 20_GB;
        config->UnorderedMergeOperationOptions->MaxDataSlicesPerJob = 10'000;

        config->OperationOptions->AsMap()->AddChild("controller_building_job_spec_count_limit", NYTree::ConvertToNode(100));
        config->OperationOptions->AsMap()->AddChild("controller_total_building_job_spec_slice_count_limit", NYTree::ConvertToNode(200'000));
    });

    registrar.Postprocessor([&] (TControllerAgentConfig* config) {
        UpdateOptions(&config->MapOperationOptions, config->OperationOptions);
        UpdateOptions(&config->ReduceOperationOptions, config->OperationOptions);
        UpdateOptions(&config->JoinReduceOperationOptions, config->OperationOptions);
        UpdateOptions(&config->EraseOperationOptions, config->OperationOptions);
        UpdateOptions(&config->OrderedMergeOperationOptions, config->OperationOptions);
        UpdateOptions(&config->UnorderedMergeOperationOptions, config->OperationOptions);
        UpdateOptions(&config->SortedMergeOperationOptions, config->OperationOptions);
        UpdateOptions(&config->MapReduceOperationOptions, config->OperationOptions);
        UpdateOptions(&config->SortOperationOptions, config->OperationOptions);
        UpdateOptions(&config->RemoteCopyOperationOptions, config->OperationOptions);
        UpdateOptions(&config->VanillaOperationOptions, config->OperationOptions);

        THashSet<TString> customJobMetricsProfilingNames;
        for (const auto& customJobMetricDescription : config->CustomJobMetrics) {
            const auto& profilingName = customJobMetricDescription.ProfilingName;

            if (customJobMetricsProfilingNames.contains(profilingName)) {
                THROW_ERROR_EXCEPTION("Custom job metric with profiling name %Qv is already presented",
                    profilingName);
            }

            for (auto metricName : TEnumTraits<NScheduler::EJobMetricName>::GetDomainValues()) {
                if (FormatEnum(metricName) == profilingName) {
                    THROW_ERROR_EXCEPTION(
                        "Custom job metric with profiling name %Qv is already presented as builtin metric",
                        profilingName);
                }
            }

            customJobMetricsProfilingNames.insert(profilingName);
        }

        if (config->TotalControllerMemoryLimit) {
            config->MemoryWatchdog->TotalControllerMemoryLimit = config->TotalControllerMemoryLimit;
        }

        // COMPAT(omgronny)
        if (config->CudaProfilerEnvironmentVariables.empty() && config->CudaProfilerEnvironment) {
            EmplaceOrCrash(
                config->CudaProfilerEnvironmentVariables,
                config->CudaProfilerEnvironment->PathEnvironmentVariableName,
                config->CudaProfilerEnvironment->PathEnvironmentVariableValue);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TControllerAgentBootstrapConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("controller_agent", &TThis::ControllerAgent)
        .DefaultNew();
    registrar.Parameter("addresses", &TThis::Addresses)
        .Default();
    registrar.Parameter("cypress_annotations", &TThis::CypressAnnotations)
        .Default(NYTree::BuildYsonNodeFluently()
            .BeginMap()
            .EndMap()
        ->AsMap());
    registrar.Parameter("abort_on_unrecognized_options", &TThis::AbortOnUnrecognizedOptions)
        .Default(false);
    registrar.Parameter("heap_profiler", &TThis::HeapProfiler)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TControllerAgentProgramConfig::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

PHOENIX_DEFINE_OPAQUE_TYPE(TEraseOperationOptions);
PHOENIX_DEFINE_OPAQUE_TYPE(TMapOperationOptions);
PHOENIX_DEFINE_OPAQUE_TYPE(TMapReduceOperationOptions);
PHOENIX_DEFINE_OPAQUE_TYPE(TOperationOptions);
PHOENIX_DEFINE_OPAQUE_TYPE(TOrderedMergeOperationOptions);
PHOENIX_DEFINE_OPAQUE_TYPE(TReduceOperationOptions);
PHOENIX_DEFINE_OPAQUE_TYPE(TRemoteCopyOperationOptions);
PHOENIX_DEFINE_OPAQUE_TYPE(TSimpleOperationOptions);
PHOENIX_DEFINE_OPAQUE_TYPE(TSortedMergeOperationOptions);
PHOENIX_DEFINE_OPAQUE_TYPE(TSortOperationOptions);
PHOENIX_DEFINE_OPAQUE_TYPE(TSortOperationOptionsBase);
PHOENIX_DEFINE_OPAQUE_TYPE(TUnorderedMergeOperationOptions);
PHOENIX_DEFINE_OPAQUE_TYPE(TVanillaOperationOptions);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
