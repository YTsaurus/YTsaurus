#include "config.h"

#include <yt/yt/core/ytree/request_complexity_limiter.h>

namespace NYT::NObjectServer {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TMutationIdempotizerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enabled", &TThis::Enabled)
        .Default(true);
    registrar.Parameter("expiration_time", &TThis::ExpirationTime)
        .Default(TDuration::Minutes(5));
    registrar.Parameter("expiration_check_period", &TThis::ExpirationCheckPeriod)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("max_expired_mutation_id_removals_per_commit", &TThis::MaxExpiredMutationIdRemovalsPerCommit)
        .Default(50000);
}

DEFINE_REFCOUNTED_TYPE(TMutationIdempotizerConfig)

////////////////////////////////////////////////////////////////////////////////

void TDynamicObjectManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_weight_per_gc_sweep", &TThis::MaxWeightPerGCSweep)
        .Default(100000);
    registrar.Parameter("gc_sweep_period", &TThis::GCSweepPeriod)
        .Default(TDuration::MilliSeconds(1000));
    registrar.Parameter("enable_gc", &TThis::EnableGC)
        .Default(true)
        .DontSerializeDefault();
    registrar.Parameter("object_removal_cells_sync_period", &TThis::ObjectRemovalCellsSyncPeriod)
        .Default(TDuration::MilliSeconds(100));
    registrar.Parameter("mutation_idempotizer", &TThis::MutationIdempotizer)
        .DefaultNew();
    registrar.Parameter("reserved_attributes", &TThis::ReservedAttributes)
        .Default();
    registrar.Parameter("yson_string_intern_length_threshold", &TThis::YsonStringInternLengthThreshold)
        .Default(DefaultYsonStringInternLengthThreshold)
        .InRange(DefaultYsonStringInternLengthThreshold, 1_GB);
    registrar.Parameter("profiling_period", &TThis::ProfilingPeriod)
        .Default(DefaultProfilingPeriod);
    registrar.Parameter("reset_hunk_storage_in_table_destroy", &TThis::ResetHunkStorageInTableDestroy)
        .Default(true)
        .DontSerializeDefault();
    registrar.Parameter("prohibit_prerequisite_revisions_differ_from_execution_paths", &TThis::ProhibitPrerequisiteRevisionsDifferFromExecutionPaths)
        .Default(false)
        .DontSerializeDefault();
}

DEFINE_REFCOUNTED_TYPE(TDynamicObjectManagerConfig)

////////////////////////////////////////////////////////////////////////////////

void TObjectServiceConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("yield_timeout", &TThis::YieldTimeout)
        .Default(TDuration::MilliSeconds(10));

    registrar.Parameter("sticky_user_error_expire_time", &TThis::StickyUserErrorExpireTime)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("cross_cell_sync_delay", &TThis::CrossCellSyncDelay)
        .Default(TDuration::MilliSeconds(10));

    registrar.Parameter("timeout_backoff_lead_time", &TThis::TimeoutBackoffLeadTime)
        .Default(TDuration::Seconds(3));
    registrar.Parameter("default_execute_timeout", &TThis::DefaultExecuteTimeout)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("forwarded_request_timeout_reserve", &TThis::ForwardedRequestTimeoutReserve)
        .Default(TDuration::Seconds(3));

    registrar.Parameter("master_cache", &TThis::MasterCache)
        .DefaultNew();

    registrar.Parameter("enable_local_read_busy_wait", &TThis::EnableLocalReadBusyWait)
        .Default(true);
}

DEFINE_REFCOUNTED_TYPE(TObjectServiceConfig)

////////////////////////////////////////////////////////////////////////////////

TReadRequestComplexity
TReadRequestComplexityLimitsConfigBase::ToReadRequestComplexity() const noexcept
{
    return {
        .NodeCount = NodeCount,
        .ResultSize = ResultSize,
    };
}

void TReadRequestComplexityLimitsConfigBase::Register(TRegistrar /*registrar*/)
{ }

void TReadRequestComplexityLimitsConfigBase::DoRegister(
    TRegistrar registrar,
    i64 nodeCount,
    i64 resultSize)
{
    registrar.Parameter("node_count", &TThis::NodeCount)
        .Default(nodeCount)
        .GreaterThanOrEqual(0);
    registrar.Parameter("result_size", &TThis::ResultSize)
        .Default(resultSize)
        .GreaterThanOrEqual(0);
}

void TDefaultReadRequestComplexityLimitsConfig::Register(TRegistrar registrar)
{
    DoRegister(registrar, /*nodeCount*/ 1'000'000, /*resultSize*/ 100_MB);
}

DEFINE_REFCOUNTED_TYPE(TDefaultReadRequestComplexityLimitsConfig)

void TMaxReadRequestComplexityLimitsConfig::Register(TRegistrar registrar)
{
    DoRegister(registrar, /*nodeCount*/ 100'000'000, /*resultSize*/ 2_GB);
}

DEFINE_REFCOUNTED_TYPE(TMaxReadRequestComplexityLimitsConfig)

////////////////////////////////////////////////////////////////////////////////

void TDynamicObjectServiceConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_two_level_cache", &TThis::EnableTwoLevelCache)
        .Default(true);
    registrar.Parameter("local_read_thread_count", &TThis::LocalReadThreadCount)
        .Alias("local_read_worker_count")
        .GreaterThan(0)
        .Default(4);
    registrar.Parameter("local_read_offload_thread_count", &TThis::LocalReadOffloadThreadCount)
        .GreaterThan(0)
        .Default(8)
        .DontSerializeDefault();
    registrar.Parameter("schedule_reply_retry_backoff", &TThis::ScheduleReplyRetryBackoff)
        .Default(TDuration::MilliSeconds(100));

    registrar.Parameter("local_read_executor_quantum_duration", &TThis::LocalReadExecutorQuantumDuration)
        .Default(TDuration::MilliSeconds(10));

    registrar.Parameter("process_sessions_period", &TThis::ProcessSessionsPeriod)
        .Default(TDuration::MilliSeconds(10));

    registrar.Parameter("minimize_execute_latency", &TThis::MinimizeExecuteLatency)
        .Default(false);

    registrar.Parameter("default_read_request_complexity_limits", &TThis::DefaultReadRequestComplexityLimits)
        .DefaultNew();

    registrar.Parameter("max_read_request_complexity_limits", &TThis::MaxReadRequestComplexityLimits)
        .DefaultNew();

    registrar.Parameter("enable_read_request_complexity_limits", &TThis::EnableReadRequestComplexityLimits)
        .Default(false);

    registrar.Parameter("local_read_request_throttler", &TThis::LocalReadRequestThrottler)
        .DefaultNew();

    registrar.Parameter("local_write_request_throttler", &TThis::LocalWriteRequestThrottler)
        .DefaultNew();
}

DEFINE_REFCOUNTED_TYPE(TDynamicObjectServiceConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
