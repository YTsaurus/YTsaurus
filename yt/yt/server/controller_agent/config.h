#pragma once

#include "private.h"

#include <yt/yt/server/lib/chunk_pools/public.h>

#include <yt/yt/server/lib/job_agent/public.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/server/lib/scheduler/job_metrics.h>

#include <yt/yt/ytlib/scheduler/config.h>

#include <yt/yt/ytlib/chunk_client/config.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/event_log/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/library/server_program/config.h>

#include <yt/yt/client/job_tracker_client/public.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/library/re2/public.h>

#include <yt/yt/library/program/config.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/phoenix/polymorphic.h>
#include <yt/yt/core/phoenix/type_decl.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

struct TIntermediateChunkScraperConfig
    : public NChunkClient::TChunkScraperConfig
{
    TDuration RestartTimeout;

    REGISTER_YSON_STRUCT(TIntermediateChunkScraperConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TIntermediateChunkScraperConfig)

////////////////////////////////////////////////////////////////////////////////

class TTestingOptions
    : public NYTree::TYsonStruct
{
public:
    //! Testing option that enables snapshot build/load cycle after operation materialization.
    bool EnableSnapshotCycleAfterMaterialization;

    //! If this option is set, these layers are used in all the user jobs
    //! and all the rootfs's become writable.
    std::vector<NYPath::TRichYPath> RootfsTestLayers;

    //! If this option is set, controller agent sleeps for this duration before performing actual unregistration.
    std::optional<TDuration> DelayInUnregistration;

    //! If this option is set, controller agent sleeps for this duration before finishing handshake.
    std::optional<TDuration> DelayInHandshake;

    bool AbortOutputTransactionAfterCompletionTransactionCommit;

    REGISTER_YSON_STRUCT(TTestingOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTestingOptions)

////////////////////////////////////////////////////////////////////////////////

struct TLowGpuPowerUsageOnWindowConfig
    : public NYTree::TYsonStruct
{
    // Size of window to analyze.
    TDuration WindowSize;

    // Period of making cumulative usage records.
    TDuration RecordPeriod;

    // Power threshold in Watts.
    double Threshold;

    REGISTER_YSON_STRUCT(TLowGpuPowerUsageOnWindowConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TLowGpuPowerUsageOnWindowConfig)

////////////////////////////////////////////////////////////////////////////////

struct TAlertManagerConfig
    : public NYTree::TYsonStruct
{
    // Period of analyzing for alerts.
    TDuration Period;

    // Maximum allowed ratio of unused tmpfs size.
    double TmpfsAlertMaxUnusedSpaceRatio;

    // Min unused space threshold. If unutilized space is less than
    // this threshold then operation alert will not be set.
    i64 TmpfsAlertMinUnusedSpaceThreshold;

    // Minimum memory usage ratio required to mute tmpfs usage alert.
    double TmpfsAlertMemoryUsageMuteRatio;

    // Alert can be set only if unused memory greater than this number of bytes.
    i64 MemoryUsageAlertMaxUnusedSize;

    // Alert can be set only if unused memory ratio greater than this threshold.
    double MemoryUsageAlertMaxUnusedRatio;

    // Alert can be set only if number of jobs less than or equal to this threshold.
    std::optional<int> MemoryUsageAlertMaxJobCount;

    // Alert can be set when memory reserve factor equals 1 and unused memory ratio greater than this threshold.
    double MemoryReserveFactorAlertMaxUnusedRatio;

    // Maximum allowed aborted jobs time. If it is violated
    // then operation alert will be set.
    i64 AbortedJobsAlertMaxAbortedTime;

    // Maximum allowed aborted jobs time ratio.
    double AbortedJobsAlertMaxAbortedTimeRatio;

    // Minimum desired job duration.
    TDuration ShortJobsAlertMinJobDuration;

    // Minimum number of completed jobs after which alert can be set.
    i64 ShortJobsAlertMinJobCount;

    // Minimum allowed ratio of operation duration to max job duration.
    double ShortJobsAlertMinAllowedOperationDurationToMaxJobDurationRatio;

    // Minimum partition size to enable data skew check.
    i64 IntermediateDataSkewAlertMinPartitionSize;

    // Minimum interquartile range to consider data to be skewed.
    i64 IntermediateDataSkewAlertMinInterquartileRange;

    // Job spec throttling alert is triggered if throttler activation
    // count is above this threshold.
    i64 JobSpecThrottlingAlertActivationCountThreshold;

    // Minimum total time sum to analyze operation
    TDuration LowCpuUsageAlertMinExecTime;
    // Minimum average job time to analyze operation
    TDuration LowCpuUsageAlertMinAverageJobTime;
    // Cpu usage threshold to send an alert
    double LowCpuUsageAlertCpuUsageThreshold;
    std::vector<NStatisticPath::TStatisticPath> LowCpuUsageAlertStatistics;
    std::vector<NJobTrackerClient::EJobState> LowCpuUsageAlertJobStates;

    // Minimum average job time to analyze operation
    TDuration HighCpuWaitAlertMinAverageJobTime;
    // Minimum cpu wait time ratio to send an alert
    double HighCpuWaitAlertThreshold;
    std::vector<NStatisticPath::TStatisticPath> HighCpuWaitAlertStatistics;
    std::vector<NJobTrackerClient::EJobState> HighCpuWaitAlertJobStates;

    // Minimum wall time of operation duration
    TDuration OperationTooLongAlertMinWallTime;

    // Threshold for estimate duration of operation
    TDuration OperationTooLongAlertEstimateDurationThreshold;

    TDuration LowGpuUsageAlertMinDuration;
    TDuration LowGpuUsageAlertMinTotalGpuDuration;
    // Ratio.
    double LowGpuUsageAlertGpuUsageThreshold;
    // Ratio.
    double LowGpuUsageAlertGpuUtilizationPowerThreshold;
    // Ratio.
    double LowGpuUsageAlertGpuUtilizationSMThreshold;
    std::vector<NStatisticPath::TStatisticPath> LowGpuUsageAlertStatistics;
    std::vector<NJobTrackerClient::EJobState> LowGpuUsageAlertJobStates;

    TLowGpuPowerUsageOnWindowConfigPtr LowGpuPowerUsageOnWindow;

    // High queue average wait time alert is triggered
    // if queues with average wait time above this threshold are found.
    TDuration QueueTotalTimeEstimateThreshold;

    // Set alert if ratio of unavailable network bandwidth time is higher than this threshold.
    double TaskUnavailableNetworkBandwidthTimeRatioAlertThreshold;

    REGISTER_YSON_STRUCT(TAlertManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAlertManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TJobSplitterConfig
    : public NYTree::TYsonStruct
{
    TDuration MinJobTime;
    double ExecToPrepareTimeRatio;
    double NoProgressJobTimeToAveragePrepareTimeRatio;
    i64 MinTotalDataWeight;
    TDuration UpdatePeriod;
    double ResidualJobFactor;
    int ResidualJobCountMinThreshold;
    double CandidatePercentile;
    double LateJobsPercentile;
    int MaxJobsPerSplit;
    int MaxInputTableCount;
    TDuration SplitTimeoutBeforeSpeculate;
    TDuration JobLoggingPeriod;

    bool EnableJobSplitting;

    bool EnableJobSpeculation;

    bool ShowRunningJobsInProgress;

    REGISTER_YSON_STRUCT(TJobSplitterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobSplitterConfig)

////////////////////////////////////////////////////////////////////////////////

class TSuspiciousJobsOptions
    : public NYTree::TYsonStruct
{
public:
    //! Duration of no activity by job to be considered as suspicious.
    TDuration InactivityTimeout;

    //! Cpu usage delta that is considered insignificant when checking if job is suspicious.
    i64 CpuUsageThreshold;

    //! Time fraction spent in idle state of JobProxy -> UserJob pipe enough for job to be considered suspicious.
    double InputPipeIdleTimeFraction;

    //! Time fraction spent in idle state of UserJob -> JobProxy pipe enough for job to be considered suspicious.
    double OutputPipeIdleTimeFraction;

    //! Suspicious jobs per operation recalculation period.
    TDuration UpdatePeriod;

    //! Maximum number of suspicious jobs that are reported in Orchid for each job type.
    i64 MaxOrchidEntryCountPerType;

    REGISTER_YSON_STRUCT(TSuspiciousJobsOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSuspiciousJobsOptions)

////////////////////////////////////////////////////////////////////////////////

class TDataBalancerOptions
    : public NYTree::TYsonStruct
{
public:
    i64 LoggingMinConsecutiveViolationCount;
    TDuration LoggingPeriod;
    double Tolerance;
    bool UseNodeIOWeight;

    REGISTER_YSON_STRUCT(TDataBalancerOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDataBalancerOptions)

////////////////////////////////////////////////////////////////////////////////

class TUserJobOptions
    : public NYTree::TYsonStruct
{
public:
    //! Thread limit for the user job is ceil(#InitialThreadLimit + #ThreadLimitMultiplier * JobCpuLimit);
    i64 ThreadLimitMultiplier;
    i64 InitialThreadLimit;

    REGISTER_YSON_STRUCT(TUserJobOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUserJobOptions)

////////////////////////////////////////////////////////////////////////////////

class TOperationOptions
    : public NYTree::TYsonStruct
    , public virtual NPhoenix::TPolymorphicBase
{
public:
    NYTree::INodePtr SpecTemplate;

    //! Controls finer initial slicing of input data to ensure even distribution of data split sizes among jobs.
    double SliceDataWeightMultiplier;

    //! Maximum number of primary data slices per job. It is the default value for user operations.
    int MaxDataSlicesPerJob;

    //! Users can tune the maximum number of primary data slices per job in their operations upto this limit.
    int MaxDataSlicesPerJobLimit;

    i64 MaxSliceDataWeight;
    i64 MinSliceDataWeight;

    int MaxInputTableCount;

    //! Maximum number of output tables times job count an operation can have.
    int MaxOutputTablesTimesJobsCount;

    //! Options controlling retries with data_weight_per_job increase that happen in some of the operations.
    int MaxBuildRetryCount;
    double DataWeightPerJobRetryFactor;

    TJobSplitterConfigPtr JobSplitter;

    //! This flags currently makes sense only for Porto environment.
    //! It forces setting container CPU limit on slot container calculated as
    //! JobCpuLimit * CpuLimitOvercommitMultiplier + InitialCpuLimitOvercommit
    bool SetContainerCpuLimit;

    double CpuLimitOvercommitMultiplier;
    double InitialCpuLimitOvercommit;

    //! Enforce slot container memory limit.
    bool SetSlotContainerMemoryLimit;

    i64 SlotContainerMemoryOverhead;

    //! Number of simultaneously building job specs after which controller starts throttling.
    std::optional<int> ControllerBuildingJobSpecCountLimit;
    //! Total slice count of currently building job specs after which controller starts throttling.
    std::optional<i64> ControllerTotalBuildingJobSpecSliceCountLimit;

    //! Limit for number of aggregated custom job statistics per operation.
    i64 CustomStatisticsCountLimit;

    TUserJobOptionsPtr UserJobOptions;

    REGISTER_YSON_STRUCT(TOperationOptions);

    static void Register(TRegistrar registrar);

private:
    PHOENIX_DECLARE_OPAQUE_TYPE(TOperationOptions, 0x6d2a0bdd);
};

DEFINE_REFCOUNTED_TYPE(TOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TSimpleOperationOptions
    : public TOperationOptions
{
public:
    int MaxJobCount;
    i64 DataWeightPerJob;

    REGISTER_YSON_STRUCT(TSimpleOperationOptions);

    static void Register(TRegistrar registrar);

private:
    PHOENIX_DECLARE_OPAQUE_TYPE(TSimpleOperationOptions, 0x875251fa);
};

DEFINE_REFCOUNTED_TYPE(TSimpleOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TMapOperationOptions
    : public TSimpleOperationOptions
{
public:
    NChunkPools::TJobSizeAdjusterConfigPtr JobSizeAdjuster;

    REGISTER_YSON_STRUCT(TMapOperationOptions);

    static void Register(TRegistrar registrar);

private:
    PHOENIX_DECLARE_OPAQUE_TYPE(TMapOperationOptions, 0x5d08252b);
};

DEFINE_REFCOUNTED_TYPE(TMapOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TUnorderedMergeOperationOptions
    : public TSimpleOperationOptions
{
public:
    REGISTER_YSON_STRUCT(TUnorderedMergeOperationOptions);

    static void Register(TRegistrar registrar);

private:
    PHOENIX_DECLARE_OPAQUE_TYPE(TUnorderedMergeOperationOptions, 0x28332598);
};

DEFINE_REFCOUNTED_TYPE(TUnorderedMergeOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TOrderedMergeOperationOptions
    : public TSimpleOperationOptions
{
public:
    REGISTER_YSON_STRUCT(TOrderedMergeOperationOptions);

    static void Register(TRegistrar registrar);

private:
    PHOENIX_DECLARE_OPAQUE_TYPE(TOrderedMergeOperationOptions, 0xc71863e6);
};

DEFINE_REFCOUNTED_TYPE(TOrderedMergeOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TSortedMergeOperationOptions
    : public TSimpleOperationOptions
{
public:
    REGISTER_YSON_STRUCT(TSortedMergeOperationOptions);

    static void Register(TRegistrar registrar);

private:
    PHOENIX_DECLARE_OPAQUE_TYPE(TSortedMergeOperationOptions, 0x9089b24a);
};

DEFINE_REFCOUNTED_TYPE(TSortedMergeOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TReduceOperationOptions
    : public TSortedMergeOperationOptions
{
public:
    REGISTER_YSON_STRUCT(TReduceOperationOptions);

    static void Register(TRegistrar registrar);

private:
    PHOENIX_DECLARE_OPAQUE_TYPE(TReduceOperationOptions, 0x91371bf5);
};

DEFINE_REFCOUNTED_TYPE(TReduceOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TEraseOperationOptions
    : public TOrderedMergeOperationOptions
{
public:
    REGISTER_YSON_STRUCT(TEraseOperationOptions);

    static void Register(TRegistrar registrar);

private:
    PHOENIX_DECLARE_OPAQUE_TYPE(TEraseOperationOptions, 0x73cb9f3b);
};

DEFINE_REFCOUNTED_TYPE(TEraseOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TSortOperationOptionsBase
    : public TOperationOptions
{
public:
    int MaxPartitionJobCount;
    int MaxPartitionCount;
    int MaxNewPartitionCount;
    int MaxPartitionFactor;
    i32 MaxSampleSize;
    i64 CompressedBlockSize;
    i64 MinPartitionWeight;
    i64 MinUncompressedBlockSize;
    i64 MaxValueCountPerSimpleSortJob;
    NChunkPools::TJobSizeAdjusterConfigPtr PartitionJobSizeAdjuster;
    TDataBalancerOptionsPtr DataBalancer;
    double CriticalNewPartitionDifferenceRatio;

    REGISTER_YSON_STRUCT(TSortOperationOptionsBase);

    static void Register(TRegistrar registrar);

private:
    PHOENIX_DECLARE_OPAQUE_TYPE(TSortOperationOptionsBase, 0x1f448453);
};

DEFINE_REFCOUNTED_TYPE(TSortOperationOptionsBase)

////////////////////////////////////////////////////////////////////////////////

class TSortOperationOptions
    : public TSortOperationOptionsBase
{
public:
    REGISTER_YSON_STRUCT(TSortOperationOptions);

    static void Register(TRegistrar registrar);

private:
    PHOENIX_DECLARE_OPAQUE_TYPE(TSortOperationOptions, 0xc11251c0);
};

DEFINE_REFCOUNTED_TYPE(TSortOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TMapReduceOperationOptions
    : public TSortOperationOptionsBase
{
public:
    REGISTER_YSON_STRUCT(TMapReduceOperationOptions);

    static void Register(TRegistrar registrar);

private:
    PHOENIX_DECLARE_OPAQUE_TYPE(TMapReduceOperationOptions, 0x91e3968d);
};

DEFINE_REFCOUNTED_TYPE(TMapReduceOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TRemoteCopyOperationOptions
    : public TSimpleOperationOptions
{
public:
    NScheduler::TCpuResource CpuLimit;
    std::optional<NNodeTrackerClient::TNetworkPreferenceList> Networks;

    REGISTER_YSON_STRUCT(TRemoteCopyOperationOptions);

    static void Register(TRegistrar registrar);

private:
    PHOENIX_DECLARE_OPAQUE_TYPE(TRemoteCopyOperationOptions, 0xf3893dc8);
};

DEFINE_REFCOUNTED_TYPE(TRemoteCopyOperationOptions)

////////////////////////////////////////////////////////////////////////////////

struct TGangManagerConfig
    : public NYTree::TYsonStruct
{
    bool Enabled;

    REGISTER_YSON_STRUCT(TGangManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TGangManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TVanillaOperationOptions
    : public TOperationOptions
{
public:
    //! Maximum number of tasks allowed.
    int MaxTaskCount;

    //! Maximum total number of jobs.
    int MaxTotalJobCount;

    TGangManagerConfigPtr GangManager;

    REGISTER_YSON_STRUCT(TVanillaOperationOptions);

    static void Register(TRegistrar registrar);

private:
    PHOENIX_DECLARE_OPAQUE_TYPE(TVanillaOperationOptions, 0x93998ffa);
};

DEFINE_REFCOUNTED_TYPE(TVanillaOperationOptions)

////////////////////////////////////////////////////////////////////////////////

struct TZombieOperationOrchidsConfig
    : public NYTree::TYsonStruct
{
    //! Maximum number of retained orchids.
    int Limit;

    //! Period for cleaning old orchids.
    TDuration CleanPeriod;

    //! Is orchid saving and cleaning enabled?
    bool Enable;

    REGISTER_YSON_STRUCT(TZombieOperationOrchidsConfig);

    static void Register(TRegistrar registrar);

private:
    PHOENIX_DECLARE_OPAQUE_TYPE(TZombieOperationOrchidsConfig, 0xbeadbead);
};

DEFINE_REFCOUNTED_TYPE(TZombieOperationOrchidsConfig)

////////////////////////////////////////////////////////////////////////////////

struct TUserJobMonitoringConfig
    : public NYTree::TYsonStruct
{
    int DefaultMaxMonitoredUserJobsPerOperation;
    int ExtendedMaxMonitoredUserJobsPerOperation;

    THashMap<EOperationType, bool> EnableExtendedMaxMonitoredUserJobsPerOperation;

    int MaxMonitoredUserJobsPerAgent;

    REGISTER_YSON_STRUCT(TUserJobMonitoringConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUserJobMonitoringConfig)

////////////////////////////////////////////////////////////////////////////////

struct TMemoryWatchdogConfig
    : public NYTree::TYsonStruct
{
    std::optional<i64> TotalControllerMemoryLimit;

    //! Memory limit (in bytes) for operation controller. If controller exceeds this limit,
    //! operations fails.
    i64 OperationControllerMemoryLimit;

    //! Memory threshold (in bytes) for operation controller.
    //! Operation controller that exceeds this threshold may fail if total controller memory limit is exceeded.
    i64 OperationControllerMemoryOverconsumptionThreshold;

    TDuration MemoryUsageCheckPeriod;

    REGISTER_YSON_STRUCT(TMemoryWatchdogConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMemoryWatchdogConfig)

////////////////////////////////////////////////////////////////////////////////

struct TUserFileLimitsConfig
    : public NYTree::TYsonStruct
{
    //! Maximum size of file allowed to be passed to jobs.
    i64 MaxSize;
    //! Maximum data weight of table file allowed to be passed to jobs.
    i64 MaxTableDataWeight;
    //! Maximum chunk count of file allowed to be passed to jobs.
    i64 MaxChunkCount;

    REGISTER_YSON_STRUCT(TUserFileLimitsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUserFileLimitsConfig)

////////////////////////////////////////////////////////////////////////////////

struct TUserFileLimitsPatchConfig
    : public NYTree::TYsonStruct
{
    //! Maximum size of file allowed to be passed to jobs.
    std::optional<i64> MaxSize;
    //! Maximum data weight of table file allowed to be passed to jobs.
    std::optional<i64> MaxTableDataWeight;
    //! Maximum chunk count of file allowed to be passed to jobs.
    std::optional<i64> MaxChunkCount;

    REGISTER_YSON_STRUCT(TUserFileLimitsPatchConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUserFileLimitsPatchConfig)

////////////////////////////////////////////////////////////////////////////////

class TJobTrackerTestingOptions
    : public NYTree::TYsonStruct
{
public:
    NScheduler::TDelayConfigPtr DelayInSettleJob;

    REGISTER_YSON_STRUCT(TJobTrackerTestingOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobTrackerTestingOptions)

struct TJobTrackerConfig
    : public NYTree::TYsonStruct
{
    TDuration NodeDisconnectionTimeout;

    TDuration JobConfirmationTimeout;

    int LoggingJobSampleSize;

    TDuration DurationBeforeJobConsideredDisappearedFromNode;

    bool CheckNodeHeartbeatSequenceNumber;

    int HeavyInvokerThreadCount;

    TJobTrackerTestingOptionsPtr TestingOptions;

    REGISTER_YSON_STRUCT(TJobTrackerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDockerRegistryConfig
    : public NYTree::TYsonStruct
{
    //! FQDN of internal docker registry for docker images stored in Cypress.
    std::string InternalRegistryAddress;

    bool UseYtTokenForInternalRegistry = false;

    bool ForwardInternalImagesToJobSpecs = false;

    bool TranslateInternalImagesIntoLayers = true;

    REGISTER_YSON_STRUCT(TDockerRegistryConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDockerRegistryConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDisallowRemoteOperationsConfig
    : public NYTree::TYsonStruct
{
    THashSet<TString> AllowedUsers;
    THashSet<TString> AllowedClusters;
    THashSet<TString> AllowedForEveryoneClusters;

    REGISTER_YSON_STRUCT(TDisallowRemoteOperationsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDisallowRemoteOperationsConfig)

////////////////////////////////////////////////////////////////////////////////

struct TControllerAgentConfig
    : public TSingletonsDynamicConfig
{
    //! Number of chunk lists to be allocated when an operation starts.
    int ChunkListPreallocationCount;

    //! Maximum number of chunk lists to request via a single request.
    int MaxChunkListAllocationCount;

    //! Better keep the number of spare chunk lists above this threshold.
    int ChunkListWatermarkCount;

    //! Each time the number of spare chunk lists drops below #ChunkListWatermarkCount or
    //! the controller requests more chunk lists than we currently have,
    //! another batch is allocated. Each time we allocate #ChunkListAllocationMultiplier times
    //! more chunk lists than previously.
    double ChunkListAllocationMultiplier;

    //! Desired number of chunks to release in one batch.
    int DesiredChunkListsPerRelease;

    //! Number of the master cells to use for intermediate data storage.
    int IntermediateOutputMasterCellCount;

    //! If |true|, snapshots are periodically constructed and uploaded into the system.
    bool EnableSnapshotBuilding;

    bool EnableSnapshotBuildingDisabledAlert;

    //! Interval between consequent snapshots.
    TDuration SnapshotPeriod;

    //! Timeout for snapshot construction.
    TDuration SnapshotTimeout;

    //! Maximum time allotted to fork during snapshot building.
    //! If process did not fork within this timeout, it crashes.
    TDuration SnapshotForkTimeout;

    //! Timeout to wait for controller suspension before constructing a snapshot.
    TDuration OperationControllerSuspendTimeout;

    //! Number of parallel operation snapshot builders.
    int ParallelSnapshotBuilderCount;

    //! Configuration for uploading snapshots to Cypress.
    NApi::TFileWriterConfigPtr SnapshotWriter;

    //! If |true|, snapshots are loaded during revival.
    bool EnableSnapshotLoading;

    bool EnableSnapshotLoadingDisabledAlert;

    //! Configuration for downloading snapshots from Cypress.
    NApi::TFileReaderConfigPtr SnapshotReader;

    TDuration TransactionsRefreshPeriod;
    TDuration OperationsUpdatePeriod;
    TDuration IntermediateMediumUsageUpdatePeriod;
    TDuration ChunkUnstagePeriod;

    bool EnableUnrecognizedAlert;

    //! Maximum number of chunk trees to attach per request.
    int MaxChildrenPerAttachRequest;

    //! Limits the rate (measured in chunks) of location requests issued by all active chunk scrapers.
    NConcurrency::TThroughputThrottlerConfigPtr ChunkLocationThrottler;

    NEventLog::TEventLogManagerConfigPtr EventLog;

    //! Controller agent-to-scheduler heartbeat timeout.
    TDuration SchedulerHandshakeRpcTimeout;

    //! Controller agent-to-scheduler handshake failure backoff.
    TDuration SchedulerHandshakeFailureBackoff;

    //! Controller agent-to-scheduler heartbeat timeout.
    TDuration SchedulerHeartbeatRpcTimeout;

    //! Controller agent-to-scheduler heartbeat failure backoff.
    TDuration SchedulerHeartbeatFailureBackoff;

    //! Controller agent-to-scheduler heartbeat period.
    TDuration SchedulerHeartbeatPeriod;

    //! Controller agent-to-scheduler heartbeat with schedule jobs period.
    TDuration ScheduleAllocationHeartbeatPeriod;

    //! Period for requesting exec nodes from scheduler.
    TDuration ExecNodesUpdatePeriod;

    //! Period for requesting config from scheduler.
    TDuration ConfigUpdatePeriod;

    //! Period for pushing any operation info from agent to scheduler.
    TDuration OperationsPushPeriod;

    //! Period for pushing operation alerts from agent to scheduler.
    TDuration OperationJobMetricsPushPeriod;

    //! Period for pushing operation alerts from agent to scheduler.
    TDuration OperationAlertsPushPeriod;

    //! Period for pushing suspicious jobs from agent to scheduler.
    TDuration SuspiciousJobsPushPeriod;

    //! Number of threads for hosting controllers' invokers.
    int ControllerThreadCount;

    //! Number of thread for hosting chunk scrapers' invokers.
    int ChunkScraperThreadCount;

    //! Number of threads for running job spec build callbacks.
    int JobSpecBuildThreadCount;

    //! Number of threads for heavy routines associated with job statistics.
    int StatisticsOffloadThreadCount;

    //! Period of controller static orchid part update.
    TDuration ControllerStaticOrchidUpdatePeriod;

    //! Period of controller orchid key update.
    TDuration ControllerOrchidKeysUpdatePeriod;

    //! Limit on the number of concurrent core dumps that can be written because
    //! of failed safe assertions inside controllers.
    int MaxConcurrentSafeCoreDumps;

    //! Timeout to store cached value of exec nodes information
    //! for scheduling tag filter without access.
    TDuration SchedulingTagFilterExpireTimeout;

    TSuspiciousJobsOptionsPtr SuspiciousJobs;

    //! Period to update aggregated running job statistics from current running jobs.
    TDuration RunningJobStatisticsUpdatePeriod;

    //! Maximum allowed running time of operation. Null value is interpreted as infinity.
    std::optional<TDuration> OperationTimeLimit;

    TDuration OperationTimeLimitCheckPeriod;

    TDuration ResourceDemandSanityCheckPeriod;

    //! Timeout on operation initialization.
    //! Prevents hanging of remote copy when remote cluster is unavailable.
    TDuration OperationInitializationTimeout;

    TDuration OperationTransactionTimeout;

    TDuration OperationTransactionPingPeriod;

    TDuration OperationLogProgressBackoff;

    TDuration AvailableExecNodesCheckPeriod;
    TDuration BannedExecNodesCheckPeriod;

    TDuration OperationBuildProgressPeriod;

    TDuration CheckTentativeTreeEligibilityPeriod;

    TDuration UpdateAccountResourceUsageLeasesPeriod;

    TDuration TaskUpdatePeriod;

    TZombieOperationOrchidsConfigPtr ZombieOperationOrchids;

    // Maximum number of jobs to save as retained in operation orchid.
    int MaxRetainedJobsPerOperation;

    //! Maximum number of job specs in archive per operation.
    int MaxArchivedJobSpecCountPerOperation;

    //! Guaranteed number of job specs in archive per operation.
    int GuaranteedArchivedJobSpecCountPerOperation;

    //! Job spec with job duration greater that this will be archived.
    TDuration MinJobDurationToArchiveJobSpec;

    //! Maximum number of chunks per single fetch.
    int MaxChunksPerFetch;

    //! Maximum number of input tables an operation can have.
    int MaxInputTableCount;

    //! Maximum number of output tables an operation can have.
    int MaxOutputTableCount;

    //! Maximum number of ranges on the input table.
    int MaxRangesOnTable;

    TUserFileLimitsConfigPtr UserFileLimits;
    THashMap<TString, TUserFileLimitsPatchConfigPtr> UserFileLimitsPerTree;

    //! Maximum number of files per user job.
    int MaxUserFileCount;

    //! Don't check resource demand for sanity if the number of online
    //! nodes is less than this bound.
    // TODO(ignat): rename to SafeExecNodeCount.
    int SafeOnlineNodeCount;

    //! Don't check resource demand for sanity if scheduler is online
    //! less than this timeout.
    TDuration SafeSchedulerOnlineTime;

    //! Time between two consecutive calls in operation controller to get exec nodes information from scheduler.
    TDuration ControllerExecNodeInfoUpdatePeriod;

    //! Maximum number of foreign chunks to locate per request.
    int MaxChunksPerLocateRequest;

    //! Enables using tmpfs if tmpfs_path is specified in user spec.
    bool EnableTmpfs;

    //! Enables dynamic change of job sizes.
    bool EnablePartitionMapJobSizeAdjustment;

    bool EnableMapJobSizeAdjustment;

    //! Enables splitting of long jobs.
    // TODO(gritukan): Remove it.
    bool EnableJobSplitting;

    //! Enables job interrupts, both for job splitting and preemption.
    bool EnableJobInterrupts;

    bool UseColumnarStatisticsDefault;

    //! Mimics the old behavior when output dynamic tables with atomicity=none were not locked.
    bool LockNonAtomicOutputDynamicTables;

    double UserJobMemoryDigestPrecision;
    double UserJobMemoryReserveQuantile;
    double JobProxyMemoryReserveQuantile;
    double MemoryDigestResourceOverdraftFactor;
    std::optional<double> UserJobResourceOverdraftMemoryMultiplier;
    std::optional<double> JobProxyResourceOverdraftMemoryMultiplier;
    bool UseResourceOverdraftMemoryMultiplierFromSpec;

    //! If user job iops threshold is exceeded, iops throttling is enabled via cgroups.
    std::optional<int> IopsThreshold;
    std::optional<int> IopsThrottlerLimit;

    //! Patch for all operation options.
    NYT::NYTree::INodePtr OperationOptions;

    //! Specific operation options.
    TMapOperationOptionsPtr MapOperationOptions;
    TReduceOperationOptionsPtr ReduceOperationOptions;
    TReduceOperationOptionsPtr JoinReduceOperationOptions;
    TEraseOperationOptionsPtr EraseOperationOptions;
    TOrderedMergeOperationOptionsPtr OrderedMergeOperationOptions;
    TUnorderedMergeOperationOptionsPtr UnorderedMergeOperationOptions;
    TSortedMergeOperationOptionsPtr SortedMergeOperationOptions;
    TMapReduceOperationOptionsPtr MapReduceOperationOptions;
    TSortOperationOptionsPtr SortOperationOptions;
    TRemoteCopyOperationOptionsPtr RemoteCopyOperationOptions;
    TVanillaOperationOptionsPtr VanillaOperationOptions;

    //! Default environment variables set for every job.
    THashMap<TString, TString> Environment;

    //! If |true|, jobs are revived from snapshot.
    bool EnableJobRevival;

    //! If |false|, all locality timeouts are considered 0.
    bool EnableLocality;

    //! Allow failing a controller by passing testing option `controller_failure`
    //! in operation spec. Used only for testing purposes.
    bool EnableControllerFailureSpecOption;

    NChunkClient::TFetcherConfigPtr Fetcher;

    NChunkClient::TChunkSliceFetcherConfigPtr ChunkSliceFetcher;

    std::optional<NYPath::TYPath> UdfRegistryPath;

    //! Discriminates between "heavy" and "light" job specs. For those with slice count
    //! not exceeding this threshold no throttling is done.
    int HeavyJobSpecSliceCountThreshold;

    //! We use the same config for input chunk scraper and intermediate chunk scraper.
    TIntermediateChunkScraperConfigPtr ChunkScraper;

    //! Total number of data slices in operation, summed up over all jobs.
    i64 MaxTotalSliceCount;

    TAlertManagerConfigPtr AlertManager;

    //! Chunk size in per-controller row buffers.
    i64 ControllerRowBufferChunkSize;

    TTestingOptionsPtr TestingOptions;

    NCompression::ECodec JobSpecCodec;

    //! Period between consequent job metrics pushes from agent to scheduler.
    TDuration JobMetricsReportPeriod;

    // Cypress path to a special layer containing YT-specific data required to
    // run jobs with custom rootfs, e.g. statically linked ytserver-exec.
    // Is applied on top of user layers if they are used.
    std::optional<TString> SystemLayerPath;

    // Cypress path to a default layer for user jobs, if no layers were specified explicitly.
    std::optional<TString> DefaultLayerPath;

    // Cypress path to the directory with CUDA toolkit layers which are required for some
    // GPU jobs. The layer is applied as an additional user layer on top of the others if they are
    // present.
    std::optional<TString> CudaToolkitLayerDirectoryPath;

    // Cypress path to the directory with GPU check layers.  This layer is used to perform GPU check before user job start.
    // The layer is applied as an additional user layer on top of the other layers (if they are present).
    std::optional<TString> GpuCheckLayerDirectoryPath;

    //! Controls handling docker images specified in user spec.
    TDockerRegistryConfigPtr DockerRegistry;

    // Running jobs cached YSON string update period.
    TDuration CachedRunningJobsUpdatePeriod;

    //! Backoff between schedule job statistics logging.
    TDuration ScheduleAllocationStatisticsLogBackoff;
    int ScheduleAllocationStatisticsMovingAverageWindowSize;

    //! Backoff between controller throttling logging.
    TDuration ControllerThrottlingLogBackoff;

    //! Controls the rate at which jobs are scheduled in termes of slices per second.
    NConcurrency::TThroughputThrottlerConfigPtr JobSpecSliceThrottler;

    TDuration StaticOrchidCacheUpdatePeriod;

    TDuration AlertsUpdatePeriod;

    std::optional<i64> TotalControllerMemoryLimit;

    EOperationControllerQueue ScheduleAllocationControllerQueue;
    EOperationControllerQueue JobEventsControllerQueue;

    TDuration InvokerPoolTotalTimeAggregationPeriod;

    TDuration ScheduleAllocationTotalTimeThreshold;
    TDuration JobEventsTotalTimeThreshold;

    // TODO(levysotsky): Get rid of this option when everybody migrates to new operation ACLs.
    bool AllowUsersGroupReadIntermediateData;

    std::vector<NScheduler::TCustomJobMetricDescription> CustomJobMetrics;

    int DynamicTableLockCheckingAttemptCountLimit;
    double DynamicTableLockCheckingIntervalScale;
    TDuration DynamicTableLockCheckingIntervalDurationMin;
    TDuration DynamicTableLockCheckingIntervalDurationMax;

    i64 DesiredBlockSize;
    i64 MaxEstimatedWriteBufferSize;
    double WriteBufferMemoryOverrunAlertFactor;

    bool EnableOperationProgressArchivation;
    TDuration OperationProgressArchivationTimeout;

    bool EnableControllerFeaturesArchivation;

    //! Regex for users having legacy live preview disabled by default.
    NRe2::TRe2Ptr LegacyLivePreviewUserBlacklist;

    bool EnableBulkInsertForEveryone;
    bool EnableVersionedRemoteCopy;
    bool EnableHunksRemoteCopy;

    NScheduler::EEnablePorto DefaultEnablePorto;

    NServer::TJobReporterConfigPtr JobReporter;

    //! Timeout for the response to a heavy request to the operation controller,
    //! such as Initialize, Prepare, Materialize, Revive or Commit.
    //! If such an action's execution time does not exceed this timeout, its result is sent in the corresponding response.
    //! Otherwise, the immediate response is empty, and the result is sent in one of the following heartbeats.
    TDuration HeavyRequestImmediateResponseTimeout;

    TDuration MemoryUsageProfilingPeriod;

    bool EnableBypassArtifactCache;

    //! List of the tags assigned to controller agent.
    std::vector<TString> Tags;

    TUserJobMonitoringConfigPtr UserJobMonitoring;

    TMemoryWatchdogConfigPtr MemoryWatchdog;

    //! List of media that require specifying account and disk space limit.
    THashSet<TString> ObligatoryAccountMedia;

    //! List of media that are deprecated to be used in disk requests.
    THashSet<TString> DeprecatedMedia;

    //! The name of the fast medium (SSD) in the communal intermediate account.
    TString FastIntermediateMedium;

    //! Per transaction intermediate data weight limit for the fast medium (SSD) in the communal intermediate account.
    i64 FastIntermediateMediumLimit;

    bool EnableMasterResourceUsageAccounting;

    //! Size limit for YT_SECRET_VAULT environment variable exposed to jobs,
    //! i.e. maximum size of secret vault encoded as text YSON.
    i64 SecureVaultLengthLimit;

    NChunkClient::TChunkTeleporterConfigPtr ChunkTeleporter;

    bool EnableColumnarStatisticsEarlyFinish;

    // COMPAT(levysotsky): See YT-16507
    bool EnableTableColumnRenaming;

    // Supposed to be used in tests.
    std::optional<i64> FootprintMemory;

    //! Enables job profiling.
    bool EnableJobProfiling;

    std::optional<TString> CudaProfilerLayerPath;

    THashMap<std::string, std::string> CudaProfilerEnvironmentVariables;
    // COMPAT(omgronnny)
    NScheduler::TCudaProfilerEnvironmentPtr CudaProfilerEnvironment;

    int MaxRunningJobStatisticsUpdateCountPerHeartbeat;
    TDuration RunningAllocationTimeStatisticsUpdatesSendPeriod;

    bool ReleaseFailedJobOnException;

    TJobTrackerConfigPtr JobTracker;

    THashSet<TString> NetworkProjectsAllowedForOffloading;

    // COMPAT(kvk1920): Remove after all masters will be updated to 23.1.
    bool SetCommittedAttributeViaTransactionAction;

    bool EnableNetworkInOperationDemand;

    // COMPAT(kvk1920): Remove after all masters will be >= 23.3.
    bool CommitOperationCypressNodeChangesViaSystemTransaction;

    NRpc::TServerDynamicConfigPtr RpcServer;

    //! How many initial successive job aborts are needed to fail operation.
    THashMap<EAbortReason, int> MaxJobAbortsUntilOperationFailure;

    bool JobIdUnequalToAllocationId;

    TDisallowRemoteOperationsConfigPtr DisallowRemoteOperations;

    bool EnableMergeSchemasDuringSchemaInfer;

    bool EnableJobArchiveTtl;

    bool FailOperationOnErrorsInLivePreview;

    bool EnableJobFailsTolerance;

    std::optional<ui32> AllocationJobCountLimit;

    REGISTER_YSON_STRUCT(TControllerAgentConfig);

    static void Register(TRegistrar registrar);

private:
    template <class TOptions>
    static void UpdateOptions(TOptions* options, NYT::NYTree::INodePtr patch);
};

DEFINE_REFCOUNTED_TYPE(TControllerAgentConfig)

////////////////////////////////////////////////////////////////////////////////

struct TControllerAgentBootstrapConfig
    : public NServer::TNativeServerBootstrapConfig
{
    TControllerAgentConfigPtr ControllerAgent;

    //! Known scheduler addresses.
    NNodeTrackerClient::TNetworkAddressList Addresses;

    NYTree::IMapNodePtr CypressAnnotations;

    bool AbortOnUnrecognizedOptions;

    THeapProfilerConfigPtr HeapProfiler;

    REGISTER_YSON_STRUCT(TControllerAgentBootstrapConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TControllerAgentBootstrapConfig)

////////////////////////////////////////////////////////////////////////////////

struct TControllerAgentProgramConfig
    : public TControllerAgentBootstrapConfig
    , public TServerProgramConfig
{
    REGISTER_YSON_STRUCT(TControllerAgentProgramConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TControllerAgentProgramConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent

#define CONFIG_INL_H_
#include "config-inl.h"
#undef CONFIG_INL_H_
