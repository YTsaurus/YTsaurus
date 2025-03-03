#pragma once

#include "public.h"

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/server/lib/rpc_proxy/config.h>

#include <yt/yt/ytlib/auth/config.h>

#include <yt/yt/ytlib/hydra/config.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/job_proxy/public.h>
#include <yt/yt/ytlib/job_proxy/config.h>

#include <yt/yt/library/server_program/config.h>

#include <yt/yt/library/containers/config.h>

#include <yt/yt/library/containers/cri/config.h>

#include <yt/yt/library/tracing/jaeger/tracer.h>

#include <yt/yt/client/file_client/config.h>

#include <yt/yt/core/bus/tcp/config.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/node.h>
#include <yt/yt/core/ytree/polymorphic_yson_struct.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/library/dns_over_rpc/client/config.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

struct TJobProxyTestingConfig
    : public NYTree::TYsonStruct
{
    bool FailOnJobProxySpawnedCall;

    REGISTER_YSON_STRUCT(TJobProxyTestingConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobProxyTestingConfig)

////////////////////////////////////////////////////////////////////////////////

struct TJobThrottlerConfig
    : public NYTree::TYsonStruct
{
    TDuration MinBackoffTime;
    TDuration MaxBackoffTime;
    double BackoffMultiplier;

    TDuration RpcTimeout;

    NConcurrency::TPrefetchingThrottlerConfigPtr BandwidthPrefetch;
    NConcurrency::TPrefetchingThrottlerConfigPtr RpsPrefetch;

    REGISTER_YSON_STRUCT(TJobThrottlerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobThrottlerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCoreWatcherConfig
    : public NYTree::TYsonStruct
{
    //! Cores lookup period.
    TDuration Period;

    //! Input/output operations timeout.
    TDuration IOTimeout;

    //! Finalization timeout.
    TDuration FinalizationTimeout;

    //! Cumulative timeout for cores processing.
    TDuration CoresProcessingTimeout;

    REGISTER_YSON_STRUCT(TCoreWatcherConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCoreWatcherConfig)

////////////////////////////////////////////////////////////////////////////////

class TUserJobNetworkAddress
    : public NYTree::TYsonStruct
{
public:
    NNet::TIP6Address Address;

    TString Name;

    REGISTER_YSON_STRUCT(TUserJobNetworkAddress);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUserJobNetworkAddress)

////////////////////////////////////////////////////////////////////////////////

struct TTmpfsManagerConfig
    : public NYTree::TYsonStruct
{
    std::vector<TString> TmpfsPaths;

    REGISTER_YSON_STRUCT(TTmpfsManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTmpfsManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TMemoryTrackerConfig
    : public NYTree::TYsonStruct
{
    bool IncludeMemoryMappedFiles;

    bool UseSMapsMemoryTracker;

    TDuration MemoryStatisticsCachePeriod;

    REGISTER_YSON_STRUCT(TMemoryTrackerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMemoryTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TBindConfig
    : public NYTree::TYsonStruct
{
    TString ExternalPath;
    TString InternalPath;
    bool ReadOnly;

    REGISTER_YSON_STRUCT(TBindConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBindConfig)

////////////////////////////////////////////////////////////////////////////////

struct TJobTraceEventProcessorConfig
    : public NYTree::TYsonStruct
{
    NServer::TJobReporterConfigPtr Reporter;

    int LoggingInterval;

    REGISTER_YSON_STRUCT(TJobTraceEventProcessorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobTraceEventProcessorConfig)

////////////////////////////////////////////////////////////////////////////////

struct TJobThrashingDetectorConfig
    : public NYTree::TYsonStruct
{
    bool Enabled;

    TDuration CheckPeriod;

    int MajorPageFaultCountLimit;

    // Job will be aborted upon violating MajorPageFaultCountLimit this number of times in a row.
    int LimitOverflowCountThresholdToAbortJob;

    REGISTER_YSON_STRUCT(TJobThrashingDetectorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobThrashingDetectorConfig)

//! Describes configuration of a single environment.
class TJobEnvironmentConfigBase
    : public NYTree::TYsonStruct
{
public:
    //! When job control is enabled, system runs user jobs under fake
    //! uids in range [StartUid, StartUid + SlotCount - 1].
    int StartUid;

    TDuration MemoryWatchdogPeriod;

    TJobThrashingDetectorConfigPtr JobThrashingDetector;

    REGISTER_YSON_STRUCT(TJobEnvironmentConfigBase);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobEnvironmentConfigBase)

////////////////////////////////////////////////////////////////////////////////

struct TSimpleJobEnvironmentConfig
    : public TJobEnvironmentConfigBase
{
    REGISTER_YSON_STRUCT(TSimpleJobEnvironmentConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSimpleJobEnvironmentConfig)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETestingJobEnvironmentScenario,
    (None)
    (IncreasingMajorPageFaultCount)
);

////////////////////////////////////////////////////////////////////////////////

struct TTestingJobEnvironmentConfig
    : public TSimpleJobEnvironmentConfig
{
    ETestingJobEnvironmentScenario TestingJobEnvironmentScenario;

    REGISTER_YSON_STRUCT(TTestingJobEnvironmentConfig);

    static void Register(TRegistrar);
};

DEFINE_REFCOUNTED_TYPE(TTestingJobEnvironmentConfig)

////////////////////////////////////////////////////////////////////////////////

struct TPortoJobEnvironmentConfig
    : public TJobEnvironmentConfigBase
{
    NContainers::TPortoExecutorDynamicConfigPtr PortoExecutor;

    TDuration BlockIOWatchdogPeriod;

    THashMap<TString, TString> ExternalBinds;

    double JobsIOWeight;
    double NodeDedicatedCpu;

    bool UseShortContainerNames;

    // COMPAT(psushin): this is compatibility option between different versions of ytcfgen and yt_node.
    //! Used by ytcfgen, when it creates "yt_daemon" subcontainer inside iss_hook_start.
    bool UseDaemonSubcontainer;

    //! For testing purposes only.
    bool UseExecFromLayer;

    //! Backoff time between container destruction attempts.
    TDuration ContainerDestructionBackoff;

    REGISTER_YSON_STRUCT(TPortoJobEnvironmentConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPortoJobEnvironmentConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCriJobEnvironmentConfig
    : public TJobEnvironmentConfigBase
{
    NContainers::NCri::TCriExecutorConfigPtr CriExecutor;

    NContainers::NCri::TCriImageCacheConfigPtr CriImageCache;

    TString JobProxyImage;

    //! Bind mounts for job proxy container.
    //! For now works as "root_fs_binds" because user job runs in the same container.
    std::vector<NJobProxy::TBindConfigPtr> JobProxyBindMounts;

    //! Do not bind mount jobproxy binary into container
    bool UseJobProxyFromImage;

    REGISTER_YSON_STRUCT(TCriJobEnvironmentConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCriJobEnvironmentConfig)

////////////////////////////////////////////////////////////////////////////////

DEFINE_POLYMORPHIC_YSON_STRUCT_FOR_ENUM(JobEnvironmentConfig, EJobEnvironmentType,
    ((Base)         (TJobEnvironmentConfigBase))
    ((Simple)     (TSimpleJobEnvironmentConfig))
    ((Porto)       (TPortoJobEnvironmentConfig))
    ((Testing)   (TTestingJobEnvironmentConfig))
    ((Cri)           (TCriJobEnvironmentConfig))
);

////////////////////////////////////////////////////////////////////////////////

struct TJobProxyInternalConfig
    : public NServer::TNativeServerBootstrapConfig
    , public TServerProgramConfig
{
    // Job-specific parameters.
    int SlotIndex = -1;

    TString SlotPath;

    TTmpfsManagerConfigPtr TmpfsManager;

    TMemoryTrackerConfigPtr MemoryTracker;

    //! Bind mounts for user job container.
    //! Includes "root_fs_binds" and artifacts if they are passed as bind mounts.
    std::vector<TBindConfigPtr> Binds;

    std::vector<int> GpuIndexes;

    //! Path for container root, if root volume is already prepared.
    std::optional<TString> RootPath;

    //! Docker image to build root volume as part of a container.
    std::optional<TString> DockerImage;

    //! Docker image id in local cache.
    std::optional<TString> DockerImageId;

    // COMPAT(artemagafonov): RootFS is always writable, so the flag should be removed after the update of all nodes.
    bool MakeRootFSWritable;

    //! Enable mount of fuse device to user job container.
    bool EnableFuse;

    //! Path to write job proxy stderr (for testing purposes).
    std::optional<TString> StderrPath;
    //! Path to write executor stderr (for testing purposes).
    std::optional<TString> ExecutorStderrPath;

    NBus::TBusClientConfigPtr SupervisorConnection;
    TDuration SupervisorRpcTimeout;

    NBus::TBusClientConfigPtr TvmBridgeConnection;
    NAuth::TTvmBridgeConfigPtr TvmBridge;

    TDuration HeartbeatPeriod;
    TDuration InputPipeBlinkerPeriod;

    TJobEnvironmentConfig JobEnvironment;

    //! Addresses derived from node local descriptor to leverage locality.
    NNodeTrackerClient::TAddressMap Addresses;
    std::string LocalHostName;
    std::optional<TString> Rack;
    std::optional<TString> DataCenter;

    i64 AheadMemoryReserve;

    bool AlwaysAbortOnMemoryReserveOverdraft;

    bool TestRootFS;

    TJobThrottlerConfigPtr JobThrottler;

    //! Hostname to set in container.
    std::optional<TString> HostName;

    bool EnableNat64;
    bool DisableNetwork;

    //! Network addresses to bind into container.
    std::vector<TUserJobNetworkAddressPtr> NetworkAddresses;

    bool AbortOnUnrecognizedOptions;

    bool AbortOnUncaughtException;

    TCoreWatcherConfigPtr CoreWatcher;

    bool TestPollJobShell;

    //! If set, user job will not receive uid.
    //! For testing purposes only.
    bool DoNotSetUserId;

    //! This option can disable memory limit check for user jobs.
    //! Used in arcadia tests, since it's almost impossible to set
    //! proper memory limits for asan builds.
    bool CheckUserJobMemoryLimit;

    //! If set, abort user job at detecting OOM kill inside container.
    bool CheckUserJobOomKill;

    //! Compat option for urgent disable of job shell audit.
    bool EnableJobShellSeccopm;

    //! Enabled using Porto kill for signalling instead of manual discovery of process pid.
    bool UsePortoKillForSignalling;

    bool ForceIdleCpuPolicy;

    bool SendHeartbeatBeforeAbort;

    //! If set, lightweight chunk specs for stderr and core tables
    //! will be sent in heartbeats to the controller agent.
    bool EnableStderrAndCoreLivePreview;

    //! Forward variables from job proxy environment to user job.
    bool ForwardAllEnvironmentVariables;

    std::optional<double> ContainerCpuLimit;

    std::optional<i64> SlotContainerMemoryLimit;

    NYT::NRpcProxy::TApiServiceConfigPtr ApiService;

    std::optional<int> StatisticsOutputTableCountLimit;

    NDns::TDnsOverRpcResolverConfigPtr DnsOverRpcResolver;

    NJobProxy::TJobTestingOptionsPtr JobTestingOptions;

    NAuth::TAuthenticationManagerConfigPtr AuthenticationManager;

    //! Supports ability to use direct connection to masters.
    NApi::NNative::TConnectionCompoundConfigPtr OriginalClusterConnection;

    TJobProxyTestingConfigPtr TestingConfig;

    bool UseRetryingChannels;

    NRpc::TRetryingChannelConfigPtr RetryingChannel;

    bool EnableCudaProfileEventStreaming;

    TJobTraceEventProcessorConfigPtr JobTraceEventProcessor;

    std::optional<int> OperationsArchiveVersion;
    TDuration PipeReaderTimeoutThreshold;

    i64 AdaptiveRowCountUpperBound;

    //! Enable root volume disk quota.
    //! Apply the quota to the entire RootFs instead of the sandbox and tmp folders individually.
    bool EnableRootVolumeDiskQuota;

    REGISTER_YSON_STRUCT(TJobProxyInternalConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobProxyInternalConfig)

////////////////////////////////////////////////////////////////////////////////

struct TJobProxyDynamicConfig
    : public NYTree::TYsonStruct
{
    NTracing::TJaegerTracerDynamicConfigPtr Jaeger;

    bool EnableJobShellSeccopm;

    bool UsePortoKillForSignalling;

    bool ForceIdleCpuPolicy;

    bool AbortOnUncaughtException;

    bool EnableStderrAndCoreLivePreview;

    //! If set, abort user job at detecting OOM kill inside container.
    bool CheckUserJobOomKill;

    TJobEnvironmentConfig JobEnvironment;

    TJobProxyTestingConfigPtr TestingConfig;

    bool UseRetryingChannels;

    NRpc::TRetryingChannelConfigPtr RetryingChannel;

    TDuration PipeReaderTimeoutThreshold;

    bool EnableCudaProfileEventStreaming;

    NJobProxy::TJobTraceEventProcessorConfigPtr JobTraceEventProcessor;

    i64 AdaptiveRowCountUpperBound;

    std::optional<TString> MemoryProfileDumpPath;

    REGISTER_YSON_STRUCT(TJobProxyDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobProxyDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
