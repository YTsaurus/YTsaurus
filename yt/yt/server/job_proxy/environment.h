#pragma once

#include "public.h"

#include <yt/yt/library/containers/public.h>
#include <yt/yt/library/containers/porto_resource_tracker.h>

#include <yt/yt/library/containers/cgroup.h>

#include <yt/yt/library/process/process.h>

#include <yt/yt/client/job_tracker_client/public.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/ytree/public.h>
#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

struct TJobEnvironmentCpuStatistics
{
    TDuration UserUsageTime;
    TDuration SystemUsageTime;
    TDuration WaitTime;
    TDuration ThrottledTime;
    ui64 ContextSwitchesDelta = 0;
    ui64 PeakThreadCount = 0;
};

void Serialize(const TJobEnvironmentCpuStatistics& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

struct TJobEnvironmentMemoryStatistics
{
    ui64 Rss = 0;
    ui64 MappedFile = 0;
    ui64 MajorPageFaults = 0;
};

void Serialize(const TJobEnvironmentMemoryStatistics& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

struct TJobEnvironmentBlockIOStatistics
{
    ui64 IOReadByte = 0;
    ui64 IOWriteByte = 0;

    ui64 IOReadOps = 0;
    ui64 IOWriteOps = 0;
    ui64 IOOps = 0;
};

void Serialize(const TJobEnvironmentBlockIOStatistics& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

struct TJobEnvironmentNetworkStatistics
{
    ui64 TxBytes = 0;
    ui64 TxPackets = 0;
    ui64 TxDrops = 0;

    ui64 RxBytes = 0;
    ui64 RxPackets = 0;
    ui64 RxDrops = 0;
};

void Serialize(const TJobEnvironmentNetworkStatistics& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

struct IProfiledEnvironment
    : public virtual TRefCounted
{
    virtual TErrorOr<std::optional<TJobEnvironmentCpuStatistics>> GetCpuStatistics() const = 0;
    virtual TErrorOr<std::optional<TJobEnvironmentBlockIOStatistics>> GetBlockIOStatistics() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IProfiledEnvironment)

////////////////////////////////////////////////////////////////////////////////

struct TUserJobEnvironmentOptions
{
    //! Path to core watcher pipes directory relative to user job working directory.
    std::optional<TString> SlotCoreWatcherDirectory;

    //! Path to core watcher pipes directory relative to job proxy working directory.
    std::optional<TString> CoreWatcherDirectory;

    std::optional<NContainers::TRootFS> RootFS;

    std::vector<TString> GpuDevices;

    std::optional<TString> HostName;
    std::vector<TUserJobNetworkAddressPtr> NetworkAddresses;
    bool EnableNat64;
    bool DisableNetwork;

    bool EnableCudaGpuCoreDump = false;

    bool EnablePortoMemoryTracking = false;

    NContainers::EEnablePorto EnablePorto = NContainers::EEnablePorto::None;

    i64 ThreadLimit;
};

////////////////////////////////////////////////////////////////////////////////

struct IUserJobEnvironment
    : public virtual IProfiledEnvironment
{
    virtual std::optional<TDuration> GetBlockIOWatchdogPeriod() const = 0;

    virtual TErrorOr<std::optional<TJobEnvironmentMemoryStatistics>> GetMemoryStatistics() const = 0;

    virtual TErrorOr<std::optional<TJobEnvironmentNetworkStatistics>> GetNetworkStatistics() const = 0;

    virtual void CleanProcesses() = 0;

    virtual void SetIOThrottle(i64 operations) = 0;

    virtual TFuture<void> SpawnUserProcess(
        const TString& path,
        const std::vector<TString>& arguments,
        const TString& workingDirectory) = 0;

    virtual NContainers::IInstancePtr GetUserJobInstance() const = 0;

    virtual std::optional<pid_t> GetJobRootPid() const = 0;
    virtual std::vector<pid_t> GetJobPids() const = 0;

    virtual bool IsPidNamespaceIsolationEnabled() const = 0;

    //! Returns the list of environment-specific environment variables in key=value format.
    virtual const std::vector<TString>& GetEnvironmentVariables() const = 0;

    virtual i64 GetMajorPageFaultCount() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IUserJobEnvironment)

////////////////////////////////////////////////////////////////////////////////

struct IJobProxyEnvironment
    : public virtual IProfiledEnvironment
{
    virtual void SetCpuGuarantee(double value) = 0;
    virtual void SetCpuLimit(double value) = 0;
    virtual void SetCpuPolicy(const TString& policy) = 0;
    virtual IUserJobEnvironmentPtr CreateUserJobEnvironment(
        NJobTrackerClient::TJobId jobId,
        const TUserJobEnvironmentOptions& options) = 0;

    // TODO(gritukan, khlebnikov): For now job proxy and user job are run in the same cgroup
    // in CRI environment. This means that their statistics are indisguishable. Remove these methods
    // when separate container for job proxy is implemented.
    virtual TErrorOr<std::optional<TJobEnvironmentMemoryStatistics>> GetJobMemoryStatistics() const = 0;
    virtual TErrorOr<std::optional<TJobEnvironmentBlockIOStatistics>> GetJobBlockIOStatistics() const = 0;
    virtual TErrorOr<std::optional<TJobEnvironmentCpuStatistics>> GetJobCpuStatistics() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobProxyEnvironment)

////////////////////////////////////////////////////////////////////////////////

IJobProxyEnvironmentPtr CreateJobProxyEnvironment(NYTree::INodePtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
