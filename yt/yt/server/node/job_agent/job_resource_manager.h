#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/node_resource_manager.h>

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt_proto/yt/client/node_tracker_client/proto/node.pb.h>

namespace NYT::NJobAgent {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EResourcesConsumerType,
    ((MasterJob)      (0))
    ((SchedulerJob)   (1))
);

class IJobResourceManager
    : public TRefCounted
{
protected:
    class TResourceAcquiringContext;
    class TImpl;

public:
    virtual void Initialize() = 0;

    virtual void Start() = 0;

    //! Returns the maximum allowed resource usage.
    virtual NClusterNode::TJobResources GetResourceLimits() const = 0;

    virtual NNodeTrackerClient::NProto::TDiskResources GetDiskResources() const = 0;

    //! Set resource limits overrides.
    virtual void SetResourceLimitsOverrides(const NNodeTrackerClient::NProto::TNodeResourceLimitsOverrides& resourceLimits) = 0;

    virtual double GetCpuToVCpuFactor() const = 0;

    //! Returns resource usage of running jobs.
    virtual NClusterNode::TJobResources GetResourceUsage(bool includeWaiting = false) const = 0;

    //! Compares new usage with resource limits. Detects resource overdraft.
    virtual bool CheckMemoryOverdraft(const NClusterNode::TJobResources& delta) = 0;

    virtual TResourceAcquiringContext GetResourceAcquiringContext() = 0;

    virtual int GetWaitingResourceHolderCount() = 0;

    virtual void RegisterResourcesConsumer(TClosure onResourcesReleased, EResourcesConsumerType consumer) = 0;

    static IJobResourceManagerPtr CreateJobResourceManager(NClusterNode::IBootstrapBase* bootstrap);

    DECLARE_INTERFACE_SIGNAL(void(), ResourcesAcquired);
    DECLARE_INTERFACE_SIGNAL(void(EResourcesConsumerType, bool), ResourcesReleased);

    DECLARE_INTERFACE_SIGNAL(
        void(i64 mapped),
        ReservedMemoryOvercommited);

protected:
    friend TResourceHolder;

    class TResourceAcquiringContext
    {
    public:
        explicit TResourceAcquiringContext(IJobResourceManager* resourceManagerImpl);
        TResourceAcquiringContext(const TResourceAcquiringContext&) = delete;
        ~TResourceAcquiringContext();

        bool TryAcquireResourcesFor(const TResourceHolderPtr& resourceHolder) &;

    private:
        NConcurrency::TForbidContextSwitchGuard Guard_;
        TImpl* const ResourceManagerImpl_;
    };
};

DEFINE_REFCOUNTED_TYPE(IJobResourceManager)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EResourcesState,
    ((Waiting)   (0))
    ((Acquired)  (1))
    ((Released)  (2))
);

////////////////////////////////////////////////////////////////////////////////

class TResourceHolder
    : public TRefCounted
{
public:
    TResourceHolder(
        IJobResourceManager* jobResourceManager,
        EResourcesConsumerType resourceConsumerType,
        NLogging::TLogger logger,
        const NClusterNode::TJobResources& jobResources,
        const NClusterNode::TJobResourceAttributes& resourceAttributes,
        int portCount);

    TResourceHolder(const TResourceHolder&) = delete;
    TResourceHolder(TResourceHolder&&) = delete;
    ~TResourceHolder();

    void ReleaseResources();

    const std::vector<int>& GetPorts() const noexcept;

    //! Returns resource usage delta.
    NClusterNode::TJobResources SetResourceUsage(NClusterNode::TJobResources newResourceUsage);

    void ReleaseCumulativeResources();

    NClusterNode::TJobResources GetResourceUsage() const noexcept;

    const NClusterNode::TJobResourceAttributes& GetResourceAttributes() const noexcept;

    const NLogging::TLogger& GetLogger() const noexcept;

    NClusterNode::TJobResources ChangeCumulativeResourceUsage(NClusterNode::TJobResources resourceUsageDelta);

    NClusterNode::TJobResources GetResourceLimits() const noexcept;

protected:
    NLogging::TLogger Logger;

    NClusterNode::ISlotPtr UserSlot_;

    std::vector<NClusterNode::ISlotPtr> GpuSlots_;

private:
    friend IJobResourceManager::TResourceAcquiringContext;
    friend IJobResourceManager::TImpl;

    IJobResourceManager::TImpl* const ResourceManagerImpl_;

    const int PortCount_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, ResourcesLock_);

    NClusterNode::TJobResources Resources_;
    NClusterNode::TJobResourceAttributes ResourceAttributes_;

    std::vector<int> Ports_;

    EResourcesState State_ = EResourcesState::Waiting;

    const EResourcesConsumerType ResourcesConsumerType_;

    class TAcquiredResources;
    void SetAcquiredResources(TAcquiredResources&& acquiredResources);
    virtual void OnResourcesAcquired() = 0;
};

DEFINE_REFCOUNTED_TYPE(TResourceHolder)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
