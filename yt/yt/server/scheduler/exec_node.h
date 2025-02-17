#pragma once

#include "public.h"
#include "helpers.h"

#include "scheduling_context.h"

#include <yt/yt/server/lib/controller_agent/helpers.h>

#include <yt/yt/server/lib/scheduler/scheduling_tag.h>
#include <yt/yt/server/lib/scheduler/exec_node_descriptor.h>

#include <yt/yt/server/lib/scheduler/proto/controller_agent_tracker_service.pb.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/scheduler/proto/scheduler_service.pb.h>

#include <yt/yt/core/concurrency/lease_manager.h>

#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/library/vector_hdrf/job_resources.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

//! Scheduler-side representation of an execution node.
/*!
 *  Thread affinity: corresponding node shard thread (unless noted otherwise).
 */
class TExecNode
    : public TRefCounted
{
private:
    using TAllocationMap = THashMap<TAllocationId, TAllocationPtr>;
    using TAllocationsToAbortMap = THashMap<TAllocationId, EAbortReason>;

public:
    DEFINE_BYVAL_RO_PROPERTY(NNodeTrackerClient::TNodeId, Id);
    DEFINE_BYREF_RW_PROPERTY(NNodeTrackerClient::TNodeDescriptor, NodeDescriptor);

    //! Node's resource limits, as reported by the node.
    DEFINE_BYREF_RW_PROPERTY(TJobResources, ResourceLimits);

    //! The most recent resource usage, as reported by the node.
    /*!
     *  Some fields are also updated by the scheduler strategy to
     *  reflect recent job set changes.
     *  E.g. when the scheduler decides to
     *  start a new job it decrements the appropriate counters.
     */
    DEFINE_BYREF_RW_PROPERTY(TJobResources, ResourceUsage);

    DEFINE_BYREF_RW_PROPERTY(TDiskResources, DiskResources);

    //! Allocations that are currently running on this node.
    DEFINE_BYREF_RW_PROPERTY(THashSet<TAllocationPtr>, Allocations);

    //! Mapping from job id to job on this node.
    DEFINE_BYREF_RW_PROPERTY(TAllocationMap, IdToAllocation);

    //! Allocations that were aborted by scheduler.
    DEFINE_BYREF_RW_PROPERTY(TAllocationsToAbortMap, AllocationsToAbort);

    //! A set of scheduling tags assigned to this node.
    DEFINE_BYREF_RO_PROPERTY(TBooleanFormulaTags, Tags);

    //! Last time when logging of jobs on node took place.
    DEFINE_BYVAL_RW_PROPERTY(std::optional<NProfiling::TCpuInstant>, LastAllocationsLogTime);

    //! Last time when missing jobs were checked on this node.
    DEFINE_BYVAL_RW_PROPERTY(std::optional<NProfiling::TCpuInstant>, LastCheckMissingAllocationsTime);

    //! Last time when heartbeat from node was processed.
    DEFINE_BYVAL_RW_PROPERTY(TInstant, LastSeenTime);

    //! Last time when controller agents were sent in heartbeat.
    DEFINE_BYVAL_RW_PROPERTY(TInstant, LastRegisteredControllerAgentsSentTime);

    //! Controls node at scheduler.
    DEFINE_BYVAL_RW_PROPERTY(NConcurrency::TLease, RegistrationLease);

    //! Controls heartbeat expiration.
    DEFINE_BYVAL_RW_PROPERTY(NConcurrency::TLease, HeartbeatLease);

    //! State of node at master.
    DEFINE_BYVAL_RW_PROPERTY(NNodeTrackerClient::ENodeState, MasterState);

    //! State of node at scheduler.
    DEFINE_BYVAL_RW_PROPERTY(ENodeState, SchedulerState);

    //! Error of node registration.
    DEFINE_BYVAL_RW_PROPERTY(TError, RegistrationError);

    //! Is |true| iff heartbeat from this node is being processed at the moment.
    DEFINE_BYVAL_RW_PROPERTY(bool, HasOngoingHeartbeat);

    //! Complexity of processing heartbeat.
    DEFINE_BYVAL_RW_PROPERTY(int, SchedulingHeartbeatComplexity);

    //! Stores the time when resources overcommit has detected.
    DEFINE_BYVAL_RW_PROPERTY(TInstant, ResourcesOvercommitStartTime);

    //! Is |true| iff the node must be unregistered but it also has an ongoing
    //! heartbeat so the unregistration has to be postponed until the heartbeat processing
    //! is complete.
    DEFINE_BYVAL_RW_PROPERTY(bool, HasPendingUnregistration);

    //! Has value iff the node jobs must be aborted but node also has an ongoing
    //! heartbeat so jobs abortion has to be postponed until the heartbeat processing
    //! is complete.
    DEFINE_BYVAL_RW_PROPERTY(std::optional<EAbortReason>, PendingAllocationsAbortionReason);

    DEFINE_BYVAL_RO_PROPERTY(double, IOWeight);

    //! Mark that node has large job archivation queues.
    DEFINE_BYVAL_RW_PROPERTY(bool, JobReporterQueueIsTooLarge);

    DEFINE_BYVAL_RW_PROPERTY(TScheduleAllocationsStatistics, LastPreemptiveHeartbeatStatistics);
    DEFINE_BYVAL_RW_PROPERTY(TScheduleAllocationsStatistics, LastNonPreemptiveHeartbeatStatistics);

    DEFINE_BYVAL_RW_PROPERTY(std::optional<std::string>, InfinibandCluster);

    DEFINE_BYVAL_RW_PROPERTY(NYTree::IAttributeDictionaryPtr, SchedulingOptions);

    DEFINE_BYVAL_RW_PROPERTY(TMatchingTreeCookie, MatchingTreeCookie);

    DEFINE_BYREF_RW_PROPERTY(TJobResources, LastHeartbeatUnscheduledResources);
    DEFINE_BYREF_RW_PROPERTY(TInstant, LastHeartbeatTime);

public:
    TExecNode(
        NNodeTrackerClient::TNodeId id,
        NNodeTrackerClient::TNodeDescriptor nodeDescriptor,
        ENodeState state);

    const std::string& GetDefaultAddress() const;
    const NNodeTrackerClient::TAddressMap& GetAddresses() const;

    //! Checks if the node can handle jobs demanding a certain #tag.
    bool CanSchedule(const TSchedulingTagFilter& filter) const;

    //! Constructs a descriptor containing the current snapshot of node's state.
    TExecNodeDescriptorPtr BuildExecDescriptor() const;

    //! Set the node's IO weight.
    void SetIOWeights(const THashMap<TString, double>& mediumToWeight);

    void SetTags(TBooleanFormulaTags tags);

    void BuildAttributes(NYTree::TFluentMap fluent);
};

DEFINE_REFCOUNTED_TYPE(TExecNode)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
