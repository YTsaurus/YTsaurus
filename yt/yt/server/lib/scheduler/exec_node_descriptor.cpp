#include "exec_node_descriptor.h"

#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/ytlib/scheduler/disk_resources.h>
#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/ytlib/controller_agent/serialize.h>

namespace NYT::NScheduler {

using namespace NNodeTrackerClient;
using namespace NConcurrency;
using namespace NYTree;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

TExecNodeDescriptor::TExecNodeDescriptor(
    NNodeTrackerClient::TNodeId id,
    // TODO(aleksandr.gaev): Remove.
    const std::string& address,
    const NNodeTrackerClient::TAddressMap& addresses,
    const std::optional<std::string>& dataCenter,
    double ioWeight,
    bool online,
    const TJobResources& resourceUsage,
    const TJobResources& resourceLimits,
    const TDiskResources& diskResources,
    const TBooleanFormulaTags& tags,
    const std::optional<std::string>& infinibandCluster,
    IAttributeDictionaryPtr schedulingOptions)
    : Id(id)
    // TODO(aleksandr.gaev): Remove.
    , Address(address)
    , Addresses(addresses)
    , DataCenter(dataCenter)
    , IOWeight(ioWeight)
    , Online(online)
    , ResourceUsage(resourceUsage)
    , ResourceLimits(resourceLimits)
    , DiskResources(diskResources)
    , Tags(tags)
    , InfinibandCluster(std::move(infinibandCluster))
    , SchedulingOptions(std::move(schedulingOptions))
{ }

bool TExecNodeDescriptor::CanSchedule(const TSchedulingTagFilter& filter) const
{
    return Online && ResourceLimits.GetUserSlots() > 0 && (filter.IsEmpty() || filter.CanSchedule(Tags));
}

void TExecNodeDescriptor::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Id);
    // TODO(aleksandr.gaev): Remove.
    Persist(context, Address);
    Persist(context, IOWeight);
    Persist(context, Online);
    Persist(context, ResourceLimits);
    Persist(context, DiskResources);
    Persist(context, Tags);

    // THIS IS MOST LIKELY WRONG. PLEASE VERIFY AND HELP
    if (context.GetVersion() >= ToUnderlying(NControllerAgent::ESnapshotVersion::AddAddressesToJob)) {
        Persist(context, Addresses);
    }
}

void ToProto(NScheduler::NProto::TExecNodeDescriptor* protoDescriptor, const NScheduler::TExecNodeDescriptor& descriptor)
{
    protoDescriptor->set_node_id(ToProto(descriptor.Id));
    // TODO(aleksandr.gaev): Remove.
    protoDescriptor->set_address(ToProto(descriptor.Address));
    ToProto(protoDescriptor->mutable_addresses(), descriptor.Addresses);
    protoDescriptor->set_io_weight(descriptor.IOWeight);
    protoDescriptor->set_online(descriptor.Online);
    ToProto(protoDescriptor->mutable_resource_limits(), descriptor.ResourceLimits);
    ToProto(protoDescriptor->mutable_disk_resources(), descriptor.DiskResources);
    for (const auto& tag : descriptor.Tags.GetSourceTags()) {
        protoDescriptor->add_tags(ToProto(tag));
    }
}

void FromProto(NScheduler::TExecNodeDescriptor* descriptor, const NScheduler::NProto::TExecNodeDescriptor& protoDescriptor)
{
    descriptor->Id = FromProto<NNodeTrackerClient::TNodeId>(protoDescriptor.node_id());
    // TODO(aleksandr.gaev): Remove.
    descriptor->Address = protoDescriptor.address();
    FromProto(&descriptor->Addresses, protoDescriptor.addresses());
    descriptor->IOWeight = protoDescriptor.io_weight();
    descriptor->Online = protoDescriptor.online();
    FromProto(&descriptor->ResourceLimits, protoDescriptor.resource_limits());
    FromProto(&descriptor->DiskResources, protoDescriptor.disk_resources());
    auto tags = FromProto<std::vector<std::string>>(protoDescriptor.tags());
    descriptor->Tags = TBooleanFormulaTags(THashSet<std::string>(tags.begin(), tags.end()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

