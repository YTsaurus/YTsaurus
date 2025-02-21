#include "workload.h"

#include <yt/yt_proto/yt/client/misc/proto/workload.pb.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/fair_share_thread_pool.h>

#include <yt/yt/core/rpc/service.h>
#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

static const i64 CategoryPriorityFactor = (i64) 1 << 56;
static const i64 BandPriorityFactor = (i64) 1 << 48;

////////////////////////////////////////////////////////////////////////////////

TWorkloadDescriptor::TWorkloadDescriptor(
    EWorkloadCategory category,
    int band,
    TInstant instant,
    std::vector<TString> annotations,
    std::optional<NConcurrency::TFairShareThreadPoolTag> compressionFairShareTag,
    std::optional<TString> diskFairShareBucketTag,
    std::optional<double> diskFairShareBucketWeight)
    : Category(category)
    , Band(band)
    , Instant(instant)
    , Annotations(std::move(annotations))
    , CompressionFairShareTag(std::move(compressionFairShareTag))
    , DiskFairShareBucketTag(std::move(diskFairShareBucketTag))
    , DiskFairShareBucketWeight(diskFairShareBucketWeight)
{ }

TWorkloadDescriptor TWorkloadDescriptor::SetCurrentInstant() const
{
    return TWorkloadDescriptor(Category, Band, TInstant::Now(), Annotations, CompressionFairShareTag, DiskFairShareBucketTag, DiskFairShareBucketWeight);
}

i64 TWorkloadDescriptor::GetPriority() const
{
    auto priority = GetBasicPriority(Category) + BandPriorityFactor * Band;
    if (Category == EWorkloadCategory::UserBatch) {
        priority -= Instant.MilliSeconds();
    }
    return priority;
}

////////////////////////////////////////////////////////////////////////////////

i64 GetBasicPriority(EWorkloadCategory category)
{
    switch (category) {
        case EWorkloadCategory::Idle:
            return 0;

        case EWorkloadCategory::SystemReplication:
        case EWorkloadCategory::SystemMerge:
        case EWorkloadCategory::SystemReincarnation:
        case EWorkloadCategory::SystemTabletCompaction:
        case EWorkloadCategory::SystemTabletPartitioning:
        case EWorkloadCategory::SystemTabletPreload:
        case EWorkloadCategory::SystemTabletReplication:
        case EWorkloadCategory::SystemTabletStoreFlush:
        case EWorkloadCategory::SystemArtifactCacheDownload:
        case EWorkloadCategory::UserBatch:
            return CategoryPriorityFactor * 1;

        case EWorkloadCategory::SystemRepair:
        case EWorkloadCategory::SystemTabletSnapshot:
            return CategoryPriorityFactor * 2;

        case EWorkloadCategory::UserInteractive:
        case EWorkloadCategory::SystemTabletRecovery:
            return CategoryPriorityFactor * 3;

        case EWorkloadCategory::UserRealtime:
        case EWorkloadCategory::SystemTabletLogging:
            return CategoryPriorityFactor * 4;

        // Graceful fallback for possible future extensions of categories.
        default:
            return 0;
    }
}

double GetBasicWeight(EWorkloadCategory category)
{
    return std::max(GetBasicPriority(category) / CategoryPriorityFactor + 1.0, 1.0);
}

IInvokerPtr GetCompressionInvoker(const TWorkloadDescriptor& workloadDescriptor)
{
    if (workloadDescriptor.CompressionFairShareTag) {
        return NRpc::TDispatcher::Get()->GetFairShareCompressionThreadPool()
            ->GetInvoker(*workloadDescriptor.CompressionFairShareTag);
    } else {
        return CreateFixedPriorityInvoker(
            NRpc::TDispatcher::Get()->GetPrioritizedCompressionPoolInvoker(),
            workloadDescriptor.GetPriority());
    }
}

struct TSerializableWorkloadDescriptor
    : public TWorkloadDescriptor
    , public TYsonStructLite
{
    REGISTER_YSON_STRUCT_LITE(TSerializableWorkloadDescriptor);

    static void Register(TRegistrar registrar)
    {
        registrar.BaseClassParameter("category", &TWorkloadDescriptor::Category);
        registrar.BaseClassParameter("band", &TWorkloadDescriptor::Band)
            .Default(0);
        registrar.BaseClassParameter("disk_fair_share_bucket_tag", &TWorkloadDescriptor::DiskFairShareBucketTag)
            .Optional();
        registrar.BaseClassParameter("disk_fair_share_bucket_weight", &TWorkloadDescriptor::DiskFairShareBucketWeight)
            .Optional();
        registrar.BaseClassParameter("annotations", &TWorkloadDescriptor::Annotations)
            .Default();
    }
};

void Serialize(const TWorkloadDescriptor& descriptor, IYsonConsumer* consumer)
{
    TSerializableWorkloadDescriptor wrapper;
    static_cast<TWorkloadDescriptor&>(wrapper) = descriptor;
    Serialize(static_cast<const TYsonStructLite&>(wrapper), consumer);
}

void Deserialize(TWorkloadDescriptor& descriptor, INodePtr node)
{
    TSerializableWorkloadDescriptor wrapper;
    Deserialize(static_cast<TYsonStructLite&>(wrapper), node);
    descriptor = static_cast<TWorkloadDescriptor&>(wrapper);
}

void Deserialize(TWorkloadDescriptor& descriptor, NYson::TYsonPullParserCursor* cursor)
{
    TSerializableWorkloadDescriptor wrapper;
    Deserialize(static_cast<TYsonStructLite&>(wrapper), cursor);
    descriptor = static_cast<TWorkloadDescriptor&>(wrapper);
}

void ToProto(NYT::NProto::TWorkloadDescriptor* protoDescriptor, const TWorkloadDescriptor& descriptor)
{
    protoDescriptor->set_category(static_cast<int>(descriptor.Category));
    protoDescriptor->set_band(descriptor.Band);
    protoDescriptor->set_instant(ToProto<i64>(descriptor.Instant));
    ToProto(protoDescriptor->mutable_annotations(), descriptor.Annotations);
    if (descriptor.DiskFairShareBucketTag) {
        protoDescriptor->set_disk_fair_share_bucket_tag(*descriptor.DiskFairShareBucketTag);
    }
    if (descriptor.DiskFairShareBucketWeight) {
        protoDescriptor->set_disk_fair_share_bucket_weight(*descriptor.DiskFairShareBucketWeight);
    }
}

void FromProto(TWorkloadDescriptor* descriptor, const NYT::NProto::TWorkloadDescriptor& protoDescriptor)
{
    descriptor->Category = EWorkloadCategory(protoDescriptor.category());
    descriptor->Band = protoDescriptor.band();
    descriptor->Instant = FromProto<TInstant>(protoDescriptor.instant());
    FromProto(&descriptor->Annotations, protoDescriptor.annotations());
    if (protoDescriptor.Hasdisk_fair_share_bucket_tag()) {
        descriptor->DiskFairShareBucketTag = protoDescriptor.disk_fair_share_bucket_tag();
    }
    if (protoDescriptor.Hasdisk_fair_share_bucket_weight()) {
        descriptor->DiskFairShareBucketWeight = protoDescriptor.disk_fair_share_bucket_weight();
    }
}

void FormatValue(
    TStringBuilderBase* builder,
    const TWorkloadDescriptor& descriptor,
    TStringBuf /*format*/)
{
    builder->AppendFormat("%v:%v",
        descriptor.Category,
        descriptor.Band);
    if (descriptor.Instant != TInstant::Zero()) {
        builder->AppendFormat(":%v",
            descriptor.Instant);
    }
    if (!descriptor.Annotations.empty()) {
        builder->AppendString(":{");
        for (size_t index = 0; index < descriptor.Annotations.size(); ++index) {
            builder->AppendString(descriptor.Annotations[index]);
            if (index != descriptor.Annotations.size() - 1) {
                builder->AppendString(", ");
            }
        }
        builder->AppendChar('}');
    }
    if (descriptor.DiskFairShareBucketTag) {
        builder->AppendFormat(":%v", *descriptor.DiskFairShareBucketTag);
    }
    if (descriptor.DiskFairShareBucketWeight) {
        builder->AppendFormat(":%v", *descriptor.DiskFairShareBucketWeight);
    }
}

TString ToString(const TWorkloadDescriptor& descriptor)
{
    return ToStringViaBuilder(descriptor);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
