#include "private.h"
#include "profiler.h"

namespace NYT::NQueryTracker {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TActiveQueriesProfilingCounter::TActiveQueriesProfilingCounter(const TProfiler& profiler)
    : ActiveQueries(profiler.Gauge("/active_queries"))
{ }

TStateTimeProfilingCounter::TStateTimeProfilingCounter(const TProfiler& profiler)
    : StateTime(profiler.WithDefaultDisabled().TimeGaugeSummary("/state_time", ESummaryPolicy::Max | ESummaryPolicy::Avg))
{ }

TProfilingTags ProfilingTagsFromActiveQueryRecord(NQueryTrackerClient::NRecords::TActiveQuery record)
{
    return TProfilingTags{
        .State = record.State,
        .Engine = record.Engine,
        .AssignedTracker = record.AssignedTracker.value_or(NoneQueryTracker),
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker

////////////////////////////////////////////////////////////////////////////////

size_t THash<NYT::NQueryTracker::TProfilingTags>::operator()(const NYT::NQueryTracker::TProfilingTags& tag) const
{
    return MultiHash(tag.State, tag.Engine, tag.AssignedTracker);
}

////////////////////////////////////////////////////////////////////////////////
