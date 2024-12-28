#ifndef PROFILER_INL_H_
#error "Direct inclusion of this file is not allowed, include profiler.h"
// For the sake of sane code completion.
#include "profiler-inl.h"
#endif

namespace NYT::NQueryTracker {

////////////////////////////////////////////////////////////////////////////////

template<class TProfilingCounter>
TProfilingCounter* GetOrCreateProfilingCounter(
    const NProfiling::TProfiler& profiler,
    const TProfilingTags& profilingTags)
{
    using TCountersMap = NConcurrency::TSyncMap<TProfilingTags, TProfilingCounter>;

    auto [counter, _] = LeakySingleton<TCountersMap>()->FindOrInsert(profilingTags, [&]{
        return TProfilingCounter(profiler
                .WithTag("state", ToString(profilingTags.State))
                .WithTag("engine", ToString(profilingTags.Engine))
                .WithTag("assigned_tracker", profilingTags.AssignedTracker));
    });

    return counter;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
