#include <yt/yt/server/lib/chunk_pools/public.h>

#include <yt/yt/server/lib/controller_agent/structs.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_pools/chunk_stripe.h>

#pragma once

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <>
void PrintTo(const TIntrusivePtr<NChunkClient::TInputChunk>& chunk, std::ostream* os);

////////////////////////////////////////////////////////////////////////////////

namespace NChunkPools {

////////////////////////////////////////////////////////////////////////////////

NLogging::TLogger GetTestLogger();

////////////////////////////////////////////////////////////////////////////////

NControllerAgent::TCompletedJobSummary SummaryWithSplitJobCount(
    TChunkStripeListPtr stripeList,
    int splitJobCount,
    std::optional<int> readRowCount = {});

////////////////////////////////////////////////////////////////////////////////

void CheckUnsuccessfulSplitMarksJobUnsplittable(IPersistentChunkPoolPtr chunkPool);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT
