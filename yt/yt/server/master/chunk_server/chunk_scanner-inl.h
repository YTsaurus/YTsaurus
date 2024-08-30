#ifndef CHUNK_SCANNER_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk_scanner.h"
// For the sake of sane code completion.
#include "chunk_scanner.h"
#endif

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/core/rpc/dispatcher.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

template <class TPayload>
void TChunkScannerWithPayload<TPayload>::Stop(int shardIndex)
{
    TBase::Stop(shardIndex);

    // If there are no more active shards, we clear the queue to drop all the
    // ephemeral references. Since queue may be huge, destruction is offloaded
    // into separate thread. Otherwise, queue is not changed. Note that queue
    // now may contain chunks from non-active shards. We have to properly handle
    // them during chunk dequeueing.
    if (ActiveShardIndices_.none()) {
        std::queue<TQueueEntry> queue;
        std::swap(Queue_, queue);
        NRpc::TDispatcher::Get()->GetHeavyInvoker()->Invoke(
            BIND([queue = std::move(queue)] { Y_UNUSED(queue); }));
    }
}

template <class TPayload>
bool TChunkScannerWithPayload<TPayload>::EnqueueChunk(TQueuedChunk chunk, std::optional<TCpuDuration> delay)
{
    if (!IsRelevant(GetChunk(chunk))) {
        return false;
    }

    if (GetScanFlag(GetChunk(chunk))) {
        return false;
    }
    SetScanFlag(GetChunk(chunk));

    auto instant = GetCpuInstant();
    RequeueDelayedChunks(instant);

    TQueueEntry queueEntry;
    if constexpr (WithPayload) {
        queueEntry = {
            .Chunk = NObjectServer::TEphemeralObjectPtr<TChunk>(chunk.Chunk),
            .Payload = std::move(chunk.Payload),
            .Instant = instant,
        };
    } else {
        queueEntry = {
            .Chunk = NObjectServer::TEphemeralObjectPtr<TChunk>(chunk),
            .Instant = instant,
        };
    }

    if (!delay) {
        Queue_.push(std::move(queueEntry));
    } else {
        delay = std::min(*delay, MaxEnqueueChunkDelay_);
        DelayedQueue_.push({
            .QueueEntry = std::move(queueEntry),
            .Deadline = instant + *delay,
        });
    }

    return true;
}

template <class TPayload>
auto TChunkScannerWithPayload<TPayload>::DequeueChunk() -> TQueuedChunk
{
    if (TBase::HasUnscannedChunk()) {
        return WithoutPayload(TGlobalChunkScanner::DequeueChunk());
    }

    RequeueDelayedChunks(GetCpuInstant());

    if (Queue_.empty()) {
        return None();
    }

    TQueuedChunk front;
    TChunk* chunk;
    if constexpr (WithPayload) {
        front = {
            .Chunk = Queue_.front().Chunk.Get(),
            .Payload = std::move(Queue_.front().Payload)
        };
        chunk = front.Chunk;
    } else {
        front = Queue_.front().Chunk.Get();
        chunk = front;
    }
    Queue_.pop();

    auto relevant = IsRelevant(chunk);
    if (IsObjectAlive(chunk)) {
        if (relevant) {
            YT_ASSERT(GetScanFlag(chunk));
            ClearScanFlag(chunk);
        } else {
            YT_ASSERT(!GetScanFlag(chunk));
        }
    }

    if (relevant) {
        return front;
    }

    return None();
}

template <class TPayload>
void TChunkScannerWithPayload<TPayload>::RequeueDelayedChunks(NProfiling::TCpuInstant deadline)
{
    while (!DelayedQueue_.empty() && DelayedQueue_.front().Deadline < deadline) {
        auto queueEntry = std::move(DelayedQueue_.front().QueueEntry);
        queueEntry.Instant = DelayedQueue_.front().Deadline;
        DelayedQueue_.pop();
        Queue_.push(std::move(queueEntry));
    }
}

template <class TPayload>
bool TChunkScannerWithPayload<TPayload>::HasUnscannedChunk(NProfiling::TCpuInstant deadline) const
{
    if (TBase::HasUnscannedChunk(deadline)) {
        return true;
    }

    if (!Queue_.empty()) {
        return Queue_.front().Instant < deadline;
    }

    if (!DelayedQueue_.empty()) {
        return DelayedQueue_.front().Deadline < deadline;
    }

    return false;
}

template <class TPayload>
int TChunkScannerWithPayload<TPayload>::GetQueueSize() const
{
    return std::ssize(Queue_) + std::ssize(DelayedQueue_) + TBase::GetQueueSize();
}

template <class TPayload>
constexpr auto TChunkScannerWithPayload<TPayload>::None() noexcept -> TQueuedChunk
{
    return WithoutPayload(nullptr);
}

template <class TPayload>
constexpr auto TChunkScannerWithPayload<TPayload>::WithoutPayload(TChunk* chunk) noexcept -> TQueuedChunk
{
    if constexpr (WithPayload) {
        return {chunk, TPayload{}};
    } else {
        return chunk;
    }
}

template <class TPayload>
constexpr TChunk* TChunkScannerWithPayload<TPayload>::GetChunk(const TQueuedChunk& chunk) noexcept
{
    if constexpr (WithPayload) {
        return chunk.Chunk;
    } else {
        return chunk;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
