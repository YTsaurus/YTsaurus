#include "file_changelog_index.h"
#include "format.h"
#include "config.h"
#include "private.h"

#include <yt/yt/server/lib/io/io_engine.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/checksum.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NYT::NHydra {

using namespace NConcurrency;
using namespace NThreading;
using namespace NIO;

////////////////////////////////////////////////////////////////////////////////

struct TFileChangelogIndexScratchTag
{ };

struct TFileChangelogIndexRecordsTag
{ };

////////////////////////////////////////////////////////////////////////////////

TFileChangelogIndex::TFileChangelogIndex(
    IIOEnginePtr ioEngine,
    IMemoryUsageTrackerPtr memoryUsageTracker,
    TString fileName,
    TFileChangelogConfigPtr config,
    EWorkloadCategory workloadCategory)
    : IOEngine_(std::move(ioEngine))
    , FileName_(std::move(fileName))
    , Config_(std::move(config))
    , WorkloadCategory_(workloadCategory)
    , WorkloadDescriptor_(workloadCategory, 0, {}, {}, {}, "TFileChangelogIndex", {})
    , Logger(HydraLogger.WithTag("Path: %v", FileName_))
    , MemoryUsageTrackerGuard_(TMemoryUsageTrackerGuard::Acquire(memoryUsageTracker, 0))
{ }

EFileChangelogIndexOpenResult TFileChangelogIndex::Open()
{
    Clear();

    if (!NFS::Exists(FileName_)) {
        Create();
        return EFileChangelogIndexOpenResult::MissingCreated;
    }

    auto handle = WaitFor(IOEngine_->Open({.Path = FileName_, .Mode = RdWr}, WorkloadDescriptor_, {}))
        .ValueOrThrow();

    auto recreate = [&] (EFileChangelogIndexOpenResult result) {
        if (handle) {
            WaitFor(IOEngine_->Close({.Handle = handle}, WorkloadDescriptor_, {}))
                .ThrowOnError();
        }

        Create();
        return result;
    };

    auto buffer = WaitFor(
        IOEngine_->Read(
            {{.Handle = handle, .Offset = 0, .Size = handle->GetLength()}},
            WorkloadDescriptor_,
            GetRefCountedTypeCookie<TFileChangelogIndexScratchTag>(),
            {}))
        .ValueOrThrow()
        .OutputBuffers[0];

    const auto* current = buffer.Begin();
    const auto* end = buffer.End();

    if (current + sizeof(TChangelogIndexHeader) > end) {
        YT_LOG_WARNING("Changelog index file is too short to fit header, recreating (Length: %v)",
            buffer.Size());
        return recreate(EFileChangelogIndexOpenResult::ExistingRecreatedBrokenIndexHeader);
    }

    const auto* header = reinterpret_cast<const TChangelogIndexHeader*>(current);
    if (header->Signature != TChangelogIndexHeader::ExpectedSignature) {
        YT_LOG_WARNING("Changelog index file signature mismatch, recreating (ExpectedSignature: %x, ActualSignature: %x)",
            TChangelogIndexHeader::ExpectedSignature,
            header->Signature);
        return recreate(EFileChangelogIndexOpenResult::ExistingRecreatedBrokenIndexHeader);
    }

    current += sizeof(TChangelogIndexHeader);

    auto truncate = [&] (EFileChangelogIndexOpenResult result) {
        IndexFilePosition_ = current - buffer.Begin();

        WaitFor(IOEngine_->Resize({.Handle = handle, .Size = IndexFilePosition_}, WorkloadDescriptor_, {}))
            .ThrowOnError();

        WaitFor(IOEngine_->FlushFile({.Handle = handle, .Mode = EFlushFileMode::All}, WorkloadDescriptor_, {}))
            .ThrowOnError();

        YT_LOG_DEBUG("Changelog index file truncated (RecordCount: %v, IndexFilePosition: %v, DataFileLength: %v)",
            GetRecordCount(),
            IndexFilePosition_,
            DataFileLength_);

        Handle_ = std::move(handle);
        return result;
    };

    auto finish = [&] {
        IndexFilePosition_ = current - buffer.Begin();

        YT_LOG_DEBUG("Changelog index file opened (RecordCount: %v, IndexFilePosition: %v, DataFileLength: %v)",
            GetRecordCount(),
            IndexFilePosition_,
            DataFileLength_);

        Handle_ = std::move(handle);
        return EFileChangelogIndexOpenResult::ExistingOpened;
    };

    int recordIndex = 0;

    while (current != end) {
        if (current + sizeof(TChangelogIndexSegmentHeader) > end) {
            YT_LOG_WARNING("Changelog index file is too short to fit segment header, truncating (IndexFilePosition: %v, Length: %v)",
                current - buffer.Begin(),
                buffer.Size());
            return truncate(EFileChangelogIndexOpenResult::ExistingTruncatedBrokenSegment);
        }

        const auto* segmentHeader = reinterpret_cast<const TChangelogIndexSegmentHeader*>(current);
        if (segmentHeader->RecordCount < 0) {
            YT_LOG_ALERT("Negative number of records in changelog index segment, recreating (IndexFilePosition: %v)",
                current - buffer.Begin());
            return recreate(EFileChangelogIndexOpenResult::ExistingRecreatedBrokenSegmentHeader);
        }

        const auto* segmentEnd = current + sizeof(TChangelogIndexSegmentHeader) + sizeof(TChangelogIndexRecord) * segmentHeader->RecordCount;
        if (segmentEnd > end) {
            YT_LOG_WARNING("Changelog index file is too short to fit full segment, truncating (IndexFilePosition: %v, Length: %v, RecordCount: %v)",
                current - buffer.Begin(),
                buffer.Size(),
                segmentHeader->RecordCount);
            return truncate(EFileChangelogIndexOpenResult::ExistingTruncatedBrokenSegment);
        }

        if (GetChecksum(buffer.Slice(current + sizeof(ui64), segmentEnd)) != segmentHeader->Checksum) {
            YT_LOG_ALERT("Invalid changelog index segment checksum, recreating (IndexFilePosition: %v)",
                current - buffer.Begin());
            return recreate(EFileChangelogIndexOpenResult::ExistingRecreatedBrokenSegmentChecksum);
        }

        current += sizeof(TChangelogIndexSegmentHeader);
        while (current != segmentEnd) {
            const auto* record = reinterpret_cast<const TChangelogIndexRecord*>(current);
            YT_VERIFY(DataFileLength_ < 0 || DataFileLength_ == record->Offset);
            YT_VERIFY(record->Length >= 0);
            AppendRecord(recordIndex, {record->Offset, record->Offset + record->Length});
            ++recordIndex;
            current += sizeof(TChangelogIndexRecord);
        }

        FlushedIndexRecordCount_ = GetRecordCount();
    }

    return finish();
}

void TFileChangelogIndex::Create()
{
    Clear();

    auto handle = WaitFor(IOEngine_->Open({.Path = FileName_, .Mode = RdWr | CreateAlways}, WorkloadDescriptor_, {}))
        .ValueOrThrow();

    auto buffer = TSharedMutableRef::Allocate<TFileChangelogIndexScratchTag>(sizeof(TChangelogIndexHeader), {.InitializeStorage = false});

    auto* header = reinterpret_cast<TChangelogIndexHeader*>(buffer.Begin());
    header->Signature = TChangelogIndexHeader::ExpectedSignature;

    WaitFor(IOEngine_->Write({
            .Handle = handle,
            .Offset = 0,
            .Buffers = {std::move(buffer)}
        },
        WorkloadDescriptor_,
        {}))
        .ThrowOnError();

    Handle_ = std::move(handle);
    IndexFilePosition_ = sizeof(TChangelogIndexHeader);

    YT_LOG_DEBUG("Changelog index file created");
}

void TFileChangelogIndex::Close()
{
    WaitFor(FlushFuture_)
        .ThrowOnError();

    if (auto handle = std::exchange(Handle_, nullptr)) {
        WaitFor(IOEngine_->Close({.Handle = handle, .Flush = true}, WorkloadDescriptor_, {}))
            .ThrowOnError();
    }
}

int TFileChangelogIndex::GetRecordCount() const
{
    return RecordCount_.load();
}

int TFileChangelogIndex::GetFlushedDataRecordCount() const
{
    return FlushedDataRecordCount_.load();
}

void TFileChangelogIndex::SetFlushedDataRecordCount(int count)
{
    YT_VERIFY(count >= FlushedDataRecordCount_.load());
    YT_VERIFY(count >= FlushedIndexRecordCount_);

    FlushedDataRecordCount_.store(count);
}

std::pair<i64, i64> TFileChangelogIndex::GetRecordRange(int recordIndex) const
{
    auto guard = ReaderGuard(ChunkListLock_);

    YT_VERIFY(recordIndex < GetRecordCount());

    return {
        GetRecordOffset(recordIndex),
        GetRecordOffset(recordIndex + 1)
    };
}

std::optional<std::pair<i64, i64>> TFileChangelogIndex::FindRecordsRange(
    int firstRecordIndex,
    int maxRecords,
    i64 maxBytes) const
{
    auto guard = ReaderGuard(ChunkListLock_);

    auto recordCount = GetRecordCount();

    YT_VERIFY(firstRecordIndex >= 0);
    YT_VERIFY(recordCount == 0 || DataFileLength_ >= 0);

    if (firstRecordIndex >= recordCount) {
        return std::nullopt;
    }

    auto startOffset = GetRecordOffset(firstRecordIndex);
    i64 endOffset = -1;
    for (int endRecordIndex = firstRecordIndex; endRecordIndex <= recordCount; ++endRecordIndex) {
        endOffset = GetRecordOffset(endRecordIndex);
        auto totalRecords = endRecordIndex - firstRecordIndex;
        auto totalBytes = endOffset - startOffset;
        if (totalRecords >= maxRecords) {
            break;
        }
        if (totalBytes >= maxBytes && totalRecords > 0) {
            break;
        }
    }
    return std::make_pair(startOffset, endOffset);
}

void TFileChangelogIndex::AppendRecord(int recordIndex, std::pair<i64, i64> range)
{
    YT_VERIFY(RecordCount_.load() == recordIndex);
    YT_VERIFY(DataFileLength_ < 0 || DataFileLength_ == range.first);

    auto [chunkIndex, perChunkIndex] = GetRecordChunkIndexes(recordIndex);

    if (perChunkIndex == 0) {
        auto chunk = AllocateChunk();
        auto guard = WriterGuard(ChunkListLock_);
        ChunkList_.push_back(std::move(chunk));
    }

    // NB: no synchronization needed since we're the only writer.
    auto& chunk = ChunkList_[chunkIndex];
    chunk[perChunkIndex].Offset = range.first;

    DataFileLength_ = range.second;
    ++RecordCount_;
}

void TFileChangelogIndex::AsyncFlush()
{
    auto flushRecordCount = FlushedDataRecordCount_.load() - FlushedIndexRecordCount_;
    if (flushRecordCount == 0) {
        return;
    }

    auto size = sizeof(TChangelogIndexSegmentHeader) + sizeof(TChangelogIndexRecord) * flushRecordCount;
    auto buffer = TSharedMutableRef::Allocate<TFileChangelogIndexScratchTag>(size, {.InitializeStorage = false});

    auto* current = buffer.Begin();
    auto* header = reinterpret_cast<TChangelogIndexSegmentHeader*>(current);
    header->RecordCount = flushRecordCount;
    header->Padding = 0;
    current += sizeof(TChangelogIndexSegmentHeader);

    for (int index = FlushedIndexRecordCount_; index < FlushedIndexRecordCount_ + flushRecordCount; ++index) {
        auto currentOffset = GetRecordOffset(index);
        auto nextOffset = GetRecordOffset(index + 1);
        auto* record = reinterpret_cast<TChangelogIndexRecord*>(current);
        record->Offset = currentOffset;
        record->Length = nextOffset - currentOffset;
        current += sizeof(TChangelogIndexRecord);
    }

    header->Checksum = GetChecksum(buffer.Slice(sizeof(ui64), size));

    YT_LOG_DEBUG("Started flushing changelog file index segment (FirstRecordIndex: %v, RecordCount: %v, IndexFilePosition: %v)",
        FlushedIndexRecordCount_,
        flushRecordCount,
        IndexFilePosition_);

    YT_VERIFY(!Flushing_.exchange(true));

    auto currentPosition = IndexFilePosition_;
    IndexFilePosition_ += size;
    FlushedIndexRecordCount_ += flushRecordCount;

    FlushFuture_ = IOEngine_->Write({
            .Handle = Handle_,
            .Offset = currentPosition,
            .Buffers = {std::move(buffer)},
            .Flush = true,
        },
        WorkloadDescriptor_,
        {})
        .Apply(BIND([=, this, this_ = MakeStrong(this)] {
            YT_VERIFY(Flushing_.exchange(false));
            YT_LOG_DEBUG("Finished flushing changelog file index segment");
        }));
}

void TFileChangelogIndex::SyncFlush()
{
    WaitFor(FlushFuture_)
        .ThrowOnError();
    AsyncFlush();
    WaitFor(FlushFuture_)
        .ThrowOnError();
}

bool TFileChangelogIndex::CanFlush() const
{
    return FlushFuture_.IsSet();
}

void TFileChangelogIndex::Clear()
{
    IndexFilePosition_ = 0;
    RecordCount_.store(0);
    FlushedDataRecordCount_.store(0);
    FlushedIndexRecordCount_ = 0;
    DataFileLength_ = -1;
    MemoryUsageTrackerGuard_.Release();

    {
        auto guard = WriterGuard(ChunkListLock_);
        ChunkList_.clear();
    }
}

TFileChangelogIndex::TChunk TFileChangelogIndex::AllocateChunk()
{
    auto size = sizeof(TRecord) * RecordsPerChunk;
    auto buffer = TSharedMutableRef::Allocate(size, {.InitializeStorage = false});
    auto holder = MakeSharedRangeHolder(buffer);
    auto chunk = TChunk(reinterpret_cast<TRecord*>(buffer.Begin()), RecordsPerChunk, std::move(holder));
    std::uninitialized_default_construct(chunk.Begin(), chunk.End());
    MemoryUsageTrackerGuard_.IncrementSize(std::ssize(buffer));
    return chunk;
}

std::pair<int, int> TFileChangelogIndex::GetRecordChunkIndexes(int recordIndex)
{
    // NB: "unsigned" produces bit shift and mask.
    return {
        static_cast<unsigned>(recordIndex) / RecordsPerChunk,
        static_cast<unsigned>(recordIndex) % RecordsPerChunk
    };
}

i64 TFileChangelogIndex::GetRecordOffset(int recordIndex) const
{
    auto recordCount = GetRecordCount();
    if (recordIndex == recordCount) {
        return DataFileLength_;
    }

    YT_VERIFY(recordIndex >= 0 && recordIndex < recordCount);
    auto [chunkIndex, perChunkIndex] = GetRecordChunkIndexes(recordIndex);
    const auto& chunk = ChunkList_[chunkIndex];
    return chunk[perChunkIndex].Offset;
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
