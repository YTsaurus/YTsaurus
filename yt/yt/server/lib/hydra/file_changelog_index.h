#pragma once

#include "private.h"

#include <yt/yt/server/lib/io/io_engine.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/memory_usage_tracker.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

#include <atomic>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EFileChangelogIndexOpenResult,
    (MissingCreated)
    (ExistingRecreatedBrokenIndexHeader)
    (ExistingRecreatedBrokenSegmentHeader)
    (ExistingRecreatedBrokenSegmentChecksum)
    (ExistingOpened)
    (ExistingTruncatedBrokenSegment)
);

//! Maintains an in-memory index of all changelog records.
/*!
 *  The instances are single-threaded unless noted otherwise.
 */
class TFileChangelogIndex
    : public TRefCounted
{
public:
    TFileChangelogIndex(
        NIO::IIOEnginePtr ioEngine,
        IMemoryUsageTrackerPtr memoryUsageTracker,
        TString fileName,
        TFileChangelogConfigPtr config,
        EWorkloadCategory workloadCategory);

    EFileChangelogIndexOpenResult Open();
    void Create();

    void Close();

    //! Returns the number of records in the index.
    /*
     *  \note
     *  Thread affinity: any
     */
    int GetRecordCount() const;

    //! Returns the number of records that are known to be flushed to data file.
    /*
     *  \note
     *  Thread affinity: any
     */
    int GetFlushedDataRecordCount() const;

    //! Sets the number of records that are flushed to data file.
    void SetFlushedDataRecordCount(int count);

    //! Returns the range of data file containing the requested record
    //! (including header and padding).
    //! Fails if changelog does not contain the requested record.
    /*
     *  \note
     *  Thread affinity: any
     */
    std::pair<i64, i64> GetRecordRange(int recordIndex) const;

    //! Returns the range of data file containing the requested records or
    //! |std::nullopt| if changelog does not contain any of the records requested.
    /*!
     *  \param maxBytes
     *  Limits the length of the range (including record headers and padding).
     *
     *  \note
     *  Thread affinity: any
     */
    std::optional<std::pair<i64, i64>> FindRecordsRange(
        int firstRecordIndex,
        int maxRecords,
        i64 maxBytes) const;

    //! Appends a new record into the index.
    /*!
     *  \param recordIndex
     *  Passed for validation only. The index expects the records to be appended without gaps.
     *
     *  \param range
     *  Represents the range of data file containing the record (including header and padding).
     */
    void AppendRecord(int recordIndex, std::pair<i64, i64> range);

    //! Asynchronously flushes the index file writing down newly appended records.
    //! Returns immediately.
    //! Can only be called if #CanFlush is true.
    void AsyncFlush();

    //! Synchronously flushes the index file.
    //! Only returns when all previously appended records are flushed.
    //! Can be called regardless of whether any asynchronous flush is currently in progress or not.
    void SyncFlush();

    //! Checks if no asynchronous flush is currently in progress.
    bool CanFlush() const;

private:
    const NIO::IIOEnginePtr IOEngine_;
    const TString FileName_;
    const TFileChangelogConfigPtr Config_;
    EWorkloadCategory WorkloadCategory_;
    TWorkloadDescriptor WorkloadDescriptor_;

    const NLogging::TLogger Logger;

    TMemoryUsageTrackerGuard MemoryUsageTrackerGuard_;

    NIO::TIOEngineHandlePtr Handle_;
    i64 IndexFilePosition_ = 0;

    std::atomic<int> RecordCount_ = 0;
    std::atomic<int> FlushedDataRecordCount_ = 0;
    int FlushedIndexRecordCount_ = 0;
    i64 DataFileLength_ = -1;

    std::atomic<bool> Flushing_ = false;
    TFuture<void> FlushFuture_ = VoidFuture;

    struct TRecord
    {
        i64 Offset = -1;
    };

    static constexpr int RecordsPerChunk = 10240;
    using TChunk = TSharedMutableRange<TRecord>;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, ChunkListLock_);
    std::vector<TChunk> ChunkList_;


    void Clear();
    TChunk AllocateChunk();
    static std::pair<int, int> GetRecordChunkIndexes(int recordIndex);
    i64 GetRecordOffset(int recordIndex) const;
};

DEFINE_REFCOUNTED_TYPE(TFileChangelogIndex)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
