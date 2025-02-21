#include "unbuffered_file_changelog.h"

#include <yt/yt/server/lib/hydra_common/file_changelog_index.h>
#include <yt/yt/server/lib/hydra_common/config.h>
#include <yt/yt/server/lib/hydra_common/format.h>
#include <yt/yt/server/lib/hydra_common/private.h>

#include <yt/yt/server/lib/io/io_engine.h>
#include <yt/yt/server/lib/io/chunk_fragment.h>

#include <yt/yt/ytlib/hydra/proto/hydra_manager.pb.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/blob_output.h>
#include <yt/yt/core/misc/checksum.h>
#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/serialize.h>

#include <util/system/align.h>
#include <util/system/flock.h>

namespace NYT::NHydra {

using namespace NHydra::NProto;
using namespace NIO;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto LockBackoffTime = TDuration::MilliSeconds(100);
static constexpr int MaxLockRetries = 100;
static constexpr auto WipeBufferSize = 16_MB;

// TODO(babenko, aleksandr.gaev): better workload category?
static const TWorkloadDescriptor UnbufferedFileChangelogWorkloadDescriptor(
    EWorkloadCategory::UserBatch,
    0,
    {},
    {},
    {},
    "TUnbufferedFileChangelog",
    {});

////////////////////////////////////////////////////////////////////////////////

struct TUnbufferedFileChangelogHeaderTag
{ };

struct TUnbufferedFileChangelogRecordHeadersTag
{ };

struct TUnbufferedFileChangelogWipeTag
{ };

////////////////////////////////////////////////////////////////////////////////

class TUnbufferedFileChangelog
    : public IUnbufferedFileChangelog
{
public:
    TUnbufferedFileChangelog(
        IIOEnginePtr ioEngine,
        IMemoryUsageTrackerPtr memoryUsageTracker,
        TString fileName,
        TFileChangelogConfigPtr config)
        : IOEngine_(std::move(ioEngine))
        , MemoryUsageTracker_(std::move(memoryUsageTracker))
        , FileName_(std::move(fileName))
        , Config_(std::move(config))
        , Logger(HydraLogger.WithTag("Path: %v", FileName_))
        , Index_(MakeIndex(MakeIndexFileName()))
    { }

    const TFileChangelogConfigPtr& GetConfig() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Config_;
    }

    const TString& GetFileName() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return FileName_;
    }


    void Open() override
    {
        Error_.ThrowOnError();
        ValidateNotOpen();

        YT_LOG_DEBUG("Opening changelog");

        try {
            NFS::WrapIOErrors([&] {
                DataFileHandle_ = WaitFor(IOEngine_->Open({.Path = FileName_, .Mode = RdWr | Seq | CloseOnExec}, UnbufferedFileChangelogWorkloadDescriptor, {}))
                    .ValueOrThrow();
                LockDataFile();

                auto headerBufferSize = std::min(static_cast<i64>(MaxChangelogHeaderSize), DataFileHandle_->GetLength());
                auto headerBuffer = WaitFor(
                    IOEngine_->Read(
                        {{.Handle = DataFileHandle_, .Offset = 0, .Size = headerBufferSize}},
                        UnbufferedFileChangelogWorkloadDescriptor,
                        GetRefCountedTypeCookie<TUnbufferedFileChangelogHeaderTag>(),
                        {}))
                    .ValueOrThrow()
                    .OutputBuffers[0];

                if (headerBuffer.Size() < MinChangelogHeaderSize) {
                    THROW_ERROR_EXCEPTION(
                        NHydra::EErrorCode::BrokenChangelog,
                        "Changelog file %v is too small to fit header",
                        FileName_)
                        << TErrorAttribute("size", headerBuffer.Size());
                }

                const auto* header = reinterpret_cast<const TChangelogHeader*>(headerBuffer.Begin());
                switch (header->Signature) {
                    case TChangelogHeader_5::ExpectedSignature:
                        Format_ = EFileChangelogFormat::V5;
                        Uuid_ = header->Uuid;
                        FileHeaderSize_ = sizeof(TChangelogHeader_5);
                        RecordHeaderSize_ = sizeof(TChangelogRecordHeader_5);
                        break;
                    default:
                        THROW_ERROR_EXCEPTION(
                            NHydra::EErrorCode::BrokenChangelog,
                            "Invalid header signature %x in changelog file %v",
                            header->Signature,
                            FileName_);
                }

                if (header->UnusedMustBeMinus2 != -2) {
                    THROW_ERROR_EXCEPTION(
                        NHydra::EErrorCode::BrokenChangelog,
                        "Changelog file %v has probably been already truncated",
                        FileName_);
                }

                SerializedMeta_ = WaitFor(
                    IOEngine_->Read(
                        {{.Handle = DataFileHandle_, .Offset = FileHeaderSize_, .Size = header->MetaSize}},
                        UnbufferedFileChangelogWorkloadDescriptor,
                        GetRefCountedTypeCookie<TUnbufferedFileChangelogHeaderTag>(),
                        {}))
                    .ValueOrThrow()
                    .OutputBuffers[0];
                DeserializeProto(&Meta_, SerializedMeta_);

                Index_->Open();

                auto currentRecordIndex = Index_->GetRecordCount();

                auto currentDataOffset = currentRecordIndex > 0
                    ? Index_->GetRecordRange(currentRecordIndex - 1).second
                    : header->FirstRecordOffset;

                auto dataFileLength = DataFileHandle_->GetLength();

                while (currentDataOffset < dataFileLength) {
                    auto guessedRecordReadSize = GuessRecordReadSize(currentDataOffset, dataFileLength);

                    auto blockSize = std::min(
                        std::max(guessedRecordReadSize, Config_->RecoveryBufferSize),
                        dataFileLength - currentDataOffset);

                    YT_LOG_DEBUG("Recoverying records (CurrentRecordIndex: %v, CurrentDataOffset: %v, DataFileLength: %v, GuessedRecordReadSize: %v, BlockSize: %v)",
                        currentRecordIndex,
                        currentDataOffset,
                        dataFileLength,
                        guessedRecordReadSize,
                        blockSize);

                    auto result = ReadAndParseRange(
                        std::make_pair(currentDataOffset, currentDataOffset + blockSize),
                        Index_->GetRecordCount(),
                        false);

                    if (result.Records.empty()) {
                        YT_LOG_DEBUG("No more records to recover");
                        break;
                    }

                    YT_LOG_DEBUG("Records recovered (RecordCount: %v)",
                        result.Records.size());

                    for (const auto& range : result.RecordRanges) {
                        Index_->AppendRecord(currentRecordIndex, range);
                        ++currentRecordIndex;
                    }

                    currentDataOffset += result.FirstUnparsedOffset;
                }

                 if (currentDataOffset < dataFileLength) {
                    WaitFor(IOEngine_->Resize({.Handle = DataFileHandle_, .Size = currentDataOffset}, UnbufferedFileChangelogWorkloadDescriptor, {}))
                        .ThrowOnError();

                    WaitFor(IOEngine_->FlushFile({.Handle = DataFileHandle_, .Mode = EFlushFileMode::All}, UnbufferedFileChangelogWorkloadDescriptor, {}))
                        .ThrowOnError();

                    YT_LOG_DEBUG("Changelog data file truncated (RecordCount: %v, DataFileLength: %v)",
                        currentRecordIndex,
                        currentDataOffset);
                 } else {
                    YT_LOG_DEBUG("Changelog data does not need truncation (RecordCount: %v, DataFileLength: %v)",
                        currentRecordIndex,
                        dataFileLength);
                 }

                CurrentFileOffset_.store(currentDataOffset);
                RecordCount_.store(currentRecordIndex);

                Index_->SetFlushedDataRecordCount(currentRecordIndex);
                Index_->SyncFlush();
            });
        } catch (const std::exception& ex) {
            Cleanup();
            RecordErrorAndThrow(TError(
                NHydra::EErrorCode::ChangelogIOError,
                "Error opening changelog %v",
                FileName_)
                << ex);
        }

        Open_ = true;

        YT_LOG_DEBUG("Changelog opened (RecordCount: %v, Format: %v)",
            RecordCount_.load(),
            Format_);
    }

    void Close() override
    {
        Error_.ThrowOnError();

        if (!Open_) {
            return;
        }

        auto recordCount = GetRecordCount();
        YT_LOG_DEBUG("Closing changelog (RecordCount: %v)",
            recordCount);

        try {
            NFS::WrapIOErrors([&] {
                WaitFor(IOEngine_->Close({.Handle = std::exchange(DataFileHandle_, nullptr), .Flush = true}, UnbufferedFileChangelogWorkloadDescriptor, {}))
                    .ThrowOnError();

                Index_->SetFlushedDataRecordCount(recordCount);
                Index_->SyncFlush();
                Index_->Close();
            });
        } catch (const std::exception& ex) {
            Cleanup();
            RecordErrorAndThrow(TError("Error closing changelog")
                << ex);
        }

        Cleanup();

        YT_LOG_DEBUG("Changelog closed");
    }

    void Create(
        const TChangelogMeta& meta,
        EFileChangelogFormat format) override
    {
        Error_.ThrowOnError();
        ValidateNotOpen();

        YT_LOG_DEBUG("Creating changelog");

        try {
            Format_ = format;
            Uuid_ = TGuid::Create();
            Meta_ = meta;
            SerializedMeta_ = SerializeProtoToRef(Meta_);
            RecordCount_ = 0;

            CreateDataFile();
            Index_->Create();

            auto fileLength = DataFileHandle_->GetLength();
            CurrentFileSize_ = fileLength;
            CurrentFileOffset_.store(fileLength);
        } catch (const std::exception& ex) {
            Cleanup();
            RecordErrorAndThrow(TError(
                NHydra::EErrorCode::ChangelogIOError,
                "Error creating changelog %v",
                FileName_)
                << ex);
        }

        Open_ = true;

        YT_LOG_DEBUG("Changelog created");
    }

    const TChangelogMeta& GetMeta() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Meta_;
    }

    int GetRecordCount() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return RecordCount_.load();
    }

    i64 GetDataSize() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return CurrentFileOffset_.load();
    }

    double GetWriteAmplificationRatio() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return WriteAmplification_.load(std::memory_order::relaxed);
    }

    bool IsOpen() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Open_.load();
    }


    void Append(
        int firstRecordIndex,
        const std::vector<TSharedRef>& records) override
    {
        Error_.ThrowOnError();
        ValidateOpen();

        switch (Format_) {
            case EFileChangelogFormat::V5:
                DoAppend<TChangelogRecordHeader_5>(firstRecordIndex, records);
                break;
            default:
                YT_ABORT();
        }
    }

    void Flush(bool withIndex) override
    {
        Error_.ThrowOnError();
        ValidateOpen();

        YT_LOG_DEBUG("Started flushing changelog (WithIndex: %v)",
            withIndex);

        try {
            WaitFor(IOEngine_->FlushFile({.Handle = DataFileHandle_, .Mode = EFlushFileMode::Data}, UnbufferedFileChangelogWorkloadDescriptor, {}))
                .ThrowOnError();

            Index_->SetFlushedDataRecordCount(GetRecordCount());

            bool indexFlushed = false;
            if (withIndex) {
                Index_->SyncFlush();
                indexFlushed = true;
            } else if (AppendedDataSizeSinceLastIndexFlush_ >= Config_->IndexFlushSize &&  Index_->CanFlush())
            {
                Index_->AsyncFlush();
                indexFlushed = true;
            }

            if (indexFlushed) {
                AppendedDataSizeSinceLastIndexFlush_ = 0;
            }
        } catch (const std::exception& ex) {
            RecordErrorAndThrow(TError(
                NHydra::EErrorCode::ChangelogIOError,
                "Error flushing changelog %v",
                FileName_)
                << ex);
        }

        YT_LOG_DEBUG("Finished flushing changelog");
    }

    std::vector<TSharedRef> Read(
        int firstRecordIndex,
        int maxRecords,
        i64 maxBytes) override
    {
        Error_.ThrowOnError();
        ValidateOpen();

        switch (Format_) {
            case EFileChangelogFormat::V5:
                return DoRead<TChangelogRecordHeader_5>(firstRecordIndex, maxRecords, maxBytes);
            default:
                YT_ABORT();
        }
    }

    IIOEngine::TReadRequest MakeChunkFragmentReadRequest(
        const TChunkFragmentDescriptor& fragmentDescriptor) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        switch (Format_) {
            case EFileChangelogFormat::V5:
                return DoMakeChunkFragmentReadRequest<TChangelogRecordHeader_5>(fragmentDescriptor);
            default:
                YT_ABORT();
        }
    }

    void Truncate(int recordCount) override
    {
        Error_.ThrowOnError();
        ValidateOpen();

        auto oldRecordCount = GetRecordCount();
        YT_VERIFY(recordCount >= 0 && recordCount <= oldRecordCount);

        if (recordCount == oldRecordCount) {
            return;
        }

        try {
            auto dataOffset = Index_->GetRecordRange(recordCount).first;

            YT_LOG_DEBUG("Started truncating file changelog (RecordCount: %v -> %v, DataOffset: %v)",
                oldRecordCount,
                recordCount,
                dataOffset);

            auto indexFileName = MakeIndexFileName();
            auto tempIndexFileName = MakeTempIndexFileName();

            auto newIndex = MakeIndex(tempIndexFileName);
            newIndex->Create();

            for (int recordIndex = 0; recordIndex < recordCount; ++recordIndex) {
                newIndex->AppendRecord(recordIndex, Index_->GetRecordRange(recordIndex));
            }

            newIndex->SetFlushedDataRecordCount(recordCount);
            newIndex->SyncFlush();

            Index_->Close();
            NFS::Remove(indexFileName);

            newIndex->Close();
            NFS::Rename(tempIndexFileName, indexFileName);

            Index_ = MakeIndex(indexFileName);
            Index_->Open();

            Index_->SetFlushedDataRecordCount(recordCount);

            WipeDataFileRange({dataOffset, DataFileHandle_->GetLength()});

            AppendedDataSizeSinceLastIndexFlush_ = 0;
            CurrentFileOffset_.store(dataOffset);
            RecordCount_.store(recordCount);

            YT_LOG_DEBUG("Finished truncating file changelog (RecordCount: %v -> %v)",
                oldRecordCount,
                recordCount);
        } catch (const std::exception& ex) {
            RecordErrorAndThrow(TError(
                NHydra::EErrorCode::ChangelogIOError,
                "Error truncating changelog %v",
                FileName_)
                << ex);
        }
    }

private:
    const IIOEnginePtr IOEngine_;
    const IMemoryUsageTrackerPtr MemoryUsageTracker_;
    const TString FileName_;
    const TFileChangelogConfigPtr Config_;
    const NLogging::TLogger Logger;

    TError Error_;
    std::atomic<bool> Open_ = false;
    EFileChangelogFormat Format_ = EFileChangelogFormat::V5;
    int FileHeaderSize_ = -1;
    int RecordHeaderSize_ = -1;
    TGuid Uuid_;

    std::atomic<int> RecordCount_ = -1;
    std::atomic<i64> CurrentFileOffset_ = -1;
    i64 CurrentFileSize_ = -1;
    i64 AppendedDataSizeSinceLastIndexFlush_ = 0;

    i64 PayloadWrittenBytes_ = 0;
    i64 MediaWrittenBytes_ = 0;
    std::atomic<double> WriteAmplification_ = 1;

    TChangelogMeta Meta_;
    TSharedRef SerializedMeta_;

    TIOEngineHandlePtr DataFileHandle_;
    TFileChangelogIndexPtr Index_;


    [[noreturn]]
    void RecordErrorAndThrow(const TError& error)
    {
        YT_LOG_ERROR(error);
        Error_ = error;
        THROW_ERROR(error);
    }

    void UpdateWriteAmplificationStat()
    {
        static constexpr i64 MeaningfulChangelogSize = 1_MB;
        if (PayloadWrittenBytes_ < MeaningfulChangelogSize) {
            // Wait while we write some data
            return;
        }

        auto amplification = static_cast<double>(MediaWrittenBytes_) / PayloadWrittenBytes_;
        WriteAmplification_.store(amplification, std::memory_order::relaxed);

        YT_LOG_DEBUG("Updating write amplification (PayloadWrittenBytes: %v, MediaWrittenBytes: %v, WriteAmplification: %v)",
            PayloadWrittenBytes_,
            MediaWrittenBytes_,
            amplification);
    }

    TString MakeIndexFileName()
    {
        return FileName_ + "." + ChangelogIndexExtension;
    }

    TString MakeTempIndexFileName()
    {
        return MakeIndexFileName() + NFS::TempFileSuffix;
    }

    TFileChangelogIndexPtr MakeIndex(TString fileName)
    {
        return New<TFileChangelogIndex>(
            IOEngine_,
            MemoryUsageTracker_,
            std::move(fileName),
            Config_,
            // TODO(capone212): better workload category?
            EWorkloadCategory::UserBatch);
    }


    void Cleanup()
    {
        Open_ = false;
        Format_ = EFileChangelogFormat::V5;
        FileHeaderSize_ = -1;
        RecordHeaderSize_ = -1;
        Uuid_ = {};
        RecordCount_.store(-1);
        CurrentFileOffset_.store(-1);
        CurrentFileSize_ = -1;
    }

    void ValidateOpen()
    {
        if (!Open_) {
            THROW_ERROR_EXCEPTION(
                NHydra::EErrorCode::InvalidChangelogState,
                "Changelog is not open");
        }
    }

    void ValidateNotOpen()
    {
        if (Open_) {
            THROW_ERROR_EXCEPTION(
                NHydra::EErrorCode::InvalidChangelogState,
                "Changelog is already open");
        }
    }

    void LockDataFile()
    {
        int index = 0;
        while (true) {
            YT_LOG_DEBUG("Locking data file");

            auto error = WaitFor(IOEngine_->Lock({.Handle = DataFileHandle_, .Mode = ELockFileMode::Exclusive, .Nonblocking = true}, UnbufferedFileChangelogWorkloadDescriptor, {}));
            if (error.IsOK()) {
                break;
            }

            if (++index >= MaxLockRetries) {
                THROW_ERROR_EXCEPTION(
                    NHydra::EErrorCode::ChangelogIOError,
                    "Cannot lock %v",
                    FileName_)
                    << error;
            }

            YT_LOG_WARNING(error, "Error locking data file; backing off and retrying");
            TDelayedExecutor::WaitForDuration(LockBackoffTime);
        }
    }

    template <class TFileHeader>
    TFileHeader MakeChangelogHeader()
    {
        TFileHeader header{};
        header.Signature = TFileHeader::ExpectedSignature;
        header.MetaSize = SerializedMeta_.Size();
        header.FirstRecordOffset = AlignUp<size_t>(sizeof(TFileHeader) + header.MetaSize, ChangelogPageAlignment);
        header.UnusedMustBeMinus2 = -2;
        header.PaddingSize = header.FirstRecordOffset - sizeof(TFileHeader) - header.MetaSize;
        header.Uuid = Uuid_;
        return header;
    }

    template <class TFileHeader, class TRecordHeader>
    void DoCreateDataFile()
    {
        FileHeaderSize_ = sizeof(TFileHeader);
        RecordHeaderSize_ = sizeof(TRecordHeader);

        auto header = MakeChangelogHeader<TFileHeader>();

        auto buffer = TSharedMutableRef::AllocatePageAligned<TUnbufferedFileChangelogHeaderTag>(header.FirstRecordOffset);
        TMemoryOutput output(buffer.Begin(), buffer.Size());

        WritePod(output, header);

        WriteRef(output, SerializedMeta_);
        WriteZeroes(output, header.PaddingSize);

        output.Finish();

        YT_VERIFY(static_cast<i32>(buffer.Size()) == header.FirstRecordOffset);

        NFS::WrapIOErrors([&] {
            auto tempFileName = FileName_ + NFS::TempFileSuffix;

            auto dataFile = WaitFor(IOEngine_->Open({.Path = tempFileName, .Mode = WrOnly | CloseOnExec | CreateAlways}, UnbufferedFileChangelogWorkloadDescriptor, {}))
                .ValueOrThrow();

            WaitFor(IOEngine_->Write({
                    .Handle = dataFile,
                    .Offset = 0,
                    .Buffers = {std::move(buffer)}
                },
                UnbufferedFileChangelogWorkloadDescriptor,
                {}))
                .ThrowOnError();

            WaitFor(IOEngine_->Close({.Handle = dataFile, .Flush = true}, UnbufferedFileChangelogWorkloadDescriptor, {}))
                .ThrowOnError();

            // TODO(babenko): use IO engine
            NFS::Replace(tempFileName, FileName_);

            DataFileHandle_ = WaitFor(IOEngine_->Open({.Path = FileName_, .Mode = RdWr | Seq | CloseOnExec}, UnbufferedFileChangelogWorkloadDescriptor, {}))
                .ValueOrThrow();
        });
    }

    void CreateDataFile()
    {
        switch (Format_) {
            case EFileChangelogFormat::V5:
                DoCreateDataFile<TChangelogHeader_5, TChangelogRecordHeader_5>();
                break;
            default:
                YT_ABORT();
        }
    }

    template <class TRecordHeader>
    void DoAppend(
        int firstRecordIndex,
        const std::vector<TSharedRef>& records)
    {
        YT_VERIFY(firstRecordIndex == RecordCount_);

        try {
            YT_LOG_DEBUG("Started appending to changelog (FirstRecordIndex: %v, RecordCount: %v)",
                firstRecordIndex,
                records.size());

            i64 prevFileOffset = CurrentFileOffset_.load();
            i64 currentFileOffset = prevFileOffset;
            i64 currentRecordIndex = RecordCount_.load();
            int payloadWrittenBytes = 0;

            // Header, payload, padding per each record.
            std::vector<TSharedRef> buffers;
            buffers.reserve(records.size() * 3);

            auto headersBuffer = TSharedMutableRef::Allocate<TUnbufferedFileChangelogRecordHeadersTag>(records.size() * sizeof(TRecordHeader));
            auto* currentHeader = reinterpret_cast<TRecordHeader*>(headersBuffer.Begin());

            for (int index = 0; index < std::ssize(records); ++index) {
                const auto& record = records[index];

                i64 qwordPaddingSize =
                    AlignUpSpace<i64>(std::ssize(record), ChangelogQWordAlignment);

                i64 pagePaddingSize = index == std::ssize(records) - 1
                    ? AlignUpSpace<i64>(currentFileOffset + sizeof(TRecordHeader) + std::ssize(record) + qwordPaddingSize, ChangelogPageAlignment)
                    : 0;
                YT_VERIFY(pagePaddingSize <= std::numeric_limits<i16>::max());

                i64 totalPaddingSize = qwordPaddingSize + pagePaddingSize;
                YT_VERIFY(totalPaddingSize <= ChangelogPageAlignment);

                auto totalSize =
                    static_cast<i64>(sizeof(TRecordHeader)) +
                    std::ssize(record) +
                    totalPaddingSize;

                // Header
                new(currentHeader) TRecordHeader();
                currentHeader->RecordIndex = firstRecordIndex + index;
                currentHeader->PayloadSize = record.Size();
                currentHeader->Checksum = GetChecksum(record);
                currentHeader->PagePaddingSize = pagePaddingSize;
                currentHeader->ChangelogUuid = Uuid_;
                buffers.push_back(headersBuffer.Slice(currentHeader, currentHeader + 1));
                ++currentHeader;
                payloadWrittenBytes += record.Size();

                // Payload
                buffers.push_back(record);

                // Padding
                static const std::array<char, ChangelogPageAlignment> Padding{};
                buffers.push_back(TSharedRef(Padding.data(), totalPaddingSize, /*nullptr*/ nullptr));

                Index_->AppendRecord(currentRecordIndex, std::make_pair(currentFileOffset, currentFileOffset + totalSize));

                currentFileOffset += totalSize;
                currentRecordIndex += 1;
            }

            YT_VERIFY(currentFileOffset % ChangelogPageAlignment == 0);

            // Preallocate file if needed.
            if (Config_->PreallocateSize && currentFileOffset > CurrentFileSize_) {
                auto newFileSize = std::max(CurrentFileSize_ + *Config_->PreallocateSize, currentFileOffset);
                WaitFor(IOEngine_->Allocate({.Handle = DataFileHandle_, .Size = newFileSize}, UnbufferedFileChangelogWorkloadDescriptor, {}))
                    .ThrowOnError();
                CurrentFileSize_ = newFileSize;
            }

            // Write blob to file.
            WaitFor(IOEngine_->Write({
                    .Handle = DataFileHandle_,
                    .Offset = CurrentFileOffset_.load(),
                    .Buffers = std::move(buffers)
                },
                UnbufferedFileChangelogWorkloadDescriptor,
                {}))
                .ThrowOnError();

            RecordCount_ += std::ssize(records);
            CurrentFileOffset_.store(currentFileOffset);

            i64 bytesWritten = currentFileOffset - prevFileOffset;
            AppendedDataSizeSinceLastIndexFlush_ += bytesWritten;
            MediaWrittenBytes_ += bytesWritten;
            PayloadWrittenBytes_ += payloadWrittenBytes;

            UpdateWriteAmplificationStat();

            YT_LOG_DEBUG("Finished appending to changelog (FirstRecordIndex: %v, RecordCount: %v, Bytes: %v)",
                firstRecordIndex,
                records.size(),
                bytesWritten);
        } catch (const std::exception& ex) {
            RecordErrorAndThrow(TError(
                NHydra::EErrorCode::ChangelogIOError,
                "Error appending to changelog %v",
                FileName_)
                << ex);
        }
    }

    template <class TRecordHeader>
    std::vector<TSharedRef> DoRead(
        int firstRecordIndex,
        int maxRecords,
        i64 maxBytes)
    {
        YT_VERIFY(firstRecordIndex >= 0);
        YT_VERIFY(maxRecords >= 0);

        try {
            YT_LOG_DEBUG("Started reading changelog (FirstRecordIndex: %v, MaxRecords: %v, MaxBytes: %v)",
                firstRecordIndex,
                maxRecords,
                maxBytes);

            auto optionalRange = Index_->FindRecordsRange(firstRecordIndex, maxRecords, maxBytes);
            if (!optionalRange) {
                return {};
            }
            auto result = DoReadAndParseRange<TRecordHeader>(
                *optionalRange,
                firstRecordIndex,
                /*throwError*/ true);

            YT_LOG_DEBUG("Finished reading changelog (RecordCount: %v, Bytes: %v)",
                result.Records.size(),
                GetByteSize(result.Records));

            return std::move(result.Records);
        } catch (const std::exception& ex) {
            RecordErrorAndThrow(TError(
                NHydra::EErrorCode::ChangelogIOError,
                "Error reading changelog %v",
                FileName_)
                << ex);
        }
    }

    template <class TRecordHeader>
    IIOEngine::TReadRequest DoMakeChunkFragmentReadRequest(const TChunkFragmentDescriptor& fragmentDescriptor)
    {
        auto makeErrorAttributes = [&] {
            return std::vector{
                TErrorAttribute("file_name", FileName_),
                TErrorAttribute("block_index", fragmentDescriptor.BlockIndex),
                TErrorAttribute("block_offset", fragmentDescriptor.BlockOffset),
                TErrorAttribute("length", fragmentDescriptor.Length)
            };
        };

        if (fragmentDescriptor.BlockIndex < 0) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::MalformedReadRequest,
                "Negative block index in fragment descriptor")
                << makeErrorAttributes();
        }

        auto flushedRecordCount = Index_->GetFlushedDataRecordCount();
        if (fragmentDescriptor.BlockIndex >= flushedRecordCount) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::MissingJournalChunkRecord,
                "Journal chunk record is missing")
                << makeErrorAttributes()
                << TErrorAttribute("flushed_record_count", flushedRecordCount);
        }

        auto range = Index_->GetRecordRange(fragmentDescriptor.BlockIndex);
        auto recordLength = range.second - range.first - static_cast<i64>(sizeof(TRecordHeader));
        if (fragmentDescriptor.BlockOffset < 0 ||
            fragmentDescriptor.BlockOffset + fragmentDescriptor.Length > recordLength)
        {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::MalformedReadRequest,
                "Fragment is out of record range")
                << makeErrorAttributes()
                << TErrorAttribute("record_length", recordLength);
        }

        return IIOEngine::TReadRequest{
            .Handle = DataFileHandle_,
            .Offset = range.first + static_cast<i64>(sizeof(TRecordHeader)) + fragmentDescriptor.BlockOffset,
            .Size = fragmentDescriptor.Length
        };
    }

    i64 GuessRecordReadSize(i64 offset, i64 dataFileLength)
    {
        switch (Format_) {
            case EFileChangelogFormat::V5:
                return DoGuessRecordReadSize<TChangelogRecordHeader_5>(offset, dataFileLength);
            default:
                YT_ABORT();
        }
    }

    template <class TRecordHeader>
    i64 DoGuessRecordReadSize(i64 offset, i64 dataFileLength)
    {
        if (offset + static_cast<i64>(sizeof(TRecordHeader)) > dataFileLength) {
            return 0;
        }

        auto buffer = WaitFor(
            IOEngine_->Read(
                {{.Handle = DataFileHandle_, .Offset = offset, .Size = sizeof(TRecordHeader)}},
                UnbufferedFileChangelogWorkloadDescriptor,
                GetRefCountedTypeCookie<TUnbufferedFileChangelogHeaderTag>(),
                {}))
            .ValueOrThrow()
            .OutputBuffers[0];

        const auto* header = reinterpret_cast<const TRecordHeader*>(buffer.Begin());
        if (header->PayloadSize < 0) {
            return 0;
        }
        if (header->PagePaddingSize < 0) {
            return 0;
        }

        return
            sizeof(TRecordHeader) +
            AlignUp<i64>(header->PayloadSize, ChangelogQWordAlignment) +
            header->PagePaddingSize;
    }

    struct TRecordsParseResult
    {
        std::vector<TSharedRef> Records;
        std::vector<std::pair<i64, i64>> RecordRanges;
        i64 FirstUnparsedOffset;
    };

    TRecordsParseResult ReadAndParseRange(
        std::pair<i64, i64> range,
        int firstRecordIndex,
        bool throwOnError)
    {
        switch (Format_) {
            case EFileChangelogFormat::V5:
                return DoReadAndParseRange<TChangelogRecordHeader_5>(range, firstRecordIndex, throwOnError);
            default:
                YT_ABORT();
        }
    }

    template <class TRecordHeader>
    TRecordsParseResult DoReadAndParseRange(
        std::pair<i64, i64> range,
        int firstRecordIndex,
        bool throwOnError)
    {
        auto buffer = WaitFor(
            IOEngine_->Read(
                {{.Handle = DataFileHandle_, .Offset = range.first, .Size = range.second - range.first}},
                UnbufferedFileChangelogWorkloadDescriptor,
                GetRefCountedTypeCookie<TUnbufferedFileChangelogHeaderTag>(),
                {}))
            .ValueOrThrow()
            .OutputBuffers[0];

        i64 currentOffset = 0;
        int currentRecordIndex = firstRecordIndex;
        std::vector<TSharedRef> records;
        std::vector<std::pair<i64, i64>> recordRanges;
        while (currentOffset < std::ssize(buffer)) {
            auto result = TryParseRecord<TRecordHeader>(
                buffer,
                currentOffset,
                currentRecordIndex,
                throwOnError);
            if (!result) {
                break;
            }

            records.push_back(std::move(result->Record));
            recordRanges.emplace_back(range.first + currentOffset, range.first + result->FirstUnparsedOffset);

            currentOffset = result->FirstUnparsedOffset;
            currentRecordIndex += 1;
        }

        return {
            std::move(records),
            std::move(recordRanges),
            currentOffset
        };
    }

    struct TRecordParseResult
    {
        TSharedRef Record;
        i64 FirstUnparsedOffset;
    };

    template <class TRecordHeader>
    std::optional<TRecordParseResult> TryParseRecord(
        const TSharedRef& buffer,
        i64 offset,
        int recordIndex,
        bool throwOnError)
    {
        auto onError = [&] (TError error) -> std::optional<TRecordParseResult> {
            if (throwOnError) {
                THROW_ERROR(std::move(error));
            }
            return {};
        };

        auto currentOffset = offset;

        if (currentOffset + static_cast<i64>(sizeof(TRecordHeader)) > std::ssize(buffer)) {
            return onError(TError(
                NHydra::EErrorCode::BrokenChangelog,
                "Record buffer is too small to fit record header")
                << TErrorAttribute("record_index", recordIndex));
        }

        const auto* header = reinterpret_cast<const TRecordHeader*>(buffer.Begin() + currentOffset);
        currentOffset += sizeof(TRecordHeader);

        if (header->RecordIndex != recordIndex) {
            return onError(TError(
                NHydra::EErrorCode::BrokenChangelog,
                "Invalid record index in header")
                << TErrorAttribute("expected_record_index", recordIndex)
                << TErrorAttribute("actual_record_index", header->RecordIndex));
        }

        if (header->ChangelogUuid != Uuid_) {
            return onError(TError(
                NHydra::EErrorCode::BrokenChangelog,
                "Invalid changelog UUID in record header")
                << TErrorAttribute("expected_uuid", Uuid_)
                << TErrorAttribute("actual_uuid", header->ChangelogUuid));
        }

        if (header->PayloadSize < 0) {
            return onError(TError(
                NHydra::EErrorCode::BrokenChangelog,
                "Negative payload size in record header")
                << TErrorAttribute("record_index", recordIndex));
        }

        if (header->PagePaddingSize < 0) {
            return onError(TError(
                NHydra::EErrorCode::BrokenChangelog,
                "Negative page padding size in record header")
                << TErrorAttribute("record_index", recordIndex));
        }

        if (currentOffset + header->PayloadSize > std::ssize(buffer)) {
            return onError(TError(
                NHydra::EErrorCode::BrokenChangelog,
                "Read buffer is too small to fit record data")
                << TErrorAttribute("record_index", recordIndex));
        }

        auto record = buffer.Slice(currentOffset, currentOffset + header->PayloadSize);
        currentOffset += header->PayloadSize;

        if (GetChecksum(record) != header->Checksum) {
            return onError(TError(
                NHydra::EErrorCode::BrokenChangelog,
                "Invalid record data checksum")
                << TErrorAttribute("record_index", recordIndex));
        }

        currentOffset += AlignUpSpace<i64>(header->PayloadSize, ChangelogQWordAlignment);
        currentOffset += header->PagePaddingSize;

        if (currentOffset > std::ssize(buffer)) {
            return onError(TError(
                NHydra::EErrorCode::BrokenChangelog,
                "Read buffer is too small to fit record padding")
                << TErrorAttribute("record_index", recordIndex));
        }

        return TRecordParseResult{
            std::move(record),
            currentOffset
        };
    }

    void WipeDataFileRange(std::pair<i64, i64> range)
    {
        YT_LOG_DEBUG("Started wiping changelog data file range (StartOffset: %v, EndOffset: %v)",
            range.first,
            range.second);

        auto wipeBuffer = TSharedMutableRef::AllocatePageAligned<TUnbufferedFileChangelogWipeTag>(
            WipeBufferSize,
            {.InitializeStorage = false});
        std::fill(wipeBuffer.Begin(), wipeBuffer.End(), 0xff);

        auto currentOffset = range.first;
        while (currentOffset < range.second) {
            auto currentSize = std::min(range.second - currentOffset, std::ssize(wipeBuffer));
            auto currentBuffer = wipeBuffer.Slice(0, currentSize);
            WaitFor(IOEngine_->Write({
                    .Handle = DataFileHandle_,
                    .Offset = currentOffset,
                    .Buffers = {std::move(currentBuffer)}
                },
                UnbufferedFileChangelogWorkloadDescriptor,
                {}))
                .ThrowOnError();
            currentOffset += currentSize;
        }

        YT_LOG_DEBUG("Finished wiping changelog data file range");
    }
};

////////////////////////////////////////////////////////////////////////////////

IUnbufferedFileChangelogPtr CreateUnbufferedFileChangelog(
    IIOEnginePtr ioEngine,
    IMemoryUsageTrackerPtr memoryUsageTracker,
    TString fileName,
    TFileChangelogConfigPtr config)
{
    return New<TUnbufferedFileChangelog>(
        std::move(ioEngine),
        std::move(memoryUsageTracker),
        std::move(fileName),
        std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
