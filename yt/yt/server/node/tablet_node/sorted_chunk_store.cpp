#include "sorted_chunk_store.h"
#include "automaton.h"
#include "in_memory_manager.h"
#include "tablet.h"
#include "transaction.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/node/query_agent/config.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/cache_reader.h>
#include <yt/yt/ytlib/chunk_client/block_tracking_chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/ref_counted_proto.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/table_client/cache_based_versioned_chunk_reader.h>
#include <yt/yt/ytlib/table_client/cached_versioned_chunk_meta.h>
#include <yt/yt/ytlib/table_client/chunk_column_mapping.h>
#include <yt/yt/ytlib/table_client/chunk_index_read_controller.h>
#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/chunk_state.h>
#include <yt/yt/ytlib/table_client/indexed_versioned_chunk_reader.h>
#include <yt/yt/ytlib/table_client/key_filter.h>
#include <yt/yt/ytlib/table_client/performance_counters.h>
#include <yt/yt/ytlib/table_client/versioned_offloading_reader.h>
#include <yt/yt/ytlib/table_client/versioned_chunk_reader.h>
#include <yt/yt/ytlib/table_client/versioned_reader_adapter.h>

#include <yt/yt/ytlib/new_table_client/versioned_chunk_reader.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/client/table_client/versioned_reader.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/iterator/enumerate.h>

namespace NYT::NTabletNode {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NRpc;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NTransactionClient;
using namespace NApi;
using namespace NDataNode;
using namespace NClusterNode;
using namespace NQueryAgent;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

bool IsNewScanReaderEnabled(const TTableMountConfigPtr& mountConfig)
{
    return mountConfig->EnableNewScanReaderForLookup || mountConfig->EnableNewScanReaderForSelect;
}

////////////////////////////////////////////////////////////////////////////////

class TFilteringReader
    : public IVersionedReader
{
public:
    TFilteringReader(
        IVersionedReaderPtr underlyingReader,
        int skipBefore,
        int skipAfter)
        : UnderlyingReader_(std::move(underlyingReader))
        , SkipBefore_(skipBefore)
        , SkipAfter_(skipAfter)
    { }

    TDataStatistics GetDataStatistics() const override
    {
        return UnderlyingReader_->GetDataStatistics();
    }

    TCodecStatistics GetDecompressionStatistics() const override
    {
        return UnderlyingReader_->GetDecompressionStatistics();
    }

    TFuture<void> Open() override
    {
        return UnderlyingReader_->Open();
    }

    TFuture<void> GetReadyEvent() const override
    {
        if (SkipBefore_ > 0) {
            return VoidFuture;
        }

        if (!SkippingAfter_) {
            return UnderlyingReader_->GetReadyEvent();
        }

        return VoidFuture;
    }

    bool IsFetchingCompleted() const override
    {
        return UnderlyingReader_->IsFetchingCompleted();
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return UnderlyingReader_->GetFailedChunkIds();
    }

    IVersionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        if (SkipBefore_ > 0) {
            return MakeSentinelRowset(options.MaxRowsPerRead, &SkipBefore_);
        }

        if (!SkippingAfter_) {
            if (auto result = UnderlyingReader_->Read(options)) {
                return result;
            }
            SkippingAfter_ = true;
        }

        if (SkipAfter_ > 0) {
            return MakeSentinelRowset(options.MaxRowsPerRead, &SkipAfter_);
        }

        return nullptr;
    }

private:
    const IVersionedReaderPtr UnderlyingReader_;

    int SkipBefore_;
    int SkipAfter_;
    bool SkippingAfter_ = false;

    static IVersionedRowBatchPtr MakeSentinelRowset(i64 maxRowsPerRead, int* counter)
    {
        std::vector<TVersionedRow> rows;
        int rowCount = std::min<i64>(maxRowsPerRead, *counter);
        rows.reserve(rowCount);
        for (int index = 0; index < rowCount; ++index) {
            rows.push_back(TVersionedRow());
        }

        *counter -= rowCount;

        return CreateBatchFromVersionedRows(MakeSharedRange(std::move(rows)));
    }
};

////////////////////////////////////////////////////////////////////////////////

// TODO(ifsmirnov, akozhikhov): Only for tests, to be removed in YT-18325.
class TKeyFilteringReader
    : public IVersionedReader
{
public:
    TKeyFilteringReader(
        IVersionedReaderPtr underlyingReader,
        std::vector<ui8> missingKeyMask)
        : UnderlyingReader_(std::move(underlyingReader))
        , MissingKeyMask_(std::move(missingKeyMask))
        , ReadRowCount_(0)
    { }

    TDataStatistics GetDataStatistics() const override
    {
        return UnderlyingReader_->GetDataStatistics();
    }

    TCodecStatistics GetDecompressionStatistics() const override
    {
        return UnderlyingReader_->GetDecompressionStatistics();
    }

    TFuture<void> Open() override
    {
        return UnderlyingReader_->Open();
    }

    TFuture<void> GetReadyEvent() const override
    {
        return UnderlyingReader_->GetReadyEvent();
    }

    bool IsFetchingCompleted() const override
    {
        return UnderlyingReader_->IsFetchingCompleted();
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return UnderlyingReader_->GetFailedChunkIds();
    }

    IVersionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        if (ReadRowCount_ == ssize(MissingKeyMask_)) {
            return nullptr;
        }

        int batchEndIndex = ReadRowCount_ + std::min(options.MaxRowsPerRead, ssize(MissingKeyMask_) - ReadRowCount_);
        int neededUnderlyingRowCount = std::count(
            MissingKeyMask_.begin() + ReadRowCount_,
            MissingKeyMask_.begin() + batchEndIndex,
            0);

        auto underlyingOptions = options;
        underlyingOptions.MaxRowsPerRead = neededUnderlyingRowCount;

        auto underlyingBatch = neededUnderlyingRowCount > 0
            ? UnderlyingReader_->Read(underlyingOptions)
            : nullptr;

        auto underlyingRows = underlyingBatch
            ? underlyingBatch->MaterializeRows()
            : TSharedRange<TVersionedRow>();

        int underlyingRowIndex = 0;
        std::vector<TVersionedRow> result;

        while (ReadRowCount_ < batchEndIndex) {
            if (MissingKeyMask_[ReadRowCount_]) {
                result.emplace_back();
                ++ReadRowCount_;
            } else if (underlyingRows && underlyingRowIndex < std::ssize(underlyingRows)) {
                result.push_back(underlyingRows[underlyingRowIndex++]);
                ++ReadRowCount_;
            } else {
                break;
            }
        }

        return CreateBatchFromVersionedRows(MakeSharedRange(std::move(result)));
    }

private:
    const IVersionedReaderPtr UnderlyingReader_;
    const std::vector<ui8> MissingKeyMask_;

    i64 ReadRowCount_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

// TODO(ifsmirnov, akozhikhov): Only for tests, to be removed in YT-18325.
bool PrepareKeyFilteringReader(
    const TTabletSnapshotPtr& tabletSnapshot,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    const IChunkReaderPtr& chunkReader,
    const TClientChunkReadOptions& chunkReadOptions,
    TSharedRange<TLegacyKey>* keys,
    std::vector<ui8> *missingKeyMask)
{
    if (!tabletSnapshot->Settings.MountConfig->TestingOnlyUseKeyFilter) {
        return false;
    }

    std::vector<IKeyFilterPtr> keyFilters;

    auto systemBlockMeta = chunkMeta->SystemBlockMeta();
    if (!systemBlockMeta) {
        return false;
    }

    int dataBlockCount = chunkMeta->DataBlockMeta()->data_blocks_size();

    IChunkReader::TReadBlocksOptions readBlocksOptions{
        .ClientOptions = chunkReadOptions,
    };

    for (const auto& [index, blockMeta] : Enumerate(systemBlockMeta->system_blocks())) {
        if (!blockMeta.HasExtension(TXorFilterSystemBlockMeta::xor_filter_system_block_meta_ext)) {
            continue;
        }

        auto xorFilterBlockMeta = blockMeta.GetExtension(
            TXorFilterSystemBlockMeta::xor_filter_system_block_meta_ext);

        auto blocks = WaitFor(chunkReader->ReadBlocks(
            readBlocksOptions,
            dataBlockCount + index,
            1))
            .ValueOrThrow();
        YT_VERIFY(ssize(blocks) == 1);

        auto compressedBlock = TBlock::Unwrap(blocks)[0];

        auto codecName = static_cast<NCompression::ECodec>(chunkMeta->Misc().compression_codec());
        auto codec = NCompression::GetCodec(codecName);
        auto decompressedBlock = codec->Decompress(compressedBlock);

        keyFilters.push_back(CreateXorFilter(xorFilterBlockMeta, std::move(decompressedBlock)));
    }

    if (keyFilters.empty()) {
        return false;
    }

    missingKeyMask->clear();
    missingKeyMask->reserve(keys->size());

    std::vector<TLegacyKey> filteredKeys;

    int filterIndex = 0;

    for (auto key : *keys) {
        // NB: This is probably incorrect, should use WidenKey or something.
        while (filterIndex < ssize(keyFilters) && keyFilters[filterIndex]->GetLastKey() < key) {
            ++filterIndex;
        }

        if (filterIndex < ssize(keyFilters) && keyFilters[filterIndex]->Contains(key)) {
            filteredKeys.push_back(std::move(key));
            missingKeyMask->push_back(0);
        } else {
            missingKeyMask->push_back(1);
        }
    }

    *keys = MakeSharedRange(std::move(filteredKeys));

    return true;
}

////////////////////////////////////////////////////////////////////////////////

TSortedChunkStore::TSortedChunkStore(
    TTabletManagerConfigPtr config,
    TStoreId id,
    TChunkId chunkId,
    const NChunkClient::TLegacyReadRange& readRange,
    TTimestamp overrideTimestamp,
    TTimestamp maxClipTimestamp,
    TTablet* tablet,
    const NTabletNode::NProto::TAddStoreDescriptor* addStoreDescriptor,
    IBlockCachePtr blockCache,
    IVersionedChunkMetaManagerPtr chunkMetaManager,
    IBackendChunkReadersHolderPtr backendReadersHolder)
    : TChunkStoreBase(
        config,
        id,
        chunkId,
        overrideTimestamp,
        tablet,
        addStoreDescriptor,
        blockCache,
        chunkMetaManager,
        std::move(backendReadersHolder))
    , KeyComparer_(tablet->GetRowKeyComparer().UUComparer)
    , MaxClipTimestamp_(maxClipTimestamp)
{
    TLegacyKey lowerBound;
    TLegacyKey upperBound;

    if (readRange.LowerLimit().HasLegacyKey()) {
        lowerBound = readRange.LowerLimit().GetLegacyKey();
    }

    if (readRange.UpperLimit().HasLegacyKey()) {
        upperBound = readRange.UpperLimit().GetLegacyKey();
    }

    ReadRange_ = MakeSingletonRowRange(lowerBound, upperBound);
}

void TSortedChunkStore::Initialize()
{
    TChunkStoreBase::Initialize();

    auto boundaryKeysExt = GetProtoExtension<TBoundaryKeysExt>(ChunkMeta_->extensions());

    MinKey_ = FromProto<TLegacyOwningKey>(boundaryKeysExt.min());
    const auto& chunkViewLowerBound = ReadRange_.Front().first;
    if (chunkViewLowerBound && chunkViewLowerBound > MinKey_) {
        MinKey_ = TLegacyOwningKey(chunkViewLowerBound);
    }
    MinKey_ = WidenKey(MinKey_, KeyColumnCount_);

    UpperBoundKey_ = FromProto<TLegacyOwningKey>(boundaryKeysExt.max());
    const auto& chunkViewUpperBound = ReadRange_.Front().second;
    if (chunkViewUpperBound && chunkViewUpperBound <= UpperBoundKey_) {
        UpperBoundKey_ = TLegacyOwningKey(chunkViewUpperBound);
    } else {
        UpperBoundKey_ = WidenKeySuccessor(UpperBoundKey_, KeyColumnCount_);
    }
}

EStoreType TSortedChunkStore::GetType() const
{
    return EStoreType::SortedChunk;
}

TSortedChunkStorePtr TSortedChunkStore::AsSortedChunk()
{
    return this;
}

void TSortedChunkStore::BuildOrchidYson(TFluentMap fluent)
{
    TChunkStoreBase::BuildOrchidYson(fluent);

    fluent
        .Item("min_key").Value(GetMinKey())
        .Item("upper_bound_key").Value(GetUpperBoundKey())
        .Item("max_clip_timestamp").Value(MaxClipTimestamp_);
}

TLegacyOwningKey TSortedChunkStore::GetMinKey() const
{
    return MinKey_;
}

TLegacyOwningKey TSortedChunkStore::GetUpperBoundKey() const
{
    return UpperBoundKey_;
}

bool TSortedChunkStore::HasNontrivialReadRange() const
{
    return ReadRange_.Front().first || ReadRange_.Front().second;
}

IVersionedReaderPtr TSortedChunkStore::CreateReader(
    const TTabletSnapshotPtr& tabletSnapshot,
    TSharedRange<TRowRange> ranges,
    TTimestamp timestamp,
    bool produceAllVersions,
    const TColumnFilter& columnFilter,
    const TClientChunkReadOptions& chunkReadOptions,
    std::optional<EWorkloadCategory> workloadCategory)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (MaxClipTimestamp_) {
        timestamp = std::min(timestamp, MaxClipTimestamp_);
    }

    ranges = FilterRowRangesByReadRange(ranges);

    // Fast lane:
    // - ranges do not intersect with chunk view;
    // - chunk timestamp is greater than requested timestamp.
    if (ranges.Empty() || (OverrideTimestamp_ && OverrideTimestamp_ > timestamp))
    {
        return CreateEmptyVersionedReader();
    }

    const auto& mountConfig = tabletSnapshot->Settings.MountConfig;
    bool enableNewScanReader = IsNewScanReaderEnabled(mountConfig);

    auto wrapReaderWithPerformanceCounting = [&] (IVersionedReaderPtr underlyingReader)
    {
        return CreateVersionedPerformanceCountingReader(
            underlyingReader,
            PerformanceCounters_,
            NTableClient::EDataSource::ChunkStore,
            ERequestType::Read);
    };

    // Fast lane: check for in-memory reads.
    if (auto chunkState = FindPreloadedChunkState()) {
        return wrapReaderWithPerformanceCounting(
            MaybeWrapWithTimestampResettingAdapter(
                CreateCacheBasedReader(
                    chunkState,
                    ranges,
                    timestamp,
                    produceAllVersions,
                    columnFilter,
                    chunkReadOptions,
                    ReadRange_,
                    enableNewScanReader)));
    }

    // Another fast lane: check for backing store.
    if (auto backingStore = GetSortedBackingStore()) {
        YT_VERIFY(!HasNontrivialReadRange());
        YT_VERIFY(!OverrideTimestamp_);
        return backingStore->CreateReader(
            tabletSnapshot,
            ranges,
            timestamp,
            produceAllVersions,
            columnFilter,
            chunkReadOptions,
            /*workloadCategory*/ std::nullopt);
    }

    auto backendReaders = GetBackendReaders(workloadCategory);
    auto chunkReader = backendReaders.ChunkReader;

    if (chunkReadOptions.MemoryReferenceTracker) {
        chunkReader = CreateBlockTrackingChunkReader(
            chunkReader,
            chunkReadOptions.MemoryReferenceTracker);
    }

    auto chunkMeta = FindCachedVersionedChunkMeta(enableNewScanReader);
    if (!chunkMeta) {
        chunkMeta = WaitForFast(GetCachedVersionedChunkMeta(
            backendReaders.ChunkReader,
            chunkReadOptions,
            enableNewScanReader))
            .ValueOrThrow();
    }

    auto chunkState = PrepareChunkState(std::move(chunkMeta));

    ValidateBlockSize(tabletSnapshot, chunkState->ChunkMeta, chunkReadOptions.WorkloadDescriptor);

    if (enableNewScanReader && chunkState->ChunkMeta->GetChunkFormat() == EChunkFormat::TableVersionedColumnar) {
        // Chunk view support.
        ranges = NNewTableClient::ClipRanges(
            ranges,
            ReadRange_.Size() > 0 ? ReadRange_.Front().first : TUnversionedRow(),
            ReadRange_.Size() > 0 ? ReadRange_.Front().second : TUnversionedRow(),
            ReadRange_.GetHolder());

        auto blockManagerFactory = NNewTableClient::CreateAsyncBlockWindowManagerFactory(
            std::move(backendReaders.ReaderConfig),
            std::move(backendReaders.ChunkReader),
            chunkState->BlockCache,
            chunkReadOptions,
            chunkState->ChunkMeta,
            // Enable current invoker for range reads.
            GetCurrentInvoker());

        return wrapReaderWithPerformanceCounting(
            MaybeWrapWithTimestampResettingAdapter(
                NNewTableClient::CreateVersionedChunkReader(
                    std::move(ranges),
                    timestamp,
                    chunkState->ChunkMeta,
                    Schema_,
                    columnFilter,
                    chunkState->ChunkColumnMapping,
                    blockManagerFactory,
                    produceAllVersions)));
    }

    // Reader can handle chunk timestamp itself if needed, no need to wrap with
    // timestamp resetting adapter.
    return wrapReaderWithPerformanceCounting(
        CreateVersionedChunkReader(
            std::move(backendReaders.ReaderConfig),
            std::move(backendReaders.ChunkReader),
            chunkState,
            chunkState->ChunkMeta,
            chunkReadOptions,
            std::move(ranges),
            columnFilter,
            timestamp,
            produceAllVersions,
            ReadRange_,
            nullptr,
            GetCurrentInvoker()));
}

IVersionedReaderPtr TSortedChunkStore::CreateCacheBasedReader(
    const TChunkStatePtr& chunkState,
    TSharedRange<TRowRange> ranges,
    TTimestamp timestamp,
    bool produceAllVersions,
    const TColumnFilter& columnFilter,
    const TClientChunkReadOptions& chunkReadOptions,
    const TSharedRange<TRowRange>& singletonClippingRange,
    bool enableNewScanReader) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    const auto& chunkMeta = chunkState->ChunkMeta;

    if (enableNewScanReader && chunkMeta->GetChunkFormat() == EChunkFormat::TableVersionedColumnar) {
        // Chunk view support.
        ranges = NNewTableClient::ClipRanges(
            ranges,
            singletonClippingRange.Size() > 0 ? singletonClippingRange.Front().first : TUnversionedRow(),
            singletonClippingRange.Size() > 0 ? singletonClippingRange.Front().second : TUnversionedRow(),
            singletonClippingRange.GetHolder());

        auto blockManagerFactory = NNewTableClient::CreateSyncBlockWindowManagerFactory(
            chunkState->BlockCache,
            chunkMeta,
            ChunkId_);

        return NNewTableClient::CreateVersionedChunkReader(
            std::move(ranges),
            timestamp,
            chunkMeta,
            Schema_,
            columnFilter,
            chunkState->ChunkColumnMapping,
            blockManagerFactory,
            produceAllVersions);
    }

    return CreateCacheBasedVersionedChunkReader(
        ChunkId_,
        chunkState,
        chunkState->ChunkMeta,
        chunkReadOptions,
        std::move(ranges),
        columnFilter,
        timestamp,
        produceAllVersions,
        singletonClippingRange);
}

class TSortedChunkStore::TSortedChunkStoreVersionedReader
    : public IVersionedReader
{
public:
    TSortedChunkStoreVersionedReader(
        int skippedBefore,
        int skippedAfter,
        TSortedChunkStore* const chunk,
        const TTabletSnapshotPtr& tabletSnapshot,
        TSharedRange<TLegacyKey> keys,
        TTimestamp timestamp,
        bool produceAllVersions,
        const TColumnFilter& columnFilter,
        const TClientChunkReadOptions& chunkReadOptions,
        std::optional<EWorkloadCategory> workloadCategory)
        : SkippedBefore_(skippedBefore)
        , SkippedAfter_(skippedAfter)
    {
        InitializationFuture_ = InitializeUnderlyingReader(
            chunk,
            tabletSnapshot,
            std::move(keys),
            timestamp,
            produceAllVersions,
            columnFilter,
            chunkReadOptions,
            workloadCategory);
    }

    TDataStatistics GetDataStatistics() const override
    {
        if (!UnderlyingReaderInitialized_.load()) {
            return {};
        }
        return UnderlyingReader_->GetDataStatistics();
    }

    TCodecStatistics GetDecompressionStatistics() const override
    {
        if (!UnderlyingReaderInitialized_.load()) {
            return {};
        }
        return UnderlyingReader_->GetDecompressionStatistics();
    }

    TFuture<void> Open() override
    {
        if (UnderlyingReaderInitialized_.load()) {
            // InitializationFuture_ may actually be unset yet.
            return UnderlyingReader_->Open();
        }

        return InitializationFuture_.Apply(BIND([=, this, this_ = MakeStrong(this)] {
            YT_VERIFY(UnderlyingReaderInitialized_.load());
            return UnderlyingReader_->Open();
        })
            .AsyncVia(GetCurrentInvoker()));
    }

    TFuture<void> GetReadyEvent() const override
    {
        YT_VERIFY(UnderlyingReaderInitialized_.load());
        return UnderlyingReader_->GetReadyEvent();
    }

    bool IsFetchingCompleted() const override
    {
        YT_ABORT();
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return {};
    }

    IVersionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        YT_VERIFY(UnderlyingReaderInitialized_.load());
        return UnderlyingReader_->Read(options);
    }

private:
    const int SkippedBefore_;
    const int SkippedAfter_;

    TFuture<void> InitializationFuture_;
    std::atomic<bool> UnderlyingReaderInitialized_ = false;
    IVersionedReaderPtr UnderlyingReader_;

    bool NeedKeyFilteringReader_ = false;
    std::vector<ui8> MissingKeyMask_;


    TFuture<void> InitializeUnderlyingReader(
        TSortedChunkStore* const chunk,
        const TTabletSnapshotPtr& tabletSnapshot,
        TSharedRange<TLegacyKey> keys,
        TTimestamp timestamp,
        bool produceAllVersions,
        const TColumnFilter& columnFilter,
        const TClientChunkReadOptions& chunkReadOptions,
        std::optional<EWorkloadCategory> workloadCategory)
    {
        const auto& mountConfig = tabletSnapshot->Settings.MountConfig;
        bool enableNewScanReader = IsNewScanReaderEnabled(mountConfig);

        // Check for in-memory reads.
        if (auto chunkState = chunk->FindPreloadedChunkState()) {
            MaybeWrapUnderlyingReader(
                chunk,
                chunk->CreateCacheBasedReader(
                    chunkState,
                    std::move(keys),
                    timestamp,
                    produceAllVersions,
                    columnFilter,
                    chunkReadOptions,
                    enableNewScanReader));
            return VoidFuture;
        }

        // Check for backing store.
        if (auto backingStore = chunk->GetSortedBackingStore()) {
            YT_VERIFY(!chunk->HasNontrivialReadRange());
            YT_VERIFY(!chunk->OverrideTimestamp_);
            UnderlyingReader_ = backingStore->CreateReader(
                std::move(tabletSnapshot),
                std::move(keys),
                timestamp,
                produceAllVersions,
                columnFilter,
                chunkReadOptions,
                /*workloadCategory*/ std::nullopt);
            UnderlyingReaderInitialized_.store(true);
            return VoidFuture;
        }

        auto backendReaders = chunk->GetBackendReaders(workloadCategory);

        if (mountConfig->EnableDataNodeLookup && backendReaders.OffloadingReader) {
            auto options = New<TOffloadingReaderOptions>(TOffloadingReaderOptions{
                .ChunkReadOptions = chunkReadOptions,
                .TableId = tabletSnapshot->TableId,
                .MountRevision = tabletSnapshot->MountRevision,
                .TableSchema = tabletSnapshot->TableSchema,
                .ColumnFilter = columnFilter,
                .Timestamp = timestamp,
                .ProduceAllVersions = produceAllVersions,
                .OverrideTimestamp = chunk->OverrideTimestamp_,
                .EnableHashChunkIndex = mountConfig->EnableHashChunkIndexForLookup
            });
            MaybeWrapUnderlyingReader(
                chunk,
                CreateVersionedOffloadingLookupReader(
                    std::move(backendReaders.OffloadingReader),
                    std::move(options),
                    std::move(keys)));
            return VoidFuture;
        }

        if (auto chunkMeta = chunk->FindCachedVersionedChunkMeta(enableNewScanReader)) {
            OnGotChunkMeta(
                chunk,
                tabletSnapshot,
                std::move(keys),
                timestamp,
                produceAllVersions,
                columnFilter,
                chunkReadOptions,
                std::move(backendReaders),
                std::move(chunkMeta));
            return VoidFuture;
        } else {
            return chunk->GetCachedVersionedChunkMeta(
                backendReaders.ChunkReader,
                chunkReadOptions,
                enableNewScanReader)
                .ApplyUnique(BIND([
                    =,
                    this,
                    this_ = MakeStrong(this),
                    // NB: We need to ref it here.
                    chunk = MakeStrong(chunk),
                    keys = std::move(keys),
                    backendReaders = std::move(backendReaders)
                ] (TCachedVersionedChunkMetaPtr&& chunkMeta) mutable {
                    OnGotChunkMeta(
                        chunk.Get(),
                        tabletSnapshot,
                        std::move(keys),
                        timestamp,
                        produceAllVersions,
                        // FIXME(akozhikhov): Heavy copy here.
                        columnFilter,
                        chunkReadOptions,
                        std::move(backendReaders),
                        std::move(chunkMeta));
                })
                .AsyncVia(GetCurrentInvoker()));
        }
    }

    void OnGotChunkMeta(
        TSortedChunkStore* const chunk,
        const TTabletSnapshotPtr& tabletSnapshot,
        TSharedRange<TLegacyKey> keys,
        TTimestamp timestamp,
        bool produceAllVersions,
        const TColumnFilter& columnFilter,
        const TClientChunkReadOptions& chunkReadOptions,
        TBackendReaders backendReaders,
        TCachedVersionedChunkMetaPtr chunkMeta)
    {
        // TODO(ifsmirnov, akozhikhov): Only for tests, to be removed in YT-18325.
        NeedKeyFilteringReader_ = PrepareKeyFilteringReader(
            tabletSnapshot,
            chunkMeta,
            backendReaders.ChunkReader,
            chunkReadOptions,
            &keys,
            &MissingKeyMask_);

        if (NeedKeyFilteringReader_ && std::count(MissingKeyMask_.begin(), MissingKeyMask_.end(), 0) == 0) {
            int initialKeyCount = SkippedBefore_ + SkippedAfter_ + keys.Size();
            UnderlyingReader_ = CreateEmptyVersionedReader(initialKeyCount);
            UnderlyingReaderInitialized_.store(true);
            return;
        }

        chunk->ValidateBlockSize(tabletSnapshot, chunkMeta, chunkReadOptions.WorkloadDescriptor);

        const auto& mountConfig = tabletSnapshot->Settings.MountConfig;

        if (mountConfig->EnableHashChunkIndexForLookup && chunkMeta->HashTableChunkIndexMeta()) {
            auto controller = CreateChunkIndexReadController(
                chunk->ChunkId_,
                columnFilter,
                std::move(chunkMeta),
                std::move(keys),
                chunk->GetKeyComparer(),
                tabletSnapshot->TableSchema,
                timestamp,
                produceAllVersions,
                chunk->BlockCache_,
                /*testingOptions*/ std::nullopt,
                TabletNodeLogger);

            MaybeWrapUnderlyingReader(
                chunk,
                CreateIndexedVersionedChunkReader(
                    std::move(chunkReadOptions),
                    std::move(controller),
                    std::move(backendReaders.ChunkReader),
                    tabletSnapshot->ChunkFragmentReader));
            return;
        }

        auto chunkState = chunk->PrepareChunkState(chunkMeta);

        if (IsNewScanReaderEnabled(mountConfig) &&
            chunkMeta->GetChunkFormat() == EChunkFormat::TableVersionedColumnar)
        {
            auto blockManagerFactory = NNewTableClient::CreateAsyncBlockWindowManagerFactory(
                std::move(backendReaders.ReaderConfig),
                std::move(backendReaders.ChunkReader),
                chunkState->BlockCache,
                std::move(chunkReadOptions),
                chunkMeta);

            MaybeWrapUnderlyingReader(
                chunk,
                NNewTableClient::CreateVersionedChunkReader(
                    std::move(keys),
                    timestamp,
                    std::move(chunkMeta),
                    chunkState->TableSchema,
                    std::move(columnFilter),
                    chunkState->ChunkColumnMapping,
                    std::move(blockManagerFactory),
                    produceAllVersions));
            return;
        }

        MaybeWrapUnderlyingReader(
            chunk,
            CreateVersionedChunkReader(
                std::move(backendReaders.ReaderConfig),
                std::move(backendReaders.ChunkReader),
                std::move(chunkState),
                std::move(chunkMeta),
                chunkReadOptions,
                std::move(keys),
                columnFilter,
                timestamp,
                produceAllVersions),
            /*needSetTimestamp*/ false);
        return;
    }

    void MaybeWrapUnderlyingReader(
        TSortedChunkStore* const chunk,
        IVersionedReaderPtr underlyingReader,
        bool needSetTimestamp = true)
    {
        // TODO(akozhikhov): Avoid extra wrappers TKeyFilteringReader and TFilteringReader,
        // implement their logic in this reader.
        if (NeedKeyFilteringReader_) {
            underlyingReader = New<TKeyFilteringReader>(
                std::move(underlyingReader),
                MissingKeyMask_);
        }

        if (SkippedBefore_ > 0 || SkippedAfter_ > 0) {
            underlyingReader = New<TFilteringReader>(
                std::move(underlyingReader),
                SkippedBefore_,
                SkippedAfter_);
        }

        if (needSetTimestamp && chunk->OverrideTimestamp_) {
            underlyingReader = CreateTimestampResettingAdapter(
                std::move(underlyingReader),
                chunk->OverrideTimestamp_);
        }

        UnderlyingReader_ = CreateVersionedPerformanceCountingReader(
            std::move(underlyingReader),
            chunk->PerformanceCounters_,
            NTableClient::EDataSource::ChunkStore,
            ERequestType::Lookup);
        UnderlyingReaderInitialized_.store(true);
    }
};

IVersionedReaderPtr TSortedChunkStore::CreateReader(
    const TTabletSnapshotPtr& tabletSnapshot,
    TSharedRange<TLegacyKey> keys,
    TTimestamp timestamp,
    bool produceAllVersions,
    const TColumnFilter& columnFilter,
    const TClientChunkReadOptions& chunkReadOptions,
    std::optional<EWorkloadCategory> workloadCategory)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (MaxClipTimestamp_) {
        timestamp = std::min(timestamp, MaxClipTimestamp_);
    }

    int initialKeyCount = keys.Size();

    if (OverrideTimestamp_ && OverrideTimestamp_ > timestamp) {
        return CreateEmptyVersionedReader(initialKeyCount);
    }

    int skippedBefore = 0;
    int skippedAfter = 0;
    auto filteredKeys = FilterKeysByReadRange(std::move(keys), &skippedBefore, &skippedAfter);
    YT_VERIFY(skippedBefore + skippedAfter + std::ssize(filteredKeys) == initialKeyCount);

    if (filteredKeys.Empty()) {
        return CreateEmptyVersionedReader(initialKeyCount);
    }

    // NB: This is a fast path for in-memory readers to avoid extra wrapper.
    // We could do the same for ext-memory readers but there is no need.
    bool needKeyRangeFiltering = skippedBefore > 0 || skippedAfter > 0;
    bool needTimestampResetting = OverrideTimestamp_;
    if (!needKeyRangeFiltering && !needTimestampResetting) {
        // Check for in-memory reads.
        if (auto chunkState = FindPreloadedChunkState()) {
            return CreateVersionedPerformanceCountingReader(
                CreateCacheBasedReader(
                    chunkState,
                    std::move(filteredKeys),
                    timestamp,
                    produceAllVersions,
                    columnFilter,
                    chunkReadOptions,
                    IsNewScanReaderEnabled(tabletSnapshot->Settings.MountConfig)),
                PerformanceCounters_,
                NTableClient::EDataSource::ChunkStore,
                ERequestType::Lookup);
        }

        // Check for backing store.
        if (auto backingStore = GetSortedBackingStore()) {
            YT_VERIFY(!HasNontrivialReadRange());
            YT_VERIFY(!OverrideTimestamp_);
            // NB: Performance counting reader is created within this call.
            return backingStore->CreateReader(
                std::move(tabletSnapshot),
                std::move(filteredKeys),
                timestamp,
                produceAllVersions,
                columnFilter,
                chunkReadOptions,
                /*workloadCategory*/ std::nullopt);
        }
    }

    return New<TSortedChunkStoreVersionedReader>(
        skippedBefore,
        skippedAfter,
        this,
        tabletSnapshot,
        std::move(filteredKeys),
        timestamp,
        produceAllVersions,
        columnFilter,
        chunkReadOptions,
        workloadCategory);
}

TSharedRange<TLegacyKey> TSortedChunkStore::FilterKeysByReadRange(
    TSharedRange<TLegacyKey> keys,
    int* skippedBefore,
    int* skippedAfter) const
{
    return NTabletNode::FilterKeysByReadRange(ReadRange_.Front(), std::move(keys), skippedBefore, skippedAfter);
}

TSharedRange<TRowRange> TSortedChunkStore::FilterRowRangesByReadRange(
    const TSharedRange<TRowRange>& ranges) const
{
    return NTabletNode::FilterRowRangesByReadRange(ReadRange_.Front(), ranges);
}

IVersionedReaderPtr TSortedChunkStore::CreateCacheBasedReader(
    const TChunkStatePtr& chunkState,
    TSharedRange<TLegacyKey> keys,
    TTimestamp timestamp,
    bool produceAllVersions,
    const TColumnFilter& columnFilter,
    const TClientChunkReadOptions& chunkReadOptions,
    bool enableNewScanReader) const
{
    const auto& chunkMeta = chunkState->ChunkMeta;

    if (enableNewScanReader && chunkMeta->GetChunkFormat() == EChunkFormat::TableVersionedColumnar) {
        auto blockManagerFactory = NNewTableClient::CreateSyncBlockWindowManagerFactory(
            chunkState->BlockCache,
            chunkMeta,
            ChunkId_);

        if (InMemoryMode_ == NTabletClient::EInMemoryMode::Uncompressed) {
            if (auto* lookupHashTable = chunkState->LookupHashTable.Get()) {
                auto keysWithHints = NNewTableClient::BuildKeyHintsUsingLookupTable(
                    *lookupHashTable,
                    std::move(keys));

                return NNewTableClient::CreateVersionedChunkReader(
                    std::move(keysWithHints),
                    timestamp,
                    chunkMeta,
                    Schema_,
                    columnFilter,
                    chunkState->ChunkColumnMapping,
                    std::move(blockManagerFactory),
                    produceAllVersions);
            }
        }

        return NNewTableClient::CreateVersionedChunkReader(
            std::move(keys),
            timestamp,
            chunkMeta,
            Schema_,
            columnFilter,
            chunkState->ChunkColumnMapping,
            std::move(blockManagerFactory),
            produceAllVersions);
    }

    return CreateCacheBasedVersionedChunkReader(
        ChunkId_,
        chunkState,
        chunkState->ChunkMeta,
        chunkReadOptions,
        std::move(keys),
        columnFilter,
        timestamp,
        produceAllVersions);
}

bool TSortedChunkStore::CheckRowLocks(
    TUnversionedRow row,
    TLockMask lockMask,
    TWriteContext* context)
{
    if (auto backingStore = GetSortedBackingStore()) {
        return backingStore->CheckRowLocks(row, lockMask, context);
    }

    auto* transaction = context->Transaction;
    context->Error = TError(
        NTabletClient::EErrorCode::CannotCheckConflictsAgainstChunkStore,
        "Checking for transaction conflicts against chunk stores is not supported; "
        "consider reducing transaction duration or increasing store retention time")
        << TErrorAttribute("transaction_id", transaction->GetId())
        << TErrorAttribute("transaction_start_time", transaction->GetStartTime())
        << TErrorAttribute("tablet_id", TabletId_)
        << TErrorAttribute("table_path", TablePath_)
        << TErrorAttribute("store_id", StoreId_)
        << TErrorAttribute("key", RowToKey(row));
    return false;
}

void TSortedChunkStore::Save(TSaveContext& context) const
{
    TStoreBase::Save(context);
    TChunkStoreBase::Save(context);

    using NYT::Save;
    Save(context, ChunkId_);
    Save(context, TLegacyOwningKey(ReadRange_[0].first));
    Save(context, TLegacyOwningKey(ReadRange_[0].second));
    Save(context, MaxClipTimestamp_);
}

void TSortedChunkStore::Load(TLoadContext& context)
{
    TStoreBase::Load(context);
    TChunkStoreBase::Load(context);

    using NYT::Load;

    Load(context, ChunkId_);

    auto lowerBound = Load<TLegacyOwningKey>(context);
    auto upperBound = Load<TLegacyOwningKey>(context);
    ReadRange_ = MakeSingletonRowRange(lowerBound, upperBound);

    Load(context, MaxClipTimestamp_);
}

IVersionedReaderPtr TSortedChunkStore::MaybeWrapWithTimestampResettingAdapter(
    IVersionedReaderPtr underlyingReader) const
{
    if (OverrideTimestamp_) {
        return CreateTimestampResettingAdapter(
            std::move(underlyingReader),
            OverrideTimestamp_);
    } else {
        return underlyingReader;
    }
}

NTableClient::TChunkColumnMappingPtr TSortedChunkStore::GetChunkColumnMapping(
    const NTableClient::TTableSchemaPtr& tableSchema,
    const NTableClient::TTableSchemaPtr& chunkSchema)
{
    // Fast lane.
    {
        auto guard = ReaderGuard(ChunkColumnMappingLock_);
        if (ChunkColumnMapping_) {
            return ChunkColumnMapping_;
        }
    }

    auto chunkColumnMapping = New<TChunkColumnMapping>(tableSchema, chunkSchema);

    {
        auto guard = WriterGuard(ChunkColumnMappingLock_);
        ChunkColumnMapping_ = chunkColumnMapping;
    }

    return chunkColumnMapping;
}

TChunkStatePtr TSortedChunkStore::PrepareChunkState(TCachedVersionedChunkMetaPtr meta)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TChunkSpec chunkSpec;
    ToProto(chunkSpec.mutable_chunk_id(), ChunkId_);

    auto chunkColumnMapping = GetChunkColumnMapping(Schema_, meta->ChunkSchema());

    auto chunkState = New<TChunkState>(
        BlockCache_,
        std::move(chunkSpec),
        std::move(meta),
        OverrideTimestamp_,
        /*lookupHashTable*/ nullptr,
        GetKeyComparer(),
        /*virtualValueDirectory*/ nullptr,
        Schema_,
        std::move(chunkColumnMapping));

    return chunkState;
}

void TSortedChunkStore::ValidateBlockSize(
    const TTabletSnapshotPtr& tabletSnapshot,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    const TWorkloadDescriptor& workloadDescriptor)
{
    auto chunkFormat = chunkMeta->GetChunkFormat();

    if ((workloadDescriptor.Category == EWorkloadCategory::UserInteractive ||
        workloadDescriptor.Category == EWorkloadCategory::UserRealtime) &&
        (chunkFormat == EChunkFormat::TableUnversionedSchemalessHorizontal ||
        chunkFormat == EChunkFormat::TableUnversionedColumnar))
    {
        // For unversioned chunks verify that block size is correct.
        const auto& mountConfig = tabletSnapshot->Settings.MountConfig;
        if (auto blockSizeLimit = mountConfig->MaxUnversionedBlockSize) {
            const auto& miscExt = chunkMeta->Misc();
            if (miscExt.max_data_block_size() > *blockSizeLimit) {
                THROW_ERROR_EXCEPTION("Maximum block size limit violated")
                    << TErrorAttribute("tablet_id", TabletId_)
                    << TErrorAttribute("chunk_id", GetId())
                    << TErrorAttribute("block_size", miscExt.max_data_block_size())
                    << TErrorAttribute("block_size_limit", *blockSizeLimit);
            }
        }
    }
}

const TKeyComparer& TSortedChunkStore::GetKeyComparer() const
{
    return KeyComparer_;
}

ISortedStorePtr TSortedChunkStore::GetSortedBackingStore() const
{
    auto backingStore = GetBackingStore();
    return backingStore ? backingStore->AsSorted() : nullptr;
}

////////////////////////////////////////////////////////////////////////////////

TSharedRange<TLegacyKey> FilterKeysByReadRange(
    const NTableClient::TRowRange& readRange,
    TSharedRange<TLegacyKey> keys,
    int* skippedBefore,
    int* skippedAfter)
{
    int begin = 0;
    int end = keys.Size();

    if (const auto& lowerLimit = readRange.first) {
        begin = std::lower_bound(
            keys.begin(),
            keys.end(),
            lowerLimit) - keys.begin();
    }

    if (const auto& upperLimit = readRange.second) {
        end = std::lower_bound(
            keys.begin(),
            keys.end(),
            upperLimit) - keys.begin();
    }

    *skippedBefore = begin;
    *skippedAfter = keys.Size() - end;

    return MakeSharedRange(
        static_cast<TRange<TLegacyKey>>(keys).Slice(begin, end),
        std::move(keys.ReleaseHolder()));
}

TSharedRange<NTableClient::TRowRange> FilterRowRangesByReadRange(
    const NTableClient::TRowRange& readRange,
    const TSharedRange<NTableClient::TRowRange>& ranges)
{
    int begin = 0;
    int end = ranges.Size();

    if (const auto& lowerLimit = readRange.first) {
        begin = std::lower_bound(
            ranges.begin(),
            ranges.end(),
            lowerLimit,
            [] (const auto& range, const auto& key) {
                return range.second <= key;
            }) - ranges.begin();
    }

    if (const auto& upperLimit = readRange.second) {
        end = std::lower_bound(
            ranges.begin(),
            ranges.end(),
            upperLimit,
            [] (const auto& range, const auto& key) {
                return range.first < key;
            }) - ranges.begin();
    }

    return ranges.Slice(begin, end);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode

