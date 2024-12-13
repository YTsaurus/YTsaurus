#include "chunk_file_reader.h"
#include "chunk_fragment.h"
#include "helpers.h"
#include "private.h"

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/format.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>

#include <yt/yt/core/misc/checksum.h>
#include <yt/yt/core/misc/fs.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NIO {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = IOLogger;

////////////////////////////////////////////////////////////////////////////////

struct TChunkFileReaderDataBufferTag
{ };

struct TChunkFileReaderMetaBufferTag
{ };

namespace {

template <class T>
void ReadHeader(
    const TSharedRef& metaFileBlob,
    const TString& fileName,
    TChunkMetaHeader_2* metaHeader,
    TRef* metaBlob)
{
    if (metaFileBlob.Size() < sizeof(T)) {
        THROW_ERROR_EXCEPTION("Chunk meta file %v is too short: at least %v bytes expected",
            fileName,
            sizeof(T));
    }
    *static_cast<T*>(metaHeader) = *reinterpret_cast<const T*>(metaFileBlob.Begin());
    *metaBlob = metaFileBlob.Slice(sizeof(T), metaFileBlob.Size());
}

} // namespace

TChunkFileReader::TChunkFileReader(
    IIOEnginePtr ioEngine,
    TChunkId chunkId,
    TString fileName,
    bool validateBlocksChecksums,
    IBlocksExtCache* blocksExtCache)
    : IOEngine_(std::move(ioEngine))
    , ChunkId_(chunkId)
    , FileName_(std::move(fileName))
    , ValidateBlockChecksums_(validateBlocksChecksums)
    , BlocksExtCache_(std::move(blocksExtCache))
{ }

TFuture<std::vector<TBlock>> TChunkFileReader::ReadBlocks(
    const TClientChunkReadOptions& options,
    const std::vector<int>& blockIndexes,
    TBlocksExtPtr blocksExt)
{
    std::vector<TFuture<std::vector<TBlock>>> futures;
    auto count = std::ssize(blockIndexes);

    try {
        // Extract maximum contiguous ranges of blocks.
        int localIndex = 0;
        while (localIndex < count) {
            int startLocalIndex = localIndex;
            int startBlockIndex = blockIndexes[startLocalIndex];
            int endLocalIndex = startLocalIndex;
            while (endLocalIndex < count &&
                   blockIndexes[endLocalIndex] == startBlockIndex + (endLocalIndex - startLocalIndex))
            {
                ++endLocalIndex;
            }

            int blockCount = endLocalIndex - startLocalIndex;
            auto subfuture = DoReadBlocks(options, startBlockIndex, blockCount, blocksExt);
            futures.push_back(std::move(subfuture));

            localIndex = endLocalIndex;
        }
    } catch (const std::exception& ex) {
        TError error(ex);
        for (const auto& future : futures) {
            future.Cancel(error);
        }
        return MakeFuture<std::vector<TBlock>>(error);
    }

    return AllSet(std::move(futures))
        .Apply(BIND([count] (const std::vector<TErrorOr<std::vector<TBlock>>>& result) {
            std::vector<TBlock> blocks;
            blocks.reserve(count);

            for (const auto& subblocksOrError : result) {
                const auto& subblocks = subblocksOrError.ValueOrThrow();
                blocks.insert(blocks.end(), subblocks.begin(), subblocks.end());
            }

            return blocks;
        }));
}

TFuture<std::vector<TBlock>> TChunkFileReader::ReadBlocks(
    const TClientChunkReadOptions& options,
    int firstBlockIndex,
    int blockCount,
    TBlocksExtPtr blocksExt)
{
    YT_VERIFY(firstBlockIndex >= 0);

    try {
        return DoReadBlocks(options, firstBlockIndex, blockCount, std::move(blocksExt));
    } catch (const std::exception& ex) {
        return MakeFuture<std::vector<TBlock>>(ex);
    }
}

TFuture<TRefCountedChunkMetaPtr> TChunkFileReader::GetMeta(
    const TClientChunkReadOptions& options,
    std::optional<int> partitionTag)
{
    try {
        return DoReadMeta(options, partitionTag);
    } catch (const std::exception& ex) {
        return MakeFuture<TRefCountedChunkMetaPtr>(ex);
    }
}

TChunkId TChunkFileReader::GetChunkId() const
{
    return ChunkId_;
}

TFuture<void> TChunkFileReader::PrepareToReadChunkFragments(
    const TClientChunkReadOptions& options,
    bool useDirectIO)
{
    auto directIOFlag = GetDirectIOFlag(useDirectIO);

    // Fast path.
    if (ChunkFragmentReadsPrepared_[directIOFlag].load()) {
        return {};
    }

    // Slow path.
    TFuture<void> chunkFragmentFuture;

    auto guard = Guard(ChunkFragmentReadsLock_);
    if (!ChunkFragmentReadsPreparedFuture_[directIOFlag]) {
        auto chunkFragmentPromise = NewPromise<void>();
        ChunkFragmentReadsPreparedFuture_[directIOFlag] = chunkFragmentPromise;
        guard.Release();

        chunkFragmentFuture = OpenDataFile(options, directIOFlag)
            .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TIOEngineHandlePtr& /*handle*/) {
                {
                    auto guard = Guard(ChunkFragmentReadsLock_);

                    if (BlocksExt_) {
                        ChunkFragmentReadsPrepared_[directIOFlag].store(true);
                        return VoidFuture;
                    }

                    if (BlocksExtCache_) {
                        BlocksExt_ = BlocksExtCache_->Find();
                    }

                    if (BlocksExt_) {
                        ChunkFragmentReadsPrepared_[directIOFlag].store(true);
                        return VoidFuture;
                    }
                }

                return DoReadMeta(options, std::nullopt)
                    .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TRefCountedChunkMetaPtr& meta) {
                        auto guard = Guard(ChunkFragmentReadsLock_);
                        BlocksExt_ = New<NIO::TBlocksExt>(GetProtoExtension<NChunkClient::NProto::TBlocksExt>(meta->extensions()));
                        if (BlocksExtCache_) {
                            BlocksExtCache_->Put(meta, BlocksExt_);
                        }
                        ChunkFragmentReadsPrepared_[directIOFlag].store(true);
                    }));
            }))
            .ToUncancelable();

        chunkFragmentPromise.SetFrom(chunkFragmentFuture);
    } else {
        chunkFragmentFuture = ChunkFragmentReadsPreparedFuture_[directIOFlag];
    }
    return chunkFragmentFuture;
}

IIOEngine::TReadRequest TChunkFileReader::MakeChunkFragmentReadRequest(
    const TChunkFragmentDescriptor& fragmentDescriptor,
    bool useDirectIO)
{
    auto directIOFlag = GetDirectIOFlag(useDirectIO);
    if (!ChunkFragmentReadsPrepared_[directIOFlag].load()) {
        directIOFlag = directIOFlag == EDirectIOFlag::On ? EDirectIOFlag::Off : EDirectIOFlag::On;
    }

    YT_ASSERT(ChunkFragmentReadsPrepared_[directIOFlag].load());

    auto makeErrorAttributes = [&] {
        return std::vector{
            TErrorAttribute("chunk_id", ChunkId_),
            TErrorAttribute("block_index", fragmentDescriptor.BlockIndex),
            TErrorAttribute("block_offset", fragmentDescriptor.BlockOffset),
            TErrorAttribute("length", fragmentDescriptor.Length)
        };
    };

    if (fragmentDescriptor.BlockIndex < 0 ||
        fragmentDescriptor.BlockIndex >= std::ssize(BlocksExt_->Blocks))
    {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::MalformedReadRequest,
            "Invalid block index in fragment descriptor")
            << makeErrorAttributes()
            << TErrorAttribute("block_count", BlocksExt_->Blocks.size());
    }

    const auto& blockInfo = BlocksExt_->Blocks[fragmentDescriptor.BlockIndex];

    auto requestLength = fragmentDescriptor.Length;
    if (requestLength == WholeBlockFragmentRequestLength) {
        requestLength = blockInfo.Size;
    } else if (requestLength < 0) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::MalformedReadRequest,
            "Negative length in fragment descriptor")
            << makeErrorAttributes();
    }

    if (fragmentDescriptor.BlockOffset < 0 ||
        fragmentDescriptor.BlockOffset + requestLength > blockInfo.Size)
    {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::MalformedReadRequest,
            "Fragment is out of block range")
            << makeErrorAttributes()
            << TErrorAttribute("block_size", blockInfo.Size);
    }

    return IIOEngine::TReadRequest{
        .Handle = DataFileHandle_[directIOFlag],
        .Offset = blockInfo.Offset + fragmentDescriptor.BlockOffset,
        .Size = requestLength,
    };
}

std::vector<TBlock> TChunkFileReader::OnBlocksRead(
    const TClientChunkReadOptions& options,
    int firstBlockIndex,
    int blockCount,
    const NIO::TBlocksExtPtr& blocksExt,
    const IIOEngine::TReadResponse& readResponse)
{
    YT_VERIFY(readResponse.OutputBuffers.size() == 1);
    const auto& buffer = readResponse.OutputBuffers[0];

    options.ChunkReaderStatistics->DataBytesReadFromDisk.fetch_add(
        buffer.Size(),
        std::memory_order::relaxed);
    options.ChunkReaderStatistics->DataIORequests.fetch_add(
        readResponse.IORequests,
        std::memory_order::relaxed
    );

    const auto& firstBlockInfo = blocksExt->Blocks[firstBlockIndex];

    std::vector<TBlock> blocks;
    blocks.reserve(blockCount);

    for (int localIndex = 0; localIndex < blockCount; ++localIndex) {
        int blockIndex = firstBlockIndex + localIndex;
        const auto& blockInfo = blocksExt->Blocks[blockIndex];
        auto block = buffer.Slice(
            blockInfo.Offset - firstBlockInfo.Offset,
            blockInfo.Offset - firstBlockInfo.Offset + blockInfo.Size);
        if (ValidateBlockChecksums_) {
            auto checksum = GetChecksum(block);
            if (checksum != blockInfo.Checksum) {
                DumpBrokenBlock(blockIndex, blockInfo, block);
                THROW_ERROR_EXCEPTION(
                    NChunkClient::EErrorCode::IncorrectChunkFileChecksum,
                    "Incorrect checksum of block %v in chunk data file %v: expected %v, actual %v",
                    blockIndex,
                    FileName_,
                    blockInfo.Checksum,
                    checksum)
                    << TErrorAttribute("first_block_index", firstBlockIndex)
                    << TErrorAttribute("block_count", blockCount);
            }
        }
        blocks.push_back(TBlock(block, blockInfo.Checksum));
    }

    return blocks;
}

TFuture<std::vector<TBlock>> TChunkFileReader::DoReadBlocks(
    const TClientChunkReadOptions& options,
    int firstBlockIndex,
    int blockCount,
    NIO::TBlocksExtPtr blocksExt,
    TIOEngineHandlePtr dataFile)
{
    if (!blocksExt && BlocksExtCache_) {
        blocksExt = BlocksExtCache_->Find();
    }

    if (!blocksExt) {
        return DoReadMeta(options, std::nullopt)
            .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TRefCountedChunkMetaPtr& meta) {
                auto loadedBlocksExt = New<NIO::TBlocksExt>(GetProtoExtension<NChunkClient::NProto::TBlocksExt>(meta->extensions()));
                if (BlocksExtCache_) {
                    BlocksExtCache_->Put(meta, loadedBlocksExt);
                }
                return DoReadBlocks(options, firstBlockIndex, blockCount, loadedBlocksExt, dataFile);
            }))
            .ToUncancelable();
    }

    if (!dataFile) {
        auto asyncDataFile = OpenDataFile(options, GetDirectIOFlag(/*useDirectIO*/ false));
        auto optionalDataFileOrError = asyncDataFile.TryGet();
        if (!optionalDataFileOrError || !optionalDataFileOrError->IsOK()) {
            return asyncDataFile
                .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TIOEngineHandlePtr& dataFile) {
                    return DoReadBlocks(options, firstBlockIndex, blockCount, blocksExt, dataFile);
                }));
        }
        dataFile = optionalDataFileOrError->Value();
    }

    int chunkBlockCount = std::ssize(blocksExt->Blocks);
    if (firstBlockIndex + blockCount > chunkBlockCount) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::MalformedReadRequest,
            "Requested to read blocks [%v,%v] from chunk %v while only %v blocks exist",
            firstBlockIndex,
            firstBlockIndex + blockCount - 1,
            FileName_,
            chunkBlockCount);
    }

    // Read all blocks within a single request.
    int lastBlockIndex = firstBlockIndex + blockCount - 1;
    const auto& firstBlockInfo = blocksExt->Blocks[firstBlockIndex];
    const auto& lastBlockInfo = blocksExt->Blocks[lastBlockIndex];
    i64 totalSize = lastBlockInfo.Offset + lastBlockInfo.Size - firstBlockInfo.Offset;

    struct TChunkFileReaderBufferTag
    { };
    return IOEngine_->Read({{
            dataFile,
            firstBlockInfo.Offset,
            totalSize
        }},
        options.WorkloadDescriptor,
        GetRefCountedTypeCookie<TChunkFileReaderBufferTag>(),
        options.ReadSessionId,
        options.UseDedicatedAllocations)
        .Apply(BIND(&TChunkFileReader::OnBlocksRead, MakeStrong(this), options, firstBlockIndex, blockCount, blocksExt));
}

TFuture<TRefCountedChunkMetaPtr> TChunkFileReader::DoReadMeta(
    const TClientChunkReadOptions& options,
    std::optional<int> partitionTag)
{
    // Partition tag filtering not implemented here
    // because there is no practical need.
    // Implement when necessary.
    YT_VERIFY(!partitionTag);

    auto metaFileName = FileName_ + ChunkMetaSuffix;

    YT_LOG_DEBUG("Started reading chunk meta file (FileName: %v)",
        metaFileName);

    return IOEngine_->ReadAll(metaFileName, options.WorkloadDescriptor, options.ReadSessionId)
        .Apply(BIND(&TChunkFileReader::OnMetaRead, MakeStrong(this), metaFileName, options.ChunkReaderStatistics));
}

TRefCountedChunkMetaPtr TChunkFileReader::OnMetaRead(
    const TString& metaFileName,
    TChunkReaderStatisticsPtr chunkReaderStatistics,
    const IIOEngine::TReadResponse& readResponse)
{
    YT_VERIFY(readResponse.OutputBuffers.size() == 1);
    const auto& metaFileBlob = readResponse.OutputBuffers[0];

    YT_LOG_DEBUG("Finished reading chunk meta file (FileName: %v)",
        metaFileName);

    if (metaFileBlob.Size() < sizeof (TChunkMetaHeaderBase)) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::BrokenChunkFileMeta,
            "Chunk meta file %v is too short: at least %v bytes expected",
            metaFileName,
            sizeof (TChunkMetaHeaderBase));
    }

    chunkReaderStatistics->MetaBytesReadFromDisk.fetch_add(
        metaFileBlob.Size(),
        std::memory_order::relaxed);
    chunkReaderStatistics->MetaIORequests.fetch_add(
        readResponse.IORequests,
        std::memory_order::relaxed);

    TChunkMetaHeader_2 metaHeader;
    TRef metaBlob;
    const auto* metaHeaderBase = reinterpret_cast<const TChunkMetaHeaderBase*>(metaFileBlob.Begin());

    switch (metaHeaderBase->Signature) {
        case TChunkMetaHeader_1::ExpectedSignature:
            ReadHeader<TChunkMetaHeader_1>(metaFileBlob, metaFileName, &metaHeader, &metaBlob);
            metaHeader.ChunkId = ChunkId_;
            break;

        case TChunkMetaHeader_2::ExpectedSignature:
            ReadHeader<TChunkMetaHeader_2>(metaFileBlob, metaFileName, &metaHeader, &metaBlob);
            break;

        default:
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::BrokenChunkFileMeta,
                "Incorrect header signature %x in chunk meta file %v",
                metaHeaderBase->Signature,
                metaFileName);
    }

    auto checksum = GetChecksum(metaBlob);
    if (checksum != metaHeader.Checksum) {
        DumpBrokenMeta(metaBlob);
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::BrokenChunkFileMeta,
            "Incorrect checksum in chunk meta file %v: expected %x, actual %x",
            metaFileName,
            metaHeader.Checksum,
            checksum)
            << TErrorAttribute("meta_file_length", metaFileBlob.Size());
    }

    if (ChunkId_ != NullChunkId && metaHeader.ChunkId != ChunkId_) {
        THROW_ERROR_EXCEPTION("Invalid chunk id in meta file %v: expected %v, actual %v",
            metaFileName,
            ChunkId_,
            metaHeader.ChunkId);
    }
    ChunkId_ = metaHeader.ChunkId;

    TChunkMeta meta;
    if (!TryDeserializeProtoWithEnvelope(&meta, metaBlob)) {
        THROW_ERROR_EXCEPTION("Failed to parse chunk meta file %v",
            metaFileName);
    }

    return New<TRefCountedChunkMeta>(std::move(meta));
}

TFuture<TIOEngineHandlePtr> TChunkFileReader::OpenDataFile(
    const TClientChunkReadOptions& options,
    EDirectIOFlag useDirectIO)
{
    auto guard = Guard(DataFileHandleLock_);
    if (!DataFileHandleFuture_[useDirectIO]) {
        YT_LOG_DEBUG("Started opening chunk data file (FileName: %v, DirectIO: %v)",
            FileName_, useDirectIO);

        EOpenMode mode = OpenExisting | RdOnly | CloseOnExec;
        if (useDirectIO == EDirectIOFlag::On) {
            TIOEngineHandle::MarkOpenForDirectIO(&mode);
        }
        DataFileHandleFuture_[useDirectIO] = IOEngine_->Open({FileName_, mode}, options.WorkloadDescriptor, static_cast<IIOEngine::TSessionId>(ChunkId_))
            .ToUncancelable()
            .Apply(BIND(&TChunkFileReader::OnDataFileOpened, MakeStrong(this), useDirectIO));
    }
    return DataFileHandleFuture_[useDirectIO];
}

TIOEngineHandlePtr TChunkFileReader::OnDataFileOpened(EDirectIOFlag useDirectIO, const TIOEngineHandlePtr& file)
{
    YT_LOG_DEBUG("Finished opening chunk data file (FileName: %v, Handle: %v, DirectIO: %v)",
        FileName_,
        static_cast<FHANDLE>(*file),
        useDirectIO);

    DataFileHandle_[useDirectIO] = file;

    return file;
}

EDirectIOFlag TChunkFileReader::GetDirectIOFlag(bool useDirectIO)
{
    auto policy = IOEngine_->UseDirectIOForReads();
    if (policy == EDirectIOPolicy::Never) {
        return EDirectIOFlag::Off;
    } else if (policy == EDirectIOPolicy::Always) {
        return EDirectIOFlag::On;
    } else {
        return useDirectIO ? EDirectIOFlag::On : EDirectIOFlag::Off;
    }
}

void TChunkFileReader::DumpBrokenMeta(TRef block) const
{
    auto fileName = FileName_ + ".broken.meta";
    TFile file(fileName, CreateAlways | WrOnly);
    file.Write(block.Begin(), block.Size());
    file.Flush();
}

void TChunkFileReader::DumpBrokenBlock(
    int blockIndex,
    const NIO::TBlockInfo& blockInfo,
    TRef block) const
{
    auto fileName = Format("%v.broken.%v.%v.%v.%v",
        FileName_,
        blockIndex,
        blockInfo.Offset,
        blockInfo.Size,
        blockInfo.Checksum);

    TFile file(fileName, CreateAlways | WrOnly);
    file.Write(block.Begin(), block.Size());
    file.Flush();
}

////////////////////////////////////////////////////////////////////////////////

TBlocksExt::TBlocksExt(const NChunkClient::NProto::TBlocksExt& message)
{
    Blocks.reserve(message.blocks_size());
    for (const auto& blockInfo : message.blocks()) {
        Blocks.push_back(TBlockInfo{
            .Offset = blockInfo.offset(),
            .Size = blockInfo.size(),
            .Checksum = blockInfo.checksum(),
        });
    }

    SyncOnClose = message.sync_on_close();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
