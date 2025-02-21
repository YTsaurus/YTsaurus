#pragma once

#include "io_engine.h"

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/ytlib/chunk_client/block.h>

#include <util/system/file.h>

#include <atomic>

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

struct TBlockInfo
{
    i64 Offset = 0;
    i64 Size = 0;
    ui64 Checksum = 0;
};

struct TBlocksExt final
{
    TBlocksExt() = default;

    explicit TBlocksExt(const NChunkClient::NProto::TBlocksExt& message);

    std::vector<TBlockInfo> Blocks;
    bool SyncOnClose = false;
};

DEFINE_REFCOUNTED_TYPE(TBlocksExt)

////////////////////////////////////////////////////////////////////////////////

struct IBlocksExtCache
{
    virtual ~IBlocksExtCache() = default;

    virtual TBlocksExtPtr Find() = 0;
    virtual void Put(
        const NChunkClient::TRefCountedChunkMetaPtr& chunkMeta,
        const TBlocksExtPtr& blocksExt) = 0;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EDirectIOFlag,
    (On)
    (Off)
);

////////////////////////////////////////////////////////////////////////////////

class TChunkFileReader
    : public virtual TRefCounted
{
public:
    //! Creates a new reader.
    /*!
     *  For chunk meta version 2+, #chunkId is validated against that stored
     *  in the meta file. Passing #NullChunkId in #chunkId suppresses this check.
     */
    TChunkFileReader(
        IIOEnginePtr ioEngine,
        NChunkClient::TChunkId chunkId,
        TString fileName,
        bool validateBlocksChecksums = true,
        IBlocksExtCache* blocksExtCache = nullptr);

    TFuture<std::vector<NChunkClient::TBlock>> ReadBlocks(
        const NChunkClient::TClientChunkReadOptions& options,
        const std::vector<int>& blockIndexes,
        NIO::TBlocksExtPtr blocksExt = nullptr);

    TFuture<std::vector<NChunkClient::TBlock>> ReadBlocks(
        const NChunkClient::TClientChunkReadOptions& options,
        int firstBlockIndex,
        int blockCount,
        NIO::TBlocksExtPtr blocksExt = nullptr);

    TFuture<NChunkClient::TRefCountedChunkMetaPtr> GetMeta(
        const NChunkClient::TClientChunkReadOptions& options,
        std::optional<int> partitionTag = std::nullopt);

    //! Returns the future indicating the moment when the underlying file is
    //! open and the relevant meta is read.
    /*!
     *  Returns null if already prepared.
     *  Blocks extensions are permanently cached in the reader after this call.
     *  If file has not been opened yet, will consider #useDirectIO as a hint to use DirectIO.
     */
    TFuture<void> PrepareToReadChunkFragments(
        const NChunkClient::TClientChunkReadOptions& options,
        bool useDirectIO);

    //! Reader must be prepared (see #PrepareToReadChunkFragments) prior to this call.
    IIOEngine::TReadRequest MakeChunkFragmentReadRequest(
        const TChunkFragmentDescriptor& fragmentDescriptor,
        bool useDirectIO);

    NChunkClient::TChunkId GetChunkId() const;

private:
    const IIOEnginePtr IOEngine_;
    NChunkClient::TChunkId ChunkId_;
    const TString FileName_;
    const bool ValidateBlockChecksums_;
    IBlocksExtCache* const BlocksExtCache_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, DataFileHandleLock_);
    TEnumIndexedVector<EDirectIOFlag, TFuture<TIOEngineHandlePtr>> DataFileHandleFuture_;
    TEnumIndexedVector<EDirectIOFlag, TIOEngineHandlePtr> DataFileHandle_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ChunkFragmentReadsLock_);
    TEnumIndexedVector<EDirectIOFlag, TFuture<void>> ChunkFragmentReadsPreparedFuture_;
    TEnumIndexedVector<EDirectIOFlag, std::atomic<bool>> ChunkFragmentReadsPrepared_;
    // Permanently caches blocks extension for readers with PrepareToReadChunkFragments invoked.
    NIO::TBlocksExtPtr BlocksExt_;

    TFuture<std::vector<NChunkClient::TBlock>> DoReadBlocks(
        const NChunkClient::TClientChunkReadOptions& options,
        int firstBlockIndex,
        int blockCount,
        NIO::TBlocksExtPtr blocksExt = nullptr,
        TIOEngineHandlePtr dataFile = nullptr);
    std::vector<NChunkClient::TBlock> OnBlocksRead(
        const NChunkClient::TClientChunkReadOptions& options,
        int firstBlockIndex,
        int blockCount,
        const NIO::TBlocksExtPtr& blocksExt,
        const IIOEngine::TReadResponse& readResponse);
    TFuture<NChunkClient::TRefCountedChunkMetaPtr> DoReadMeta(
        const NChunkClient::TClientChunkReadOptions& options,
        std::optional<int> partitionTag);
    NChunkClient::TRefCountedChunkMetaPtr OnMetaRead(
        const TString& metaFileName,
        NChunkClient::TChunkReaderStatisticsPtr chunkReaderStatistics,
        const TSharedRef& data);

    TFuture<TIOEngineHandlePtr> OpenDataFile(
        const NChunkClient::TClientChunkReadOptions& options,
        EDirectIOFlag useDirectIO);
    TIOEngineHandlePtr OnDataFileOpened(EDirectIOFlag useDirectIO, const TIOEngineHandlePtr& file);
    EDirectIOFlag GetDirectIOFlag(bool useDirectIO);

    void DumpBrokenBlock(
        int blockIndex,
        const NIO::TBlockInfo& blockInfo,
        TRef block) const;
    void DumpBrokenMeta(TRef block) const;
};

DEFINE_REFCOUNTED_TYPE(TChunkFileReader)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
