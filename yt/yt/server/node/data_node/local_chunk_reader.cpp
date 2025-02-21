#include "local_chunk_reader.h"
#include "chunk.h"
#include "chunk_store.h"
#include "private.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>

#include <yt/yt/server/lib/io/chunk_fragment.h>

#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/config.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NDataNode {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTableClient;
using namespace NDataNode;
using namespace NClusterNode;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NIO;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TLocalChunkReader
    : public IChunkReader
{
public:
    TLocalChunkReader(
        TReplicationReaderConfigPtr config,
        IChunkPtr chunk,
        IBlockCachePtr blockCache,
        TBlockMetaCachePtr blockMetaCache)
        : Config_(std::move(config))
        , Chunk_(std::move(chunk))
        , BlockCache_(std::move(blockCache))
        , BlockMetaCache_(std::move(blockMetaCache))
    { }

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TReadBlocksOptions& options,
        const std::vector<int>& blockIndexes) override
    {
        auto session = New<TReadBlockSetSession>();
        static_cast<TClientChunkReadOptions&>(session->Options) = options.ClientOptions;
        session->Options.BlockCache = BlockCache_;
        session->Options.PopulateCache = Config_->PopulateCache;
        session->BlockIndexes = blockIndexes;
        session->Blocks.resize(blockIndexes.size());

        RequestBlockSet(session);

        return session->Promise.ToFuture();
    }

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TReadBlocksOptions& options,
        int firstBlockIndex,
        int blockCount) override
    {
        TChunkReadOptions chunkReadOptions;
        static_cast<TClientChunkReadOptions&>(chunkReadOptions) = options.ClientOptions;
        chunkReadOptions.BlockCache = BlockCache_;
        chunkReadOptions.PopulateCache = Config_->PopulateCache;

        auto asyncResult = Chunk_->ReadBlockRange(
            firstBlockIndex,
            blockCount,
            chunkReadOptions);

        return asyncResult.ApplyUnique(BIND(
            [=, this, this_ = MakeStrong(this)]
            (TErrorOr<std::vector<TBlock>>&& blocksOrError)
            {
                if (!blocksOrError.IsOK()) {
                    ThrowError(blocksOrError);
                }
                return std::move(blocksOrError.Value());
            }));
    }

    TFuture<TRefCountedChunkMetaPtr> GetMeta(
        const TClientChunkReadOptions& clientOptions,
        std::optional<int> partitionTag,
        const std::optional<std::vector<int>>& extensionTags) override
    {
        TChunkReadOptions options;
        static_cast<TClientChunkReadOptions&>(options) = clientOptions;

        auto asyncResult = Chunk_->ReadMeta(options, extensionTags);
        return asyncResult.Apply(BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<TRefCountedChunkMetaPtr>& metaOrError) {
            if (!metaOrError.IsOK()) {
                ThrowError(metaOrError);
            }
            const auto& meta = metaOrError.Value();

            if (!partitionTag) {
                return meta;
            }

            auto cachedBlockMeta = BlockMetaCache_
                ? BlockMetaCache_->Find(GetChunkId())
                : TCachedBlockMetaPtr();

            if (!cachedBlockMeta) {
                auto blockMetaExt = GetProtoExtension<TDataBlockMetaExt>(meta->extensions());
                cachedBlockMeta = New<TCachedBlockMeta>(GetChunkId(), std::move(blockMetaExt));
                if (BlockMetaCache_) {
                    BlockMetaCache_->TryInsert(cachedBlockMeta);
                }
            }

            return New<TRefCountedChunkMeta>(FilterChunkMetaByPartitionTag(*meta, cachedBlockMeta, *partitionTag));
        }));
    }

    TChunkId GetChunkId() const override
    {
        return Chunk_->GetId();
    }

    TInstant GetLastFailureTime() const override
    {
        return TInstant();
    }

private:
    const TReplicationReaderConfigPtr Config_;
    const IChunkPtr Chunk_;
    const IBlockCachePtr BlockCache_;
    const TBlockMetaCachePtr BlockMetaCache_;

    struct TReadBlockSetSession
        : public TRefCounted
    {
        TChunkReadOptions Options;
        std::vector<int> BlockIndexes;
        std::vector<TBlock> Blocks;
        const TPromise<std::vector<TBlock>> Promise = NewPromise<std::vector<TBlock>>();
    };

    using TReadBlockSetSessionPtr = TIntrusivePtr<TReadBlockSetSession>;

    void RequestBlockSet(const TReadBlockSetSessionPtr& session)
    {
        try {
            std::vector<int> localIndexes;
            std::vector<int> blockIndexes;
            for (int index = 0; index < std::ssize(session->Blocks); ++index) {
                if (!session->Blocks[index]) {
                    localIndexes.push_back(index);
                    blockIndexes.push_back(session->BlockIndexes[index]);
                }
            }

            if (localIndexes.empty()) {
                session->Promise.Set(std::move(session->Blocks));
                return;
            }

            auto asyncResult = Chunk_->ReadBlockSet(
                blockIndexes,
                session->Options);
            asyncResult.Subscribe(
                BIND(&TLocalChunkReader::OnBlockSetRead, MakeStrong(this), session, localIndexes));
        } catch (const std::exception& ex) {
            session->Promise.Set(TError(ex));
        }
    }

    void OnBlockSetRead(
        TReadBlockSetSessionPtr session,
        const std::vector<int>& localIndexes,
        const TErrorOr<std::vector<TBlock>>& blocksOrError)
    {
        try {
            if (!blocksOrError.IsOK()) {
                ThrowError(blocksOrError);
            }

            const auto& blocks = blocksOrError.Value();
            for (int responseIndex = 0; responseIndex < std::ssize(blocks); ++responseIndex) {
                const auto& block = blocks[responseIndex];
                int localIndex = localIndexes[responseIndex];
                int blockIndex = session->BlockIndexes[localIndex];
                if (!block) {
                    ThrowError(TError("Block %v cannot be read",
                        TBlockId(Chunk_->GetId(), blockIndex)));
                }
                session->Blocks[localIndex] = block;
            }

            RequestBlockSet(session);
        } catch (const std::exception& ex) {
            session->Promise.Set(TError(ex));
        }
    }

    void ThrowError(const TError& error)
    {
        THROW_ERROR_EXCEPTION(
            NDataNode::EErrorCode::LocalChunkReaderFailed,
            "Error accessing local chunk %v",
            Chunk_->GetId())
            << error;
    }
};

////////////////////////////////////////////////////////////////////////////////

IChunkReaderPtr CreateLocalChunkReader(
    TReplicationReaderConfigPtr config,
    IChunkPtr chunk,
    IBlockCachePtr blockCache,
    TBlockMetaCachePtr blockMetaCache)
{
    return New<TLocalChunkReader>(
        std::move(config),
        std::move(chunk),
        std::move(blockCache),
        std::move(blockMetaCache));
}

////////////////////////////////////////////////////////////////////////////////

class TLocalChunkFragmentReader
    : public ILocalChunkFragmentReader
{
public:
    TLocalChunkFragmentReader(
        TChunkReadGuard guard,
        bool useDirectIO)
        : Guard_(std::move(guard))
        , UseDirectIO_(useDirectIO)
    { }

    TFuture<void> PrepareToReadChunkFragments(
        const NChunkClient::TClientChunkReadOptions& options) override
    {
        return Guard_.GetChunk()->PrepareToReadChunkFragments(options, UseDirectIO_);
    }

    TFuture<TReadFragmentsResponse> ReadFragments(
        TClientChunkReadOptions options,
        std::vector<TChunkFragmentRequest> requests) override
    {
        const auto& chunk = Guard_.GetChunk();

        i64 fragmentsSize = 0;
        std::vector<IIOEngine::TReadRequest> readRequests;
        readRequests.reserve(requests.size());
        for (const auto& request : requests) {
            YT_VERIFY(request.ChunkId == chunk->GetId());

            fragmentsSize += request.Length;
            readRequests.push_back(chunk->MakeChunkFragmentReadRequest(
                TChunkFragmentDescriptor{
                    .Length = static_cast<int>(request.Length),
                    .BlockIndex = request.BlockIndex,
                    .BlockOffset = request.BlockOffset
                },
                UseDirectIO_));
        }

        YT_LOG_DEBUG("Local chunk reader will read fragments "
            "(FragmentCount: %v, FragmentsSize: %v, UseDirectIO: %v, ReadSessionId: %v, ChunkId: %v)",
            readRequests.size(),
            fragmentsSize,
            UseDirectIO_,
            options.ReadSessionId,
            chunk->GetId());

        struct TChunkFragmentBufferTag
        { };

        const auto& ioEngine = chunk->GetLocation()->GetIOEngine();
        return ioEngine->Read(
            std::move(readRequests),
            options.WorkloadDescriptor,
            GetRefCountedTypeCookie<TChunkFragmentBufferTag>(),
            options.ReadSessionId)
            .ApplyUnique(BIND([
                // NB: Keep Guard_ alive.
                this_ = MakeStrong(this),
                options = std::move(options)
            ] (IIOEngine::TReadResponse&& response) {
                options.ChunkReaderStatistics->DataBytesReadFromDisk.fetch_add(
                    response.PaddedBytes,
                    std::memory_order::relaxed);
                options.ChunkReaderStatistics->DataIORequests.fetch_add(
                    response.IORequests,
                    std::memory_order::relaxed);

                return TReadFragmentsResponse{
                    .Fragments = std::move(response.OutputBuffers)
                };
            }));
    }

private:
    const TChunkReadGuard Guard_;
    const bool UseDirectIO_;
};

////////////////////////////////////////////////////////////////////////////////

ILocalChunkFragmentReaderPtr CreateLocalChunkFragmentReader(
    IChunkPtr chunk,
    bool useDirectIO)
{
    // TODO(akozhikhov): Stop throwing from this.
    auto guard = TChunkReadGuard::Acquire(std::move(chunk));
    return New<TLocalChunkFragmentReader>(std::move(guard), useDirectIO);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
