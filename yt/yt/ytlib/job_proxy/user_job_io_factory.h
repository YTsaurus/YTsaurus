#pragma once

#include "public.h"

#include <yt/yt_proto/yt/client/chunk_client/proto/data_statistics.pb.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>
#include <yt/yt/ytlib/chunk_client/data_sink.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_writer.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

struct IUserJobIOFactory
    : public virtual TRefCounted
{
    virtual NTableClient::ISchemalessMultiChunkReaderPtr CreateReader(
        TClosure onNetworkReleased,
        NTableClient::TNameTablePtr nameTable,
        const NTableClient::TColumnFilter& columnFilter) = 0;

    virtual NTableClient::ISchemalessMultiChunkWriterPtr CreateWriter(
        NApi::NNative::IClientPtr client,
        NTableClient::TTableWriterConfigPtr config,
        NTableClient::TTableWriterOptionsPtr options,
        NChunkClient::TChunkListId chunkListId,
        NTransactionClient::TTransactionId transactionId,
        NTableClient::TTableSchemaPtr tableSchema,
        NTableClient::TMasterTableSchemaId schemaId,
        const NTableClient::TChunkTimestamps& chunkTimestamps,
        const std::optional<NChunkClient::TDataSink>& dataSink,
        NChunkClient::IChunkWriter::TWriteBlocksOptions writeBlocksOptions) = 0;

    virtual std::optional<NChunkClient::NProto::TDataStatistics> GetPreparationDataStatistics() { return std::nullopt; }
};

DEFINE_REFCOUNTED_TYPE(IUserJobIOFactory)

////////////////////////////////////////////////////////////////////////////////

IUserJobIOFactoryPtr CreateUserJobIOFactory(
    const IJobSpecHelperPtr& jobSpecHelper,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    NChunkClient::TChunkReaderHostPtr chunkReaderHost,
    TString localHostName,
    NConcurrency::IThroughputThrottlerPtr outBandwidthThrottler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
