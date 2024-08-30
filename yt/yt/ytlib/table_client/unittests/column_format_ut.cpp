#include "column_format_ut.h"

namespace NYT::NTableClient {

using namespace NTableChunkFormat;
using namespace NTableChunkFormat::NProto;

////////////////////////////////////////////////////////////////////////////////

TSingleColumnWriter::TSingleColumnWriter(TWriterCreatorFunc writerCreator)
    : ValueColumnWriter_(writerCreator(&BlockWriter_))
{ }

std::pair<TSharedRef, TColumnMeta> TSingleColumnWriter::WriteSingleSegmentBlock(
    const std::vector<TUnversionedOwningRow>& rows)
{
    std::vector<TUnversionedRow> nonOwningRows;
    nonOwningRows.reserve(rows.size());

    for (auto row : rows) {
        nonOwningRows.emplace_back(row);
    }

    ValueColumnWriter_->WriteUnversionedValues(nonOwningRows);
    ValueColumnWriter_->FinishCurrentSegment();
    RowCount_ += rows.size();

    auto block = BlockWriter_.DumpBlock(BlockIndex_, RowCount_);
    auto* codec = NCompression::GetCodec(NCompression::ECodec::None);
    const auto data = codec->Compress(block.Data);
    auto columnMeta = ValueColumnWriter_->ColumnMeta();

    return std::pair(data, columnMeta);
}

////////////////////////////////////////////////////////////////////////////////

TSingleColumnReader::TSingleColumnReader(TReaderFactory readerCreator)
    : Factory_(readerCreator)
{ }

std::vector<TUnversionedOwningRow> TSingleColumnReader::ReadBlock(
    const TSharedRef& data,
    const TColumnMeta& meta,
    ui16 columnId)
{
    auto reader = Factory_(meta, 0, columnId, std::nullopt, TColumnSchema());
    reader->SetCurrentBlock(data, 0);
    i64 totalRowCount = 0;
    for (const auto& segment : meta.segments()) {
        totalRowCount += segment.row_count();
    }

    TChunkedMemoryPool pool;
    std::vector<TMutableUnversionedRow> mutableRows;
    mutableRows.reserve(totalRowCount);
    for (auto i = 0; i < totalRowCount; ++i) {
        mutableRows.push_back(TMutableUnversionedRow::Allocate(&pool, 1));
    }

    reader->ReadValues(TMutableRange(mutableRows));

    std::vector<TUnversionedOwningRow> rows;
    rows.reserve(mutableRows.size());
    for (auto row : mutableRows) {
        rows.emplace_back(row);
    }
    return rows;
}

////////////////////////////////////////////////////////////////////////////////

TVersionedColumnTestBase::TVersionedColumnTestBase(TColumnSchema columnSchema)
    : ColumnSchema_(std::move(columnSchema))
{ }

void TVersionedColumnTestBase::SetUp()
{
    TDataBlockWriter blockWriter;
    MemoryTracker_ = New<TTestNodeMemoryTracker>(std::numeric_limits<i64>::max());
    auto columnWriter = CreateColumnWriter(&blockWriter, MemoryTracker_);

    Write(columnWriter.get());

    auto block = blockWriter.DumpBlock(0, 8);
    auto* codec = NCompression::GetCodec(NCompression::ECodec::None);
    Data_ = codec->Compress(block.Data);

    ColumnMeta_ = columnWriter->ColumnMeta();
}

std::unique_ptr<IVersionedColumnReader> TVersionedColumnTestBase::CreateColumnReader()
{
    auto reader = DoCreateColumnReader();
    reader->SetCurrentBlock(Data_, 0);
    return reader;
}

TVersionedRow TVersionedColumnTestBase::CreateRowWithValues(const std::vector<TVersionedValue>& values) const
{
    TVersionedRowBuilder builder(RowBuffer_);

    for (const auto& value : values) {
        builder.AddValue(value);
    }

    return builder.FinishRow();
}

void TVersionedColumnTestBase::WriteSegment(IValueColumnWriter* columnWriter, const std::vector<TVersionedRow>& rows)
{
    columnWriter->WriteVersionedValues(TRange(rows));
    columnWriter->FinishCurrentSegment();
}

void TVersionedColumnTestBase::Validate(
    const std::vector<TVersionedRow>& original,
    int beginRowIndex,
    int endRowIndex,
    NTableClient::TTimestamp timestamp)
{
    auto actual = AllocateRows(endRowIndex - beginRowIndex);

    auto originalRange = TRange<NTableClient::TVersionedRow>(
        original.data() + beginRowIndex,
        original.data() + endRowIndex);

    auto expected = GetExpectedRows(originalRange, timestamp);

    auto timestampIndexRanges = GetTimestampIndexRanges(originalRange, timestamp);

    auto reader = CreateColumnReader();
    reader->SkipToRowIndex(beginRowIndex);
    reader->ReadValues(
        TMutableRange<NTableClient::TMutableVersionedRow>(actual.data(), actual.size()),
        TRange(timestampIndexRanges),
        false);


    ASSERT_EQ(expected.size(), actual.size());
    for (int rowIndex = 0; rowIndex < std::ssize(expected); ++rowIndex) {
        auto expectedRow = expected[rowIndex];
        auto actualRow = actual[rowIndex];

        ASSERT_EQ(expectedRow.GetValueCount(), actualRow.GetValueCount()) << Format("Row index - %v", rowIndex);
        for (int valueIndex = 0; valueIndex < expectedRow.GetValueCount(); ++valueIndex) {
            ValidateValues(
                expectedRow.Values()[valueIndex],
                actualRow.Values()[valueIndex],
                rowIndex);
        }
    }
}

void TVersionedColumnTestBase::ValidateValues(const TVersionedValue& expected, const TVersionedValue& actual, i64 rowIndex)
{
    ASSERT_TRUE(TBitwiseVersionedValueEqual()(expected, actual)) << Format("Row index - %v", rowIndex);
}

std::vector<TMutableVersionedRow> TVersionedColumnTestBase::AllocateRows(int count)
{
    std::vector<TMutableVersionedRow> rows;
    while (std::ssize(rows) < count) {
        rows.push_back(TMutableVersionedRow::Allocate(&Pool_, 0, MaxValueCount, 0, 0));
        rows.back().SetValueCount(0);
    }
    return rows;
}

std::vector<TVersionedRow> TVersionedColumnTestBase::GetExpectedRows(
    TRange<TVersionedRow> rows,
    TTimestamp timestamp) const
{
    std::vector<TVersionedRow> expected;
    for (auto row : rows) {
        // Find delete timestamp.
        auto deleteTimestamp = NullTimestamp;
        for (auto currentTimestamp : row.DeleteTimestamps()) {
            if (currentTimestamp <= timestamp) {
                deleteTimestamp = std::max(currentTimestamp, deleteTimestamp);
            }
        }

        // Find values.
        std::vector<TVersionedValue> values;
        for (const auto& value : row.Values()) {
            if (value.Id == ColumnId &&
                value.Timestamp <= timestamp &&
                value.Timestamp > deleteTimestamp)
            {
                values.push_back(value);
            }
        }

        // Build row.
        TVersionedRowBuilder builder(RowBuffer_);
        for (const auto& value : values) {
            builder.AddValue(value);
            if (!ColumnSchema_.Aggregate()) {
                break;
            }
        }
        auto expectedRow = builder.FinishRow();

        // Replace timestamps with indexes.
        for (auto& value : expectedRow.Values()) {
            for (int timestampIndex = 0; timestampIndex < row.GetWriteTimestampCount(); ++timestampIndex) {
                if (value.Timestamp == row.WriteTimestamps()[timestampIndex]) {
                    value.Timestamp = timestampIndex;
                }
            }
        }

        expected.push_back(expectedRow);
    }
    return expected;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
