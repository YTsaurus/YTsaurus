#include "handler_base.h"

#include "config.h"
#include "profiler.h"

#include <yt/yt/ytlib/query_tracker_client/records/query.record.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/client/table_client/record_helpers.h>
#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/iterator/functools.h>

namespace NYT::NQueryTracker {

using namespace NApi;
using namespace NTransactionClient;
using namespace NLogging;
using namespace NConcurrency;
using namespace NQueryTrackerClient;
using namespace NQueryTrackerClient::NRecords;
using namespace NTableClient;
using namespace NFuncTools;
using namespace NYson;
using namespace NYTree;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static TLogger Logger("QueryHandler");

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

TSharedRef TruncateRowset(TSharedRange<NTableClient::TUnversionedRow>& rows)
{
    if (rows.Empty()) {
        return TSharedRef::MakeEmpty();
    }

    auto baseWeight = GetUnversionedRowByteSize(rows.Size());

    // Wire format doesn't provide result size before serializing, so we have to make some estimations and verify them.
    i64 estimationThresholds[] = {MaxStringValueLength, MaxStringValueLength - 1_MB, 1_MB, (i64)baseWeight + (i64)GetDataWeight(rows[0])};
    for (auto threshold : estimationThresholds) {
        auto weight = baseWeight;
        i64 rowCount = 0;
        for (const auto& row : rows) {
            if (weight + GetDataWeight(row) <= (size_t)threshold) {
                rowCount++;
                weight += GetDataWeight(row);
            } else {
                break;
            }
        }

        auto writer = CreateWireProtocolWriter();
        writer->WriteSchemafulRowset(rows.Slice(0, rowCount));
        auto refs = writer->Finish();
        struct THandlerTag { };
        auto truncatedRowset = MergeRefsToRef<THandlerTag>(refs);
        if (truncatedRowset.Size() <= MaxStringValueLength) {
            return truncatedRowset;
        }
    }

    return TSharedRef::MakeEmpty();
}

void ProcessRowset(TFinishedQueryResultPartial& newRecord, TWireRowset wireSchemaAndSchemafulRowset, i64 ValueLengthLimit)
{
    try {
        YT_LOG_DEBUG("Processing wire rowset");
        // Process schema.
        YT_LOG_DEBUG("Reading schema");
        TWireProtocolOptions readerOptions;
        readerOptions.MaxStringValueLength = ValueLengthLimit;
        readerOptions.MaxAnyValueLength = ValueLengthLimit;
        readerOptions.MaxCompositeValueLength = ValueLengthLimit;
        auto reader = CreateWireProtocolReader(wireSchemaAndSchemafulRowset.Rowset, TRowBufferPtr(), readerOptions);
        auto schema = reader->ReadTableSchema();
        auto schemaNode = ConvertToNode(schema);
        // Values in tables cannot have top-level attributes, but we do not need them anyway.
        schemaNode->MutableAttributes()->Clear();
        newRecord.Schema = ConvertToYsonString(schemaNode);

        // Process schemaful rowset.
        YT_LOG_DEBUG("Reading schemaful rowset");
        auto rowset = reader->Slice(reader->GetCurrent(), reader->GetEnd());
        auto schemaData = IWireProtocolReader::GetSchemaData(schema);
        TSharedRange<NTableClient::TUnversionedRow> rows;
        try {
            rows = reader->ReadSchemafulRowset(schemaData, /*captureValues*/ false);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Failed to read resulting rowset. Try using INSERT INTO to save result") << ex;
        }
        TDataStatistics dataStatistics;
        dataStatistics.set_row_count(rows.size());
        dataStatistics.set_data_weight(GetDataWeight(rows));
        newRecord.DataStatistics = ConvertToYsonString(dataStatistics);
        newRecord.IsTruncated = wireSchemaAndSchemafulRowset.IsTruncated;
        if (rowset.Size() <= MaxStringValueLength) {
            // Fast path. Copy full rowset.
            YT_LOG_DEBUG("Copying full rowset of size %v", rowset.Size());
            newRecord.Error = TError();
            newRecord.Rowset = TString(rowset.ToStringBuf());
        } else {
            // Slow path. Truncate rowset.
            YT_LOG_DEBUG("Truncating rowset of size %v", rowset.Size());
            newRecord.IsTruncated = true;
            auto truncatedRowset = TruncateRowset(rows);
            YT_LOG_DEBUG("Copying truncated rowset of size %v", truncatedRowset.Size());
            newRecord.Error = TError();
            newRecord.Rowset = TString(truncatedRowset.ToStringBuf());
        }
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Failed to save rowset");
        newRecord.Schema = ConvertToYsonString(TString());
        newRecord.DataStatistics = ConvertToYsonString(TDataStatistics());
        newRecord.IsTruncated = true;
        newRecord.Error = TError("Failed to save rowset") << ex;
        newRecord.Rowset = TString();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TQueryHandlerBase::TQueryHandlerBase(
    const IClientPtr& stateClient,
    const NYPath::TYPath& stateRoot,
    const IInvokerPtr controlInvoker,
    const TEngineConfigBasePtr& config,
    const NQueryTrackerClient::NRecords::TActiveQuery& activeQuery)
    : StateClient_(stateClient)
    , StateRoot_(stateRoot)
    , ControlInvoker_(std::move(controlInvoker))
    , Config_(config)
    , Query_(activeQuery.Query)
    , QueryId_(activeQuery.Key.QueryId)
    , Incarnation_(activeQuery.Incarnation)
    , User_(activeQuery.User)
    , Engine_(activeQuery.Engine)
    , SettingsNode_(ConvertToNode(activeQuery.Settings))
    , AcquisitionTime_(TInstant::Now())
    , Logger(NQueryTracker::Logger().WithTag("QueryId: %v, Engine: %v", activeQuery.Key.QueryId, activeQuery.Engine))
    , ProgressWriter_(New<TPeriodicExecutor>(ControlInvoker_, BIND(&TQueryHandlerBase::TryWriteProgress, MakeWeak(this)), Config_->QueryProgressWritePeriod))
{
    YT_LOG_INFO("Query handler instantiated");
}

void TQueryHandlerBase::StartProgressWriter()
{
    YT_LOG_INFO("Starting progress writer");
    ProgressWriter_->Start();
}

void TQueryHandlerBase::StopProgressWriter()
{
    YT_LOG_INFO("Stopping progress writer");
    if (ProgressWriter_) {
        YT_UNUSED_FUTURE(ProgressWriter_->Stop());
    }
}

std::pair<ITransactionPtr, TActiveQuery> TQueryHandlerBase::StartIncarnationTransaction(EQueryState previousState) const
{
    YT_LOG_DEBUG("Starting incarnation transaction");
    auto transaction = WaitFor(StateClient_->StartTransaction(ETransactionType::Tablet))
        .ValueOrThrow();
    TLookupRowsOptions options;
    options.Timestamp = transaction->GetStartTimestamp();
    const auto& idMapping = TActiveQueryDescriptor::Get()->GetIdMapping();
    options.ColumnFilter = {
        *idMapping.AssignedTracker,
        *idMapping.Engine,
        *idMapping.Incarnation,
        *idMapping.StartTime,
        *idMapping.State,
    };
    options.KeepMissingRows = true;
    TActiveQueryKey key{.QueryId = QueryId_};
    auto rowBuffer = New<TRowBuffer>();
    std::vector keys{
        key.ToKey(rowBuffer),
    };
    auto rowset = WaitFor(StateClient_->LookupRows(
        StateRoot_ + "/active_queries",
        TActiveQueryDescriptor::Get()->GetNameTable(),
        MakeSharedRange(std::move(keys), std::move(rowBuffer)),
        options))
        .ValueOrThrow()
        .Rowset;
    auto optionalRecords = ToOptionalRecords<TActiveQuery>(rowset);
    YT_VERIFY(optionalRecords.size() == 1);
    if (!optionalRecords[0]) {
        THROW_ERROR_EXCEPTION(
            NQueryTrackerClient::EErrorCode::IncarnationMismatch,
            "Query %v record is missing",
            QueryId_);
    }
    if (optionalRecords[0]->Incarnation != Incarnation_) {
        THROW_ERROR_EXCEPTION(
            NQueryTrackerClient::EErrorCode::IncarnationMismatch,
            "Query %v incarnation mismatch: expected %v, actual %v",
            QueryId_,
            Incarnation_,
            optionalRecords[0]->Incarnation);
    }
    if (optionalRecords[0]->State != previousState) {
        THROW_ERROR_EXCEPTION(
            NQueryTrackerClient::EErrorCode::StateMismatch,
            "Query %v is not in state %Qlv, actual state is %Qlv",
            QueryId_,
            previousState,
            optionalRecords[0]->State);
    }
    YT_LOG_DEBUG("Incarnation transaction started (TransactionId: %v)", transaction->GetId());
    return {transaction, *optionalRecords[0]};
}

void TQueryHandlerBase::OnProgress(TYsonString progress)
{
    YT_LOG_DEBUG("Query progress received (ProgressBytes: %v)", progress.AsStringBuf().size());

    auto guard = Guard(ProgressSpinLock_);
    std::swap(Progress_, progress);
    ProgressVersion_++;
}

void TQueryHandlerBase::OnQueryFailed(const TError& error)
{
    YT_LOG_INFO(error, "Query failed");

    while (true) {
        if (TryWriteQueryState(EQueryState::Failing, EQueryState::Running, error, {})) {
            break;
        }
        TDelayedExecutor::WaitForDuration(Config_->QueryStateWriteBackoff);
    }
}

void TQueryHandlerBase::OnQueryStarted()
{
    YT_LOG_INFO("Query started");

    while (true) {
        if (TryWriteQueryState(EQueryState::Running, EQueryState::Pending, {}, {})) {
            break;
        }
        TDelayedExecutor::WaitForDuration(Config_->QueryStateWriteBackoff);
    }
}

void TQueryHandlerBase::OnQueryThrottled()
{
    YT_LOG_INFO("Query throttled");

    while (true) {
        if (TryWriteQueryState(EQueryState::Pending, EQueryState::Running, {}, {})) {
            break;
        }
        TDelayedExecutor::WaitForDuration(Config_->QueryStateWriteBackoff);
    }
}

void TQueryHandlerBase::OnQueryCompleted(const std::vector<TErrorOr<TRowset>>& rowsetOrErrors)
{
    std::vector<TErrorOr<TWireRowset>> wireRowsetOrErrors;
    for (const auto& rowsetOrError : rowsetOrErrors) {
        if (rowsetOrError.IsOK()) {
            const auto& rowset = rowsetOrError.Value().Rowset;
            auto writer = CreateWireProtocolWriter();
            writer->WriteTableSchema(*rowset->GetSchema());
            writer->WriteSchemafulRowset(rowset->GetRows());
            auto refs = writer->Finish();
            struct THandlerTag { };
            auto result = MergeRefsToRef<THandlerTag>(refs);
            wireRowsetOrErrors.push_back(TWireRowset{.Rowset = std::move(result), .IsTruncated = rowsetOrError.Value().IsTruncated});
        } else {
            wireRowsetOrErrors.push_back(static_cast<TError>(rowsetOrError));
        }
    }
    OnQueryCompletedWire(wireRowsetOrErrors);
}

void TQueryHandlerBase::OnQueryCompletedWire(const std::vector<TErrorOr<TWireRowset>>& wireRowsetOrErrors)
{
    YT_LOG_INFO("Query completed (ResultCount: %v)", wireRowsetOrErrors.size());
    for (const auto& [index, wireRowsetOrError] : Enumerate(wireRowsetOrErrors)) {
        if (wireRowsetOrError.IsOK()) {
            YT_LOG_DEBUG("Result rowset (Index: %v, WireRowsetBytes: %v)",
                index,
                wireRowsetOrError.Value().Rowset.size());
        } else {
            YT_LOG_DEBUG("Result error (Index: %v, Error: %v)",
                index,
                static_cast<TError>(wireRowsetOrError));
        }
    }

    while (true) {
        if (TryWriteQueryState(EQueryState::Completing, EQueryState::Running, {}, wireRowsetOrErrors)) {
            break;
        }
        TDelayedExecutor::WaitForDuration(Config_->QueryStateWriteBackoff);
    }
}

void TQueryHandlerBase::TryWriteProgress()
{
    TYsonString progress;
    int progressVersion;
    {
        auto guard = Guard(ProgressSpinLock_);
        if (LastSavedProgressVersion_ == ProgressVersion_) {
            return;
        }
        progress = Progress_;
        progressVersion = ProgressVersion_;
    }

    YT_LOG_DEBUG("Trying to save progress (Version: %v)", progressVersion);
    try {
        auto transaction = StartIncarnationTransaction().first;
        auto rowBuffer = New<TRowBuffer>();
        {
            TActiveQueryPartial newRecord{
                .Key = {.QueryId = QueryId_},
                .Progress = progress,
            };
            std::vector newRows = {
                newRecord.ToUnversionedRow(rowBuffer, TActiveQueryDescriptor::Get()->GetIdMapping()),
            };
            transaction->WriteRows(
                StateRoot_ + "/active_queries",
                TActiveQueryDescriptor::Get()->GetNameTable(),
                MakeSharedRange(std::move(newRows), rowBuffer));
        }
        WaitFor(transaction->Commit())
            .ThrowOnError();

        LastSavedProgressVersion_ = progressVersion;

        YT_LOG_DEBUG("Query progress written");
    } catch (const std::exception& ex) {
        if (const auto* errorException = dynamic_cast<const TErrorException*>(&ex)) {
            if (errorException->Error().FindMatching(NQueryTrackerClient::EErrorCode::IncarnationMismatch)) {
                YT_LOG_INFO(ex, "Stopping trying to write query progress due to incarnation mismatch");
                Detach();
            }
        }
        YT_LOG_ERROR(ex, "Failed to write query progress");
    }
}

bool TQueryHandlerBase::TryWriteQueryState(EQueryState state, EQueryState previousState, const TError& error, const std::vector<TErrorOr<TWireRowset>>& wireRowsetOrErrors)
{
    try {
        YT_LOG_INFO("Writing query state (State: %v, PreviousState: %v)", state, previousState);
        ITransactionPtr transaction;
        TActiveQuery record;
        std::tie(transaction, record) = StartIncarnationTransaction(previousState);

        auto now = TInstant::Now();
        auto rowBuffer = New<TRowBuffer>();
        {
            TActiveQueryPartial newRecord{
                .Key = {.QueryId = QueryId_},
                .State = state,
                .Progress = Progress_,
                .Error = error,
            };
            if (state == EQueryState::Completing || state == EQueryState::Failing) {
                newRecord.FinishTime = now;
                newRecord.ResultCount = wireRowsetOrErrors.size();
            }
            if (state == EQueryState::Running) {
                newRecord.ExecutionStartTime = now;
            }

            std::vector newRows = {
                newRecord.ToUnversionedRow(rowBuffer, TActiveQueryDescriptor::Get()->GetIdMapping()),
            };
            YT_LOG_DEBUG("Writing active query state (FinishTime: %v, ResultCount: %v)",
                newRecord.FinishTime,
                newRecord.ResultCount);
            transaction->WriteRows(
                StateRoot_ + "/active_queries",
                TActiveQueryDescriptor::Get()->GetNameTable(),
                MakeSharedRange(std::move(newRows), rowBuffer));
        }
        {
            std::vector<TUnversionedRow> newRows;
            for (const auto& [index, wireRowsetOrError] : Enumerate(wireRowsetOrErrors)) {
                TFinishedQueryResultPartial newRecord{
                    .Key = {
                        .QueryId = QueryId_,
                        .Index = i64(index),
                    },
                };
                if (wireRowsetOrError.IsOK()) {
                    NDetail::ProcessRowset(newRecord, wireRowsetOrError.Value(), Config_->ResultingRowsetValueLengthLimit);
                } else {
                    newRecord.Error = static_cast<TError>(wireRowsetOrError);
                    newRecord.DataStatistics = ConvertToYsonString(TDataStatistics());
                }
                YT_LOG_DEBUG("Writing finished query result (Index: %v, ErrorMessageSize: %v, RowsetSize: %v)",
                    index,
                    newRecord.Error ? newRecord.Error->GetMessage().size() : 0,
                    newRecord.Rowset && *newRecord.Rowset ? (**newRecord.Rowset).size() : 0);
                newRows.push_back(newRecord.ToUnversionedRow(rowBuffer, TFinishedQueryResultDescriptor::Get()->GetIdMapping()));
            }
            YT_LOG_INFO("Writing finished query result");
            transaction->WriteRows(
                StateRoot_ + "/finished_query_results",
                TFinishedQueryResultDescriptor::Get()->GetNameTable(),
                MakeSharedRange(std::move(newRows), rowBuffer));
        }
        WaitFor(transaction->Commit())
            .ThrowOnError();

        StateTimes_[previousState] += now - LastStateChange_.value_or(AcquisitionTime_);
        LastStateChange_ = now;

        for (auto& [state, time] : StateTimes_) {
            // Save profile counter.
            auto& stateTimeGauge = GetOrCreateProfilingCounter<TStateTimeProfilingCounter>(
                QueryTrackerProfiler,
                ProfilingTagsFromActiveQueryRecord(record))->StateTime;
            stateTimeGauge.Update(time);
        }

        YT_LOG_INFO("Query state written (State: %v)", state);
        return true;
    } catch (const std::exception& ex) {
        if (const auto* errorException = dynamic_cast<const TErrorException*>(&ex)) {
            if (errorException->Error().FindMatching(NQueryTrackerClient::EErrorCode::IncarnationMismatch)) {
                YT_LOG_INFO(ex, "Stopping trying to write query state due to incarnation mismatch");
                return true;
            }
        }
        YT_LOG_ERROR(ex, "Failed to write query state, backing off");
        return false;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
