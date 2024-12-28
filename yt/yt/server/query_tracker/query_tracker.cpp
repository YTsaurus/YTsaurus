#include "query_tracker.h"

#include "config.h"
#include "engine.h"
#include "profiler.h"
#include "ql_engine.h"
#include "yql_engine.h"
#include "chyt_engine.h"
#include "mock_engine.h"
#include "spyt_engine.h"

#include <yt/yt/server/lib/state_checker/state_checker.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/transaction_client/transaction_manager.h>

#include <yt/yt/ytlib/query_tracker_client/records/query.record.h>
#include <yt/yt/ytlib/query_tracker_client/helpers.h>

#include <yt/yt/client/table_client/record_helpers.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ypath_proxy.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/core/utilex/random.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NQueryTracker {

using namespace NAlertManager;
using namespace NApi;
using namespace NYPath;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NRecords;
using namespace NComponentStateChecker;
using namespace NTableClient;
using namespace NLogging;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NYson;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

static TLogger Logger("QueryTracker");

////////////////////////////////////////////////////////////////////////////////

class TQueryTracker
    : public IQueryTracker
{
public:
    TQueryTracker(
        TQueryTrackerDynamicConfigPtr config,
        std::string selfAddress,
        IInvokerPtr controlInvoker,
        IAlertCollectorPtr alertCollector,
        NApi::NNative::IClientPtr stateClient,
        IComponentStateCheckerPtr ComponentStateChecker,
        TYPath stateRoot,
        int minRequiredStateVersion)
        : SelfAddress_(std::move(selfAddress))
        , ControlInvoker_(std::move(controlInvoker))
        , AlertCollector_(std::move(alertCollector))
        , StateClient_(std::move(stateClient))
        , ComponentStateChecker_(std::move(ComponentStateChecker))
        , StateRoot_(std::move(stateRoot))
        , MinRequiredStateVersion_(minRequiredStateVersion)
        , AcquisitionExecutor_(New<TPeriodicExecutor>(
            ControlInvoker_,
            BIND(&TQueryTracker::AcquireQueries, MakeWeak(this))))
        , HealthCheckExecutor_(New<TPeriodicExecutor>(
            ControlInvoker_,
            BIND(&TQueryTracker::OnHealthCheck, MakeWeak(this)),
            config->HealthCheckPeriod))
    {
        Engines_[EQueryEngine::Mock] = CreateMockEngine(StateClient_, StateRoot_);
        Engines_[EQueryEngine::Ql] = CreateQLEngine(StateClient_, StateRoot_);
        Engines_[EQueryEngine::Yql] = CreateYqlEngine(StateClient_, StateRoot_);
        Engines_[EQueryEngine::Chyt] = CreateChytEngine(StateClient_, StateRoot_);
        Engines_[EQueryEngine::Spyt] = CreateSpytEngine(StateClient_, StateRoot_);
        // This is a correct call, despite being virtual call in constructor.
        TQueryTracker::Reconfigure(config);
    }

    void Start() override
    {
        AcquisitionExecutor_->Start();
        HealthCheckExecutor_->Start();

        ControlInvoker_->Invoke(BIND(&TQueryTracker::StartLeaseTransaction, MakeWeak(this)));
    }

    void Reconfigure(const TQueryTrackerDynamicConfigPtr& config) override
    {
        Config_ = config;
        AcquisitionExecutor_->SetPeriod(config->ActiveQueryAcquisitionPeriod);
        Engines_[EQueryEngine::Mock]->Reconfigure(config->MockEngine);
        Engines_[EQueryEngine::Ql]->Reconfigure(config->QLEngine);
        Engines_[EQueryEngine::Yql]->Reconfigure(config->YqlEngine);
        Engines_[EQueryEngine::Chyt]->Reconfigure(config->ChytEngine);
        Engines_[EQueryEngine::Spyt]->Reconfigure(config->SpytEngine);
    }

    IYPathServicePtr GetOrchidService() const override
    {
        auto producer = BIND(&TQueryTracker::DoBuildOrchid, MakeStrong(this));
        return IYPathService::FromProducer(producer);
    }

private:
    const std::string SelfAddress_;
    const IInvokerPtr ControlInvoker_;
    const IAlertCollectorPtr AlertCollector_;
    const NApi::NNative::IClientPtr StateClient_;
    const IComponentStateCheckerPtr ComponentStateChecker_;
    const TYPath StateRoot_;
    const int MinRequiredStateVersion_;

    const TPeriodicExecutorPtr AcquisitionExecutor_;
    const TPeriodicExecutorPtr HealthCheckExecutor_;

    NApi::ITransactionPtr LeaseTransaction_;

    TQueryTrackerDynamicConfigPtr Config_;

    THashMap<EQueryEngine, IQueryEnginePtr> Engines_;

    struct TAcquiredQuery
    {
        IQueryHandlerPtr Handler;
        i64 Incarnation;
        TTransactionId LeaseTransactionId;
    };

    THashMap<TQueryId, TAcquiredQuery> AcquiredQueries_;

    std::atomic<int> AcquisitionIterations_ = 0;

    void OnHealthCheck()
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);

        YT_LOG_INFO("Requesting query tracker state version");
        TGetNodeOptions options;
        options.ReadFrom = EMasterChannelKind::MasterCache;
        auto asyncResult = StateClient_->GetNode(StateRoot_ + "/@version", options);
        auto rspOrError = WaitFor(asyncResult);
        if (!rspOrError.IsOK()) {
            AlertCollector_->StageAlert(CreateAlert(
                NAlerts::EErrorCode::QueryTrackerInvalidState,
                "Erroneous query tracker state",
                /*tags*/ {},
                rspOrError));
        } else {
            int stateVersion = ConvertTo<int>(rspOrError.Value());
            if (stateVersion < MinRequiredStateVersion_) {
                auto alert = TError(NAlerts::EErrorCode::QueryTrackerInvalidState, "Min required state version is not met")
                    << TErrorAttribute("version", stateVersion)
                    << TErrorAttribute("min_required_version", MinRequiredStateVersion_);
                AlertCollector_->StageAlert(CreateAlert(
                    NAlerts::EErrorCode::QueryTrackerInvalidState,
                    "Erroneous query tracker state",
                    /*tags*/ {},
                    alert));
            }
        }

        AlertCollector_->PublishAlerts();
    }

    void AcquireQueries()
    {
        VERIFY_SERIALIZED_INVOKER_AFFINITY(ControlInvoker_);

        AcquisitionIterations_.fetch_add(1);

        auto traceContext = TTraceContext::NewRoot("QuerySelect");
        auto guard = TCurrentTraceContextGuard(traceContext);

        if (!LeaseTransaction_) {
            YT_LOG_DEBUG("Skip active queries acquisition, since lease transaction is not started");
            return;
        }

        if (ComponentStateChecker_->IsComponentBanned()) {
            YT_LOG_DEBUG("Skip active queries acquisition, since query tracker instance is banned");
            return;
        }

        YT_LOG_DEBUG("Selecting active queries for potential acquisition");

        std::vector<TActiveQuery> queryRecords;

        try {
            // TODO(max42): select as little fields as possible; lookup full row in TryAcquireQuery instead.
            // Select queries with expired leases.
            auto selectQuery = Format(
                "[query_id], [incarnation], [assigned_tracker], [lease_transaction_id], [engine], [state], [user], [query], [settings], [files] from [%v]",
                StateRoot_ + "/active_queries");
            auto selectResult = WaitFor(StateClient_->SelectRows(selectQuery))
                .ValueOrThrow();
            queryRecords = ToRecords<TActiveQuery>(selectResult.Rowset);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Error while selecting queries with expired leases");
            return;
        }

        YT_LOG_DEBUG("Active queries selected (ActiveQueryCount: %v)",
            queryRecords.size());

        THashSet<TTransactionId> leaseTransactionIds;
        for (const auto& record : queryRecords) {
            leaseTransactionIds.insert(record.LeaseTransactionId);
        }

        THashSet<TTransactionId> activeLeaseTransactionIds;
        try {
            activeLeaseTransactionIds = WaitFor(GetAliveTransactions(leaseTransactionIds))
                .ValueOrThrow();
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Error while getting alive lease transactions for active queries");
            return;
        }

        // Save profile counters.
        THashMap<TProfilingTags, int> activeQueryCounts;

        LeakySingleton<TActiveQueriesProfilingCountersMap>()->Flush();
        LeakySingleton<TActiveQueriesProfilingCountersMap>()->IterateReadOnly([&](const TProfilingTags& tags, const TActiveQueriesProfilingCounter&) {
            activeQueryCounts[tags] = 0;
        });
        for (const auto& record : queryRecords) {
            ++activeQueryCounts[ProfilingTagsFromActiveQueryRecord(record)];
        }
        for (const auto&[tags, count] : activeQueryCounts) {
            auto& activeQueriesCounter = GetOrCreateProfilingCounter<TActiveQueriesProfilingCounter>(
                QueryTrackerProfilerGlobal,
                tags)->ActiveQueries;
            activeQueriesCounter.Update(count);
        }

        std::vector<TActiveQuery> orphanedQueries;
        for (const auto& record : queryRecords) {
            if (!activeLeaseTransactionIds.contains(record.LeaseTransactionId)) {
                orphanedQueries.push_back(record);
            }
        }

        YT_LOG_INFO("Selected orphaned active queries (OrphanedQueryCount: %v)",
            orphanedQueries.size());

        // Ensure even distribution of queries across trackers by introducing a random delay
        // between 0 and acquisition period.
        for (const auto& record : orphanedQueries) {
            auto delay = RandomDuration(Config_->ActiveQueryAcquisitionPeriod);
            YT_LOG_INFO(
                "Scheduling acquisition of query (QueryId: %v, Engine: %v, User: %v, "
                "Incarnation: %v, LeaseTransactionId: %v, AssignedTracker: %v, Delay: %v)",
                record.Key.QueryId,
                record.Engine,
                record.User,
                record.Incarnation,
                record.LeaseTransactionId,
                record.AssignedTracker,
                delay);
            TDelayedExecutor::Submit(
                BIND_NO_PROPAGATE(&TQueryTracker::TryAcquireQuery, MakeWeak(this), record),
                delay,
                ControlInvoker_);
        }
    }

    void TryAcquireQuery(TActiveQuery queryRecord)
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);

        auto traceContext = TTraceContext::NewRoot("QueryAcquisition");
        auto guard = TCurrentTraceContextGuard(traceContext);

        try {
            GuardedTryAcquireQuery(std::move(queryRecord));
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Error acquiring query");
        }
    }

    void GuardedTryAcquireQuery(TActiveQuery queryRecord)
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);

        auto queryId = queryRecord.Key.QueryId;
        auto Logger = NQueryTracker::Logger().WithTag("QueryId: %v", queryId);
        YT_LOG_DEBUG("Starting acquisition transaction");
        auto transaction = WaitFor(StateClient_->StartTransaction(ETransactionType::Tablet))
            .ValueOrThrow();
        YT_LOG_DEBUG("Acquisition transaction started (TransactionId: %v)", transaction->GetId());

        const auto& idMapping = TActiveQueryDescriptor::Get()->GetIdMapping();
        auto optionalRecord = WaitFor(
            LookupActiveQuery(
                queryId,
                transaction->GetStartTimestamp(),
                {*idMapping.Incarnation, *idMapping.LeaseTransactionId, *idMapping.AssignedTracker, *idMapping.State}))
            .ValueOrThrow();

        TTransactionId leaseTransactionId;
        if (LeaseTransaction_) {
            leaseTransactionId = LeaseTransaction_->GetId();
        } else {
            YT_LOG_INFO("Failed to acquire query since lease transaction is not active");
            return;
        }

        if (!optionalRecord) {
            YT_LOG_INFO("Query is no longer present (Timestamp: %v)", transaction->GetStartTimestamp());
            return;
        } else if (optionalRecord->Incarnation != queryRecord.Incarnation) {
            YT_LOG_INFO(
                "Query was already acquired by another entity (Incarnation: %v, LeaseTransactionId: %v, AssignedTracker: %v, Timestamp: %v)",
                optionalRecord->Incarnation,
                optionalRecord->LeaseTransactionId,
                optionalRecord->AssignedTracker,
                transaction->GetStartTimestamp());
            return;
        }

        // If incarnation was not changed, query must have that same (dead) lease transaction.
        YT_VERIFY(optionalRecord->LeaseTransactionId == queryRecord.LeaseTransactionId);

        auto newIncarnation = queryRecord.Incarnation + 1;

        YT_LOG_INFO(
            "Query is still expired, acquiring it "
            "(Timestamp: %v, Incarnation: %v, LeaseTransactionId: %v, State: %v)",
            transaction->GetStartTimestamp(),
            newIncarnation,
            leaseTransactionId,
            optionalRecord->State);

        // If current query state is "running", switch it to "pending". Otherwise, keep the existing state of a query;
        // in particular, it may be "failing" or "completing" if the previous incarnation succeeded in reaching pre-terminating state.
        auto newState = optionalRecord->State == EQueryState::Running ? EQueryState::Pending : optionalRecord->State;

        auto rowBuffer = New<TRowBuffer>();
        TActiveQueryPartial newRecord{
            .Key = queryRecord.Key,
            .State = newState,
            .Incarnation = newIncarnation,
            .LeaseTransactionId = leaseTransactionId,
            .AssignedTracker = SelfAddress_,
        };
        std::vector newRows = {
            newRecord.ToUnversionedRow(rowBuffer, idMapping),
        };
        transaction->WriteRows(
            StateRoot_ + "/active_queries",
            TActiveQueryDescriptor::Get()->GetNameTable(),
            MakeSharedRange(std::move(newRows), rowBuffer));
        auto commitResultOrError = WaitFor(transaction->Commit());
        if (!commitResultOrError.IsOK()) {
            YT_LOG_DEBUG(commitResultOrError, "Failed to acquire query");
            return;
        }

        // This is a rare but possible race: lease transaction was aborted during write into the table.
        // Just do nothing: other query tracker (or even us) will find a query with dead lease transaction
        // and will try to acquire it.
        if (!LeaseTransaction_ || LeaseTransaction_->GetId() != leaseTransactionId) {
            YT_LOG_INFO("Failed to acquire query since lease transaction was aborted during acquisition "
                "(LeaseTransactionId: %v)",
                leaseTransactionId);
            return;
        }

        // Do not forget to update query record with new values.
        queryRecord.Incarnation = newIncarnation;
        queryRecord.LeaseTransactionId = leaseTransactionId;
        YT_LOG_INFO(
            "Query acquired (CommitTimestamp: %v, Incarnation: %v, LeaseTransactionId: %v)",
            commitResultOrError.Value().PrimaryCommitTimestamp,
            newIncarnation,
            leaseTransactionId);

        IQueryHandlerPtr handler;
        if (!IsPreFinishedState(optionalRecord->State)) {
            try {
                handler = Engines_[queryRecord.Engine]->StartOrAttachQuery(queryRecord);
                handler->Start();
            } catch (const std::exception& ex) {
                YT_LOG_INFO(ex, "Unrecoverable error on query start, finishing query");
                FinishQueryLoop(queryId, TError(ex), EQueryState::Failed);
                return;
            }
            InsertOrCrash(AcquiredQueries_, std::pair{queryId, TAcquiredQuery{
                .Handler = std::move(handler),
                .Incarnation = newIncarnation,
                .LeaseTransactionId = leaseTransactionId,
            }});
        } else {
            InsertOrCrash(AcquiredQueries_, std::pair{queryId, TAcquiredQuery{
                .Handler = nullptr,
                .Incarnation = newIncarnation,
                .LeaseTransactionId = leaseTransactionId,
            }});

        }

        PingLoop(queryId, newIncarnation);
    }

    void PingLoop(TQueryId queryId, i64 incarnation)
    {
        while (true) {
            if (!TryPingQuery(queryId, incarnation)) {
                break;
            }
            auto backoffDuration = RandomDuration(Config_->ActiveQueryPingPeriod) + Config_->ActiveQueryPingPeriod / 2.0;
            TDelayedExecutor::WaitForDuration(backoffDuration);
        }
    }

    //! Ping query assuming it is of given incarnation. Returns true if pinging must continue and false otherwise.
    bool TryPingQuery(TQueryId queryId, i64 incarnation)
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);

        auto Logger = QueryTrackerLogger().WithTag("QueryId: %v, Incarnation: %v", queryId, incarnation);

        if (auto iter = AcquiredQueries_.find(queryId);
            iter == AcquiredQueries_.end() || iter->second.Incarnation != incarnation)
        {
            YT_LOG_DEBUG("Cancelling obsolete ping");
            DetachQuery(queryId);
            return false;
        }

        try {
            YT_LOG_DEBUG("Starting ping transaction");

            auto transaction = WaitFor(StateClient_->StartTransaction(ETransactionType::Tablet))
                .ValueOrThrow();

            YT_LOG_DEBUG("Ping transaction started (TransactionId: %v)", transaction->GetId());
            const auto& idMapping = TActiveQueryDescriptor::Get()->GetIdMapping();
            auto activeQueryRecord = WaitFor(
                LookupActiveQuery(
                    queryId,
                    transaction->GetStartTimestamp(),
                    {*idMapping.Incarnation, *idMapping.State, *idMapping.AbortRequest, *idMapping.Error}))
                .ValueOrThrow();

            if (!activeQueryRecord) {
                YT_LOG_INFO("Query record is missing, cancelling ping");
                DetachQuery(queryId);
                return false;
            }

            if (IsPreFinishedState(activeQueryRecord->State)) {
                YT_LOG_INFO("Query is in pre-terminating state, pinging stopped");
                TError error;
                EQueryState finalState;
                switch (activeQueryRecord->State) {
                    case EQueryState::Aborting:
                        error = ConvertTo<TError>(*activeQueryRecord->AbortRequest);
                        finalState = EQueryState::Aborted;
                        if (AcquiredQueries_[queryId].Handler) {
                            AcquiredQueries_[queryId].Handler->Abort();
                        }
                        YT_LOG_INFO("Query abort was requested (Error: %v)", error);
                        break;
                    case EQueryState::Failing:
                        error = *activeQueryRecord->Error;
                        finalState = EQueryState::Failed;
                        YT_LOG_INFO("Query failed (Error: %v)", error);
                        break;
                    case EQueryState::Completing:
                        finalState = EQueryState::Completed;
                        YT_LOG_INFO("Query completed");
                        break;
                    default:
                        YT_ABORT();
                }
                FinishQueryLoop(queryId, error, finalState);
                DetachQuery(queryId);
                return false;
            }

            YT_LOG_DEBUG("Query is still running, doing nothing");

            return true;
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Error pinging query");
            return true;
        }
    }

    void DetachQuery(TQueryId queryId)
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);
        if (auto it = AcquiredQueries_.find(queryId); it != AcquiredQueries_.end()) {
            const auto& [queryId, query] = *it;
            YT_LOG_INFO("Query detached (QueryId: %v)", queryId);
            if (query.Handler) {
                query.Handler->Detach();
            }
            AcquiredQueries_.erase(it);
        }
    }

    TFuture<std::optional<TActiveQuery>> LookupActiveQuery(TQueryId queryId, TTimestamp timestamp, TColumnFilter columnFilter = {})
    {
        TLookupRowsOptions options;
        options.Timestamp = timestamp;
        options.ColumnFilter = columnFilter;
        options.KeepMissingRows = true;
        TActiveQueryKey key{.QueryId = queryId};
        auto rowBuffer = New<TRowBuffer>();
        std::vector keys{
            key.ToKey(rowBuffer),
        };
        auto asyncLookupResult = StateClient_->LookupRows(
            StateRoot_ + "/active_queries",
            TActiveQueryDescriptor::Get()->GetNameTable(),
            MakeSharedRange(std::move(keys), std::move(rowBuffer)),
            options);
        return asyncLookupResult.Apply(BIND([] (const TUnversionedLookupRowsResult& result) {
            auto optionalRecords = ToOptionalRecords<TActiveQuery>(result.Rowset);
            YT_VERIFY(optionalRecords.size() == 1);
            return optionalRecords[0];
        }));
    }

    void FinishQueryLoop(TQueryId queryId, TError error, EQueryState finalState)
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);

        if (finalState == EQueryState::Aborted || finalState == EQueryState::Failed) {
            error = TError("Query %v %lv", queryId, finalState)
                << error
                << TErrorAttribute("query_id", queryId);
        }

        while (true) {
            if (!TryFinishQuery(queryId, error, finalState)) {
                break;
            }
            auto backoffDuration = RandomDuration(Config_->QueryFinishBackoff) + Config_->QueryFinishBackoff / 2.0;
            TDelayedExecutor::WaitForDuration(backoffDuration);
        }
    }

    //! Finishes query by atomically moving its record from active to finished query table.
    //! Returns true if finishing was not successful and must be retried, and false otherwise
    //! (including situations when we lost lease and finishing must not be retried).
    bool TryFinishQuery(TQueryId queryId, TError error, EQueryState finalState)
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);

        auto Logger = NQueryTracker::Logger().WithTag("QueryId: %v", queryId);

        try {
            YT_LOG_DEBUG("Starting finish transaction");
            auto transaction = WaitFor(StateClient_->StartTransaction(ETransactionType::Tablet))
                .ValueOrThrow();
            YT_LOG_DEBUG("Finish transaction started (TransactionId: %v)", transaction->GetId());

            auto activeQueryRecord = WaitFor(
                LookupActiveQuery(
                    queryId,
                    transaction->GetStartTimestamp()))
                .ValueOrThrow();

            if (!activeQueryRecord) {
                YT_LOG_INFO("Query record is missing, cancelling finish");
                return false;
            }

            auto rowBuffer = New<TRowBuffer>();

            {
                std::vector keysToDelete = {
                    TActiveQueryKey{.QueryId = queryId}.ToKey(rowBuffer),
                };
                transaction->DeleteRows(
                    StateRoot_ + "/active_queries",
                    TActiveQueryDescriptor::Get()->GetNameTable(),
                    MakeSharedRange(std::move(keysToDelete), rowBuffer));
            }

            {
                // We must copy all fields of active query except for incarnation, ping time, assigned query and abort request
                // (which do not matter for finished query) and filter factors field (which goes to finished_queries_by_start_time,
                // finished_queries_by_user_and_start_time, finished_queries_by_aco_and_start_time tables).
                static_assert(TActiveQueryDescriptor::FieldCount == 20 && TFinishedQueryDescriptor::FieldCount == 14);
                TFinishedQuery newRecord{
                    .Key = TFinishedQueryKey{.QueryId = queryId},
                    .Engine = activeQueryRecord->Engine,
                    .Query = activeQueryRecord->Query,
                    .Files = activeQueryRecord->Files,
                    .Settings = activeQueryRecord->Settings,
                    .User = activeQueryRecord->User,
                    .AccessControlObjects = activeQueryRecord->AccessControlObjects.value_or(TYsonString(TString("[]"))),
                    .StartTime = activeQueryRecord->StartTime,
                    .State = finalState,
                    .Progress = activeQueryRecord->Progress,
                    .Error = error,
                    .ResultCount = activeQueryRecord->ResultCount,
                    .FinishTime = activeQueryRecord->FinishTime,
                    .Annotations = activeQueryRecord->Annotations,
                };
                std::vector newRows = {
                    newRecord.ToUnversionedRow(rowBuffer, TFinishedQueryDescriptor::Get()->GetIdMapping()),
                };
                transaction->WriteRows(
                    StateRoot_ + "/finished_queries",
                    TFinishedQueryDescriptor::Get()->GetNameTable(),
                    MakeSharedRange(std::move(newRows), rowBuffer));
            }

            {
                static_assert(TActiveQueryDescriptor::FieldCount == 20 && TFinishedQueryByStartTimeDescriptor::FieldCount == 7);
                TFinishedQueryByStartTime newRecord{
                    .Key = TFinishedQueryByStartTimeKey{.MinusStartTime = -i64(activeQueryRecord->StartTime.MicroSeconds()), .QueryId = queryId},
                    .Engine = activeQueryRecord->Engine,
                    .User = activeQueryRecord->User,
                    .AccessControlObjects = activeQueryRecord->AccessControlObjects.value_or(TYsonString(TString("[]"))),
                    .State = finalState,
                    .FilterFactors = activeQueryRecord->FilterFactors,
                };
                std::vector newRows = {
                    newRecord.ToUnversionedRow(rowBuffer, TFinishedQueryByStartTimeDescriptor::Get()->GetIdMapping()),
                };
                transaction->WriteRows(
                    StateRoot_ + "/finished_queries_by_start_time",
                    TFinishedQueryByStartTimeDescriptor::Get()->GetNameTable(),
                    MakeSharedRange(std::move(newRows), rowBuffer));
            }

            {
                static_assert(TActiveQueryDescriptor::FieldCount == 20 && TFinishedQueryByUserAndStartTimeDescriptor::FieldCount == 6);
                TFinishedQueryByUserAndStartTime newRecord{
                    .Key = TFinishedQueryByUserAndStartTimeKey{.User = activeQueryRecord->User, .MinusStartTime = -i64(activeQueryRecord->StartTime.MicroSeconds()), .QueryId = queryId},
                    .Engine = activeQueryRecord->Engine,
                    .State = finalState,
                    .FilterFactors = activeQueryRecord->FilterFactors,
                };
                std::vector newRows = {
                    newRecord.ToUnversionedRow(rowBuffer, TFinishedQueryByUserAndStartTimeDescriptor::Get()->GetIdMapping()),
                };
                transaction->WriteRows(
                    StateRoot_ + "/finished_queries_by_user_and_start_time",
                    TFinishedQueryByUserAndStartTimeDescriptor::Get()->GetNameTable(),
                    MakeSharedRange(std::move(newRows), rowBuffer));
            }

            {
                static_assert(TActiveQueryDescriptor::FieldCount == 20 && TFinishedQueryByAcoAndStartTimeDescriptor::FieldCount == 7);

                auto accessControlObjects = activeQueryRecord->AccessControlObjects ? ConvertTo<std::vector<TString>>(activeQueryRecord->AccessControlObjects) : std::vector<TString>{};
                if (!accessControlObjects.empty()) {
                    std::vector<TUnversionedRow> newRows;
                    newRows.reserve(accessControlObjects.size());
                    for (const auto& aco : accessControlObjects) {
                        TFinishedQueryByAcoAndStartTime newRecord{
                            .Key = TFinishedQueryByAcoAndStartTimeKey{.AccessControlObject = aco, .MinusStartTime = -i64(activeQueryRecord->StartTime.MicroSeconds()), .QueryId = queryId},
                            .Engine = activeQueryRecord->Engine,
                            .User = activeQueryRecord->User,
                            .State = finalState,
                            .FilterFactors = activeQueryRecord->FilterFactors,
                        };
                        newRows.push_back(newRecord.ToUnversionedRow(rowBuffer, TFinishedQueryByAcoAndStartTimeDescriptor::Get()->GetIdMapping()));
                    }
                    transaction->WriteRows(
                        StateRoot_ + "/finished_queries_by_aco_and_start_time",
                        TFinishedQueryByAcoAndStartTimeDescriptor::Get()->GetNameTable(),
                        MakeSharedRange(std::move(newRows), rowBuffer));
                }
            }

            auto commitResultOrError = WaitFor(transaction->Commit());
            if (!commitResultOrError.IsOK()) {
                YT_LOG_ERROR(commitResultOrError, "Failed to finish query, backing off");
                return true;
            }

            {
                // Save profile counter.
                auto& stateTimeGauge = GetOrCreateProfilingCounter<TStateTimeProfilingCounter>(
                    QueryTrackerProfiler,
                    ProfilingTagsFromActiveQueryRecord(*activeQueryRecord))->StateTime;
                auto now = TInstant::Now();
                stateTimeGauge.Update(now - activeQueryRecord->FinishTime.value());
            }

            YT_LOG_INFO("Query finished (CommitTimestamp: %v)", commitResultOrError.Value().PrimaryCommitTimestamp);
            return false;
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Error while finishing query");
            return true;
        }
    }

    static void ValidateIncarnation(i64 expectedIncarnation, const TActiveQuery& record)
    {
        if (record.Incarnation != expectedIncarnation) {
            THROW_ERROR_EXCEPTION(
                NQueryTrackerClient::EErrorCode::IncarnationMismatch,
                "Query incarnation mismatch: expected %v, actual %v",
                expectedIncarnation,
                record.Incarnation)
                    << TErrorAttribute("expected_incarnation", expectedIncarnation)
                    << TErrorAttribute("actual_incarnation", record.Incarnation);
        }
    }

    // Lease transaction management.
    void StartLeaseTransaction()
    {
        VERIFY_SERIALIZED_INVOKER_AFFINITY(ControlInvoker_);

        YT_VERIFY(!LeaseTransaction_);

        YT_LOG_DEBUG("Starting lease transaction");

        auto transaction = WaitFor(StateClient_->StartTransaction(ETransactionType::Master))
            .ValueOrThrow();
        YT_VERIFY(!std::exchange(LeaseTransaction_, std::move(transaction)));

        YT_LOG_DEBUG("Lease transaction started (TransactionId: %v)", LeaseTransaction_->GetId());

        LeaseTransaction_->SubscribeAborted(BIND(
            &TQueryTracker::OnLeaseTransactionAborted,
            MakeWeak(this),
            LeaseTransaction_->GetId())
            .Via(ControlInvoker_));
    }

    void OnLeaseTransactionAborted(TTransactionId transactionId, const TError& error)
    {
        VERIFY_SERIALIZED_INVOKER_AFFINITY(ControlInvoker_);

        YT_LOG_WARNING(error, "Lease transaction aborted (TransactionId: %v)", transactionId);

        YT_VERIFY(LeaseTransaction_);
        LeaseTransaction_.Reset();

        StartLeaseTransaction();

        auto activeQueries = std::exchange(AcquiredQueries_, {});
        for (const auto& [queryId, query] : activeQueries) {
            if (query.LeaseTransactionId != transactionId) {
                YT_LOG_WARNING("Active query has unexpected lease transaction id "
                    " during lease transaction abort handling, detaching it "
                    "(QueryId: %v, LeaseTransactionId: %v, ExpectedLeaseTransactionId: %v)",
                    queryId,
                    query.LeaseTransactionId,
                    transactionId);
            }

            DetachQuery(queryId);
        }
    }

    //! Returns the subset of transactions that are still alive.
    TFuture<THashSet<TTransactionId>> GetAliveTransactions(const THashSet<TTransactionId>& transactionIds)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto proxy = CreateObjectServiceReadProxy(StateClient_, NApi::EMasterChannelKind::Follower);
        auto batchReq = proxy.ExecuteBatch();

        for (const auto& transactionId : transactionIds) {
            auto req = TCypressYPathProxy::Exists(FromObjectId(transactionId));
            req->Tag() = transactionId;
            batchReq->AddRequest(std::move(req));
        }

        return batchReq->Invoke().Apply(BIND([] (const TObjectServiceProxy::TRspExecuteBatchPtr& batchRsp) {
            THashSet<TTransactionId> aliveTransactions;

            for (const auto& [tag, rspOrError] : batchRsp->GetTaggedResponses<TCypressYPathProxy::TRspExists>()) {
                auto transactionId = std::any_cast<TTransactionId>(tag);

                const auto& rsp = rspOrError.ValueOrThrow();
                if (rsp->value()) {
                    InsertOrCrash(aliveTransactions, transactionId);
                }
            }

            return aliveTransactions;
        }));
    }

    void DoBuildOrchid(IYsonConsumer* consumer) const
    {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("acquisition_iterations").Value(AcquisitionIterations_.load())
            .EndMap();
    }
};

DEFINE_REFCOUNTED_TYPE(TQueryTracker)

////////////////////////////////////////////////////////////////////////////////

IQueryTrackerPtr CreateQueryTracker(
    TQueryTrackerDynamicConfigPtr config,
    std::string selfAddress,
    IInvokerPtr controlInvoker,
    IAlertCollectorPtr alertCollector,
    NApi::NNative::IClientPtr stateClient,
    IComponentStateCheckerPtr ComponentStateChecker,
    TYPath stateRoot,
    int minRequiredStateVersion)
{
    return New<TQueryTracker>(
        std::move(config),
        std::move(selfAddress),
        std::move(controlInvoker),
        std::move(alertCollector),
        std::move(stateClient),
        std::move(ComponentStateChecker),
        std::move(stateRoot),
        minRequiredStateVersion);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
