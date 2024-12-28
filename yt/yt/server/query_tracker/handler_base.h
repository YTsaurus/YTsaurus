#pragma once

#include "engine.h"

#include <yt/yt/ytlib/query_tracker_client/records/query.record.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/yson/string.h>

namespace NYT::NQueryTracker {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

struct TWireRowset
{
    TSharedRef Rowset;
    bool IsTruncated = false;
};

struct TRowset
{
    NApi::IUnversionedRowsetPtr Rowset;
    bool IsTruncated = false;
};

////////////////////////////////////////////////////////////////////////////////

class TQueryHandlerBase
    : public IQueryHandler
{
public:
    TQueryHandlerBase(
        const NApi::IClientPtr& stateClient,
        const NYPath::TYPath& stateRoot,
        const IInvokerPtr controlInvoker,
        const TEngineConfigBasePtr& config,
        const NQueryTrackerClient::NRecords::TActiveQuery& activeQuery);

    //! Starts a transaction and validates that by the moment of its start timestamp,
    //! incarnation of a query is still the same. Context switch happens inside.
    std::pair<NApi::ITransactionPtr, NQueryTrackerClient::NRecords::TActiveQuery> StartIncarnationTransaction(EQueryState previousState = EQueryState::Running) const;

protected:
    const NApi::IClientPtr StateClient_;
    const NYPath::TYPath StateRoot_;
    const IInvokerPtr ControlInvoker_;
    const TEngineConfigBasePtr Config_;
    const TString Query_;
    const TQueryId QueryId_;
    const i64 Incarnation_;
    const TString User_;
    const EQueryEngine Engine_;
    const NYTree::INodePtr SettingsNode_;
    const TInstant AcquisitionTime_;

    const NLogging::TLogger Logger;

    const NConcurrency::TPeriodicExecutorPtr ProgressWriter_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ProgressSpinLock_);
    TYsonString Progress_ = TYsonString(TString("{}"));
    int ProgressVersion_ = 0;
    int LastSavedProgressVersion_ = 0;

    std::optional<TInstant> LastStateChange_;
    THashMap<EQueryState, TDuration> StateTimes_;

    void StartProgressWriter();
    void StopProgressWriter();

    void OnProgress(TYsonString progress);

    void OnQueryStarted();
    void OnQueryThrottled();
    void OnQueryFailed(const TError& error);
    void OnQueryCompleted(const std::vector<TErrorOr<TRowset>>& rowsetOrErrors);
    void OnQueryCompletedWire(const std::vector<TErrorOr<TWireRowset>>& wireRowsetOrErrors);

    void TryWriteProgress();
    bool TryWriteQueryState(EQueryState state, EQueryState previousState, const TError& error, const std::vector<TErrorOr<TWireRowset>>& wireRowsetOrErrors);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
