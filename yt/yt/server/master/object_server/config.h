#pragma once

#include "public.h"

#include <yt/yt/core/misc/config.h>

#include <yt/yt/core/rpc/config.h>

#include <yt/yt/core/ytree/request_complexity_limits.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/ytlib/object_client/config.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

class TMutationIdempotizerConfig
    : public NYTree::TYsonStruct
{
public:
    bool Enabled;
    TDuration ExpirationTime;
    TDuration ExpirationCheckPeriod;
    int MaxExpiredMutationIdRemovalsPerCommit;

    REGISTER_YSON_STRUCT(TMutationIdempotizerConfig);

    static void Register(TRegistrar registrar);
};

DECLARE_REFCOUNTED_CLASS(TMutationIdempotizerConfig)

////////////////////////////////////////////////////////////////////////////////

class TObjectManagerConfig
    : public NYTree::TYsonStruct
{
    REGISTER_YSON_STRUCT(TObjectManagerConfig);

    static void Register(TRegistrar)
    { }
};

DEFINE_REFCOUNTED_TYPE(TObjectManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicObjectManagerConfig
    : public NYTree::TYsonStruct
{
public:
    static constexpr auto DefaultProfilingPeriod = TDuration::MilliSeconds(100);

    //! Maximum total weight of objects processed per a single GC mutation.
    int MaxWeightPerGCSweep;

    //! Period between subsequent GC queue checks.
    TDuration GCSweepPeriod;

    bool EnableGC;

    //! Period between pairwise secondary cells sync, which enables
    //! advancing from |RemovalAwaitingCellsSync| to |RemovalCommitted| life stage.
    TDuration ObjectRemovalCellsSyncPeriod;

    TMutationIdempotizerConfigPtr MutationIdempotizer;

    //! Per-type list of attributes which will become interned in future versions
    //! and thus should not be set. Maps attribute names to error messages.
    THashMap<EObjectType, THashMap<TString, TString>> ReservedAttributes;

    //! Minimum length of YSON strings that will be interned during mutations.
    //! Outside mutations DefaultYsonStringInternLengthThreshold is always used.
    int YsonStringInternLengthThreshold;

    TDuration ProfilingPeriod;

    // COMPAT(akozhikhov).
    bool ResetHunkStorageInTableDestroy;

    REGISTER_YSON_STRUCT(TDynamicObjectManagerConfig);

    static void Register(TRegistrar registrar);
};

DECLARE_REFCOUNTED_CLASS(TDynamicObjectManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TObjectServiceConfig
    : public NYTree::TYsonStruct
{
public:
    //! Maximum amount of a single batch of Execute requests is allowed to occupy the automaton thread.
    TDuration YieldTimeout;

    //! When user is banned or exceeds the queue size limit, Object Service replies
    //! with an error and caches this error for the given period of time.
    //! This helps to offload the Automaton Thread in case of DOS attack.
    TDuration StickyUserErrorExpireTime;

    //! Maximum time to wait before syncing with another cell.
    TDuration CrossCellSyncDelay;

    //! The amount of time remaining to a batch request timeout when the object
    //! service shall try and send partial (subbatch) response.
    //! NB: This will have no effect if the request's timeout is shorter than this.
    TDuration TimeoutBackoffLeadTime;

    //! Default timeout for ObjectService::Execute (if a client has not provided one).
    TDuration DefaultExecuteTimeout;

    //! Amount of time to reserve when computing the timeout for a forwarded request.
    TDuration ForwardedRequestTimeoutReserve;

    NObjectClient::TObjectServiceCacheConfigPtr MasterCache;

    bool EnableLocalReadBusyWait;

    REGISTER_YSON_STRUCT(TObjectServiceConfig);

    static void Register(TRegistrar registrar);
};

DECLARE_REFCOUNTED_CLASS(TObjectServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TReadRequestComplexityLimitsConfigBase
    : public NYTree::TYsonStruct
{
public:
    i64 NodeCount;
    i64 ResultSize;

    NYTree::TReadRequestComplexity ToReadRequestComplexity() const noexcept;

    REGISTER_YSON_STRUCT(TReadRequestComplexityLimitsConfigBase);

    static void Register(TRegistrar registrar);

protected:
    static void DoRegister(TRegistrar registrar, i64 nodeCount, i64 resultSize);
};

class TDefaultReadRequestComplexityLimitsConfig
    : public TReadRequestComplexityLimitsConfigBase
{
public:
    REGISTER_YSON_STRUCT(TDefaultReadRequestComplexityLimitsConfig);

    static void Register(TRegistrar registrar);
};


class TMaxReadRequestComplexityLimitsConfig
    : public TReadRequestComplexityLimitsConfigBase
{
public:
    REGISTER_YSON_STRUCT(TMaxReadRequestComplexityLimitsConfig);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

class TDynamicObjectServiceConfig
    : public NYTree::TYsonStruct
{
public:
    bool EnableTwoLevelCache;
    int LocalReadThreadCount;
    int LocalReadOffloadThreadCount;
    TDuration ScheduleReplyRetryBackoff;

    TDuration LocalReadExecutorQuantumDuration;

    TDuration ProcessSessionsPeriod;
    bool MinimizeExecuteLatency;

    TDefaultReadRequestComplexityLimitsConfigPtr DefaultReadRequestComplexityLimits;
    TMaxReadRequestComplexityLimitsConfigPtr MaxReadRequestComplexityLimits;

    bool EnableReadRequestComplexityLimits;

    //! These throttlers control the rate of local read/write ObjectService.Execute subrequests
    //! from *all* users except root (!) that are allowed to pass through to be scheduled
    //! into the Automaton mutation queue.
    //! They can be used as a form of mutation congestion control to limit the size of the
    //! query/mutation queue insofar as it is induced by users' Execute requests.
    //! These throttlers are acquired simultaneously with per-user request throttling.
    NConcurrency::TThroughputThrottlerConfigPtr LocalReadRequestThrottler;
    NConcurrency::TThroughputThrottlerConfigPtr LocalWriteRequestThrottler;

    REGISTER_YSON_STRUCT(TDynamicObjectServiceConfig);

    static void Register(TRegistrar registrar);
};

DECLARE_REFCOUNTED_CLASS(TDynamicObjectServiceConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
