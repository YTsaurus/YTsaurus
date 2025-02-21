#pragma once

#include "public.h"

#include <yt/yt/core/actions/invoker.h>
#include <yt/yt/core/actions/public.h>


namespace NYT::NConcurrency::NFairShareWeightedThreadPool {

////////////////////////////////////////////////////////////////////////////////

struct IInvokerWithExpectedBytes
    : public virtual IInvoker
{
    virtual void InvokeWithExpectedBytes(TClosure callback, i64 expectedBytes) = 0;
    virtual void InvokeWithExpectedBytes(TMutableRange<std::pair<TClosure, i64>> callbacks) = 0;
};

DECLARE_REFCOUNTED_STRUCT(IInvokerWithExpectedBytes)
DEFINE_REFCOUNTED_TYPE(IInvokerWithExpectedBytes)

////////////////////////////////////////////////////////////////////////////////

struct IPoolWeightProvider
    : public virtual TRefCounted
{
    virtual double GetWeight(const TString& poolName) = 0;
};

DEFINE_REFCOUNTED_TYPE(IPoolWeightProvider)

////////////////////////////////////////////////////////////////////////////////

struct IFairShareWeightedThreadPool
    : public virtual TRefCounted
{
    virtual int GetThreadCount() = 0;
    virtual void Configure(int threadCount) = 0;
    virtual void Shutdown() = 0;

    using TWaitTimeObserver = std::function<void(TDuration)>;
    virtual void RegisterWaitTimeObserver(TWaitTimeObserver waitTimeObserver) = 0;

    virtual IInvokerWithExpectedBytesPtr GetInvokerWithExpectedBytes(
        const TString& poolName,
        const TFairShareThreadPoolTag& tag,
        double bucketWeight = 1.0) = 0;
};

DEFINE_REFCOUNTED_TYPE(IFairShareWeightedThreadPool)

////////////////////////////////////////////////////////////////////////////////

struct TFairShareWeightedThreadPoolOptions
{
    IPoolWeightProviderPtr PoolWeightProvider = nullptr;
    bool VerboseLogging = false;
    TDuration PollingPeriod = TDuration::MilliSeconds(10);
    TDuration PoolRetentionTime = TDuration::Seconds(30);
};

IFairShareWeightedThreadPoolPtr CreateFairShareWeightedThreadPool(
    int threadCount,
    const TString& threadNamePrefix,
    const TFairShareWeightedThreadPoolOptions& options = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
