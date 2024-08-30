#include "job_throttler.h"

#include <yt/yt/server/lib/job_proxy/config.h>
#include <yt/yt/server/lib/job_proxy/public.h>

#include <yt/yt/client/rpc/helpers.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

namespace NYT::NJobProxy {

using namespace NRpc;
using namespace NConcurrency;
using namespace NJobTrackerClient;
using namespace NExecNode;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const double MaxBackoffMultiplier = 1000.0;

////////////////////////////////////////////////////////////////////////////////

class TThrottlingSession
    : public TRefCounted
{
public:
    TThrottlingSession(const TJobThrottlerConfigPtr& jobThrottlerConfig, const IChannelPtr& nodeChannel)
        : Config_(jobThrottlerConfig)
        , Proxy_(nodeChannel)
    {
        Proxy_.SetDefaultTimeout(jobThrottlerConfig->RpcTimeout);
    }

    TFuture<void> Throttle(i64 amount, EJobThrottlerType throttleDirection, TWorkloadDescriptor descriptor, TJobId jobId)
    {
        auto request = Proxy_.ThrottleJob();
        SetRequestWorkloadDescriptor(request, descriptor);
        request->set_throttler_type(ToProto<int>(throttleDirection));
        request->set_amount(amount);
        ToProto(request->mutable_job_id(), jobId);

        request->Invoke().Subscribe(BIND(&TThrottlingSession::OnThrottlingResponse, MakeStrong(this)));

        return ThrottlePromise_.ToFuture();
    }

private:
    const TJobThrottlerConfigPtr Config_;
    TSupervisorServiceProxy Proxy_;
    TPromise<void> ThrottlePromise_ = NewPromise<void>();

    TGuid PollRequestId_;
    int PollIndex_ = 0;

    void OnThrottlingResponse(const TSupervisorServiceProxy::TErrorOrRspThrottleJobPtr& errorOrRsp)
    {
        if (!errorOrRsp.IsOK()) {
            // Some error occurred communicating with local node.
            ThrottlePromise_.Set(errorOrRsp);
            return;
        }

        const auto& rsp = errorOrRsp.Value();
        if (rsp->has_throttling_request_id()) {
            PollRequestId_ = FromProto<TGuid>(rsp->throttling_request_id());
            TDelayedExecutor::Submit(
                BIND(&TThrottlingSession::PollThrottlingRequest, MakeStrong(this)),
                Config_->MinBackoffTime);

        } else {
            // Throttling session is over.
            ThrottlePromise_.Set();
        }
    }

    void PollThrottlingRequest()
    {
        auto request = Proxy_.PollThrottlingRequest();
        ToProto(request->mutable_throttling_request_id(), PollRequestId_);

        auto errorOrRsp = WaitFor(request->Invoke());
        if (!errorOrRsp.IsOK()) {
            ThrottlePromise_.Set(errorOrRsp);
            return;
        }

        const auto& rsp = errorOrRsp.Value();
        if (rsp->completed()) {
            ThrottlePromise_.Set();
        } else {
            ++PollIndex_;

            auto backoffMultiplier = std::min(std::pow(Config_->BackoffMultiplier, PollIndex_), MaxBackoffMultiplier);
            auto backoffTime = std::min(Config_->MinBackoffTime * backoffMultiplier, Config_->MaxBackoffTime);

            TDelayedExecutor::Submit(
                BIND(&TThrottlingSession::PollThrottlingRequest, MakeStrong(this)),
                backoffTime);
        }
    }
};

DECLARE_REFCOUNTED_CLASS(TThrottlingSession)
DEFINE_REFCOUNTED_TYPE(TThrottlingSession)

////////////////////////////////////////////////////////////////////////////////

class TJobBandwidthThrottler
    : public IThroughputThrottler
{
public:
    TJobBandwidthThrottler(
        const TJobThrottlerConfigPtr& config,
        const IChannelPtr& channel,
        EJobThrottlerType throttlerType,
        const TWorkloadDescriptor& descriptor,
        TJobId jobId)
        : Config_(config)
        , Channel_(channel)
        , ThrottlerType_(throttlerType)
        , Descriptor_(descriptor)
        , JobId_(jobId)
    { }

    TFuture<void> Throttle(i64 amount) override
    {
        auto throttlingSession = New<TThrottlingSession>(Config_, Channel_);
        return throttlingSession->Throttle(amount, ThrottlerType_, Descriptor_, JobId_);
    }

    bool TryAcquire(i64 /*amount*/) override
    {
        YT_UNIMPLEMENTED();
    }

    i64 TryAcquireAvailable(i64 /*amount*/) override
    {
        YT_UNIMPLEMENTED();
    }

    void Acquire(i64 /*amount*/) override
    {
        YT_UNIMPLEMENTED();
    }

    void Release(i64 /*amount*/) override
    {
        // NB: This method may be called only if prefetching throttler is disabled.
        return;
    }

    bool IsOverdraft() override
    {
        YT_UNIMPLEMENTED();
    }

    i64 GetQueueTotalAmount() const override
    {
        YT_UNIMPLEMENTED();
    }

    TDuration GetEstimatedOverdraftDuration() const override
    {
        YT_UNIMPLEMENTED();
    }

    i64 GetAvailable() const override
    {
        YT_UNIMPLEMENTED();
    }

private:
    const TJobThrottlerConfigPtr Config_;
    const IChannelPtr Channel_;
    const EJobThrottlerType ThrottlerType_;
    const TWorkloadDescriptor Descriptor_;
    const TJobId JobId_;
};

////////////////////////////////////////////////////////////////////////////////

IThroughputThrottlerPtr CreateInJobBandwidthThrottler(
    const TJobThrottlerConfigPtr& config,
    const IChannelPtr& channel,
    const TWorkloadDescriptor& descriptor,
    TJobId jobId,
    const NLogging::TLogger& logger)
{
    auto underlying = New<TJobBandwidthThrottler>(
        config,
        channel,
        EJobThrottlerType::InBandwidth,
        descriptor,
        jobId);
    return CreatePrefetchingThrottler(
        config->BandwidthPrefetch,
        underlying,
        logger);
}

IThroughputThrottlerPtr CreateOutJobBandwidthThrottler(
    const TJobThrottlerConfigPtr& config,
    const IChannelPtr& channel,
    const TWorkloadDescriptor& descriptor,
    TJobId jobId,
    const NLogging::TLogger& logger)
{
    auto underlying = New<TJobBandwidthThrottler>(
        config,
        channel,
        EJobThrottlerType::OutBandwidth,
        descriptor,
        jobId);
    return CreatePrefetchingThrottler(
        config->BandwidthPrefetch,
        underlying,
        logger);
}

IThroughputThrottlerPtr CreateOutJobRpsThrottler(
    const TJobThrottlerConfigPtr& config,
    const IChannelPtr& channel,
    const TWorkloadDescriptor& descriptor,
    TJobId jobId,
    const NLogging::TLogger& logger)
{
    auto underlying = New<TJobBandwidthThrottler>(
        config,
        channel,
        EJobThrottlerType::OutRps,
        descriptor,
        jobId);
    return CreatePrefetchingThrottler(
        config->RpsPrefetch,
        underlying,
        logger);
}

IThroughputThrottlerPtr CreateUserJobContainerCreationThrottler(
    const TJobThrottlerConfigPtr& config,
    const IChannelPtr& channel,
    const TWorkloadDescriptor& descriptor,
    TJobId jobId)
{
    return New<TJobBandwidthThrottler>(
        config,
        channel,
        EJobThrottlerType::ContainerCreation,
        descriptor,
        jobId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
