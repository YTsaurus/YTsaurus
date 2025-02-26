#include "io_workload_model.h"

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/actions/signal.h>
#include <yt/yt/core/actions/bind.h>

#include <util/generic/bitops.h>

#include <library/cpp/histogram/hdr/histogram.h>

#include <yt/yt/core/profiling/timing.h>
#include <yt/yt/core/profiling/tscp.h>

#include <numeric>

namespace NYT::NIO {

using namespace NConcurrency;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const std::vector<i64> RequestSizeBins = {
    4_KB, 16_KB, 64_KB, 128_KB, 512_KB, 1_MB, 4_MB, 16_MB, 64_MB, 128_MB
};

static const std::vector<i64> RequestLatencyBins = {
    1, 2, 4, 6, 8, 10, 15, 20, 40, 60, 80, 100, 200, 400, 600, 1000, 4000
};

////////////////////////////////////////////////////////////////////////////////

void TFixedBinsHistogramBase::RecordValue(i64 value, i64 count)
{
    const int binsCount = std::ssize(BinValues_);
    for (int index = 0; index < binsCount; ++index) {
        if (BinValues_[index] >= value) {
            Counters_[index] += count;
            return;
        }
    }
    Counters_.back() += count;
}

TFixedBinsHistogramBase::TFixedBinsHistogramBase(TBins bins)
    : BinValues_(std::move(bins))
    , Counters_(BinValues_.size())
{ }

const TFixedBinsHistogramBase::TBins& TFixedBinsHistogramBase::GetBins() const
{
    return BinValues_;
}

const TFixedBinsHistogramBase::TCounters& TFixedBinsHistogramBase::GetCounters() const
{
    return Counters_;
}

void TFixedBinsHistogramBase::Add(const TFixedBinsHistogramBase& other)
{
    YT_VERIFY(std::ssize(GetBins()) == std::ssize(other.GetBins()));

    const auto& counters = other.GetCounters();

    for (int binIndex = 0; binIndex < std::ssize(GetBins()); ++binIndex) {
        RecordValue(binIndex, counters[binIndex]);
    }
}

////////////////////////////////////////////////////////////////////////////////

TRequestSizeHistogram::TRequestSizeHistogram()
    : TFixedBinsHistogramBase(RequestSizeBins)
{ }

TRequestSizeHistogram& TRequestSizeHistogram::operator+=(const TRequestSizeHistogram& other)
{
    Add(other);
    return *this;
}

TRequestLatencyHistogram::TRequestLatencyHistogram()
    : TFixedBinsHistogramBase(RequestLatencyBins)
{ }

TRequestLatencyHistogram& TRequestLatencyHistogram::operator+=(const TRequestLatencyHistogram& other)
{
    Add(other);
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

THistogramSummary ComputeHistogramSummary(const TFixedBinsHistogramBase& hist)
{
    const auto& counters = hist.GetCounters();

    THistogramSummary summary;
    int currentBinIndex = 0;
    summary.TotalCount = std::accumulate(counters.begin(), counters.end(), 0LL);

    i64 counter = 0;
    auto computeNextQuantile = [&] (double quantile) {
        while (currentBinIndex < std::ssize(counters) - 1 && counter < quantile * summary.TotalCount) {
            counter += counters[currentBinIndex];
            ++currentBinIndex;
        }
        return hist.GetBins()[currentBinIndex];
    };

    summary.P90 = computeNextQuantile(0.9);
    summary.P99 = computeNextQuantile(0.99);
    summary.P99_9 = computeNextQuantile(0.999);
    summary.P99_99 = computeNextQuantile(0.9999);
    summary.Max = computeNextQuantile(1);
    return summary;
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TEnumIndexedArray<EWorkloadCategory, TRequestSizeHistogram>& statsByCategory,
    TStringBuf /*spec*/)
{
    builder->AppendString("{");

    for (auto category : TEnumTraits<EWorkloadCategory>::GetDomainValues()) {
        const auto& histogram = statsByCategory[category];
        const auto& counters = histogram.GetCounters();
        const auto& bins = histogram.GetBins();

        if (std::reduce(counters.begin(), counters.end()) == 0) {
            continue;
        }

        builder->AppendFormat("\"%v\": {", category);
        for (int index = 0; index < std::ssize(counters); ++index) {
            if (counters[index]) {
                builder->AppendFormat("%v:%v, ", bins[index], counters[index]);
            }
        }
        builder->AppendString("}, ");
    }

    builder->AppendString("}");
}

////////////////////////////////////////////////////////////////////////////////

// TODO(capone212): make config parameter.
static constexpr auto RequestSizesModelingPeriod = TDuration::Minutes(5);
static constexpr auto RequestLatenciesModelingPeriod = TDuration::Seconds(5);

DECLARE_REFCOUNTED_CLASS(TWorkloadModelManager)

////////////////////////////////////////////////////////////////////////////////

template <typename THistogram>
void MergeHistograms(const THistogram& input, THistogram& output)
{
    for (auto category : TEnumTraits<EWorkloadCategory>::GetDomainValues()) {
        output[category] += input[category];
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename TModel>
void MergeModels(const TModel& input, TModel& output)
{
    MergeHistograms(input.Reads, output.Reads);
    MergeHistograms(input.Writes, output.Writes);
}

////////////////////////////////////////////////////////////////////////////////

class TThreadSafeWorkloadModel
{
public:
    void RegisterRead(i64 requestSize, EWorkloadCategory category, TDuration requestTime)
    {
        auto guard = Guard(ModelLock_);

        RequestSizes_.Reads[category].RecordValue(requestSize);
        RequestLatencies_.Reads[category].RecordValue(requestTime.MilliSeconds());
    }

    void RegisterWrite(i64 requestSize, EWorkloadCategory category, TDuration requestTime)
    {
        auto guard = Guard(ModelLock_);

        RequestSizes_.Writes[category].RecordValue(requestSize);
        RequestLatencies_.Writes[category].RecordValue(requestTime.MilliSeconds());
    }

    TRequestSizes ReleaseSizes()
    {
        TRequestSizes result;
        {
            auto guard = Guard(ModelLock_);
            std::swap(RequestSizes_, result);
        }

        return result;
    }

    TRequestLatencies ReleaseLatencies()
    {
        TRequestLatencies result;
        {
            auto guard = Guard(ModelLock_);
            std::swap(RequestLatencies_, result);
        }

        return result;
    }

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ModelLock_);
    TRequestSizes RequestSizes_;
    TRequestLatencies RequestLatencies_;
};

class TPerCpuWorkloadModel
{
public:
    void RegisterRead(i64 requestSize, EWorkloadCategory category, TDuration requestTime)
    {
        auto& shard = Shards_[TTscp::Get().ProcessorId].Holder;
        shard.RegisterRead(requestSize, category, requestTime);
    }

    void RegisterWrite(i64 requestSize, EWorkloadCategory category, TDuration requestTime)
    {
        auto& shard = Shards_[TTscp::Get().ProcessorId].Holder;
        shard.RegisterWrite(requestSize, category, requestTime);
    }

    TRequestSizes ReleaseSizes()
    {
        TRequestSizes combinedModel;

        for (auto& shard : Shards_) {
            auto shardModel = shard.Holder.ReleaseSizes();
            MergeModels(shardModel, combinedModel);
        }

        return combinedModel;
    }

    TRequestLatencies ReleaseLatencies()
    {
        TRequestLatencies combinedModel;

        for (auto& shard : Shards_) {
            auto shardModel = shard.Holder.ReleaseLatencies();
            MergeModels(shardModel, combinedModel);
        }

        return combinedModel;
    }

private:
    struct alignas(2 * CacheLineSize) TShard
    {
        TThreadSafeWorkloadModel Holder;
    };

    std::array<TShard, TTscp::MaxProcessorId> Shards_;
};

////////////////////////////////////////////////////////////////////////////////

class TWorkloadModelManager
    : public virtual TRefCounted
{
public:
    TWorkloadModelManager(TString locationId, NLogging::TLogger logger)
        : LocationId_(std::move(locationId))
        , Logger(std::move(logger))
        , ActionQueue_(New<TActionQueue>(Format("IOWM:%v", LocationId_)))
        , Invoker_(ActionQueue_->GetInvoker())
        , ModelCreationRoundExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TWorkloadModelManager::OnModelCreationRound, MakeWeak(this)),
            RequestSizesModelingPeriod))
        , LatenciesMeasuringExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TWorkloadModelManager::OnLatenciesReportingRound, MakeWeak(this)),
            RequestLatenciesModelingPeriod))
        , Model_(std::make_unique<TPerCpuWorkloadModel>())
    {
        YT_LOG_DEBUG("Initialized workload model manager");

        ModelCreationRoundExecutor_->Start();
        LatenciesMeasuringExecutor_->Start();
    }

    ~TWorkloadModelManager()
    {
        YT_UNUSED_FUTURE(ModelCreationRoundExecutor_->Stop());
        YT_UNUSED_FUTURE(LatenciesMeasuringExecutor_->Stop());
    }

    void RegisterRead(IIOEngine::TReadRequest request, EWorkloadCategory category, TDuration requestTime)
    {
        Model_->RegisterRead(request.Size, category, requestTime);
    }

    void RegisterWrite(IIOEngine::TWriteRequest request, EWorkloadCategory category, TDuration requestTime)
    {
        Model_->RegisterWrite(GetByteSize(request.Buffers), category, requestTime);
    }

    DEFINE_SIGNAL(void(const TRequestSizes&), RequestSizesSignal);
    DEFINE_SIGNAL(void(const TRequestLatencies&), RequestLatenciesSignal);

private:
    const TString LocationId_;
    const NLogging::TLogger Logger;

    const TActionQueuePtr ActionQueue_;
    const IInvokerPtr Invoker_;

    const TPeriodicExecutorPtr ModelCreationRoundExecutor_;
    const TPeriodicExecutorPtr LatenciesMeasuringExecutor_;

    std::unique_ptr<TPerCpuWorkloadModel> Model_;

    void OnModelCreationRound()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        auto model = Model_->ReleaseSizes();

        YT_LOG_DEBUG("Observed IO request sizes (Reads: %v, Writes: %v)",
            model.Reads,
            model.Writes);

        RequestSizesSignal_.Fire(model);
    }

    void OnLatenciesReportingRound()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        auto latencies = Model_->ReleaseLatencies();

        RequestLatenciesSignal_.Fire(latencies);
    }
};

DEFINE_REFCOUNTED_TYPE(TWorkloadModelManager)

////////////////////////////////////////////////////////////////////////////////

class TIOModelInterceptor
    : public IIOEngineWorkloadModel
{
public:
    TIOModelInterceptor(TString locationId, IIOEnginePtr underlying, NLogging::TLogger logger)
        : Underlying_(std::move(underlying))
        , Logger(std::move(logger))
        , ModelManager_(New<TWorkloadModelManager>(std::move(locationId), Logger))
    {
        ModelManager_->SubscribeRequestSizesSignal(
            BIND(&TIOModelInterceptor::OnUpdateRequestSizes, MakeWeak(this)));

        ModelManager_->SubscribeRequestLatenciesSignal(
            BIND(&TIOModelInterceptor::OnUpdateRequestLatencies, MakeWeak(this)));
    }

    TFuture<TReadResponse> Read(
        std::vector<TReadRequest> requests,
        const TWorkloadDescriptor& descriptor,
        TRefCountedTypeCookie tagCookie,
        TSessionId sessionId,
        bool useDedicatedAllocations) override
    {
        NProfiling::TWallTimer requestTimer;
        auto future = Underlying_->Read(
            requests,
            descriptor,
            tagCookie,
            sessionId,
            useDedicatedAllocations);

        // XXX(akozhikhov): Cannot use ApplyUnique without AsVoid() here. But this seems too heavy.
        future.AsVoid().Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const NYT::TError& /*error*/) {
            auto duration = requestTimer.GetElapsedTime();
            for (const auto& request : requests) {
                ModelManager_->RegisterRead(request, descriptor.Category, duration);
            }
        }));

        return future;
    }

    TFuture<void> Write(
        TWriteRequest request,
        const TWorkloadDescriptor& descriptor,
        TSessionId sessionId) override
    {
        NProfiling::TWallTimer requestTimer;

        auto future = Underlying_->Write(request, descriptor, sessionId);

        future.Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const NYT::TErrorOr<void>&) {
            ModelManager_->RegisterWrite(request, descriptor.Category, requestTimer.GetElapsedTime());
        }));

        return future;
    }

    TFuture<void> FlushFile(
        TFlushFileRequest request,
        const TWorkloadDescriptor& descriptor,
        TSessionId sessionId) override
    {
        return Underlying_->FlushFile(std::move(request), descriptor, sessionId);
    }

    TFuture<void> FlushFileRange(
        TFlushFileRangeRequest request,
        const TWorkloadDescriptor& descriptor,
        TSessionId sessionId) override
    {
        return Underlying_->FlushFileRange(std::move(request), descriptor, sessionId);
    }

    TFuture<void> FlushDirectory(
        TFlushDirectoryRequest request,
        const TWorkloadDescriptor& descriptor,
        TSessionId sessionId) override
    {
        return Underlying_->FlushDirectory(std::move(request), descriptor, sessionId);
    }

    TFuture<TIOEngineHandlePtr> Open(
        TOpenRequest request,
        const TWorkloadDescriptor& descriptor,
        TSessionId sessionId) override
    {
        return Underlying_->Open(std::move(request), descriptor, sessionId);
    }

    TFuture<void> Close(
        TCloseRequest request,
        const TWorkloadDescriptor& descriptor,
        TSessionId sessionId) override
    {
        return Underlying_->Close(std::move(request), descriptor, sessionId);
    }

    TFuture<void> Allocate(
        TAllocateRequest request,
        const TWorkloadDescriptor& descriptor,
        TSessionId sessionId) override
    {
        return Underlying_->Allocate(std::move(request), descriptor, sessionId);
    }

    TFuture<void> Lock(
        TLockRequest request,
        const TWorkloadDescriptor& descriptor,
        TSessionId sessionId) override
    {
        return Underlying_->Lock(std::move(request), descriptor, sessionId);
    }

    TFuture<void> Resize(
        TResizeRequest request,
        const TWorkloadDescriptor& descriptor,
        TSessionId sessionId) override
    {
        return Underlying_->Resize(std::move(request), descriptor, sessionId);
    }

    bool IsSick() const override
    {
        return Underlying_->IsSick();
    }

    const IInvokerPtr& GetAuxPoolInvoker() override
    {
        return Underlying_->GetAuxPoolInvoker();
    }

    std::optional<TRequestSizes> GetRequestSizes() override
    {
        auto guard = Guard(StatsLock_);
        return RequestSizeStats_;
    }

    std::optional<TRequestLatencies> GetRequestLatencies() override
    {
        auto guard = Guard(StatsLock_);
        return RequestLatencyStats_;
    }

    i64 GetTotalReadBytes() const override
    {
        return Underlying_->GetTotalReadBytes();
    }

    i64 GetTotalWrittenBytes() const override
    {
        return Underlying_->GetTotalWrittenBytes();
    }

    void Reconfigure(const NYTree::INodePtr& config) override
    {
        Underlying_->Reconfigure(config);
    }

    EDirectIOPolicy UseDirectIOForReads() const override
    {
        return Underlying_->UseDirectIOForReads();
    }

    bool IsInFlightReadRequestLimitExceeded() const override
    {
        return Underlying_->IsInFlightReadRequestLimitExceeded();
    }

    bool IsInFlightWriteRequestLimitExceeded() const override
    {
        return Underlying_->IsInFlightWriteRequestLimitExceeded();
    }

    i64 GetInFlightReadRequestCount() const override
    {
        return Underlying_->GetInFlightReadRequestCount();
    }

    i64 GetReadRequestLimit() const override
    {
        return Underlying_->GetReadRequestLimit();
    }

    i64 GetInFlightWriteRequestCount() const override
    {
        return Underlying_->GetInFlightWriteRequestCount();
    }

    i64 GetWriteRequestLimit() const override
    {
        return Underlying_->GetWriteRequestLimit();
    }

private:
    const IIOEnginePtr Underlying_;
    const NLogging::TLogger Logger;
    const TWorkloadModelManagerPtr ModelManager_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, StatsLock_);
    std::optional<TRequestSizes> RequestSizeStats_;
    std::optional<TRequestLatencies> RequestLatencyStats_;

    void OnUpdateRequestSizes(const TRequestSizes& requestSizes)
    {
        auto guard = Guard(StatsLock_);
        RequestSizeStats_ = requestSizes;
    }

    void OnUpdateRequestLatencies(const TRequestLatencies& requestLatencies)
    {
        auto guard = Guard(StatsLock_);
        RequestLatencyStats_ = requestLatencies;
    }
};

////////////////////////////////////////////////////////////////////////////////

IIOEngineWorkloadModelPtr CreateIOModelInterceptor(
    TString locationId,
    IIOEnginePtr underlying,
    NLogging::TLogger logger)
{
    return New<TIOModelInterceptor>(
        std::move(locationId),
        std::move(underlying),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
