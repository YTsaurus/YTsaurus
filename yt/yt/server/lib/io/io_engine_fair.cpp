#include "io_engine.h"
#include "io_engine_base.h"
#include "io_engine_uring.h"
#include "io_request_slicer.h"
#include "private.h"

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/two_level_fair_share_thread_pool.h>
#include <yt/yt/core/concurrency/fair_share_weighted_thread_pool.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/threading/thread.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/sync_cache.h>

#include <yt/yt/client/misc/workload.h>

#include <library/cpp/yt/threading/notification_handle.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <library/cpp/yt/system/handle_eintr.h>

#include <util/generic/size_literals.h>
#include <util/generic/xrange.h>
#include <util/generic/set.h>

#include <array>

#ifdef _linux_
    #include <sys/uio.h>
#endif

namespace NYT::NIO {

using namespace NConcurrency::NFairShareWeightedThreadPool;
using namespace NProfiling;

using TSessionId = TIOEngineBase::TSessionId;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TFairIOEngineConfig)

class TFairIOEngineConfig
    : public TIOEngineConfigBase
{
public:
    // Request size in bytes.
    int DesiredRequestSize;
    int MinRequestSize;

    // always flush after write with specified flags
    bool AlwaysFlushAfterWrite;
    bool SyncFileRangeWaitBefore;
    bool SyncFileRangeWrite;
    bool SyncFileRangeWaitAfter;

    bool FlushAfterWrite;
    bool AsyncFlushAfterWrite;

    // Thread count for read and write operations.
    int ReadWriteThreadCount;

    // Fair IO engine tries to estimate disk load of a single operation by the number of bytes submitted to disk.
    // This approximation is bad for HDDs because even small request can force HDD to rotate and take some time.
    // AdditiveCostOfOperationInBytes increases estimation for all reqests by this amount of bytes.
    // This applies only to read and write operations.
    int AdditiveCostOfOperationInBytes;

    THashMap<EWorkloadCategory, double> PoolWeights;
    TDuration BucketTtl;

    REGISTER_YSON_STRUCT(TFairIOEngineConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("desired_request_size", &TThis::DesiredRequestSize)
            .GreaterThanOrEqual(4_KB)
            .Default(1_MB);
        registrar.Parameter("min_request_size", &TThis::MinRequestSize)
            .GreaterThanOrEqual(512)
            .Default(32_KB);

        registrar.Parameter("always_flush_after_write", &TThis::AlwaysFlushAfterWrite)
            .Default(true);
        registrar.Parameter("sync_file_range_write_before", &TThis::SyncFileRangeWaitBefore)
            .Default(true);
        registrar.Parameter("sync_file_range_write", &TThis::SyncFileRangeWrite)
            .Default(true);
        registrar.Parameter("sync_file_range_write_after", &TThis::SyncFileRangeWaitAfter)
            .Default(true);

        registrar.Parameter("flush_after_write", &TThis::FlushAfterWrite)
            .Default(false);
        registrar.Parameter("async_flush_after_write", &TThis::AsyncFlushAfterWrite)
            .Default(false);

        registrar.Parameter("readwrite_thread_count", &TThis::ReadWriteThreadCount)
            .GreaterThanOrEqual(1)
            .Default(8);

        registrar.Parameter("additive_cost_of_operation_in_bytes", &TThis::AdditiveCostOfOperationInBytes)
            .GreaterThanOrEqual(0)
            .Default(0);

        registrar.Parameter("pool_weights", &TThis::PoolWeights)
            .Default();
        registrar.Parameter("bucket_ttl", &TThis::BucketTtl)
            .Default(TDuration::Minutes(5));

        registrar.Postprocessor([] (TThis* config) {
            for (auto category : TEnumTraits<EWorkloadCategory>::GetDomainValues()) {
                if (!config->PoolWeights.contains(category)) {
                    config->PoolWeights[category] = GetBasicWeight(category);
                }
                if (config->PoolWeights[category] <= 0) {
                    THROW_ERROR_EXCEPTION("Pool weight for category %Qv must be positive. Current weight: %v", category, config->PoolWeights[category]);
                }
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TFairIOEngineConfig)

////////////////////////////////////////////////////////////////////////////////

class TTwoLevelPoolWeightProvider
    : public IPoolWeightProvider
{
public:
    TTwoLevelPoolWeightProvider(THashMap<EWorkloadCategory, double> poolWeights)
        : PoolWeights_(std::move(poolWeights))
    { }

    double GetWeight(const TString& poolName) override
    {
        auto category = TEnumTraits<EWorkloadCategory>::FindValueByLiteral(poolName);
        if (category) {
            return PoolWeights_.contains(*category) ? PoolWeights_[*category] : 1.0;
        } else {
            return 1.0;
        }
    }

    void Configure(THashMap<EWorkloadCategory, double> poolWeights)
    {
        PoolWeights_ = std::move(poolWeights);
    }

private:
    THashMap<EWorkloadCategory, double> PoolWeights_;
};

struct TBucketCacheEntry final
{
    NProfiling::TCpuInstant LastAccessTime;
    TString PoolName;
    TString BucketName;
    IInvokerWithExpectedBytesPtr InvokerPtr;
};

bool operator < (const TIntrusivePtr<TBucketCacheEntry>& lhs, const TIntrusivePtr<TBucketCacheEntry>& rhs)
{
    return std::tie(lhs->LastAccessTime, lhs->PoolName, lhs->BucketName) < std::tie(rhs->LastAccessTime, rhs->PoolName, rhs->BucketName);
}

// Caches pointers to buckets in order to prolong their live(and progress) between calls.
class TBucketCache final
{
public:
    TBucketCache(
        TDuration bucketEntryTtl,
        NLogging::TLogger logger)
        : EntryTtl_(bucketEntryTtl)
        , Logger_(logger)
    { }

    IInvokerWithExpectedBytesPtr Get(
        const IFairShareWeightedThreadPoolPtr threadPool,
        const TString& poolName,
        const TString& bucketTag,
        const double bucketWeight)
    {
        auto guard = Guard(Lock_);
        auto now = NProfiling::GetCpuInstant();
        FlushByTtl(now);

        auto entry = KeyToEntry_.find(std::make_pair(poolName, bucketTag));
        if (entry != KeyToEntry_.end()) {
            EntriesSet_.erase(entry->second);
            entry->second->LastAccessTime = now;
            EntriesSet_.insert(entry->second);
            return entry->second->InvokerPtr;
        } else {
            auto newEntry = New<TBucketCacheEntry>();
            newEntry->LastAccessTime = now;
            newEntry->PoolName = poolName;
            newEntry->BucketName = bucketTag;
            newEntry->InvokerPtr = threadPool->GetInvokerWithExpectedBytes(poolName, bucketTag, bucketWeight);
            KeyToEntry_[std::make_pair(poolName, bucketTag)] = newEntry;
            EntriesSet_.insert(newEntry);
            return newEntry->InvokerPtr;
        }
    }

private:
    // Should be called with Lock acquired.
    void FlushByTtl(NProfiling::TCpuInstant now)
    {
        while (!EntriesSet_.empty() && (*EntriesSet_.begin())->LastAccessTime + NProfiling::DurationToCpuDuration(EntryTtl_) < now) {
            auto entry = *EntriesSet_.begin();
            EntriesSet_.erase(EntriesSet_.begin());
            KeyToEntry_.erase(std::make_pair(entry->PoolName, entry->BucketName));
        }
    }

private:
    const TDuration EntryTtl_;
    const NLogging::TLogger Logger_;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    THashMap<std::pair<TString, TString>, TIntrusivePtr<TBucketCacheEntry>> KeyToEntry_;
    TSet<TIntrusivePtr<TBucketCacheEntry>> EntriesSet_;
};

class TTwoLevelFairShareThreadPool
{
public:
    TTwoLevelFairShareThreadPool(
        TFairIOEngineConfigPtr config,
        const TString& locationId,
        NLogging::TLogger logger)
        : PoolWeightProvider_(New<TTwoLevelPoolWeightProvider>(config->PoolWeights))
        , ReadWriteThreadPool_(CreateFairShareWeightedThreadPool(
            config->ReadWriteThreadCount,
            Format("TLFLoc:%v", locationId),
            {
                PoolWeightProvider_,
                false
            }))
        , Logger(logger)
        , BucketCache_(New<TBucketCache>(config->BucketTtl, logger))
    { }

    IInvokerWithExpectedBytesPtr GetInvoker(const TWorkloadDescriptor& workloadDescriptor, TSessionId sessionId)
    {
        const auto& poolName = ToString(workloadDescriptor.Category);
        const auto& bucketTag = GetBucketTag(workloadDescriptor, sessionId);
        const auto bucketWeight = GetBucketWeight(workloadDescriptor);
        return BucketCache_->Get(ReadWriteThreadPool_, poolName, bucketTag, bucketWeight);
    }

    void Reconfigure(const TFairIOEngineConfigPtr& config)
    {
        PoolWeightProvider_->Configure(config->PoolWeights);
        ReadWriteThreadPool_->Configure(config->ReadWriteThreadCount);
    }

private:
    TString GetBucketTag(const TWorkloadDescriptor& workloadDescriptor, TSessionId sessionId)
    {
        if (workloadDescriptor.DiskFairShareBucketTag) {
            return *workloadDescriptor.DiskFairShareBucketTag;
        } else if (!sessionId.IsEmpty()) {
            return ToString(sessionId);
        } else {
            // TODO a better solution than random bucket?
            return "Random-" + ToString(TSessionId::Create());
        }
    }

    double GetBucketWeight(const TWorkloadDescriptor& workloadDescriptor)
    {
        if (workloadDescriptor.DiskFairShareBucketWeight && *workloadDescriptor.DiskFairShareBucketWeight > 0) {
            return *workloadDescriptor.DiskFairShareBucketWeight;
        } else {
            return 1.0;
        }
    }

private:
    TIntrusivePtr<TTwoLevelPoolWeightProvider> PoolWeightProvider_;
    const IFairShareWeightedThreadPoolPtr ReadWriteThreadPool_;
    const NLogging::TLogger Logger;
    TIntrusivePtr<TBucketCache> BucketCache_;
};

class TWrappedInvokerWithExpectedBytes
    : public virtual IInvoker
{
public:
    TWrappedInvokerWithExpectedBytes(IInvokerWithExpectedBytesPtr invoker, i64 expectedBytes)
        : Underlying_(std::move(invoker))
        , ExpectedBytes_(expectedBytes)
    {}

    //! Schedules invocation of a given callback.
    void Invoke(TClosure callback) override
    {
        Underlying_->InvokeWithExpectedBytes(callback, ExpectedBytes_);
    }

    //! Schedules multiple callbacks.
    void Invoke(TMutableRange<TClosure> callbacks) override
    {
        std::vector<std::pair<TClosure, i64>> newCallbacks;
        for (auto& callback : callbacks) {
            newCallbacks.push_back({callback, ExpectedBytes_});
        }
        Underlying_->InvokeWithExpectedBytes(newCallbacks);
    }

    //! Returns the thread id this invoker is bound to.
    //! For invokers not bound to any particular thread,
    //! returns |InvalidThreadId|.
    NThreading::TThreadId GetThreadId() const override
    {
        return Underlying_->GetThreadId();
    }

    //! Returns true if this invoker is either equal to #invoker or wraps it,
    //! in some sense.
    bool CheckAffinity(const IInvokerPtr& invoker) const override
    {
        return Underlying_->CheckAffinity(invoker);
    }

    //! Returns true if invoker is serialized, i.e. never executes
    //! two callbacks concurrently.
    bool IsSerialized() const override
    {
        return Underlying_->IsSerialized();
    }

    using TWaitTimeObserver = std::function<void(TDuration)>;
    void RegisterWaitTimeObserver(TWaitTimeObserver waitTimeObserver) override
    {
        return Underlying_->RegisterWaitTimeObserver(waitTimeObserver);
    }

private:
    IInvokerWithExpectedBytesPtr Underlying_;
    i64 ExpectedBytes_;
};

DEFINE_REFCOUNTED_TYPE(TWrappedInvokerWithExpectedBytes)

template <typename TRequestSlicer>
class TFairIOEngine
    : public TIOEngineBase
{
public:
    using TConfig = TFairIOEngineConfig;
    using TConfigPtr = TIntrusivePtr<TConfig>;

    TFairIOEngine(
        TFairIOEngineConfigPtr config,
        TString locationId,
        TProfiler profiler,
        NLogging::TLogger logger)
        : TIOEngineBase(
            config,
            std::move(locationId),
            std::move(profiler),
            std::move(logger))
        , StaticConfig_(std::move(config))
        , ThreadPool_(StaticConfig_, LocationId_, Logger)
        , Config_(StaticConfig_)
        , RequestSlicer_(StaticConfig_->DesiredRequestSize, StaticConfig_->MinRequestSize)
    { }

    TFuture<TReadResponse> Read(
        std::vector<TReadRequest> requests,
        const TWorkloadDescriptor& descriptor,
        TRefCountedTypeCookie tagCookie,
        TSessionId sessionId,
        bool useDedicatedAllocations) override
    {
        std::vector<TFuture<void>> futures;
        futures.reserve(requests.size());

        auto invoker = ThreadPool_.GetInvoker(descriptor, sessionId);

        i64 paddedBytes = 0;
        for (const auto& request : requests) {
            paddedBytes += GetPaddedSize(request.Offset, request.Size, DefaultPageSize);
        }
        auto buffers = AllocateReadBuffers(requests, tagCookie, useDedicatedAllocations);

        for (int index = 0; index < std::ssize(requests); ++index) {
            for (auto& slice : RequestSlicer_.Slice(std::move(requests[index]), buffers[index])) {
                i64 expectedBytes = Config_.Acquire()->AdditiveCostOfOperationInBytes + slice.Request.Size;
                futures.push_back(
                    BIND(&TFairIOEngine::DoRead,
                        MakeStrong(this),
                        std::move(slice.Request),
                        std::move(slice.OutputBuffer),
                        TWallTimer(),
                        descriptor,
                        sessionId)
                    .AsyncVia(New<TWrappedInvokerWithExpectedBytes>(invoker, expectedBytes))
                    .Run());
            }
        }

        TReadResponse response{
            .PaddedBytes = paddedBytes,
            .IORequests = std::ssize(futures),
        };
        response.OutputBuffers.assign(buffers.begin(), buffers.end());

        return AllSucceeded(std::move(futures))
            .Apply(BIND([response = std::move(response)] () mutable {
                return std::move(response);
            }));
    }

    TFuture<void> Write(
        TWriteRequest request,
        const TWorkloadDescriptor& descriptor,
        TSessionId sessionId) override
    {
        YT_ASSERT(request.Handle);
        std::vector<TFuture<void>> futures;
        for (auto& slice : RequestSlicer_.Slice(std::move(request))) {
            auto expectedBytes = Config_.Acquire()->AdditiveCostOfOperationInBytes + static_cast<i64>(GetByteSize(slice.Buffers));
            futures.push_back(
                BIND(&TFairIOEngine::DoWrite, MakeStrong(this), std::move(slice), TWallTimer())
                .AsyncVia(New<TWrappedInvokerWithExpectedBytes>(ThreadPool_.GetInvoker(descriptor, sessionId), expectedBytes))
                .Run());
        }
        return AllSucceeded(std::move(futures));
    }

    TFuture<void> FlushFile(
        TFlushFileRequest request,
        const TWorkloadDescriptor& descriptor,
        TSessionId sessionId) override
    {
        auto expectedBytes = 0;
        return BIND(&TFairIOEngine::DoFlushFile, MakeStrong(this), std::move(request))
            .AsyncVia(New<TWrappedInvokerWithExpectedBytes>(ThreadPool_.GetInvoker(descriptor, sessionId), expectedBytes))
            .Run();
    }

    virtual TFuture<void> FlushFileRange(
        TFlushFileRangeRequest request,
        const TWorkloadDescriptor& descriptor,
        TSessionId sessionId) override
    {
        auto expectedBytes = 0;
        std::vector<TFuture<void>> futures;
        for (auto& slice : RequestSlicer_.Slice(std::move(request))) {
            futures.push_back(
                BIND(&TFairIOEngine::DoFlushFileRange, MakeStrong(this), std::move(slice))
                .AsyncVia(New<TWrappedInvokerWithExpectedBytes>(ThreadPool_.GetInvoker(descriptor, sessionId), expectedBytes))
                .Run());
        }
        return AllSucceeded(std::move(futures));
    }

protected:
    const TFairIOEngineConfigPtr StaticConfig_;
    TTwoLevelFairShareThreadPool ThreadPool_;

private:
    TAtomicIntrusivePtr<TFairIOEngineConfig> Config_;
    TRequestSlicer RequestSlicer_;


    std::vector<TSharedMutableRef> AllocateReadBuffers(
        const std::vector<TReadRequest>& requests,
        TRefCountedTypeCookie tagCookie,
        bool useDedicatedAllocations)
    {
        bool shouldBeAligned = std::any_of(
            requests.begin(),
            requests.end(),
            [] (const TReadRequest& request) {
                return request.Handle->IsOpenForDirectIO();
            });

        auto allocate = [&] (size_t size) {
            TSharedMutableRefAllocateOptions options{
                .InitializeStorage = false
            };
            return shouldBeAligned
                ? TSharedMutableRef::AllocatePageAligned(size, options, tagCookie)
                : TSharedMutableRef::Allocate(size, options, tagCookie);
        };

        std::vector<TSharedMutableRef> results;
        results.reserve(requests.size());

        if (useDedicatedAllocations) {
            for (const auto& request : requests) {
                results.push_back(allocate(request.Size));
            }
            return results;
        }

        // Collocate blocks in single buffer.
        i64 totalSize = 0;
        for (const auto& request : requests) {
            totalSize += shouldBeAligned
                ? AlignUp<i64>(request.Size, DefaultPageSize)
                : request.Size;
        }

        auto buffer = allocate(totalSize);
        i64 offset = 0;
        for (const auto& request : requests) {
            results.push_back(buffer.Slice(offset, offset + request.Size));
            offset += shouldBeAligned
                ? AlignUp<i64>(request.Size, DefaultPageSize)
                : request.Size;
        }
        return results;
    }

    void DoRead(
        const TReadRequest& request,
        TMutableRef buffer,
        TWallTimer timer,
        const TWorkloadDescriptor& descriptor,
        TSessionId sessionId)
    {
        Y_UNUSED(descriptor); // TODO fix
        YT_VERIFY(std::ssize(buffer) == request.Size);

        const auto readWaitTime = timer.GetElapsedTime();
        AddReadWaitTimeSample(readWaitTime);
        Sensors_->UpdateKernelStatistics();

        auto toReadRemaining = static_cast<i64>(buffer.Size());
        auto fileOffset = request.Offset;
        i64 bufferOffset = 0;

        YT_LOG_DEBUG_IF(descriptor.Category == EWorkloadCategory::UserInteractive,
            "Started reading from disk (Handle: %v, RequestSize: %v, ReadSessionId: %v, ReadWaitTime: %v)",
            static_cast<FHANDLE>(*request.Handle),
            request.Size,
            sessionId,
            readWaitTime);

        NFS::WrapIOErrors([&] {
            auto config = Config_.Acquire();

            while (toReadRemaining > 0) {
                auto toRead = static_cast<ui32>(Min(toReadRemaining, config->MaxBytesPerRead));

                i64 reallyRead;
                {
                    TRequestStatsGuard statsGuard(Sensors_->ReadSensors);
                    NTracing::TNullTraceContextGuard nullTraceContextGuard;
                    reallyRead = HandleEintr(::pread, *request.Handle, buffer.Begin() + bufferOffset, toRead, fileOffset);

                    YT_LOG_DEBUG_IF(descriptor.Category == EWorkloadCategory::UserInteractive,
                        "Finished reading from disk (Handle: %v, ReadBytes: %v, ReadSessionId: %v, ReadTime: %v)",
                        static_cast<FHANDLE>(*request.Handle),
                        reallyRead,
                        sessionId,
                        statsGuard.GetElapsedTime());
                }

                if (reallyRead < 0) {
                    // TODO(aozeritsky): ythrow is placed here consciously.
                    // WrapIOErrors rethrows some kind of arcadia-style exception.
                    // So in order to keep the old behaviour we should use ythrow or
                    // rewrite WrapIOErrors.
                    ythrow TFileError();
                }

                if (reallyRead == 0) {
                    break;
                }

                Sensors_->RegisterReadBytes(reallyRead);
                if (StaticConfig_->SimulatedMaxBytesPerRead) {
                    reallyRead = Min(reallyRead, *StaticConfig_->SimulatedMaxBytesPerRead);
                }

                fileOffset += reallyRead;
                bufferOffset += reallyRead;
                toReadRemaining -= reallyRead;
            }
        });

        if (toReadRemaining > 0) {
            THROW_ERROR_EXCEPTION(NFS::EErrorCode::IOError, "Unexpected end-of-file in read request")
                << TErrorAttribute("to_read_remaining", toReadRemaining)
                << TErrorAttribute("max_bytes_per_read", StaticConfig_->MaxBytesPerRead)
                << TErrorAttribute("request_size", request.Size)
                << TErrorAttribute("request_offset", request.Offset)
                << TErrorAttribute("file_size", request.Handle->GetLength())
                << TErrorAttribute("handle", static_cast<FHANDLE>(*request.Handle));
        }
    }

    void DoWrite(
        const TWriteRequest& request,
        TWallTimer timer)
    {
        AddWriteWaitTimeSample(timer.GetElapsedTime());
        Sensors_->UpdateKernelStatistics();

        auto fileOffset = request.Offset;

        NFS::WrapIOErrors([&] {
            NTracing::TNullTraceContextGuard nullTraceContextGuard;

            auto toWriteRemaining = static_cast<i64>(GetByteSize(request.Buffers));

            int bufferIndex = 0;
            i64 bufferOffset = 0; // within current buffer

            auto config = Config_.Acquire();
            while (toWriteRemaining > 0) {
                auto isPwritevSupported = [&] {
#ifdef _linux_
                    return true;
#else
                    return false;
#endif
                };

                auto pwritev = [&] {
#ifdef _linux_
                    std::array<iovec, MaxIovCountPerRequest> iov;
                    int iovCount = 0;
                    i64 toWrite = 0;
                    while (bufferIndex + iovCount < std::ssize(request.Buffers) &&
                           iovCount < std::ssize(iov) &&
                           toWrite < config->MaxBytesPerWrite)
                    {
                        const auto& buffer = request.Buffers[bufferIndex + iovCount];
                        auto& iovPart = iov[iovCount];
                        iovPart = {
                            .iov_base = const_cast<char*>(buffer.Begin()),
                            .iov_len = buffer.Size()
                        };
                        if (iovCount == 0) {
                            iovPart.iov_base = static_cast<char*>(iovPart.iov_base) + bufferOffset;
                            iovPart.iov_len -= bufferOffset;
                        }
                        if (toWrite + static_cast<i64>(iovPart.iov_len) > config->MaxBytesPerWrite) {
                            iovPart.iov_len = config->MaxBytesPerWrite - toWrite;
                        }
                        toWrite += iovPart.iov_len;
                        ++iovCount;
                    }

                    i64 reallyWritten;
                    {
                        TRequestStatsGuard statsGuard(Sensors_->WriteSensors);
                        NTracing::TNullTraceContextGuard nullTraceContextGuard;
                        reallyWritten = HandleEintr(::pwritev, *request.Handle, iov.data(), iovCount, fileOffset);
                    }

                    if (reallyWritten < 0) {
                        ythrow TFileError();
                    }

                    Sensors_->RegisterWrittenBytes(reallyWritten);
                    if (StaticConfig_->SimulatedMaxBytesPerWrite) {
                        reallyWritten = Min(reallyWritten, *StaticConfig_->SimulatedMaxBytesPerWrite);
                    }

                    while (reallyWritten > 0) {
                        const auto& buffer = request.Buffers[bufferIndex];
                        i64 toAdvance = Min(static_cast<i64>(buffer.Size()) - bufferOffset, reallyWritten);
                        fileOffset += toAdvance;
                        bufferOffset += toAdvance;
                        reallyWritten -= toAdvance;
                        toWriteRemaining -= toAdvance;
                        if (bufferOffset == std::ssize(buffer)) {
                            ++bufferIndex;
                            bufferOffset = 0;
                        }
                    }
#else
                    YT_ABORT();
#endif
                };

                auto pwrite = [&] {
                    const auto& buffer = request.Buffers[bufferIndex];
                    auto toWrite = static_cast<ui32>(Min(toWriteRemaining, config->MaxBytesPerWrite, static_cast<i64>(buffer.Size()) - bufferOffset));

                    i32 reallyWritten;
                    {
                        TRequestStatsGuard statsGuard(Sensors_->WriteSensors);
                        NTracing::TNullTraceContextGuard nullTraceContextGuard;
                        reallyWritten = HandleEintr(::pwrite, *request.Handle, const_cast<char*>(buffer.Begin()) + bufferOffset, toWrite, fileOffset);
                    }

                    if (reallyWritten < 0) {
                        ythrow TFileError();
                    }

                    Sensors_->RegisterWrittenBytes(reallyWritten);
                    fileOffset += reallyWritten;
                    bufferOffset += reallyWritten;
                    toWriteRemaining -= reallyWritten;
                    if (bufferOffset == std::ssize(buffer)) {
                        ++bufferIndex;
                        bufferOffset = 0;
                    }
                };

                if (isPwritevSupported()) {
                    pwritev();
                } else {
                    pwrite();
                }
            }
        });

        auto writtenBytes = fileOffset - request.Offset;

        auto config = Config_.Acquire();
        if (config->AlwaysFlushAfterWrite && writtenBytes) {
            DoFlushFileRange(TFlushFileRangeRequest{
                .Handle = request.Handle,
                .Offset = request.Offset,
                .Size = writtenBytes,
                .UseSpecifiedFlags = true,
                .SyncFileRangeWaitBefore = config->SyncFileRangeWaitBefore,
                .SyncFileRangeWrite = config->SyncFileRangeWrite,
                .SyncFileRangeWaitAfter = config->SyncFileRangeWaitAfter
                });
        } else if (config->FlushAfterWrite && request.Flush && writtenBytes) {
            DoFlushFileRange(TFlushFileRangeRequest{
                .Handle = request.Handle,
                .Offset = request.Offset,
                .Size = writtenBytes
            });
        } else if (config->AsyncFlushAfterWrite && writtenBytes) {
            DoFlushFileRange(TFlushFileRangeRequest{
                .Handle = request.Handle,
                .Offset = request.Offset,
                .Size = writtenBytes,
                .Async = true
            });
        }
    }

    void DoFlushFile(const TFlushFileRequest& request)
    {
        Sensors_->UpdateKernelStatistics();
        if (!StaticConfig_->EnableSync) {
            return;
        }

        auto doFsync = [&] {
            TRequestStatsGuard statsGuard(Sensors_->SyncSensors);
            return HandleEintr(::fsync, *request.Handle);
        };

#ifdef _linux_
        auto doFdatasync = [&] {
            TRequestStatsGuard statsGuard(Sensors_->DataSyncSensors);
            return HandleEintr(::fdatasync, *request.Handle);
        };
#else
        auto doFdatasync = doFsync;
#endif

        NFS::WrapIOErrors([&] {
            NTracing::TNullTraceContextGuard nullTraceContextGuard;
            int result;
            switch (request.Mode) {
                case EFlushFileMode::All:
                    result = doFsync();
                    break;
                case EFlushFileMode::Data:
                    result = doFdatasync();
                    break;
                default:
                    YT_ABORT();
            }
            if (result != 0) {
                ythrow TFileError();
            }
        });
    }

    void DoFlushFileRange(const TFlushFileRangeRequest& request)
    {
        Sensors_->UpdateKernelStatistics();
        if (!StaticConfig_->EnableSync) {
            return;
        }

#ifdef _linux_
        NFS::WrapIOErrors([&] {
            NTracing::TNullTraceContextGuard nullTraceContextGuard;
            int result = 0;
            {
                TRequestStatsGuard statsGuard(Sensors_->DataSyncSensors);
                auto flags = request.Async
                    ? SYNC_FILE_RANGE_WRITE
                    : SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_AFTER;
                if (request.UseSpecifiedFlags) {
                    flags = 0;
                    if (request.SyncFileRangeWaitBefore) {
                        flags |= SYNC_FILE_RANGE_WAIT_BEFORE;
                    }
                    if (request.SyncFileRangeWrite) {
                        flags |= SYNC_FILE_RANGE_WRITE;
                    }
                    if (request.SyncFileRangeWaitAfter) {
                        flags |= SYNC_FILE_RANGE_WAIT_AFTER;
                    }
                }
                result = HandleEintr(::sync_file_range, *request.Handle, request.Offset, request.Size, flags);
            };
            if (result != 0) {
                ythrow TFileError();
            }
        });
#else

    Y_UNUSED(request);

#endif

    }

    void DoReconfigure(const NYTree::INodePtr& node) override
    {
        auto config = UpdateYsonStruct(StaticConfig_, node);

        ThreadPool_.Reconfigure(config);
        Config_.Store(config);
    }
};

////////////////////////////////////////////////////////////////////////////////

IIOEnginePtr CreateIOEngineFair(
    NYTree::INodePtr ioConfig,
    TString locationId,
    NProfiling::TProfiler profiler,
    NLogging::TLogger logger)

{
#ifdef _linux_

    return CreateIOEngine<TFairIOEngine<TIORequestSlicer>>(
        std::move(ioConfig),
        std::move(locationId),
        std::move(profiler),
        std::move(logger));

#endif
    return { };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
