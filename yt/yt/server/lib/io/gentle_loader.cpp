#include "gentle_loader.h"
#include "config.h"
#include "io_workload_model.h"

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/periodic_yielder.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/nonblocking_queue.h>

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/misc/fs.h>

#include <util/random/random.h>

namespace NYT::NIO {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static constexpr int PageSize = 4_KB;
static constexpr int IOScale = 100;

// TODO(aleksandr.gaev): better workload category?
static const TWorkloadDescriptor GentleLoaderWorkloadDescriptor(
    EWorkloadCategory::UserBatch,
    0,
    {},
    {},
    {},
    "TGentleLoader",
    {});

////////////////////////////////////////////////////////////////////////////////

class TRandomReader
    : public TRefCounted
{
public:
    TRandomReader(
        TGentleLoaderConfigPtr config,
        IRandomFileProviderPtr fileProvider,
        IIOEngineWorkloadModelPtr engine,
        NLogging::TLogger logger)
        : Config_(std::move(config))
        , IOEngine_(std::move(engine))
        , FileProvider(fileProvider)
        , Logger(std::move(logger))
    {
        OpenNewFiles();
    }

    void OpenNewFiles()
    {
        std::vector<TFuture<TReadFileInfo>> futures;
        for (int i = 0; i < Config_->ReaderCount; ++i) {
            auto file = FileProvider->GetRandomExistingFile();
            if (!file) {
                continue;
            }

            auto mode = OpenExisting | RdOnly | CloseOnExec;
            if (Config_->UseDirectIO) {
                mode |= DirectAligned;
            }

            auto future = IOEngine_->Open({file->Path, mode}, GentleLoaderWorkloadDescriptor, {})
                .Apply(BIND([file] (const TIOEngineHandlePtr& handle) {
                    return TReadFileInfo{
                        .Handle = handle,
                        .FileSize = file->DiskSpace,
                        .FilePath = file->Path,
                    };
                }));
            futures.push_back(future);
        }

        auto results = WaitFor(AllSet(futures))
            .Value();

        std::vector<TReadFileInfo> filesToRead;
        for (const auto& result : results) {
            if (result.IsOK()) {
                filesToRead.push_back(result.Value());
                YT_LOG_TRACE("Opened file for read (FilePath: %v, FileHandle: %v, FileSize: %v, FileLen: %v)",
                    result.Value().FilePath,
                    static_cast<FHANDLE>(*result.Value().Handle),
                    result.Value().FileSize,
                    result.Value().Handle->GetLength());
            }
        }

        std::swap(filesToRead, FilesToRead_);
    }

    TFuture<TDuration> Read(EWorkloadCategory category, i64 packetSize)
    {
        if (FilesToRead_.empty()) {
            return MakeFuture<TDuration>(TError("No files to read."));
        }
        auto index = RandomNumber<ui32>(FilesToRead_.size());
        return DoRandomRead(FilesToRead_[index], category, packetSize);
    }

private:
    struct TReadFileInfo
    {
        TIOEngineHandlePtr Handle;
        i64 FileSize = 0;
        TString FilePath;
    };

    const TGentleLoaderConfigPtr Config_;
    const IIOEngineWorkloadModelPtr IOEngine_;
    const IRandomFileProviderPtr FileProvider;
    const NLogging::TLogger Logger;
    std::vector<TReadFileInfo> FilesToRead_;

    TFuture<TDuration> DoRandomRead(
        TReadFileInfo fileInfo,
        EWorkloadCategory category,
        i64 readSize)
    {
        i64 offset = 0;
        if (fileInfo.FileSize > (readSize + PageSize)) {
            auto maxPageIndex = (fileInfo.FileSize - readSize) / PageSize;
            offset = RandomNumber<ui32>(maxPageIndex) * PageSize;
        } else {
            readSize = PageSize;
        }

        if (readSize + offset > fileInfo.FileSize) {
            return MakeFuture<TDuration>(TError("Wrong packets size")
                << TErrorAttribute("read_size", readSize)
                << TErrorAttribute("offset", offset)
                << TErrorAttribute("disk_space", fileInfo.FileSize));
        }

        struct TChunkFileReaderBufferTag
        { };

        auto descriptor = GentleLoaderWorkloadDescriptor;
        descriptor.Category = category;

        NProfiling::TWallTimer requestTimer;
        return IOEngine_->Read(
            {{fileInfo.Handle, offset, readSize}},
            descriptor,
            GetRefCountedTypeCookie<TChunkFileReaderBufferTag>(),
            {})
            .AsVoid()
            .Apply(BIND([requestTimer] {
                return requestTimer.GetElapsedTime();
            })).ToUncancelable();
    }
};

using TRandomReaderPtr = TIntrusivePtr<TRandomReader>;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TRandomWriter)

class TRandomWriter
    : public TRefCounted
{
public:
    struct TStopGuard
    {
        explicit TStopGuard(TRandomWriterPtr writer) :
            Writer(std::move(writer))
        { }

        ~TStopGuard()
        {
            Writer->Stop();
        }

        TRandomWriterPtr Writer;
    };

    TRandomWriter(
        TGentleLoaderConfigPtr config,
        const TString& path,
        IIOEngineWorkloadModelPtr engine,
        NLogging::TLogger logger)
        : Config_(std::move(config))
        , IOEngine_(std::move(engine))
        , Logger(std::move(logger))
        , WriteThreadPool_(CreateThreadPool(4, "RandomWriter"))
        , TempFilesDir(NFS::JoinPaths(path, Config_->WriterDirectory))
    {
        if (!NFS::Exists(TempFilesDir)) {
            NFS::MakeDirRecursive(TempFilesDir);
        }

        NFS::CleanTempFiles(TempFilesDir);
        auto invoker = WriteThreadPool_->GetInvoker();
        WritersQueue_.reserve(Config_->WriterCount);

        for (int index = 0; index < Config_->WriterCount; ++index) {
            WritersQueue_.push_back(std::make_shared<TNonblockingQueue<TRequestInfo>>());
            invoker->Invoke(BIND(&TRandomWriter::RunWriter, MakeStrong(this), index));
        }
    }

    TFuture<TDuration> Write(EWorkloadCategory category, i64 packetSize)
    {
        TRequestInfo request{
            .Category = category,
            .PacketSize = packetSize,
            .Promise = NewPromise<TDuration>(),
        };
        auto future = request.Promise.ToFuture();

        auto index = RandomNumber<ui32>(WritersQueue_.size());
        WritersQueue_[index]->Enqueue(std::move(request));

        return future;
    }

    void Stop()
    {
        auto invoker = WriteThreadPool_->GetInvoker();
        for (const auto& queue : WritersQueue_) {
            queue->Enqueue(TRequestInfo{
                .Stop = true,
            });
        }
    }

    TStopGuard StopGuard()
    {
        return TStopGuard{MakeStrong(this)};
    }

private:
    struct TRequestInfo
    {
        EWorkloadCategory Category = EWorkloadCategory::Idle;
        i64 PacketSize = 0;
        TPromise<TDuration> Promise;

        bool Stop = false;
    };

    using TNonblockingQueuePtr = std::shared_ptr<TNonblockingQueue<TRequestInfo>>;

    using TStaleFilesQueue = std::deque<TString>;

    struct TWriterInfo
    {
        int WriterIndex = 0;

        TString FilePath;
        TIOEngineHandlePtr Handle;
        i64 Offset = 0;

        TStaleFilesQueue StaleFiles;
    };


    const TGentleLoaderConfigPtr Config_;
    const IIOEngineWorkloadModelPtr IOEngine_;
    const NLogging::TLogger Logger;
    const IThreadPoolPtr WriteThreadPool_;
    const TString TempFilesDir;

    std::vector<TNonblockingQueuePtr> WritersQueue_;
    std::atomic<ui32> FileCounter_;

    void CloseFile(TWriterInfo& info)
    {
        if (info.Handle) {
            info.StaleFiles.push_back(info.FilePath);
        }
        info.Handle.Reset();
        info.Offset = 0;
        info.FilePath = {};
    }

    void CleanupStaleFiles(TStaleFilesQueue& staleFiles, int countToKeep = 0)
    {
        while (std::ssize(staleFiles) > countToKeep) {
            auto fileName = staleFiles.front();
            staleFiles.pop_front();

            try {
                NFS::Remove(fileName);
            } catch (const std::exception& ex) {
                YT_LOG_DEBUG(ex, "Can not remove file (FileName: %v)",
                    fileName);
            }
        }
    }

    void OpenNewFile(TWriterInfo& info)
    {
        CloseFile(info);
        CleanupStaleFiles(info.StaleFiles, Config_->StaleFileCountPerWriter);

        info.FilePath = NFS::JoinPaths(TempFilesDir, Format("%v%v", ++FileCounter_, NFS::TempFileSuffix));

        EOpenMode mode = CreateAlways | WrOnly | CloseOnExec;
        if (Config_->UseDirectIO) {
            mode |= DirectAligned;
        }

        info.Handle = WaitFor(IOEngine_->Open({info.FilePath, mode}, GentleLoaderWorkloadDescriptor, {}))
            .ValueOrThrow();

        if (Config_->PreallocateWriteFiles) {
            YT_LOG_INFO("Preallocating test write file (WriterIndex: %v, FileSize: %v)",
                info.WriterIndex,
                Config_->MaxWriteFileSize);

            WaitFor(IOEngine_->Allocate({.Handle = info.Handle, .Size = Config_->MaxWriteFileSize}, GentleLoaderWorkloadDescriptor, {}))
                .ThrowOnError();
        }
    }

    TDuration DoWrite(TWriterInfo& fileInfo, EWorkloadCategory category, i64 packetSize)
    {
        if (!fileInfo.Handle  || fileInfo.Offset >= Config_->MaxWriteFileSize) {
            YT_LOG_DEBUG("Opening new file (WriterIndex: %v)",
                fileInfo.WriterIndex);
            OpenNewFile(fileInfo);
        }

        auto handle = fileInfo.Handle;
        auto descriptor = GentleLoaderWorkloadDescriptor;
        descriptor.Category = category;

        NProfiling::TWallTimer requestTimer;
        auto future = IOEngine_->Write({
                .Handle = handle,
                .Offset = fileInfo.Offset,
                .Buffers = {MakeRandomBuffer(packetSize)},
            },
            descriptor,
            {})
            .Apply(BIND([&] () {
                if (Config_->FlushAfterWrite) {
                    return IOEngine_->FlushFile({handle, EFlushFileMode::Data}, descriptor, {});
                }
                return VoidFuture;
            }));

        WaitFor(future)
            .ThrowOnError();

        fileInfo.Offset += Config_->PacketSize;
        return requestTimer.GetElapsedTime();
    }

    void RunWriter(int writerIndex)
    {
        YT_LOG_DEBUG("Random writer started (WriterIndex: %v)",
                writerIndex);

        auto queue = WritersQueue_[writerIndex];
        TWriterInfo writerInfo{.WriterIndex = writerIndex};
        while (true) {
            auto future = queue->Dequeue();
            auto request = WaitFor(future).Value();
            if (request.Stop) {
                break;
            }

            try {
                auto duration = DoWrite(writerInfo, request.Category, request.PacketSize);
                request.Promise.Set(duration);
            } catch (std::exception& ex) {
                request.Promise.Set(TError(ex));
            }
        }

        CloseFile(writerInfo);
        if (Config_->RemoveWrittenFiles) {
            CleanupStaleFiles(writerInfo.StaleFiles);
        }

        YT_LOG_DEBUG("Random writer stopped (WriterIndex: %v)",
                writerIndex);
    }

    static TSharedMutableRef MakeRandomBuffer(int size)
    {
        auto data = TSharedMutableRef::AllocatePageAligned(size, {.InitializeStorage = false});
        for (int index = 0; index < size; ++index) {
            data[index] = RandomNumber<ui8>();
        }
        return data;
    }
};

DEFINE_REFCOUNTED_TYPE(TRandomWriter)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM_WITH_UNDERLYING_TYPE(ECongestedStatus, i16,
    ((OK)               (0))
    ((Overload)         (1))
    ((HeavyOverload)    (2))
);

struct TCongestedState
{
    ui16 Epoch = 0;
    ECongestedStatus Status = ECongestedStatus::OK;
};

////////////////////////////////////////////////////////////////////////////////

using TAtomicCongestedState = std::atomic<TCongestedState>;
static_assert(TAtomicCongestedState::is_always_lock_free);

class TInteractiveRequestsObserver
    : public TRefCounted
{
public:
    TInteractiveRequestsObserver(
        TCongestionDetectorConfigPtr config,
        IIOEngineWorkloadModelPtr engine,
        NLogging::TLogger logger)
        : Config_(std::move(config))
        , Logger(std::move(logger))
        , IOEngine_(std::move(engine))
    { }

    ECongestedStatus GetCongestionStatus()
    {
        auto current = GetCurrentCongestionStatus();
        if (current == ECongestedStatus::OK) {
            FailedProbesCounter_ = 0;
        } else {
            ++FailedProbesCounter_;
        }

        if (FailedProbesCounter_ >= Config_->UserRequestFailedProbesThreshold) {
            return current;
        }
        return ECongestedStatus::OK;
    }

private:
    const TCongestionDetectorConfigPtr Config_;
    const NLogging::TLogger Logger;
    const IIOEngineWorkloadModelPtr IOEngine_;

    i64 FailedProbesCounter_ = 0;

    ECongestedStatus GetCurrentCongestionStatus()
    {
        auto latencyHistogram = IOEngine_->GetRequestLatencies();
        if (!latencyHistogram) {
            return ECongestedStatus::OK;
        }

        const auto& userReadLatency = latencyHistogram->Reads[EWorkloadCategory::UserInteractive];
        auto quantiles = ComputeHistogramSummary(userReadLatency);
        auto q99Latency = TDuration::MilliSeconds(quantiles.P99);

        if (quantiles.TotalCount) {
            YT_LOG_DEBUG("User interactive request congestion status (Count: %v, 90p: %v ms, 99p: %v ms, 99.9p: %v ms)",
                quantiles.TotalCount,
                quantiles.P90,
                quantiles.P99,
                quantiles.P99_9);
        }

        if (q99Latency > Config_->UserRequestHeavyOverloadThreshold) {
            return ECongestedStatus::HeavyOverload;
        } else if (q99Latency > Config_->UserRequestOverloadThreshold) {
            return ECongestedStatus::Overload;
        }

        return ECongestedStatus::OK;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TProbesCongestionDetector
    : public TRefCounted
{
public:
    TProbesCongestionDetector(
        TCongestionDetectorConfigPtr config,
        IIOEngineWorkloadModelPtr engine,
        IInvokerPtr invoker,
        TRandomReaderPtr randomReader,
        NLogging::TLogger logger)
        : Config_(std::move(config))
        , Logger(std::move(logger))
        , IOEngine_(std::move(engine))
        , Invoker_(std::move(invoker))
        , RandomReader_(std::move(randomReader))
        , ProbesExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TProbesCongestionDetector::DoProbe, MakeWeak(this)),
            Config_->ProbesInterval))
    {
        if (Config_->ProbesEnabled) {
            ProbesExecutor_->Start();
        }
    }

    ~TProbesCongestionDetector()
    {
        YT_UNUSED_FUTURE(ProbesExecutor_->Stop());
    }

    ECongestedStatus GetCongestionStatus()
    {
        if (std::ssize(Probes_) < Config_->ProbesPerRound) {
            return ECongestedStatus::OK;
        }

        if (std::ssize(Probes_) > Config_->MaxInFlightProbeCount / 2) {
            return ECongestedStatus::HeavyOverload;
        }

        std::vector<TFuture<TDuration>> lastProbes;
        lastProbes.assign(Probes_.rbegin(), Probes_.rbegin() + Config_->ProbesPerRound);
        auto results = WaitFor(AllSetWithTimeout(lastProbes, Config_->ProbeDeadline))
            .Value();

        int failedRequestCount = 0;
        std::vector<TDuration> timings;
        for (const auto& result : results) {
            if (!result.IsOK()) {
                YT_LOG_DEBUG(result, "Probe is failed");
            } else {
                timings.push_back(result.Value());
            }
            if (!result.IsOK() || result.Value() > Config_->ProbeDeadline) {
                ++failedRequestCount;
            }
        }
        int failedRequestsPercentage = failedRequestCount * 100 / lastProbes.size();

        YT_LOG_DEBUG("Probes congested status (Count: %v, Failed: %v, Percentage: %v, TotalCount: %v, Timings: %v)",
            lastProbes.size(),
            failedRequestCount,
            failedRequestsPercentage,
            Probes_.size(),
            timings);

        if (failedRequestsPercentage > Config_->HeavyOverloadThreshold) {
            return ECongestedStatus::HeavyOverload;
        } else if (failedRequestsPercentage > Config_->OverloadThreshold) {
            return ECongestedStatus::Overload;
        }
        return ECongestedStatus::OK;
    }

private:
    const TCongestionDetectorConfigPtr Config_;
    const NLogging::TLogger Logger;
    const IIOEngineWorkloadModelPtr IOEngine_;
    const IInvokerPtr Invoker_;
    const TRandomReaderPtr RandomReader_;
    const TPeriodicExecutorPtr ProbesExecutor_;

    std::deque<TFuture<TDuration>> Probes_;

    void DoProbe()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        try {
            // Dropping old data.
            while (std::ssize(Probes_) > Config_->ProbesPerRound && Probes_.front().IsSet()) {
                Probes_.pop_front();
            }

            if (std::ssize(Probes_) < Config_->MaxInFlightProbeCount) {
                Probes_.push_back(RandomReader_->Read(EWorkloadCategory::Idle, Config_->PacketSize));
            } else {
                YT_LOG_ERROR("Congestion Detector max probe request count reached");
            }
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Congestion Detector probes failed");
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCongestionDetector
    : public TRefCounted
{
public:
    TCongestionDetector(
        TCongestionDetectorConfigPtr config,
        IIOEngineWorkloadModelPtr engine,
        IInvokerPtr invoker,
        TRandomReaderPtr randomReader,
        NLogging::TLogger logger)
        : Invoker_(std::move(invoker))
        , ProbesExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TCongestionDetector::DoProbe, MakeWeak(this)),
            config->ProbesInterval * config->ProbesPerRound))
    {
        InteractiveObserver_ = New<TInteractiveRequestsObserver>(config, engine, logger);
        ProbesObserver_ = New<TProbesCongestionDetector>(
            config,
            engine,
            Invoker_,
            randomReader,
            logger);

        ProbesExecutor_->Start();
    }

    ~TCongestionDetector()
    {
        YT_UNUSED_FUTURE(ProbesExecutor_->Stop());
    }

    TCongestedState GetState() const
    {
        return State_.load(std::memory_order::relaxed);
    }

private:
    const IInvokerPtr Invoker_;
    const TPeriodicExecutorPtr ProbesExecutor_;

    ui16 EpochCounter_ = 0;
    TAtomicCongestedState State_ = {};

    TIntrusivePtr<TInteractiveRequestsObserver> InteractiveObserver_;
    TIntrusivePtr<TProbesCongestionDetector> ProbesObserver_;

    void DoProbe()
    {
        TCongestedState state;
        state.Epoch = ++EpochCounter_;

        state.Status = std::max(
            InteractiveObserver_->GetCongestionStatus(),
            ProbesObserver_->GetCongestionStatus());

        State_.store(state, std::memory_order::relaxed);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRequestSampler final
{
public:
    struct TRequestInfo
    {
        EWorkloadCategory Workload = EWorkloadCategory::Idle;
        i64 Size = 0;
    };

    TRequestSampler(
        i64 defaultSize,
        const TEnumIndexedVector<EWorkloadCategory, TRequestSizeHistogram>&  distribution)
    {
        for (auto category : TEnumTraits<EWorkloadCategory>::GetDomainValues()) {
            const auto& hist = distribution[category];
            const auto& bins = hist.GetBins();
            const auto& counters = hist.GetCounters();

            for (int index = 0; index < std::ssize(bins); ++index) {
                if (counters[index] == 0) {
                    continue;
                }
                Weights_.push_back(counters[index]);
                TotalByteCount_ += counters[index] * bins[index];
                Requests_.push_back(TRequestInfo{
                    .Workload = category,
                    .Size = bins[index],
                });
            }
        }

        if (Weights_.empty()) {
            Weights_.push_back(1);
            Requests_.push_back(TRequestInfo{
                .Size = defaultSize,
            });
            TotalByteCount_ = defaultSize;
        }

        std::partial_sum(Weights_.begin(), Weights_.end(), Weights_.begin());

        AverageRequestSize_ = TotalByteCount() / TotalIOCount();
    }

    TRequestInfo Next() const
    {
        auto value = RandomNumber<ui64>(Weights_.back());
        auto it = std::upper_bound(Weights_.begin(), Weights_.end(), value);
        auto index = std::distance(Weights_.begin(), it);

        return Requests_[index];
    }

    i64 TotalByteCount() const
    {
        return TotalByteCount_;
    }

    i64 TotalIOCount() const
    {
        return Weights_.back();
    }

    i64 AverageRequestSize() const
    {
        return AverageRequestSize_;
    }

private:
    std::vector<i64> Weights_;
    std::vector<TRequestInfo> Requests_;

    i64 TotalByteCount_ = 0;
    i64 AverageRequestSize_ = 0;
};

using TRequestSamplerPtr = TIntrusivePtr<TRequestSampler>;

////////////////////////////////////////////////////////////////////////////////

class TLoadAdjuster
    : public ILoadAdjuster
{
public:
    TLoadAdjuster(
        TGentleLoaderConfigPtr config,
        IIOEngineWorkloadModelPtr engine,
        NLogging::TLogger logger)
        : Config_(std::move(config))
        , IOEngine_(std::move(engine))
        , Logger(std::move(logger))
    { }

    bool ShouldSkipRead(i64 size) override
    {
        Adjust();
        SyntheticReadBytes_ += size;
        return RandomNumber<double>() < SkipReadProbability_;
    }

    bool ShouldSkipWrite(i64 size) override
    {
        Adjust();
        SyntheticWrittenBytes_ += size;
        return RandomNumber<double>() < SkipWriteProbability_;
    }

    void ResetStatistics() override
    {
        LastTotalReadBytes_ = IOEngine_->GetTotalReadBytes();
        LastTotalWrittenBytes_ = IOEngine_->GetTotalWrittenBytes();
    }

    virtual double GetReadProbability() const override
    {
        return SkipReadProbability_;
    }

    virtual double GetWriteProbability() const override
    {
        return SkipWriteProbability_;
    }

private:
    const TGentleLoaderConfigPtr Config_;
    const IIOEngineWorkloadModelPtr IOEngine_;
    const NLogging::TLogger Logger;

    TInstant LoadAdjustedAt_;
    i64 SyntheticWrittenBytes_ = 0;
    i64 SyntheticReadBytes_ = 0;
    i64 LastTotalReadBytes_ = 0;
    i64 LastTotalWrittenBytes_ = 0;

    double SkipWriteProbability_ = 0;
    double SkipReadProbability_ = 0;

    void Adjust()
    {
        auto now = TInstant::Now();
        if (now - LoadAdjustedAt_ < Config_->LoadAdjustingInterval) {
            return;
        }

        auto totalReadBytes = IOEngine_->GetTotalReadBytes();
        auto totalWrittenBytes = IOEngine_->GetTotalWrittenBytes();

        i64 totalReadDelta = totalReadBytes - LastTotalReadBytes_;
        i64 totalWrittenDelta = totalWrittenBytes - LastTotalWrittenBytes_;

        YT_VERIFY(totalReadDelta >= 0);
        YT_VERIFY(totalWrittenDelta >= 0);

        SkipReadProbability_ = CalculateSkipProbability(totalReadDelta, SyntheticReadBytes_);
        SkipWriteProbability_ = CalculateSkipProbability(totalWrittenDelta, SyntheticWrittenBytes_);

        // These are excessive load that we can't mitigate with the next round.
        i64 readExtraDebt = std::max<i64>(0, totalReadDelta - SyntheticReadBytes_);
        i64 writeExtraDebt = std::max<i64>(0, totalWrittenDelta - SyntheticWrittenBytes_);

        YT_LOG_TRACE("Adjusting for background load "
            "(TotalRead: %v, TotalWritten: %v, SyntheticRead: %v, SyntheticWritten: %v, "
            "ReadSkip: %v, WriteSkip: %v, ReadDebt: %v, WriteDebt: %v)",
            totalReadDelta,
            totalWrittenDelta,
            SyntheticReadBytes_,
            SyntheticWrittenBytes_,
            SkipReadProbability_,
            SkipWriteProbability_,
            readExtraDebt,
            writeExtraDebt);

        SyntheticReadBytes_ = 0;
        SyntheticWrittenBytes_ = 0;
        LastTotalReadBytes_ = totalReadBytes - readExtraDebt;
        LastTotalWrittenBytes_ = totalWrittenBytes - writeExtraDebt;

        YT_VERIFY(LastTotalReadBytes_ >= 0);
        YT_VERIFY(LastTotalWrittenBytes_ >= 0);

        LoadAdjustedAt_ = now;
    }

    static double CalculateSkipProbability(i64 totalBytes, i64 syntheticTargetBytes)
    {
        auto value = static_cast<double>(totalBytes - syntheticTargetBytes) / (syntheticTargetBytes + 1);
        value = std::max(value, 0.0);
        value = std::min(value, 1.0);
        return value;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TGentleLoader
    : public IGentleLoader
{
public:
    TGentleLoader(
        TGentleLoaderConfigPtr config,
        TString locationRoot,
        IIOEngineWorkloadModelPtr engine,
        IRandomFileProviderPtr fileProvider,
        IInvokerPtr invoker,
        NLogging::TLogger logger)
        : Config_(std::move(config))
        , LocationRoot_(std::move(locationRoot))
        , Logger(std::move(logger))
        , IOEngine_(std::move(engine))
        , RandomFileProvider_(std::move(fileProvider))
        , Invoker_(std::move(invoker))
        , ReadToWriteRatio_(GetReadToWriteRatio(Config_, IOEngine_))
        , LoadAdjuster_(CreateLoadAdjuster(Config_, IOEngine_, Logger))
        , Started_(false)
    { }

    virtual void Start(const TRequestSizes& workloadModel) override
    {
        YT_VERIFY(!Started_);
        Started_ = true;
        YT_UNUSED_FUTURE(BIND(&TGentleLoader::Run, MakeStrong(this), std::move(workloadModel))
            .AsyncVia(Invoker_)
            .Run());
    }

    virtual void Stop() override
    {
        YT_VERIFY(Started_);
        Started_ = false;
        YT_LOG_DEBUG("Signaling Gentle Loader to stop");
    }

    DEFINE_SIGNAL_OVERRIDE(void(i64 /*currentWindow*/), Congested);
    DEFINE_SIGNAL_OVERRIDE(void(i64 /*currentWindow*/), ProbesRoundFinished);

private:
    const TGentleLoaderConfigPtr Config_;
    const TString LocationRoot_;
    const NLogging::TLogger Logger;
    const IIOEngineWorkloadModelPtr IOEngine_;
    const IRandomFileProviderPtr RandomFileProvider_;
    const IInvokerPtr Invoker_;

    int ReadToWriteRatio_;
    ILoadAdjusterPtr LoadAdjuster_;
    TRequestSamplerPtr ReadRequestSampler_;
    TRequestSamplerPtr WriteRequestSampler_;
    std::atomic_bool Started_;
    std::vector<TFuture<TDuration>> Results_;
    TInstant YieldTimeCounter_;


    void Run(const TRequestSizes& workloadModel)
    {
        while (Started_) {
            try {
                DoRun(workloadModel);
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(ex, "Gentle Loader run failed");
                TDelayedExecutor::WaitForDuration(Config_->WaitAfterCongested);
            }
        }
    }

    void ResetStatistics()
    {
        LoadAdjuster_->ResetStatistics();
        YieldTimeCounter_ = TInstant::Now();
    }

    void DoRun(const TRequestSizes& workloadModel)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        auto randomWriter = New<TRandomWriter>(
            Config_,
            LocationRoot_,
            IOEngine_,
            Logger);

        auto writerStopGuard = randomWriter->StopGuard();

        auto randomReader = New<TRandomReader>(
            Config_,
            RandomFileProvider_,
            IOEngine_,
            Logger);

        auto congestionDetector = New<TCongestionDetector>(
            Config_->CongestionDetector,
            IOEngine_,
            Invoker_,
            randomReader,
            Logger);

        ImbueWorkloadModel(workloadModel);

        const int maxWindowSize = GetMaxWindowSize();
        const int initialWindow = Config_->InitialWindowSize;

        // State variable that limits the amount of data to send to medium.
        int congestionWindow = initialWindow;

        auto congestionWindowChanged = TInstant::Now();

        // State variable determines whether the slow start or congestion avoidance algorithm
        // is used to control data transmission
        int slowStartThreshold = Config_->InitialSlowStartThreshold
            ? std::min(maxWindowSize, Config_->InitialSlowStartThreshold)
            : maxWindowSize;

        TCongestedState lastState;
        i64 requestCounter = 0;
        ResetStatistics();

        while (Started_) {
            try {
                SendRequest(congestionWindow, *randomReader, *randomWriter);
                ++requestCounter;

                auto state = congestionDetector->GetState();
                if (state.Epoch == lastState.Epoch) {
                    continue;
                }

                randomReader->OpenNewFiles();

                auto prevWindow = congestionWindow;
                auto windowTime = TInstant::Now() - congestionWindowChanged;

                if (congestionWindow == maxWindowSize && windowTime >= Config_->WindowPeriod) {
                    YT_LOG_INFO("Reporting congested state because we have reached maximum window size"
                        " (Index: %v, Status: %v, CongestionWindow: %v, MaxWindowSize: %v)",
                        state.Epoch,
                        state.Status,
                        congestionWindow,
                        maxWindowSize);

                    state.Status = ECongestedStatus::Overload;
                }

                lastState = state;

                switch (state.Status) {
                    case ECongestedStatus::OK:
                        if (windowTime < Config_->WindowPeriod) {
                            // It is too early to increase window.
                            break;
                        }

                        if (congestionWindow >= slowStartThreshold) {
                            // Congestion avoidance algorithm.
                            congestionWindow += Config_->SegmentSize;
                        } else {
                            // Slow start algorithm
                            // (which is actually quite fast).
                            congestionWindow = std::min(congestionWindow * 2, slowStartThreshold);
                        }
                        break;

                    case ECongestedStatus::Overload:
                        slowStartThreshold = congestionWindow / 2;
                        congestionWindow = slowStartThreshold;
                        break;

                    case ECongestedStatus::HeavyOverload:
                        slowStartThreshold = congestionWindow / 2;
                        congestionWindow = initialWindow;
                        break;

                    default:
                        YT_ABORT();
                }

                // Keeping window in sane limits.
                congestionWindow = std::min(maxWindowSize, congestionWindow);
                congestionWindow = std::max(initialWindow, congestionWindow);

                if (prevWindow != congestionWindow) {
                    congestionWindowChanged = TInstant::Now();
                }

                YT_LOG_INFO("New congestion message received"
                    " (Index: %v, Status: %v, CongestionWindow: %v, SlowStartThreshold: %v, RequestCounter: %v)",
                    state.Epoch,
                    state.Status,
                    congestionWindow,
                    slowStartThreshold,
                    requestCounter);

                // Signal probing round finished.
                ProbesRoundFinished_.Fire(prevWindow);

                if (state.Status != ECongestedStatus::OK) {
                    // Signal congested.
                    Congested_.Fire(prevWindow);
                    // If congested wait all inflight events before starting new round.
                    WaitAfterCongested(congestionDetector);
                    lastState = congestionDetector->GetState();
                    ResetStatistics();
                }
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(ex, "Gentle Loader loop failed");
            }
        }
    }

    void WaitAfterCongested(TIntrusivePtr<TCongestionDetector> congestionDetector)
    {
        auto wait = WaitFor(AllSet(Results_));
        Y_UNUSED(wait);

        static const int MaxWaitAttempt = 10;
        for (int attempt = 0; attempt < MaxWaitAttempt; ++attempt) {
            YT_LOG_DEBUG("Waiting after congested (Duration: %v, Attempt: %v)",
                Config_->WaitAfterCongested,
                attempt);

            TDelayedExecutor::WaitForDuration(Config_->WaitAfterCongested);
            auto state = congestionDetector->GetState();
            if (state.Status == ECongestedStatus::OK) {
                break;
            }
        }
    }

    void SendRequest(int congestionWindow, TRandomReader& reader, TRandomWriter& writer)
    {
        if (std::ssize(Results_) >= Config_->MaxInFlightCount) {
            auto _ = WaitFor(AnySet(Results_, {.CancelInputOnShortcut = false}));
            Results_.erase(std::remove_if(
                Results_.begin(),
                Results_.end(),
                [&] (const TFuture<TDuration>& future) {
                    if (future.IsSet() && !future.Get().IsOK()) {
                        YT_LOG_DEBUG(future.Get(), "Request is failed");
                    }
                    return future.IsSet();
                }),
                Results_.end());
        }

        i64 desiredWriteIOPS = congestionWindow * (IOScale - ReadToWriteRatio_) / IOScale;

        TFuture<TDuration> future = ReadToWriteRatio_ > static_cast<int>(RandomNumber<ui32>(IOScale))
            ? SendReadRequest(reader)
            : SendWriteRequest(writer, desiredWriteIOPS);

        if (future) {
            // Slowing down IOEngine executor for testing purposes.
            if (Config_->SimulatedRequestLatency) {
                auto simulatedLatency = Config_->SimulatedRequestLatency;
                future = future.Apply(BIND([simulatedLatency] (TDuration duration) {
                    Sleep(simulatedLatency);
                    return duration + simulatedLatency;
                }));
            }

            Results_.push_back(std::move(future));
        }
        YT_VERIFY(congestionWindow > 0);
        YieldTimeCounter_ += TDuration::Seconds(1) / congestionWindow;
        auto yieldTimeout = std::max(YieldTimeCounter_ - TInstant::Now(), TDuration{});
        TDelayedExecutor::WaitForDuration(yieldTimeout);
    }

    TFuture<TDuration> SendReadRequest(TRandomReader& reader)
    {
        auto sample = ReadRequestSampler_->Next();

        if (LoadAdjuster_->ShouldSkipRead(sample.Size)) {
            // Adjusting for background workload.
            return {};
        }

        return reader.Read(sample.Workload, sample.Size);
    }

    TFuture<TDuration> SendWriteRequest(TRandomWriter& writer, i64 desiredWriteIOPS)
    {
        // Respect max write limit.
        i64 maxWriteIOPS = Config_->MaxWriteRate / WriteRequestSampler_->AverageRequestSize();
        YT_VERIFY(maxWriteIOPS >= 0);
        desiredWriteIOPS = std::max<i64>(desiredWriteIOPS, 1);

        if (RandomNumber<double>() > static_cast<double>(maxWriteIOPS) / desiredWriteIOPS) {
            // Skip because we are exceeding write limit.
            return {};
        }

        auto sample = WriteRequestSampler_->Next();

        // Respect background load.
        if (LoadAdjuster_->ShouldSkipWrite(sample.Size)) {
            // Adjusting for background workload.
            return {};
        }

        return writer.Write(sample.Workload, sample.Size);
    }

    void ImbueWorkloadModel(const TRequestSizes& model) {
        YT_LOG_INFO("Using workload model for io test (Reads: %v, Writes: %v)",
            model.Reads,
            model.Writes);

        ReadRequestSampler_ = New<TRequestSampler>(Config_->PacketSize, model.Reads);
        WriteRequestSampler_ = New<TRequestSampler>(Config_->PacketSize, model.Writes);

        static const i64 DefaultIOCount = 1;

        auto readsCount = ReadRequestSampler_->TotalIOCount();
        auto writesCount = WriteRequestSampler_->TotalIOCount();

        if (readsCount != DefaultIOCount || writesCount != DefaultIOCount) {
            ReadToWriteRatio_ = (readsCount * 100) / (readsCount + writesCount);
        }
    }

    int GetMaxWindowSize() const
    {
        if (!Config_->LimitMaxWindowSizesByMaxWriteRate) {
            return Config_->MaxWindowSize;
        }

        int maxWriteIOPS = Config_->MaxWriteRate / WriteRequestSampler_->AverageRequestSize();
        int maxTotalIops = maxWriteIOPS * IOScale / std::max(IOScale - ReadToWriteRatio_, 1);

        int calculatedMaxWindowSize = std::min(Config_->MaxWindowSize, maxTotalIops);
        calculatedMaxWindowSize = std::max(calculatedMaxWindowSize, 1);

        YT_LOG_DEBUG("Max window size calculated based on MaxWriteRate "
            "(MaxWriteRate: %v, ReadToWriteRatio: %v, ConfigMaxWindowSize: %v, CalculatedMaxWindowSize: %v)",
            Config_->MaxWriteRate,
            ReadToWriteRatio_,
            Config_->MaxWindowSize,
            calculatedMaxWindowSize);

        return calculatedMaxWindowSize;
    }

    // TODO(capone212): Remove after using request sizes histogram everywhere.
     static int GetReadToWriteRatio(
        const TGentleLoaderConfigPtr& config,
        IIOEngineWorkloadModelPtr engine)
    {
        i64 totalBytes = engine->GetTotalReadBytes() + engine->GetTotalWrittenBytes();

        if (config->AdaptiveReadToWriteRatioThreshold > totalBytes) {
            return config->DefaultReadToWriteRatio;
        }

        return engine->GetTotalReadBytes() * 100 / totalBytes;
    }
};

////////////////////////////////////////////////////////////////////////////////

IGentleLoaderPtr CreateGentleLoader(
    TGentleLoaderConfigPtr config,
    TString locationRoot,
    IIOEngineWorkloadModelPtr engine,
    IRandomFileProviderPtr fileProvider,
    IInvokerPtr invoker,
    NLogging::TLogger logger)
{
    return New<TGentleLoader>(
        std::move(config),
        std::move(locationRoot),
        std::move(engine),
        std::move(fileProvider),
        std::move(invoker),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

ILoadAdjusterPtr CreateLoadAdjuster(
    TGentleLoaderConfigPtr config,
    IIOEngineWorkloadModelPtr engine,
    NLogging::TLogger logger)
{
    return New<TLoadAdjuster>(
        std::move(config),
        std::move(engine),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
