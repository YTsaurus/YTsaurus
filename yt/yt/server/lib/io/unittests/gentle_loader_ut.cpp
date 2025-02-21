#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/lib/io/config.h>
#include <yt/yt/server/lib/io/gentle_loader.h>
#include <yt/yt/server/lib/io/io_workload_model.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/nonblocking_queue.h>
#include <yt/yt/core/ytree/convert.h>

#include <util/system/fs.h>
#include <util/system/tempfile.h>

namespace NYT::NIO {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static inline const NLogging::TLogger Logger("GentleLoadTest");

////////////////////////////////////////////////////////////////////////////////

// Borrowed from ads/quality/ltp/libs/util/temp.h
class TTempDirectory
{
public:
    TTempDirectory()
    {
        auto tmpName = MakeTempName();
        NFs::Remove(tmpName);
        Name_ = tmpName;
        Y_ENSURE(NFs::MakeDirectory(Name_));
    }

    ~TTempDirectory()
    {
        NFs::RemoveRecursive(Name_);
    }

    const TString& GetName() const
    {
        return Name_;
    }

private:
    TString Name_;
};

////////////////////////////////////////////////////////////////////////////////

struct TRandomFileProviderMock
    : public IRandomFileProvider
{
    virtual std::optional<TFileInfo> GetRandomExistingFile()
    {
        return TFileInfo{
            .Path = "fake_random_file",
            .DiskSpace = 1_GB,
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TIOEngineMockConfig
{
    int ThreadsCount = 4;
    TDuration OpenLatency;
    TDuration ReadLatency;
    TDuration WriteLatency;

    // Measured in percents.
    ui32 ReadFailingProbability = 0;
    ui32 WriteFailingProbability = 0;
    ui32 OpenFailingProbability = 0;

    TDuration UserRequestsP99;
};

////////////////////////////////////////////////////////////////////////////////

class TIOEngineMock
    : public IIOEngineWorkloadModel
{
public:
    explicit TIOEngineMock(const TIOEngineMockConfig& config)
        : Config_(config)
        , ThreadPool_(CreateThreadPool(Config_.ThreadsCount, "MockWriter"))
        , Invoker_(ThreadPool_->GetInvoker())
    { }

    TFuture<TReadResponse> Read(
        std::vector<TReadRequest> /*requests*/,
        const TWorkloadDescriptor& /*descriptor*/,
        TRefCountedTypeCookie /*tagCookie*/,
        TSessionId /*sessionId*/,
        bool /*useDedicatedAllocations*/) override
    {
        return RunRequest(Config_.ReadLatency, Config_.ReadFailingProbability)
            .Apply(BIND([] {
                return TReadResponse{};
            }));
    }

    TFuture<void> Write(
        TWriteRequest /*request*/,
        const TWorkloadDescriptor& /*descriptor*/,
        TSessionId /*sessionId*/) override
    {
        return RunRequest(Config_.WriteLatency, Config_.WriteFailingProbability);
    }

    TFuture<void> FlushFile(
        TFlushFileRequest /*request*/,
        const TWorkloadDescriptor& /*descriptor*/,
        TSessionId /*sessionId*/) override
    {
        return RunRequest(TDuration{});
    }

    TFuture<void> FlushFileRange(
        TFlushFileRangeRequest /*request*/,
        const TWorkloadDescriptor& /*descriptor*/,
        TSessionId /*sessionId*/) override
    {
        return RunRequest(TDuration{});
    }

    TFuture<void> FlushDirectory(
        TFlushDirectoryRequest /*request*/,
        const TWorkloadDescriptor& /*descriptor*/,
        TSessionId /*sessionId*/) override
    {
        return RunRequest(TDuration{});
    }

    TFuture<TIOEngineHandlePtr> Open(
        TOpenRequest /*request*/,
        const TWorkloadDescriptor& /*descriptor*/,
        TSessionId /*sessionId*/) override
    {
        return RunRequest(Config_.OpenLatency, Config_.OpenFailingProbability)
            .Apply(BIND([] {
                return New<TIOEngineHandle>();
            }));
    }

    TFuture<void> Close(
        TCloseRequest /*request*/,
        const TWorkloadDescriptor& /*descriptor*/,
        TSessionId /*sessionId*/) override
    {
        return RunRequest(TDuration{});
    }

    TFuture<void> Allocate(
        TAllocateRequest /*request*/,
        const TWorkloadDescriptor& /*descriptor*/,
        TSessionId /*sessionId*/) override
    {
        return RunRequest(TDuration{});
    }

    TFuture<void> Lock(
        TLockRequest /*request*/,
        const TWorkloadDescriptor& /*descriptor*/,
        TSessionId /*sessionId*/) override
    {
        return RunRequest(TDuration{});
    }

    TFuture<void> Resize(
        TResizeRequest /*request*/,
        const TWorkloadDescriptor& /*descriptor*/,
        TSessionId /*sessionId*/) override
    {
        return RunRequest(TDuration{});
    }


    bool IsSick() const override
    {
        return false;
    }

    const IInvokerPtr& GetAuxPoolInvoker() override
    {
        YT_UNIMPLEMENTED();
    }

    std::optional<TRequestSizes> GetRequestSizes() override
    {
        YT_UNIMPLEMENTED();
    }

    std::optional<TRequestLatencies> GetRequestLatencies() override
    {
        TRequestLatencies latencies;
        auto& interatives = latencies.Reads[EWorkloadCategory::UserInteractive];
        interatives.RecordValue(Config_.UserRequestsP99.MilliSeconds(), 1000);
        latencies.Duration = TDuration::Seconds(5);
        return latencies;
    }

    i64 GetTotalReadBytes() const override
    {
        return TotalReadCounter_;
    }

    i64 GetTotalWrittenBytes() const override
    {
        return TotalWriteCounter_;
    }

    TFuture<void> RunRequest(TDuration latency, ui32 failingProbability = 0)
    {
        return BIND([=] {
            Sleep(latency);
            if (failingProbability && RandomNumber<ui32>(100) < failingProbability) {
                THROW_ERROR_EXCEPTION("Mock request failed");
            }
        }).AsyncVia(Invoker_).Run().ToUncancelable();
    }

    void Reconfigure(const NYTree::INodePtr& config) override
    {
        Y_UNUSED(config);
    }

    void IncreaseReads(i64 size)
    {
        TotalReadCounter_ += size;
    }

    void IncreaseWrites(i64 size)
    {
        TotalWriteCounter_ += size;
    }

    EDirectIOPolicy UseDirectIOForReads() const override
    {
        return EDirectIOPolicy::Never;
    }

private:
    const TIOEngineMockConfig Config_;
    const IThreadPoolPtr ThreadPool_;
    const IInvokerPtr Invoker_;

    i64 TotalReadCounter_ = 0;
    i64 TotalWriteCounter_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TGentleLoaderTest
    : public ::testing::Test
{
public:
    TGentleLoaderTest()
        : GentleLoaderConfig_(New<TGentleLoaderConfig>())
        , ActionQueue_(New<TActionQueue>("TestActionQueue"))
    {
        GentleLoaderConfig_->CongestionDetector->OverloadThreshold = 70;
        GentleLoaderConfig_->CongestionDetector->HeavyOverloadThreshold = 90;
        GentleLoaderConfig_->CongestionDetector->ProbeDeadline = TDuration::MilliSeconds(30);
        GentleLoaderConfig_->CongestionDetector->ProbesInterval = TDuration::MilliSeconds(30);
        GentleLoaderConfig_->WaitAfterCongested = TDuration::MilliSeconds(100);
        GentleLoaderConfig_->WriterCount = 10;
        GentleLoaderConfig_->ReaderCount = 10;

        IOEngineConfig_ = TIOEngineMockConfig{
            .OpenLatency = TDuration::MilliSeconds(1),
            .ReadLatency = TDuration::MilliSeconds(10),
            .WriteLatency = TDuration::MilliSeconds(10),
        };
    }

protected:
    TTempDirectory LocationRoot_;
    TGentleLoaderConfigPtr GentleLoaderConfig_;
    TIOEngineMockConfig IOEngineConfig_;
    TActionQueuePtr ActionQueue_;

    std::vector<i64> Run(int roundsCount)
    {
        auto ioEngine = New<TIOEngineMock>(IOEngineConfig_);
        auto gentleLoader = CreateGentleLoader(
            GentleLoaderConfig_,
            LocationRoot_.GetName(),
            ioEngine,
            New<TRandomFileProviderMock>(),
            ActionQueue_->GetInvoker(),
            Logger);

        auto queue = std::make_shared<TNonblockingQueue<i64>>();

        gentleLoader->SubscribeCongested(BIND([queue] (i64 window) {
            queue->Enqueue(window);
        }));

        gentleLoader->Start({});

        std::vector<i64> result;

        for (int index = 0; index < roundsCount; ++index) {
            auto roundResult = queue->Dequeue().Get()
                .ValueOrThrow();
            result.push_back(roundResult);
            YT_LOG_INFO("Next congested step (Index: %v, IOPS: %v)",
                index,
                roundResult);
        }

        gentleLoader->Stop();

        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TGentleLoaderTest, TestSimple)
{
    IOEngineConfig_.ThreadsCount = 4;
    IOEngineConfig_.ReadLatency = TDuration::MilliSeconds(10);
    IOEngineConfig_.WriteLatency = TDuration::MilliSeconds(10);

    auto results = Run(5);
    EXPECT_EQ(std::ssize(results), 5);
    // 4 threads with 100 IOPS each should get about 400 IOPS
    auto lastResult = results.back();
    EXPECT_GE(lastResult, 200);
    EXPECT_LE(lastResult, 500);
}

TEST_F(TGentleLoaderTest, TestErrorHandling)
{
    IOEngineConfig_.ThreadsCount = 4;
    IOEngineConfig_.ReadLatency = TDuration::MilliSeconds(10);
    IOEngineConfig_.WriteLatency = TDuration::MilliSeconds(10);
    IOEngineConfig_.OpenFailingProbability = 5;
    IOEngineConfig_.ReadFailingProbability = 5;
    IOEngineConfig_.WriteFailingProbability = 5;

    GentleLoaderConfig_->WriterCount = 20;

    auto results = Run(5);
    EXPECT_EQ(std::ssize(results), 5);
    // Check that we get sane values even in case of errors.
    auto lastResult = results.back();
    EXPECT_GE(lastResult, 200);
    EXPECT_LE(lastResult, 600);
}

TEST_F(TGentleLoaderTest, TestAllFailing)
{
    IOEngineConfig_.ThreadsCount = 4;
    IOEngineConfig_.ReadLatency = TDuration::MilliSeconds(10);
    IOEngineConfig_.WriteLatency = TDuration::MilliSeconds(10);
    IOEngineConfig_.ReadFailingProbability = 100;
    IOEngineConfig_.WriteFailingProbability = 100;

    GentleLoaderConfig_->SegmentSize = 1;

    auto results = Run(5);
    EXPECT_EQ(std::ssize(results), 5);

    // Check that we some small IOPS before overload.
    auto lastResult = results.back();
    EXPECT_LE(lastResult, 10);
}

TEST_F(TGentleLoaderTest, TestInteractiveOverloaded)
{
    // Setup high-rate environment.
    IOEngineConfig_.ThreadsCount = 4;
    IOEngineConfig_.ReadLatency = TDuration::MilliSeconds(1);
    IOEngineConfig_.WriteLatency = TDuration::MilliSeconds(1);

    GentleLoaderConfig_->SegmentSize = 1;

    // User interactive requests already overloaded.
    GentleLoaderConfig_->CongestionDetector->UserRequestOverloadThreshold = TDuration::MilliSeconds(100);
    IOEngineConfig_.UserRequestsP99 = TDuration::Seconds(1);

    auto results = Run(5);
    EXPECT_EQ(std::ssize(results), 5);
    // Check that we have some small IOPS before overload.
    auto lastResult = results.back();
    EXPECT_LE(lastResult, 10);
}

TEST_F(TGentleLoaderTest, TestWriteLimit)
{
    GentleLoaderConfig_->MaxWriteRate = GentleLoaderConfig_->PacketSize * 50;
    GentleLoaderConfig_->DefaultReadToWriteRatio =  50; // only writes

    IOEngineConfig_.ThreadsCount = 4;
    IOEngineConfig_.ReadLatency = TDuration::MilliSeconds(10);
    IOEngineConfig_.WriteLatency = TDuration::MilliSeconds(10);

    auto results = Run(5);
    EXPECT_EQ(std::ssize(results), 5);
    // 4 threads with 100 IOPS each should get about 400 IOPS
    // but because we skip most writes we should get about 650 IOPS.
    auto lastResult = results.back();

    EXPECT_GE(lastResult, 300);
    EXPECT_LE(lastResult, 800);
}

////////////////////////////////////////////////////////////////////////////////

struct TSkipTestStep
{
    i64 Background = 0;
    i64 Synthetic = 0;
    double SkipProbability = 0;
};

void ExecuteWriteSkipProbabilityTest(const std::vector<TSkipTestStep>& steps)
{
    auto config = New<TGentleLoaderConfig>();
    config->LoadAdjustingInterval = TDuration::MicroSeconds(100);
    auto engine = New<TIOEngineMock>(TIOEngineMockConfig{.ThreadsCount = 1});
    auto loadAdjuster = CreateLoadAdjuster(config, engine, Logger);

    for (const auto& step : steps) {
        loadAdjuster->ShouldSkipWrite(step.Synthetic);

        auto writeSkipProb = loadAdjuster->GetWriteProbability();
        auto totalWrites = step.Background + (step.Synthetic - step.Synthetic * writeSkipProb);
        engine->IncreaseWrites(totalWrites);

        EXPECT_NEAR(writeSkipProb, step.SkipProbability, 0.01);

        Sleep(TDuration::MilliSeconds(1));
    }
}

void ExecuteReadSkipProbabilityTest(const std::vector<TSkipTestStep>& steps)
{
    auto config = New<TGentleLoaderConfig>();
    config->LoadAdjustingInterval = TDuration::MicroSeconds(100);
    auto engine = New<TIOEngineMock>(TIOEngineMockConfig{.ThreadsCount = 1});
    auto loadAdjuster = CreateLoadAdjuster(config, engine, Logger);

    for (const auto& step : steps) {
        loadAdjuster->ShouldSkipRead(step.Synthetic);

        auto readSkipProb = loadAdjuster->GetReadProbability();
        auto totalWrites = step.Background + (step.Synthetic - step.Synthetic * readSkipProb);
        engine->IncreaseReads(totalWrites);

        EXPECT_NEAR(readSkipProb, step.SkipProbability, 0.01);

        Sleep(TDuration::MilliSeconds(1));
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TCalculateSkipProbabilityTest, Simple)
{
    std::vector<TSkipTestStep> testSteps = {
        {40, 100, 0},
        {40, 100, 0.4},
        {40, 100, 0.4},
        {50, 100, 0.4},
        {50, 100, 0.5},
        {60, 100, 0.5},
        {60, 100, 0.6},
        {240, 100, 0.6},    // debt 200
        {40, 100, 1},       // debt 140
        {40, 100, 1},       // debt 80
        {40, 100, 1},       // debt 20
        {40, 100, 0.6},     // debt 0
        {40, 100, 0.4},
        {0, 100, 0.4},
        {0, 100, 0},
    };

    ExecuteWriteSkipProbabilityTest(testSteps);
    ExecuteReadSkipProbabilityTest(testSteps);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NIO
