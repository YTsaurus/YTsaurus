#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/utilex/random.h>

#include <yt/yt/server/lib/io/io_engine.h>

#include <util/system/fs.h>
#include <util/system/tempfile.h>

namespace NYT::NIO {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TSharedMutableRef GenerateRandomBlob(i64 size)
{
    auto data = TSharedMutableRef::Allocate(size);
    for (ui32 index = 0; index < data.Size(); ++index) {
        data[index] = ((index + size) * 1103515245L) >> 24;
    }
    return data;
}

void WriteFile(const TString& fileName, TRef data)
{
    TFile file(fileName, WrOnly | CreateAlways);
    file.Pwrite(data.Begin(), data.Size(), 0);
}

////////////////////////////////////////////////////////////////////////////////

class TIOEngineTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        EIOEngineType,
        const char*,
        bool
    >>
{
protected:
    EIOEngineType GetIOEngineType()
    {
        return std::get<0>(GetParam());
    }

    IIOEnginePtr CreateIOEngine()
    {
        auto type = GetIOEngineType();
        auto config = NYTree::ConvertTo<NYTree::INodePtr>(
            NYson::TYsonString(TString(std::get<1>(GetParam()))));
        return NIO::CreateIOEngine(type, config);
    }

    void SetUp() override
    {
        auto supportedTypes = GetSupportedIOEngineTypes();
        auto type = GetIOEngineType();
        if (std::find(supportedTypes.begin(), supportedTypes.end(), type) == supportedTypes.end()) {
            GTEST_SKIP() << Format("Skipping Test: IOEngine %v is not supported.", type);
        }
    }

    bool UseDedicatedAllocations()
    {
        return std::get<2>(GetParam());
    }
};

TEST_P(TIOEngineTest, WriteError)
{
    if (GetIOEngineType() != EIOEngineType::Uring && GetIOEngineType() != EIOEngineType::FairShareUring) {
        GTEST_SKIP();
    }

    auto engine = CreateIOEngine();

    auto fileName = GenerateRandomFileName("IOEngine");
    TTempFile tempFile(fileName);

    auto file = engine->Open({fileName, RdWr | CreateAlways}, {}, {})
        .Get()
        .ValueOrThrow();

    constexpr auto S = 64_KB;
    auto data = GenerateRandomBlob(S);

    auto writeToFail = [&] {
        return engine->Write({
            .Handle= file,
            .Offset = -10,
            .Buffers = {data.Slice(0, 10), data.Slice(20, 30), data, data.Slice(40, 50)},
            .Flush = true,
        },
        {},
        {})
            .Get();
    };

    for (int i = 0; i < 1000; ++i) {
        auto result = writeToFail();
        EXPECT_FALSE(result.IsOK());
    }
}

TEST_P(TIOEngineTest, ReadWrite)
{
    auto engine = CreateIOEngine();

    auto fileName = GenerateRandomFileName("IOEngine");
    TTempFile tempFile(fileName);

    auto file = engine->Open({fileName, RdWr | CreateAlways}, {}, {})
        .Get()
        .ValueOrThrow();

    constexpr auto S = 64_KB;
    auto data = GenerateRandomBlob(S);

    auto write = [&] {
        engine->Write({
            .Handle= file,
            .Offset = 0,
            .Buffers = {data.Slice(1, 1), data.Slice(1, 1), data, data.Slice(1, 1)},
            .Flush = true,
        },
        {},
        {})
            .Get()
            .ThrowOnError();
    };

    auto flush = [&] {
        engine->FlushFile({file, EFlushFileMode::Data}, {}, {})
            .Get()
            .ThrowOnError();
    };

    auto read = [&] (i64 offset, i64 size) {
        auto result = engine->Read({{file, offset, size}}, {}, {}, {}, UseDedicatedAllocations())
            .Get()
            .ValueOrThrow();
        EXPECT_TRUE(result.OutputBuffers.size() == 1);
        EXPECT_TRUE(TRef::AreBitwiseEqual(result.OutputBuffers[0], data.Slice(offset, offset + size)));
    };

    write();
    flush();
    file->Resize(17);
    read(0, 17);
    write();
    read(0, S);
    read(0, S - 7);
    read(7, S - 7);
    read(100, 0);
    read(S, 0);
    EXPECT_THROW({
        read(S, 10);
    }, TErrorException);
    EXPECT_THROW({
        read(S - 10, 20);
    }, TErrorException);
}

TEST_P(TIOEngineTest, ReadAll)
{
    auto engine = CreateIOEngine();

    auto fileName = GenerateRandomFileName("IOEngine");
    TTempFile tempFile(fileName);

    constexpr auto S = 124097;
    auto data = GenerateRandomBlob(S);

    WriteFile(fileName, data);

    auto readData = engine->ReadAll(fileName, {}, {})
        .Get()
        .ValueOrThrow();

    EXPECT_TRUE(TRef::AreBitwiseEqual(readData, data));
}

TEST_P(TIOEngineTest, DirectIO)
{
    if (GetIOEngineType() != EIOEngineType::Uring && GetIOEngineType() != EIOEngineType::FairShareUring) {
        GTEST_SKIP() << "Skipping Test: Unaligned direct IO is only supported by uring engine.";
    }

    auto engine = CreateIOEngine();

    auto fileName = GenerateRandomFileName("IOEngine");
    TTempFile tempFile(fileName);

    constexpr auto S = 64_KB;
    auto data = GenerateRandomBlob(S);

    WriteFile(fileName, data);

    auto file = engine->Open({fileName, RdOnly | DirectAligned}, {}, {})
        .Get()
        .ValueOrThrow();

    auto read = [&] (i64 offset, i64 size) {
        auto result = engine->Read({{file, offset, size}}, {}, {}, {}, UseDedicatedAllocations())
            .Get()
            .ValueOrThrow();
        EXPECT_TRUE(result.OutputBuffers.size() == 1);
        EXPECT_TRUE(TRef::AreBitwiseEqual(result.OutputBuffers[0], data.Slice(offset, offset + size)));
    };

    read(1, S - 2);
    read(0, S);
    read(4_KB, 8_KB);

    read(0, S);
    read(100, 200);
    read(4_KB - 1, 2);
    read(4_KB - 1, 4_KB + 2);
}

TEST_P(TIOEngineTest, DirectIOUnalignedFile)
{
    if (GetIOEngineType() != EIOEngineType::Uring && GetIOEngineType() != EIOEngineType::FairShareUring) {
        GTEST_SKIP() << "Skipping Test: Unaligned direct IO is only supported by uring engine.";
    }

    auto engine = CreateIOEngine();

    auto fileName = GenerateRandomFileName("IOEngine");
    TTempFile tempFile(fileName);

    constexpr auto S = 8_KB + 13;
    auto data = GenerateRandomBlob(S);

    WriteFile(fileName, data);

    auto file = engine->Open({fileName, RdOnly | DirectAligned}, {}, {})
        .Get()
        .ValueOrThrow();

    auto read = [&] (i64 offset, i64 size) {
        auto result = engine->Read({{file, offset, size}}, {}, {}, {}, UseDedicatedAllocations())
            .Get()
            .ValueOrThrow();
        EXPECT_TRUE(result.OutputBuffers.size() == 1);
        EXPECT_TRUE(TRef::AreBitwiseEqual(result.OutputBuffers[0], data.Slice(offset, offset + size)));
    };

    read(1, S - 2);
    read(0, S);
    read(S-4_KB-10, 4_KB);
    read(4_KB, 4_KB);
}

TEST_P(TIOEngineTest, ManyConcurrentDirectIOReads)
{
    if (GetIOEngineType() != EIOEngineType::Uring && GetIOEngineType() != EIOEngineType::FairShareUring) {
        GTEST_SKIP() << "Skipping Test: Unaligned direct IO is only supported by uring engine.";
    }

    auto engine = CreateIOEngine();

    auto fileName = GenerateRandomFileName("IOEngine");
    TTempFile tempFile(fileName);

    constexpr auto S = 4_KB;
    auto data = GenerateRandomBlob(S);

    WriteFile(fileName, data);

    auto file = engine->Open({fileName, RdOnly | DirectAligned}, {}, {})
        .Get()
        .ValueOrThrow();

    std::vector<TFuture<IIOEngine::TReadResponse>> futures;
    constexpr auto N = 100;

    for (int i = 0; i < N; ++i) {
        futures.push_back(engine->Read({{file, 10, 20}}, {}, {}, {}, UseDedicatedAllocations()));
    }

    AllSucceeded(std::move(futures))
        .Get()
        .ThrowOnError();
}

class TTestIORequester
{
public:
    explicit TTestIORequester(IIOEnginePtr engine)
        : Engine_(std::move(engine))
        , ActionQueue_(New<TActionQueue>(("TestIO")))
        , Invoker_(ActionQueue_->GetInvoker())
    { }

    void Stop()
    {
        Stopped_.store(true);
    }

    TFuture<void> Run()
    {
        return BIND(&TTestIORequester::DoRun, this)
            .AsyncVia(Invoker_)
            .Run();
    }

private:
    static constexpr int RequestCount = 100;
    static constexpr int SubRequestCount = 3;

    void DoRun()
    {
        auto fileName = GenerateRandomFileName("IOEngine");
        TTempFile tempFile(fileName);

        constexpr auto FileSize = 4_MB;
        auto data = GenerateRandomBlob(FileSize);

        WriteFile(fileName, data);

        auto file = Engine_->Open({fileName, RdOnly}, {}, {})
            .Get()
            .ValueOrThrow();

        while (!Stopped_) {
            std::vector<TFuture<IIOEngine::TReadResponse>> futures;

            for (int requestIndex = 0; requestIndex < RequestCount; ++requestIndex) {
                std::vector<IIOEngine::TReadRequest> readRequests;
                readRequests.reserve(SubRequestCount);

                for (int subRequestIndex = 0; subRequestIndex < SubRequestCount; ++subRequestIndex) {
                    readRequests.push_back({
                        .Handle = file,
                        .Offset = 8_KBs * subRequestIndex,
                        .Size = 10,
                    });
                }
                futures.push_back(Engine_->Read(readRequests, {}, {}, {}, true));
            }

            WaitFor(AllSucceeded(futures, {.CancelInputOnShortcut = false}))
                .ValueOrThrow();
        }
    }

private:
    const IIOEnginePtr Engine_;
    const TActionQueuePtr ActionQueue_;
    const IInvokerPtr Invoker_;
    std::atomic<int> Stopped_ = false;
};

TEST_P(TIOEngineTest, ChangeDynamicConfig)
{
    if (GetIOEngineType() != EIOEngineType::Uring && GetIOEngineType() != EIOEngineType::FairShareUring) {
        GTEST_SKIP() << "Skipping Test: Test intended for uring engines only.";
    }

    static const TString ConfigTemplate = R"(
        {
            uring_thread_count = %v;
            read_thread_count = %v;
            simulated_max_bytes_per_write = 512;
        }
    )";

    const int FullTestIterationCount = 3;

    for (int fullIndex = 0; fullIndex < FullTestIterationCount; ++fullIndex) {
        auto engine = CreateIOEngine();
        constexpr int TestIterationCount = 100;

        for (int index = 0; index < TestIterationCount; ++index) {
            std::vector<std::unique_ptr<TTestIORequester>> requesters;
            requesters.resize(10);
            std::vector<TFuture<void>> futures;
            for (auto& requester : requesters) {
                requester = std::make_unique<TTestIORequester>(engine);
                futures.push_back(requester->Run()
                    .WithTimeout(TDuration::Seconds(30)));
            }

            Sleep(RandomDuration(TDuration::MilliSeconds(10)));

            auto readThreadCount = RandomNumber<ui32>(7) + 1;
            auto config = Format(ConfigTemplate,
                readThreadCount,
                readThreadCount);

            engine->Reconfigure(NYTree::ConvertTo<NYTree::INodePtr>(
                NYson::TYsonString(config)));

            for (const auto& requester : requesters) {
                requester->Stop();
            }

            for (const auto& future : futures) {
                auto result = WaitFor(future);
                EXPECT_NO_THROW(result.ThrowOnError());
                YT_VERIFY(result.IsOK());
            }
        }
    }

}

TEST_P(TIOEngineTest, DirectIOAligned)
{
    auto engine = CreateIOEngine();

    auto fileName = GenerateRandomFileName("IOEngine");
    TTempFile tempFile(fileName);

    constexpr auto S = 4_MB;
    auto data = GenerateRandomBlob(S);

    WriteFile(fileName, data);

    auto file = engine->Open({fileName, RdOnly | DirectAligned}, {}, {})
        .Get()
        .ValueOrThrow();

    auto read = [&] (std::vector<IIOEngine::TReadRequest> requests) {
        for (auto& request : requests) {
            request.Handle = file;
        }
        auto result = engine->Read(requests, {}, {}, {}, UseDedicatedAllocations())
            .Get()
            .ValueOrThrow();

        EXPECT_TRUE(result.OutputBuffers.size() == requests.size());
        for (int index = 0; index < std::ssize(requests); ++index) {
            const auto& request = requests[index];
            EXPECT_TRUE(TRef::AreBitwiseEqual(
                result.OutputBuffers[index],
                data.Slice(request.Offset, request.Offset + request.Size)));
        }
    };

    read({
        {.Offset=16_KB, .Size=32_KB},
        {.Offset=0, .Size=4_KB},
        {.Offset=4_KB, .Size=32_KB},
        {.Offset=S-4_KB, .Size=4_KB},
    });

    read({
        {.Offset=0, .Size=S},
    });

    read({
        {.Offset=0, .Size=1_MB},
        {.Offset=2_MB, .Size=2_MB},
    });
}

const char DefaultConfig[] =
    "{"
    "}";

const char CustomConfig[] =
    "{"
    "    simulated_max_bytes_per_read = 4096;"
    "    simulated_max_bytes_per_write = 4096;"
    "    large_unaligned_direct_io_read_size = 16384;"
    "    flush_after_write = true;"
    "}";

bool AllocatorBehaviourCollocate(false);
bool AllocatorBehaviourSeparate(true);

INSTANTIATE_TEST_SUITE_P(
    TIOEngineTest,
    TIOEngineTest,
    ::testing::Values(
        std::make_tuple(EIOEngineType::ThreadPool, DefaultConfig, AllocatorBehaviourCollocate),
        std::make_tuple(EIOEngineType::ThreadPool, CustomConfig, AllocatorBehaviourCollocate),
        std::make_tuple(EIOEngineType::ThreadPool, DefaultConfig, AllocatorBehaviourSeparate),

        std::make_tuple(EIOEngineType::FairShareThreadPool, DefaultConfig, AllocatorBehaviourCollocate),
        std::make_tuple(EIOEngineType::FairShareThreadPool, CustomConfig, AllocatorBehaviourCollocate),
        std::make_tuple(EIOEngineType::FairShareThreadPool, DefaultConfig, AllocatorBehaviourSeparate),

        std::make_tuple(EIOEngineType::WeightedFairShareThreadPool, DefaultConfig, AllocatorBehaviourCollocate),
        std::make_tuple(EIOEngineType::WeightedFairShareThreadPool, CustomConfig, AllocatorBehaviourCollocate),
        std::make_tuple(EIOEngineType::WeightedFairShareThreadPool, DefaultConfig, AllocatorBehaviourSeparate),

        std::make_tuple(EIOEngineType::Uring, DefaultConfig, AllocatorBehaviourCollocate),
        std::make_tuple(EIOEngineType::Uring, CustomConfig, AllocatorBehaviourCollocate),
        std::make_tuple(EIOEngineType::Uring, DefaultConfig, AllocatorBehaviourSeparate),

        std::make_tuple(EIOEngineType::FairShareUring, DefaultConfig, AllocatorBehaviourCollocate),
        std::make_tuple(EIOEngineType::FairShareUring, CustomConfig, AllocatorBehaviourCollocate),
        std::make_tuple(EIOEngineType::FairShareUring, DefaultConfig, AllocatorBehaviourSeparate)
    )
);

TEST_P(TIOEngineTest, Lock)
{
    auto engine = CreateIOEngine();

    auto fileName = GenerateRandomFileName("IOEngine");
    TTempFile tempFile(fileName);

    auto firstHandler = engine->Open({fileName, RdWr | CreateAlways}, {}, {})
        .Get()
        .ValueOrThrow();

    engine->Lock({firstHandler, ELockFileMode::Exclusive}, {}, {})
        .Get()
        .ThrowOnError();

    // Open file once more.
    auto secondHandler = engine->Open({fileName, RdOnly}, {}, {})
        .Get()
        .ValueOrThrow();

    // Shared lock attempts should fail.
    EXPECT_THROW({
        engine->Lock({
            .Handle = secondHandler,
            .Mode = ELockFileMode::Shared,
            .Nonblocking = true,
        },
        {},
        {})
            .Get()
            .ThrowOnError();
    }, NYT::TErrorException);

    engine->Lock({firstHandler, ELockFileMode::Unlock}, {}, {})
        .Get()
        .ThrowOnError();

    engine->Lock({
        .Handle = secondHandler,
        .Mode = ELockFileMode::Shared,
        .Nonblocking = true,
    },
    {},
    {})
        .Get()
        .ThrowOnError();

    EXPECT_THROW({
        engine->Lock({
            .Handle = firstHandler,
            .Mode = ELockFileMode::Exclusive,
            .Nonblocking = true,
        },
        {},
        {})
            .Get()
            .ThrowOnError();
    }, NYT::TErrorException);

    engine->Lock({
        .Handle = firstHandler,
        .Mode = ELockFileMode::Shared,
        .Nonblocking = true,
    },
    {},
    {})
        .Get()
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NIO
