#include "io_engine_uring.h"
#include "io_engine_base.h"
#include "io_request_slicer.h"
#include "private.h"
#include "read_request_combiner.h"

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/moody_camel_concurrent_queue.h>

#include <yt/yt/core/threading/thread.h>

#include <yt/yt/core/misc/mpsc_fair_share_queue.h>

#include <library/cpp/yt/containers/intrusive_linked_list.h>

#include <library/cpp/yt/threading/notification_handle.h>

#include <util/generic/size_literals.h>
#include <util/generic/xrange.h>

#ifdef _linux_
    #include <liburing.h>
    #include <sys/uio.h>
#endif

namespace NYT::NIO {

using namespace NConcurrency;
using namespace NThreading;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

static constexpr auto& Logger = IOLogger;

static constexpr int UringEngineNotificationCount = 2;
static constexpr int MaxUringConcurrentRequestsPerThread = 128;
static constexpr int MaxUringThreadCount = 16;

// See SetRequestUserData/GetRequestUserData.
static constexpr int MaxSubrequestCount = 1 << 16;
static constexpr int TypicalSubrequestCount = 64;

// NB: -1 is reserved for LIBURING_UDATA_TIMEOUT.
static constexpr intptr_t StopNotificationUserData = -2;
static constexpr intptr_t RequestNotificationUserData = -3;

static constexpr int RequestNotificationIndex = 1;

static constexpr auto OffloadRequestDelay = TDuration::MicroSeconds(100);

DEFINE_ENUM(EUringRequestType,
    (FlushFile)
    (Read)
    (Write)
);

DEFINE_ENUM(EQueueSubmitResult,
    (Ok)
    (NeedReconfigure)
);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TUringIOEngineConfig)

class TUringIOEngineConfig
    : public TIOEngineConfigBase
{
public:
    int UringThreadCount;

    //! Limits the number of concurrent (outstanding) #IIOEngine requests per a single uring thread.
    int MaxConcurrentRequestsPerThread;

    int DirectIOPageSize;

    bool FlushAfterWrite;

    // Request size in bytes.
    int DesiredRequestSize;
    int MinRequestSize;

    double UserInteractiveRequestWeight;
    double UserRealtimeRequestWeight;
    bool EnableIOUringLogging;

    REGISTER_YSON_STRUCT(TUringIOEngineConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("uring_thread_count", &TThis::UringThreadCount)
            .GreaterThanOrEqual(1)
            .LessThanOrEqual(MaxUringThreadCount)
            .Default(1);
        registrar.Parameter("max_concurrent_requests_per_thread", &TThis::MaxConcurrentRequestsPerThread)
            .GreaterThan(0)
            .LessThanOrEqual(MaxUringConcurrentRequestsPerThread)
            .Default(22);

        registrar.Parameter("direct_io_page_size", &TThis::DirectIOPageSize)
            .GreaterThan(0)
            .Default(DefaultPageSize);

        registrar.Parameter("flush_after_write", &TThis::FlushAfterWrite)
            .Default(false);

        registrar.Parameter("desired_request_size", &TThis::DesiredRequestSize)
            .GreaterThanOrEqual(4_KB)
            .Default(128_KB);
        registrar.Parameter("min_request_size", &TThis::MinRequestSize)
            .GreaterThanOrEqual(512)
            .Default(32_KB);

        registrar.Parameter("user_interactive_weight", &TThis::UserInteractiveRequestWeight)
            .GreaterThanOrEqual(1)
            .Default(100);

        registrar.Parameter("user_realtime_request_weight", &TThis::UserRealtimeRequestWeight)
            .GreaterThanOrEqual(1)
            .Default(200);

        registrar.Parameter("enable_io_uring_logging", &TThis::EnableIOUringLogging)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TUringIOEngineConfig)

////////////////////////////////////////////////////////////////////////////////

class TUring
    : private TNonCopyable
{
public:
    explicit TUring(int queueSize)
    {
        auto result = HandleUringEintr(io_uring_queue_init, queueSize, &Uring_, /*flags*/ 0);
        if (result < 0) {
            THROW_ERROR_EXCEPTION("Failed to initialize uring")
                << TError::FromSystem(-result);
        }

        CheckUringResult(
            io_uring_ring_dontfork(&Uring_),
            TStringBuf("io_uring_ring_dontfork"));
    }

    ~TUring()
    {
        io_uring_queue_exit(&Uring_);
    }

    TError TryRegisterBuffers(TRange<iovec> iovs)
    {
        int result = HandleUringEintr(io_uring_register_buffers, &Uring_, iovs.Begin(), iovs.Size());
        return result == 0 ? TError() : TError::FromSystem(-result);
    }

    io_uring_cqe* WaitCqe()
    {
        io_uring_cqe* cqe;
        CheckUringResult(
            HandleUringEintr(io_uring_wait_cqe, &Uring_, &cqe),
            TStringBuf("io_uring_wait_cqe"));
        return cqe;
    }

    io_uring_cqe* PeekCqe()
    {
        io_uring_cqe* cqe;
        bool result = ValidateUringNonBlockingResult(
            HandleUringEintr(io_uring_peek_cqe, &Uring_, &cqe),
            TStringBuf("io_uring_peek_cqe"));
        return result ? cqe : nullptr;
    }

    int GetSQSpaceLeft()
    {
        return io_uring_sq_space_left(&Uring_);
    }

    io_uring_sqe* TryGetSqe()
    {
        return io_uring_get_sqe(&Uring_);
    }

    void CqeSeen(io_uring_cqe* cqe)
    {
        io_uring_cqe_seen(&Uring_, cqe);
    }

    int Submit()
    {
        int count = 0;
        while (true) {
            int result = HandleUringEintr(io_uring_submit, &Uring_);
            CheckUringResult(result, TStringBuf("io_uring_submit"));
            if (result == 0) {
                break;
            }
            count += result;
        }
        return count;
    }

private:
    io_uring Uring_;

    template <class F,  class... Args>
    static auto HandleUringEintr(F f, Args&&... args) -> decltype(f(args...))
    {
        while (true) {
            auto result = f(std::forward<Args>(args)...);
            if (result != -EINTR) {
                return result;
            }
        }
    }

    static void CheckUringResult(int result, TStringBuf callName)
    {
        YT_LOG_FATAL_IF(result < 0, TError::FromSystem(-result), "Uring %Qv call failed",
            callName);
    }

    static bool ValidateUringNonBlockingResult(int result, TStringBuf callName)
    {
        if (result == -EAGAIN) {
            return false;
        }
        CheckUringResult(result, callName);
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

using TUringIovBuffer = std::array<iovec, MaxIovCountPerRequest>;

struct TUringRequest
    : public TIntrusiveLinkedListNode<TUringRequest>
{
    struct TRequestToNode
    {
        TIntrusiveLinkedListNode<TUringRequest>* operator() (TUringRequest* request) const {
            return request;
        }
    };

    EUringRequestType Type;
    std::optional<TRequestStatsGuard> RequestTimeGuard_;
    TRequestCounterGuard RequestCounterGuard;

    virtual ~TUringRequest();

    void StartTimeTracker(const TIOEngineSensors::TRequestSensors& sensors)
    {
        RequestTimeGuard_.emplace(sensors);
    }

    void StopTimeTracker()
    {
        RequestTimeGuard_.reset();
    }

    virtual void SetPromise() = 0;
};

TUringRequest::~TUringRequest() = default;

using TUringRequestPtr = std::unique_ptr<TUringRequest>;

template <typename TResponse>
struct TUringRequestBase
    : public TUringRequest
{
    const TPromise<TResponse> Promise = NewPromise<TResponse>();

    virtual ~TUringRequestBase();

    std::optional<TError> OptionalError;

    void SetPromise() override
    {
        RequestCounterGuard.Release();

        if (!OptionalError.has_value()) {
            return;
        }

        if (OptionalError->IsOK()) {
            Promise.TrySet();
        } else {
            Promise.TrySet(std::move(*OptionalError));
        }
    }

    void TrySetSucceeded()
    {
        if (!OptionalError.has_value()) {
            OptionalError = TError{ };
        }
    }

    void TrySetFailed(TError error)
    {
        OptionalError = std::move(error);
    }

    void TrySetFailed(const io_uring_cqe* cqe)
    {
        YT_VERIFY(cqe->res < 0);
        TrySetFailed(TError::FromSystem(-cqe->res));
    }

    void TrySetFinished(const io_uring_cqe* cqe)
    {
        if (cqe->res >= 0) {
            TrySetSucceeded();
        } else {
            TrySetFailed(cqe);
        }
    }
};

template <typename TResponse>
TUringRequestBase<TResponse>::~TUringRequestBase()
{ }

struct TFlushFileUringRequest
    : public TUringRequestBase<void>
{
    IIOEngine::TFlushFileRequest FlushFileRequest;
};

struct TAllocateUringRequest
    : public TUringRequestBase<void>
{
    IIOEngine::TAllocateRequest AllocateRequest;
};

struct TWriteUringRequest
    : public TUringRequestBase<void>
{
    IIOEngine::TWriteRequest WriteRequest;
    int CurrentWriteSubrequestIndex = 0;
    TUringIovBuffer* WriteIovBuffer = nullptr;
    i64 WrittenBytes = 0;

    int FinishedSubrequestCount = 0;
    bool SyncedFileRange = false;
};

struct TReadUringRequest
    : public TUringRequestBase<void>
{
    struct TReadSubrequestState
    {
        iovec Iov;
        TSharedMutableRef Buffer;
    };

    std::vector<IIOEngine::TReadRequest> ReadSubrequests;
    TCompactVector<TReadSubrequestState, TypicalSubrequestCount> ReadSubrequestStates;
    TCompactVector<int, TypicalSubrequestCount> PendingReadSubrequestIndexes;
    const IReadRequestCombinerPtr ReadRequestCombiner;

    i64 PaddedBytes = 0;
    int FinishedSubrequestCount = 0;

    explicit TReadUringRequest(IReadRequestCombinerPtr combiner)
        : ReadRequestCombiner(std::move(combiner))
    { }
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TUringConfigProvider)

#define YT_DECLARE_ATOMIC_FIELD(type, name) \
    std::atomic<type> name##_;\
    type Get##name(std::memory_order order = std::memory_order::relaxed) { \
        return name##_.load(order); \
    }

#define YT_STORE_ATOMIC_FIELD(config, name) \
    name##_.store(config->name);

struct TUringConfigProvider final
{
    const std::optional<i64> SimulatedMaxBytesPerRead;
    const std::optional<i64> SimulatedMaxBytesPerWrite;
    const int MaxBytesPerRead;
    const int MaxBytesPerWrite;
    const bool EnableIOUringLogging;

    YT_DECLARE_ATOMIC_FIELD(int, UringThreadCount)
    YT_DECLARE_ATOMIC_FIELD(int, MaxConcurrentRequestsPerThread)
    YT_DECLARE_ATOMIC_FIELD(int, DirectIOPageSize)
    YT_DECLARE_ATOMIC_FIELD(bool, FlushAfterWrite)
    YT_DECLARE_ATOMIC_FIELD(int, DesiredRequestSize)
    YT_DECLARE_ATOMIC_FIELD(int, MinRequestSize)
    YT_DECLARE_ATOMIC_FIELD(double, UserInteractiveRequestWeight)
    YT_DECLARE_ATOMIC_FIELD(double, UserRealtimeRequestWeight)

    explicit TUringConfigProvider(const TUringIOEngineConfigPtr& config)
        : SimulatedMaxBytesPerRead(config->SimulatedMaxBytesPerRead)
        , SimulatedMaxBytesPerWrite(config->SimulatedMaxBytesPerWrite)
        , MaxBytesPerRead(config->MaxBytesPerRead)
        , MaxBytesPerWrite(config->MaxBytesPerWrite)
        , EnableIOUringLogging(config->EnableIOUringLogging)
    {
        Update(config);
    }

    void Update(const TUringIOEngineConfigPtr& newConfig)
    {
        YT_STORE_ATOMIC_FIELD(newConfig, UringThreadCount)
        YT_STORE_ATOMIC_FIELD(newConfig, MaxConcurrentRequestsPerThread)
        YT_STORE_ATOMIC_FIELD(newConfig, DirectIOPageSize)
        YT_STORE_ATOMIC_FIELD(newConfig, FlushAfterWrite)
        YT_STORE_ATOMIC_FIELD(newConfig, DesiredRequestSize)
        YT_STORE_ATOMIC_FIELD(newConfig, MinRequestSize)
        YT_STORE_ATOMIC_FIELD(newConfig, UserInteractiveRequestWeight)
        YT_STORE_ATOMIC_FIELD(newConfig, UserRealtimeRequestWeight)
    }
};

DEFINE_REFCOUNTED_TYPE(TUringConfigProvider)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IUringThreadPool)

struct IUringThreadPool
    : public TRefCounted
{
    virtual const TString& Name() const = 0;

    virtual void PrepareDequeue(int threadIndex) = 0;

    virtual TUringRequestPtr TryDequeue(int threadIndex) = 0;

    virtual void MarkFinished(int threadIndex, TUringRequestPtr request) = 0;

    virtual const TNotificationHandle& GetNotificationHandle(int threadIndex) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IUringThreadPool)

////////////////////////////////////////////////////////////////////////////////

class TUringThread
    : public NThreading::TThread
{
public:
    TUringThread(IUringThreadPool* threadPool, TUringConfigProviderPtr config, int index, TIOEngineSensorsPtr sensors)
        : TThread(Format("%v:%v", threadPool->Name(), index))
        , ThreadPool_(threadPool)
        , Config_(std::move(config))
        , ThreadIndex_(index)
        , EnableIOUringLogging_(Config_->EnableIOUringLogging)
        , Uring_(MaxUringConcurrentRequestsPerThread + UringEngineNotificationCount)
        , AllIovBuffers_(MaxUringConcurrentRequestsPerThread + UringEngineNotificationCount)
        , Sensors_(std::move(sensors))
    {
        InitIovBuffers();
        Start();
    }

private:
    IUringThreadPool* const ThreadPool_;
    const TUringConfigProviderPtr Config_;
    const int ThreadIndex_;
    const bool EnableIOUringLogging_;

    TUring Uring_;

    // Linked list of requests that have subrequests yet to be started.
    TIntrusiveLinkedList<TUringRequest, TUringRequest::TRequestToNode> UndersubmittedRequests_;

    int PendingSubmissionsCount_ = 0;
    bool RequestNotificationReadArmed_ = false;

    TNotificationHandle StopNotificationHandle_{true};
    bool Stopping_ = false;

    std::vector<TUringIovBuffer> AllIovBuffers_;
    std::vector<TUringIovBuffer*> FreeIovBuffers_;

    static constexpr int StopNotificationIndex = 0;

    std::array<ui64, UringEngineNotificationCount> NotificationReadBuffer_;
    std::array<iovec, UringEngineNotificationCount> NotificationIov_;

    const TIOEngineSensorsPtr Sensors_;


    void InitIovBuffers()
    {
        FreeIovBuffers_.reserve(AllIovBuffers_.size());
        for (auto& buffer : AllIovBuffers_) {
            FreeIovBuffers_.push_back(&buffer);
        }
    }

    void StopPrologue() override
    {
        StopNotificationHandle_.Raise();
    }

    void ThreadMain() override
    {
        YT_LOG_INFO("Uring thread started");

        ArmStopNotificationRead();
        ArmRequestNotificationRead();
        SubmitSqes();

        do {
            ThreadMainStep();
        } while (!IsUringDrained());

        YT_LOG_INFO("Uring thread stopped");
    }

    bool IsUringDrained()
    {
        if (Stopping_) {
            YT_LOG_DEBUG("Uring thread stopping (PendingSubmissionsCount_: %v, UndersubmittedRequestsSize: %v)",
                PendingSubmissionsCount_,
                UndersubmittedRequests_.GetSize());
        }
        return Stopping_ && PendingSubmissionsCount_ == 0;
    }

    void ThreadMainStep()
    {
        YT_VERIFY(!CanHandleMoreSubmissions() || RequestNotificationReadArmed_ || Stopping_);

        io_uring_cqe* cqe = GetCqe(true);
        while (cqe) {
            auto* userData = io_uring_cqe_get_data(cqe);
            if (userData == reinterpret_cast<void*>(StopNotificationUserData)) {
                YT_VERIFY(cqe->res == sizeof(ui64));
                HandleStop();
            } else if (userData == reinterpret_cast<void*>(RequestNotificationUserData)) {
                YT_VERIFY(cqe->res == sizeof(ui64));
                YT_VERIFY(RequestNotificationReadArmed_);
                RequestNotificationReadArmed_ = false;
            } else {
                HandleCompletion(&*cqe);
            }

            cqe = GetCqe(false);
        }

        while (UndersubmittedRequests_.GetSize() > 0 && CanHandleMoreSubmissions()) {
            YT_LOG_DEBUG_IF(EnableIOUringLogging_, "Submitting extra request from undersubmitted list.");
            auto* request = UndersubmittedRequests_.GetFront();
            HandleRequest(request);
        }

        HandleSubmissions();
        SubmitSqes();
    }

    TUringRequestPtr TryDequeue()
    {
        if (Stopping_) {
            return nullptr;
        }

        auto request = ThreadPool_->TryDequeue(ThreadIndex_);
        if (!request) {
            return nullptr;
        }

        YT_LOG_DEBUG_IF(EnableIOUringLogging_, "Request dequeued (Request: %v)",
            request.get());
        return request;
    }

    void HandleStop()
    {
        YT_LOG_INFO("Stop received by uring thread (PendingRequestCount: %v)",
            PendingSubmissionsCount_);

        YT_VERIFY(!Stopping_);
        Stopping_ = true;
    }

    bool CanHandleMoreSubmissions()
    {
        bool result = PendingSubmissionsCount_ < Config_->GetMaxConcurrentRequestsPerThread();

        YT_VERIFY(!result || Uring_.GetSQSpaceLeft() > 0);

        return result;
    }

    void HandleSubmissions()
    {
        ThreadPool_->PrepareDequeue(ThreadIndex_);

        while (true) {
            if (!CanHandleMoreSubmissions()) {
                YT_LOG_DEBUG_IF(EnableIOUringLogging_, "Cannot handle more submissions");
                break;
            }

            auto request = TryDequeue();
            if (!request) {
                break;
            }

            HandleRequest(request.release());
        }

        ArmRequestNotificationRead();
    }

    void HandleRequest(TUringRequest* request)
    {
        switch (request->Type) {
            case EUringRequestType::Read:
                HandleReadRequest(static_cast<TReadUringRequest*>(request));
                break;
            case EUringRequestType::Write:
                HandleWriteRequest(static_cast<TWriteUringRequest*>(request));
                break;
            case EUringRequestType::FlushFile:
                HandleFlushFileRequest(static_cast<TFlushFileUringRequest*>(request));
                break;

            default:
                YT_ABORT();
        }
    }

    void HandleReadRequest(TReadUringRequest* request)
    {
        auto totalSubrequestCount = std::ssize(request->ReadSubrequests);

        YT_LOG_DEBUG_IF(EnableIOUringLogging_, "Handling read request (Request: %p, FinishedSubrequestCount: %v, TotalSubrequestCount: %v)",
            request,
            request->FinishedSubrequestCount,
            totalSubrequestCount);

        if (request->Prev || UndersubmittedRequests_.GetFront() == request) {
            UndersubmittedRequests_.Remove(request);
        }

        if (request->FinishedSubrequestCount == totalSubrequestCount) {
            request->TrySetSucceeded();

            DisposeRequest(request);
            return;
        } else if (request->PendingReadSubrequestIndexes.empty()) {
            return;
        }

        YT_VERIFY(CanHandleMoreSubmissions());
        while (!request->PendingReadSubrequestIndexes.empty() && CanHandleMoreSubmissions())
        {
            auto subrequestIndex = request->PendingReadSubrequestIndexes.back();
            request->PendingReadSubrequestIndexes.pop_back();

            const auto& subrequest = request->ReadSubrequests[subrequestIndex];
            auto& subrequestState = request->ReadSubrequestStates[subrequestIndex];
            auto& buffer = subrequestState.Buffer;

            auto* sqe = AllocateSqe();
            subrequestState.Iov = {
                .iov_base = buffer.Begin(),
                .iov_len = Min<size_t>(buffer.Size(), Config_->MaxBytesPerRead)
            };

            YT_LOG_DEBUG_IF(EnableIOUringLogging_, "Submitting read operation (Request: %p/%v, FD: %v, Offset: %v, Buffer: %p@%v)",
                request,
                subrequestIndex,
                static_cast<FHANDLE>(*subrequest.Handle),
                subrequest.Offset,
                subrequestState.Iov.iov_base,
                subrequestState.Iov.iov_len);

            io_uring_prep_readv(
                sqe,
                *subrequest.Handle,
                &subrequestState.Iov,
                1,
                subrequest.Offset);

            SetRequestUserData(sqe, request, Sensors_->ReadSensors, subrequestIndex);
        }

        if (!request->PendingReadSubrequestIndexes.empty()) {
            UndersubmittedRequests_.PushBack(request);
        }
    }

    bool SyncFileRangeAfterWrite(TWriteUringRequest* request)
    {
        if (!Config_->GetFlushAfterWrite() || !request->WriteRequest.Flush || request->SyncedFileRange) {
            return false;
        }

        request->SyncedFileRange = true;
        auto* sqe = AllocateSqe();
        auto flags = SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_AFTER;
        auto offset = request->WriteRequest.Offset - request->WrittenBytes;
        YT_VERIFY(offset >= 0);

        io_uring_prep_sync_file_range(
            sqe,
            *request->WriteRequest.Handle,
            request->WrittenBytes,
            offset,
            flags);

        SetRequestUserData(sqe, request, Sensors_->DataSyncSensors);
        return true;
    }

    void HandleWriteRequest(TWriteUringRequest* request)
    {
        auto totalSubrequestCount = std::ssize(request->WriteRequest.Buffers);

        YT_LOG_DEBUG_IF(EnableIOUringLogging_, "Handling write request (Request: %p, FinishedSubrequestCount: %v, TotalSubrequestCount: %v)",
            request,
            request->FinishedSubrequestCount,
            totalSubrequestCount);

        if (request->CurrentWriteSubrequestIndex == totalSubrequestCount) {
            ReleaseIovBuffer(request);

            if (SyncFileRangeAfterWrite(request)) {
                // Sent command to flush written data to disk.
                return;
            }

            request->TrySetSucceeded();
            DisposeRequest(request);
            return;
        }

        YT_VERIFY(request->CurrentWriteSubrequestIndex < totalSubrequestCount);

        if (!request->WriteIovBuffer) {
            request->WriteIovBuffer = AllocateIovBuffer();
        }

        int iovCount = 0;
        i64 toWrite = 0;
        while (request->CurrentWriteSubrequestIndex + iovCount < totalSubrequestCount &&
            iovCount < std::ssize(*request->WriteIovBuffer) &&
            toWrite < Config_->MaxBytesPerWrite)
        {
            const auto& buffer = request->WriteRequest.Buffers[request->CurrentWriteSubrequestIndex + iovCount];
            auto& iov = (*request->WriteIovBuffer)[iovCount];
            iov = {
                .iov_base = const_cast<char*>(buffer.Begin()),
                .iov_len = buffer.Size()
            };
            if (toWrite + static_cast<i64>(iov.iov_len) > Config_->MaxBytesPerWrite) {
                iov.iov_len = Config_->MaxBytesPerWrite - toWrite;
            }
            toWrite += iov.iov_len;
            ++iovCount;
        }

        YT_LOG_DEBUG_IF(EnableIOUringLogging_, "Submitting write operation (Request: %p, FD: %v, Offset: %v, Buffers: %v)",
            request,
            static_cast<FHANDLE>(*request->WriteRequest.Handle),
            request->WriteRequest.Offset,
            MakeFormattableView(
                xrange(request->WriteIovBuffer->begin(), request->WriteIovBuffer->begin() + iovCount),
                [] (auto* builder, const auto* iov) {
                    builder->AppendFormat("%p@%v",
                        iov->iov_base,
                        iov->iov_len);
                }));

        auto* sqe = AllocateSqe();
        io_uring_prep_writev(
            sqe,
            *request->WriteRequest.Handle,
            &request->WriteIovBuffer->front(),
            iovCount,
            request->WriteRequest.Offset);

        SetRequestUserData(sqe, request, Sensors_->WriteSensors);
    }

    static ui32 GetSyncFlags(EFlushFileMode mode)
    {
        switch (mode) {
            case EFlushFileMode::All:
                return 0;
            case EFlushFileMode::Data:
                return IORING_FSYNC_DATASYNC;
            default:
                YT_ABORT();
        }
    }

    void HandleFlushFileRequest(TFlushFileUringRequest* request)
    {
        YT_LOG_DEBUG_IF(EnableIOUringLogging_, "Handling flush file request (Request: %p)",
            request);

        YT_LOG_DEBUG_IF(EnableIOUringLogging_, "Submitting flush file request (Request: %p, FD: %v, Mode: %v)",
            request,
            static_cast<FHANDLE>(*request->FlushFileRequest.Handle),
            request->FlushFileRequest.Mode);

        auto* sqe = AllocateSqe();
        io_uring_prep_fsync(sqe, *request->FlushFileRequest.Handle, GetSyncFlags(request->FlushFileRequest.Mode));
        SetRequestUserData(sqe, request, Sensors_->SyncSensors);
    }

    void HandleCompletion(const io_uring_cqe* cqe)
    {
        auto [request, _] = GetRequestUserData<TUringRequest>(cqe);
        request->StopTimeTracker();
        switch (request->Type) {
            case EUringRequestType::Read:
                HandleReadCompletion(cqe);
                break;
            case EUringRequestType::Write:
                HandleWriteCompletion(cqe);
                break;
            case EUringRequestType::FlushFile:
                HandleFlushFileCompletion(cqe);
                break;

            default:
                YT_ABORT();
        }
    }

    void HandleReadCompletion(const io_uring_cqe* cqe)
    {
        auto [request, subrequestIndex] = GetRequestUserData<TReadUringRequest>(cqe);

        YT_LOG_DEBUG_IF(EnableIOUringLogging_, "Handling read completion (Request: %p/%v)",
            request,
            subrequestIndex);

        auto& subrequest = request->ReadSubrequests[subrequestIndex];
        auto& subrequestState = request->ReadSubrequestStates[subrequestIndex];

        if (cqe->res == 0 && subrequestState.Buffer.Size() > 0) {
            auto error = request->ReadRequestCombiner->CheckEof(subrequestState.Buffer);
            if (!error.IsOK()) {
                YT_LOG_DEBUG_IF(EnableIOUringLogging_, "Read subrequest failed at EOF (Request: %p/%v, Remaining: %v)",
                    request,
                    subrequestIndex,
                    subrequestState.Buffer.Size());
                request->TrySetFailed(std::move(error));
            } else {
                YT_LOG_DEBUG_IF(EnableIOUringLogging_, "Read subrequest succeeded at EOF (Request: %p/%v, Remaining: %v)",
                    request,
                    subrequestIndex,
                    subrequestState.Buffer.Size());
            }
            subrequestState.Buffer = { };
            ++request->FinishedSubrequestCount;
        } else if (cqe->res < 0) {
            ++request->FinishedSubrequestCount;
            request->TrySetFailed(cqe);
        } else {
            i64 readSize = cqe->res;
            if (Config_->SimulatedMaxBytesPerRead) {
                readSize = Min(readSize, *Config_->SimulatedMaxBytesPerRead);
            }
            Sensors_->RegisterReadBytes(readSize);
            auto bufferSize = static_cast<i64>(subrequestState.Buffer.Size());
            subrequest.Offset += readSize;
            if (bufferSize == readSize) {
                YT_LOG_DEBUG_IF(EnableIOUringLogging_, "Read subrequest fully succeeded (Request: %p/%v, Size: %v)",
                    request,
                    subrequestIndex,
                    readSize);
                subrequestState.Buffer = { };
                ++request->FinishedSubrequestCount;
            } else {
                YT_LOG_DEBUG_IF(EnableIOUringLogging_, "Read subrequest partially succeeded (Request: %p/%v, Size: %v)",
                    request,
                    subrequestIndex,
                    readSize);
                subrequestState.Buffer = subrequestState.Buffer.Slice(readSize, bufferSize);
                request->PendingReadSubrequestIndexes.push_back(subrequestIndex);
            }
        }

        HandleReadRequest(request);
    }

    void HandleWriteCompletion(const io_uring_cqe* cqe)
    {
        auto [request, _] = GetRequestUserData<TWriteUringRequest>(cqe);

        YT_LOG_DEBUG_IF(EnableIOUringLogging_, "Handling write completion (Request: %p)",
            request);

        if (cqe->res < 0) {
            request->TrySetFailed(cqe);
            ReleaseIovBuffer(request);
            DisposeRequest(request);
            return;
        }

        if (request->CurrentWriteSubrequestIndex == std::ssize(request->WriteRequest.Buffers)) {
            return HandleWriteRequest(request);
        }

        i64 writtenSize = cqe->res;
        if (Config_->SimulatedMaxBytesPerWrite) {
            writtenSize = Min(writtenSize, *Config_->SimulatedMaxBytesPerWrite);
        }
        Sensors_->RegisterWrittenBytes(writtenSize);
        request->WriteRequest.Offset += writtenSize;
        request->WrittenBytes += writtenSize;

        do {
            auto& buffer = request->WriteRequest.Buffers[request->CurrentWriteSubrequestIndex];
            auto bufferSize = static_cast<i64>(buffer.Size());
            if (bufferSize <= writtenSize) {
                YT_LOG_DEBUG_IF(EnableIOUringLogging_, "Write subrequest fully succeeded (Request: %p/%v, Size: %v)",
                    request,
                    request->CurrentWriteSubrequestIndex,
                    writtenSize);
                writtenSize -= bufferSize;
                buffer = { };
                ++request->CurrentWriteSubrequestIndex;
            } else {
                YT_LOG_DEBUG_IF(EnableIOUringLogging_, "Write subrequest partially succeeded (Request: %p/%v, Size: %v)",
                    request,
                    request->CurrentWriteSubrequestIndex,
                    writtenSize);
                buffer = buffer.Slice(writtenSize, bufferSize);
                writtenSize = 0;
            }
        } while (writtenSize > 0);

        HandleWriteRequest(request);
    }

    void HandleFlushFileCompletion(const io_uring_cqe* cqe)
    {
        auto [request, _] = GetRequestUserData<TFlushFileUringRequest>(cqe);

        YT_LOG_DEBUG_IF(EnableIOUringLogging_, "Handling sync completion (Request: %p)",
            request);

        request->TrySetFinished(cqe);
        DisposeRequest(request);
    }

    void HandleAllocateCompletion(const io_uring_cqe* cqe)
    {
        auto [request, _] = GetRequestUserData<TAllocateUringRequest>(cqe);

        YT_LOG_DEBUG_IF(EnableIOUringLogging_, "Handling allocate completion (Request: %p)",
            request);

        request->TrySetFinished(cqe);
        DisposeRequest(request);
    }

    void ArmNotificationRead(
        int notificationIndex,
        const TNotificationHandle& notificationHandle,
        intptr_t notificationUserData)
    {
        YT_VERIFY(notificationIndex >= 0);
        YT_VERIFY(notificationIndex < std::ssize(NotificationIov_));
        YT_VERIFY(notificationIndex < std::ssize(NotificationReadBuffer_));

        auto iov = &NotificationIov_[notificationIndex];
        *iov = {
            .iov_base = &NotificationReadBuffer_[notificationIndex],
            .iov_len = sizeof(NotificationReadBuffer_[notificationIndex])
        };

        auto* sqe = AllocateNonRequestSqe();
        io_uring_prep_readv(sqe, notificationHandle.GetFD(), iov, 1, 0);
        io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(notificationUserData));
    }

    void ArmStopNotificationRead()
    {
        YT_LOG_DEBUG_IF(EnableIOUringLogging_, "Arming stop notification read");
        ArmNotificationRead(StopNotificationIndex, StopNotificationHandle_, StopNotificationUserData);
    }

    void ArmRequestNotificationRead()
    {
        if (!RequestNotificationReadArmed_ &&
            !Stopping_ &&
            CanHandleMoreSubmissions())
        {
            RequestNotificationReadArmed_ = true;
            auto& handle = ThreadPool_->GetNotificationHandle(ThreadIndex_);
            YT_LOG_DEBUG_IF(EnableIOUringLogging_, "Arming request notification read (Handle: %v)",
                handle.GetFD());

            ArmNotificationRead(
                RequestNotificationIndex,
                handle,
                RequestNotificationUserData);
        }
    }

    TUringIovBuffer* AllocateIovBuffer()
    {
        YT_VERIFY(!FreeIovBuffers_.empty());
        auto* result = FreeIovBuffers_.back();
        FreeIovBuffers_.pop_back();
        return result;
    }

    void ReleaseIovBuffer(TWriteUringRequest* request)
    {
        if (request->WriteIovBuffer) {
            FreeIovBuffers_.push_back(std::exchange(request->WriteIovBuffer, nullptr));
        }
    }

    io_uring_cqe* GetCqe(bool wait)
    {
        io_uring_cqe* cqe = wait ? Uring_.WaitCqe() : Uring_.PeekCqe();
        if (cqe == nullptr) {
            YT_VERIFY(!wait);
            return nullptr;
        }
        auto userData = reinterpret_cast<intptr_t>(io_uring_cqe_get_data(cqe));
        if (userData != StopNotificationUserData && userData != RequestNotificationUserData) {
            --PendingSubmissionsCount_;
        }
        YT_VERIFY(PendingSubmissionsCount_ >= 0);
        YT_LOG_DEBUG_IF(EnableIOUringLogging_, "CQE received (PendingRequestCount: %v)",
            PendingSubmissionsCount_);

        Uring_.CqeSeen(cqe);
        return cqe;
    }

    io_uring_sqe* AllocateNonRequestSqe()
    {
        auto* sqe = Uring_.TryGetSqe();
        YT_VERIFY(sqe);
        return sqe;
    }

    io_uring_sqe* AllocateSqe()
    {
        auto* sqe = Uring_.TryGetSqe();
        YT_VERIFY(sqe);
        PendingSubmissionsCount_++;
        return sqe;
    }

    void SubmitSqes()
    {
        int count = 0;
        {
            TRequestStatsGuard statsGuard(Sensors_->IOSubmitSensors);
            count = Uring_.Submit();
        }
        if (count > 0) {
            YT_LOG_DEBUG_IF(EnableIOUringLogging_, "SQEs submitted (SqeCount: %v, PendingRequestCount: %v)",
                count,
                PendingSubmissionsCount_);
        }
    }

    static void SetRequestUserData(
        io_uring_sqe* sqe,
        TUringRequest* request,
        const TIOEngineSensors::TRequestSensors& sensors,
        int subrequestIndex = 0)
    {
        request->StartTimeTracker(sensors);

        auto userData = reinterpret_cast<void*>(
            reinterpret_cast<uintptr_t>(request) |
            (static_cast<uintptr_t>(subrequestIndex) << 48));
        io_uring_sqe_set_data(sqe, userData);
    }

    template <typename TUringRequest>
    static std::tuple<TUringRequest*, int> GetRequestUserData(const io_uring_cqe* cqe)
    {
        constexpr ui64 requestMask = (1ULL << 48) - 1;
        auto userData = reinterpret_cast<uintptr_t>(io_uring_cqe_get_data(cqe));
        return {
            reinterpret_cast<TUringRequest*>(userData & requestMask),
            userData >> 48
        };
    }

    void DisposeRequest(TUringRequest* request)
    {
        YT_LOG_DEBUG_IF(EnableIOUringLogging_, "Request disposed (Request: %v)",
            request);

        ThreadPool_->MarkFinished(ThreadIndex_, TUringRequestPtr(request));
    }
};

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////

class TMoodyCamelQueue
    : public TRefCounted
{
public:
    TMoodyCamelQueue(TString /*locationId*/, TUringConfigProviderPtr config)
        : EnableIOUringLogging_(config->EnableIOUringLogging)
    { }

    EQueueSubmitResult Enqueue(
        TUringRequestPtr request,
        EWorkloadCategory /*category*/,
        IIOEngine::TSessionId /*sessionId*/)
    {
        Queue_.enqueue(std::move(request));

        if (!RequestNotificationHandleRaised_.exchange(true)) {
            YT_LOG_DEBUG_IF(EnableIOUringLogging_, "Request notification raise (Handle: %v, Reason: Enqueue)",
                RequestNotificationHandle_.GetFD());

            RequestNotificationHandle_.Raise();
        }

        return EQueueSubmitResult::Ok;
    }

    EQueueSubmitResult Enqueue(
        std::vector<TUringRequestPtr>& requests,
        EWorkloadCategory /*category*/,
        IIOEngine::TSessionId /*sessionId*/)
    {
        for (auto& request : requests) {
            Queue_.enqueue(std::move(request));
        }

        if (!RequestNotificationHandleRaised_.exchange(true)) {
            YT_LOG_DEBUG_IF(EnableIOUringLogging_, "Request notification raise (Handle: %v, Reason: EnqueueMany)",
                RequestNotificationHandle_.GetFD());

            RequestNotificationHandle_.Raise();
        }

        return EQueueSubmitResult::Ok;
    }

    void PrepareDequeue(int /*threadIndex*/)
    { }

    TUringRequestPtr TryDequeue(int /*threadIndex*/)
    {
        RequestNotificationHandleRaised_.store(false);

        TUringRequestPtr request;
        Queue_.try_dequeue(request);

        return request;
    }

    void MarkFinished(int /*threadIndex*/, TUringRequestPtr request)
    {
        request->SetPromise();
    }

    const TNotificationHandle& GetNotificationHandle(int /*threadIndex*/) const
    {
        return RequestNotificationHandle_;
    }

    void Reconfigure(int /*threadCount*/)
    {
        YT_LOG_DEBUG("Request notification raise (Handle: %v, Reason: Reconfigure)",
            RequestNotificationHandle_.GetFD());

        RequestNotificationHandle_.Raise();
    }

    static TString GetThreadPoolName()
    {
        return "IOU";
    }

private:
    const bool EnableIOUringLogging_;
    moodycamel::ConcurrentQueue<TUringRequestPtr> Queue_;
    TNotificationHandle RequestNotificationHandle_{true};
    std::atomic<bool> RequestNotificationHandleRaised_ = false;
};

////////////////////////////////////////////////////////////////////////////////

class TFairShareQueue
    : public TRefCounted
{
public:
    TFairShareQueue(TString locationId, TUringConfigProviderPtr config)
        : Config_(std::move(config))
        , EnableIOUringLogging_(Config_->EnableIOUringLogging)
        , Shards_(MaxUringThreadCount)
        , OffloadActionQueue_(New<TActionQueue>(Format("%v:%v", "UOffload", locationId)))
        , OffloadInvoker_(OffloadActionQueue_->GetInvoker())
        , OffloadScheduled_(false)
    { }

    EQueueSubmitResult Enqueue(
        TUringRequestPtr request,
        EWorkloadCategory category,
        IIOEngine::TSessionId sessionId)
    {
        auto shardIndex = GetShardIndex(sessionId);
        auto& shard = Shards_[shardIndex];

        shard.Queue.Enqueue({
            .Item = std::move(request),
            .PoolId = category,
            .PoolWeight = GetPoolWeight(category),
            .FairShareTag = sessionId,
        });

        NotifyIfNeeded(shard);
        return CheckShardAfterEnqueue(shardIndex);
    }

    EQueueSubmitResult Enqueue(
        std::vector<TUringRequestPtr>& request,
        EWorkloadCategory category,
        IIOEngine::TSessionId sessionId)
    {
        auto shardIndex = GetShardIndex(sessionId);
        auto& shard = Shards_[shardIndex];

        std::vector<TRequestQueue::TEnqueuedTask> toEnqueue;
        toEnqueue.reserve(request.size());

        for (auto& request : request) {
            toEnqueue.push_back({
                .Item = std::move(request),
                .PoolId = category,
                .PoolWeight = GetPoolWeight(category),
                .FairShareTag = sessionId,
            });
        }

        shard.Queue.EnqueueMany(std::move(toEnqueue));
        NotifyIfNeeded(shard);
        return CheckShardAfterEnqueue(shardIndex);
    }

    void PrepareDequeue(int threadIndex)
    {
        auto& shard = Shards_[threadIndex];
        shard.RequestNotificationHandleRaised.store(false);

        shard.Queue.PrepareDequeue();
    }

    TUringRequestPtr TryDequeue(int threadIndex)
    {
        auto& queue = Shards_[threadIndex].Queue;
        return queue.TryDequeue();
    }

    void MarkFinished(int threadIndex, TUringRequestPtr request)
    {
        auto now = GetCpuInstant();

        auto& queue = Shards_[threadIndex].Queue;
        queue.MarkFinished(request, now);

        OffloadedRequests_.Enqueue(std::move(request));
        ScheduleOffload();
    }

    const TNotificationHandle& GetNotificationHandle(int threadIndex) const
    {
        return Shards_[threadIndex].RequestNotificationHandle;
    }

    void Reconfigure(int threadCount)
    {
        // Resharding
        for (int sourceShardIndex = threadCount; sourceShardIndex < std::ssize(Shards_); ++sourceShardIndex) {
            auto& destinationShard = Shards_[RandomNumber<ui32>(threadCount)];
            destinationShard.Queue.EnqueueMany(DrainShard(sourceShardIndex));
        }

        for (int shardIndex = 0; shardIndex < threadCount; ++shardIndex) {
            auto& shard = Shards_[shardIndex];
            YT_LOG_DEBUG("Request notification raise (Handle: %v, Reason: Reconfigure)",
                shard.RequestNotificationHandle.GetFD());

            shard.RequestNotificationHandle.Raise();
        }
    }

    static TString GetThreadPoolName()
    {
        return "FSU";
    }

private:
    using TRequestQueue = TMpscFairShareQueue<EWorkloadCategory, TUringRequestPtr, TIOEngineBase::TSessionId>;

    struct alignas(2 * CacheLineSize) TQueueShard
    {
        TRequestQueue Queue;
        TNotificationHandle RequestNotificationHandle{true};
        std::atomic<bool> RequestNotificationHandleRaised = false;
    };

    const TUringConfigProviderPtr Config_;
    const bool EnableIOUringLogging_;
    std::vector<TQueueShard> Shards_;

    TMpscShardedQueue<TUringRequestPtr> OffloadedRequests_;
    TActionQueuePtr OffloadActionQueue_;
    IInvokerPtr OffloadInvoker_;
    std::atomic<bool> OffloadScheduled_;

    void NotifyIfNeeded(TQueueShard& shard)
    {
        if (shard.RequestNotificationHandleRaised.load(std::memory_order::relaxed)) {
            return;
        }

        if (!shard.RequestNotificationHandleRaised.exchange(true)) {
            YT_LOG_DEBUG_IF(EnableIOUringLogging_, "Request notification raise (Handle: %v, Reason: NotifyIfNeeded)",
                shard.RequestNotificationHandle.GetFD());

            shard.RequestNotificationHandle.Raise();
        }
    }

    EQueueSubmitResult CheckShardAfterEnqueue(int shardIndex)
    {
        if (Y_UNLIKELY(shardIndex >= Config_->GetUringThreadCount(std::memory_order::seq_cst))) {
            return EQueueSubmitResult::NeedReconfigure;
        }

        return EQueueSubmitResult::Ok;
    }

    double GetPoolWeight(EWorkloadCategory category) const
    {
        constexpr double DefaultWeight = 1;

        switch (category) {
            case EWorkloadCategory::UserInteractive:
                return Config_->GetUserInteractiveRequestWeight();

            case EWorkloadCategory::UserRealtime:
                return Config_->GetUserRealtimeRequestWeight();

            default:
                return DefaultWeight;
        }
    }

    int GetShardIndex(TIOEngineBase::TSessionId sessionId) const
    {
        if (!sessionId) {
            return static_cast<int>(RandomNumber<ui32>(Config_->GetUringThreadCount()));
        }

        THash<IIOEngine::TSessionId> hasher;
        return hasher(sessionId) % Config_->GetUringThreadCount();
    }

    void ScheduleOffload()
    {
        if (OffloadScheduled_.load(std::memory_order::relaxed)) {
            return;
        }

        if (OffloadScheduled_.exchange(true)) {
            // Already scheduled.
            return;
        }

        TDelayedExecutor::Submit(
            BIND(&TFairShareQueue::DoOffload, MakeStrong(this)),
            OffloadRequestDelay,
            OffloadInvoker_);
    }

    void DoOffload()
    {
        OffloadScheduled_.store(false);
        OffloadedRequests_.ConsumeAll([] (std::vector<TUringRequestPtr>& batch) {
            for (auto& request : batch) {
                request->SetPromise();
                request.reset();
            }
        });
    }

    std::vector<TRequestQueue::TEnqueuedTask> DrainShard(int shardIndex)
    {
        std::vector<TRequestQueue::TEnqueuedTask> shardTasks;
        auto& queue = Shards_[shardIndex].Queue;

        while(true) {
            bool dequeued = false;
            queue.PrepareDequeue();

            while (auto request = queue.TryDequeue()) {
                dequeued = true;
                queue.MarkFinished(request, {});
                shardTasks.push_back({
                    .Item = std::move(request),
                });
            }

            if (!dequeued) {
                break;
            }
        }

        return shardTasks;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TQueue>
class TUringThreadPoolBase
    : public IUringThreadPool
{
public:
    TUringThreadPoolBase(
        TString threadNamePrefix,
        TString locationId,
        TUringIOEngineConfigPtr config,
        TIOEngineSensorsPtr sensors,
        IInvokerPtr reconfigureInvoker)
        : Config_(New<TUringConfigProvider>(config))
        , ThreadNamePrefix_(std::move(threadNamePrefix))
        , Sensors_(std::move(sensors))
        , ReconfigureInvoker_(std::move(reconfigureInvoker))
        , EnableIOUringLogging_(Config_->EnableIOUringLogging)
        , RequestQueue_(New<TQueue>(locationId, Config_))
    {
        ResizeThreads();
    }

    ~TUringThreadPoolBase()
    {
        StopThreads();
    }

    virtual const TString& Name() const override
    {
        return ThreadNamePrefix_;
    }

    virtual void PrepareDequeue(int threadIndex) override
    {
        return RequestQueue_->PrepareDequeue(threadIndex);
    }

    virtual TUringRequestPtr TryDequeue(int threadIndex) override
    {
        return RequestQueue_->TryDequeue(threadIndex);
    }

    virtual void MarkFinished(int threadIndex, TUringRequestPtr request) override
    {
        return RequestQueue_->MarkFinished(threadIndex, std::move(request));
    }

    virtual const TNotificationHandle& GetNotificationHandle(int threadIndex) const override
    {
        return RequestQueue_->GetNotificationHandle(threadIndex);
    }

    void Reconfigure(const TUringIOEngineConfigPtr& config)
    {
        Config_->Update(config);
        ReconfigureInvoker_->Invoke(BIND(&TUringThreadPoolBase::DoReconfigure, MakeWeak(this)));
    }

    void SubmitRequest(
        TUringRequestPtr request,
        EWorkloadCategory category,
        IIOEngine::TSessionId sessionId)
    {
        YT_LOG_DEBUG_IF(EnableIOUringLogging_, "Request enqueued (Request: %v, Type: %v)",
            request.get(),
            request->Type);

        auto result = RequestQueue_->Enqueue(std::move(request), category, sessionId);
        ReconfigureQueueIfNeeded(result);
    }

    void SubmitRequests(
        std::vector<TUringRequestPtr>& requests,
        EWorkloadCategory category,
        IIOEngine::TSessionId sessionId)
    {
        if (!requests.empty()) {
            YT_LOG_DEBUG_IF(EnableIOUringLogging_, "Requests enqueued (RequestCount: %v, Type: %v)",
            std::ssize(requests),
            requests.front()->Type);
        }

        auto result = RequestQueue_->Enqueue(requests, category, sessionId);
        ReconfigureQueueIfNeeded(result);
    }

    void ReconfigureQueueIfNeeded(EQueueSubmitResult result)
    {
        if (Y_LIKELY(result != EQueueSubmitResult::NeedReconfigure)) {
            return;
        }

        YT_LOG_DEBUG("Reconfiguring request queue is requested");

        ReconfigureInvoker_->Invoke(BIND(&TUringThreadPoolBase::ReconfigureQueue, MakeWeak(this)));
    }

private:
    const TUringConfigProviderPtr Config_;
    const TString ThreadNamePrefix_;
    const TIOEngineSensorsPtr Sensors_;
    const IInvokerPtr ReconfigureInvoker_;
    const bool EnableIOUringLogging_;

    using TUringThreadPtr = TIntrusivePtr<TUringThread>;

    std::vector<TUringThreadPtr> Threads_;
    TIntrusivePtr<TQueue> RequestQueue_;

    void StopThreads()
    {
        for (const auto& thread : Threads_) {
            thread->Stop();
        }
    }

    void ReconfigureQueue()
    {
        VERIFY_INVOKER_AFFINITY(ReconfigureInvoker_);

        RequestQueue_->Reconfigure(std::ssize(Threads_));
    }

    void DoReconfigure()
    {
        VERIFY_INVOKER_AFFINITY(ReconfigureInvoker_);

        ResizeThreads();
    }

    void ResizeThreads()
    {
        int oldThreadCount = std::ssize(Threads_);
        int newThreadCount = Config_->GetUringThreadCount();

        if (oldThreadCount == newThreadCount) {
            return;
        }

        while (std::ssize(Threads_) < newThreadCount) {
            auto threadIndex = Threads_.size();
            auto thread = New<TUringThread>(this, Config_, threadIndex, Sensors_);
            Threads_.push_back(std::move(thread));
        }

        while (std::ssize(Threads_) > newThreadCount) {
            auto thread = Threads_.back();
            Threads_.pop_back();
            thread->Stop();
        }

        YT_LOG_DEBUG("Uring thread pool reconfigured (ThreadNamePrefix: %v, ThreadPoolSize: %v -> %v)",
            ThreadNamePrefix_,
            oldThreadCount,
            newThreadCount);

        RequestQueue_->Reconfigure(std::ssize(Threads_));
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TRequestQueue, typename TRequestSlicer>
class TUringIOEngineBase
    : public TIOEngineBase
{
public:
    using TConfig = TUringIOEngineConfig;
    using TConfigPtr = TIntrusivePtr<TConfig>;

    using TUringThreadPool = TUringThreadPoolBase<TRequestQueue>;
    using TUringThreadPoolPtr = TIntrusivePtr<TUringThreadPool>;

    TUringIOEngineBase(
        TConfigPtr config,
        TString locationId,
        TProfiler profiler,
        NLogging::TLogger logger)
        : TIOEngineBase(
            config,
            locationId,
            std::move(profiler),
            std::move(logger))
        , StaticConfig_(std::move(config))
        , Config_(New<TUringConfigProvider>(StaticConfig_))
        , ReconfigureInvoker_(CreateSerializedInvoker(GetAuxPoolInvoker(), NProfiling::TTagSet({{"invoker", "uring_io_engine_base"}, {"location_id", locationId}})))
        , ThreadPool_(New<TUringThreadPool>(
            Format("%v:%v", TRequestQueue::GetThreadPoolName(), LocationId_),
            LocationId_,
            StaticConfig_,
            Sensors_,
            ReconfigureInvoker_))
    { }

    ~TUringIOEngineBase()
    {
        GetFinalizerInvoker()->Invoke(BIND([threadPool = std::move(ThreadPool_)] () mutable {
            threadPool.Reset();
        }));
    }

    TFuture<TReadResponse> Read(
        std::vector<TReadRequest> requests,
        const TWorkloadDescriptor& descriptor,
        TRefCountedTypeCookie tagCookie,
        TSessionId sessionId,
        bool useDedicatedAllocations) override
    {
        if (std::ssize(requests) > MaxSubrequestCount) {
            return MakeFuture<TReadResponse>(TError("Too many read requests: %v > %v",
                requests.size(),
                MaxSubrequestCount));
        }

        auto readRequestCombiner = useDedicatedAllocations
            ? CreateDummyReadRequestCombiner()
            : CreateReadRequestCombiner();

        auto combinedRequests = readRequestCombiner->Combine(
            std::move(requests),
            Config_->GetDirectIOPageSize(),
            tagCookie);

        TRequestSlicer requestSlicer(Config_->GetDesiredRequestSize(), Config_->GetMinRequestSize());

        return SubmitReadRequests(
            readRequestCombiner,
            combinedRequests,
            requestSlicer,
            descriptor.Category,
            sessionId);
    }

    TFuture<void> Write(
        TWriteRequest request,
        const TWorkloadDescriptor& descriptor,
        TSessionId sessionId) override
    {
        TRequestSlicer requestSlicer(Config_->GetDesiredRequestSize(), Config_->GetMinRequestSize());

        auto slices = requestSlicer.Slice(std::move(request));

        std::vector<TFuture<void>> futures;
        std::vector<TUringRequestPtr> uringRequests;

        futures.reserve(slices.size());
        uringRequests.reserve(slices.size());

        for (auto& slice : slices) {
            auto uringRequest = std::make_unique<TWriteUringRequest>();
            uringRequest->Type = EUringRequestType::Write;
            uringRequest->WriteRequest = std::move(slice);
            uringRequest->RequestCounterGuard = CreateInFlightRequestGuard(EIOEngineRequestType::Write);

            futures.push_back(uringRequest->Promise.ToFuture());
            uringRequests.push_back(std::move(uringRequest));
        }

        ThreadPool_->SubmitRequests(uringRequests, descriptor.Category, sessionId);

        return AllSucceeded(std::move(futures));
    }

    TFuture<void> FlushFile(
        TFlushFileRequest request,
        const TWorkloadDescriptor& descriptor,
        TSessionId sessionId) override
    {
        auto uringRequest = std::make_unique<TFlushFileUringRequest>();
        uringRequest->Type = EUringRequestType::FlushFile;
        uringRequest->FlushFileRequest = std::move(request);
        return SubmitRequest<void>(std::move(uringRequest), descriptor.Category, sessionId);
    }

    virtual TFuture<void> FlushFileRange(
        TFlushFileRangeRequest /*request*/,
        const TWorkloadDescriptor& /*descriptor*/,
        TSessionId /*sessionId*/) override
    {
        // TODO (capone212): implement
        return VoidFuture;
    }

private:
    const TConfigPtr StaticConfig_;
    const TUringConfigProviderPtr Config_;
    const IInvokerPtr ReconfigureInvoker_;
    TUringThreadPoolPtr ThreadPool_;

    template <typename TUringResponse, typename TUringRequest>
    TFuture<TUringResponse> SubmitRequest(
        TUringRequest request,
        EWorkloadCategory category,
        IIOEngine::TSessionId sessionId)
    {
        auto future = request->Promise.ToFuture();
        ThreadPool_->SubmitRequest(std::move(request), category, sessionId);
        return future;
    }

    void DoReconfigure(const NYTree::INodePtr& node) override
    {
        auto config = UpdateYsonStruct(StaticConfig_, node);
        Config_->Update(config);

        ThreadPool_->Reconfigure(config);
    }

    TFuture<TReadResponse> SubmitReadRequests(
        IReadRequestCombinerPtr readRequestCombiner,
        std::vector<IReadRequestCombiner::TCombinedRequest>& combinedRequests,
        TDummyRequestSlicer& /*slicer*/,
        EWorkloadCategory category,
        TSessionId sessionId)
    {
        auto uringRequest = std::make_unique<TReadUringRequest>(readRequestCombiner);
        uringRequest->Type = EUringRequestType::Read;
        uringRequest->ReadSubrequests.reserve(combinedRequests.size());
        uringRequest->ReadSubrequestStates.reserve(combinedRequests.size());
        uringRequest->PendingReadSubrequestIndexes.reserve(combinedRequests.size());
        uringRequest->RequestCounterGuard = CreateInFlightRequestGuard(EIOEngineRequestType::Read);

        for (int index = 0; index < std::ssize(combinedRequests); ++index) {
            const auto& ioRequest = combinedRequests[index].ReadRequest;
            uringRequest->PaddedBytes += GetPaddedSize(
                ioRequest.Offset,
                ioRequest.Size,
                ioRequest.Handle->IsOpenForDirectIO() ? Config_->GetDirectIOPageSize() : DefaultPageSize);
            uringRequest->ReadSubrequests.push_back({
                .Handle = std::move(ioRequest.Handle),
                .Offset = ioRequest.Offset,
                .Size = ioRequest.Size
            });
            uringRequest->ReadSubrequestStates.push_back({
                .Buffer = std::move(combinedRequests[index].ResultBuffer)
            });
            uringRequest->PendingReadSubrequestIndexes.push_back(index);
        }

        auto paddedBytes = uringRequest->PaddedBytes;
        auto subrequestCount = std::ssize(uringRequest->PendingReadSubrequestIndexes);

        return SubmitRequest<void>(std::move(uringRequest), category, sessionId)
            .Apply(BIND([combiner = std::move(readRequestCombiner), paddedBytes, subrequestCount] {
                return TReadResponse{
                    .OutputBuffers = std::move(combiner->ReleaseOutputBuffers()),
                    .PaddedBytes = paddedBytes,
                    .IORequests = subrequestCount,
                };
            }));
    }

    TFuture<TReadResponse> SubmitReadRequests(
        IReadRequestCombinerPtr readRequestCombiner,
        std::vector<IReadRequestCombiner::TCombinedRequest>& combinedRequests,
        TIORequestSlicer& slicer,
        EWorkloadCategory category,
        TSessionId sessionId)
    {
        std::vector<TUringRequestPtr> uringRequests;
        uringRequests.reserve(combinedRequests.size());
        i64 paddedBytes = 0;

        std::vector<TFuture<void>> futures;
        futures.reserve(combinedRequests.size());

        for (auto& request : combinedRequests) {
            for (auto& slice : slicer.Slice(std::move(request.ReadRequest), request.ResultBuffer)) {
                auto uringRequest = std::make_unique<TReadUringRequest>(readRequestCombiner);
                uringRequest->Type = EUringRequestType::Read;

                uringRequest->ReadSubrequests.reserve(1);
                uringRequest->ReadSubrequestStates.reserve(1);
                uringRequest->PendingReadSubrequestIndexes.reserve(1);
                uringRequest->RequestCounterGuard = CreateInFlightRequestGuard(EIOEngineRequestType::Read);

                paddedBytes += GetPaddedSize(
                    slice.Request.Offset,
                    slice.Request.Size,
                    slice.Request.Handle->IsOpenForDirectIO() ? Config_->GetDirectIOPageSize() : DefaultPageSize);
                uringRequest->ReadSubrequests.push_back({
                    .Handle = std::move(slice.Request.Handle),
                    .Offset = slice.Request.Offset,
                    .Size = slice.Request.Size
                });
                uringRequest->ReadSubrequestStates.push_back({
                    .Buffer = slice.OutputBuffer,
                });
                uringRequest->PendingReadSubrequestIndexes.push_back(0);

                futures.push_back(uringRequest->Promise.ToFuture());

                uringRequests.push_back(std::move(uringRequest));
            }
        }

        ThreadPool_->SubmitRequests(uringRequests, category, sessionId);

        auto subrequestCount = std::ssize(futures);

        return AllSucceeded(std::move(futures))
            .Apply(BIND([paddedBytes, combiner = std::move(readRequestCombiner), subrequestCount] {
                return TReadResponse{
                    .OutputBuffers = std::move(combiner->ReleaseOutputBuffers()),
                    .PaddedBytes = paddedBytes,
                    .IORequests = subrequestCount,
                };
            }));
    }
};

#endif

////////////////////////////////////////////////////////////////////////////////

IIOEnginePtr CreateIOEngineUring(
    EIOEngineType engineType,
    NYTree::INodePtr ioConfig,
    TString locationId,
    NProfiling::TProfiler profiler,
    NLogging::TLogger logger)

{
#ifdef _linux_

    using TFairUringIOEngine = TUringIOEngineBase<TFairShareQueue, TIORequestSlicer>;
    using TUringIOEngine = TUringIOEngineBase<TMoodyCamelQueue, TDummyRequestSlicer>;

    switch (engineType) {
        case EIOEngineType::FairShareUring:
            return CreateIOEngine<TFairUringIOEngine>(
                std::move(ioConfig),
                std::move(locationId),
                std::move(profiler),
                std::move(logger));

        case EIOEngineType::Uring:
            return CreateIOEngine<TUringIOEngine>(
                std::move(ioConfig),
                std::move(locationId),
                std::move(profiler),
                std::move(logger));

        default:
            YT_ABORT();
    };

#endif
    return { };
}

////////////////////////////////////////////////////////////////////////////////

bool IsUringIOEngineSupported()
{
#ifdef _linux_
    io_uring uring;
    auto result = io_uring_queue_init(MaxUringConcurrentRequestsPerThread, &uring, /*flags*/ 0);
    if (result < 0) {
        return false;
    }
    io_uring_queue_exit(&uring);
    return true;

#endif
    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
