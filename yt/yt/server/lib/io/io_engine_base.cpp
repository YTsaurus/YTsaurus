#include "io_engine_base.h"

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/proc.h>

#include <library/cpp/yt/system/handle_eintr.h>

#include <library/cpp/yt/misc/tls.h>

namespace NYT::NIO {

using namespace NConcurrency;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

void TIOEngineConfigBase::Register(TRegistrar registrar)
{
    registrar.Parameter("aux_thread_count", &TThis::AuxThreadCount)
        .GreaterThanOrEqual(1)
        .Default(1);
    registrar.Parameter("fsync_thread_count", &TThis::FsyncThreadCount)
        .GreaterThanOrEqual(1)
        .Default(1);

    registrar.Parameter("enable_sync", &TThis::EnableSync)
        .Default(true);

    registrar.Parameter("max_bytes_per_read", &TThis::MaxBytesPerRead)
        .GreaterThanOrEqual(1)
        .Default(256_MB);
    registrar.Parameter("max_bytes_per_write", &TThis::MaxBytesPerWrite)
        .GreaterThanOrEqual(1)
        .Default(256_MB);

    registrar.Parameter("simulated_max_bytes_per_read", &TThis::SimulatedMaxBytesPerRead)
        .Default()
        .GreaterThan(0);
    registrar.Parameter("simulated_max_bytes_per_write", &TThis::SimulatedMaxBytesPerWrite)
        .Default()
        .GreaterThan(0);

    registrar.Parameter("sick_read_time_threshold", &TThis::SickReadTimeThreshold)
        .GreaterThanOrEqual(TDuration::Zero())
        .Default();
    registrar.Parameter("sick_read_time_window", &TThis::SickReadTimeWindow)
        .GreaterThanOrEqual(TDuration::Zero())
        .Default();
    registrar.Parameter("sick_write_time_threshold", &TThis::SickWriteTimeThreshold)
        .GreaterThanOrEqual(TDuration::Zero())
        .Default();
    registrar.Parameter("sick_write_time_window", &TThis::SickWriteTimeWindow)
        .GreaterThanOrEqual(TDuration::Zero())
        .Default();
    registrar.Parameter("sickness_expiration_timeout", &TThis::SicknessExpirationTimeout)
        .GreaterThanOrEqual(TDuration::Zero())
        .Default();

    registrar.Parameter("use_direct_io_for_reads", &TThis::UseDirectIOForReads)
        .Default(EDirectIOPolicy::Never);

    registrar.Parameter("write_request_limit", &TThis::WriteRequestLimit)
        .Default(std::numeric_limits<i64>::max());
    registrar.Parameter("read_request_limit", &TThis::ReadRequestLimit)
        .Default(std::numeric_limits<i64>::max());
}

////////////////////////////////////////////////////////////////////////////////

void TInflightCounter::Increment()
{
    if (!State_) {
        return;
    }
    State_->Counter.fetch_add(1, std::memory_order::relaxed);
}

void TInflightCounter::Decrement()
{
    if (!State_) {
        return;
    }
    State_->Counter.fetch_sub(1, std::memory_order::relaxed);
}

TInflightCounter TInflightCounter::Create(TProfiler& profiler, const TString& name)
{
    TInflightCounter counter;
    counter.State_ = New<TState>();
    profiler.AddFuncGauge(name, counter.State_, [state = counter.State_.Get()] {
        return state->Counter.load(std::memory_order::relaxed);
    });
    return counter;
}

////////////////////////////////////////////////////////////////////////////////

void TIOEngineSensors::RegisterWrittenBytes(i64 count)
{
    WrittenBytesCounter.Increment(count);
    TotalWrittenBytesCounter.fetch_add(count, std::memory_order::relaxed);
}

void TIOEngineSensors::RegisterReadBytes(i64 count)
{
    ReadBytesCounter.Increment(count);
    TotalReadBytesCounter.fetch_add(count, std::memory_order::relaxed);
}

YT_PREVENT_TLS_CACHING void TIOEngineSensors::UpdateKernelStatistics()
{
    constexpr auto UpdatePeriod = TDuration::Seconds(1);

    thread_local std::optional<TInstant> LastUpdateInstant;
    thread_local TTaskDiskStatistics LastStatistics;

    auto now = TInstant::Now();
    if (!LastUpdateInstant || (now - *LastUpdateInstant) > UpdatePeriod) {
        if (LastUpdateInstant) {
            auto current = GetSelfThreadTaskDiskStatistics();

            KernelReadBytesCounter.Increment(current.ReadBytes - LastStatistics.ReadBytes);
            KernelWrittenBytesCounter.Increment(current.WriteBytes - LastStatistics.WriteBytes);

            LastStatistics = current;
        }

        LastUpdateInstant = now;
    }
}

////////////////////////////////////////////////////////////////////////////////

TRequestStatsGuard::TRequestStatsGuard(TIOEngineSensors::TRequestSensors sensors)
    : Sensors_(std::move(sensors))
{
    Sensors_.Counter.Increment();
    Sensors_.InflightCounter.Increment();
}

TRequestStatsGuard::~TRequestStatsGuard()
{
    auto duration = Timer_.GetElapsedTime();
    Sensors_.Timer.Record(duration);
    Sensors_.TotalTimeCounter.Add(duration);
    Sensors_.InflightCounter.Decrement();
}

TDuration TRequestStatsGuard::GetElapsedTime() const
{
    return Timer_.GetElapsedTime();
}

////////////////////////////////////////////////////////////////////////////////

TRequestCounterGuard::TRequestCounterGuard()
{
    Engine_ = nullptr;
}

TRequestCounterGuard::TRequestCounterGuard(TIntrusivePtr<TIOEngineBase> engine, EIOEngineRequestType requestType)
    : Engine_(std::move(engine))
    , RequestType_(requestType)
{
    YT_VERIFY(Engine_);

    switch (RequestType_) {
        case EIOEngineRequestType::Read:
            Engine_->InFlightReadRequestCount_.fetch_add(1);
            break;
        case EIOEngineRequestType::Write:
            Engine_->InFlightWriteRequestCount_.fetch_add(1);
            break;
        default:
            YT_ABORT();
    }
}

TRequestCounterGuard::TRequestCounterGuard(TRequestCounterGuard&& other)
{
    MoveFrom(std::move(other));
}

TRequestCounterGuard::~TRequestCounterGuard()
{
    Release();
}

TRequestCounterGuard& TRequestCounterGuard::operator=(TRequestCounterGuard&& other)
{
    if (this != &other) {
        Release();
        MoveFrom(std::move(other));
    }
    return *this;
}

void TRequestCounterGuard::Release()
{
    if (Engine_) {
        switch (RequestType_) {
            case EIOEngineRequestType::Read:
                Engine_->InFlightReadRequestCount_.fetch_sub(1);
                break;
            case EIOEngineRequestType::Write:
                Engine_->InFlightWriteRequestCount_.fetch_sub(1);
                break;
            default:
                YT_ABORT();
        }

        Engine_.Reset();
    }
}

void TRequestCounterGuard::MoveFrom(TRequestCounterGuard&& other)
{
    Engine_ = other.Engine_;
    RequestType_ = other.RequestType_;

    other.Engine_.Reset();
}

////////////////////////////////////////////////////////////////////////////////

TFuture<TIOEngineHandlePtr> TIOEngineBase::Open(TOpenRequest request, const TWorkloadDescriptor& descriptor, TSessionId /*sessionId*/)
{
    return BIND(&TIOEngineBase::DoOpen, MakeStrong(this), std::move(request))
        .AsyncVia(NConcurrency::CreateFixedPriorityInvoker(AuxInvoker_, GetBasicPriority(descriptor.Category)))
        .Run();
}

TFuture<void> TIOEngineBase::Close(TCloseRequest request, const TWorkloadDescriptor& descriptor, TSessionId /*sessionId*/)
{
    auto invoker = (request.Flush || request.Size) ? FsyncInvoker_ : AuxInvoker_;
    return BIND(&TIOEngineBase::DoClose, MakeStrong(this), std::move(request))
        .AsyncVia(NConcurrency::CreateFixedPriorityInvoker(invoker, GetBasicPriority(descriptor.Category)))
        .Run();
}

TFuture<void> TIOEngineBase::FlushDirectory(TFlushDirectoryRequest request, const TWorkloadDescriptor& descriptor, TSessionId /*sessionId*/)
{
    return BIND(&TIOEngineBase::DoFlushDirectory, MakeStrong(this), std::move(request))
        .AsyncVia(CreateFixedPriorityInvoker(FsyncInvoker_, GetBasicPriority(descriptor.Category)))
        .Run();
}

TFuture<void> TIOEngineBase::Allocate(TAllocateRequest request, const TWorkloadDescriptor& descriptor, TSessionId /*sessionId*/)
{
    return BIND(&TIOEngineBase::DoAllocate, MakeStrong(this), std::move(request))
        .AsyncVia(CreateFixedPriorityInvoker(AuxInvoker_, GetBasicPriority(descriptor.Category)))
        .Run();
}

TFuture<void> TIOEngineBase::Lock(TLockRequest request, const TWorkloadDescriptor& descriptor, TSessionId /*sessionId*/)
{
    return BIND(&TIOEngineBase::DoLock, MakeStrong(this), std::move(request))
        .AsyncVia(CreateFixedPriorityInvoker(AuxInvoker_, GetBasicPriority(descriptor.Category)))
        .Run();
}

TFuture<void> TIOEngineBase::Resize(TResizeRequest request, const TWorkloadDescriptor& descriptor, TSessionId /*sessionId*/)
{
    return BIND(&TIOEngineBase::DoResize, MakeStrong(this), std::move(request))
        .AsyncVia(CreateFixedPriorityInvoker(AuxInvoker_, GetBasicPriority(descriptor.Category)))
        .Run();
}

bool TIOEngineBase::IsSick() const
{
    return Sick_;
}

const IInvokerPtr& TIOEngineBase::GetAuxPoolInvoker()
{
    return AuxThreadPool_->GetInvoker();
}

i64 TIOEngineBase::GetTotalReadBytes() const
{
    return Sensors_->TotalReadBytesCounter.load(std::memory_order::relaxed);
}

i64 TIOEngineBase::GetTotalWrittenBytes() const
{
    return Sensors_->TotalWrittenBytesCounter.load(std::memory_order::relaxed);
}

EDirectIOPolicy TIOEngineBase::UseDirectIOForReads() const
{
    return Config_.Acquire()->UseDirectIOForReads;
}

bool TIOEngineBase::IsInFlightReadRequestLimitExceeded() const
{
    return InFlightReadRequestCount_.load(std::memory_order_relaxed) >= Config_.Acquire()->ReadRequestLimit;
}

i64 TIOEngineBase::GetInFlightReadRequestCount() const
{
    return InFlightReadRequestCount_.load(std::memory_order_relaxed);
}

i64 TIOEngineBase::GetReadRequestLimit() const
{
    return Config_.Acquire()->ReadRequestLimit;
}

bool TIOEngineBase::IsInFlightWriteRequestLimitExceeded() const
{
    return InFlightWriteRequestCount_.load(std::memory_order_relaxed) >= Config_.Acquire()->WriteRequestLimit;
}

i64 TIOEngineBase::GetInFlightWriteRequestCount() const
{
    return InFlightWriteRequestCount_.load(std::memory_order_relaxed);
}

i64 TIOEngineBase::GetWriteRequestLimit() const
{
    return Config_.Acquire()->WriteRequestLimit;
}

TIOEngineBase::TIOEngineBase(
    TConfigPtr config,
    TString locationId,
    NProfiling::TProfiler profiler,
    NLogging::TLogger logger)
    : LocationId_(std::move(locationId))
    , Logger(std::move(logger))
    , Profiler(std::move(profiler))
    , StaticConfig_(std::move(config))
    , Config_(StaticConfig_)
    , AuxThreadPool_(CreateThreadPool(StaticConfig_->AuxThreadCount, Format("IOA:%v", LocationId_)))
    , FsyncThreadPool_(CreateThreadPool(StaticConfig_->FsyncThreadCount, Format("IOS:%v", LocationId_)))
    , AuxInvoker_(CreatePrioritizedInvoker(AuxThreadPool_->GetInvoker(), NProfiling::TTagSet({{"invoker", "io_engine_base_aux"}, {"location_id", LocationId_}})))
    , FsyncInvoker_(CreatePrioritizedInvoker(FsyncThreadPool_->GetInvoker(), NProfiling::TTagSet({{"invoker", "io_engine_base_fsync"}, {"location_id", LocationId_}})))
{
    InitProfilerSensors();
}

TIOEngineHandlePtr TIOEngineBase::DoOpen(const TOpenRequest& request)
{
    Sensors_->UpdateKernelStatistics();
    auto handle = [&] {
        NTracing::TNullTraceContextGuard nullTraceContextGuard;
        return New<TIOEngineHandle>(request.Path, request.Mode);
    }();
    if (!handle->IsOpen()) {
        THROW_ERROR_EXCEPTION(
            "Cannot open %v",
            request.Path)
            << TErrorAttribute("mode", DecodeOpenMode(request.Mode))
            << TError::FromSystem();
    }
    return handle;
}

void TIOEngineBase::DoFlushDirectory(const TFlushDirectoryRequest& request)
{
    Sensors_->UpdateKernelStatistics();
    NFS::WrapIOErrors([&] {
        NTracing::TNullTraceContextGuard nullTraceContextGuard;
        if (StaticConfig_->EnableSync) {
            NFS::FlushDirectory(request.Path);
        }
    });
}

void TIOEngineBase::DoClose(const TCloseRequest& request)
{
    Sensors_->UpdateKernelStatistics();
    NFS::WrapIOErrors([&] {
        NTracing::TNullTraceContextGuard nullTraceContextGuard;
        if (request.Size) {
            request.Handle->Resize(*request.Size);
        }
        if (request.Flush && StaticConfig_->EnableSync) {
            TRequestStatsGuard statsGuard(Sensors_->SyncSensors);
            request.Handle->Flush();
        }
        request.Handle->Close();
    });
}

void TIOEngineBase::DoAllocate(const TAllocateRequest& request)
{
    Sensors_->UpdateKernelStatistics();
#ifdef _linux_
    NTracing::TNullTraceContextGuard nullTraceContextGuard;
    int mode = EnableFallocateConvertUnwritten_.load() ? FALLOC_FL_CONVERT_UNWRITTEN : 0;
    int result = HandleEintr(::fallocate, *request.Handle, mode, 0, request.Size);
    if (result != 0) {
        if ((errno == EPERM || errno == EOPNOTSUPP) && mode == FALLOC_FL_CONVERT_UNWRITTEN) {
            if (EnableFallocateConvertUnwritten_.exchange(false)) {
                YT_LOG_INFO(TError::FromSystem(), "fallocate call failed; disabling FALLOC_FL_CONVERT_UNWRITTEN mode");
            }
        } else {
            THROW_ERROR_EXCEPTION(NFS::EErrorCode::IOError, "fallocate call failed")
                << TError::FromSystem();
        }
    }
#else
    Y_UNUSED(request);
#endif
}

int TIOEngineBase::GetLockOp(ELockFileMode mode)
{
    switch (mode) {
        case ELockFileMode::Shared:
            return LOCK_SH;
        case ELockFileMode::Exclusive:
            return LOCK_EX;
        case ELockFileMode::Unlock:
            return LOCK_UN;
        default:
            YT_ABORT();
    }
}

void TIOEngineBase::DoLock(const TLockRequest& request)
{
    Sensors_->UpdateKernelStatistics();
    NFS::WrapIOErrors([&] {
        auto op = GetLockOp(request.Mode) + (request.Nonblocking ? LOCK_NB : 0);
        if (HandleEintr(::flock, *request.Handle, op) != 0) {
            ythrow TFileError();
        }
    });
}

void TIOEngineBase::DoResize(const TResizeRequest& request)
{
    Sensors_->UpdateKernelStatistics();
    NFS::WrapIOErrors([&] {
        if (!request.Handle->Resize(request.Size)) {
            ythrow TFileError();
        }
    });
}

void TIOEngineBase::AddWriteWaitTimeSample(TDuration duration)
{
    auto config = Config_.Acquire();
    if (config->SickWriteTimeThreshold && config->SickWriteTimeWindow && config->SicknessExpirationTimeout && !Sick_) {
        if (duration > *config->SickWriteTimeThreshold) {
            auto now = GetInstant();
            auto guard = Guard(WriteWaitLock_);
            if (!SickWriteWaitStart_) {
                SickWriteWaitStart_ = now;
            } else if (now - *SickWriteWaitStart_ > *config->SickWriteTimeWindow) {
                auto error = TError("Write is too slow")
                    << TErrorAttribute("sick_write_wait_start", *SickWriteWaitStart_);
                guard.Release();
                SetSickFlag(error);
            }
        } else {
            auto guard = Guard(WriteWaitLock_);
            SickWriteWaitStart_.reset();
        }
    }
}

void TIOEngineBase::AddReadWaitTimeSample(TDuration duration)
{
    auto config = Config_.Acquire();
    if (config->SickReadTimeThreshold && config->SickReadTimeWindow && config->SicknessExpirationTimeout && !Sick_) {
        if (duration > *config->SickReadTimeThreshold) {
            auto now = GetInstant();
            auto guard = Guard(ReadWaitLock_);
            if (!SickReadWaitStart_) {
                SickReadWaitStart_ = now;
            } else if (now - *SickReadWaitStart_ > *config->SickReadTimeWindow) {
                auto error = TError("Read is too slow")
                    << TErrorAttribute("sick_read_wait_start", *SickReadWaitStart_);
                guard.Release();
                SetSickFlag(error);
            }
        } else {
            auto guard = Guard(ReadWaitLock_);
            SickReadWaitStart_.reset();
        }
    }
}

TRequestCounterGuard TIOEngineBase::CreateInFlightRequestGuard(EIOEngineRequestType requestType)
{
    return TRequestCounterGuard(MakeStrong(this), requestType);
}

void TIOEngineBase::Reconfigure(const NYTree::INodePtr& node)
{
    auto realConfig = NYTree::UpdateYsonStruct(StaticConfig_, node);

    AuxThreadPool_->Configure(realConfig->AuxThreadCount);
    FsyncThreadPool_->Configure(realConfig->FsyncThreadCount);

    Config_.Store(realConfig);

    DoReconfigure(node);
}

void TIOEngineBase::InitProfilerSensors()
{
    Profiler.AddFuncGauge("/sick", MakeStrong(this), [this] {
        return Sick_.load();
    });

    Profiler.AddFuncGauge("/alive", MakeStrong(this), [] {
        return 1;
    });

    Profiler.AddFuncCounter("/sick_events", MakeStrong(this), [this] {
        return SicknessCounter_.load();
    });

    Sensors_->WrittenBytesCounter = Profiler.Counter("/written_bytes");
    Sensors_->ReadBytesCounter = Profiler.Counter("/read_bytes");

    Sensors_->KernelWrittenBytesCounter = Profiler.Counter("/kernel_written_bytes");
    Sensors_->KernelReadBytesCounter = Profiler.Counter("/kernel_read_bytes");

    auto makeRequestSensors = [] (TProfiler profiler) {
        TIOEngineSensors::TRequestSensors sensors;
        sensors.Timer = profiler.Timer("/time");
        sensors.TotalTimeCounter = profiler.TimeCounter("/total_time");
        sensors.Counter = profiler.Counter("/request_count");
        sensors.InflightCounter = TInflightCounter::Create(profiler, "/inflight_count");
        return sensors;
    };

    Sensors_->ReadSensors = makeRequestSensors(Profiler.WithPrefix("/read"));
    Sensors_->WriteSensors = makeRequestSensors(Profiler.WithPrefix("/write"));
    Sensors_->SyncSensors = makeRequestSensors(Profiler.WithPrefix("/sync"));
    Sensors_->DataSyncSensors = makeRequestSensors(Profiler.WithPrefix("/datasync"));
    Sensors_->IOSubmitSensors = makeRequestSensors(Profiler.WithPrefix("/uring_io_submit"));
}

void TIOEngineBase::SetSickFlag(const TError& error)
{
    auto config = Config_.Acquire();

    if (!config->SicknessExpirationTimeout) {
        return;
    }

    if (!Sick_.exchange(true)) {
        ++SicknessCounter_;

        TDelayedExecutor::Submit(
            BIND(&TIOEngineBase::ResetSickFlag, MakeStrong(this)),
            *config->SicknessExpirationTimeout);

        YT_LOG_WARNING(error, "Sick flag set");
    }
}

void TIOEngineBase::ResetSickFlag()
{
    {
        auto guard = Guard(WriteWaitLock_);
        SickWriteWaitStart_.reset();
    }

    {
        auto guard = Guard(ReadWaitLock_);
        SickReadWaitStart_.reset();
    }

    Sick_ = false;

    YT_LOG_WARNING("Sick flag reset");
}

////////////////////////////////////////////////////////////////////////////////

i64 GetPaddedSize(i64 offset, i64 size, i64 alignment)
{
    return AlignUp(offset + size, alignment) - AlignDown(offset, alignment);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO

