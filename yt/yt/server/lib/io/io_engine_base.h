#pragma once

#include "io_engine.h"

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/threading/public.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#ifdef _linux_
    #ifndef FALLOC_FL_CONVERT_UNWRITTEN
        #define FALLOC_FL_CONVERT_UNWRITTEN 0x4
    #endif
#endif


namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

constexpr auto DefaultPageSize = 4_KB;
constexpr auto MaxIovCountPerRequest = 64;

////////////////////////////////////////////////////////////////////////////////

class TIOEngineConfigBase
    : public NYTree::TYsonStruct
{
public:
    int AuxThreadCount;
    int FsyncThreadCount;

    bool EnableSync;

    i64 MaxBytesPerRead;
    i64 MaxBytesPerWrite;

    // For tests only.
    std::optional<i64> SimulatedMaxBytesPerWrite;
    std::optional<i64> SimulatedMaxBytesPerRead;

    std::optional<TDuration> SickReadTimeThreshold;
    std::optional<TDuration> SickReadTimeWindow;
    std::optional<TDuration> SickWriteTimeThreshold;
    std::optional<TDuration> SickWriteTimeWindow;
    std::optional<TDuration> SicknessExpirationTimeout;

    EDirectIOPolicy UseDirectIOForReads;

    REGISTER_YSON_STRUCT(TIOEngineConfigBase);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

class TInflightCounter
{
public:
    void Increment();
    void Decrement();

    static TInflightCounter Create(NProfiling::TProfiler& profiler, const TString& name);

private:
    struct TState
        : public TRefCounted
    {
        std::atomic<int> Counter = 0;
    };

    NYT::TIntrusivePtr<TState> State_;
};

////////////////////////////////////////////////////////////////////////////////

struct TIOEngineSensors final
{
    TIOEngineSensors() = default;
    TIOEngineSensors(const TIOEngineSensors&) = delete;
    TIOEngineSensors& operator=(const TIOEngineSensors&) = delete;

    struct TRequestSensors
    {
        // Single request time.
        NProfiling::TEventTimer Timer;

        // Cumulative execution time of all requests.
        NProfiling::TTimeCounter TotalTimeCounter;

        // Total requests count.
        NProfiling::TCounter Counter;

        // Currently executing requests count.
        TInflightCounter InflightCounter;
    };

    NProfiling::TCounter WrittenBytesCounter;
    NProfiling::TCounter ReadBytesCounter;

    NProfiling::TCounter KernelWrittenBytesCounter;
    NProfiling::TCounter KernelReadBytesCounter;

    TRequestSensors ReadSensors;
    TRequestSensors WriteSensors;
    TRequestSensors SyncSensors;
    TRequestSensors DataSyncSensors;
    TRequestSensors IoSubmitSensors;

    std::atomic<i64> TotalReadBytesCounter = 0;
    std::atomic<i64> TotalWrittenBytesCounter = 0;

    void RegisterWrittenBytes(i64 count);
    void RegisterReadBytes(i64 count);

    void UpdateKernelStatistics();
};

using TIOEngineSensorsPtr = TIntrusivePtr<TIOEngineSensors>;

////////////////////////////////////////////////////////////////////////////////

class TRequestStatsGuard
{
public:
    TRequestStatsGuard(TIOEngineSensors::TRequestSensors sensors);
    TRequestStatsGuard(TRequestStatsGuard&& other) = default;

    ~TRequestStatsGuard();

    TDuration GetElapsedTime() const;

private:
    TIOEngineSensors::TRequestSensors Sensors_;
    NProfiling::TWallTimer Timer_;
};

////////////////////////////////////////////////////////////////////////////////

class TIOEngineBase
    : public IIOEngine
{
public:
    TFuture<TIOEngineHandlePtr> Open(TOpenRequest request, const TWorkloadDescriptor& descriptor, TSessionId sessionId) override;

    TFuture<void> Close(TCloseRequest request, const TWorkloadDescriptor& descriptor, TSessionId sessionId) override;

    TFuture<void> FlushDirectory(TFlushDirectoryRequest request, const TWorkloadDescriptor& descriptor, TSessionId sessionId) override;

    TFuture<void> Allocate(TAllocateRequest request, const TWorkloadDescriptor& descriptor, TSessionId sessionId) override;

    virtual TFuture<void> Lock(TLockRequest request, const TWorkloadDescriptor& descriptor, TSessionId sessionId) override;

    virtual TFuture<void> Resize(TResizeRequest request, const TWorkloadDescriptor& descriptor, TSessionId sessionId) override;

    bool IsSick() const override;

    const IInvokerPtr& GetAuxPoolInvoker() override;

    i64 GetTotalReadBytes() const override;

    i64 GetTotalWrittenBytes() const override;

    EDirectIOPolicy UseDirectIOForReads() const override;

protected:
    using TConfig = TIOEngineConfigBase;
    using TConfigPtr = TIntrusivePtr<TConfig>;

    const TString LocationId_;
    const NLogging::TLogger Logger;
    const NProfiling::TProfiler Profiler;
    const TIOEngineSensorsPtr Sensors_ = New<TIOEngineSensors>();

    TIOEngineBase(
        TConfigPtr config,
        TString locationId,
        NProfiling::TProfiler profiler,
        NLogging::TLogger logger);

    TIOEngineHandlePtr DoOpen(const TOpenRequest& request);

    void DoFlushDirectory(const TFlushDirectoryRequest& request);
    void DoClose(const TCloseRequest& request);
    void DoAllocate(const TAllocateRequest& request);
    static int GetLockOp(ELockFileMode mode);
    void DoLock(const TLockRequest& request);
    void DoResize(const TResizeRequest& request);
    void AddWriteWaitTimeSample(TDuration duration);
    void AddReadWaitTimeSample(TDuration duration);
    void Reconfigure(const NYTree::INodePtr& node) override;

private:
    const TConfigPtr StaticConfig_;
    TAtomicIntrusivePtr<TConfig> Config_;

    const NConcurrency::IThreadPoolPtr AuxThreadPool_;
    const NConcurrency::IThreadPoolPtr FsyncThreadPool_;
    const IPrioritizedInvokerPtr AuxInvoker_;
    const IPrioritizedInvokerPtr FsyncInvoker_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ReadWaitLock_);
    std::optional<TInstant> SickReadWaitStart_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, WriteWaitLock_);
    std::optional<TInstant> SickWriteWaitStart_;

    std::atomic<bool> Sick_ = false;
    std::atomic<i64> SicknessCounter_ = 0;

    NProfiling::TGauge SickGauge_;
    NProfiling::TGauge SickEventsGauge_;

    std::atomic<bool> EnableFallocateConvertUnwritten_ = true;


    void InitProfilerSensors();
    void SetSickFlag(const TError& error);
    void ResetSickFlag();
    virtual void DoReconfigure(const NYTree::INodePtr& node) = 0;
};

////////////////////////////////////////////////////////////////////////////////

i64 GetPaddedSize(i64 offset, i64 size, i64 alignment);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO

#define IO_ENGINE_BASE_INL_H_
#include "io_engine_base-inl.h"
#undef IO_ENGINE_BASE_INL_H_
