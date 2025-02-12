#pragma once

#include <cstdint>

namespace NYT::NFio {

struct TPluginOptions
{
    void *Pad;
    const char *Config;
    const char *ConfigPatch;
    uint32_t PrintConfig;
    uint32_t PrintConfigSchema;
};

struct TThreadOptions
{
    const char* JobName;
    uint32_t JobIndex;
    uint32_t IODepth;
    uint32_t NumFiles;
};

enum class EOperation
{
    Read = 0,           // DDIR_READ
    Write = 1,          // DDIR_WRITE
    Trim = 2,           // DDIR_TRIM
    Sync = 3,           // DDIR_SYNC
};

enum class EOperationStatus
{
    Completed = 0,      // FIO_Q_COMPLETED
    Queued = 1,         // FIO_Q_QUEUED
    Busy = 2,           // FIO_Q_BUSY
};

struct TOperationResult
{
    EOperationStatus Status;
    int Error = 0;
    uintptr_t IO = 0;
    uint64_t Residual = 0;
};

struct TFileOptions
{
    bool Append;
    bool Atomic;
    bool Create;
    bool Direct;
    bool Read;
    bool Random;
    bool Trim;
    bool Write;

    uint64_t FileOffset;
    uint64_t IOSize;
};

struct TFile;

struct IThread
{
    virtual int64_t GetFileSize(const char *name, TFile* file) = 0;
    virtual int UnlinkFile(const char *name, TFile* file) = 0;
    virtual TFile* OpenFile(const char *name, const TFileOptions& options) = 0;
    virtual void CloseFile(TFile* file) = 0;
    virtual int Prepare(TFile* file, EOperation op) = 0;
    virtual TOperationResult Queue(TFile* file, EOperation op, void *buf, uint64_t size, uint64_t offset, uintptr_t io) = 0;
    virtual int Cancel(TFile* file) = 0;
    virtual int Commit() = 0;
    virtual int GetEvents(int minEvents, int maxEvents, const struct timespec *timeout) = 0;
    virtual TOperationResult GetResult(int event) = 0;
    virtual void Put() = 0;
};

IThread* GetThread(const TPluginOptions& pluginOptions, const TThreadOptions& options);

} // namespace NYT::NFio
