#pragma once

#include "public.h"

#include <yt/yt/server/lib/controller_agent/public.h>

#include <yt/yt/server/lib/job_agent/structs.h>

#include <yt/yt/ytlib/controller_agent/public.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/exec_node/public.h>

#include <yt/yt/ytlib/node_tracker_client/proto/node_tracker_service.pb.h>

#include <yt/yt/ytlib/scheduler/proto/resources.pb.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/property.h>

namespace NYT::NServer {

////////////////////////////////////////////////////////////////////////////////

struct TJobEvent
{
    explicit TJobEvent(NJobTrackerClient::EJobState state);
    explicit TJobEvent(NExecNode::EJobPhase phase);
    TJobEvent(NJobTrackerClient::EJobState state, NExecNode::EJobPhase phase);
    explicit TJobEvent(std::optional<NScheduler::EInterruptionReason> interruptionReason);

    DEFINE_BYREF_RO_PROPERTY(TInstant, Timestamp);
    DEFINE_BYREF_RO_PROPERTY(std::optional<NJobTrackerClient::EJobState>, State);
    DEFINE_BYREF_RO_PROPERTY(std::optional<NExecNode::EJobPhase>, Phase);
    DEFINE_BYREF_RO_PROPERTY(std::optional<NScheduler::EInterruptionReason>, InterruptionReason);
};

using TJobEvents = std::vector<TJobEvent>;

void Serialize(const TJobEvents& events, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

struct TJobInterruptionInfo
{
    struct TPreemptedFor
    {
        NScheduler::TAllocationId AllocationId;
        NControllerAgent::TOperationId OperationId;
    };

    TInstant Time;

    NScheduler::EInterruptionReason InterruptionReason;

    std::optional<TDuration> InterruptionTimeout;

    std::optional<TString> PreemptionReason;

    std::optional<TPreemptedFor> PreemptedFor;
};

void Serialize(const TJobInterruptionInfo& interruptionInfo, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

class TJobReport
{
public:
    TJobReport ExtractSpec() const;
    TJobReport ExtractStderr() const;
    TJobReport ExtractFailContext() const;
    TJobReport ExtractProfile() const;
    TJobReport ExtractIds() const;

    bool IsEmpty() const;

    DEFINE_BYREF_RO_PROPERTY(NJobTrackerClient::TOperationId, OperationId);
    DEFINE_BYREF_RO_PROPERTY(NJobTrackerClient::TJobId, JobId);
    DEFINE_BYREF_RO_PROPERTY(std::optional<NJobTrackerClient::EJobType>, Type);
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, State);
    DEFINE_BYREF_RO_PROPERTY(std::optional<i64>, StartTime);
    DEFINE_BYREF_RO_PROPERTY(std::optional<i64>, FinishTime);
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, Error);
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, InterruptionInfo);
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, Spec);
    DEFINE_BYREF_RO_PROPERTY(std::optional<i64>, SpecVersion);
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, Statistics);
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, Events);
    DEFINE_BYREF_RO_PROPERTY(std::optional<ui64>, StderrSize);
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, Stderr);
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, FailContext);
    DEFINE_BYREF_RO_PROPERTY(std::optional<NJobAgent::TJobProfile>, Profile);
    DEFINE_BYREF_RO_PROPERTY(std::optional<NControllerAgent::TCoreInfos>, CoreInfos);
    DEFINE_BYREF_RO_PROPERTY(NJobTrackerClient::TJobId, JobCompetitionId);
    DEFINE_BYREF_RO_PROPERTY(NJobTrackerClient::TJobId, ProbingJobCompetitionId);
    DEFINE_BYREF_RO_PROPERTY(std::optional<bool>, HasCompetitors);
    DEFINE_BYREF_RO_PROPERTY(std::optional<bool>, HasProbingCompetitors);
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, ExecAttributes);
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, TaskName);
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, TreeId);
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, MonitoringDescriptor);
    DEFINE_BYREF_RO_PROPERTY(std::optional<ui64>, JobCookie);
    // TODO(aleksandr.gaev): Remove.
    DEFINE_BYREF_RO_PROPERTY(std::optional<std::string>, Address);
    DEFINE_BYREF_RO_PROPERTY(std::optional<NNodeTrackerClient::TAddressMap>, Addresses);
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, ControllerState);
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, ArchiveFeatures);
    DEFINE_BYREF_RO_PROPERTY(std::optional<TDuration>, Ttl);
    DEFINE_BYREF_RO_PROPERTY(std::optional<std::string>, OperationIncarnation);

protected:
    TJobReport() = default;
};

////////////////////////////////////////////////////////////////////////////////

struct TGpuDevice
    : public NYTree::TYsonStruct
{
    int DeviceNumber;

    TString DeviceName;

    REGISTER_YSON_STRUCT(TGpuDevice);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TGpuDevice);

struct TExecAttributes
    : public NYTree::TYsonStructLite
{
    //! Job slot index.
    int SlotIndex = -1;

    //! Job container IP addresses.
    //! If job is not using network isolation its IPs
    //! coincide with node's IPs.
    std::vector<std::string> IPAddresses;

    //! Absolute path to job sandbox directory.
    TString SandboxPath;

    //! Medium of disk acquired by slot.
    TString MediumName;

    //! Absolute path to job proxy socket file.
    TString JobProxySocketPath;

    //! GPU devices used by job.
    std::vector<TIntrusivePtr<TGpuDevice>> GpuDevices;

    REGISTER_YSON_STRUCT_LITE(TExecAttributes);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NServer
