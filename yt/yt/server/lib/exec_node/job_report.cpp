#include "job_report.h"

#include <yt/yt/core/yson/attributes_stripper.h>

namespace NYT::NExecNode {

using namespace NYTree;
using namespace NYson;
using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

TNodeJobReport TNodeJobReport::OperationId(TOperationId operationId)
{
    OperationId_ = operationId;
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::JobId(TJobId jobId)
{
    JobId_ = jobId;
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::Type(EJobType type)
{
    Type_ = type;
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::State(EJobState state)
{
    State_ = FormatEnum(state);
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::StartTime(TInstant startTime)
{
    StartTime_ = startTime.MicroSeconds();
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::FinishTime(TInstant finishTime)
{
    FinishTime_ = finishTime.MicroSeconds();
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::Error(const TError& error)
{
    if (!error.IsOK()) {
        Error_ = ConvertToYsonString(error).ToString();
    }
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::InterruptionInfo(TJobInterruptionInfo interruptionInfo)
{
    InterruptionInfo_ = ConvertToYsonString(interruptionInfo).ToString();
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::Spec(const NControllerAgent::NProto::TJobSpec& spec)
{
    TString specString;
    YT_VERIFY(spec.SerializeToString(&specString));
    Spec_ = std::move(specString);
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::SpecVersion(i64 specVersion)
{
    SpecVersion_ = specVersion;
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::Statistics(const TYsonString& statistics)
{
    Statistics_ = StripAttributes(statistics).ToString();
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::Events(const TJobEvents& events)
{
    Events_ = ConvertToYsonString(events).ToString();
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::StderrSize(i64 stderrSize)
{
    YT_VERIFY(!Stderr_.has_value());
    StderrSize_ = stderrSize;
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::Stderr(const TString& stderr)
{
    Stderr_ = stderr;
    StderrSize_ = Stderr_->size();
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::FailContext(const TString& failContext)
{
    FailContext_ = failContext;
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::Profile(const NJobAgent::TJobProfile& profile)
{
    Profile_ = profile;
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::CoreInfos(NControllerAgent::TCoreInfos coreInfos)
{
    CoreInfos_ = std::move(coreInfos);
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::ExecAttributes(const TYsonString& execAttributes)
{
    ExecAttributes_ = StripAttributes(execAttributes).ToString();
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::TreeId(TString treeId)
{
    TreeId_ = std::move(treeId);
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::MonitoringDescriptor(TString monitoringDescriptor)
{
    MonitoringDescriptor_ = std::move(monitoringDescriptor);
    return std::move(*this);
}

// TODO(aleksandr.gaev): Remove.
TNodeJobReport TNodeJobReport::Address(std::optional<std::string> address)
{
    Address_ = address;
    return std::move(*this);
}

TNodeJobReport TNodeJobReport::Addresses(std::optional<NNodeTrackerClient::TAddressMap> addresses)
{
    Addresses_ = addresses;
    return std::move(*this);
}

void TNodeJobReport::SetStatistics(const TYsonString& statistics)
{
    Statistics_ = StripAttributes(statistics).ToString();
}

void TNodeJobReport::SetStartTime(TInstant startTime)
{
    StartTime_ = startTime.MicroSeconds();
}

void TNodeJobReport::SetFinishTime(TInstant finishTime)
{
    FinishTime_ = finishTime.MicroSeconds();
}

void TNodeJobReport::SetJobCompetitionId(TJobId jobCompetitionId)
{
    JobCompetitionId_ = jobCompetitionId;
}

void TNodeJobReport::SetProbingJobCompetitionId(TJobId probingJobCompetitionId)
{
    ProbingJobCompetitionId_ = probingJobCompetitionId;
}

void TNodeJobReport::SetTaskName(const TString& taskName)
{
    TaskName_ = taskName;
}

TNodeJobReport TNodeJobReport::ArchiveFeatures(const TYsonString& archiveFeatures)
{
    ArchiveFeatures_ = archiveFeatures.ToString();
    return std::move(*this);
}

void TNodeJobReport::SetTtl(TDuration ttl)
{
    Ttl_ = ttl;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
