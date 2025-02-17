#pragma once

#include "public.h"

#include <yt/yt/server/lib/misc/job_report.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

class TNodeJobReport
    : public NServer::TJobReport
{
public:
    TNodeJobReport OperationId(TOperationId operationId);
    TNodeJobReport JobId(TJobId jobId);
    TNodeJobReport Type(EJobType type);
    TNodeJobReport State(EJobState state);
    TNodeJobReport StartTime(TInstant startTime);
    TNodeJobReport FinishTime(TInstant finishTime);
    TNodeJobReport Error(const TError& error);
    TNodeJobReport InterruptionInfo(NServer::TJobInterruptionInfo interruptionInfo);
    TNodeJobReport Spec(const NControllerAgent::NProto::TJobSpec& spec);
    TNodeJobReport SpecVersion(i64 specVersion);
    TNodeJobReport Statistics(const NYson::TYsonString& statistics);
    TNodeJobReport Events(const NServer::TJobEvents& events);
    TNodeJobReport StderrSize(i64 stderrSize);
    TNodeJobReport Stderr(const TString& stderr);
    TNodeJobReport FailContext(const TString& failContext);
    TNodeJobReport Profile(const NJobAgent::TJobProfile& profile);
    TNodeJobReport CoreInfos(NControllerAgent::TCoreInfos coreInfos);
    TNodeJobReport ExecAttributes(const NYson::TYsonString& execAttributes);
    TNodeJobReport TreeId(TString treeId);
    TNodeJobReport MonitoringDescriptor(TString monitoringDescriptor);
    // TODO(aleksandr.gaev): Remove.
    TNodeJobReport Address(std::optional<std::string> address);
    TNodeJobReport Addresses(std::optional<NNodeTrackerClient::TAddressMap> addresses);
    TNodeJobReport ArchiveFeatures(const NYson::TYsonString& archiveFeatures);

    void SetStatistics(const NYson::TYsonString& statistics);
    void SetStartTime(TInstant startTime);
    void SetFinishTime(TInstant finishTime);
    void SetJobCompetitionId(TJobId jobCompetitionId);
    void SetProbingJobCompetitionId(TJobId CompetitionId);
    void SetTaskName(const TString& taskName);
    void SetTtl(TDuration ttl);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
