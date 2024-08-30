#pragma once

#include "public.h"

#include <yt/yt/server/lib/misc/job_report.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

class TNodeJobReport
    : public TJobReport
{
public:
    TNodeJobReport OperationId(TOperationId operationId);
    TNodeJobReport JobId(TJobId jobId);
    TNodeJobReport Type(EJobType type);
    TNodeJobReport State(EJobState state);
    TNodeJobReport StartTime(TInstant startTime);
    TNodeJobReport FinishTime(TInstant finishTime);
    TNodeJobReport Error(const TError& error);
    TNodeJobReport InterruptionInfo(TJobInterruptionInfo interruptionInfo);
    TNodeJobReport Spec(const NControllerAgent::NProto::TJobSpec& spec);
    TNodeJobReport SpecVersion(i64 specVersion);
    TNodeJobReport Statistics(const NYson::TYsonString& statistics);
    TNodeJobReport Events(const TJobEvents& events);
    TNodeJobReport StderrSize(i64 stderrSize);
    TNodeJobReport Stderr(const TString& stderr);
    TNodeJobReport FailContext(const TString& failContext);
    TNodeJobReport Profile(const NJobAgent::TJobProfile& profile);
    TNodeJobReport CoreInfos(NControllerAgent::TCoreInfos coreInfos);
    TNodeJobReport ExecAttributes(const NYson::TYsonString& execAttributes);
    TNodeJobReport TreeId(TString treeId);
    TNodeJobReport MonitoringDescriptor(TString monitoringDescriptor);
    TNodeJobReport Address(std::optional<std::string> address);
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
