#pragma once

#include <yt/yt/server/lib/misc/job_report.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

class TControllerJobReport
    : public NServer::TJobReport
{
public:
    TControllerJobReport OperationId(TOperationId operationId);
    TControllerJobReport JobId(TJobId jobId);
    TControllerJobReport HasCompetitors(bool hasCompetitors, EJobCompetitionType competitionType);
    TControllerJobReport JobCookie(ui64 jobCookie);
    // TODO(aleksandr.gaev): Remove.
    TControllerJobReport Address(std::optional<std::string> address);
    TControllerJobReport Addresses(std::optional<NNodeTrackerClient::TAddressMap> addresses);
    TControllerJobReport ControllerState(EJobState controllerState);
    TControllerJobReport Ttl(std::optional<TDuration> ttl);
    TControllerJobReport OperationIncarnation(std::string operationIncarnation);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
