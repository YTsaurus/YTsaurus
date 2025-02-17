#include "job_report.h"

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

TControllerJobReport TControllerJobReport::OperationId(TOperationId operationId)
{
    OperationId_ = operationId;
    return std::move(*this);
}

TControllerJobReport TControllerJobReport::JobId(TJobId jobId)
{
    JobId_ = jobId;
    return std::move(*this);
}

TControllerJobReport TControllerJobReport::HasCompetitors(bool hasCompetitors, EJobCompetitionType competitionType)
{
    switch (competitionType) {
        case EJobCompetitionType::Speculative:
            HasCompetitors_ = hasCompetitors;
            break;
        case EJobCompetitionType::Probing:
            HasProbingCompetitors_ = hasCompetitors;
            break;
        case EJobCompetitionType::Experiment:
            break;
        default:
            YT_ABORT();
    }
    return std::move(*this);
}

TControllerJobReport TControllerJobReport::JobCookie(ui64 jobCookie)
{
    JobCookie_ = jobCookie;
    return std::move(*this);
}

// TODO(aleksandr.gaev): Remove.
TControllerJobReport TControllerJobReport::Address(std::optional<std::string> address)
{
    Address_ = address;
    return std::move(*this);
}

TControllerJobReport TControllerJobReport::Addresses(std::optional<NNodeTrackerClient::TAddressMap> addresses)
{
    Addresses_ = addresses;
    return std::move(*this);
}

TControllerJobReport TControllerJobReport::ControllerState(EJobState controllerState)
{
    ControllerState_ = FormatEnum(controllerState);
    return std::move(*this);
}

TControllerJobReport TControllerJobReport::Ttl(std::optional<TDuration> ttl)
{
    Ttl_ = ttl;
    return std::move(*this);
}

TControllerJobReport TControllerJobReport::OperationIncarnation(std::string incarnation)
{
    OperationIncarnation_ = std::move(incarnation);
    return std::move(*this);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
