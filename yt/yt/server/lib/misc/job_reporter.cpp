#include "job_reporter.h"

#include "config.h"
#include "estimate_size_helpers.h"

#include <yt/yt/server/lib/job_agent/config.h>

#include <yt/yt/server/lib/misc/archive_reporter.h>
#include <yt/yt/server/lib/misc/job_report.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/controller_agent/helpers.h>

#include <yt/yt/ytlib/scheduler/helpers.h>
#include <yt/yt/ytlib/scheduler/records/job.record.h>
#include <yt/yt/ytlib/scheduler/records/job_fail_context.record.h>
#include <yt/yt/ytlib/scheduler/records/operation_id.record.h>
#include <yt/yt/ytlib/scheduler/records/job_profile.record.h>
#include <yt/yt/ytlib/scheduler/records/job_stderr.record.h>
#include <yt/yt/ytlib/scheduler/records/job_spec.record.h>

#include <yt/yt/client/api/connection.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/record_helpers.h>

#include <yt/yt/core/compression/codec.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/nonblocking_batcher.h>
#include <yt/yt/core/concurrency/async_semaphore.h>

#include <yt/yt/core/utilex/random.h>

namespace NYT::NServer {

using namespace NNodeTrackerClient;
using namespace NTransactionClient;
using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;
using namespace NControllerAgent;
using namespace NApi;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NProfiling;
using namespace NScheduler;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

static const TProfiler ReporterProfiler("/job_reporter");

////////////////////////////////////////////////////////////////////////////////

namespace {

class TJobRowlet
    : public IArchiveRowlet
{
public:
    TJobRowlet(
        TJobReport&& report,
        bool reportStatisticsLz4)
        : Report_(std::move(report))
        , ReportStatisticsLz4_(reportStatisticsLz4)
    { }

    size_t EstimateSize() const override
    {
        return NServer::EstimateSizes(
            Report_.OperationId().Underlying(),
            Report_.JobId().Underlying(),
            Report_.State(),
            Report_.StartTime(),
            Report_.FinishTime(),
            /*updateTime*/ TInstant::Now().MicroSeconds(),
            // TODO(aleksandr.gaev): Remove.
            Report_.Address(),
            Report_.Addresses(),
            Report_.StderrSize(),
            Report_.HasCompetitors(),
            Report_.HasProbingCompetitors(),
            Report_.TaskName(),
            Report_.TreeId(),
            Report_.MonitoringDescriptor(),
            Report_.Type(),
            Report_.Error(),
            Report_.InterruptionInfo(),
            Report_.Statistics(),
            Report_.Events(),
            Report_.Spec().has_value(),
            /*failContextSize*/ i64{0},
            Report_.CoreInfos(),
            Report_.JobCompetitionId().Underlying(),
            Report_.ProbingJobCompetitionId().Underlying(),
            Report_.ExecAttributes(),
            Report_.JobCookie(),
            Report_.ControllerState(),
            Report_.ArchiveFeatures(),
            Report_.Ttl(),
            Report_.OperationIncarnation());
    }

    TUnversionedOwningRow ToRow(int archiveVersion) const override
    {
        auto operationIdAsGuid = Report_.OperationId().Underlying();
        auto jobIdAsGuid = Report_.JobId().Underlying();
        NRecords::TJobPartial record{
            .Key = {
                .OperationIdHi = operationIdAsGuid.Parts64[0],
                .OperationIdLo = operationIdAsGuid.Parts64[1],
                .JobIdHi = jobIdAsGuid.Parts64[0],
                .JobIdLo = jobIdAsGuid.Parts64[1],
            },
            .TransientState = Report_.State(),
            .StartTime = Report_.StartTime(),
            .FinishTime = Report_.FinishTime(),
            .UpdateTime = TInstant::Now().MicroSeconds(),
            // TODO(aleksandr.gaev): Remove.
            .Address = Report_.Address(),
            .StderrSize = Report_.StderrSize(),
            .HasCompetitors = Report_.HasCompetitors(),
            .HasProbingCompetitors = Report_.HasProbingCompetitors(),
            .TaskName = Report_.TaskName(),
            .PoolTree = Report_.TreeId(),
            .MonitoringDescriptor = Report_.MonitoringDescriptor(),
        };

        if (Report_.Type()) {
            record.Type = FormatEnum(*Report_.Type());
        }
        if (Report_.Error()) {
            record.Error = TYsonString(*Report_.Error());
        }
        // COMPAT(omgronny)
        if (archiveVersion >= 50 && Report_.InterruptionInfo()) {
            record.InterruptionInfo = TYsonString(*Report_.InterruptionInfo());
        }
        if (Report_.Statistics()) {
            if (ReportStatisticsLz4_) {
                auto codec = NCompression::GetCodec(NCompression::ECodec::Lz4);
                record.StatisticsLz4 = ToString(codec->Compress(TSharedRef::FromString(*Report_.Statistics())));
            } else {
                record.Statistics = TYsonString(*Report_.Statistics());
            }
            record.BriefStatistics = BuildBriefStatistics(ConvertToNode(TYsonStringBuf(*Report_.Statistics())));
        }
        if (Report_.Events()) {
            record.Events = TYsonString(*Report_.Events());
        }
        if (Report_.Spec()) {
            record.HasSpec = Report_.Spec().has_value();
        }
        if (Report_.FailContext()) {
            record.FailContextSize = Report_.FailContext()->size();
        }
        if (Report_.CoreInfos()) {
            record.CoreInfos = ConvertToYsonString(*Report_.CoreInfos());
        }
        if (Report_.JobCompetitionId()) {
            record.JobCompetitionId = ToString(Report_.JobCompetitionId());
        }
        if (Report_.ProbingJobCompetitionId()) {
            record.ProbingJobCompetitionId = ToString(Report_.ProbingJobCompetitionId());
        }
        if (Report_.ExecAttributes()) {
            record.ExecAttributes = TYsonString(*Report_.ExecAttributes());
        }
        // COMPAT(renadeen)
        if (archiveVersion >= 47) {
            record.JobCookie = Report_.JobCookie();
        }
        // COMPAT(omgronny)
        if (archiveVersion >= 48) {
            record.ControllerState = Report_.ControllerState();
        }
        // COMPAT(omgronny)
        if (archiveVersion >= 51 && Report_.ArchiveFeatures()) {
            record.ArchiveFeatures = TYsonString(*Report_.ArchiveFeatures());
        }
        // COMPAT(omgronny)
        if (archiveVersion >= 53 && Report_.Ttl()) {
            record.Ttl = Report_.Ttl()->MilliSeconds();
        }

        // COMPAT(eshcherbin)
        if (archiveVersion >= 55 && Report_.OperationIncarnation()) {
            record.OperationIncarnation = Report_.OperationIncarnation();
        }

        // COMPAT(aleksandr.gaev)
        if (archiveVersion >= 56 && Report_.Addresses()) {
            record.Addresses = ConvertToYsonString(*Report_.Addresses());
        }

        return FromRecord(record);
    }

private:
    const TJobReport Report_;
    const bool ReportStatisticsLz4_;
};

////////////////////////////////////////////////////////////////////////////////

class TOperationIdRowlet
    : public IArchiveRowlet
{
public:
    explicit TOperationIdRowlet(TJobReport&& report)
        : Report_(std::move(report))
    { }

    size_t EstimateSize() const override
    {
        return ::NYT::NServer::EstimateSizes(
            Report_.OperationId().Underlying(),
            Report_.JobId().Underlying());
    }

    TUnversionedOwningRow ToRow(int /*archiveVersion*/) const override
    {
        auto operationIdAsGuid = Report_.OperationId().Underlying();
        auto jobIdAsGuid = Report_.JobId().Underlying();
        return FromRecord(NRecords::TOperationId{
            .Key = {
                .JobIdHi = jobIdAsGuid.Parts64[0],
                .JobIdLo = jobIdAsGuid.Parts64[1],
            },
            .OperationIdHi = operationIdAsGuid.Parts64[0],
            .OperationIdLo = operationIdAsGuid.Parts64[1],
        });
    }

private:
    const TJobReport Report_;
};

////////////////////////////////////////////////////////////////////////////////

class TJobSpecRowlet
    : public IArchiveRowlet
{
public:
    explicit TJobSpecRowlet(TJobReport&& report)
        : Report_(std::move(report))
    { }

    size_t EstimateSize() const override
    {
        return ::NYT::NServer::EstimateSizes(
            Report_.OperationId().Underlying(),
            Report_.JobId().Underlying(),
            Report_.Spec(),
            Report_.SpecVersion());
    }

    TUnversionedOwningRow ToRow(int /*archiveVersion*/) const override
    {
        auto jobIdAsGuid = Report_.JobId().Underlying();
        NRecords::TJobSpecPartial record{
            .Key = {
                .JobIdHi = jobIdAsGuid.Parts64[0],
                .JobIdLo = jobIdAsGuid.Parts64[1],
            },
            .Spec = Report_.Spec(),
            .SpecVersion = Report_.SpecVersion(),
        };
        if (Report_.Type()) {
            record.Type = FormatEnum(*Report_.Type());
        }
        return FromRecord(record);
    }

private:
    const TJobReport Report_;
};

////////////////////////////////////////////////////////////////////////////////

class TJobStderrRowlet
    : public IArchiveRowlet
{
public:
    explicit TJobStderrRowlet(TJobReport&& report)
        : Report_(std::move(report))
    { }

    size_t EstimateSize() const override
    {
        return ::NYT::NServer::EstimateSizes(
            Report_.OperationId().Underlying(),
            Report_.JobId().Underlying(),
            Report_.Stderr());
    }

    TUnversionedOwningRow ToRow(int /*archiveVersion*/) const override
    {
        auto operationIdAsGuid = Report_.OperationId().Underlying();
        auto jobIdAsGuid = Report_.JobId().Underlying();

        if (!Report_.Stderr()) {
            return {};
        }

        return FromRecord(NRecords::TJobStderr{
            .Key = {
                .OperationIdHi = operationIdAsGuid.Parts64[0],
                .OperationIdLo = operationIdAsGuid.Parts64[1],
                .JobIdHi = jobIdAsGuid.Parts64[0],
                .JobIdLo = jobIdAsGuid.Parts64[1],
            },
            .Stderr = *Report_.Stderr(),
        });
    }

private:
    const TJobReport Report_;
};

////////////////////////////////////////////////////////////////////////////////

class TJobFailContextRowlet
    : public IArchiveRowlet
{
public:
    explicit TJobFailContextRowlet(TJobReport&& report)
        : Report_(std::move(report))
    { }

    size_t EstimateSize() const override
    {
        return NServer::EstimateSizes(
            Report_.OperationId().Underlying(),
            Report_.JobId().Underlying(),
            Report_.FailContext());
    }

    TUnversionedOwningRow ToRow(int archiveVersion) const override
    {
        if (archiveVersion < 21 || !Report_.FailContext() || Report_.FailContext()->size() > MaxStringValueLength) {
            return {};
        }

        auto operationIdAsGuid = Report_.OperationId().Underlying();
        auto jobIdAsGuid = Report_.JobId().Underlying();

        return FromRecord(NRecords::TJobFailContext{
            .Key = {
                .OperationIdHi = operationIdAsGuid.Parts64[0],
                .OperationIdLo = operationIdAsGuid.Parts64[1],
                .JobIdHi = jobIdAsGuid.Parts64[0],
                .JobIdLo = jobIdAsGuid.Parts64[1],
            },
            .FailContext = *Report_.FailContext(),
        });
    }

private:
    const TJobReport Report_;
};

////////////////////////////////////////////////////////////////////////////////

class TJobProfileRowlet
    : public IArchiveRowlet
{
public:
    explicit TJobProfileRowlet(TJobReport&& report)
        : Report_(std::move(report))
    { }

    size_t EstimateSize() const override
    {
        return ::NYT::NServer::EstimateSizes(
            Report_.OperationId().Underlying(),
            Report_.JobId().Underlying(),
            Report_.Profile().value_or(NJobAgent::TJobProfile{}).Type,
            /*partIndex*/ int{0},
            Report_.Profile().value_or(NJobAgent::TJobProfile{}).Blob,
            Report_.Profile().value_or(NJobAgent::TJobProfile{}).ProfilingProbability);
    }

    TUnversionedOwningRow ToRow(int archiveVersion) const override
    {
        const auto& profile = Report_.Profile();

        if (archiveVersion < 27 || !profile) {
            return {};
        }

        auto operationIdAsGuid = Report_.OperationId().Underlying();
        auto jobIdAsGuid = Report_.JobId().Underlying();

        return FromRecord(NRecords::TJobProfile{
            .Key = {
                .OperationIdHi = operationIdAsGuid.Parts64[0],
                .OperationIdLo = operationIdAsGuid.Parts64[1],
                .JobIdHi = jobIdAsGuid.Parts64[0],
                .JobIdLo = jobIdAsGuid.Parts64[1],
                .PartIndex = 0,
            },
            .ProfileType = profile->Type,
            .ProfileBlob = profile->Blob,
            .ProfilingProbability = profile->ProfilingProbability,
        });
    }

private:
    const TJobReport Report_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TJobReporter::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TJobReporterConfigPtr reporterConfig,
        const NApi::NNative::IConnectionPtr& connection)
        : Client_(connection->CreateNativeClient(
            TClientOptions::FromUser(reporterConfig->User)))
        , Config_(std::move(reporterConfig))
        , JobHandler_(
            CreateArchiveReporter(
                Version_,
                Config_,
                Config_->JobHandler,
                NRecords::TJobDescriptor::Get()->GetNameTable(),
                "jobs",
                Client_,
                Reporter_->GetInvoker(),
                ReporterProfiler.WithTag("reporter_type", "jobs")))
        , OperationIdHandler_(
            CreateArchiveReporter(
                Version_,
                Config_,
                Config_->OperationIdHandler,
                NRecords::TOperationIdDescriptor::Get()->GetNameTable(),
                "operation_ids",
                Client_,
                Reporter_->GetInvoker(),
                ReporterProfiler.WithTag("reporter_type", "operation_ids")))
        , JobSpecHandler_(
            CreateArchiveReporter(
                Version_,
                Config_,
                Config_->JobSpecHandler,
                NRecords::TJobSpecDescriptor::Get()->GetNameTable(),
                "job_specs",
                Client_,
                Reporter_->GetInvoker(),
                ReporterProfiler.WithTag("reporter_type", "job_specs")))
        , JobStderrHandler_(
            CreateArchiveReporter(
                Version_,
                Config_,
                Config_->JobStderrHandler,
                NRecords::TJobStderrDescriptor::Get()->GetNameTable(),
                "stderrs",
                Client_,
                Reporter_->GetInvoker(),
                ReporterProfiler.WithTag("reporter_type", "stderrs")))
        , JobFailContextHandler_(
            CreateArchiveReporter(
                Version_,
                Config_,
                Config_->JobFailContextHandler,
                NRecords::TJobFailContextDescriptor::Get()->GetNameTable(),
                "fail_contexts",
                Client_,
                Reporter_->GetInvoker(),
                ReporterProfiler.WithTag("reporter_type", "fail_contexts")))
        , JobProfileHandler_(
            CreateArchiveReporter(
                Version_,
                Config_,
                Config_->JobProfileHandler,
                NRecords::TJobProfileDescriptor::Get()->GetNameTable(),
                "profiles",
                Client_,
                Reporter_->GetInvoker(),
                ReporterProfiler.WithTag("reporter_type", "profiles")))
    { }

    void HandleJobReport(TJobReport&& jobReport)
    {
        if (jobReport.Spec()) {
            JobSpecHandler_->Enqueue(std::make_unique<TJobSpecRowlet>(jobReport.ExtractSpec()));
        }
        if (jobReport.Stderr()) {
            JobStderrHandler_->Enqueue(std::make_unique<TJobStderrRowlet>(jobReport.ExtractStderr()));
        }
        if (jobReport.FailContext()) {
            JobFailContextHandler_->Enqueue(std::make_unique<TJobFailContextRowlet>(jobReport.ExtractFailContext()));
        }
        if (jobReport.Profile()) {
            JobProfileHandler_->Enqueue(std::make_unique<TJobProfileRowlet>(jobReport.ExtractProfile()));
        }
        if (!jobReport.IsEmpty()) {
            OperationIdHandler_->Enqueue(std::make_unique<TOperationIdRowlet>(jobReport.ExtractIds()));
            JobHandler_->Enqueue(std::make_unique<TJobRowlet>(
                std::move(jobReport),
                Config_->ReportStatisticsLz4));
        }
    }

    void SetEnabled(bool enable)
    {
        JobHandler_->SetEnabled(enable);
        OperationIdHandler_->SetEnabled(enable);
    }

    void SetOperationsArchiveVersion(int version)
    {
        Version_->Set(version);
    }

    int ExtractWriteFailuresCount()
    {
        return
            JobHandler_->ExtractWriteFailuresCount() +
            JobSpecHandler_->ExtractWriteFailuresCount() +
            JobStderrHandler_->ExtractWriteFailuresCount() +
            JobFailContextHandler_->ExtractWriteFailuresCount() +
            JobProfileHandler_->ExtractWriteFailuresCount();
    }

    bool IsQueueTooLarge()
    {
        return
            JobHandler_->IsQueueTooLarge() ||
            JobSpecHandler_->IsQueueTooLarge() ||
            JobStderrHandler_->IsQueueTooLarge() ||
            JobFailContextHandler_->IsQueueTooLarge() ||
            JobProfileHandler_->IsQueueTooLarge();
    }

    void UpdateConfig(const TJobReporterConfigPtr& config)
    {
        JobHandler_->SetEnabled(config->EnableJobReporter);
        JobSpecHandler_->SetEnabled(config->EnableJobSpecReporter);
        JobStderrHandler_->SetEnabled(config->EnableJobStderrReporter);
        JobProfileHandler_->SetEnabled(config->EnableJobProfileReporter);
        JobFailContextHandler_->SetEnabled(config->EnableJobFailContextReporter);
    }

    void OnDynamicConfigChanged(
        const TJobReporterConfigPtr& /*oldConfig*/,
        const TJobReporterConfigPtr& newConfig)
    {
        JobHandler_->OnConfigChanged(
            newConfig,
            newConfig->JobHandler);
        JobSpecHandler_->OnConfigChanged(
            newConfig,
            newConfig->JobSpecHandler);
        JobStderrHandler_->OnConfigChanged(
            newConfig,
            newConfig->JobStderrHandler);
        JobProfileHandler_->OnConfigChanged(
            newConfig,
            newConfig->JobProfileHandler);
        JobFailContextHandler_->OnConfigChanged(
            newConfig,
            newConfig->JobFailContextHandler);
    }

private:
    const NNative::IClientPtr Client_;
    const TJobReporterConfigPtr Config_;
    const TActionQueuePtr Reporter_ = New<TActionQueue>("JobReporter");
    const TArchiveVersionHolderPtr Version_ = New<TArchiveVersionHolder>();
    const IArchiveReporterPtr JobHandler_;
    const IArchiveReporterPtr OperationIdHandler_;
    const IArchiveReporterPtr JobSpecHandler_;
    const IArchiveReporterPtr JobStderrHandler_;
    const IArchiveReporterPtr JobFailContextHandler_;
    const IArchiveReporterPtr JobProfileHandler_;
};

////////////////////////////////////////////////////////////////////////////////

TJobReporter::TJobReporter(
    TJobReporterConfigPtr reporterConfig,
    const NApi::NNative::IConnectionPtr& connection)
    : Impl_(
        reporterConfig->Enabled
            ? New<TImpl>(std::move(reporterConfig), connection)
            : nullptr)
{ }

TJobReporter::~TJobReporter() = default;

void TJobReporter::HandleJobReport(TJobReport&& jobReport)
{
    if (Impl_) {
        Impl_->HandleJobReport(std::move(jobReport));
    }
}

void TJobReporter::SetOperationsArchiveVersion(int version)
{
    if (Impl_) {
        Impl_->SetOperationsArchiveVersion(version);
    }
}

int TJobReporter::ExtractWriteFailuresCount()
{
    if (Impl_) {
        return Impl_->ExtractWriteFailuresCount();
    }
    return 0;
}

bool TJobReporter::GetQueueIsTooLarge()
{
    if (Impl_) {
        return Impl_->IsQueueTooLarge();
    }
    return false;
}

void TJobReporter::UpdateConfig(const TJobReporterConfigPtr& config)
{
    if (Impl_) {
        Impl_->UpdateConfig(config);
    }
}

void TJobReporter::OnDynamicConfigChanged(
    const TJobReporterConfigPtr& oldConfig,
    const TJobReporterConfigPtr& newConfig)
{
    if (Impl_) {
        Impl_->OnDynamicConfigChanged(oldConfig, newConfig);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NServer
