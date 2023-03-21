#include "controller_agent_tracker.h"
#include "scheduler.h"
#include "scheduler_strategy.h"
#include "controller_agent.h"
#include "operation.h"
#include "node_manager.h"
#include "operation_controller_impl.h"
#include "scheduling_context.h"
#include "master_connector.h"
#include "bootstrap.h"

#include <yt/yt/server/lib/scheduler/config.h>
#include <yt/yt/server/lib/scheduler/helpers.h>
#include <yt/yt/server/lib/scheduler/job_metrics.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/lease_manager.h>

#include <yt/yt/core/rpc/response_keeper.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/ypath_proxy.h>

#include <yt/yt/build/build.h>

#include <util/string/join.h>

namespace NYT::NScheduler {

using namespace NConcurrency;
using namespace NRpc;
using namespace NYson;
using namespace NYTree;
using namespace NControllerAgent;
using namespace NTracing;

using NJobTrackerClient::TReleaseJobFlags;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerLogger;

////////////////////////////////////////////////////////////////////////////////

struct TOperationInfo
{
    TOperationId OperationId;
    TOperationJobMetrics JobMetrics;
    THashMap<EOperationAlertType, TError> AlertMap;
    TControllerRuntimeDataPtr ControllerRuntimeData;
    TYsonString SuspiciousJobsYson;
};

void FromProto(TOperationInfo* operationInfo, const NProto::TOperationInfo& operationInfoProto)
{
    operationInfo->OperationId = FromProto<TOperationId>(operationInfoProto.operation_id());
    operationInfo->JobMetrics = FromProto<TOperationJobMetrics>(operationInfoProto.job_metrics());
    if (operationInfoProto.has_alerts()) {
        THashMap<EOperationAlertType, TError> alertMap;
        for (const auto& protoAlert : operationInfoProto.alerts().alerts()) {
            alertMap[EOperationAlertType(protoAlert.type())] = FromProto<TError>(protoAlert.error());
        }
        operationInfo->AlertMap = std::move(alertMap);
    }

    if (operationInfoProto.has_suspicious_jobs()) {
        operationInfo->SuspiciousJobsYson = TYsonString(operationInfoProto.suspicious_jobs(), EYsonType::MapFragment);
    } else {
        operationInfo->SuspiciousJobsYson = TYsonString();
    }

    auto controllerData = New<TControllerRuntimeData>();

    TCompositeNeededResources neededResources;
    FromProto(&neededResources, operationInfoProto.composite_needed_resources());
    controllerData->SetNeededResources(std::move(neededResources));

    controllerData->MinNeededJobResources() = FromProto<TJobResourcesWithQuotaList>(operationInfoProto.min_needed_job_resources());
    operationInfo->ControllerRuntimeData = std::move(controllerData);
}

////////////////////////////////////////////////////////////////////////////////

void ProcessScheduleJobMailboxes(
    const TControllerAgentTracker::TCtxAgentScheduleJobHeartbeatPtr& context,
    const TControllerAgentPtr& agent,
    const TNodeManagerPtr& nodeManager,
    std::vector<std::vector<const NProto::TScheduleJobResponse*>>& groupedScheduleJobResponses)
{
    auto* request = &context->Request();
    auto* response = &context->Response();

    const auto Logger = SchedulerLogger
        .WithTag("RequestId: %v, IncarnationId: %v", context->GetRequestId(), request->agent_id());

    YT_LOG_DEBUG("Processing schedule job mailboxes");

    agent->GetScheduleJobResponsesInbox()->HandleIncoming(
        request->mutable_agent_to_scheduler_schedule_job_responses(),
        [&] (auto* protoEvent) {
            auto jobId = FromProto<TJobId>(protoEvent->job_id());
            auto shardId = nodeManager->GetNodeShardId(NodeIdFromJobId(jobId));
            groupedScheduleJobResponses[shardId].push_back(protoEvent);
        });
    agent->GetScheduleJobResponsesInbox()->ReportStatus(
        response->mutable_agent_to_scheduler_schedule_job_responses());

    agent->GetScheduleJobRequestsOutbox()->HandleStatus(
        request->scheduler_to_agent_schedule_job_requests());
    agent->GetScheduleJobRequestsOutbox()->BuildOutcoming(
        response->mutable_scheduler_to_agent_schedule_job_requests(),
        [] (auto* protoRequest, const auto& request) {
            ToProto(protoRequest, *request);
        });

    YT_LOG_DEBUG("Schedule job mailboxes processed");
}

void ProcessScheduleJobResponses(
    TControllerAgentTracker::TCtxAgentScheduleJobHeartbeatPtr context,
    const std::vector<TNodeShardPtr>& nodeShards,
    const std::vector<IInvokerPtr>& nodeShardInvokers,
    std::vector<std::vector<const NProto::TScheduleJobResponse*>> groupedScheduleJobResponses,
    const IInvokerPtr& dtorInvoker)
{
    auto Logger = SchedulerLogger
        .WithTag("RequestId: %v, IncarnationId: %v", context->GetRequestId(), context->Request().agent_id());

    YT_LOG_DEBUG("Processing schedule job responses");

    std::vector<TFuture<void>> futures;
    for (int shardId = 0; shardId < std::ssize(nodeShards); ++shardId) {
        futures.push_back(
            BIND([
                context,
                nodeShard = nodeShards[shardId],
                protoResponses = std::move(groupedScheduleJobResponses[shardId]),
                Logger = SchedulerLogger
            ] {
                for (const auto* protoResponse : protoResponses) {
                    auto operationId = FromProto<TOperationId>(protoResponse->operation_id());
                    auto jobId = FromProto<TJobId>(protoResponse->job_id());
                    auto controllerEpoch = protoResponse->controller_epoch();
                    auto expectedControllerEpoch = nodeShard->GetOperationControllerEpoch(operationId);

                    auto traceContext = TTraceContext::NewChildFromRpc(
                        protoResponse->tracing_ext(),
                        /*spanName*/ Format("ScheduleJob:%v", jobId),
                        context->GetRequestId(),
                        /*forceTracing*/ false);

                    {
                        TCurrentTraceContextGuard traceContextGuard(traceContext);

                        if (controllerEpoch != expectedControllerEpoch) {
                            YT_LOG_DEBUG(
                                "Received job schedule result with unexpected controller epoch; result is ignored "
                                "(OperationId: %v, JobId: %v, ControllerEpoch: %v, ExpectedControllerEpoch: %v)",
                                operationId,
                                jobId,
                                controllerEpoch,
                                expectedControllerEpoch);
                            continue;
                        }
                        if (nodeShard->IsOperationControllerTerminated(operationId)) {
                            YT_LOG_DEBUG(
                                "Received job schedule result for operation whose controller is terminated; "
                                "result is ignored (OperationId: %v, JobId: %v)",
                                operationId,
                                jobId);
                            continue;
                        }
                        nodeShard->EndScheduleJob(*protoResponse);
                    }
                }
            })
            .AsyncVia(nodeShardInvokers[shardId])
            .Run());
    }

    AllSet(std::move(futures))
        .Subscribe(
            BIND([context = std::move(context)] (const TError&) {
                auto request = std::move(context->Request());
                Y_UNUSED(request);
            })
            .Via(dtorInvoker));

    YT_LOG_DEBUG("Schedule job responses are processed");
}

////////////////////////////////////////////////////////////////////////////////

class TControllerAgentTracker::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TSchedulerConfigPtr config,
        TBootstrap* bootstrap)
        : SchedulerConfig_(std::move(config))
        , Config_(SchedulerConfig_->ControllerAgentTracker)
        , Bootstrap_(bootstrap)
        , MessageOffloadThreadPool_(CreateThreadPool(Config_->MessageOffloadThreadCount, "MsgOffload"))
        , ResponseKeeper_(CreateResponseKeeper(
            Config_->ResponseKeeper,
            Bootstrap_->GetControlInvoker(EControlQueue::AgentTracker),
            SchedulerLogger,
            SchedulerProfiler))
    { }

    void Initialize()
    {
        auto* masterConnector = Bootstrap_->GetScheduler()->GetMasterConnector();
        masterConnector->SubscribeMasterConnected(BIND_NO_PROPAGATE(
            &TImpl::OnMasterConnected,
            Unretained(this)));
        masterConnector->SubscribeMasterDisconnected(BIND_NO_PROPAGATE(
            &TImpl::OnMasterDisconnected,
            Unretained(this)));

        masterConnector->AddCommonWatcher(
            BIND_NO_PROPAGATE(&TImpl::RequestControllerAgentInstances, Unretained(this)),
            BIND_NO_PROPAGATE(&TImpl::HandleControllerAgentInstances, Unretained(this)));
    }

    std::vector<TControllerAgentPtr> GetAgents() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        std::vector<TControllerAgentPtr> result;
        result.reserve(IdToAgent_.size());
        for (const auto& [agentId, agent] : IdToAgent_) {
            result.push_back(agent);
        }
        return result;
    }

    IOperationControllerPtr CreateController(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return New<TOperationControllerImpl>(Bootstrap_, SchedulerConfig_, operation);
    }

    TControllerAgentPtr PickAgentForOperation(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto controllerAgentTag = operation->GetRuntimeParameters()->ControllerAgentTag;

        if (!AgentTagsFetched_ || TagsWithTooFewAgents_.contains(controllerAgentTag)) {
            YT_LOG_INFO(
                "Failed to pick agent since number of agent with matching tag is too low (OperationId: %v, ControllerAgentTag: %v)",
                operation->GetId(),
                controllerAgentTag);

            return nullptr;
        }

        int nonMathcingTagCount = 0;
        int nonRegisteredCount = 0;
        int missingMemoryStatisticsCount = 0;
        int notEnoughMemoryCount = 0;

        std::vector<TControllerAgentPtr> aliveAgents;
        for (const auto& [agentId, agent] : IdToAgent_) {
            if (agent->GetState() != EControllerAgentState::Registered) {
                ++nonRegisteredCount;
                continue;
            }
            if (!agent->GetTags().contains(controllerAgentTag)) {
                ++nonMathcingTagCount;
                continue;
            }
            aliveAgents.push_back(agent);
        }

        TControllerAgentPtr pickedAgent = nullptr;

        switch (Config_->AgentPickStrategy) {
            case EControllerAgentPickStrategy::Random: {
                std::vector<TControllerAgentPtr> agents;
                for (const auto& agent : aliveAgents) {
                    auto memoryStatistics = agent->GetMemoryStatistics();
                    if (memoryStatistics) {
                        auto minAgentAvailableMemory = std::max(
                            Config_->MinAgentAvailableMemory,
                            static_cast<i64>(Config_->MinAgentAvailableMemoryFraction * memoryStatistics->Limit));
                        if (memoryStatistics->Usage + minAgentAvailableMemory >= memoryStatistics->Limit) {
                            ++notEnoughMemoryCount;
                            continue;
                        }
                    }
                    agents.push_back(agent);
                }

                if (!agents.empty()) {
                    pickedAgent = agents[RandomNumber(agents.size())];
                }
                break;
            }
            case EControllerAgentPickStrategy::MemoryUsageBalanced: {
                double scoreSum = 0.0;
                for (const auto& agent : aliveAgents) {
                    auto memoryStatistics = agent->GetMemoryStatistics();
                    if (!memoryStatistics) {
                        ++missingMemoryStatisticsCount;
                        YT_LOG_WARNING("Controller agent skipped since it did not report memory information "
                            "and memory usage balanced pick strategy used (AgentId: %v)",
                            agent->GetId());
                        continue;
                    }

                    auto minAgentAvailableMemory = std::max(
                        Config_->MinAgentAvailableMemory,
                        static_cast<i64>(Config_->MinAgentAvailableMemoryFraction * memoryStatistics->Limit));
                    if (memoryStatistics->Usage + minAgentAvailableMemory >= memoryStatistics->Limit) {
                        ++notEnoughMemoryCount;
                        continue;
                    }

                    i64 freeMemory = std::max(static_cast<i64>(0), memoryStatistics->Limit - memoryStatistics->Usage);
                    double rawScore = static_cast<double>(freeMemory) / memoryStatistics->Limit;
                    double score = std::pow(rawScore, Config_->MemoryBalancedPickStrategyScorePower);

                    scoreSum += score;
                    if (RandomNumber<float>() <= static_cast<float>(score) / scoreSum) {
                        pickedAgent = agent;
                    }
                }
                break;
            }
            default: {
                YT_ABORT();
            }
        }

        if (!pickedAgent) {
            YT_LOG_INFO(
                "Failed to pick agent for operation ("
                "OperationId: %v, ControllerAgentTag: %v, "
                "NonMathcingTagCount: %v, NonRegisteredCount: %v, "
                "MissingMemoryStatisticsCount: %v, NotEnoughMemoryCount: %v)",
                operation->GetId(),
                controllerAgentTag,
                nonMathcingTagCount,
                nonRegisteredCount,
                missingMemoryStatisticsCount,
                notEnoughMemoryCount);
        }

        return pickedAgent;
    }

    void AssignOperationToAgent(
        const TOperationPtr& operation,
        const TControllerAgentPtr& agent)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_VERIFY(agent->Operations().insert(operation).second);
        operation->SetAgent(agent.Get());

        YT_LOG_INFO("Operation assigned to agent (AgentId: %v, Tags: %v, OperationId: %v)",
            agent->GetId(),
            agent->GetTags(),
            operation->GetId());
    }


    void HandleAgentFailure(
        const TControllerAgentPtr& agent,
        const TError& error)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_LOG_WARNING(error, "Agent failed; unregistering (AgentId: %v, IncarnationId: %v)",
            agent->GetId(),
            agent->GetIncarnationId());

        Bootstrap_->GetControlInvoker(EControlQueue::AgentTracker)->Invoke(
            BIND(&TImpl::UnregisterAgent, MakeStrong(this), agent));
    }


    void UnregisterOperationFromAgent(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto agent = operation->FindAgent();
        if (!agent) {
            return;
        }

        EraseOrCrash(agent->Operations(), operation);

        YT_LOG_DEBUG("Operation unregistered from agent (AgentId: %v, OperationId: %v)",
            agent->GetId(),
            operation->GetId());
    }

    const TControllerAgentTrackerConfigPtr& GetConfig() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return Config_;
    }

    void UpdateConfig(TSchedulerConfigPtr config)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        SchedulerConfig_ = std::move(config);
        Config_ = SchedulerConfig_->ControllerAgentTracker;
    }

    const IResponseKeeperPtr& GetResponseKeeper() const
    {
        return ResponseKeeper_;
    }

    TControllerAgentPtr FindAgent(const TAgentId& id)
    {
        auto it = IdToAgent_.find(id);
        return it == IdToAgent_.end() ? nullptr : it->second;
    }

    TControllerAgentPtr GetAgentOrThrow(const TAgentId& id)
    {
        auto agent = FindAgent(id);
        if (!agent) {
            THROW_ERROR_EXCEPTION("Agent %v is not registered",
                id);
        }
        return agent;
    }

    void ProcessAgentHandshake(const TCtxAgentHandshakePtr& context)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto* request = &context->Request();
        auto* response = &context->Response();

        const auto& agentId = request->agent_id();
        auto existingAgent = FindAgent(agentId);
        if (existingAgent) {
            auto state = existingAgent->GetState();
            if (state == EControllerAgentState::Registered || state == EControllerAgentState::WaitingForInitialHeartbeat) {
                YT_LOG_INFO("Kicking out agent due to id conflict (AgentId: %v, ExistingIncarnationId: %v)",
                    agentId,
                    existingAgent->GetIncarnationId());
                UnregisterAgent(existingAgent);
            }

            context->Reply(TError("Agent %Qv is in %Qlv state; please retry",
                agentId,
                state));
            return;
        }

        auto agent = [&] {
            auto addresses = FromProto<NNodeTrackerClient::TAddressMap>(request->agent_addresses());
            auto tags = FromProto<THashSet<TString>>(request->tags());
            // COMPAT(gritukan): Remove it when controller agents will be fresh enough.
            if (tags.empty()) {
                tags.insert(DefaultOperationTag);
            }

            auto address = NNodeTrackerClient::GetAddressOrThrow(addresses, Bootstrap_->GetLocalNetworks());
            auto channel = Bootstrap_->GetClient()->GetChannelFactory()->CreateChannel(address);

            YT_LOG_INFO("Registering agent (AgentId: %v, Addresses: %v, Tags: %v)",
                agentId,
                addresses,
                tags);

            auto agent = New<TControllerAgent>(
                agentId,
                std::move(addresses),
                std::move(tags),
                std::move(channel),
                Bootstrap_->GetControlInvoker(EControlQueue::AgentTracker),
                CreateSerializedInvoker(MessageOffloadThreadPool_->GetInvoker()));

            agent->SetState(EControllerAgentState::Registering);
            EmplaceOrCrash(IdToAgent_, agent->GetId(), agent);

            return agent;
        }();

        YT_LOG_INFO(
            "Starting agent incarnation transaction (AgentId: %v)",
            agentId);

        WaitFor(
            BIND(&TImpl::DoRegisterAgent, MakeStrong(this), agent)
                .AsyncVia(GetCancelableControlInvoker())
                .Run())
            .ThrowOnError();

        context->SetResponseInfo(
            "IncarnationId: %v",
            agent->GetIncarnationId());
        ToProto(response->mutable_incarnation_id(), agent->GetIncarnationId());
        response->set_config(ConvertToYsonString(SchedulerConfig_).ToString());
        response->set_scheduler_version(GetVersion());
        context->Reply();
    }

    void ProcessAgentHeartbeat(const TCtxAgentHeartbeatPtr& context)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& scheduler = Bootstrap_->GetScheduler();

        auto* request = &context->Request();
        auto* response = &context->Response();

        const auto& agentId = request->agent_id();
        auto incarnationId = FromProto<NControllerAgent::TIncarnationId>(request->incarnation_id());

        context->SetRequestInfo("AgentId: %v, IncarnationId: %v, OperationCount: %v, Memory: %v/%v",
            agentId,
            incarnationId,
            request->operations_size(),
            request->controller_memory_usage(),
            request->controller_memory_limit());

        auto agent = GetAgentOrThrow(agentId);
        if (agent->GetState() != EControllerAgentState::Registered && agent->GetState() != EControllerAgentState::WaitingForInitialHeartbeat) {
            context->Reply(TError("Agent %Qv is in %Qlv state",
                agentId,
                agent->GetState()));
            return;
        }
        if (incarnationId != agent->GetIncarnationId()) {
            context->Reply(TError("Wrong agent incarnation id: expected %v, got %v",
                agent->GetIncarnationId(),
                incarnationId));
            return;
        }
        if (agent->GetState() == EControllerAgentState::WaitingForInitialHeartbeat) {
            YT_LOG_INFO("Agent registration confirmed by heartbeat (AgentId: %v)", agentId);
            agent->SetState(EControllerAgentState::Registered);
        }

        TLeaseManager::RenewLease(agent->GetLease(), Config_->HeartbeatTimeout);

        SwitchTo(agent->GetCancelableInvoker());

        agent->OnHeartbeatReceived();

        std::vector<TOperationInfo> operationInfos;
        auto parseOperationsFuture = BIND([&operationsProto = request->operations(), &operationInfos = operationInfos] () {
                operationInfos.reserve(operationsProto.size());
                for (const auto& operationInfoProto : operationsProto) {
                    operationInfos.emplace_back(FromProto<TOperationInfo>(operationInfoProto));
                }
            })
            .AsyncVia(scheduler->GetBackgroundInvoker())
            .Run();
        WaitFor(parseOperationsFuture)
            .ThrowOnError();

        TOperationIdToOperationJobMetrics operationIdToOperationJobMetrics;
        for (const auto& operationInfo : operationInfos) {
            auto operationId = operationInfo.OperationId;
            auto operation = scheduler->FindOperation(operationId);
            if (!operation) {
                // TODO(eshcherbin): This is used for flap diagnostics. Remove when TestPoolMetricsPorto is fixed (YT-12207).
                THashMap<TString, i64> treeIdToOperationTotalTimeDelta;
                for (const auto& [treeId, metrics] : operationInfo.JobMetrics) {
                    treeIdToOperationTotalTimeDelta.emplace(treeId, metrics.Values()[EJobMetricName::TotalTime]);
                }

                YT_LOG_DEBUG("Unknown operation is running at agent; unregister requested (AgentId: %v, OperationId: %v, TreeIdToOperationTotalTimeDelta: %v)",
                    agent->GetId(),
                    operationId,
                    treeIdToOperationTotalTimeDelta);
                ToProto(response->add_operation_ids_to_unregister(), operationId);
                continue;
            }
            YT_VERIFY(operationIdToOperationJobMetrics.emplace(operationId, std::move(operationInfo.JobMetrics)).second);

            for (const auto& [alertType, alert] : operationInfo.AlertMap) {
                YT_UNUSED_FUTURE(scheduler->SetOperationAlert(operationId, alertType, alert));
            }

            if (operationInfo.SuspiciousJobsYson) {
                operation->SetSuspiciousJobs(operationInfo.SuspiciousJobsYson);
            }

            auto controllerRuntimeDataError = CheckControllerRuntimeData(operationInfo.ControllerRuntimeData);
            if (controllerRuntimeDataError.IsOK()) {
                operation->GetController()->SetControllerRuntimeData(operationInfo.ControllerRuntimeData);
                YT_UNUSED_FUTURE(scheduler->SetOperationAlert(operationId, EOperationAlertType::InvalidControllerRuntimeData, TError()));
            } else {
                auto error = TError("Controller agent reported invalid data for operation")
                    << TErrorAttribute("operation_id", operation->GetId())
                    << std::move(controllerRuntimeDataError);
                YT_UNUSED_FUTURE(scheduler->SetOperationAlert(operationId, EOperationAlertType::InvalidControllerRuntimeData, error));
            }
        }

        scheduler->GetStrategy()->ApplyJobMetricsDelta(std::move(operationIdToOperationJobMetrics));

        auto nodeManager = scheduler->GetNodeManager();
        std::vector<std::vector<const NProto::TAgentToSchedulerJobEvent*>> groupedJobEvents(nodeManager->GetNodeShardCount());

        // TODO(eshcherbin): Capturing by reference is dangerous, should fix this.
        RunInMessageOffloadInvoker(agent, [&] {
            agent->GetJobEventsInbox()->HandleIncoming(
                request->mutable_agent_to_scheduler_job_events(),
                [&] (auto* protoEvent) {
                    auto jobId = FromProto<TJobId>(protoEvent->job_id());
                    auto shardId = nodeManager->GetNodeShardId(NodeIdFromJobId(jobId));
                    groupedJobEvents[shardId].push_back(protoEvent);
                });
            agent->GetJobEventsInbox()->ReportStatus(
                response->mutable_agent_to_scheduler_job_events());

            agent->GetJobEventsOutbox()->HandleStatus(
                request->scheduler_to_agent_job_events());
            agent->GetJobEventsOutbox()->BuildOutcoming(
                response->mutable_scheduler_to_agent_job_events(),
                Config_->MaxMessageJobEventCount);

            agent->GetOperationEventsOutbox()->HandleStatus(
                request->scheduler_to_agent_operation_events());
            agent->GetOperationEventsOutbox()->BuildOutcoming(
                response->mutable_scheduler_to_agent_operation_events(),
                [] (auto* protoEvent, const auto& event) {
                    protoEvent->set_event_type(static_cast<int>(event.EventType));
                    ToProto(protoEvent->mutable_operation_id(), event.OperationId);
                });
        });

        agent->GetOperationEventsInbox()->HandleIncoming(
            request->mutable_agent_to_scheduler_operation_events(),
            [&] (auto* protoEvent) {
                auto eventType = static_cast<EAgentToSchedulerOperationEventType>(protoEvent->event_type());
                auto operationId = FromProto<TOperationId>(protoEvent->operation_id());
                auto controllerEpoch = protoEvent->controller_epoch();
                auto error = FromProto<TError>(protoEvent->error());
                auto operation = scheduler->FindOperation(operationId);
                if (!operation) {
                    return;
                }

                if (operation->ControllerEpoch() != controllerEpoch) {
                    YT_LOG_DEBUG("Received operation event with unexpected controller epoch; ignored "
                        "(OperationId: %v, ControllerEpoch: %v, EventType: %v)",
                        operationId,
                        controllerEpoch,
                        eventType);
                    return;
                }

                switch (eventType) {
                    case EAgentToSchedulerOperationEventType::Completed:
                        scheduler->OnOperationCompleted(operation);
                        break;
                    case EAgentToSchedulerOperationEventType::Suspended:
                        scheduler->OnOperationSuspended(operation, error);
                        break;
                    case EAgentToSchedulerOperationEventType::Aborted:
                        scheduler->OnOperationAborted(operation, error);
                        break;
                    case EAgentToSchedulerOperationEventType::Failed:
                        scheduler->OnOperationFailed(operation, error);
                        break;
                    case EAgentToSchedulerOperationEventType::BannedInTentativeTree: {
                        auto treeId = protoEvent->tentative_tree_id();
                        auto jobIds = FromProto<std::vector<TJobId>>(protoEvent->tentative_tree_job_ids());
                        scheduler->OnOperationBannedInTentativeTree(operation, treeId, jobIds);
                        break;
                    }
                    case EAgentToSchedulerOperationEventType::InitializationFinished: {
                        TErrorOr<TOperationControllerInitializeResult> resultOrError;
                        if (error.IsOK()) {
                            YT_ASSERT(protoEvent->has_initialize_result());

                            TOperationControllerInitializeResult result;
                            FromProto(
                                &result,
                                protoEvent->initialize_result(),
                                operationId,
                                Bootstrap_,
                                SchedulerConfig_->OperationTransactionPingPeriod);

                            resultOrError = std::move(result);
                        } else {
                            resultOrError = std::move(error);
                        }

                        operation->GetController()->OnInitializationFinished(resultOrError);
                        break;
                    }
                    case EAgentToSchedulerOperationEventType::PreparationFinished: {
                        TErrorOr<TOperationControllerPrepareResult> resultOrError;
                        if (error.IsOK()) {
                            YT_ASSERT(protoEvent->has_prepare_result());
                            resultOrError = FromProto<TOperationControllerPrepareResult>(protoEvent->prepare_result());
                        } else {
                            resultOrError = std::move(error);
                        }

                        operation->GetController()->OnPreparationFinished(resultOrError);
                        break;
                    }
                    case EAgentToSchedulerOperationEventType::MaterializationFinished: {
                        TErrorOr<TOperationControllerMaterializeResult> resultOrError;
                        if (error.IsOK()) {
                            YT_ASSERT(protoEvent->has_materialize_result());
                            resultOrError = FromProto<TOperationControllerMaterializeResult>(protoEvent->materialize_result());
                        } else {
                            resultOrError = std::move(error);
                        }

                        operation->GetController()->OnMaterializationFinished(resultOrError);
                        break;
                    }
                    case EAgentToSchedulerOperationEventType::RevivalFinished: {
                        TErrorOr<TOperationControllerReviveResult> resultOrError;
                        if (error.IsOK()) {
                            YT_ASSERT(protoEvent->has_revive_result());

                            TOperationControllerReviveResult result;
                            FromProto(
                                &result,
                                protoEvent->revive_result(),
                                operationId,
                                incarnationId,
                                operation->GetController()->GetPreemptionMode());

                            resultOrError = std::move(result);
                        } else {
                            resultOrError = std::move(error);
                        }

                        operation->GetController()->OnRevivalFinished(resultOrError);
                        break;
                    }
                    case EAgentToSchedulerOperationEventType::CommitFinished: {
                        TErrorOr<TOperationControllerCommitResult> resultOrError;
                        if (error.IsOK()) {
                            YT_ASSERT(protoEvent->has_commit_result());
                            resultOrError = FromProto<TOperationControllerCommitResult>(protoEvent->commit_result());
                        } else {
                            resultOrError = std::move(error);
                        }

                        operation->GetController()->OnCommitFinished(resultOrError);
                        break;
                    }
                    default:
                        YT_ABORT();
                }
            });

        agent->GetOperationEventsInbox()->ReportStatus(
            response->mutable_agent_to_scheduler_operation_events());

        if (request->has_controller_memory_limit()) {
            agent->SetMemoryStatistics(TControllerAgentMemoryStatistics{request->controller_memory_limit(), request->controller_memory_usage()});
        }

        if (request->exec_nodes_requested()) {
            RunInMessageOffloadInvoker(agent, [&] {
                const auto Logger = SchedulerLogger
                    .WithTag("RequestId: %v, IncarnationId: %v", context->GetRequestId(), request->agent_id());
                YT_LOG_DEBUG("Filling exec node descriptors");
                auto descriptors = scheduler->GetCachedExecNodeDescriptors();
                for (const auto& [_, descriptor] : *descriptors) {
                    ToProto(response->mutable_exec_nodes()->add_exec_nodes(), descriptor);
                }
                YT_LOG_DEBUG("Exec node descriptors filled");
            });
        }

        RunInMessageOffloadInvoker(agent, [
            context,
            nodeShards = nodeManager->GetNodeShards(),
            nodeShardInvokers = nodeManager->GetNodeShardInvokers(),
            groupedJobEvents = std::move(groupedJobEvents),
            dtorInvoker = MessageOffloadThreadPool_->GetInvoker()
        ] {
            const auto Logger = SchedulerLogger
                .WithTag("RequestId: %v, IncarnationId: %v", context->GetRequestId(), context->Request().agent_id());

            YT_LOG_DEBUG("Processing job events");

            for (int shardId = 0; shardId < std::ssize(nodeShards); ++shardId) {
                nodeShardInvokers[shardId]->Invoke(
                    BIND([
                        context,
                        nodeShard = nodeShards[shardId],
                        protoEvents = std::move(groupedJobEvents[shardId]),
                        Logger = SchedulerLogger
                    ] {
                        for (const auto* protoEvent : protoEvents) {
                            auto eventType = CheckedEnumCast<EAgentToSchedulerJobEventType>(protoEvent->event_type());
                            auto jobId = FromProto<TJobId>(protoEvent->job_id());
                            auto controllerEpoch = protoEvent->controller_epoch();
                            auto error = FromProto<TError>(protoEvent->error());
                            auto interruptReason = CheckedEnumCast<EInterruptReason>(protoEvent->interrupt_reason());

                            auto expectedControllerEpoch = nodeShard->GetJobControllerEpoch(jobId);

                            // NB(gritukan, ignat): If job is released, either it is stored into operation snapshot
                            // or operation is completed. In both cases controller epoch actually is not important.
                            bool shouldValidateEpoch = eventType != EAgentToSchedulerJobEventType::Released;

                            if (shouldValidateEpoch && (controllerEpoch != expectedControllerEpoch)) {
                                YT_LOG_DEBUG("Received job event with unexpected controller epoch; ignored "
                                             "(JobId: %v, EventType: %v, ControllerEpoch: %v, ExpectedControllerEpoch: %v)",
                                    jobId,
                                    eventType,
                                    controllerEpoch,
                                    expectedControllerEpoch);
                                continue;
                            }

                            switch (eventType) {
                                case EAgentToSchedulerJobEventType::Interrupted:
                                    nodeShard->InterruptJob(jobId, interruptReason);
                                    break;
                                case EAgentToSchedulerJobEventType::Aborted:
                                    nodeShard->AbortJob(jobId, error);
                                    break;
                                case EAgentToSchedulerJobEventType::Failed:
                                    nodeShard->FailJob(jobId);
                                    break;
                                case EAgentToSchedulerJobEventType::Released:
                                    nodeShard->ReleaseJob(jobId, FromProto<TReleaseJobFlags>(protoEvent->release_job_flags()));
                                    break;
                                default:
                                    YT_ABORT();
                            }
                        }
                    }));
            }
            YT_LOG_DEBUG("Job events are processed");
        });

        response->set_operation_archive_version(Bootstrap_->GetScheduler()->GetOperationArchiveVersion());

        context->SetResponseInfo("IncarnationId: %v", incarnationId);

        context->Reply();
    }


    void ProcessAgentScheduleJobHeartbeat(const TCtxAgentScheduleJobHeartbeatPtr& context)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto* request = &context->Request();
        const auto& agentId = request->agent_id();
        auto incarnationId = FromProto<NControllerAgent::TIncarnationId>(request->incarnation_id());

        context->SetRequestInfo("AgentId: %v, IncarnationId: %v", agentId, incarnationId);

        auto agent = GetAgentOrThrow(agentId);
        if (agent->GetState() != EControllerAgentState::Registered && agent->GetState() != EControllerAgentState::WaitingForInitialHeartbeat) {
            context->Reply(TError("Agent %Qv is in %Qlv state",
                agentId,
                agent->GetState()));
            return;
        }
        if (incarnationId != agent->GetIncarnationId()) {
            context->Reply(TError("Wrong agent incarnation id: expected %v, got %v",
                agent->GetIncarnationId(),
                incarnationId));
            return;
        }
        if (agent->GetState() == EControllerAgentState::WaitingForInitialHeartbeat) {
            YT_LOG_INFO("Agent registration confirmed by heartbeat");
            agent->SetState(EControllerAgentState::Registered);
        }

        TLeaseManager::RenewLease(agent->GetLease(), Config_->HeartbeatTimeout);

        SwitchTo(agent->GetCancelableInvoker());

        const auto& nodeManager = Bootstrap_->GetScheduler()->GetNodeManager();
        RunInMessageOffloadInvoker(agent, [
            context,
            agent,
            nodeManager,
            nodeShards = nodeManager->GetNodeShards(),
            nodeShardInvokers = nodeManager->GetNodeShardInvokers(),
            dtorInvoker = MessageOffloadThreadPool_->GetInvoker()
        ] {
            std::vector<std::vector<const NProto::TScheduleJobResponse*>> groupedScheduleJobResponses(nodeManager->GetNodeShardCount());
            ProcessScheduleJobMailboxes(context, agent, nodeManager, groupedScheduleJobResponses);
            ProcessScheduleJobResponses(
                context,
                nodeShards,
                nodeShardInvokers,
                std::move(groupedScheduleJobResponses),
                dtorInvoker);
        });

        context->SetResponseInfo("IncarnationId: %v", incarnationId);

        context->Reply();
    }

private:
    TSchedulerConfigPtr SchedulerConfig_;
    TControllerAgentTrackerConfigPtr Config_;
    TBootstrap* const Bootstrap_;
    const IThreadPoolPtr MessageOffloadThreadPool_;

    IResponseKeeperPtr ResponseKeeper_;

    THashMap<TAgentId, TControllerAgentPtr> IdToAgent_;

    THashSet<TString> TagsWithTooFewAgents_;
    bool AgentTagsFetched_{};

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    template <class F>
    void RunInMessageOffloadInvoker(const TControllerAgentPtr& agent, F func)
    {
        Y_UNUSED(WaitFor(BIND(func)
            .AsyncVia(agent->GetMessageOffloadInvoker())
            .Run()));
    }

    void DoRegisterAgent(TControllerAgentPtr agent)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        NApi::TTransactionStartOptions options;
        options.Timeout = Config_->IncarnationTransactionTimeout;
        if (Config_->IncarnationTransactionPingPeriod) {
            options.PingPeriod = Config_->IncarnationTransactionPingPeriod;
        }
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("title", Format("Controller agent incarnation for %v", agent->GetId()));
        options.Attributes = std::move(attributes);
        const auto& lockTransaction = Bootstrap_->GetScheduler()->GetMasterConnector()->GetLockTransaction();
        auto transactionOrError = WaitFor(lockTransaction->StartTransaction(NTransactionClient::ETransactionType::Master, options));

        if (!transactionOrError.IsOK()) {
            Bootstrap_->GetScheduler()->Disconnect(transactionOrError);
            THROW_ERROR TError{"Failed to start incarnation transaction"} << transactionOrError;
        }

        if (agent->GetState() != EControllerAgentState::Registering) {
            THROW_ERROR_EXCEPTION("Failed to complete agent registration (AgentState: %Qlv)", agent->GetState());
        }

        auto transaction = std::move(transactionOrError.Value());

        agent->SetIncarnationTransaction(transaction);

        const auto& nodeManager = Bootstrap_->GetScheduler()->GetNodeManager();
        nodeManager->RegisterAgentAtNodeShards(
            agent->GetId(),
            agent->GetAgentAddresses(),
            agent->GetIncarnationId());

        agent->SetLease(TLeaseManager::CreateLease(
            Config_->HeartbeatTimeout,
            BIND_NO_PROPAGATE(&TImpl::OnAgentHeartbeatTimeout, MakeWeak(this), MakeWeak(agent))
                .Via(GetCancelableControlInvoker())));

        transaction->SubscribeAborted(
            BIND_NO_PROPAGATE(&TImpl::OnAgentIncarnationTransactionAborted, MakeWeak(this), MakeWeak(agent))
                .Via(GetCancelableControlInvoker()));

        YT_LOG_INFO(
            "Agent incarnation transaction started (AgentId: %v, IncarnationId: %v)",
            agent->GetId(),
            agent->GetIncarnationId());

        agent->SetState(EControllerAgentState::WaitingForInitialHeartbeat);
    }

    void UnregisterAgent(const TControllerAgentPtr& agent)
    {
        if (agent->GetState() == EControllerAgentState::Unregistering ||
            agent->GetState() == EControllerAgentState::Unregistered)
        {
            return;
        }

        YT_LOG_INFO("Notify operations that agent is going to unregister (AgentId: %v, IncarnationId: %v)",
            agent->GetId(),
            agent->GetIncarnationId());

        YT_VERIFY(agent->GetState() == EControllerAgentState::Registered || agent->GetState() == EControllerAgentState::WaitingForInitialHeartbeat);

        const auto& scheduler = Bootstrap_->GetScheduler();
        for (const auto& operation : agent->Operations()) {
            scheduler->OnOperationAgentUnregistered(operation);
        }

        TerminateAgent(agent);

        YT_LOG_INFO("Aborting agent incarnation transaction (AgentId: %v, IncarnationId: %v)",
            agent->GetId(),
            agent->GetIncarnationId());

        agent->SetState(EControllerAgentState::Unregistering);
        agent->GetIncarnationTransaction()->Abort()
            .Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TError& error) {
                VERIFY_THREAD_AFFINITY(ControlThread);

                if (!error.IsOK()) {
                    Bootstrap_->GetScheduler()->Disconnect(error);
                    return;
                }

                if (agent->GetState() != EControllerAgentState::Unregistering) {
                    return;
                }

                YT_LOG_INFO("Agent unregistered (AgentId: %v, IncarnationId: %v)",
                    agent->GetId(),
                    agent->GetIncarnationId());

                agent->SetState(EControllerAgentState::Unregistered);
                EraseOrCrash(IdToAgent_, agent->GetId());
            })
            .Via(GetCancelableControlInvoker()));

        scheduler->GetNodeManager()->UnregisterAgentFromNodeShards(agent->GetId());
    }

    void TerminateAgent(const TControllerAgentPtr& agent)
    {
        TLeaseManager::CloseLease(agent->GetLease());
        agent->SetLease(TLease());

        TError error("Agent disconnected");
        agent->GetChannel()->Terminate(error);
        agent->Cancel(error);
    }

    void OnAgentHeartbeatTimeout(const TWeakPtr<TControllerAgent>& weakAgent)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto agent = weakAgent.Lock();
        if (!agent) {
            return;
        }

        YT_LOG_WARNING("Agent heartbeat timeout; unregistering (AgentId: %v, IncarnationId: %v)",
            agent->GetId(),
            agent->GetIncarnationId());

        UnregisterAgent(agent);
    }

    void OnAgentIncarnationTransactionAborted(const TWeakPtr<TControllerAgent>& weakAgent, const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto agent = weakAgent.Lock();
        if (!agent) {
            return;
        }

        YT_LOG_WARNING(error, "Agent incarnation transaction aborted; unregistering (AgentId: %v, IncarnationId: %v)",
            agent->GetId(),
            agent->GetIncarnationId());

        UnregisterAgent(agent);
    }

    void RequestControllerAgentInstances(const NObjectClient::TObjectServiceProxy::TReqExecuteBatchPtr& batchReq) const
    {
        YT_LOG_INFO("Requesting controller agents list");

        auto req = TYPathProxy::Get("//sys/controller_agents/instances");
        req->mutable_attributes()->add_keys("tags");
        batchReq->AddRequest(req, "get_agent_list");
    }

    void HandleControllerAgentInstances(const NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr& batchRsp)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_agent_list");
        if (!rspOrError.IsOK()) {
            THROW_ERROR_EXCEPTION(rspOrError.Wrap(EErrorCode::WatcherHandlerFailed, "Error getting controller agent list"));
        }

        const auto& rsp = rspOrError.Value();

        auto tagToAgentIds = [&] {
            THashMap<TString, std::vector<TString>> tagToAgentIds;

            auto children = ConvertToNode(TYsonString(rsp->value()))->AsMap()->GetChildren();
            for (auto& [agentId, node] : children) {
                const auto tags = [&node{node}, &agentId{agentId}] () -> THashSet<TString> {
                    try {
                        const auto children = node->Attributes().ToMap()->GetChildOrThrow("tags")->AsList()->GetChildren();
                        THashSet<TString> tags;
                        tags.reserve(std::size(children));

                        for (const auto& tagNode : children) {
                            tags.insert(tagNode->AsString()->GetValue());
                        }
                        return tags;
                    } catch (const std::exception& ex) {
                        YT_LOG_WARNING(ex, "Cannot parse tags of agent %v", agentId);
                        return {};
                    }
                }();

                tagToAgentIds.reserve(std::size(tags));
                for (auto& tag : tags) {
                    tagToAgentIds[std::move(tag)].push_back(agentId);
                }
            }

            return tagToAgentIds;
        }();

        std::vector<TError> errors;
        THashSet<TString> tagsWithTooFewAgents;
        for (const auto& [tag, thresholds] : Config_->TagToAliveControllerAgentThresholds) {
            std::vector<TStringBuf> aliveAgentWithCurrentTag;
            aliveAgentWithCurrentTag.reserve(32);

            for (const auto& [agentId, agent] : IdToAgent_) {
                if (agent->GetTags().contains(tag)) {
                    aliveAgentWithCurrentTag.push_back(agentId);
                }
            }

            const auto agentsWithTag = std::move(tagToAgentIds[tag]);
            const auto agentWithTagCount = std::ssize(agentsWithTag);
            const auto aliveAgentWithTagCount = std::ssize(aliveAgentWithCurrentTag);
            if (aliveAgentWithTagCount < thresholds.Absolute ||
                (agentWithTagCount &&
                    1.0 * aliveAgentWithTagCount / agentWithTagCount < thresholds.Relative)) {

                tagsWithTooFewAgents.insert(tag);
                errors.push_back(
                    TError{"Too few agents matching tag"}
                        << TErrorAttribute{"controller_agent_tag", tag}
                        << TErrorAttribute{"alive_agents", aliveAgentWithCurrentTag}
                        << TErrorAttribute{"agents", agentsWithTag}
                        << TErrorAttribute{"min_alive_agent_count", thresholds.Absolute}
                        << TErrorAttribute{"min_alive_agent_ratio", thresholds.Relative});
            }
        }

        TagsWithTooFewAgents_ = std::move(tagsWithTooFewAgents);
        AgentTagsFetched_ = true;

        TError error;
        if (!errors.empty()) {
            error = TError{EErrorCode::WatcherHandlerFailed, "Too few matching agents"} << std::move(errors);
            YT_LOG_WARNING(error);
        }
        Bootstrap_->GetScheduler()->GetMasterConnector()->SetSchedulerAlert(
            ESchedulerAlertType::TooFewControllerAgentsAlive, error);
    }


    void DoCleanup()
    {
        for (const auto& [agentId, agent] : IdToAgent_) {
            TerminateAgent(agent);
            agent->SetState(EControllerAgentState::Unregistered);
        }
        IdToAgent_.clear();
    }

    void OnMasterConnected()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        DoCleanup();

        ResponseKeeper_->Start();

        YT_LOG_INFO("Master connected for controller agent tracker");
    }

    void OnMasterDisconnected()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        ResponseKeeper_->Stop();

        DoCleanup();

        YT_LOG_INFO("Master disconnected for controller agent tracker");
    }

    const IInvokerPtr& GetCancelableControlInvoker()
    {
        return Bootstrap_
            ->GetScheduler()
            ->GetMasterConnector()
            ->GetCancelableControlInvoker(EControlQueue::AgentTracker);
    }
};

////////////////////////////////////////////////////////////////////////////////

TControllerAgentTracker::TControllerAgentTracker(
    TSchedulerConfigPtr config,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(std::move(config), bootstrap))
{ }

TControllerAgentTracker::~TControllerAgentTracker() = default;

void TControllerAgentTracker::Initialize()
{
    Impl_->Initialize();
}

std::vector<TControllerAgentPtr> TControllerAgentTracker::GetAgents() const
{
    return Impl_->GetAgents();
}

IOperationControllerPtr TControllerAgentTracker::CreateController(const TOperationPtr& operation)
{
    return Impl_->CreateController(operation);
}

TControllerAgentPtr TControllerAgentTracker::PickAgentForOperation(const TOperationPtr& operation)
{
    return Impl_->PickAgentForOperation(operation);
}

void TControllerAgentTracker::AssignOperationToAgent(
    const TOperationPtr& operation,
    const TControllerAgentPtr& agent)
{
    Impl_->AssignOperationToAgent(operation, agent);
}

void TControllerAgentTracker::HandleAgentFailure(
    const TControllerAgentPtr& agent,
    const TError& error)
{
    Impl_->HandleAgentFailure(agent, error);
}

void TControllerAgentTracker::UnregisterOperationFromAgent(const TOperationPtr& operation)
{
    Impl_->UnregisterOperationFromAgent(operation);
}

const TControllerAgentTrackerConfigPtr& TControllerAgentTracker::GetConfig() const
{
    return Impl_->GetConfig();
}

void TControllerAgentTracker::UpdateConfig(TSchedulerConfigPtr config)
{
    Impl_->UpdateConfig(std::move(config));
}

const IResponseKeeperPtr& TControllerAgentTracker::GetResponseKeeper() const
{
    return Impl_->GetResponseKeeper();
}

void TControllerAgentTracker::ProcessAgentHeartbeat(const TCtxAgentHeartbeatPtr& context)
{
    Impl_->ProcessAgentHeartbeat(context);
}

void TControllerAgentTracker::ProcessAgentScheduleJobHeartbeat(const TCtxAgentScheduleJobHeartbeatPtr& context)
{
    Impl_->ProcessAgentScheduleJobHeartbeat(context);
}

void TControllerAgentTracker::ProcessAgentHandshake(const TCtxAgentHandshakePtr& context)
{
    Impl_->ProcessAgentHandshake(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
