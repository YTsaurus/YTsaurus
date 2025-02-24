#include "operation_controller_impl.h"

#include "bootstrap.h"
#include "controller_agent_tracker.h"
#include "helpers.h"
#include "node_manager.h"
#include "private.h"
#include "scheduler.h"

#include <yt/yt/server/lib/exec_node/public.h>

#include <yt/yt/server/lib/controller_agent/helpers.h>

#include <yt/yt/server/lib/scheduler/config.h>
#include <yt/yt/server/lib/scheduler/experiments.h>
#include <yt/yt/server/lib/scheduler/helpers.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

namespace NYT::NScheduler {

using namespace NConcurrency;
using namespace NRpc;
using namespace NYson;
using namespace NYTree;
using namespace NControllerAgent;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

namespace {

bool IsAgentFailureError(const TError& error)
{
    if (IsChannelFailureError(error)) {
        return true;
    }
    auto code = error.GetCode();
    return code == NYT::EErrorCode::Timeout;
}

bool IsAgentDisconnectionError(const TError& error)
{
    return error.FindMatching(NObjectClient::EErrorCode::PrerequisiteCheckFailed).has_value();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TOperationControllerImpl::TOperationControllerImpl(
    TBootstrap* bootstrap,
    TSchedulerConfigPtr config,
    const TOperationPtr& operation)
    : Bootstrap_(bootstrap)
    , Config_(std::move(config))
    , OperationId_(operation->GetId())
    , PreemptionMode_(operation->Spec()->PreemptionMode)
    , Logger(SchedulerLogger().WithTag("OperationId: %v", OperationId_))
    , ControllerRuntimeData_(New<TControllerRuntimeData>())
{ }


void TOperationControllerImpl::AssignAgent(const TControllerAgentPtr& agent, TControllerEpoch epoch)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    auto guard = Guard(SpinLock_);

    YT_VERIFY(!IncarnationId_);
    IncarnationId_ = agent->GetIncarnationId();
    Agent_ = agent;

    ControllerAgentTrackerProxy_ = std::make_unique<TControllerAgentServiceProxy>(agent->GetChannel());

    Epoch_.store(epoch);

    AllocationEventsOutbox_ = agent->GetAllocationEventsOutbox();
    OperationEventsOutbox_ = agent->GetOperationEventsOutbox();
    ScheduleAllocationRequestsOutbox_ = agent->GetScheduleAllocationRequestsOutbox();
}

bool TOperationControllerImpl::RevokeAgent()
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    auto guard = Guard(SpinLock_);

    if (!IncarnationId_) {
        return false;
    }

    PendingInitializeResult_ = TPromise<TOperationControllerInitializeResult>();
    PendingPrepareResult_ = TPromise<TOperationControllerPrepareResult>();
    PendingMaterializeResult_ = TPromise<TOperationControllerMaterializeResult>();
    PendingReviveResult_ = TPromise<TOperationControllerReviveResult>();
    PendingCommitResult_ = TPromise<TOperationControllerCommitResult>();

    ControllerAgentTrackerProxy_.reset();

    IncarnationId_ = {};
    Agent_.Reset();

    YT_LOG_INFO("Agent revoked for operation");

    return true;
}

TControllerAgentPtr TOperationControllerImpl::FindAgent() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return Agent_.Lock();
}

TControllerEpoch TOperationControllerImpl::GetEpoch() const
{
    return Epoch_.load();
}

TFuture<TOperationControllerInitializeResult> TOperationControllerImpl::Initialize(
    const std::optional<TOperationTransactions>& transactions,
    const INodePtr& cumulativeSpecPatch)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);
    YT_VERIFY(IncarnationId_);

    auto agent = Agent_.Lock();
    if (!agent) {
        throw TFiberCanceledException();
    }

    YT_VERIFY(!PendingInitializeResult_);
    PendingInitializeResult_ = NewPromise<TOperationControllerInitializeResult>();

    auto req = ControllerAgentTrackerProxy_->InitializeOperation();
    ToProto(req->mutable_operation_id(), OperationId_);
    req->SetTimeout(Config_->ControllerAgentTracker->LightRpcTimeout);
    if (transactions) {
        req->set_clean(false);
        ToProto(req->mutable_transaction_ids(), *transactions);
    } else {
        req->set_clean(true);
    }
    if (cumulativeSpecPatch) {
        YT_VERIFY(transactions);
        ToProto(req->mutable_cumulative_spec_patch(), ConvertToYsonString(cumulativeSpecPatch).ToString());
    }
    InvokeAgent<TControllerAgentServiceProxy::TRspInitializeOperation>(req).Subscribe(
        BIND([
            this,
            this_ = MakeStrong(this)
        ] (const TErrorOr<TControllerAgentServiceProxy::TRspInitializeOperationPtr>& rspOrError) {
            if (!IncarnationId_) {
                // Operation agent was revoked.
                return;
            }
            if (!rspOrError.IsOK()) {
                OnInitializationFinished(static_cast<TError>(rspOrError));
                return;
            }

            auto rsp = rspOrError.Value();
            if (rsp->has_result()) {
                TOperationControllerInitializeResult result;
                FromProto(
                    &result,
                    rsp->result(),
                    OperationId_,
                    Bootstrap_,
                    Config_->OperationTransactionPingPeriod);

                OnInitializationFinished(result);
            }
        })
        .Via(agent->GetCancelableControlInvoker()));

    return PendingInitializeResult_;
}

TFuture<TOperationControllerPrepareResult> TOperationControllerImpl::Prepare()
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);
    YT_VERIFY(IncarnationId_);

    auto agent = Agent_.Lock();
    if (!agent) {
        throw TFiberCanceledException();
    }

    YT_VERIFY(!PendingPrepareResult_);
    PendingPrepareResult_ = NewPromise<TOperationControllerPrepareResult>();

    auto req = ControllerAgentTrackerProxy_->PrepareOperation();
    ToProto(req->mutable_operation_id(), OperationId_);
    req->SetTimeout(Config_->ControllerAgentTracker->LightRpcTimeout);
    InvokeAgent<TControllerAgentServiceProxy::TRspPrepareOperation>(req).Subscribe(
        BIND([
            this,
            this_ = MakeStrong(this)
        ] (const TErrorOr<TControllerAgentServiceProxy::TRspPrepareOperationPtr>& rspOrError) {
            if (!IncarnationId_) {
                // Operation agent was revoked.
                return;
            }
            if (!rspOrError.IsOK()) {
                OnPreparationFinished(static_cast<TError>(rspOrError));
                return;
            }

            auto rsp = rspOrError.Value();
            if (rsp->has_result()) {
                OnPreparationFinished(FromProto<TOperationControllerPrepareResult>(rsp->result()));
            }
        })
        .Via(agent->GetCancelableControlInvoker()));

    return PendingPrepareResult_;
}

TFuture<TOperationControllerMaterializeResult> TOperationControllerImpl::Materialize()
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);
    YT_VERIFY(IncarnationId_);

    auto agent = Agent_.Lock();
    if (!agent) {
        throw TFiberCanceledException();
    }

    YT_VERIFY(!PendingMaterializeResult_);
    PendingMaterializeResult_ = NewPromise<TOperationControllerMaterializeResult>();

    auto req = ControllerAgentTrackerProxy_->MaterializeOperation();
    req->SetTimeout(Config_->ControllerAgentTracker->LightRpcTimeout);
    ToProto(req->mutable_operation_id(), OperationId_);
    InvokeAgent<TControllerAgentServiceProxy::TRspMaterializeOperation>(req).Subscribe(
        BIND([
            this,
            this_ = MakeStrong(this)
        ] (const TErrorOr<TControllerAgentServiceProxy::TRspMaterializeOperationPtr>& rspOrError) {
            if (!IncarnationId_) {
                // Operation agent was revoked.
                return;
            }
            if (!rspOrError.IsOK()) {
                OnMaterializationFinished(static_cast<TError>(rspOrError));
                return;
            }

            auto rsp = rspOrError.Value();
            if (rsp->has_result()) {
                OnMaterializationFinished(FromProto<TOperationControllerMaterializeResult>(rsp->result()));
            }
        })
        .Via(agent->GetCancelableControlInvoker()));

    return PendingMaterializeResult_;
}

TFuture<TOperationControllerReviveResult> TOperationControllerImpl::Revive()
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);
    YT_VERIFY(IncarnationId_);

    auto agent = Agent_.Lock();
    if (!agent) {
        throw TFiberCanceledException();
    }

    YT_VERIFY(!PendingReviveResult_);
    PendingReviveResult_ = NewPromise<TOperationControllerReviveResult>();

    auto req = ControllerAgentTrackerProxy_->ReviveOperation();
    req->SetTimeout(Config_->ControllerAgentTracker->LightRpcTimeout);
    ToProto(req->mutable_operation_id(), OperationId_);
    InvokeAgent<TControllerAgentServiceProxy::TRspReviveOperation>(req).Subscribe(
        BIND([
            this,
            this_ = MakeStrong(this),
            operationId = OperationId_,
            incarnationId = agent->GetIncarnationId(),
            preemptionMode = PreemptionMode_
        ] (const TErrorOr<TControllerAgentServiceProxy::TRspReviveOperationPtr>& rspOrError) {
            if (!IncarnationId_) {
                // Operation agent was revoked.
                return;
            }
            if (!rspOrError.IsOK()) {
                OnRevivalFinished(static_cast<TError>(rspOrError));
                return;
            }

            auto rsp = rspOrError.Value();
            if (rsp->has_result()) {
                TOperationControllerReviveResult result;
                FromProto(
                    &result,
                    rsp->result(),
                    operationId,
                    incarnationId,
                    preemptionMode);

                OnRevivalFinished(result);
            }
        })
        .Via(agent->GetCancelableControlInvoker()));

    return PendingReviveResult_;
}

TFuture<TOperationControllerCommitResult> TOperationControllerImpl::Commit()
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);
    YT_VERIFY(IncarnationId_);

    auto agent = Agent_.Lock();
    if (!agent) {
        throw TFiberCanceledException();
    }

    YT_VERIFY(!PendingCommitResult_);
    PendingCommitResult_ = NewPromise<TOperationControllerCommitResult>();

    auto req = ControllerAgentTrackerProxy_->CommitOperation();
    ToProto(req->mutable_operation_id(), OperationId_);
    req->SetTimeout(Config_->ControllerAgentTracker->LightRpcTimeout);
    InvokeAgent<TControllerAgentServiceProxy::TRspCommitOperation>(req).Subscribe(
        BIND([
            this,
            this_ = MakeStrong(this)
        ] (const TErrorOr<TControllerAgentServiceProxy::TRspCommitOperationPtr>& rspOrError) {
            if (!IncarnationId_) {
                // Operation agent was revoked.
                return;
            }
            if (!rspOrError.IsOK()) {
                OnCommitFinished(static_cast<TError>(rspOrError));
                return;
            }

            auto rsp = rspOrError.Value();
            if (rsp->has_result()) {
                OnCommitFinished(FromProto<TOperationControllerCommitResult>(rsp->result()));
            }
        })
        .Via(agent->GetCancelableControlInvoker()));

    return PendingCommitResult_;
}

TFuture<void> TOperationControllerImpl::Terminate(EOperationState finalState)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    YT_LOG_INFO("Terminating operation controller");

    if (!IncarnationId_) {
        YT_LOG_INFO("Operation has no agent assigned; terminate request ignored");
        return VoidFuture;
    }

    YT_VERIFY(finalState == EOperationState::Aborted || finalState == EOperationState::Failed);
    EControllerState controllerFinalState = finalState == EOperationState::Aborted
        ? EControllerState::Aborted
        : EControllerState::Failed;

    auto req = ControllerAgentTrackerProxy_->TerminateOperation();
    ToProto(req->mutable_operation_id(), OperationId_);
    req->set_controller_final_state(ToProto(controllerFinalState));
    req->SetTimeout(Config_->ControllerAgentTracker->HeavyRpcTimeout);
    return InvokeAgent<TControllerAgentServiceProxy::TRspTerminateOperation>(req).As<void>();
}

TFuture<void> TOperationControllerImpl::Complete()
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);
    YT_VERIFY(IncarnationId_);

    auto req = ControllerAgentTrackerProxy_->CompleteOperation();
    ToProto(req->mutable_operation_id(), OperationId_);
    req->SetTimeout(Config_->ControllerAgentTracker->HeavyRpcTimeout);
    return InvokeAgent<TControllerAgentServiceProxy::TRspCompleteOperation>(req).As<void>();
}

TFuture<void> TOperationControllerImpl::Register(const TOperationPtr& operation)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    auto agent = Agent_.Lock();
    // Called synchronously just after assigning agent.
    YT_VERIFY(agent);

    YT_LOG_DEBUG("Registering operation at agent (AgentId: %v, OperationId: %v)",
        agent->GetId(),
        operation->GetId());

    TControllerAgentServiceProxy proxy(agent->GetChannel());
    auto req = proxy.RegisterOperation();
    req->SetTimeout(Config_->ControllerAgentTracker->HeavyRpcTimeout);

    auto* descriptor = req->mutable_operation_descriptor();
    ToProto(descriptor->mutable_operation_id(), operation->GetId());
    descriptor->set_operation_type(ToProto(operation->GetType()));
    descriptor->set_spec(operation->GetSpecString().ToString());
    descriptor->set_experiment_assignments(ConvertToYsonString(operation->ExperimentAssignments()).ToString());
    descriptor->set_start_time(ToProto(operation->GetStartTime()));
    descriptor->set_authenticated_user(operation->GetAuthenticatedUser());
    if (operation->GetSecureVault()) {
        descriptor->set_secure_vault(ConvertToYsonString(operation->GetSecureVault()).ToString());
    }
    descriptor->set_acl(ConvertToYsonString(operation->GetRuntimeParameters()->Acl).ToString());
    if (auto acoName = operation->GetRuntimeParameters()->AcoName) {
        descriptor->set_aco_name(*acoName);
    }
    ToProto(descriptor->mutable_pool_tree_controller_settings_map(), operation->PoolTreeControllerSettingsMap());
    ToProto(descriptor->mutable_user_transaction_id(), operation->GetUserTransactionId());
    descriptor->set_controller_epoch(operation->ControllerEpoch().Underlying());

    return InvokeAgent<TControllerAgentServiceProxy::TRspRegisterOperation>(req).As<void>();
}

TFuture<TOperationControllerUnregisterResult> TOperationControllerImpl::Unregister()
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    if (!IncarnationId_) {
        YT_LOG_INFO("Operation has no agent assigned; unregister request ignored");
        return MakeFuture<TOperationControllerUnregisterResult>({});
    }

    YT_LOG_INFO("Unregistering operation controller");

    auto req = ControllerAgentTrackerProxy_->UnregisterOperation();
    ToProto(req->mutable_operation_id(), OperationId_);
    req->SetTimeout(Config_->ControllerAgentTracker->HeavyRpcTimeout);
    return InvokeAgent<TControllerAgentServiceProxy::TRspUnregisterOperation>(req).Apply(
        BIND([] (const TControllerAgentServiceProxy::TRspUnregisterOperationPtr& rsp) {
            return TOperationControllerUnregisterResult{FromProto<TOperationJobMetrics>(rsp->residual_job_metrics())};
        }));
}

TFuture<void> TOperationControllerImpl::UpdateRuntimeParameters(TOperationRuntimeParametersUpdatePtr update)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);
    if (!IncarnationId_) {
        return VoidFuture;
    }

    auto req = ControllerAgentTrackerProxy_->UpdateOperationRuntimeParameters();
    ToProto(req->mutable_operation_id(), OperationId_);
    ToProto(req->mutable_parameters(), ConvertToYsonString(update).ToString());
    req->SetTimeout(Config_->ControllerAgentTracker->HeavyRpcTimeout);
    return InvokeAgent<TControllerAgentServiceProxy::TRspUpdateOperationRuntimeParameters>(req).As<void>();
}

TFuture<void> TOperationControllerImpl::PatchSpec(const INodePtr& newCumulativeSpecPatch, bool dryRun)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);
    if (!IncarnationId_) {
        return MakeFuture(
            TError(
                NScheduler::EErrorCode::OperationHasNoController,
                "Cannot validate spec patch while controller agent is not assigned"));
    }

    auto req = ControllerAgentTrackerProxy_->PatchSpec();
    ToProto(req->mutable_operation_id(), OperationId_);
    ToProto(req->mutable_new_cumulative_spec_patch(), ConvertToYsonString(newCumulativeSpecPatch).ToString());
    req->set_dry_run(dryRun);
    req->SetTimeout(Config_->ControllerAgentTracker->HeavyRpcTimeout);

    auto agent = Agent_.Lock();
    if (!agent) {
        throw TFiberCanceledException();
    }

    return InvokeAgent<TControllerAgentServiceProxy::TRspPatchSpec>(req).Apply(BIND([
        agent,
        dryRun,
        agentTracker=Bootstrap_->GetControllerAgentTracker()
    ] (const TErrorOr<TControllerAgentServiceProxy::TRspPatchSpecPtr>& rspOrError) {
        if (!dryRun && !rspOrError.IsOK()) {
            if (!rspOrError.FindMatching(NControllerAgent::EErrorCode::ExceptionLeadingToOperationFailure)) {
                // Unknown error. Controller will not be able to fail operation
                // by itself, so we help it by, well, killing it.
                agentTracker->HandleAgentFailure(agent, rspOrError);
            }
        }
        rspOrError.ThrowOnError();
    }));
}

void TOperationControllerImpl::OnAllocationAborted(
    TAllocationId allocationId,
    const TError& error,
    bool scheduled,
    EAbortReason abortReason,
    TControllerEpoch allocationEpoch)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    if (ShouldSkipAllocationAbortEvent(allocationId, allocationEpoch)) {
        return;
    }

    TAbortedAllocationSummary eventSummary{
        .OperationId = OperationId_,
        .Id = allocationId,
        .FinishTime = TInstant::Now(),
        .AbortReason = abortReason,
        .Error = std::move(error).Truncate(),
        .Scheduled = scheduled,
    };

    auto result = EnqueueAllocationEvent(TSchedulerToAgentAllocationEvent{std::move(eventSummary)});
    YT_LOG_TRACE(
        "%v abort notification %v (AllocationId: %v)",
        scheduled ? "Allocation" : "Nonscheduled allocation",
        result ? "enqueued" : "dropped",
        allocationId);
}

void TOperationControllerImpl::OnAllocationAborted(
    const TAllocationPtr& allocation,
    const TError& error,
    bool scheduled,
    EAbortReason abortReason)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    OnAllocationAborted(allocation->GetId(), error, scheduled, abortReason, allocation->GetControllerEpoch());
}

void TOperationControllerImpl::OnAllocationFinished(const TAllocationPtr& allocation)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto allocationId = allocation->GetId();

    if (ShouldSkipAllocationAbortEvent(allocationId, allocation->GetControllerEpoch())) {
        return;
    }

    TFinishedAllocationSummary summary{
        .OperationId = allocation->GetOperationId(),
        .Id = allocationId,
        .FinishTime = TInstant::Now(),
    };

    auto result = EnqueueAllocationEvent(TSchedulerToAgentAllocationEvent{std::move(summary)});
    YT_LOG_TRACE(
        "Allocation finish notification %v (AllocationId: %v)",
        result ? "enqueued" : "dropped",
        allocationId);
}

void TOperationControllerImpl::OnNonscheduledAllocationAborted(
    TAllocationId allocationId,
    EAbortReason abortReason,
    TControllerEpoch allocationEpoch)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    OnAllocationAborted(allocationId, TError{}, false, abortReason, allocationEpoch);
}

void TOperationControllerImpl::OnInitializationFinished(const TErrorOr<TOperationControllerInitializeResult>& resultOrError)
{
    YT_VERIFY(PendingInitializeResult_);

    if (resultOrError.IsOK()) {
        YT_LOG_DEBUG("Successful initialization result received");
    } else {
        YT_LOG_DEBUG(resultOrError, "Unsuccessful initialization result received");
        ProcessControllerAgentError(resultOrError);
    }

    PendingInitializeResult_.TrySet(resultOrError);
}

void TOperationControllerImpl::OnPreparationFinished(const TErrorOr<TOperationControllerPrepareResult>& resultOrError)
{
    YT_VERIFY(PendingPrepareResult_);

    if (resultOrError.IsOK()) {
        YT_LOG_DEBUG("Successful preparation result received");
    } else {
        YT_LOG_DEBUG(resultOrError, "Unsuccessful preparation result received");
        ProcessControllerAgentError(resultOrError);
    }

    PendingPrepareResult_.TrySet(resultOrError);
}

void TOperationControllerImpl::OnMaterializationFinished(const TErrorOr<TOperationControllerMaterializeResult>& resultOrError)
{
    YT_VERIFY(PendingMaterializeResult_);

    auto onError = [&] (const TError& error) {
        YT_LOG_DEBUG(resultOrError, "Unsuccessful materialization result received");
        ProcessControllerAgentError(resultOrError);
        PendingMaterializeResult_.TrySet(error);
    };

    if (!resultOrError.IsOK()) {
        onError(resultOrError);
        return;
    }

    auto result = resultOrError.Value();

    ControllerRuntimeData_->SetNeededResources(result.InitialNeededResources);
    ControllerRuntimeData_->GroupedNeededResources() = result.InitialGroupedNeededResources;

    if (auto runtimeDataCheckError = CheckControllerRuntimeData(ControllerRuntimeData_);
        !runtimeDataCheckError.IsOK())
    {
        onError(runtimeDataCheckError);
        return;
    }

    InitialGroupedNeededResources_ = result.InitialGroupedNeededResources;

    YT_LOG_DEBUG(
        "Successful materialization result received "
        "(Suspend: %v, InitialNeededResources: %v, InitialGroupedNeededResources: %v)",
        result.Suspend,
        FormatResources(result.InitialNeededResources),
        InitialGroupedNeededResources_);

    PendingMaterializeResult_.TrySet(resultOrError);
}

void TOperationControllerImpl::OnRevivalFinished(const TErrorOr<TOperationControllerReviveResult>& resultOrError)
{
    YT_VERIFY(PendingReviveResult_);

    auto onError = [&] (const TError& error) {
        YT_LOG_DEBUG(resultOrError, "Unsuccessful revival result received");
        ProcessControllerAgentError(resultOrError);
        PendingReviveResult_.TrySet(error);
    };

    if (!resultOrError.IsOK()) {
        onError(resultOrError);
        return;
    }

    auto result = resultOrError.Value();

    ControllerRuntimeData_->SetNeededResources(result.NeededResources);
    ControllerRuntimeData_->GroupedNeededResources() = result.GroupedNeededResources;

    if (auto runtimeDataCheckError = CheckControllerRuntimeData(ControllerRuntimeData_);
        !runtimeDataCheckError.IsOK())
    {
        onError(runtimeDataCheckError);
        return;
    }

    InitialGroupedNeededResources_ = result.InitialGroupedNeededResources;

    YT_LOG_DEBUG(
        "Successful revival result received "
        "(RevivedFromSnapshot: %v, RevivedAllocationCount: %v, RevivedBannedTreeIds: %v, "
        "NeededResources: %v, InitialGroupedNeededResources: %v)",
        result.RevivedFromSnapshot,
        result.RevivedAllocations.size(),
        result.RevivedBannedTreeIds,
        FormatResources(result.NeededResources),
        InitialGroupedNeededResources_);

    PendingReviveResult_.TrySet(resultOrError);
}

void TOperationControllerImpl::OnCommitFinished(const TErrorOr<TOperationControllerCommitResult>& resultOrError)
{
    YT_VERIFY(PendingCommitResult_);

    if (resultOrError.IsOK()) {
        YT_LOG_DEBUG("Successful commit result received");
    } else {
        YT_LOG_DEBUG(resultOrError, "Unsuccessful commit result received");
        ProcessControllerAgentError(resultOrError);
    }

    PendingCommitResult_.TrySet(resultOrError);
}

void TOperationControllerImpl::SetControllerRuntimeData(const TControllerRuntimeDataPtr& controllerData)
{
    ControllerRuntimeData_ = controllerData;
}

TFuture<void> TOperationControllerImpl::GetFullHeartbeatProcessed()
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    auto agent = Agent_.Lock();
    if (!agent) {
        return VoidFuture;
    }
    return agent->GetFullHeartbeatProcessed();
}

TFuture<TControllerScheduleAllocationResultPtr> TOperationControllerImpl::ScheduleAllocation(
    const ISchedulingContextPtr& context,
    const TJobResources& allocationLimits,
    const TDiskResources& diskResourceLimits,
    const TString& treeId,
    const TString& poolPath,
    const TFairShareStrategyTreeConfigPtr& treeConfig)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto nodeId = context->GetNodeDescriptor()->Id;
    auto cellTag = Bootstrap_->GetClient()->GetNativeConnection()->GetPrimaryMasterCellTag();
    auto allocationId = GenerateAllocationId(cellTag, nodeId);

    const auto& nodeManager = Bootstrap_->GetScheduler()->GetNodeManager();
    const auto shardId = nodeManager->GetNodeShardId(nodeId);
    const auto& nodeShard = nodeManager->GetNodeShards()[shardId];

    if (!nodeShard->IsOperationRegistered(OperationId_)) {
        YT_LOG_DEBUG(
            "Allocation schedule request cannot be served since operation is not registered at node shard (AllocationId: %v)",
            allocationId);

        auto result = New<TControllerScheduleAllocationResult>();
        result->RecordFail(NControllerAgent::EScheduleFailReason::OperationNotRunning);
        return MakeFuture(std::move(result));
    }

    if (nodeShard->AreNewAllocationsForbiddenForOperation(OperationId_)) {
        YT_LOG_DEBUG(
            "Allocation schedule request cannot be served since new allocations are forbidden for operation (AllocationId: %v)",
            allocationId);

        auto result = New<TControllerScheduleAllocationResult>();
        result->RecordFail(NControllerAgent::EScheduleFailReason::NewJobsForbidden);
        return MakeFuture(std::move(result));
    }

    auto request = std::make_unique<TScheduleAllocationRequest>();
    request->OperationId = OperationId_;
    request->AllocationId = allocationId;
    request->AllocationResourceLimits = allocationLimits;
    request->TreeId = treeId;
    request->PoolPath = poolPath;
    request->NodeId = nodeId;
    request->NodeResourceLimits = context->ResourceLimits();
    request->NodeDiskResources = diskResourceLimits;
    request->Spec.WaitingForResourcesOnNodeTimeout = treeConfig->WaitingForResourcesOnNodeTimeout;

    TIncarnationId incarnationId;
    {
        auto guard = Guard(SpinLock_);
        if (!IncarnationId_) {
            guard.Release();

            YT_LOG_DEBUG(
                "Allocation schedule request cannot be served since no agent is assigned (AllocationId: %v)",
                allocationId);

            auto result = New<TControllerScheduleAllocationResult>();
            result->RecordFail(EScheduleFailReason::NoAgentAssigned);

            return MakeFuture(result);
        }

        incarnationId = IncarnationId_;
        ScheduleAllocationRequestsOutbox_->Enqueue(std::move(request));
    }

    YT_LOG_TRACE(
        "Allocation schedule request enqueued (AllocationId: %v, NodeAddress: %v)",
        allocationId,
        context->GetNodeDescriptor()->Address);

    return nodeShard->BeginScheduleAllocation(incarnationId, OperationId_, allocationId);
}

void TOperationControllerImpl::UpdateGroupedNeededResources()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    EnqueueOperationEvent({
        .EventType = ESchedulerToAgentOperationEventType::UpdateGroupedNeededResources,
        .OperationId = OperationId_,
    });

    YT_LOG_DEBUG("Grouped needed resources update request enqueued");
}

TCompositeNeededResources TOperationControllerImpl::GetNeededResources() const
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    return ControllerRuntimeData_->GetNeededResources();
}

TAllocationGroupResourcesMap TOperationControllerImpl::GetGroupedNeededResources() const
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    return ControllerRuntimeData_->GroupedNeededResources();
}

TAllocationGroupResourcesMap TOperationControllerImpl::GetInitialGroupedNeededResources() const
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    return InitialGroupedNeededResources_;
}

EPreemptionMode TOperationControllerImpl::GetPreemptionMode() const
{
    return PreemptionMode_;
}

bool TOperationControllerImpl::ShouldSkipAllocationAbortEvent(
    TAllocationId allocationId,
    TControllerEpoch allocationEpoch) const
{
    auto currentEpoch = Epoch_.load();
    if (allocationEpoch != currentEpoch) {
        YT_LOG_DEBUG(
            "Allocation abort notification skipped since controller epoch mismatch (AllocationId: %v, AllocationEpoch: %v, CurrentEpoch: %v)",
            allocationId,
            allocationEpoch,
            currentEpoch);
        return true;
    }
    return false;
}

bool TOperationControllerImpl::EnqueueAllocationEvent(TSchedulerToAgentAllocationEvent&& event)
{
    auto guard = Guard(SpinLock_);
    if (IncarnationId_) {
        AllocationEventsOutbox_->Enqueue(std::move(event));
        return true;
    } else {
        // All allocation notifications must be dropped after agent disconnection.
        // Allocation revival machinery will reconsider this event further.
        return false;
    }
}

void TOperationControllerImpl::EnqueueOperationEvent(TSchedulerToAgentOperationEvent&& event)
{
    YT_VERIFY(IncarnationId_);
    OperationEventsOutbox_->Enqueue(std::move(event));
}

void TOperationControllerImpl::EnqueueScheduleAllocationRequest(TScheduleAllocationRequestPtr&& event)
{
    YT_VERIFY(IncarnationId_);
    ScheduleAllocationRequestsOutbox_->Enqueue(std::move(event));
}

void TOperationControllerImpl::ProcessControllerAgentError(const TError& error)
{
    if (IsAgentDisconnectionError(error)) {
        auto agent = Agent_.Lock();
        if (!agent) {
            throw TFiberCanceledException();
        }
        const auto& agentTracker = Bootstrap_->GetControllerAgentTracker();
        agentTracker->HandleAgentFailure(agent, error);
    }
}

template <class TResponse, class TRequest>
TFuture<TIntrusivePtr<TResponse>> TOperationControllerImpl::InvokeAgent(
    const TIntrusivePtr<TRequest>& request)
{
    auto agent = Agent_.Lock();
    if (!agent) {
        throw NConcurrency::TFiberCanceledException();
    }
    auto method = request->GetMethod();

    YT_LOG_DEBUG("Sending request to agent (AgentId: %v, IncarnationId: %v, OperationId: %v, Method: %v)",
        agent->GetId(),
        agent->GetIncarnationId(),
        OperationId_,
        method);

    ToProto(request->mutable_incarnation_id(), agent->GetIncarnationId());

    return request->Invoke().Apply(BIND([
        agent,
        method,
        operationId=OperationId_,
        Logger=Logger,
        agentTracker=Bootstrap_->GetControllerAgentTracker()
    ] (const TErrorOr<TIntrusivePtr<TResponse>>& rspOrError) {
        YT_LOG_DEBUG(rspOrError, "Agent response received (AgentId: %v, OperationId: %v, Method: %v)",
            agent->GetId(),
            operationId,
            method);
        if (IsAgentFailureError(rspOrError) || IsAgentDisconnectionError(rspOrError)) {
            agentTracker->HandleAgentFailure(agent, rspOrError);
            // Unregistration should happen before actions that subscribed on this result.
            return rspOrError.ValueOrThrow();
        } else if (rspOrError.GetCode() == NControllerAgent::EErrorCode::AgentCallFailed) {
            YT_VERIFY(rspOrError.InnerErrors().size() == 1);
            THROW_ERROR rspOrError.InnerErrors()[0];
        } else {
            return rspOrError.ValueOrThrow();
        }
    }));
}

std::pair<NApi::ITransactionPtr, TString> TOperationControllerImpl::GetIntermediateMediumTransaction()
{
    return {nullptr, {}};
}

void TOperationControllerImpl::UpdateIntermediateMediumUsage(i64 /*usage*/)
{
    YT_UNIMPLEMENTED();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
