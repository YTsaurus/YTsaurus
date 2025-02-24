#include "scheduler.h"

#include "private.h"
#include "fair_share_strategy.h"
#include "helpers.h"
#include "master_connector.h"
#include "node_manager.h"
#include "scheduler_strategy.h"
#include "controller_agent.h"
#include "operation_controller.h"
#include "bootstrap.h"
#include "operations_cleaner.h"
#include "controller_agent_tracker.h"
#include "persistent_scheduler_state.h"

#include <yt/yt/server/lib/scheduler/allocation_tracker_service_proxy.h>
#include <yt/yt/server/lib/scheduler/config.h>
#include <yt/yt/server/lib/scheduler/experiments.h>
#include <yt/yt/server/lib/scheduler/scheduling_tag.h>
#include <yt/yt/server/lib/scheduler/event_log.h>
#include <yt/yt/server/lib/scheduler/helpers.h>

#include <yt/yt/ytlib/scheduler/helpers.h>
#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

#include <yt/yt/client/security_client/acl.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/ytlib/table_client/schemaless_buffered_table_writer.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/unversioned_writer.h>
#include <yt/yt/client/table_client/table_consumer.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/node_tracker_client/helpers.h>

#include <yt/yt/client/scheduler/spec_patch.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/controller_agent/controller_agent_service_proxy.h>

#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/ytlib/security_client/helpers.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>

#include <yt/yt/core/actions/cancelable_context.h>
#include <yt/yt/core/actions/new_with_offloaded_dtor.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/rpc/message.h>
#include <yt/yt/core/rpc/response_keeper.h>

#include <yt/yt/core/logging/fluent_log.h>

#include <yt/yt/core/misc/mpsc_stack.h>
#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/sync_expiring_cache.h>

#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/service_combiner.h>
#include <yt/yt/core/ytree/virtual.h>
#include <yt/yt/core/ytree/exception_helpers.h>
#include <yt/yt/core/ytree/permission.h>

#include <yt/yt/build/build.h>

#include <library/cpp/yt/misc/numeric_helpers.h>

#include <util/generic/size_literals.h>

namespace NYT::NScheduler {

using namespace NProfiling;
using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NRpc;
using namespace NNet;
using namespace NApi;
using namespace NObjectClient;
using namespace NHydra;
using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NTableClient;
using namespace NNodeTrackerClient::NProto;
using namespace NSecurityClient;
using namespace NEventLog;
using namespace NTransactionClient;
using namespace NControllerAgent;

using NNodeTrackerClient::TNodeId;
using NNodeTrackerClient::TNodeDescriptor;

using std::placeholders::_1;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = SchedulerLogger;

static const TString UnknownTreeId = "<unknown>";

////////////////////////////////////////////////////////////////////////////////

struct TPoolTreeKeysHolder
{
    TPoolTreeKeysHolder()
    {
        auto treeConfigTemplate = New<TFairShareStrategyTreeConfig>();
        auto treeConfigKeys = treeConfigTemplate->GetRegisteredKeys();

        auto poolConfigTemplate = New<TPoolConfig>();
        auto poolConfigKeys = poolConfigTemplate->GetRegisteredKeys();

        Keys.reserve(treeConfigKeys.size() + poolConfigKeys.size() + 3);
        Keys.insert(Keys.end(), treeConfigKeys.begin(), treeConfigKeys.end());
        Keys.insert(Keys.end(), poolConfigKeys.begin(), poolConfigKeys.end());
        Keys.insert(Keys.end(), DefaultTreeAttributeName);
        Keys.insert(Keys.end(), TreeConfigAttributeName);
        Keys.insert(Keys.end(), IdAttributeName);
    }

    std::vector<TString> Keys;
};

////////////////////////////////////////////////////////////////////////////////

class TScheduler::TImpl
    : public TRefCounted
    , public ISchedulerStrategyHost
    , public INodeManagerHost
    , public IOperationsCleanerHost
    , public TEventLogHostBase
{
public:
    using TEventLogHostBase::LogEventFluently;

    TImpl(
        TSchedulerConfigPtr config,
        TBootstrap* bootstrap)
        : Config_(config)
        , InitialConfig_(Config_)
        , Bootstrap_(bootstrap)
        , SpecTemplate_(Config_->SpecTemplate)
        , MasterConnector_(std::make_unique<TMasterConnector>(Config_, Bootstrap_))
        , OrchidWorkerPool_(CreateThreadPool(Config_->OrchidWorkerThreadCount, "OrchidWorker"))
        , FairShareUpdatePool_(CreateThreadPool(Config_->FairShareUpdateThreadCount, "FSUpdatePool"))
        , BackgroundThreadPool_(CreateThreadPool(Config_->BackgroundThreadCount, "Background"))
        , OperationServiceResponseKeeper_(CreateResponseKeeper(
            Config_->OperationServiceResponseKeeper,
            GetControlInvoker(EControlQueue::UserRequest),
            SchedulerLogger(),
            SchedulerProfiler()))
        , NodeManager_(New<TNodeManager>(Config_, this, Bootstrap_))
        , ExperimentsAssigner_(Config_->Experiments)
    {
        YT_VERIFY(config);
        YT_VERIFY(bootstrap);
        YT_ASSERT_INVOKER_THREAD_AFFINITY(GetControlInvoker(EControlQueue::Default), ControlThread);

        OperationsCleaner_ = New<TOperationsCleaner>(Config_->OperationsCleaner, this, Bootstrap_);

        OperationsCleaner_->SubscribeOperationsRemovedFromCypress(BIND_NO_PROPAGATE(&TImpl::OnOperationsRemovedFromCypress, MakeWeak(this)));

        ServiceAddress_ = BuildServiceAddress(
            GetLocalHostName(),
            Bootstrap_->GetConfig()->RpcPort);

        {
            std::vector<IInvokerPtr> feasibleInvokers;
            for (auto controlQueue : TEnumTraits<EControlQueue>::GetDomainValues()) {
                feasibleInvokers.push_back(Bootstrap_->GetControlInvoker(controlQueue));
            }

            Strategy_ = CreateFairShareStrategy(Config_, this, std::move(feasibleInvokers));
        }
    }

    void Initialize()
    {
        MasterConnector_->AddCommonWatcher(
            BIND_NO_PROPAGATE(&TImpl::RequestConfig, Unretained(this)),
            BIND_NO_PROPAGATE(&TImpl::HandleConfig, Unretained(this)),
            ESchedulerAlertType::UpdateConfig);

        MasterConnector_->SetCustomWatcher(
            EWatcherType::PoolTrees,
            BIND_NO_PROPAGATE(&TImpl::RequestPoolTrees, Unretained(this)),
            BIND_NO_PROPAGATE(&TImpl::HandlePoolTrees, Unretained(this)),
            Config_->WatchersUpdatePeriod,
            ESchedulerAlertType::UpdatePools,
            TWatcherLockOptions{
                .LockPath = GetPoolTreesLockPath(),
                .CheckBackoff = Config_->PoolTreesLockCheckBackoff,
                .WaitTimeout = Config_->PoolTreesLockTransactionTimeout
            });

        MasterConnector_->SetCustomWatcher(
            EWatcherType::NodeAttributes,
            BIND_NO_PROPAGATE(&TImpl::RequestNodesAttributes, Unretained(this)),
            BIND_NO_PROPAGATE(&TImpl::HandleNodesAttributes, Unretained(this)),
            Config_->NodesAttributesUpdatePeriod);

        MasterConnector_->AddCommonWatcher(
            BIND_NO_PROPAGATE(&TImpl::RequestOperationsEffectiveAcl, Unretained(this)),
            BIND_NO_PROPAGATE(&TImpl::HandleOperationsEffectiveAcl, Unretained(this)));

        MasterConnector_->AddCommonWatcher(
            BIND_NO_PROPAGATE(&TImpl::RequestOperationsArchiveVersion, Unretained(this)),
            BIND_NO_PROPAGATE(&TImpl::HandleOperationsArchiveVersion, Unretained(this)));

        MasterConnector_->AddCommonWatcher(
            BIND_NO_PROPAGATE(&TImpl::RequestClusterName, Unretained(this)),
            BIND_NO_PROPAGATE(&TImpl::HandleClusterName, Unretained(this)));

        MasterConnector_->AddCommonWatcher(
            BIND_NO_PROPAGATE(&TImpl::RequestUserToDefaultPoolMap, Unretained(this)),
            BIND_NO_PROPAGATE(&TImpl::HandleUserToDefaultPoolMap, Unretained(this)));

        MasterConnector_->SubscribeMasterConnecting(BIND_NO_PROPAGATE(
            &TImpl::OnMasterConnecting,
            Unretained(this)));
        MasterConnector_->SubscribeMasterHandshake(BIND_NO_PROPAGATE(
            &TImpl::OnMasterHandshake,
            Unretained(this)));
        MasterConnector_->SubscribeMasterConnected(BIND_NO_PROPAGATE(
            &TImpl::OnMasterConnected,
            Unretained(this)));
        MasterConnector_->SubscribeMasterDisconnected(BIND_NO_PROPAGATE(
            &TImpl::OnMasterDisconnected,
            Unretained(this)));

        MasterConnector_->Start();

        ProfilingExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(EControlQueue::SchedulerProfiling),
            BIND(&TImpl::OnProfiling, MakeWeak(this)),
            Config_->ProfilingUpdatePeriod);
        ProfilingExecutor_->Start();

        EventLogWriter_ = CreateStaticTableEventLogWriter(
            Config_->EventLog,
            GetClient(),
            Bootstrap_->GetControlInvoker(EControlQueue::EventLog),
            /*writeBlocksOptions*/ {});
        ControlEventLogWriterConsumer_ = EventLogWriter_->CreateConsumer();
        OffloadedEventLogWriterConsumer_ = EventLogWriter_->CreateConsumer();

        LogEventFluently(&SchedulerStructuredLogger(), ELogEventType::SchedulerStarted)
            .Item("address").Value(ServiceAddress_);

        ClusterInfoLoggingExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(EControlQueue::EventLog),
            BIND(&TImpl::OnClusterInfoLogging, MakeWeak(this)),
            Config_->ClusterInfoLoggingPeriod);
        ClusterInfoLoggingExecutor_->Start();

        NodesInfoLoggingExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(EControlQueue::EventLog),
            BIND(&TImpl::OnNodesInfoLogging, MakeWeak(this)),
            Config_->NodesInfoLoggingPeriod);
        NodesInfoLoggingExecutor_->Start();

        UpdateExecNodeDescriptorsExecutor_ = New<TPeriodicExecutor>(
            GetBackgroundInvoker(),
            BIND(&TImpl::UpdateExecNodeDescriptors, MakeWeak(this)),
            Config_->ExecNodeDescriptorsUpdatePeriod);
        UpdateExecNodeDescriptorsExecutor_->Start();

        JobReporterWriteFailuresChecker_ = New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(EControlQueue::CommonPeriodicActivity),
            BIND(&TImpl::CheckJobReporterIssues, MakeWeak(this)),
            Config_->JobReporterIssuesCheckPeriod);
        JobReporterWriteFailuresChecker_->Start();

        CachedExecNodeMemoryDistributionByTags_ = New<TSyncExpiringCache<TSchedulingTagFilter, TMemoryDistribution>>(
            BIND(&TImpl::CalculateMemoryDistribution, MakeStrong(this)),
            Config_->SchedulingTagFilterExpireTimeout,
            GetBackgroundInvoker());

        StrategyStuckOperationsChecker_ = New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(EControlQueue::OperationsPeriodicActivity),
            BIND(&TImpl::CheckStuckOperations, MakeWeak(this)),
            Config_->OperationStuckCheck->Period);
        StrategyStuckOperationsChecker_->Start();

        ExperimentAssignmentErrorChecker_ = New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(EControlQueue::Default),
            BIND(&TImpl::CheckExperimentAssignmentError, MakeWeak(this)),
            Config_->ExperimentAssignmentErrorCheckPeriod);
        ExperimentAssignmentErrorChecker_->Start();

        MeteringRecordCountCounter_ = SchedulerProfiler()
            .Counter("/metering/record_count");
        MeteringUsageQuantityCounter_ = SchedulerProfiler()
            .Counter("/metering/usage_quantity");

        AllocationMeteringRecordCountCounter_ = SchedulerProfiler()
            .Counter("/metering/allocation/record_count");
        AllocationMeteringUsageQuantityCounter_ = SchedulerProfiler()
            .Counter("/metering/allocation/usage_quantity");

        GuaranteesMeteringRecordCountCounter_ = SchedulerProfiler()
            .Counter("/metering/guarantees/record_count");
        GuaranteesMeteringUsageQuantityCounter_ = SchedulerProfiler()
            .Counter("/metering/guarantees/usage_quantity");

        TotalResourceLimitsProfiler_.Init(SchedulerProfiler().WithPrefix("/total_resource_limits"));
        TotalResourceUsageProfiler_.Init(SchedulerProfiler().WithPrefix("/total_resource_usage"));
    }

    const NApi::NNative::IClientPtr& GetClient() const
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return Bootstrap_->GetClient();
    }

    IYPathServicePtr CreateOrchidService()
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto staticOrchidProducer = BIND(&TImpl::BuildStaticOrchid, MakeStrong(this));
        auto staticOrchidService = IYPathService::FromProducer(staticOrchidProducer)
            ->Via(GetControlInvoker(EControlQueue::StaticOrchid))
            ->Cached(
                Config_->StaticOrchidCacheUpdatePeriod,
                OrchidWorkerPool_->GetInvoker(),
                SchedulerProfiler().WithPrefix("/static_orchid"));
        StaticOrchidService_.Reset(dynamic_cast<ICachedYPathService*>(staticOrchidService.Get()));
        YT_VERIFY(StaticOrchidService_);

        auto lightStaticOrchidProducer = BIND(&TImpl::BuildLightStaticOrchid, MakeStrong(this));
        auto lightStaticOrchidService = IYPathService::FromProducer(lightStaticOrchidProducer)
            ->Via(GetControlInvoker(EControlQueue::StaticOrchid));

        auto dynamicOrchidService = GetDynamicOrchidService()
            ->Via(GetControlInvoker(EControlQueue::DynamicOrchid));

        auto strategyDynamicOrchidService = Strategy_->GetOrchidService()
            ->Via(GetControlInvoker(EControlQueue::DynamicOrchid));

        auto combinedOrchidService = New<TServiceCombiner>(
            std::vector<IYPathServicePtr>{
                staticOrchidService,
                std::move(lightStaticOrchidService),
                std::move(dynamicOrchidService),
                std::move(strategyDynamicOrchidService),
            },
            Config_->OrchidKeysUpdatePeriod);
        CombinedOrchidService_.Reset(combinedOrchidService.Get());
        YT_VERIFY(CombinedOrchidService_);
        return combinedOrchidService;
    }

    TRefCountedExecNodeDescriptorMapPtr GetCachedExecNodeDescriptors() const
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return CachedExecNodeDescriptors_.Acquire();
    }

    TSharedRef GetCachedProtoExecNodeDescriptors() const
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto cachedExecNodeDescriptors = CachedSerializedExecNodeDescriptors_.Acquire();

        return
            cachedExecNodeDescriptors ?
            cachedExecNodeDescriptors->ExecNodeDescriptors :
            TSharedRef::MakeEmpty();
    }

    const TSchedulerConfigPtr& GetConfig() const
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        return Config_;
    }

    const TNodeManagerPtr& GetNodeManager() const
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return NodeManager_;
    }

    bool IsConnected() const
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return MasterConnector_->GetState() == EMasterConnectorState::Connected;
    }

    void ValidateConnected()
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        if (!IsConnected()) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::Unavailable,
                "Master is not connected");
        }
    }

    TMasterConnector* GetMasterConnector() const
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return MasterConnector_.get();
    }

    void Disconnect(const TError& error) override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        MasterConnector_->Disconnect(error);
    }

    TInstant GetConnectionTime() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return MasterConnector_->GetConnectionTime();
    }

    TOperationPtr FindOperation(const TOperationIdOrAlias& idOrAlias) const
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        return Visit(idOrAlias.Payload,
            [&] (const TOperationId& id) -> TOperationPtr {
                auto it = IdToOperation_.find(id);
                return it == IdToOperation_.end() ? nullptr : it->second;
            },
            [&] (const TString& alias) -> TOperationPtr {
                auto it = OperationAliases_.find(alias);
                return it == OperationAliases_.end() ? nullptr : it->second.Operation;
            });
    }

    TOperationPtr GetOperation(const TOperationIdOrAlias& idOrAlias) const
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto operation = FindOperation(idOrAlias);
        YT_VERIFY(operation);
        return operation;
    }

    TOperationPtr GetOperationOrThrow(const TOperationIdOrAlias& idOrAlias) const
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto operation = FindOperation(idOrAlias);
        if (!operation) {
            THROW_ERROR_EXCEPTION(NScheduler::EErrorCode::NoSuchOperation,
                "No such operation %v",
                idOrAlias);
        }
        return operation;
    }

    TMemoryDistribution GetExecNodeMemoryDistribution(const TSchedulingTagFilter& filter) const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return CachedExecNodeMemoryDistributionByTags_->Get(filter);
    }

    void SetSchedulerAlert(ESchedulerAlertType alertType, const TError& alert) override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        if (!alert.IsOK()) {
            YT_LOG_WARNING(alert, "Setting scheduler alert (AlertType: %v)", alertType);
        } else {
            YT_LOG_DEBUG("Reset scheduler alert (AlertType: %v)", alertType);
        }

        MasterConnector_->SetSchedulerAlert(alertType, alert);
    }

    TFuture<void> SetOperationAlert(
        TOperationId operationId,
        EOperationAlertType alertType,
        const TError& alert,
        std::optional<TDuration> timeout = {}) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return BIND(&TImpl::DoSetOperationAlert, MakeStrong(this), operationId, alertType, alert, timeout)
            .AsyncVia(GetControlInvoker(EControlQueue::Operation))
            .Run();
    }

    void ValidatePoolPermission(
        const TString& treeId,
        NObjectClient::TObjectId poolObjectId,
        const TString& poolName,
        const std::string& user,
        EPermission permission) const override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto path = poolObjectId
            ? Format("#%v", poolObjectId)
            : Config_->PoolTreesRoot;

        YT_LOG_DEBUG("Validating pool permission (Permission: %v, User: %v, Pool: %v, Path: %v, TreeId: %v)",
            permission,
            user,
            poolName,
            path,
            treeId);

        auto result = WaitFor(GetClient()->CheckPermission(user, path, permission))
            .ValueOrThrow();
        if (result.Action == ESecurityAction::Deny) {
            THROW_ERROR_EXCEPTION(
                NSecurityClient::EErrorCode::AuthorizationError,
                "User %Qv has been denied access to pool %Qv in pool tree %Qv",
                user,
                poolName,
                treeId)
                << result.ToError(user, permission)
                << TErrorAttribute("path", path);
        }

        YT_LOG_DEBUG("Pool permission successfully validated");
    }

    TFuture<void> ValidateOperationAccess(
        const std::string& user,
        TOperationId operationId,
        EPermissionSet permissions)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto doValidateOperationAccess = BIND([=, this, this_ = MakeStrong(this)] {
            NScheduler::ValidateOperationAccess(
                user,
                operationId,
                TAllocationId(),
                permissions,
                GetOperationOrThrow(operationId)->GetAccessControlRule(),
                GetClient(),
                Logger());
        });

        return doValidateOperationAccess
            .AsyncVia(GetControlInvoker(EControlQueue::Operation))
            .Run();
    }

    // COMPAT(pogorelov)
    void DoValidateJobShellAccess(
        const std::string& user,
        const TString& jobShellName,
        const std::vector<TString>& jobShellOwners)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        YT_LOG_DEBUG(
            "Validating job shell access (User: %v, Name: %v, Owners: %v)",
            user,
            jobShellName,
            jobShellOwners);

        NControllerAgent::ValidateJobShellAccess(
            Bootstrap_->GetClient(),
            user,
            jobShellName,
            jobShellOwners);
    }

    // COMPAT(pogorelov)
    TFuture<void> ValidateJobShellAccess(
        const std::string& user,
        const TString& jobShellName,
        const std::vector<TString>& jobShellOwners)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return BIND(&TImpl::DoValidateJobShellAccess, MakeStrong(this), user, jobShellName, jobShellOwners)
            .AsyncVia(GetControlInvoker(EControlQueue::Operation))
            .Run();
    }

    TFuture<TPreprocessedSpec> AssignExperimentsAndParseSpec(
        EOperationType type,
        const std::string& user,
        TYsonString specString)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        // NB: We cannot use Config_ and SpecTemplate_ fields in threads other than control,
        // so we make a copy of intrusive pointers here and pass them as arguments.
        return BIND(
            &TImpl::DoAssignExperimentsAndParseSpec,
            MakeStrong(this),
            type,
            user,
            Passed(std::move(specString)),
            SpecTemplate_)
            .AsyncVia(GetBackgroundInvoker())
            .Run();
    }

    TFuture<TOperationPtr> StartOperation(
        EOperationType type,
        TTransactionId transactionId,
        TMutationId mutationId,
        const std::string& user,
        TPreprocessedSpec preprocessedSpec)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        if (std::ssize(IdToOperation_) >= Config_->MaxOperationCount) {
            THROW_ERROR_EXCEPTION(NScheduler::EErrorCode::TooManyOperations,
                "Limit for the total number of concurrent operations %v has been reached",
                Config_->MaxOperationCount);
        }

        auto spec = std::move(preprocessedSpec.Spec);
        auto secureVault = std::move(spec->SecureVault);

        auto baseAcl = GetOperationBaseAcl();
        if (spec->AddAuthenticatedUserToAcl.value_or(true)) {
            baseAcl.Entries.emplace_back(
                ESecurityAction::Allow,
                std::vector{user},
                EPermissionSet(EPermission::Read | EPermission::Manage));
        }

        auto operationId = TOperationId(MakeRandomId(
            EObjectType::Operation,
            GetClient()->GetNativeConnection()->GetPrimaryMasterCellTag()));

        auto runtimeParameters = New<TOperationRuntimeParameters>();
        InitOperationRuntimeParameters(runtimeParameters, spec, baseAcl, user, type, operationId);

        auto operation = NewWithOffloadedDtor<TOperation>(
            GetBackgroundInvoker(),
            operationId,
            type,
            mutationId,
            transactionId,
            spec,
            std::move(preprocessedSpec.CustomSpecPerTree),
            std::move(preprocessedSpec.SpecString),
            std::move(preprocessedSpec.TrimmedAnnotations),
            std::move(preprocessedSpec.BriefVanillaTaskSpecs),
            secureVault,
            /*temporaryTokenNodeId*/ std::nullopt,
            runtimeParameters,
            std::move(baseAcl),
            user,
            /*startTime*/ TInstant::Now(),
            MasterConnector_->GetCancelableControlInvoker(EControlQueue::Operation),
            spec->Alias,
            std::move(preprocessedSpec.ExperimentAssignments),
            std::move(preprocessedSpec.ProvidedSpecString));

        IdToStartingOperation_.emplace(operationId, operation);

        if (spec->Owners && !spec->Owners->empty()) {
            // COMPAT(egor-gutrov): this is compat alert, remove it
            operation->SetAlertWithoutArchivation(
                EOperationAlertType::OwnersInSpecIgnored,
                TError("\"owners\" field in spec ignored as it was specified simultaneously with \"acl\""));
        }

        operation->SetStateAndEnqueueEvent(EOperationState::Starting);

        auto codicilGuard = operation->MakeCodicilGuard();

        YT_LOG_INFO("Starting operation (OperationType: %v, OperationId: %v, TransactionId: %v, User: %v, ExperimentAssignments: %v)",
            type,
            operationId,
            transactionId,
            user,
            operation->GetExperimentAssignmentNames());

        YT_LOG_INFO("Total resource limits (OperationId: %v, ResourceLimits: %v)",
            operationId,
            FormatResources(GetResourceLimits(EmptySchedulingTagFilter)));

        try {
            WaitFor(Strategy_->ValidateOperationStart(operation.Get()))
                .ThrowOnError();
        } catch (const std::exception& ex) {
            // It means that scheduler was disconnected during check.
            if (operation->GetStarted().IsSet()) {
                return operation->GetStarted();
            }
            auto wrappedError = TError("Operation has failed to start")
                << ex;
            operation->SetStarted(wrappedError);
            EraseOrCrash(IdToStartingOperation_, operationId);
            THROW_ERROR(wrappedError);
        }

        if (operation->Spec()->TestingOperationOptions->DelayBeforeStart) {
            TDelayedExecutor::WaitForDuration(*operation->Spec()->TestingOperationOptions->DelayBeforeStart);
        }

        if (!preprocessedSpec.ExperimentAssignmentErrors.empty()) {
            LastExperimentAssignmentError_ = preprocessedSpec.ExperimentAssignmentErrors.front()
                << TErrorAttribute("operation_id", operationId);
            LastExperimentAssignmentErrorTime_ = TInstant::Now();
        }

        operation->GetCancelableControlInvoker()->Invoke(
            BIND(&TImpl::DoStartOperation, MakeStrong(this), operation));

        return operation->GetStarted();
    }

    TFuture<void> AbortOperation(
        const TOperationPtr& operation,
        const TError& error,
        const std::string& user)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        if (operation->GetState() == EOperationState::None) {
            THROW_ERROR_EXCEPTION("Operation is not started yet");
        }

        WaitFor(ValidateOperationAccess(user, operation->GetId(), EPermissionSet(EPermission::Manage)))
            .ThrowOnError();

        if (operation->IsFinishingState() || operation->IsFinishedState()) {
            YT_LOG_INFO(error, "Operation is already shutting down (OperationId: %v, State: %v)",
                operation->GetId(),
                operation->GetState());
            return operation->GetFinished();
        }

        if (operation->GetState() == EOperationState::Orphaned) {
            operation->SetOrhanedOperationAbortionError(error);
        } else {
            operation->GetCancelableControlInvoker()->Invoke(
                BIND(&TImpl::DoAbortOperation, MakeStrong(this), operation, error));
        }

        return operation->GetFinished();
    }

    TFuture<void> SuspendOperation(
        const TOperationPtr& operation,
        const std::string& user,
        bool abortRunningAllocations,
        const std::optional<std::string>& reason)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        if (operation->GetState() == EOperationState::None) {
            THROW_ERROR_EXCEPTION("Operation is not started yet");
        }

        WaitFor(ValidateOperationAccess(user, operation->GetId(), EPermissionSet(EPermission::Manage)))
            .ThrowOnError();

        if (operation->IsFinishingState() || operation->IsFinishedState()) {
            return MakeFuture(TError(NScheduler::EErrorCode::InvalidOperationState,
                "Cannot suspend operation in %Qlv state",
                operation->GetState()));
        }

        auto error = TError("Suspend operation by user request");
        if (reason) {
            error <<= TErrorAttribute("reason", *reason);
        }
        DoSuspendOperation(
            operation,
            error,
            abortRunningAllocations,
            /*setAlert*/ reason.has_value());

        return MasterConnector_->FlushOperationNode(operation);
    }

    TFuture<void> ResumeOperation(
        const TOperationPtr& operation,
        const std::string& user)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        if (operation->GetState() == EOperationState::None) {
            THROW_ERROR_EXCEPTION("Operation is not started yet");
        }

        WaitFor(ValidateOperationAccess(user, operation->GetId(), EPermissionSet(EPermission::Manage)))
            .ThrowOnError();

        if (!operation->GetSuspended()) {
            return MakeFuture(TError(NScheduler::EErrorCode::InvalidOperationState,
                "Operation is in %Qlv state",
                operation->GetState()));
        }

        NodeManager_->ResumeOperationScheduling(operation->GetId());

        operation->SetSuspended(false);
        DoSetOperationAlert(operation->GetId(), EOperationAlertType::OperationSuspended, TError());

        YT_LOG_INFO("Operation resumed (OperationId: %v)",
            operation->GetId());

        return MasterConnector_->FlushOperationNode(operation);
    }

    TFuture<void> CompleteOperation(
        const TOperationPtr& operation,
        const TError& error,
        const std::string& user)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        WaitFor(ValidateOperationAccess(user, operation->GetId(), EPermissionSet(EPermission::Manage)))
            .ThrowOnError();

        if (operation->IsFinishingState() || operation->IsFinishedState()) {
            YT_LOG_INFO(error, "Operation is already shutting down (OperationId: %v, State: %v)",
                operation->GetId(),
                operation->GetState());
            return operation->GetFinished();
        }

        if (operation->GetState() != EOperationState::Running) {
            return MakeFuture(TError(NScheduler::EErrorCode::InvalidOperationState,
                "Operation is in %Qlv state",
                operation->GetState()));
        }

        YT_LOG_INFO(error, "Completing operation (OperationId: %v, State: %v)",
            operation->GetId(),
            operation->GetState());

        DoSetOperationAlert(
            operation->GetId(),
            EOperationAlertType::OperationCompletedByUserRequest,
            TError("Operation completed by user request")
                << TErrorAttribute("user", user));

        const auto& controller = operation->GetController();
        auto completeError = WaitFor(controller->Complete());
        if (!completeError.IsOK()) {
            THROW_ERROR_EXCEPTION("Failed to complete operation %v", operation->GetId())
                << completeError;
        }

        return operation->GetFinished();
    }

    void OnOperationCompleted(const TOperationPtr& operation)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        operation->GetCancelableControlInvoker()->Invoke(
            BIND(&TImpl::DoCompleteOperation, MakeStrong(this), operation));
    }

    void OnOperationAborted(const TOperationPtr& operation, const TError& error)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        operation->GetCancelableControlInvoker()->Invoke(
            BIND(&TImpl::DoAbortOperation, MakeStrong(this), operation, error));
    }

    void OnOperationFailed(const TOperationPtr& operation, const TError& error)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        operation->GetCancelableControlInvoker()->Invoke(
            BIND(&TImpl::DoFailOperation, MakeStrong(this), operation, error));
    }

    void OnOperationSuspended(const TOperationPtr& operation, const TError& error)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        operation->GetCancelableControlInvoker()->Invoke(BIND(
            &TImpl::DoSuspendOperation,
            MakeStrong(this),
            operation,
            error,
            /*abortRunningAllocations*/ true,
            /*setAlert*/ true));
    }

    void OnOperationAgentUnregistered(const TOperationPtr& operation)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        if (operation->GetUnregistering()) {
            // Operation marked as completed, but unregistration in controller agent has not processed yet.
            return;
        } else if (operation->IsFinishedState()) {
            // Operation marked as completed, but it may be not persisted to Cypress.
            // We should perform operation revival in this case.
        }

        const auto& controller = operation->GetController();
        if (!controller->RevokeAgent()) {
            // Agent has already been revoked.
            return;
        }

        Strategy_->DisableOperation(operation.Get());

        operation->Restart(TError("Agent unregistered"));
        operation->SetStateAndEnqueueEvent(EOperationState::Orphaned);
        AdvanceEpoch(operation->ControllerEpoch());

        NodeManager_->StartOperationRevival(operation->GetId(), operation->ControllerEpoch());

        AddOperationToTransientQueue(operation);
    }

    void OnOperationBannedInTentativeTree(
        const TOperationPtr& operation,
        const TString& treeId,
        const std::vector<TAllocationId>& allocationIds)
    {
        YT_LOG_INFO(
            "Operation banned in tentative tree (OperationId: %v, TreeId: %v)",
            operation->GetId(),
            treeId);

        auto error = TError("Allocation was in banned tentative pool tree")
            << TErrorAttribute("abort_reason", EAbortReason::BannedInTentativeTree);
        NodeManager_->AbortAllocations(allocationIds, error, EAbortReason::BannedInTentativeTree);

        LogEventFluently(&SchedulerStructuredLogger(), ELogEventType::OperationBannedInTree)
            .Item("operation_id").Value(operation->GetId())
            .Item(EventLogPoolTreeKey).Value(treeId);

        GetControlInvoker(EControlQueue::Operation)->Invoke(
            BIND(&TImpl::UnregisterOperationFromTreeForBannedTree, MakeStrong(this), operation, treeId));
    }

    void UnregisterOperationFromTreeForBannedTree(const TOperationPtr& operation, const TString& treeId)
    {
        const auto& schedulingOptionsPerPoolTree = operation->GetRuntimeParameters()->SchedulingOptionsPerPoolTree;
        if (schedulingOptionsPerPoolTree.find(treeId) != schedulingOptionsPerPoolTree.end()) {
            UnregisterOperationFromTrees(operation, {treeId});
        } else {
            YT_LOG_INFO("Operation was already unregistered from tree (OperationId: %v, TreeId: %v)",
                operation->GetId(),
                treeId);
        }
    }

    void UnregisterOperationFromTrees(const TOperationPtr& operation, const std::vector<TString>& treeIds)
    {
        for (const auto& treeId : treeIds) {
            YT_LOG_INFO("Unregistering operation from tree (OperationId: %v, TreeId: %v)",
                operation->GetId(),
                treeId);

            Strategy_->UnregisterOperationFromTree(operation->GetId(), treeId);
        }

        operation->EraseTrees(treeIds);
    }

    void ValidateOperationRuntimeParametersUpdate(
        const TOperationPtr& operation,
        const TOperationRuntimeParametersUpdatePtr& update)
    {
        // TODO(renadeen): Remove this someday.
        if (!Config_->PoolChangeIsAllowed) {
            if (update->Pool) {
                THROW_ERROR_EXCEPTION("Pool updates temporary disabled");
            }
            for (const auto& [treeId, schedulingOptions] : update->SchedulingOptionsPerPoolTree) {
                if (schedulingOptions->Pool) {
                    THROW_ERROR_EXCEPTION("Pool updates temporary disabled");
                }
            }
        }

        {
            const auto& shells = operation->Spec()->JobShells;
            THashSet<TString> jobShellNames;
            for (const auto& shell : shells) {
                jobShellNames.insert(shell->Name);
            }
            for (const auto& [jobShellName, options] : update->OptionsPerJobShell) {
                if (!jobShellNames.contains(jobShellName)) {
                    THROW_ERROR_EXCEPTION("Job shell is not specified in operation")
                        << TErrorAttribute("job_shell", jobShellName);
                }
            }
        }

        bool hasAcl = update->Acl || !operation->GetRuntimeParameters()->Acl.Entries.empty();
        bool hasAcoName = update->AcoName || operation->GetRuntimeParameters()->AcoName;
        if (hasAcl && hasAcoName) {
            THROW_ERROR_EXCEPTION(NScheduler::EErrorCode::CannotUseBothAclAndAco, "Cannot use both ACL and ACO name");
        }

        // NB(eshcherbin): We don't want to allow operation pool changes during materialization or revival
        // because we rely on them being unchanged in |FinishOperationMaterialization|.
        auto state = operation->GetState();
        if (state == EOperationState::Materializing || state == EOperationState::RevivingJobs) {
            THROW_ERROR_EXCEPTION("Operation runtime parameters update is forbidden while "
                                  "operation is in materializing or reviving jobs state");
        }
    }

    void DoUpdateOperationParameters(
        TOperationPtr operation,
        const std::string& user,
        INodePtr parameters)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        auto update = ConvertTo<TOperationRuntimeParametersUpdatePtr>(parameters);

        WaitFor(ValidateOperationAccess(user, operation->GetId(), update->GetRequiredPermissions()))
            .ThrowOnError();

        for (const auto& [jobShellName, _] : update->OptionsPerJobShell) {
            WaitFor(ValidateJobShellAccess(user, jobShellName, operation->GetJobShellOwners(jobShellName)))
                .ThrowOnError();
        }

        if (update->Acl.has_value()) {
            update->Acl->Entries.insert(
                update->Acl->Entries.end(),
                operation->BaseAcl().Entries.begin(),
                operation->BaseAcl().Entries.end());
        }

        // Perform asynchronous validation of the new runtime parameters.
        {
            ValidateOperationRuntimeParametersUpdate(operation, update);
            auto newParams = UpdateRuntimeParameters(operation->GetRuntimeParameters(), update, operation->GetAuthenticatedUser());
            WaitFor(Strategy_->ValidateOperationRuntimeParameters(operation.Get(), newParams, /*validatePools*/ update->ContainsPool()))
                .ThrowOnError();
            if (auto delay = operation->Spec()->TestingOperationOptions->DelayInsideValidateRuntimeParameters) {
                TDelayedExecutor::WaitForDuration(*delay);
            }
        }

        // We recalculate params, since original runtime params may change during asynchronous validation.
        auto newParams = UpdateRuntimeParameters(operation->GetRuntimeParameters(), update, operation->GetAuthenticatedUser());
        if (update->ContainsPool()) {
            Strategy_->ValidatePoolLimitsOnPoolChange(operation.Get(), newParams);
        }
        operation->SetRuntimeParameters(newParams);
        Strategy_->ApplyOperationRuntimeParameters(operation.Get());

        // Updating ACL and other attributes.
        WaitFor(MasterConnector_->FlushOperationNode(operation))
            .ThrowOnError();

        if (auto controller = operation->GetController()) {
            WaitFor(controller->UpdateRuntimeParameters(update))
                .ThrowOnError();
        }

        LogEventFluently(&SchedulerStructuredLogger(), ELogEventType::RuntimeParametersInfo)
            .Item("operation_id").Value(operation->GetId())
            .Item("authenticated_user").Value(user)
            .Item("runtime_parameters").Value(newParams);

        YT_LOG_INFO("Operation runtime parameters updated (OperationId: %v)",
            operation->GetId());
    }

    TFuture<void> UpdateOperationParameters(
        const TOperationPtr& operation,
        const std::string& user,
        INodePtr parameters)
    {
        return BIND(&TImpl::DoUpdateOperationParameters, MakeStrong(this), operation, user, std::move(parameters))
            .AsyncVia(operation->GetCancelableControlInvoker())
            .Run();
    }

    void DoPatchOperationSpec(
        TOperationPtr operation,
        const std::string& user,
        TSpecPatchList patches)
    {
        if (operation->PatchSpecInProgress()) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::TransientFailure,
                "Cannot patch spec while another patch is in progress")
                << TErrorAttribute("concurrent_patch_spec_user", operation->PatchSpecInProgress()->User)
                << TErrorAttribute("concurrent_patch_spec_start_time", operation->PatchSpecInProgress()->StartTime);
        }

        operation->PatchSpecInProgress().emplace(TPatchSpecInProgressInfo{
            .User = user,
            .StartTime = TInstant::Now(),
        });

        auto resetInProgress = Finally([&] {
            operation->PatchSpecInProgress().reset();
        });

        auto controller = operation->GetController();
        THROW_ERROR_EXCEPTION_UNLESS(
            controller,
            NScheduler::EErrorCode::OperationHasNoController,
            "Controller agent is not assigned to operation %v",
            operation->GetId());

        THROW_ERROR_EXCEPTION_UNLESS(
            operation->ControllerAttributes().InitializeAttributes,
            "Operation %v is not initialized yet",
            operation->GetId());

        THROW_ERROR_EXCEPTION_UNLESS(
            operation->GetState() == EOperationState::Running,
            "Operation %v is not running",
            operation->GetId());

        auto newCumulativePatch = operation->CumulativeSpecPatch()
            ? CloneNode(operation->CumulativeSpecPatch())
            : GetEphemeralNodeFactory()->CreateMap();

        for (const auto& patch : patches) {
            SetNodeByYPath(newCumulativePatch, patch->Path, patch->Value, /*force*/ true);
        }

        // Validate that the new structure is not too deep.
        Y_UNUSED(ConvertToYsonStringNestingLimited(newCumulativePatch, MasterConnector_->GetYsonNestingLevelLimit()));

        auto maybeDelay = [&] (auto sleepType) {
            const auto& protocolOptions = operation->Spec()->TestingOperationOptions->PatchSpecProtocol;
            if (protocolOptions) {
                MaybeDelay((*protocolOptions).*sleepType);
            }
        };

        // (2PC-like) Prepare.
        WaitFor(controller->PatchSpec(newCumulativePatch, /*dryRun*/ true))
            .ThrowOnError();

        maybeDelay(&TPatchSpecProtocolTestingOptions::DelayBeforeCypressFlush);

        // (2PC-like) Coordinator eager commit.
        operation->MutableCumulativeSpecPatch() = newCumulativePatch;
        operation->SetShouldFlushSpecPatch(true);
        WaitFor(MasterConnector_->FlushOperationNode(operation))
            .ThrowOnError();

        maybeDelay(&TPatchSpecProtocolTestingOptions::DelayBeforeApply);

        // (2PC-like) Participant commit.
        WaitFor(controller->PatchSpec(newCumulativePatch, /*dryRun*/ false))
            .ThrowOnError();
    }

    TFuture<void> PatchOperationSpec(
        TOperationPtr operation,
        const std::string& user,
        TSpecPatchList patches)
    {
        return BIND(&TImpl::DoPatchOperationSpec, MakeStrong(this), operation, user, Passed(std::move(patches)))
            .AsyncVia(operation->GetCancelableControlInvoker())
            .Run();
    }

    TFuture<TNodeDescriptor> GetAllocationNode(TAllocationId allocationId) const
    {
        return NodeManager_->GetAllocationNode(allocationId);
    }

    void ProcessNodeHeartbeat(const TCtxNodeHeartbeatPtr& context)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        NodeManager_->ProcessNodeHeartbeat(context);
    }

    // ISchedulerStrategyHost implementation
    TJobResources GetResourceLimits(const TSchedulingTagFilter& filter) const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return NodeManager_->GetResourceLimits(filter);
    }

    TJobResources GetResourceUsage(const TSchedulingTagFilter& filter) const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return NodeManager_->GetResourceUsage(filter);
    }

    void MarkOperationAsRunningInStrategy(TOperationId operationId) override
    {
        auto operation = GetOperation(operationId);

        if (operation->IsRunningInStrategy()) {
            // Operation is already marked as schedulable by strategy.
            return;
        }

        auto codicilGuard = operation->MakeCodicilGuard();

        DoSetOperationAlert(operationId, EOperationAlertType::OperationPending, TError());

        operation->SetRunningInStrategy();

        TryStartOperationMaterialization(operation);
    }

    void AbortOperation(TOperationId operationId, const TError& error) override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto operation = GetOperation(operationId);

        DoAbortOperation(operation, error);
    }

    void FlushOperationNode(TOperationId operationId) override
    {
        auto operation = GetOperation(operationId);

        Y_UNUSED(MasterConnector_->FlushOperationNode(operation));
    }

    void TryStartOperationMaterialization(const TOperationPtr& operation)
    {
        if (operation->GetState() != EOperationState::Pending || !operation->IsRunningInStrategy()) {
            // Operation can be in finishing or initializing state or can be pending by strategy.
            return;
        }

        YT_LOG_INFO("Materializing operation (OperationId: %v, RevivedFromSnapshot: %v)",
            operation->GetId(),
            operation->GetRevivedFromSnapshot());

        TFuture<TOperationControllerMaterializeResult> materializationFuture;
        std::vector<TFuture<void>> futures;
        if (operation->GetRevivedFromSnapshot()) {
            operation->SetStateAndEnqueueEvent(EOperationState::RevivingJobs);
            futures.push_back(RegisterAllocationsFromRevivedOperation(operation));
        } else {
            operation->SetStateAndEnqueueEvent(EOperationState::Materializing);
            materializationFuture = operation->GetController()->Materialize();
            futures.push_back(materializationFuture.AsVoid());

            futures.push_back(NodeManager_->ResetOperationRevival(operation));
        }

        // TODO(eshcherbin): Remove config option for this feature and move all logic inside strategy.
        bool scheduleOperationInSingleTree = operation->Spec()->ScheduleInSingleTree && Config_->EnableScheduleInSingleTree;
        if (scheduleOperationInSingleTree) {
            // NB(eshcherbin): We need to make sure that all necessary information is in fair share tree snapshots
            // before choosing the best single tree for this operation during |FinishOperationMaterialization| later.
            futures.push_back(Strategy_->GetFullFairShareUpdateFinished());
        }

        auto expectedState = operation->GetState();
        AllSucceeded(std::move(futures)).Subscribe(
            BIND(&TImpl::FinishOperationMaterialization,
                MakeStrong(this),
                operation,
                expectedState,
                scheduleOperationInSingleTree,
                Passed(std::move(materializationFuture)))
            .Via(operation->GetCancelableControlInvoker()));
    }

    void FinishOperationMaterialization(
        const TOperationPtr& operation,
        EOperationState expectedState,
        bool scheduleOperationInSingleTree,
        TFuture<TOperationControllerMaterializeResult> materializationFuture,
        const TError& error)
    {
        // EOperationState::RevivingJobs or EOperationState::Materializing
        if (operation->GetState() != expectedState) {
            YT_LOG_INFO(
                "Operation state changed during materialization, skip materialization postprocessing "
                "(ActualState: %v, ExpectedState: %v)",
                operation->GetState(),
                expectedState);
            return;
        }

        bool shouldSuspend = false;
        if (materializationFuture) {
            // Async materialize result is ready here as the combined future already has finished.
            YT_VERIFY(materializationFuture.IsSet());

            const auto& materializationResultOrError = materializationFuture.Get();
            if (!materializationResultOrError.IsOK()) {
                YT_LOG_WARNING(materializationResultOrError,
                    "Operation has failed to materialize (OperationId: %v)",
                    operation->GetId());
                auto wrappedError = TError("Operation has failed to materialize")
                    << materializationResultOrError;
                OnOperationFailed(operation, wrappedError);
            } else {
                shouldSuspend = materializationResultOrError.Value().Suspend;
            }
        }

        if (!error.IsOK()) {
            return;
        }

        DoFinishOperationMaterialization(operation, shouldSuspend, scheduleOperationInSingleTree);
    }

    void DoFinishOperationMaterialization(
        const TOperationPtr& operation,
        bool shouldSuspend,
        // This option must have the same value as at materialization start.
        bool scheduleOperationInSingleTree)
    {
        MaybeDelay(operation->Spec()->TestingOperationOptions->DelayInsideMaterializeScheduler);

        std::vector<TString> treeIdsToErase;
        auto error = Strategy_->OnOperationMaterialized(
            operation->GetId(),
            scheduleOperationInSingleTree,
            operation->GetController()->GetNeededResources(),
            operation->GetRevivedFromSnapshot(),
            &treeIdsToErase);

        if (!error.IsOK()) {
            OnOperationFailed(operation, error);
            return;
        }

        if (!treeIdsToErase.empty()) {
            operation->EraseTrees(treeIdsToErase);

            // NB(eshcherbin): Persist info about erased trees and min needed resources to master. This flush is safe because nothing should
            // happen to |operation| until its state is set to EOperationState::Running. The only possible exception would be the case when
            // materialization fails and the operation is terminated, but we've already checked for any fail beforehand.
            // Result is ignored since failure causes scheduler disconnection.
            auto expectedState = operation->GetState();
            Y_UNUSED(WaitFor(MasterConnector_->FlushOperationNode(operation)));
            if (operation->GetState() != expectedState) {
                return;
            }
        }

        operation->SetStateAndEnqueueEvent(EOperationState::Running);
        Strategy_->EnableOperation(operation.Get());

        if (shouldSuspend) {
            DoSuspendOperation(
                operation,
                TError("Operation suspended due to suspend_operation_after_materialization spec option"),
                /*abortRunningAllocations*/ false,
                /*setAlert*/ false);
        }

        LogEventFluently(&SchedulerStructuredLogger(), ELogEventType::OperationMaterialized)
            .Item("operation_id").Value(operation->GetId());
    }

    IInvokerPtr GetControlInvoker(EControlQueue queue) const override
    {
        return Bootstrap_->GetControlInvoker(queue);
    }

    IInvokerPtr GetFairShareLoggingInvoker() const override
    {
        return EventLoggingActionQueue_->GetInvoker();
    }

    IInvokerPtr GetFairShareProfilingInvoker() const override
    {
        return FairShareProfilingActionQueue_->GetInvoker();
    }

    IInvokerPtr GetFairShareUpdateInvoker() const override
    {
        return FairShareUpdatePool_->GetInvoker();
    }

    IInvokerPtr GetBackgroundInvoker() const override
    {
        return BackgroundThreadPool_->GetInvoker();
    }

    IInvokerPtr GetOperationsCleanerInvoker() const override
    {
        return OperationsCleanerActionQueue_->GetInvoker();
    }

    IInvokerPtr GetOrchidWorkerInvoker() const override
    {
        return OrchidWorkerPool_->GetInvoker();
    }

    int GetNodeShardId(NNodeTrackerClient::TNodeId nodeId) const override
    {
        return NodeManager_->GetNodeShardId(nodeId);
    }

    const std::vector<IInvokerPtr>& GetNodeShardInvokers() const override
    {
        return NodeManager_->GetNodeShardInvokers();
    }

    IYsonConsumer* GetControlEventLogConsumer()
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        return ControlEventLogWriterConsumer_.get();
    }

    IYsonConsumer* GetFairShareEventLogConsumer()
    {
        YT_ASSERT_INVOKER_AFFINITY(GetFairShareLoggingInvoker());

        return OffloadedEventLogWriterConsumer_.get();
    }

    IYsonConsumer* GetEventLogConsumer() override
    {
        // By default, the control thread's consumer is used.
        return GetControlEventLogConsumer();
    }

    // NB(eshcherbin): This version of the method is deprecated in scheduler.
    // You should pass the logger explicitly. See: YT-15900.
    TFluentLogEvent LogEventFluently(ELogEventType /*eventType*/) override
    {
        YT_ABORT();
    }

    const NLogging::TLogger* GetEventLogger() override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return &SchedulerEventLogger();
    }

    void LogResourceMetering(
        const TMeteringKey& key,
        const TMeteringStatistics& statistics,
        const THashMap<TString, TString>& otherTags,
        TInstant connectionTime,
        TInstant previousLogTime,
        TInstant currentTime) override
    {
        if (!ClusterName_) {
            return;
        }

        auto buildCommonLogEventPart = [&] (const TString& schema, i64 usageQuantity, TInstant startTime, TInstant finishTime) {
            return NLogging::LogStructuredEventFluently(SchedulerResourceMeteringLogger(), NLogging::ELogLevel::Info)
                .Item("schema").Value(schema)
                .Item("id").Value(TGuid::Create())
                .Item("abc_id").Value(key.AbcId)
                .Item("usage").BeginMap()
                    .Item("quantity").Value(usageQuantity)
                    .Item("unit").Value("milliseconds")
                    .Item("start").Value(startTime.Seconds())
                    .Item("finish").Value(finishTime.Seconds())
                .EndMap()
                .Item("labels").BeginMap()
                    .Item("pool_tree").Value(key.TreeId)
                    .Item("pool").Value(key.PoolId)
                .EndMap()
                .Item("version").Value("1")
                .Item("source_wt").Value((finishTime - TInstant()).Seconds());
        };

        if (Config_->ResourceMetering->EnableSeparateSchemaForAllocation) {
            for (auto [startTime, finishTime] : SplitTimeIntervalByHours(previousLogTime, currentTime)) {
                auto usageQuantity = (finishTime - startTime).MilliSeconds();
                buildCommonLogEventPart("yt.scheduler.pools.compute_guarantee.v1", usageQuantity, startTime, finishTime)
                    .Item("tags").BeginMap()
                        .Item("strong_guarantee_resources").Value(statistics.StrongGuaranteeResources())
                        .Item("resource_flow").Value(statistics.ResourceFlow())
                        .Item("burst_guarantee_resources").Value(statistics.BurstGuaranteeResources())
                        .Item("cluster").Value(ClusterName_)
                        .Item("gpu_type").Value(GuessGpuType(key.TreeId))
                        .DoFor(otherTags, [] (TFluentMap fluent, const std::pair<TString, TString>& pair) {
                            fluent.Item(pair.first).Value(pair.second);
                        })
                        .DoFor(key.MeteringTags, [] (TFluentMap fluent, const std::pair<TString, TString>& pair) {
                            fluent.Item(pair.first).Value(pair.second);
                        })
                    .EndMap();
                GuaranteesMeteringRecordCountCounter_.Increment();
                GuaranteesMeteringUsageQuantityCounter_.Increment(usageQuantity);
            }

            auto usageLogStartTime = std::max(previousLogTime, connectionTime);
            auto usageDuration = (currentTime - usageLogStartTime).SecondsFloat();
            auto averageAllocatedResources = statistics.AccumulatedResourceUsage() / usageDuration;
            for (auto [startTime, finishTime] : SplitTimeIntervalByHours(usageLogStartTime, currentTime)) {
                double timeRatio = (finishTime - startTime).SecondsFloat() / usageDuration;
                auto usageQuantity = (finishTime - startTime).MilliSeconds();
                buildCommonLogEventPart("yt.scheduler.pools.compute_allocation.v1", usageQuantity, startTime, finishTime)
                    .Item("tags").BeginMap()
                        .Item("allocated_resources").Value(averageAllocatedResources * timeRatio)
                        .Item("cluster").Value(ClusterName_)
                        .Item("gpu_type").Value(GuessGpuType(key.TreeId))
                        .DoFor(otherTags, [] (TFluentMap fluent, const std::pair<TString, TString>& pair) {
                            fluent.Item(pair.first).Value(pair.second);
                        })
                        .DoFor(key.MeteringTags, [] (TFluentMap fluent, const std::pair<TString, TString>& pair) {
                            fluent.Item(pair.first).Value(pair.second);
                        })
                    .EndMap();
                AllocationMeteringRecordCountCounter_.Increment();
                AllocationMeteringUsageQuantityCounter_.Increment(usageQuantity);
            }
        } else {
            for (auto [startTime, finishTime] : SplitTimeIntervalByHours(previousLogTime, currentTime)) {
                auto usageQuantity = (finishTime - startTime).MilliSeconds();
                buildCommonLogEventPart("yt.scheduler.pools.compute.v1", usageQuantity, startTime, finishTime)
                    .Item("tags").BeginMap()
                        .Item("strong_guarantee_resources").Value(statistics.StrongGuaranteeResources())
                        .Item("resource_flow").Value(statistics.ResourceFlow())
                        .Item("burst_guarantee_resources").Value(statistics.BurstGuaranteeResources())
                        .Item("allocated_resources").Value(statistics.AllocatedResources())
                        .Item("cluster").Value(ClusterName_)
                        .Item("gpu_type").Value(GuessGpuType(key.TreeId))
                        .DoFor(otherTags, [] (TFluentMap fluent, const std::pair<TString, TString>& pair) {
                            fluent.Item(pair.first).Value(pair.second);
                        })
                        .DoFor(key.MeteringTags, [] (TFluentMap fluent, const std::pair<TString, TString>& pair) {
                            fluent.Item(pair.first).Value(pair.second);
                        })
                    .EndMap();
                MeteringRecordCountCounter_.Increment();
                MeteringUsageQuantityCounter_.Increment(usageQuantity);
            }
        }
    }

    int GetDefaultAbcId() const override
    {
        return Config_->ResourceMetering->DefaultAbcId;
    }

    // NB(eshcherbin): Separate method due to separate invoker.
    TFluentLogEvent LogFairShareEventFluently(TInstant now) override
    {
        YT_ASSERT_INVOKER_AFFINITY(GetFairShareLoggingInvoker());

        return LogEventFluently(
            &SchedulerEventLogger(),
            GetFairShareEventLogConsumer(),
            ELogEventType::FairShareInfo,
            now);
    }

    // NB(eshcherbin): Separate method due to separate invoker.
    TFluentLogEvent LogAccumulatedUsageEventFluently(TInstant now) override
    {
        YT_ASSERT_INVOKER_AFFINITY(GetFairShareLoggingInvoker());

        return LogEventFluently(
            &SchedulerStructuredLogger(),
            GetFairShareEventLogConsumer(),
            ELogEventType::AccumulatedUsageInfo,
            now);
    }

    std::optional<int> FindMediumIndexByName(const TString& mediumName) const override
    {
        const auto& mediumDirectory = Bootstrap_
            ->GetClient()
            ->GetNativeConnection()
            ->GetMediumDirectory();
        const auto* descriptor = mediumDirectory->FindByName(mediumName);
        return descriptor ? std::optional(descriptor->Index) : std::nullopt;
    }

    const TString& GetMediumNameByIndex(int mediumIndex) const override
    {
        const auto& mediumDirectory = Bootstrap_
            ->GetClient()
            ->GetNativeConnection()
            ->GetMediumDirectory();
        const auto* descriptor = mediumDirectory->FindByIndex(mediumIndex);
        return descriptor->Name;
    }

    const ISchedulerStrategyPtr& GetStrategy() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return Strategy_;
    }

    const TOperationsCleanerPtr& GetOperationsCleaner() const
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return OperationsCleaner_;
    }

    int GetOperationsArchiveVersion() const final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return OperationsArchiveVersion_.load();
    }

    TSerializableAccessControlList GetOperationBaseAcl() const
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        YT_VERIFY(OperationBaseAcl_.has_value());

        return *OperationBaseAcl_;
    }

    TString FormatResources(const TJobResourcesWithQuota& resources) const override
    {
        return NScheduler::FormatResources(resources);
    }

    void SerializeResources(const TJobResourcesWithQuota& resources, IYsonConsumer* consumer) const override
    {
        auto mediumDirectory = Bootstrap_
            ->GetClient()
            ->GetNativeConnection()
            ->GetMediumDirectory();
        SerializeJobResourcesWithQuota(resources, mediumDirectory, consumer);
    }

    void SerializeDiskQuota(const TDiskQuota& diskQuota, NYson::IYsonConsumer* consumer) const override
    {
        auto mediumDirectory = Bootstrap_
            ->GetClient()
            ->GetNativeConnection()
            ->GetMediumDirectory();
        NScheduler::SerializeDiskQuota(diskQuota, mediumDirectory, consumer);
    }

    TString FormatHeartbeatResourceUsage(
        const TJobResources& usage,
        const TJobResources& limits,
        const NNodeTrackerClient::NProto::TDiskResources& diskResources) const override
    {
        THashMap<int, std::vector<i64>> mediumIndexToFreeResources;
        for (const auto& locationResources : diskResources.disk_location_resources()) {
            int mediumIndex = locationResources.medium_index();
            mediumIndexToFreeResources[mediumIndex].push_back(locationResources.limit() - locationResources.usage());
        }

        auto mediumDirectory = Bootstrap_
            ->GetClient()
            ->GetNativeConnection()
            ->GetMediumDirectory();

        return Format("{%v, FreeDiskResources: %v}",
            NScheduler::FormatResourceUsage(usage, limits),
            MakeFormattableView(mediumIndexToFreeResources, [&mediumDirectory] (TStringBuilderBase* builder, const std::pair<int, std::vector<i64>>& pair) {
                int mediumIndex = pair.first;
                const auto& freeDiskSpace = pair.second;
                auto* mediumDescriptor = mediumDirectory->FindByIndex(mediumIndex);
                TStringBuf mediumName = mediumDescriptor
                    ? mediumDescriptor->Name
                    : TStringBuf("unknown");
                builder->AppendFormat("%v: %v", mediumName, freeDiskSpace);
            }));
    }

    void InvokeStoringStrategyState(TPersistentStrategyStatePtr strategyState) override
    {
        MasterConnector_->InvokeStoringStrategyState(std::move(strategyState));
    }

    TFuture<void> UpdateLastMeteringLogTime(TInstant time) override
    {
        if (Config_->UpdateLastMeteringLogTime) {
            return MasterConnector_->UpdateLastMeteringLogTime(time);
        } else {
            return VoidFuture;
        }
    }

    TFuture<TOperationId> FindOperationIdByAllocationId(TAllocationId allocationId) const
    {
        return NodeManager_->FindOperationIdByAllocationId(allocationId);
    }

    const IResponseKeeperPtr& GetOperationServiceResponseKeeper() const
    {
        return OperationServiceResponseKeeper_;
    }

    TAllocationBriefInfo GetAllocationBriefInfo(
        TAllocationId allocationId,
        TAllocationInfoToRequest requestedAllocationInfo) const
    {
        TAllocationBriefInfo result;
        result.AllocationId = allocationId;

        if (requestedAllocationInfo.NodeDescriptor) {
            result.NodeDescriptor = WaitFor(GetAllocationNode(allocationId))
                .ValueOrThrow();
        }

        if (!requestedAllocationInfo.OperationId &&
            !requestedAllocationInfo.OperationAcl &&
            !requestedAllocationInfo.ControllerAgentDescriptor)
        {
            return result;
        }

        auto operationId = WaitFor(NodeManager_->FindOperationIdByAllocationId(allocationId))
            .ValueOrThrow();

        if (!operationId) {
            THROW_ERROR_EXCEPTION(
                NScheduler::EErrorCode::NoSuchAllocation,
                "Allocation %v not found",
                allocationId);
        }

        auto operation = GetOperationOrThrow(operationId);

        if (requestedAllocationInfo.OperationId) {
            result.OperationId = operationId;
        }

        if (requestedAllocationInfo.ControllerAgentDescriptor) {
            const auto& controllerAgent = operation->GetController()->FindAgent();

            if (!controllerAgent) {
                THROW_ERROR_EXCEPTION(
                    NScheduler::EErrorCode::AgentRevoked,
                    "Agent of operation %v is revoked",
                    operationId);
            }

            result.ControllerAgentDescriptor = NControllerAgent::TControllerAgentDescriptor{
                .Addresses = controllerAgent->GetAgentAddresses(),
                .IncarnationId = controllerAgent->GetIncarnationId(),
                .AgentId = controllerAgent->GetId(),
            };
        }

        if (requestedAllocationInfo.OperationAcl) {
            result.OperationAcl = operation->GetRuntimeParameters()->Acl;
        }
        if (requestedAllocationInfo.OperationAcoName) {
            result.OperationAcoName = operation->GetRuntimeParameters()->AcoName;
        }

        return result;
    }

private:
    TSchedulerConfigPtr Config_;
    const TSchedulerConfigPtr InitialConfig_;
    int ConfigRevision_ = 0;

    TBootstrap* const Bootstrap_;

    NYTree::INodePtr SpecTemplate_;

    const std::unique_ptr<TMasterConnector> MasterConnector_;
    std::atomic<bool> Connected_ = false;

    TOperationsCleanerPtr OperationsCleaner_;

    const IThreadPoolPtr OrchidWorkerPool_;
    const TActionQueuePtr EventLoggingActionQueue_ = New<TActionQueue>("EventLogging");
    const TActionQueuePtr FairShareProfilingActionQueue_ = New<TActionQueue>("FSProfiling");
    const TActionQueuePtr OperationsCleanerActionQueue_ = New<TActionQueue>("OpsCleaner");
    const IThreadPoolPtr FairShareUpdatePool_;
    const IThreadPoolPtr BackgroundThreadPool_;

    IResponseKeeperPtr OperationServiceResponseKeeper_;

    std::optional<TString> ClusterName_;

    const TNodeManagerPtr NodeManager_;

    ISchedulerStrategyPtr Strategy_;

    struct TOperationAlias
    {
        //! Id of an operation assigned to a given alias.
        TOperationId OperationId;
        //! Operation assigned to a given alias. May be nullptr if operation has already completed.
        //! (in this case we still remember the operation id, though).
        TOperationPtr Operation;
    };

    THashMap<TOperationId, TOperationPtr> IdToOperation_;
    THashMap<TString, TOperationAlias> OperationAliases_;
    THashMap<TOperationId, IYPathServicePtr> IdToOperationService_;

    THashMap<TOperationId, TOperationPtr> IdToStartingOperation_;

    TAtomicIntrusivePtr<TRefCountedExecNodeDescriptorMap> CachedExecNodeDescriptors_{New<TRefCountedExecNodeDescriptorMap>()};

    struct TRefCountedExecNodeDescriptorList
        : public TRefCounted
    {
        explicit TRefCountedExecNodeDescriptorList(TSharedRef execNodeDescriptors)
            : ExecNodeDescriptors(std::move(execNodeDescriptors))
        { }

        TSharedRef ExecNodeDescriptors;
    };
    TAtomicIntrusivePtr<TRefCountedExecNodeDescriptorList> CachedSerializedExecNodeDescriptors_;

    TIntrusivePtr<TSyncExpiringCache<TSchedulingTagFilter, TMemoryDistribution>> CachedExecNodeMemoryDistributionByTags_;

    TJobResourcesProfiler TotalResourceLimitsProfiler_;
    TJobResourcesProfiler TotalResourceUsageProfiler_;

    NProfiling::TCounter MeteringRecordCountCounter_;
    NProfiling::TCounter MeteringUsageQuantityCounter_;

    NProfiling::TCounter AllocationMeteringRecordCountCounter_;
    NProfiling::TCounter AllocationMeteringUsageQuantityCounter_;
    NProfiling::TCounter GuaranteesMeteringRecordCountCounter_;
    NProfiling::TCounter GuaranteesMeteringUsageQuantityCounter_;

    TPeriodicExecutorPtr ProfilingExecutor_;
    TPeriodicExecutorPtr ClusterInfoLoggingExecutor_;
    TPeriodicExecutorPtr NodesInfoLoggingExecutor_;
    TPeriodicExecutorPtr UpdateExecNodeDescriptorsExecutor_;
    TPeriodicExecutorPtr JobReporterWriteFailuresChecker_;
    TPeriodicExecutorPtr StrategyStuckOperationsChecker_;
    TPeriodicExecutorPtr ExperimentAssignmentErrorChecker_;
    TPeriodicExecutorPtr OrphanedOperationQueueScanPeriodExecutor_;
    TPeriodicExecutorPtr TransientOperationQueueScanPeriodExecutor_;
    TPeriodicExecutorPtr PendingByPoolOperationScanPeriodExecutor_;

    std::string ServiceAddress_;

    struct TOperationProgress
    {
        NYson::TYsonString Progress;
        NYson::TYsonString BriefProgress;
        NYson::TYsonString Alerts;
    };

    IEventLogWriterPtr EventLogWriter_;
    std::unique_ptr<IYsonConsumer> ControlEventLogWriterConsumer_;
    std::unique_ptr<IYsonConsumer> OffloadedEventLogWriterConsumer_;

    std::atomic<int> OperationsArchiveVersion_ = -1;

    TEnumIndexedArray<EOperationState, std::vector<TOperationPtr>> StateToTransientOperations_;
    TInstant OperationToAgentAssignmentFailureTime_;

    std::optional<NSecurityClient::TSerializableAccessControlList> OperationBaseAcl_;

    TIntrusivePtr<NYTree::ICachedYPathService> StaticOrchidService_;
    TIntrusivePtr<NYTree::TServiceCombiner> CombinedOrchidService_;

    THashMap<TString, TString> UserToDefaultPoolMap_;

    TExperimentAssigner ExperimentsAssigner_;
    TError LastExperimentAssignmentError_;
    TInstant LastExperimentAssignmentErrorTime_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    void InitOperationRuntimeParameters(
        const TOperationRuntimeParametersPtr& runtimeParameters,
        const TOperationSpecBasePtr& spec,
        const TSerializableAccessControlList& baseAcl,
        const std::string& user,
        EOperationType operationType,
        TOperationId operationId)
    {
        if (spec->AcoName) {
            runtimeParameters->AcoName = spec->AcoName;
        } else {
            runtimeParameters->Acl = baseAcl;
            if (spec->Acl) {
                runtimeParameters->Acl.Entries.insert(
                    runtimeParameters->Acl.Entries.end(),
                    spec->Acl->Entries.begin(),
                    spec->Acl->Entries.end());
            }
        }

        auto annotations = spec->Annotations;
        auto description = spec->Description;
        if (description) {
            if (!annotations) {
                annotations = GetEphemeralNodeFactory()->CreateMap();
            }
            annotations->AddChild("description", description);
        }
        runtimeParameters->Annotations = annotations;

        runtimeParameters->ControllerAgentTag = spec->ControllerAgentTag;

        Strategy_->InitOperationRuntimeParameters(runtimeParameters, spec, user, operationType, operationId);
    }

    TOperationRuntimeParametersPtr UpdateRuntimeParameters(
        const TOperationRuntimeParametersPtr& origin,
        const TOperationRuntimeParametersUpdatePtr& update,
        const std::string& user)
    {
        YT_VERIFY(origin);
        auto result = CloneYsonStruct(origin);

        if (update->Acl) {
            result->Acl = *update->Acl;
        } else if (update->AcoName) {
            result->AcoName = *update->AcoName;
        }

        ApplyJobShellOptionsUpdate(&result->OptionsPerJobShell, update->OptionsPerJobShell);

        if (update->Annotations) {
            auto annotationsPatch = *update->Annotations;
            if (!result->Annotations) {
                result->Annotations = annotationsPatch;
            } else if (!annotationsPatch) {
                result->Annotations = nullptr;
            } else {
                result->Annotations = NYTree::PatchNode(result->Annotations, annotationsPatch)->AsMap();
            }
        }

        if (update->ControllerAgentTag) {
            result->ControllerAgentTag = *update->ControllerAgentTag;
        }

        Strategy_->UpdateRuntimeParameters(result, update, user);

        return result;
    }

    void DoSetOperationAlert(
        TOperationId operationId,
        EOperationAlertType alertType,
        const TError& alert,
        std::optional<TDuration> timeout = std::nullopt)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto operation = FindOperation(operationId);
        if (!operation) {
            return;
        }

        if (alert.IsOK()) {
            if (operation->HasAlert(alertType)) {
                operation->ResetAlertWithoutArchivation(alertType);
                OperationsCleaner_->EnqueueOperationAlertEvent(operationId, alertType, alert);

                YT_LOG_DEBUG("Operation alert reset (OperationId: %v, Type: %v)",
                    operationId,
                    alertType);
            }
        } else if (operation->SetAlertWithoutArchivation(alertType, alert)) {
            OperationsCleaner_->EnqueueOperationAlertEvent(operationId, alertType, alert);

            // NB: We don't want to recreate reset action if we already have one
            // due to performance issues.
            if (timeout && !operation->HasAlertResetCookie(alertType)) {
                auto resetCallback = BIND(&TImpl::DoSetOperationAlert, MakeStrong(this), operationId, alertType, TError(), std::nullopt)
                    .Via(operation->GetCancelableControlInvoker());
                auto resetCookie = TDelayedExecutor::Submit(resetCallback, *timeout);
                operation->SetAlertResetCookie(alertType, resetCookie);
            }

            YT_LOG_DEBUG(alert, "Operation alert set (OperationId: %v, Type: %v)",
                operationId,
                alertType);
        }
    }

    void OnProfiling()
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        TotalResourceLimitsProfiler_.Update(GetResourceLimits(EmptySchedulingTagFilter));
        TotalResourceUsageProfiler_.Update(GetResourceUsage(EmptySchedulingTagFilter));
    }

    void OnClusterInfoLogging()
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        if (IsConnected()) {
            LogEventFluently(&SchedulerEventLogger(), ELogEventType::ClusterInfo)
                .Item("exec_node_count").Value(NodeManager_->GetExecNodeCount())
                .Item("total_node_count").Value(NodeManager_->GetTotalNodeCount())
                .Item("resource_limits").Value(GetResourceLimits(EmptySchedulingTagFilter))
                .Item("resource_usage").Value(GetResourceUsage(EmptySchedulingTagFilter));
        }
    }

    void OnNodesInfoLogging()
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        if (!IsConnected()) {
            return;
        }

        auto nodeYsonList = NodeManager_->BuildNodeYsonList();
        THashMap<TString, std::vector<TYsonString>> nodeYsonsPerTree;
        for (auto& [nodeId, nodeYson] : nodeYsonList) {
            auto treeId = Strategy_->GetMaybeTreeIdForNode(nodeId).value_or(UnknownTreeId);
            nodeYsonsPerTree[treeId].push_back(std::move(nodeYson));
        }

        auto nodesInfoEventId = TGuid::Create();
        auto now = TInstant::Now();
        for (const auto& [treeId, nodeYsons] : nodeYsonsPerTree) {
            std::vector<TYsonString> splitNodeYsons;
            TYsonMapFragmentBatcher nodesConsumer(&splitNodeYsons, Config_->MaxEventLogNodeBatchSize);
            BuildYsonMapFragmentFluently(&nodesConsumer)
                .DoFor(nodeYsons, [] (TFluentMap fluent, const TYsonString& nodeYson) {
                    fluent.Items(nodeYson);
                });
            nodesConsumer.Flush();

            for (int batchIndex = 0; batchIndex < std::ssize(splitNodeYsons); ++batchIndex) {
                const auto& batch = splitNodeYsons[batchIndex];
                LogEventFluently(&SchedulerEventLogger(), ELogEventType::NodesInfo, now)
                    .Item("nodes_info_event_id").Value(nodesInfoEventId)
                    .Item("nodes_batch_index").Value(batchIndex)
                    .Item("tree_id").Value(treeId)
                    .Item("nodes").BeginMap()
                        .Items(batch)
                    .EndMap();
            }
        }
    }

    void OnMasterConnecting()
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        // NB: We cannot be sure the previous incarnation did a proper cleanup due to possible
        // fiber cancelation.
        DoCleanup();

        // NB: Must start the keeper before registering operations.
        OperationServiceResponseKeeper_->Start();

        OperationsCleaner_->Start();
    }

    void OnMasterHandshake(const TMasterHandshakeResult& result)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        ValidateConfig();

        {
            YT_LOG_INFO("Connecting node manager");

            // TODO(eshcherbin): Rename to OnMasterHandshake?
            NodeManager_->OnMasterConnected(result);
        }

        {
            YT_LOG_INFO("Registering existing operations");

            for (const auto& operation : result.Operations) {
                if (operation->GetMutationId()) {
                    NProto::TRspStartOperation response;
                    ToProto(response.mutable_operation_id(), operation->GetId());
                    auto responseMessage = CreateResponseMessage(response);
                    if (auto setResponseKeeperPromise =
                        OperationServiceResponseKeeper_->EndRequest(operation->GetMutationId(), responseMessage))
                    {
                        setResponseKeeperPromise();
                    }
                }

                // NB: It is valid to reset state, since operation revival descriptor
                // has necessary information about state.
                operation->SetStateAndEnqueueEvent(EOperationState::Orphaned);

                if (operation->Alias()) {
                    RegisterOperationAlias(operation);
                }
                RegisterOperation(operation, /*waitingForRevival*/ true);

                AddOperationToTransientQueue(operation);
            }
        }

        Strategy_->OnMasterHandshake(result);
    }

    void OnMasterConnected()
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        OrphanedOperationQueueScanPeriodExecutor_ = New<TPeriodicExecutor>(
            MasterConnector_->GetCancelableControlInvoker(EControlQueue::OperationsPeriodicActivity),
            BIND(&TImpl::HandleOrphanedOperations, MakeWeak(this)),
            Config_->TransientOperationQueueScanPeriod);
        OrphanedOperationQueueScanPeriodExecutor_->Start();

        TransientOperationQueueScanPeriodExecutor_ = New<TPeriodicExecutor>(
            MasterConnector_->GetCancelableControlInvoker(EControlQueue::OperationsPeriodicActivity),
            BIND(&TImpl::ScanTransientOperationQueue, MakeWeak(this)),
            Config_->TransientOperationQueueScanPeriod);
        TransientOperationQueueScanPeriodExecutor_->Start();

        PendingByPoolOperationScanPeriodExecutor_ = New<TPeriodicExecutor>(
            MasterConnector_->GetCancelableControlInvoker(EControlQueue::OperationsPeriodicActivity),
            BIND(&TImpl::ScanPendingOperations, MakeWeak(this)),
            Config_->PendingByPoolOperationScanPeriod);
        PendingByPoolOperationScanPeriodExecutor_->Start();

        Strategy_->OnMasterConnected();

        TotalResourceLimitsProfiler_.Start();
        TotalResourceUsageProfiler_.Start();

        SchedulerProfiler().AddFuncGauge("/jobs/registered_job_count", MakeStrong(this), [this] {
            return NodeManager_->GetActiveAllocationCount();
        });
        SchedulerProfiler().AddFuncGauge("/jobs/submit_to_strategy_count", MakeStrong(this), [this] {
            return NodeManager_->GetSubmitToStrategyAllocationCount();
        });
        SchedulerProfiler().AddFuncGauge("/total_scheduling_heartbeat_complexity", MakeStrong(this), [this] {
            return NodeManager_->GetTotalConcurrentHeartbeatComplexity();
        });
        SchedulerProfiler().AddFuncGauge("/exec_node_count", MakeStrong(this), [this] {
            return NodeManager_->GetExecNodeCount();
        });
        SchedulerProfiler().AddFuncGauge("/total_node_count", MakeStrong(this), [this] {
            return NodeManager_->GetTotalNodeCount();
        });

        LogEventFluently(&SchedulerStructuredLogger(), ELogEventType::MasterConnected)
            .Item("address").Value(ServiceAddress_);

        YT_LOG_INFO("Master connected for scheduler");
    }

    void DoCleanup()
    {
        TotalResourceLimitsProfiler_.Stop();
        TotalResourceUsageProfiler_.Stop();

        {
            auto error = TError(NScheduler::EErrorCode::MasterDisconnected, "Master disconnected");
            for (const auto& [operationId, operation] : IdToOperation_) {
                if (!operation->IsFinishedState()) {
                    // This awakes those waiting for start promise.
                    SetOperationFinalState(
                        operation,
                        EOperationState::Aborted,
                        error);
                }
                operation->Cancel(error);
            }
            for (const auto& [operationId, operation] : IdToStartingOperation_) {
                YT_VERIFY(!operation->IsFinishedState());
                SetOperationFinalState(
                    operation,
                    EOperationState::Aborted,
                    error);
                operation->Cancel(error);
            }
            OperationAliases_.clear();
            IdToOperation_.clear();
            IdToOperationService_.clear();
            IdToStartingOperation_.clear();
        }

        for (auto& queue : StateToTransientOperations_) {
            queue.clear();
        }

        OperationServiceResponseKeeper_->Stop();

        if (OrphanedOperationQueueScanPeriodExecutor_) {
            YT_UNUSED_FUTURE(OrphanedOperationQueueScanPeriodExecutor_->Stop());
            OrphanedOperationQueueScanPeriodExecutor_.Reset();
        }
        if (TransientOperationQueueScanPeriodExecutor_) {
            YT_UNUSED_FUTURE(TransientOperationQueueScanPeriodExecutor_->Stop());
            TransientOperationQueueScanPeriodExecutor_.Reset();
        }

        if (PendingByPoolOperationScanPeriodExecutor_) {
            YT_UNUSED_FUTURE(PendingByPoolOperationScanPeriodExecutor_->Stop());
            PendingByPoolOperationScanPeriodExecutor_.Reset();
        }

        Strategy_->OnMasterDisconnected();
        OperationsCleaner_->Stop();
    }

    void OnMasterDisconnected()
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        LogEventFluently(&SchedulerStructuredLogger(), ELogEventType::MasterDisconnected)
            .Item("address").Value(ServiceAddress_);

        if (Config_->TestingOptions->MasterDisconnectDelay) {
            Sleep(*Config_->TestingOptions->MasterDisconnectDelay);
        }

        DoCleanup();

        {
            YT_LOG_INFO("Started disconnecting node manager");

            NodeManager_->OnMasterDisconnected();

            YT_LOG_INFO("Finished disconnecting node manager");
        }

        YT_LOG_INFO("Master disconnected for scheduler");
    }

    void ValidateOperationState(const TOperationPtr& operation, EOperationState expectedState)
    {
        if (operation->GetState() != expectedState) {
            YT_LOG_INFO("Operation has unexpected state (OperationId: %v, State: %v, ExpectedState: %v)",
                operation->GetId(),
                operation->GetState(),
                expectedState);
            throw TFiberCanceledException();
        }
    }

    void RequestPoolTrees(TObjectServiceProxy::TReqExecuteBatchPtr batchReq)
    {
        static const TPoolTreeKeysHolder PoolTreeKeysHolder;

        YT_LOG_INFO("Requesting pool trees");

        auto req = TYPathProxy::Get(Config_->PoolTreesRoot);

        ToProto(req->mutable_attributes()->mutable_keys(), PoolTreeKeysHolder.Keys);
        batchReq->AddRequest(req, "get_pool_trees");
    }

    void HandlePoolTrees(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_pool_trees");
        if (!rspOrError.IsOK()) {
            THROW_ERROR(rspOrError.Wrap(NScheduler::EErrorCode::WatcherHandlerFailed, "Error getting pool trees"));
        }

        auto poolTreesYson = TYsonString(rspOrError.Value()->value());
        Strategy_->UpdatePoolTrees(poolTreesYson);
    }

    void RequestNodesAttributes(TObjectServiceProxy::TReqExecuteBatchPtr batchReq)
    {
        YT_LOG_INFO("Requesting exec nodes information");

        auto req = TYPathProxy::List(GetExecNodesPath());
        req->set_limit(CypressNodeLimit);
        ToProto(req->mutable_attributes()->mutable_keys(), std::vector<TString>{
            "id",
            "tags",
            "state",
            "io_weights",
            "data_center",
            "annotations",
            "scheduling_options",
        });
        batchReq->AddRequest(req, "get_nodes");
    }

    void HandleNodesAttributes(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspList>("get_nodes");
        if (!rspOrError.IsOK()) {
            YT_LOG_WARNING(rspOrError, "Error getting exec nodes information");
            return;
        }

        try {
            const auto& rsp = rspOrError.Value();

            auto future =
                BIND([nodeListYson = TYsonString(rsp->value())] {
                    return ConvertToNode(nodeListYson)->AsList();
                })
                .AsyncVia(GetBackgroundInvoker())
                .Run();
            auto nodeList = WaitFor(future)
                .ValueOrThrow();

            auto error = NodeManager_->HandleNodesAttributes(nodeList);
            SetSchedulerAlert(ESchedulerAlertType::UpdateNodesFailed, error);

            YT_LOG_INFO("Exec nodes information updated");
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Error updating exec nodes information");
        }
    }

    void RequestOperationsEffectiveAcl(const TObjectServiceProxy::TReqExecuteBatchPtr& batchReq)
    {
        YT_LOG_INFO("Requesting operations effective acl");

        auto req = TYPathProxy::Get("//sys/operations/@effective_acl");
        batchReq->AddRequest(req, "get_operations_effective_acl");
    }

    void HandleOperationsEffectiveAcl(const TObjectServiceProxy::TRspExecuteBatchPtr& batchRsp)
    {
        auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_operations_effective_acl");
        if (!rspOrError.IsOK()) {
            YT_LOG_WARNING(rspOrError, "Error getting operations effective ACL");
            return;
        }

        TSerializableAccessControlList operationsEffectiveAcl;
        try {
            const auto& rsp = rspOrError.Value();
            operationsEffectiveAcl = ConvertTo<TSerializableAccessControlList>(TYsonString(rsp->value()));
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Error parsing operations effective ACL");
            return;
        }

        OperationBaseAcl_.emplace();
        for (const auto& ace : operationsEffectiveAcl.Entries) {
            if (ace.Action == ESecurityAction::Allow && Any(ace.Permissions & EPermission::Write)) {
                OperationBaseAcl_->Entries.emplace_back(
                    ESecurityAction::Allow,
                    ace.Subjects,
                    EPermissionSet(EPermission::Read | EPermission::Manage | EPermission::Administer));
            }
        }
    }

    void RequestConfig(const TObjectServiceProxy::TReqExecuteBatchPtr& batchReq)
    {
        YT_LOG_INFO("Requesting scheduler configuration");

        auto req = TYPathProxy::Get("//sys/scheduler/config");
        batchReq->AddRequest(req, "get_config");
    }

    void HandleConfig(const TObjectServiceProxy::TRspExecuteBatchPtr& batchRsp)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_config");
        if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            // No config in Cypress, just ignore.
            return;
        }
        if (!rspOrError.IsOK()) {
            THROW_ERROR(rspOrError.Wrap(NScheduler::EErrorCode::WatcherHandlerFailed, "Error getting scheduler configuration"));
        }

        auto newConfig = CloneYsonStruct(InitialConfig_);
        try {
            const auto& rsp = rspOrError.Value();
            auto configFromCypress = ConvertToNode(TYsonString(rsp->value()));
            try {
                newConfig->Load(configFromCypress, /*validate*/ true, /*setDefaults*/ false);
            } catch (const std::exception& ex) {
                auto error = TError(NScheduler::EErrorCode::WatcherHandlerFailed, "Error updating scheduler configuration")
                    << ex;
                THROW_ERROR(error);
            }
        } catch (const std::exception& ex) {
            auto error = TError(NScheduler::EErrorCode::WatcherHandlerFailed, "Error parsing updated scheduler configuration")
                << ex;
            THROW_ERROR(error);
        }

        auto oldConfigNode = ConvertToNode(Config_);
        auto newConfigNode = ConvertToNode(newConfig);

        if (!AreNodesEqual(oldConfigNode, newConfigNode)) {
            YT_LOG_INFO("Scheduler configuration updated");

            Config_ = newConfig;
            ValidateConfig();

            SpecTemplate_ = CloneNode(Config_->SpecTemplate);

            NodeManager_->UpdateConfig(Config_);

            Bootstrap_->Reconfigure(Config_);

            Strategy_->UpdateConfig(Config_);
            MasterConnector_->UpdateConfig(Config_);
            OperationsCleaner_->UpdateConfig(Config_->OperationsCleaner);
            CachedExecNodeMemoryDistributionByTags_->SetExpirationTimeout(Config_->SchedulingTagFilterExpireTimeout);

            ProfilingExecutor_->SetPeriod(Config_->ProfilingUpdatePeriod);
            ClusterInfoLoggingExecutor_->SetPeriod(Config_->ClusterInfoLoggingPeriod);
            NodesInfoLoggingExecutor_->SetPeriod(Config_->NodesInfoLoggingPeriod);
            UpdateExecNodeDescriptorsExecutor_->SetPeriod(Config_->ExecNodeDescriptorsUpdatePeriod);
            JobReporterWriteFailuresChecker_->SetPeriod(Config_->JobReporterIssuesCheckPeriod);
            StrategyStuckOperationsChecker_->SetPeriod(Config_->OperationStuckCheck->Period);
            ExperimentAssignmentErrorChecker_->SetPeriod(Config_->ExperimentAssignmentErrorCheckPeriod);

            if (OrphanedOperationQueueScanPeriodExecutor_) {
                OrphanedOperationQueueScanPeriodExecutor_->SetPeriod(Config_->TransientOperationQueueScanPeriod);
            }
            if (TransientOperationQueueScanPeriodExecutor_) {
                TransientOperationQueueScanPeriodExecutor_->SetPeriod(Config_->TransientOperationQueueScanPeriod);
            }
            if (PendingByPoolOperationScanPeriodExecutor_) {
                PendingByPoolOperationScanPeriodExecutor_->SetPeriod(Config_->PendingByPoolOperationScanPeriod);
            }
            StaticOrchidService_->SetCachePeriod(Config_->StaticOrchidCacheUpdatePeriod);
            CombinedOrchidService_->SetUpdatePeriod(Config_->OrchidKeysUpdatePeriod);

            Bootstrap_->GetControllerAgentTracker()->UpdateConfig(Config_);

            EventLogWriter_->UpdateConfig(Config_->EventLog);

            OrchidWorkerPool_->SetThreadCount(Config_->OrchidWorkerThreadCount);
            FairShareUpdatePool_->SetThreadCount(Config_->FairShareUpdateThreadCount);
            BackgroundThreadPool_->SetThreadCount(Config_->BackgroundThreadCount);

            ExperimentsAssigner_.UpdateExperimentConfigs(Config_->Experiments);
        }

        ++ConfigRevision_;
    }


    void RequestOperationsArchiveVersion(TObjectServiceProxy::TReqExecuteBatchPtr batchReq)
    {
        YT_LOG_INFO("Requesting operation archive version");

        auto req = TYPathProxy::Get(GetOperationsArchiveVersionPath());
        batchReq->AddRequest(req, "get_operations_archive_version");
    }

    void HandleOperationsArchiveVersion(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_operations_archive_version");
        if (!rspOrError.IsOK()) {
            YT_LOG_INFO(rspOrError, "Error getting operation archive version");
            return;
        }

        try {
            auto version = ConvertTo<int>(TYsonString(rspOrError.Value()->value()));
            OperationsArchiveVersion_.store(version, std::memory_order::relaxed);
            OperationsCleaner_->SetArchiveVersion(version);
            SetSchedulerAlert(ESchedulerAlertType::UpdateArchiveVersion, TError());

            if (version < Config_->MinRequiredArchiveVersion) {
                SetSchedulerAlert(
                    ESchedulerAlertType::ArchiveIsOutdated,
                    TError("Min required archive version is not met")
                        << TErrorAttribute("version", version)
                        << TErrorAttribute("min_required_version", Config_->MinRequiredArchiveVersion));
            } else {
                SetSchedulerAlert(ESchedulerAlertType::ArchiveIsOutdated, TError());
            }
        } catch (const std::exception& ex) {
            auto error = TError("Error parsing operation archive version")
                << ex;
            SetSchedulerAlert(ESchedulerAlertType::UpdateArchiveVersion, error);
        }
    }

    void RequestClusterName(TObjectServiceProxy::TReqExecuteBatchPtr batchReq)
    {
        YT_LOG_INFO("Requesting cluster name");

        auto req = TYPathProxy::Get(GetClusterNamePath());
        batchReq->AddRequest(req, "get_cluster_name");
    }

    void HandleClusterName(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_cluster_name");
        if (!rspOrError.IsOK()) {
            YT_LOG_INFO(rspOrError, "Error getting cluster name");
            return;
        }

        ClusterName_ = ConvertTo<TString>(TYsonString(rspOrError.Value()->value()));
    }

    void RequestUserToDefaultPoolMap(TObjectServiceProxy::TReqExecuteBatchPtr batchReq)
    {
        YT_LOG_DEBUG("Requesting mapping from user to default pool");

        auto req = TYPathProxy::Get(GetUserToDefaultPoolMapPath());
        req->set_limit(CypressNodeLimit);

        batchReq->AddRequest(std::move(req), "get_user_to_default_pool");
    }

    void HandleUserToDefaultPoolMap(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_user_to_default_pool");
        if (!rspOrError.IsOK() && rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            YT_LOG_INFO(rspOrError, "Mapping from user to default pool does not exist");
            return;
        }
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error requesting mapping from user to default pool");

        auto future =
            BIND([userToDefaultPoolMapYson = TYsonString(rspOrError.Value()->value())] {
                return ConvertTo<THashMap<TString, TString>>(userToDefaultPoolMapYson);
            })
            .AsyncVia(GetBackgroundInvoker())
            .Run();
        auto userToDefaultPoolMap = WaitFor(future)
            .ValueOrThrow();

        auto error = Strategy_->UpdateUserToDefaultPoolMap(userToDefaultPoolMap);
        if (error.IsOK()) {
            UserToDefaultPoolMap_ = std::move(userToDefaultPoolMap);
        }
    }

    void UpdateExecNodeDescriptors()
    {
        YT_ASSERT_INVOKER_AFFINITY(GetBackgroundInvoker());

        auto execNodeDescriptors = NodeManager_->GetExecNodeDescriptors();

        CachedExecNodeDescriptors_.Store(execNodeDescriptors);

        NProto::TExecNodeDescriptorList serializedExecNodeDescriptors;

        serializedExecNodeDescriptors.mutable_exec_nodes()->Reserve(std::ssize(*execNodeDescriptors));
        for (const auto& [_, descriptor] : *execNodeDescriptors) {
            ToProto(serializedExecNodeDescriptors.add_exec_nodes(), *descriptor);
        }

        CachedSerializedExecNodeDescriptors_.Store(
            New<TRefCountedExecNodeDescriptorList>(
                SerializeProtoToRefWithEnvelope(serializedExecNodeDescriptors)));
    }

    void CheckJobReporterIssues()
    {
        int writeFailures = NodeManager_->ExtractJobReporterWriteFailuresCount();
        int queueIsTooLargeNodeCount = NodeManager_->GetJobReporterQueueIsTooLargeNodeCount();

        std::vector<TError> errors;
        if (writeFailures > Config_->JobReporterWriteFailuresAlertThreshold) {
            auto error = TError("Too many job archive writes failed")
                << TErrorAttribute("aggregation_period", Config_->JobReporterIssuesCheckPeriod)
                << TErrorAttribute("threshold", Config_->JobReporterWriteFailuresAlertThreshold)
                << TErrorAttribute("write_failures", writeFailures);
            errors.push_back(error);
        }
        if (queueIsTooLargeNodeCount > Config_->JobReporterQueueIsTooLargeAlertThreshold) {
            auto error = TError("Too many nodes have large job archivation queues")
                << TErrorAttribute("threshold", Config_->JobReporterQueueIsTooLargeAlertThreshold)
                << TErrorAttribute("queue_is_too_large_node_count", queueIsTooLargeNodeCount);
            errors.push_back(error);
        }

        TError resultError;
        if (!errors.empty()) {
            resultError = TError("Job archivation issues detected")
                << errors;
        }

        SetSchedulerAlert(ESchedulerAlertType::JobsArchivation, resultError);
    }

    void CheckStuckOperations()
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        for (const auto& [operationId, error] : Strategy_->GetStuckOperations()) {
            if (auto operation = FindOperation(operationId)) {
                OnOperationFailed(operation, error);
            }
        }
    }

    void CheckExperimentAssignmentError()
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        if (!LastExperimentAssignmentError_.IsOK() &&
            TInstant::Now() - LastExperimentAssignmentErrorTime_ < Config_->ExperimentAssignmentAlertDuration)
        {
            SetSchedulerAlert(ESchedulerAlertType::ExperimentAssignmentError, LastExperimentAssignmentError_);
        } else {
            SetSchedulerAlert(ESchedulerAlertType::ExperimentAssignmentError, TError());
        }
    }

    TMemoryDistribution CalculateMemoryDistribution(const TSchedulingTagFilter& filter) const
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        YT_LOG_DEBUG("Calculating node memory distribution (SchedulingTagFilter: %v)", filter);

        auto descriptors = CachedExecNodeDescriptors_.Acquire();

        TMemoryDistribution result;
        for (const auto& [nodeId, descriptor] : *descriptors) {
            if (descriptor->Online && filter.CanSchedule(descriptor->Tags)) {
                ++result[RoundUp<i64>(descriptor->ResourceLimits.GetMemory(), 1_GB)];
            }
        }

        YT_LOG_DEBUG("Finished calculating node memory distribution (SchedulingTagFilter: %v)", filter);

        return result;
    }

    void AbortAllocationsAtNode(TNodeId nodeId, EAbortReason reason) override
    {
        NodeManager_->AbortAllocationsAtNode(nodeId, reason);
    }

    void DoStartOperation(const TOperationPtr& operation)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        bool failedToStartDueToPoolLimitViolation = false;
        auto startError = [&] {
            TForbidContextSwitchGuard contextSwitchGuard;

            ValidateOperationState(operation, EOperationState::Starting);

            bool aliasRegistered = false;
            try {
                if (operation->Alias()) {
                    RegisterOperationAlias(operation);
                    aliasRegistered = true;
                }

                // NB(babenko): now we only validate this on start but not during revival
                // NB(ignat): this validation must be just before operation registration below
                // to avoid violation of pool limits. See YT-10802.

                auto poolLimitViolations = Strategy_->GetPoolLimitViolations(operation.Get(), operation->GetRuntimeParameters());

                std::vector<TString> erasedTreeIds;
                for (const auto& [treeId, error] : poolLimitViolations) {
                    bool shouldEraseTree = false;
                    if (GetSchedulingOptionsPerPoolTree(operation.Get(), treeId)->Tentative) {
                        YT_LOG_INFO(
                            error,
                            "Tentative tree is erased for operation because pool limits are violated (OperationId: %v, TreeId: %v)",
                            operation->GetId(),
                            treeId);

                        shouldEraseTree = true;
                    } else if (operation->Spec()->EraseTreesWithPoolLimitViolations) {
                        YT_LOG_INFO(
                            error,
                            "Tree is erased for operation because pool limits are violated (OperationId: %v, TreeId: %v)",
                            operation->GetId(),
                            treeId);

                        shouldEraseTree = true;
                    }

                    if (shouldEraseTree) {
                        erasedTreeIds.push_back(treeId);
                    } else {
                        failedToStartDueToPoolLimitViolation = true;
                        THROW_ERROR error;
                    }
                }

                operation->EraseTrees(erasedTreeIds);

                if (operation->AreAllTreesErased()) {
                    std::vector<TError> treeErrors;
                    for (const auto& [treeId, error] : poolLimitViolations) {
                        treeErrors.push_back(error
                            << TErrorAttribute("tree_id", treeId));
                    }

                    THROW_ERROR_EXCEPTION("All trees have been erased for operation")
                        << treeErrors
                        << TErrorAttribute("operation_id", operation->GetId());
                }
            } catch (const std::exception& ex) {
                if (aliasRegistered) {
                    auto it = OperationAliases_.find(*operation->Alias());
                    YT_VERIFY(it != OperationAliases_.end());
                    YT_VERIFY(it->second.Operation == operation);
                    OperationAliases_.erase(it);
                }

                EraseOrCrash(IdToStartingOperation_, operation->GetId());

                return TError(ex);
            }

            EraseOrCrash(IdToStartingOperation_, operation->GetId());

            ValidateOperationState(operation, EOperationState::Starting);

            RegisterOperation(operation, /*waitingForRevival*/ false);

            if (operation->GetRuntimeParameters()->SchedulingOptionsPerPoolTree.empty()) {
                UnregisterOperation(operation);
                return TError("No pool trees found for operation");
            }

            return TError();
        }();

        if (!startError.IsOK()) {
            if (failedToStartDueToPoolLimitViolation && operation->GetUserTransactionId()) {
                auto transactionAliveError = WaitFor(MasterConnector_->CheckTransactionAlive(operation->GetUserTransactionId()));
                if (!transactionAliveError.IsOK()) {
                    startError = transactionAliveError;
                }
            }

            auto wrappedError = TError("Operation has failed to start")
                << startError;
            operation->SetStarted(wrappedError);
            return;
        }

        if (operation->Spec()->IssueTemporaryToken) {
            try {
                auto temporaryToken = WaitFor(MasterConnector_->IssueTemporaryOperationToken(operation))
                    .ValueOrThrow();
                // NB: This modifies secure vault contents.
                operation->SetTemporaryToken(temporaryToken.Token, temporaryToken.NodeId);
            } catch (const std::exception& ex) {
                auto wrappedError = TError("Failed to issue temporary token for operation %v",
                    operation->GetId())
                    << ex;
                operation->SetStarted(wrappedError);
                UnregisterOperation(operation);
                return;
            }
        }

        try {
            WaitFor(MasterConnector_->CreateOperationNode(operation))
                .ThrowOnError();
        } catch (const std::exception& ex) {
            auto wrappedError = TError("Failed to create Cypress node for operation %v",
                operation->GetId())
                << ex;
            operation->SetStarted(wrappedError);
            UnregisterOperation(operation);
            return;
        }

        ValidateOperationState(operation, EOperationState::Starting);

        operation->SetStateAndEnqueueEvent(EOperationState::WaitingForAgent);
        AddOperationToTransientQueue(operation);

        // NB: Once we've registered the operation in Cypress we're free to complete
        // StartOperation request. Preparation will happen in a non-blocking
        // fashion.
        operation->SetStarted(TError());
    }

    NYson::TYsonString BuildBriefSpec(const TOperationPtr& operation) const
    {
        auto briefSpec = BuildYsonStringFluently()
            .BeginMap()
                .Items(operation->ControllerAttributes().InitializeAttributes->BriefSpec)
            .EndMap();
        return briefSpec;
    }

    void DoInitializeOperation(const TOperationPtr& operation)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        auto operationId = operation->GetId();

        ValidateOperationState(operation, EOperationState::Initializing);

        try {
            RegisterAssignedOperation(operation);

            const auto& controller = operation->GetController();

            auto initializeResult = WaitFor(controller->Initialize(/*transactions*/ std::nullopt, /*cumulativeSpecPatch*/ nullptr))
                .ValueOrThrow();

            ValidateOperationState(operation, EOperationState::Initializing);

            operation->Transactions() = initializeResult.Transactions;
            operation->ControllerAttributes().InitializeAttributes = std::move(initializeResult.Attributes);
            operation->BriefSpecString() = BuildBriefSpec(operation);

            if (initializeResult.EraseOffloadingTrees) {
                EraseOffloadingTrees(operation);
            }

            WaitFor(MasterConnector_->UpdateInitializedOperationNode(operation))
                .ThrowOnError();

            ValidateOperationState(operation, EOperationState::Initializing);
        } catch (const std::exception& ex) {
            auto wrappedError = TError("Operation has failed to initialize")
                << ex;
            OnOperationFailed(operation, wrappedError);
            return;
        }

        ValidateOperationState(operation, EOperationState::Initializing);

        LogEventFluently(&SchedulerStructuredLogger(), ELogEventType::OperationStarted)
            .Do(std::bind(&TImpl::BuildOperationInfoForEventLog, MakeStrong(this), operation, _1))
            .Do(std::bind(&ISchedulerStrategy::BuildOperationInfoForEventLog, Strategy_, operation.Get(), _1));

        YT_LOG_INFO("Preparing operation (OperationId: %v)",
            operationId);

        operation->SetStateAndEnqueueEvent(EOperationState::Preparing);

        try {
            // Run async preparation.
            const auto& controller = operation->GetController();

            {
                auto result = WaitFor(controller->Prepare())
                    .ValueOrThrow();

                operation->ControllerAttributes().PrepareAttributes = std::move(result.Attributes);
            }

            ValidateOperationState(operation, EOperationState::Preparing);

            operation->SetStateAndEnqueueEvent(EOperationState::Pending);
        } catch (const std::exception& ex) {
            auto wrappedError = TError(NScheduler::EErrorCode::OperationFailedToPrepare, "Operation has failed to prepare")
                << ex;
            OnOperationFailed(operation, wrappedError);
            return;
        }

        YT_LOG_INFO("Operation prepared (OperationId: %v)",
            operationId);

        LogEventFluently(&SchedulerStructuredLogger(), ELogEventType::OperationPrepared)
            .Item("operation_id").Value(operationId)
            .Item("unrecognized_spec").Value(operation->ControllerAttributes().InitializeAttributes->UnrecognizedSpec);

        TryStartOperationMaterialization(operation);
    }

    void DoReviveOperation(const TOperationPtr& operation)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        auto operationId = operation->GetId();

        ValidateOperationState(operation, EOperationState::ReviveInitializing);

        YT_LOG_INFO("Reviving operation (OperationId: %v)",
            operationId);

        try {
            RegisterAssignedOperation(operation);

            ValidateOperationState(operation, EOperationState::ReviveInitializing);

            const auto& controller = operation->GetController();

            {
                YT_VERIFY(operation->RevivalDescriptor());
                auto result = WaitFor(controller->Initialize(
                    operation->Transactions(),
                    /*cumulativeSpecPatch*/ operation->CumulativeSpecPatch()))
                    .ValueOrThrow();

                operation->Transactions() = std::move(result.Transactions);
                operation->ControllerAttributes().InitializeAttributes = std::move(result.Attributes);
                operation->BriefSpecString() = BuildBriefSpec(operation);
            }

            ValidateOperationState(operation, EOperationState::ReviveInitializing);

            WaitFor(MasterConnector_->UpdateInitializedOperationNode(operation))
                .ThrowOnError();

            ValidateOperationState(operation, EOperationState::ReviveInitializing);

            operation->SetStateAndEnqueueEvent(EOperationState::Reviving);

            {
                auto result = WaitFor(controller->Revive())
                    .ValueOrThrow();

                ValidateOperationState(operation, EOperationState::Reviving);

                operation->ControllerAttributes().PrepareAttributes = result.Attributes;
                operation->SetRevivedFromSnapshot(result.RevivedFromSnapshot);
                operation->RevivedAllocations() = std::move(result.RevivedAllocations);
                for (const auto& bannedTreeId : result.RevivedBannedTreeIds) {
                    // If operation is already erased from the tree, |UnregisterOperationFromTrees| will produce unnecessary log messages.
                    // However, I believe that this way the code is simpler and more concise.
                    // NB(eshcherbin): this procedure won't abort allocations that are running in banned tentative trees.
                    // So in case of an unfortunate scheduler failure, these allocations will continue running.
                    const auto& schedulingOptionsPerPoolTree = operation->GetRuntimeParameters()->SchedulingOptionsPerPoolTree;
                    if (schedulingOptionsPerPoolTree.find(bannedTreeId) != schedulingOptionsPerPoolTree.end()) {
                        UnregisterOperationFromTrees(operation, {bannedTreeId});
                    }
                }
            }

            YT_LOG_INFO("Operation has been revived (OperationId: %v)",
                operationId);

            operation->RevivalDescriptor().reset();

            operation->SetStateAndEnqueueEvent(
                EOperationState::Pending,
                BuildYsonStringFluently().BeginMap()
                    .Item("revived_from_snapshot").Value(operation->GetRevivedFromSnapshot())
                .EndMap());

        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Operation has failed to revive (OperationId: %v)",
                operationId);
            auto wrappedError = TError("Operation has failed to revive")
                << ex;
            OnOperationFailed(operation, wrappedError);
        }

        TryStartOperationMaterialization(operation);
    }

    TFuture<void> RegisterAllocationsFromRevivedOperation(const TOperationPtr& operation)
    {
        auto allocations = std::move(operation->RevivedAllocations());
        YT_LOG_INFO(
            "Registering running allocations from the revived operation (OperationId: %v, AllocationCount: %v)",
            operation->GetId(),
            allocations.size());

        if (auto delay = operation->Spec()->TestingOperationOptions->DelayInsideRegisterAllocationsFromRevivedOperation) {
            TDelayedExecutor::WaitForDuration(*delay);
        }

        // First, unfreeze operation and register allocations in strategy. Do this synchronously as we are in the scheduler control thread.
        Strategy_->RegisterAllocationsFromRevivedOperation(operation->GetId(), allocations);

        // Second, register allocations at the node manager.
        return NodeManager_->FinishOperationRevival(
            operation->GetId(),
            std::move(allocations));
    }

    void BuildOperationOrchid(const TOperationPtr& operation, IYsonConsumer* consumer)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto agent = operation->FindAgent();

        BuildYsonFluently(consumer)
            .BeginMap()
                .Do(BIND(&NScheduler::BuildFullOperationAttributes, operation, /*includeOperationId*/ false, /*includeHeavyAttributes*/ true))
                .DoIf(static_cast<bool>(agent), [&] (TFluentMap fluent) {
                    fluent
                        .Item("agent_id").Value(agent->GetId());
                })
                .OptionalItem("alias", operation->Alias())
            .EndMap();
    }

    IYPathServicePtr CreateOperationOrchidService(const TOperationPtr& operation)
    {
        // TODO(renadeen): remove when YPathDesignatedServiceFromProducer proves worthy.
        auto operationAttributesOrchidService = Config_->EnableOptimizedOperationOrchid
            ? IYPathService::FromProducerLazy(BIND(&TImpl::BuildOperationOrchid, MakeStrong(this), operation))
                ->Via(GetControlInvoker(EControlQueue::DynamicOrchid))
            : IYPathService::FromProducer(BIND(&TImpl::BuildOperationOrchid, MakeStrong(this), operation))
                ->Via(GetControlInvoker(EControlQueue::DynamicOrchid));
        return New<TServiceCombiner>(
            std::vector<IYPathServicePtr>{
                NewWithOffloadedDtor<TOperationService>(GetBackgroundInvoker(), this, operation),
                operationAttributesOrchidService,
            },
            /*cacheUpdatePeriod*/ std::nullopt,
            /*updateKeysOnMissing*/ true);
    }

    void RegisterOperationAlias(const TOperationPtr& operation)
    {
        YT_VERIFY(operation->Alias());

        TOperationAlias alias{operation->GetId(), operation};
        auto it = OperationAliases_.find(*operation->Alias());
        if (it != OperationAliases_.end()) {
            if (it->second.Operation) {
                THROW_ERROR_EXCEPTION("Operation alias is already used by an operation")
                    << TErrorAttribute("operation_alias", operation->Alias())
                    << TErrorAttribute("operation_id", it->second.OperationId);
            }
            YT_LOG_DEBUG("Assigning an already existing alias to a new operation (Alias: %v, OldOperationId: %v, NewOperationId: %v)",
                *operation->Alias(),
                it->second.OperationId,
                operation->GetId());
            it->second = std::move(alias);
        } else {
            YT_LOG_DEBUG("Assigning a new alias to a new operation (Alias: %v, OperationId: %v)",
                *operation->Alias(),
                operation->GetId());
            OperationAliases_[*operation->Alias()] = std::move(alias);
        }
    }

    void RegisterOperation(const TOperationPtr& operation, bool waitingForRevival)
    {
        YT_VERIFY(operation->GetState() == EOperationState::Starting || operation->GetState() == EOperationState::Orphaned);
        YT_VERIFY(IdToOperation_.emplace(operation->GetId(), operation).second);

        const auto& agentTracker = Bootstrap_->GetControllerAgentTracker();
        auto controller = agentTracker->CreateController(operation);
        operation->SetController(controller);

        std::vector<TString> unknownTreeIds;
        TPoolTreeControllerSettingsMap poolTreeControllerSettingsMap;

        Strategy_->RegisterOperation(operation.Get(), &unknownTreeIds, &poolTreeControllerSettingsMap);
        operation->PoolTreeControllerSettingsMap() = poolTreeControllerSettingsMap;
        operation->EraseTrees(unknownTreeIds);

        YT_LOG_DEBUG_UNLESS(
            unknownTreeIds.empty(),
            "Operation has unknown pool trees after registration (OperationId: %v, TreeIds: %v)",
            operation->GetId(),
            unknownTreeIds);

        NodeManager_->RegisterOperation(
            operation->GetId(),
            operation->ControllerEpoch(),
            operation->GetController(),
            waitingForRevival);

        MasterConnector_->RegisterOperation(operation);

        auto service = CreateOperationOrchidService(operation);
        YT_VERIFY(IdToOperationService_.emplace(operation->GetId(), service).second);

        YT_LOG_DEBUG(
            "Operation registered (OperationId: %v, OperationAlias: %v, WaitingForRevival: %v)",
            operation->GetId(),
            operation->Alias(),
            waitingForRevival);
    }

    void RegisterAssignedOperation(const TOperationPtr& operation)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto agent = operation->FindAgent();
        if (!agent || agent->GetState() != EControllerAgentState::Registered) {
            YT_LOG_INFO("Assigned agent is missing or not registered, operation returned to waiting for agent state (OperationId: %v)",
                operation->GetId());
            operation->SetStateAndEnqueueEvent(EOperationState::WaitingForAgent);
            AddOperationToTransientQueue(operation);
            throw NConcurrency::TFiberCanceledException();
        }

        const auto& controller = operation->GetController();
        controller->AssignAgent(agent, operation->ControllerEpoch());
        WaitFor(controller->Register(operation))
            .ThrowOnError();
    }

    void UnregisterOperation(const TOperationPtr& operation)
    {
        EraseOrCrash(IdToOperation_, operation->GetId());
        EraseOrCrash(IdToOperationService_, operation->GetId());

        if (operation->Alias()) {
            auto& alias = GetOrCrash(OperationAliases_, *operation->Alias());
            YT_LOG_DEBUG("Alias now corresponds to an unregistered operation (Alias: %v, OperationId: %v)",
                *operation->Alias(),
                operation->GetId());
            YT_VERIFY(alias.Operation == operation);
            alias.Operation = nullptr;
        }

        const auto& controller = operation->GetController();
        if (controller) {
            controller->RevokeAgent();
        }
        operation->SetController(nullptr);

        NodeManager_->UnregisterOperation(operation->GetId());

        Strategy_->UnregisterOperation(operation.Get());

        const auto& agentTracker = Bootstrap_->GetControllerAgentTracker();
        agentTracker->UnregisterOperationFromAgent(operation);

        MasterConnector_->UnregisterOperation(operation);

        YT_LOG_DEBUG("Operation unregistered (OperationId: %v)",
            operation->GetId());
    }

    void AbortOperationAllocations(const TOperationPtr& operation, const TError& error, EAbortReason abortReason, bool terminated)
    {
        NodeManager_->AbortOperationAllocations(operation->GetId(), error, abortReason, terminated);

        YT_LOG_INFO(
            error,
            "All operations allocations aborted at scheduler (OperationId: %v)",
            operation->GetId());

        const auto& agent = operation->FindAgent();
        if (agent) {
            try {
                WaitFor(agent->GetFullHeartbeatProcessed())
                    .ThrowOnError();
                YT_LOG_DEBUG(
                    "Full heartbeat from agent processed, "
                    "aborted allocations supposed to be considered by controller agent (OperationId: %v)",
                    operation->GetId());
            } catch (const std::exception& ex) {
                auto error = TError("Failed to wait full heartbeat from agent for operation %v", operation->GetId())
                    << ex;
                YT_LOG_WARNING(error);
                Bootstrap_->GetControllerAgentTracker()->HandleAgentFailure(agent, error);
            }
        }
    }

    void BuildOperationInfoForEventLog(const TOperationPtr& operation, TFluentMap fluent)
    {
        fluent
            .Item("operation_id").Value(operation->GetId())
            .Item("operation_type").Value(operation->GetType())
            .OptionalItem("trimmed_annotations", operation->GetTrimmedAnnotations())
            .Item("spec").Value(operation->GetSpecString())
            .Item("experiment_assignments").Value(operation->ExperimentAssignments())
            .Item("experiment_assignment_names").Value(operation->GetExperimentAssignmentNames())
            .Item("authenticated_user").Value(operation->GetAuthenticatedUser())
            .Item("start_time").Value(operation->GetStartTime())
            .OptionalItem("finish_time", operation->GetFinishTime());
    }

    void SetOperationFinalState(const TOperationPtr& operation, EOperationState state, TError error)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto truncatedError = std::move(error).Truncate();

        if (!operation->GetStarted().IsSet()) {
            operation->SetStarted(truncatedError);
        }
        operation->SetStateAndEnqueueEvent(state);
        operation->SetFinishTime(TInstant::Now());
        ToProto(operation->MutableResult().mutable_error(), truncatedError);
    }

    void FinishOperation(const TOperationPtr& operation)
    {
        if (!operation->GetFinished().IsSet()) {
            operation->SetFinished();
            UnregisterOperation(operation);
        }
        operation->Cancel(TError("Operation finished"));
    }

    void UnregisterOperationAtController(const TOperationPtr& operation) const
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        const auto& controller = operation->GetController();
        if (!controller) {
            return;
        }

        // Notify controller that it is going to be disposed.
        auto unregisterFuture = controller->Unregister();
        std::vector<TFuture<void>> futures = {unregisterFuture.As<void>()};
        // NB(eshcherbin): We wait for full heartbeat to ensure that allocation metrics have been fully collected. See: YT-12207.
        futures.push_back(controller->GetFullHeartbeatProcessed());

        // Failure is intentionally ignored.
        Y_UNUSED(WaitFor(AllSet(futures)));

        YT_VERIFY(unregisterFuture.IsSet());
        auto resultOrError = unregisterFuture.Get();
        if (!resultOrError.IsOK()) {
            return;
        }

        auto result = resultOrError.Value();
        if (!result.ResidualJobMetrics.empty()) {
            GetStrategy()->ApplyJobMetricsDelta({{operation->GetId(), result.ResidualJobMetrics}});
        }
    }

    void DoCompleteOperation(const TOperationPtr& operation)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        if (operation->IsFinishedState() || operation->IsFinishingState()) {
            // Operation is probably being aborted.
            return;
        }

        auto codicilGuard = operation->MakeCodicilGuard();

        auto operationId = operation->GetId();
        YT_LOG_INFO("Completing operation (OperationId: %v)",
            operationId);

        operation->SetStateAndEnqueueEvent(EOperationState::Completing);
        operation->SetSuspended(false);

        // The operation may still have running allocations (e.g. those started speculatively).
        AbortOperationAllocations(
            operation,
            TError("Operation completed"),
            EAbortReason::OperationCompleted,
            /*terminated*/ true);

        TOperationProgress operationProgress;
        try {
            // First flush: ensure that all stderrs are attached and the
            // state is changed to Completing.
            {
                auto asyncResult = MasterConnector_->FlushOperationNode(operation);
                // Result is ignored since failure causes scheduler disconnection.
                Y_UNUSED(WaitFor(asyncResult));
                ValidateOperationState(operation, EOperationState::Completing);
            }

            // Should be called before commit in controller.
            operationProgress = WaitFor(BIND(&TImpl::RequestOperationProgress, MakeStrong(this), operation)
                .AsyncVia(operation->GetCancelableControlInvoker())
                .Run())
                .ValueOrThrow();

            ValidateOperationState(operation, EOperationState::Completing);

            {
                const auto& controller = operation->GetController();
                WaitFor(controller->Commit())
                    .ThrowOnError();

                ValidateOperationState(operation, EOperationState::Completing);

                MaybeDelay(Config_->TestingOptions->FinishOperationTransitionDelay);
            }

            YT_VERIFY(operation->GetState() == EOperationState::Completing);
            SetOperationFinalState(operation, EOperationState::Completed, TError());

            // Second flush: ensure that state is changed to Completed.
            {
                auto asyncResult = MasterConnector_->FlushOperationNode(operation);
                WaitFor(asyncResult)
                    .ThrowOnError();
                YT_VERIFY(operation->GetState() == EOperationState::Completed);
            }
        } catch (const std::exception& ex) {
            OnOperationFailed(operation, ex);
            return;
        }

        operation->SetUnregistering();

        // Switch to regular control invoker.
        // No recurrent complete is possible after this point.
        SwitchTo(operation->GetControlInvoker());

        SubmitOperationToCleaner(operation, operationProgress);

        UnregisterOperationAtController(operation);

        LogOperationFinished(
            operation,
            ELogEventType::OperationCompleted,
            TError(),
            operationProgress.Progress,
            operationProgress.Alerts);

        FinishOperation(operation);

        YT_LOG_INFO("Operation completed (OperationId: %v)",
            operationId);
    }

    void DoFailOperation(
        const TOperationPtr& operation,
        const TError& error)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        // NB: Finishing state is ok, do not skip operation fail in this case.
        if (operation->IsFinishedState()) {
            // Operation is already terminated.
            return;
        }

        auto codicilGuard = operation->MakeCodicilGuard();

        YT_LOG_INFO(error, "Operation failed (OperationId: %v)",
            operation->GetId());

        TerminateOperation(
            operation,
            EOperationState::Failing,
            EOperationState::Failed,
            ELogEventType::OperationFailed,
            error);
    }

    void DoAbortOperation(const TOperationPtr& operation, const TError& error)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        // NB: Finishing state is ok, do not skip operation abort in this case.
        if (operation->IsFinishedState()) {
            // Operation is already terminated.
            return;
        }

        auto codicilGuard = operation->MakeCodicilGuard();

        YT_LOG_INFO(error, "Aborting operation (OperationId: %v, State: %v)",
            operation->GetId(),
            operation->GetState());

        if (operation->Spec()->TestingOperationOptions->DelayInsideAbort) {
            TDelayedExecutor::WaitForDuration(*operation->Spec()->TestingOperationOptions->DelayInsideAbort);
        }

        TerminateOperation(
            operation,
            EOperationState::Aborting,
            EOperationState::Aborted,
            ELogEventType::OperationAborted,
            error);
    }

    void DoSuspendOperation(
        const TOperationPtr& operation,
        const TError& error,
        bool abortRunningAllocations,
        bool setAlert)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        // NB: Finishing state is ok, do not skip operation fail in this case.
        if (operation->IsFinishedState()) {
            // Operation is already terminated.
            return;
        }

        auto codicilGuard = operation->MakeCodicilGuard();

        operation->SetSuspended(true);

        if (abortRunningAllocations) {
            AbortOperationAllocations(
                operation,
                error
                    << TErrorAttribute("abort_reason", EAbortReason::OperationSuspended),
                EAbortReason::OperationSuspended,
                /*terminated*/ false);
        } else {
            // |AbortOperationAllocations| implies suspension, so we only need to do it explicitly if
            // |abortRunningAllocations| is false.
            NodeManager_->SuspendOperationScheduling(operation->GetId());
        }

        if (setAlert) {
            DoSetOperationAlert(operation->GetId(), EOperationAlertType::OperationSuspended, error);
        }

        YT_LOG_INFO(error, "Operation suspended (OperationId: %v)",
            operation->GetId());
    }

    TOperationProgress RequestOperationProgress(const TOperationPtr& operation) const
    {
        auto agent = operation->FindAgent();

        if (agent) {
            NControllerAgent::TControllerAgentServiceProxy proxy(agent->GetChannel());
            auto req = proxy.GetOperationInfo();
            req->SetTimeout(Config_->ControllerAgentTracker->LightRpcTimeout);
            ToProto(req->mutable_operation_id(), operation->GetId());
            auto rspOrError = WaitFor(req->Invoke());
            if (rspOrError.IsOK()) {
                auto rsp = rspOrError.Value();
                TOperationProgress result;
                // TODO(asaitgalin): Can we build map in controller instead of map fragment?
                result.Progress = BuildYsonStringFluently()
                    .BeginMap()
                        .Items(TYsonString(rsp->progress(), EYsonType::MapFragment))
                    .EndMap();
                result.BriefProgress = BuildYsonStringFluently()
                    .BeginMap()
                        .Items(TYsonString(rsp->brief_progress(), EYsonType::MapFragment))
                    .EndMap();
                result.Alerts = BuildYsonStringFluently()
                    .BeginMap()
                        .Items(TYsonString(rsp->alerts(), EYsonType::MapFragment))
                    .EndMap();
                return result;
            } else {
                YT_LOG_INFO(rspOrError, "Failed to get operation info from controller agent (OperationId: %v)",
                    operation->GetId());
            }
        }

        // If we failed to get progress from controller then we try to fetch it from Cypress.
        {
            auto attributesOrError = WaitFor(MasterConnector_->GetOperationNodeProgressAttributes(operation));
            if (attributesOrError.IsOK()) {
                auto attributes = ConvertToAttributes(attributesOrError.Value());

                TOperationProgress result;
                result.Progress = attributes->FindYson("progress");
                result.BriefProgress = attributes->FindYson("brief_progress");
                result.Alerts = attributes->FindYson("alerts");
                return result;
            } else {
                YT_LOG_INFO(attributesOrError, "Failed to get operation progress from Cypress (OperationId: %v)",
                    operation->GetId());
            }
        }

        return TOperationProgress();
    }

    void SubmitOperationToCleaner(
        const TOperationPtr& operation,
        const TOperationProgress& operationProgress) const
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto archivationReq = OperationsCleaner_->InitializeRequestFromOperation(operation);
        archivationReq.Progress = operationProgress.Progress;
        archivationReq.BriefProgress = operationProgress.BriefProgress;
        archivationReq.Alerts = operationProgress.Alerts;

        OperationsCleaner_->SubmitForArchivation(std::move(archivationReq));
    }

    void TerminateOperation(
        const TOperationPtr& operation,
        EOperationState intermediateState,
        EOperationState finalState,
        ELogEventType logEventType,
        const TError& error)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto initialState = operation->GetState();
        if (IsOperationFinished(initialState) ||
            initialState == EOperationState::Failing ||
            initialState == EOperationState::Aborting)
        {
            // Safe to call multiple times, just ignore it.
            return;
        }

        operation->SetStateAndEnqueueEvent(intermediateState);
        operation->SetSuspended(false);

        YT_VERIFY(finalState == EOperationState::Aborted || finalState == EOperationState::Failed);
        auto allocationAbortReason = finalState == EOperationState::Aborted
            ? EAbortReason::OperationAborted
            : EAbortReason::OperationFailed;

        AbortOperationAllocations(
            operation,
            TError("Operation terminated")
                << TErrorAttribute("state", initialState)
                << TErrorAttribute("abort_reason", allocationAbortReason)
                << error,
            allocationAbortReason,
            /*terminated*/ true);

        // First flush: ensure that all stderrs are attached and the
        // state is changed to its intermediate value.
        {
            // Result is ignored since failure causes scheduler disconnection.
            Y_UNUSED(WaitFor(MasterConnector_->FlushOperationNode(operation)));
            if (operation->GetState() != intermediateState) {
                return;
            }
        }

        MaybeDelay(Config_->TestingOptions->FinishOperationTransitionDelay);

        auto operationProgress = WaitFor(BIND(&TImpl::RequestOperationProgress, MakeStrong(this), operation)
            .AsyncVia(operation->GetCancelableControlInvoker())
            .Run())
            .ValueOrThrow();

        if (const auto& controller = operation->GetController()) {
            try {
                WaitFor(controller->Terminate(finalState))
                    .ThrowOnError();
            } catch (const std::exception& ex) {
                auto error = TError("Failed to abort controller of operation %v", operation->GetId())
                    << ex;
                if (auto agent = operation->FindAgent()) {
                    YT_LOG_WARNING(error);
                    Bootstrap_->GetControllerAgentTracker()->HandleAgentFailure(agent, error);
                } else {
                    YT_LOG_WARNING(error, "Operation termination failed but looks like controller is already unregistered");
                }
                return;
            }
        }

        bool owningTransactions =
            initialState == EOperationState::WaitingForAgent ||
            initialState == EOperationState::Orphaned ||
            initialState == EOperationState::Initializing ||
            initialState == EOperationState::ReviveInitializing;
        if (owningTransactions && operation->Transactions()) {
            try {
                WaitFor(operation->AbortCommonTransactions())
                    .ThrowOnError();
            } catch (const std::exception& ex) {
                YT_LOG_DEBUG(
                    ex,
                    "Failed to abort transactions of orphaned operation (OperationId: %v)",
                    operation->GetId());
            }
        } else {
            YT_LOG_DEBUG(
                "Skipping transactions abort (OperationId: %v, InitialState: %v, HasTransaction: %v)",
                operation->GetId(),
                initialState,
                static_cast<bool>(operation->Transactions()));
        }

        SetOperationFinalState(operation, finalState, error);

        // Second flush: ensure that the state is changed to its final value.
        {
            // Result is ignored since failure causes scheduler disconnection.
            Y_UNUSED(WaitFor(MasterConnector_->FlushOperationNode(operation)));
            if (operation->GetState() != finalState) {
                return;
            }
        }

        operation->SetUnregistering();

        // Switch to regular control invoker.
        // No recurrent terminate is possible after this point.
        SwitchTo(operation->GetControlInvoker());

        SubmitOperationToCleaner(operation, operationProgress);

        UnregisterOperationAtController(operation);

        LogOperationFinished(
            operation,
            logEventType,
            error,
            operationProgress.Progress,
            operationProgress.Alerts);

        FinishOperation(operation);
    }


    void CompleteOperationWithoutRevival(const TOperationPtr& operation)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        YT_LOG_INFO("Completing operation without revival (OperationId: %v)",
            operation->GetId());

        if (operation->RevivalDescriptor()->ShouldCommitOutputTransaction) {
            WaitFor(operation->Transactions()->OutputTransaction->Commit())
                .ThrowOnError();
            // We don't know whether debug transaction is committed.
            if (operation->Transactions()->DebugTransaction) {
                Y_UNUSED(operation->Transactions()->DebugTransaction->Commit());
            }
            for (auto transaction : {operation->Transactions()->InputTransaction, operation->Transactions()->AsyncTransaction}) {
                if (transaction) {
                    Y_UNUSED(transaction->Abort());
                }
            }
        }

        SetOperationFinalState(operation, EOperationState::Completed, TError());

        // Result is ignored since failure causes scheduler disconnection.
        Y_UNUSED(WaitFor(MasterConnector_->FlushOperationNode(operation)));

        auto operationProgress = WaitFor(BIND(&TImpl::RequestOperationProgress, MakeStrong(this), operation)
            .AsyncVia(operation->GetCancelableControlInvoker())
            .Run())
            .ValueOrThrow();

        SubmitOperationToCleaner(operation, operationProgress);

        LogOperationFinished(
            operation,
            ELogEventType::OperationCompleted,
            TError(),
            operationProgress.Progress,
            operationProgress.Alerts);

        FinishOperation(operation);
    }

    void AbortOperationWithoutRevival(const TOperationPtr& operation, const TError& error)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        YT_LOG_INFO(error, "Aborting operation without revival (OperationId: %v)",
            operation->GetId());

        // Fire-and-forget.
        YT_UNUSED_FUTURE(operation->AbortCommonTransactions());

        SetOperationFinalState(operation, EOperationState::Aborted, error);

        // Result is ignored since failure causes scheduler disconnection.
        Y_UNUSED(WaitFor(MasterConnector_->FlushOperationNode(operation)));

        auto operationProgress = WaitFor(BIND(&TImpl::RequestOperationProgress, MakeStrong(this), operation)
            .AsyncVia(operation->GetCancelableControlInvoker())
            .Run())
            .ValueOrThrow();

        SubmitOperationToCleaner(operation, operationProgress);

        LogOperationFinished(
            operation,
            ELogEventType::OperationAborted,
            error,
            operationProgress.Progress,
            operationProgress.Alerts);

        FinishOperation(operation);
    }

    TYsonString BuildSuspiciousJobsYson()
    {
        TStringBuilder builder;
        for (const auto& [operationId, operation] : IdToOperation_) {
            builder.AppendString(operation->GetSuspiciousJobs().AsStringBuf());
        }
        return TYsonString(builder.Flush(), EYsonType::MapFragment);
    }

    void BuildStaticOrchid(IYsonConsumer* consumer)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto nodeYsons = NodeManager_->BuildNodeYsonList();
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("controller_agents").DoMapFor(Bootstrap_->GetControllerAgentTracker()->GetAgents(), [] (TFluentMap fluent, const auto& agent) {
                    fluent
                        .Item(agent->GetId()).BeginMap()
                            .Item("state").Value(agent->GetState())
                            .DoIf(agent->GetState() == EControllerAgentState::Registered, [&] (TFluentMap fluent) {
                                fluent.Item("incarnation_id").Value(agent->GetIncarnationId());
                            })
                            .Item("operation_ids").DoListFor(agent->Operations(), [] (TFluentList fluent, const auto& operation) {
                                fluent.Item().Value(operation->GetId());
                            })
                        .EndMap();
                })
                .Item("cluster").BeginMap()
                    .Item("resource_limits").Value(GetResourceLimits(EmptySchedulingTagFilter))
                    .Item("resource_usage").Value(GetResourceUsage(EmptySchedulingTagFilter))
                    .Item("exec_node_count").Value(NodeManager_->GetExecNodeCount())
                    .Item("total_node_count").Value(NodeManager_->GetTotalNodeCount())
                    .Item("nodes_memory_distribution").Value(GetExecNodeMemoryDistribution(TSchedulingTagFilter()))
                    // TODO(renadeen): we should move this map into tree orchid.
                    .Item("resource_limits_by_tags")
                        .DoMapFor(Strategy_->GetResourceLimitsByTagFilter(), [] (TFluentMap fluent, const auto& pair) {
                            const auto& [filter, limits] = pair;
                            fluent.Item(ToString(filter)).Value(limits);
                        })
                    .Item("medium_directory").Value(
                        Bootstrap_
                        ->GetClient()
                        ->GetNativeConnection()
                        ->GetMediumDirectory())
                .EndMap()
                .Item("suspicious_jobs").BeginMap()
                    .Items(BuildSuspiciousJobsYson())
                .EndMap()
                .Item("nodes").DoMapFor(nodeYsons, [] (TFluentMap fluent, const TNodeYsonList::value_type& pair) {
                        const auto& [_, nodeYson] = pair;
                        fluent.Items(nodeYson);
                })
                .Item("user_to_default_pool").Value(UserToDefaultPoolMap_)
                .Do(std::bind(&ISchedulerStrategy::BuildOrchid, Strategy_, _1))
            .EndMap();
    }

    void BuildLightStaticOrchid(IYsonConsumer* consumer)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("config").Value(Config_)
                .Item("config_revision").Value(ConfigRevision_)
                .Item("operations_cleaner").BeginMap()
                    .Do(std::bind(&TOperationsCleaner::BuildOrchid, OperationsCleaner_, _1))
                .EndMap()
                .Item("operation_base_acl").Value(OperationBaseAcl_)
                .Item("service").BeginMap()
                    // This information used by scheduler_uptime odin check and we want
                    // to receive all these fields by single request.
                    .Item("connected").Value(IsConnected())
                    .Item("last_connection_time").Value(GetConnectionTime())
                    .Item("build_version").Value(GetVersion())
                    .Item("hostname").Value(GetDefaultAddress(Bootstrap_->GetLocalAddresses()))
                .EndMap()
                .Item("supported_features").BeginMap()
                    .Do(BuildSupportedFeatures)
                .EndMap()
            .EndMap();
    }

    IYPathServicePtr GetNodesOrchidService()
    {
        auto nodesService = New<TCompositeMapService>();

        nodesService->AddChild("ongoing_heartbeat_count", IYPathService::FromProducer(BIND(
            [scheduler{this}] (IYsonConsumer* consumer) {
                BuildYsonFluently(consumer).Value(scheduler->NodeManager_->GetOngoingHeartbeatsCount());
            })));

        nodesService->AddChild("total_scheduling_heartbeat_complexity", IYPathService::FromProducer(BIND(
            [scheduler{this}] (IYsonConsumer* consumer) {
                BuildYsonFluently(consumer).Value(scheduler->NodeManager_->GetTotalConcurrentHeartbeatComplexity());
            })));

        return nodesService;
    }

    IYPathServicePtr GetDynamicOrchidService()
    {
        auto dynamicOrchidService = New<TCompositeMapService>();
        dynamicOrchidService->AddChild("operations", New<TOperationsService>(this));
        dynamicOrchidService->AddChild("allocations", New<TAllocationsService>(this));
        dynamicOrchidService->AddChild("node_shards", GetNodesOrchidService());
        return dynamicOrchidService;
    }

    TAllocationDescription GetAllocationDescription(TAllocationId allocationId) const
    {
        return WaitFor(NodeManager_->GetAllocationDescription(allocationId))
            .ValueOrThrow();
    }

    void ValidateConfig()
    {
        // First reset the alert.
        SetSchedulerAlert(ESchedulerAlertType::UnrecognizedConfigOptions, TError());

        if (!Config_->EnableUnrecognizedAlert) {
            return;
        }

        auto unrecognized = Config_->GetRecursiveUnrecognized();
        if (unrecognized && unrecognized->GetChildCount() > 0) {
            YT_LOG_WARNING("Scheduler config contains unrecognized options (Unrecognized: %v)",
                ConvertToYsonString(unrecognized, EYsonFormat::Text));
            SetSchedulerAlert(
                ESchedulerAlertType::UnrecognizedConfigOptions,
                TError("Scheduler config contains unrecognized options")
                    << TErrorAttribute("unrecognized", unrecognized));
        }
    }


    void AddOperationToTransientQueue(const TOperationPtr& operation)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        StateToTransientOperations_[operation->GetState()].push_back(operation);

        if (operation->GetState() == EOperationState::Orphaned) {
            if (OrphanedOperationQueueScanPeriodExecutor_) {
                OrphanedOperationQueueScanPeriodExecutor_->ScheduleOutOfBand();
            }
        } else if (TransientOperationQueueScanPeriodExecutor_) {
            TransientOperationQueueScanPeriodExecutor_->ScheduleOutOfBand();
        }

        YT_LOG_DEBUG("Operation added to transient queue (OperationId: %v, State: %v)",
            operation->GetId(),
            operation->GetState());
    }

    bool HandleWaitingForAgentOperation(const TOperationPtr& operation)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        const auto& agentTracker = Bootstrap_->GetControllerAgentTracker();
        auto agent = agentTracker->PickAgentForOperation(operation);
        if (!agent) {
            OperationToAgentAssignmentFailureTime_ = TInstant::Now();
            return false;
        }

        agentTracker->AssignOperationToAgent(operation, agent);

        auto eventAttributes = BuildYsonStringFluently()
            .BeginMap()
                .Item("controller_agent_address").Value(GetDefaultAddress(agent->GetAgentAddresses()))
            .EndMap();

        if (operation->RevivalDescriptor()) {
            operation->SetStateAndEnqueueEvent(EOperationState::ReviveInitializing, eventAttributes);
            operation->GetCancelableControlInvoker()->Invoke(
                BIND(&TImpl::DoReviveOperation, MakeStrong(this), operation));
        } else {
            operation->SetStateAndEnqueueEvent(EOperationState::Initializing, eventAttributes);
            operation->GetCancelableControlInvoker()->Invoke(
                BIND(&TImpl::DoInitializeOperation, MakeStrong(this), operation));
        }

        return true;
    }

    void HandleOrphanedOperation(const TOperationPtr& operation)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto operationId = operation->GetId();

        auto codicilGuard = operation->MakeCodicilGuard();

        YT_LOG_DEBUG("Handling orphaned operation (OperationId: %v)",
            operation->GetId());

        try {
            ValidateOperationState(operation, EOperationState::Orphaned);

            YT_VERIFY(operation->RevivalDescriptor());
            const auto& revivalDescriptor = *operation->RevivalDescriptor();

            YT_LOG_DEBUG("Operation revival descriptor flags "
                "(UserTransactionAborted: %v, OperationAborting: %v, OperationCommitted: %v, ShouldCommitOutputTransaction: %v, "
                "OperationId: %v)",
                revivalDescriptor.UserTransactionAborted,
                revivalDescriptor.OperationAborting,
                revivalDescriptor.OperationCommitted,
                revivalDescriptor.ShouldCommitOutputTransaction,
                operation->GetId());

            if (revivalDescriptor.OperationCommitted) {
                CompleteOperationWithoutRevival(operation);
                return;
            }

            if (revivalDescriptor.OperationAborting) {
                AbortOperationWithoutRevival(
                    operation,
                    TError("Operation aborted since it was found in \"aborting\" state during scheduler revival"));
                return;
            }

            if (auto abortionError = operation->GetOrhanedOperationAbortionError(); !abortionError.IsOK()) {
                AbortOperationWithoutRevival(
                    operation,
                    abortionError);
                return;
            }

            if (operation->GetRuntimeParameters()->SchedulingOptionsPerPoolTree.empty()) {
                AbortOperationWithoutRevival(
                    operation,
                    TError("Operation aborted since it has no active trees after revival"));
                return;
            }

            if (revivalDescriptor.UserTransactionAborted) {
                AbortOperationWithoutRevival(
                    operation,
                    GetUserTransactionAbortedError(operation->GetUserTransactionId()));
                return;
            }

            WaitFor(Strategy_->ValidateOperationStart(operation.Get()))
                .ThrowOnError();

            ValidateOperationState(operation, EOperationState::Orphaned);

            operation->SetStateAndEnqueueEvent(EOperationState::WaitingForAgent);
            AddOperationToTransientQueue(operation);
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Operation has failed to revive (OperationId: %v)",
                operationId);
            auto wrappedError = TError("Operation has failed to revive")
                << ex;
            OnOperationFailed(operation, wrappedError);
        }
    }

    void HandleOrphanedOperations()
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        if (auto delay = Config_->TestingOptions->HandleOrphanedOperationsDelay) {
            TDelayedExecutor::WaitForDuration(*delay);
        }

        auto& queuedOperations = StateToTransientOperations_[EOperationState::Orphaned];
        std::vector<TOperationPtr> operations;
        operations.reserve(queuedOperations.size());
        for (const auto& operation : queuedOperations) {
            if (operation->GetState() != EOperationState::Orphaned) {
                YT_LOG_DEBUG("Operation is no longer orphaned (OperationId: %v, State: %v)",
                    operation->GetId(),
                    operation->GetState());
                continue;
            }
            operations.push_back(operation);
        }
        queuedOperations.clear();

        if (operations.empty()) {
            return;
        }

        YT_LOG_INFO("Fetching revival descriptors for operations (OperationCount: %v)", operations.size());

        auto result = WaitFor(MasterConnector_->FetchOperationRevivalDescriptors(operations));
        if (!result.IsOK()) {
            YT_LOG_ERROR(result, "Error fetching revival descriptors");
            MasterConnector_->Disconnect(result);
            return;
        }

        YT_LOG_INFO("Revival descriptors are fetched (OperationCount: %v)", operations.size());

        for (const auto& operation : operations) {
            operation->GetCancelableControlInvoker()->Invoke(
                BIND(&TImpl::HandleOrphanedOperation, MakeStrong(this), operation));
        }
    }

    void ScanPendingOperations()
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        YT_LOG_DEBUG("Started scanning pending operations");

        Strategy_->ScanPendingOperations();

        YT_LOG_DEBUG("Finished scanning pending operations");
    }

    void ScanTransientOperationQueue()
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        YT_LOG_DEBUG("Started scanning transient operation queue");

        if (TInstant::Now() > OperationToAgentAssignmentFailureTime_ + Config_->OperationToAgentAssignmentBackoff) {
            int scannedOperationCount = 0;

            auto& queuedOperations = StateToTransientOperations_[EOperationState::WaitingForAgent];
            std::vector<TOperationPtr> newQueuedOperations;
            for (const auto& operation : queuedOperations) {
                if (operation->GetState() != EOperationState::WaitingForAgent) {
                    YT_LOG_DEBUG("Operation is no longer waiting for agent (OperationId: %v, State: %v)",
                        operation->GetId(),
                        operation->GetState());
                    continue;
                }
                ++scannedOperationCount;
                if (!HandleWaitingForAgentOperation(operation)) {
                    newQueuedOperations.push_back(operation);
                }
            }
            queuedOperations = std::move(newQueuedOperations);

            YT_LOG_DEBUG("Waiting for agent operations handled (OperationCount: %v)", scannedOperationCount);
        }

        YT_LOG_DEBUG("Finished scanning transient operation queue");
    }

    void OnOperationsRemovedFromCypress(const std::vector<TArchiveOperationRequest>& removedOperationRequests)
    {
        for (const auto& request : removedOperationRequests) {
            if (request.Alias) {
                // NB: Some other operation could have already used this alias (and even be removed after they completed),
                // so we check if it is still assigned to an operation id we expect.
                auto it = OperationAliases_.find(*request.Alias);
                if (it == OperationAliases_.end()) {
                    // This case may happen due to reordering of removal requests inside operation cleaner
                    // (e.g. some of the removal requests may fail due to lock conflict).
                    YT_LOG_DEBUG("Operation alias has already been removed (Alias: %v, OperationId: %v)",
                        request.Alias,
                        request.Id);
                } else if (it->second.OperationId == request.Id) {
                    // We should have already dropped the pointer to the operation. Let's assert that.
                    YT_VERIFY(!it->second.Operation);
                    YT_LOG_DEBUG("Operation alias is still assigned to an operation, removing it (Alias: %v, OperationId: %v)",
                        request.Alias,
                        request.Id);
                    OperationAliases_.erase(it);
                } else {
                    YT_LOG_DEBUG("Operation alias was reused by another operation, doing nothing "
                        "(Alias: %v, OldOperationId: %v, NewOperationId: %v)",
                        request.Alias,
                        request.Id,
                        it->second.OperationId);
                }
            }
        }
    }

    TPreprocessedSpec DoAssignExperimentsAndParseSpec(
        EOperationType type,
        const std::string& user,
        TYsonString specString,
        NYTree::INodePtr specTemplate)
    {
        YT_ASSERT_INVOKER_AFFINITY(GetBackgroundInvoker());

        auto specNode = ConvertSpecStringToNode(specString, Config_->OperationSpecTreeSizeLimit);
        auto providedSpecNode = CloneNode(specNode)->AsMap();
        providedSpecNode->RemoveChild("secure_vault");

        TPreprocessedSpec result;
        auto experimentAssignments = ExperimentsAssigner_.Assign(type, user, specNode);
        result.ExperimentAssignments.reserve(std::size(experimentAssignments));

        for (const auto& assignmentOrError : experimentAssignments) {
            if (!assignmentOrError.IsOK()) {
                result.ExperimentAssignmentErrors.emplace_back(assignmentOrError);
                continue;
            }

            const auto& assignment = assignmentOrError.Value();
            if (const auto& patch = assignment->Effect->SchedulerSpecTemplatePatch) {
                specTemplate = specTemplate ? PatchNode(specTemplate, patch) : patch;
            }
            if (const auto& patch = assignment->Effect->SchedulerSpecPatch) {
                specNode = PatchNode(specNode, patch)->AsMap();
            }
            if (const auto& controllerAgentTag = assignment->Effect->ControllerAgentTag) {
                specNode->AddChild("controller_agent_tag", ConvertToNode(*controllerAgentTag));
            }
            result.ExperimentAssignments.emplace_back(assignment);
        }

        ParseSpec(std::move(specNode), specTemplate, type, /*operationId*/ std::nullopt, &result);
        result.ProvidedSpecString = ConvertToYsonString(providedSpecNode);

        return result;
    }

    const THashMap<TString, TString>& GetUserDefaultParentPoolMap() const override
    {
        return UserToDefaultPoolMap_;
    }

    struct TOperationFinishedLogEventAttributes
    {
        TInstant LogInstant;

        // Already serialized light attributes.
        TYsonString SerializedLightAttributes;

        // Heavy attributes that will be copied/serialized in the offloading invoker.
        TYsonString Progress;
        TYsonString Alerts;
        TOperationRuntimeParametersPtr RuntimeParameters;
    };

    void LogOperationFinished(
        const TOperationPtr& operation,
        ELogEventType logEventType,
        const TError& error,
        TYsonString progress,
        TYsonString alerts)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        TOperationFinishedLogEventAttributes attributes{
            .Progress = std::move(progress),
            .Alerts = std::move(alerts),
            .RuntimeParameters = operation->GetRuntimeParameters(),
        };

        attributes.SerializedLightAttributes = BuildYsonStringFluently<EYsonType::MapFragment>()
            .Do(BIND(&TImpl::BuildOperationInfoForEventLog, MakeStrong(this), operation))
            .Do(std::bind(&ISchedulerStrategy::BuildOperationInfoForEventLog, Strategy_, operation.Get(), _1))
            .Item("error").Value(error)
            .Finish();

        EventLoggingActionQueue_->GetInvoker()
            ->Invoke(BIND(
                &TImpl::DoLogOperationFinished,
                MakeStrong(this),
                logEventType,
                std::move(attributes),
                /*logInstant*/ TInstant::Now()));
    }

    void DoLogOperationFinished(
        ELogEventType logEventType,
        const TOperationFinishedLogEventAttributes& attributes,
        TInstant logInstant)
    {
        YT_ASSERT_INVOKER_AFFINITY(EventLoggingActionQueue_->GetInvoker());

        LogEventFluently(
            &SchedulerStructuredLogger(),
            OffloadedEventLogWriterConsumer_.get(),
            logEventType,
            logInstant)
            .Items(attributes.SerializedLightAttributes)
            .DoIf(attributes.Progress.operator bool(), [&] (TFluentMap fluent) {
                fluent.Item("progress").Value(attributes.Progress);
            })
            .DoIf(attributes.Alerts.operator bool(), [&] (TFluentMap fluent) {
                fluent.Item("alerts").Value(attributes.Alerts);
            })
            .Item("runtime_parameters").Value(attributes.RuntimeParameters);
    }

    void EraseOffloadingTrees(const TOperationPtr& operation)
    {
        std::vector<TString> offloadingTrees;
        for (const auto& [treeName, options] : operation->GetRuntimeParameters()->SchedulingOptionsPerPoolTree) {
            if (options->Offloading) {
                offloadingTrees.push_back(treeName);
            }
        }

        UnregisterOperationFromTrees(operation, offloadingTrees);
    }

    class TOperationService
        : public TVirtualMapBase
    {
    public:
        TOperationService(const TScheduler::TImpl* scheduler, const TOperationPtr& operation)
            : TVirtualMapBase(/*owningNode*/ nullptr)
            , Operation_(operation)
            , OperationIdService_(
                IYPathService::FromProducer(BIND([operationId = operation->GetId()] (IYsonConsumer* consumer) {
                    BuildYsonFluently(consumer).Value(operationId);
                })))
            , OperationProgressService_(
                IYPathService::FromProducer(
                    BIND([strategy = scheduler->Strategy_, operationId = operation->GetId()] (IYsonConsumer* consumer) {
                        BuildYsonFluently(consumer)
                            .BeginMap()
                                .Do(BIND(&ISchedulerStrategy::BuildOperationProgress, strategy, operationId))
                            .EndMap();
                    }))
                    ->Via(scheduler->GetControlInvoker(EControlQueue::DynamicOrchid)))
            , OperationBriefProgressService_(
                IYPathService::FromProducer(
                    BIND([strategy = scheduler->Strategy_, operationId = operation->GetId()] (IYsonConsumer* consumer) {
                        BuildYsonFluently(consumer)
                            .BeginMap()
                                .Do(BIND(&ISchedulerStrategy::BuildBriefOperationProgress, strategy, operationId))
                            .EndMap();
                    }))
                    ->Via(scheduler->GetControlInvoker(EControlQueue::DynamicOrchid)))
        { }

        i64 GetSize() const override
        {
            return 3;
        }

        std::vector<std::string> GetKeys(i64 limit) const override
        {
            std::vector<std::string> keys{"operation_id", "progress", "brief_progress"};
            if (limit < std::ssize(keys)) {
                keys.resize(limit);
            }
            return keys;
        }

        IYPathServicePtr FindItemService(const std::string& key) const override
        {
            if (key == "operation_id") {
                return OperationIdService_;
            } else if (key == "progress") {
                return OperationProgressService_;
            } else if (key == "brief_progress") {
                return OperationBriefProgressService_;
            } else {
                return nullptr;
            }
        }

    private:
        const TOperationPtr Operation_;

        IYPathServicePtr OperationIdService_;
        IYPathServicePtr OperationProgressService_;
        IYPathServicePtr OperationBriefProgressService_;
    };

    class TOperationsService
        : public TVirtualMapBase
    {
    public:
        explicit TOperationsService(const TScheduler::TImpl* scheduler)
            : TVirtualMapBase(/*owningNode*/ nullptr)
            , Scheduler_(scheduler)
        { }

        i64 GetSize() const override
        {
            return Scheduler_->IdToOperationService_.size() + Scheduler_->OperationAliases_.size();
        }

        std::vector<std::string> GetKeys(i64 limit) const override
        {
            std::vector<std::string> keys;
            keys.reserve(std::min(limit, std::ssize(Scheduler_->IdToOperation_) + std::ssize(Scheduler_->OperationAliases_)));
            for (const auto& [operationId, operation] : Scheduler_->IdToOperation_) {
                if (std::ssize(keys) >= limit) {
                    break;
                }
                keys.push_back(ToString(operationId));
            }
            for (const auto& [aliasString, alias] : Scheduler_->OperationAliases_) {
                if (std::ssize(keys) >= limit) {
                    break;
                }
                keys.push_back(aliasString);
            }
            return keys;
        }

        IYPathServicePtr FindItemService(const std::string& key) const override
        {
            if (key.starts_with(OperationAliasPrefix)) {
                // If operation is still registered, we will return the operation service.
                // If it has finished, but we still have an entry in alias -> operation id internal
                // mapping, we return a fictive map { operation_id = <operation_id> }. It is useful
                // for alias resolution when operation is not archived yet but already finished.
                auto it = Scheduler_->OperationAliases_.find(TString(key));
                if (it == Scheduler_->OperationAliases_.end()) {
                    return nullptr;
                } else {
                    auto jt = Scheduler_->IdToOperationService_.find(it->second.OperationId);
                    if (jt == Scheduler_->IdToOperationService_.end()) {
                        // The operation is unregistered, but we still return a fictive map.
                        return IYPathService::FromProducer(BIND([=] (IYsonConsumer* consumer) {
                            BuildYsonFluently(consumer)
                                .BeginMap()
                                    .Item("operation_id").Value(it->second.OperationId)
                                .EndMap();
                        }));
                    } else {
                        return jt->second;
                    }
                }
            } else {
                auto operationId = TOperationId(TGuid::FromString(key));
                auto it = Scheduler_->IdToOperationService_.find(operationId);
                return it == Scheduler_->IdToOperationService_.end() ? nullptr : it->second;
            }
        }

    private:
        const TScheduler::TImpl* const Scheduler_;
    };

    class TAllocationsService
        : public TVirtualMapBase
    {
    public:
        explicit TAllocationsService(const TScheduler::TImpl* scheduler)
            : TVirtualMapBase(/*owningNode*/ nullptr)
            , Scheduler_(scheduler)
        { }

        void GetSelf(
            TReqGet* /*request*/,
            TRspGet* /*response*/,
            const TCtxGetPtr& context) override
        {
            ThrowMethodNotSupported(context->GetMethod());
        }

        void ListSelf(
            TReqList* /*request*/,
            TRspList* /*response*/,
            const TCtxListPtr& context) override
        {
            ThrowMethodNotSupported(context->GetMethod());
        }

        i64 GetSize() const override
        {
            YT_ABORT();
        }

        std::vector<std::string> GetKeys(i64 /*limit*/) const override
        {
            YT_ABORT();
        }

        IYPathServicePtr FindItemService(const std::string& key) const override
        {
            auto allocationId = TAllocationId(TGuid::FromString(key));
            auto buildAllocationYsonCallback = BIND(
                &TAllocationsService::BuildAllocationYson,
                MakeStrong(this),
                allocationId);

            return IYPathService::FromProducer(buildAllocationYsonCallback)
                ->Via(Scheduler_->GetControlInvoker(EControlQueue::DynamicOrchid));
        }

    private:
        void BuildAllocationYson(TAllocationId allocationId, IYsonConsumer* consumer) const
        {
            auto allocationDescription = Scheduler_->GetAllocationDescription(allocationId);

            auto allocationYson = BuildYsonStringFluently()
                .BeginMap()
                    .Item("running").Value(allocationDescription.Running)
                    .Item("node_id").Value(allocationDescription.NodeId)
                    .OptionalItem("node_address", allocationDescription.NodeAddress)
                    .DoIf(allocationDescription.Properties.has_value(), [&] (TFluentMap fluent) {
                        const auto& allocationProperties = *allocationDescription.Properties;

                        YT_VERIFY(allocationProperties.OperationId);
                        fluent
                            .Item("operation_id").Value(allocationProperties.OperationId)
                            .Item("start_time").Value(allocationProperties.StartTime)
                            .Item("state").Value(allocationProperties.State)
                            .Item("tree_id").Value(allocationProperties.TreeId)
                            .Item("preempted").Value(allocationProperties.Preempted)
                            .Item("preemption_reason").Value(allocationProperties.PreemptionReason)
                            .Item("preemption_timeout").Value(allocationProperties.PreemptionTimeout)
                            .Item("preemptible_progress_start_time").Value(allocationProperties.PreemptibleProgressStartTime);

                        auto const& operation = Scheduler_->FindOperation(allocationProperties.OperationId);
                        if (operation) {
                            const auto& agent = operation->GetAgentOrThrow();

                            if (agent) {
                                fluent
                                    .Item("controller_agent")
                                        .BeginMap()
                                            .Item("address").Value(agent->GetAgentAddresses())
                                            .Item("id").Value(agent->GetId())
                                            .Item("incarnation_id").Value(agent->GetIncarnationId())
                                        .EndMap();
                            }
                        }
                    })
                .EndMap();

            consumer->OnRaw(allocationYson);
        }

        const TScheduler::TImpl* Scheduler_;
    };
};

////////////////////////////////////////////////////////////////////////////////

TScheduler::TScheduler(
    TSchedulerConfigPtr config,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

TScheduler::~TScheduler() = default;

void TScheduler::Initialize()
{
    Impl_->Initialize();
}

ISchedulerStrategyPtr TScheduler::GetStrategy() const
{
    return Impl_->GetStrategy();
}

const TOperationsCleanerPtr& TScheduler::GetOperationsCleaner() const
{
    return Impl_->GetOperationsCleaner();
}

IYPathServicePtr TScheduler::CreateOrchidService() const
{
    return Impl_->CreateOrchidService();
}

TRefCountedExecNodeDescriptorMapPtr TScheduler::GetCachedExecNodeDescriptors() const
{
    return Impl_->GetCachedExecNodeDescriptors();
}

TSharedRef TScheduler::GetCachedProtoExecNodeDescriptors() const
{
    return Impl_->GetCachedProtoExecNodeDescriptors();
}

const TSchedulerConfigPtr& TScheduler::GetConfig() const
{
    return Impl_->GetConfig();
}

IInvokerPtr TScheduler::GetBackgroundInvoker() const
{
    return Impl_->GetBackgroundInvoker();
}

const TNodeManagerPtr& TScheduler::GetNodeManager() const
{
    return Impl_->GetNodeManager();
}

bool TScheduler::IsConnected() const
{
    return Impl_->IsConnected();
}

void TScheduler::ValidateConnected()
{
    Impl_->ValidateConnected();
}

TMasterConnector* TScheduler::GetMasterConnector() const
{
    return Impl_->GetMasterConnector();
}

void TScheduler::Disconnect(const TError& error)
{
    Impl_->Disconnect(error);
}

TOperationPtr TScheduler::FindOperation(TOperationId id) const
{
    return Impl_->FindOperation(id);
}

TOperationPtr TScheduler::GetOperationOrThrow(const TOperationIdOrAlias& idOrAlias) const
{
    return Impl_->GetOperationOrThrow(idOrAlias);
}

TFuture<TPreprocessedSpec> TScheduler::AssignExperimentsAndParseSpec(
    EOperationType type,
    const std::string& user,
    TYsonString specString) const
{
    return Impl_->AssignExperimentsAndParseSpec(
        type,
        user,
        std::move(specString));
}

TFuture<TOperationPtr> TScheduler::StartOperation(
    EOperationType type,
    TTransactionId transactionId,
    TMutationId mutationId,
    const std::string& user,
    TPreprocessedSpec preprocessedSpec)
{
    return Impl_->StartOperation(
        type,
        transactionId,
        mutationId,
        user,
        std::move(preprocessedSpec));
}

TFuture<void> TScheduler::AbortOperation(
    TOperationPtr operation,
    const TError& error,
    const std::string& user)
{
    return Impl_->AbortOperation(operation, error, user);
}

TFuture<void> TScheduler::SuspendOperation(
    TOperationPtr operation,
    const std::string& user,
    bool abortRunningAllocations,
    const std::optional<std::string>& reason)
{
    return Impl_->SuspendOperation(operation, user, abortRunningAllocations, reason);
}

TFuture<void> TScheduler::ResumeOperation(
    TOperationPtr operation,
    const std::string& user)
{
    return Impl_->ResumeOperation(operation, user);
}

TFuture<void> TScheduler::CompleteOperation(
    TOperationPtr operation,
    const TError& error,
    const std::string& user)
{
    return Impl_->CompleteOperation(operation, error, user);
}

void TScheduler::OnOperationCompleted(const TOperationPtr& operation)
{
    Impl_->OnOperationCompleted(operation);
}

void TScheduler::OnOperationAborted(const TOperationPtr& operation, const TError& error)
{
    Impl_->OnOperationAborted(operation, error);
}

void TScheduler::OnOperationFailed(const TOperationPtr& operation, const TError& error)
{
    Impl_->OnOperationFailed(operation, error);
}

void TScheduler::OnOperationSuspended(const TOperationPtr& operation, const TError& error)
{
    Impl_->OnOperationSuspended(operation, error);
}

void TScheduler::OnOperationAgentUnregistered(const TOperationPtr& operation)
{
    Impl_->OnOperationAgentUnregistered(operation);
}

void TScheduler::OnOperationBannedInTentativeTree(const TOperationPtr& operation, const TString& treeId, const std::vector<TAllocationId>& allocationIds)
{
    Impl_->OnOperationBannedInTentativeTree(operation, treeId, allocationIds);
}

TFuture<void> TScheduler::UpdateOperationParameters(
    TOperationPtr operation,
    const std::string& user,
    INodePtr parameters)
{
    return Impl_->UpdateOperationParameters(operation, user, parameters);
}

TFuture<void> TScheduler::PatchOperationSpec(
    TOperationPtr operation,
    const std::string& user,
    TSpecPatchList patches)
{
    return Impl_->PatchOperationSpec(std::move(operation), user, std::move(patches));
}

void TScheduler::ProcessNodeHeartbeat(const TCtxNodeHeartbeatPtr& context)
{
    Impl_->ProcessNodeHeartbeat(context);
}

TSerializableAccessControlList TScheduler::GetOperationBaseAcl() const
{
    return Impl_->GetOperationBaseAcl();
}

int TScheduler::GetOperationsArchiveVersion() const
{
    return Impl_->GetOperationsArchiveVersion();
}

TString TScheduler::FormatResources(const TJobResourcesWithQuota& resources) const
{
    return Impl_->FormatResources(resources);
}

TFuture<void> TScheduler::SetOperationAlert(
        TOperationId operationId,
        EOperationAlertType alertType,
        const TError& alert,
        std::optional<TDuration> timeout)
{
    return Impl_->SetOperationAlert(operationId, alertType, alert, timeout);
}

TFuture<void> TScheduler::ValidateJobShellAccess(
    const std::string& user,
    const TString& jobShellName,
    const std::vector<TString>& jobShellOwners)
{
    return Impl_->ValidateJobShellAccess(user, jobShellName, jobShellOwners);
}

TFuture<TOperationId> TScheduler::FindOperationIdByAllocationId(TAllocationId allocationId) const
{
    return Impl_->FindOperationIdByAllocationId(allocationId);
}

const IResponseKeeperPtr& TScheduler::GetOperationServiceResponseKeeper() const
{
    return Impl_->GetOperationServiceResponseKeeper();
}

TAllocationBriefInfo TScheduler::GetAllocationBriefInfo(
    TAllocationId allocationId,
    TAllocationInfoToRequest requestedAllocationInfo) const
{
    return Impl_->GetAllocationBriefInfo(allocationId, requestedAllocationInfo);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
