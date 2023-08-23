#pragma once

#include "public.h"
#include "automaton.h"

#include <yt/yt/server/master/chunk_server/public.h>

#include <yt/yt/server/master/cypress_server/public.h>

#include <yt/yt/server/lib/hydra_common/serialize.h>
#include <yt/yt/server/lib/hydra_common/checkpointable_stream.h>

#include <yt/yt/server/master/node_tracker_server/public.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/master/security_server/public.h>

#include <yt/yt/server/master/table_server/public.h>

#include <yt/yt/server/master/tablet_server/public.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/core/concurrency/thread_pool.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

NHydra::TReign GetCurrentReign();
bool ValidateSnapshotReign(NHydra::TReign reign);
NHydra::EFinalRecoveryAction GetActionToRecoverFromReign(NHydra::TReign reign);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMasterReign,
    // 22.4 starts here.
    ((ZookeeperShards)                                              (2200))  // gritukan
    ((QueueAgentStageWritabilityAndDefaults)                        (2201))  // achulkov2
    ((UserPassword)                                                 (2202))  // gritukan
    ((SetUserPassword)                                              (2203))  // gritukan
    ((RefCountedCoWs)                                               (2204))  // babenko
    ((RemovableQueueAgentStage)                                     (2205))  // achulkov2
    ((FixTransactionRotator)                                        (2206))  // kvk1920
    ((BundleControllerConfigAttribute)                              (2207))  // capone212
    ((FixNodeRegistration)                                          (2208))  // kvk1920
    ((ChunkReincarnator)                                            (2209))  // kvk1920
    ((InternedForcedChunkViewCompactionRevision)                    (2210))  // ifsmirnov
    ((MaintenanceRequests)                                          (2211))  // kvk1920
    ((HydraDynamicConfig)                                           (2212))  // aleksandra-zh
    ((RecomputeAccountRefCounters)                                  (2213))  // gritukan
    ((SharedLockIsEnoughForMountConfig)                             (2214))  // ifsmirnov
    ((RemoveCacheMedium)                                            (2215))  // gritukan
    ((PerAccountMergerStatistics)                                   (2216))  // aleksandra-zh
    ((SplitNodeDisposal)                                            (2217))  // aleksandra-zh
    ((MultisetAttributesReplication)                                (2218))  // shakurov
    ((SetLastMountTransactionInMount)                               (2219))  // savrus
    ((ChunkFormat)                                                  (2220))  // babenko
    ((DropDNLCompats)                                               (2221))  // akozhikhov
    ((MountConfigExperiments)                                       (2222))  // ifsmirnov
    ((RemountNeededNotification)                                    (2223))  // ifsmirnov
    ((FixZombification)                                             (2224))  // gritukan
    ((TabletBalancerConfigUnrecognizedStrategyKeep)                 (2225))  // alexelexa
    ((FixClonedTrunkNodeStatistics_22_4)                            (2226))  // shakurov
    ((FixAccountResourceUsageCharge)                                (2227))  // gritukan
    ((FixTouchTime)                                                 (2228))  // shakurov
    ((AllowSettingChunkMergerMode)                                  (2229))  // aleksandra-zh
    ((ObjectRevisions)                                              (2230))  // shakurov
    ((IncreaseCollocationSizeLimit)                                 (2231))  // akozhikhov
    ((FlagToDisableIncomingReplication)                             (2232))  // akozhikhov
    ((FixTouchTime2)                                                (2233))  // shakurov
    // 23.1
    ((RipEnableUnlockCommand)                                       (2300))  // babenko
    ((RipEnableRevisionChangingForBuiltinAttributes)                (2301))  // babenko
    ((RipForbidSetCommand)                                          (2302))  // babenko
    ((RootstocksAndScions)                                          (2303))  // gritukan
    ((FixClonedTrunkNodeStatistics)                                 (2304))  // shakurov
    ((MultisetAttributesForEveryone)                                (2305))  // kvk1920
    ((AddTabletMountTime)                                           (2306))  // alexelexa
    ((ThrowOnNullColumnMount)                                       (2307))  // alexelexa
    ((SequoiaCreate)                                                (2308))  // gritukan
    ((FixAttachValidation)                                          (2309))  // gritukan
    ((RemoveNewHydraFlag)                                           (2310))  // aleksandra-zh
    ((HashTableChunkIndex)                                          (2311))  // akozhikhov
    ((HistoricallyNonVital)                                         (2312))  // gritukan
    ((DeprecateCypressListNodes)                                    (2313))  // kvk1920
    ((ConfigurableCollocationSizeLimit)                             (2314))  // akozhikhov
    ((GeneralizeMaintenanceRequestsApi)                             (2315))  // kvk1920
    ((ReadRequestComplexityLimits)                                  (2316))  // kvk1920
    ((DropChunkExpirationTracker)                                   (2317))  // shakurov
    ((CypressTransactions)                                          (2318))  // gritukan
    ((UseMetadataCellIds)                                           (2319))  // ponasenko-rs
    ((UpdatePerUserThrottlerLimits)                                 (2320))  // h0pless
    ((RequireMediumUsePermissionForChunkOwnerCreation)              (2321))  // kvk1920
    ((MakePerformanceCountersOpaque)                                (2322))  // alexelexa
    ((FixMulticellHunkStorage)                                      (2323))  // gritukan
    ((FixAttachHunksWithDynamicStoreRead)                           (2324))  // aleksandra-zh
    ((BundlesBan)                                                   (2325))  // alexelexa
    ((TooManyLocksCheck)                                            (2326))  // h0pless
    ((RemoveDefaultSecondaryRoles)                                  (2327))  // aleksandra-zh
    ((MasterCellChunkStatisticsCollector)                           (2328))  // kvk1920
    ((FixHunkChunksAttach)                                          (2329))  // gritukan
    ((ExportMasterTableSchemas)                                     (2330))  // h0pless
    ((SupportAccountChunkMergerCriteria)                            (2331))  // danilalexeev
    ((PerUserReadRequestComplexityLimits)                           (2332))  // kvk1920
    ((SimplerChunkExportDataSaveLoad)                               (2333))  // shakurov
    ((MoveReplicatorEnabledCheckPeriodToDynamicConfig)              (2334))  // danilalexeev
    ((FixAlterWithSchemaId)                                         (2335))  // h0pless
    ((LocationDirectory)                                            (2336))  // kvk1920
    ((FixChunkCreationTimeHistogram)                                (2337))  // kvk1920
    ((HunksBackup)                                                  (2338))  // akozhikhov
    ((SysOperationsTransactionAction)                               (2339))  // kvk1920
    ((ExportEmptyMasterTableSchemas)                                (2340))  // h0pless
    ((LimitParallelismOfCfr)                                        (2341))  // akozhikhov
    ((ExTransactionCoordinatorCellRole)                             (2342))  // shakurov
    ((RecomputeMasterTableSchemaRefCounters)                        (2343))  // h0pless
    ((FixBulkInsertAtomicityNone)                                   (2344))  // ifsmirnov
    ((DropNodesWithFlavorsVectorFromSnapshot)                       (2345))  // shakurov
    ((FixSymlinkCyclicityCheck)                                     (2346))  // h0pless
    ((FixChunkCreationTimeHistograms)                               (2347))  // gritukan
    ((IncludeOnlyOldStyleMountConfigAttributesInList)               (2348))  // ifsmirnov
    ((RefactorSchemaExport)                                         (2349))  // h0pless
    ((MaxErasureJournalReplicasPerRack)                             (2350))  // vovamelnikov
    ((FixChunkCreationTimeHistogramAgain)                           (2351))  // kvk1920
    ((ReliableNodeStateGossip)                                      (2352))  // aleksandra-zh
    ((ReadRequestComplexityLimitsToggle)                            (2353))  // kvk1920
    // 23.2 starts here.
    ((TabletServants)                                               (2400))  // ifsmirnov
    ((MediumBase)                                                   (2401))  // gritukan
    ((S3Medium)                                                     (2402))  // gritukan
    ((ColumnRenamingSeparateFlags)                                  (2403))  // orlovorlov
    ((MasterCellChunkStatisticsCollectorConfig)                     (2404))  // kvk1920
    ((QueueReplicatedTablesList)                                    (2405))  // cherepashka
    ((PendingRestartMaintenanceFlag)                                (2406))  // danilalexeev
    ((MakeDestroyedReplicasSetSharded)                              (2407))  // danilalexeev
    ((AvenuesInTabletManager)                                       (2408))  // ifsmirnov
    ((ChaosReplicatedQueuesAndConsumersList)                        (2409))  // cherepashka
    ((ValidateTableSettingsInTabletActions)                         (2410))  // alexelexa
    ((GetRidOfCellIndex)                                            (2411))  // kvk1920
    ((DontForgetToCommitInSetNodeByYPath)                           (2412))  // kvk1920
    ((ResetErrorCountOfUnmountedTablets)                            (2413))  // alexelexa
    ((SequoiaReplicas)                                              (2414))  // aleksandra-zh
    ((AutoTurnOffPendingRestartMaintenanceFlag)                     (2415))  // danilalexeev
    ((AllowSetMountConfigUnderTransaction)                          (2416))  // dave11ar
    ((AddChunkSchemas)                                              (2417))  // h0pless
    ((ChaosManagerSnapshotSaveAndLoadMovement)                      (2418))  // cherepashka
);

////////////////////////////////////////////////////////////////////////////////

class TSaveContext
    : public NHydra::TSaveContext
{
public:
    TSaveContext(
        NHydra::ICheckpointableOutputStream* output,
        NLogging::TLogger logger,
        NConcurrency::IThreadPoolPtr backgroundThreadPool);
    TSaveContext(
        IZeroCopyOutput* output,
        const TSaveContext* parentContext);

    TEntitySerializationKey RegisterInternedYsonString(NYson::TYsonString str);

    EMasterReign GetVersion();

private:
    const TSaveContext* const ParentContext_ = nullptr;

    using TYsonStringMap = THashMap<NYson::TYsonString, TEntitySerializationKey>;
    TYsonStringMap InternedYsonStrings_;
};

////////////////////////////////////////////////////////////////////////////////

class TLoadContext
    : public NHydra::TLoadContext
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TBootstrap*, Bootstrap);

public:
    TLoadContext(
        TBootstrap* bootstrap,
        NHydra::ICheckpointableInputStream* input);

    NObjectServer::TObject* GetWeakGhostObject(NObjectServer::TObjectId id) const;

    template <class T>
    const TInternRegistryPtr<T>& GetInternRegistry() const;

    NYson::TYsonString GetInternedYsonString(TEntitySerializationKey key);
    TEntitySerializationKey RegisterInternedYsonString(NYson::TYsonString str);

    EMasterReign GetVersion();

private:
    std::vector<NYson::TYsonString> InternedYsonStrings_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster

#define SERIALIZE_INL_H_
#include "serialize-inl.h"
#undef SERIALIZE_INL_H_
