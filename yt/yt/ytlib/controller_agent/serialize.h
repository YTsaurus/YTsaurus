#pragma once

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESnapshotVersion,
    // 24.1 starts here
    ((BumpTo_24_1)                           (301500))
    ((InterruptionReasonInJoblet)            (301501))
    ((PersistMonitoringCounts)               (301502))
    ((WaitingForResourcesDuration)           (301503))
    ((ForceAllowJobInterruption)             (301504))
    ((BatchRowCount_24_1)                    (301505))
    ((InputManagerIntroduction)              (301506))
    ((ChunkSliceStatistics)                  (301507))
    ((AllocationMap)                         (301508))
    ((SingleChunkTeleportStrategy)           (301509))
    ((OutputNodeDirectory)                   (301510))
    ((RemoteInputForOperations)              (301511))
    ((JobDeterminismValidation)              (301512))
    ((MultipleJobsInAllocation)              (301513))
    ((OperationIncarnationInJoblet)          (301514))
    ((TeleportedOutputRowCount)              (301515))
    // 24.2 starts here
    ((BumpTo_24_2)                           (301600))
    ((DropLegacyWirePartitionKeys)           (301601))
    ((VersionedMapReduceWrite)               (301602))
    ((IntroduceGangManager)                  (301603))
    ((DropOriginalTableSchemaRevision)       (301604))
    ((AcoName)                               (301605))
    ((DisableShrinkingJobs)                  (301606))
    ((MultipleOrderedTasks)                  (301607))
    ((RemoteCopyDynamicTableWithHunks)       (301608))
    ((JobFailTolerance)                      (301609))
    ((NewJobsForbiddenReason)                (301610))
    // 25.1 starts here
    ((BumpTo_25_1)                           (301700))
    ((OperationIncarnationIsStrongTypedef)   (301701))
    ((PhoenixSchema)                         (301702))
    ((PreserveJobCookieForAllocationInGangs) (301703))
    ((ThrottlingOfRemoteReads)               (301704))
    ((TableWriteBufferEstimation)            (301705))
    ((OperationIncarnationIsOptional)        (301706))
    ((GroupedNeededResources)                (301707))
    ((AddAddressesToJob)                     (301708))
);

////////////////////////////////////////////////////////////////////////////////

ESnapshotVersion GetCurrentSnapshotVersion();
bool ValidateSnapshotVersion(int version);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
