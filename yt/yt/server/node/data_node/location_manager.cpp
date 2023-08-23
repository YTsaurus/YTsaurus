#include "location_manager.h"

#include "bootstrap.h"
#include "private.h"

#include <yt/yt/server/node/data_node/chunk_store.h>

#include <yt/yt/server/lib/misc/reboot_manager.h>

#include <yt/yt/library/containers/disk_manager/disk_info_provider.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/atomic_object.h>
#include <yt/yt/core/misc/fs.h>

namespace NYT::NDataNode {

using namespace NClusterNode;
using namespace NConcurrency;
using namespace NContainers;
using namespace NFS;
using namespace NProfiling;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TLocationManager::TLocationManager(
    IBootstrap* bootstrap,
    TChunkStorePtr chunkStore,
    IInvokerPtr controlInvoker,
    TDiskInfoProviderPtr diskInfoProvider)
    : DiskInfoProvider_(std::move(diskInfoProvider))
    , ChunkStore_(std::move(chunkStore))
    , ControlInvoker_(std::move(controlInvoker))
    , OrchidService_(CreateOrchidService())
{
    bootstrap->SubscribePopulateAlerts(
        BIND(&TLocationManager::PopulateAlerts, MakeWeak(this)));
}

void TLocationManager::BuildOrchid(IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("disk_ids_mismatched").Value(DiskIdsMismatched_.load())
        .EndMap();
}

IYPathServicePtr TLocationManager::CreateOrchidService()
{
    return IYPathService::FromProducer(BIND(&TLocationManager::BuildOrchid, MakeStrong(this)))
        ->Via(ControlInvoker_);
}

IYPathServicePtr TLocationManager::GetOrchidService()
{
    return OrchidService_;
}

TFuture<void> TLocationManager::FailDiskByName(
    const TString& diskName,
    const TError& error)
{
    return DiskInfoProvider_->GetYTDiskInfos()
        .Apply(BIND([=, this, this_ = MakeStrong(this)] (const std::vector<TDiskInfo>& diskInfos) {
            for (const auto& diskInfo : diskInfos) {
                if (diskInfo.DeviceName == diskName &&
                    diskInfo.State == NContainers::EDiskState::Ok)
                {
                    // Try to fail not accessible disk.
                    return DiskInfoProvider_->FailDisk(
                        diskInfo.DiskId,
                        error.GetMessage())
                        .Apply(BIND([=] (const TError& result) {
                            if (!result.IsOK()) {
                                YT_LOG_ERROR(result,
                                    "Error marking the disk as failed (DiskName: %v)",
                                    diskInfo.DeviceName);
                            }
                        }));
                }
            }

            return VoidFuture;
        }));
}

void TLocationManager::PopulateAlerts(std::vector<TError>* alerts)
{
    for (const auto* alertHolder : {&DiskFailedAlert_}) {
        if (auto alert = alertHolder->Load(); !alert.IsOK()) {
            alerts->push_back(std::move(alert));
        }
    }
}

void TLocationManager::SetDiskAlert(TError alert)
{
    if (DiskFailedAlert_.Load().IsOK() && !alert.IsOK()) {
        DiskFailedAlert_.Store(alert);
    } else if (!DiskFailedAlert_.Load().IsOK() && alert.IsOK()) {
        DiskFailedAlert_.Store(alert);
    }
}

std::vector<TLocationLivenessInfo> TLocationManager::MapLocationToLivenessInfo(
    const std::vector<TDiskInfo>& disks)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    THashMap<TString, TString> diskNameToId;
    THashSet<TString> failedDisks;

    for (const auto& disk : disks) {
        diskNameToId.emplace(std::make_pair(disk.DeviceName, disk.DiskId));
        if (disk.State == NContainers::EDiskState::Failed) {
            failedDisks.insert(disk.DeviceName);
        }
    }

    std::vector<TLocationLivenessInfo> locationLivenessInfos;
    const auto& locations = ChunkStore_->Locations();
    locationLivenessInfos.reserve(locations.size());

    for (const auto& location : locations) {
        auto it = diskNameToId.find(location->GetStaticConfig()->DeviceName);

        if (it == diskNameToId.end()) {
            YT_LOG_ERROR("Unknown location disk (DeviceName: %v)",
                location->GetStaticConfig()->DeviceName);
        } else {
            locationLivenessInfos.push_back(TLocationLivenessInfo{
                .Location = location,
                .DiskId = it->second,
                .LocationState = location->GetState(),
                .IsDiskAlive = !failedDisks.contains(location->GetStaticConfig()->DeviceName)
            });
        }
    }

    return locationLivenessInfos;
}

TFuture<std::vector<TLocationLivenessInfo>> TLocationManager::GetLocationsLiveness()
{
    return DiskInfoProvider_->GetYTDiskInfos()
        .Apply(BIND(&TLocationManager::MapLocationToLivenessInfo, MakeStrong(this))
        .AsyncVia(ControlInvoker_));
}

TFuture<std::vector<TDiskInfo>> TLocationManager::GetDiskInfos()
{
    return DiskInfoProvider_->GetYTDiskInfos();
}

const std::vector<TString>& TLocationManager::GetConfigDiskIds()
{
    return DiskInfoProvider_->GetConfigDiskIds();
}

void TLocationManager::UpdateOldDiskIds(THashSet<TString> oldDiskIds)
{
    OldDiskIds_ = oldDiskIds;
}

const THashSet<TString>& TLocationManager::GetOldDiskIds() const
{
    return OldDiskIds_;
}

std::vector<TGuid> TLocationManager::DoDisableLocations(const THashSet<TGuid>& locationUuids)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    std::vector<TGuid> locationsForDisable;

    for (const auto& location : ChunkStore_->Locations()) {
        if (locationUuids.contains(location->GetUuid())) {
            // Manual location disable.
            auto result = location->ScheduleDisable(TError("Manual location disabling")
                << TErrorAttribute("location_uuid", location->GetUuid())
                << TErrorAttribute("location_path", location->GetPath())
                << TErrorAttribute("location_disk", location->GetStaticConfig()->DeviceName));

            if (result) {
                locationsForDisable.push_back(location->GetUuid());
            }
        }
    }

    return locationsForDisable;
}

std::vector<TGuid> TLocationManager::DoDestroyLocations(const THashSet<TGuid>& locationUuids)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    std::vector<TGuid> locationsForDestroy;

    for (const auto& location : ChunkStore_->Locations()) {
        if (locationUuids.contains(location->GetUuid())) {
            // Manual location destroy.
            if (location->StartDestroy()) {
                locationsForDestroy.push_back(location->GetUuid());
            }
        }
    }

    return locationsForDestroy;
}

std::vector<TGuid> TLocationManager::DoResurrectLocations(const THashSet<TGuid>& locationUuids)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    std::vector<TGuid> locationsForResurrect;

    for (const auto& location : ChunkStore_->Locations()) {
        if (locationUuids.contains(location->GetUuid())) {
            // Manual location resurrect.

            if (location->Resurrect()) {
                locationsForResurrect.push_back(location->GetUuid());
            }
        }
    }

    return locationsForResurrect;
}

TFuture<std::vector<TGuid>> TLocationManager::DestroyChunkLocations(const THashSet<TGuid>& locationUuids)
{
    return BIND(&TLocationManager::DoDestroyLocations, MakeStrong(this))
        .AsyncVia(ControlInvoker_)
        .Run(locationUuids);
}

TFuture<std::vector<TGuid>> TLocationManager::DisableChunkLocations(const THashSet<TGuid>& locationUuids)
{
    return BIND(&TLocationManager::DoDisableLocations, MakeStrong(this))
        .AsyncVia(ControlInvoker_)
        .Run(locationUuids);
}

TFuture<std::vector<TGuid>> TLocationManager::ResurrectChunkLocations(const THashSet<TGuid>& locationUuids)
{
    return BIND(&TLocationManager::DoResurrectLocations, MakeStrong(this))
        .AsyncVia(ControlInvoker_)
        .Run(locationUuids);
}

TFuture<void> TLocationManager::RecoverDisk(const TString& diskId)
{
    return DiskInfoProvider_->RecoverDisk(diskId);
}

void TLocationManager::SetDiskIdsMismatched()
{
    return DiskIdsMismatched_.store(true);
}

////////////////////////////////////////////////////////////////////////////////

TLocationHealthChecker::TLocationHealthChecker(
    TChunkStorePtr chunkStore,
    TLocationManagerPtr locationManager,
    IInvokerPtr invoker,
    TRebootManagerPtr rebootManager,
    const TProfiler& profiler)
    : DynamicConfig_(New<TLocationHealthCheckerDynamicConfig>())
    , ChunkStore_(std::move(chunkStore))
    , LocationManager_(std::move(locationManager))
    , Invoker_(std::move(invoker))
    , RebootManager_(std::move(rebootManager))
    , HealthCheckerExecutor_(New<TPeriodicExecutor>(
        Invoker_,
        BIND(&TLocationHealthChecker::OnHealthCheck, MakeWeak(this)),
        DynamicConfig_.Acquire()->HealthCheckPeriod))
    , Profiler_(profiler)
{
    DiskOkGauge_ = Profiler_
        .WithTag("state", FormatEnum(EDiskState::Ok))
        .Gauge("/disk_state");
    DiskFailedGauge_ = Profiler_
        .WithTag("state", FormatEnum(EDiskState::Failed))
        .Gauge("/disk_state");
    DiskRecoverWaitGauge_ = Profiler_
        .WithTag("state", FormatEnum(EDiskState::RecoverWait))
        .Gauge("/disk_state");
}

void TLocationHealthChecker::Initialize()
{
    VERIFY_THREAD_AFFINITY_ANY();

    for (const auto& location : ChunkStore_->Locations()) {
        location->SubscribeDiskCheckFailed(
            BIND(&TLocationHealthChecker::OnDiskHealthCheckFailed, MakeStrong(this), location));
    }
}

void TLocationHealthChecker::Start()
{
    HealthCheckerExecutor_->Start();
}

void TLocationHealthChecker::OnDynamicConfigChanged(const TLocationHealthCheckerDynamicConfigPtr& newConfig)
{
    auto config = DynamicConfig_.Acquire();
    auto newHealthCheckPeriod = newConfig->HealthCheckPeriod;
    HealthCheckerExecutor_->SetPeriod(newHealthCheckPeriod);

    DynamicConfig_.Store(newConfig);
}

void TLocationHealthChecker::OnDiskHealthCheckFailed(
    const TStoreLocationPtr& location,
    const TError& error)
{
    auto config = DynamicConfig_.Acquire();

    if (config->Enabled && config->EnableManualDiskFailures) {
        YT_UNUSED_FUTURE(LocationManager_->FailDiskByName(location->GetStaticConfig()->DeviceName, error));
    }
}

void TLocationHealthChecker::OnHealthCheck()
{
    auto config = DynamicConfig_.Acquire();

    if (config->Enabled) {
        OnLocationsHealthCheck();
    }
}

void TLocationHealthChecker::OnDiskHealthCheck(const std::vector<TDiskInfo>& diskInfos)
{
    THashSet<TString> diskIds;
    THashSet<TString> aliveDiskIds;
    THashSet<TString> oldDiskIds = LocationManager_->GetOldDiskIds();
    THashSet<TString> configDiskIds;

    for (const auto& diskInfo : diskInfos) {
        diskIds.insert(diskInfo.DiskId);

        if (diskInfo.State == NContainers::EDiskState::Ok) {
            aliveDiskIds.insert(diskInfo.DiskId);
        }
    }

    for (const auto& diskId : LocationManager_->GetConfigDiskIds()) {
        configDiskIds.insert(diskId);
    }

    auto checkDisks = [] (const THashSet<TString>& oldDisks, const THashSet<TString>& newDisks) {
        for (const auto& newDiskId : newDisks) {
            if (!oldDisks.contains(newDiskId)) {
                return false;
            }
        }

        return true;
    };

    if (!oldDiskIds.empty() && !configDiskIds.empty()) {
        if (!checkDisks(oldDiskIds, aliveDiskIds) ||
            !checkDisks(configDiskIds, diskIds))
        {
            YT_LOG_WARNING("Set disk ids mismatched flag");
            LocationManager_->SetDiskIdsMismatched();
        }
    }

    LocationManager_->UpdateOldDiskIds(aliveDiskIds);
}

void TLocationHealthChecker::OnLocationsHealthCheck()
{
    auto diskInfosOrError = WaitFor(LocationManager_->GetDiskInfos());

    // Fast path.
    if (!diskInfosOrError.IsOK()) {
        YT_LOG_ERROR(diskInfosOrError, "Failed to list disk infos");
        return;
    }

    auto diskInfos = diskInfosOrError.Value();
    auto livenessInfos = LocationManager_->MapLocationToLivenessInfo(diskInfos);

    auto diskAlert = TError();
    std::optional<TString> diskAlertId;
    for (const auto& diskInfo : diskInfos) {
        if (diskInfo.State == NContainers::EDiskState::Failed) {
            diskAlertId = diskInfo.DiskId;
            diskAlert = TError("Disk failed, need hot swap")
                << TErrorAttribute("disk_id", diskInfo.DiskId)
                << TErrorAttribute("disk_model", diskInfo.DiskModel)
                << TErrorAttribute("disk_state", diskInfo.State)
                << TErrorAttribute("disk_path", diskInfo.DevicePath)
                << TErrorAttribute("disk_name", diskInfo.DeviceName);
            break;
        }
    }

    i64 diskWithOkState = 0;
    i64 diskWithFailedState = 0;
    i64 diskWithRecoverWaitState = 0;

    for (const auto& diskInfo : diskInfos) {
        if (diskInfo.State == NContainers::EDiskState::Ok) {
            diskWithOkState++;
        } else if (diskInfo.State == NContainers::EDiskState::RecoverWait) {
            diskWithRecoverWaitState++;
        } else if (diskInfo.State == NContainers::EDiskState::Failed) {
            diskWithFailedState++;
        }
    }

    DiskOkGauge_.Update(diskWithOkState);
    DiskFailedGauge_.Update(diskWithFailedState);
    DiskRecoverWaitGauge_.Update(diskWithRecoverWaitState);

    LocationManager_->SetDiskAlert(diskAlert);

    if (diskAlertId) {
        bool diskLinkedWithLocation = false;

        for (const auto& livenessInfo : livenessInfos) {
            if (livenessInfo.DiskId == diskAlertId.value()) {
                diskLinkedWithLocation = true;
                break;
            }
        }

        if (!diskLinkedWithLocation) {
            auto result = WaitFor(LocationManager_->RecoverDisk(diskAlertId.value()));

            YT_LOG_ERROR_IF(!result.IsOK(), result);
        }
    }

    if (DynamicConfig_.Acquire()->EnableNewDiskChecker) {
        OnDiskHealthCheck(diskInfos);
    }

    THashSet<TString> diskWithLivenessLocations;
    THashSet<TString> diskWithNotDestroyingLocations;
    THashSet<TString> diskWithDestroyingLocations;

    for (const auto& livenessInfo : livenessInfos) {
        const auto& location = livenessInfo.Location;

        if (livenessInfo.IsDiskAlive) {
            diskWithLivenessLocations.insert(livenessInfo.DiskId);
        } else {
            location->MarkLocationDiskFailed();
        }

        if (livenessInfo.LocationState == ELocationState::Destroying) {
            diskWithDestroyingLocations.insert(livenessInfo.DiskId);
        } else {
            diskWithNotDestroyingLocations.insert(livenessInfo.DiskId);
        }
    }

    for (const auto& diskId : diskWithDestroyingLocations) {
        TError error;

        if (diskWithLivenessLocations.contains(diskId)) {
            error = TError("Disk cannot be repaired, because it contains alive locations");
        } else if (diskWithNotDestroyingLocations.contains(diskId)) {
            error = TError("Disk contains not destroying locations");
        } else {
            error = WaitFor(LocationManager_->RecoverDisk(diskId));
        }

        for (const auto& livenessInfo : livenessInfos) {
            // If disk recover request is successful than mark locations as destroyed.
            if (livenessInfo.DiskId == diskId &&
                livenessInfo.LocationState == ELocationState::Destroying)
            {
                livenessInfo.Location->FinishDestroy(error.IsOK(), error);
            }
        }
    }

    for (const auto& livenessInfo : livenessInfos) {
        if (livenessInfo.IsDiskAlive &&
            livenessInfo.LocationState == ELocationState::Destroyed)
        {
            livenessInfo.Location->OnDiskRepaired();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TLocationHealthCheckerPtr CreateLocationHealthChecker(
    TChunkStorePtr chunkStore,
    TLocationManagerPtr locationManager,
    IInvokerPtr invoker,
    TRebootManagerPtr rebootManager)
{
    return New<TLocationHealthChecker>(
        chunkStore,
        locationManager,
        invoker,
        rebootManager,
        NProfiling::TProfiler("/data_node"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
