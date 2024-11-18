#pragma once

#include "bundle_scheduler.h"

namespace NYT::NCellBalancer::NOrchid {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TInstanceInfo)
DECLARE_REFCOUNTED_STRUCT(TAlert)
DECLARE_REFCOUNTED_STRUCT(TAllocatingInstanceInfo)
DECLARE_REFCOUNTED_STRUCT(TBundleInfo)
DECLARE_REFCOUNTED_STRUCT(TDataCenterRacksInfo)
DECLARE_REFCOUNTED_STRUCT(TScanBundleCounter)

////////////////////////////////////////////////////////////////////////////////

struct TInstanceInfo
    : public NYTree::TYsonStruct
{
    NBundleControllerClient::TInstanceResourcesPtr Resource;

    TString PodId;
    TString YPCluster;

    std::optional<TString> DataCenter;
    std::optional<bool> Removing;

    REGISTER_YSON_STRUCT(TInstanceInfo);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TInstanceInfo)

////////////////////////////////////////////////////////////////////////////////

struct TAlert
    : public NYTree::TYsonStruct
{
    TString Id;
    TString Description;

    REGISTER_YSON_STRUCT(TAlert);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAlert)

////////////////////////////////////////////////////////////////////////////////

struct TAllocatingInstanceInfo
    : public NYTree::TYsonStruct
{
    TString HulkRequestState;
    TString HulkRequestLink;
    TInstanceInfoPtr InstanceInfo;

    REGISTER_YSON_STRUCT(TAllocatingInstanceInfo);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAllocatingInstanceInfo)

////////////////////////////////////////////////////////////////////////////////

struct TBundleInfo
    : public NYTree::TYsonStruct
{
    NBundleControllerClient::TInstanceResourcesPtr ResourceQuota;
    NBundleControllerClient::TInstanceResourcesPtr ResourceAllocated;
    NBundleControllerClient::TInstanceResourcesPtr ResourceAlive;
    NBundleControllerClient::TInstanceResourcesPtr ResourceTarget;

    THashMap<TString, TInstanceInfoPtr> AllocatedTabletNodes;
    THashMap<TString, TInstanceInfoPtr> AllocatedRpcProxies;

    THashMap<TString, TAllocatingInstanceInfoPtr> AllocatingTabletNodes;
    THashMap<TString, TAllocatingInstanceInfoPtr> AllocatingRpcProxies;

    THashMap<TString, TInstanceInfoPtr> AssignedSpareTabletNodes;
    THashMap<TString, TInstanceInfoPtr> AssignedSpareRpcProxies;

    int RemovingCellCount;
    int AllocatingTabletNodeCount;
    int DeallocatingTabletNodeCount;
    int AllocatingRpcProxyCount;
    int DeallocatingRpcProxyCount;

    std::vector<TAlertPtr> Alerts;

    REGISTER_YSON_STRUCT(TBundleInfo);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBundleInfo)

using TBundlesInfo = THashMap<TString, TBundleInfoPtr>;

////////////////////////////////////////////////////////////////////////////////

struct TDataCenterRacksInfo
    : public NYTree::TYsonStruct
{
    THashMap<TString, int> RackToBundleNodes;
    THashMap<TString, int> RackToSpareNodes;
    int RequiredSpareNodeCount;

    REGISTER_YSON_STRUCT(TDataCenterRacksInfo);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDataCenterRacksInfo)

using TDataCenterRackInfo = THashMap<TString, TDataCenterRacksInfoPtr>;
using TZonesRacksInfo = THashMap<TString, TDataCenterRackInfo>;

////////////////////////////////////////////////////////////////////////////////

struct TScanBundleCounter
    : public NYTree::TYsonStruct
{
    int Successful;
    int Failed;

    REGISTER_YSON_STRUCT(TScanBundleCounter);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TScanBundleCounter)

////////////////////////////////////////////////////////////////////////////////

TBundlesInfo GetBundlesInfo(const TSchedulerInputState& state, const TSchedulerMutations& mutations);

TZonesRacksInfo GetZonesRacksInfo(const TSchedulerInputState& state);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer::NOrchid
