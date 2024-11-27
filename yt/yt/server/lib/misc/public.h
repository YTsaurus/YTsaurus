#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TJobReport;

DECLARE_REFCOUNTED_CLASS(TServerConfig)

DECLARE_REFCOUNTED_CLASS(TJobReporterConfig)
DECLARE_REFCOUNTED_CLASS(TJobReporter)

DECLARE_REFCOUNTED_CLASS(TDiskHealthChecker)
DECLARE_REFCOUNTED_CLASS(TDiskHealthCheckerConfig)

DECLARE_REFCOUNTED_CLASS(TDiskLocationConfig)
DECLARE_REFCOUNTED_CLASS(TDiskLocationDynamicConfig)

DECLARE_REFCOUNTED_CLASS(TFormatConfigBase)
DECLARE_REFCOUNTED_CLASS(TFormatConfig)
DECLARE_REFCOUNTED_CLASS(TFormatManager)

DECLARE_REFCOUNTED_CLASS(THeapProfilerTestingOptions)
DECLARE_REFCOUNTED_CLASS(THeapUsageProfiler)

class TServiceProfilerGuard;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TForkCounters)

////////////////////////////////////////////////////////////////////////////////

extern const TString ExecProgramName;
extern const TString JobProxyProgramName;

////////////////////////////////////////////////////////////////////////////////

extern const TString BanMessageAttributeName;
extern const TString ConfigAttributeName;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TArchiveReporterConfig)
DECLARE_REFCOUNTED_CLASS(TArchiveHandlerConfig)
DECLARE_REFCOUNTED_CLASS(TArchiveVersionHolder)
DECLARE_REFCOUNTED_CLASS(TRestartManager)

DECLARE_REFCOUNTED_STRUCT(IArchiveReporter)

struct IArchiveRowlet;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TClusterThrottlersConfig)
DECLARE_REFCOUNTED_CLASS(TClusterLimitsConfig)
DECLARE_REFCOUNTED_CLASS(TLimitConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
