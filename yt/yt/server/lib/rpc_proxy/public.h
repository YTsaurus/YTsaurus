#pragma once

#include <yt/yt/client/api/rpc_proxy/public.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TApiServiceConfig)
DECLARE_REFCOUNTED_CLASS(TApiServiceDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TSecurityManagerDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TStructuredLoggingTopicDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TStructuredLoggingMethodDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TQueryCorpusReporterConfig)
DECLARE_REFCOUNTED_CLASS(TProxyHeapUsageProfiler)
DECLARE_REFCOUNTED_CLASS(TApiTestingOptions)

DECLARE_REFCOUNTED_STRUCT(IAccessChecker)
DECLARE_REFCOUNTED_STRUCT(IProxyCoordinator)
DECLARE_REFCOUNTED_STRUCT(ISecurityManager)
DECLARE_REFCOUNTED_STRUCT(IApiService)
DECLARE_REFCOUNTED_STRUCT(IQueryCorpusReporter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
