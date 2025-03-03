#pragma once

#include "public.h"

#include <yt/yt/ytlib/object_client/config.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/misc/cache_config.h>

namespace NYT::NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

struct TPermissionCacheConfig
    : public TAsyncExpiringCacheConfig
{
    NApi::TSerializableMasterReadOptionsPtr MasterReadOptions;
    std::string RefreshUser;
    bool AlwaysUseRefreshUser;

    REGISTER_YSON_STRUCT(TPermissionCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPermissionCacheConfig)

////////////////////////////////////////////////////////////////////////////////

//! Keep in mind that when you are using this cache, the delay with which attribute
//! changes are visible depend directly on the value of ExpireAfterSuccessfulUpdateTime,
//! the provided master read options and, if relevant, node/master cache configuration
//! themselves. The defaults are chosen to make this cache no more expensive than the
//! permission cache in its default configuration.
class TUserAttributeCacheConfig
    : public NObjectClient::TObjectAttributeCacheConfig
{
public:
    REGISTER_YSON_STRUCT(TUserAttributeCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUserAttributeCacheConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
