#pragma once

#include <yt/yt/client/security_client/public.h>

namespace NYT::NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

//! All accounts are uniformly divided into |AccountShardCount| shards.
// BEWARE: Changing this value requires reign promotion since rolling update
// is not possible.
constexpr int AccountShardCount = 60;
static_assert(AccountShardCount < std::numeric_limits<i8>::max(), "AccountShardCount must fit into i8");

struct TSerializableAccessControlEntry;
struct TSerializableAccessControlList;
struct TPermissionKey;

DECLARE_REFCOUNTED_CLASS(TPermissionCache)
DECLARE_REFCOUNTED_STRUCT(TPermissionCacheConfig)

DECLARE_REFCOUNTED_CLASS(TUserAttributes)
DECLARE_REFCOUNTED_CLASS(TUserAttributeCache)
DECLARE_REFCOUNTED_CLASS(TUserAttributeCacheConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient

