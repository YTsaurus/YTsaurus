#pragma once

#include "public.h"
#include "config.h"

#include <yt/yt/ytlib/object_client/object_attribute_cache.h>

namespace NYT::NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

//! Stores a representation of user object attributes that are resolved by the cache defined below.
//! Extend with care.
class TUserAttributes
    : public NYTree::TYsonStruct
{
public:
    bool Banned;
    THashSet<std::string> MemberOfClosure;

    REGISTER_YSON_STRUCT(TUserAttributes);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUserAttributes)

////////////////////////////////////////////////////////////////////////////////

//! Caches meta-information about user objects, see TUserAttributes above for contents.
class TUserAttributeCache
    : public NObjectClient::TObjectAttributeAsYsonStructCacheBase<std::string, TUserAttributesPtr>
{
public:
    using TBase = NObjectClient::TObjectAttributeAsYsonStructCacheBase<std::string, TUserAttributesPtr>;
    using TBase::TBase;

private:
    NYPath::TYPath GetPath(const std::string& key) const override;
};

DEFINE_REFCOUNTED_TYPE(TUserAttributeCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
