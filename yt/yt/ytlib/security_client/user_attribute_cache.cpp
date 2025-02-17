#include "user_attribute_cache.h"

#include <yt/yt/client/security_client/helpers.h>

namespace NYT::NSecurityClient {

using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

void TUserAttributes::Register(TRegistrar registrar)
{
    registrar.Parameter("banned", &TThis::Banned);
    registrar.Parameter("member_of_closure", &TThis::MemberOfClosure);
}

////////////////////////////////////////////////////////////////////////////////

TYPath TUserAttributeCache::GetPath(const std::string& key) const
{
    return GetUserPath(key);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
