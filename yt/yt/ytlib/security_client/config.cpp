#include "config.h"

#include <yt/yt/client/api/client.h>

namespace NYT::NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

void TPermissionCacheConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("master_read_options", &TThis::MasterReadOptions)
        .DefaultNew();

    registrar.Parameter("refresh_user", &TThis::RefreshUser)
        // COMPAT(babenko): separate user
        .Default(RootUserName);
    registrar.Parameter("always_use_refresh_user", &TThis::AlwaysUseRefreshUser)
        // COMPAT(babenko): turn this off and remove the feature flag
        .Default(true);

    registrar.Preprocessor([] (TThis* config) {
        config->MasterReadOptions->ReadFrom = NApi::EMasterChannelKind::Cache;
        config->MasterReadOptions->CacheStickyGroupSize = 1;

        config->ExpireAfterAccessTime = TDuration::Minutes(5);
        config->ExpireAfterSuccessfulUpdateTime = TDuration::Minutes(3);
        config->RefreshTime = TDuration::Minutes(1);
        config->BatchUpdate = true;
    });

    registrar.Postprocessor([] (TThis* config) {
        config->MasterReadOptions->ExpireAfterSuccessfulUpdateTime = config->ExpireAfterSuccessfulUpdateTime;
        config->MasterReadOptions->ExpireAfterFailedUpdateTime = config->ExpireAfterFailedUpdateTime;
    });
}

////////////////////////////////////////////////////////////////////////////////

void TUserAttributeCacheConfig::Register(TRegistrar registrar)
{
    registrar.Preprocessor([] (TThis* config) {
        config->MasterReadOptions->ReadFrom = NApi::EMasterChannelKind::Cache;
        config->MasterReadOptions->CacheStickyGroupSize = 1;

        // This way the load induced by this cache is not larger than
        // the one produced by the permission cache by default.
        config->ExpireAfterAccessTime = TDuration::Minutes(5);
        config->ExpireAfterSuccessfulUpdateTime = TDuration::Minutes(3);
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
