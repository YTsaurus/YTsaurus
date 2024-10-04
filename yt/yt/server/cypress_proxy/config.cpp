#include "config.h"

#include <yt/yt/ytlib/api/native/config.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

void TCypressProxyConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("abort_on_unrecognized_options", &TThis::AbortOnUnrecognizedOptions)
        .Default(false);

    registrar.Parameter("cypress_registrar", &TThis::CypressRegistrar)
        .DefaultNew();

    registrar.Parameter("root_path", &TThis::RootPath)
        .Default("//sys/cypress_proxies");

    registrar.Parameter("dynamic_config_manager", &TThis::DynamicConfigManager)
        .DefaultNew();
    registrar.Parameter("dynamic_config_path", &TThis::DynamicConfigPath)
        .Default();

    registrar.Parameter("user_directory_synchronizer", &TThis::UserDirectorySynchronizer)
        .DefaultNew();

    registrar.Postprocessor([] (TThis* config) {
        if (!config->DynamicConfigPath) {
            config->DynamicConfigPath = config->RootPath + "/@config";
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TObjectServiceDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("thread_pool_size", &TThis::ThreadPoolSize)
        .Default(1);
    registrar.Parameter("allow_bypass_master_resolve", &TThis::AllowBypassMasterResolve)
        .Default(false);
    registrar.Parameter("alert_on_mixed_read_write_batch", &TThis::AlertOnMixedReadWriteBatch)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TUserDirectorySynchronizerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("sync_period", &TThis::SyncPeriod)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("sync_splay", &TThis::SyncSplay)
        .Default(TDuration::Seconds(30));
}

////////////////////////////////////////////////////////////////////////////////

void TCypressProxyDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("object_service", &TThis::ObjectService)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
