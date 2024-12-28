#include "config.h"

#include <yt/yt/server/lib/alert_manager/config.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/client/security_client/public.h>

namespace NYT::NQueryTracker {

using namespace NSecurityClient;

////////////////////////////////////////////////////////////////////////////////

void TEngineConfigBase::Register(TRegistrar registrar)
{
    registrar.Parameter("query_state_write_backoff", &TThis::QueryStateWriteBackoff)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("query_progress_write_backoff", &TThis::QueryProgressWritePeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("row_count_limit", &TThis::RowCountLimit)
        .Default(10'000);
    registrar.Parameter("resulting_rowset_value_length_limit", &TThis::ResultingRowsetValueLengthLimit)
        .Default(1_GB);
}

////////////////////////////////////////////////////////////////////////////////

void TYqlEngineConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("stage", &TThis::Stage)
        .Default("production");
    registrar.Parameter("update_progress_period", &TThis::QueryProgressGetPeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("start_query_attempt_period", &TThis::StartQueryAttemptPeriod)
        .Default(TDuration::Seconds(2));
}

////////////////////////////////////////////////////////////////////////////////

void TChytEngineConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("default_clique", &TThis::DefaultClique)
        .Default("ch_public");
    registrar.Parameter("default_cluster", &TThis::DefaultCluster)
        .Default();
    registrar.Parameter("progress_poll_period", &TThis::ProgressPollPeriod)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TQLEngineConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("default_cluster", &TThis::DefaultCluster)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TSpytEngineConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("default_cluster", &TThis::DefaultCluster)
        .Default();
    registrar.Parameter("default_discovery_path", &TThis::DefaultDiscoveryPath)
        .Default();
    registrar.Parameter("default_discovery_group", &TThis::DefaultDiscoveryGroup)
        .Default("spyt_public");
    registrar.Parameter("spyt_home", &TThis::SpytHome)
        .Default("//home/spark");
    registrar.Parameter("http_client", &TThis::HttpClient)
        .DefaultNew();
    registrar.Parameter("status_poll_period", &TThis::StatusPollPeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("token_expiration_timeout", &TThis::TokenExpirationTimeout)
        .Default(TDuration::Minutes(20));
    registrar.Parameter("refresh_token_period", &TThis::RefreshTokenPeriod)
        .Default(TDuration::Minutes(10));
}

////////////////////////////////////////////////////////////////////////////////

void TQueryTrackerProxyConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_query_file_count", &TThis::MaxQueryFileCount)
        .Default(8192);
    registrar.Parameter("max_query_file_name_size_bytes", &TThis::MaxQueryFileNameSizeBytes)
        .Default(1_KB);
    registrar.Parameter("max_query_file_content_size_bytes", &TThis::MaxQueryFileContentSizeBytes)
        .Default(2_KB);
}

////////////////////////////////////////////////////////////////////////////////

void TQueryTrackerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("state_check_period", &TThis::StateCheckPeriod)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("active_query_acquisition_period", &TThis::ActiveQueryAcquisitionPeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("active_query_ping_period", &TThis::ActiveQueryPingPeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("query_finish_backoff", &TThis::QueryFinishBackoff)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("health_check_period", &TThis::HealthCheckPeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("ql_engine", &TThis::QLEngine)
        .DefaultNew();
    registrar.Parameter("yql_engine", &TThis::YqlEngine)
        .DefaultNew();
    registrar.Parameter("chyt_engine", &TThis::ChytEngine)
        .DefaultNew();
    registrar.Parameter("spyt_engine", &TThis::SpytEngine)
        .DefaultNew();
    registrar.Parameter("mock_engine", &TThis::MockEngine)
        .DefaultNew();
    registrar.Parameter("proxy_config", &TThis::ProxyConfig)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TQueryTrackerServerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("min_required_state_version", &TThis::MinRequiredStateVersion)
        .Default(12);
    registrar.Parameter("abort_on_unrecognized_options", &TThis::AbortOnUnrecognizedOptions)
        .Default(false);
    registrar.Parameter("proxy_thread_pool_size", &TThis::ProxyThreadPoolSize)
        .Default(4);
    registrar.Parameter("user", &TThis::User);
    registrar.Parameter("cypress_annotations", &TThis::CypressAnnotations)
        .Default(NYTree::BuildYsonNodeFluently()
            .BeginMap()
            .EndMap()
        ->AsMap());
    registrar.Parameter("election_manager", &TThis::ElectionManager)
        .DefaultNew();
    registrar.Parameter("dynamic_config_manager", &TThis::DynamicConfigManager)
        .DefaultNew();
    registrar.Parameter("dynamic_config_path", &TThis::DynamicConfigPath)
        .Default();
    registrar.Parameter("root", &TThis::Root)
        .Default("//sys/query_tracker");

    registrar.Postprocessor([] (TThis* config) {
        if (auto& lockPath = config->ElectionManager->LockPath; lockPath.empty()) {
            lockPath = config->Root + "/leader_lock";
        }
        if (auto& dynamicConfigPath = config->DynamicConfigPath; dynamicConfigPath.empty()) {
            dynamicConfigPath = config->Root + "/config";
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TQueryTrackerServerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("alert_manager", &TThis::AlertManager)
        .DefaultNew();
    registrar.Parameter("query_tracker", &TThis::QueryTracker)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
