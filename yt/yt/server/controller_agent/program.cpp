#include "program.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/server/lib/misc/cluster_connection.h>

#include <yt/yt/ytlib/program/native_singletons.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/library/server_program/server_program.h>

#include <yt/yt/library/program/program_config_mixin.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

class TControllerAgentProgram
    : public TServerProgram
    , public TProgramConfigMixin<TControllerAgentBootstrapConfig>
{
public:
    TControllerAgentProgram()
        : TProgramConfigMixin(Opts_)
    {
        Opts_
            .AddLongOption(
                "remote-cluster-proxy",
                "if set, controller agent would download cluster connection from //sys/@cluster_connection "
                "on cluster CLUSTER using http interface and then run as a local controller agent for CLUSTER."
                "WARNING: Do not use this option unless you are sure that remote cluster has schedulers that "
                "are aware of controller agent tags!")
            .StoreResult(&RemoteClusterProxy_)
            .RequiredArgument("CLUSTER")
            .Optional();
        Opts_
            .AddLongOption(
                "tag",
                "if set, sets controller agent tag for local run mode and does nothing in normal mode.")
            .StoreResult(&Tag_)
            .RequiredArgument("TAG")
            .Optional();

        SetMainThreadName("CtrlAgent");
    }

protected:
    void DoStart() final
    {
        NControllerAgent::TControllerAgentBootstrapConfigPtr config;
        NYTree::INodePtr configNode;
        if (RemoteClusterProxy_) {
            // Form a default controller agent config.
            auto defaultConfig = New<NControllerAgent::TControllerAgentBootstrapConfig>();
            defaultConfig->Logging = NLogging::TLogManagerConfig::CreateYTServer(
                /*componentName*/ "controller_agent",
                /*directory*/ ".",
                /*structuredCategoryToWriterName*/ {
                    {"ChunkPool", "chunk_pool"},
                });
            // Set controller agent tag.
            if (!Tag_) {
                THROW_ERROR_EXCEPTION("Controller agent tag should be presented in local mode");
            }
            defaultConfig->ControllerAgent->Tags = std::vector<TString>({Tag_});
            defaultConfig->RpcPort = 9014;
            defaultConfig->ClusterConnection = New<NApi::NNative::TConnectionCompoundConfig>();
            defaultConfig->ClusterConnection->Static = New<NApi::NNative::TConnectionStaticConfig>();
            defaultConfig->ClusterConnection->Dynamic = New<NApi::NNative::TConnectionDynamicConfig>();
            // Building and loading snapshots at local controller agent seems pretty dangerous and useless, so let's disable it by default.
            defaultConfig->ControllerAgent->EnableSnapshotBuilding = false;
            defaultConfig->ControllerAgent->EnableSnapshotBuildingDisabledAlert = false;
            defaultConfig->ControllerAgent->EnableSnapshotLoading = false;
            defaultConfig->ControllerAgent->EnableSnapshotLoadingDisabledAlert = false;
            // Scheduler will not work with controller agent without memory limit.
            defaultConfig->ControllerAgent->MemoryWatchdog->TotalControllerMemoryLimit = 100_GB;
            // Dump it into node and apply patch from config file (if present).
            configNode = NYTree::ConvertToNode(defaultConfig);
            if (auto configNodePatch = GetConfigNode(true /*returnNullIfNotSupplied*/)) {
                configNode = NYTree::PatchNode(configNode, configNodePatch);
            }
            // Finally load it back.
            config = New<NControllerAgent::TControllerAgentBootstrapConfig>();
            config->SetUnrecognizedStrategy(NYTree::EUnrecognizedStrategy::KeepRecursive);
            config->Load(configNode);
        } else {
            config = GetConfig();
            configNode = GetConfigNode();
        }

        ConfigureAllocator({.SnapshotUpdatePeriod = config->HeapProfiler->SnapshotUpdatePeriod});

        ConfigureNativeSingletons(config);

        if (RemoteClusterProxy_) {
            // Set controller agent cluster connection.
            auto clusterConnectionNode = DownloadClusterConnection(RemoteClusterProxy_, NControllerAgent::ControllerAgentLogger());
            config->ClusterConnection = ConvertTo<NApi::NNative::TConnectionCompoundConfigPtr>(clusterConnectionNode);
        }

        // TODO(babenko): This memory leak is intentional.
        // We should avoid destroying bootstrap since some of the subsystems
        // may be holding a reference to it and continue running some actions in background threads.
        auto* bootstrap = new TBootstrap(std::move(config), std::move(configNode));
        DoNotOptimizeAway(bootstrap);
        bootstrap->Run();
    }

private:
    TString RemoteClusterProxy_;
    TString Tag_;
};

////////////////////////////////////////////////////////////////////////////////

void RunControllerAgentProgram(int argc, const char** argv)
{
    TControllerAgentProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
