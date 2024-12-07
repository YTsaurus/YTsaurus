#include "bootstrap.h"
#include "config.h"

#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/helpers.h>
#include <yt/yt/library/program/program_config_mixin.h>
#include <yt/yt/library/program/program_pdeathsig_mixin.h>
#include <yt/yt/library/program/program_setsid_mixin.h>

#include <yt/yt/ytlib/program/native_singletons.h>

#include <yt/yt/core/misc/ref_counted_tracker_profiler.h>

#include <library/cpp/yt/phdr_cache/phdr_cache.h>

#include <library/cpp/yt/mlock/mlock.h>

#include <util/system/compiler.h>
#include <util/system/thread.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

class TQueueAgentProgram
    : public virtual TProgram
    , public TProgramPdeathsigMixin
    , public TProgramSetsidMixin
    , public TProgramConfigMixin<TQueueAgentServerConfig>
{
public:
    TQueueAgentProgram()
        : TProgramPdeathsigMixin(Opts_)
        , TProgramSetsidMixin(Opts_)
        , TProgramConfigMixin(Opts_)
    { }

protected:
    void DoRun(const NLastGetopt::TOptsParseResult& /*parseResult*/) override
    {
        TThread::SetCurrentThreadName("QueueAgentMain");

        ConfigureUids();
        ConfigureIgnoreSigpipe();
        ConfigureCrashHandler();
        ConfigureExitZeroOnSigterm();
        EnablePhdrCache();
        ConfigureAllocator();
        MlockFileMappings();
        RunMixinCallbacks();

        auto config = GetConfig();
        auto configNode = GetConfigNode();

        ConfigureNativeSingletons(config);

        // TODO(babenko): This memory leak is intentional.
        // We should avoid destroying bootstrap since some of the subsystems
        // may be holding a reference to it and continue running some actions in background threads.
        auto* bootstrap = new TBootstrap(std::move(config), std::move(configNode));
        DoNotOptimizeAway(bootstrap);
        bootstrap->Run();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
