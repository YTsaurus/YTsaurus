#include "bootstrap.h"
#include "program.h"

#include <yt/yt/ytlib/program/native_singletons.h>

#include <yt/yt/library/program/helpers.h>

#include <yt/yt/core/misc/ref_counted_tracker_profiler.h>

#include <library/cpp/yt/phdr_cache/phdr_cache.h>

#include <library/cpp/yt/mlock/mlock.h>

#include <util/system/thread.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

TTabletBalancerProgram::TTabletBalancerProgram()
    : TProgramPdeathsigMixin(Opts_)
    , TProgramSetsidMixin(Opts_)
    , TProgramConfigMixin(Opts_)
{ }

void TTabletBalancerProgram::DoRun(const NLastGetopt::TOptsParseResult& /*parseResult*/)
{
    TThread::SetCurrentThreadName("TabletBalancerMain");

    ConfigureUids();
    ConfigureIgnoreSigpipe();
    ConfigureCrashHandler();
    ConfigureExitZeroOnSigterm();
    EnablePhdrCache();
    ConfigureAllocator();
    MlockFileMappings();

    if (HandleSetsidOptions()) {
        return;
    }
    if (HandlePdeathsigOptions()) {
        return;
    }
    if (HandleConfigOptions()) {
        return;
    }

    auto config = GetConfig();

    ConfigureNativeSingletons(config);

    auto configNode = GetConfigNode();

    auto* bootstrap = CreateBootstrap(std::move(config), std::move(configNode)).release();
    bootstrap->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
