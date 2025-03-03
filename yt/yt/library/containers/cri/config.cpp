#include "config.h"
#include "cri_api.h"

#include <yt/yt/library/re2/re2.h>

namespace NYT::NContainers::NCri {

////////////////////////////////////////////////////////////////////////////////

void TCriExecutorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("runtime_endpoint", &TThis::RuntimeEndpoint)
        .Default(TString(DefaultCriEndpoint));

    registrar.Parameter("image_endpoint", &TThis::ImageEndpoint)
        .Default(TString(DefaultCriEndpoint));

    registrar.Parameter("namespace", &TThis::Namespace)
        .NonEmpty();

    registrar.Parameter("runtime_handler", &TThis::RuntimeHandler)
        .Optional();

    registrar.Parameter("base_cgroup", &TThis::BaseCgroup)
        .NonEmpty();

    registrar.Parameter("cpu_period", &TThis::CpuPeriod)
        .Default(TDuration::MilliSeconds(100));

    registrar.Parameter("memory_oom_group", &TThis::MemoryOomGroup)
        .Default(false);

    registrar.Parameter("retry_error_prefixes", &TThis::RetryErrorPrefixes)
        .Default({
            // https://github.com/containerd/containerd/pull/9565
            "server is not initialized yet",
            // https://github.com/containerd/containerd/issues/9160
            "failed to create containerd task: failed to create shim task: OCI runtime create failed: runc create failed: unable to create new parent process: namespace path: lstat /proc/0/ns/ipc: no such file or directory: unknown",
        });

    registrar.Parameter("retry_error_pattern", &TThis::RetryErrorPattern)
        .DefaultNew("^failed to set removing state for container .*: container is in starting state, can't be removed");
}

////////////////////////////////////////////////////////////////////////////////

void TCriAuthConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("username", &TThis::Username)
        .Optional();

    registrar.Parameter("password", &TThis::Password)
        .Optional();

    registrar.Parameter("auth", &TThis::Auth)
        .Optional();

    registrar.Parameter("server_address", &TThis::ServerAddress)
        .Optional();

    registrar.Parameter("identity_token", &TThis::IdentityToken)
        .Optional();

    registrar.Parameter("registry_token", &TThis::RegistryToken)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

void TCriImageCacheConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("managed_prefixes", &TThis::ManagedPrefixes)
        .Optional();

    registrar.Parameter("unmanaged_prefixes", &TThis::UnmanagedPrefixes)
        .Optional();

    registrar.Parameter("pinned_images", &TThis::PinnedImages)
        .Optional();

    registrar.Parameter("image_size_estimation", &TThis::ImageSizeEstimation)
        .Default(512_MB);

    registrar.Parameter("image_compression_ratio_estimation", &TThis::ImageCompressionRatioEstimation)
        .Default(4.0);

    registrar.Parameter("always_pull_latest", &TThis::AlwaysPullLatest)
        .Default(true);

    registrar.Parameter("pull_period", &TThis::PullPeriod)
        .Default();

    registrar.Preprocessor([] (TThis* config) {
        config->Capacity = Max<i64>();
        config->ShardCount = 1;
        config->TouchBufferCapacity = 32;
        config->YoungerSizeFraction = 0.5;
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers::NCri
