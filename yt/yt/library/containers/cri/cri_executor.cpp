#include "cri_executor.h"
#include "private.h"

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/rpc/grpc/channel.h>

#include <yt/yt/core/rpc/retrying_channel.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/library/re2/re2.h>

namespace NYT::NContainers::NCri {

using namespace NRpc;
using namespace NRpc::NGrpc;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TCriDescriptor& descriptor, TStringBuf /*spec*/)
{
    builder->AppendFormat("%v (%s)", descriptor.Id.substr(0, 12), descriptor.Name);
}

void FormatValue(TStringBuilderBase* builder, const TCriPodDescriptor& descriptor, TStringBuf /*spec*/)
{
    builder->AppendFormat("%v (%s)", descriptor.Id.substr(0, 12), descriptor.Name);
}

void FormatValue(TStringBuilderBase* builder, const TCriImageDescriptor& descriptor, TStringBuf /*spec*/)
{
    builder->AppendString(descriptor.Image);
}

static TError DecodeExitCode(int exitCode, const TString& reason)
{
    if (exitCode == 0) {
        return TError();
    }

    // Common bash notation for signals: 128 + signal
    if (exitCode > 128) {
        int signalNumber = exitCode - 128;
        return TError(
            EProcessErrorCode::Signal,
            "Process terminated by signal %v",
            signalNumber)
            << TErrorAttribute("signal", signalNumber)
            << TErrorAttribute("reason", reason)
            << TErrorAttribute("oom_killed", reason == "OOMKilled");
    }

    // TODO(khkebnikov) check these
    // 125 - container failed to run
    // 126 - non executable
    // 127 - command not found
    // 128 - invalid exit code
    // 255 - exit code out of range

    return TError(
        EProcessErrorCode::NonZeroExitCode,
        "Process exited with code %v",
        exitCode)
        << TErrorAttribute("exit_code", exitCode)
        << TErrorAttribute("reason", reason);
}

////////////////////////////////////////////////////////////////////////////////

class TCriProcess
    : public TProcessBase
{
public:
    TCriProcess(
        const TString& path,
        ICriExecutorPtr executor,
        TCriContainerSpecPtr containerSpec,
        const TCriPodDescriptor& podDescriptor,
        TCriPodSpecPtr podSpec,
        TDuration pollPeriod = TDuration::MilliSeconds(100))
        : TProcessBase(path)
        , Executor_(std::move(executor))
        , ContainerSpec_(std::move(containerSpec))
        , PodDescriptor_(podDescriptor)
        , PodSpec_(std::move(podSpec))
        , PollPeriod_(pollPeriod)
    {
        // Just for symmetry with sibling classes.
        AddArgument(Path_);
    }

    void Kill(int /*signal*/) override
    {
        if (Finished_) {
            return;
        }
        Finished_ = true;

        if (!Started_) {
            THROW_ERROR_EXCEPTION("Process is not started yet");
        }

        YT_LOG_DEBUG("Killing process");
        WaitFor(Executor_->StopContainer(ContainerDescriptor_))
            .ThrowOnError();
    }

    NNet::IConnectionWriterPtr GetStdInWriter() override
    {
        THROW_ERROR_EXCEPTION("Not implemented for CRI process");
    }

    NNet::IConnectionReaderPtr GetStdOutReader() override
    {
        THROW_ERROR_EXCEPTION("Not implemented for CRI process");
    }

    NNet::IConnectionReaderPtr GetStdErrReader() override
    {
        THROW_ERROR_EXCEPTION("Not implemented for CRI process");
    }

private:
    const ICriExecutorPtr Executor_;
    const TCriContainerSpecPtr ContainerSpec_;
    const TCriPodDescriptor PodDescriptor_;
    const TCriPodSpecPtr PodSpec_;
    const TDuration PollPeriod_;

    NLogging::TLogger Logger = NCri::Logger();

    TCriDescriptor ContainerDescriptor_;

    TPeriodicExecutorPtr AsyncWaitExecutor_;

    void DoSpawn() override
    {
        if (ContainerSpec_->Command.empty()) {
            ContainerSpec_->Command = {Path_};
        }
        ContainerSpec_->Arguments = std::vector<TString>(Args_.begin() + 1, Args_.end());
        ContainerSpec_->WorkingDirectory = WorkingDirectory_;

        for (const auto& keyVal : Env_) {
            TStringBuf key, val;
            if (TStringBuf(keyVal).TrySplit('=', key, val)) {
                ContainerSpec_->Environment[key] = val;
            }
        }

        Logger.AddTag("Pod: %v", PodDescriptor_);

        YT_LOG_DEBUG("Creating container (Container: %v)",
            ContainerSpec_->Name);

        ContainerDescriptor_ = WaitFor(Executor_->CreateContainer(ContainerSpec_, PodDescriptor_, PodSpec_))
            .ValueOrThrow();

        Logger.AddTag("Container: %v", ContainerDescriptor_);

        YT_LOG_DEBUG("Spawning process (Command: %v, Environment: %v)",
            ContainerSpec_->Command[0],
            ContainerSpec_->Environment);

        YT_VERIFY(!Started_);
        Started_ = true;

        if (Finished_) {
            THROW_ERROR_EXCEPTION("Process is already killed");
        }

        WaitFor(Executor_->StartContainer(ContainerDescriptor_))
            .ThrowOnError();

        // TODO(khkebnikov) replace polling with CRI event
        AsyncWaitExecutor_ = New<TPeriodicExecutor>(
            GetSyncInvoker(),
            BIND(&TCriProcess::PollContainerStatus, MakeStrong(this)),
            PollPeriod_);

        AsyncWaitExecutor_->Start();
    }

    void PollContainerStatus()
    {
        Executor_->GetContainerStatus(ContainerDescriptor_)
            .SubscribeUnique(BIND(&TCriProcess::OnContainerStatus, MakeStrong(this)));
    }

    void OnContainerStatus(TErrorOr<TCriRuntimeApi::TRspContainerStatusPtr>&& responseOrError)
    {
        auto response = responseOrError.ValueOrThrow();
        if (!response->has_status()) {
            return;
        }
        auto status = response->status();
        if (status.state() == NProto::CONTAINER_EXITED) {
            auto error = DecodeExitCode(status.exit_code(), status.reason());
            YT_LOG_DEBUG(error, "Process finished");
            YT_UNUSED_FUTURE(AsyncWaitExecutor_->Stop());
            FinishedPromise_.TrySet(error);
        }
    }

    void CleanUpParent() override
    { }
};

DEFINE_REFCOUNTED_TYPE(TCriProcess)

////////////////////////////////////////////////////////////////////////////////

class TCriExecutor
    : public ICriExecutor
{
public:
    TCriExecutor(
        TCriExecutorConfigPtr config,
        IChannelFactoryPtr channelFactory)
        : Config_(std::move(config))
        , RuntimeApi_(CreateRetryingChannel(Config_, channelFactory->CreateChannel(Config_->RuntimeEndpoint), GetRetryChecker()))
        , ImageApi_(CreateRetryingChannel(Config_, channelFactory->CreateChannel(Config_->ImageEndpoint), GetRetryChecker()))
        , Attempt_(RandomNumber<ui32>())
    { }

    TString GetPodCgroup(TString podName) const override
    {
        TStringBuilder cgroup;
        cgroup.AppendString(Config_->BaseCgroup);
        cgroup.AppendString("/");

        if (Config_->BaseCgroup.EndsWith(SystemdSliceSuffix)) {
            auto sliceName = podName;

            // Build name of nested slice in systemd notaion, see manpage systemd.slice.
            SubstGlobal(sliceName, "-", "_");
            auto parentSlice = NFS::GetFileNameWithoutExtension(Config_->BaseCgroup);
            if (parentSlice != "-") {
                sliceName = Format("%v-%v", parentSlice, sliceName);
            }

            cgroup.AppendString(sliceName);
            cgroup.AppendString(SystemdSliceSuffix);
        } else {
            cgroup.AppendString(podName);
        }

        return cgroup.Flush();
    }

    TFuture<TCriRuntimeApi::TRspStatusPtr> GetRuntimeStatus(bool verbose = false) override
    {
        auto req = RuntimeApi_.Status();
        req->set_verbose(verbose);
        return req->Invoke();
    }

    TFuture<TCriRuntimeApi::TRspListPodSandboxPtr> ListPodSandbox(
        std::function<void(NProto::PodSandboxFilter&)> initFilter = nullptr) override
    {
        auto req = RuntimeApi_.ListPodSandbox();

        {
            auto* filter = req->mutable_filter();

            if (auto namespace_ = Config_->Namespace) {
                auto& labels = *filter->mutable_label_selector();
                labels[YTPodNamespaceLabel] = namespace_;
            }

            if (initFilter) {
                initFilter(*filter);
            }
        }

        return req->Invoke();
    }

    TFuture<TCriRuntimeApi::TRspListContainersPtr> ListContainers(
        std::function<void(NProto::ContainerFilter&)> initFilter = nullptr) override
    {
        auto req = RuntimeApi_.ListContainers();

        {
            auto* filter = req->mutable_filter();

            if (auto namespace_ = Config_->Namespace) {
                auto& labels = *filter->mutable_label_selector();
                labels[YTPodNamespaceLabel] = namespace_;
            }

            if (initFilter) {
                initFilter(*filter);
            }
        }

        return req->Invoke();
    }

    TFuture<void> ForEachPodSandbox(
        const TCallback<void(const TCriPodDescriptor&, const NProto::PodSandbox&)>& callback,
        std::function<void(NProto::PodSandboxFilter&)> initFilter) override
    {
        return ListPodSandbox(initFilter).Apply(BIND([=] (const TCriRuntimeApi::TRspListPodSandboxPtr& rsp) {
            for (const auto& pod : rsp->items()) {
                TCriPodDescriptor descriptor{.Name=pod.metadata().name(), .Id=pod.id()};
                callback(descriptor, pod);
            }
        }));
    }

    TFuture<void> ForEachContainer(
        const TCallback<void(const TCriDescriptor&, const NProto::Container&)>& callback,
        std::function<void(NProto::ContainerFilter&)> initFilter = nullptr) override
    {
        return ListContainers(initFilter).Apply(BIND([=] (const TCriRuntimeApi::TRspListContainersPtr& rsp) {
            for (const auto& container : rsp->containers()) {
                TCriDescriptor descriptor{.Name = container.metadata().name(), .Id = container.id()};
                callback(descriptor, container);
            }
        }));
    }

    TFuture<TCriRuntimeApi::TRspPodSandboxStatusPtr> GetPodSandboxStatus(
        const TCriPodDescriptor& podDescriptor, bool verbose = false) override
    {
        auto req = RuntimeApi_.PodSandboxStatus();
        req->set_pod_sandbox_id(podDescriptor.Id);
        req->set_verbose(verbose);
        return req->Invoke();
    }

    TFuture<TCriRuntimeApi::TRspContainerStatusPtr> GetContainerStatus(
        const TCriDescriptor& descriptor, bool verbose = false) override
    {
        auto req = RuntimeApi_.ContainerStatus();
        req->set_container_id(descriptor.Id);
        req->set_verbose(verbose);
        return req->Invoke();
    }

    TFuture<TCriPodDescriptor> RunPodSandbox(TCriPodSpecPtr podSpec) override
    {
        auto req = RuntimeApi_.RunPodSandbox();

        FillPodSandboxConfig(req->mutable_config(), *podSpec);

        if (Config_->RuntimeHandler) {
            req->set_runtime_handler(Config_->RuntimeHandler);
        }

        return req->Invoke().Apply(BIND([name = podSpec->Name] (const TCriRuntimeApi::TRspRunPodSandboxPtr& rsp) -> TCriPodDescriptor {
            return TCriPodDescriptor{.Name = name, .Id = rsp->pod_sandbox_id()};
        }));
    }

    TFuture<void> StopPodSandbox(const TCriPodDescriptor& podDescriptor) override
    {
        auto req = RuntimeApi_.StopPodSandbox();
        req->set_pod_sandbox_id(podDescriptor.Id);
        return req->Invoke().AsVoid();
    }

    TFuture<void> RemovePodSandbox(const TCriPodDescriptor& podDescriptor) override
    {
        auto req = RuntimeApi_.RemovePodSandbox();
        req->set_pod_sandbox_id(podDescriptor.Id);
        return req->Invoke().AsVoid();
    }

    TFuture<void> UpdatePodResources(
        const TCriPodDescriptor& /*pod*/,
        const TCriContainerResources& /*resources*/) override
    {
        return MakeFuture(TError("Not implemented"));
    }

    TFuture<TCriDescriptor> CreateContainer(
        TCriContainerSpecPtr containerSpec,
        const TCriPodDescriptor& podDescriptor,
        TCriPodSpecPtr podSpec) override
    {
        auto req = RuntimeApi_.CreateContainer();
        req->set_pod_sandbox_id(podDescriptor.Id);

        auto* config = req->mutable_config();

        {
            auto* metadata = config->mutable_metadata();
            metadata->set_name(containerSpec->Name);

            // Set unique attempt for each newly created container.
            metadata->set_attempt(Attempt_++);
        }

        {
            auto& labels = *config->mutable_labels();

            for (const auto& [key, val] : containerSpec->Labels) {
                labels[key] = val;
            }

            labels[YTPodNamespaceLabel] = Config_->Namespace;
            labels[YTPodNameLabel] = podSpec->Name;
            labels[YTContainerNameLabel] = containerSpec->Name;
        }

        FillImageSpec(config->mutable_image(), containerSpec->Image);

        for (const auto& mountSpec : containerSpec->BindMounts) {
            auto* mount = config->add_mounts();
            mount->set_container_path(mountSpec.ContainerPath);
            mount->set_host_path(mountSpec.HostPath);
            mount->set_readonly(mountSpec.ReadOnly);
            mount->set_propagation(NProto::PROPAGATION_PRIVATE);
        }

        for (const auto& deviceSpec : containerSpec->BindDevices) {
            auto* device = config->add_devices();
            device->set_container_path(deviceSpec.ContainerPath);
            device->set_host_path(deviceSpec.HostPath);

            TString permissions;
            if (Any(deviceSpec.Permissions & ECriBindDevicePermissions::Read)) {
                permissions += "r";
            }
            if (Any(deviceSpec.Permissions & ECriBindDevicePermissions::Write)) {
                permissions += "w";
            }
            if (Any(deviceSpec.Permissions & ECriBindDevicePermissions::Create)) {
                permissions += "m";
            }
            device->set_permissions(permissions);
        }

        {
            ToProto(config->mutable_command(), containerSpec->Command);
            ToProto(config->mutable_args(), containerSpec->Arguments);

            config->set_working_dir(containerSpec->WorkingDirectory);

            for (const auto& [key, val] : containerSpec->Environment) {
                auto* env = config->add_envs();
                env->set_key(key);
                env->set_value(val);
            }
        }

        {
            auto* linux = config->mutable_linux();
            FillLinuxContainerResources(linux->mutable_resources(), containerSpec->Resources);

            auto* security = linux->mutable_security_context();

            auto* namespaces = security->mutable_namespace_options();
            namespaces->set_network(NProto::NODE);

            security->set_readonly_rootfs(containerSpec->ReadOnlyRootFS);

            if (containerSpec->Credentials.Uid) {
                security->mutable_run_as_user()->set_value(*containerSpec->Credentials.Uid);
            }
            if (containerSpec->Credentials.Gid) {
                security->mutable_run_as_group()->set_value(*containerSpec->Credentials.Gid);
            }
            ToProto(security->mutable_supplemental_groups(), containerSpec->Credentials.Groups);

            auto* capabilities = security->mutable_capabilities();
            for (const auto& capability : containerSpec->CapabilitiesToAdd) {
                capabilities->add_add_capabilities(capability);
            }
        }

        FillPodSandboxConfig(req->mutable_sandbox_config(), *podSpec);

        return req->Invoke()
            .Apply(BIND([name = containerSpec->Name] (const TCriRuntimeApi::TRspCreateContainerPtr& rsp) -> TCriDescriptor {
                return TCriDescriptor{.Name = name, .Id = rsp->container_id()};
            }))
            .ToUncancelable();
    }

    TFuture<void> StartContainer(const TCriDescriptor& descriptor) override
    {
        auto req = RuntimeApi_.StartContainer();
        req->set_container_id(descriptor.Id);
        return req->Invoke().AsVoid();
    }

    TFuture<void> StopContainer(const TCriDescriptor& descriptor, TDuration timeout) override
    {
        auto req = RuntimeApi_.StopContainer();
        req->set_container_id(descriptor.Id);
        req->set_timeout(timeout.Seconds());
        return req->Invoke().AsVoid();
    }

    TFuture<void> RemoveContainer(const TCriDescriptor& descriptor) override
    {
        auto req = RuntimeApi_.RemoveContainer();
        req->set_container_id(descriptor.Id);
        return req->Invoke().AsVoid();
    }

    TFuture<void> UpdateContainerResources(const TCriDescriptor& descriptor, const TCriContainerResources& resources) override
    {
        auto req = RuntimeApi_.UpdateContainerResources();
        req->set_container_id(descriptor.Id);
        FillLinuxContainerResources(req->mutable_linux(), resources);
        return req->Invoke().AsVoid();
    }

    void CleanNamespace() override
    {
        YT_VERIFY(Config_->Namespace);
        auto pods = WaitFor(ListPodSandbox())
            .ValueOrThrow();

        {
            std::vector<TFuture<void>> futures;
            futures.reserve(pods->items_size());
            for (const auto& pod : pods->items()) {
                TCriPodDescriptor podDescriptor{.Name = pod.metadata().name(), .Id = pod.id() };
                futures.push_back(StopPodSandbox(podDescriptor));
            }
            WaitFor(AllSucceeded(std::move(futures)))
                .ThrowOnError();
        }

        {
            std::vector<TFuture<void>> futures;
            futures.reserve(pods->items_size());
            for (const auto& pod : pods->items()) {
                TCriPodDescriptor podDescriptor{.Name = pod.metadata().name(), .Id = pod.id()};
                futures.push_back(RemovePodSandbox(podDescriptor));
            }
            WaitFor(AllSucceeded(std::move(futures)))
                .ThrowOnError();
        }
    }

    void CleanPodSandbox(const TCriPodDescriptor& podDescriptor) override
    {
        auto containers = WaitFor(ListContainers([=] (NProto::ContainerFilter& filter) {
                filter.set_pod_sandbox_id(podDescriptor.Id);
            }))
            .ValueOrThrow();

        {
            std::vector<TFuture<void>> futures;
            futures.reserve(containers->containers_size());
            for (const auto& container : containers->containers()) {
                TCriDescriptor containerDescriptor{.Name = container.metadata().name(), .Id = container.id()};
                futures.push_back(StopContainer(containerDescriptor, TDuration::Zero()));
            }
            WaitFor(AllSucceeded(std::move(futures)))
                .ThrowOnError();
        }

        {
            std::vector<TFuture<void>> futures;
            futures.reserve(containers->containers_size());
            for (const auto& container : containers->containers()) {
                TCriDescriptor containerDescriptor{.Name = container.metadata().name(), .Id = container.id()};
                futures.push_back(RemoveContainer(containerDescriptor));
            }
            WaitFor(AllSucceeded(std::move(futures)))
                .ThrowOnError();
        }
    }

    TFuture<TCriImageApi::TRspListImagesPtr> ListImages(
        std::function<void(NProto::ImageFilter&)> initFilter = nullptr) override
    {
        auto req = ImageApi_.ListImages();
        if (initFilter) {
            initFilter(*req->mutable_filter());
        }
        return req->Invoke();
    }

    TFuture<TCriImageApi::TRspImageStatusPtr> GetImageStatus(
        const TCriImageDescriptor& image,
        bool verbose = false) override
    {
        auto req = ImageApi_.ImageStatus();
        FillImageSpec(req->mutable_image(), image);
        req->set_verbose(verbose);
        return req->Invoke();
    }

    TFuture<TCriImageApi::TRspPullImagePtr> PullImage(
        const TCriImageDescriptor& image,
        TCriAuthConfigPtr authConfig = nullptr,
        TCriPodSpecPtr podSpec = nullptr) override
    {
        auto req = ImageApi_.PullImage();
        FillImageSpec(req->mutable_image(), image);
        if (authConfig) {
            FillAuthConfig(req->mutable_auth(), *authConfig);
        }
        if (podSpec) {
            FillPodSandboxConfig(req->mutable_sandbox_config(), *podSpec);
        }
        return req->Invoke();
    }

    TFuture<void> RemoveImage(const TCriImageDescriptor& image) override
    {
        auto req = ImageApi_.RemoveImage();
        FillImageSpec(req->mutable_image(), image);
        return req->Invoke().AsVoid();
    }

    TProcessBasePtr CreateProcess(
        const TString& path,
        TCriContainerSpecPtr containerSpec,
        const TCriPodDescriptor& podDescriptor,
        TCriPodSpecPtr podSpec) override
    {
        return New<TCriProcess>(path, this, std::move(containerSpec), podDescriptor, std::move(podSpec));
    }

private:
    const TCriExecutorConfigPtr Config_;
    TCriRuntimeApi RuntimeApi_;
    TCriImageApi ImageApi_;

    std::atomic<ui32> Attempt_;

    void FillLinuxContainerResources(NProto::LinuxContainerResources* resources, const TCriContainerResources& spec)
    {
        auto* unified = resources->mutable_unified();

        if (spec.CpuLimit) {
            i64 period = Config_->CpuPeriod.MicroSeconds();
            i64 quota = period * *spec.CpuLimit;

            resources->set_cpu_period(period);
            resources->set_cpu_quota(quota);
        }

        if (spec.MemoryLimit) {
            resources->set_memory_limit_in_bytes(*spec.MemoryLimit);
        }

        if (spec.MemoryRequest) {
            (*unified)["memory.low"] = ToString(*spec.MemoryRequest);
        }

        if (spec.MemoryOomGroup.value_or(Config_->MemoryOomGroup)) {
            (*unified)["memory.oom.group"] = "1";
        }

        if (const auto& cpusetCpus = spec.CpusetCpus) {
            resources->set_cpuset_cpus(*cpusetCpus);
        }
    }

    void FillPodSandboxConfig(NProto::PodSandboxConfig* config, const TCriPodSpec& spec)
    {
        {
            auto* metadata = config->mutable_metadata();
            metadata->set_namespace_(Config_->Namespace);
            metadata->set_name(spec.Name);
            metadata->set_uid(spec.Name);
        }

        {
            auto& labels = *config->mutable_labels();
            labels[YTPodNamespaceLabel] = Config_->Namespace;
            labels[YTPodNameLabel] = spec.Name;
        }

        {
            auto* linux = config->mutable_linux();
            linux->set_cgroup_parent(GetPodCgroup(spec.Name));

            auto* security = linux->mutable_security_context();
            auto* namespaces = security->mutable_namespace_options();
            namespaces->set_network(NProto::NODE);
        }
    }

    void FillImageSpec(NProto::ImageSpec* spec, const TCriImageDescriptor& image)
    {
        spec->set_image(image.Image);
    }

    void FillAuthConfig(NProto::AuthConfig* auth, const TCriAuthConfig& authConfig)
    {
        if (!authConfig.Username.empty()) {
            auth->set_username(authConfig.Username);
        }
        if (!authConfig.Password.empty()) {
            auth->set_password(authConfig.Password);
        }
        if (!authConfig.Auth.empty()) {
            auth->set_auth(authConfig.Auth);
        }
        if (!authConfig.ServerAddress.empty()) {
            auth->set_server_address(authConfig.ServerAddress);
        }
        if (!authConfig.IdentityToken.empty()) {
            auth->set_identity_token(authConfig.IdentityToken);
        }
        if (!authConfig.RegistryToken.empty()) {
            auth->set_registry_token(authConfig.RegistryToken);
        }
    }

    TRetryChecker GetRetryChecker()
    {
        return BIND_NO_PROPAGATE([config = Config_] (const TError& error) {
            if (IsRetriableError(error)) {
                return true;
            }
            if (error.GetCode() == NYT::EErrorCode::Generic) {
                const auto& message = error.GetMessage();
                for (const auto& prefix : config->RetryErrorPrefixes) {
                    if (message.starts_with(prefix)) {
                        return true;
                    }
                }
                if (config->RetryErrorPattern) {
                    return NRe2::RE2::PartialMatch(message, *config->RetryErrorPattern);
                }
            }
            return false;
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

ICriExecutorPtr CreateCriExecutor(TCriExecutorConfigPtr config)
{
    return New<TCriExecutor>(
        std::move(config),
        GetGrpcChannelFactory());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers::NCri
