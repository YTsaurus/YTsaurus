#include "plugin.hpp"

#include <mutex>

#include <yt/cpp/mapreduce/interface/client.h>

#include <yt/cpp/mapreduce/library/parallel_io/parallel_file_reader.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/async_stream.h>

#include <yt/yt/library/auth/auth.h>

#include <yt/yt/library/program/helpers.h>
#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/config.h>

#include <library/cpp/yson/node/node_io.h>

//#include <yt/yt/client/cache/rpc.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/config.h>
#include <yt/yt/client/api/options.h>
#include <yt/yt/client/api/file_reader.h>
#include <yt/yt/client/api/file_writer.h>

#include <yt/yt/client/chunk_client/helpers.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/replication_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/file_client/chunk_meta_extensions.h>

#include <yt/yt/core/logging/log.h>

//#include <yt/cpp/mapreduce/interface/logging/logger.h>

#include <library/cpp/yt/memory/new.h>

#include <yt/yt/core/ytree/yson_struct.h>

// #include <yt/yt/core/logging/log.h>

#include <util/system/env.h>
#include <util/system/getpid.h>

namespace NYT::NFio {

using namespace NConcurrency;

static NLogging::TLogger Logger("fio_plugin");

//

DEFINE_ENUM(EClientType,
    (Http)
    (Rpc)
    (Native)
);

DECLARE_REFCOUNTED_CLASS(TPluginConfig)

class TPluginConfig
    : public TSingletonsConfig
{
public:
    EClientType ClientType;
    bool PrintFileChunks;

    NApi::TFileReaderConfigPtr FileReaderConfig;
    NApi::TFileWriterConfigPtr FileWriterConfig;

    REGISTER_YSON_STRUCT(TPluginConfig);

    static void Register(TRegistrar registrar) {
        registrar.Parameter("client_type", &TThis::ClientType)
            .Default(EClientType::Http);
        registrar.Parameter("print_file_chunks", &TThis::PrintFileChunks)
            .Default();
        registrar.Parameter("file_reader_config", &TThis::FileReaderConfig)
            .DefaultNew();
        registrar.Parameter("file_writer_config", &TThis::FileWriterConfig)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TPluginConfig);

//

DECLARE_REFCOUNTED_STRUCT(TFile)

struct TFile
    : public TRefCounted
{
    TFile(TStringBuf name)
        : Name(name)
    {
    }

    TString Name;

    uint64_t ReaderOffset;
    IFileReaderPtr Reader;
    IFileWriterPtr Writer;

    IAsyncInputStreamPtr AsyncReader;
    NApi::IFileWriterPtr AsyncWriter;

    TFuture<void> Future;

    bool UnlinkAtClose = false;
};

DEFINE_REFCOUNTED_TYPE(TFile)

struct TAsyncIO
{
    TFuture<void> Future;
    TOperationResult Result;
};

//

static ui64 TransferDataZeroCopy(IInputStream *input, IZeroCopyOutput *output, ui64 length)
{
    char* ptr = nullptr;
    size_t size = 0;
    ui64 result = 0;

    while (result < length) {
        if (!size) {
            size = output->Next(&ptr);
        }
        auto len = input->Read(ptr, size);
        if (!len) {
            break;
        }
        result += len;
        ptr += len;
        size -= len;
    }

    if (size) {
        output->Undo(size);
    }

    return result;
}

//

DECLARE_REFCOUNTED_STRUCT(THttpThread)

struct THttpThread
    : public IThread
    , public TRefCounted
{
    THttpThread(IClientPtr client, TPluginConfigPtr config, const TThreadOptions& options)
        : Client(client)
        , Config(config)
    {
    }

    int64_t GetFileSize(const char *name, TFile* file) override
    {
        TYPath path(name);
        int64_t size = -1;
        if (Client->Exists(path)) {
            size = Client->Get(path + "/@uncompressed_data_size").AsInt64();
        }
        YT_LOG_DEBUG("GetFileSize %v %v", path, size);
        return size;
    }

    int UnlinkFile(const char *name, TFile* file) override
    {
        if (file) {
            file->UnlinkAtClose = true;
            return 0;
        }
        TYPath path(name);
        if (Client->Exists(path)) {
            YT_LOG_DEBUG("UnlinkFile %v", path);
            Client->Remove(path, TRemoveOptions().Force(true));
        }
        return 0;
    }

    TFile* OpenFile(const char *name, const TFileOptions& options) override
    {
        auto file = New<TFile>(name);

        if (options.Read) {
            YT_LOG_INFO("OpenFile %v for read offset %v length %v", name, options.FileOffset, options.IOSize);
            auto readerOptions = TFileReaderOptions();
            readerOptions.Offset(options.FileOffset);
            if (options.IOSize) {
                readerOptions.Length(options.IOSize);
            }
            readerOptions.CreateTransaction(options.Atomic);
            if (Config->FileReaderConfig) {
                auto yson = NYson::ConvertToYsonString(Config->FileReaderConfig);
                readerOptions.Config(NodeFromYsonString(yson.AsStringBuf()));
            }
            file->Reader = Client->CreateFileReader(TYPath(file->Name), readerOptions);
            file->ReaderOffset = options.FileOffset;
        }

        if (options.Write) {
            YT_LOG_INFO("OpenFile %v for write", name);
            TFileWriterOptions writerOptions;
            writerOptions.CreateTransaction(options.Atomic);
            TRichYPath path(file->Name);
            path.Append(options.Append);
            file->Writer = Client->CreateFileWriter(path, writerOptions);
        }

        return file.Release();
    }

    void CloseFile(TFile *file) override
    {
        YT_LOG_DEBUG("CloseFile %v", file->Name);

        if (file->Writer) {
            YT_LOG_DEBUG("Finish writer %v", file->Name);
            file->Writer->Finish();
        }

        if (file->UnlinkAtClose) {
            UnlinkFile(file->Name.c_str(), nullptr);
        }

        NYT::Unref(file);
    }

    int Prepare(TFile* file, EOperation op) override
    {
        return 0;
    }

    TOperationResult Queue(TFile* file, EOperation op, void *buf, uint64_t size, uint64_t offset, uintptr_t io) override
    {
        switch (op) {
            case EOperation::Read: {

                if (file->ReaderOffset != offset) {
                    YT_LOG_INFO("Reopen file %v from %v to %v", file->Name, file->ReaderOffset, offset);
                    file->Reader = Client->CreateFileReader(
                        TYPath(file->Name),
                        TFileReaderOptions().Offset(offset).CreateTransaction(false));
                    file->ReaderOffset = offset;
                }

                TMemoryOutput output(buf, size);
                auto length = TransferDataZeroCopy(file->Reader.Get(), &output, size);
                file->ReaderOffset += length;

                YT_LOG_DEBUG("Read %v offset %v size %v result %v", file->Name, offset, size, length);
                return TOperationResult {
                    .Status = EOperationStatus::Completed,
                    .Residual = size - length,
                };
            }
            case EOperation::Write: {
                file->Writer->Write(buf, size);
                YT_LOG_DEBUG("Write %v size %v", file->Name, size);
                return TOperationResult {
                    .Status = EOperationStatus::Completed,
                };
            }
            default:
                return TOperationResult {
                    .Status = EOperationStatus::Busy,
                };
        }
    }

    int Cancel(TFile* file) override
    {
        Client->Shutdown();
        return 0;
    }

    int Commit() override
    {
        return 0;
    }

    int GetEvents(int minEvents, int maxEvents, const struct timespec *timeout) override
    {
        return 0;
    }

    TOperationResult GetResult(int event) override
    {
        return TOperationResult{
            .Status = EOperationStatus::Completed,
        };
    }

    void Put() override
    {
        YT_LOG_DEBUG("PutThread");
        Client->Shutdown();
        NYT::Unref(this);
    }

    IClientPtr Client;
    const TPluginConfigPtr Config;
};

DEFINE_REFCOUNTED_TYPE(THttpThread)

//

DECLARE_REFCOUNTED_STRUCT(TRpcThread)

struct TRpcThread
    : public IThread
    , public TRefCounted
{
    TRpcThread(
        NApi::IClientPtr client,
        NApi::NNative::IClientPtr nativeClient,
        TPluginConfigPtr config,
        const TThreadOptions& options)
        : Client(client)
        , NativeClient(nativeClient)
        , Config(config)
        , IODepth(options.IODepth)
        , AsyncIO(IODepth)

    {
        for (size_t slot = 0; slot < IODepth; ++slot) {
            FreeSlots.push(slot);
        }
        CompleteSlots.reserve(IODepth);
    }

    int64_t GetFileSize(const char *name, TFile* file) override
    {
        TYPath path(name);
        int64_t size = -1;
        auto sizeOrError = WaitFor(Client->GetNode(path + "/@uncompressed_data_size"));
        if (sizeOrError.IsOK()) {
            size = NYTree::ConvertTo<ui64>(sizeOrError.Value());
        } else if (!sizeOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            sizeOrError.ThrowOnError();
        }
        YT_LOG_DEBUG("GetFileSize %v %v", path, size);
        return size;
    }

    void PrintFileChunks(TString path)
    {
        if (!NativeClient) {
            return;
        }

        using namespace NYT::NChunkClient;

        TUserObject userObject(path);

        GetUserObjectBasicAttributes(
            NativeClient,
            {&userObject},
            NObjectClient::NullTransactionId,
            Logger,
            NYTree::EPermission::Read,
            TGetUserObjectBasicAttributesOptions{
                .SuppressAccessTracking = true,
                .SuppressExpirationTimeoutRenewal = true,
            });

        YT_LOG_INFO("Object %v %v %v", userObject.Path, userObject.ObjectId, userObject.Type);

        auto connection = NativeClient->GetNativeConnection();
        auto chunkSpecs = NChunkClient::FetchChunkSpecs(
            NativeClient,
            connection->GetNodeDirectory(),
            userObject,
            {NChunkClient::TReadRange()},
            /*chunkCount*/ -1,
            connection->GetConfig()->MaxChunksPerFetch,
            connection->GetConfig()->MaxChunksPerLocateRequest,
            [&] (const NChunkClient::TChunkOwnerYPathProxy::TReqFetchPtr& request) {
                request->set_fetch_all_meta_extensions(true);
                // request->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
                // request->add_extension_tags(TProtoExtensionTag<NFileClient::NProto::TBlocksExt>::Value);
            },
            Logger);

        auto chunkReaderHost = TChunkReaderHost::FromClient(NativeClient);

        YT_LOG_INFO("File %v chunks %v", path, chunkSpecs.size());
        for (auto& chunkSpec : chunkSpecs) {
            auto chunkId = FromProto<TChunkId>(chunkSpec.chunk_id());

            auto readerConfig = New<TReplicationReaderConfig>();
            auto remoteReaderOptions = New<TRemoteReaderOptions>();

            auto chunkReplicas = GetReplicasFromChunkSpec(chunkSpec);

            auto reader = CreateReplicationReader(
                std::move(readerConfig),
                remoteReaderOptions,
                chunkReaderHost,
                chunkId,
                chunkReplicas);

            TClientChunkReadOptions chunkReadOptions{};

            auto chunkMetaOrError = WaitFor(reader->GetMeta(
                    chunkReadOptions,
                    /*partitionTag*/ std::nullopt,
                    /*extensionTags*/ std::nullopt));
            auto chunkMeta = chunkMetaOrError.ValueOrThrow();

            YT_LOG_INFO("Chunk %v", chunkId);

            if (chunkMeta->has_extensions()) {
                auto extensionsCount = chunkMeta->extensions().extensions_size();
                std::vector<int> extensions;
                for (int index = 0; index < extensionsCount; index++) {
                    extensions.push_back(chunkMeta->extensions().extensions(index).tag());
                }
                YT_LOG_INFO("Extensions %v", extensions);
            }

            if (auto blocksExt = FindProtoExtension<NChunkClient::NProto::TBlocksExt>(chunkMeta->extensions())) {
                auto blockCount = blocksExt->blocks_size();
                YT_LOG_INFO("Blocks %v", blockCount);
                for (int index = 0; index < blockCount; ++index) {
                    auto& block = blocksExt->blocks(index);
                    YT_LOG_INFO("Block %v size %v", index, block.size());
                }
            }

            if (auto fileBlocksExt = FindProtoExtension<NFileClient::NProto::TBlocksExt>(chunkMeta->extensions())) {
                auto blockCount = fileBlocksExt->blocks_size();
                YT_LOG_INFO("File blocks %v", blockCount);
                for (int index = 0; index < blockCount; ++index) {
                    auto& block = fileBlocksExt->blocks(index);
                    YT_LOG_INFO("Block %v size %v", index, block.size());
                }
            }
        }
    }

    int UnlinkFile(const char *name, TFile* file) override
    {
        if (file) {
            file->UnlinkAtClose = true;
            return 0;
        }
        TYPath path(name);
        NApi::TRemoveNodeOptions removeOptions;
        removeOptions.Force = true;
        YT_LOG_DEBUG("UnlinkFile %v", path);
        WaitFor(Client->RemoveNode(path, removeOptions))
            .ThrowOnError();
        return 0;
    }

    TFile* OpenFile(const char *name, const TFileOptions& options) override
    {
        if (Config->PrintFileChunks) {
            PrintFileChunks(name);
        }

        auto file = New<TFile>(name);

        if (options.Read) {
            YT_LOG_INFO("OpenFile %v for read offset %v length %v", name, options.FileOffset, options.IOSize);
            NApi::TFileReaderOptions readerOptions;
            readerOptions.Offset = options.FileOffset;
            if (options.IOSize) {
                readerOptions.Length = options.FileOffset + options.IOSize;
            }
            readerOptions.Config = Config->FileReaderConfig;
            auto reader = WaitFor(Client->CreateFileReader(TYPath(file->Name), readerOptions))
                .ValueOrThrow();
            file->AsyncReader = CreateCopyingAdapter(reader);
            file->ReaderOffset = options.FileOffset;
        }

        if (options.Write) {
            NYPath::TRichYPath path(file->Name);
    
            {
                // https://ytsaurus.tech/docs/en/api/cpp/description#create
                NApi::TCreateNodeOptions createOptions;
                createOptions.Recursive = true;
                createOptions.IgnoreExisting = true;
                WaitFor(Client->CreateNode(path.GetPath(), NObjectClient::EObjectType::File, createOptions))
                    .ThrowOnError();
            }

            if (options.Append) {
                path.SetAppend(true);
            }

            NApi::TFileWriterOptions writerOptions;
            writerOptions.Config = Config->FileWriterConfig;
            file->AsyncWriter = Client->CreateFileWriter(path, writerOptions);
            WaitFor(file->AsyncWriter->Open())
                .ThrowOnError();
        }

        return file.Release();
    }

    void CloseFile(TFile *file) override
    {
        YT_LOG_DEBUG("CloseFile %v", file->Name);

        if (file->AsyncWriter) {
            WaitFor(file->AsyncWriter->Close())
                .ThrowOnError();
        }

        if (file->UnlinkAtClose) {
            UnlinkFile(file->Name.c_str(), nullptr);
        }

        NYT::Unref(file);
    }

    int Prepare(TFile* file, EOperation op) override
    {
        return 0;
    }

    TOperationResult Queue(TFile* file, EOperation op, void *buf, uint64_t size, uint64_t offset, uintptr_t io) override
    {
        if (file->Future && !file->Future.IsSet()) {
            YT_LOG_WARNING("File %v Busy", file->Name);
            return TOperationResult {
                .Status = EOperationStatus::Busy,
            };
        }
        switch (op) {
            case EOperation::Read: {

                if (file->ReaderOffset != offset) {
                    YT_LOG_INFO("Reopen file %v from %v to %v", file->Name, file->ReaderOffset, offset);
                    NApi::TFileReaderOptions readerOptions;
                    readerOptions.Offset = offset;
                    auto reader = WaitFor(Client->CreateFileReader(TYPath(file->Name), readerOptions)).ValueOrThrow();
                    file->AsyncReader = CreateCopyingAdapter(reader);
                    file->ReaderOffset = offset;
                }

                if (FreeSlots.empty()) {
                    TMemoryOutput output(buf, size);
                    auto reader = CreateSyncAdapter(file->AsyncReader);
                    auto length = TransferDataZeroCopy(reader.get(), &output, size);
                    YT_LOG_DEBUG("Sync read %v offset %v size %v result %v", file->Name, offset, size, length);
                    file->ReaderOffset += length;
                    return TOperationResult {
                        .Status = EOperationStatus::Completed,
                        .Residual = size - length,
                    };
                }

                auto slot = FreeSlots.front();
                FreeSlots.pop();

                YT_LOG_DEBUG("Queue read %v offset %v size %v slot %v", file->Name, offset, size, slot);
                AsyncIO[slot].Result = TOperationResult {
                    .Status = EOperationStatus::Completed,
                    .IO = io,
                };
                auto* result = &AsyncIO[slot].Result;
                AsyncIO[slot].Future = file->AsyncReader->Read(TSharedMutableRef(buf, size, nullptr))
                    .Apply(BIND([=] (size_t length) {
                        YT_LOG_DEBUG("Complete slot slot %v size %v result %v", slot, size, length);
                        file->ReaderOffset += length;
                        result->Residual = size - length;
                    }));
                if (AsyncIO[slot].Future.IsSet()) {
                    YT_LOG_DEBUG("Sync complete slot %v residual %v", slot, result->Residual);
                    AsyncIO[slot].Future.Reset();
                    FreeSlots.push(slot);
                    return *result;
                }
                file->Future = AsyncIO[slot].Future;
                return TOperationResult {
                    .Status = EOperationStatus::Queued,
                };
            }
            case EOperation::Write: {
                if (FreeSlots.empty()) {
                    WaitFor(file->AsyncWriter->Write(TSharedRef(buf, size, nullptr)))
                        .ThrowOnError();
                    YT_LOG_DEBUG("Sync write %v size %v", file->Name, size);
                    return TOperationResult {
                        .Status = EOperationStatus::Completed,
                    };
                }

                auto slot = FreeSlots.front();
                FreeSlots.pop();

                YT_LOG_DEBUG("Queue write %v size %v slot %v", file->Name, size, slot);
                AsyncIO[slot].Result = TOperationResult {
                    .Status = EOperationStatus::Completed,
                    .IO = io,
                };
                AsyncIO[slot].Future = file->AsyncWriter->Write(TSharedRef(buf, size, nullptr));
                file->Future = AsyncIO[slot].Future;
                return TOperationResult {
                    .Status = EOperationStatus::Queued,
                };
            }
            case EOperation::Sync: {
                return TOperationResult {
                    .Status = EOperationStatus::Completed,
                };
            }
            default:
                return TOperationResult {
                    .Status = EOperationStatus::Busy,
                };
        }
    }

    int Cancel(TFile* file) override
    {
        YT_LOG_DEBUG("Cancel");
        Client->Terminate();
        return 0;
    }

    int Commit() override
    {
        YT_LOG_DEBUG("Commit");
        return 0;
    }

    int GetEvents(int minEvents, int maxEvents, const struct timespec *timeout) override
    {
        size_t startSlot = NextSlot;
        size_t slot = startSlot;
        ssize_t waitSlot = -1;
        CompleteSlots.clear();
        while (true) {
            if (AsyncIO[slot].Future) {
                if (AsyncIO[slot].Future.IsSet()) {
                    YT_LOG_DEBUG("Get complete slot %v", slot);
                    AsyncIO[slot].Future.Reset();
                    FreeSlots.push(slot);
                    CompleteSlots.push_back(slot);
                    if (std::ssize(CompleteSlots) >= maxEvents) {
                        if (++slot == IODepth) {
                            slot = 0;
                        }
                        NextSlot = slot;
                        break;
                    }
                } else {
                    waitSlot = slot;
                }
            }
            if (++slot == IODepth) {
                slot = 0;
            }
            if (slot == startSlot) {
                if (std::ssize(CompleteSlots) >= minEvents || waitSlot < 0) {
                    break;
                }
                YT_LOG_DEBUG("Wait slot %v", waitSlot);
                WaitFor(AsyncIO[waitSlot].Future)
                    .ThrowOnError();
            }
        }
        auto events = std::ssize(CompleteSlots);
        YT_LOG_DEBUG("GetEvents result %v", events);
        return events;
    }

    TOperationResult GetResult(int event) override
    {
        auto slot = CompleteSlots[event];
        YT_LOG_DEBUG("GetResult event %v slot %v residual %v", event, slot, AsyncIO[slot].Result.Residual);
        return AsyncIO[slot].Result;
    }

    void Put() override
    {
        YT_LOG_DEBUG("PutThread");
        Client->Terminate();
        Client->GetConnection()->Terminate();
        NYT::Unref(this);
    }

    NApi::IClientPtr Client;
    NApi::NNative::IClientPtr NativeClient;
    TPluginConfigPtr Config;

    const size_t IODepth;
    size_t NextSlot = 0;
    std::vector<TAsyncIO> AsyncIO;
    TRingQueue<size_t> FreeSlots;
    std::vector<size_t> CompleteSlots;
};

DEFINE_REFCOUNTED_TYPE(TRpcThread)

//

static void Initialize(const TPluginConfigPtr& config)
{
    static std::once_flag onceFlag;
    std::call_once(onceFlag, [&] {
        YT_LOG_INFO("Initialize (pid=%v)", GetPID());
        NYT::Initialize();
        if (config->ClientType != EClientType::Http) {
            ConfigureSingletons(config);
        }
    });
}

static TPluginConfigPtr GetConfig(const TPluginOptions& pluginOptions)
{
    auto config = New<TPluginConfig>();
    config->SetUnrecognizedStrategy(NYTree::EUnrecognizedStrategy::ThrowRecursive);

    if (pluginOptions.Config) {
        TFileInput stream(pluginOptions.Config);
        NYson::TYsonPullParser parser(&stream, NYson::EYsonType::Node);
        NYson::TYsonPullParserCursor cursor(&parser);
        config->Load(&cursor);
    }

    if (pluginOptions.ConfigPatch) {
        TMemoryInput stream(pluginOptions.ConfigPatch);
        NYson::TYsonPullParser parser(&stream, NYson::EYsonType::Node);
        NYson::TYsonPullParserCursor cursor(&parser);
        config->Load(&cursor);
    }

    if (pluginOptions.PrintConfig) {
        NYson::TYsonWriter writer(&Cout, NYson::EYsonFormat::Pretty);
        config->Save(&writer);
        Cout << Endl;
    }

    if (pluginOptions.PrintConfigSchema) {
        NYson::TYsonWriter writer(&Cout, NYson::EYsonFormat::Pretty);
        config->WriteSchema(&writer);
        Cout << Endl;
    }

    return config;
}

IThread* GetThread(const TPluginOptions& pluginOptions, const TThreadOptions& threadOptions)
{
    YT_LOG_DEBUG("GetThread (job=%v, index=%v, pid=%v)", threadOptions.JobName, threadOptions.JobIndex, GetPID());
    auto config = GetConfig(pluginOptions);
    Initialize(config);

    switch (config->ClientType) {
        case EClientType::Http: {
            auto httpClient = CreateClientFromEnv();
            return New<THttpThread>(httpClient, config, threadOptions).Release();
        }
        case EClientType::Rpc: {
            auto connectionConfig = NApi::NRpcProxy::TConnectionConfig::CreateFromClusterUrl(GetEnv("YT_PROXY"));
            auto connection = NApi::NRpcProxy::CreateConnection(connectionConfig);

            // auto clientOptions = NApi::GetClientOpsFromEnv();
            auto clientOptions = NAuth::TAuthenticationOptions::FromToken(NAuth::LoadToken().value_or(""));

            // auto client = NYT::NClient::NCache::CreateClient();
            auto client = connection->CreateClient(clientOptions);

            return New<TRpcThread>(client, /*nativeClient*/ nullptr, config, threadOptions).Release();
        }
        case EClientType::Native: {
            auto httpClient = CreateClientFromEnv();
            auto httpAuthInfo = httpClient->WhoAmI();

            auto clusterConnectionConfig = httpClient->Get("//sys/@cluster_connection");

            YT_LOG_INFO("Cluster connection: %v", NodeToYsonString(clusterConnectionConfig, NYson::EYsonFormat::Text));

            auto compoundConfig = New<NApi::NNative::TConnectionCompoundConfig>(
                NYTree::ConvertToNode(NYson::TYsonStringBuf(NodeToYsonString(clusterConnectionConfig))));

            NApi::NNative::TConnectionOptions connectionOptions;
            auto connection = NApi::NNative::CreateConnection(compoundConfig, std::move(connectionOptions));

            auto clientOptions = NYT::NApi::TClientOptions::FromUser(httpAuthInfo.Login);

            auto client = connection->CreateNativeClient(clientOptions);

            return New<TRpcThread>(client, client, config, threadOptions).Release();
        }
    }
}

} // namespace NYT::NFio
