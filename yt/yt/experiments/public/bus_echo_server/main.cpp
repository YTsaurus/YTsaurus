#include <yt/yt/library/program/program.h>

#include <yt/yt/core/bus/bus.h>
#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/crypto/config.h>

#include <yt/yt/core/yson/writer.h>

namespace NYT {

using namespace NBus;

static const auto Logger = NLogging::TLogger("BusEchoServer");

////////////////////////////////////////////////////////////////////////////////

class TBusEchoMessageHandler
    : public IMessageHandler
{
public:
    virtual void HandleMessage(
        TSharedRefArray message,
        IBusPtr replyBus) noexcept override
    {
        const auto& peer = replyBus->GetEndpointDescription();
        auto id = Counter_++;

        YT_LOG_INFO("Received message with %v parts (Peer: %v, MessageId: %v)", message.Size(), peer, id);
        for (size_t i = 0; i < message.Size(); ++i) {
            YT_LOG_INFO("parts[%v] = %Qv (Size: %v, MessageId: %v)", i, message[i], message[i].Size(), id);
        }

        auto future = replyBus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full});
        future.Subscribe(BIND([id] (const TError& error) {
            if (error.IsOK()) {
                YT_LOG_INFO("Message was echoed successfully (MessageId: %v)", id);
            } else {
                YT_LOG_ERROR(error, "Failed to echo message (MessageId: %v)", id);
            }
        }));
    }

private:
    std::atomic<int> Counter_ = {0};
};

class TBusEchoServer
    : public TProgram
{
public:
    TBusEchoServer()
    {
        Opts_.AddLongOption("port").StoreResult(&Port_).Required();
        Opts_.AddLongOption("encryption_mode").StoreResult(&EncryptionMode_);
        Opts_.AddLongOption("cert_file").StoreResult(&CertChainFile_);
        Opts_.AddLongOption("private_key_file").StoreResult(&PrivateKeyFile_);
        Opts_.AddLongOption("cipher_list").StoreResult(&CipherList_);
    }

protected:
    void DoRun() override
    {
        if (Port_ <= 0 || Port_ > 65535) {
            OnError("Invalid port (must be > 0 and <= 65535)");
            return;
        }

        auto config = New<TBusServerConfig>();
        config->Port = Port_;
        if (!EncryptionMode_.empty()) {
            config->EncryptionMode = TEnumTraits<EEncryptionMode>::FromString(EncryptionMode_);
        }

        if (!CipherList_.empty()) {
            config->CipherList = CipherList_;
        }

        if (!CertChainFile_.empty()) {
            config->CertificateChain = New<NCrypto::TPemBlobConfig>();
            config->CertificateChain->FileName = CertChainFile_;
        }

        if (!PrivateKeyFile_.empty()) {
            config->PrivateKey = New<NCrypto::TPemBlobConfig>();
            config->PrivateKey->FileName = PrivateKeyFile_;
        }

        YT_LOG_INFO("Starting echo server on port %v", Port_);

        auto server = CreateBusServer(config);
        auto handler = New<TBusEchoMessageHandler>();

        server->Start(handler);

        TString line;
        for (;;) {
            if (!Cin.ReadLine(line)) {
                break;
            }
        }

        YT_UNUSED_FUTURE(server->Stop());
    }

private:
    int Port_;
    TString EncryptionMode_;
    TString CertChainFile_;
    TString PrivateKeyFile_;
    TString CipherList_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NAuth

int main(int argc, const char** argv)
{
    return NYT::TBusEchoServer().Run(argc, argv);
}
