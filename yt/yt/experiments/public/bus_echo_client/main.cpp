#include <yt/yt/library/program/program.h>

#include <yt/yt/core/bus/bus.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/client.h>

#include <yt/yt/core/profiling/timing.h>

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
    TBusEchoMessageHandler(int expectedCounter)
        : ExpectedCounter_(expectedCounter)
    { }

    virtual void HandleMessage(
        TSharedRefArray message,
        IBusPtr replyBus) noexcept override
    {
        const auto& peer = replyBus->GetEndpointDescription();
        auto id = Counter_++;

        YT_LOG_INFO("Received message with %v parts (Peer: %v, MessageId: %v)", message.Size(), peer, id);
        for (size_t i = 0; i < message.Size(); ++i) {
            YT_LOG_INFO("in[%v] = %Qv (Size: %v, MessageId: %v)", i, message[i], message[i].Size(), id);
        }

        if (id + 1 == ExpectedCounter_) {
            Promise_.Set();
        }
    }

    void Terminate(const TErrorOr<void>& errorOr)
    {
        Promise_.TrySet(errorOr);
    }

    TFuture<void> GetFuture()
    {
        return Promise_;
    }

private:
    const int ExpectedCounter_ = 0;
    std::atomic<int> Counter_ = {0};

    TPromise<void> Promise_ = NewPromise<void>();
};

class TBusEchoClient
    : public TProgram
{
public:
    TBusEchoClient()
    {
        Opts_.AddLongOption("address").StoreResult(&Address_).Required();
        Opts_.SetFreeArgsMin(0);
        Opts_.SetFreeArgsMax(100);
        Opts_.AddLongOption("flood").NoArgument().SetFlag(&Flood_);
        Opts_.AddLongOption("ca_file").StoreResult(&CAFile_);
        Opts_.AddLongOption("encryption_mode").StoreResult(&EncryptionMode_);
        Opts_.AddLongOption("verification_mode").StoreResult(&VerificationMode_);
    }

protected:
    void DoRun() override
    {
        DoSingleRun();

        if (!Flood_) {
            return;
        }

        NProfiling::TWallTimer timer;
        for (int index = 0; index < 1000; ++index) {
            timer.Restart();
            DoSingleRun();
            auto elapsed = timer.GetElapsedTime();
            if (elapsed > TDuration::MilliSeconds(200)) {
                Cout << Format("Attempt %v connected in %v", index, elapsed) << Endl;
            }
        }
    }

    void DoSingleRun()
    {
        auto config = New<TBusClientConfig>();
        config->Address = Address_;
        config->EnableAggressiveReconnect = true;

        if (!EncryptionMode_.empty()) {
            config->EncryptionMode = TEnumTraits<EEncryptionMode>::FromString(EncryptionMode_);
        }

        if (!VerificationMode_.empty()) {
            config->VerificationMode = TEnumTraits<EVerificationMode>::FromString(VerificationMode_);
        }

        if (!CAFile_.empty()) {
            config->CA = New<NCrypto::TPemBlobConfig>();
            config->CA->FileName = CAFile_;
        }

        YT_LOG_INFO("Connecting echo client to %v", Address_);

        auto client = CreateBusClient(config);
        auto handler = New<TBusEchoMessageHandler>(1);

        auto bus = client->CreateBus(handler);
        bus->SubscribeTerminated(BIND([handler] (const TErrorOr<void>& errorOr) {
            handler->Terminate(errorOr);
        }));

        const auto& parseResult = GetOptsParseResult();
        auto args = parseResult.GetFreeArgs();
        TSharedRefArrayBuilder arrayBuilder(args.size());

        std::vector<TSharedRef> parts;
        for (const auto& arg : args) {
            arrayBuilder.Add(TSharedRef::FromString(arg));
        }
        auto message = arrayBuilder.Finish();

        YT_LOG_INFO("Getting ready future");

        auto readyFuture = bus->GetReadyFuture();
        auto res = readyFuture.Get();
        if (!res.IsOK()) {
            YT_LOG_INFO("bus is NOT ready for use %v", res.GetMessage());
            return;
        }

        YT_LOG_INFO("bus is ready for use");

        YT_LOG_INFO("Sending message with %v parts", message.Size());
        for (size_t i = 0; i < message.Size(); ++i) {
            YT_LOG_INFO("out[%v] = %Qv (Size: %v)", i, message[i], message[i].Size());
        }

        auto future = bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full});
        future.Subscribe(BIND([] (const TError& error) {
            if (error.IsOK()) {
                YT_LOG_INFO("Message was sent");
            } else {
                YT_LOG_ERROR(error, "Failed to send message");
            }
        }));

        handler->GetFuture().Get();
    }

private:
    TString Address_;
    TString CAFile_;
    TString EncryptionMode_;
    TString VerificationMode_;
    bool Flood_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NAuth

int main(int argc, const char** argv)
{
    return NYT::TBusEchoClient().Run(argc, argv);
}
