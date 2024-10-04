#include "signature_generator.h"

#include "signature.h"
#include "signature_header.h"
#include "signature_preprocess.h"

#include <yt/yt/core/misc/error.h>

namespace NYT::NSignatureService {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NLogging;
using namespace NYson;
using namespace std::chrono_literals;

////////////////////////////////////////////////////////////////////////////////

TSignatureGenerator::TSignatureGenerator(IKeyStoreWriter* keyStore)
    : Store_(keyStore)
    , Owner_(Store_->GetOwner())
    , Logger(TLogger("SignatureGenerator").WithTag("Owner: %v", Owner_))
{
    InitializeCryptography();
    YT_LOG_INFO("Signature generator initialized");
}

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TSignatureGenerator::Rotate()
{
    YT_LOG_INFO(
        "Rotating keypair (PreviousKeyPair: %v)",
        (KeyPair_ ? std::optional{KeyPair_->KeyInfo().Meta().Id} : std::nullopt));

    auto now = Now();
    TKeyPair newKeyPair(TKeyPairMetadata{
        .Owner = Owner_,
        .Id = TKeyId{TGuid::Create()},
        .CreatedAt = now,
        .ValidAfter = now - TimeSyncMargin,
        .ExpiresAt = now + KeyExpirationTime});

    return Store_->RegisterKey(newKeyPair.KeyInfo()).Apply(
        BIND([this, keyPair = std::move(newKeyPair)] () mutable {
            {
                auto guard = WriterGuard(KeyPairLock_);
                KeyPair_ = std::move(keyPair);
            }
            YT_LOG_INFO("Rotated keypair (NewKeyPair: %v)", KeyPair_->KeyInfo().Meta().Id);
        }));
}

////////////////////////////////////////////////////////////////////////////////

[[nodiscard]] const TKeyInfo& TSignatureGenerator::KeyInfo() const noexcept
{
    YT_VERIFY(KeyPair_);
    return KeyPair_->KeyInfo();
}

////////////////////////////////////////////////////////////////////////////////

[[nodiscard]] TSignaturePtr TSignatureGenerator::Sign(TYsonString&& payload) const
{
    auto signatureId = TGuid::Create();
    auto now = Now();
    TSignaturePtr result = New<TSignature>();
    TSignatureHeader header;

    {
        auto guard = ReaderGuard(KeyPairLock_);

        if (!KeyPair_) {
            THROW_ERROR_EXCEPTION("Trying to sign with an uninitialized generator");
        }

        header = TSignatureHeaderImpl<TSignatureVersion{0, 1}>{
            .Issuer = Owner_.Underlying(),
            .KeypairId = KeyPair_->KeyInfo().Meta().Id.Underlying(),
            .SignatureId = signatureId,
            .IssuedAt = now,
            .ValidAfter = now - TimeSyncMargin,
            .ExpiresAt = now + SignatureExpirationTime,
        };

        result->Header_ = ConvertToYsonString(header, EYsonFormat::Binary);
        result->Payload_ = std::move(payload);

        auto toSign = PreprocessSignature(result->Header_, result->Payload_);

        if (!KeyPair_->KeyInfo().Meta().IsValid()) {
            YT_LOG_WARNING(
                "Signing with an invalid keypair (SignatureId: %v, KeyPair: %v)",
                signatureId,
                KeyPair_->KeyInfo().Meta().Id);
        }

        KeyPair_->Sign(toSign, result->Signature_);
    }

    YT_LOG_TRACE(
        "Created signature (SignatureId: %v, Header: %v, Payload: %v)",
        signatureId,
        header,
        ConvertToYsonString(result->Payload_, EYsonFormat::Text).ToString());

    YT_LOG_DEBUG(
        "Created signature (SignatureId: %v)",
        signatureId);

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignatureService
