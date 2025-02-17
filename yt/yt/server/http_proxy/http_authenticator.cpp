#include "http_authenticator.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/security_client/user_attribute_cache.h>

#include <yt/yt/library/auth_server/config.h>
#include <yt/yt/library/auth_server/token_authenticator.h>
#include <yt/yt/library/auth_server/ticket_authenticator.h>
#include <yt/yt/library/auth_server/cookie_authenticator.h>
#include <yt/yt/library/auth_server/credentials.h>
#include <yt/yt/library/auth_server/helpers.h>
#include <yt/yt/library/auth_server/authentication_manager.h>

#include <yt/yt/core/http/http.h>
#include <yt/yt/core/http/helpers.h>

#include <yt/yt/core/ytree/fluent.h>

#include <util/string/strip.h>

namespace NYT::NHttpProxy {

using namespace NApi::NNative;
using namespace NAuth;
using namespace NHttp;
using namespace NYTree;
using namespace NConcurrency;

DEFINE_REFCOUNTED_TYPE(THttpAuthenticator)
DEFINE_REFCOUNTED_TYPE(TCompositeHttpAuthenticator)

////////////////////////////////////////////////////////////////////////////////

void SetStatusFromAuthError(const NHttp::IResponseWriterPtr& rsp, const TError& error)
{
    if (error.FindMatching(NRpc::EErrorCode::InvalidCredentials)) {
        rsp->SetStatus(EStatusCode::Unauthorized);
    } else if (error.FindMatching(NRpc::EErrorCode::InvalidCsrfToken)) {
        rsp->SetStatus(EStatusCode::Unauthorized);
    } else if (error.FindMatching(NSecurityClient::EErrorCode::AuthenticationError)) {
        rsp->SetStatus(EStatusCode::Unauthorized);
    } else {
        rsp->SetStatus(EStatusCode::ServiceUnavailable);
    }
}

////////////////////////////////////////////////////////////////////////////////

THttpAuthenticator::THttpAuthenticator(
    TBootstrap* bootstrap,
    const TAuthenticationManagerConfigPtr& authManagerConfig,
    const IAuthenticationManagerPtr& authManager)
    : Bootstrap_(bootstrap)
    , Config_(authManagerConfig)
    , AuthenticationManager_(authManager)
    , TokenAuthenticator_(authManager->GetTokenAuthenticator())
    , CookieAuthenticator_(authManager->GetCookieAuthenticator())
{
    YT_VERIFY(Config_);
}

void THttpAuthenticator::HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp)
{
    auto result = Authenticate(req, true);
    if (result.IsOK()) {
        rsp->SetStatus(EStatusCode::OK);
        ProtectCsrfToken(rsp);

        auto csrfSecret = Config_->GetCsrfSecret();
        auto csrfToken = SignCsrfToken(result.Value().Result.Login, csrfSecret, TInstant::Now());

        ReplyJson(rsp, [&] (NYson::IYsonConsumer* consumer) {
            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("login").Value(result.Value().Result.Login)
                    .Item("realm").Value(result.Value().Result.Realm)
                    .Item("real_login").Value(GetRealLogin(result.Value().Result))
                    .Item("csrf_token").Value(csrfToken)
                .EndMap();
        });
    } else {
        SetStatusFromAuthError(rsp, TError(result));
        ReplyJson(rsp, [&] (auto consumer) {
            BuildYsonFluently(consumer)
                .Value(TError(result));
        });
    }
}

TErrorOr<TAuthenticationResultAndToken> THttpAuthenticator::Authenticate(
    const IRequestPtr& request,
    bool disableCsrfTokenCheck)
{
    constexpr TStringBuf UserNameHeader = "X-YT-User-Name";
    if (!Config_->RequireAuthentication) {
        auto user = NRpc::RootUserName;
        if (auto userNameHeader = request->GetHeaders()->Find(UserNameHeader)) {
            user = *userNameHeader;
        }
        static const auto UserTicket = TString();
        TAuthenticationResult result{
            .Login = user,
            .Realm = "YT",
            .UserTicket = UserTicket,
        };
        return TAuthenticationResultAndToken{result, TString()};
    }

    auto userIP = request->GetRemoteAddress();
    auto realIP = FindBalancerRealIP(request);
    if (realIP) {
        auto parsedRealIP = NNet::TNetworkAddress::TryParse(*realIP);
        if (parsedRealIP.IsOK()) {
            userIP = parsedRealIP.ValueOrThrow();
        }
    }

    NTracing::TChildTraceContextGuard authSpan("HttpProxy.Auth");

    constexpr TStringBuf AuthorizationHeaderName = "Authorization";
    // COMPAT(achulkov2): Remove once yql_agent is added to superusers everywhere.
    const THashSet<TStringBuf> UserImpersonationWhitelist{"yql_agent"};
    if (auto authorizationHeader = request->GetHeaders()->Find(AuthorizationHeaderName)) {
        static const TStringBuf Prefix = "OAuth ";
        if (!authorizationHeader->StartsWith(Prefix)) {
            return TError(
                NRpc::EErrorCode::InvalidCredentials,
                "Malformed Authorization header");
        }

        TTokenCredentials credentials{
            .Token = authorizationHeader->substr(Prefix.size()),
            .UserIP = userIP
        };

        if (!credentials.Token.empty()) {
            if (!TokenAuthenticator_) {
                return TError(
                    NRpc::EErrorCode::InvalidCredentials,
                    "Client has provided a token but no token authenticator is configured");
            }

            auto rsp = WaitFor(TokenAuthenticator_->Authenticate(credentials));
            if (!rsp.IsOK()) {
                return TError(rsp);
            }

            auto authenticationResult = rsp.Value();

            if (auto userHeader = request->GetHeaders()->Find(UserNameHeader)) {
                auto isWhitelisted = UserImpersonationWhitelist.contains(authenticationResult.Login);

                auto isSuperuserOrError = WaitFor(IsSuperuser(Bootstrap_->GetNativeConnection(), authenticationResult.Login));
                // There should be no errors in checking for superuser status even for whitelisted users, so let's throw.
                if (!isSuperuserOrError.IsOK()) {
                    return TError(isSuperuserOrError);
                }

                // This should almost always return straight from cache.
                auto isUserBannedOrError = WaitForFast(IsUserBanned(Bootstrap_->GetNativeConnection(), authenticationResult.Login));
                if (!isUserBannedOrError.IsOK()) {
                    return TError(isUserBannedOrError);
                }

                // COMPAT(achulkov2): While keeping the whitelist for compatibility reasons, the ability to ban impersonation from yql_agent
                // seems useful. To be simplified once whitelist is removed.
                if (!isUserBannedOrError.Value() && (isSuperuserOrError.Value() || isWhitelisted)) {
                    authenticationResult.Login = *userHeader;
                    authenticationResult.Realm += ":impersonation";
                    authenticationResult.RealLogin = authenticationResult.Login;
                } else {
                    return TError(
                        NRpc::EErrorCode::InvalidCredentials,
                        "Client has provided %v header but authenticated user %v is not whitelisted, or a superuser (or is banned)",
                        UserNameHeader,
                        authenticationResult.Login)
                        << TErrorAttribute("is_superuser", isSuperuserOrError.Value())
                        << TErrorAttribute("is_banned", isUserBannedOrError.Value())
                        << TErrorAttribute("is_whitelisted", isWhitelisted);
                }
            }

            auto tokenHash = GetCryptoHash(credentials.Token);
            return TAuthenticationResultAndToken{authenticationResult, tokenHash};
        }
    }

    constexpr TStringBuf CookieHeaderName = "Cookie";
    if (auto cookieHeader = request->GetHeaders()->Find(CookieHeaderName)) {
        TCookieCredentials credentials{
            .Cookies = ParseCookies(*cookieHeader),
            .UserIP = userIP,
        };
        if (CookieAuthenticator_->CanAuthenticate(credentials)) {
            auto authResult = WaitFor(CookieAuthenticator_->Authenticate(credentials));
            if (!authResult.IsOK()) {
                return TError(authResult);
            }

            if (request->GetMethod() != EMethod::Get && !disableCsrfTokenCheck) {
                constexpr TStringBuf CrfTokenHeaderName = "X-Csrf-Token";
                auto csrfTokenHeader = request->GetHeaders()->Find(CrfTokenHeaderName);
                if (!csrfTokenHeader) {
                    return TError(
                        NRpc::EErrorCode::InvalidCredentials,
                        "CSRF token is missing");
                }

                auto error = CheckCsrfToken(
                    Strip(*csrfTokenHeader),
                    authResult.Value().Login,
                    Config_->GetCsrfSecret(),
                    Config_->GetCsrfTokenExpirationTime());

                auto dynamicConfig = Bootstrap_->GetDynamicConfig();
                if (!error.IsOK() && !dynamicConfig->RelaxCsrfCheck) {
                    return error;
                }
            }

            return TAuthenticationResultAndToken{authResult.Value(), TString()};
        }
    }

    if (auto userTicketHeader = request->GetHeaders()->Find(NHeaders::UserTicketHeaderName)) {
        const auto& ticketAuthenticator = AuthenticationManager_->GetTicketAuthenticator();
        if (!ticketAuthenticator) {
            return TError(
                NRpc::EErrorCode::InvalidCredentials,
                "Client has provided a user ticket, but no ticket authenticator is configured");
        }

        TTicketCredentials credentials{
            .Ticket = *userTicketHeader,
        };
        auto authResult = WaitFor(ticketAuthenticator->Authenticate(credentials));
        if (!authResult.IsOK()) {
            return TError(authResult);
        }

        return TAuthenticationResultAndToken{authResult.Value(), {}};
    }

    if (auto serviceTicketHeader = request->GetHeaders()->Find(NHeaders::ServiceTicketHeaderName)) {
        const auto& ticketAuthenticator = AuthenticationManager_->GetTicketAuthenticator();
        if (!ticketAuthenticator) {
            return TError(
                NRpc::EErrorCode::InvalidCredentials,
                "Client has provided a service ticket, but no ticket authenticator is configured");
        }

        TServiceTicketCredentials credentials{
            .Ticket = *serviceTicketHeader,
        };
        auto authResult = WaitFor(ticketAuthenticator->Authenticate(credentials));
        if (!authResult.IsOK()) {
            return TError(authResult);
        }

        return TAuthenticationResultAndToken{authResult.Value(), {}};
    }

    return TError(
        NRpc::EErrorCode::InvalidCredentials,
        "Client is missing credentials");
}

const ITokenAuthenticatorPtr& THttpAuthenticator::GetTokenAuthenticator() const
{
    return TokenAuthenticator_;
}

////////////////////////////////////////////////////////////////////////////////

TCompositeHttpAuthenticator::TCompositeHttpAuthenticator(const THashMap<int, THttpAuthenticatorPtr>& portAuthenticators)
    : PortAuthenticators_(std::move(portAuthenticators))
{ }

void TCompositeHttpAuthenticator::HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp)
{
    auto result = GetPortAuthenticator(req->GetPort());
    if (result.IsOK()) {
        result.Value()->HandleRequest(req, rsp);
        return;
    }

    SetStatusFromAuthError(rsp, TError(result));
    ReplyJson(rsp, [&] (auto consumer) {
        BuildYsonFluently(consumer)
            .Value(TError(result));
    });
}

TErrorOr<TAuthenticationResultAndToken> TCompositeHttpAuthenticator::Authenticate(
    const IRequestPtr& request,
    bool disableCsrfTokenCheck)
{
    auto result = GetPortAuthenticator(request->GetPort());
    if (!result.IsOK()) {
        return TError(result);
    }

    return result.Value()->Authenticate(request, disableCsrfTokenCheck);
}

const ITokenAuthenticatorPtr& TCompositeHttpAuthenticator::GetTokenAuthenticatorOrThrow(int port) const
{
    auto result = GetPortAuthenticator(port);
    if (!result.IsOK()) {
        THROW_ERROR_EXCEPTION(TError(result));
    }

    return result.Value()->GetTokenAuthenticator();
}

TErrorOr<THttpAuthenticatorPtr> TCompositeHttpAuthenticator::GetPortAuthenticator(int port) const {
    auto it = PortAuthenticators_.find(port);
    if (it != PortAuthenticators_.end() && it->second) {
        return it->second;
    }

    return TError(
        NRpc::EErrorCode::InvalidCredentials,
        "No authenticator configured for port %v",
        port);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
