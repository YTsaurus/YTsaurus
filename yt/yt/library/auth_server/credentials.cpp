#include "credentials.h"

#include <library/cpp/yt/misc/hash.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

std::string GetRealLogin(const TAuthenticationResult& result)
{
    return result.RealLogin.value_or(result.Login);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth

using namespace NYT;
using namespace NYT::NAuth;

size_t THash<TTokenCredentials>::operator()(const NYT::NAuth::TTokenCredentials& credentials) const
{
    size_t result = 0;

    HashCombine(result, credentials.Token);
    HashCombine(result, credentials.UserIP);

    return result;
}

size_t THash<TCookieCredentials>::operator()(const NYT::NAuth::TCookieCredentials& credentials) const
{
    size_t result = 0;

    std::vector<std::pair<TString, TString>> cookies(
        credentials.Cookies.begin(),
        credentials.Cookies.end());
    std::sort(cookies.begin(), cookies.end());
    for (const auto& cookie : cookies) {
        HashCombine(result, cookie.first);
        HashCombine(result, cookie.second);
    }

    HashCombine(result, credentials.UserIP);

    return result;
}

////////////////////////////////////////////////////////////////////////////////
