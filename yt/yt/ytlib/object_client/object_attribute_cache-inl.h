#ifndef OBJECT_ATTRIBUTE_CACHE_INL_H_
#error "Direct inclusion of this file is not allowed, include object_attribute_cache.h"
// For the sake of sane code completion.
#include "object_attribute_cache.h"
#endif

#include "public.h"
#include "config.h"

#include <yt/yt/ytlib/cypress_client/batch_attribute_fetcher.h>

namespace NYT::NObjectClient {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
TObjectAttributeCacheBase<TKey, TValue>::TObjectAttributeCacheBase(
    TObjectAttributeCacheConfigPtr config,
    NApi::NNative::IClientPtr client,
    IInvokerPtr invoker,
    const NLogging::TLogger& logger,
    NProfiling::TProfiler profiler)
    : TAsyncExpiringCache<TKey, TValue>(
        config,
        ObjectClientLogger().WithTag("Cache: ObjectAttribute"),
        std::move(profiler))
    , Config_(std::move(config))
    , Logger(logger.WithTag("ObjectAttributeCacheId: %v", TGuid::Create()))
    , Client_(std::move(client))
    , Invoker_(std::move(invoker))
{ }

template <class TKey, class TValue>
TFuture<TValue> TObjectAttributeCacheBase<TKey, TValue>::DoGet(
    const TKey& key,
    bool isPeriodicUpdate) noexcept
{
    return DoGetMany({key}, isPeriodicUpdate)
        .Apply(BIND([] (const std::vector<TErrorOr<TValue>>& response) {
            return response[0].ValueOrThrow();
        }));
}

template <class TKey, class TValue>
TFuture<std::vector<TErrorOr<TValue>>> TObjectAttributeCacheBase<TKey, TValue>::DoGetMany(
    const std::vector<TKey>& keys,
    bool /*isPeriodicUpdate*/) noexcept
{
    YT_LOG_DEBUG("Updating object attribute cache (ObjectCount: %v)", keys.size());

    return GetFromClient(keys, Client_);
}

template <class TKey, class TValue>
TFuture<std::vector<TErrorOr<TValue>>> TObjectAttributeCacheBase<TKey, TValue>::GetFromClient(
    const std::vector<TKey>& keys,
    const NApi::NNative::IClientPtr& client) noexcept
{
    YT_LOG_DEBUG("Fetching attributes via object attribute cache (ObjectCount: %v)", keys.size());

    std::vector<NYPath::TYPath> paths;
    paths.reserve(keys.size());
    for (const auto& key : keys) {
        paths.push_back(GetPath(key));
    }

    auto fetcher = New<NCypressClient::TBatchAttributeFetcher>(paths, GetAttributeNames(), client, Invoker_, Logger, *Config_->MasterReadOptions);

    return fetcher->Fetch().Apply(BIND([this, this_ = MakeStrong(this), fetcher = std::move(fetcher)] {
        std::vector<TErrorOr<TValue>> result;
        result.reserve(fetcher->Attributes().size());
        for (const TErrorOr<NYTree::IAttributeDictionaryPtr>& objectAttributesOrError : fetcher->Attributes()) {
            if (objectAttributesOrError.IsOK()) {
                result.push_back(ParseValue(objectAttributesOrError.Value()));
            } else {
                result.push_back(TError(objectAttributesOrError));
            }
        }
        return result;
    }));
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
    requires std::derived_from<typename TValue::TUnderlying, NYTree::TYsonStruct>
TObjectAttributeAsYsonStructCacheBase<TKey, TValue>::TObjectAttributeAsYsonStructCacheBase(
    TObjectAttributeCacheConfigPtr config,
    NApi::NNative::IClientPtr client,
    IInvokerPtr invoker,
    const NLogging::TLogger& logger,
    NProfiling::TProfiler profiler)
    : TObjectAttributeCacheBase<TKey, TValue>(std::move(config), std::move(client), std::move(invoker), logger, std::move(profiler))
{
    const auto& attributeNameSet = New<typename TValue::TUnderlying>()->GetRegisteredKeys();
    AttributeNames_ = std::vector<std::string>(attributeNameSet.begin(), attributeNameSet.end());
}

template <class TKey, class TValue>
    requires std::derived_from<typename TValue::TUnderlying, NYTree::TYsonStruct>
TValue TObjectAttributeAsYsonStructCacheBase<TKey, TValue>::ParseValue(const NYTree::IAttributeDictionaryPtr& attributes) const
{
    return ConvertTo<TValue>(attributes);
}

template <class TKey, class TValue>
    requires std::derived_from<typename TValue::TUnderlying, NYTree::TYsonStruct>
const std::vector<std::string>& TObjectAttributeAsYsonStructCacheBase<TKey, TValue>::GetAttributeNames() const
{
    return AttributeNames_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient
