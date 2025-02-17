#include "object_attribute_cache.h"

#include <yt/yt/ytlib/cypress_client/batch_attribute_fetcher.h>

namespace NYT::NObjectClient {

using namespace NApi;
using namespace NYTree;
using namespace NYPath;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

TObjectAttributeCache::TObjectAttributeCache(
    TObjectAttributeCacheConfigPtr config,
    std::vector<std::string> attributeNames,
    NNative::IClientPtr client,
    IInvokerPtr invoker,
    const NLogging::TLogger& logger,
    NProfiling::TProfiler profiler)
    : TObjectAttributeCacheBase(
        std::move(config),
        std::move(client),
        std::move(invoker),
        logger,
        std::move(profiler))
    , AttributeNames_(std::move(attributeNames))
{ }

NYPath::TYPath TObjectAttributeCache::GetPath(const NYPath::TYPath& key) const
{
    return key;
}

NYTree::IAttributeDictionaryPtr TObjectAttributeCache::ParseValue(const NYTree::IAttributeDictionaryPtr& attributes) const
{
    return attributes;
}

const std::vector<std::string>& TObjectAttributeCache::GetAttributeNames() const
{
    return AttributeNames_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient
