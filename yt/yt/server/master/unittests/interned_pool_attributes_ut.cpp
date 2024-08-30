#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/master/scheduler_pool_server/scheduler_pool.h>

#include <yt/yt/core/ytree/interned_attributes.h>

namespace NYT::NSchedulerPoolServer {
namespace {

using namespace ::testing;
using namespace NScheduler;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TEST(TInternedPoolAttributesTest, AllPoolFieldsAreInternedAttributes)
{
    auto config = New<TPoolConfig>();
    for (const auto& poolField : config->GetRegisteredKeys()) {
        auto attributeKey = TInternedAttributeKey::Lookup(poolField);
        if (attributeKey == InvalidInternedAttribute) {
            EXPECT_TRUE(false) << Format("Pool config attribute %Qv is not interned in master!", poolField);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NSchedulerPoolServer
