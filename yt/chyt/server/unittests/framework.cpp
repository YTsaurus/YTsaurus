#include <library/cpp/testing/hook/hook.h>

#include <yt/chyt/server/clickhouse_singletons.h>
#include <yt/chyt/server/custom_data_types.h>

Y_TEST_HOOK_BEFORE_RUN(GTEST_CHYT_SETUP)
{
    NYT::NClickHouseServer::RegisterClickHouseSingletons();
    NYT::NClickHouseServer::RegisterDataTypeBoolean();
    NYT::NClickHouseServer::RegisterDataTypeTimestamp();
}
