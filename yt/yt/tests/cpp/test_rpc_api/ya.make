GTEST(cpp-integration-test-rpc-api)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    test_distributed_table_signatures.cpp
    test_rpc_api.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/tests/cpp/common_tests.make.inc)

EXPLICIT_DATA()
DATA(arcadia/yt/yt/tests/cpp/test_rpc_api/config.yson)

IF(NOT OPENSOURCE)
    INCLUDE(ya_non_opensource.inc)
ENDIF()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/tests/cpp/test_base
    yt/yt/ytlib
    yt/yt/core/test_framework
    yt/yt/library/named_value
    contrib/libs/apache/arrow
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/tests/recipe/recipe.inc)

SIZE(MEDIUM)

REQUIREMENTS(ram:20)

END()
