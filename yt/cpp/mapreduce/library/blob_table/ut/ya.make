UNITTEST_WITH_CUSTOM_ENTRY_POINT()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    blob_table_ut.cpp
)

PEERDIR(
    library/cpp/testing/gtest

    yt/cpp/mapreduce/library/blob_table
    yt/cpp/mapreduce/tests/yt_unittest_lib
    yt/cpp/mapreduce/tests/gtest_main
)

SIZE(MEDIUM)

IF (NOT OPENSOURCE)
    INCLUDE(${ARCADIA_ROOT}/yt/recipe/basic/recipe.inc)
ENDIF()

END()
