GTEST(unittester-library-error-skeleton)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    skeleton_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/library/error_skeleton
    yt/yt/core/test_framework
)

END()
