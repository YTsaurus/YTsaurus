GTEST(unittester-python)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    dynamic_ring_buffer_ut.cpp
    stream_ut.cpp
    tee_input_stream_ut.cpp
)

USE_PYTHON3()

# This module should not be exported under CMake since it requires Python build
NO_BUILD_IF(STRICT EXPORT_CMAKE)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/build
    yt/yt/core/test_framework
    yt/yt/core
    yt/yt/python/common
)

END()
