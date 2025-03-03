LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PEERDIR(
    yt/yt/core
    yt/yt/core/rpc/grpc
    yt/yt/contrib/cri-api
    yt/yt/library/re2
)

SRCS(
    cri_api.cpp
    cri_executor.cpp
    config.cpp
    image_cache.cpp
)

ADDINCL(
    ONE_LEVEL
    yt/yt/contrib/cri-api
)

END()
