PROTO_LIBRARY()

ONLY_TAGS(GO_PROTO)

PEERDIR(
    yt/go/proto/core/misc
)

PROTO_ADDINCL(
    GLOBAL
    yt
)

SRCS(${ARCADIA_ROOT}/yt/yt_proto/yt/core/bus/proto/bus.proto)

END()
