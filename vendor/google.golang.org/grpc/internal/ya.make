GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    experimental.go
    internal.go
    xds_handshake_cluster.go
)

IF (OS_LINUX)
    SRCS(
        tcp_keepalive_unix.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        tcp_keepalive_unix.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        tcp_keepalive_windows.go
    )
ENDIF()

END()

RECURSE(
    admin
    backoff
    balancer
    balancergroup
    balancerload
    binarylog
    buffer
    cache
    channelz
    credentials
    envconfig
    googlecloud
    grpclog
    grpcrand
    grpcsync
    grpctest
    grpcutil
    hierarchy
    idle
    leakcheck
    metadata
    pretty
    profiling
    proto
    resolver
    serviceconfig
    status
    stubserver
    testutils
    transport
    wrr
    xds
    # yo
)

IF (OS_LINUX)
    RECURSE(
        syscall
    )
ENDIF()

IF (OS_DARWIN)
    RECURSE(
        syscall
    )
ENDIF()

IF (OS_WINDOWS)
    RECURSE(
        syscall
    )
ENDIF()
