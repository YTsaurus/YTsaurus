PROGRAM(benchmark_bus)

ALLOCATOR(TCMALLOC)

PROTO_NAMESPACE(yt)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/core
    library/cpp/yt/phdr_cache
    library/cpp/getopt/small
)

END()
