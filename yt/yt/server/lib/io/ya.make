LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    config.cpp
    gentle_loader.cpp
    io_engine.cpp
    io_request_slicer.cpp
    io_tracker.cpp
    io_workload_model.cpp
    chunk_fragment.cpp
    chunk_file_reader.cpp
    chunk_file_reader_adapter.cpp
    chunk_file_writer.cpp
    helpers.cpp
    read_request_combiner.cpp
    dynamic_io_engine.cpp
    io_engine_base.cpp
    io_engine_fair.cpp
    io_engine_uring.cpp
)

PEERDIR(
    yt/yt/core/test_framework
    yt/yt/library/profiling
    yt/yt/ytlib
)

IF(OS_LINUX)
    PEERDIR(
        contrib/libs/liburing
    )
ENDIF()

END()

RECURSE_FOR_TESTS(
    unittests
)
