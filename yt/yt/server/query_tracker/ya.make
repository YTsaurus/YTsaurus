LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    bootstrap.cpp
    chyt_engine.cpp
    config.cpp
    dynamic_config_manager.cpp
    handler_base.cpp
    profiler.cpp
    proxy_service.cpp
    query_tracker.cpp
    query_tracker_proxy.cpp
    mock_engine.cpp
    program.cpp
    ql_engine.cpp
    spyt_discovery.cpp
    spyt_engine.cpp
    yql_engine.cpp
)

PEERDIR(
    library/cpp/yt/phdr_cache

    library/cpp/getopt

    yt/chyt/client

    yt/yt/library/dynamic_config
    yt/yt/library/clickhouse_discovery
    yt/yt/library/coredumper
    yt/yt/library/server_program

    yt/yt/server/lib/admin
    yt/yt/server/lib/alert_manager
    yt/yt/server/lib/cypress_election
    yt/yt/server/lib/cypress_registrar
    yt/yt/server/lib/misc
    yt/yt/server/lib/state_checker

    yt/yt/library/query/engine
    yt/yt/library/query/row_comparer

    yt/yt/ytlib
    yt/yt/ytlib/query_tracker_client

    yt/yt/client
)

END()

RECURSE_FOR_TESTS(
    unittests
)
