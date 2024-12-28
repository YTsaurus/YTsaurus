#!/usr/bin/python3

from yt.wrapper import YtClient, config

from yt.environment.migrationlib import TableInfo, Migration, Conversion

import yt.yson as yson

import argparse
import logging

DEFAULT_BUNDLE_NAME = "default"
SYS_BUNDLE_NAME = "sys"
DEFAULT_STATE_PATH = "//sys/query_tracker"
DEFAULT_SHARD_COUNT = 1

INITIAL_TABLE_INFOS = {
    "active_queries": TableInfo(
        [
            ("query_id", "string"),
        ],
        [
            ("engine", "string", {"lock": "client"}),
            ("query", "string", {"lock": "client"}),
            ("settings", "any", {"lock": "client"}),
            ("user", "string", {"lock": "client"}),
            ("start_time", "timestamp", {"lock": "client"}),
            ("filter_factors", "string", {"lock": "client"}),
            ("state", "string", {"lock": "common"}),
            ("incarnation", "int64", {"lock": "query_tracker"}),
            ("ping_time", "timestamp", {"lock": "query_tracker"}),
            ("assigned_tracker", "string", {"lock": "query_tracker"}),
            ("progress", "any", {"lock": "query_tracker"}),
            ("error", "any", {"lock": "query_tracker"}),
            ("result_count", "int64", {"lock": "query_tracker"}),
            ("finish_time", "timestamp", {"lock": "common"}),
            ("abort_request", "any", {"lock": "client"}),
            ("annotations", "any", {"lock": "client"}),
        ],
        optimize_for="lookup",
        attributes={
            "tablet_cell_bundle": DEFAULT_BUNDLE_NAME,
        },
    ),
    "finished_queries": TableInfo(
        [
            ("query_id", "string"),
        ],
        [
            ("engine", "string"),
            ("query", "string"),
            ("settings", "any"),
            ("user", "string"),
            ("start_time", "timestamp"),
            ("state", "string"),
            ("progress", "any"),
            ("error", "any"),
            ("result_count", "int64"),
            ("finish_time", "timestamp"),
            ("annotations", "any"),
        ],
        optimize_for="lookup",
        attributes={
            "tablet_cell_bundle": DEFAULT_BUNDLE_NAME,
        },
    ),
    "finished_queries_by_start_time": TableInfo(
        [("start_time", "timestamp"), ("query_id", "string")],
        [
            ("engine", "string"),
            ("user", "string"),
            ("state", "string"),
            ("filter_factors", "string"),
        ],
        optimize_for="lookup",
        attributes={
            "tablet_cell_bundle": DEFAULT_BUNDLE_NAME,
        },
    ),
    "finished_query_results": TableInfo(
        [
            ("query_id", "string"),
            ("result_index", "int64"),
        ],
        [
            ("error", "any"),
            ("schema", "any"),
            ("data_statistics", "any"),
            ("rowset", "string"),
        ],
        optimize_for="lookup",
        attributes={
            "tablet_cell_bundle": DEFAULT_BUNDLE_NAME,
        },
    ),
}

INITIAL_VERSION = 0
TRANSFORMS = {}
ACTIONS = {}

TRANSFORMS[1] = [
    Conversion(
        "active_queries",
        table_info=TableInfo(
            [
                ("query_id", "string"),
            ],
            [
                ("engine", "string", {"lock": "client"}),
                ("query", "string", {"lock": "client"}),
                ("settings", "any", {"lock": "client"}),
                ("user", "string", {"lock": "client"}),
                ("start_time", "timestamp", {"lock": "client"}),
                ("filter_factors", "string", {"lock": "client"}),
                ("state", "string", {"lock": "common"}),
                ("incarnation", "int64", {"lock": "query_tracker"}),
                ("ping_time", "timestamp", {"lock": "query_tracker"}),
                ("assigned_tracker", "string", {"lock": "query_tracker"}),
                ("progress", "any", {"lock": "query_tracker_progress"}),
                ("error", "any", {"lock": "query_tracker"}),
                ("result_count", "int64", {"lock": "query_tracker"}),
                ("finish_time", "timestamp", {"lock": "common"}),
                ("abort_request", "any", {"lock": "client"}),
                ("annotations", "any", {"lock": "client"}),
            ],
            optimize_for="lookup",
            attributes={
                "tablet_cell_bundle": DEFAULT_BUNDLE_NAME,
            },
        )
    )
]

TRANSFORMS[2] = [
    Conversion(
        "active_queries",
        table_info=TableInfo(
            [
                ("query_id", "string"),
            ],
            [
                ("engine", "string", {"lock": "client"}),
                ("query", "string", {"lock": "client"}),
                ("files", "any", {"lock": "client"}),
                ("settings", "any", {"lock": "client"}),
                ("user", "string", {"lock": "client"}),
                ("start_time", "timestamp", {"lock": "client"}),
                ("filter_factors", "string", {"lock": "client"}),
                ("state", "string", {"lock": "common"}),
                ("incarnation", "int64", {"lock": "query_tracker"}),
                ("ping_time", "timestamp", {"lock": "query_tracker"}),
                ("assigned_tracker", "string", {"lock": "query_tracker"}),
                ("progress", "any", {"lock": "query_tracker_progress"}),
                ("error", "any", {"lock": "query_tracker"}),
                ("result_count", "int64", {"lock": "query_tracker"}),
                ("finish_time", "timestamp", {"lock": "common"}),
                ("abort_request", "any", {"lock": "client"}),
                ("annotations", "any", {"lock": "client"}),
            ],
            optimize_for="lookup",
            attributes={
                "tablet_cell_bundle": DEFAULT_BUNDLE_NAME,
            },
        )
    ),
    Conversion(
        "finished_queries",
        table_info=TableInfo(
            [
                ("query_id", "string"),
            ],
            [
                ("engine", "string"),
                ("query", "string"),
                ("files", "any"),
                ("settings", "any"),
                ("user", "string"),
                ("start_time", "timestamp"),
                ("state", "string"),
                ("progress", "any"),
                ("error", "any"),
                ("result_count", "int64"),
                ("finish_time", "timestamp"),
                ("annotations", "any"),
            ],
            optimize_for="lookup",
            attributes={
                "tablet_cell_bundle": DEFAULT_BUNDLE_NAME,
            },
        ),
    )
]

TRANSFORMS[3] = [
    Conversion(
        "active_queries",
        table_info=TableInfo(
            [
                ("query_id", "string"),
            ],
            [
                ("engine", "string", {"lock": "client"}),
                ("query", "string", {"lock": "client"}),
                ("files", "any", {"lock": "client"}),
                ("settings", "any", {"lock": "client"}),
                ("user", "string", {"lock": "client"}),
                ("start_time", "timestamp", {"lock": "client"}),
                ("filter_factors", "string", {"lock": "client"}),
                ("state", "string", {"lock": "common"}),
                ("incarnation", "int64", {"lock": "query_tracker"}),
                ("ping_time", "timestamp", {"lock": "query_tracker"}),
                ("assigned_tracker", "string", {"lock": "query_tracker"}),
                ("progress", "any", {"lock": "query_tracker_progress"}),
                ("error", "any", {"lock": "query_tracker"}),
                ("result_count", "int64", {"lock": "query_tracker"}),
                ("finish_time", "timestamp", {"lock": "common"}),
                ("abort_request", "any", {"lock": "client"}),
                ("annotations", "any", {"lock": "client"}),
            ],
            optimize_for="lookup",
            attributes={
                "tablet_cell_bundle": SYS_BUNDLE_NAME,
            },
        )
    ),
    Conversion(
        "finished_queries",
        table_info=TableInfo(
            [
                ("query_id", "string"),
            ],
            [
                ("engine", "string"),
                ("query", "string"),
                ("files", "any"),
                ("settings", "any"),
                ("user", "string"),
                ("start_time", "timestamp"),
                ("state", "string"),
                ("progress", "any"),
                ("error", "any"),
                ("result_count", "int64"),
                ("finish_time", "timestamp"),
                ("annotations", "any"),
            ],
            optimize_for="lookup",
            attributes={
                "tablet_cell_bundle": SYS_BUNDLE_NAME,
            },
        ),
    ),
    Conversion(
        "finished_queries_by_start_time",
        table_info=TableInfo(
            [
                ("start_time", "timestamp"),
                ("query_id", "string")
            ],
            [
                ("engine", "string"),
                ("user", "string"),
                ("state", "string"),
                ("filter_factors", "string"),
            ],
            optimize_for="lookup",
            attributes={
                "tablet_cell_bundle": SYS_BUNDLE_NAME,
            },
        ),
    ),
    Conversion(
        "finished_query_results",
        table_info=TableInfo(
            [
                ("query_id", "string"),
                ("result_index", "int64"),
            ],
            [
                ("error", "any"),
                ("schema", "any"),
                ("data_statistics", "any"),
                ("rowset", "string"),
            ],
            optimize_for="lookup",
            attributes={
                "tablet_cell_bundle": SYS_BUNDLE_NAME,
            },
        )
    ),
]

TRANSFORMS[4] = [
    Conversion(
        "finished_query_results",
        table_info=TableInfo(
            [
                ("query_id", "string"),
                ("result_index", "int64"),
            ],
            [
                ("error", "any"),
                ("schema", "any"),
                ("data_statistics", "any"),
                ("rowset", "string"),
                ("is_truncated", "boolean"),
            ],
            optimize_for="lookup",
            attributes={
                "tablet_cell_bundle": SYS_BUNDLE_NAME,
            },
        )
    ),
]

TRANSFORMS[5] = [
    Conversion(
        "active_queries",
        table_info=TableInfo(
            [
                ("query_id", "string"),
            ],
            [
                ("engine", "string", {"lock": "client"}),
                ("query", "string", {"lock": "client"}),
                ("files", "any", {"lock": "client"}),
                ("settings", "any", {"lock": "client"}),
                ("user", "string", {"lock": "client"}),
                ("access_control_object", "string", {"lock": "client"}),
                ("start_time", "timestamp", {"lock": "client"}),
                ("filter_factors", "string", {"lock": "client"}),
                ("state", "string", {"lock": "common"}),
                ("incarnation", "int64", {"lock": "query_tracker"}),
                ("ping_time", "timestamp", {"lock": "query_tracker"}),
                ("assigned_tracker", "string", {"lock": "query_tracker"}),
                ("progress", "any", {"lock": "query_tracker_progress"}),
                ("error", "any", {"lock": "query_tracker"}),
                ("result_count", "int64", {"lock": "query_tracker"}),
                ("finish_time", "timestamp", {"lock": "common"}),
                ("abort_request", "any", {"lock": "client"}),
                ("annotations", "any", {"lock": "client"}),
            ],
            optimize_for="lookup",
            attributes={
                "tablet_cell_bundle": SYS_BUNDLE_NAME,
            },
        )
    ),
    Conversion(
        "finished_queries",
        table_info=TableInfo(
            [
                ("query_id", "string"),
            ],
            [
                ("engine", "string"),
                ("query", "string"),
                ("files", "any"),
                ("settings", "any"),
                ("user", "string"),
                ("access_control_object", "string"),
                ("start_time", "timestamp"),
                ("state", "string"),
                ("progress", "any"),
                ("error", "any"),
                ("result_count", "int64"),
                ("finish_time", "timestamp"),
                ("annotations", "any"),
            ],
            optimize_for="lookup",
            attributes={
                "tablet_cell_bundle": SYS_BUNDLE_NAME,
            },
        ),
    ),
    Conversion(
        "finished_queries_by_start_time",
        table_info=TableInfo(
            [
                ("start_time", "timestamp"),
                ("query_id", "string")
            ],
            [
                ("engine", "string"),
                ("user", "string"),
                ("access_control_object", "string"),
                ("state", "string"),
                ("filter_factors", "string"),
            ],
            optimize_for="lookup",
            attributes={
                "tablet_cell_bundle": SYS_BUNDLE_NAME,
            },
        ),
    ),
]

TRANSFORMS[6] = [
    Conversion(
        "active_queries",
        table_info=TableInfo(
            [
                ("query_id", "string"),
            ],
            [
                ("engine", "string", {"lock": "client"}),
                ("query", "string", {"lock": "client"}),
                ("files", "any", {"lock": "client"}),
                ("settings", "any", {"lock": "client"}),
                ("user", "string", {"lock": "client"}),
                ("access_control_object", "string", {"lock": "client"}),
                ("start_time", "timestamp", {"lock": "client"}),
                ("filter_factors", "string", {"lock": "client"}),
                ("state", "string", {"lock": "common"}),
                ("incarnation", "int64", {"lock": "query_tracker"}),
                ("lease_transaction_id", "string", {"lock": "query_tracker"}),
                ("assigned_tracker", "string", {"lock": "query_tracker"}),
                ("progress", "any", {"lock": "query_tracker_progress"}),
                ("error", "any", {"lock": "query_tracker"}),
                ("result_count", "int64", {"lock": "query_tracker"}),
                ("finish_time", "timestamp", {"lock": "common"}),
                ("abort_request", "any", {"lock": "client"}),
                ("annotations", "any", {"lock": "client"}),
            ],
            optimize_for="lookup",
            attributes={
                "tablet_cell_bundle": SYS_BUNDLE_NAME,
            },
        ),
        use_default_mapper=True,
    ),
]

TRANSFORMS[7] = [
    Conversion(
        "active_queries",
        table_info=TableInfo(
            [
                ("query_id", "string"),
            ],
            [
                ("engine", "string", {"lock": "client"}),
                ("query", "string", {"lock": "client"}),
                ("files", "any", {"lock": "client"}),
                ("settings", "any", {"lock": "client"}),
                ("user", "string", {"lock": "client"}),
                ("access_control_object", "string", {"lock": "client"}),
                ("start_time", "timestamp", {"lock": "client"}),
                ("filter_factors", "string", {"lock": "client"}),
                ("state", "string", {"lock": "common"}),
                ("incarnation", "int64", {"lock": "query_tracker"}),
                ("ping_time", "timestamp", {"lock": "query_tracker"}),
                ("lease_transaction_id", "string", {"lock": "query_tracker"}),
                ("assigned_tracker", "string", {"lock": "query_tracker"}),
                ("progress", "any", {"lock": "query_tracker_progress"}),
                ("error", "any", {"lock": "query_tracker"}),
                ("result_count", "int64", {"lock": "query_tracker"}),
                ("finish_time", "timestamp", {"lock": "common"}),
                ("abort_request", "any", {"lock": "client"}),
                ("annotations", "any", {"lock": "client"}),
            ],
            optimize_for="lookup",
            attributes={
                "tablet_cell_bundle": SYS_BUNDLE_NAME,
            },
        ),
        use_default_mapper=True,
    ),
]


def get_access_control_objects_mapper(table):
    column_names = table.user_columns

    def access_control_objects_mapper(row):
        result = dict([(key, row.get(key)) for key in column_names])

        aco = row.get("access_control_object")
        if aco is None:
            result["access_control_objects"] = yson.YsonList([])
        else:
            result["access_control_objects"] = yson.YsonList([aco])

        yield result

    return access_control_objects_mapper


ACTIVE_QUERIES_TABLE_V8 = TableInfo(
    [
        ("query_id", "string"),
    ],
    [
        ("engine", "string", {"lock": "client"}),
        ("query", "string", {"lock": "client"}),
        ("files", "any", {"lock": "client"}),
        ("settings", "any", {"lock": "client"}),
        ("user", "string", {"lock": "client"}),
        ("access_control_objects", "any", {"lock": "client"}),
        ("start_time", "timestamp", {"lock": "client"}),
        ("filter_factors", "string", {"lock": "client"}),
        ("state", "string", {"lock": "common"}),
        ("incarnation", "int64", {"lock": "query_tracker"}),
        ("ping_time", "timestamp", {"lock": "query_tracker"}),
        ("lease_transaction_id", "string", {"lock": "query_tracker"}),
        ("assigned_tracker", "string", {"lock": "query_tracker"}),
        ("progress", "any", {"lock": "query_tracker_progress"}),
        ("error", "any", {"lock": "query_tracker"}),
        ("result_count", "int64", {"lock": "query_tracker"}),
        ("finish_time", "timestamp", {"lock": "common"}),
        ("abort_request", "any", {"lock": "client"}),
        ("annotations", "any", {"lock": "client"}),
    ],
    optimize_for="lookup",
    attributes={
        "tablet_cell_bundle": SYS_BUNDLE_NAME,
    },
)

FINISHED_QUERIES_TABLE_V8 = TableInfo(
    [
        ("query_id", "string"),
    ],
    [
        ("engine", "string"),
        ("query", "string"),
        ("files", "any"),
        ("settings", "any"),
        ("user", "string"),
        ("access_control_objects", "any"),
        ("start_time", "timestamp"),
        ("state", "string"),
        ("progress", "any"),
        ("error", "any"),
        ("result_count", "int64"),
        ("finish_time", "timestamp"),
        ("annotations", "any"),
    ],
    optimize_for="lookup",
    attributes={
        "tablet_cell_bundle": SYS_BUNDLE_NAME,
    },
)

FINISHED_QUERIES_TABLE_BY_START_TIME_V8 = TableInfo(
    [
        ("start_time", "timestamp"),
        ("query_id", "string")
    ],
    [
        ("engine", "string"),
        ("user", "string"),
        ("access_control_objects", "any"),
        ("state", "string"),
        ("filter_factors", "string"),
    ],
    optimize_for="lookup",
    attributes={
        "tablet_cell_bundle": SYS_BUNDLE_NAME,
    },
)

TRANSFORMS[8] = [
    Conversion(
        "active_queries",
        table_info=ACTIVE_QUERIES_TABLE_V8,
        mapper=get_access_control_objects_mapper(ACTIVE_QUERIES_TABLE_V8),
    ),
    Conversion(
        "finished_queries",
        table_info=FINISHED_QUERIES_TABLE_V8,
        mapper=get_access_control_objects_mapper(FINISHED_QUERIES_TABLE_V8)
    ),
    Conversion(
        "finished_queries_by_start_time",
        table_info=FINISHED_QUERIES_TABLE_BY_START_TIME_V8,
        mapper=get_access_control_objects_mapper(FINISHED_QUERIES_TABLE_BY_START_TIME_V8)
    ),
]


def get_minus_start_time_mapper(table):
    column_names = table.user_columns

    def minus_start_time_mapper(row):
        result = dict([(key, row.get(key)) for key in column_names])
        result["minus_start_time"] = -row.get("start_time")
        yield result

    return minus_start_time_mapper


def finished_queries_table_by_aco_and_start_time_mapper(row):
    column_names = FINISHED_QUERIES_TABLE_BY_ACO_AND_START_TIME_V9.user_columns

    result = dict([(key, row.get(key)) for key in column_names])
    result["minus_start_time"] = -row.get("start_time")

    acos = yson.YsonList(row.get("access_control_objects"))
    for aco in acos:
        if aco != "nobody":
            result["access_control_object"] = aco
            yield result


FINISHED_QUERIES_TABLE_BY_ACO_AND_START_TIME_V9 = TableInfo(
    [
        ("access_control_object", "string"),
        ("minus_start_time", "int64"),
        ("query_id", "string")
    ],
    [
        ("engine", "string"),
        ("user", "string"),
        ("state", "string"),
        ("filter_factors", "string"),
    ],
    optimize_for="lookup",
    attributes={
        "tablet_cell_bundle": SYS_BUNDLE_NAME,
    },
)

FINISHED_QUERIES_TABLE_BY_USER_AND_START_TIME_V9 = TableInfo(
    [
        ("user", "string"),
        ("minus_start_time", "int64"),
        ("query_id", "string")
    ],
    [
        ("engine", "string"),
        ("state", "string"),
        ("filter_factors", "string"),
    ],
    optimize_for="lookup",
    attributes={
        "tablet_cell_bundle": SYS_BUNDLE_NAME,
    },
)

FINISHED_QUERIES_TABLE_BY_START_TIME_V9 = TableInfo(
    [
        ("minus_start_time", "int64"),
        ("query_id", "string")
    ],
    [
        ("engine", "string"),
        ("user", "string"),
        ("access_control_objects", "any"),
        ("state", "string"),
        ("filter_factors", "string"),
    ],
    optimize_for="lookup",
    attributes={
        "tablet_cell_bundle": SYS_BUNDLE_NAME,
    },
)

TRANSFORMS[9] = [
    Conversion(
        "finished_queries_by_aco_and_start_time",
        source="finished_queries_by_start_time",
        table_info=FINISHED_QUERIES_TABLE_BY_ACO_AND_START_TIME_V9,
        mapper=finished_queries_table_by_aco_and_start_time_mapper
    ),
    Conversion(
        "finished_queries_by_user_and_start_time",
        source="finished_queries_by_start_time",
        table_info=FINISHED_QUERIES_TABLE_BY_USER_AND_START_TIME_V9,
        mapper=get_minus_start_time_mapper(FINISHED_QUERIES_TABLE_BY_USER_AND_START_TIME_V9)
    ),
    Conversion(
        "finished_queries_by_start_time",
        source="finished_queries_by_start_time",
        table_info=FINISHED_QUERIES_TABLE_BY_START_TIME_V9,
        mapper=get_minus_start_time_mapper(FINISHED_QUERIES_TABLE_BY_START_TIME_V9)
    ),
]

ACTIVE_QUERIES_V10 = TableInfo(
    [
        ("query_id", "string"),
    ],
    [
        ("engine", "string", {"lock": "client"}),
        ("query", "string", {"lock": "client"}),
        ("files", "any", {"lock": "client"}),
        ("settings", "any", {"lock": "client"}),
        ("user", "string", {"lock": "client"}),
        ("access_control_objects", "any", {"lock": "client"}),
        ("start_time", "timestamp", {"lock": "client"}),
        ("filter_factors", "string", {"lock": "client"}),
        ("state", "string", {"lock": "common"}),
        ("incarnation", "int64", {"lock": "query_tracker"}),
        ("ping_time", "timestamp", {"lock": "query_tracker"}),
        ("lease_transaction_id", "string", {"lock": "query_tracker"}),
        ("assigned_tracker", "string", {"lock": "query_tracker"}),
        ("progress", "any", {"lock": "query_tracker_progress"}),
        ("error", "any", {"lock": "query_tracker"}),
        ("result_count", "int64", {"lock": "query_tracker"}),
        ("finish_time", "timestamp", {"lock": "common"}),
        ("abort_request", "any", {"lock": "client"}),
        ("annotations", "any", {"lock": "client"}),
    ],
    optimize_for="lookup",
    attributes={
        "tablet_cell_bundle": SYS_BUNDLE_NAME,
        "min_data_ttl": 0,
        "merge_rows_on_flush": True,
        "auto_compaction_period": 3600000,
    },
)

FINISHED_QUERIES_V10 = TableInfo(
    [
        ("query_id", "string"),
    ],
    [
        ("engine", "string"),
        ("query", "string"),
        ("files", "any"),
        ("settings", "any"),
        ("user", "string"),
        ("access_control_objects", "any"),
        ("start_time", "timestamp"),
        ("state", "string"),
        ("progress", "any"),
        ("error", "any"),
        ("result_count", "int64"),
        ("finish_time", "timestamp"),
        ("annotations", "any"),
    ],
    optimize_for="lookup",
    attributes={
        "tablet_cell_bundle": SYS_BUNDLE_NAME,
        "min_data_ttl": 0,
        "merge_rows_on_flush": True,
        "auto_compaction_period": 3600000,
    },
)

FINISHED_QUERIES_TABLE_BY_ACO_AND_START_TIME_V10 = TableInfo(
    [
        ("access_control_object", "string"),
        ("minus_start_time", "int64"),
        ("query_id", "string")
    ],
    [
        ("engine", "string"),
        ("user", "string"),
        ("state", "string"),
        ("filter_factors", "string"),
    ],
    optimize_for="lookup",
    attributes={
        "tablet_cell_bundle": SYS_BUNDLE_NAME,
        "min_data_ttl": 0,
        "merge_rows_on_flush": True,
        "auto_compaction_period": 3600000,
    },
)

FINISHED_QUERIES_TABLE_BY_USER_AND_START_TIME_V10 = TableInfo(
    [
        ("user", "string"),
        ("minus_start_time", "int64"),
        ("query_id", "string")
    ],
    [
        ("engine", "string"),
        ("state", "string"),
        ("filter_factors", "string"),
    ],
    optimize_for="lookup",
    attributes={
        "tablet_cell_bundle": SYS_BUNDLE_NAME,
        "min_data_ttl": 0,
        "merge_rows_on_flush": True,
        "auto_compaction_period": 3600000,
    },
)

FINISHED_QUERIES_TABLE_BY_START_TIME_V10 = TableInfo(
    [
        ("minus_start_time", "int64"),
        ("query_id", "string")
    ],
    [
        ("engine", "string"),
        ("user", "string"),
        ("access_control_objects", "any"),
        ("state", "string"),
        ("filter_factors", "string"),
    ],
    optimize_for="lookup",
    attributes={
        "tablet_cell_bundle": SYS_BUNDLE_NAME,
        "min_data_ttl": 0,
        "merge_rows_on_flush": True,
        "auto_compaction_period": 3600000,
    },
)

FINISHED_QUERIES_RESULTS_V10 = TableInfo(
    [
        ("query_id", "string"),
        ("result_index", "int64"),
    ],
    [
        ("error", "any"),
        ("schema", "any"),
        ("data_statistics", "any"),
        ("rowset", "string"),
        ("is_truncated", "boolean"),
    ],
    optimize_for="lookup",
    attributes={
        "tablet_cell_bundle": SYS_BUNDLE_NAME,
        "min_data_ttl": 0,
        "merge_rows_on_flush": True,
        "auto_compaction_period": 3600000,
    },
)

TRANSFORMS[10] = [
    Conversion(
        "finished_queries_by_aco_and_start_time",
        table_info=FINISHED_QUERIES_TABLE_BY_ACO_AND_START_TIME_V10,
    ),
    Conversion(
        "finished_queries_by_user_and_start_time",
        table_info=FINISHED_QUERIES_TABLE_BY_USER_AND_START_TIME_V10,
    ),
    Conversion(
        "finished_queries_by_start_time",
        table_info=FINISHED_QUERIES_TABLE_BY_START_TIME_V10,
    ),
    Conversion(
        "finished_queries",
        table_info=FINISHED_QUERIES_V10,
    ),
    Conversion(
        "finished_queries_results",
        table_info=FINISHED_QUERIES_RESULTS_V10,
    ),
    Conversion(
        "active_queries",
        table_info=ACTIVE_QUERIES_V10,
    ),]

ACTIVE_QUERIES_V11 = TableInfo(
    [
        ("query_id", "string"),
    ],
    [
        ("engine", "string", {"lock": "client"}),
        ("query", "string", {"lock": "client"}),
        ("files", "any", {"lock": "client"}),
        ("settings", "any", {"lock": "client"}),
        ("user", "string", {"lock": "client"}),
        ("access_control_objects", "any", {"lock": "client"}),
        ("start_time", "timestamp", {"lock": "client"}),
        ("filter_factors", "string", {"lock": "client"}),
        ("state", "string", {"lock": "common"}),
        ("incarnation", "int64", {"lock": "query_tracker"}),
        ("ping_time", "timestamp", {"lock": "query_tracker"}),
        ("lease_transaction_id", "string", {"lock": "query_tracker"}),
        ("assigned_tracker", "string", {"lock": "query_tracker"}),
        ("progress", "any", {"lock": "query_tracker_progress"}),
        ("error", "any", {"lock": "query_tracker"}),
        ("result_count", "int64", {"lock": "query_tracker"}),
        ("finish_time", "timestamp", {"lock": "common"}),
        ("abort_request", "any", {"lock": "client"}),
        ("annotations", "any", {"lock": "client"}),
    ],
    optimize_for="lookup",
    attributes={
        "tablet_cell_bundle": SYS_BUNDLE_NAME,
        "min_data_ttl": 60000,
        "merge_rows_on_flush": True,
        "auto_compaction_period": 3600000,
    },
)

FINISHED_QUERIES_V11 = TableInfo(
    [
        ("query_id", "string"),
    ],
    [
        ("engine", "string"),
        ("query", "string"),
        ("files", "any"),
        ("settings", "any"),
        ("user", "string"),
        ("access_control_objects", "any"),
        ("start_time", "timestamp"),
        ("state", "string"),
        ("progress", "any"),
        ("error", "any"),
        ("result_count", "int64"),
        ("finish_time", "timestamp"),
        ("annotations", "any"),
    ],
    optimize_for="lookup",
    attributes={
        "tablet_cell_bundle": SYS_BUNDLE_NAME,
        "min_data_ttl": 60000,
        "merge_rows_on_flush": True,
        "auto_compaction_period": 3600000,
    },
)

FINISHED_QUERIES_TABLE_BY_ACO_AND_START_TIME_V11 = TableInfo(
    [
        ("access_control_object", "string"),
        ("minus_start_time", "int64"),
        ("query_id", "string")
    ],
    [
        ("engine", "string"),
        ("user", "string"),
        ("state", "string"),
        ("filter_factors", "string"),
    ],
    optimize_for="lookup",
    attributes={
        "tablet_cell_bundle": SYS_BUNDLE_NAME,
        "min_data_ttl": 60000,
        "merge_rows_on_flush": True,
        "auto_compaction_period": 3600000,
    },
)

FINISHED_QUERIES_TABLE_BY_USER_AND_START_TIME_V11 = TableInfo(
    [
        ("user", "string"),
        ("minus_start_time", "int64"),
        ("query_id", "string")
    ],
    [
        ("engine", "string"),
        ("state", "string"),
        ("filter_factors", "string"),
    ],
    optimize_for="lookup",
    attributes={
        "tablet_cell_bundle": SYS_BUNDLE_NAME,
        "min_data_ttl": 60000,
        "merge_rows_on_flush": True,
        "auto_compaction_period": 3600000,
    },
)

FINISHED_QUERIES_TABLE_BY_START_TIME_V11 = TableInfo(
    [
        ("minus_start_time", "int64"),
        ("query_id", "string")
    ],
    [
        ("engine", "string"),
        ("user", "string"),
        ("access_control_objects", "any"),
        ("state", "string"),
        ("filter_factors", "string"),
    ],
    optimize_for="lookup",
    attributes={
        "tablet_cell_bundle": SYS_BUNDLE_NAME,
        "min_data_ttl": 60000,
        "merge_rows_on_flush": True,
        "auto_compaction_period": 3600000,
    },
)

FINISHED_QUERIES_RESULTS_V11 = TableInfo(
    [
        ("query_id", "string"),
        ("result_index", "int64"),
    ],
    [
        ("error", "any"),
        ("schema", "any"),
        ("data_statistics", "any"),
        ("rowset", "string"),
        ("is_truncated", "boolean"),
    ],
    optimize_for="lookup",
    attributes={
        "tablet_cell_bundle": SYS_BUNDLE_NAME,
        "min_data_ttl": 60000,
        "merge_rows_on_flush": True,
        "auto_compaction_period": 3600000,
    },
)

TRANSFORMS[11] = [
    Conversion(
        "finished_queries_by_aco_and_start_time",
        table_info=FINISHED_QUERIES_TABLE_BY_ACO_AND_START_TIME_V11,
    ),
    Conversion(
        "finished_queries_by_user_and_start_time",
        table_info=FINISHED_QUERIES_TABLE_BY_USER_AND_START_TIME_V11,
    ),
    Conversion(
        "finished_queries_by_start_time",
        table_info=FINISHED_QUERIES_TABLE_BY_START_TIME_V11,
    ),
    Conversion(
        "finished_queries",
        table_info=FINISHED_QUERIES_V11,
    ),
    Conversion(
        "finished_queries_results",
        table_info=FINISHED_QUERIES_RESULTS_V11,
    ),
    Conversion(
        "active_queries",
        table_info=ACTIVE_QUERIES_V11,
    ),]

ACTIVE_QUERIES_TABLE_V12 = TableInfo(
    [
        ("query_id", "string"),
    ],
    [
        ("engine", "string", {"lock": "client"}),
        ("query", "string", {"lock": "client"}),
        ("files", "any", {"lock": "client"}),
        ("settings", "any", {"lock": "client"}),
        ("user", "string", {"lock": "client"}),
        ("access_control_objects", "any", {"lock": "client"}),
        ("start_time", "timestamp", {"lock": "client"}),
        ("execution_start_time", "timestamp", {"lock": "query_tracker"}),
        ("filter_factors", "string", {"lock": "client"}),
        ("state", "string", {"lock": "common"}),
        ("incarnation", "int64", {"lock": "query_tracker"}),
        ("ping_time", "timestamp", {"lock": "query_tracker"}),
        ("lease_transaction_id", "string", {"lock": "query_tracker"}),
        ("assigned_tracker", "string", {"lock": "query_tracker"}),
        ("progress", "any", {"lock": "query_tracker_progress"}),
        ("error", "any", {"lock": "query_tracker"}),
        ("result_count", "int64", {"lock": "query_tracker"}),
        ("finish_time", "timestamp", {"lock": "common"}),
        ("abort_request", "any", {"lock": "client"}),
        ("annotations", "any", {"lock": "client"}),
    ],
    optimize_for="lookup",
    attributes={
        "tablet_cell_bundle": SYS_BUNDLE_NAME,
        "min_data_ttl": 60000,
        "merge_rows_on_flush": True,
        "auto_compaction_period": 3600000,
    },
)


def start_time_to_execution_start_time_mapper(row):
    column_names = ACTIVE_QUERIES_TABLE_V12.user_columns
    result = dict([(key, row.get(key)) for key in column_names])
    result["execution_start_time"] = row.get("start_time")
    yield result


TRANSFORMS[12] = [
    Conversion(
        "active_queries",
        source="active_queries",
        table_info=ACTIVE_QUERIES_TABLE_V12,
        mapper=start_time_to_execution_start_time_mapper
    ),
]


# NB(mpereskokova): don't forget to update min_required_state_version at yt/yt/server/query_tracker/config.cpp and state at yt/yt/ytlib/query_tracker_client/records/query.yaml

MIGRATION = Migration(
    initial_table_infos=INITIAL_TABLE_INFOS,
    initial_version=INITIAL_VERSION,
    transforms=TRANSFORMS,
    actions=ACTIONS,
)


def get_latest_version():
    """ Get latest version of the query tracker state migration """
    return MIGRATION.get_latest_version()


# Warning! This function does NOT perform actual transformations, it only creates tables with latest schemas.
def create_tables_latest_version(client, override_tablet_cell_bundle="default", shard_count=1, state_path=DEFAULT_STATE_PATH):
    """ Creates query tracker tables of latest version """

    MIGRATION.create_tables(
        client=client,
        target_version=MIGRATION.get_latest_version(),
        tables_path=state_path,
        shard_count=shard_count,
        override_tablet_cell_bundle=override_tablet_cell_bundle
    )


# Warning! This function does NOT perform actual transformations, it only creates tables with required version schemas.
def create_tables_required_version(client, version, override_tablet_cell_bundle="default", shard_count=1, state_path=DEFAULT_STATE_PATH):
    """ Creates query tracker tables of required version """

    MIGRATION.create_tables(
        client=client,
        target_version=version,
        tables_path=state_path,
        shard_count=shard_count,
        override_tablet_cell_bundle=override_tablet_cell_bundle
    )


def run_migration(client, version, force=True, shard_count=1, state_path=DEFAULT_STATE_PATH):
    """Run query tracker migration to the required version """

    MIGRATION.run(
        client=client,
        tables_path=state_path,
        target_version=version,
        shard_count=shard_count,
        force=force,
        retransform=False,
    )


def build_arguments_parser():
    parser = argparse.ArgumentParser(description="Transform query tracker state")
    parser.add_argument("--force", action="store_true", default=False)
    parser.add_argument("--state-path", type=str, default=DEFAULT_STATE_PATH)
    parser.add_argument("--shard-count", type=int, default=DEFAULT_SHARD_COUNT)
    parser.add_argument("--proxy", type=str, default=config["proxy"]["url"])

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--target-version", type=int)
    group.add_argument("--latest", action="store_true")
    return parser


def main():
    logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)

    args = build_arguments_parser().parse_args()
    client = YtClient(proxy=args.proxy, token=config["token"])

    target_version = args.target_version
    if args.latest:
        target_version = MIGRATION.get_latest_version()

    run_migration(client, target_version, args.force, args.shard_count, args.state_path)


if __name__ == "__main__":
    main()
