#!/usr/bin/python3

import yt.yson as yson
from yt.wrapper import YtClient, config, ypath_join

from yt.environment.init_cluster import get_default_resource_limits

from yt.environment.migrationlib import TableInfo, Conversion, Migration

import argparse
import logging
import time


DEFAULT_SHARD_COUNT = 100
DEFAULT_ARCHIVE_PATH = "//sys/operations_archive"
OPERATIONS_ARCHIVE_ACCOUNT_NAME = "operations_archive"
SYS_ACCOUNT_NAME = "sys"
DEFAULT_BUNDLE_NAME = "default"
SYS_BUNDLE_NAME = "sys"
SYS_BLOBS_BUNDLE_NAME = "sys_blobs"
RESOURCE_LIMITS_ATTRIBUTE = "resource_limits"
NODES_LIMIT_ATTRIBUTE = "node_count"
JOB_TABLE_PARTITION_COUNT = 10


def get_job_table_pivots(shard_count):
    pivot_keys = [[]]
    shards_per_partition = shard_count // JOB_TABLE_PARTITION_COUNT + 1
    for job_id_partition in range(JOB_TABLE_PARTITION_COUNT):
        for i in range(shards_per_partition):
            pivot_keys.append([yson.YsonUint64(job_id_partition), yson.YsonUint64(i * 2**64 / shards_per_partition)])
    return pivot_keys


def get_default_pivots(shard_count):
    return [[]] + [[yson.YsonUint64((i * 2 ** 64) / shard_count)] for i in range(1, shard_count)]


def set_table_ttl(client, table, ttl=None, auto_compaction_period=None, forbid_obsolete_rows=False):
    if ttl is not None:
        client.set(table + "/@max_data_ttl", ttl)
    if auto_compaction_period is not None:
        client.set(table + "/@auto_compaction_period", auto_compaction_period)
    if forbid_obsolete_rows:
        client.set(table + "/@max_data_versions", 1)
        client.set(table + "/@min_data_ttl", 0)
    client.set(table + "/@min_data_versions", 0)
    client.set(table + "/@forced_compaction_revision", 1)
    if client.get(table + "/@tablet_state") == "mounted":
        client.remount_table(table)


def table_init_callback(client, tables_path):
    one_day = 1000 * 3600 * 24
    one_week = one_day * 7
    one_month = one_day * 30
    two_years = one_month * 12 * 2
    for name in ["jobs", "stderrs", "job_specs", "fail_contexts", "operation_ids"]:
        table = ypath_join(tables_path, name)
        set_table_ttl(client, table, ttl=one_week, auto_compaction_period=one_day, forbid_obsolete_rows=True)
    for name in ["ordered_by_id", "ordered_by_start_time"]:
        table = ypath_join(tables_path, name)
        set_table_ttl(client, table, ttl=two_years, auto_compaction_period=one_month, forbid_obsolete_rows=True)


def update_tablet_cell_bundle(client, tablet_cell_bundle):
    if (
        tablet_cell_bundle == SYS_BLOBS_BUNDLE_NAME
        and not client.exists("//sys/tablet_cell_bundles/{}".format(SYS_BLOBS_BUNDLE_NAME))
    ):
        tablet_cell_bundle = SYS_BUNDLE_NAME
    if (
        tablet_cell_bundle == SYS_BUNDLE_NAME
        and not client.exists("//sys/tablet_cell_bundles/{}".format(SYS_BUNDLE_NAME))
    ):
        tablet_cell_bundle = DEFAULT_BUNDLE_NAME
    return tablet_cell_bundle


def create_operations_archive_account(client):
    if not client.exists("//sys/accounts/{0}".format(OPERATIONS_ARCHIVE_ACCOUNT_NAME)):
        logging.info("Creating account: %s", OPERATIONS_ARCHIVE_ACCOUNT_NAME)
        client.create("account", attributes={
            "name": OPERATIONS_ARCHIVE_ACCOUNT_NAME,
            "resource_limits": get_default_resource_limits(),
        })
        while client.get("//sys/accounts/{0}/@life_stage".format(OPERATIONS_ARCHIVE_ACCOUNT_NAME)) != "creation_committed":
            time.sleep(0.1)

    limits = client.get("//sys/accounts/{0}/@{1}".format(SYS_ACCOUNT_NAME, RESOURCE_LIMITS_ATTRIBUTE))
    limits[NODES_LIMIT_ATTRIBUTE] = 100
    logging.info("Setting account limits %s", limits)
    client.set("//sys/accounts/{0}/@{1}".format(OPERATIONS_ARCHIVE_ACCOUNT_NAME, RESOURCE_LIMITS_ATTRIBUTE), limits)


INITIAL_TABLE_INFOS = {
    "jobs": TableInfo(
        [
            ("job_id_partition_hash", "uint64", "farm_hash(job_id_hi, job_id_lo) % {}".format(JOB_TABLE_PARTITION_COUNT)),
            ("operation_id_hash", "uint64", "farm_hash(operation_id_hi, operation_id_lo)"),
            ("operation_id_hi", "uint64"),
            ("operation_id_lo", "uint64"),
            ("job_id_hi", "uint64"),
            ("job_id_lo", "uint64"),
        ], [
            ("type", "string"),
            ("state", "string"),
            ("start_time", "int64"),
            ("finish_time", "int64"),
            ("address", "string"),
            ("error", "any"),
            ("statistics", "any"),
            ("stderr_size", "uint64"),
            ("spec", "string"),
            ("spec_version", "int64"),
            ("has_spec", "boolean"),
            ("has_fail_context", "boolean"),
            ("fail_context_size", "uint64"),
            ("events", "any"),
            ("transient_state", "string"),
            ("update_time", "int64"),
            ("core_infos", "any"),
            ("job_competition_id", "string"),
            ("has_competitors", "boolean"),
            ("exec_attributes", "any"),
            ("task_name", "string"),
            ("statistics_lz4", "string"),
            ("brief_statistics", "any"),
            ("pool_tree", "string"),
            ("monitoring_descriptor", "string"),
            ("probing_job_competition_id", "string"),
            ("has_probing_competitors", "boolean"),
            ("job_cookie", "int64"),
        ],
        get_pivot_keys=get_job_table_pivots,
        default_lock="operations_cleaner",
        attributes={
            "atomicity": "none",
            "tablet_cell_bundle": SYS_BUNDLE_NAME,
            "account": OPERATIONS_ARCHIVE_ACCOUNT_NAME,
        }),
    "operation_ids": TableInfo(
        [
            ("job_id_hash", "uint64", "farm_hash(job_id_hi, job_id_lo)"),
            ("job_id_hi", "uint64"),
            ("job_id_lo", "uint64")
        ], [
            ("operation_id_hi", "uint64"),
            ("operation_id_lo", "uint64"),
        ],
        attributes={
            "atomicity": "none",
            "tablet_cell_bundle": SYS_BUNDLE_NAME,
            "account": OPERATIONS_ARCHIVE_ACCOUNT_NAME,
        }),
    "job_specs": TableInfo(
        [
            ("job_id_hash", "uint64", "farm_hash(job_id_hi, job_id_lo)"),
            ("job_id_hi", "uint64"),
            ("job_id_lo", "uint64"),
        ], [
            ("spec", "string"),
            ("spec_version", "int64"),
            ("type", "string"),
        ],
        get_pivot_keys=get_default_pivots,
        attributes={
            "atomicity": "none",
            "tablet_cell_bundle": SYS_BLOBS_BUNDLE_NAME,
            "account": OPERATIONS_ARCHIVE_ACCOUNT_NAME,
        }),
    "stderrs": TableInfo(
        [
            ("operation_id_hash", "uint64", "farm_hash(operation_id_hi, operation_id_lo)"),
            ("operation_id_hi", "uint64"),
            ("operation_id_lo", "uint64"),
            ("job_id_hi", "uint64"),
            ("job_id_lo", "uint64")
        ], [
            ("stderr", "string"),
        ],
        get_pivot_keys=get_default_pivots,
        attributes={
            "atomicity": "none",
            "tablet_cell_bundle": SYS_BLOBS_BUNDLE_NAME,
            "account": OPERATIONS_ARCHIVE_ACCOUNT_NAME,
        }),
    "fail_contexts": TableInfo(
        [
            ("operation_id_hash", "uint64", "farm_hash(operation_id_hi, operation_id_lo)"),
            ("operation_id_hi", "uint64"),
            ("operation_id_lo", "uint64"),
            ("job_id_hi", "uint64"),
            ("job_id_lo", "uint64")
        ], [
            ("fail_context", "string"),
        ],
        get_pivot_keys=get_default_pivots,
        attributes={
            "atomicity": "none",
            "tablet_cell_bundle": SYS_BLOBS_BUNDLE_NAME,
            "account": OPERATIONS_ARCHIVE_ACCOUNT_NAME,
        }),
    "ordered_by_id": TableInfo(
        [
            ("id_hash", "uint64", "farm_hash(id_hi, id_lo)"),
            ("id_hi", "uint64"),
            ("id_lo", "uint64"),
        ], [
            ("state", "string"),
            ("authenticated_user", "string"),
            ("operation_type", "string"),
            ("progress", "any", {"lock": "controller_agent"}),
            ("provided_spec", "any"),
            ("spec", "any"),
            ("full_spec", "any"),
            ("experiment_assignments", "any"),
            ("experiment_assignment_names", "any"),
            ("brief_progress", "any", {"lock": "controller_agent"}),
            ("brief_spec", "any"),
            ("start_time", "int64"),
            ("finish_time", "int64"),
            ("filter_factors", "string"),
            ("result", "any"),
            ("events", "any"),
            ("alerts", "any"),
            ("slot_index", "int64"),
            ("unrecognized_spec", "any"),
            ("runtime_parameters", "any"),
            ("slot_index_per_pool_tree", "any"),
            ("annotations", "any"),
            ("task_names", "any"),
            ("controller_features", "any"),
            ("alert_events", "any"),
        ],
        get_pivot_keys=get_default_pivots,
        default_lock="operations_cleaner",
        attributes={
            "tablet_cell_bundle": SYS_BUNDLE_NAME,
            "account": OPERATIONS_ARCHIVE_ACCOUNT_NAME,
        }),
    "ordered_by_start_time": TableInfo(
        [
            ("start_time", "int64"),
            ("id_hi", "uint64"),
            ("id_lo", "uint64"),
        ], [
            ("operation_type", "string"),
            ("state", "string"),
            ("authenticated_user", "string"),
            ("filter_factors", "string"),
            ("pool", "string"),
            ("pools", "any"),
            ("has_failed_jobs", "boolean"),
            ("acl", "any"),
            ("pool_tree_to_pool", "any"),
        ],
        default_lock="operations_cleaner",
        attributes={
            "tablet_cell_bundle": SYS_BUNDLE_NAME,
            "account": OPERATIONS_ARCHIVE_ACCOUNT_NAME,
        }),
    "operation_aliases": TableInfo(
        [
            ("alias_hash", "uint64", "farm_hash(alias)"),
            ("alias", "string"),
        ], [
            ("operation_id_hi", "uint64"),
            ("operation_id_lo", "uint64"),
        ],
        get_pivot_keys=get_default_pivots,
        attributes={
            "tablet_cell_bundle": SYS_BUNDLE_NAME,
            "account": OPERATIONS_ARCHIVE_ACCOUNT_NAME,
        }),
    "job_profiles": TableInfo(
        [
            ("operation_id_hash", "uint64", "farm_hash(operation_id_hi, operation_id_lo)"),
            ("operation_id_hi", "uint64"),
            ("operation_id_lo", "uint64"),
            ("job_id_hi", "uint64"),
            ("job_id_lo", "uint64"),
            ("part_index", "int64"),
        ], [
            ("profile_type", "string"),
            ("profile_blob", "string"),
            ("profiling_probability", "double"),
        ],
        attributes={
            "atomicity": "none",
            "tablet_cell_bundle": SYS_BUNDLE_NAME,
            "account": OPERATIONS_ARCHIVE_ACCOUNT_NAME,
        }),
}

INITIAL_VERSION = 47


TRANSFORMS = {}
ACTIONS = {}


TRANSFORMS[48] = [
    Conversion(
        "jobs",
        table_info=TableInfo(
            [
                ("job_id_partition_hash", "uint64", "farm_hash(job_id_hi, job_id_lo) % {}".format(JOB_TABLE_PARTITION_COUNT)),
                ("operation_id_hash", "uint64", "farm_hash(operation_id_hi, operation_id_lo)"),
                ("operation_id_hi", "uint64"),
                ("operation_id_lo", "uint64"),
                ("job_id_hi", "uint64"),
                ("job_id_lo", "uint64")
            ], [
                ("type", "string"),
                ("state", "string"),
                ("start_time", "int64"),
                ("finish_time", "int64"),
                ("address", "string"),
                ("error", "any"),
                ("statistics", "any"),
                ("stderr_size", "uint64"),
                ("spec", "string"),
                ("spec_version", "int64"),
                ("has_spec", "boolean"),
                ("has_fail_context", "boolean"),
                ("fail_context_size", "uint64"),
                ("events", "any"),
                ("transient_state", "string"),
                ("update_time", "int64"),
                ("core_infos", "any"),
                ("job_competition_id", "string"),
                ("has_competitors", "boolean"),
                ("exec_attributes", "any"),
                ("task_name", "string"),
                ("statistics_lz4", "string"),
                ("brief_statistics", "any"),
                ("pool_tree", "string"),
                ("monitoring_descriptor", "string"),
                ("probing_job_competition_id", "string"),
                ("has_probing_competitors", "boolean"),
                ("job_cookie", "int64"),
                ("controller_state", "string"),
            ],
            default_lock="operations_cleaner",
            attributes={
                "atomicity": "none",
                "tablet_cell_bundle": SYS_BUNDLE_NAME,
                "account": OPERATIONS_ARCHIVE_ACCOUNT_NAME,
            })),
]

TRANSFORMS[49] = [
    Conversion(
        "job_specs",
        table_info=TableInfo(
            [
                ("job_id_hash", "uint64", "farm_hash(job_id_hi, job_id_lo)"),
                ("job_id_hi", "uint64"),
                ("job_id_lo", "uint64"),
            ], [
                ("spec", "string", {"max_inline_hunk_size": 64}),
                ("spec_version", "int64"),
                ("type", "string"),
            ],
            attributes={
                "atomicity": "none",
                "tablet_cell_bundle": SYS_BLOBS_BUNDLE_NAME,
                "account": OPERATIONS_ARCHIVE_ACCOUNT_NAME,
            })),
    Conversion(
        "stderrs",
        table_info=TableInfo(
            [
                ("operation_id_hash", "uint64", "farm_hash(operation_id_hi, operation_id_lo)"),
                ("operation_id_hi", "uint64"),
                ("operation_id_lo", "uint64"),
                ("job_id_hi", "uint64"),
                ("job_id_lo", "uint64")
            ], [
                ("stderr", "string", {"max_inline_hunk_size": 512}),
            ],
            attributes={
                "atomicity": "none",
                "tablet_cell_bundle": SYS_BLOBS_BUNDLE_NAME,
                "account": OPERATIONS_ARCHIVE_ACCOUNT_NAME,
            })),
    Conversion(
        "job_profiles",
        table_info=TableInfo(
            [
                ("operation_id_hash", "uint64", "farm_hash(operation_id_hi, operation_id_lo)"),
                ("operation_id_hi", "uint64"),
                ("operation_id_lo", "uint64"),
                ("job_id_hi", "uint64"),
                ("job_id_lo", "uint64"),
                ("part_index", "int64"),
            ], [
                ("profile_type", "string"),
                ("profile_blob", "string", {"max_inline_hunk_size": 64}),
                ("profiling_probability", "double"),
            ],
            attributes={
                "atomicity": "none",
                "tablet_cell_bundle": SYS_BUNDLE_NAME,
                "account": OPERATIONS_ARCHIVE_ACCOUNT_NAME,
            })),
]

TRANSFORMS[50] = [
    Conversion(
        "jobs",
        table_info=TableInfo(
            [
                ("job_id_partition_hash", "uint64", "farm_hash(job_id_hi, job_id_lo) % {}".format(JOB_TABLE_PARTITION_COUNT)),
                ("operation_id_hash", "uint64", "farm_hash(operation_id_hi, operation_id_lo)"),
                ("operation_id_hi", "uint64"),
                ("operation_id_lo", "uint64"),
                ("job_id_hi", "uint64"),
                ("job_id_lo", "uint64")
            ], [
                ("type", "string"),
                ("state", "string"),
                ("start_time", "int64"),
                ("finish_time", "int64"),
                ("address", "string"),
                ("error", "any"),
                ("interruption_info", "any"),
                ("statistics", "any"),
                ("stderr_size", "uint64"),
                ("spec", "string"),
                ("spec_version", "int64"),
                ("has_spec", "boolean"),
                ("has_fail_context", "boolean"),
                ("fail_context_size", "uint64"),
                ("events", "any"),
                ("transient_state", "string"),
                ("update_time", "int64"),
                ("core_infos", "any"),
                ("job_competition_id", "string"),
                ("has_competitors", "boolean"),
                ("exec_attributes", "any"),
                ("task_name", "string"),
                ("statistics_lz4", "string"),
                ("brief_statistics", "any"),
                ("pool_tree", "string"),
                ("monitoring_descriptor", "string"),
                ("probing_job_competition_id", "string"),
                ("has_probing_competitors", "boolean"),
                ("job_cookie", "int64"),
                ("controller_state", "string"),
            ],
            default_lock="operations_cleaner",
            attributes={
                "atomicity": "none",
                "tablet_cell_bundle": SYS_BUNDLE_NAME,
                "account": OPERATIONS_ARCHIVE_ACCOUNT_NAME,
            })),
]

TRANSFORMS[51] = [
    Conversion(
        "jobs",
        table_info=TableInfo(
            [
                ("job_id_partition_hash", "uint64", "farm_hash(job_id_hi, job_id_lo) % {}".format(JOB_TABLE_PARTITION_COUNT)),
                ("operation_id_hash", "uint64", "farm_hash(operation_id_hi, operation_id_lo)"),
                ("operation_id_hi", "uint64"),
                ("operation_id_lo", "uint64"),
                ("job_id_hi", "uint64"),
                ("job_id_lo", "uint64")
            ], [
                ("type", "string"),
                ("state", "string"),
                ("start_time", "int64"),
                ("finish_time", "int64"),
                ("address", "string"),
                ("error", "any"),
                ("interruption_info", "any"),
                ("statistics", "any"),
                ("stderr_size", "uint64"),
                ("spec", "string"),
                ("spec_version", "int64"),
                ("has_spec", "boolean"),
                ("has_fail_context", "boolean"),
                ("fail_context_size", "uint64"),
                ("events", "any"),
                ("transient_state", "string"),
                ("update_time", "int64"),
                ("core_infos", "any"),
                ("job_competition_id", "string"),
                ("has_competitors", "boolean"),
                ("exec_attributes", "any"),
                ("task_name", "string"),
                ("statistics_lz4", "string"),
                ("brief_statistics", "any"),
                ("pool_tree", "string"),
                ("monitoring_descriptor", "string"),
                ("probing_job_competition_id", "string"),
                ("has_probing_competitors", "boolean"),
                ("job_cookie", "int64"),
                ("controller_state", "string"),
                ("archive_features", "any"),
            ],
            default_lock="operations_cleaner",
            attributes={
                "atomicity": "none",
                "tablet_cell_bundle": SYS_BUNDLE_NAME,
                "account": OPERATIONS_ARCHIVE_ACCOUNT_NAME,
            })),
]

TRANSFORMS[52] = [
    Conversion(
        "ordered_by_id",
        table_info=TableInfo(
            [
                ("id_hash", "uint64", "farm_hash(id_hi, id_lo)"),
                ("id_hi", "uint64"),
                ("id_lo", "uint64"),
            ], [
                ("state", "string"),
                ("authenticated_user", "string"),
                ("operation_type", "string"),
                ("progress", "any", {"lock": "controller_agent"}),
                ("provided_spec", "any"),
                ("spec", "any"),
                ("full_spec", "any"),
                ("experiment_assignments", "any"),
                ("experiment_assignment_names", "any"),
                ("brief_progress", "any", {"lock": "controller_agent"}),
                ("brief_spec", "any"),
                ("start_time", "int64"),
                ("finish_time", "int64"),
                ("filter_factors", "string"),
                ("result", "any"),
                ("events", "any"),
                ("alerts", "any"),
                ("slot_index", "int64"),
                ("unrecognized_spec", "any"),
                ("runtime_parameters", "any"),
                ("slot_index_per_pool_tree", "any"),
                ("annotations", "any"),
                ("task_names", "any"),
                ("controller_features", "any"),
                ("alert_events", "any"),
                ("scheduling_attributes_per_pool_tree", "any"),
            ],
            in_memory=False,
            default_lock="operations_cleaner",
            attributes={
                "tablet_cell_bundle": SYS_BUNDLE_NAME,
                "account": OPERATIONS_ARCHIVE_ACCOUNT_NAME,
            })),
]

TRANSFORMS[53] = [
    Conversion(
        "job_trace_events",
        table_info=TableInfo(
            [
                ("operation_id_hash", "uint64", "farm_hash(operation_id_hi, operation_id_lo)"),
                ("operation_id_hi", "uint64"),
                ("operation_id_lo", "uint64"),
                ("job_id_hi", "uint64"),
                ("job_id_lo", "uint64"),
                ("trace_id_hi", "uint64"),
                ("trace_id_lo", "uint64"),
                ("event_index", "int64"),
            ], [
                ("event", "string"),
                ("event_time", "int64"),
            ],
            default_lock="operations_cleaner",
            get_pivot_keys=get_default_pivots,
            attributes={
                "tablet_cell_bundle": SYS_BUNDLE_NAME,
                "account": OPERATIONS_ARCHIVE_ACCOUNT_NAME,
                "atomicity": "none",
            })),
]

TRANSFORMS[54] = [
    Conversion(
        "jobs",
        table_info=TableInfo(
            [
                ("job_id_partition_hash", "uint64", "farm_hash(job_id_hi, job_id_lo) % {}".format(JOB_TABLE_PARTITION_COUNT)),
                ("operation_id_hash", "uint64", "farm_hash(operation_id_hi, operation_id_lo)"),
                ("operation_id_hi", "uint64"),
                ("operation_id_lo", "uint64"),
                ("job_id_hi", "uint64"),
                ("job_id_lo", "uint64")
            ], [
                ("type", "string"),
                ("state", "string"),
                ("start_time", "int64"),
                ("finish_time", "int64"),
                ("address", "string"),
                ("error", "any"),
                ("interruption_info", "any"),
                ("statistics", "any"),
                ("stderr_size", "uint64"),
                ("spec", "string"),
                ("spec_version", "int64"),
                ("has_spec", "boolean"),
                ("has_fail_context", "boolean"),
                ("fail_context_size", "uint64"),
                ("events", "any"),
                ("transient_state", "string"),
                ("update_time", "int64"),
                ("core_infos", "any"),
                ("job_competition_id", "string"),
                ("has_competitors", "boolean"),
                ("exec_attributes", "any"),
                ("task_name", "string"),
                ("statistics_lz4", "string"),
                ("brief_statistics", "any"),
                ("pool_tree", "string"),
                ("monitoring_descriptor", "string"),
                ("probing_job_competition_id", "string"),
                ("has_probing_competitors", "boolean"),
                ("job_cookie", "int64"),
                ("controller_state", "string"),
                ("archive_features", "any"),
                ("$ttl", "uint64"),
            ],
            default_lock="operations_cleaner",
            attributes={
                "atomicity": "none",
                "tablet_cell_bundle": SYS_BUNDLE_NAME,
                "account": OPERATIONS_ARCHIVE_ACCOUNT_NAME,
            })),
]

TRANSFORMS[55] = [
    Conversion(
        "jobs",
        table_info=TableInfo(
            [
                ("job_id_partition_hash", "uint64", "farm_hash(job_id_hi, job_id_lo) % {}".format(JOB_TABLE_PARTITION_COUNT)),
                ("operation_id_hash", "uint64", "farm_hash(operation_id_hi, operation_id_lo)"),
                ("operation_id_hi", "uint64"),
                ("operation_id_lo", "uint64"),
                ("job_id_hi", "uint64"),
                ("job_id_lo", "uint64")
            ], [
                ("type", "string"),
                ("state", "string"),
                ("start_time", "int64"),
                ("finish_time", "int64"),
                ("address", "string"),
                ("error", "any"),
                ("interruption_info", "any"),
                ("statistics", "any"),
                ("stderr_size", "uint64"),
                ("spec", "string"),
                ("spec_version", "int64"),
                ("has_spec", "boolean"),
                ("has_fail_context", "boolean"),
                ("fail_context_size", "uint64"),
                ("events", "any"),
                ("transient_state", "string"),
                ("update_time", "int64"),
                ("core_infos", "any"),
                ("job_competition_id", "string"),
                ("has_competitors", "boolean"),
                ("exec_attributes", "any"),
                ("task_name", "string"),
                ("statistics_lz4", "string"),
                ("brief_statistics", "any"),
                ("pool_tree", "string"),
                ("monitoring_descriptor", "string"),
                ("probing_job_competition_id", "string"),
                ("has_probing_competitors", "boolean"),
                ("job_cookie", "int64"),
                ("controller_state", "string"),
                ("archive_features", "any"),
                ("$ttl", "uint64"),
                ("operation_incarnation", "string"),
            ],
            default_lock="operations_cleaner",
            attributes={
                "atomicity": "none",
                "tablet_cell_bundle": SYS_BUNDLE_NAME,
                "account": OPERATIONS_ARCHIVE_ACCOUNT_NAME,
            })),
]

def get_addresses_mapper(table):
    column_names = table.user_columns

    def addresses_mapper(row):
        result = dict([(key, row.get(key)) for key in column_names])

        address = row.get("address")
        if address is None:
            result["addresses"] = yson.YsonMap({})
        else:
            result["addresses"] = yson.YsonMap({"default": address})

        yield result

    return addresses_mapper

JOBS_TABLE_V56 = TableInfo(
    [
        ("job_id_partition_hash", "uint64", "farm_hash(job_id_hi, job_id_lo) % {}".format(JOB_TABLE_PARTITION_COUNT)),
        ("operation_id_hash", "uint64", "farm_hash(operation_id_hi, operation_id_lo)"),
        ("operation_id_hi", "uint64"),
        ("operation_id_lo", "uint64"),
        ("job_id_hi", "uint64"),
        ("job_id_lo", "uint64")
    ], [
        ("type", "string"),
        ("state", "string"),
        ("start_time", "int64"),
        ("finish_time", "int64"),
        ("address", "string"),
        ("error", "any"),
        ("interruption_info", "any"),
        ("statistics", "any"),
        ("stderr_size", "uint64"),
        ("spec", "string"),
        ("spec_version", "int64"),
        ("has_spec", "boolean"),
        ("has_fail_context", "boolean"),
        ("fail_context_size", "uint64"),
        ("events", "any"),
        ("transient_state", "string"),
        ("update_time", "int64"),
        ("core_infos", "any"),
        ("job_competition_id", "string"),
        ("has_competitors", "boolean"),
        ("exec_attributes", "any"),
        ("task_name", "string"),
        ("statistics_lz4", "string"),
        ("brief_statistics", "any"),
        ("pool_tree", "string"),
        ("monitoring_descriptor", "string"),
        ("probing_job_competition_id", "string"),
        ("has_probing_competitors", "boolean"),
        ("job_cookie", "int64"),
        ("controller_state", "string"),
        ("archive_features", "any"),
        ("$ttl", "uint64"),
        ("operation_incarnation", "string"),
        ("addresses", "any"),
    ],
    default_lock="operations_cleaner",
    attributes={
        "atomicity": "none",
        "tablet_cell_bundle": SYS_BUNDLE_NAME,
        "account": OPERATIONS_ARCHIVE_ACCOUNT_NAME,
    },
)

TRANSFORMS[56] = [
    Conversion(
        "jobs",
        table_info=JOBS_TABLE_V56,
        mapper=get_addresses_mapper(JOBS_TABLE_V56),
    )
]

# NB(renadeen): don't forget to update min_required_archive_version at yt/yt/server/lib/scheduler/config.cpp


def are_hunks_enabled(client):
    # Hunks are enabled by default now
    return True


def is_ttl_column_available(client):
    host = client.list("//sys/primary_masters")[0]
    version = client.get(f"//sys/primary_masters/{host}/orchid/build_info/binary_version")
    version = float(version[:4])
    return version >= 24.1


def check_operations_archive_version(client, target_version):
    if not are_hunks_enabled(client) and target_version >= 48:
        raise Exception("Unable to init operations archive: hunks are not enabled")

    if not is_ttl_column_available(client) and target_version >= 54:
        logging.info("Master's version is lower than 24.1. Archive target version reduced to 53")
        target_version = 53

    return target_version


def prepare_migration(client, archive_path):
    if not client.exists(archive_path):
        create_operations_archive_account(client)

    # update all tablet_cell_bundles to existing ones
    for table in INITIAL_TABLE_INFOS.values():
        table.attributes["tablet_cell_bundle"] = update_tablet_cell_bundle(client, table.attributes["tablet_cell_bundle"])

    for transform in TRANSFORMS.values():
        for conversion in transform:
            if conversion.table_info and conversion.table_info.attributes.get("tablet_cell_bundle") is not None:
                conversion.table_info.attributes["tablet_cell_bundle"] = update_tablet_cell_bundle(
                    client,
                    conversion.table_info.attributes["tablet_cell_bundle"])

    migration = Migration(
        initial_table_infos=INITIAL_TABLE_INFOS,
        initial_version=INITIAL_VERSION,
        transforms=TRANSFORMS,
        actions=ACTIONS,
        table_init_callback=table_init_callback,
    )
    return migration


def get_latest_version():
    """ Get latest version of the operation archive migtation """
    migration = Migration(
        initial_table_infos=INITIAL_TABLE_INFOS,
        initial_version=INITIAL_VERSION,
        transforms=TRANSFORMS,
        actions=ACTIONS,
        table_init_callback=table_init_callback,
    )
    return migration.get_latest_version()


def create_tables(client, target_version=None, override_tablet_cell_bundle="default", shard_count=1, archive_path=DEFAULT_ARCHIVE_PATH):
    """ Creates operation archive tables of given version """
    migration = prepare_migration(client, archive_path)

    if override_tablet_cell_bundle is not None:
        override_tablet_cell_bundle = update_tablet_cell_bundle(client, override_tablet_cell_bundle)

    if target_version is None:
        target_version = migration.get_latest_version()

    migration.create_tables(
        client=client,
        target_version=target_version,
        tables_path=archive_path,
        shard_count=shard_count,
        override_tablet_cell_bundle=override_tablet_cell_bundle,
    )


# Warning! This function does NOT perform actual transformations, it only creates tables with latest schemas.
def create_tables_latest_version(client, override_tablet_cell_bundle="default", shard_count=1, archive_path=DEFAULT_ARCHIVE_PATH):
    """ Creates operation archive tables of latest version """
    create_tables(
        client,
        override_tablet_cell_bundle=override_tablet_cell_bundle,
        shard_count=shard_count,
        archive_path=archive_path,
    )


def build_arguments_parser():
    parser = argparse.ArgumentParser(description="Transform operations archive")
    parser.add_argument("--force", action="store_true", default=False)
    parser.add_argument("--archive-path", type=str, default=DEFAULT_ARCHIVE_PATH)
    parser.add_argument("--shard-count", type=int, default=DEFAULT_SHARD_COUNT)
    parser.add_argument("--proxy", type=str, default=config["proxy"]["url"])
    parser.add_argument("--retransform", action="store_true", default=False)

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--target-version", type=int)
    group.add_argument("--latest", action="store_true")
    return parser


def run(client, archive_path, target_version, shard_count, latest, force, retransform):
    migration = prepare_migration(client, archive_path)

    target_version = target_version
    if latest:
        target_version = migration.get_latest_version()

    target_version = check_operations_archive_version(client, target_version)

    migration.run(
        client=client,
        tables_path=archive_path,
        target_version=target_version,
        shard_count=shard_count,
        force=force,
        retransform=retransform,
    )


def main():
    logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)

    args = build_arguments_parser().parse_args()
    client = YtClient(proxy=args.proxy, token=config["token"])

    run(
        client=client,
        archive_path=args.archive_path,
        target_version=args.target_version,
        shard_count=args.shard_count,
        latest=args.latest,
        force=args.force,
        retransform=args.retransform,
    )


if __name__ == "__main__":
    main()
