from yt_env_setup import YTEnvSetup, Restarter, CONTROLLER_AGENTS_SERVICE

from yt_commands import (
    alter_table, authors, wait, create, ls, get, set, remove, exists,
    insert_rows, delete_rows,
    write_table, map, merge, sort, vanilla, remote_copy, get_table_columnar_statistics,
    sync_create_cells, sync_mount_table, sync_flush_table, sync_compact_table,
    raises_yt_error)

from yt_type_helpers import (
    make_schema, make_column,
    optional_type,
)

from yt_helpers import skip_if_renaming_disabled, profiler_factory

from yt.common import YtError

from yt.yson.yson_types import YsonEntity

import pytest
import random
import string


class _TestColumnarStatisticsBase(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "enable_map_job_size_adjustment": False,
            "user_file_limits": {
                "max_table_data_weight": 2000,
            },
            "operation_options": {
                "spec_template": {
                    "use_columnar_statistics": True,
                },
            },
        },
        "heap_profiler_update_snapshot_period": 100,
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "data_node": {
                "testing_options": {
                    "columnar_statistics_chunk_meta_fetch_max_delay": 5000,
                    "columnar_statistics_read_timeout_fraction": 0.1,
                },
            },
        },
    }

    DELTA_DRIVER_CONFIG = {
        "fetcher": {
            "node_rpc_timeout": 10000
        },
        "table_writer": {
            "enable_large_columnar_statistics": True,
        },
    }

    @staticmethod
    def _expect_data_weight_statistics(
        lower_row_index,
        upper_row_index,
        columns,
        expected_data_weights,
        expected_timestamp_weight=None,
        expected_legacy_data_weight=0,
        fetcher_mode="from_nodes",
        table="//tmp/t",
        enable_early_finish=False,
        expected_estimated_unique_counts=None
    ):
        path = '["{0}{{{1}}}[{2}:{3}]";]'.format(
            table,
            columns,
            "#" + str(lower_row_index) if lower_row_index is not None else "",
            "#" + str(upper_row_index) if upper_row_index is not None else "",
        )
        statistics = get_table_columnar_statistics(path,
                                                   fetcher_mode=fetcher_mode,
                                                   enable_early_finish=enable_early_finish)[0]
        assert statistics["legacy_chunks_data_weight"] == expected_legacy_data_weight
        assert statistics["column_data_weights"] == dict(zip(columns.split(","), expected_data_weights))
        if expected_timestamp_weight is not None:
            assert statistics["timestamp_total_weight"] == expected_timestamp_weight
        if expected_estimated_unique_counts is not None:
            assert statistics["column_estimated_unique_counts"] == dict(zip(columns.split(','), expected_estimated_unique_counts))

    def _expect_multi_statistics(
        self,
        paths,
        lower_row_indices,
        upper_row_indices,
        all_columns,
        all_expected_data_weights,
        all_expected_timestamp_weight=None,
        schema_columns=None,
    ):
        assert len(paths) == len(all_columns)
        for index in range(len(paths)):
            paths[index] = "{0}{1}[{2}:{3}]".format(
                paths[index],
                "{" + all_columns[index] + "}" if all_columns[index] is not None else "",
                "#" + str(lower_row_indices[index]) if lower_row_indices[index] is not None else "",
                "#" + str(upper_row_indices[index]) if upper_row_indices[index] is not None else "",
            )
        yson_paths = "["
        for path in paths:
            yson_paths += '"' + path + '";'
        yson_paths += "]"
        all_statistics = get_table_columnar_statistics(yson_paths)
        assert len(all_statistics) == len(all_expected_data_weights)
        for index in range(len(all_statistics)):
            assert all_statistics[index]["legacy_chunks_data_weight"] == 0
            expected_columns = all_columns[index] if all_columns[index] is not None else schema_columns[index]
            assert all_statistics[index]["column_data_weights"] == dict(
                zip(expected_columns.split(","), all_expected_data_weights[index])
            )
            if all_expected_timestamp_weight is not None:
                assert all_statistics[index] == all_expected_timestamp_weight[index]

    def _create_simple_dynamic_table(self, path, optimize_for="lookup"):
        create(
            "table",
            path,
            attributes={
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ],
                "dynamic": True,
                "optimize_for": optimize_for,
            },
        )

    @staticmethod
    def _expect_columnar_statistics(
        table,
        columns,
        lower_row_index=None,
        upper_row_index=None,
        **expected_statistics
    ):
        columns = ",".join(columns)
        lower_row_index = "#" + str(lower_row_index) if lower_row_index is not None else ""
        upper_row_index = "#" + str(upper_row_index) if upper_row_index is not None else ""
        path = f'["{table}{{{columns}}}[{lower_row_index}:{upper_row_index}]";]'
        statistics = get_table_columnar_statistics(path)[0]
        LIST_OF_COLUMNAR_STATISTICS_NAMES = [
            "column_min_values",
            "column_max_values",
            "column_non_null_value_counts",
            "column_data_weights",
            "chunk_row_count",
            "legacy_chunk_row_count",
        ]
        for statistics_name in LIST_OF_COLUMNAR_STATISTICS_NAMES:
            if expected_statistics.get(statistics_name) is not None:
                assert statistics.get(statistics_name) == expected_statistics.get(statistics_name), \
                    "Error when checking {}, table path is {}".format(statistics_name, path)


@pytest.mark.enabled_multidaemon
class TestColumnarStatistics(_TestColumnarStatisticsBase):
    ENABLE_MULTIDAEMON = True

    @authors("max42")
    def test_get_table_columnar_statistics_basic(self):
        create("table", "//tmp/t")
        write_table("<append=%true>//tmp/t", [{"a": "x" * 100, "b": 42}, {"c": 1.2}])
        write_table("<append=%true>//tmp/t", [{"a": "x" * 200}, {"c": True}])
        write_table("<append=%true>//tmp/t", [{"b": None, "c": 0}, {"a": "x" * 1000}])
        with pytest.raises(YtError):
            get_table_columnar_statistics('["//tmp/t";]')
        self._expect_data_weight_statistics(2, 2, "a,b,c", [0, 0, 0])
        self._expect_data_weight_statistics(0, 6, "a,b,c", [1300, 8, 17], expected_estimated_unique_counts=[3, 1, 2])
        self._expect_data_weight_statistics(0, 6, "a,c,x", [1300, 17, 0], expected_estimated_unique_counts=[3, 2, 0])
        self._expect_data_weight_statistics(1, 5, "a,b,c", [1300, 8, 17], expected_estimated_unique_counts=[3, 1, 2])
        self._expect_data_weight_statistics(2, 5, "a", [1200], expected_estimated_unique_counts=[2])
        self._expect_data_weight_statistics(1, 4, "", [])

    @authors("orlovorlov")
    def test_get_table_columnar_statistics_merge(self):
        create("table", "//tmp/t")
        create("table", "//tmp/d")
        write_table("<append=%true>//tmp/t", [{"a": "x" * 100, "b": 42}, {"c": 1.2}])
        write_table("<append=%true>//tmp/t", [{"a": "x" * 200}, {"c": True}])
        write_table("<append=%true>//tmp/t", [{"b": None, "c": 0}, {"a": "x" * 1000}])
        self._expect_data_weight_statistics(0, 6, "a,b,c", [1300, 8, 17], table="//tmp/t", expected_estimated_unique_counts=[3, 1, 2])
        op = merge(in_="//tmp/t{a,b,c}", out="//tmp/d", spec={"combine_chunks": True})

        op.track()
        assert 1 == get('//tmp/d/@chunk_count')
        self._expect_data_weight_statistics(0, 6, "a,b,c", [1300, 8, 17], table="//tmp/d", expected_estimated_unique_counts=[3, 1, 3])

    @authors("gritukan")
    def test_get_table_approximate_statistics(self):
        def make_table(column_weights):
            if exists("//tmp/t"):
                remove("//tmp/t")
            create("table", "//tmp/t")
            if column_weights:
                write_table(
                    "//tmp/t",
                    [{"x{}".format(i): "a" * column_weights[i] for i in range(len(column_weights))}],
                )

        make_table([255, 12, 45, 1, 0])
        self._expect_data_weight_statistics(
            0,
            1,
            "x0,x1,x2,x3,x4,zzz",
            [255, 12, 45, 1, 0, 0],
            fetcher_mode="from_master",
        )

        create("table", "//tmp/t2")
        remote_copy(
            in_="//tmp/t",
            out="//tmp/t2",
            spec={"cluster_connection": self.__class__.Env.configs["driver"]},
        )
        self._expect_data_weight_statistics(
            0,
            1,
            "x0,x1,x2,x3,x4,zzz",
            [255, 12, 45, 1, 0, 0],
            fetcher_mode="from_master",
            table="//tmp/t2",
        )

        make_table([510, 12, 13, 1, 0])
        self._expect_data_weight_statistics(0, 1, "x0,x1,x2,x3,x4", [510, 12, 14, 2, 0], fetcher_mode="from_master")

        make_table([256, 12, 13, 1, 0])
        self._expect_data_weight_statistics(0, 1, "x0,x1,x2,x3,x4", [256, 12, 14, 2, 0], fetcher_mode="from_master")

        make_table([1])
        self._expect_data_weight_statistics(0, 1, "", [], fetcher_mode="from_master")

        make_table([])
        self._expect_data_weight_statistics(0, 1, "x", [0], fetcher_mode="from_master")

        set("//sys/@config/chunk_manager/max_heavy_columns", 1)
        make_table([255, 42])
        self._expect_data_weight_statistics(0, 1, "x0,x1,zzz", [255, 255, 255], fetcher_mode="from_master")

        set("//sys/@config/chunk_manager/max_heavy_columns", 0)
        make_table([256, 42])
        self._expect_data_weight_statistics(
            0,
            1,
            "x0,x1,zzz",
            [0, 0, 0],
            fetcher_mode="from_master",
            expected_legacy_data_weight=299,
        )
        self._expect_data_weight_statistics(0, 1, "x0,x1,zzz", [256, 42, 0], fetcher_mode="fallback")

    @authors("dakovalkov")
    def test_get_table_columnar_statistics_multi(self):
        create("table", "//tmp/t")
        write_table("<append=%true>//tmp/t", [{"a": "x" * 10, "b": 42}, {"c": 1.2}])
        write_table("<append=%true>//tmp/t", [{"a": "x" * 20}, {"c": True}])
        write_table("<append=%true>//tmp/t", [{"b": None, "c": 0}, {"a": "x" * 100}])

        create("table", "//tmp/t2")
        write_table("<append=%true>//tmp/t2", [{"a": "x" * 100, "b": 42}, {"c": 1.2}])
        write_table("<append=%true>//tmp/t2", [{"a": "x" * 200}, {"c": True}])
        write_table("<append=%true>//tmp/t2", [{"b": None, "c": 0}, {"a": "x" * 1000}])

        paths = []
        lower_row_indices = []
        upper_row_indices = []
        all_columns = []
        all_expected_data_weights = []

        paths.append("//tmp/t")
        lower_row_indices.append(2)
        upper_row_indices.append(2)
        all_columns.append("a,b,c")
        all_expected_data_weights.append([0, 0, 0])

        paths.append("//tmp/t2")
        lower_row_indices.append(0)
        upper_row_indices.append(6)
        all_columns.append("a,b,c")
        all_expected_data_weights.append([1300, 8, 17])

        paths.append("//tmp/t")
        lower_row_indices.append(0)
        upper_row_indices.append(6)
        all_columns.append("a,c,x")
        all_expected_data_weights.append([130, 17, 0])

        paths.append("//tmp/t2")
        lower_row_indices.append(1)
        upper_row_indices.append(5)
        all_columns.append("a,b,c")
        all_expected_data_weights.append([1300, 8, 17])

        paths.append("//tmp/t")
        lower_row_indices.append(2)
        upper_row_indices.append(5)
        all_columns.append("a")
        all_expected_data_weights.append([120])

        paths.append("//tmp/t2")
        lower_row_indices.append(1)
        upper_row_indices.append(4)
        all_columns.append("")
        all_expected_data_weights.append([])

        self._expect_multi_statistics(
            paths,
            lower_row_indices,
            upper_row_indices,
            all_columns,
            all_expected_data_weights,
        )

    @authors("achulkov2")
    def test_get_table_columnar_statistics_schema_fetching(self):
        create("table", "//tmp/t")
        write_table("<append=%true>//tmp/t", [{"a": "x" * 10, "b": 42}, {"c": 1.2}])
        write_table("<append=%true>//tmp/t", [{"a": "x" * 20}, {"c": True}])
        write_table("<append=%true>//tmp/t", [{"b": None, "c": 0}, {"a": "x" * 100}])

        create("table", "//tmp/t2", attributes={
            "schema": [
                {"name": "a", "type": "string"},
                {"name": "b", "type": "int64"},
                {"name": "c", "type": "any"}
            ]
        })
        write_table("<append=%true>//tmp/t2", [{"a": "x" * 100, "b": 42}, {"c": 1.2}])
        write_table("<append=%true>//tmp/t2", [{"a": "x" * 200}, {"c": True}])
        write_table("<append=%true>//tmp/t2", [{"b": None, "c": 0}, {"a": "x" * 1000}])

        paths = []
        lower_row_indices = []
        upper_row_indices = []
        all_columns = []
        all_expected_data_weights = []
        schema_columns = []

        paths.append("//tmp/t")
        lower_row_indices.append(None)
        upper_row_indices.append(None)
        all_columns.append("a,b")
        schema_columns.append(None)
        all_expected_data_weights.append([130, 8])

        paths.append("//tmp/t2")
        lower_row_indices.append(None)
        upper_row_indices.append(None)
        all_columns.append(None)
        schema_columns.append("a,b,c")
        all_expected_data_weights.append([1300, 8, 17])

        paths.append("//tmp/t2")
        lower_row_indices.append(None)
        upper_row_indices.append(None)
        all_columns.append("c,a,x")
        schema_columns.append(None)
        all_expected_data_weights.append([17, 1300, 0])

        paths.append("//tmp/t2")
        lower_row_indices.append(None)
        upper_row_indices.append(None)
        all_columns.append("a,b,c")
        schema_columns.append(None)
        all_expected_data_weights.append([1300, 8, 17])

        paths.append("//tmp/t")
        lower_row_indices.append(2)
        upper_row_indices.append(5)
        all_columns.append("a")
        schema_columns.append(None)
        all_expected_data_weights.append([120])

        paths.append("//tmp/t2")
        lower_row_indices.append(None)
        upper_row_indices.append(None)
        all_columns.append(None)
        schema_columns.append("a,b,c")
        all_expected_data_weights.append([1300, 8, 17])

        self._expect_multi_statistics(
            paths,
            lower_row_indices,
            upper_row_indices,
            all_columns,
            all_expected_data_weights,
            schema_columns=schema_columns
        )

    @authors("max42")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_dynamic_tables(self, optimize_for):
        sync_create_cells(1)
        self._create_simple_dynamic_table("//tmp/t")
        set("//tmp/t/@optimize_for", optimize_for)

        sync_mount_table("//tmp/t")
        rows = [{"key": i, "value": str(i) * 1000} for i in range(10)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        self._expect_data_weight_statistics(None, None, "key,value", [80, 10080], expected_timestamp_weight=(8 * 10))

        rows = [{"key": i, "value": str(i // 2) * 1000} for i in range(10)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        self._expect_data_weight_statistics(None, None, "key,value", [160, 20160], expected_timestamp_weight=(8 * 20))

        sync_compact_table("//tmp/t")

        self._expect_data_weight_statistics(None, None, "key,value", [80, 20160], expected_timestamp_weight=(8 * 20))

        rows = [{"key": i} for i in range(10)]
        delete_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        self._expect_data_weight_statistics(None, None, "key,value", [160, 20160], expected_timestamp_weight=(8 * 30))

        sync_compact_table("//tmp/t")

        self._expect_data_weight_statistics(None, None, "key,value", [80, 20160], expected_timestamp_weight=(8 * 30))

    @authors("prime")
    def test_max_node_per_fetch(self):
        create("table", "//tmp/t")

        for _ in range(100):
            write_table("<append=%true>//tmp/t", [{"a": "x" * 100}, {"c": 1.2}])

        statistics0 = get_table_columnar_statistics('["//tmp/t{a}";]')
        statistics1 = get_table_columnar_statistics('["//tmp/t{a}";]', max_chunks_per_node_fetch=1)
        assert statistics0 == statistics1

    @authors("denvid")
    def test_value_statistics(self):
        MAX_STRING_VALUE_LENGTH = 100
        table_path = "//tmp/t"
        create("table", table_path)
        write_table("<append=%true>" + table_path, [{"a": "x" * (MAX_STRING_VALUE_LENGTH + 1), "b": 12, 'x': ['A', {'B': 'b', 'C': 'c'}]}])
        write_table("<append=%true>" + table_path, [{"c": True, "x": 1}])
        write_table("<append=%true>" + table_path, [{"a": "t" * MAX_STRING_VALUE_LENGTH, "b": 23, "c": False}])
        write_table("<append=%true>" + table_path, [{"a": "u", "b": -10, "c": False, "d": 5}])
        write_table("<append=%true>" + table_path, [{"d": False}])

        VALUE_MIN = YsonEntity()
        VALUE_MIN.attributes = {"type": "min"}
        VALUE_MAX = YsonEntity()
        VALUE_MAX.attributes = {"type": "max"}
        VALUE_NULL = YsonEntity()

        self._expect_columnar_statistics(
            table_path, ["a", "b", "c", "d", "x"],
            column_min_values={"a": "t" * MAX_STRING_VALUE_LENGTH, "b": -10, "c": False, "d": 5, "x": VALUE_MIN},
            column_max_values={"a": "x" * (MAX_STRING_VALUE_LENGTH - 1) + "y", "b": 23, "c": True, "d": False, "x": VALUE_MAX},
            column_non_null_value_counts={"a": 3, "b": 3, "c": 3, "d": 2, "x": 2},
            chunk_row_count=5,
            legacy_chunk_row_count=0
        )

        self._expect_columnar_statistics(
            table_path, ["a", "b", "d", "x"], 1, 4,
            column_min_values={"a": "t" * MAX_STRING_VALUE_LENGTH, "b": -10, "d": 5, "x": 1},
            column_max_values={"a": "u", "b": 23, "d": 5, "x": 1},
            column_non_null_value_counts={"a": 2, "b": 2, "d": 1, "x": 1},
            chunk_row_count=3,
            legacy_chunk_row_count=0
        )

        self._expect_columnar_statistics(
            table_path, ["a", "b"], 3, 3,
            column_min_values={"a": None, "b": None},
            column_max_values={"a": None, "b": None},
            column_non_null_value_counts={"a": 0, "b": 0},
            chunk_row_count=0,
            legacy_chunk_row_count=0
        )

        self._expect_columnar_statistics(
            table_path, ["a", "b", "c"], upper_row_index=1,
            column_min_values={"a": "x" * MAX_STRING_VALUE_LENGTH, "b": 12, "c": VALUE_NULL},
            column_max_values={"a": "x" * (MAX_STRING_VALUE_LENGTH - 1) + "y", "b": 12, "c": VALUE_NULL},
            column_non_null_value_counts={"a": 1, "b": 1, "c": 0},
            chunk_row_count=1,
            legacy_chunk_row_count=0
        )

    @authors("denvid")
    def test_dynamic_table_value_statistics(self):
        schema = [
            {
                "name": "a",
                "type": "int64",
                "sort_order": "ascending",
                "required": True,
            },
            {
                "name": "b",
                "type": "string",
            }
        ]
        table_path = "//tmp/t"
        VALUE_NULL = YsonEntity()

        # Setting ttl so that overwriting will be performed immediately after compaction.
        create("table", table_path, attributes={"schema": schema, "dynamic": True, "min_data_ttl": 0, "max_data_ttl": 0})
        sync_create_cells(1)
        sync_mount_table(table_path)

        insert_rows(table_path, [{"a": 5, "b": "sol"},
                                 {"a": 3, "b": "mi"}])
        insert_rows(table_path, [{"a": 4, "b": "fa"}])
        insert_rows(table_path, [{"a": 100, "b": "re"}])
        sync_compact_table(table_path)

        self._expect_columnar_statistics(
            table_path, ["a", "b", "c"],
            column_min_values={"a": 3, "b": "fa", "c": VALUE_NULL},
            column_max_values={"a": 100, "b": "sol", "c": VALUE_NULL},
            column_non_null_value_counts={"a": 4, "b": 4, "c": 0},
            chunk_row_count=4,
            legacy_chunk_row_count=0
        )

        # Add more rows.
        insert_rows(table_path, [{"a": 1, "b": "v"}])
        insert_rows(table_path, [{"a": 101, "b": "do"}])
        sync_compact_table(table_path)

        self._expect_columnar_statistics(
            table_path, ["a", "b"],
            column_min_values={"a": 1, "b": "do"},
            column_max_values={"a": 101, "b": "v"},
            column_non_null_value_counts={"a": 6, "b": 6},
            chunk_row_count=6,
            legacy_chunk_row_count=0
        )

        # Overwrite some value.
        insert_rows(table_path, [{"a": 101, "b": "lya"}])
        sync_compact_table(table_path)

        self._expect_columnar_statistics(
            table_path, ["a", "b"],
            column_min_values={"a": 1, "b": "fa"},
            chunk_row_count=6,
            legacy_chunk_row_count=0
        )

##################################################################


class TestColumnarStatisticsOperations(_TestColumnarStatisticsBase):
    ENABLE_MULTIDAEMON = False  # There are component restarts.

    @authors("max42")
    def test_map_thin_column(self):
        create("table", "//tmp/t", attributes={"optimize_for": "scan"})
        create("table", "//tmp/d")
        for i in range(10):
            write_table(
                "<append=%true>//tmp/t",
                [{"a": "x" * 90, "b": "y" * 10} for j in range(100)],
            )
        assert get("//tmp/t/@data_weight") == 101 * 10 ** 3
        self._expect_data_weight_statistics(0, 1000, "a,b", [90 * 10 ** 3, 10 * 10 ** 3])
        op = map(
            in_="//tmp/t{b}",
            out="//tmp/d",
            spec={"data_weight_per_job": 1000},
            command="echo '{a=1}'",
        )
        op.track()
        assert 9 <= get("//tmp/d/@chunk_count") <= 11

    @authors("max42")
    def test_sorted_merge_thin_column(self):
        create(
            "table",
            "//tmp/t",
            attributes={
                "optimize_for": "scan",
                "schema": [
                    {"name": "a", "sort_order": "ascending", "type": "string"},
                    {"name": "b", "type": "string"},
                ],
            },
        )
        create("table", "//tmp/d")
        for i in range(10):
            write_table(
                "<append=%true>//tmp/t",
                [{"a": "x" * 90, "b": "y" * 10} for j in range(100)],
            )
        assert get("//tmp/t/@data_weight") == 101 * 10 ** 3
        self._expect_data_weight_statistics(0, 1000, "a,b", [90 * 10 ** 3, 10 * 10 ** 3])
        op = merge(in_="//tmp/t{b}", out="//tmp/d", spec={"data_weight_per_job": 1000})
        op.track()
        assert 9 <= get("//tmp/d/@chunk_count") <= 11

    @authors("max42")
    def test_map_thin_column_dynamic(self):
        sync_create_cells(1)
        self._create_simple_dynamic_table("//tmp/t")
        set("//tmp/t/@enable_dynamic_store_read", True)
        set("//tmp/t/@optimize_for", "scan")
        set("//tmp/t/@enable_compaction_and_partitioning", False)
        sync_mount_table("//tmp/t")
        for i in range(10):
            insert_rows(
                "//tmp/t",
                [{"key": j, "value": "y" * 80} for j in range(i * 100, (i + 1) * 100)],
            )
            sync_flush_table("//tmp/t")
        create("table", "//tmp/d")
        wait(lambda: get("//tmp/t/@chunk_count") == 12)
        assert get("//tmp/t/@data_weight") == (8 + (80 + 8) + 8) * 10 ** 3
        self._expect_data_weight_statistics(
            None,
            None,
            "key,value",
            [8 * 10 ** 3, (80 + 8) * 10 ** 3],
            expected_timestamp_weight=(8 * 1000),
        )
        op = map(
            in_="//tmp/t{key}",
            out="//tmp/d",
            spec={"data_weight_per_job": 1600},
            command="echo '{a=1}'",
        )
        op.track()
        assert 9 <= get("//tmp/d/@chunk_count") <= 11

    @authors("max42")
    def test_empty_column_selector(self):
        create("table", "//tmp/t")
        create("table", "//tmp/d")
        write_table("<append=%true>//tmp/t", [{"a": "x" * 100}] * 100)
        op = merge(in_="//tmp/t{}", out="//tmp/d", spec={"data_weight_per_job": 10})
        op.track()
        assert 9 <= get("//tmp/d/@chunk_count") <= 11

    @authors("max42")
    def test_table_file_in_sandbox(self):
        create("table", "//tmp/t")
        s = "x" * 100
        for i in range(5):
            write_table("<append=%true>//tmp/t", [{"a": s, "b": s, "c": s, "d": s, "e": s}])
        with pytest.raises(YtError):
            vanilla(
                spec={
                    "tasks": {
                        "task": {
                            "job_count": 1,
                            "command": "exit",
                            "file_paths": ["<format=dsv>//tmp/t"],
                        },
                    },
                }
            )

        vanilla(
            spec={
                "tasks": {
                    "task": {
                        "job_count": 1,
                        "command": "exit",
                        "file_paths": ["<format=dsv>//tmp/t[#1:#4]"],
                    },
                },
            }
        )

        vanilla(
            spec={
                "tasks": {
                    "task": {
                        "job_count": 1,
                        "command": "exit",
                        "file_paths": ["<format=dsv>//tmp/t{b,c,d}"],
                    },
                },
            }
        )

    @authors("max42", "ni-stoiko")
    def test_fetch_cancellation(self):
        create("table", "//tmp/t", attributes={"optimize_for": "scan"})
        create("table", "//tmp/d")
        for i in range(10):
            write_table(
                "<append=%true>//tmp/t",
                [{"a": "x" * 90, "b": "y" * 10} for j in range(100)],
            )

        # Restart controller agent to ensure our operation taking memory tagged statistics slot 0.
        with Restarter(self.Env, [CONTROLLER_AGENTS_SERVICE]):
            pass

        controller_agents = ls("//sys/controller_agents/instances")
        assert len(controller_agents) == 1
        controller_agent_orchid = "//sys/controller_agents/instances/{}/orchid/controller_agent".format(
            controller_agents[0]
        )

        op = map(
            track=False,
            in_="//tmp/t{b}",
            out="//tmp/d",
            spec={
                "data_weight_per_job": 1000,
                "testing": {"cancellation_stage": "columnar_statistics_fetch"},
            },
            command="echo '{a=1}'",
        )

        with raises_yt_error("Test operation failure"):
            op.track()

        def operation_disposed():
            return not get(controller_agent_orchid + "/tagged_memory_statistics")

        wait(operation_disposed)

    @authors("gritukan", "apollo1321")
    @pytest.mark.parametrize("mode", ["from_nodes", "from_master"])
    def test_estimated_input_statistics(self, mode):
        create(
            "table",
            "//tmp/in",
            attributes={
                "optimize_for": "scan",
                "compression_codec": "none",
                "schema": make_schema([
                    {"name": "a", "type": "string"},
                    {"name": "b", "type": "string"},
                ]),
            },
        )
        create("table", "//tmp/out")
        for i in range(10):
            write_table(
                "<append=%true>//tmp/in",
                [{"a": "x" * 90, "b": "y" * 10} for j in range(100)],
            )

        op = map(
            in_="//tmp/in{b}",
            out="//tmp/out",
            command="echo '{a=1}'",
            spec={
                "input_table_columnar_statistics": {
                    "mode": mode,
                },
            },
        )
        op.track()

        progress = get(op.get_path() + "/@progress")
        statistics = progress["estimated_input_statistics"]
        estimated_compressed_data_size = statistics["compressed_data_size"]
        estimated_uncompressed_data_size = statistics["uncompressed_data_size"]

        if mode == "from_nodes":
            job_input_statistics = progress["job_statistics_v2"]["data"]["input"]
            actual_compressed_data_size = job_input_statistics["compressed_data_size"][0]["summary"]["sum"]
            actual_uncompressed_data_size = job_input_statistics["uncompressed_data_size"][0]["summary"]["sum"]

            assert 0.99 * estimated_compressed_data_size <= actual_compressed_data_size <= 1.01 * estimated_compressed_data_size
            assert 0.99 * estimated_uncompressed_data_size <= actual_uncompressed_data_size <= 1.01 * estimated_uncompressed_data_size
        else:
            # Mode "from_master" doest not account for rle encoding, input size estimation
            # is rather inaccurate in this case.
            assert 10000 <= estimated_compressed_data_size <= 12000
            assert 10000 <= estimated_uncompressed_data_size <= 12000

    @authors("coteeq")
    @pytest.mark.parametrize("mode", ["from_nodes", "from_master", "fallback"])
    def test_columnar_statistics_mode(self, mode):
        create("table", "//tmp/t", attributes={"optimize_for": "scan"})
        create("table", "//tmp/d")
        for i in range(10):
            write_table(
                "<append=%true>//tmp/t",
                [{"a": "x" * 90, "b": "y" * 10} for j in range(100)],
            )

        def get_requests_stats():
            return {
                node: profiler_factory()
                .at_node(node)
                .with_tags({
                    "method": "GetColumnarStatistics",
                    "yt_service": "DataNodeService"
                })
                .counter("rpc/server/request_count")
                .get()
                for node in ls('//sys/cluster_nodes')
            }

        requests_by_node = get_requests_stats()

        map(
            in_="//tmp/t{b}",
            out="//tmp/d",
            spec={
                "input_table_columnar_statistics": {
                    "mode": mode,
                },
            },
            command="echo '{a=1}'",
        )

        if mode == "from_nodes":
            assert get_requests_stats() != requests_by_node
        else:
            assert get_requests_stats() == requests_by_node

    @authors("dakovalkov")
    def test_heavy_column_statistics_op_output(self):
        create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"}]})
        write_table("//tmp/t", [{"a": 1}])
        self._expect_data_weight_statistics(None, None, "a", [8], fetcher_mode="from_master")

        op = map(
            in_="//tmp/t",
            out="//tmp/t",
            command="echo '{a=2}'",
        )
        op.track()
        self._expect_data_weight_statistics(None, None, "a", [8], fetcher_mode="from_master")

        op = merge(
            in_="//tmp/t",
            out="//tmp/t",
            spec={
                "force_transform": True,
            },
        )
        op.track()
        self._expect_data_weight_statistics(None, None, "a", [8], fetcher_mode="from_master")

        op = sort(
            in_="//tmp/t",
            out="//tmp/t",
            sort_by=["a"],
        )
        op.track()
        self._expect_data_weight_statistics(None, None, "a", [8], fetcher_mode="from_master")


##################################################################


class TestColumnarStatisticsOperationsEarlyFinish(TestColumnarStatisticsOperations):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "enable_map_job_size_adjustment": False,
            "user_file_limits": {
                "max_table_data_weight": 2000,
            },
            "operation_options": {
                "spec_template": {
                    "use_columnar_statistics": True,
                },
            },
            "enable_columnar_statistics_early_finish": True,
            "fetcher": {
                "node_rpc_timeout": 3000
            },
        },
        "heap_profiler_update_snapshot_period": 100,
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "data_node": {
                "testing_options": {
                    "columnar_statistics_chunk_meta_fetch_max_delay": 6000,
                    "columnar_statistics_read_timeout_fraction": 0.3,
                },
            },
        },
    }

    DELTA_DRIVER_CONFIG = {
        "fetcher": {
            "node_rpc_timeout": 3000
        },
        "enable_large_columnar_statistics": True,
    }

    @classmethod
    def _expect_data_weight_statistics(cls, *args, **kwargs):
        if "enable_early_finish" not in kwargs:
            kwargs["enable_early_finish"] = True
        super(TestColumnarStatisticsOperationsEarlyFinish, cls)._expect_data_weight_statistics(*args, **kwargs)


##################################################################


@pytest.mark.enabled_multidaemon
class TestColumnarStatisticsCommandEarlyFinish(_TestColumnarStatisticsBase):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "data_node": {
                "testing_options": {
                    "columnar_statistics_chunk_meta_fetch_max_delay": 6000,
                    "columnar_statistics_read_timeout_fraction": 0.3,
                },
            },
        },
    }

    DELTA_DRIVER_CONFIG = {
        "fetcher": {
            "node_rpc_timeout": 3000
        },
        "enable_large_columnar_statistics": True,
    }

    @authors("achulkov2")
    def test_get_table_columnar_statistics_with_early_finish(self):
        create("table", "//tmp/t", attributes={"replication_factor": 1})
        write_table("<append=%true>//tmp/t", [{"a": "x" * 100, "b": 42}, {"c": 1.2}])
        write_table("<append=%true>//tmp/t", [{"a": "x" * 100, "b": 42}, {"c": 1.2}])
        write_table("<append=%true>//tmp/t", [{"a": "x" * 100, "b": 42}, {"c": 1.2}])
        write_table("<append=%true>//tmp/t", [{"a": "x" * 100, "b": 42}, {"c": 1.2}])
        write_table("<append=%true>//tmp/t", [{"a": "x" * 100, "b": 42}, {"c": 1.2}])
        write_table("<append=%true>//tmp/t", [{"a": "x" * 100, "b": 42}, {"c": 1.2}])
        write_table("<append=%true>//tmp/t", [{"a": "x" * 100, "b": 42}, {"c": 1.2}])
        write_table("<append=%true>//tmp/t", [{"a": "x" * 200}, {"c": True}])
        write_table("<append=%true>//tmp/t", [{"b": None, "c": 0}, {"a": "x" * 1000}])

        with pytest.raises(YtError):
            self._expect_data_weight_statistics(None, None, "a,b,c", [1900, 56, 65], enable_early_finish=False)

        self._expect_data_weight_statistics(None, None, "a,b,c", [1900, 56, 65], enable_early_finish=True)


##################################################################


@pytest.mark.enabled_multidaemon
class TestColumnarStatisticsCommandEarlyFinishRpcProxy(TestColumnarStatisticsCommandEarlyFinish):
    ENABLE_MULTIDAEMON = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

    DELTA_RPC_DRIVER_CONFIG = {
        "fetcher": {
            "node_rpc_timeout": 3000
        },
    }


##################################################################


@pytest.mark.enabled_multidaemon
class TestColumnarStatisticsRenamedColumns(_TestColumnarStatisticsBase):
    ENABLE_MULTIDAEMON = True

    @authors("levysotsky")
    def test_get_table_columnar_statistics(self):
        skip_if_renaming_disabled(self.Env)

        schema1 = make_schema([
            make_column("a", optional_type("string")),
            make_column("b", optional_type("int64")),
            make_column("c", optional_type("double")),
        ])
        schema2 = make_schema([
            make_column("a_new", optional_type("string"), stable_name="a"),
            make_column("c_new", optional_type("double"), stable_name="c"),
            make_column("b", optional_type("int64")),
        ])

        create("table", "//tmp/t", attributes={"schema": schema1})

        write_table("<append=%true>//tmp/t", [{"a": "x" * 100, "b": 42}, {"c": 1.2}])

        alter_table("//tmp/t", schema=schema2)

        write_table("<append=%true>//tmp/t", [{"a_new": "x" * 200}, {"c_new": 6.5}])
        write_table("<append=%true>//tmp/t", [{"b": None, "c_new": 0.0}, {"a_new": "x" * 1000}])

        self._expect_data_weight_statistics(2, 2, "a,b,c", [0, 0, 0])  # Note the wrong names.
        self._expect_data_weight_statistics(2, 2, "a_new,b,c_new", [0, 0, 0])
        self._expect_data_weight_statistics(0, 6, "a,b,c", [0, 8, 0])  # Note the wrong names.
        self._expect_data_weight_statistics(0, 6, "a_new,b,c_new", [1300, 8, 24])
        self._expect_data_weight_statistics(0, 6, "a_new,c_new,x", [1300, 24, 0])
        self._expect_data_weight_statistics(1, 5, "a,b,c", [0, 8, 0])  # Note the wrong names.
        self._expect_data_weight_statistics(1, 5, "a_new,b,c_new", [1300, 8, 24])
        self._expect_data_weight_statistics(2, 5, "a_new", [1200])
        self._expect_data_weight_statistics(1, 4, "", [])

        alter_table("//tmp/t", schema=schema1)

        write_table("<append=%true>//tmp/t", [{"b": None, "c": 0.0}, {"a": "x" * 1000}])

        self._expect_data_weight_statistics(2, 2, "a,b,c", [0, 0, 0])
        self._expect_data_weight_statistics(0, 6, "a,b,c", [1300, 8, 24])
        self._expect_data_weight_statistics(0, 6, "a,c,x", [1300, 24, 0])
        self._expect_data_weight_statistics(1, 5, "a,b,c", [1300, 8, 24])
        self._expect_data_weight_statistics(2, 5, "a", [1200])
        self._expect_data_weight_statistics(1, 4, "", [])

    @authors("levysotsky")
    def test_map_thin_column(self):
        schema1 = make_schema([
            make_column("a", "string"),
            make_column("b", "string"),
        ])
        schema2 = make_schema([
            make_column("b_new", "string", stable_name="b"),
            make_column("a_new", "string", stable_name="a"),
        ])
        create("table", "//tmp/t", attributes={"optimize_for": "scan", "schema": schema1})
        create("table", "//tmp/d")
        for i in range(10):
            a_name = ["a", "a_new"][i % 2]
            b_name = ["b", "b_new"][i % 2]
            schema = [schema1, schema2][i % 2]
            alter_table("//tmp/t", schema=schema)
            write_table(
                "<append=%true>//tmp/t",
                [{a_name: "x" * 90, b_name: "y" * 10} for _ in range(100)],
            )

        for a_name, b_name, schema in [("a", "b", schema1), ("a_new", "b_new", schema2)]:
            alter_table("//tmp/t", schema=schema)
            assert get("//tmp/t/@data_weight") == 101 * 10 ** 3
            self._expect_data_weight_statistics(
                0, 1000,
                "{},{}".format(a_name, b_name),
                [90 * 10 ** 3, 10 * 10 ** 3],
            )
            op = map(
                in_="//tmp/t{{{}}}".format(b_name),
                out="//tmp/d",
                spec={"data_weight_per_job": 1000},
                command="echo '{a=1}'",
            )
            op.track()
            assert 9 <= get("//tmp/d/@chunk_count") <= 11


##################################################################


@pytest.mark.enabled_multidaemon
class TestColumnarStatisticsRpcProxy(TestColumnarStatistics):
    ENABLE_MULTIDAEMON = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True


##################################################################


@pytest.mark.enabled_multidaemon
class TestColumnarStatisticsUseControllerAgentDefault(_TestColumnarStatisticsBase):
    ENABLE_MULTIDAEMON = True
    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "use_columnar_statistics_default": True,
        },
    }

    @authors("yuryalekseev")
    def test_columnar_statistics_with_map(self):
        create("table", "//tmp/t", attributes={"optimize_for": "scan"})
        create("table", "//tmp/d")
        for i in range(10):
            write_table(
                "<append=%true>//tmp/t",
                [{"a": "x" * 50, "b": "y" * 50} for j in range(10)],
            )
        assert get("//tmp/t/@data_weight") == 101 * 10 ** 2
        self._expect_data_weight_statistics(0, 100, "a,b", [50 * 10 ** 2, 50 * 10 ** 2])

        op = map(
            in_="//tmp/t{a}",
            out="//tmp/d",
            spec={"data_weight_per_job": 1000},
            command="echo '{a=1}'",
        )

        op.track()

        # Here we rely on the fact that number of jobs <= number of chunks.
        # Without the use_columnar_statistics_default option the number of
        # jobs (chunks) is 11. This test expects that with the
        # use_columnar_statistics_default option the number of jobs will be 6.
        assert 5 <= get("//tmp/d/@chunk_count") <= 7


##################################################################


@pytest.mark.enabled_multidaemon
class TestReadSizeEstimation(_TestColumnarStatisticsBase):
    ENABLE_MULTIDAEMON = True

    NUM_TEST_PARTITIONS = 12

    NUM_NODES = 16

    @staticmethod
    def make_random_string(size) -> str:
        return ''.join(random.choice(string.ascii_letters) for _ in range(size))

    @authors("apollo1321")
    @pytest.mark.parametrize("strict", [False, True])
    @pytest.mark.parametrize("mode", ["from_nodes", "from_master"])
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    @pytest.mark.parametrize("erasure_codec", ["none", "isa_reed_solomon_6_3", "isa_lrc_12_2_2"])
    @pytest.mark.parametrize("striped_erasure", [False, True])
    @pytest.mark.parametrize("use_groups", [True, False])
    def test_estimated_input_statistics_with_column_groups(self, strict, mode, optimize_for, erasure_codec, striped_erasure, use_groups):
        if striped_erasure and erasure_codec != "isa_lrc_12_2_2":
            pytest.skip()

        if not use_groups and (optimize_for != "scan" or erasure_codec != "none" or striped_erasure):
            pytest.skip()

        create("table", "//tmp/t_in", attributes={
            "schema": make_schema([
                {"name": "small", "type": "string", "group": "group_1"},
                {"name": "large_1", "type": "string", "group": "group_2"},
                {"name": "large_2", "type": "string", "group": "group_1"},
            ] if use_groups else [
                {"name": "small", "type": "string"},
                {"name": "large_1", "type": "string"},
                {"name": "large_2", "type": "string"},
            ], strict=strict),
            "optimize_for": optimize_for,
        })

        set("//tmp/t_in/@erasure_codec", erasure_codec)

        writer_config = {
            "block_size": 1,
        }

        if striped_erasure:
            writer_config["erasure_stripe_size"] = 512

        for row_batch in range(3):
            rows = []
            for index in range(50):
                rows += [
                    {
                        "small": self.make_random_string(200),
                        "large_1": self.make_random_string(400),
                        "large_2": self.make_random_string(800),
                    },
                    {
                        "large_2": self.make_random_string(1600),
                    },
                    {
                        "small": self.make_random_string(3200),
                        "large_1": self.make_random_string(6400),
                    },
                ]

                if not strict:
                    rows[1]["unknown1"] = self.make_random_string(12800)
                    rows[2]["unknown2"] = self.make_random_string(25600)

            write_table("<append=%true>//tmp/t_in", rows, table_writer=writer_config)

        create("table", "//tmp/t_out")

        columns_selectors = [
            [],
            ["small"],
            ["large_1"],
            ["large_1", "large_2"],
            ["small", "large_1", "large_2"],
            ["small", "unknown1"],
            ["large_1", "unknown1", "unknown2"],
            ["unknown1", "unknown2"],
        ]

        for columns in columns_selectors:
            op = map(
                in_="//tmp/t_in{{{}}}".format(",".join(columns)),
                out="//tmp/t_out",
                command="cat > /dev/null",
                spec={
                    "input_table_columnar_statistics": {
                        "mode": mode,
                    },
                },
            )

            progress = get(op.get_path() + "/@progress")
            actual_uncompressed_data_size = progress["job_statistics_v2"]["data"]["input"]["uncompressed_data_size"][0]["summary"]["sum"]
            actual_compressed_data_size = progress["job_statistics_v2"]["data"]["input"]["compressed_data_size"][0]["summary"]["sum"]

            estimated_uncompressed_data_size = progress["estimated_input_statistics"]["uncompressed_data_size"]
            estimated_compressed_data_size = progress["estimated_input_statistics"]["compressed_data_size"]

            delta = 0.02 if mode == "from_nodes" else 0.05

            estimated_compressed_data_size_lower_bound = min(
                estimated_compressed_data_size * (1 - delta),
                max(estimated_compressed_data_size - 10, 0))

            estimated_uncompressed_data_size_lower_bound = min(
                estimated_uncompressed_data_size * (1 - delta),
                max(estimated_uncompressed_data_size - 10, 0))

            assert estimated_compressed_data_size_lower_bound <= actual_compressed_data_size <= estimated_compressed_data_size * (1 + delta)
            assert estimated_uncompressed_data_size_lower_bound <= actual_uncompressed_data_size <= estimated_uncompressed_data_size * (1 + delta)

    @authors("apollo1321")
    @pytest.mark.parametrize("mode", ["from_nodes", "from_master"])
    def test_disable_read_size_estimation(self, mode):
        create("table", "//tmp/t_in", attributes={
            "schema": make_schema([
                {"name": "small", "type": "string", "group": "group_1"},
                {"name": "large_1", "type": "string", "group": "group_2"},
                {"name": "large_2", "type": "string", "group": "group_1"},
            ]),
            "optimize_for": "scan",
        })

        write_table("//tmp/t_in", [{
            "small": self.make_random_string(2**10),
            "large_1": self.make_random_string(5 * 2**20),
            "large_2": self.make_random_string(5 * 2**20),
        }])

        create("table", "//tmp/t_out")

        op = map(
            in_="//tmp/t_in{small}",
            out="//tmp/t_out",
            command="cat > /dev/null",
            spec={
                "input_table_columnar_statistics": {
                    "mode": mode,
                },
                "enable_read_size_estimation": False,
            },
        )

        progress = get(op.get_path() + "/@progress")
        actual_compressed_data_size = progress["job_statistics_v2"]["data"]["input"]["compressed_data_size"][0]["summary"]["sum"]
        estimated_compressed_data_size = progress["estimated_input_statistics"]["compressed_data_size"]

        assert 4 * 2**20 < actual_compressed_data_size < 7 * 2**20
        if mode == "from_master":
            assert 512 < estimated_compressed_data_size < 40 * 2**10
        else:
            assert 512 < estimated_compressed_data_size < 2 * 2**10
