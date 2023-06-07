from yt_env_setup import (YTEnvSetup)

from yt_helpers import profiler_factory

import re
import pytest
import platform
import os.path

from yt_commands import (authors, wait, read_table, ls, create, write_table, set, update_nodes_dynamic_config)


@authors("capone212")
class TestIoEngine(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 6
    NUM_SCHEDULERS = 0
    NODE_IO_ENGINE_TYPE = "thread_pool"

    def get_write_sensors(self, node):
        node_profiler = profiler_factory().at_node(node)
        return [
            node_profiler.counter(name="location/written_bytes", tags={"location_type": "store"}),
            node_profiler.counter(name="location/write/request_count", tags={"location_type": "store"}),
            node_profiler.counter(name="location/write/total_time", tags={"location_type": "store"}),
        ]

    def get_read_sensors(self, node):
        node_profiler = profiler_factory().at_node(node)
        return [
            node_profiler.counter(name="location/read_bytes", tags={"location_type": "store"}),
            node_profiler.counter(name="location/read/request_count", tags={"location_type": "store"}),
            node_profiler.counter(name="location/read/total_time", tags={"location_type": "store"}),
        ]

    def check_node_sensors(self, node_sensors):
        for sensor in node_sensors:
            value = sensor.get()
            if (value is None) or (sensor.get() == 0):
                return False
        return True

    @authors("capone212")
    def test_disk_statistics(self):
        create("table", "//tmp/t")
        REPLICATION_FACTOR = 3
        set("//tmp/t/@replication_factor", REPLICATION_FACTOR)
        nodes = ls("//sys/cluster_nodes")
        write_sensors = [
            self.get_write_sensors(node) for node in nodes
        ]
        write_table("//tmp/t", [{"a": i} for i in range(100)])
        wait(lambda: sum(1 for node_sensor in write_sensors if self.check_node_sensors(node_sensor)) >= REPLICATION_FACTOR)
        # check read stats
        read_sensors = [
            self.get_read_sensors(node) for node in nodes
        ]
        assert len(read_table("//tmp/t")) != 0
        # we should receive read stats from at least one node
        wait(lambda: any(self.check_node_sensors(node_sensor) for node_sensor in read_sensors))

    @authors("prime")
    def test_dynamic_sick_detector(self):
        create("table", "//tmp/sick")
        write_table("//tmp/sick", [{"a": i} for i in range(100)])

        def get_sick_count():
            return sum(
                profiler_factory().at_node(node).gauge(name="location/sick", fixed_tags={"location_type": "store"}).get()
                for node in ls("//sys/cluster_nodes")
            )

        assert get_sick_count() == 0

        update_nodes_dynamic_config({
            "data_node": {
                "store_location_config_per_medium": {
                    "default": {
                        "io_config": {
                            "sick_write_time_threshold": 0,
                            "sick_write_time_window": 0,
                            "sickness_expiration_timeout": 1000000,
                        }
                    }
                }
            }
        })

        write_table("//tmp/sick", [{"a": i} for i in range(100)])
        wait(lambda: get_sick_count() > 0)

    @authors("prime")
    def test_dynamic_io_engine(self):
        create("table", "//tmp/table")

        def use_engine(name):
            update_nodes_dynamic_config({
                "data_node": {
                    "store_location_config_per_medium": {
                        "default": {
                            "io_engine_type": name
                        }
                    }
                }
            })

            def enabled_engines():
                return sum(
                    profiler_factory().at_node(node).gauge(name="location/engine_enabled", fixed_tags={"location_type": "store", "engine_type": name}).get()
                    for node in ls("//sys/cluster_nodes")
                )

            wait(lambda: enabled_engines() == self.NUM_NODES)

            write_table("//tmp/table", [{"a": i} for i in range(100)])

        use_engine("thread_pool")
        use_engine("fair_share_thread_pool")

        if not is_uring_supported() or is_uring_disabled():
            return

        # uring is broken in CI
        # use_engine("uring")


def parse_version(vstring):
    pattern = r'^(\d+)\.(\d+)\.(\d+).*'
    match = re.match(pattern, vstring)
    if not match:
        raise ValueError("invalid version number '%s'" % vstring)
    (major, minor, patch) = match.group(1, 2, 3)
    return tuple(map(int, [major, minor, patch]))


def is_uring_supported():
    if platform.system() != "Linux":
        return False
    supported = False
    try:
        supported = parse_version(platform.release()) >= (5, 4, 0)
    finally:
        return supported


def is_uring_disabled():
    proc_file = "/proc/sys/kernel/io_uring_perm"
    if not os.path.exists(proc_file):
        return False
    with open(proc_file, "r") as myfile:
        return myfile.read() == '0'


@authors("capone212")
@pytest.mark.skip("YT-15905 io_uring is broken in CI")
@pytest.mark.skipif(not is_uring_supported() or is_uring_disabled(), reason="io_uring is not available on this host")
class TestIoEngineUringStats(TestIoEngine):
    NODE_IO_ENGINE_TYPE = "uring"
