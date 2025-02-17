from yt_env_setup import (
    YTEnvSetup,
    Restarter,
    SCHEDULERS_SERVICE,
    NODES_SERVICE,
    is_asan_build,
)


from yt_commands import (
    authors, print_debug, wait, wait_no_assert,
    wait_breakpoint, release_breakpoint, with_breakpoint,
    events_on_fs, create, create_pool, create_table,
    ls, get, set, remove, link, exists, create_network_project, create_tmpdir,
    create_user, make_ace, start_transaction, lock, list_user_tokens,
    write_file, read_table, remote_copy, get_driver,
    write_table, map, abort_op, sort,
    vanilla, run_test_vanilla, abort_job, get_job_spec,
    list_jobs, get_job, get_job_stderr, get_operation,
    sync_create_cells, get_singular_chunk_id,
    update_nodes_dynamic_config, set_node_banned, check_all_stderrs,
    assert_statistics, assert_statistics_v2, extract_statistic_v2,
    heal_exec_node,
    make_random_string, raises_yt_error, update_controller_agent_config, update_scheduler_config,
    get_supported_erasure_codecs)


import subprocess

import yt_error_codes

from yt_driver_bindings import Driver

from yt_helpers import profiler_factory, is_uring_supported, is_uring_disabled

import yt.environment.init_operations_archive as init_operations_archive
import yt.yson as yson
from yt.test_helpers import are_almost_equal
from yt.common import update, YtError

from yt.wrapper.common import generate_uuid

from flaky import flaky

from copy import deepcopy

import pytest
import time
import datetime
import os
import shutil
import re

##################################################################


def find_operation_by_mutation_id(mutation_id):
    for bucket in ls("//sys/operations"):
        for operation_id, item in get("//sys/operations/{}".format(bucket), attributes=["mutation_id"]).items():
            if item.attributes.get("mutation_id", "") == mutation_id:
                return operation_id
    return None


##################################################################


class TestSandboxTmpfs(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1
    USE_PORTO = True

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "cypress_manager": {
            "default_table_replication_factor": 1,
            "default_file_replication_factor": 1,
        }
    }

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        if not os.path.exists(cls.default_disk_path):
            os.makedirs(cls.default_disk_path)
        config["exec_node"]["slot_manager"]["locations"][0]["path"] = cls.default_disk_path

    @authors("ignat")
    def test_simple(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        op = map(
            command="cat; echo 'content' > tmpfs/file; ls tmpfs/ >&2; cat tmpfs/file >&2;",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_size": 1024 * 1024,
                    "tmpfs_path": "tmpfs",
                },
                "max_failed_job_count": 1,
            },
        )

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        content = op.read_stderr(job_ids[0]).decode("ascii")

        words = content.strip().split()
        assert ["file", "content"] == words

    @authors("ignat")
    def test_custom_tmpfs_path(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        op = map(
            command="cat; echo 'content' > my_dir/file; ls my_dir/ >&2; cat my_dir/file >&2;",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_size": 1024 * 1024,
                    "tmpfs_path": "my_dir",
                }
            },
        )

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        content = op.read_stderr(job_ids[0]).decode("ascii")
        words = content.strip().split()
        assert ["file", "content"] == words

    @authors("ignat")
    def test_tmpfs_profiling(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        map(
            track=False,
            command="cat; echo 'content' > tmpfs/file; sleep 1000;",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_size": 1024 * 1024,
                    "tmpfs_path": "tmpfs",
                },
                "max_failed_job_count": 1,
            },
        )

        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 1
        node = nodes[0]

        tmpfs_limit = profiler_factory().at_node(node).gauge("job_controller/tmpfs/limit")
        tmpfs_usage = profiler_factory().at_node(node).gauge("job_controller/tmpfs/usage")
        wait(lambda: tmpfs_limit.get() == 1024 * 1024)
        wait(lambda: tmpfs_usage.get() > 0)
        assert tmpfs_usage.get() <= 4 * 1024

    @authors("ignat")
    def test_tmpfs_statistics(self):
        op = run_test_vanilla(
            with_breakpoint("echo 'content' > tmpfs/file; BREAKPOINT;"),
            task_patch={
                "tmpfs_size": 1024 * 1024,
                "tmpfs_path": "tmpfs",
            },
            track=False,
        )

        wait_breakpoint()

        @wait_no_assert
        def check():
            statistics = get(op.get_path() + "/controller_orchid/progress/job_statistics_v2")

            max_usage = extract_statistic_v2(
                statistics,
                key="user_job.tmpfs_volumes.0.max_usage",
                job_state="running",
                job_type="task",
                summary_type="sum")
            assert max_usage is not None
            assert 0 < max_usage and max_usage <= 4 * 1024

            limit = extract_statistic_v2(
                statistics,
                key="user_job.tmpfs_volumes.0.limit",
                job_state="running",
                job_type="task",
                summary_type="sum")
            assert limit is not None
            assert limit == 1024 * 1024

    @authors("ignat")
    def test_dot_tmpfs_path(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        op = map(
            command="cat; mkdir my_dir; echo 'content' > my_dir/file; ls my_dir/ >&2; cat my_dir/file >&2;",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_size": 1024 * 1024,
                    "tmpfs_path": ".",
                }
            },
        )

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        content = op.read_stderr(job_ids[0]).decode("ascii")
        words = content.strip().split()
        assert ["file", "content"] == words

        create("file", "//tmp/test_file")
        write_file("//tmp/test_file", b"".join([b"0"] * (1024 * 1024 + 1)))
        map(
            command="cat",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_size": 1024 * 1024,
                    "tmpfs_path": ".",
                    "file_paths": ["//tmp/test_file"],
                }
            },
        )

        map(
            command="cat",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_size": 1024 * 1024,
                    "tmpfs_path": "./",
                    "file_paths": ["//tmp/test_file"],
                }
            },
        )

        script = (
            b"#!/usr/bin/env python3\n"
            b"import sys\n"
            b"sys.stdout.write(sys.stdin.read())\n"
            b"with open('test_file', 'w') as f: f.write('Hello world!')"
        )
        create("file", "//tmp/script")
        write_file("//tmp/script", script)
        set("//tmp/script/@executable", True)

        map(
            command="./script.py",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_size": 100 * 1024 * 1024,
                    "tmpfs_path": ".",
                    "copy_files": True,
                    "file_paths": [
                        "//tmp/test_file",
                        yson.to_yson_type("//tmp/script", attributes={"file_name": "script.py"}),
                    ],
                }
            },
        )

        with pytest.raises(YtError):
            map(
                command="cat; cp test_file local_file;",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                spec={
                    "mapper": {
                        "tmpfs_size": 1024 * 1024,
                        "tmpfs_path": ".",
                        "file_paths": ["//tmp/test_file"],
                    },
                    "max_failed_job_count": 1,
                },
            )

        op = map(
            command="cat",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_size": 1024 * 1024 + 10000,
                    "tmpfs_path": ".",
                    "file_paths": ["//tmp/test_file"],
                    "copy_files": True,
                },
                "max_failed_job_count": 1,
            },
        )

        wait(lambda: assert_statistics(
            op,
            "user_job.tmpfs_volumes.0.max_size",
            lambda tmpfs_size: 0.9 * 1024 * 1024 <= tmpfs_size <= 1.1 * 1024 * 1024))

        with pytest.raises(YtError):
            map(
                command="cat",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                spec={
                    "mapper": {
                        "tmpfs_size": 1024 * 1024,
                        "tmpfs_path": ".",
                        "file_paths": ["//tmp/test_file"],
                        "copy_files": True,
                    },
                    "max_failed_job_count": 1,
                },
            )

        # Per-file `copy_file' attribute has higher priority than `copy_files' in spec.
        with pytest.raises(YtError):
            map(
                command="cat",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                spec={
                    "mapper": {
                        "tmpfs_size": 1024 * 1024,
                        "tmpfs_path": ".",
                        "file_paths": ["<copy_file=%true>//tmp/test_file"],
                        "copy_files": False,
                    },
                    "max_failed_job_count": 1,
                },
            )
        map(
            command="cat",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_size": 1024 * 1024,
                    "tmpfs_path": ".",
                    "file_paths": ["<copy_file=%false>//tmp/test_file"],
                    "copy_files": True,
                },
                "max_failed_job_count": 1,
            },
        )

    @authors("ignat")
    def test_incorrect_tmpfs_path(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        with pytest.raises(YtError):
            map(
                command="cat",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                spec={
                    "mapper": {
                        "tmpfs_size": 1024 * 1024,
                        "tmpfs_path": "../",
                    }
                },
            )

        with pytest.raises(YtError):
            map(
                command="cat",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                spec={
                    "mapper": {
                        "tmpfs_size": 1024 * 1024,
                        "tmpfs_path": "/tmp",
                    }
                },
            )

    @authors("ignat")
    def test_tmpfs_remove_failed(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        with pytest.raises(YtError):
            map(
                command="cat; rm -rf tmpfs",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                spec={
                    "mapper": {
                        "tmpfs_size": 1024 * 1024,
                        "tmpfs_path": "tmpfs",
                    },
                    "max_failed_job_count": 1,
                },
            )

    @authors("ignat")
    def test_tmpfs_size_limit(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        with pytest.raises(YtError):
            map(
                command="set -e; cat; dd if=/dev/zero of=tmpfs/file bs=1100000 count=1",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                spec={"mapper": {"tmpfs_size": 1024 * 1024}, "max_failed_job_count": 1},
            )

    @authors("ignat")
    @pytest.mark.skipif(is_asan_build(), reason="Memore dependent tests are not working under ASAN")
    def test_memory_reserve_and_tmpfs(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        op = map(
            command="python3 -c 'import time; x = \"0\" * (200 * 1000 * 1000); time.sleep(2)'",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {"tmpfs_path": "tmpfs", "memory_limit": 350 * 1000 * 1000},
                "max_failed_job_count": 1,
            },
        )

        assert get(op.get_path() + "/@progress/jobs/aborted/total") == 0

    @authors("gritukan")
    def test_mapped_file_memory_accounting(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        mapper = b"""#!/usr/bin/env python3

import mmap, time

f = open('tmpfs/f', 'r+b')
mm = mmap.mmap(f.fileno(), 0)

s = mm.read()

time.sleep(10)
"""
        create("file", "//tmp/mapper.py")
        write_file("//tmp/mapper.py", mapper)
        set("//tmp/mapper.py/@executable", True)

        # String is in process' memory twice: one copy is a mapped tmpfs file and one copy is a local variable s.
        # Process' mmap of tmpfs should not be counted.
        op = map(
            command="fallocate -l 200M tmpfs/f; python3 mapper.py",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            file="//tmp/mapper.py",
            spec={
                "mapper": {
                    "tmpfs_path": "tmpfs",
                    "memory_limit": 430 * 1024 * 1024,
                    "use_smaps_memory_tracker": True,
                },
                "max_failed_job_count": 1,
            },
        )
        op.track()
        assert get(op.get_path() + "/@progress/jobs/aborted/total") == 0

        wait(lambda: assert_statistics(op, "user_job.max_memory", lambda max_memory: max_memory > 200 * 1024 * 1024))

        # Smaps memory tracker is disabled. Job should fail.
        with pytest.raises(YtError):
            memory_limit = 430 * 1024 * 1024
            op = map(
                command="fallocate -l 200M tmpfs/f; python3 mapper.py",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                file="//tmp/mapper.py",
                spec={
                    "mapper": {
                        "tmpfs_path": "tmpfs",
                        "memory_limit": memory_limit,
                        "use_smaps_memory_tracker": False,
                    },
                    "max_failed_job_count": 1,
                },
            )
            wait(lambda: assert_statistics(
                op,
                "user_job.max_memory",
                lambda max_memory: max_memory > memory_limit,
                job_state="failed"))

        # String is in memory twice: one copy is mapped non-tmpfs file and one copy is a local variable s.
        # Both allocations should be counted.
        with pytest.raises(YtError):
            memory_limit = 300 * 1024 * 1024
            op = map(
                command="fallocate -l 200M tmpfs/f; python3 mapper.py",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                file="//tmp/mapper.py",
                spec={
                    "mapper": {
                        "tmpfs_path": "other_tmpfs",
                        "memory_limit": memory_limit,
                        "use_smaps_memory_tracker": True,
                    },
                    "max_failed_job_count": 1,
                },
            )
            wait(lambda: assert_statistics(
                op,
                "user_job.max_memory",
                lambda max_memory: max_memory > memory_limit,
                job_state="failed"))

    @authors("psushin")
    def test_inner_files(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        create("file", "//tmp/file.txt")
        write_file("//tmp/file.txt", b"{trump = moron};\n")

        map(
            command="cat; cat ./tmpfs/trump.txt",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            file=['<file_name="./tmpfs/trump.txt">//tmp/file.txt'],
            spec={
                "mapper": {
                    "tmpfs_path": "tmpfs",
                    "tmpfs_size": 1024 * 1024,
                },
                "max_failed_job_count": 1,
            },
        )

        assert get("//tmp/t_output/@row_count") == 2

    @authors("ignat")
    def test_multiple_tmpfs_volumes(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        op = map(
            command="cat; "
            "echo 'content_1' > tmpfs_1/file; ls tmpfs_1/ >&2; cat tmpfs_1/file >&2;"
            "echo 'content_2' > tmpfs_2/file; ls tmpfs_2/ >&2; cat tmpfs_2/file >&2;",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_volumes": [
                        {
                            "size": 1024 * 1024,
                            "path": "tmpfs_1",
                        },
                        {
                            "size": 1024 * 1024,
                            "path": "tmpfs_2",
                        },
                    ]
                },
                "max_failed_job_count": 1,
            },
        )

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        content = op.read_stderr(job_ids[0]).decode("ascii")
        words = content.strip().split()
        assert ["file", "content_1", "file", "content_2"] == words

    @authors("ignat")
    def test_incorrect_multiple_tmpfs_volumes(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        with pytest.raises(YtError):
            map(
                command="cat",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                spec={
                    "mapper": {
                        "tmpfs_volumes": [
                            {
                                "path": "tmpfs",
                            },
                        ]
                    },
                    "max_failed_job_count": 1,
                },
            )

        with pytest.raises(YtError):
            map(
                command="cat",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                spec={
                    "mapper": {
                        "tmpfs_volumes": [
                            {
                                "size": 1024 * 1024,
                            },
                        ]
                    },
                    "max_failed_job_count": 1,
                },
            )

        with pytest.raises(YtError):
            map(
                command="cat",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                spec={
                    "mapper": {
                        "tmpfs_volumes": [
                            {
                                "path": "tmpfs",
                                "size": 1024 * 1024,
                            },
                            {
                                "path": "tmpfs/inner",
                                "size": 1024 * 1024,
                            },
                        ]
                    },
                    "max_failed_job_count": 1,
                },
            )

        with pytest.raises(YtError):
            map(
                command="cat",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                spec={
                    "mapper": {
                        "tmpfs_volumes": [
                            {
                                "path": "tmpfs/fake_inner/../",
                                "size": 1024 * 1024,
                            },
                            {
                                "path": "tmpfs/inner",
                                "size": 1024 * 1024,
                            },
                        ]
                    },
                    "max_failed_job_count": 1,
                },
            )

    @authors("ignat")
    def test_multiple_tmpfs_volumes_with_common_prefix(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        op = map(
            command="cat; "
            "echo 'content_1' > tmpfs_dir/file; ls tmpfs_dir/ >&2; cat tmpfs_dir/file >&2;"
            "echo 'content_2' > tmpfs_dir_fedor/file; ls tmpfs_dir_fedor/ >&2; cat tmpfs_dir_fedor/file >&2;",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_volumes": [
                        {
                            "size": 1024 * 1024,
                            "path": "tmpfs_dir",
                        },
                        {
                            "size": 1024 * 1024,
                            "path": "tmpfs_dir_fedor",
                        },
                    ]
                },
                "max_failed_job_count": 1,
            },
        )

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        content = op.read_stderr(job_ids[0]).decode("ascii")
        words = content.strip().split()
        assert ["file", "content_1", "file", "content_2"] == words

    @authors("ignat")
    @pytest.mark.timeout(150)
    def test_vanilla(self):
        vanilla(
            spec={
                "tasks": {
                    "a": {"job_count": 2, "command": "sleep 5", "tmpfs_volumes": []},
                    "b": {
                        "job_count": 1,
                        "command": "sleep 10",
                        "tmpfs_volumes": [
                            {
                                "path": "tmpfs",
                                "size": 1024 * 1024,
                            },
                        ],
                    },
                    "c": {
                        "job_count": 3,
                        "command": "sleep 15",
                        "tmpfs_volumes": [
                            {
                                "path": "tmpfs",
                                "size": 1024 * 1024,
                            },
                            {
                                "path": "other_tmpfs",
                                "size": 1024 * 1024,
                            },
                        ],
                    },
                },
            }
        )

    @authors("gritukan")
    def test_quota_with_tmpfs_sandbox_and_disk_space_limit(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        # Should not apply disk space limit to sandbox.
        map(
            command="cat",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_size": 1024 * 1024,
                    "tmpfs_path": ".",
                    "disk_space_limit": 1024 * 1024,
                },
                "max_failed_job_count": 1,
            },
        )


class TestTmpfsWithDiskLimit(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1
    USE_PORTO = True

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "slot_manager": {
                "disk_resources_update_period": 100,
            },
        },
        "data_node": {
            "volume_manager": {
                "enable_disk_quota": False
            },
        },
    }

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "cypress_manager": {
            "default_table_replication_factor": 1,
            "default_file_replication_factor": 1,
        }
    }

    @authors("ignat")
    def test_tmpfs_sandbox_with_disk_space_limit(self):
        update_nodes_dynamic_config({
            "exec_node": {
                "slot_manager": {
                    "check_disk_space_limit": True,
                },
            },
        })

        # Should not apply disk space limit to sandbox and check it.
        run_test_vanilla(
            track=True,
            command="dd if=/dev/zero of=local_file  bs=2M  count=1; sleep 5",
            task_patch={
                "tmpfs_size": 3 * 1000 ** 2,
                "tmpfs_path": ".",
                "disk_request": {
                    "disk_space": 1000 ** 2,
                },
            },
            spec={
                "max_failed_job_count": 1,
            },
        )


##################################################################


class TestSandboxDiskUsage(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1
    USE_PORTO = True

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "slot_manager": {
                "disk_resources_update_period": 100,
            },
        },
        "data_node": {
            "volume_manager": {
                "enable_disk_quota": False
            },
        },
    }

    @authors("abodrov")
    def test_error_message(self):
        update_nodes_dynamic_config({
            "exec_node": {
                "slot_manager": {
                    "check_disk_space_limit": True,
                },
            },
        })

        with raises_yt_error("Disk usage overdraft occurred: "):
            run_test_vanilla(
                track=True,
                command=" ; ".join([
                    "dd if=/dev/zero of=local_file  bs=2M  count=1; sleep 5",
                ]),
                task_patch={
                    "disk_request": {
                        "disk_space": 1000 ** 2,
                    },
                },
                spec={"max_failed_job_count": 1},
            )

##################################################################


class TestSandboxTmpfsOverflow(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True
    USE_PORTO = True
    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "memory": 6 * 1024 ** 3,
            }
        }
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_reporter": {
                    "reporting_period": 10,
                    "min_repeat_delay": 10,
                    "max_repeat_delay": 10,
                },
            }
        }
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "enable_job_reporter": True,
            "enable_job_spec_reporter": True,
            "enable_job_stderr_reporter": True,
        }
    }

    def setup_method(self, method):
        super(TestSandboxTmpfsOverflow, self).setup_method(method)
        sync_create_cells(1)
        init_operations_archive.create_tables_latest_version(
            self.Env.create_native_client(), override_tablet_cell_bundle="default"
        )
        self._tmpdir = create_tmpdir("jobids")

    @authors("ignat")
    @flaky(max_runs=3)
    def test_multiple_tmpfs_overflow(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        op = map(
            track=False,
            command=with_breakpoint(
                "dd if=/dev/zero of=tmpfs_1/file  bs=1M  count=512; ls tmpfs_1/ >&2; "
                "dd if=/dev/zero of=tmpfs_2/file  bs=1M  count=512; ls tmpfs_2/ >&2; "
                "BREAKPOINT; "
                "python3 -c 'import time; x = \"A\" * (200 * 1024 * 1024); time.sleep(100);'"
            ),
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_volumes": [
                        {
                            "size": 512 * 1024 ** 2,
                            "path": "tmpfs_1",
                        },
                        {
                            "size": 512 * 1024 ** 2,
                            "path": "tmpfs_2",
                        },
                    ],
                    "memory_limit": 1024 ** 3 + 200 * 1024 * 1024,
                },
                "max_failed_job_count": 1,
            },
        )

        op.ensure_running()

        jobs = wait_breakpoint(timeout=datetime.timedelta(seconds=300))
        assert len(jobs) == 1
        job = jobs[0]

        def get_tmpfs_size():
            job_info = get_job(op.id, job)
            try:
                sum = 0
                for key, value in job_info["statistics"]["user_job"]["tmpfs_volumes"].items():
                    sum += value["max_size"]["sum"]
                return sum
            except KeyError:
                print_debug(f"Job info: {job_info}")
                return 0

        wait(lambda: get_tmpfs_size() >= 1024 ** 3)

        assert op.get_state() == "running"

        release_breakpoint()

        wait(lambda: op.get_state() == "failed")

        assert op.get_error().contains_code(1200)


##################################################################


class TestDisabledSandboxTmpfs(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {"exec_node": {"slot_manager": {"enable_tmpfs": False}}}

    @authors("ignat")
    def test_simple(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        op = map(
            command="cat; echo 'content' > tmpfs/file; ls tmpfs/ >&2; cat tmpfs/file >&2;",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_size": 1024 * 1024,
                    "tmpfs_path": "tmpfs",
                }
            },
        )

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        content = op.read_stderr(job_ids[0]).decode("ascii")
        words = content.strip().split()
        assert ["file", "content"] == words


##################################################################


class TestFilesInSandbox(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "static_orchid_cache_update_period": 100,
        }
    }

    @authors("ignat")
    @flaky(max_runs=3)
    def test_operation_abort_with_lost_file(self):
        create(
            "file",
            "//tmp/script",
            attributes={"replication_factor": 1, "executable": True},
        )
        write_file("//tmp/script", b"#!/bin/bash\ncat")

        chunk_id = get_singular_chunk_id("//tmp/script")

        replicas = get("#{0}/@stored_replicas".format(chunk_id))
        assert len(replicas) == 1
        replica_to_ban = str(replicas[0])  # str() is for attribute stripping.

        banned = False
        for node in ls("//sys/cluster_nodes"):
            if node == replica_to_ban:
                set_node_banned(node, True)
                banned = True
        assert banned

        wait(lambda: get("#{0}/@replication_status/default/lost".format(chunk_id)))

        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})
        op = map(
            track=False,
            command="./script",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={"mapper": {"file_paths": ["//tmp/script"]}},
        )

        wait(lambda: op.get_job_count("running") == 1)

        time.sleep(1)
        op.abort()

        wait(
            lambda: op.get_state() == "aborted"
            and are_almost_equal(get("//sys/scheduler/orchid/scheduler/cluster/resource_usage/cpu"), 0)
        )

    @authors("gritukan")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_rename_columns_artifact_table(self, optimize_for):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "x", "type": "int64"}],
            "optimize_for": optimize_for,
        })

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t", [{"x": 42}])
        write_table("//tmp/t_in", [{"a": "b"}])

        map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            file=["<format=<format=text>yson;rename_columns={x=y}>//tmp/t"],
            command="cat t",
            spec={"mapper": {"format": yson.loads(b"<format=text>yson")}},
        )

        assert read_table("//tmp/t_out") == [{"y": 42}]

    @authors("levysotsky")
    def test_rename_columns_artifact_table_no_schema(self):
        create("table", "//tmp/t")

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t", [{"x": 42}])
        write_table("//tmp/t_in", [{"a": "b"}])

        with raises_yt_error(yt_error_codes.OperationFailedToPrepare):
            map(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                file=["<format=<format=text>yson;rename_columns={x=y}>//tmp/t"],
                command="cat t",
                spec={"mapper": {"format": yson.loads(b"<format=text>yson")}},
            )


##################################################################


class TestArtifactCacheBypass(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    USE_PORTO = True

    @authors("babenko")
    def test_bypass_artifact_cache_for_file(self):
        counters = [
            profiler_factory().at_node(node).counter("job_controller/chunk_cache/cache_bypassed_artifacts_size")
            for node in sorted(ls("//sys/cluster_nodes"))
        ]

        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        create("file", "//tmp/file")
        write_file("//tmp/file", b'{"hello": "world"}')
        op = map(
            command="cat file",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "file_paths": ["<bypass_artifact_cache=%true>//tmp/file"],
                    "output_format": "json",
                },
                "max_failed_job_count": 1,
            },
        )

        wait(lambda: assert_statistics(
            op,
            "exec_agent.artifacts.cache_bypassed_artifacts_size",
            lambda bypassed_size: bypassed_size == 18))

        wait(lambda: sum(counter.get_delta() for counter in counters) == 18)

        assert read_table("//tmp/t_output") == [{"hello": "world"}]

    @authors("babenko")
    def test_bypass_artifact_cache_for_table(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        create("table", "//tmp/table")
        write_table("//tmp/table", [{"hello": "world"}])
        map(
            command="cat table",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "file_paths": ["<bypass_artifact_cache=%true;format=json>//tmp/table"],
                    "output_format": "json",
                },
                "max_failed_job_count": 1,
            },
        )

        assert read_table("//tmp/t_output") == [{"hello": "world"}]

    @authors("gritukan")
    def test_sandbox_in_tmpfs_overflow(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        create("file", "//tmp/file")
        write_file("//tmp/file", b"A" * 10 ** 7)

        with raises_yt_error(yt_error_codes.TmpfsOverflow):
            map(
                command="cat table",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                spec={
                    "mapper": {
                        "tmpfs_size": 5 * 1024 * 1024,
                        "tmpfs_path": ".",
                        "file_paths": ["<bypass_artifact_cache=%true>//tmp/file"],
                        "output_format": "json",
                    },
                    "max_failed_job_count": 1,
                },
            )
        # In tests we crash if slot location is disabled.
        # Thus, if this test passed successfully, location was not disabled.

    @authors("gritukan")
    @pytest.mark.parametrize("bypass_artifact_cache", [False, True])
    def test_lost_artifact(self, bypass_artifact_cache):
        update_nodes_dynamic_config({
            "data_node": {
                "artifact_cache_reader": {
                    "retry_count": 1,
                    "min_backoff_time": 100,
                },
            },
        })

        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        create("file", "//tmp/file", attributes={"replication_factor": 1})
        write_file("//tmp/file", b"A" * 100)

        chunk_id = get_singular_chunk_id("//tmp/file")
        replica = str(get("#{}/@stored_replicas/0".format(chunk_id)))
        set_node_banned(replica, True)

        if bypass_artifact_cache:
            file_path = "<bypass_artifact_cache=%true>//tmp/file"
        else:
            file_path = "//tmp/file"

        op = map(
            track=False,
            command="cat table",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "file_paths": [file_path],
                    "output_format": "json",
                },
                "max_failed_job_count": 1,
            },
        )

        # Job should be aborted and location should not be disabled
        def check():
            try:
                return get("{0}/@progress/jobs/aborted/total".format(op.get_path())) > 0
            except YtError:
                return False
        wait(check, sleep_backoff=0.6)

        op.abort()


##################################################################


class TestUserJobIsolation(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    DELTA_NODE_CONFIG = {
        "exec_node": {
            "slot_manager": {
                "job_environment": {
                    "type": "porto",
                    "porto_executor": {
                        "enable_network_isolation": False
                    }
                },
            }
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "vanilla_operation_options": {
                # NB: ytserver-exec requires many threads on start.
                "user_job_options": {
                    "thread_limit_multiplier": 5,
                    "initial_thread_limit": 100,
                },
            },
        },
    }

    USE_PORTO = True

    @authors("gritukan")
    def test_create_network_project_map(self):
        create("network_project_map", "//tmp/n")

    @authors("gritukan")
    def test_network_project_in_spec(self):
        create_user("u1")
        create_user("u2")
        create_network_project("n")
        set("//sys/network_projects/n/@project_id", 0xDEADBEEF)
        set("//sys/network_projects/n/@acl", [make_ace("allow", "u1", "use")])

        # Non-existent network project. Job should fail.
        with pytest.raises(YtError):
            op = run_test_vanilla("true", task_patch={"network_project": "x"})
            op.track()

        # User `u2` is not allowed to use `n`. Job should fail.
        with pytest.raises(YtError):
            op = run_test_vanilla("true", task_patch={"network_project": "n"}, authenticated_user="u2")
            op.track()

        op = run_test_vanilla(
            with_breakpoint("echo $YT_NETWORK_PROJECT_ID >&2; hostname >&2; BREAKPOINT"),
            task_patch={"network_project": "n"},
            authenticated_user="u1",
        )

        job_id = wait_breakpoint()[0]
        network_project_id, hostname, _ = get_job_stderr(op.id, job_id).split(b"\n")
        assert network_project_id == str(int("0xDEADBEEF", base=16)).encode("ascii")
        hostname_prefix = b"slot_"
        if self.Env.get_component_version("ytserver-job-proxy").abi > (25, 1):
            hostname_prefix = b"slot-"
        assert hostname.startswith(hostname_prefix)
        release_breakpoint()
        op.track()

    @authors("psushin")
    def test_network_disabled(self):
        create_network_project("n")
        set("//sys/network_projects/n/@project_id", 0xDEADBEEF)
        set("//sys/network_projects/n/@disable_network", True)

        op = run_test_vanilla(
            with_breakpoint("ip -o a 1>&2; BREAKPOINT"),
            task_patch={"network_project": "n"},
            spec={"max_failed_job_count": 1}
        )

        # ip6tnl0 is some kind of technical interface used by porto.
        pattern = re.compile(b".: (lo|ip6tnl0)")
        job_id = wait_breakpoint()[0]
        interfaces = get_job_stderr(op.id, job_id)
        for line in interfaces.split(b'\n'):
            if line:
                r'''
                Typical response from ip -o.
                1: lo    inet 127.0.0.1/8 scope host lo\       valid_lft forever preferred_lft forever
                2: ip6tnl0    inet6 fe80::8c2d:f7ff:fe9f:eebb/64 scope link \       valid_lft forever preferred_lft forever
                '''
                assert pattern.match(line), line

        release_breakpoint()
        op.track()

    @authors("gritukan")
    def test_hostname_in_etc_hosts(self):
        create_network_project("n")
        set("//sys/network_projects/n/@project_id", 0xDEADBEEF)

        op = run_test_vanilla(
            with_breakpoint("getent hosts $(hostname) | awk '{ print $1 }' >&2; BREAKPOINT"),
            task_patch={"network_project": "n"},
        )

        job_id = wait_breakpoint()[0]
        assert b"dead:beef" in get_job_stderr(op.id, job_id)

        release_breakpoint()
        op.track()

    @authors("gritukan")
    def test_thread_limit(self):
        def run_fork_bomb(thread_count):
            cmd = "for i in $(seq 1 {}); do nohup sleep 10 & done; wait".format(thread_count)
            op = run_test_vanilla(cmd, spec={"max_failed_job_count": 1})
            op.track()

        run_fork_bomb(50)
        with pytest.raises(YtError):
            run_fork_bomb(200)

    @authors("gritukan")
    def test_thread_count_statistics(self):
        cmd = "for i in $(seq 1 32); do nohup sleep 5 & done; wait"

        def check():
            op = run_test_vanilla(cmd, spec={"max_failed_job_count": 1})
            op.track()
            return assert_statistics(
                op,
                "user_job.cpu.peak_thread_count",
                lambda thread_count: 32 <= thread_count <= 42,
                job_type="task")

        wait(lambda: check())

    @authors("don-dron")
    def test_job_processes_clean(self):
        op = run_test_vanilla(
            command="sleep 1234567",
            track=False,
            spec={
                "job_count": 1,
                "job_testing_options": {
                    "delay_before_run_job_proxy": 5000,
                }
            }
        )

        def check_state_jp(state):
            job_ids = op.list_jobs()

            if len(job_ids) == 0:
                return False

            return op.get_job_phase(job_ids[0]) == state

        wait(lambda: check_state_jp("spawning_job_proxy"))

        op.abort()

        try:
            op.track()
        except Exception:
            pass

        def check_command_ended():
            output = subprocess.check_output(["ps", "aux"]).decode("utf-8")
            process_count = 0
            for line in output.split('\n'):
                if "sleep 1234567" in line:
                    process_count += 1
            return process_count == 0

        wait(lambda: check_command_ended())


##################################################################


class TestJobStderr(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_MASTER_CONFIG = {
        "chunk_manager": {
            "allow_multiple_erasure_parts_per_node": True
        }
    }

    @authors("ignat")
    def test_stderr_ok(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        command = "cat > /dev/null;\n"\
                  "echo stderr 1>&2;\n"\
                  "echo \"{operation=\"\'\"\'$YT_OPERATION_ID\'\"\'}\';\';\n"\
                  "echo \"{job_index=$YT_JOB_INDEX}\";"

        op = map(in_="//tmp/t1", out="//tmp/t2", command=command)

        assert read_table("//tmp/t2") == [{"operation": op.id}, {"job_index": 0}]
        check_all_stderrs(op, b"stderr\n", 1)

    @authors("ignat")
    def test_stderr_failed(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        command = "echo stderr 1>&2 ; exit 1"

        op = map(track=False, in_="//tmp/t1", out="//tmp/t2", command=command, fail_fast=False)

        with pytest.raises(YtError):
            op.track()

        check_all_stderrs(op, b"stderr\n", 10)

    @authors("ignat")
    def test_stderr_limit(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        op = map(
            track=False,
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat > /dev/null; echo stderr 1>&2; exit 125",
            spec={"max_failed_job_count": 5},
        )

        # If all jobs failed then operation is also failed
        with pytest.raises(YtError):
            op.track()

        check_all_stderrs(op, b"stderr\n", 5)

    @authors("ignat")
    @pytest.mark.ignore_in_opensource_ci
    def test_stderr_max_size(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        op = map(
            in_="//tmp/t1",
            out="//tmp/t2",
            command='cat > /dev/null; python3 -c \'print("head" + "0" * 10000000); print("1" * 10000000 + "tail")\' >&2;',
            spec={"max_failed_job_count": 1, "mapper": {"max_stderr_size": 1000000}},
        )

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        stderr = op.read_stderr(job_ids[0]).strip().decode("ascii")

        # Stderr buffer size is equal to 1000000, we should add it to limit
        assert len(stderr) <= 4000000
        assert stderr[:1004] == "head" + "0" * 1000
        assert stderr[-1004:] == "1" * 1000 + "tail"
        assert "skipped" in stderr

    @authors("prime")
    def test_stderr_chunks_not_created_for_completed_jobs(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"row_id": "row_" + str(i)} for i in range(100)])

        # One job hangs, so that we can poke into transaction.
        command = """
                if [ "$YT_JOB_INDEX" -eq 1 ]; then
                    sleep 1000
                else
                    cat > /dev/null; echo message > /dev/stderr
                fi;"""

        op = map(
            track=False,
            in_="//tmp/t1",
            out="//tmp/t2",
            command=command,
            spec={"job_count": 2, "max_stderr_count": 0},
        )

        def enough_jobs_completed():
            if not exists(op.get_path() + "/@progress"):
                return False
            progress = get(op.get_path() + "/@progress")
            if "jobs" in progress and "completed" in progress["jobs"]:
                return progress["jobs"]["completed"]["total"] >= 1
            return False

        wait(enough_jobs_completed, sleep_backoff=0.6)

        stderr_tx = get(op.get_path() + "/@async_scheduler_transaction_id")
        staged_objects = get("//sys/transactions/{0}/@staged_object_ids".format(stderr_tx))
        assert sum(len(ids) for ids in staged_objects.values()) == 0, str(staged_objects)

    @authors("ignat")
    def test_stderr_of_failed_jobs(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"row_id": "row_" + str(i)} for i in range(20)])

        command = with_breakpoint(
            """
                BREAKPOINT;
                IS_FAILING_JOB=$(($YT_JOB_INDEX>=19));
                echo stderr 1>&2;
                if [ $IS_FAILING_JOB -eq 1 ]; then
                    if mkdir {lock_dir}; then
                        exit 125;
                    else
                        exit 0
                    fi;
                else
                    exit 0;
                fi;""".format(
                lock_dir=events_on_fs()._get_event_filename("lock_dir")
            )
        )
        op = map(
            track=False,
            label="stderr_of_failed_jobs",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=command,
            spec={"max_failed_job_count": 1, "max_stderr_count": 10, "job_count": 20},
        )

        release_breakpoint()
        with pytest.raises(YtError):
            op.track()

        # The default number of stderr is 10. We check that we have 11-st stderr of failed job,
        # that is last one.
        check_all_stderrs(op, b"stderr\n", 11)


class TestJobStderrMulticell(TestJobStderr):
    NUM_SECONDARY_MASTER_CELLS = 2


class TestJobStderrPorto(TestJobStderr):
    USE_PORTO = True


@authors("khlebnikov")
class TestJobStderrCri(TestJobStderr):
    JOB_ENVIRONMENT_TYPE = "cri"


##################################################################


class TestUserFiles(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_MASTER_CONFIG = {
        "chunk_manager": {
            "allow_multiple_erasure_parts_per_node": True
        }
    }

    @authors("ignat")
    @pytest.mark.parametrize("copy_files", [False, True])
    def test_file_with_integer_name(self, copy_files):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")

        write_table("//tmp/t_input", [{"hello": "world"}])

        file = "//tmp/1000"
        create("file", file)
        write_file(file, b"{value=42};\n")

        map(
            in_="//tmp/t_input",
            out=["//tmp/t_output"],
            command="cat 1000 >&2; cat",
            file=[file],
            verbose=True,
            spec={"mapper": {"copy_files": copy_files}},
        )

        assert read_table("//tmp/t_output") == [{"hello": "world"}]

    @authors("ignat")
    @pytest.mark.parametrize("copy_files", [False, True])
    def test_file_with_subdir(self, copy_files):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")

        write_table("//tmp/t_input", [{"hello": "world"}])

        file = "//tmp/test_file"
        create("file", file)
        write_file(file, b"{value=42};\n")

        map(
            in_="//tmp/t_input",
            out=["//tmp/t_output"],
            command="cat dir/my_file >&2; cat",
            file=[yson.to_yson_type("//tmp/test_file", attributes={"file_name": "dir/my_file"})],
            spec={"mapper": {"copy_files": copy_files}},
            verbose=True,
        )

        with pytest.raises(YtError):
            map(
                in_="//tmp/t_input",
                out=["//tmp/t_output"],
                command="cat dir/my_file >&2; cat",
                file=[yson.to_yson_type("//tmp/test_file", attributes={"file_name": "../dir/my_file"})],
                spec={"max_failed_job_count": 1, "mapper": {"copy_files": copy_files}},
                verbose=True,
            )

        assert read_table("//tmp/t_output") == [{"hello": "world"}]

    @authors("levysotsky")
    def test_unlinked_file(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")

        write_table("//tmp/t_input", [{"hello": "world"}])

        file = "//tmp/test_file"
        create("file", file)
        write_file(file, b"{value=42};\n")
        tx = start_transaction(timeout=30000)
        file_id = get(file + "/@id")
        assert lock(file, mode="snapshot", tx=tx)
        remove(file)

        map(
            in_="//tmp/t_input",
            out=["//tmp/t_output"],
            command="cat my_file; cat",
            file=[yson.to_yson_type("#" + file_id, attributes={"file_name": "my_file"})],
            verbose=True,
        )

        with pytest.raises(YtError):
            # TODO(levysotsky): Error is wrong.
            # Instead of '#' + file it must be '#' + file_id.
            map(
                in_="//tmp/t_input",
                out=["//tmp/t_output"],
                command="cat my_file; cat",
                file=[yson.to_yson_type("#" + file)],
                spec={"max_failed_job_count": 1},
                verbose=True,
            )

        assert read_table("//tmp/t_output") == [{"value": 42}, {"hello": "world"}]

    @authors("levysotsky")
    def test_file_names_priority(self):
        create("table", "//tmp/input")
        write_table("//tmp/input", {"foo": "bar"})
        create("table", "//tmp/output")

        file1 = "//tmp/file1"
        file2 = "//tmp/file2"
        file3 = "//tmp/file3"
        for f in [file1, file2, file3]:
            create("file", f)
            write_file(f, b'{name="' + f.encode("ascii") + b'"};\n')
        set(file2 + "/@file_name", "file2_name_in_attribute")
        set(file3 + "/@file_name", "file3_name_in_attribute")

        map(
            in_="//tmp/input",
            out="//tmp/output",
            command="cat > /dev/null; cat file1; cat file2_name_in_attribute; cat file3_name_in_path",
            file=[
                file1,
                file2,
                yson.to_yson_type(file3, attributes={"file_name": "file3_name_in_path"}),
            ],
        )

        assert read_table("//tmp/output") == [
            {"name": "//tmp/file1"},
            {"name": "//tmp/file2"},
            {"name": "//tmp/file3"},
        ]

    @authors("ignat")
    def test_with_user_files(self):
        create("table", "//tmp/input")
        write_table("//tmp/input", {"foo": "bar"})

        create("table", "//tmp/output")

        file1 = "//tmp/some_file.txt"
        file2 = "//tmp/renamed_file.txt"
        file3 = "//tmp/link_file.txt"

        create("file", file1)
        create("file", file2)

        write_file(file1, b"{value=42};\n")
        write_file(file2, b"{a=b};\n")
        link(file2, file3)

        create("table", "//tmp/table_file")
        write_table("//tmp/table_file", {"text": "info", "other": "trash"})

        map(
            in_="//tmp/input",
            out="//tmp/output",
            command="cat > /dev/null; cat some_file.txt; cat my_file.txt; cat table_file;",
            file=[
                file1,
                "<file_name=my_file.txt>" + file2,
                "<format=yson; columns=[text]>//tmp/table_file",
            ],
        )

        assert read_table("//tmp/output") == [
            {"value": 42},
            {"a": "b"},
            {"text": "info"},
        ]

        map(
            in_="//tmp/input",
            out="//tmp/output",
            command="cat > /dev/null; cat link_file.txt; cat my_file.txt;",
            file=[file3, "<file_name=my_file.txt>" + file3],
        )

        assert read_table("//tmp/output") == [{"a": "b"}, {"a": "b"}]

        with pytest.raises(YtError):
            map(
                in_="//tmp/input",
                out="//tmp/output",
                command="cat",
                file=["<format=invalid_format>//tmp/table_file"],
            )

        # missing format
        with pytest.raises(YtError):
            map(
                in_="//tmp/input",
                out="//tmp/output",
                command="cat",
                file=["//tmp/table_file"],
            )

    @authors("ignat")
    def test_empty_user_files(self):
        create("table", "//tmp/input")
        write_table("//tmp/input", {"foo": "bar"})

        create("table", "//tmp/output")

        file1 = "//tmp/empty_file.txt"
        create("file", file1)

        table_file = "//tmp/table_file"
        create("table", table_file)

        command = "cat > /dev/null; cat empty_file.txt; cat table_file"

        map(
            in_="//tmp/input",
            out="//tmp/output",
            command=command,
            file=[file1, "<format=yamr>" + table_file],
        )

        assert read_table("//tmp/output") == []

    @authors("ignat")
    def test_multi_chunk_user_files(self):
        create("table", "//tmp/input")
        write_table("//tmp/input", {"foo": "bar"})

        create("table", "//tmp/output")

        file1 = "//tmp/regular_file"
        create("file", file1)
        write_file(file1, b"{value=42};\n")
        set(file1 + "/@compression_codec", "lz4")
        write_file("<append=true>" + file1, b"{a=b};\n")

        table_file = "//tmp/table_file"
        create("table", table_file)
        write_table(table_file, {"text": "info"})
        set(table_file + "/@compression_codec", "snappy")
        write_table("<append=true>" + table_file, {"text": "info"})

        command = "cat > /dev/null; cat regular_file; cat table_file"

        map(
            in_="//tmp/input",
            out="//tmp/output",
            command=command,
            file=[file1, "<format=yson>" + table_file],
        )

        assert read_table("//tmp/output") == [
            {"value": 42},
            {"a": "b"},
            {"text": "info"},
            {"text": "info"},
        ]

    @authors("ignat")
    @pytest.mark.parametrize("erasure_codec", get_supported_erasure_codecs(["reed_solomon_6_3", "isa_reed_solomon_6_3"]))
    def test_erasure_user_files(self, erasure_codec):
        create("table", "//tmp/input")
        write_table("//tmp/input", {"foo": "bar"})

        create("table", "//tmp/output")

        create("file", "//tmp/regular_file", attributes={"erasure_coded": "lrc_12_2_2"})
        write_file("<append=true>//tmp/regular_file", b"{value=42};\n")
        write_file("<append=true>//tmp/regular_file", b"{a=b};\n")

        create(
            "table",
            "//tmp/table_file",
            attributes={"erasure_codec": erasure_codec},
        )
        write_table("<append=true>//tmp/table_file", {"text1": "info1"})
        write_table("<append=true>//tmp/table_file", {"text2": "info2"})

        command = "cat > /dev/null; cat regular_file; cat table_file"

        map(
            in_="//tmp/input",
            out="//tmp/output",
            command=command,
            file=["//tmp/regular_file", "<format=yson>//tmp/table_file"],
        )

        assert read_table("//tmp/output") == [
            {"value": 42},
            {"a": "b"},
            {"text1": "info1"},
            {"text2": "info2"},
        ]


class TestUserFilesMulticell(TestUserFiles):
    NUM_SECONDARY_MASTER_CELLS = 2


class TestUserFilesPorto(TestUserFiles):
    USE_PORTO = True


@authors("khlebnikov")
class TestUserFilesCri(TestUserFiles):
    JOB_ENVIRONMENT_TYPE = "cri"


##################################################################


class TestSecureVault(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    secure_vault = {
        "int64": 42424243,
        "uint64": yson.YsonUint64(1234),
        "string": "penguin",
        "boolean": True,
        "double": 3.14,
        "composite": {"token1": "SeNsItIvE", "token2": "InFo"},
    }

    secure_values = ("42424243", "SeNsItIvE", "InFo")

    def run_map_with_secure_vault(self, spec=None):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})
        create("table", "//tmp/t_out")
        merged_spec = {"secure_vault": self.secure_vault, "max_failed_job_count": 1}
        if spec is not None:
            merged_spec = update(merged_spec, spec)
        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec=merged_spec,
            command=with_breakpoint("""
                echo -e "{YT_SECURE_VAULT=$YT_SECURE_VAULT};"
                echo -e "{YT_SECURE_VAULT_int64=$YT_SECURE_VAULT_int64};"
                echo -e "{YT_SECURE_VAULT_uint64=$YT_SECURE_VAULT_uint64};"
                echo -e "{YT_SECURE_VAULT_string=$YT_SECURE_VAULT_string};"
                echo -e "{YT_SECURE_VAULT_boolean=$YT_SECURE_VAULT_boolean};"
                echo -e "{YT_SECURE_VAULT_double=$YT_SECURE_VAULT_double};"
                printf "{YT_SECURE_VAULT_composite=\\"%q\\"};" "$YT_SECURE_VAULT_composite"
                BREAKPOINT;
           """),
        )
        return op

    def check_content(self, res):
        assert len(res) == 7
        assert res[0] == {"YT_SECURE_VAULT": self.secure_vault}
        assert res[1] == {"YT_SECURE_VAULT_int64": self.secure_vault["int64"]}
        assert res[2] == {"YT_SECURE_VAULT_uint64": self.secure_vault["uint64"]}
        assert res[3] == {"YT_SECURE_VAULT_string": self.secure_vault["string"]}
        # Boolean values are represented with 0/1.
        assert res[4] == {"YT_SECURE_VAULT_boolean": 1}
        assert res[5] == {"YT_SECURE_VAULT_double": self.secure_vault["double"]}
        assert res[6] == {"YT_SECURE_VAULT_composite": '{"token1"="SeNsItIvE";"token2"="InFo";}'}

    @authors("ignat")
    def test_secure_vault_not_visible(self):
        op = self.run_map_with_secure_vault()
        cypress_info = str(op.get_path() + "/@")
        scheduler_info = str(get("//sys/scheduler/orchid/scheduler/operations/{0}".format(op.id)))

        wait_breakpoint()

        jobs = list(op.get_running_jobs())
        assert len(jobs) == 1
        job_id = jobs[0]

        job_spec_str = str(get_job_spec(job_id))
        for value in self.secure_values:
            assert job_spec_str.find(value) == -1

        release_breakpoint()

        op.track()

        # Check that secure environment variables is neither presented in the Cypress node of the
        # operation nor in scheduler Orchid representation of the operation.
        for info in [cypress_info, scheduler_info]:
            for value in self.secure_values:
                assert info.find(value) == -1

    @authors("ignat")
    def test_secure_vault_simple(self):
        op = self.run_map_with_secure_vault()
        release_breakpoint()
        op.track()
        res = read_table("//tmp/t_out")
        self.check_content(res)

    @authors("ignat")
    def test_secure_vault_with_revive(self):
        op = self.run_map_with_secure_vault()
        wait_breakpoint()

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        release_breakpoint()
        op.track()

        res = read_table("//tmp/t_out")
        self.check_content(res)

    @authors("ignat")
    def test_allowed_variable_names(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})
        create("table", "//tmp/t_out")
        with pytest.raises(YtError):
            map(
                track=False,
                in_="//tmp/t_in",
                out="//tmp/t_out",
                spec={"secure_vault": {"=_=": 42}},
                command="cat",
            )
        with pytest.raises(YtError):
            map(
                track=False,
                in_="//tmp/t_in",
                out="//tmp/t_out",
                spec={"secure_vault": {"x" * (2 ** 16 + 1): 42}},
                command="cat",
            )

    @authors("gepardo")
    def test_secure_vault_limit(self):
        secure_vault_len = len(yson.dumps(self.secure_vault))

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})
        create("table", "//tmp/t_out")

        update_controller_agent_config("secure_vault_length_limit", secure_vault_len - 1)
        with pytest.raises(YtError):
            map(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                spec={"secure_vault": self.secure_vault},
                command="cat",
            )

        update_controller_agent_config("secure_vault_length_limit", secure_vault_len)
        map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"secure_vault": self.secure_vault},
            command="cat",
        )

    @authors("ignat")
    def test_cypress_missing_secure_vault(self):
        update_scheduler_config("testing_options/secure_vault_creation_delay", {
            "duration": 3000,
            "type": "async",
        })

        update_scheduler_config("operations_cleaner", {
            "enable": True,
            "hard_retained_operation_count": 0,
            "clean_delay": 0,
        })

        mutation_id = generate_uuid()

        op_response = run_test_vanilla(
            spec={"secure_vault": self.secure_vault},
            command="sleep 5",
            return_response=True,
            mutation_id=mutation_id,
        )

        wait(lambda: find_operation_by_mutation_id(mutation_id))

        op_id = find_operation_by_mutation_id(mutation_id)
        op_path = "//sys/operations/{}/{}".format(op_id[-2:], op_id)
        assert get(op_path + "/@has_secure_vault")
        assert not exists(op_path + "/secure_vault")

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            time.sleep(2)

        op_response.wait()
        assert not op_response.is_ok()

        wait(lambda: not exists(op_path))

        op = run_test_vanilla(
            spec={"secure_vault": self.secure_vault},
            command="sleep 5",
            mutation_id=mutation_id,
        )

        assert op.id != op_id

        op.track()


##################################################################

class TestTemporaryTokens(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    ENABLE_HTTP_PROXY = True
    NUM_HTTP_PROXIES = 1

    USE_DYNAMIC_TABLES = True

    DELTA_PROXY_CONFIG = {
        "auth": {
            "enable_authentication": True,
        },
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "operations_cleaner": {
                "enable": True,
                "hard_retained_operation_count": 0,
                "clean_delay": 0,
                "analysis_period": 100,
                "min_archivation_retry_sleep_delay": 100,
                "max_archivation_retry_sleep_delay": 110,
                "max_removal_sleep_delay": 100,
            },
        }
    }

    def setup_method(self, method):
        super(TestTemporaryTokens, self).setup_method(method)
        sync_create_cells(1)
        init_operations_archive.create_tables_latest_version(
            self.Env.create_native_client(),
            override_tablet_cell_bundle="default",
        )

    @authors("achulkov2")
    def test_basic(self):
        create_user("alice")
        assert not list_user_tokens("alice")

        command = with_breakpoint(f"""
            export YT_PROXY={self.Env.get_proxy_address()};
            curl -X POST \"http://$YT_PROXY/api/v4/create?path=//tmp/created_from_operation&type=map_node\" \\
                -H \"Accept: application/json\" -H \"Authorization: OAuth $YT_SECURE_VAULT_YT_TOKEN\" \\
                >&2;
            BREAKPOINT;
        """)

        op = run_test_vanilla(spec={"max_failed_job_count": 1, "issue_temporary_token": True}, command=command, authenticated_user="alice")
        op_path = op.get_path()

        wait_breakpoint()

        token_node_id = get(op_path + "/@temporary_token_node_id")
        token_hash = get(f"#{token_node_id}/@key")

        alice_tokens = list_user_tokens("alice", with_metadata=True)
        assert len(alice_tokens) == 1

        assert token_hash in alice_tokens
        assert op.id in alice_tokens[token_hash]["description"]
        assert alice_tokens[token_hash]["effective_expiration"] == {"time": yson.YsonEntity(), "timeout": yson.YsonEntity()}

        assert get("//tmp/created_from_operation/@owner") == "alice"

        release_breakpoint()

        op.track()

        wait(lambda: not exists(op_path))

        assert not list_user_tokens("alice")

    @authors("achulkov2")
    def test_secure_vault_keys(self):
        create_user("alice")
        assert not list_user_tokens("alice")

        command = f"""
            export YT_PROXY={self.Env.get_proxy_address()};
            curl -X POST \"http://$YT_PROXY/api/v4/create?path=//tmp/created_from_operation_$YT_SECURE_VAULT_NODE_SUFFIX&type=map_node\" \\
                -H \"Accept: application/json\" -H \"Authorization: OAuth $YT_SECURE_VAULT_ANOTHER_YT_TOKEN\" \\
                >&2;
        """

        op = run_test_vanilla(
            spec={
                "max_failed_job_count": 1,
                "issue_temporary_token": True,
                "temporary_token_environment_variable_name": "ANOTHER_YT_TOKEN",
                "secure_vault": {"NODE_SUFFIX": "first"},
            },
            command=command,
            authenticated_user="alice")
        op.track()
        assert get("//tmp/created_from_operation_first/@owner") == "alice"

        op = run_test_vanilla(
            spec={
                "max_failed_job_count": 1,
                "issue_temporary_token": True,
                "temporary_token_environment_variable_name": "ANOTHER_YT_TOKEN",
                "secure_vault": {"NODE_SUFFIX": "second", "ANOTHER_YT_TOKEN": "i-am-not-a-token"},
            },
            command='test "$YT_SECURE_VAULT_ANOTHER_YT_TOKEN" = i-am-not-a-token',
            authenticated_user="alice")
        op.track()
        wait(lambda: not exists(op.get_path()))
        assert not list_user_tokens("alice")

    @authors("achulkov2")
    def test_revive(self):
        create_user("alice")
        assert not list_user_tokens("alice")

        command = with_breakpoint(f"""
            BREAKPOINT;
            export YT_PROXY={self.Env.get_proxy_address()};
            curl -X POST \"http://$YT_PROXY/api/v4/create?path=//tmp/created_from_operation&type=map_node\" \\
                -H \"Accept: application/json\" -H \"Authorization: OAuth $YT_SECURE_VAULT_YT_TOKEN\" \\
                >&2;
        """, breakpoint_name="first_breakpoint")
        command += with_breakpoint("BREAKPOINT;", breakpoint_name="second_breakpoint")

        op = run_test_vanilla(spec={"max_failed_job_count": 1, "issue_temporary_token": True}, command=command, authenticated_user="alice")

        wait_breakpoint("first_breakpoint")

        assert len(list_user_tokens("alice")) == 1

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            time.sleep(2)

        release_breakpoint("first_breakpoint")
        wait_breakpoint("second_breakpoint")

        # No new token should be created, value should still be in secure vault.
        assert len(list_user_tokens("alice")) == 1
        assert get("//tmp/created_from_operation/@owner") == "alice"

        release_breakpoint("second_breakpoint")

        op.track()

        wait(lambda: not exists(op.get_path()))

        # Token should still be removed, token node id is persisted through revival
        assert not list_user_tokens("alice")

    @authors("achulkov2")
    def test_token_expiration_timeout(self):
        update_scheduler_config("testing_options/operation_node_creation_delay", {
            "duration": 3000,
            "type": "async",
        })

        # This should be enough time for the remainder of the test.
        expiration_timeout = 20000
        update_scheduler_config("temporary_operation_token_expiration_timeout", expiration_timeout)

        create_user("alice")
        assert not list_user_tokens("alice")

        driver_config = deepcopy(self.Env.configs["driver"])
        driver_config["api_version"] = 4
        driver_config["scheduler"]["retry_backoff"]["invocation_count"] = 1
        driver = Driver(driver_config)

        op_response = run_test_vanilla(
            spec={"max_failed_job_count": 1, "issue_temporary_token": True},
            command="sleep 5",
            return_response=True,
            authenticated_user="alice",
            driver=driver)

        wait(lambda: len(list_user_tokens("alice")) == 1)

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        op_response.wait()
        assert not op_response.is_ok()

        alice_tokens = list_user_tokens("alice", with_metadata=True)
        assert len(alice_tokens) == 1
        list(alice_tokens.values())[0]["effective_expiration"]["timeout"] == expiration_timeout

        wait(lambda: not list_user_tokens("alice"))


##################################################################


class TestUserJobMonitoring(YTEnvSetup):
    USE_DYNAMIC_TABLES = True
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_PORTO = True

    PROFILING_PERIOD = 5 * 1000

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "job_proxy": {
                "job_proxy_heartbeat_period": 100,
            },
            "gpu_manager": {
                "testing": {
                    "test_resource": True,
                    "test_gpu_count": 8,
                },
            },
        },
        "job_resource_manager": {
            "resource_limits": {
                "user_slots": 20,
                "cpu": 20,
            },
        }
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_reporter": {
                    "reporting_period": 10,
                    "min_repeat_delay": 10,
                    "max_repeat_delay": 10,
                },
            }
        }
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "enable_job_reporter": True,
            "enable_job_spec_reporter": True,
            "enable_job_stderr_reporter": True,
            "alerts_update_period": 100,
        },
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "user_job_monitoring": {
                "max_monitored_user_jobs_per_operation": 2,
                "max_monitored_user_jobs_per_agent": 7,
            },
        },
    }

    def setup_method(self, method):
        super(TestUserJobMonitoring, self).setup_method(method)
        sync_create_cells(1)
        init_operations_archive.create_tables_latest_version(
            self.Env.create_native_client(),
            override_tablet_cell_bundle="default",
        )

    @authors("levysotsky")
    @pytest.mark.parametrize("default_sensor_names", [True, False])
    def test_basic(self, default_sensor_names):
        monitoring_config = {
            "enable": True,
            "request_gpu_monitoring": default_sensor_names,
        }
        if not default_sensor_names:
            monitoring_config["sensor_names"] = ["cpu/user", "gpu/utilization_power"]

        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT; for (( c=1; c>0; c++ )); do : ; done"),
            job_count=1,
            task_patch={
                "monitoring": monitoring_config,
                "gpu_limit": 1,
                "enable_gpu_layers": False,
            },
        )
        job_id, = wait_breakpoint()
        release_breakpoint()

        wait(lambda: "monitoring_descriptor" in get_job(op.id, job_id))

        expected_time = 10 * 1000  # 10 seconds.
        job_info = get_job(op.id, job_id)
        node = job_info["address"]
        descriptor = job_info["monitoring_descriptor"]

        profiler = profiler_factory().at_node(node)

        cpu_user_counter = profiler.counter("user_job/cpu/user", tags={"job_descriptor": descriptor})
        wait(lambda: cpu_user_counter.get_delta() >= expected_time)

        controller_agent_address = get(op.get_path() + "/@controller_agent_address")
        controller_agent_orchid = "//sys/controller_agents/instances/{}/orchid/controller_agent".format(
            controller_agent_address
        )
        incarnation_id = get("{}/incarnation_id".format(controller_agent_orchid))
        expected_job_descriptor = "{}/0".format(incarnation_id)
        assert descriptor == expected_job_descriptor

        jobs_with_descriptor = list_jobs(op.id, with_monitoring_descriptor=True)["jobs"]
        assert len(jobs_with_descriptor) == len(list_jobs(op.id)["jobs"])
        assert any(job["monitoring_descriptor"] == expected_job_descriptor for job in jobs_with_descriptor)

        assert get_operation(op.id)["brief_progress"]["registered_monitoring_descriptor_count"] == len(jobs_with_descriptor)

        for _ in range(10):
            time.sleep(0.5)
            assert profiler.get(
                "user_job/gpu/utilization_power",
                {"job_descriptor": expected_job_descriptor, "gpu_slot": "0"},
                postprocessor=float) == 0

    @authors("omgronny")
    def test_basic_map_operation(self):
        update_controller_agent_config(
            "user_job_monitoring/default_max_monitored_user_jobs_per_operation", 2)

        create_table("//tmp/t_in")
        write_table("//tmp/t_in", [{"key": 1}])
        create_table("//tmp/t_out")
        op = map(
            in_=["//tmp/t_in"],
            out=["//tmp/t_out"],
            spec={
                "mapper": {
                    "monitoring": {
                        "enable": True,
                    },
                },
                "max_failed_job_count": 0,
            },
            track=False,
            command=with_breakpoint("BREAKPOINT; for (( c=1; c>0; c++ )); do : ; done"),
        )
        job_id, = wait_breakpoint()
        release_breakpoint()

        wait(lambda: "monitoring_descriptor" in get_job(op.id, job_id))

        expected_time = 10 * 1000  # 10 seconds.
        job_info = get_job(op.id, job_id)
        node = job_info["address"]
        descriptor = job_info["monitoring_descriptor"]

        profiler = profiler_factory().at_node(node)

        cpu_user_counter = profiler.counter("user_job/cpu/user", tags={"job_descriptor": descriptor})
        wait(lambda: cpu_user_counter.get_delta() >= expected_time)

        controller_agent_address = get(op.get_path() + "/@controller_agent_address")
        controller_agent_orchid = "//sys/controller_agents/instances/{}/orchid/controller_agent".format(
            controller_agent_address
        )
        incarnation_id = get("{}/incarnation_id".format(controller_agent_orchid))
        expected_job_descriptor = "{}/0".format(incarnation_id)
        assert descriptor == expected_job_descriptor

        jobs_with_descriptor = list_jobs(op.id, with_monitoring_descriptor=True)
        assert len(jobs_with_descriptor) == len(list_jobs(op.id))
        assert any(job["monitoring_descriptor"] == expected_job_descriptor for job in jobs_with_descriptor["jobs"])

    @authors("omgronny")
    def test_profiling(self):
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=2,
            task_patch={
                "monitoring": {
                    "enable": True,
                },
            },
        )
        wait_breakpoint()

        controller_agent_address = get(op.get_path() + "/@controller_agent_address")
        controller_agent_profiler = profiler_factory().at_controller_agent(controller_agent_address)
        monitored_user_job_counter = controller_agent_profiler.gauge("controller_agent/monitored_user_job_count")

        wait(lambda: monitored_user_job_counter.get() == 2)

        release_breakpoint()

        op.wait_for_state("completed")

        wait(lambda: monitored_user_job_counter.get() == 0)

    @authors("levysotsky")
    def test_limits(self):
        controller_agents = ls("//sys/controller_agents/instances")
        assert len(controller_agents) == 1
        agent_alerts_path = "//sys/controller_agents/instances/{}/@alerts".format(
            controller_agents[0]
        )

        def get_agent_alerts():
            return get(agent_alerts_path)

        def run_op(job_count):
            breakpoint_name = make_random_string()
            op = run_test_vanilla(
                with_breakpoint("BREAKPOINT", breakpoint_name=breakpoint_name),
                job_count=job_count,
                task_patch={
                    "monitoring": {
                        "enable": True,
                        "sensor_names": ["cpu/user"],
                    },
                },
            )
            wait_breakpoint(breakpoint_name=breakpoint_name, job_count=job_count)
            op.breakpoint_name = breakpoint_name
            return op

        op = run_op(2)

        # No alerts here as limit per operation is 2.
        @wait_no_assert
        def no_alerts():
            assert len(ls(op.get_path() + "/@alerts")) == 0
            assert len(get_agent_alerts()) == 0

        release_breakpoint(breakpoint_name=op.breakpoint_name)
        op.track()

        op = run_op(3)

        # Expect an alert here as limit per operation is 2.
        @wait_no_assert
        def op_alert():
            assert ls(op.get_path() + "/@alerts") == ["user_job_monitoring_limited"]
            assert len(get_agent_alerts()) == 0

        release_breakpoint(breakpoint_name=op.breakpoint_name)
        op.track()

        ops = [run_op(2) for _ in range(3)]

        @wait_no_assert  # noqa: F811
        def no_alerts():  # noqa: F811
            for op in ops:
                # Expect no alerts here as limit per agent is 7.
                assert len(ls(op.get_path() + "/@alerts")) == 0
                assert len(get_agent_alerts()) == 0

        next_op = run_op(2)

        # Expect an alert here as limit per agent is 7.
        @wait_no_assert
        def agent_alert():
            assert ls(next_op.get_path() + "/@alerts") == ["user_job_monitoring_limited"]
            assert len(get_agent_alerts()) == 1
            assert "Limit of monitored user jobs per controller agent reached" in get_agent_alerts()[0]["message"]

        for op in ops:
            release_breakpoint(breakpoint_name=op.breakpoint_name)
            op.track()

        for job in list_jobs(next_op.id)["jobs"]:
            abort_job(job["id"])
        wait_breakpoint(breakpoint_name=next_op.breakpoint_name, job_count=2)

        @wait_no_assert  # noqa: F811
        def no_alerts():  # noqa: F811
            assert len(ls(op.get_path() + "/@alerts")) == 0
            assert len(get_agent_alerts()) == 0

        release_breakpoint(breakpoint_name=next_op.breakpoint_name)
        next_op.track()

    @authors("levysotsky")
    def test_bad_spec(self):
        with pytest.raises(YtError):
            run_test_vanilla(
                "echo",
                track=True,
                job_count=1,
                task_patch={
                    "monitoring": {
                        "enable": True,
                        "sensor_names": ["nonexistent_sensor"],
                    },
                },
            )

    @authors("ignat")
    def test_dynamic_config(self):
        update_nodes_dynamic_config({
            "exec_node": {
                "job_controller": {
                    "job_common": {
                        "user_job_monitoring": {
                            "sensors": {
                                "my_memory_reserve": {
                                    "source": "statistics",
                                    "path": "/user_job/memory_reserve",
                                    "type": "gauge",
                                    "profiling_name": "/my_job/memory_reserve",
                                }
                            }
                        }
                    },
                },
            }
        })

        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=1,
            task_patch={
                "monitoring": {
                    "enable": True,
                    "sensor_names": ["my_memory_reserve"],
                },
                "memory_limit": 1024 ** 3,
                "memory_reserve_factor": 1.0,
            },
        )

        job_id, = wait_breakpoint()

        job_info = get_job(op.id, job_id)
        node = job_info["address"]
        descriptor = job_info["monitoring_descriptor"]

        profiler = profiler_factory().at_node(node)
        memory_reserve_gauge = profiler.gauge("my_job/memory_reserve", fixed_tags={"job_descriptor": descriptor})
        wait(lambda: memory_reserve_gauge.get(default=0.0) >= 1024 ** 3)

    @authors("ignat")
    def test_descriptor_reusage(self):
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            task_patch={
                "monitoring": {
                    "enable": True,
                    "sensor_names": ["cpu/user"],
                },
            },
        )
        job1_id, = wait_breakpoint()

        wait(lambda: "monitoring_descriptor" in get_job(op.id, job1_id))

        job_info = get_job(op.id, job1_id)
        job1_descriptor = job_info["monitoring_descriptor"]

        abort_job(job1_id)

        job1_id, = wait_breakpoint()

        wait(lambda: "monitoring_descriptor" in get_job(op.id, job1_id))

        job_info = get_job(op.id, job1_id)
        job2_descriptor = job_info["monitoring_descriptor"]

        assert job1_descriptor == job2_descriptor

    @authors("eshcherbin")
    def test_no_sensors_after_job_finish(self):
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            spec={"job_testing_options": {"delay_in_cleanup": 100000}},
            task_patch={
                "monitoring": {
                    "enable": True,
                    "sensor_names": ["cpu/user"],
                },
            },
        )

        job_id, = wait_breakpoint()

        wait(lambda: "monitoring_descriptor" in get_job(op.id, job_id))

        job = get_job(op.id, job_id)
        node = job["address"]
        descriptor = job["monitoring_descriptor"]

        profiler = profiler_factory().at_node(node)
        cpu_user_counter = profiler.counter("user_job/cpu/user", tags={"job_descriptor": descriptor})
        wait(lambda: cpu_user_counter.get_delta() > 0)

        release_breakpoint()

        wait(lambda: cpu_user_counter.get() is None)

        # In order to remove the job.
        with Restarter(self.Env, NODES_SERVICE):
            pass

    @authors("omgronny")
    def test_has_monitoring_before_start(self):
        op = run_test_vanilla(
            "for (( c=1; c>0; c++ )); do : ; done",
            job_count=1,
            spec={
                "job_testing_options": {
                    "delay_before_run_job_proxy": 1000000,
                },
            },
            task_patch={
                "monitoring": {
                    "enable": True,
                    "sensor_names": ["cpu/user", "gpu/utilization_power"]
                },
                "gpu_limit": 1,
                "enable_gpu_layers": False,
            },
        )

        wait(lambda: len(list_jobs(op.id)["jobs"]) > 0)

        job_id = list_jobs(op.id)["jobs"][0]["id"]

        wait(lambda: "monitoring_descriptor" in get_job(op.id, job_id))

        job = get_job(op.id, job_id)
        node = job["address"]
        descriptor = job["monitoring_descriptor"]

        profiler = profiler_factory().at_node(node)
        wait(lambda: profiler.get(
            "user_job/cpu/user",
            {"job_descriptor": descriptor},
            postprocessor=float) == 0)
        wait(lambda: profiler.get(
            "user_job/gpu/utilization_power",
            {"job_descriptor": descriptor, "gpu_slot": "0"},
            postprocessor=float) == 0)

    @authors("eshcherbin")
    def test_request_gpu_monitoring(self):
        op = run_test_vanilla(
            "for (( c=1; c>0; c++ )); do : ; done",
            job_count=1,
            task_patch={
                "monitoring": {
                    "enable": True,
                    "sensor_names": [],
                    "request_gpu_monitoring": True,
                },
                "gpu_limit": 1,
                "enable_gpu_layers": False,
            },
        )

        wait(lambda: len(list_jobs(op.id)["jobs"]) > 0)

        job_id = list_jobs(op.id)["jobs"][0]["id"]

        wait(lambda: "monitoring_descriptor" in get_job(op.id, job_id))

        job = get_job(op.id, job_id)
        node = job["address"]
        descriptor = job["monitoring_descriptor"]

        profiler = profiler_factory().at_node(node)
        wait(lambda: profiler.get(
            "user_job/gpu/utilization_power",
            {"job_descriptor": descriptor, "gpu_slot": "0"},
            postprocessor=float) == 0)
        wait(lambda: profiler.get(
            "user_job/gpu/utilization_gpu",
            {"job_descriptor": descriptor, "gpu_slot": "0"},
            postprocessor=float) == 0)
        assert profiler.get(
            "user_job/cpu/user",
            {"job_descriptor": descriptor},
            postprocessor=float) is None


##################################################################


class TestHealExecNode(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1
    USE_PORTO = True

    DELTA_NODE_CONFIG = {
        "data_node": {
            "disk_health_checker": {
                "check_period": 1000,
            },
        },
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "slot_manager": {
                    "enable_job_environment_resurrection": True
                },
                "scheduler_connector": {
                    "heartbeat_executor": {
                        "period": 200,  # 200 msec
                    },
                },
                "controller_agent_connector": {
                    "heartbeat_executor": {
                        "period": 200,  # 200 msec
                    }
                }
            }
        }
    }

    @authors("alexkolodezny")
    def test_heal_locations(self):
        update_nodes_dynamic_config({"data_node": {"abort_on_location_disabled": False}})

        node_address = ls("//sys/cluster_nodes")[0]

        locations = get("//sys/cluster_nodes/{0}/orchid/config/exec_node/slot_manager/locations".format(node_address))

        for location in locations:
            with open("{}/disabled".format(location["path"]), "w") as f:
                f.write("{foo=bar}")

        def is_disabled():
            return get(f"//sys/cluster_nodes/{node_address}/@resource_limits/user_slots") == 0

        wait(is_disabled)

        op = run_test_vanilla("sleep 0.1")

        with raises_yt_error("Healing requested for unknown location"):
            heal_exec_node(node_address, ["unknownslot"])

        with raises_yt_error("Lock file is found"):
            heal_exec_node(node_address, ["slot0"])

        for location in locations:
            os.remove("{}/disabled".format(location["path"]))

        assert op.get_state() != "completed"

        heal_exec_node(node_address, ["slot0"])

        wait(lambda: not is_disabled())

        op.track()

        wait(lambda: op.get_state() == "completed")

        for location in locations:
            if os.path.exists("{}/disabled".format(location["path"])):
                os.remove("{}/disabled".format(location["path"]))

    @authors("ignat")
    def test_reset_alerts(self):
        update_nodes_dynamic_config({"data_node": {"abort_on_location_disabled": False}})

        node_address = ls("//sys/cluster_nodes")[0]

        locations = get("//sys/cluster_nodes/{0}/orchid/config/exec_node/slot_manager/locations".format(node_address))

        for location in locations:
            if os.path.exists("{}/disabled".format(location["path"])):
                os.remove("{}/disabled".format(location["path"]))

        job_proxy_path = os.path.join(self.bin_path, "ytserver-job-proxy")
        assert os.path.exists(job_proxy_path)

        node_address = ls("//sys/cluster_nodes")[0]
        assert not get("//sys/cluster_nodes/{}/@alerts".format(node_address))

        def has_job_proxy_build_info_missing_alert():
            return any(error["code"] == yt_error_codes.JobProxyUnavailable
                       for error in get("//sys/cluster_nodes/{}/@alerts".format(node_address)))

        job_proxy_moved_path = os.path.join(self.bin_path, "ytserver-job-proxy-moved")

        try:
            shutil.move(job_proxy_path, job_proxy_moved_path)
            wait(has_job_proxy_build_info_missing_alert)
            update_nodes_dynamic_config({
                "exec_node": {
                    "job_controller": {
                        "job_proxy_build_info_update_period": 600000,
                    }
                }
            })
        finally:
            if not os.path.exists(job_proxy_path):
                shutil.move(job_proxy_moved_path, job_proxy_path)

        wait(lambda: has_job_proxy_build_info_missing_alert())

        # TODO(arkady-e1ppa): Make this part of the raises_yt_error
        # functionality?
        try:
            with raises_yt_error("can only be fixed by a force reset"):
                heal_exec_node(node_address, alert_types_to_reset=["job_proxy_unavailable"])
        except YtError as wrong_error:
            print_debug("Attempt to reset alert \"job_proxy_unavailable\" raised a wrong exception")
            print_debug("Expected to contain: \"can only be fixed by a force reset\"")
            print_debug(f"Actual: {wrong_error}")

        heal_exec_node(node_address, alert_types_to_reset=["job_proxy_unavailable"], force_reset=True)
        wait(lambda: not get("//sys/cluster_nodes/{}/@alerts".format(node_address)))

        for location in locations:
            if os.path.exists("{}/disabled".format(location["path"])):
                os.remove("{}/disabled".format(location["path"]))

    @authors("ignat")
    def test_force_reset_persistent_alert(self):
        update_nodes_dynamic_config({"data_node": {"abort_on_location_disabled": False}})

        node_address = ls("//sys/cluster_nodes")[0]

        locations = get("//sys/cluster_nodes/{0}/orchid/config/exec_node/slot_manager/locations".format(node_address))

        for location in locations:
            if os.path.exists("{}/disabled".format(location["path"])):
                os.remove("{}/disabled".format(location["path"]))

        assert not get("//sys/cluster_nodes/{}/@alerts".format(node_address))

        update_nodes_dynamic_config({
            "exec_node": {
                "job_controller": {
                    "job_common": {
                        "waiting_for_job_cleanup_timeout": 500,
                    }
                }

            },
        })

        op = run_test_vanilla(
            "sleep 10",
            spec={"job_testing_options": {"delay_in_cleanup": 1000}, "sanity_check_delay": 60 * 1000},
        )

        wait(lambda: op.list_jobs())

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        job_id = job_ids[0]

        abort_job(job_id)

        persistent_alerts = [
            "porto_failure",
            "job_environment_failure",
        ]

        def check_persistent_alerts():
            alerts = get("//sys/cluster_nodes/{}/orchid/exec_node/slot_manager/alerts".format(node_address))

            for persistent_alert in persistent_alerts:
                if persistent_alert in alerts:
                    print_debug(f"Found alert: {persistent_alert}")
                    return True

            return False

        wait(lambda: get("//sys/cluster_nodes/{}/@alerts".format(node_address)))
        wait(check_persistent_alerts)

        wait(lambda: len(op.get_running_jobs()) == 0)

        heal_exec_node(node_address, alert_types_to_reset=persistent_alerts, force_reset=True)
        wait(lambda: not get("//sys/cluster_nodes/{}/@alerts".format(node_address)))

        def check_resurrect():
            node = ls("//sys/cluster_nodes")[0]
            alerts = get("//sys/cluster_nodes/{}/@alerts".format(node))

            return len(alerts) == 0

        wait(lambda: check_resurrect())

        op.track()

        for location in locations:
            if os.path.exists("{}/disabled".format(location["path"])):
                os.remove("{}/disabled".format(location["path"]))


##################################################################


class TestArtifactInvalidFormat(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    USE_PORTO = True

    def _prepare_tables(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"first": "second"})
        create("table", "//tmp/table")
        write_table("//tmp/table", [{"key_first": "value_first", "key_second": 42},
                                    {"key_first": "Value_second", "key_second": 43}])

    def _run_map_with_format(self, fmt):
        map(
            command='echo "{\\"key1\\": \\"value1\\", \\"size\\": $(wc -l <table)}"',
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "file_paths": ["<format=" + fmt + ">//tmp/table"],
                    "output_format": "json"
                },
                "max_failed_job_count": 1,
            },
        )

    @authors("gepardo")
    def test_bad_skiff(self):
        # This test reproduces YT-14726.
        self._prepare_tables()
        bad_skiff_format = """<
    "table_skiff_schemas" = [
        "$table"
    ];
    "skiff_schema_registry" = {
        "table" = {
            "children" = [
                {
                    "name" = "key_first";
                    "wire_type" = "string32"
                };
                {
                    "wire_type" = "int64"
                };
            ];
            "wire_type" = "tuple"
        }
    }
>skiff"""
        with raises_yt_error(yt_error_codes.InvalidFormat):
            self._run_map_with_format(bad_skiff_format)

    @authors("gepardo")
    def test_bad_protobuf(self):
        self._prepare_tables()
        bad_protobuf_format = "protobuf"
        with raises_yt_error(yt_error_codes.InvalidFormat):
            self._run_map_with_format(bad_protobuf_format)

    @authors("gepardo")
    def test_bad_format(self):
        self._prepare_tables()
        with raises_yt_error('Invalid format name "bad-format"'):
            self._run_map_with_format("bad-format")

    @authors("gepardo")
    def test_bad_dsv(self):
        self._prepare_tables()
        bad_dsv_format = "<key_value_separator = %true>dsv"
        with raises_yt_error(yt_error_codes.InvalidFormat):
            self._run_map_with_format(bad_dsv_format)

    @authors("gepardo")
    def test_bad_yamred_dsv(self):
        self._prepare_tables()
        bad_dsv_format = '<enable_escaping = "some_long_string">yamred_dsv'
        with raises_yt_error(yt_error_codes.InvalidFormat):
            self._run_map_with_format(bad_dsv_format)

    @authors("gepardo")
    def test_bad_schemaful_dsv(self):
        self._prepare_tables()
        bad_dsv_format = '<enable_escaping = "some_long_string">schemaful_dsv'
        with raises_yt_error(yt_error_codes.InvalidFormat):
            self._run_map_with_format(bad_dsv_format)

    @authors("gepardo")
    def test_bad_web_json(self):
        self._prepare_tables()
        bad_web_json_format = '<field_weight_limit = -42>web_json'
        with raises_yt_error(yt_error_codes.InvalidFormat):
            self._run_map_with_format(bad_web_json_format)


##################################################################


class TestJobProxyFailBeforeStart(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    @authors("alexkolodezny")
    def test_job_proxy_fail_before_start(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        create("table", "//tmp/t_stderr")
        write_table("//tmp/t_in", {"foo": "bar"})

        op = map(
            in_="//tmp/t_in",
            out=["//tmp/t_out"],
            command="cat",
            spec={
                "stderr_table_path": "//tmp/t_stderr",
                "job_testing_options": {"fail_before_job_start": True}
            },
            track=False)

        with raises_yt_error("Fail before job started"):
            op.track()


##################################################################


class TestConsecutiveJobAborts(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "slot_manager": {
                    "max_consecutive_job_aborts": 2,
                },
            },
        },
    }

    @authors("achulkov2")
    def test_nonexistent_intermediate_medium(self):
        v1 = {"key": "aaa"}
        v2 = {"key": "bb"}
        v3 = {"key": "bbxx"}
        v4 = {"key": "zfoo"}
        v5 = {"key": "zzz"}

        create("table", "//tmp/t_in")
        for i in range(0, 10):
            write_table("<append=true>//tmp/t_in", [v3, v5, v1, v2, v4])  # some random order

        create("table", "//tmp/t_out")

        with raises_yt_error("Job failed with fatal error"):
            sort(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                sort_by="key",
                spec={
                    "partition_count": 5,
                    "partition_job_count": 2,
                    "data_weight_per_sort_job": 1,
                    "intermediate_data_medium": "non_existent",
                },
            )

        for node, attributes in get("//sys/exec_nodes", attributes=["alerts"]).items():
            assert not attributes.attributes.get("alerts", [])


##################################################################


class TestConsecutiveJobAbortsPorto(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    USE_PORTO = True

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "gpu_manager": {
                "driver_version": "0",
                "testing": {
                    "test_resource": True,
                    "test_layers": True,
                    "test_gpu_count": 1,
                },
            },
            "job_proxy": {
                "test_root_fs": True,
            },
            "slot_manager": {
                "job_environment": {
                    "type": "porto",
                },
            },
        },
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "slot_manager": {
                    "max_consecutive_gpu_job_failures": 2,
                    "max_consecutive_job_aborts": 2,
                },
            },
        },
    }

    @authors("ignat")
    def test_consecutive_gpu_job_failures(self):
        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 1
        node = nodes[0]

        op = run_test_vanilla(
            command="sleep 2; exit 17",
            spec={
                "max_failed_job_count": 3,
            },
            task_patch={
                "job_count": 1,
                "gpu_limit": 1,
                "enable_gpu_layers": False,
            },
        )

        with raises_yt_error(yt_error_codes.MaxFailedJobsLimitExceeded):
            op.track()

        wait(lambda: get("//sys/cluster_nodes/{}/@alerts".format(node)))

        alerts = get("//sys/cluster_nodes/{}/@alerts".format(node))
        assert len(alerts) == 1

        alert = alerts[0]
        assert "Too many consecutive GPU job failures" == alert["message"]


##################################################################


class TestIdleSlots(YTEnvSetup):
    USE_PORTO = True

    NUM_SCHEDULERS = 1
    NUM_MASTERS = 1
    NUM_NODES = 1

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "slot_manager": {
                "job_environment": {
                    "type": "porto",
                },
            },
        },
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 20,
                "user_slots": 10,
            },
        }
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "slot_manager": {
                    "idle_cpu_fraction": 0.1,
                }
            }
        }
    }

    def _get_job_info(self, op):
        wait(lambda: len(op.get_running_jobs()) >= 1)
        jobs = op.get_running_jobs()
        job = jobs[list(jobs)[0]]

        job_ids = op.list_jobs()
        assert len(job_ids) >= 1
        job_id = job_ids[0]

        return (job_id, job["address"])

    def _check_slot_count(self, node, common_used, idle_used):
        slot_count = get(f"//sys/cluster_nodes/{node}/orchid/exec_node/slot_manager/slot_count")
        free_slot_count = get(f"//sys/cluster_nodes/{node}/orchid/exec_node/slot_manager/free_slot_count")
        used_idle_slot_count = get(f"//sys/cluster_nodes/{node}/orchid/exec_node/slot_manager/used_idle_slot_count")

        return (free_slot_count + common_used + idle_used == slot_count) and (used_idle_slot_count == idle_used)

    def _update_cpu_fraction(self, fraction=0.1):
        update_nodes_dynamic_config({
            "exec_node": {
                "slot_manager": {
                    "idle_cpu_fraction": fraction,
                },
            },
        })

    def _get_actual_cpu_policy(self, op, job_id):
        return op.read_stderr(job_id).decode("utf-8").strip()

    def _run_idle_operation(self, cpu_limit, breakpoint_id="default"):
        return run_test_vanilla(
            with_breakpoint("portoctl get self cpu_policy >&2; BREAKPOINT", breakpoint_id),
            task_patch={"cpu_limit": cpu_limit, "enable_porto": "isolate"},
            spec={"allow_idle_cpu_policy": True}
        )

    @authors("pogorelov", "nadya73")
    def test_schedule_in_idle_container(self):
        self._update_cpu_fraction()

        all_operations_info = []
        for cpu_limit, common_used, idle_used, expected_cpu_policy in [
            (1, 0, 1, "idle"),
            (0.5, 0, 2, "idle"),
            (0.4, 0, 3, "idle"),
            (0.2, 1, 3, "normal"),
        ]:
            op = self._run_idle_operation(cpu_limit)

            job_id, node = self._get_job_info(op)

            all_operations_info += [(op, job_id, expected_cpu_policy)]

            wait_breakpoint()

            wait(lambda: self._check_slot_count(node, common_used, idle_used))

        release_breakpoint()
        for op, job_id, expected_cpu_policy in all_operations_info:
            op.track()
            assert self._get_actual_cpu_policy(op, job_id) == expected_cpu_policy

        self._check_slot_count(node, 0, 0)

    @authors("pogorelov", "nadya73")
    def test_update_idle_cpu_fraction(self):
        self._update_cpu_fraction(fraction=0.1)
        op = self._run_idle_operation(cpu_limit=1, breakpoint_id="1")

        wait_breakpoint("1")
        job_id, _ = self._get_job_info(op)
        release_breakpoint("1")
        op.track()
        assert self._get_actual_cpu_policy(op, job_id) == "idle"

        self._update_cpu_fraction(fraction=0.04)
        op = self._run_idle_operation(cpu_limit=1, breakpoint_id="2")
        wait_breakpoint("2")
        job_id, _ = self._get_job_info(op)
        release_breakpoint("2")
        op.track()
        assert self._get_actual_cpu_policy(op, job_id) == "normal"

    @authors("pogorelov", "nadya73")
    def test_common_in_common_container(self):
        self._update_cpu_fraction()

        op = run_test_vanilla(
            with_breakpoint("portoctl get self cpu_policy >&2; BREAKPOINT"),
            task_patch={"cpu_limit": 1, "enable_porto": "isolate"},
        )

        job_id, node = self._get_job_info(op)

        wait_breakpoint()

        self._check_slot_count(node, 1, 0)

        release_breakpoint()
        op.track()

        assert self._get_actual_cpu_policy(op, job_id) == "normal"

        wait(lambda: self._check_slot_count(node, 0, 0))

    @authors("renadeen")
    def test_allow_idle_cpu_policy_on_pool(self):
        self._update_cpu_fraction()
        create_pool("idle_pool", attributes={"allow_idle_cpu_policy": True})

        op = run_test_vanilla(
            with_breakpoint("portoctl get self cpu_policy >&2; BREAKPOINT"),
            task_patch={"enable_porto": "isolate"},
            spec={"pool": "idle_pool"}
        )

        job_id, node = self._get_job_info(op)
        wait_breakpoint()

        wait(lambda: self._check_slot_count(node, 0, 1))

        release_breakpoint()
        op.track()
        assert self._get_actual_cpu_policy(op, job_id) == "idle"

        self._check_slot_count(node, 0, 0)

##################################################################


@pytest.mark.skip("Test should be overwritten (YT-20666)")
class TestCpuSet(YTEnvSetup):
    USE_PORTO = True

    NUM_SCHEDULERS = 1
    NUM_MASTERS = 1
    NUM_NODES = 1

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "slot_manager": {
                "job_environment": {
                    "type": "porto",
                },
                "numa_nodes": [
                    {
                        "numa_node_id": 0,
                        "cpu_count": 4,
                        "cpu_set": "1-3,7"
                    },
                    {
                        "numa_node_id": 1,
                        "cpu_count": 3,
                        "cpu_set": "4-6"
                    }
                ],
            },
        },
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 20,
                "user_slots": 10,
            },
        }
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "slot_manager": {
                    "idle_cpu_fraction": 0.2,
                }
            }
        }
    }

    def _get_job_info(self, op):
        wait(lambda: len(op.get_running_jobs()) >= 1)
        jobs = op.get_running_jobs()
        job = jobs[list(jobs)[0]]

        job_ids = op.list_jobs()
        assert len(job_ids) >= 1
        job_id = job_ids[0]

        return (job_id, job["address"])

    def _get_actual_cpu_set(self, op, job_id):
        return op.read_stderr(job_id).decode("utf-8").strip()

    def _enable_numa_node_scheduling(self, enable):
        update_nodes_dynamic_config({
            "exec_node": {
                "slot_manager": {
                    "enable_numa_node_scheduling": enable,
                },
            },
        })

    def _get_numa_node_free_cpu_count(self, node, numa_node_id):
        return get(f"//sys/cluster_nodes/{node}/orchid/exec_node/slot_manager/numa_node_states/node_{numa_node_id}/free_cpu_count")

    @authors("pogorelov", "nadya73")
    def test_greedy(self):
        self._enable_numa_node_scheduling(True)
        node = ""

        all_operations_info = []
        for cpu_limit, free_cpu_count_after, numa_node_id, expected_cpu_set in [
            (2, 2.0, 0, "1-3,7"),
            (2, 1.0, 1, "4-6"),
            (1, 1.0, 0, "1-3,7"),
            (2, None, None, None)
        ]:
            op = run_test_vanilla(
                with_breakpoint("portoctl get self cpu_set_affinity >&2; BREAKPOINT"),
                task_patch={"cpu_limit": cpu_limit, "enable_porto": "isolate"},
            )

            job_id, node = self._get_job_info(op)

            all_operations_info += [(op, job_id, expected_cpu_set)]

            wait_breakpoint()
            if numa_node_id is not None:
                wait(lambda: self._get_numa_node_free_cpu_count(node, numa_node_id) == free_cpu_count_after)

        release_breakpoint()
        for op, job_id, expected_cpu_set in all_operations_info:
            op.track()
            if expected_cpu_set is not None:
                assert self._get_actual_cpu_set(op, job_id) == expected_cpu_set
            else:
                assert self._get_actual_cpu_set(op, job_id)[:2] == "0-"

        wait(lambda: self._get_numa_node_free_cpu_count(node, 0) == 4.0)
        wait(lambda: self._get_numa_node_free_cpu_count(node, 1) == 3.0)

    @authors("pogorelov", "nadya73")
    def test_release_numa_nodes_cpu(self):
        self._enable_numa_node_scheduling(True)

        op_with_numa_node = run_test_vanilla(
            with_breakpoint("portoctl get self cpu_set_affinity >&2; BREAKPOINT", "1"),
            task_patch={"cpu_limit": 4, "enable_porto": "isolate"},
        )

        job_id_with_numa_node, node_with_numa_node = self._get_job_info(op_with_numa_node)

        wait_breakpoint("1")
        wait(lambda: self._get_numa_node_free_cpu_count(node_with_numa_node, 0) == 0.0)
        wait(lambda: self._get_numa_node_free_cpu_count(node_with_numa_node, 1) == 3.0)

        # Not enough cpu on any numa node for this operation.
        op_without_numa_node = run_test_vanilla(
            with_breakpoint("portoctl get self cpu_set_affinity >&2; BREAKPOINT", "2"),
            task_patch={"cpu_limit": 4, "enable_porto": "isolate"},
        )

        job_id_without_numa_node, node_without_numa_node = self._get_job_info(op_without_numa_node)

        wait_breakpoint("2")

        wait(lambda: self._get_numa_node_free_cpu_count(node_with_numa_node, 0) == 0.0)
        wait(lambda: self._get_numa_node_free_cpu_count(node_with_numa_node, 1) == 3.0)

        release_breakpoint("1")
        release_breakpoint("2")

        op_with_numa_node.track()
        op_without_numa_node.track()
        wait(lambda: get(f"//sys/cluster_nodes/{node_with_numa_node}/orchid/exec_node/slot_manager/free_slot_count") == 10)

        wait(lambda: self._get_numa_node_free_cpu_count(node_with_numa_node, 0) == 4.0)
        wait(lambda: self._get_numa_node_free_cpu_count(node_with_numa_node, 1) == 3.0)

        assert self._get_actual_cpu_set(op_with_numa_node, job_id_with_numa_node) == "1-3,7"
        assert self._get_actual_cpu_set(op_without_numa_node, job_id_without_numa_node)[:2] == "0-"

        # Here first operation was finished and we can use her numa_node again.
        op_with_numa_node = run_test_vanilla(
            with_breakpoint("portoctl get self cpu_set_affinity >&2; BREAKPOINT", "3"),
            task_patch={"cpu_limit": 4, "enable_porto": "isolate"},
        )

        job_id_with_numa_node, node_with_numa_node = self._get_job_info(op_with_numa_node)
        wait_breakpoint("3")

        wait(lambda: self._get_numa_node_free_cpu_count(node_with_numa_node, 0) == 0.0)
        wait(lambda: self._get_numa_node_free_cpu_count(node_with_numa_node, 1) == 3.0)

        release_breakpoint("3")

        op_with_numa_node.track()
        assert self._get_actual_cpu_set(op_with_numa_node, job_id_with_numa_node) == "1-3,7"

    @authors("pogorelov", "nadya73")
    def test_disable_by_config(self):
        self._enable_numa_node_scheduling(False)
        op = run_test_vanilla(
            with_breakpoint("portoctl get self cpu_set_affinity >&2; BREAKPOINT"),
            task_patch={"cpu_limit": 4, "enable_porto": "isolate"},
        )

        job_id, node = self._get_job_info(op)
        wait_breakpoint()
        release_breakpoint()
        op.track()

        assert self._get_actual_cpu_set(op, job_id)[:2] == "0-"


class TestCpuSetCri(YTEnvSetup):
    JOB_ENVIRONMENT_TYPE = "cri"

    NUM_SCHEDULERS = 1
    NUM_MASTERS = 1
    NUM_NODES = 1

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "slot_manager": {
                "numa_nodes": [
                    {
                        "numa_node_id": 0,
                        "cpu_count": 4,
                        "cpu_set": "1-3,7"
                    },
                    {
                        "numa_node_id": 1,
                        "cpu_count": 3,
                        "cpu_set": "4-6"
                    }
                ],
            },
        },
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 20,
                "user_slots": 10,
            },
        }
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "slot_manager": {
                    "enable_numa_node_scheduling": True,
                },
            },
        },
    }

    @authors("gritukan")
    def test_simple(self):
        all_operations_info = []
        for cpu_limit, expected_cpu_set in [
            (2, "1-3,7"),
            (2, "4-6"),
            (1, "1-3,7"),
        ]:
            op = run_test_vanilla(
                "taskset -cp $$ >&2; sleep 5",
                task_patch={"cpu_limit": cpu_limit},
            )

            wait(lambda: len(op.list_jobs()) >= 1)
            job_id = op.list_jobs()[0]

            all_operations_info += [(op, job_id, expected_cpu_set)]

        for op, job_id, expected_cpu_set in all_operations_info:
            op.track()
            stderr = op.read_stderr(job_id).decode("utf-8").strip()
            # pid 3252083's current affinity list: 0-63
            actual_cpu_set = stderr.split(": ")[1]
            assert actual_cpu_set == expected_cpu_set


class TestSlotManagerResurrect(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1
    USE_PORTO = True

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "cypress_manager": {
            "default_table_replication_factor": 1,
            "default_file_replication_factor": 1,
        }
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "slot_manager": {
                    "abort_on_jobs_disabled": False,
                },
            }
        }
    }

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "slot_manager": {
                "job_environment": {
                    "type": "porto",
                    "porto_executor": {
                        "api_timeout": 1000
                    },
                },
            },
            "job_proxy": {
                "test_root_fs": True,
            },
        },
    }

    def setup_files(self):
        create("file", "//tmp/layer", attributes={"replication_factor": 1})
        file_name = "layers/test.tar.gz"
        write_file(
            "//tmp/layer",
            open(file_name, "rb").read(),
            file_writer={"upload_replication_factor": 1},
        )

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        if not os.path.exists(cls.default_disk_path):
            os.makedirs(cls.default_disk_path)
        config["exec_node"]["slot_manager"]["locations"][0]["path"] = cls.default_disk_path

    @authors("don-dron")
    def test_job_env_resurrect_with_volume_and_container_leak(self):
        self.setup_files()
        nodes = ls("//sys/cluster_nodes")
        locations = get("//sys/cluster_nodes/{0}/orchid/config/exec_node/slot_manager/locations".format(nodes[0]))
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        update_nodes_dynamic_config({
            "enable_job_environment_resurrection": True,
            "job_environment": {
                "type": "porto",
                "porto_executor": {
                    "api_timeout": 1000
                },
            },
        }, path="exec_node/slot_manager")

        def check_enable():
            node = ls("//sys/cluster_nodes")[0]
            alerts = get("//sys/cluster_nodes/{}/@alerts".format(node))
            print_debug(alerts)

            return len(alerts) == 0

        wait(lambda: check_enable())

        ##################################################################

        def run_op(time=30):
            return run_test_vanilla(
                command="sleep {}".format(time),
                task_patch={
                    "max_failed_job_count": 1,
                    "layer_paths": ["//tmp/layer"]
                }
            )

        op = run_op(1)

        wait(lambda: get(op.get_path() + "/@state") == "completed")

        op = run_op()

        wait(lambda: get(op.get_path() + "/@state") == "running")

        wait(lambda: len(op.list_jobs()) == 1)

        job = op.list_jobs()[0]
        wait(lambda: get_job(op.id, job)["state"] == "running")

        update_nodes_dynamic_config({
            "enable_test_porto_failures": True,
        }, path="exec_node/slot_manager/job_environment/porto_executor")

        def check_disable():
            node = ls("//sys/cluster_nodes")[0]
            alerts = get("//sys/cluster_nodes/{}/@alerts".format(node))

            if len(alerts) == 0:
                return False
            else:
                for alert in alerts:
                    if alert['message'] == "Scheduler jobs disabled" or alert['message'] == 'Job environment disabling':
                        return True

                return False

        wait(lambda: check_disable())

        abort_op(op.id)
        wait(lambda: get(op.get_path() + "/@state") == "failed" or get(op.get_path() + "/@state") == "aborted")

        ##################################################################

        update_nodes_dynamic_config({
            "enable_test_porto_not_responding": False,
            "enable_test_porto_failures": False,
        }, path="exec_node/slot_manager/job_environment/porto_executor")

        def check_resurrect():
            node = ls("//sys/cluster_nodes")[0]
            alerts = get("//sys/cluster_nodes/{}/@alerts".format(node))

            return len(alerts) == 0

        wait(lambda: check_resurrect())

        ##################################################################

        op = run_op(0)

        wait(lambda: get(op.get_path() + "/@state") == "completed")

        for location in locations:
            path = "{}/disabled".format(location["path"])
            if os.path.exists(path):
                os.remove(path)

    @authors("don-dron")
    @pytest.mark.skip(reason="Test broken")
    def test_simple_job_env_resurrect(self):
        nodes = ls("//sys/cluster_nodes")
        locations = get("//sys/cluster_nodes/{0}/orchid/config/exec_node/slot_manager/locations".format(nodes[0]))
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        update_nodes_dynamic_config({
            "exec_node": {
                "slot_manager": {
                    "enable_job_environment_resurrection": True,
                    "job_environment": {
                        "type": "porto",
                    }
                }
            },
        })

        def check_enable():
            node = ls("//sys/cluster_nodes")[0]
            alerts = get("//sys/cluster_nodes/{}/@alerts".format(node))

            return len(alerts) == 0

        ##################################################################

        def run_op():
            return map(
                command="cat; echo 'content' > tmpfs/file; ls tmpfs/ >&2; cat tmpfs/file >&2;",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                spec={
                    "mapper": {
                        "tmpfs_size": 1024 * 1024,
                        "tmpfs_path": "tmpfs",
                    },
                    "max_failed_job_count": 1,
                },
            )

        op = run_op()

        wait(lambda: get(op.get_path() + "/@state") == "completed")

        ##################################################################

        update_nodes_dynamic_config({
            "porto_executor": {
                "enable_test_porto_failures": True
            }
        }, path="exec_node/slot_manager/job_environment")

        def check_disable():
            node = ls("//sys/cluster_nodes")[0]
            alerts = get("//sys/cluster_nodes/{}/@alerts".format(node))

            if len(alerts) == 0:
                return False
            else:
                for alert in alerts:
                    if alert['message'] == "Scheduler jobs disabled":
                        return True

                return False

        wait(lambda: check_disable())

        ##################################################################
        update_nodes_dynamic_config({
            "porto_executor": {
                "enable_test_porto_failures": False,
            }
        }, path="exec_node/slot_manager/job_environment")

        def check_resurrect():
            node = ls("//sys/cluster_nodes")[0]
            alerts = get("//sys/cluster_nodes/{}/@alerts".format(node))

            return len(alerts) == 0

        wait(lambda: check_resurrect())

        ##################################################################

        op = run_op()

        wait(lambda: get(op.get_path() + "/@state") == "completed")

        for location in locations:
            path = "{}/disabled".format(location["path"])
            if os.path.exists(path):
                os.remove(path)


##################################################################


class TestGpuStatistics(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "gpu_manager": {
                "testing": {
                    "test_resource": True,
                    "test_gpu_count": 1,
                    "test_utilization_gpu_rate": 0.5,
                },
            },
        },
    }

    @authors("eshcherbin")
    def test_do_not_account_prepare_time(self):
        op = run_test_vanilla(
            command="sleep 1",
            spec={
                "job_testing_options": {"delay_after_node_directory_prepared": 10000},
            },
            task_patch={
                "gpu_limit": 1,
                "enable_gpu_layers": False,
            },
            track=True,
        )

        wait(lambda: assert_statistics_v2(
            op,
            "user_job.gpu.cumulative_utilization_gpu",
            lambda utilization: 500 <= utilization <= 2500,
            job_type="task",
        ))


##################################################################


class TestCriJobStatistics(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    JOB_ENVIRONMENT_TYPE = "cri"

    @authors("gritukan")
    def test_job_statistics(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        op = map(
            command="python3 -c 'import time; x = \"X\" * (200 * 1000 * 1000); time.sleep(5)'",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "max_failed_job_count": 1,
            },
        )

        assert op.get_statistics()["job"]["memory"]["rss"][0]["summary"]["max"] > 200 * 1000 * 1000


##################################################################


class TestPortoFuseDevice(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    USE_PORTO = True

    @authors("ignat")
    def test_fuse_device(self):
        op = run_test_vanilla(
            command=with_breakpoint("stat /dev/fuse >&2; BREAKPOINT"),
            task_patch={
                "enable_fuse": True,
            },
        )

        job_id = wait_breakpoint()[0]

        assert b"File: /dev/fuse" in get_job_stderr(op.id, job_id)


##################################################################


class TestJobInputCache(YTEnvSetup):
    NUM_TEST_PARTITIONS = 5
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 15

    JOB_COUNT = 5

    SORTED_SCHEMA = [
        {"name": "key", "type": "int64", "sort_order": "ascending"},
        {"name": "value", "type": "string"},
    ]

    UNSORTED_SCHEMA = [
        {"name": "key", "type": "int64"},
        {"name": "value", "type": "string"},
    ]

    DELTA_NODE_CONFIG = {
        "resource_limits": {
            "memory_limits": {
                "lookup_rows_cache": {
                    "type": "static",
                    "value": 0,
                },
                "block_cache": {
                    "type": "static",
                    "value": 0,
                }
            },
            "tablet_static": {
                "type": "static",
                "value": 0,
            }
        },
        "data_node": {
            "p2p": {
                "enabled": False,
            },
        },
        "exec_node_is_not_data_node": True
    }

    DELTA_NODE_FLAVORS = [["data"] for _ in range(NUM_NODES)]
    DELTA_NODE_FLAVORS[0] = ["exec"]
    DELTA_NODE_FLAVORS[1] = ["data", "exec"]

    NUM_REMOTE_CLUSTERS = 1

    NUM_MASTERS_REMOTE_0 = 1
    NUM_SCHEDULERS_REMOTE_0 = 0

    REMOTE_CLUSTER_NAME = "remote_0"

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 500,
            "remote_copy_operation_options": {
                "spec_template": {
                    "use_remote_master_caches": True,
                },
            },
        },
    }

    def _set_config(self):
        update_nodes_dynamic_config({
            "data_node": {
                "p2p": {
                    "enabled": False,
                },
            },
            "exec_node": {
                "job_input_cache": {
                    "summary_block_size_in_flight": 0,
                    "enabled": True,
                    "block_cache": {
                        "compressed_data": {
                            "capacity": 256 * 1024 * 1024
                        }
                    },
                    "meta_cache": {
                        "capacity": 256 * 1024 * 1024
                    }
                }
            }
        })

    def _seed_counter(self, node, path):
        return profiler_factory().at_node(node).counter(path)

    def _seed_gauge(self, node, path):
        return profiler_factory().at_node(node).gauge(path)

    def _fill_table(self,
                    table,
                    replication_factor,
                    erasure_codec,
                    optimize_for,
                    sort,
                    compression_codec):
        schema = self.SORTED_SCHEMA if sort else self.UNSORTED_SCHEMA

        create("table", table, attributes={
            "schema": schema,
            "replication_factor": 1,
            "erasure_codec": erasure_codec,
            "optimize_for": optimize_for,
            "compression_codec": compression_codec,
        })

        for key in range(self.JOB_COUNT):
            rows = [{"key": key, "value": "value"}]
            write_table("<append=%true>{}".format(table), rows)

    def _run_job(self, node, sort, retry, cache_disabled):
        hit_compressed_block_cache = self._seed_counter(node, "exec_node/job_input_cache/block_cache/compressed_data/hit_count")
        missed_compressed_block_cache = self._seed_counter(node, "exec_node/job_input_cache/block_cache/compressed_data/missed_count")

        hit_meta_cache = self._seed_counter(node, "exec_node/job_input_cache/meta_cache/hit_count")
        missed_meta_cache = self._seed_counter(node, "exec_node/job_input_cache/meta_cache/missed_count")

        block_compressed_cache_size = self._seed_gauge(node, "exec_node/job_input_cache/block_cache/compressed_data/size")
        meta_cache_size = self._seed_gauge(node, "exec_node/job_input_cache/meta_cache/size")

        map(
            in_="<read_via_exec_node=%true>//tmp/input{}".format("[0:1,3:4]" if sort else ""),
            out="//tmp/output",
            command="cat",
            spec={
                "mapper": {
                    "format": "json",
                },
                "data_weight_per_job": 1,
                "max_failed_job_count": 1,
                "scheduling_tag_filter": node
            })

        if cache_disabled:
            wait(lambda: missed_compressed_block_cache.get_delta() == 0)
            wait(lambda: missed_meta_cache.get_delta() == 0)

            wait(lambda: block_compressed_cache_size.get() == 0)
            wait(lambda: meta_cache_size.get() == 0)

            wait(lambda: hit_compressed_block_cache.get_delta() == 0)
            wait(lambda: hit_meta_cache.get_delta() == 0)
            return

        wait(lambda: get("//sys/cluster_nodes/{}/@statistics/memory/job_input_block_cache/limit".format(node)) == 256 * 1024 * 1024)
        wait(lambda: get("//sys/cluster_nodes/{}/@statistics/memory/job_input_chunk_meta_cache/limit".format(node)) == 256 * 1024 * 1024)

        if not retry:
            wait(lambda: missed_compressed_block_cache.get_delta() > 0)
            wait(lambda: missed_meta_cache.get_delta() > 0)

            wait(lambda: block_compressed_cache_size.get() > 0)
            wait(lambda: meta_cache_size.get() > 0)

            wait(lambda: get("//sys/cluster_nodes/{}/@statistics/memory/job_input_block_cache/used".format(node)) > 0)
            wait(lambda: get("//sys/cluster_nodes/{}/@statistics/memory/job_input_chunk_meta_cache/used".format(node)) > 0)
        else:
            wait(lambda: hit_compressed_block_cache.get_delta() > 0)
            wait(lambda: hit_meta_cache.get_delta() > 0)

            wait(lambda: block_compressed_cache_size.get() > 0)
            wait(lambda: meta_cache_size.get() > 0)

    @pytest.mark.timeout(200)
    @authors("don-dron")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    @pytest.mark.parametrize("sort", [True, False])
    @pytest.mark.parametrize("erasure_codec", ["none", "isa_reed_solomon_6_3"])
    def test_job_input_cache(self, optimize_for, sort, erasure_codec):
        self._set_config()

        self._fill_table("//tmp/input", 1, erasure_codec, optimize_for, sort, "none")
        self._fill_table("//tmp/output", 1, erasure_codec, optimize_for, sort, "none")

        nodes_to_run = [
            self.find_node_with_flavors(["exec"]),
            self.find_node_with_flavors(["data", "exec"]),
        ]

        for node in nodes_to_run:
            self._run_job(node, sort=sort, retry=False, cache_disabled=False)
            self._run_job(node, sort=sort, retry=True, cache_disabled=False)

    @authors("don-dron")
    def test_with_disabled_cache(self):
        self._fill_table("//tmp/input", 1, "none", "scan", True, "none")
        self._fill_table("//tmp/output", 1, "none", "scan", True, "none")

        nodes_to_run = [
            self.find_node_with_flavors(["exec"]),
            self.find_node_with_flavors(["data", "exec"]),
        ]

        for node in nodes_to_run:
            self._run_job(node, sort=True, retry=False, cache_disabled=True)
            self._run_job(node, sort=True, retry=True, cache_disabled=True)

    @authors("don-dron")
    def test_disable_for_remote_copy(self):
        self._set_config()

        remote_driver = get_driver(cluster=self.REMOTE_CLUSTER_NAME)

        create("table", "//tmp/t1", driver=remote_driver)
        write_table("//tmp/t1", {"a": "b"}, driver=remote_driver)

        create("table", "//tmp/t2")

        node = self.find_node_with_flavors(["exec"])

        block_compressed_cache_size = self._seed_gauge(node, "exec_node/job_input_cache/block_cache/compressed_data/size")
        meta_cache_size = self._seed_gauge(node, "exec_node/job_input_cache/meta_cache/size")

        remote_copy(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={
                "cluster_name": self.REMOTE_CLUSTER_NAME,
                "read_via_exec_node": True
            },
        )

        assert read_table("//tmp/t2") == [{"a": "b"}]

        wait(lambda: block_compressed_cache_size.get() == 0)
        wait(lambda: meta_cache_size.get() == 0)


class TestJobStatistics(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    NODE_IO_ENGINE_TYPE = "thread_pool"

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        for store_location in config["data_node"]["store_locations"]:
            store_location["io_config"]["enable_sync"] = True

    @authors("ngc224")
    def test_io_statistics(self):
        create("table", "//tmp/t_input")
        write_table("//tmp/t_input", {"foo": "bar"})

        create("table", "//tmp/t_output1")
        create("table", "//tmp/t_output2")

        script = (
            b"#!/usr/bin/env python3\n"
            b"import sys\n"
            b"import os\n"
            b"data = sys.stdin.read()\n"
            b"sys.stdout.write(data)\n"
            b"with os.fdopen(4, 'w') as f: f.write(data)\n"
        )

        create("file", "//tmp/script")
        write_file("//tmp/script", script)
        set("//tmp/script/@executable", True)

        upload_replication_factor = 3

        op = map(
            command="./script.py",
            in_="//tmp/t_input",
            out=["//tmp/t_output1", "//tmp/t_output2"],
            spec={
                "mapper": {
                    "copy_files": True,
                    "file_paths": [
                        yson.to_yson_type("//tmp/script", attributes={"file_name": "script.py"}),
                    ],
                },
                "job_io": {
                    "table_writer": {
                        "min_upload_replication_factor": upload_replication_factor,
                        "upload_replication_factor": upload_replication_factor,
                    },
                },
            },
            track=True,
        )

        statistics = op.get_statistics()

        def get_statistics(key, output):
            return extract_statistic_v2(statistics, f"chunk_writer_statistics.{output}.{key}")

        for output in [0, 1]:
            assert get_statistics("data_bytes_written_to_disk", output) > 0
            assert get_statistics("data_io_write_requests", output) == upload_replication_factor
            assert get_statistics("data_io_sync_requests", output) == upload_replication_factor
            assert get_statistics("meta_bytes_written_to_disk", output) > 0
            assert get_statistics("meta_io_write_requests", output) == upload_replication_factor
            # two sync requests expected:
            # - one - on chunk writer's Close call to flush meta file
            # - another - on chunk writer's FlushDirectory call to sync chunk directory meta
            assert get_statistics("meta_io_sync_requests", output) == 2 * upload_replication_factor


@authors("ngc224")
@pytest.mark.skipif(not is_uring_supported() or is_uring_disabled(), reason="io_uring is not available on this host")
class TestJobStatisticsUring(TestJobStatistics):
    NODE_IO_ENGINE_TYPE = "uring"


##################################################################


@authors("khlebnikov")
class TestJobSetupCommandCri(YTEnvSetup):
    JOB_ENVIRONMENT_TYPE = "cri"

    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    def test_success(self):
        update_nodes_dynamic_config({
            "exec_node": {
                "job_controller": {
                    "job_common": {
                        "job_setup_command": {
                            "path": "/bin/bash",
                            "args": ["-c", "echo success >setup.txt"],
                        },
                    },
                },
            },
        })

        op = run_test_vanilla("cat ../home/setup.txt >&2", track=True)
        check_all_stderrs(op, b"success\n", 1)

    def test_failure(self):
        update_nodes_dynamic_config({
            "exec_node": {
                "job_controller": {
                    "job_common": {
                        "job_setup_command": {
                            "path": "/bin/false",
                            "args": [],
                        },
                    },
                },
            },
        })

        op = run_test_vanilla("true")
        op.track(raise_on_failed=False)
        assert op.get_state() == "failed"
        err = op.get_error()
        assert err.contains_code(10000)
        assert err.find_matching_error(10000).attributes.get("exit_code") == 1
