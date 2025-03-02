from yt_commands import (raises_yt_error, create, write_table, authors, create_user)
import yt_error_codes

import yt.yson

import pytest

from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
import json
import tempfile


class _TestProxyFormatConfigBase(metaclass=ABCMeta):
    FORMAT_CONFIG = {
        "yamr": {
            "enable": False,
            "user_overrides": {
                "good_user": {
                    "enable": True,
                },
                "bad_user": {
                    "enable": False,
                },
            },
        },
        "yamred_dsv": {
            "enable": True,
            "user_overrides": {
                "good_user": {
                    "enable": True,
                },
                "bad_user": {
                    "enable": False,
                },
            },
        },
        "json": {
            "enable": True,
            "user_overrides": {
                "no_json_user": {
                    "enable": False,
                },
            },
        },
        "yson": {
            "default_attributes": {
                "format": "text",
            },
            "user_overrides": {
                "binary_user": {
                    "default_attributes": {
                        "format": "binary",
                    },
                },
                "text_user": {
                    "default_attributes": {
                        "format": "text",
                    },
                },
                "pretty_user": {
                    "default_attributes": {
                        "format": "pretty",
                    },
                },
                "empty_user": {
                    "default_attributes": {},
                },
            },
        },
    }

    TABLE_CONTENT_TWO_COLUMNS = [
        {"key": "1", "value": "bar"},
        {"key": "2", "value": "baz"},
        {"key": "3", "value": "qux"},
    ]
    TABLE_CONTENT_ONE_COLUMN = [
        {"key": "1"},
        {"key": "2"},
        {"key": "3"},
    ]
    YAMR = yt.yson.to_yson_type("yamr")
    YAMRED_DSV = yt.yson.to_yson_type("yamred_dsv", attributes={
        "columns": ["key", "value"],
        "key_column_names": ["key"],
    })
    YSON = yt.yson.to_yson_type("yson")
    BINARY_YSON = yt.yson.to_yson_type("yson", attributes={"format": "binary"})
    TEXT_YSON = yt.yson.to_yson_type("yson", attributes={"format": "text"})
    PRETTY_YSON = yt.yson.to_yson_type("yson", attributes={"format": "pretty"})

    @abstractmethod
    def _do_run_operation(self, op_type, spec, user, use_start_op):
        return

    @abstractmethod
    def _test_format_enable(self, format, user, enable):
        return

    @staticmethod
    def _parse_format(format, data, tabular=True):
        format_name = str(format)
        if format_name == "yson":
            yson_type = "list_fragment" if tabular else "node"
            return list(yt.yson.loads(data, yson_type=yson_type))
        elif format_name == "json":
            if tabular:
                return [yt.yson.convert.json_to_yson(json.loads(line)) for line in data.split('\n')]
            else:
                return yt.yson.convert.json_to_yson(json.loads(data))
        elif format_name in ("yamr", "yamred_dsv"):
            assert tabular
            result = []
            for line in data.strip().split(b"\n"):
                k, v = line.strip().split()
                if format_name == "yamred_dsv":
                    assert v.startswith(b"value=")
                    v = v[len(b"value="):]
                result.append({"key": k.decode("ascii"), "value": v.decode("ascii")})
            return result
        else:
            assert False

    @staticmethod
    def _write_format(format, data, tabular=True):
        format_name = str(format)
        if format_name == "yson":
            yson_type = "list_fragment" if tabular else "node"
            return yt.yson.dumps(data, yson_type=yson_type)
        elif format_name == "json":
            if tabular:
                return b"\n".join(json.dumps(yt.yson.convert.yson_to_json(row)).encode() for row in data)
            else:
                return json.dumps(yt.yson.convert.yson_to_json(data))
        elif format_name == "yamr":
            assert tabular
            return b"\n".join(b"\t".join([row["key"].encode("ascii"), row["value"].encode("ascii")]) for row in data)
        elif format_name == "yamred_dsv":
            assert tabular
            return b"\n".join(b"\t".join([row["key"].encode("ascii"), b"value=" + row["value"].encode("ascii")]) for row in data)
        else:
            assert False

    def _get_context_manager(self, enable):
        if enable:
            @contextmanager
            def manager():
                yield
        else:
            def manager():
                return raises_yt_error(yt_error_codes.FormatDisabled)
        return manager

    def _run_operation(self, op_type, spec, user, use_start_op=True, content=[]):
        schema = [
            {"name": "key", "type": "string", "sort_order": "ascending"},
        ]
        create("table", "//tmp/t_in", force=True, attributes={"schema": schema})
        write_table("//tmp/t_in", content)
        create("table", "//tmp/t_out", force=True)
        spec["input_table_paths"] = ["//tmp/t_in"]
        spec["output_table_paths"] = ["//tmp/t_out"]
        op = self._do_run_operation(op_type, spec, user, use_start_op)
        op.track()

    def _test_format_enable_operations(self, format, user, enable):
        map_spec = {
            "mapper": {
                "command": "sleep 0",
                "output_format": format,
            },
        }
        reduce_spec = {
            "reduce_by": ["key"],
            "reducer": {
                "command": "sleep 0",
                "input_format": format,
            }
        }
        map_reduce_spec = {
            "reduce_by": ["key"],
            "sort_by": ["key"],
            "mapper": {
                "command": "sleep 0",
                "input_format": format,
            },
            "reducer": {
                "command": "sleep 0",
                "format": format,
            }
        }
        vanilla_spec = {
            "tasks": {
                "task1": {
                    "command": "sleep 0",
                    "output_format": format,
                    "job_count": 1,
                },
            },
        }
        file_path = yt.yson.to_yson_type("//tmp/file", attributes={"format": format})
        create("table", file_path, force=True)
        write_table(file_path, self.TABLE_CONTENT_TWO_COLUMNS)
        vanilla_spec_with_file = {
            "tasks": {
                "task1": {
                    "command": "sleep 0",
                    "job_count": 1,
                    "file_paths": [file_path],
                },
            },
        }

        manager = self._get_context_manager(enable)
        with manager():
            self._run_operation("map", map_spec, user)
        with manager():
            self._run_operation("map", map_spec, user, use_start_op=False)
        with manager():
            self._run_operation("reduce", reduce_spec, user)
        with manager():
            self._run_operation("join_reduce", reduce_spec, user)
        with manager():
            self._run_operation("map_reduce", map_reduce_spec, user)
        with manager():
            self._run_operation("vanilla", vanilla_spec, user)
        with manager():
            self._run_operation("vanilla", vanilla_spec_with_file, user)

    def _test_format_defaults_operations(self, format, user, content, expected_content):
        with tempfile.NamedTemporaryFile() as op_input:
            spec = {
                "mapper": {
                    "command": "cat > {}".format(op_input.name),
                    "input_format": format,
                },
            }
            self._run_operation("map", spec, user, content=content)
            assert op_input.read() == expected_content

        with tempfile.NamedTemporaryFile() as file_from_spec:
            file_path = yt.yson.to_yson_type("//tmp/file_table", attributes={"format": format})
            create("table", file_path, force=True)
            write_table(file_path, content)
            spec = {
                "tasks": {
                    "task1": {
                        "command": "cat ./file_table > {}".format(file_from_spec.name),
                        "job_count": 1,
                        "file_paths": [file_path],
                    },
                },
            }
            self._run_operation("vanilla", spec, user)
            actual_content = file_from_spec.read()
            assert actual_content == expected_content

    @authors("levysotsky")
    @pytest.mark.parametrize("user, yamr, yamred_dsv, yson", [
        pytest.param("some_user", False, True, True),
        pytest.param("root", True, True, True),
        pytest.param("good_user", True, True, True),
        pytest.param("bad_user", False, False, True),
    ])
    @pytest.mark.timeout(120)
    def test_format_enable(self, user, yamr, yamred_dsv, yson):
        if user != "root":
            create_user(user)

        self._test_format_enable(self.YAMR, user, yamr)
        self._test_format_enable(self.YAMRED_DSV, user, yamred_dsv)
        self._test_format_enable(self.YSON, user, yson)

    @authors("levysotsky")
    @pytest.mark.parametrize("user, expected_format", [
        pytest.param("binary_user", BINARY_YSON),
        pytest.param("text_user", TEXT_YSON),
        pytest.param("pretty_user", PRETTY_YSON),
        pytest.param("default_user", TEXT_YSON),
        pytest.param("empty_user", BINARY_YSON),
    ])
    def test_format_defaults(self, user, expected_format):
        create_user(user)

        content = self.TABLE_CONTENT_ONE_COLUMN

        self._test_format_defaults(self.YSON, user, content, expected_format)
        self._test_format_defaults(self.TEXT_YSON, user, content, self.TEXT_YSON)
        self._test_format_defaults(self.BINARY_YSON, user, content, self.BINARY_YSON)
