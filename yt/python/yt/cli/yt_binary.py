# PYTHON_ARGCOMPLETE_OK

from __future__ import print_function

from yt.common import copy_docstring_from

import yt.wrapper.yson as yson
from yt.wrapper.common import (
    chunk_iter_stream, MB,
    DoNotReplaceAction, get_binary_std_stream, get_disk_space_from_resources,
    chunk_iter_rows, format_disk_space, is_arcadia_python)
from yt.wrapper.cli_helpers import (
    write_silently, print_to_output, run_main, ParseStructuredArgument, populate_argument_help, SUBPARSER_KWARGS,
    add_subparser, parse_arguments, parse_data, output_format, dump_data, add_argument, add_structured_argument,
    YT_STRUCTURED_DATA_FORMAT, YT_ARGUMENTS_FORMAT, OUTPUT_FORMATS)
from yt.wrapper.constants import DOC_ROOT_URL, TUTORIAL_DOC_URL
from yt.wrapper.default_config import get_default_config, RemotePatchableValueBase
from yt.wrapper.admin_commands import add_switch_leader_parser
from yt.wrapper.dirtable_commands import add_dirtable_parsers
from yt.wrapper.flow_commands import get_controller_logs, wait_pipeline_state
from yt.wrapper.spec_builders import (
    MapSpecBuilder, ReduceSpecBuilder, MapReduceSpecBuilder, EraseSpecBuilder,
    MergeSpecBuilder, SortSpecBuilder, JoinReduceSpecBuilder, RemoteCopySpecBuilder,
    VanillaSpecBuilder)
import yt.wrapper.cli_impl as cli_impl
import yt.wrapper.job_tool as yt_job_tool
import yt.wrapper.run_compression_benchmarks as yt_run_compression_benchmarks
import yt.wrapper.completers as completers
try:
    import yt.wrapper.idm_cli_helpers as idm
    HAS_IDM_CLI_HELPERS = True
except ImportError:
    HAS_IDM_CLI_HELPERS = False

try:
    from yt.packages.six import PY3, iteritems
    from yt.packages.six.moves import builtins, map as imap, zip_longest as izip_longest
except ImportError:
    from six import PY3, iteritems
    from six.moves import builtins, map as imap, zip_longest as izip_longest

import yt.wrapper as yt
import yt.clickhouse as chyt

import json
import os
import sys
import inspect
import itertools
import fnmatch
import shlex
import signal
import time
from argparse import ArgumentParser, Action, RawDescriptionHelpFormatter
from datetime import datetime

from .strawberry_parser import add_strawberry_ctl_parser
from .command_explain_id import add_explain_id_parser

HAS_SKY_SHARE = hasattr(yt, "sky_share")

DESCRIPTION = '''Shell utility to work with YT system.\n
Cypress (metainformation tree) commands:
    exists, list, find, create, get, set, remove, copy, move, link,
    concatenate, externalize, create-account, create-pool
File commands:
    read-file, write-file
Table commands:
    read-table, read-blob-table, write-table, create-temp-table,
    alter-table, get-table-columnar-statistics, dirtable
Dynamic table commands:
    mount-table, unmount-table, remount-table, freeze-table, unfreeze-table, get-tablet-infos,
    reshard-table, reshard-table-automatic, balance-tablet-cells, trim-rows, alter-table-replica,
    select-rows, explain-query, lookup-rows, insert-rows, delete-rows, get-tablet-errors
Run operation commands:
    erase, merge, sort, map, reduce, join-reduce, remote-copy, vanilla, map-reduce
Operation commands:
    abort-op, suspend-op, resume-op, complete-op, update-op-parameters,
    get-operation, list-operations, track-op
Job commands:
    run-job-shell, abort-job, get-job-stderr, get-job-input,
    get-job-input-paths, get-job, list-jobs
Transaction commands:
    start-tx, commit-tx, abort-tx, ping-tx,
    lock, unlock
ACL (permission) commands:
    check-permission, add-member, remove-member
Operation tools:
    shuffle, transform
Diagnostics:
    job-tool, download-core-dump
Other commands:
    show-spec, execute, execute-batch, sky-share, show-default-config, transfer-account-resources, transfer-pool-resources,
    generate-timestamp
Subtools:
    chyt, spyt, jupyt, idm, flow
'''

EPILOG = '''Examples:

List content of root directory of Hume cluster and check access to system.
$  yt list / --proxy <cluster_host>
>> home
   sys
   tmp

Set YT_PROXY = <cluster_host> to avoid usage --proxy option.
$ export YT_PROXY=<cluster_host>
(Add this string to ~/.bashrc)

$  yt create table //tmp/sepulki
>> [...] Access denied: "write" permission for node //tmp is not allowed by any matching ACE [...]

Oops! Forgotten token. Follow {doc_root_url}/user-guide/storage/auth

$  yt create table //tmp/sepulki
>> 1-2-3-4

See all attributes created table.
$ yt get "//tmp/sepulki/@"
>> {{
        "chunk_list_id" = "5c6a-1c2459-65-1f9943e3";
        "inherit_acl" = "true";
        [...]
    }}

You can use GUID of object #1-2-3-4 instead path.
See also https://ytsaurus.tech/docs/en/user-guide/storage/ypath

Note! Create parent directory of table before writing to it. Recursive path creating is not available during operation.

Write to table in Cypress from local file.
$ cat my_data_in_json | yt write //tmp/sepulki --format json

For appending to table instead rewriting specify append mode.
$ cat my_data_in_yson | yt write '<append=true>//tmp/sepulki' --format yson

Output table to client stdout.
$ yt read //tmp/test_table --format dsv
>> b=hello  a=10
   b=world  a=20

Run map-reduce operation, append result to destination table.
$ yt map-reduce --mapper "python my_mapper.py" --map-local-file "~/my_mapper.py" --reducer "./my_reducer.pl" --reduce-file "//tmp/my_reducer.pl"
--src //project/some_existing_table  --src some_another_table_in_prefix_directory --dst <append=true>//tmp/sepulki
--reduce-by "column_one" --reduce-by "column_two"
>> [...] operation 5-6-7-8 initializing [...]

Sort the table.
$ yt sort --src //project/some_existing_table --dst //tmp/sepulki --sort-by "column-one"

Sort the table in descending order.
$ yt sort --src //project/some_existing_table --dst //tmp/sepulki --sort-by "{{name=column-one; sort_order=descending;}}"

Note! Without modification `append=true` destination table is overwritten.
Note! Specify paths to user scripts!

Lookup rows from dynamic table.
$ echo '{{host=abc.com}}' | yt lookup-rows //my/dynamic/table
>> {{"host"="abc.com";"last_seen_time": "2017-04-10T12:35:10"}}

See also:
    YT CLI client           {doc_root_url}/api/cli/cli

    user documentation      {doc_root_url}
    access to the system    {doc_root_url}/user-guide/storage/auth
    tutorial                {tutorial_doc_url}
    command specification   {doc_root_url}/api/commands
    ACL                     {doc_root_url}/user-guide/storage/access-control
    transactions            {doc_root_url}/user-guide/storage/transactions
'''.format(
    doc_root_url=DOC_ROOT_URL,
    tutorial_doc_url=TUTORIAL_DOC_URL
)


def parse_structured_arguments_or_file(string, **kwargs):
    if os.path.isfile(string):
        with open(string, 'rb') as fh:
            file_data = fh.read()
            data = parse_arguments(file_data, **kwargs)
    else:
        data = parse_arguments(string, **kwargs)
    if isinstance(data, yson.YsonUnicode):
        raise ValueError("Parameter should be '{<some_yson>}' of 'some_file.yson'")
    return data


class ParseFormat(Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, yt.create_format(values))


class ParseStructuredArguments(Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, builtins.list(imap(parse_arguments, values)))


class ParseTimeArgument(Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, self.parse_time(values))

    def parse_time(self, value):
        try:
            return datetime.utcfromtimestamp(int(value))
        except ValueError:
            return value


class ParseStructuredData(Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, parse_data(values))


class ParseAppendStructuredDataOrString(Action):
    def __call__(self, parser, namespace, values, option_string=None):
        parsed_value = values
        if parsed_value.startswith("{"):
            parsed_value = parse_arguments(values)
        value = getattr(namespace, self.dest) or []
        value.append(parsed_value)
        setattr(namespace, self.dest, value)


class ParseMemoryLimit(Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if values is not None:
            values = yt.common.MB * int(values)
        setattr(namespace, self.dest, values)


def add_hybrid_argument(parser, name, help=None, description=None, group_required=True, aliases=[], **kwargs):
    group = parser.add_mutually_exclusive_group(required=group_required)
    dest = None
    positional_name = name
    if "dest" in kwargs:
        dest = kwargs.pop("dest")
        positional_name = dest
    # Positional argument
    positional = add_argument(parser=group,
                              name=positional_name,
                              help=help,
                              description=description,
                              nargs="?",
                              action=DoNotReplaceAction,
                              **kwargs)
    # Optional argument
    if dest is not None:
        kwargs["dest"] = dest
    optional = add_argument(parser=group,
                            name="--" + name.replace("_", "-"),
                            help=help,
                            description=description,
                            aliases=aliases,
                            **kwargs)
    return positional, optional


def add_ypath_argument(parser, name, help="address in Cypress", hybrid=False, **kwargs):
    description = "See also: https://ytsaurus.tech/docs/en/user-guide/storage/ypath"

    if hybrid:
        positional, optional = add_hybrid_argument(parser, name, help=help, description=description, **kwargs)
        positional.completer = completers.complete_ypath
        optional.completer = completers.complete_ypath
    else:
        argument = add_argument(parser, name, help, description=description, **kwargs)
        argument.completer = completers.complete_ypath


def add_structured_format_argument(parser, name="--format", help="", **kwargs):
    description = 'response or input format: yson or json, for example: "<format=binary>yson". '\
                  'See also: https://ytsaurus.tech/docs/en/user-guide/storage/formats'
    add_argument(parser, name, help, description=description, action=ParseFormat, **kwargs)


def add_format_argument(parser, name="--format", help="", **kwargs):
    description = '(yson string), one of "yson", "json", "yamr", "dsv", "yamred_dsv", "schemaful_dsv" '\
                  'with modifications. See also: https://ytsaurus.tech/docs/en/user-guide/storage/formats'
    add_argument(parser, name, help, description=description, action=ParseFormat, **kwargs)


def add_type_argument(parser, name, hybrid=False):
    NODE_TYPE_HELP = "one of table, file, document, account, user, list_node, map_node, "\
                     "string_node, int64_node, uint64_node, double_node, ..."
    if not hybrid:
        add_argument(parser, name, "", description=NODE_TYPE_HELP)
    else:
        add_hybrid_argument(parser, name, "", description=NODE_TYPE_HELP)


def add_read_from_arguments(parser):
    parser.add_argument("--read-from", help='Can be set to "cache" to enable reads from system cache')
    parser.add_argument("--cache-sticky-group-size", type=int,
                        help='Size of sticky group size for read_from="cache" mode')


def add_sort_order_argument(parser, name, help, required):
    DESCRIPTION = "In order to choose descending sort order, provide a map of form '{name=foo; sort_order=descending}'"
    add_argument(parser, name, help, DESCRIPTION, action=ParseAppendStructuredDataOrString, required=required)


def add_boolean_argument(parser, name, negation_prefix="no", default=None, required=False, help=None):
    hyphenated_name = name.replace("_", "-")
    group = parser.add_mutually_exclusive_group(required=required)
    group.add_argument("--" + hyphenated_name, dest=name, action="store_true", help=help)
    group.add_argument("--" + negation_prefix + "-" + hyphenated_name, dest=name, action="store_false", help=help)
    parser.set_defaults(**{name: default})


@copy_docstring_from(yt.exists)
def exists(**kwargs):
    print("true" if yt.exists(**kwargs) else "false")


def add_exists_parser(add_parser):
    parser = add_parser("exists", exists)
    add_ypath_argument(parser, "path", hybrid=True)
    parser.add_argument("--suppress-transaction-coordinator-sync", action="store_true",
                        help="suppress transaction coordinator sync")
    add_read_from_arguments(parser)


DEFAULT_TIME_TYPE = "modification_time"
LONG_FORMAT_ATTRIBUTES = ("type", "target_path", "account", "resource_usage")


def formatted_print(obj, path, long_format, recursive_resource_usage, time_type):
    attributes = getattr(obj, "attributes", {})

    def get_attr(attr_name):
        if attr_name not in attributes:
            attributes.update(yt.get(path + "&/@"))
        return attributes[attr_name]

    if long_format:
        type = get_attr("type")
        link_tail = (" -> " + str(get_attr("target_path"))) if type == "link" else ""
        user = get_attr("account")
        if recursive_resource_usage:
            resource_usage = yt.get(path + "&/@recursive_resource_usage")
        else:
            resource_usage = get_attr("resource_usage")
        size = get_disk_space_from_resources(resource_usage)
        date, time = get_attr(time_type).split("T")
        time = time[:5]
        sys.stdout.write("%10s %20s %10s %10s %5s %s%s\n" %
                         (type, user, format_disk_space(size), date, time, str(obj), link_tail))
    else:
        print_to_output(obj)


@copy_docstring_from(yt.list)
def list(**kwargs):
    list_args = dict(kwargs)
    list_args.pop("long_format")
    list_args.pop("recursive_resource_usage")
    time_type = kwargs.get("time_type", DEFAULT_TIME_TYPE)
    if kwargs["long_format"]:
        assert kwargs["format"] is None
        list_args["attributes"] = LONG_FORMAT_ATTRIBUTES + (time_type,)
    list = yt.list(**list_args)
    if kwargs["format"] is None:
        if kwargs["attributes"] is not None:
            print("WARNING! Attributes are ignored if format is not specified.", file=sys.stderr)
        for elem in list:
            formatted_print(
                elem,
                yt.ypath_join(kwargs["path"], yt.escape_ypath_literal(elem)),
                kwargs["long_format"],
                kwargs["recursive_resource_usage"],
                time_type)
        if list.attributes.get("incomplete", False):
            print("WARNING: list is incomplete, check --max-size option", file=sys.stderr)
    else:
        print_to_output(list, eoln=False)


def add_list_parser(add_parser):
    parser = add_parser("list", list, epilog="Warning! --attribute arguments are ignored if format is not specified.")
    add_ypath_argument(parser, "path", hybrid=True)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-l", "--long-format", action="store_true", help="print some extra information about nodes")
    add_structured_format_argument(group)
    parser.add_argument("--attribute", action="append", dest="attributes", help="node attributes to add into response")
    parser.add_argument("--max-size", type=int,
                        help=("maximum size of entries returned by list; "
                              "if actual directory size exceeds that value only subset "
                              "of entries will be listed (it's not specified which subset); "
                              "default value is enough to list any nonsystem directory."))
    parser.add_argument("--recursive-resource-usage", action="store_true",
                        help="use recursive resource usage for in long format mode")
    parser.add_argument("--suppress-transaction-coordinator-sync", action="store_true",
                        help="suppress transaction coordinator sync")
    add_read_from_arguments(parser)
    group.add_argument("--absolute", action="store_true", default=False, help="print absolute paths")


def find(**kwargs):
    path_filter = None
    if kwargs["name"] is not None:
        path_filter = lambda path: fnmatch.fnmatch(os.path.basename(path), kwargs["name"]) # noqa

    attributes = dict([(x, kwargs[x]) for x in ["account", "owner"] if kwargs[x] is not None])
    if kwargs["attribute_filter"] is not None:
        try:
            attributes_filter = yson._loads_from_native_str(kwargs["attribute_filter"], yson_type="map_fragment")
        except yson.YsonError as err:
            raise yt.YtError("Specified attribute filter must be a valid yson map fragment", inner_errors=[err])
        attributes.update(attributes_filter)

    long_format_attributes = builtins.list(LONG_FORMAT_ATTRIBUTES) if kwargs["long_format"] else []

    object_filter = lambda obj: \
        all([obj.attributes.get(attr) == attributes[attr] for attr in attributes]) # noqa

    result = yt.search(kwargs["path"],
                       node_type=kwargs["type"],
                       path_filter=path_filter,
                       attributes=builtins.list(attributes) + long_format_attributes,
                       object_filter=object_filter,
                       depth_bound=kwargs["depth"],
                       follow_links=kwargs["follow_links"],
                       read_from=kwargs["read_from"],
                       cache_sticky_group_size=kwargs["cache_sticky_group_size"])

    for elem in result:
        formatted_print(elem, elem, kwargs["long_format"], kwargs["recursive_resource_usage"], kwargs.get("time_type", DEFAULT_TIME_TYPE))


def add_find_parser(add_parser):
    parser = add_parser("find", yt.search)
    add_ypath_argument(parser, "path", hybrid=True)
    parser.add_argument("--name", "-name", help="pattern of node name, use shell-style wildcards: *, ?, [seq], [!seq]")
    add_type_argument(parser, "--type")
    parser.add_argument("--account")
    parser.add_argument("--owner")
    parser.add_argument("--follow-links", action="store_true", help="follow links")
    parser.add_argument("--attribute-filter", help="yson map fragment with filtering attributes, e.g. k1=v1;k2=v2")
    parser.add_argument("--depth", type=int, help="recursion depth (infinite by default)")
    parser.add_argument("-l", "--long-format", action="store_true", help="print some extra information about nodes")
    parser.add_argument("--recursive-resource-usage", action="store_true",
                        help="use recursive resource usage for in long format mode")
    parser.add_argument("--time-type", choices=['access_time', 'modification_time', 'creation_time'],
                        default=DEFAULT_TIME_TYPE,
                        help="type of time to use in long-format")
    add_read_from_arguments(parser)
    parser.set_defaults(func=find)


@copy_docstring_from(yt.read_table)
def read_table(**kwargs):
    def add_commas(iterator):
        current_iter, next_iter = itertools.tee(iterator)
        next(next_iter, None)
        for current_chunk, next_chunk in izip_longest(current_iter, next_iter):
            if next_chunk is not None:
                yield current_chunk.replace(b"\n", b",\n")
            else:
                yield current_chunk[:-1].replace(b"\n", b",\n")
                yield b"\n"

    as_json_list = kwargs.get("as_json_list", False)
    if "as_json_list" in kwargs:
        kwargs.pop("as_json_list")

    stream = yt.read_table(**kwargs)
    if hasattr(stream, "_read_rows"):
        iterator = chunk_iter_rows(stream, yt.config["read_buffer_size"])
    else:
        iterator = chunk_iter_stream(stream, yt.config["read_buffer_size"])

    if isinstance(kwargs["format"], yt.JsonFormat) and as_json_list:
        get_binary_std_stream(sys.stdout).write(b"[\n")
        write_silently(add_commas(iterator))
        get_binary_std_stream(sys.stdout).write(b"]\n")
    else:
        write_silently(iterator)


def add_read_table_parser(add_parser):
    for name, func in [("read", read_table), ("read-table", read_table)]:
        parser = add_parser(name, func)
        add_ypath_argument(parser, "table", hybrid=True)
        add_format_argument(parser, help="output format")
        add_structured_argument(parser, "--table-reader")
        add_structured_argument(parser, "--control-attributes")
        parser.add_argument("--unordered", action="store_true")
        parser.add_argument(
            "--as-json-list",
            action="store_true",
            help="In case of JSON format output stream as JSON list instead of JSON lines format")


@copy_docstring_from(yt.read_file)
def read_file(**kwargs):
    write_silently(chunk_iter_stream(yt.read_file(**kwargs), yt.config["read_buffer_size"]))


def add_read_file_parser(add_parser):
    for name, func in [("read-file", read_file), ("download", read_file)]:
        parser = add_parser(name, func)
        add_ypath_argument(parser, "path", hybrid=True)
        add_structured_argument(parser, "--file-reader")
        parser.add_argument("--offset", type=int, help="offset in input file in bytes, 0 by default")
        parser.add_argument("--length", type=int, help="length in bytes of desired part of input file, "
                                                       "all file without offset by default")


@copy_docstring_from(yt.read_blob_table)
def read_blob_table(**kwargs):
    stream = yt.read_blob_table(**kwargs)
    iterator = chunk_iter_stream(stream, yt.config["read_buffer_size"])
    write_silently(iterator)


def add_read_blob_table_parser(add_parser):
    parser = add_parser("read-blob-table", read_blob_table)
    add_ypath_argument(parser, "table", hybrid=True)
    parser.add_argument("--part-index-column-name", help="name of column with part indexes")
    parser.add_argument("--data-column-name", help="name of column with data")
    parser.add_argument("--part-size", type=int, help="size of each blob")
    add_structured_argument(parser, "--table-reader")


@copy_docstring_from(yt.write_file)
def write_file(**kwargs):
    func_args = dict(kwargs)
    func_args.pop("executable")
    if func_args.pop("no_compression", False):
        yt.config["proxy"]["content_encoding"] = "identity"
    yt.write_file(stream=get_binary_std_stream(sys.stdin), **func_args)
    if kwargs["executable"]:
        yt.set(kwargs["destination"] + "/@executable", True)


@copy_docstring_from(yt.dump_parquet)
def dump_parquet(**kwargs):
    yt.dump_parquet(**kwargs)


def add_dump_parquet_parser(add_parser):
    parser = add_parser("dump-parquet", dump_parquet)
    add_ypath_argument(parser, "table", hybrid=True)
    parser.add_argument("--output-path", type=str, required=False)
    parser.add_argument("--output-file", type=str, required=False)
    parser.add_argument("--enable-several-files", action="store_true",
                        help="allowing parquet to be written to multiple files")
    parser.add_argument("--unordered", action="store_true")


@copy_docstring_from(yt.upload_parquet)
def upload_parquet(**kwargs):
    yt.upload_parquet(**kwargs)


def add_upload_parquet_parser(add_parser):
    parser = add_parser("upload-parquet", upload_parquet)
    add_ypath_argument(parser, "table", hybrid=True)
    parser.add_argument("--input-file", type=str, required=True)


@copy_docstring_from(yt.dump_orc)
def dump_orc(**kwargs):
    yt.dump_orc(**kwargs)


def add_dump_orc_parser(add_parser):
    parser = add_parser("dump-orc", dump_orc)
    add_ypath_argument(parser, "table", hybrid=True)
    parser.add_argument("--output-file", type=str, required=True)


@copy_docstring_from(yt.upload_orc)
def upload_orc(**kwargs):
    yt.upload_orc(**kwargs)


def add_upload_orc_parser(add_parser):
    parser = add_parser("upload-orc", upload_orc)
    add_ypath_argument(parser, "table", hybrid=True)
    parser.add_argument("--input-file", type=str, required=True)


@copy_docstring_from(yt.write_table)
def write_table(**kwargs):
    func_args = dict(kwargs)
    if func_args.pop("no_compression", False):
        yt.config["proxy"]["content_encoding"] = "identity"
    yt.write_table(input_stream=get_binary_std_stream(sys.stdin), **func_args)


def add_compressed_arg(parser):
    parser.add_argument("--compressed", action="store_true", dest="is_stream_compressed",
                        help="expect stream to contain compressed file data. Warning! This option disables retries. "
                             "Data is passed directly to proxy without recompression.")


def add_no_compression_arg(parser):
    parser.add_argument("--no-compression", action="store_true", help="disable compression")


def add_write_file_parser(add_parser):
    for name, func in [("write-file", write_file), ("upload", write_file)]:
        parser = add_parser(name, func)
        add_ypath_argument(parser, "destination", hybrid=True)
        add_structured_argument(parser, "--file-writer")
        add_compressed_arg(parser)
        parser.add_argument("--executable", action="store_true", help="do file executable")
        parser.add_argument("--compute-md5", action="store_true", help="compute md5 of file content")
        add_no_compression_arg(parser)


def add_write_table_parser(add_parser):
    for name, func in [("write", write_table), ("write-table", write_table)]:
        parser = add_parser(
            name,
            func,
            epilog="Rewrite table by default. For append mode specify <append=true> before path.")
        add_ypath_argument(parser, "table", hybrid=True)
        add_format_argument(parser, help="input format")
        add_structured_argument(parser, "--table-writer")
        add_compressed_arg(parser)
        add_no_compression_arg(parser)


@copy_docstring_from(yt.create_temp_table)
def create_temp_table(**kwargs):
    kwargs["prefix"] = kwargs.pop("name_prefix", None)
    print_to_output(yt.create_temp_table(**kwargs))


def add_create_temp_table_parser(add_parser):
    parser = add_parser("create-temp-table", create_temp_table)
    add_ypath_argument(parser, "--path", help="path where temporary table will be created")
    parser.add_argument("--name-prefix", help="prefix of table name")
    parser.add_argument("--expiration-timeout", type=int, help="expiration timeout in ms")
    add_structured_argument(parser, "--attributes")


@copy_docstring_from(yt.create)
def create(**kwargs):
    print_to_output(yt.create(**kwargs))


def add_create_parser(add_parser):
    parser = add_parser("create", create)
    add_type_argument(parser, "type", hybrid=True)
    add_ypath_argument(parser, "path", hybrid=True, group_required=False)
    parser.add_argument("-r", "--recursive", action="store_true")
    parser.add_argument("-i", "--ignore-existing", action="store_true")
    parser.add_argument("-l", "--lock-existing", action="store_true")
    parser.add_argument("-f", "--force", action="store_true")
    add_structured_argument(parser, "--attributes")


def patch_attributes(kwargs, attribute_names):
    if kwargs.get("attributes", None) is None:
        kwargs["attributes"] = {}
    for attr_name in attribute_names:
        attr_value = kwargs.pop(attr_name, None)
        if attr_value is not None:
            kwargs["attributes"][attr_name] = attr_value


def create_account(**kwargs):
    patch_attributes(kwargs, ("name", "parent_name", "resource_limits", "allow_children_limit_overcommit"))
    print(yt.create("account", **kwargs))


def add_create_account_parser(add_parser):
    parser = add_parser("create-account", create_account, help="creates account")
    add_hybrid_argument(parser, "name")
    parser.add_argument("--parent-name")
    add_structured_argument(parser, "--resource-limits")
    parser.add_argument("-i", "--ignore-existing", action="store_true")
    parser.add_argument("--allow-children-limit-overcommit", action="store_true")
    add_structured_argument(parser, "--attributes")


def create_pool(**kwargs):
    patch_attributes(kwargs, ("name", "pool_tree", "parent_name", "weight", "mode", "fifo_sort_parameters",
                              "max_operation_count", "max_running_operation_count", "forbid_immediate_operations",
                              "resource_limits", "min_share_resources", "create_ephemeral_subpools",
                              "ephemeral_subpool_config"))
    if "pool_tree" not in kwargs["attributes"]:
        try:
            kwargs["attributes"]["pool_tree"] = yt.get("//sys/scheduler/orchid/scheduler/default_pool_tree")
        except yt.YtError as err:
            raise yt.YtError("Failed to retrieve default pool tree from scheduler orchid; "
                             "please specify the pool_tree argument",
                             inner_errors=[err])
    print(yt.create("scheduler_pool", **kwargs))


def add_create_pool_parser(add_parser):
    parser = add_parser("create-pool", create_pool, help="creates scheduler pool")
    add_hybrid_argument(parser, "name")
    add_hybrid_argument(parser, "pool_tree", group_required=False)
    parser.add_argument("--parent-name")

    parser.add_argument("--weight", type=float)
    parser.add_argument("--mode", help="fifo or fair_share")
    add_structured_argument(parser, "--fifo-sort-parameters")
    parser.add_argument("--max-operation-count", type=int)
    parser.add_argument("--max-running-operation-count", type=int)
    parser.add_argument("--forbid-immediate-operations", action="store_true")
    add_structured_argument(parser, "--resource-limits")
    add_structured_argument(parser, "--min-share-resources")
    parser.add_argument("--create-ephemeral-subpools", action="store_true")
    add_structured_argument(parser, "--ephemeral-subpool-config")

    parser.add_argument("-i", "--ignore-existing", action="store_true")
    add_structured_argument(parser, "--attributes")


def get_helper(yt_method):

    @copy_docstring_from(yt_method)
    def inner(**kwargs):
        result = yt_method(**kwargs)
        if kwargs["format"] is None:
            result = dump_data(result)
        print_to_output(result)

    return inner


def add_get_parser(add_parser):
    parser = add_parser("get", get_helper(yt.get))
    add_ypath_argument(parser, "path", hybrid=True)
    parser.add_argument("--max-size", type=int,
                        help=("maximum size of entries returned by get; "
                              "if actual directory size exceeds that value only subset "
                              "of entries will be listed (it's not specified which subset); "
                              "default value is enough to list any nonsystem directory."))
    add_structured_format_argument(parser, default=output_format)
    parser.add_argument("--attribute", action="append", dest="attributes",
                        help="desired node attributes in the response")
    parser.add_argument("--suppress-transaction-coordinator-sync", action="store_true",
                        help="suppress transaction coordinator sync")
    add_read_from_arguments(parser)


def set_helper(yt_method):

    @copy_docstring_from(yt_method)
    def inner(**kwargs):
        if kwargs["value"] is None:
            value = get_binary_std_stream(sys.stdin).read()
        else:
            value = kwargs["value"]
            if PY3:
                value = value.encode("utf-8")
        if kwargs["format"] is None:
            value = parse_arguments(value)
        kwargs["value"] = value
        yt_method(**kwargs)

    return inner


def add_set_parser(add_parser):
    parser = add_parser("set", set_helper(yt.set))
    add_ypath_argument(parser, "path", hybrid=True)
    add_structured_format_argument(parser, default=YT_STRUCTURED_DATA_FORMAT)
    parser.add_argument("-r", "--recursive", action="store_true")
    parser.add_argument("-f", "--force", action="store_true")
    parser.add_argument("--suppress-transaction-coordinator-sync", action="store_true",
                        help="suppress transaction coordinator sync")
    add_hybrid_argument(parser, "value", group_required=False,
                        help="new node attribute value, in {0} format. You can specify in bash pipe: "
                             "\"cat file_with_value | yt set //tmp/my_node\"".format(YT_ARGUMENTS_FORMAT))


def add_set_attribute_parser(add_parser):
    parser = add_parser("set-attribute", cli_impl._set_attribute)
    add_ypath_argument(parser, "path", hybrid=True)
    parser.add_argument("name")
    add_structured_argument(parser, "value")
    parser.add_argument("-r", "--recursive", action="store_true")


def add_remove_parser(add_parser):
    parser = add_parser("remove", yt.remove)
    add_ypath_argument(parser, "path", hybrid=True)
    parser.add_argument("-r", "--recursive", action="store_true")
    parser.add_argument("-f", "--force", action="store_true")


def add_remove_attribute_parser(add_parser):
    parser = add_parser("remove-attribute", cli_impl._remove_attribute)
    add_ypath_argument(parser, "path", hybrid=True)
    parser.add_argument("name")
    parser.add_argument("-r", "--recursive", action="store_true")


def add_copy_move_preserve_arguments(parser):
    preserve_account_parser = parser.add_mutually_exclusive_group(required=False)
    preserve_account_parser.add_argument("--preserve-account", dest="preserve_account",
                                         default=None, action="store_true")
    preserve_account_parser.add_argument("--no-preserve-account", dest="preserve_account",
                                         default=None, action="store_false")

    preserve_owner_parser = parser.add_mutually_exclusive_group(required=False)
    preserve_owner_parser.add_argument("--preserve-owner", dest="preserve_owner",
                                       default=None, action="store_true")
    preserve_owner_parser.add_argument("--no-preserve-owner", dest="preserve_owner",
                                       default=None, action="store_false")

    parser.add_argument("--preserve-creation-time", action="store_true",
                        help="preserve creation time of node")
    parser.add_argument("--preserve-modification-time", action="store_true",
                        help="preserve modification time of node")
    parser.add_argument("--preserve-expiration-time", action="store_true",
                        help="preserve expiration time of node")
    parser.add_argument("--preserve-expiration-timeout", action="store_true",
                        help="preserve expiration timeout of node")


def add_copy_parser(add_parser):
    parser = add_parser("copy", yt.copy)

    add_ypath_argument(parser, "source_path", help="source address, path must exist", hybrid=True)
    add_ypath_argument(parser, "destination_path", help="destination address, path must not exist", hybrid=True)

    add_copy_move_preserve_arguments(parser)

    preserve_acl_parser = parser.add_mutually_exclusive_group(required=False)
    preserve_acl_parser.add_argument("--preserve-acl", dest="preserve_acl",
                                     default=None, action="store_true")
    preserve_acl_parser.add_argument("--no-preserve-acl", dest="preserve_acl",
                                     default=None, action="store_false")

    parser.add_argument("-r", "--recursive", action="store_true")
    parser.add_argument("-i", "--ignore-existing", action="store_true")
    parser.add_argument("-l", "--lock-existing", action="store_true")
    parser.add_argument("-f", "--force", action="store_true")
    parser.add_argument("--no-pessimistic-quota-check", dest="pessimistic_quota_check", action="store_false")


def add_move_parser(add_parser):
    parser = add_parser("move", yt.move)

    add_ypath_argument(parser, "source_path", help="old node address, path must exist", hybrid=True)
    add_ypath_argument(parser, "destination_path", help="new node address, path must not exist", hybrid=True)

    add_copy_move_preserve_arguments(parser)

    parser.add_argument("-r", "--recursive", action="store_true")
    parser.add_argument("-f", "--force", action="store_true")
    parser.add_argument("--no-pessimistic-quota-check", dest="pessimistic_quota_check", action="store_false")


def add_link_parser(add_parser):
    parser = add_parser("link", yt.link)

    add_ypath_argument(parser, "target_path", help="address of original node to link, path must exist", hybrid=True)
    add_ypath_argument(parser, "link_path", help="address of resulting link, path must not exist", hybrid=True)

    parser.add_argument("-r", "--recursive", action="store_true", help="create parent nodes recursively")
    parser.add_argument("-i", "--ignore-existing", action="store_true")
    parser.add_argument("-l", "--lock-existing", action="store_true")
    parser.add_argument("-f", "--force", action="store_true",
                        help="force create link even if destination already exists "
                             "(supported only on cluster with 19+ version)")
    add_structured_argument(parser, "--attributes")


def add_concatenate_parser(add_parser):
    parser = add_parser("concatenate", yt.concatenate)
    parser.add_argument("--src", action="append", required=True, dest="source_paths", help="Source paths")
    parser.add_argument("--dst", required=True, dest="destination_path", help="Destination paths")


def add_externalize_parser(add_parser):
    parser = add_parser("externalize", yt.externalize)
    add_ypath_argument(parser, "path", hybrid=True)
    parser.add_argument("--cell-tag", type=int, required=True)


def add_internalize_parser(add_parser):
    parser = add_parser("internalize", yt.internalize)
    add_ypath_argument(parser, "path", hybrid=True)
    parser.add_argument("--cell-tag", type=int, required=True)


def tablet_args(parser):
    add_ypath_argument(parser, "path", hybrid=True)
    parser.add_argument("--first-tablet-index", type=int)
    parser.add_argument("--last-tablet-index", type=int)


def add_mount_table_parser(add_parser):
    parser = add_parser("mount-table", yt.mount_table)
    tablet_args(parser)
    parser.add_argument("--freeze", action="store_true")
    parser.add_argument("--sync", action="store_true")
    cell_id_parser = parser.add_mutually_exclusive_group(required=False)
    cell_id_parser.add_argument(
        "--cell-id",
        help="tablet cell id where the tablets will be mounted to, "
             "if omitted then an appropriate cell is chosen automatically")
    cell_id_parser.add_argument(
        "--target-cell-ids", nargs="+",
        help="tablet cell id for each tablet in range "
             "[first-tablet-index, last-tablet-index]. Should be used if exact "
             "destination cell for each tablet is known.")


def add_unmount_table_parser(add_parser):
    parser = add_parser("unmount-table", yt.unmount_table)
    tablet_args(parser)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--sync", action="store_true")


def add_remount_table_parser(add_parser):
    parser = add_parser("remount-table", yt.remount_table)
    tablet_args(parser)


def add_freeze_table_parser(add_parser):
    parser = add_parser("freeze-table", yt.freeze_table)
    tablet_args(parser)
    parser.add_argument("--sync", action="store_true")


def add_unfreeze_table_parser(add_parser):
    parser = add_parser("unfreeze-table", yt.unfreeze_table)
    tablet_args(parser)
    parser.add_argument("--sync", action="store_true")


@copy_docstring_from(yt.get_tablet_infos)
def get_tablet_infos(**kwargs):
    result = yt.get_tablet_infos(**kwargs)
    if kwargs["format"] is None:
        result = dump_data(result)
    print_to_output(result, eoln=False)


def add_get_tablet_infos_parser(add_parser):
    parser = add_parser("get-tablet-infos", get_tablet_infos)
    add_ypath_argument(parser, "path", hybrid=True, help="path to dynamic table")
    parser.add_argument("--tablet-indexes", nargs="+", type=int)
    add_structured_format_argument(parser)


@copy_docstring_from(yt.get_tablet_errors)
def get_tablet_errors(**kwargs):
    result = yt.get_tablet_errors(**kwargs)
    if kwargs["format"] is None:
        result = dump_data(result)
    print_to_output(result, eoln=False)


def add_get_tablet_errors_parser(add_parser):
    parser = add_parser("get-tablet-errors", get_tablet_errors)
    add_ypath_argument(parser, "path", hybrid=True, help="path to dynamic table")
    parser.add_argument("--limit", type=int, help="number of tablets with errors")
    add_structured_format_argument(parser)


@copy_docstring_from(yt.reshard_table)
def reshard_table(**kwargs):
    if kwargs.get("pivot_keys") == []:
        del kwargs["pivot_keys"]
    if kwargs.get("uniform") is False:
        del kwargs["uniform"]
    if kwargs.get("enable_slicing") is False:
        del kwargs["enable_slicing"]
    yt.reshard_table(**kwargs)


def add_reshard_table_parser(add_parser):
    parser = add_parser("reshard-table", reshard_table)
    tablet_args(parser)
    parser.add_argument("pivot_keys", action=ParseStructuredArguments, nargs="*")
    parser.add_argument("--tablet-count", type=int)
    parser.add_argument("--sync", action="store_true")
    parser.add_argument("--uniform", action="store_true")
    parser.add_argument("--enable-slicing", action="store_true")
    parser.add_argument("--slicing-accuracy", type=float)
    parser.add_argument("--trimmed-row-counts", type=int, nargs="+")


def add_reshard_table_automatic_parser(add_parser):
    parser = add_parser("reshard-table-automatic", yt.reshard_table_automatic)
    add_ypath_argument(parser, "path", hybrid=True)
    parser.add_argument("--sync", action="store_true")


def add_balance_tablet_cells_parser(add_parser):
    parser = add_parser("balance-tablet-cells", yt.balance_tablet_cells)
    add_hybrid_argument(parser, "bundle", help="tablet cell bundle", type=str)
    parser.add_argument("--tables", nargs="*", help="tables to balance. If omitted, all tables of bundle"
                                                    " will be balanced")
    parser.add_argument("--sync", action="store_true")


def add_trim_rows_parser(add_parser):
    parser = add_parser("trim-rows", yt.trim_rows)
    add_ypath_argument(parser, "path", hybrid=True)
    parser.add_argument("tablet_index", type=int)
    parser.add_argument("trimmed_row_count", type=int)


def add_alter_table_parser(add_parser):
    parser = add_parser("alter-table", yt.alter_table)
    add_ypath_argument(parser, "path", hybrid=True)
    parser.add_argument("--schema", action=ParseStructuredArgument, action_load_method=parse_arguments, nargs="?",
                        help="new schema value, in {0} format.".format(YT_ARGUMENTS_FORMAT))
    dynamic_parser = parser.add_mutually_exclusive_group(required=False)
    dynamic_parser.add_argument("--dynamic", dest="dynamic", default=None, action="store_true")
    dynamic_parser.add_argument("--static", dest="dynamic", default=None, action="store_false")
    parser.add_argument("--upstream-replica-id", help="upstream replica id")


@copy_docstring_from(yt.alter_table_replica)
def alter_table_replica(**kwargs):
    if kwargs["enable"]:
        kwargs["enabled"] = True
    if kwargs["disable"]:
        kwargs["enabled"] = False

    if kwargs["disable_replicated_table_tracker"]:
        kwargs["enable_replicated_table_tracker"] = False

    kwargs.pop("enable")
    kwargs.pop("disable")
    kwargs.pop("disable_replicated_table_tracker")

    yt.alter_table_replica(**kwargs)


def add_alter_table_replica_parser(add_parser):
    parser = add_parser("alter-table-replica", alter_table_replica)
    parser.add_argument("replica_id")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--enable", action="store_true", help="enable table replica")
    group.add_argument("--disable", action="store_true", help="disable table replica")

    replicated_table_tracker_group = parser.add_mutually_exclusive_group()
    replicated_table_tracker_group.add_argument("--enable-replicated-table-tracker", action="store_true",
                                                help="enable replicated table tracker for table replica")
    replicated_table_tracker_group.add_argument("--disable-replicated-table-tracker", action="store_true",
                                                help="disable replicated table tracker for table replica")

    parser.add_argument("--mode", help='alternation mode, can be "sync" or "async"')


@copy_docstring_from(yt.select_rows)
def select_rows(print_statistics=None, **kwargs):
    response_parameters = None
    if print_statistics:
        response_parameters = {}

    select_result = yt.select_rows(raw=True, response_parameters=response_parameters, **kwargs)
    write_silently(chunk_iter_stream(select_result, yt.config["read_buffer_size"]))
    if print_statistics:
        print_to_output(dump_data(response_parameters), output_stream=sys.stderr)


def add_select_rows_parser(add_parser):
    parser = add_parser(
        "select-rows", select_rows,
        epilog="Supported features: "
               "https://ytsaurus.tech/docs/en/user-guide/dynamic-tables/dyn-query-language")
    add_hybrid_argument(parser, "query")
    parser.add_argument("--timestamp", type=int)
    parser.add_argument("--input-row-limit", type=int)
    parser.add_argument("--output-row-limit", type=int)
    parser.add_argument("--verbose-logging", default=None, action="store_true")
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--enable-code-cache", dest="enable_code_cache", default=None, action="store_true")
    group.add_argument("--disable-code-cache", dest="enable_code_cache", default=None, action="store_false")
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--allow-full-scan", dest="allow_full_scan", default=None, action="store_true")
    group.add_argument("--forbid-full-scan", dest="allow_full_scan", default=None, action="store_false")
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--allow-join-without-index", dest="allow_join_without_index",
                       default=None, action="store_true")
    group.add_argument("--forbid-join-without-index", dest="allow_join_without_index",
                       default=None, action="store_false")
    parser.add_argument("--execution-pool", type=str)
    parser.add_argument("--format", action=ParseFormat)
    parser.add_argument("--print-statistics", default=None, action="store_true")
    parser.add_argument("--syntax-version", type=int)
    parser.add_argument("--udf-registry-path", type=str)
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--merge-versioned-rows", dest="merge_versioned_rows",
                       default=None, action="store_true")
    group.add_argument("--do-not-merge-versioned-rows", dest="merge_versioned_rows",
                       default=None, action="store_false")

    error_message = "Use 'select-rows' instead of 'select'"

    def print_error():
        raise RuntimeError(error_message)
    parser = add_parser("select", print_error, help=error_message)


@copy_docstring_from(yt.explain_query)
def explain_query(**args):
    write_silently(chunk_iter_stream(yt.explain_query(raw=True, **args), yt.config["read_buffer_size"]))


def add_explain_query_parser(add_parser):
    parser = add_parser("explain-query", explain_query)
    add_hybrid_argument(parser, "query")
    parser.add_argument("--timestamp", type=int)
    parser.add_argument("--input-row-limit", type=int)
    parser.add_argument("--output-row-limit", type=int)
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--allow-full-scan", dest="allow_full_scan", default=None, action="store_true")
    group.add_argument("--forbid-full-scan", dest="allow_full_scan", default=None, action="store_false")
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--allow-join-without-index", dest="allow_join_without_index",
                       default=None, action="store_true")
    group.add_argument("--forbid-join-without-index", dest="allow_join_without_index",
                       default=None, action="store_false")
    parser.add_argument("--execution-pool", type=str)
    parser.add_argument("--format", action=ParseFormat)
    parser.add_argument("--syntax-version", type=int)
    parser.add_argument("--udf-registry-path", type=str)


@copy_docstring_from(yt.lookup_rows)
def lookup_rows(**kwargs):
    write_silently(chunk_iter_stream(yt.lookup_rows(raw=True, **kwargs), yt.config["read_buffer_size"]))


def add_lookup_rows_parser(add_parser):
    parser = add_parser("lookup-rows", lookup_rows)
    add_ypath_argument(parser, "table", hybrid=True)
    add_format_argument(parser, help="input format")
    parser.add_argument("--versioned", action="store_true", help="return all versions of the requested rows")
    parser.add_argument("--column-name", action="append", help="column name to lookup", dest="column_names")
    parser.set_defaults(input_stream=get_binary_std_stream(sys.stdin))

    error_message = "Use 'lookup-rows' instead of 'lookup'"

    def print_error():
        raise RuntimeError(error_message)
    parser = add_parser("lookup", print_error, help=error_message)


def add_write_rows_arguments(parser):
    parser.add_argument("--atomicity", choices=("full", "none"))
    parser.add_argument("--durability", choices=("sync", "async"))
    require_sync_replica_parser = parser.add_mutually_exclusive_group(required=False)
    require_sync_replica_parser.add_argument(
        "--require-sync-replica", dest="require_sync_replica", default=None, action="store_true")
    require_sync_replica_parser.add_argument(
        "--no-require-sync-replica", dest="require_sync_replica", default=None, action="store_false")


def add_insert_rows_parser(add_parser):
    parser = add_parser("insert-rows", yt.insert_rows)
    add_ypath_argument(parser, "table", hybrid=True)
    add_format_argument(parser, help="input format")
    add_write_rows_arguments(parser)
    parser.set_defaults(input_stream=get_binary_std_stream(sys.stdin), raw=True)

    update_parser = parser.add_mutually_exclusive_group(required=False)
    update_parser.add_argument("--update", dest="update", default=None, action="store_true")
    update_parser.add_argument("--no-update", dest="update", default=None, action="store_false")
    aggregate_parser = parser.add_mutually_exclusive_group(required=False)
    aggregate_parser.add_argument("--aggregate", dest="aggregate", default=None, action="store_true")
    aggregate_parser.add_argument("--no-aggregate", dest="aggregate", default=None, action="store_false")

    parser.add_argument("--lock_type", dest="lock_type", default=None, type=str)

    error_message = "Use 'insert-rows' instead of 'insert'"

    def print_error():
        raise RuntimeError(error_message)
    parser = add_parser("insert", print_error, help=error_message)


def add_delete_rows_parser(add_parser):
    parser = add_parser("delete-rows", yt.delete_rows)
    add_ypath_argument(parser, "table", hybrid=True)
    add_format_argument(parser, help="input format")
    add_write_rows_arguments(parser)
    parser.set_defaults(input_stream=get_binary_std_stream(sys.stdin), raw=True)

    error_message = "Use 'delete-rows' instead of 'delete'"

    def print_error():
        raise RuntimeError(error_message)
    parser = add_parser("delete", print_error, help=error_message)


def add_register_queue_consumer_parser(add_parser):
    parser = add_parser("register-queue-consumer", yt.register_queue_consumer)
    add_ypath_argument(parser, "queue_path", hybrid=True)
    add_ypath_argument(parser, "consumer_path", hybrid=True)
    add_boolean_argument(parser, "vital", negation_prefix="non", required=True, help="Whether the consumer is vital")
    parser.add_argument("--partitions", type=int, nargs="*")


def add_unregister_queue_consumer_parser(add_parser):
    parser = add_parser("unregister-queue-consumer", yt.unregister_queue_consumer)
    add_ypath_argument(parser, "queue_path", hybrid=True)
    add_ypath_argument(parser, "consumer_path", hybrid=True)


@copy_docstring_from(yt.list_queue_consumer_registrations)
def list_queue_consumer_registrations(**kwargs):
    result = yt.list_queue_consumer_registrations(**kwargs)
    if kwargs["format"] is None:
        result = dump_data(result)
    print_to_output(result, eoln=False)


def add_list_queue_consumer_registrations_parser(add_parser):
    parser = add_parser("list-queue-consumer-registrations", list_queue_consumer_registrations)
    add_ypath_argument(parser, "--queue-path", help="Path to queue in Cypress; cluster may be specified")
    add_ypath_argument(parser, "--consumer-path", help="Path to consumer in Cypress; cluster may be specified")
    add_structured_format_argument(parser)


@copy_docstring_from(yt.pull_queue)
def pull_queue(print_statistics=None, **kwargs):
    result = yt.pull_queue(raw=True, **kwargs)
    write_silently(chunk_iter_stream(result, yt.config["read_buffer_size"]))


def add_pull_queue_parser(add_parser):
    parser = add_parser("pull-queue", pull_queue)
    add_ypath_argument(parser, "queue_path", hybrid=True)
    parser.add_argument("--offset", type=int, required=True)
    parser.add_argument("--partition-index", type=int, required=True)
    parser.add_argument("--max-row-count", type=int)
    parser.add_argument("--max-data-weight", type=int)
    parser.add_argument("--replica-consistency", type=str, choices=["none", "sync"])
    add_structured_format_argument(parser)


@copy_docstring_from(yt.pull_consumer)
def pull_consumer(print_statistics=None, **kwargs):
    result = yt.pull_consumer(raw=True, **kwargs)
    write_silently(chunk_iter_stream(result, yt.config["read_buffer_size"]))


def add_pull_consumer_parser(add_parser):
    parser = add_parser("pull-consumer", pull_consumer)
    add_ypath_argument(parser, "consumer_path", hybrid=True)
    add_ypath_argument(parser, "queue_path", hybrid=True)
    parser.add_argument("--offset", type=int, required=True)
    parser.add_argument("--partition-index", type=int, required=True)
    parser.add_argument("--max-row-count", type=int)
    parser.add_argument("--max-data-weight", type=int)
    parser.add_argument("--replica-consistency", type=str, choices=["none", "sync"])
    add_structured_format_argument(parser)


@copy_docstring_from(yt.pull_queue_consumer)
def pull_queue_consumer(print_statistics=None, **kwargs):
    result = yt.pull_queue_consumer(raw=True, **kwargs)
    write_silently(chunk_iter_stream(result, yt.config["read_buffer_size"]))


def add_pull_queue_consumer_parser(add_parser):
    parser = add_parser("pull-queue-consumer", pull_queue_consumer)
    add_ypath_argument(parser, "consumer_path", hybrid=True)
    add_ypath_argument(parser, "queue_path", hybrid=True)
    parser.add_argument("--offset", type=int, required=True)
    parser.add_argument("--partition-index", type=int, required=True)
    parser.add_argument("--max-row-count", type=int)
    parser.add_argument("--max-data-weight", type=int)
    parser.add_argument("--replica-consistency", type=str, choices=["none", "sync"])
    add_structured_format_argument(parser)


def add_advance_consumer_parser(add_parser):
    parser = add_parser("advance-consumer", yt.advance_consumer)
    add_ypath_argument(parser, "consumer_path", hybrid=True)
    add_ypath_argument(parser, "queue_path", hybrid=True)
    parser.add_argument("--partition-index", type=int, required=True)
    parser.add_argument("--old-offset", type=int)
    parser.add_argument("--new-offset", type=int, required=True)


def add_advance_queue_consumer_parser(add_parser):
    parser = add_parser("advance-queue-consumer", yt.advance_queue_consumer)
    add_ypath_argument(parser, "consumer_path", hybrid=True)
    add_ypath_argument(parser, "queue_path", hybrid=True)
    parser.add_argument("--partition-index", type=int, required=True)
    parser.add_argument("--old-offset", type=int)
    parser.add_argument("--new-offset", type=int, required=True)


@copy_docstring_from(yt.push_queue_producer)
def push_queue_producer(**kwargs):
    result = yt.push_queue_producer(**kwargs)
    if kwargs["output_format"] is None:
        result = dump_data(result)
    print_to_output(result, eoln=False)


def add_push_queue_producer_parser(add_parser):
    parser = add_parser("push-queue-producer", push_queue_producer)
    add_ypath_argument(parser, "producer_path", hybrid=True)
    add_ypath_argument(parser, "queue_path", hybrid=True)
    parser.add_argument("--session-id", type=str, required=True)
    parser.add_argument("--epoch", type=int, required=True)

    require_sync_replica_parser = parser.add_mutually_exclusive_group(required=False)
    require_sync_replica_parser.add_argument(
        "--require-sync-replica", dest="require_sync_replica", default=None, action="store_true")
    require_sync_replica_parser.add_argument(
        "--no-require-sync-replica", dest="require_sync_replica", default=None, action="store_false")

    add_format_argument(parser, name="--input-format", help="input format")
    add_structured_format_argument(parser, name="--output-format", help="output format")

    parser.set_defaults(input_stream=get_binary_std_stream(sys.stdin), raw=True)


@copy_docstring_from(yt.create_queue_producer_session)
def create_queue_producer_session(**kwargs):
    result = yt.create_queue_producer_session(**kwargs)
    if kwargs["format"] is None:
        result = dump_data(result)
    print_to_output(result, eoln=False)


def add_create_queue_producer_session_parser(add_parser):
    parser = add_parser("create-queue-producer-session", create_queue_producer_session)
    add_ypath_argument(parser, "--producer-path", help="Path to producer in Cypress; cluster may be specified")
    add_ypath_argument(parser, "--queue-path", help="Path to queue in Cypress; cluster may be specified")
    parser.add_argument("--session-id", type=str, required=True)
    add_structured_format_argument(parser)


def add_remove_queue_producer_session_parser(add_parser):
    parser = add_parser("remove-queue-producer-session", yt.remove_queue_producer_session)
    add_ypath_argument(parser, "--producer-path", help="Path to producer in Cypress; cluster may be specified")
    add_ypath_argument(parser, "--queue-path", help="Path to queue in Cypress; cluster may be specified")
    parser.add_argument("--session-id", type=str, required=True)


@copy_docstring_from(yt.start_query)
def start_query(*args, **kwargs):
    query_id = yt.start_query(*args, **kwargs)
    print(query_id)


def add_start_query_parser(add_parser):
    parser = add_parser("start-query", start_query)
    parser.add_argument("engine", type=str, help='engine of a query, one of "ql", "yql", "chyt", "spyt"')
    parser.add_argument("query", type=str, help="query text")
    add_structured_argument(parser, "--settings", help="additional settings of a query in structured form")
    add_structured_argument(parser, "--files", help='query files, a YSON list of files, each of which is represented by a map with keys "name", "content", "type". '
                                                    'Field "type" is one of "raw_inline_data", "url"')
    parser.add_argument("--access-control-object", type=str, help='optional access control object name (deprecated)')
    add_structured_argument(parser, "--access-control-objects", help='access control objects, a YSON list of ACO names')
    parser.add_argument("--stage", type=str, help='query tracker stage, defaults to "production"')


@copy_docstring_from(yt.run_query)
def run_query(*args, format=None, structured=None, **kwargs):
    is_async = kwargs.pop("async", False)

    if structured:
        if format is None:
            format = yt.format.create_format(OUTPUT_FORMATS[YT_STRUCTURED_DATA_FORMAT])
    else:
        # Perform an early check similar to the one in read_query_result to
        # avoid late errors (after query execution) in case user forgets to specify format.
        if format is None:
            raise yt.YtError("You should specify format")

    query = yt.run_query(*args, sync=not is_async, **kwargs)
    if is_async:
        print(query.id)
        return

    if structured:
        result = []
        for query_result in query.get_results():
            attributes = query_result.get_attributes()
            if "error" not in attributes:
                attributes["rows"] = builtins.list(query_result.read_rows(validate_not_truncated=False, raw=False))
            result.append(attributes)
        print_to_output(format.dumps_node(result))
    else:
        query_results = query.get_results()
        if len(query_results) == 1:
            query_result = query_results[0]
            write_silently(chunk_iter_stream(query_result.read_rows(format=format), yt.config["read_buffer_size"]))
        else:
            raise yt.YtError("Multiple query results are not supported when format is specified")


def add_query_parser(add_parser):
    parser = add_parser("query", run_query)
    parser.add_argument("engine", type=str, help='engine of a query, one of "ql", "yql", "chyt", "spyt"')
    parser.add_argument("query", type=str, help="query text")
    add_structured_argument(parser, "--settings", help="additional settings of a query in structured form")
    add_structured_argument(parser, "--files",
                            help='query files, a YSON list of files, each of which is represented by a map with keys '
                                 '"name", "content", "type". Field "type" is one of "raw_inline_data", "url"')
    add_structured_argument(parser, "--access-control-objects",
                            help='access control objects, a YSON list of ACO names')
    parser.add_argument("--stage", type=str, help='query tracker stage, defaults to "production"')
    parser.add_argument("--structured", action="store_true", help="output all query results together with their "
                                                                  "metainformation in structured form")
    parser.add_argument("--async", action="store_true", default=False, help="do not track query progress")
    add_format_argument(parser, help="output format")


def add_abort_query_parser(add_parser):
    parser = add_parser("abort-query", yt.abort_query)
    parser.add_argument("query_id", type=str, help="query id")
    parser.add_argument("--message", type=str, help="optional abort message")
    parser.add_argument("--stage", type=str, help='query tracker stage, defaults to "production"')


@copy_docstring_from(yt.read_query_result)
def read_query_result(**kwargs):
    write_silently(chunk_iter_stream(yt.read_query_result(raw=True, **kwargs), yt.config["read_buffer_size"]))


def add_read_query_result_parser(add_parser):
    parser = add_parser("read-query-result", read_query_result)
    parser.add_argument("query_id", type=str, help="query id")
    parser.add_argument("--result-index", type=int, help="index of query result, defaults to 0")
    parser.add_argument("--stage", type=str, help='query tracker stage, defaults to "production"')
    add_format_argument(parser, help="output format")


@copy_docstring_from(yt.get_query)
def get_query(**kwargs):
    result = yt.get_query(**kwargs)
    if kwargs["format"] is None:
        result = dump_data(result)
    print_to_output(result, eoln=False)


def add_get_query_parser(add_parser):
    parser = add_parser("get-query", get_query)
    parser.add_argument("query_id", type=str, help="query id")
    parser.add_argument("--attribute", action="append", dest="attributes", help="desired attributes in the response")
    parser.add_argument("--stage", type=str, help='query tracker stage, defaults to "production"')
    add_structured_format_argument(parser)


@copy_docstring_from(yt.get_query_result)
def get_query_result(**kwargs):
    result = yt.get_query_result(**kwargs)
    if kwargs["format"] is None:
        result = dump_data(result)
    print_to_output(result, eoln=False)


def add_get_query_result_parser(add_parser):
    parser = add_parser("get-query-result", get_query_result)
    parser.add_argument("query_id", type=str, help="query id")
    parser.add_argument("--result-index", type=int, help="index of query result, defaults to 0")
    parser.add_argument("--stage", type=str, help='query tracker stage, defaults to "production"')
    add_structured_format_argument(parser)


@copy_docstring_from(yt.list_queries)
def list_queries(**kwargs):
    result = yt.list_queries(**kwargs)
    if kwargs["format"] is None:
        result = dump_data(result)
    print_to_output(result, eoln=False)


def add_list_queries_parser(add_parser):
    parser = add_parser("list-queries", list_queries)
    parser.add_argument("--user", help="filter queries by user")
    parser.add_argument("--engine", help="filter queries by engine")
    parser.add_argument("--state", help="filter queries by state")
    parser.add_argument("--filter", help="filter queries by some text factor. "
                                         "For example, part of the query can be passed to this option")
    add_time_argument(parser, "--from-time", help="lower limit for operations start time")
    add_time_argument(parser, "--to-time", help="upper limit for operations start time")
    add_time_argument(parser, "--cursor-time",
                      help="cursor time. Used in combination with --cursor-direction and --limit")
    parser.add_argument("--cursor-direction", help='cursor direction, can be one of ("none", "past", "future"). '
                                                   'Used in combination with --cursor-time and --limit')
    parser.add_argument("--limit", type=int, help="maximum number of operations in output")
    parser.add_argument("--attribute", action="append", dest="attributes", help="desired attributes in the response")
    parser.add_argument("--stage", type=str, help='query tracker stage, defaults to "production"')
    add_structured_format_argument(parser)


def add_alter_query_parser(add_parser):
    parser = add_parser("alter-query", yt.alter_query)
    parser.add_argument("query_id", type=str, help="query id")
    add_structured_argument(parser, "--annotations", help='a YSON map of annotations')
    add_structured_argument(parser, "--access-control-objects", help='access control objects, a YSON list of ACO names')
    parser.add_argument("--stage", type=str, help='query tracker stage, defaults to "production"')


SPEC_BUILDERS = {
    "map": MapSpecBuilder,
    "reduce": ReduceSpecBuilder,
    "map-reduce": MapReduceSpecBuilder,
    "join-reduce": JoinReduceSpecBuilder,
    "merge": MergeSpecBuilder,
    "sort": SortSpecBuilder,
    "erase": EraseSpecBuilder,
    "remote-copy": RemoteCopySpecBuilder
}


def get_spec_description(spec_builder_class, depth=0):
    spec_options = []
    methods = sorted(dir(spec_builder_class))
    for method_name in methods:
        method = getattr(spec_builder_class, method_name)
        if getattr(method, "is_spec_method", False):
            spec_options.append("  {0} - {1}".format(method_name, method.description))
            if method.nested_spec_builder:
                spec_options += ["  " + get_spec_description(method.nested_spec_builder, depth + 1)]
    result = ("\n" + "  " * depth).join(spec_options)
    return result


def show_spec(operation):
    """Shows available spec options of the operation."""
    if operation not in SPEC_BUILDERS:
        raise yt.YtError("No such operation", attributes={"operation": operation})

    spec_description = get_spec_description(SPEC_BUILDERS[operation])
    print("Spec options for {0} operation\n".format(operation))
    print(spec_description)
    print("\nSee more in the documentation: https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/overview")


def add_show_spec_parser(add_parser):
    parser = add_parser("show-spec", show_spec)
    parser.add_argument("operation", help="operation type")


def add_src_arg(parser, **kwargs):
    parser.add_argument("--src", action="append", nargs="+", required=True, dest="source_table", **kwargs)


def add_operation_handler_related_args(parser):
    parser.add_argument("--print-statistics", action="store_true", default=False)
    parser.add_argument("--async", action="store_true", default=False, help="do not track operation progress")


def operation_handler(function):
    def handler(*args, **kwargs):
        print_statistics = kwargs.pop("print_statistics", False)
        kwargs["sync"] = not kwargs.pop("async", False)

        operation = function(*args, **kwargs)

        if not kwargs["sync"]:
            print(operation.id)

        if print_statistics:
            print_to_output(dump_data(operation.get_attributes()["progress"]), output_stream=sys.stderr)

    handler.__doc__ = inspect.getdoc(function)

    return handler


def operation_args(parser):
    add_hybrid_argument(parser, "binary", metavar="command")
    add_src_arg(parser)
    parser.add_argument("--dst", action="append", required=True, dest="destination_table")
    parser.add_argument("--file", action="append", dest="yt_files")
    parser.add_argument("--local-file", action="append", dest="local_files")
    parser.add_argument("--job-count", type=int)
    parser.add_argument("--memory-limit", type=int, action=ParseMemoryLimit, help="in MB")
    add_structured_argument(parser, "--spec")
    parser.add_argument("--format", action=ParseFormat)
    parser.add_argument("--input-format", action=ParseFormat)
    parser.add_argument("--output-format", action=ParseFormat)
    add_operation_handler_related_args(parser)


def add_erase_parser(add_parser):
    parser = add_parser("erase", operation_handler(yt.run_erase),
                        epilog="Note you can specify range in table to erase.")
    add_ypath_argument(parser, "table", help="path to table to erase", hybrid=True)
    add_operation_handler_related_args(parser)
    add_structured_argument(parser, "--spec")


def add_merge_parser(add_parser):
    parser = add_parser("merge", operation_handler(yt.run_merge))
    add_src_arg(parser)
    add_ypath_argument(
        parser, "--dst", required=True, dest="destination_table",
        help="path to destination table. For append mode add <append=true> before path.")
    parser.add_argument(
        "--mode", default="auto", choices=["unordered", "ordered", "sorted", "auto"],
        help="use sorted mode for saving sortedness. unordered mode by default, ordered for saving order of chunks. "
             "Mode auto chooses from sorted and unordered modes depending on sortedness of source tables.")
    add_operation_handler_related_args(parser)
    add_structured_argument(parser, "--spec")


def add_sort_parser(add_parser):
    parser = add_parser("sort", operation_handler(yt.run_sort))
    add_src_arg(parser)
    parser.add_argument("--dst", required=True, dest="destination_table")
    add_sort_order_argument(parser, "--sort-by", "Columns to sort by.", required=True)
    add_operation_handler_related_args(parser)
    add_structured_argument(parser, "--spec")


def add_map_parser(add_parser):
    parser = add_parser("map", operation_handler(yt.run_map),
                        epilog="See also yt map-reduce --help for brief options description.")
    operation_args(parser)
    parser.add_argument("--ordered", action="store_true", help="Force ordered input for mapper.")


def add_reduce_parser(add_parser):
    parser = add_parser(
        "reduce",
        operation_handler(yt.run_reduce),
        epilog="See also yt map-reduce --help for brief options description.\n"
               "Note, source tables must be sorted! "
               "For reducing not sorted table use map-reduce command without --mapper")
    operation_args(parser)
    add_sort_order_argument(parser, "--reduce-by", help="Columns to reduce by.", required=False)
    add_sort_order_argument(parser, "--sort-by", help="Columns to sort by.", required=False)
    add_sort_order_argument(parser, "--join-by", help="Columns to join by.", required=False)


def add_join_reduce_parser(add_parser):
    parser = add_parser(
        "join-reduce",
        operation_handler(yt.run_join_reduce),
        epilog="See also yt map-reduce --help for brief options description.\n Note, source tables must be sorted!")
    operation_args(parser)
    add_sort_order_argument(parser, "--join-by", help="Columns to join by.", required=True)


def add_remote_copy_parser(add_parser):
    parser = add_parser(
        "remote-copy",
        operation_handler(yt.run_remote_copy),
        epilog="Note, --proxy for destination, --cluster for source")
    add_src_arg(parser, help="path to source tables in remote cluster")
    parser.add_argument("--dst", required=True, dest="destination_table",
                        help="path to destination table in current cluster")
    parser.add_argument("--cluster", required=True, dest="cluster_name",
                        help="remote cluster proxy, like smith")
    parser.add_argument("--network", dest="network_name")
    parser.add_argument("--copy-attributes", action="store_true",
                        help="specify this flag to coping node attributes too")
    add_operation_handler_related_args(parser)
    add_structured_argument(parser, "--cluster-connection")
    add_structured_argument(parser, "--spec")


def run_vanilla_operation(sync, tasks, spec):
    """Run vanilla operation."""
    spec_builder = VanillaSpecBuilder()
    if tasks is not None:
        for name, description in iteritems(tasks):
            spec_builder.task(name, description)
    spec_builder.spec(spec)
    return yt.run_operation(spec_builder, sync=sync)


def add_vanilla_parser(add_parser):
    parser = add_parser("vanilla", operation_handler(run_vanilla_operation))
    add_operation_handler_related_args(parser)
    add_structured_argument(parser, "--tasks", help="task descriptions")
    add_structured_argument(parser, "--spec")


@copy_docstring_from(yt.shuffle_table)
def shuffle_handler(*args, **kwargs):
    if "temp_column_name" in kwargs and kwargs["temp_column_name"] is None:
        del kwargs["temp_column_name"]

    operation_handler(yt.shuffle_table)(*args, **kwargs)


def add_shuffle_parser(add_parser):
    parser = add_parser("shuffle", shuffle_handler)
    parser.add_argument("--table", required=True)
    parser.add_argument("--temp-column-name")
    parser.add_argument("--print-statistics", action="store_true", default=False)
    parser.add_argument("--async", action="store_true", default=False, help="do not track operation progress")


def add_map_reduce_parser(add_parser):
    MAP_REDUCE_EPILOG = '''Options --mapper, --reducer, --combiner specify bash commands to run.
--mapper and --reduce-combiner suboperations are optional. Only --reducer is required!
For pure bash command like "grep sepulki" there is no need for specifying any path.
For user script like "python my_script.py" you must specify YT path to script by some way.
User scripts are searched in all --<operation>-file path, and in //tmp path if --<operation>-local-file is specified.
(--<operation>-local-file option specify path to script on your local machine and upload to yt //tmp directory.)
These option --*-file can be specified multiple times.

For every operation it is possible to specify memory limit per one user job (in MB), input and output formats.
(Your script will receive binary or text data in input format. System will parse output of script as output format data).
Option --format specify both input and output for all suboperations, more specific format overwrite it.

Source and destination tables can be specified multiple times (but be ready to process table switchers in your scripts).

For append mode in destination table add <append=true> modificator to path.
'''
    parser = add_parser("map-reduce", operation_handler(yt.run_map_reduce),
                        epilog=MAP_REDUCE_EPILOG, formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("--mapper", required=False)
    parser.add_argument("--reducer", required=True)
    parser.add_argument("--reduce-combiner", required=False)
    parser.add_argument("--src", action="append", required=True, dest="source_table")
    parser.add_argument("--dst", action="append", required=True, dest="destination_table")
    parser.add_argument("--map-file", action="append", dest="map_yt_files")
    parser.add_argument("--map-local-file", action="append", dest="map_local_files")
    parser.add_argument("--reduce-file", action="append", dest="reduce_yt_files")
    parser.add_argument("--reduce-local-file", action="append", dest="reduce_local_files")
    parser.add_argument("--reduce-combiner-file", action="append", dest="reduce_combiner_yt_files")
    parser.add_argument("--reduce-combiner-local-file", action="append", dest="reduce_combiner_local_files")
    parser.add_argument("--mapper-memory-limit", "--map-memory-limit", type=int, action=ParseMemoryLimit, help="in MB")
    parser.add_argument("--reducer-memory-limit", "--reduce-memory-limit", type=int, action=ParseMemoryLimit, help="in MB")
    parser.add_argument("--reduce-combiner-memory-limit", type=int, action=ParseMemoryLimit, help="in MB")
    add_sort_order_argument(parser, "--reduce-by", help="Columns to reduce by.", required=True)
    add_sort_order_argument(parser, "--sort-by", help="Columns to sort by. Must be superset of reduce-by columns. "
                                                      "By default is equal to --reduce-by option.", required=False)
    add_structured_argument(parser, "--spec")
    add_format_argument(parser)
    parser.add_argument("--map-input-format", action=ParseFormat, help="see --format help")
    parser.add_argument("--map-output-format", action=ParseFormat, help="see --format help")
    parser.add_argument("--reduce-input-format", action=ParseFormat, help="see --format help")
    parser.add_argument("--reduce-output-format", action=ParseFormat, help="see --format help")
    parser.add_argument("--reduce-combiner-input-format", action=ParseFormat, help="see --format help")
    parser.add_argument("--reduce-combiner-output-format", action=ParseFormat, help="see --format help")
    parser.add_argument("--async", action="store_true", default=False, help="do not track operation progress")


def operation_id_args(parser, **kwargs):
    add_hybrid_argument(parser, "operation", help="operation id", **kwargs)


def add_abort_op_parser(add_parser):
    parser = add_parser("abort-op", yt.abort_operation)
    parser.add_argument("--reason", help="abort reason")
    operation_id_args(parser)


def add_suspend_op_parser(add_parser):
    parser = add_parser("suspend-op", yt.suspend_operation)
    operation_id_args(parser)
    parser.add_argument("--abort-running-jobs", help="abort running jobs", action="store_true")
    parser.add_argument("--reason", help="suspend reason")


def add_resume_op_parser(add_parser):
    parser = add_parser("resume-op", yt.resume_operation)
    operation_id_args(parser)


@copy_docstring_from(yt.Operation.wait)
def track_op(**kwargs):
    yt.config["operation_tracker"]["abort_on_sigint"] = False
    operation = yt.Operation(kwargs["operation"])
    operation.wait()


def add_track_op_parser(add_parser):
    parser = add_parser("track-op", track_op)
    operation_id_args(parser)


def add_complete_op_parser(add_parser):
    parser = add_parser("complete-op", yt.complete_operation)
    operation_id_args(parser)


def add_update_op_parameters_parser(add_parser):
    parser = add_parser("update-op-parameters", yt.update_operation_parameters)
    operation_id_args(parser, dest="operation_id")
    add_structured_argument(parser, "parameters")


@copy_docstring_from(yt.get_operation)
def get_operation(**kwargs):
    result = yt.get_operation(**kwargs)
    if kwargs["format"] is None:
        result = dump_data(result)
    print_to_output(result, eoln=False)


def add_get_operation_parser(add_parser):
    parser = add_parser("get-operation", get_operation)
    parser.add_argument("--attribute", action="append", dest="attributes", help="desired attributes in the response")
    parser.add_argument("--include-runtime", action="store_true", help="request runtime operation information")
    operation_id_args(parser, dest="operation_id")
    add_structured_format_argument(parser)


def add_time_argument(parser, name, help="", **kwargs):
    description = "Time is accepted as unix timestamp or time string in YT format"
    add_argument(parser, name, help, description=description, action=ParseTimeArgument, **kwargs)


@copy_docstring_from(yt.list_operations)
def list_operations(**kwargs):
    kwargs["include_counters"] = not kwargs.pop("no_include_counters")
    result = yt.list_operations(**kwargs)
    if kwargs["format"] is None:
        result = dump_data(result)
    print_to_output(result, eoln=False)


@copy_docstring_from(yt.list_operations)
def add_list_operations_parser(add_parser):
    parser = add_parser("list-operations", list_operations)
    parser.add_argument("--user", help="filter operations by user")
    parser.add_argument("--state", help="filter operations by state")
    parser.add_argument("--type", help="filter operations by operation type")
    parser.add_argument("--filter", help="filter operation by some text factor. "
                                         "For example, part of the title can be passed to this option")
    parser.add_argument("--pool-tree", help="filter operations by pool tree")
    parser.add_argument("--pool", help="filter operations by pool. "
                                       "If --pool-tree is set, filters operations with this pool in specified pool tree")

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--with-failed-jobs", action="store_const", const=True,
                       help="show only operations with failed jobs")
    group.add_argument("--without-failed-jobs", action="store_const", dest="with_failed_jobs", const=False,
                       help="show only operations without failed jobs")

    add_time_argument(parser, "--from-time", help="lower limit for operations start time")
    add_time_argument(parser, "--to-time", help="upper limit for operations start time")
    add_time_argument(parser, "--cursor-time",
                      help="cursor time. Used in combination with --cursor-direction and --limit")
    parser.add_argument("--cursor-direction", help='cursor direction, can be one of ("none", "past", "future"). '
                                                   'Used in combination with --cursor-time and --limit')
    parser.add_argument("--include-archive", action="store_true", help="include operations from archive in result")
    parser.add_argument("--no-include-counters", action="store_true",
                        help="do not include operation counters in result")
    parser.add_argument("--limit", type=int, help="maximum number of operations in output")
    add_structured_format_argument(parser)


@copy_docstring_from(yt.start_transaction)
def start_tx(**kwargs):
    print(yt.start_transaction(**kwargs))


def add_start_tx_parser(add_parser):
    parser = add_parser("start-tx", start_tx)
    add_structured_argument(parser, "--attributes")
    parser.add_argument("--timeout", type=int,
                        help="transaction lifetime since last ping in milliseconds")


def transaction_args(parser):
    add_hybrid_argument(parser, "transaction",
                        help="transaction id, for example: 5c51-24e204-1-9f3f6437")


def add_commit_tx_parser(add_parser):
    parser = add_parser("commit-tx", yt.commit_transaction)
    transaction_args(parser)


def add_abort_tx_parser(add_parser):
    parser = add_parser("abort-tx", yt.abort_transaction)
    transaction_args(parser)


def add_ping_tx_parser(add_parser):
    parser = add_parser("ping-tx", yt.ping_transaction)
    transaction_args(parser)


def lock(**kwargs):
    """Tries to lock the path. Do not forget about global `--tx` option"""
    lock_id = yt.lock(**kwargs)
    if lock_id is not None:
        print(lock_id)
    else:
        print("lock was not taken")


def add_lock_parser(add_parser):
    parser = add_parser("lock", lock)
    add_ypath_argument(parser, "path", hybrid=True)
    parser.add_argument("--mode", choices=["snapshot", "shared", "exclusive"],
                        help="blocking type, exclusive by default")
    parser.add_argument("--waitable", action="store_true", help="wait for lock if node is under blocking")
    parser.add_argument("--wait-for", type=int, help="wait interval in milliseconds")
    parser.add_argument("--child-key", help="child key of shared lock")
    parser.add_argument("--attribute-key", help="attribute key of shared lock")


def add_unlock_parser(add_parser):
    parser = add_parser("unlock", yt.unlock)
    add_ypath_argument(parser, "path", hybrid=True)


@copy_docstring_from(yt.check_permission)
def check_permission(**kwargs):
    print_to_output(yt.check_permission(**kwargs), eoln=False)


def add_check_permission_parser(add_parser):
    parser = add_parser("check-permission", check_permission)
    add_hybrid_argument(parser, "user")
    add_hybrid_argument(parser, "permission", help="one of read, write, administer, create, use")
    add_ypath_argument(parser, "path", hybrid=True)
    add_read_from_arguments(parser)
    add_structured_argument(parser, "--columns")
    add_structured_format_argument(parser, default=output_format)


def member_args(parser):
    add_hybrid_argument(parser, "member")
    add_hybrid_argument(parser, "group")


def add_add_member_parser(add_parser):
    parser = add_parser("add-member", yt.add_member)
    member_args(parser)


def add_remove_member_parser(add_parser):
    parser = add_parser("remove-member", yt.remove_member)
    member_args(parser)


def execute(**kwargs):
    if "output_format" not in kwargs["execute_params"]:
        kwargs["execute_params"]["output_format"] = yt.create_format(output_format)
    data = chunk_iter_stream(sys.stdin, 16 * MB) if "input_format" in kwargs["execute_params"] else None
    result = yt.driver.make_request(kwargs["command_name"], kwargs["execute_params"], data=data)
    if result is not None:
        print_to_output(result, eoln=False)


def add_execute_parser(add_parser):
    parser = add_parser("execute", help="execute your command")
    parser.add_argument("command_name")
    add_structured_argument(parser, "execute_params")
    parser.set_defaults(func=execute)


@copy_docstring_from(yt.get_table_columnar_statistics)
def get_table_columnar_statistics(**kwargs):
    print_to_output(dump_data(yt.get_table_columnar_statistics(**kwargs)), eoln=False)


def add_get_table_columnar_statistics_parser(add_parser):
    parser = add_parser("get-table-columnar-statistics", get_table_columnar_statistics)
    add_ypath_argument(parser, "--path", required=True, action="append", dest="paths",
                       help="Path to source table.")


@copy_docstring_from(yt.execute_batch)
def execute_batch(**kwargs):
    print_to_output(dump_data(yt.execute_batch(**kwargs)), eoln=False)


def add_execute_batch_parser(add_parser):
    parser = add_parser("execute-batch", execute_batch)
    parser.add_argument("requests", action=ParseStructuredArguments, nargs="+", help="Request description")


def add_transfer_account_resources_parser(add_parser):
    parser = add_parser("transfer-account-resources", yt.transfer_account_resources)
    add_hybrid_argument(parser, "source_account", aliases=["--src"])
    add_hybrid_argument(parser, "destination_account", aliases=["--dst"])
    add_structured_argument(parser, "--resource-delta")


def add_transfer_pool_resources_parser(add_parser):
    parser = add_parser("transfer-pool-resources", yt.transfer_pool_resources)
    add_hybrid_argument(parser, "source_pool", aliases=["--src"])
    add_hybrid_argument(parser, "destination_pool", aliases=["--dst"])
    add_hybrid_argument(parser, "pool_tree")
    add_structured_argument(parser, "--resource-delta")


@copy_docstring_from(yt.generate_timestamp)
def generate_timestamp():
    print_to_output(str(yt.generate_timestamp()))


def add_generate_timestamp_parser(add_parser):
    add_parser("generate-timestamp", generate_timestamp)


def add_run_job_shell_parser(add_parser):
    parser = add_parser("run-job-shell", yt.run_job_shell)
    parser.add_argument("job_id", help="job id, for example: 5c51-24e204-384-9f3f6437")
    parser.add_argument("--shell-name", type=str, help="name of the job shell to start")
    parser.add_argument("--timeout", type=int, help="inactivity timeout in milliseconds after job has "
                                                    "finished, by default 60000 milliseconds")
    add_hybrid_argument(parser, "command", group_required=False)


def add_abort_job_parser(add_parser):
    parser = add_parser("abort-job", yt.abort_job)
    parser.add_argument("job_id", help="job id, for example: 5c51-24e204-384-9f3f6437")
    parser.add_argument("--interrupt-timeout", type=int,
                        help="try to interrupt job before abort during timeout (in ms)")


@copy_docstring_from(yt.get_job_stderr)
def get_job_stderr(**kwargs):
    write_silently(chunk_iter_stream(yt.get_job_stderr(**kwargs), yt.config["read_buffer_size"]))


def add_get_job_stderr_parser(add_parser):
    parser = add_parser("get-job-stderr", get_job_stderr)
    add_hybrid_argument(parser, "job_id", help="job id, for example: 5c51-24e204-384-9f3f6437")
    add_hybrid_argument(parser, "operation_id", help="operation id, for example: 876084ca-efd01a47-3e8-7a62e787")


@copy_docstring_from(yt.get_job_input)
def get_job_input(**kwargs):
    write_silently(chunk_iter_stream(yt.get_job_input(**kwargs), yt.config["read_buffer_size"]))


def add_get_job_input_parser(add_parser):
    parser = add_parser("get-job-input", get_job_input)
    add_hybrid_argument(parser, "job_id", help="job id, for example: 5c51-24e204-384-9f3f6437")


@copy_docstring_from(yt.get_job_input_paths)
def get_job_input_paths(**kwargs):
    paths = yt.get_job_input_paths(**kwargs)
    print_to_output(dump_data(map(yt.YPath.to_yson_type, paths)), eoln=False)


def add_get_job_input_paths_parser(add_parser):
    parser = add_parser("get-job-input-paths", get_job_input_paths)
    add_hybrid_argument(parser, "job_id", help="job id, for example: 5c51-24e204-384-9f3f6437")


@copy_docstring_from(yt.get_job_spec)
def get_job_spec(**kwargs):
    job_spec = yt.get_job_spec(**kwargs)
    print_to_output(dump_data(job_spec))


def add_get_job_spec_parser(add_parser):
    parser = add_parser("get-job-spec", get_job_spec)
    add_hybrid_argument(parser, "job_id", help="job id, for example: 5c51-24e204-384-9f3f6437")
    add_boolean_argument(parser, "omit_node_directory", help="Whether node directory should be removed from job spec (true by default)", default=True)
    add_boolean_argument(parser, "omit_input_table_specs", help='Whether input table specs should be removed from job spec (false by default)', default=False)
    add_boolean_argument(parser, "omit_output_table_specs", help='Whether output table specs should be removed from job spec (false by default)', default=False)


def add_set_user_password_parser(add_parser):
    parser = add_parser("set-user-password", cli_impl._set_user_password_interactive)
    parser.add_argument("user", help="user to set password")


@copy_docstring_from(cli_impl._issue_token_interactive)
def issue_token(**kwargs):
    result = cli_impl._issue_token_interactive(**kwargs)
    print_to_output(result)


def add_issue_token_parser(add_parser):
    parser = add_parser("issue-token", issue_token)
    parser.add_argument("user", help="user to issue token")


def add_revoke_token_parser(add_parser):
    parser = add_parser("revoke-token", cli_impl._revoke_token_interactive)
    parser.add_argument("user", help="user to revoke token")
    parser.add_argument("--token-sha256", type=str, help="sha256-encoded token to revoke; if omitted, token value "
                                                         "is requested interactively")


@copy_docstring_from(cli_impl._list_user_tokens_interactive)
def list_user_tokens(**kwargs):
    result = cli_impl._list_user_tokens_interactive(**kwargs)
    print_to_output(result)


def add_list_user_tokens(add_parser):
    parser = add_parser("list-user-tokens", list_user_tokens)
    parser.add_argument("user", help="user to revoke token")


@copy_docstring_from(yt.get_supported_features)
def get_supported_features(**kwargs):
    result = yt.get_supported_features(**kwargs)
    if kwargs["format"] is None:
        result = dump_data(result)
    print_to_output(result, eoln=False)


def add_get_supported_features_parser(add_parser):
    parser = add_parser("get-features",
                        function=get_supported_features,
                        help="Get cluster features (types, codecs etc.)")
    add_structured_format_argument(parser)


@copy_docstring_from(yt.get_job)
def get_job(**kwargs):
    result = yt.get_job(**kwargs)
    if kwargs["format"] is None:
        result = dump_data(result)
    print_to_output(result, eoln=False)


def add_get_job_parser(add_parser):
    parser = add_parser("get-job", get_job)
    add_hybrid_argument(parser, "job_id", help="job id, for example: 5c51-24e204-384-9f3f6437")
    add_hybrid_argument(parser, "operation_id", help="operation id, for example: 876084ca-efd01a47-3e8-7a62e787")
    add_structured_format_argument(parser)


@copy_docstring_from(yt.list_jobs)
def list_jobs(**kwargs):
    result = yt.list_jobs(**kwargs)
    if kwargs["format"] is None:
        result = dump_data(result)
    print_to_output(result, eoln=False)


def add_list_jobs_parser(add_parser):
    parser = add_parser("list-jobs", list_jobs)
    operation_id_args(parser, dest="operation_id")
    parser.add_argument("--job-type", help="filter jobs by job type")
    parser.add_argument("--job-state", help="filter jobs by job state")
    parser.add_argument("--address", help="filter jobs by node address")
    parser.add_argument("--job-competition-id", help="filter jobs by job competition id")
    parser.add_argument("--sort-field",
                        choices=("type", "state", "start_time", "finish_time",
                                 "address", "duration", "progress", "id"),
                        help="field to sort jobs by")
    parser.add_argument("--sort-order", help='sort order. Can be either "ascending" or "descending"')
    parser.add_argument("--limit", type=int, help="output limit")
    parser.add_argument("--offset", type=int, help="offset starting from zero")
    parser.add_argument("--with-spec", default=None, action="store_true")
    parser.add_argument("--with-stderr", default=None, action="store_true")
    parser.add_argument("--with-fail-context", default=None, action="store_true")
    parser.add_argument("--with-competitors", default=None, action="store_true", help="with competitive jobs")
    parser.add_argument("--with-monitoring-descriptor", default=None, action="store_true")
    parser.add_argument(
        "--include-cypress", action="store_true",
        help='include jobs from Cypress in result. Have effect only if --data-source is set to "manual"')
    parser.add_argument(
        "--include-runtime", action="store_true",
        help='include jobs from controller agents in result. Have effect only if --data-source is set to "manual"')
    parser.add_argument(
        "--include-archive", action="store_true",
        help='include jobs from archive in result. Have effect only if --data-source is set to "manual"')
    parser.add_argument(
        "--data-source",
        choices=("auto", "runtime", "archive", "manual"),
        help='data sources to list jobs from')
    add_structured_format_argument(parser)


def add_transform_parser(add_parser):
    parser = add_parser("transform", yt.transform)
    add_hybrid_argument(parser, "src", help="source table", dest="source_table")
    add_hybrid_argument(parser, "dst", dest="destination_table", group_required=False,
                        help="destination table (if not specified source table will be overwritten)")
    parser.add_argument("--erasure-codec", help="desired erasure codec for table")
    parser.add_argument("--compression-codec", help="desired compression codec for table")
    parser.add_argument("--optimize-for",
                        help='desired chunk format for table. Possible values: ["scan", "lookup"]')
    parser.add_argument("--desired-chunk-size", type=int, help="desired chunk size in bytes")
    parser.add_argument("--check-codecs", action="store_true",
                        help="check if table already has proper codecs before transforming")
    add_structured_argument(parser, "--spec")


def add_job_tool_parser(add_parser):
    parser = add_parser("job-tool",
                        function=yt_job_tool.process_job_tool_arguments,
                        pythonic_help=yt_job_tool.DESCRIPTION)
    yt_job_tool.create_job_tool_parser(parser)


def run_compression_benchmarks(**kwargs):
    yt_run_compression_benchmarks.run_compression_benchmarks(**kwargs)


def add_compression_benchmark_parser(add_parser):
    parser = add_parser(
        "run-compression-benchmarks",
        function=run_compression_benchmarks,
        pythonic_help=yt_run_compression_benchmarks.DESCRIPTION
    )
    add_ypath_argument(parser, "table", hybrid=True)
    parser.add_argument("--all-codecs", action="store_true", help="benchmark every level of codecs with levels")
    parser.add_argument("--sample-size", type=int, default=10**9, help="approximate table's sample fragment size in bytes")
    parser.add_argument("--format", default="json", choices=["json", "csv"], help="output format")
    parser.add_argument("--max-operations", type=int, default=10, help="max count of parallel operations")
    parser.add_argument("--time-limit-sec", type=int, default=200, help="time limit for one operation in seconds")


if HAS_SKY_SHARE:
    @copy_docstring_from(yt.sky_share)
    def sky_share(**kwargs):
        print_to_output(yt.sky_share(**kwargs))

    def add_sky_share_parser(add_parser):
        parser = add_parser("sky-share", sky_share)
        parser.add_argument("path", help="table path")
        parser.add_argument("--cluster", help="cluster name, by default it is derived from proxy url")
        parser.add_argument("--key-column", help="create a separate torrent for each unique key and print rbtorrent list in JSON format", action="append", dest="key_columns")
        parser.add_argument("--enable-fastbone", help="download over fastbone if all necessary firewall rules are present", action="store_true")
        parser.add_argument('--share-operation-options', help="share operation options in json format", type=json.loads)


@copy_docstring_from(get_default_config)
def show_default_config(**kwargs):
    config = get_default_config()
    if (kwargs["with_remote_patch"] or kwargs["only_remote_patch"]) and not yt.config["proxy"]["url"]:
        raise yt.YtError("Missed '--proxy' flag")
    if kwargs["with_remote_patch"]:
        RemotePatchableValueBase.patch_config_with_remote_data(yt, config)
    if kwargs["only_remote_patch"]:
        config = RemotePatchableValueBase._get_remote_patch(yt)
    print_to_output(dump_data(config), eoln=False)


def add_show_default_config_parser(add_parser):
    parser = add_parser("show-default-config", show_default_config)
    parser.add_argument("--with-remote-patch", action="store_true", default=False, dest="with_remote_patch", help="with patch from cluster")
    parser.add_argument("--only-remote-patch", action="store_true", default=False, dest="only_remote_patch", help="show only patch from cluster")


def detect_porto_layer(**kwargs):
    if not yt.config["proxy"]["url"]:
        raise yt.YtError("Missed '--proxy' flag")
    layers = yt.spec_builders.BaseLayerDetector._get_default_layer(yt, layer_type="porto")
    print_to_output(", ".join(layers) if layers else "-")


def add_detect_porto_layer_parser(add_parser):
    add_parser("detect-porto-layer", detect_porto_layer)


def add_download_core_dump_parser(add_parser):
    parser = add_parser("download-core-dump",
                        function=yt.download_core_dump,
                        help="Tool for downloading job core dumps")
    parser.add_argument("--operation-id",
                        help="Operation id (should be specified if proper core file naming is needed)")
    parser.add_argument("--core-table-path",
                        help="A path to the core table")
    parser.add_argument("--job-id",
                        help="Id of a job that produced core dump. "
                             "If not specified, an arbitrary job with core dumps is taken")
    parser.add_argument("--core-index",
                        type=int,
                        action="append",
                        dest="core_indices",
                        help="Indices of core dumps to download (indexing inside single job). "
                             "Several indices may be specified. If not specified, all core dumps will be downloaded. "
                             "Requires --job-id to be specified")
    parser.add_argument("--output-directory",
                        default=".",
                        help="A directory to save the core dumps. Defaults to the current working directory")


if HAS_IDM_CLI_HELPERS:
    def add_idm_parser(root_subparsers):
        parser = populate_argument_help(root_subparsers.add_parser("idm", description="IDM-related commands"))

        object_group = parser.add_mutually_exclusive_group(required=True)
        object_group.add_argument("--path", help="Cypress node path")
        object_group.add_argument("--account", help="Account name")
        object_group.add_argument("--bundle", dest="tablet_cell_bundle", help="Tablet cell bundle name")
        object_group.add_argument("--group", help="YT group name")
        object_group.add_argument("--pool", help="Pool name")
        object_group.add_argument("--network-project", help="Network project name")

        parser.add_argument("--pool-tree", help="Pool tree name")

        parser.add_argument("--address", help="IDM integration service address")

        idm_subparsers = parser.add_subparsers(metavar="idm_command", **SUBPARSER_KWARGS)

        # idm show
        add_idm_show_parser(idm_subparsers)

        # idm request
        request_parser = populate_argument_help(idm_subparsers.add_parser("request", help="Request IDM role"))
        add_idm_request_or_revoke_parser(request_parser)
        request_parser.add_argument(
            "--permissions", "-p", action="store", dest="permissions",
            type=idm.decode_permissions, default=[],
            help="Permissions like: R - read; RW - read, write, remove; M - mount, U - use")
        request_parser.set_defaults(func=idm.request)

        # idm revoke
        revoke_parser = populate_argument_help(idm_subparsers.add_parser("revoke", help="Revoke IDM role"))
        add_idm_request_or_revoke_parser(revoke_parser)
        revoke_parser.add_argument(
            "--permissions", "-p", action="store", dest="permissions",
            type=idm.decode_permissions, default=[],
            help="Permissions like: R - read; RW - read, write, remove; M - mount, U - use")
        revoke_parser.add_argument("--revoke-all-roles", help="Revoke all IDM roles", action="store_true")
        revoke_parser.set_defaults(func=idm.revoke)

        # idm copy
        add_idm_copy_parser(idm_subparsers)

    def add_idm_show_parser(idm_subparsers):
        parser = populate_argument_help(idm_subparsers.add_parser("show", help="Show IDM information"))
        parser.add_argument(
            "--immediate", "-i", help="Show only immediate IDM information (not inherited)",
            action="store_true", default=False)
        parser.set_defaults(func=idm.show)

    def add_idm_request_or_revoke_parser(parser):
        # Approvers definition
        parser.add_argument(
            "--responsibles", "-r", action="store", dest="responsibles",
            nargs="*", default=[], help="User logins space separated")
        parser.add_argument(
            "--read-approvers", "-a", action="store", dest="read_approvers",
            nargs="*", default=[], help="User logins space separated")
        parser.add_argument(
            "--auditors", "-u", action="store", dest="auditors",
            nargs="*", default=[], help="User logins space separated")

        # Flags definition
        inherit_acl_group = parser.add_mutually_exclusive_group(required=False)
        inherit_acl_group.add_argument(
            "--set-inherit-acl", action="store_true", dest="inherit_acl", default=None,
            help="Enable ACL inheritance")
        inherit_acl_group.add_argument(
            "--unset-inherit-acl", action="store_false", dest="inherit_acl",
            help="Disable ACL inheritance")

        inherit_resps_group = parser.add_mutually_exclusive_group(required=False)
        inherit_resps_group.add_argument(
            "--set-inherit-responsibles", action="store_true", dest="inherit_responsibles", default=None,
            help="Enable inheritance of responsibles, read approvers, auditors, boss_approval")
        inherit_resps_group.add_argument(
            "--unset-inherit-responsibles", action="store_false", dest="inherit_responsibles",
            help="Disable inheritance of responsibles, read approvers, auditors, boss_approval")

        boss_approval_group = parser.add_mutually_exclusive_group(required=False)
        boss_approval_group.add_argument(
            "--set-boss-approval", action="store_true", dest="boss_approval", default=None,
            help="Enable boss approval requirement for personal roles")
        boss_approval_group.add_argument(
            "--unset-boss-approval", action="store_false", dest="boss_approval",
            help="Disable boss approval requirement for personal roles")

        parser.add_argument(
            "--subjects", "-s", dest="subjects", nargs="*", default=[],
            help="Space separated user logins or staff/ABC groups like idm-group:ID or tvm apps like tvm-app:ID")

        parser.add_argument("--comment", dest="comment", help="Comment for the role")
        parser.add_argument(
            "--dry-run", action="store_true", dest="dry_run",
            help="Do not make real changes", default=False)

    def add_idm_copy_parser(idm_subparsers):
        parser = populate_argument_help(idm_subparsers.add_parser("copy", help="Copy IDM permissions"))
        parser.add_argument("destination", help="Destination object")
        parser.add_argument("--dst-pool-tree", default="",
                            help="Destination pool tree override; by default copying is done within a single pool tree")
        parser.add_argument(
            "--immediate", "-i", help="Only copy immediate IDM permissions",
            action="store_true", default=False)
        parser.add_argument("--erase", "-e", action="store_true",
                            help="Erase all existing permissions from destination object")
        parser.add_argument("--dst-cluster", default="",
                            help="Destination cluster override; by default copying is done within a single cluster")
        parser.add_argument(
            "--dry-run", action="store_true", dest="dry_run",
            help="Do not make real changes", default=False)
        parser.set_defaults(func=idm.copy)


@copy_docstring_from(yt.add_maintenance)
def add_maintenance(**kwargs):
    response = yt.add_maintenance(**kwargs)
    print_to_output(yson.dumps(response))


@copy_docstring_from(yt.remove_maintenance)
def remove_maintenance(**kwargs):
    response = yt.remove_maintenance(**kwargs)
    print_to_output(yson.dumps(response))


def add_maintenance_request_parsers(add_parser):
    parser = add_parser("add-maintenance", add_maintenance)
    parser.add_argument("-c", "--component", type=str)
    parser.add_argument("-a", "--address", type=str)
    parser.add_argument("-t", "--type", type=str)
    parser.add_argument("--comment", type=str)

    parser = add_parser("remove-maintenance", remove_maintenance)
    parser.add_argument("-c", "--component", type=str)
    parser.add_argument("-a", "--address", type=str)
    parser.add_argument("--id", default=None)
    add_structured_argument(parser, "--ids", default=None)
    parser.add_argument("-t", "--type", default=None)
    parser.add_argument("-u", "--user", default=None)
    parser.add_argument("--mine", default=False)
    parser.add_argument("--all", default=False)


def add_admin_parser(root_subparsers):
    parser = populate_argument_help(root_subparsers.add_parser("admin", description="Administer commands"))

    admin_subparsers = parser.add_subparsers(metavar="admin_command", **SUBPARSER_KWARGS)

    # switch leader
    add_switch_leader_parser(admin_subparsers)


def add_dirtable_parser(root_subparsers):
    parser = populate_argument_help(root_subparsers.add_parser("dirtable", description="Upload/download to file system commands"))
    dirtable_subparsers = parser.add_subparsers(metavar="dirtable_command", **SUBPARSER_KWARGS)
    add_dirtable_parsers(dirtable_subparsers)


def add_livy_parser(add_parser):
    parser = add_parser("livy", pythonic_help="SPYT Livy commands")
    livy_subparsers = parser.add_subparsers(metavar="livy_command", **SUBPARSER_KWARGS)
    add_spyt_liyv_subparser = add_subparser(livy_subparsers, params_argument=False)
    add_strawberry_ctl_parser(add_spyt_liyv_subparser, "livy")


def add_spark_parser(root_subparsers):
    # NB: py2 version of argparse library does not support aliases in add_parser.
    # Rewrite it when py2 support is dropped or deprecate a legacy "spark" alias.
    for name in ("spyt", "spark"):
        parser = populate_argument_help(root_subparsers.add_parser(
            name, description="SPYT commands"))

        spark_subparsers = parser.add_subparsers(metavar="spark_command", **SUBPARSER_KWARGS)
        add_spark_subparser = add_subparser(spark_subparsers, params_argument=False)
        add_find_spark_cluster_parser(add_spark_subparser)
        add_livy_parser(add_spark_subparser)


@copy_docstring_from(chyt.start_clique)
def clickhouse_start_clique_handler(*args, **kwargs):
    op = chyt.start_clique(*args, **kwargs)
    print(op.id)


def add_clickhouse_start_clique_parser(add_parser):
    parser = add_parser("start-clique", clickhouse_start_clique_handler)
    parser.add_argument("--instance-count", required=True, type=int)
    parser.add_argument("--alias", "--operation-alias", help="Alias for clique; may be also specified "
                                                             "via CHYT_PROXY env variable")
    parser.add_argument("--cypress-ytserver-clickhouse-path")
    parser.add_argument("--cypress-clickhouse-trampoline-path")
    parser.add_argument("--cypress-ytserver-log-tailer-path")
    parser.add_argument("--cypress-base-config-path", default="//sys/clickhouse/config")
    parser.add_argument("--cpu-limit", type=int)
    parser.add_argument("--cypress-geodata-path")
    parser.add_argument("--abort-existing", action="store_true", help="Abort existing operation under same alias")
    parser.add_argument("--artifact-path", help="path for artifact directory; by default equals to "
                                                "//sys/clickhouse/kolkhoz/<operation_alias>")
    parser.add_argument("--skip-version-compatibility-validation", action="store_true", help="(For developer use only)")
    add_structured_argument(parser, "--spec")
    add_structured_argument(parser, "--clickhouse-config", "ClickHouse configuration patch")
    add_structured_argument(parser, "--memory-config", "Memory configuration")


@copy_docstring_from(chyt.execute)
def clickhouse_execute_handler(**kwargs):
    class FakeStream:
        def __init__(self, stream):
            self.stream = stream

        def _read_rows(self):
            return (row + b"\n" for row in self.stream)

    settings = kwargs.pop("setting")
    if settings is not None:
        parsed_settings = {}
        for setting in settings:
            if '=' not in setting:
                raise ValueError("Invalid setting '" + setting + "'. "
                                 "Setting is expected to be in format <key>=<value>")

            setting_key, setting_value = setting.split('=', 1)

            if setting_key in parsed_settings:
                raise ValueError("Setting with key '" + setting_key + "' occurs multiple times")

            parsed_settings[setting_key] = setting_value
        kwargs["settings"] = parsed_settings

    iterator = chunk_iter_rows(FakeStream(chyt.execute(**kwargs)), yt.config["read_buffer_size"])
    write_silently(iterator)


def add_clickhouse_execute_parser(add_parser):
    parser = add_parser("execute", clickhouse_execute_handler)
    parser.add_argument("--alias", "--operation-alias", help="Alias for clique; may be also specified "
                                                             "via CHYT_PROXY env variable")
    add_hybrid_argument(parser, "query", help="Query to execute; do not specify FORMAT in query, use --format instead")
    parser.add_argument("--format",
                        help="ClickHouse data format; refer to https://clickhouse.tech/docs/en/interfaces/formats/; "
                             "default is TabSeparated",
                        default="TabSeparated")
    parser.add_argument("--setting", action="append", help="Add ClickHouse setting to query in format <key>=<value>.")


def add_chyt_parser(root_subparsers):
    # NB: py2 version of argparse library does not support aliases in add_parser.
    # Rewrite it when py2 support is dropped or deprecate a legacy "clickhouse" alias.
    for name in ("chyt", "clickhouse"):
        parser = populate_argument_help(root_subparsers.add_parser(
            name, description="ClickHouse over YT commands"))

        clickhouse_subparsers = parser.add_subparsers(metavar="clickhouse_command", **SUBPARSER_KWARGS)
        add_clickhouse_subparser = add_subparser(clickhouse_subparsers, params_argument=False)
        add_clickhouse_start_clique_parser(add_clickhouse_subparser)
        add_clickhouse_execute_parser(add_clickhouse_subparser)
        add_strawberry_ctl_parser(add_clickhouse_subparser, "chyt")


def add_jupyt_parser(root_subparsers):
    parser = populate_argument_help(root_subparsers.add_parser(
        "jupyt", description="Jupyter over YT commands"))

    jupyter_subparsers = parser.add_subparsers(metavar="jupyter_command", **SUBPARSER_KWARGS)

    add_jupyter_subparser = add_subparser(jupyter_subparsers, params_argument=False)
    add_strawberry_ctl_parser(add_jupyter_subparser, "jupyt")


def add_tryt_parser(root_subparsers):
   parser = populate_argument_help(root_subparsers.add_parser(
       "tryt", description="Transfer over YT commands"))

   tryt_subparsers = parser.add_subparsers(metavar="transfer_command", **SUBPARSER_KWARGS)

   add_tryt_subparser = add_subparser(tryt_subparsers, params_argument=False)
   add_strawberry_ctl_parser(add_tryt_subparser, "tryt")


@copy_docstring_from(yt.find_spark_cluster)
def find_spark_cluster_handler(*args, **kwargs):
    spark_cluster = yt.find_spark_cluster(*args, **kwargs)
    print("Master: {}".format(spark_cluster.master_endpoint))
    print("Master Web UI: http://{}/".format(spark_cluster.master_web_ui_url))
    print("Master REST API Endpoint: {}".format(spark_cluster.master_rest_endpoint))
    print("Operation: {}".format(spark_cluster.operation_url()))
    print("Spark History Server: http://{}/".format(spark_cluster.shs_url))


def add_find_spark_cluster_parser(add_parser):
    parser = add_parser("find-cluster", find_spark_cluster_handler, help="Print URLs of running Spark cluster")
    parser.add_argument("--discovery-path", help="Cypress path for discovery files and logs, "
                                                 "the same path must be used in start-spark-cluster. "
                                                 "SPARK_YT_DISCOVERY_PATH env variable is used by default")


def add_flow_parser(root_subparsers):
    parser = populate_argument_help(root_subparsers.add_parser(
        "flow", description="YT Flow commands"))

    flow_subparsers = parser.add_subparsers(metavar="flow_command", **SUBPARSER_KWARGS)

    add_flow_subparser = add_subparser(flow_subparsers, params_argument=False)

    add_flow_start_pipeline_parser(add_flow_subparser)
    add_flow_stop_pipeline_parser(add_flow_subparser)
    add_flow_pause_pipeline_parser(add_flow_subparser)
    add_flow_get_pipeline_spec_parser(add_flow_subparser)
    add_flow_set_pipeline_spec_parser(add_flow_subparser)
    add_flow_remove_pipeline_spec_parser(add_flow_subparser)
    add_flow_get_pipeline_dynamic_spec_parser(add_flow_subparser)
    add_flow_set_pipeline_dynamic_spec_parser(add_flow_subparser)
    add_flow_remove_pipeline_dynamic_spec_parser(add_flow_subparser)
    add_flow_get_pipeline_state_parser(add_flow_subparser)
    add_flow_get_flow_view_parser(add_flow_subparser)
    add_flow_show_logs_parser(add_flow_subparser)


def wait_pipeline_change(operation, state):
    @copy_docstring_from(operation)
    def wrapper(**kwargs):
        sync = kwargs.pop("sync")

        operation(**kwargs)

        if sync:
            wait_pipeline_state(target_state=state, **kwargs)

    return wrapper


def add_flow_start_pipeline_parser(add_parser):
    parser = add_parser("start-pipeline", wait_pipeline_change(yt.start_pipeline, "working"),
                        help="Start YT Flow pipeline")
    add_ypath_argument(parser, "pipeline_path", hybrid=True)
    parser.add_argument("--sync", action="store_true",
                        help="Wait for the pipeline to start")


def add_flow_stop_pipeline_parser(add_parser):
    parser = add_parser("stop-pipeline", wait_pipeline_change(yt.stop_pipeline, "stopped"),
                        help="Stop YT Flow pipeline")
    add_ypath_argument(parser, "pipeline_path", hybrid=True)
    parser.add_argument("--sync", action="store_true",
                        help="Wait for the pipeline to stop")


def add_flow_pause_pipeline_parser(add_parser):
    parser = add_parser("pause-pipeline", wait_pipeline_change(yt.pause_pipeline, "paused"),
                        help="Pause YT Flow pipeline")
    add_ypath_argument(parser, "pipeline_path", hybrid=True)
    parser.add_argument("--sync", action="store_true",
                        help="Wait for the pipeline to pause")


def add_flow_get_pipeline_spec_parser(add_parser):
    parser = add_parser("get-pipeline-spec", get_helper(yt.get_pipeline_spec),
                        help="Get YT Flow pipeline spec")
    add_ypath_argument(parser, "pipeline_path", hybrid=True)
    add_structured_format_argument(parser, default=output_format)
    parser.add_argument("--spec-path", help="Path inside pipeline spec yson struct, starting with /")


def add_flow_set_pipeline_spec_parser(add_parser):
    parser = add_parser("set-pipeline-spec", set_helper(yt.set_pipeline_spec),
                        help="Set YT Flow pipeline spec")
    add_ypath_argument(parser, "pipeline_path", hybrid=True)
    add_structured_format_argument(parser, default=YT_STRUCTURED_DATA_FORMAT)
    parser.add_argument("--expected-version", type=int,
                        help="Pipeline spec expected version")
    parser.add_argument("--force", action="store_true",
                        help="Set spec even if pipeline is paused")
    parser.add_argument("--spec-path", help="Path inside pipeline spec yson struct, starting with /")
    add_hybrid_argument(parser, "value", group_required=False,
                        help="new spec attribute value")


def add_flow_remove_pipeline_spec_parser(add_parser):
    parser = add_parser("remove-pipeline-spec", yt.remove_pipeline_spec,
                        help="Remove YT Flow pipeline spec")
    add_ypath_argument(parser, "pipeline_path", hybrid=True)
    parser.add_argument("--expected-version", type=int,
                        help="Pipeline spec expected version")
    parser.add_argument("--force", action="store_true",
                        help="Remove spec even if pipeline is paused")
    parser.add_argument("--spec-path", help="Path inside pipeline spec yson struct, starting with /")


def add_flow_get_pipeline_dynamic_spec_parser(add_parser):
    parser = add_parser("get-pipeline-dynamic-spec", get_helper(yt.get_pipeline_dynamic_spec),
                        help="Get YT Flow pipeline dynamic spec")
    add_ypath_argument(parser, "pipeline_path", hybrid=True)
    add_structured_format_argument(parser, default=output_format)
    parser.add_argument("--spec-path", help="Path inside pipeline dynamic spec yson struct, starting with /")


def add_flow_set_pipeline_dynamic_spec_parser(add_parser):
    parser = add_parser("set-pipeline-dynamic-spec", set_helper(yt.set_pipeline_dynamic_spec),
                        help="Set YT Flow pipeline dynamic spec")
    add_ypath_argument(parser, "pipeline_path", hybrid=True)
    add_structured_format_argument(parser, default=YT_STRUCTURED_DATA_FORMAT)
    parser.add_argument("--expected-version", type=int,
                        help="Pipeline spec expected version")
    parser.add_argument("--spec-path", help="Path inside pipeline dynamic spec yson struct, starting with /")
    add_hybrid_argument(parser, "value", group_required=False,
                        help="new spec attribute value")


def add_flow_remove_pipeline_dynamic_spec_parser(add_parser):
    parser = add_parser("remove-pipeline-dynamic-spec", yt.remove_pipeline_dynamic_spec,
                        help="Remove YT Flow pipeline dynamic spec")
    add_ypath_argument(parser, "pipeline_path", hybrid=True)
    parser.add_argument("--expected-version", type=int,
                        help="Pipeline spec expected version")
    parser.add_argument("--spec-path", help="Path inside pipeline dynamic spec yson struct, starting with /")


@copy_docstring_from(yt.get_pipeline_state)
def get_pipeline_state(**kwargs):
    print_to_output(yt.get_pipeline_state(**kwargs))


def add_flow_get_pipeline_state_parser(add_parser):
    parser = add_parser("get-pipeline-state", get_pipeline_state,
                        help="Get YT Flow pipeline state")
    add_ypath_argument(parser, "pipeline_path", hybrid=True)


def add_flow_get_flow_view_parser(add_parser):
    parser = add_parser("get-flow-view", get_helper(yt.get_flow_view),
                        help="Get YT Flow flow view")
    add_ypath_argument(parser, "pipeline_path", hybrid=True)
    add_structured_format_argument(parser, default=output_format)
    parser.add_argument("--view-path", help="Path inside flow view yson struct, starting with /")
    parser.add_argument("--cache", help="Use controller cache")


@copy_docstring_from(get_controller_logs)
def show_flow_logs(**kwargs):
    attach = kwargs.pop("attach")
    rate = kwargs.pop("rate")
    print_host = kwargs.pop("print_host")

    def format(value):
        prefix = ""
        if print_host:
            prefix = f"Host: {value['host']}\t"
        return prefix + value["data"] + "\n"

    if not attach:
        result, _ = get_controller_logs(**kwargs)
        write_silently((format(value) for value in result), True)
        return

    offset = None
    while True:
        result, offset = get_controller_logs(offset=offset, **kwargs)
        write_silently((format(value) for value in result), True)
        time.sleep(rate)


def add_flow_show_logs_parser(add_parser):
    parser = add_parser("show-logs", show_flow_logs,
                        help="Show YT FLow logs")
    add_ypath_argument(parser, "pipeline_path", hybrid=True)
    parser.add_argument("--count", type=int, default=25,
                        help="The number of last logs")
    parser.add_argument("--attach", action="store_true", default=False,
                        help="Attach to the logs stream")
    parser.add_argument("--rate", type=float, default=1.0,
                        help="Logs reading period in seconds")
    parser.add_argument("--print-host", action="store_true", default=False,
                        help="Print controller's hostname")


@copy_docstring_from(yt.run_command_with_lock)
def run_command_with_lock_handler(**kwargs):
    kwargs["popen_kwargs"] = dict(
        stdout=sys.stdout,
        stderr=sys.stderr,
        shell=kwargs.pop("shell"),
        env=os.environ.copy(),
        preexec_fn=os.setsid)
    kwargs["forward_signals"] = [signal.SIGTERM, signal.SIGINT]

    conflict_exit_code = kwargs.pop("conflict_exit_code")

    def lock_conflict_callback():
        sys.exit(conflict_exit_code)

    kwargs["lock_conflict_callback"] = lock_conflict_callback

    def ping_failed_callback(proc):
        proc.send_signal(2)
        time.sleep(1.0)
        proc.terminate()
        time.sleep(1.0)
        proc.kill()

    kwargs["ping_failed_callback"] = ping_failed_callback

    kwargs["create_lock_options"] = {"recursive": kwargs.pop("recursive")}

    return_code = yt.run_command_with_lock(**kwargs)

    sys.exit(return_code)


def add_run_command_with_lock_parser(add_parser):
    parser = add_parser("run-command-with-lock", help="Run command under lock")
    parser.add_argument("path",
                        help="Path to the lock in Cypress")
    parser.add_argument("command", nargs="+",
                        help="Command to execute")
    parser.add_argument("--shell", action="store_true", default=False,
                        help="Run command in subshell")
    parser.add_argument("--poll-period", type=float, default=1.0,
                        help="Poll period for command process in seconds")
    parser.add_argument("--conflict-exit-code", type=int, default=1,
                        help="Exit code in case of lock conflict")
    parser.add_argument("--set-address", action="store_true", default=False,
                        help="Set address of current host (in lock attribute by default)")
    parser.add_argument("--address-path",
                        help="Path to set host address")
    parser.add_argument("--recursive", action="store_true", default=False,
                        help="Create lock path recursively")
    parser.set_defaults(func=run_command_with_lock_handler)


def _get_version():
    yt_version = yt.get_version()
    result = ["Version: YT wrapper {}".format(yt_version)]
    if is_arcadia_python():
        return "\n".join(result)

    result.append("")

    try:
        import yt_yson_bindings
        yson_version = yt_yson_bindings.VERSION
        result.append("Component version: YSON bindings {}".format(yson_version))
    except ModuleNotFoundError:
        pass

    try:
        log_level = os.environ.get("YT_LOG_LEVEL")
        if not log_level:
            os.environ["YT_LOG_LEVEL"] = "WARNING"
        import yt_driver_rpc_bindings.version
        if not log_level:
            del os.environ["YT_LOG_LEVEL"]
        rpc_version = yt_driver_rpc_bindings.version.VERSION
        result.append("Component version: RPC bindings {}".format(rpc_version))
    except ModuleNotFoundError:
        pass

    python_version = "{}.{} ({})".format(sys.version_info.major, sys.version_info.minor, sys.executable)
    result.append("Component version: Python {}".format(python_version))

    return "\n".join(result)


def _prepare_config_parser():
    config_parser = ArgumentParser(add_help=False)
    config_parser.add_argument("--proxy", help="specify cluster to run command, "
                                               "by default YT_PROXY from environment")
    config_parser.add_argument("--prefix", help="specify common prefix for all relative paths, "
                                                "by default YT_PREFIX from environment")
    config_parser.add_argument("--config", action=ParseStructuredArgument, action_load_method=parse_structured_arguments_or_file,
                               help="specify configuration", default={})
    config_parser.add_argument("--tx", help="perform command in the context of the given "
                                            "transaction, by default 0-0-0-0")
    config_parser.add_argument("--master-cell-id", help="perform command in master specified by this id; "
                                                        "supported only for native driver and testing purposes ")
    config_parser.add_argument("--ping-ancestor-txs", action="store_true",
                               help="turn on pinging ancestor transactions")
    config_parser.add_argument("--trace", action="store_true",
                               help="trace execution of request using jaeger")
    return config_parser


def _prepare_parser():
    config_parser = _prepare_config_parser()

    parser = ArgumentParser(parents=[config_parser],
                            formatter_class=RawDescriptionHelpFormatter,
                            description=DESCRIPTION,
                            epilog=EPILOG)

    parser.add_argument("--version", action="version", version=_get_version())

    subparsers = parser.add_subparsers(metavar="command")
    subparsers.required = True

    add_parser = add_subparser(subparsers)

    add_exists_parser(add_parser)
    add_list_parser(add_parser)
    add_find_parser(add_parser)
    add_create_parser(add_parser)
    add_create_account_parser(add_parser)
    add_create_pool_parser(add_parser)
    add_read_table_parser(add_parser)
    add_write_table_parser(add_parser)
    add_create_temp_table_parser(add_parser)

    add_read_blob_table_parser(add_parser)

    add_read_file_parser(add_parser)
    add_write_file_parser(add_parser)

    add_get_parser(add_parser)
    add_set_parser(add_parser)
    add_set_attribute_parser(add_parser)
    add_copy_parser(add_parser)
    add_move_parser(add_parser)
    add_link_parser(add_parser)
    add_remove_parser(add_parser)
    add_remove_attribute_parser(add_parser)
    add_externalize_parser(add_parser)
    add_internalize_parser(add_parser)
    add_concatenate_parser(add_parser)

    add_mount_table_parser(add_parser)
    add_unmount_table_parser(add_parser)
    add_remount_table_parser(add_parser)
    add_reshard_table_parser(add_parser)
    add_reshard_table_automatic_parser(add_parser)
    add_balance_tablet_cells_parser(add_parser)
    add_trim_rows_parser(add_parser)
    add_alter_table_parser(add_parser)
    add_freeze_table_parser(add_parser)
    add_unfreeze_table_parser(add_parser)
    add_get_tablet_infos_parser(add_parser)
    add_get_tablet_errors_parser(add_parser)

    add_alter_table_replica_parser(add_parser)

    add_select_rows_parser(add_parser)
    add_explain_query_parser(add_parser)
    add_lookup_rows_parser(add_parser)
    add_insert_rows_parser(add_parser)
    add_delete_rows_parser(add_parser)

    add_register_queue_consumer_parser(add_parser)
    add_unregister_queue_consumer_parser(add_parser)
    add_list_queue_consumer_registrations_parser(add_parser)
    add_pull_queue_parser(add_parser)
    add_pull_consumer_parser(add_parser)
    add_pull_queue_consumer_parser(add_parser)
    add_advance_consumer_parser(add_parser)
    add_advance_queue_consumer_parser(add_parser)
    add_push_queue_producer_parser(add_parser)
    add_create_queue_producer_session_parser(add_parser)
    add_remove_queue_producer_session_parser(add_parser)

    add_start_query_parser(add_parser)
    add_query_parser(add_parser)
    add_abort_query_parser(add_parser)
    add_read_query_result_parser(add_parser)
    add_get_query_parser(add_parser)
    add_get_query_result_parser(add_parser)
    add_list_queries_parser(add_parser)
    add_alter_query_parser(add_parser)

    add_erase_parser(add_parser)
    add_merge_parser(add_parser)
    add_sort_parser(add_parser)
    add_map_parser(add_parser)
    add_reduce_parser(add_parser)
    add_join_reduce_parser(add_parser)
    add_map_reduce_parser(add_parser)
    add_remote_copy_parser(add_parser)
    add_vanilla_parser(add_parser)
    add_shuffle_parser(add_parser)

    add_abort_op_parser(add_parser)
    add_suspend_op_parser(add_parser)
    add_resume_op_parser(add_parser)
    add_track_op_parser(add_parser)
    add_complete_op_parser(add_parser)
    add_update_op_parameters_parser(add_parser)
    add_get_operation_parser(add_parser)
    add_list_operations_parser(add_parser)

    add_start_tx_parser(add_parser)
    add_abort_tx_parser(add_parser)
    add_commit_tx_parser(add_parser)
    add_ping_tx_parser(add_parser)

    add_lock_parser(add_parser)
    add_unlock_parser(add_parser)

    add_add_member_parser(add_parser)
    add_remove_member_parser(add_parser)
    add_check_permission_parser(add_parser)
    add_get_table_columnar_statistics_parser(add_parser)
    add_execute_batch_parser(add_parser)
    add_transfer_account_resources_parser(add_parser)
    add_transfer_pool_resources_parser(add_parser)
    add_generate_timestamp_parser(add_parser)

    add_run_job_shell_parser(add_parser)
    add_get_job_stderr_parser(add_parser)
    add_get_job_input_parser(add_parser)
    add_get_job_input_paths_parser(add_parser)
    add_abort_job_parser(add_parser)
    add_list_jobs_parser(add_parser)
    add_get_job_spec_parser(add_parser)
    add_get_job_parser(add_parser)

    add_set_user_password_parser(add_parser)
    add_issue_token_parser(add_parser)
    add_revoke_token_parser(add_parser)
    add_list_user_tokens(add_parser)

    add_execute_parser(add_parser)

    add_transform_parser(add_parser)

    add_job_tool_parser(add_parser)

    add_compression_benchmark_parser(add_parser)

    add_dump_parquet_parser(add_parser)

    add_upload_parquet_parser(add_parser)

    add_dump_orc_parser(add_parser)

    add_upload_orc_parser(add_parser)

    if HAS_SKY_SHARE:
        add_sky_share_parser(add_parser)

    add_show_default_config_parser(add_parser)
    add_detect_porto_layer_parser(add_parser)
    add_explain_id_parser(add_parser)
    add_show_spec_parser(add_parser)

    add_download_core_dump_parser(add_parser)

    add_get_supported_features_parser(add_parser)

    add_run_command_with_lock_parser(add_parser)

    add_chyt_parser(subparsers)
    add_jupyt_parser(subparsers)
    add_spark_parser(subparsers)
    add_flow_parser(subparsers)

    if HAS_IDM_CLI_HELPERS:
        add_idm_parser(subparsers)

    add_maintenance_request_parsers(add_parser)

    add_admin_parser(subparsers)

    add_dirtable_parser(subparsers)

    return parser


def main_func():
    config_parser = _prepare_config_parser()
    parser = _prepare_parser()

    if "_ARGCOMPLETE" in os.environ:
        completers.autocomplete(parser, append_space=False)

    aliases_filename = os.path.join(os.path.expanduser("~"), ".yt/aliases")
    if os.path.isfile(aliases_filename):
        aliases = {}
        for line in open(aliases_filename, "r"):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            name, flags = line.split("=")
            aliases[name] = shlex.split(flags)

        if len(sys.argv) > 1 and sys.argv[1] in aliases:
            sys.argv = sys.argv[0:1] + aliases[sys.argv[1]] + sys.argv[2:]

    config_args, unparsed = config_parser.parse_known_args()

    if config_args.tx is not None:
        yt.config.COMMAND_PARAMS["transaction_id"] = config_args.tx
        yt.config.COMMAND_PARAMS["ping_ancestor_transactions"] = config_args.ping_ancestor_txs
    if config_args.master_cell_id is not None:
        yt.config.COMMAND_PARAMS["master_cell_id"] = config_args.master_cell_id
    if config_args.proxy is not None:
        yt.config["backend"] = "http"
        yt.config["proxy"]["url"] = config_args.proxy
    if config_args.prefix is not None:
        yt.config["prefix"] = config_args.prefix

    if "read_progress_bar" not in config_args.config:
        config_args.config["read_progress_bar"] = {}
    if "enable" not in config_args.config["read_progress_bar"]:
        config_args.config["read_progress_bar"]["enable"] = True

    yt.config.update_config(config_args.config)

    if config_args.trace:
        if yt.config["driver_config"] is not None:
            yt.config["driver_config"]["force_tracing"] = True
        yt.config["proxy"]["force_tracing"] = True

    yt.config["default_value_of_raw_option"] = True

    args, unrecognized = parser.parse_known_args(unparsed)

    if unrecognized:
        msg = "unrecognized arguments: {}".format(' '.join(unrecognized))
        if hasattr(args, "last_parser"):
            args.last_parser.error(msg)
        else:
            parser.error(msg)

    func_args = dict(vars(args))

    if func_args.get("params") is not None:
        params = func_args["params"]
        for key in params:
            yt.config.COMMAND_PARAMS[key] = params[key]

    processed_keys = [
        "func",
        "tx",
        "trace",
        "ping_ancestor_txs",
        "prefix",
        "proxy",
        "config",
        "master_cell_id",
        "params",
        "last_parser",
    ]

    for key in processed_keys:
        if key in func_args:
            func_args.pop(key)

    args.func(**func_args)

    yt.config._cleanup()


def main():
    run_main(main_func)
