from __future__ import print_function

from yt.wrapper.client import create_client_with_command_params
from yt.wrapper.format import YsonFormat

from yt.common import YtResponseError

import logging


logger = logging.getLogger("TestHelpers")


# This class provides an interface for the Solomon exporter component which manages all the profiling data
# of our server components. For every sensor and for every possible set of tags Solomon exporter keeps
# an aggregated value in the same manner as it would be seen in Solomon. A set of tags and the corresponding
# aggregated value is called a projection. Note that only the most recent projections are stored, so
# it is impossible to get any historical data.
#
# How to start:
# (1) Create a Profiler object for the needed component via one of the static Profiler.at_xxx() methods.
# (2) Read the most recent value of a sensor using profiler.get(name, tags).
#
# Use gauge(), counter(), summary() and histogram() methods to create a convenient wrapper for
# the corresponding type of sensor.
#
# Example:
# > profiler = ProfilerFactory(yt_client).at_scheduler(fixed_tags={"tree": "default"})
# > total_time_completed_parent_counter =
# >     profiler.counter("scheduler/pools/metrics/total_time_completed", {"pool": "parent"})
# > ...
# > wait(lambda: total_time_completed_parent_counter.get_delta() > 0)
#
# Troubleshooting and debugging:
# (*) You can list all the sensors via the profiler.list() method.
# (*) Be sure to specify the _exact_ set of tags for the wanted projection. If a projection could not be found,
#     check for any required tags you might be missing. It is super useful to call sensor.get_all() method to list
#     all the available projections for the sensor.
class Profiler(object):
    def __init__(self, yt_client, path, fixed_tags=None, namespace="yt"):
        if fixed_tags is None:
            fixed_tags = {}

        self.yt_client = yt_client
        self.path = path
        self.namespace = namespace
        self.fixed_tags = fixed_tags

    def with_tags(self, tags):
        return Profiler(self.yt_client, self.path, fixed_tags=dict(self.fixed_tags, **tags), namespace=self.namespace)

    def list(self, **kwargs):
        return self.yt_client.list(self.path, format=YsonFormat(), **kwargs)

    def get(self, name, tags=None, verbose=True, verbose_value_name="value", postprocessor=lambda value: value, default=None, **kwargs):
        if tags is None:
            tags = {}
        tags = dict(self.fixed_tags, **tags)

        export_summary_as_max = kwargs.pop("export_summary_as_max", True)
        read_all_projections = kwargs.pop("read_all_projections", False)
        summary_as_max_for_all_time = kwargs.pop("summary_as_max_for_all_time", False)

        client = create_client_with_command_params(
            self.yt_client,
            tags=tags,
            name="{}/{}".format(self.namespace, name),
            export_summary_as_max=export_summary_as_max,
            read_all_projections=read_all_projections,
            summary_as_max_for_all_time=summary_as_max_for_all_time,
        )

        try:
            value = client.get(self.path, **kwargs)
        except YtResponseError as err:
            if err.is_resolve_error():
                value = default
            else:
                raise
        if value is not None:
            value = postprocessor(value)
        if verbose:
            logger.info(
                "{} of sensor \"{}\" with tags {}: {}"
                .format(verbose_value_name.capitalize(), name, tags, value))

        return value

    def get_all(self, name, tags=None, **kwargs):
        if tags is None:
            tags = {}
        return self.get(name, tags, verbose_value_name="projections", read_all_projections=True, default=[], **kwargs)

    class _Sensor(object):
        def __init__(self, profiler, name, fixed_tags):
            self.profiler = profiler
            self.name = name
            self.fixed_tags = fixed_tags

        def get(self, tags=None, **kwargs):
            if tags is None:
                tags = {}
            return self.profiler.get(self.name, dict(self.fixed_tags, **tags), **kwargs)

        # NB(eshcherbin): This method is used mostly for debugging purposes.
        def get_all(self, tags=None, **kwargs):
            if tags is None:
                tags = {}
            return self.profiler.get_all(self.name, dict(self.fixed_tags, **tags), **kwargs)

    class Gauge(_Sensor):
        def get(self, *args, **kwargs):
            return super(Profiler.Gauge, self).get(*args, postprocessor=float, **kwargs)

    class Counter(_Sensor):
        def __init__(self, profiler, name, tags):
            super(Profiler.Counter, self).__init__(profiler, name, fixed_tags=tags)
            self.start_value = self.get(default=0)

        def get_delta(self, **kwargs):
            # To support it, get_all could be called in __init__.
            assert "tags" not in kwargs, "Tags is not supported now, use fixed_tags"
            return self.get(
                verbose_value_name="delta",
                postprocessor=lambda value: int(value) - self.start_value,
                default=0,
                **kwargs
            )

    class Summary(_Sensor):
        def get_summary(self, *args, **kwargs):
            return self.get(
                export_summary_as_max=False,
                verbose_value_name="summary",
                *args,
                **kwargs
            )

        def get_max(self, *args, **kwargs):
            return self.get(
                export_summary_as_max=True,
                verbose_value_name="max",
                *args,
                **kwargs
            )

        def _get_summary_part(self, part, *args, **kwargs):
            return self.get(
                export_summary_as_max=False,
                verbose_value_name=part,
                postprocessor=lambda summary: summary[part],
                *args,
                **kwargs
            )

        def get_min(self, *args, **kwargs):
            return self._get_summary_part("min", *args, **kwargs)

        def get_last(self, *args, **kwargs):
            return self._get_summary_part("last", *args, **kwargs)

        def get_count(self, *args, **kwargs):
            return self._get_summary_part("count", *args, **kwargs)

    class Histogram(_Sensor):
        def get_bins(self, *args, **kwargs):
            return self.get(
                verbose_value_name="bins",
                default=[],
                *args,
                **kwargs
            )

    def gauge(self, name, fixed_tags=None):
        if fixed_tags is None:
            fixed_tags = {}
        return Profiler.Gauge(self, name, fixed_tags)

    def counter(self, name, tags=None):
        if tags is None:
            tags = {}
        return Profiler.Counter(self, name, tags)

    def summary(self, name, fixed_tags=None):
        if fixed_tags is None:
            fixed_tags = {}
        return Profiler.Summary(self, name, fixed_tags)

    def histogram(self, name, fixed_tags=None):
        if fixed_tags is None:
            fixed_tags = {}
        return Profiler.Histogram(self, name, fixed_tags)


class ProfilerFactory(object):
    def __init__(self, yt_client):
        self.yt_client = yt_client

    def at_scheduler(self, **kwargs):
        return Profiler(self.yt_client, "//sys/scheduler/orchid/sensors", **kwargs)

    def at_controller_agent(self, controller_agent, **kwargs):
        return Profiler(self.yt_client, "//sys/controller_agents/instances/{0}/orchid/sensors".format(controller_agent), **kwargs)

    def at_node(self, node, *args, **kwargs):
        return Profiler(self.yt_client, "//sys/cluster_nodes/{0}/orchid/sensors".format(node), *args, **kwargs)

    def at_job_proxy(self, node, *args, **kwargs):
        return Profiler(self.yt_client, "//sys/cluster_nodes/{0}/orchid/exec_node/job_proxy_sensors".format(node), *args, **kwargs)

    def at_tablet_node(self, table, tablet_cell_bundle="default", fixed_tags=None):
        if fixed_tags is None:
            fixed_tags = {}
        fixed_tags["tablet_cell_bundle"] = tablet_cell_bundle

        tablets = self.yt_client.get(table + "/@tablets")
        address = self.yt_client.get("#{0}/@peers/0/address".format(tablets[0]["cell_id"]))
        return Profiler(
            self.yt_client,
            "//sys/cluster_nodes/{0}/orchid/sensors".format(address),
            namespace="yt/tablet_node",
            fixed_tags=fixed_tags
        )

    def at_primary_master(self, master_address, **kwargs):
        return self._at_master("//sys/primary_masters", master_address, **kwargs)

    def at_secondary_master(self, cell_tag, master_address, **kwargs):
        return self._at_master("//sys/secondary_masters/{}".format(cell_tag), master_address, **kwargs)

    def _at_master(self, masters_path_prefix, master_address, **kwargs):
        path = "{}/{}/orchid/sensors".format(masters_path_prefix, master_address)
        return Profiler(self.yt_client, path, **kwargs)

    def at_http_proxy(self, proxy, **kwargs):
        return Profiler(self.yt_client, "//sys/http_proxies/{0}/orchid/sensors".format(proxy), **kwargs)

    def at_rpc_proxy(self, proxy, *args, **kwargs):
        return Profiler(self.yt_client, "//sys/rpc_proxies/{0}/orchid/sensors".format(proxy), *args, **kwargs)

    def at_cypress_proxy(self, proxy, **kwargs):
        return Profiler(self.yt_client, "//sys/cypress_proxies/{0}/orchid/sensors".format(proxy), **kwargs)

    def at_yql_agent(self, yqla, **kwargs):
        return Profiler(self.yt_client, "//sys/yql_agent/instances/{0}/orchid/sensors".format(yqla), **kwargs)

    def at_query_tracker(self, qt, **kwargs):
        return Profiler(self.yt_client, "//sys/query_tracker/instances/{0}/orchid/sensors".format(qt), **kwargs)
