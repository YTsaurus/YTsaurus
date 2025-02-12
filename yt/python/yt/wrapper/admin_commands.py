from .batch_response import apply_function_to_result
from .common import set_param
from .driver import make_request, make_formatted_request
from .errors import YtError
from .yson import YsonMap


def build_snapshot(cell_id=None, set_read_only=None, wait_for_snapshot_completion=None, client=None):
    """Builds snapshot of a given cell."""
    params = {
        "cell_id": cell_id,
    }
    if set_read_only is not None:
        params["set_read_only"] = set_read_only
    if wait_for_snapshot_completion is not None:
        params["wait_for_snapshot_completion"] = wait_for_snapshot_completion

    return make_request("build_snapshot", params=params, client=client)


def build_master_snapshots(set_read_only=None, wait_for_snapshot_completion=None, retry=None, client=None):
    """Build snapshots for all master cells."""
    params = {}
    if set_read_only is not None:
        params["set_read_only"] = set_read_only
    if wait_for_snapshot_completion is not None:
        params["wait_for_snapshot_completion"] = wait_for_snapshot_completion
    if retry is not None:
        params["retry"] = retry

    return make_request("build_master_snapshots", params=params, client=client)


def exit_read_only(cell_id=None, client=None):
    """Exit read-only at given master cell."""
    params = {
        "cell_id": cell_id,
    }

    return make_request("exit_read_only", params=params, client=client)


def master_exit_read_only(retry=None, client=None):
    """Exits read-only mode at all master cells."""
    params = {}
    if retry is not None:
        params["retry"] = retry

    return make_request("master_exit_read_only", params=params, client=client)


def discombobulate_nonvoting_peers(cell_id=None, client=None):
    """Discombobulate nonvoting peers at given master cell."""
    params = {
        "cell_id": cell_id,
    }

    return make_request("discombobulate_nonvoting_peers", params=params, client=client)


def switch_leader(cell_id=None, new_leader_address=None, client=None):
    """Switch leader of given master cell."""
    params = {
        "cell_id": cell_id,
        "new_leader_address": new_leader_address,
    }

    return make_request("switch_leader", params=params, client=client)


def add_switch_leader_parser(subparsers):
    parser = subparsers.add_parser("switch-leader", help="Switch master cell leader")
    parser.set_defaults(func=switch_leader)
    parser.add_argument("--cell-id")
    parser.add_argument("--new-leader-address", type=str)


def suspend_tablet_cells(cell_ids, client=None):
    """Suspends listed tablet cells."""
    params = {
        "cell_ids": cell_ids,
    }

    return make_request("suspend_tablet_cells", params=params, client=client)


def resume_tablet_cells(cell_ids, client=None):
    """Resumes listed tablet cells."""
    params = {
        "cell_ids": cell_ids,
    }

    return make_request("resume_tablet_cells", params=params, client=client)


def add_maintenance(component, address, type, comment, client=None):
    """Adds maintenance request for a given node.

    :param component: component type. There are 4 component types: `cluster_node`, `http_proxy`,
    `rpc_proxy`, `host`. Note that `host` type is "virtual" since hosts do not support maintenance
    flags. Instead command is applied to all nodes on the given host.
    :param address: component address.
    :param type: maintenance type. There are 6 maintenance types: ban, decommission,
    disable_scheduler_jobs, disable_write_sessions, disable_tablet_cells, pending_restart.
    :param comment: any string with length not larger than 512 characters.
    :return: YSON map-node with maintenance IDs for each target. Maintenance IDs are unique
    per target.
    """
    params = {
        "component": component,
        "address": address,
        "type": type,
        "comment": comment,
        "supports_per_target_response": True,
    }

    request = make_formatted_request("add_maintenance", params=params, format=None, client=client)

    def _process_result(result):
        if "id" in result:
            # COMPAT(kvk1920): Compatibility with pre-24.2 HTTP proxies.
            return YsonMap({address: result["id"]})
        else:
            return result

    return apply_function_to_result(_process_result, request)


def remove_maintenance(component,
                       address,
                       id=None,
                       ids=None,
                       type=None,
                       user=None,
                       mine=False,
                       all=False,
                       client=None):
    """Removes maintenance requests from given node by id or filter.

    :param component: component type. There are 4 component types: `cluster_node`, `http_proxy`, `rpc_proxy`, `host`.
    :param address: component address.
    :param id: single maintenance id. The same as `ids` but accepts single id instead of list.
    Cannot be used at the same time with `ids`.
    :param ids: maintenance ids. Only maintenance requests which id is listed can be removed.
    :param type: maintenance type. If set only maintenance requests with given type will be removed.
    There are 6 maintenance types: ban, decommission, disable_scheduler_jobs, disable_write_sessions,
    disable_tablet_cell, pending_restart.
    :param user: only maintenance requests with this user will be removed.
    :param mine: only maintenance requests with authenticated user will be removed.
    Cannot be used with `user`.
    :param all: all maintenance requests from given node will be removed.
    Cannot be used with other options.
    :return: two-level YSON map-node: {target -> {maintenance_type -> count}}.
    """
    params = {
        "component": component,
        "address": address,
        "supports_per_target_response": True,
    }

    if not (user or mine or all or type or id or ids):
        raise YtError("At least one of {\"id\", \"all\", \"type\", \"user\", \"mine\"} must be specified")

    if id and ids:
        raise YtError("At most one of {\"id\", \"ids\"} can be specified")

    if id is not None:
        set_param(params, "id", id)
    elif ids is not None:
        set_param(params, "ids", ids)
    if type is not None:
        set_param(params, "type", type)
    if mine:
        set_param(params, "mine", True)
    if all:
        set_param(params, "all", True)
    if user is not None:
        set_param(params, "user", user)

    def _process_result(result):
        if not result:
            return {}

        # Since 24.2 remove_maintenance() returns 2-level dictionary:
        # { address: { type: count }}
        if isinstance(list(result.values())[0], dict):
            return result

        # COMPAT(kvk1920): Compatibility with pre-24.2 HTTP proxies.
        return {address: result}

    result = make_formatted_request("remove_maintenance", params=params, format=None, client=client)
    return apply_function_to_result(_process_result, result)


def disable_chunk_locations(node_address, location_uuids, client=None):
    """Disable locations by uuids.

    :param node_address: node address.
    :param location_uuids: location uuids.
    """
    params = {
        "node_address": node_address,
        "location_uuids": location_uuids
    }

    return make_request("disable_chunk_locations", params=params, client=client)


def destroy_chunk_locations(node_address, location_uuids, client=None):
    """Mark locations for destroying. Disks of these locations can be recovered.

    :param node_address: node address.
    :param location_uuids: location uuids.
    """
    params = {
        "node_address": node_address,
        "location_uuids": location_uuids
    }

    return make_request("destroy_chunk_locations", params=params, client=client)


def resurrect_chunk_locations(node_address, location_uuids, client=None):
    """Try resurrect disabled locations.

    :param node_address: node address.
    :param location_uuids: location uuids.
    """
    params = {
        "node_address": node_address,
        "location_uuids": location_uuids
    }

    return make_request("resurrect_chunk_locations", params=params, client=client)
