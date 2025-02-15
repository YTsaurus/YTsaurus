from .config import get_config, get_option, set_option, get_backend_type
from .common import require, get_value, total_seconds, generate_uuid, generate_traceparent, forbidden_inside_job, hide_auth_headers
from .constants import OAUTH_URL, FEEDBACK_URL
from .retries import Retrier, default_chaos_monkey
from .errors import (YtError, YtTokenError, YtProxyUnavailable, YtIncorrectResponse, YtHttpResponseError,
                     YtRequestQueueSizeLimitExceeded, YtRpcUnavailable,
                     YtRequestTimedOut, YtRetriableError, YtTransportError, YtNoSuchTransaction,
                     create_http_response_error, YtSequoiaRetriableError)
from .framing import unframed_iter_content
from .command import parse_commands
from .format import JsonFormat, YsonFormat

import yt.logger as logger
import yt.yson as yson
import yt.json_wrapper as json
from yt.common import _pretty_format_for_logging, get_fqdn as _get_fqdn

import os
import sys
import time
import types
import socket
import stat
import weakref

from datetime import datetime
from urllib.parse import urlparse
from socket import error as SocketError
from abc import ABCMeta, abstractmethod
from copy import deepcopy

# We cannot use requests.HTTPError in module namespace because of conflict with python3 http library
from http.client import BadStatusLine, IncompleteRead, LineTooLong

# Used to distinguish
try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

requests = None

RECEIVE_TOKEN_FROM_SSH_SESSION = \
    int(os.environ.get("RECEIVE_TOKEN_FROM_SSH_SESSION", True)) and \
    "TEST_TOOL" not in os.environ

TVM_ONLY_HTTP_PROXY_PORT = 9026
TVM_ONLY_HTTPS_PROXY_PORT = 9443

_FAILED_TO_RECEIVE_TOKEN_WARNED = False


def format_logging_params(params):
    return ", ".join(["{}: {}".format(key, value) for key, value in params.items()])


def _hexify(message):
    def convert(byte):
        if byte < 128:
            return chr(byte)
        else:
            return hex(byte)
    return "".join(map(convert, message))


def _hexify_recursively(obj):
    if isinstance(obj, dict):
        for key in obj:
            obj[key] = _hexify_recursively(obj[key])
    elif isinstance(obj, list):
        for index in range(len(obj)):
            obj[key] = _hexify_recursively(obj[key])
    elif isinstance(obj, bytes):
        return _hexify(obj)
    else:
        return obj


def get_retriable_errors():
    """List or errors that API will retry in HTTP requests."""
    from yt.packages.requests import HTTPError, ConnectionError, ReadTimeout, Timeout
    from yt.packages.requests.exceptions import ChunkedEncodingError
    from yt.packages.urllib3.exceptions import ConnectTimeoutError, ReadTimeoutError  # YTADMINREQ-22305 , YTADMINREQ-30006
    return (HTTPError, ConnectTimeoutError, ConnectionError, ReadTimeout, Timeout, IncompleteRead, BadStatusLine,
            SocketError, ChunkedEncodingError, ReadTimeoutError,
            YtIncorrectResponse, YtProxyUnavailable,
            YtRequestQueueSizeLimitExceeded, YtRpcUnavailable,
            YtRequestTimedOut, YtRetriableError, YtTransportError,
            YtSequoiaRetriableError)


class ProxyProvider(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def __call__(self):
        pass

    @abstractmethod
    def on_error_occurred(self, error):
        pass


def lazy_import_requests():
    global requests
    if requests is None:
        import yt.packages.requests
        requests = yt.packages.requests


def _setup_new_session(client):
    lazy_import_requests()
    session = requests.Session()
    configure_proxy(session,
                    get_config(client)["proxy"]["http_proxy"],
                    get_config(client)["proxy"]["https_proxy"])
    configure_ip(session,
                 get_config(client)["proxy"]["force_ipv4"],
                 get_config(client)["proxy"]["force_ipv6"])

    ca_bundle_path = get_config(client)["proxy"]["ca_bundle_path"]
    if ca_bundle_path:
        if os.path.exists(ca_bundle_path):
            if os.environ.get("REQUESTS_CA_BUNDLE"):
                logger.warning("CA override (%s -> %s)", ca_bundle_path, os.environ.get("REQUESTS_CA_BUNDLE"))
            os.environ["REQUESTS_CA_BUNDLE"] = ca_bundle_path
        else:
            logger.error("CA file is absent (%s)", ca_bundle_path)

    return session


def _get_session(client=None):
    if get_option("_requests_session", client) is None:
        session = _setup_new_session(client)
        set_option("_requests_session", session, client)
        set_option("_requests_session_origin_id", id(client), client)
    return get_option("_requests_session", client)


def _cleanup_http_session(client=None):
    session = _setup_new_session(client)
    set_option("_requests_session", session, client)


def configure_proxy(session, http_proxy, https_proxy):
    lazy_import_requests()
    proxies = {}
    if http_proxy:
        proxies["http"] = http_proxy
    if https_proxy:
        proxies["https"] = https_proxy
    if len(proxies):
        session.proxies.update(proxies)


def configure_ip(session, force_ipv4=False, force_ipv6=False):
    lazy_import_requests()
    if force_ipv4 or force_ipv6:
        protocol = socket.AF_INET if force_ipv4 else socket.AF_INET6

        class HTTPAdapter(requests.adapters.HTTPAdapter):
            def init_poolmanager(self, *args, **kwargs):
                return super(HTTPAdapter, self).init_poolmanager(*args,
                                                                 socket_af=protocol,
                                                                 **kwargs)
        session.mount("http://", HTTPAdapter())
        session.mount("https://", HTTPAdapter())


def get_error_from_headers(headers):
    if int(headers.get("x-yt-response-code", 0)) != 0:
        return headers["x-yt-error"]
    return None


def get_header_format(client):
    if get_config(client)["proxy"]["header_format"] is not None:
        return get_config(client)["proxy"]["header_format"]
    if get_config(client)["structured_data_format"] is not None:
        structured_data_format = get_config(client)["structured_data_format"]
        if isinstance(structured_data_format, YsonFormat):
            return "yson"
        if isinstance(structured_data_format, JsonFormat):
            return "json"
        return structured_data_format
    return "yson"


def check_response_is_decodable(response, format):
    if format == "json":
        try:
            response.json()
        except (json.JSONDecodeError, TypeError):
            raise YtIncorrectResponse("Response body can not be decoded from JSON", response)
    elif format == "yson":
        try:
            yson.loads(response.content, encoding=None)
        except (yson.YsonError, TypeError):
            raise YtIncorrectResponse("Response body can not be decoded from YSON", response)


def create_response(response, request_info, request_id, error_format, client):
    if error_format is None:
        error_format = "json"

    def loads(str):
        header_format = get_header_format(client)
        if header_format == "json":
            return yson.json_to_yson(json.loads(str))
        if header_format == "yson":
            # NOTE: Actually this is latin-1 encoding. urllib uses it to
            # decode headers, so it is used here to encode them back to bytes.
            str = str.encode("iso-8859-1")
            return yson.loads(str)
        raise YtError("Incorrect header format: {0}".format(header_format))

    def get_error():
        error_content = get_error_from_headers(response.headers)
        if error_content is None and not str(response.status_code).startswith("2"):
            check_response_is_decodable(response, error_format)
            error_content = response.content
        if error_content is not None:
            if error_format == "json":
                error_content = json.loads(error_content)
            elif error_format == "yson":
                error_content = yson.loads(error_content)
            else:
                raise YtError("Incorrect error format: {0}".format(error_format))
        return error_content

    def error(self):
        return self._error

    def is_ok(self):
        return self._error is None

    if "X-YT-Response-Parameters" in response.headers:
        response.headers["X-YT-Response-Parameters"] = loads(response.headers["X-YT-Response-Parameters"])
    if "X-YT-Framing" in response.headers:
        response.framed_iter_content = response.iter_content
        response.iter_content = lambda *args: unframed_iter_content(response, *args)
    response.request_info = request_info
    response.request_id = request_id
    response._error = get_error()
    response.error = types.MethodType(error, weakref.proxy(response))
    response.is_ok = types.MethodType(is_ok, weakref.proxy(response))
    response.framing_error = None
    return response


def _process_request_backoff(current_time, client):
    backoff = get_config(client)["proxy"]["request_backoff_time"]
    if backoff is not None:
        last_request_time = getattr(_get_session(client=client), "last_request_time", 0)
        now_seconds = total_seconds(current_time - datetime(1970, 1, 1))
        diff = now_seconds - last_request_time
        if diff * 1000.0 < float(backoff):
            time.sleep(float(backoff) / 1000.0 - diff)
        _get_session(client=client).last_request_time = now_seconds


def raise_for_token(response, request_info):
    request_id = response.headers.get("X-YT-Request-ID", "missing")
    proxy = response.headers.get("X-YT-Proxy")
    error_exc = create_http_response_error(
        response.error(),
        url=request_info["url"],
        request_headers=request_info["headers"],
        response_headers=dict(**response.headers),
        params=request_info["params"])
    raise YtTokenError(
        "Your request {0} has failed to authenticate at {1}. "
        "Make sure that you have provided an OAuth token with the request. "
        "In case you do not have a valid token, please refer to {2} for obtaining one. "
        "If the error persists and system keeps rejecting your token, "
        "please kindly submit a request to {3}"
        .format(request_id, proxy, OAUTH_URL, FEEDBACK_URL),
        inner_errors=[error_exc])


def _raise_for_status(response, request_info):
    if response.status_code in (500, 503):
        raise YtProxyUnavailable(response)
    if response.status_code == 401:
        raise_for_token(response, request_info)
    if not response.is_ok():
        error_exc = create_http_response_error(
            response.error(),
            url=request_info["url"],
            request_headers=request_info["headers"],
            response_headers=dict(**response.headers),
            params=request_info["params"])
        raise error_exc


class RequestRetrier(Retrier):
    def __init__(self, method, url=None, make_retries=True, response_format=None, error_format=None,
                 params=None, timeout=None, retry_action=None, data_log="", is_ping=False,
                 proxy_provider=None, retry_config=None, client=None, **kwargs):
        self.method = method
        self.url = url
        self.make_retries = make_retries
        self.response_format = response_format
        self.error_format = error_format
        self.params = params
        self.retry_action = retry_action
        self.data_log = data_log
        self.proxy_provider = proxy_provider
        self.client = client
        self.kwargs = kwargs

        random_generator = get_option("_random_generator", self.client)
        self.request_id = "%08x" % random_generator.randrange(16**8)

        if self.proxy_provider is None:
            self.request_url = self.url
        else:
            self.request_url = "'undiscovered'"

        retry_config = get_value(
            retry_config,
            get_config(client)["proxy"]["retries"])
        if timeout is None:
            timeout = get_config(client)["proxy"]["request_timeout"]
        self.requests_timeout = timeout
        retries_timeout = timeout[1] if isinstance(timeout, tuple) else timeout

        non_retriable_errors = [LineTooLong]
        retriable_errors = list(get_retriable_errors())
        if is_ping:
            retriable_errors.append(YtHttpResponseError)
            non_retriable_errors.append(YtNoSuchTransaction)

        headers = get_value(kwargs.get("headers", {}), {})
        headers["X-YT-Correlation-Id"] = generate_uuid(get_option("_random_generator", client))

        if get_config(client)["proxy"]["force_tracing"]:
            headers["traceparent"] = generate_traceparent(get_option("_random_generator", client))

        self.headers = headers

        chaos_monkey_enable = get_option("_ENABLE_HTTP_CHAOS_MONKEY", client)
        super(RequestRetrier, self).__init__(exceptions=tuple(retriable_errors),
                                             ignore_exceptions=tuple(non_retriable_errors),
                                             timeout=retries_timeout,
                                             retry_config=retry_config,
                                             chaos_monkey=default_chaos_monkey(chaos_monkey_enable))

    def action(self):
        url = self.url
        if self.proxy_provider is not None:
            proxy = self.proxy_provider()
            url = self.url.format(proxy=proxy)
            self.request_url = url

        logging_params = {
            "headers": hide_auth_headers(self.headers),
            "request_id": self.request_id,
        }
        if self.data_log:
            logging_params["data"] = self.data_log

        logger.debug("Perform HTTP %s request %s (%s)",
                     self.method,
                     url,
                     format_logging_params(logging_params))

        request_start_time = datetime.now()
        _process_request_backoff(request_start_time, client=self.client)
        request_info = {"headers": self.headers, "url": url, "params": self.params}

        try:
            session = _get_session(client=self.client)
            if isinstance(self.requests_timeout, tuple):
                timeout = tuple(map(lambda elem: elem / 1000.0, self.requests_timeout))
            else:
                timeout = self.requests_timeout / 1000.0
            response = create_response(session.request(self.method, url, timeout=deepcopy(timeout), **self.kwargs),
                                       request_info, self.request_id, self.error_format, self.client)

        except requests.ConnectionError as error:
            # Module requests patched to process response from YT proxy
            # in case of large chunked-encoding write requests.
            # Here we check that this response was added to the error.
            if hasattr(error, "response") and error.response is not None:
                exc_info = sys.exc_info()
                try:
                    # We should perform it under try..except due to response may be incomplete.
                    # See YT-4053.
                    rsp = create_response(error.response, request_info, self.request_id, self.error_format, self.client)
                except:  # noqa
                    raise exc_info[1].with_traceback(exc_info[2])
                check_response_is_decodable(rsp, "json")
                _raise_for_status(rsp, request_info)
            raise

        # Sometimes (quite often) we obtain incomplete response with body expected to be JSON.
        # So we should retry such requests.
        if self.response_format is not None and get_config(self.client)["proxy"]["check_response_format"]:
            if str(response.status_code).startswith("2"):
                response_format = self.response_format
            else:
                response_format = "json"
            check_response_is_decodable(response, response_format)

        logging_params = {
            "headers": hide_auth_headers(dict(response.headers)),
            "request_id": self.request_id,
            "status_code": response.status_code,
        }
        logger.debug("Response received (%s)", format_logging_params(logging_params))

        _raise_for_status(response, request_info)
        return response

    def except_action(self, error, attempt):
        logging_params = {
            "request_headers": hide_auth_headers(dict(self.headers)),
            "request_id": self.request_id,
        }
        if isinstance(error, YtError):
            try:
                logging_params["full_error"] = _pretty_format_for_logging(error)
            except:  # noqa
                logger.exception("Failed to format error")

        logger.warning("HTTP %s request %s failed with error %s (%s)",
                       self.method,
                       self.request_url,
                       repr(error),
                       format_logging_params(logging_params))
        self.is_connection_timeout_error = isinstance(error, requests.exceptions.ConnectTimeout)
        if self.proxy_provider is not None:
            self.proxy_provider.on_error_occurred(error)
        if self.make_retries:
            if self.retry_action is not None:
                self.retry_action(error, self.kwargs)
        else:
            raise error

    def backoff_action(self, attempt, backoff):
        logging_params = {
            "request_id": self.request_id,
        }
        skip_backoff = get_config(self.client)["proxy"]["skip_backoff_if_connect_timed_out"] \
            and self.is_connection_timeout_error
        if not skip_backoff:
            logger.warning("Sleep for %.2lf seconds before next retry (%s)", backoff, format_logging_params(logging_params))
            time.sleep(backoff)
        logger.warning("New retry (%d) for request id %s...", attempt + 1, self.request_id)


def make_request_with_retries(method, url=None, **kwargs):
    """Performs HTTP request to YT proxy with retries.

    This function is for backward compatibility and convenience of use.
    """
    return RequestRetrier(method=method, url=url, **kwargs).run()


def _get_proxy_url_parts(required=True, client=None, replace_host_proxy=None):
    """Get proxy url parts from config or params (try to guess scheme from config)
    """
    proxy_config = get_config(client=client)["proxy"]
    proxy = replace_host_proxy if replace_host_proxy else proxy_config["url"]

    if proxy is None:
        if required:
            require(proxy, lambda: YtError("You should specify proxy"))
        return None, None

    aliases_config = proxy_config["aliases"]
    if proxy in aliases_config:
        proxy = aliases_config[proxy]

    if "//" not in proxy:
        proxy = "//" + proxy
    parts = urlparse(proxy)
    scheme, hostname, port = parts.scheme, parts.hostname, parts.port

    # get scheme for replace_host_proxy from original proxy
    if replace_host_proxy and not scheme and proxy_config["url"].startswith("https://"):
        scheme = "https"

    # expand host aliases
    if "." not in hostname and ":" not in hostname and "localhost" not in hostname and not port and not scheme:
        hostname = hostname + proxy_config["default_suffix"]

    # get scheme from config
    prefer_https = proxy_config["prefer_https"]
    if not scheme:
        scheme = "https" if prefer_https else "http"

    # tvm host/port
    tvm_only = proxy_config["tvm_only"]
    if tvm_only and not port:
        if not hostname.startswith("tvm.") and not replace_host_proxy:
            hostname = "tvm." + hostname
        port = TVM_ONLY_HTTP_PROXY_PORT if scheme == "http" else TVM_ONLY_HTTPS_PROXY_PORT

    # IPv6 address RFC-2732
    if parts.netloc.startswith('['):
        hostname = '[' + hostname + ']'

    return (scheme, hostname + (":" + str(port) if port else ""))


def get_proxy_address_netloc(required=True, client=None, replace_host=None):
    """Get proxy "hostname:port" from config or params
    """
    scheme, netloc = _get_proxy_url_parts(required=required, client=client, replace_host_proxy=replace_host)
    return netloc


def get_proxy_address_url(required=True, client=None, add_path=None, replace_host=None):
    """Get proxy "schema://hostname:port" from config or params
    """
    scheme, netloc = _get_proxy_url_parts(required=required, client=client, replace_host_proxy=replace_host)
    if not netloc:
        return None
    if add_path and not add_path.startswith("/"):
        add_path = "/" + add_path
    return "{}://{}{}".format(scheme, netloc, add_path if add_path else "")


def get_proxy_url(required=True, client=None):
    """Get proxy "hostname:port" from config or params (legacy, use get_proxy_address_netloc or get_proxy_address_url)
    """
    return get_proxy_address_netloc(required=required, client=client)


def _request_api(version=None, client=None):
    location = "api" if version is None else "api/" + version
    return make_request_with_retries(
        "get",
        get_proxy_address_url(client=client, add_path=location),
        response_format="json",
        client=client
    ).json()


def get_http_api_version(client=None):
    api_version_option = get_option("_api_version", client)
    if api_version_option:
        return api_version_option

    api_version_from_config = get_config(client)["api_version"]
    if api_version_from_config:
        set_option("_api_version", api_version_from_config, client)
        return api_version_from_config

    default_api_version_for_http = get_config(client)["default_api_version_for_http"]
    if default_api_version_for_http is not None:
        api_version = default_api_version_for_http
    else:
        api_versions = _request_api(client=client)
        # To deprecate using api/v2
        if "v2" in api_versions:
            api_versions.remove("v2")
        api_version = "v3"
        require(api_version in api_versions, lambda: YtError("API {0} is not supported".format(api_version)))

    set_option("_api_version", api_version, client)

    return api_version


def get_http_api_commands(client=None):
    if get_option("_commands", client):
        return get_option("_commands", client)

    commands = parse_commands(
        _request_api(
            version=get_http_api_version(client),
            client=client))

    # TODO(nadya73): remove me in YT-23538.
    if "push_queue_producer" in commands:
        commands["push_queue_producer"].input_type = "tabular"

    set_option("_commands", commands, client)

    return commands


def get_fqdn(client=None):
    if get_option("_fqdn", client):
        return get_option("_fqdn", client)

    fqdn = _get_fqdn()
    set_option("_fqdn", fqdn, client)

    return fqdn


def _get_token_by_ssh_session(client):
    if yatest_common is not None:
        return None

    try:
        import library.python.oauth as lpo
    except ImportError:
        return

    try:
        token = lpo.get_token(get_config(client)["oauth_client_id"], get_config(client)["oauth_client_secret"])
    except Exception:
        token = None
    if not token:
        global _FAILED_TO_RECEIVE_TOKEN_WARNED
        if not _FAILED_TO_RECEIVE_TOKEN_WARNED:
            logger.warning("Failed to receive token using current ssh session")
            _FAILED_TO_RECEIVE_TOKEN_WARNED = True
        token = None

    return token


def validate_token(token, client):
    if token is not None:
        require(all(33 <= ord(c) <= 126 for c in token),
                lambda: YtTokenError("You have an improper authentication token"))

    if token is None and get_config(client)["check_token"]:
        raise YtTokenError("Token must be specified, to disable this check set 'check_token' option to False")


def _get_token_from_config(client):
    token = get_config(client)["token"]
    if token is not None:
        logger.debug("Token got from environment variable or config")
        return token


def _check_token_file_permissions(token_path):
    file_mode = os.stat(token_path).st_mode
    if (file_mode & stat.S_IRGRP) or (file_mode & stat.S_IROTH):
        logger.warning("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
        logger.warning("@         WARNING: UNPROTECTED TOKEN KEY FILE!            @")
        logger.warning("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
        logger.warning("Permissions {} for '{}' are too open.".format(oct(file_mode)[4:], token_path))
        logger.warning("It is required that your token file is NOT accessible by others.")


def _get_token_from_file(client):
    token_path = get_config(client=client)["token_path"]
    if token_path is None:
        token_path = os.path.join("~", ".yt", "token")
    token_path = os.path.expanduser(token_path)
    if os.path.isfile(token_path):
        with open(token_path, "r") as token_file:
            token = token_file.read().strip()
        logger.debug("Token got from file %s", token_path)
        _check_token_file_permissions(token_path)
        return token


def get_token(token=None, client=None):
    """Extracts token from given `token` and `client` arguments. Also checks token for correctness."""
    if token is not None:
        validate_token(token, client)
        return token

    if not get_config(client)["enable_token"]:
        return None

    # NB: config has higher priority than cache.
    if not token:
        token = _get_token_from_config(client)

    if not token and get_option("_token_cached", client=client):
        logger.debug("Token got from cache")
        return get_option("_token", client=client)

    if not token:
        token = _get_token_from_file(client)
    if not token:
        receive_token_by_ssh_session = \
            get_config(client)["allow_receive_token_by_current_ssh_session"] and \
            RECEIVE_TOKEN_FROM_SSH_SESSION
        if receive_token_by_ssh_session:
            token = _get_token_by_ssh_session(client)
            # Update token in default location.
            if get_config(client=client)["token_path"] is None and token is not None:
                try:
                    token_dir = os.path.join(os.path.expanduser("~"), ".yt")
                    if not os.path.exists(token_dir):
                        os.makedirs(token_dir)
                    token_path = os.path.join(token_dir, "token")
                    with open(token_path, "w") as fout:
                        fout.write(token)
                    os.chmod(token_path, 0o600)
                except IOError:
                    pass

    # Empty token considered as missing.
    if not token:
        token = None

    validate_token(token, client)

    if token is not None and get_config(client=client)["cache_token"]:
        set_option("_token", token, client=client)
        set_option("_token_cached", True, client=client)

    return token


@forbidden_inside_job
def get_user_name(token=None, headers=None, client=None):
    """Requests auth method at proxy to receive user name by token or by cookies in header."""
    if get_backend_type(client) != "http":
        raise YtError("Function 'get_user_name' cannot be implemented for not http clients")

    if token is None and headers is None:
        token = get_token(client=client)

    version = get_http_api_version(client=client)

    headers = headers or {}

    if version in ("v3", "v4"):
        if token is not None:
            headers["Authorization"] = "OAuth " + token.strip()
        data = None
        verb = "whoami"
    else:
        if not token:
            return None
        data = "token=" + token.strip()
        verb = "login"

    impersonation_user = get_config(client).get("impersonation_user")
    if impersonation_user is not None:
        headers["X-YT-User-Name"] = impersonation_user

    response = make_request_with_retries(
        "post",
        get_proxy_address_url(client=client, add_path="/auth/{}".format(verb)),
        headers=headers,
        data=data,
        client=client)
    login = response.json()["login"]
    if not login:
        return None
    return login


def get_cluster_name(client=None):
    proxy_url = get_proxy_address_netloc(required=False, client=client)
    default_suffix = get_config(client)["proxy"]["default_suffix"]
    if proxy_url is not None and default_suffix and proxy_url.endswith(default_suffix.lower()):
        return proxy_url[:-len(default_suffix)]
    return None
