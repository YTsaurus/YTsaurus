from .helpers import get_tests_sandbox
from .conftest import YtTestEnvironment, authors

import mock

import yt.wrapper as yt
import yt.wrapper.tvm as tvm


@authors("ignat")
def test_cypress_authentication_using_rpc_proxy():
    config = {"api_version": "v3", "backend": "rpc"}
    environment = None
    try:
        environment = YtTestEnvironment(
            get_tests_sandbox(),
            "TestAuthenticationCypress",
            config,
            env_options={"native_client_supported": True},
            delta_proxy_config={"authentication": {"enable": True}})
        environment.env.create_native_client().set("//sys/tokens/abc", "pony")
        environment.env.create_native_client().create("user", attributes={"name": "pony"})
        yt.config["token"] = "abc"
        yt.config["enable_token"] = True
        assert yt.exists("/")
    finally:
        if environment is not None:
            environment.cleanup()


@authors("denvr")
def test_tvm_user_ticket():

    tvm_auth = tvm.UserTicketFixedAuth()
    client = yt.YtClient(
        "localhost",
        config={
            "tvm_auth": tvm_auth,
            "proxy": {
                "retries": {
                    "enable": False,
                },
            },
        },
    )

    # skip client prepare
    client._api_version = "v4"
    client._commands = {"get": yt.command.Command(name="get", input_type=None, output_type="output_type", is_volatile=False, is_heavy=False)}

    with mock.patch('yt.packages.urllib3.connectionpool.HTTPConnectionPool.urlopen') as m:
        try:
            tvm_auth.set_user_ticket("3:user:b64BODY:b64SIGN")
            client.get('//')
        except Exception:
            pass
        assert m.call_args.kwargs["headers"]["X-Ya-User-Ticket"] == "3:user:b64BODY:b64SIGN"
