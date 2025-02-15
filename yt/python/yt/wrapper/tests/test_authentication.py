from .helpers import get_tests_sandbox
from .conftest import YtTestEnvironment, authors
from .helpers import TEST_DIR, wait

import mock
import pytest

import yt.wrapper as yt
import yt.wrapper.tvm as tvm

from yt.common import update


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


@pytest.mark.usefixtures("yt_env_with_authentication")
class TestImpresonation(object):
    @authors("achulkov2")
    def test_impersonation(self):
        yt.create("user", attributes={"name": "alice"})
        yt.set("//sys/tokens/bob", "alice")

        yt.set(f"{TEST_DIR}/@acl/end", {"action": "allow", "subjects": ["alice"], "permissions": ["read", "write"]})

        root_client_with_impersonation = yt.YtClient(config=update(yt.config.config, {"impersonation_user": "alice"}))

        assert root_client_with_impersonation.get_user_name() == "alice"
        node = TEST_DIR + "/node"
        root_client_with_impersonation.create("map_node", node, attributes={"account": "tmp"})
        assert root_client_with_impersonation.get(node + "/@owner") == "alice"

        alice_client_with_impersonation = yt.YtClient(config=update(yt.config.config, {"token": "bob", "impersonation_user": "root"}))
        # Oh my, Alice, what are you doing, you are not a superuser!
        with pytest.raises(yt.YtError):
            alice_client_with_impersonation.exists("/")

        yt.add_member("alice", "superusers")

        # Now it is OK, once caches are updated.
        wait(lambda: alice_client_with_impersonation.exists("/"), ignore_exceptions=True)

        yt.set("//sys/users/alice/@banned", True)

        def wait_for_error(callback):
            def check():
                try:
                    callback()
                    return False
                except yt.YtError:
                    return True

            wait(check)

        # Alice is banned, she can't do anything.
        wait_for_error(lambda: alice_client_with_impersonation.exists("/"))
