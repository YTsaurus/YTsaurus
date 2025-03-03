from yt_env_setup import YTEnvSetup, Restarter, MASTERS_SERVICE

from yt_commands import (
    authors, wait, create, ls, get, set, exists, remove,
    create_account, create_network_project,
    create_user, create_group, create_tablet_cell_bundle, make_ace,
    add_member, remove_member, remove_group, remove_user,
    remove_network_project, start_transaction, raises_yt_error,
    set_user_password, issue_token, revoke_token, list_user_tokens,
    build_snapshot, print_debug,
    make_batch_request, execute_batch, raise_batch_error,
)

import yt_error_codes

from yt_helpers import profiler_factory

from yt.environment.helpers import assert_items_equal
from yt.common import YtError

from yt.yson import YsonBoolean, YsonEntity

import pytest
import builtins
import datetime
import time


##################################################################


class TestUsers(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_MASTERS = 1
    NUM_NODES = 0

    DELTA_MASTER_CONFIG = {
        "object_service": {
            "sticky_user_error_expire_time": 0
        }
    }

    @authors("babenko", "ignat")
    def test_user_ban1(self):
        create_user("u")

        assert not get("//sys/users/u/@banned")
        get("//tmp", authenticated_user="u")

        set("//sys/users/u/@banned", True)
        assert get("//sys/users/u/@banned")
        with raises_yt_error("User \"u\" is banned"):
            get("//tmp", authenticated_user="u")

        set("//sys/users/u/@banned", False)
        assert not get("//sys/users/u/@banned")

        get("//tmp", authenticated_user="u")

    @authors("babenko", "ignat", "gritukan")
    def test_ban_superuser(self):
        def is_banned(user):
            try:
                get("//tmp", authenticated_user=user)
                return False
            except YtError:
                return True

        create_user("u")
        add_member("u", "superusers")
        assert not is_banned("u")
        set("//sys/users/u/@banned", True)
        assert is_banned("u")

        assert not is_banned("root")
        with raises_yt_error("User \"root\" cannot be banned"):
            set("//sys/users/root/@banned", True)
        assert not is_banned("root")

    @authors("babenko")
    def test_request_rate_limit1(self):
        create_user("u")
        with raises_yt_error("cannot be negative"):
            set("//sys/users/u/@read_request_rate_limit", -1)
        with raises_yt_error("cannot be negative"):
            set("//sys/users/u/@write_request_rate_limit", -1)

    @authors("babenko")
    def test_request_rate_limit2(self):
        create_user("u")
        set("//sys/users/u/@request_rate_limit", 1)

    @authors("babenko")
    def test_request_queue_size_limit1(self):
        create_user("u")
        with raises_yt_error("cannot be negative"):
            set("//sys/users/u/@request_queue_size_limit", -1)

    @authors("babenko")
    def test_request_queue_size_limit2(self):
        create_user("u")
        set("//sys/users/u/@request_queue_size_limit", 1)

    @authors("babenko")
    def test_request_queue_size_limit3(self):
        create_user("u")
        set("//sys/users/u/@request_queue_size_limit", 0)
        with raises_yt_error("has exceeded its request queue size limit"):
            ls("/", authenticated_user="u")
        set("//sys/users/u/@request_queue_size_limit", 1)
        ls("/", authenticated_user="u")

    @authors("aozeritsky")
    def test_request_limits_per_cell(self):
        create_user("u")
        set("//sys/users/u/@request_limits/read_request_rate/default", 1337)
        assert get("//sys/users/u/@request_limits/read_request_rate/default") == 1337

        set("//sys/users/u/@request_limits/read_request_rate/per_cell", {"10": 1338})
        assert get("//sys/users/u/@request_limits/read_request_rate/per_cell/10") == 1338

    @authors("babenko", "aleksandr.gaev")
    def test_builtin_init(self):
        assert_items_equal(get("//sys/groups/everyone/@members"), ["users", "guest"])
        assert_items_equal(
            get("//sys/groups/users/@members"),
            ["superusers", "owner", "admins"],
        )
        assert_items_equal(
            get("//sys/groups/superusers/@members"),
            [
                "root",
                "scheduler",
                "job",
                "replicator",
                "file_cache",
                "operations_cleaner",
                "operations_client",
                "tablet_cell_changelogger",
                "tablet_cell_snapshotter",
                "table_mount_informer",
                "alien_cell_synchronizer",
                "queue_agent",
                "tablet_balancer",
            ],
        )
        assert_items_equal(get("//sys/groups/admins/@members"), [])

        assert_items_equal(get("//sys/users/root/@member_of"), ["superusers"])
        assert_items_equal(get("//sys/users/guest/@member_of"), ["everyone"])
        assert_items_equal(get("//sys/users/scheduler/@member_of"), ["superusers"])
        assert_items_equal(get("//sys/users/job/@member_of"), ["superusers"])
        assert_items_equal(get("//sys/users/replicator/@member_of"), ["superusers"])
        assert_items_equal(get("//sys/users/file_cache/@member_of"), ["superusers"])
        assert_items_equal(get("//sys/users/operations_cleaner/@member_of"), ["superusers"])
        assert_items_equal(get("//sys/users/tablet_cell_changelogger/@member_of"), ["superusers"])
        assert_items_equal(get("//sys/users/tablet_cell_snapshotter/@member_of"), ["superusers"])
        assert_items_equal(get("//sys/users/table_mount_informer/@member_of"), ["superusers"])
        assert_items_equal(get("//sys/users/queue_agent/@member_of"), ["superusers"])
        assert_items_equal(get("//sys/users/tablet_balancer/@member_of"), ["superusers"])

        assert_items_equal(
            get("//sys/users/root/@member_of_closure"),
            ["superusers", "users", "everyone"],
        )
        assert_items_equal(get("//sys/users/guest/@member_of_closure"), ["everyone"])
        assert_items_equal(
            get("//sys/users/scheduler/@member_of_closure"),
            ["superusers", "users", "everyone"],
        )
        assert_items_equal(
            get("//sys/users/job/@member_of_closure"),
            ["superusers", "users", "everyone"],
        )
        assert_items_equal(
            get("//sys/users/replicator/@member_of_closure"),
            ["superusers", "users", "everyone"],
        )
        assert_items_equal(
            get("//sys/users/file_cache/@member_of_closure"),
            ["superusers", "users", "everyone"],
        )
        assert_items_equal(
            get("//sys/users/operations_cleaner/@member_of_closure"),
            ["superusers", "users", "everyone"],
        )
        assert_items_equal(
            get("//sys/users/tablet_cell_changelogger/@member_of_closure"),
            ["superusers", "users", "everyone"],
        )
        assert_items_equal(
            get("//sys/users/tablet_cell_snapshotter/@member_of_closure"),
            ["superusers", "users", "everyone"],
        )
        assert_items_equal(
            get("//sys/users/table_mount_informer/@member_of_closure"),
            ["superusers", "users", "everyone"],
        )
        assert_items_equal(
            get("//sys/users/queue_agent/@member_of_closure"),
            ["superusers", "users", "everyone"],
        )
        assert_items_equal(
            get("//sys/users/tablet_balancer/@member_of_closure"),
            ["superusers", "users", "everyone"],
        )

    @authors("babenko", "ignat")
    def test_create_user1(self):
        create_user("max")
        assert get("//sys/users/max/@name") == "max"
        assert "max" in get("//sys/groups/users/@members")
        assert get("//sys/users/max/@member_of") == ["users"]

    @authors("babenko", "ignat")
    def test_create_user2(self):
        create_user("max")
        with raises_yt_error("User \"max\" already exists"):
            create_user("max")
        with raises_yt_error("User \"max\" already exists"):
            create_group("max")

    @authors("babenko", "ignat")
    def test_create_group1(self):
        create_group("devs")
        assert get("//sys/groups/devs/@name") == "devs"

    @authors("babenko", "ignat")
    def test_create_group2(self):
        create_group("devs")
        with raises_yt_error("Group \"devs\" already exists"):
            create_user("devs")
        with raises_yt_error("Group \"devs\" already exists"):
            create_group("devs")

    @authors("babenko", "ignat")
    def test_user_remove_builtin(self):
        with raises_yt_error("Cannot remove a built-in user"):
            remove_user("root")
        with raises_yt_error("Cannot remove a built-in user"):
            remove_user("guest")

    @authors("babenko", "ignat")
    def test_group_remove_builtin(self):
        with raises_yt_error("Cannot remove a built-in group"):
            remove_group("everyone")
        with raises_yt_error("Cannot remove a built-in group"):
            remove_group("users")

    @authors("ignat")
    def test_membership1(self):
        create_user("max")
        create_group("devs")
        add_member("max", "devs")

        assert get("//sys/groups/devs/@members") == ["max"]
        assert get("//sys/groups/devs/@members") == ["max"]

        with raises_yt_error(yt_error_codes.IsAlreadyPresentInGroup):
            add_member("max", "devs")

    @authors("asaitgalin", "babenko", "ignat")
    def test_membership2(self):
        create_user("u1")
        create_user("u2")
        create_group("g1")
        create_group("g2")

        add_member("u1", "g1")
        add_member("g2", "g1")
        add_member("u2", "g2")

        assert sorted(get("//sys/groups/g1/@members")) == sorted(["u1", "g2"])
        assert get("//sys/groups/g2/@members") == ["u2"]

        assert sorted(get("//sys/users/u1/@member_of")) == sorted(["g1", "users"])
        assert sorted(get("//sys/users/u2/@member_of")) == sorted(["g2", "users"])

        assert sorted(get("//sys/users/u1/@member_of_closure")) == sorted(["g1", "users", "everyone"])
        assert sorted(get("//sys/users/u2/@member_of_closure")) == sorted(["g1", "g2", "users", "everyone"])

        remove_member("g2", "g1")

        assert get("//sys/groups/g1/@members") == ["u1"]
        assert get("//sys/groups/g2/@members") == ["u2"]

        assert sorted(get("//sys/users/u1/@member_of")) == sorted(["g1", "users"])
        assert sorted(get("//sys/users/u2/@member_of")) == sorted(["g2", "users"])

        assert sorted(get("//sys/users/u1/@member_of_closure")) == sorted(["g1", "users", "everyone"])
        assert sorted(get("//sys/users/u2/@member_of_closure")) == sorted(["g2", "users", "everyone"])

    @authors("babenko", "ignat")
    def test_membership3(self):
        create_group("g1")
        create_group("g2")
        create_group("g3")

        add_member("g2", "g1")
        add_member("g3", "g2")
        with raises_yt_error("would produce a cycle"):
            add_member("g1", "g3")

    @authors("ignat")
    def test_membership4(self):
        create_user("u")
        create_group("g")
        add_member("u", "g")
        remove_user("u")
        assert get("//sys/groups/g/@members") == []

    @authors("ignat")
    def test_membership5(self):
        create_user("u")
        create_group("g")
        add_member("u", "g")
        assert sorted(get("//sys/users/u/@member_of")) == sorted(["g", "users"])
        remove_group("g")
        assert get("//sys/users/u/@member_of") == ["users"]

    @authors("babenko", "ignat")
    def test_membership6(self):
        create_user("u")
        create_group("g")

        with raises_yt_error("is not present in group"):
            remove_member("u", "g")

        add_member("u", "g")
        with raises_yt_error("is already present in group"):
            add_member("u", "g")

    @authors("babenko")
    def test_membership7(self):
        create_group("g")
        with raises_yt_error("would produce a cycle"):
            add_member("g", "g")

    @authors("ignat")
    def test_modify_builtin(self):
        create_user("u")
        with raises_yt_error("Cannot modify group"):
            remove_member("u", "everyone")
        with raises_yt_error("Cannot modify group"):
            remove_member("u", "users")
        with raises_yt_error("Cannot modify group"):
            add_member("u", "everyone")
        with raises_yt_error("Cannot modify group"):
            add_member("u", "users")

    @authors("babenko")
    def test_create_banned_user(self):
        create_user("u", attributes={"banned": True})
        users = ls("//sys/users", attributes=["banned"])
        assert get("//sys/users/u/@banned")
        found = False
        for item in users:
            if str(item) == "u":
                assert item.attributes["banned"]
                found = True
        assert found

    @authors("asaitgalin", "babenko")
    def test_remove_group(self):
        create_user("u")
        create_group("g")
        add_member("u", "g")

        assert sorted(get("//sys/users/u/@member_of")) == sorted(["g", "users"])
        assert sorted(get("//sys/users/u/@member_of_closure")) == sorted(["g", "users", "everyone"])

        remove_group("g")

        assert get("//sys/users/u/@member_of") == ["users"]
        assert sorted(get("//sys/users/u/@member_of_closure")) == sorted(["users", "everyone"])

    @authors("prime")
    def test_prerequisite_transactions(self):
        create_group("g8")

        with raises_yt_error("Unknown transaction cell tag"):
            add_member("root", "g8", prerequisite_transaction_ids=["a-b-c-d"])

        with raises_yt_error("Unknown transaction cell tag"):
            remove_member("root", "g8", prerequisite_transaction_ids=["a-b-c-d"])

        tx = start_transaction()
        add_member("root", "g8", prerequisite_transaction_ids=[tx])
        remove_member("root", "g8", prerequisite_transaction_ids=[tx])

    @authors("shakurov")
    def test_usable_accounts(self):
        create_user("u")

        create_account("a1")
        create_account("a2")

        assert sorted(get("//sys/users/u/@usable_accounts")) == [
            "intermediate",
            "tmp",
        ]  # these are defaults

        set("//sys/accounts/a1/@acl", [make_ace("allow", "u", "use")])

        assert sorted(get("//sys/users/u/@usable_accounts")) == [
            "a1",
            "intermediate",
            "tmp",
        ]

        create_group("g")

        set("//sys/accounts/a2/@acl", [make_ace("allow", "g", "use")])

        assert sorted(get("//sys/users/u/@usable_accounts")) == [
            "a1",
            "intermediate",
            "tmp",
        ]

        add_member("u", "g")

        assert sorted(get("//sys/users/u/@usable_accounts")) == [
            "a1",
            "a2",
            "intermediate",
            "tmp",
        ]

    @authors("s-v-m")
    def test_usable_tablet_cell_bundles(self):
        create_user("u")
        create_tablet_cell_bundle("tcb1")
        create_tablet_cell_bundle("tcb2")
        set("//sys/tablet_cell_bundles/tcb1/@acl", [make_ace("allow", "u", "use")])
        assert sorted(get("//sys/users/u/@usable_tablet_cell_bundles")) == ['default', "sequoia", "tcb1"]
        create_group("g")
        add_member("u", "g")
        set("//sys/tablet_cell_bundles/tcb2/@acl", [make_ace("allow", "g", "use")])
        assert sorted(get("//sys/users/u/@usable_tablet_cell_bundles")) == ['default', "sequoia", "tcb1", "tcb2"]

    @authors("babenko", "kiselyovp")
    def test_delayed_membership_closure(self):
        create_group("g1")
        create_group("g2")
        create_user("u")
        add_member("g1", "g2")

        set(
            "//sys/@config/security_manager/membership_closure_recomputation_period",
            3000,
        )
        set(
            "//sys/@config/security_manager/enable_delayed_membership_closure_recomputation",
            True,
        )
        add_member("u", "g1")

        wait(lambda: builtins.set(["g1", "g2"]) <= builtins.set(get("//sys/users/u/@member_of_closure")))

    @authors("gritukan")
    def test_network_projects(self):
        create_network_project("a")

        with pytest.raises(YtError):
            create_network_project("a")

        set("//sys/network_projects/a/@project_id", 123)
        assert get("//sys/network_projects/a/@project_id") == 123

        with pytest.raises(YtError):
            set("//sys/network_projects/a/@project_id", "abc")

        with pytest.raises(YtError):
            set("//sys/network_projects/a/@project_id", -1)

        set("//sys/network_projects/a/@name", "b")
        assert not exists("//sys/network_projects/a")
        assert get("//sys/network_projects/b/@project_id") == 123

        remove_network_project("b")
        assert not exists("//sys/network_projects/b")

    @authors("gritukan")
    def test_network_projects_acl(self):
        create_user("u1")
        create_user("u2")

        create_network_project("a")

        set("//sys/network_projects/a/@acl", [make_ace("allow", "u1", "use")])
        assert sorted(get("//sys/users/u1/@usable_network_projects")) == ["a"]
        assert get("//sys/users/u2/@usable_network_projects") == []

        create_group("g")
        set("//sys/network_projects/a/@acl", [make_ace("allow", "g", "use")])
        add_member("u2", "g")
        assert sorted(get("//sys/users/u2/@usable_network_projects")) == ["a"]

    @authors("ifsmirnov")
    def test_create_non_external_table(self):
        create("table", "//tmp/t1", attributes={"external": False})

        create_user("u")
        with raises_yt_error("is not allowed to create explicitly non-external nodes"):
            create(
                "table",
                "//tmp/t2",
                attributes={"external": False},
                authenticated_user="u",
            )

        set("//sys/users/u/@allow_external_false", True)
        create("table", "//tmp/t3", attributes={"external": False}, authenticated_user="u")

        set("//sys/users/u/@allow_external_false", False)
        with raises_yt_error("is not allowed to create explicitly non-external nodes"):
            create(
                "table",
                "//tmp/t4",
                attributes={"external": False},
                authenticated_user="u",
            )

    @authors("aleksandra-zh")
    def test_distributed_throttler_simple(self):
        create_user("u")

        set("//sys/@config/security_manager/enable_distributed_throttler", True)
        get("//tmp", authenticated_user="u")

        set("//sys/@config/security_manager/enable_distributed_throttler", False)
        get("//tmp", authenticated_user="u")

    @authors("aleksandra-zh")
    def test_distributed_throttler_profiler(self):
        create_user("u")

        set("//sys/@config/security_manager/enable_distributed_throttler", True)
        get("//tmp", authenticated_user="u")

        master_address = ls("//sys/primary_masters")[0]
        profiler = profiler_factory().at_primary_master(master_address)

        def get_throttler_gauge(name):
            # Provoke limits update
            get("//tmp", authenticated_user="u")

            path = "security/distributed_throttler/{}".format(name)
            gauge = profiler.gauge(name=path, fixed_tags={"throttler_id": "u:request_count:Read"}).get()
            if gauge is None:
                return 0
            return gauge

        wait(lambda: get_throttler_gauge("usage") > 0)
        wait(lambda: get_throttler_gauge("limit") > 0)

        for _ in range(5):
            with Restarter(self.Env, MASTERS_SERVICE):
                pass

            wait(lambda: get_throttler_gauge("usage") > 0)
            wait(lambda: get_throttler_gauge("limit") > 0)

    @authors("gritukan")
    def test_recomute_membership_closure_on_group_destruction(self):
        create_user("u")
        create_group("g1")
        create_group("g2")

        add_member("u", "g1")
        add_member("g1", "g2")

        assert_items_equal(get("//sys/users/u/@member_of_closure"), ["g1", "g2", "everyone", "users"])
        assert_items_equal(get("//sys/groups/g1/@member_of_closure"), ["g2"])

        # Disable periodic membership closure recomputation.
        set("//sys/@config/security_manager/enable_delayed_membership_closure_recomputation", True)
        set("//sys/@config/security_manager/membership_closure_recomputation_period", 10**8)

        remove_group("g2")

        assert_items_equal(get("//sys/users/u/@member_of_closure"), ["g1", "everyone", "users"])
        assert_items_equal(get("//sys/groups/g1/@member_of_closure"), [])

    @authors("gritukan")
    def test_set_user_password(self):
        if self.DRIVER_BACKEND == "rpc":
            return

        create_user("u")
        create_user("v")

        assert not exists("//sys/users/u/@hashed_password")
        assert not exists("//sys/users/u/@password_salt")
        assert not exists("//sys/users/u/@password")
        rev1 = get("//sys/users/u/@password_revision")

        set_user_password("u", "admin")
        enc2 = get("//sys/users/u/@hashed_password")
        assert len(enc2) == 64
        salt2 = get("//sys/users/u/@password_salt")
        assert len(salt2) == 32
        rev2 = get("//sys/users/u/@password_revision")
        assert rev2 > rev1

        with raises_yt_error("User provided invalid password"):
            set_user_password("u", "admin2", authenticated_user="u")
        with raises_yt_error("User provided invalid password"):
            set_user_password("u", "admin2", "123456", authenticated_user="u")
        with raises_yt_error("Password change can be performed either"):
            set_user_password("u", "admin2", authenticated_user="v")
        set("//sys/users/u/@acl", [make_ace("allow", "v", "administer")])
        set_user_password("u", "admin2", authenticated_user="v")
        set_user_password("u", "admin3", "admin2", authenticated_user="u")

        enc3 = get("//sys/users/u/@hashed_password")
        assert enc3 != enc2
        salt3 = get("//sys/users/u/@password_salt")
        assert salt3 != salt2
        rev3 = get("//sys/users/u/@password_revision")
        assert rev3 > rev2

        add_member("v", "superusers")

        # Same password, another salt.
        set_user_password("u", "admin2", authenticated_user="v")
        set("//sys/users/u/@password", "admin")
        assert get("//sys/users/u/@hashed_password") != enc3
        assert get("//sys/users/u/@password_salt") != salt3
        rev3 = get("//sys/users/u/@password_revision")
        assert rev3 > rev2

        remove("//sys/users/u/@hashed_password")
        remove("//sys/users/u/@password_salt")
        assert not exists("//sys/users/u/@hashed_password")
        assert not exists("//sys/users/u/@password_salt")
        rev4 = get("//sys/users/u/@password_revision")
        assert rev4 > rev3

    @authors("gritukan", "aleksandr.gaev")
    def test_tokens(self):
        if self.DRIVER_BACKEND == "rpc":
            return

        create_user("u")
        create_user("v")
        set_user_password("u", "u")
        set_user_password("v", "v")

        t1_token, t1_hash = issue_token("u")
        assert t1_token[:5] == "ytct-" and t1_token[9] == "-"
        assert get(f"//sys/cypress_tokens/{t1_hash}/@user") == "u"
        assert_items_equal(list_user_tokens("u"), [t1_hash])
        assert list_user_tokens("v") == []

        t2_token, t2_hash = issue_token("u", "u", authenticated_user="u")
        assert t2_token[:5] == "ytct-" and t2_token[9] == "-"
        assert_items_equal(list_user_tokens("u"), [t1_hash, t2_hash])

        with raises_yt_error("User provided invalid password"):
            issue_token("u", "a", authenticated_user="u")
        with raises_yt_error("Token issuance can be performed"):
            issue_token("u", "v", authenticated_user="v")

        with raises_yt_error("Provided token is not recognized"):
            revoke_token("u", "xxx", "u", authenticated_user="u")
        with raises_yt_error("User provided invalid password"):
            revoke_token("u", t1_hash, "a", authenticated_user="u")
        with raises_yt_error("Provided token is not recognized"):
            revoke_token("v", t1_hash, "v", authenticated_user="v")
        with raises_yt_error("Token revocation can be performed"):
            revoke_token("u", t1_hash, "v", authenticated_user="v")
        assert_items_equal(list_user_tokens("u"), [t1_hash, t2_hash])

        revoke_token("u", t1_hash)
        assert_items_equal(list_user_tokens("u"), [t2_hash])

        revoke_token("u", t2_hash, "u", authenticated_user="u")
        assert list_user_tokens("u") == []

    @authors("aleksandr.gaev")
    def test_tokens_with_metadata(self):
        if self.DRIVER_BACKEND == "rpc":
            return

        create_user("u")
        create_user("v")
        set_user_password("u", "u")
        set_user_password("v", "v")

        assert list_user_tokens("v", with_metadata=True) == {}

        token, token_hash = issue_token("u")
        result = list_user_tokens("u", with_metadata=True)
        ct = get(f"//sys/cypress_tokens/{token_hash}/@creation_time")
        assert token_hash in result
        assert result[token_hash]["description"] == ""
        assert len(result[token_hash]["token_prefix"]) == len("ytct-abcd-") and result[token_hash]["token_prefix"][:5] == "ytct-" and result[token_hash]["token_prefix"][9] == "-"
        assert token[:10] == result[token_hash]["token_prefix"]
        assert result[token_hash]["creation_time"] == ct
        assert result[token_hash]["effective_expiration"] == {"time": YsonEntity(), "timeout": YsonEntity()}
        revoke_token("u", token_hash)

        token, token_hash = issue_token("u", description="")
        result = list_user_tokens("u", with_metadata=True)
        ct = get(f"//sys/cypress_tokens/{token_hash}/@creation_time")
        assert token_hash in result
        assert result[token_hash]["description"] == ""
        assert len(result[token_hash]["token_prefix"]) == len("ytct-abcd-") and result[token_hash]["token_prefix"][:5] == "ytct-" and result[token_hash]["token_prefix"][9] == "-"
        assert token[:10] == result[token_hash]["token_prefix"]
        assert result[token_hash]["creation_time"] == ct
        assert result[token_hash]["effective_expiration"] == {"time": YsonEntity(), "timeout": YsonEntity()}
        revoke_token("u", token_hash)

        token, token_hash = issue_token("u", description="desc")
        result = list_user_tokens("u", with_metadata=True)
        ct = get(f"//sys/cypress_tokens/{token_hash}/@creation_time")
        assert token_hash in result
        assert result[token_hash]["description"] == "desc"
        assert len(result[token_hash]["token_prefix"]) == len("ytct-abcd-") and result[token_hash]["token_prefix"][:5] == "ytct-" and result[token_hash]["token_prefix"][9] == "-"
        assert token[:10] == result[token_hash]["token_prefix"]
        assert result[token_hash]["creation_time"] == ct
        assert result[token_hash]["effective_expiration"] == {"time": YsonEntity(), "timeout": YsonEntity()}

    @authors("shakurov")
    def test_user_request_profiling(self):
        master_address = ls("//sys/primary_masters")[0]
        profiler = profiler_factory().at_primary_master(master_address)

        def check_profiling_counters(user_name, should_exist):
            read_time = profiler.counter("security/user_read_time", tags={"user": user_name})
            write_time = profiler.counter("security/user_write_time", tags={"user": user_name})
            read_request_count = profiler.counter("security/user_read_request_count", tags={"user": user_name})
            write_request_count = profiler.counter("security/user_write_request_count", tags={"user": user_name})
            request_count = profiler.counter("security/user_request_count", tags={"user": user_name})
            request_queue_size = profiler.counter("security/user_request_queue_size", tags={"user": user_name})

            wait(lambda: (read_time.get() is not None) == should_exist)
            wait(lambda: (write_time.get() is not None) == should_exist)
            wait(lambda: (write_request_count.get() is not None) == should_exist)
            wait(lambda: (read_request_count.get() is not None) == should_exist)
            wait(lambda: (request_count.get() is not None) == should_exist)
            wait(lambda: (request_queue_size.get() is not None) == should_exist)

        create_user("u")
        check_profiling_counters("u", True)

        set("//sys/users/u/@name", "v")
        assert not exists("//sys/users/u")
        assert exists("//sys/users/v")

        check_profiling_counters("u", False)
        check_profiling_counters("v", True)

        build_snapshot(cell_id=None)

        # Shutdown masters and wait a bit.
        with Restarter(self.Env, MASTERS_SERVICE):
            pass

        check_profiling_counters("u", False)
        check_profiling_counters("v", True)

    def _get_last_seen_time(self, username):
        last_seen_yson = get(f"//sys/users/{username}/@last_seen_time")
        return datetime.datetime.strptime(last_seen_yson, "%Y-%m-%dT%H:%M:%S.%fZ")

    @authors("cherepashka")
    def test_last_seen_time_is_increasing(self):
        create_user("u")
        wait(lambda: self._get_last_seen_time("u").year > 1970, timeout=10)
        last_seen = self._get_last_seen_time("u")

        for table_ind in range(5):
            create("table", f"//tmp/t{table_ind}", authenticated_user="u")
            wait(lambda: self._get_last_seen_time("u") > last_seen, timeout=2)
            assert self._get_last_seen_time("u") - last_seen < datetime.timedelta(seconds=2)
            last_seen = self._get_last_seen_time("u")

        last_seen = self._get_last_seen_time("u")
        set("//sys/users/u/@name", "v", authenticated_user="u")
        wait(lambda: self._get_last_seen_time("v") > last_seen, timeout=2)

    @authors("krock21")
    def test_password_temporary(self):
        if self.DRIVER_BACKEND == "rpc":
            pytest.skip()

        create_user("u")
        assert not exists("//sys/users/u/@password_is_temporary")

        set_user_password("u", "p1")
        assert get("//sys/users/u/@password_is_temporary") == YsonBoolean(False)

        set_user_password("u", "p2", password_is_temporary=True)
        assert get("//sys/users/u/@password_is_temporary") == YsonBoolean(True)

        set("//sys/users/u/@password_is_temporary", "abc")
        assert get("//sys/users/u/@password_is_temporary") == "abc"

        set_user_password("u", "p3", current_password="p2", password_is_temporary=True)
        assert get("//sys/users/u/@password_is_temporary") == YsonBoolean(True)

        set_user_password("u", "p4")
        assert get("//sys/users/u/@password_is_temporary") == YsonBoolean(False)

        set("//sys/users/u/@password_is_temporary", False)
        assert get("//sys/users/u/@password_is_temporary") == YsonBoolean(False)

        set("//sys/users/u/@password_is_temporary", True)
        assert get("//sys/users/u/@password_is_temporary") == YsonBoolean(True)

        set("//sys/users/u/@password_is_temporary", "cde")
        assert get("//sys/users/u/@password_is_temporary") == "cde"

        remove("//sys/users/u/@password_is_temporary")
        assert not exists("//sys/users/u/@password_is_temporary")

    @authors("babenko")
    def test_alias_auth(self):
        create_user("u", attributes={"aliases": ["u1", "u2"]})
        for user in ["u", "u1", "u2"]:
            assert exists("/", authenticated_user=user)


##################################################################


@pytest.mark.enabled_multidaemon
class TestRequestThrottling(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 3
    NUM_NODES = 0

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "security_manager": {
            "request_rate_smoothing_period": 100,
        },
        "object_service": {
            "local_read_request_throttler": {
                "period": 100,
            },
            "local_write_request_throttler": {
                "period": 100,
            },
        },
    }

    @staticmethod
    def _prepare_user_write_request_throttling_parameters(user):
        return {
            "set_limit_function": lambda: set(f"//sys/users/{user}/@write_request_rate_limit", 1),
            "make_batch_request_function": lambda i: make_batch_request("create", type="map_node", path=f"//tmp/alice-{i:02d}"),
            "request_count": 10,
            # Very conservative, this ratio was closer to 100 in local runs.
            "throttled_to_non_throttled_ratio_threshold": 5,
            # The throttled execution time should be close to 10 seconds, but let's be generous.
            "throttled_batch_execution_time_lower_bound": 5.0,
        }

    @staticmethod
    def _prepare_user_read_request_throttling_parameters(user):
        create("map_node", "//tmp/read_me")

        return {
            "set_limit_function": lambda: set(f"//sys/users/{user}/@read_request_rate_limit", 1),
            "make_batch_request_function": lambda _: make_batch_request("get", path="//tmp/read_me/@owner"),
            "request_count": 10,
            # Very conservative, this ratio was closer to 100 (or more) in local runs.
            "throttled_to_non_throttled_ratio_threshold": 5,
            # The throttled execution time should be close to 10 seconds, but let's be generous.
            "throttled_batch_execution_time_lower_bound": 5.0,
        }

    @staticmethod
    def _prepare_automaton_write_request_throttling_parameters(_):
        return {
            "set_limit_function": lambda: set("//sys/@config/object_service/local_write_request_throttler/limit", 1),
            "make_batch_request_function": lambda i: make_batch_request("create", type="map_node", path=f"//tmp/alice-{i:02d}"),
            "request_count": 10,
            # Very conservative, this ratio was closer to 100 in local runs.
            "throttled_to_non_throttled_ratio_threshold": 5,
            # The throttled execution time should be close to 10 seconds, but let's be generous.
            "throttled_batch_execution_time_lower_bound": 5.0,
        }

    @staticmethod
    def _prepare_automaton_read_request_throttling_parameters(_):
        create("map_node", "//tmp/read_me")

        return {
            "set_limit_function": lambda: set("//sys/@config/object_service/local_read_request_throttler/limit", 1),
            "make_batch_request_function": lambda _: make_batch_request("get", path="//tmp/read_me/@owner"),
            "request_count": 10,
            # Very conservative, this ratio was closer to 100 in local runs.
            "throttled_to_non_throttled_ratio_threshold": 5,
            # The throttled execution time should be close to 10 seconds, but let's be generous.
            "throttled_batch_execution_time_lower_bound": 5.0,
        }

    @staticmethod
    def _prepare_automaton_and_user_write_request_throttling_parameters(user):
        def set_limit():
            set("//sys/@config/object_service/local_write_request_throttler/limit", 1)
            set(f"//sys/users/{user}/@write_request_rate_limit", 1)

        return {
            "set_limit_function": set_limit,
            "make_batch_request_function": lambda i: make_batch_request("create", type="map_node", path=f"//tmp/alice-{i:02d}"),
            "request_count": 10,
            # Very conservative, this ratio was closer to 100 in local runs.
            "throttled_to_non_throttled_ratio_threshold": 5,
            # The throttled execution time should be close to 10 seconds, but let's be generous.
            "throttled_batch_execution_time_lower_bound": 5.0,
        }

    @staticmethod
    def _prepare_automaton_and_user_read_write_requests_throttling_parameters(user):
        create("map_node", "//tmp/read_me")

        def set_limit():
            set("//sys/@config/object_service/local_read_request_throttler/limit", 1)
            set(f"//sys/users/{user}/@read_request_rate_limit", 1)
            set("//sys/@config/object_service/local_write_request_throttler/limit", 1)
            set(f"//sys/users/{user}/@write_request_rate_limit", 1)

        return {
            "set_limit_function": set_limit,
            "make_batch_request_function": lambda i: make_batch_request("get", path="//tmp/read_me/@owner")
                if i % 2 == 0
                else make_batch_request("create", type="map_node", path=f"//tmp/alice-{i:02d}"),
            "request_count": 20,
            # Very conservative, this ratio was closer to 100 in local runs.
            "throttled_to_non_throttled_ratio_threshold": 5,
            # The throttled execution time should be close to 10 seconds, but let's be generous.
            "throttled_batch_execution_time_lower_bound": 5.0,
        }

    @staticmethod
    def _prepare_automaton_and_user_read_request_throttling_parameters(user):
        create("map_node", "//tmp/read_me")

        def set_limit():
            set("//sys/@config/object_service/local_read_request_throttler/limit", 1)
            set(f"//sys/users/{user}/@read_request_rate_limit", 1)

        return {
            "set_limit_function": set_limit,
            "make_batch_request_function": lambda _: make_batch_request("get", path="//tmp/read_me/@owner"),
            "request_count": 10,
            # Very conservative, this ratio was closer to 100 in local runs.
            "throttled_to_non_throttled_ratio_threshold": 5,
            # The throttled execution time should be close to 10 seconds, but let's be generous.
            "throttled_batch_execution_time_lower_bound": 5.0,
        }

    @authors("achulkov2")
    @pytest.mark.parametrize("prepare_test_parameters", [
        _prepare_user_write_request_throttling_parameters,
        _prepare_user_read_request_throttling_parameters,
        _prepare_automaton_read_request_throttling_parameters,
        _prepare_automaton_write_request_throttling_parameters,
        _prepare_automaton_and_user_write_request_throttling_parameters,
        _prepare_automaton_and_user_read_write_requests_throttling_parameters,
        _prepare_automaton_and_user_read_request_throttling_parameters,
    ])
    def test_request_throttling(self, prepare_test_parameters):
        user = "alice"
        create_user(user)

        print_debug("Performing run without master user throttling")

        params = prepare_test_parameters(user)

        start_time = time.time()

        request_results = execute_batch([params["make_batch_request_function"](i) for i in range(params["request_count"])], authenticated_user=user)
        for request_result in request_results:
            raise_batch_error(request_result)

        no_throttling_batch_execution_time = time.time() - start_time

        throttled_batch_execution_time = 0

        print_debug("Performing run with master user throttling")

        params["set_limit_function"]()

        start_time = time.time()

        request_results = execute_batch([params["make_batch_request_function"](i + params["request_count"]) for i in range(params["request_count"])], authenticated_user=user)
        for request_result in request_results:
            raise_batch_error(request_result)

        throttled_batch_execution_time = time.time() - start_time

        # This assertion depends on how fast requests are executed without throttling.
        # You can tweak this constant if the test turns out to be flaky, but in this case I would
        # also advise to investigate why requests are running so slowly, because the constants
        # are set quite conservatively.
        assert throttled_batch_execution_time > params["throttled_to_non_throttled_ratio_threshold"] * no_throttling_batch_execution_time
        # This constant is also configured quite generously, do not change without some investigation.
        assert throttled_batch_execution_time > params["throttled_batch_execution_time_lower_bound"]

        print_debug(f"Non-throttled batch request execution took {no_throttling_batch_execution_time:.2f} seconds")
        print_debug(f"Throttled batch request execution took {throttled_batch_execution_time:.2f} seconds")

    @authors("achulkov2")
    def test_automaton_request_throttling_does_not_impact_read_requests(self):
        create_user("alice")

        create("map_node", "//tmp/read_me")

        request_count = 10
        local_write_request_throttler_limit_path = "//sys/@config/object_service/local_write_request_throttler/limit"

        # This throttler should only impact write requests.
        set(local_write_request_throttler_limit_path, 1)

        start_time = time.time()

        create_results = execute_batch([make_batch_request("get", path="//tmp/read_me/@owner") for i in range(request_count)], authenticated_user="alice")
        for create_result in create_results:
            raise_batch_error(create_result)

        no_throttling_batch_execution_time = time.time() - start_time

        # This is also a very generous threshold compared to my local runs.
        # Do not change without some investigation.
        assert no_throttling_batch_execution_time < 3

        print_debug(f"Non-throttled batch request execution took {no_throttling_batch_execution_time:.2f} seconds")


##################################################################


class TestUsersRpcProxy(TestUsers):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True


##################################################################


class TestUsersMulticell(TestUsers):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_SECONDARY_MASTER_CELLS = 2

    @authors("aleksandra-zh")
    def test_request_limit_cell_names(self):
        create_user("u")
        set("//sys/@config/multicell_manager/cell_descriptors/10", {"name": "Julia"})
        set("//sys/users/u/@request_limits/read_request_rate/per_cell", {"Julia": 100, "11": 200})
        assert get("//sys/users/u/@request_limits/read_request_rate/per_cell") == {"Julia": 100, "11": 200}

        set("//sys/@config/multicell_manager/cell_descriptors/11", {"name": "George"})
        assert get("//sys/users/u/@request_limits/read_request_rate/per_cell") == {"Julia": 100, "George": 200}

    @authors("cherepashka")
    def test_last_seen_via_visit_portal(self):
        # Make sure secondary cell can host portal entries.
        set("//sys/@config/multicell_manager/cell_descriptors", {"11": {"roles": ["cypress_node_host"]}})

        create_user("u")
        wait(lambda: self._get_last_seen_time("u").year > 1970, timeout=10)
        last_seen = self._get_last_seen_time("u")

        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        create("table", "//tmp/p/t", authenticated_user="u")
        wait(lambda: self._get_last_seen_time("u") > last_seen, timeout=2)
        assert self._get_last_seen_time("u") - last_seen < datetime.timedelta(seconds=2)
