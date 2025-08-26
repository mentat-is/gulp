import asyncio
import pytest
import pytest_asyncio
from muty.log import MutyLogger

from gulp_client.common import _ensure_test_operation
from gulp_client.db import GulpAPIDb
from gulp_client.operation import GulpAPIOperation
from gulp_client.query import GulpAPIQuery
from gulp_client.user import GulpAPIUser
from gulp_client.test_values import (
    TEST_OPERATION_ID,
    TEST_CONTEXT_ID,
    TEST_SOURCE_ID,
    TEST_WS_ID,
    TEST_REQ_ID,
)
from gulp_client.note import GulpAPINote
from gulp_client.operation import GulpAPIOperation
from gulp.api.collab.structs import GulpCollabFilter
from gulp_client.object_acl import GulpAPIObjectACL


@pytest_asyncio.fixture(scope="function", autouse=True)
async def _setup():
    await _ensure_test_operation()


@pytest.mark.asyncio
async def test_user():
    # login admin, guest
    admin_token = await GulpAPIUser.login("admin", "admin")
    assert admin_token

    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token

    # test user creation
    test_user_id = "test_user"
    test_user_password = "Test123!"
    try:
        await GulpAPIUser.user_delete(admin_token, test_user_id)
    except Exception:
        pass
    user = await GulpAPIUser.user_create(
        admin_token, test_user_id, test_user_password, ["read"], "test@example.com"
    )
    assert user.get("email") == "test@example.com"
    assert user.get("permission") == ["read"]

    # test user listing
    users = await GulpAPIUser.user_list(admin_token)
    assert users and len(users) >= 1

    # test user update
    updated = await GulpAPIUser.user_update(
        admin_token,
        test_user_id,
        permission=["read", "edit"],
        email="updated@example.com",
        user_data={
            "hello": "world",
        },
    )
    assert updated.get("email") == "updated@example.com"
    assert updated.get("permission") == ["read", "edit"]
    assert updated.get("user_data") == {"hello": "world"}

    # test user deletion
    res = await GulpAPIUser.user_delete(admin_token, test_user_id)
    assert res["id"] == test_user_id

    # verify deletion
    _ = await GulpAPIUser.user_get_by_id(admin_token, test_user_id, expected_status=404)

    # logout admin
    t = await GulpAPIUser.logout(admin_token)
    assert t == admin_token

    # admin should be logget out, calling any api should return 401
    await GulpAPIUser.user_list(admin_token, expected_status=401)

    # guest tests now!

    # guest cannot create, list, delete users
    await GulpAPIUser.user_create(
        guest_token, "new_user", "Password#1234!", ["read"], expected_status=401
    )
    await GulpAPIUser.user_list(guest_token, expected_status=401)
    await GulpAPIUser.user_delete(guest_token, "editor", expected_status=401)

    # guest should not be able to update its own permission
    await GulpAPIUser.user_update(
        guest_token, "guest", permission=["read", "edit"], expected_status=401
    )

    # guest should not be able to update other users
    await GulpAPIUser.user_update(
        guest_token, "editor", password="Hacked#1234!", expected_status=401
    )

    # guest should be able to get their own details
    guest_data = await GulpAPIUser.user_get_by_id(guest_token, "guest")
    assert guest_data["id"] == "guest"

    # guest should be able to update their own password or email
    updated = await GulpAPIUser.user_update(
        guest_token,
        "guest",
        password="Password#1234!",
        email="mynewemail@email.com",
    )
    assert updated["email"] == "mynewemail@email.com"

    # set back to normal
    updated = await GulpAPIUser.user_update(
        guest_token,
        "guest",
        password="guest",
    )
    assert updated["email"] == "mynewemail@email.com"

    MutyLogger.get_instance().info(test_user.__name__ + " passed")


@pytest.mark.asyncio
async def test_session_expiration_update():
    admin_token = await GulpAPIUser.login("admin", "admin")
    assert admin_token

    # get session expiration time *prev*
    users = await GulpAPIUser.user_list(admin_token)
    prev_expiration_time: int = 0
    current_expiration_time: int = 0
    for u in users:
        if u["id"] != "admin":
            continue
        prev_expiration_time = u["session"]["time_expire"]
        break

    # get again, performing another operation causes session expiration time to be updated
    users = await GulpAPIUser.user_list(admin_token)
    for u in users:
        if u["id"] != "admin":
            continue
        current_expiration_time = u["session"]["time_expire"]
        break

    assert current_expiration_time > prev_expiration_time
    MutyLogger.get_instance().info(test_user.__name__ + " passed")


@pytest.mark.asyncio
async def test_user_vs_operations():
    # ingest some data
    from tests.ingest.test_ingest import test_win_evtx

    await test_win_evtx()

    # login admin, guest
    admin_token = await GulpAPIUser.login("admin", "admin")
    assert admin_token

    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token

    # test admin user creation
    test_user_id = "test_admin"
    test_user_password = "Test123!"
    try:
        await GulpAPIUser.user_delete(admin_token, test_user_id)
    except Exception:
        pass
    test_admin = await GulpAPIUser.user_create(
        admin_token,
        test_user_id,
        test_user_password,
        ["admin"],
        "testadmin@example.com",
    )
    assert test_admin.get("email") == "testadmin@example.com"
    assert test_admin.get("permission") == ["admin", "read"]

    # test guest user creation
    test_user_id = "test_user"
    test_user_password = "Test123!"
    try:
        await GulpAPIUser.user_delete(admin_token, test_user_id)
    except Exception:
        pass
    test_user = await GulpAPIUser.user_create(
        admin_token, test_user_id, test_user_password, ["read"], "testuser@example.com"
    )
    assert test_user.get("email") == "testuser@example.com"
    assert test_user.get("permission") == ["read"]

    # login test users
    test_admin_token = await GulpAPIUser.login("test_admin", "Test123!")
    assert test_admin_token
    test_user_token = await GulpAPIUser.login("test_user", "Test123!")
    assert test_user_token

    # query operations
    operations = await GulpAPIQuery.query_operations(admin_token)
    assert operations and len(operations) == 1

    operations = await GulpAPIQuery.query_operations(test_admin_token)
    assert operations and len(operations) == 1

    # test_user should not see any operations
    operations = await GulpAPIQuery.query_operations(test_user_token)
    assert not operations

    # grant user access to the test operation
    await GulpAPIObjectACL.object_add_granted_user(
        admin_token,
        obj_id=TEST_OPERATION_ID,
        obj_type="operation",
        user_id="test_user",
    )

    # test_user should not see any operations
    operations = await GulpAPIQuery.query_operations(test_user_token)
    assert operations and len(operations) == 1

    # remove and retest
    await GulpAPIObjectACL.object_remove_granted_user(
        admin_token,
        obj_id=TEST_OPERATION_ID,
        obj_type="operation",
        user_id="test_user",
    )
    operations = await GulpAPIQuery.query_operations(test_user_token)
    assert not operations

    # delete test users
    res = await GulpAPIUser.user_delete(admin_token, test_user["id"])
    assert res["id"] == test_user["id"]
    res = await GulpAPIUser.user_delete(admin_token, test_admin["id"])
    assert res["id"] == test_admin["id"]
    MutyLogger.get_instance().info(test_user_vs_operations.__name__ + " passed")
