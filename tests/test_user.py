import pytest
import pytest_asyncio
from muty.log import MutyLogger

from gulp.api.rest.client.common import _test_init
from gulp.api.rest.client.db import GulpAPIDb
from gulp.api.rest.client.operation import GulpAPIOperation
from gulp.api.rest.client.query import GulpAPIQuery
from gulp.api.rest.client.user import GulpAPIUser
from gulp.api.rest.test_values import TEST_OPERATION_ID


@pytest_asyncio.fixture(scope="function", autouse=True)
async def _setup():
    """
    this is called before any test, to initialize the environment
    """
    await _test_init(recreate=True)


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

    MutyLogger.get_instance().info(test_user.__name__ + " passed")


@pytest.mark.asyncio
async def test_user_vs_operations():
    # login admin, guest
    admin_token = await GulpAPIUser.login("admin", "admin")
    assert admin_token

    # reset collab full
    await GulpAPIDb.postgres_reset_collab(admin_token, full_reset=True)
    admin_token = await GulpAPIUser.login("admin", "admin")
    assert admin_token
    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token
    res = await GulpAPIOperation.operation_reset(admin_token, TEST_OPERATION_ID)
    assert res["id"] == TEST_OPERATION_ID

    # ingest some data
    from tests.ingest.test_ingest import test_win_evtx

    await test_win_evtx()

    # test admin user creation
    test_user_id = "test_admin"
    test_user_password = "Test123!"
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

    operations = await GulpAPIQuery.query_operations(test_user_token)
    assert not operations

    # delete test users
    res = await GulpAPIUser.user_delete(admin_token, test_user["id"])
    assert res["id"] == test_user["id"]
    res = await GulpAPIUser.user_delete(admin_token, test_admin["id"])
    assert res["id"] == test_admin["id"]
    MutyLogger.get_instance().info(test_user_vs_operations.__name__ + " passed")
