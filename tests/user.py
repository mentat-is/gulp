import pytest
from muty.log import MutyLogger
from gulp.api.rest.test_values import TEST_HOST, TEST_REQ_ID, TEST_WS_ID
from tests.common import GulpAPICommon


@pytest.mark.asyncio
async def test():
    gulp_api = GulpAPICommon(host=TEST_HOST, req_id=TEST_REQ_ID, ws_id=TEST_WS_ID)

    # reset first
    await gulp_api.reset_gulp_collab()

    # login admin, guest
    admin_token = await gulp_api.login("admin", "admin")
    assert admin_token

    guest_token = await gulp_api.login("guest", "guest")
    assert guest_token

    # test user creation
    test_user_id = "test_user"
    test_user_password = "Test123!"
    user = await gulp_api.user_create(
        admin_token, test_user_id, test_user_password, ["read"], "test@example.com"
    )
    assert user.get("email") == "test@example.com"
    assert user.get("permission") == ["read"]

    # test user listing
    users = await gulp_api.user_list(admin_token)
    assert users and len(users) >= 1

    # test user update
    updated = await gulp_api.user_update(
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
    res = await gulp_api.user_delete(admin_token, test_user_id)
    assert res == test_user_id

    # verify deletion
    _ = await gulp_api.user_get(admin_token, test_user_id, expected_status=404)

    # logout admin
    t = await gulp_api.logout(admin_token)
    assert t == admin_token

    # admin should be logget out
    await gulp_api.user_list(admin_token, expected_status=404)

    # guest tests now!

    # guest cannot create, list, delete users
    await gulp_api.user_create(
        guest_token, "new_user", "Password#1234!", ["read"], expected_status=401
    )
    await gulp_api.user_list(guest_token, expected_status=401)
    await gulp_api.user_delete(guest_token, "editor", expected_status=401)

    # guest should not be able to update its own permission
    await gulp_api.user_update(
        guest_token, "guest", permission=["read", "edit"], expected_status=401
    )

    # guest should not be able to update other users
    await gulp_api.user_update(
        guest_token, "editor", password="Hacked#1234!", expected_status=401
    )

    # guest should be able to get their own details
    guest_data = await gulp_api.user_get(guest_token, "guest")
    assert guest_data["id"] == "guest"

    # guest should be able to update their own password or email
    updated = await gulp_api.user_update(
        guest_token,
        "guest",
        password="Password#1234!",
        email="mynewemail@email.com",
    )
    assert updated["email"] == "mynewemail@email.com"

    MutyLogger.get_instance().info("all USER tests succeeded!")
