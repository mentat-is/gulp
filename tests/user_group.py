import pytest

from gulp.api.rest.client.common import GulpAPICommon
from gulp.api.rest.client.db import GulpAPIDb
from gulp.api.rest.client.user import GulpAPIUser
from gulp.api.rest.client.user_group import GulpAPIUserGroup
from gulp.api.rest.test_values import TEST_HOST, TEST_INDEX, TEST_REQ_ID, TEST_WS_ID


@pytest.mark.asyncio
async def test():
    """
    test user groups
    """
    # init api common
    GulpAPICommon.get_instance().init(
        host=TEST_HOST, ws_id=TEST_WS_ID, req_id=TEST_REQ_ID, index=TEST_INDEX
    )

    # reset first
    await GulpAPIDb.reset_collab_as_admin()

    # get tokens
    admin_token = await GulpAPIUser.login("admin", "admin")
    assert admin_token
    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token

    # create test group
    test_group = await GulpAPIUserGroup.usergroup_create(
        admin_token,
        name="test_group",
        permission=["read", "edit"],
        description="Test group",
        expected_status=200,
    )
    assert test_group["name"] == "test_group"
    assert "read" in test_group["permission"]
    assert "edit" in test_group["permission"]

    # guest cannot create group
    _ = await GulpAPIUserGroup.usergroup_create(
        guest_token, name="guest_group", permission=["read"], expected_status=401
    )

    # update group
    updated_group = await GulpAPIUserGroup.usergroup_update(
        admin_token,
        group_id=test_group["id"],
        permission=["read"],
        description="Updated test group",
        expected_status=200,
    )
    assert updated_group["description"] == "Updated test group"
    assert updated_group["permission"] == ["read"]

    # guest cannot update group
    _ = await GulpAPIUserGroup.usergroup_update(
        guest_token, group_id=test_group["id"], permission=["read"], expected_status=401
    )

    # create test user
    test_user = await GulpAPIUser.user_create(
        admin_token, "test_user", "TestPass123!", ["read"], email="test@localhost.com"
    )
    assert test_user["id"] == "test_user"

    # add user to group
    group_with_user = await GulpAPIUserGroup.usergroup_add_user(
        admin_token,
        user_id=test_user["id"],
        group_id=test_group["id"],
        expected_status=200,
    )
    assert test_user["id"] in [u["id"] for u in group_with_user["users"]]

    # guest cannot add user to group
    _ = await GulpAPIUserGroup.usergroup_add_user(
        guest_token,
        user_id=test_user["id"],
        group_id=test_group["id"],
        expected_status=401,
    )

    # get group by id
    group = await GulpAPIUserGroup.usergroup_get_by_id(
        admin_token, group_id=test_group["id"], expected_status=200
    )
    assert group["id"] == test_group["id"]

    # guest cannot get group
    _ = await GulpAPIUserGroup.usergroup_get_by_id(
        guest_token, group_id=test_group["id"], expected_status=401
    )

    # list groups
    groups = await GulpAPIUserGroup.usergroup_list(admin_token, expected_status=200)
    assert len(groups) > 0
    assert any(g["id"] == test_group["id"] for g in groups)

    # guest cannot list groups
    _ = await GulpAPIUserGroup.usergroup_list(guest_token, expected_status=401)

    # remove user from group
    group_without_user = await GulpAPIUserGroup.usergroup_remove_user(
        admin_token,
        user_id=test_user["id"],
        group_id=test_group["id"],
        expected_status=200,
    )
    assert test_user["id"] not in [u["id"] for u in group_without_user["users"]]

    # guest cannot remove user from group
    _ = await GulpAPIUserGroup.usergroup_remove_user(
        guest_token,
        user_id=test_user["id"],
        group_id=test_group["id"],
        expected_status=401,
    )

    # guest cannot delete group
    _ = await GulpAPIUserGroup.usergroup_delete(
        guest_token, group_id=test_group["id"], expected_status=401
    )

    # delete group
    _ = await GulpAPIUserGroup.usergroup_delete(
        admin_token, group_id=test_group["id"], expected_status=200
    )

    # verify group is deleted
    _ = await GulpAPIUserGroup.usergroup_get_by_id(
        admin_token, group_id=test_group["id"], expected_status=404
    )

    # cleanup test user
    await GulpAPIUser.user_delete(admin_token, test_user["id"])
