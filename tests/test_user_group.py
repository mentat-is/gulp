import pytest
import pytest_asyncio
from muty.log import MutyLogger

from gulp.api.rest.client.common import _ensure_test_operation
from gulp.api.rest.client.user import GulpAPIUser
from gulp.api.rest.client.user_group import GulpAPIUserGroup


@pytest_asyncio.fixture(scope="function", autouse=True)
async def _setup():
    await _ensure_test_operation()


@pytest.mark.asyncio
async def test_user_group():
    """
    test user groups
    """
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
    _ = await GulpAPIUser.user_get_by_id(
        admin_token, test_user["id"], expected_status=404
    )

    MutyLogger.get_instance().debug(test_user_group.__name__ + " passed")
