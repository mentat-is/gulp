import pytest
from muty.log import MutyLogger
from gulp.api.collab.structs import GulpCollabFilter
from gulp.api.rest.test_values import TEST_HOST, TEST_INDEX, TEST_REQ_ID, TEST_WS_ID
from tests.api.common import GulpAPICommon
from tests.api.user import GulpAPIUser
from tests.api.operation import GulpAPIOperation
from tests.api.db import GulpAPIDb


@pytest.mark.asyncio
async def test():
    GulpAPICommon.get_instance().init(
        host=TEST_HOST, ws_id=TEST_WS_ID, req_id=TEST_REQ_ID, index=TEST_INDEX
    )

    # reset first
    await GulpAPIDb.reset_collab_as_admin()

    # login users
    editor_token = await GulpAPIUser.login("editor", "editor")
    assert editor_token

    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token

    admin_token = await GulpAPIUser.login("admin", "admin")
    assert admin_token

    # guest user cannot create operation
    await GulpAPIOperation.operation_create(
        guest_token, "test_op", TEST_INDEX, expected_status=401
    )

    # admin can create operation
    operation = await GulpAPIOperation.operation_create(
        admin_token,
        "test_op",
        TEST_INDEX,
        description="Test operation",
    )
    assert operation.get("name") == "test_op"
    assert operation.get("index") == TEST_INDEX
    assert operation.get("description") == "Test operation"

    # editor cannot update operation
    await GulpAPIOperation.operation_update(
        editor_token,
        operation["id"],
        description="Updated description",
        expected_status=401,
    )

    # admin can update operation
    updated = await GulpAPIOperation.operation_update(
        admin_token, operation["id"], description="Updated description"
    )
    assert updated.get("description") == "Updated description"

    # guest cannot delete operation
    await GulpAPIOperation.operation_delete(
        guest_token, updated["id"], index=TEST_INDEX, expected_status=401
    )

    # list operations (the one created and the default one)
    operations = await GulpAPIOperation.operation_list(guest_token)
    assert operations and len(operations) == 2

    # operation filter by name
    operations = await GulpAPIOperation.operation_list(
        guest_token, GulpCollabFilter(names=["test_op"])
    )
    assert operations and len(operations) == 1

    # editor cannot delete operation
    await GulpAPIOperation.operation_delete(
        editor_token, updated["id"], index=TEST_INDEX, expected_status=401
    )

    # admin can delete operation
    d = await GulpAPIOperation.operation_delete(
        admin_token, updated["id"], index=TEST_INDEX
    )
    assert d["id"] == updated["id"]

    # back to one operation
    operations = await GulpAPIOperation.operation_list(guest_token)
    assert operations and len(operations) == 1

    MutyLogger.get_instance().info("all OPERATION tests succeeded!")
