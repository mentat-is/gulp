import asyncio
import json

import pytest
import pytest_asyncio
import websockets
from muty.log import MutyLogger

from gulp.api.collab.structs import GulpCollabFilter, GulpCollabType
from gulp.api.collab.user_group import ADMINISTRATORS_GROUP_ID
from gulp.api.rest.client.common import GulpAPICommon
from gulp.api.rest.client.db import GulpAPIDb
from gulp.api.rest.client.object_acl import GulpAPIObjectACL
from gulp.api.rest.client.operation import GulpAPIOperation
from gulp.api.rest.client.query import GulpAPIQuery
from gulp.api.rest.client.user import GulpAPIUser
from gulp.api.rest.client.user_group import GulpAPIUserGroup
from gulp.api.rest.test_values import (
    TEST_HOST,
    TEST_INDEX,
    TEST_OPERATION_ID,
    TEST_REQ_ID,
    TEST_WS_ID,
)
from gulp.api.ws_api import GulpQueryDonePacket, GulpWsAuthPacket


async def _ws_loop():
    _, host = TEST_HOST.split("://")
    ws_url = f"ws://{host}/ws"
    test_completed = False

    async with websockets.connect(ws_url) as ws:
        # connect websocket
        p: GulpWsAuthPacket = GulpWsAuthPacket(token="monitor", ws_id=TEST_WS_ID)
        await ws.send(p.model_dump_json(exclude_none=True))

        # receive responses
        try:
            while True:
                response = await ws.recv()
                data = json.loads(response)
                if data["type"] == "ws_connected":
                    # ws connected
                    MutyLogger.get_instance().debug("ws connected: %s", data)

                elif data["type"] == "query_done":
                    # query done
                    q_done_packet: GulpQueryDonePacket = (
                        GulpQueryDonePacket.model_validate(data["data"])
                    )
                    if q_done_packet.total_hits == 0:
                        test_completed = True
                    else:
                        raise ValueError(
                            f"unexpected total hits: {
                                q_done_packet.total_hits}"
                        )
                    break
                # ws delay
                await asyncio.sleep(0.1)

        except websockets.exceptions.ConnectionClosed:
            MutyLogger.get_instance().warning("WebSocket connection closed")

    assert test_completed


@pytest_asyncio.fixture(scope="function", autouse=True)
async def _setup():
    """
    this is called before any test, to initialize the environment
    """
    GulpAPICommon.get_instance().init(
        host=TEST_HOST, ws_id=TEST_WS_ID, req_id=TEST_REQ_ID, index=TEST_INDEX
    )


@pytest.mark.asyncio
async def test_operation_api():
    """
    this tests operation, acl, user groups
    """
    admin_token = await GulpAPIUser.login("admin", "admin")
    assert admin_token

    # clear indexes
    indexes = await GulpAPIDb.opensearch_list_index(admin_token)
    for l in indexes:
        await GulpAPIDb.opensearch_delete_index(
            admin_token, l["name"], delete_operation=False
        )
    indexes = await GulpAPIDb.opensearch_list_index(admin_token)
    assert not indexes

    # reset whole admin and collab
    await GulpAPIDb.reset_all_as_admin()

    # login users
    editor_token = await GulpAPIUser.login("editor", "editor")
    assert editor_token

    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token

    admin_token = await GulpAPIUser.login("admin", "admin")
    assert admin_token

    ingest_token = await GulpAPIUser.login("ingest", "ingest")
    assert ingest_token

    # recreate test operation
    await GulpAPIOperation.operation_delete(admin_token, TEST_OPERATION_ID)
    await GulpAPIOperation.operation_create(
        admin_token, TEST_OPERATION_ID, set_default_grants=True
    )

    # ingest some data
    from tests.ingest.test_ingest import test_csv_file_mapping

    await test_csv_file_mapping()

    # guest user cannot create operation
    await GulpAPIOperation.operation_create(
        guest_token, TEST_OPERATION_ID, set_default_grants=True, expected_status=401
    )

    # editor cannot update operation
    await GulpAPIOperation.operation_update(
        editor_token,
        TEST_OPERATION_ID,
        description="Updated description",
        expected_status=401,
    )

    # ingest can update operation
    updated = await GulpAPIOperation.operation_update(
        ingest_token,
        TEST_OPERATION_ID,
        description="Updated description",
        operation_data={"hello": "world"},
    )
    assert updated.get("description") == "Updated description"
    assert updated.get("operation_data")["hello"] == "world"

    updated = await GulpAPIOperation.operation_update(
        ingest_token, TEST_OPERATION_ID, operation_data={"hello": "1234", "abc": "def"}
    )
    assert updated.get("description") == "Updated description"
    assert updated.get("operation_data")["hello"] == "1234"
    assert updated.get("operation_data")["abc"] == "def"

    # guest cannot delete operation
    await GulpAPIOperation.operation_delete(
        guest_token, updated["id"], expected_status=401
    )

    # create new operation with just owner's grants
    new_operation_id = "new_operation"
    new_operation = await GulpAPIOperation.operation_create(
        admin_token, "new_operation"
    )
    assert new_operation.get("name") == new_operation_id
    assert new_operation.get("index") == new_operation_id
    assert new_operation.get("id") == new_operation_id

    # list operations (ingest can see only one operation)
    operations = await GulpAPIOperation.operation_list(ingest_token)
    assert operations and len(operations) == 1
    assert operations[0]["id"] == TEST_OPERATION_ID

    # admin can also see the new operation
    operations = await GulpAPIOperation.operation_list(admin_token)
    for o in operations:
        assert o["id"] in [TEST_OPERATION_ID, new_operation_id]
    assert operations and len(operations) == 2

    # allow ingest to see the new operation (ingest cannot do it)
    await GulpAPIObjectACL.object_add_granted_user(
        token=ingest_token,
        obj_id=new_operation_id,
        obj_type=GulpCollabType.OPERATION,
        user_id="ingest",
        expected_status=401,
    )

    # allow ingest to see the new operation (admin can)
    await GulpAPIObjectACL.object_add_granted_user(
        token=admin_token,
        obj_id=new_operation_id,
        obj_type=GulpCollabType.OPERATION,
        user_id="ingest",
    )

    # guest can still see the test operation
    operations = await GulpAPIOperation.operation_list(
        guest_token, GulpCollabFilter(names=[TEST_OPERATION_ID])
    )
    assert operations and len(operations) == 1 and operations[0]["id"] == updated["id"]

    # ingest can also see the new operation
    operations = await GulpAPIOperation.operation_list(ingest_token)
    for o in operations:
        assert o["id"] in [TEST_OPERATION_ID, new_operation_id]
    assert operations and len(operations) == 2

    # now no more
    await GulpAPIObjectACL.object_remove_granted_user(
        token=ingest_token,
        obj_id=new_operation_id,
        obj_type=GulpCollabType.OPERATION,
        user_id="ingest",
        expected_status=401,
    )
    await GulpAPIObjectACL.object_remove_granted_user(
        token=admin_token,
        obj_id=new_operation_id,
        obj_type=GulpCollabType.OPERATION,
        user_id="ingest",
    )
    operations = await GulpAPIOperation.operation_list(ingest_token)
    assert operations and len(operations) == 1

    # add ingest to administrators group
    await GulpAPIUserGroup.usergroup_add_user(
        admin_token, "ingest", ADMINISTRATORS_GROUP_ID
    )

    # now ingest can see the new operation again

    operations = await GulpAPIOperation.operation_list(ingest_token)
    assert operations and len(operations) == 2

    # list contexts
    contexts = await GulpAPIOperation.context_list(guest_token, TEST_OPERATION_ID)
    assert contexts and len(contexts) == 1
    context_id = contexts[0]["id"]

    # list sources
    sources = await GulpAPIOperation.source_list(
        guest_token, TEST_OPERATION_ID, context_id=context_id
    )
    assert sources and len(sources) == 1

    for s in sources:
        n: str = s["name"]
        if n.endswith(".csv"):
            source_id = s["id"]
            break

    # delete source with data
    d = await GulpAPIOperation.source_delete(
        ingest_token,
        TEST_OPERATION_ID,
        context_id,
        source_id,
    )
    # check data on opensearch (should be empty)
    res = await GulpAPIQuery.query_gulp(guest_token, TEST_OPERATION_ID)
    assert not res
    await _ws_loop()

    # verify that the source is deleted
    sources = await GulpAPIOperation.source_list(
        guest_token, TEST_OPERATION_ID, context_id=context_id
    )
    assert len(sources) == 0

    # also delete operation (should delete the context)
    await GulpAPIOperation.operation_delete(ingest_token, TEST_OPERATION_ID)

    # verify that the operation is deleted
    operations = await GulpAPIOperation.operation_list(guest_token)
    assert len(operations) == 0

    operations = await GulpAPIOperation.operation_list(ingest_token)
    # ingest can still see new operation
    assert len(operations) == 1

    # also delete the new operation
    await GulpAPIOperation.operation_delete(ingest_token, new_operation_id)
    operations = await GulpAPIOperation.operation_list(ingest_token)
    assert len(operations) == 0

    contexts = await GulpAPIOperation.context_list(ingest_token, TEST_OPERATION_ID)
    assert len(contexts) == 0

    sources = await GulpAPIOperation.source_list(
        ingest_token, TEST_OPERATION_ID, context_id=context_id
    )
    assert len(sources) == 0
    MutyLogger.get_instance().info("all OPERATION tests succeeded!")
