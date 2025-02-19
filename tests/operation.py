import asyncio
import json

import pytest
import websockets
from muty.log import MutyLogger

from gulp.api.collab.structs import GulpCollabFilter, GulpCollabType
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.rest.test_values import (
    TEST_HOST,
    TEST_INDEX,
    TEST_OPERATION_ID,
    TEST_REQ_ID,
    TEST_WS_ID,
)
from gulp.api.ws_api import GulpQueryDonePacket, GulpWsAuthPacket
from tests.api.common import GulpAPICommon
from tests.api.db import GulpAPIDb
from tests.api.object_acl import GulpAPIObjectACL
from tests.api.operation import GulpAPIOperation
from tests.api.query import GulpAPIQuery
from tests.api.user import GulpAPIUser


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


@pytest.mark.asyncio
async def test():
    GulpAPICommon.get_instance().init(
        host=TEST_HOST, ws_id=TEST_WS_ID, req_id=TEST_REQ_ID, index=TEST_INDEX
    )

    # reset first
    await GulpAPIDb.reset_all_as_admin()

    # ingest some data
    from tests.ingest import test_csv_file_mapping

    await test_csv_file_mapping()

    # login users
    editor_token = await GulpAPIUser.login("editor", "editor")
    assert editor_token

    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token

    admin_token = await GulpAPIUser.login("admin", "admin")
    assert admin_token

    ingest_token = await GulpAPIUser.login("ingest", "ingest")
    assert ingest_token

    # guest user cannot create operation
    await GulpAPIOperation.operation_create(
        guest_token, "test_op", TEST_INDEX, expected_status=401
    )

    # ingest can create operation
    operation = await GulpAPIOperation.operation_create(
        ingest_token,
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

    # ingest can update operation
    updated = await GulpAPIOperation.operation_update(
        ingest_token,
        operation["id"],
        description="Updated description",
        operation_data={"hello": "world"},
    )
    assert updated.get("description") == "Updated description"
    assert updated.get("operation_data")["hello"] == "world"

    updated = await GulpAPIOperation.operation_update(
        ingest_token, operation["id"], operation_data={"hello": "1234", "abc": "def"}
    )
    assert updated.get("description") == "Updated description"
    assert updated.get("operation_data")["hello"] == "1234"
    assert updated.get("operation_data")["abc"] == "def"

    # guest cannot delete operation
    await GulpAPIOperation.operation_delete(
        guest_token, updated["id"], index=TEST_INDEX, expected_status=401
    )

    # list operations (the one created and the default one)
    operations = await GulpAPIOperation.operation_list(ingest_token)
    assert operations and len(operations) == 2
    operations = await GulpAPIOperation.operation_list(admin_token)
    assert operations and len(operations) == 2

    # for now, guest can see only the default operation
    operations = await GulpAPIOperation.operation_list(guest_token)
    assert len(operations) == 1

    # allow user to see operations
    await GulpAPIObjectACL.object_add_granted_user(
        token=ingest_token,
        object_id=updated["id"],
        object_type=GulpCollabType.OPERATION,
        user_id="guest",
    )

    # operation filter by name
    operations = await GulpAPIOperation.operation_list(
        guest_token, GulpCollabFilter(names=["test_op"])
    )
    assert operations and len(operations) == 1 and operations[0]["id"] == updated["id"]

    # editor cannot delete operation
    await GulpAPIOperation.operation_delete(
        editor_token, updated["id"], index=TEST_INDEX, expected_status=401
    )

    # ingest can delete operation
    d = await GulpAPIOperation.operation_delete(
        ingest_token, updated["id"], index=TEST_INDEX
    )
    assert d["id"] == updated["id"]

    # back to test operation only
    operations = await GulpAPIOperation.operation_list(guest_token)
    assert len(operations) == 1
    await GulpAPIObjectACL.object_add_granted_user(
        token=ingest_token,
        object_id=TEST_OPERATION_ID,
        object_type=GulpCollabType.OPERATION,
        user_id="guest",
    )
    operations = await GulpAPIOperation.operation_list(guest_token)
    assert (
        operations and len(operations) == 1 and operations[0]["id"] == TEST_OPERATION_ID
    )

    contexts = await GulpAPIOperation.context_list(guest_token, TEST_OPERATION_ID)
    assert contexts and len(contexts) == 1
    context_id = contexts[0]["id"]

    # list sources (test source + 1 ingested source)
    sources = await GulpAPIOperation.source_list(
        guest_token, TEST_OPERATION_ID, context_id=context_id
    )
    assert sources and len(sources) == 2

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
        index=TEST_INDEX,
    )
    # check data on opensearch (should be empty)
    res = await GulpAPIQuery.query_gulp(
        guest_token, TEST_INDEX, flt=GulpQueryFilter(operation_ids=[TEST_OPERATION_ID])
    )
    assert not res
    await _ws_loop()

    # verify that the source is deleted
    sources = await GulpAPIOperation.source_list(
        guest_token, TEST_OPERATION_ID, context_id=context_id
    )
    assert len(sources) == 1
    assert sources[0]["id"] != source_id

    # also delete operation (should delete the context and the remaining source, data is already deleted)
    await GulpAPIOperation.operation_delete(
        ingest_token, TEST_OPERATION_ID, index=TEST_INDEX
    )

    # check data on opensearch (should be empty)
    # res = await GulpAPIQuery.query_operations(ingest_token, TEST_INDEX)
    # assert not res

    # verify that the operation is deleted
    operations = await GulpAPIOperation.operation_list(guest_token)
    assert len(operations) == 0

    contexts = await GulpAPIOperation.context_list(guest_token, TEST_OPERATION_ID)
    assert len(contexts) == 0

    sources = await GulpAPIOperation.source_list(
        guest_token, TEST_OPERATION_ID, context_id=context_id
    )
    assert len(sources) == 0
    MutyLogger.get_instance().info("all OPERATION tests succeeded!")
