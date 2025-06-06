#!/usr/bin/env python3
import asyncio
import json
import os

import muty.file
import pytest
import pytest_asyncio
import websockets
from muty.log import MutyLogger

from gulp.api.collab.structs import COLLABTYPE_OPERATION, GulpCollabFilter
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.opensearch.query import GulpQueryParameters
from gulp.api.rest.client.common import _ensure_test_operation, _test_ingest_ws_loop
from gulp.api.rest.client.ingest import GulpAPIIngest
from gulp.api.rest.client.note import GulpAPINote
from gulp.api.rest.client.object_acl import GulpAPIObjectACL
from gulp.api.rest.client.operation import GulpAPIOperation
from gulp.api.rest.client.query import GulpAPIQuery
from gulp.api.rest.client.user import GulpAPIUser
from gulp.api.rest.test_values import (
    TEST_CONTEXT_ID,
    TEST_HOST,
    TEST_OPERATION_ID,
    TEST_WS_ID,
)
from gulp.api.ws_api import (
    GulpQueryDonePacket,
    GulpQueryGroupMatchPacket,
    GulpWsAuthPacket,
)
from gulp.structs import GulpMappingParameters

TEST_QUERY_RAW = {
    "query": {
        "query_string": {
            "query": "event.code: 4625 AND (gulp.operation_id: %s)"
            % (TEST_OPERATION_ID),
        }
    }
}


@pytest_asyncio.fixture(scope="function", autouse=True)
async def _setup():
    """
    this is called before any test, to initialize the environment
    """
    await _ensure_test_operation()


@pytest.mark.asyncio
async def test_queries():
    """
    NOTE: assumes the test windows samples in ./samples/win_evtx are ingested

    and the gulp server running on http://localhost:8080
    """

    async def _test_query_raw(token: str):
        _, host = TEST_HOST.split("://")
        ws_url = f"ws://{host}/ws"
        test_completed = False

        async with websockets.connect(ws_url) as ws:
            # connect websocket
            p: GulpWsAuthPacket = GulpWsAuthPacket(token=token, ws_id=TEST_WS_ID)
            await ws.send(p.model_dump_json(exclude_none=True))

            # receive responses
            try:
                while True:
                    response = await ws.recv()
                    data = json.loads(response)

                    if data["type"] == "ws_connected":
                        # run test
                        q_options = GulpQueryParameters()
                        q_options.name = "test_raw_query"
                        await GulpAPIQuery.query_raw(
                            token,
                            TEST_OPERATION_ID,
                            [TEST_QUERY_RAW],
                            q_options=q_options,
                        )
                    elif data["type"] == "query_done":
                        # query done
                        q_done_packet: GulpQueryDonePacket = (
                            GulpQueryDonePacket.model_validate(data["data"])
                        )
                        MutyLogger.get_instance().debug(
                            "query done, name=%s", q_done_packet.name
                        )
                        if q_done_packet.name == "test_raw_query":
                            assert q_done_packet.total_hits == 1
                            test_completed = True
                        else:
                            raise ValueError(
                                f"unexpected query name: {q_done_packet.name}"
                            )
                        break

                    # ws delay
                    await asyncio.sleep(0.1)

            except websockets.exceptions.ConnectionClosed as ex:
                MutyLogger.get_instance().exception(ex)

        assert test_completed
        MutyLogger.get_instance().info(_test_query_raw.__name__ + " succeeded!")

    async def _test_query_gulp(token: str):
        _, host = TEST_HOST.split("://")
        ws_url = f"ws://{host}/ws"
        test_completed = False

        async with websockets.connect(ws_url) as ws:
            # connect websocket
            p: GulpWsAuthPacket = GulpWsAuthPacket(token=token, ws_id=TEST_WS_ID)
            await ws.send(p.model_dump_json(exclude_none=True))

            # receive responses
            try:
                while True:
                    response = await ws.recv()
                    data = json.loads(response)

                    if data["type"] == "ws_connected":
                        # run test
                        q_options = GulpQueryParameters()
                        q_options.name = "test_gulp_query"
                        await GulpAPIQuery.query_gulp(
                            token,
                            TEST_OPERATION_ID,
                            flt=GulpQueryFilter(
                                operation_ids=[TEST_OPERATION_ID],
                                context_ids=[TEST_CONTEXT_ID],
                            ),
                            q_options=q_options,
                        )
                    elif data["type"] == "query_done":
                        # query done
                        q_done_packet: GulpQueryDonePacket = (
                            GulpQueryDonePacket.model_validate(data["data"])
                        )
                        MutyLogger.get_instance().debug(
                            "query done, name=%s", q_done_packet.name
                        )
                        if q_done_packet.name == "test_gulp_query":
                            assert q_done_packet.total_hits == 7
                            test_completed = True
                        else:
                            raise ValueError(
                                f"unexpected query name: {q_done_packet.name}"
                            )
                        break

                    # ws delay
                    await asyncio.sleep(0.1)

            except websockets.exceptions.ConnectionClosed as ex:
                MutyLogger.get_instance().exception(ex)

        assert test_completed
        MutyLogger.get_instance().info(_test_query_gulp.__name__ + " succeeded!")

    async def _test_query_single_id(token: str):
        """
        {
            "@timestamp": "2016-06-29T15:24:36.686000+00:00",
            "gulp.timestamp": 1467213876686000128,
            "gulp.operation_id": "test_operation",
            "gulp.context_id": "66d98ed55d92b6b7382ffc77df70eda37a6efaa1",
            "gulp.source_id": "64e7c3a4013ae243aa13151b5449aac884e36081",
            "log.file.path": "/Users/valerino/repos/gulp/tests/ingest/../../samples/win_evtx/Security_short_selected.evtx",
            "agent.type": "win_evtx",
            "event.original": "{\n  \"Event\": {\n    \"#attributes\": {\n      \"xmlns\": \"http://schemas.microsoft.com/win/2004/08/events/event\"\n    },\n    \"System\": {\n      \"Provider\": {\n        \"#attributes\": {\n          \"Name\": \"Microsoft-Windows-Security-Auditing\",\n          \"Guid\": \"54849625-5478-4994-A5BA-3E3B0328C30D\"\n        }\n      },\n      \"EventID\": 4611,\n      \"Version\": 0,\n      \"Level\": 0,\n      \"Task\": 12289,\n      \"Opcode\": 0,\n      \"Keywords\": \"0x8020000000000000\",\n      \"TimeCreated\": {\n        \"#attributes\": {\n          \"SystemTime\": \"2016-06-29T15:24:36.686000Z\"\n        }\n      },\n      \"EventRecordID\": 319457830,\n      \"Correlation\": null,\n      \"Execution\": {\n        \"#attributes\": {\n          \"ProcessID\": 768,\n          \"ThreadID\": 2764\n        }\n      },\n      \"Channel\": \"Security\",\n      \"Computer\": \"temporal\",\n      \"Security\": null\n    },\n    \"EventData\": {\n      \"SubjectUserSid\": \"S-1-5-18\",\n      \"SubjectUserName\": \"TEMPORAL$\",\n      \"SubjectDomainName\": \"WORKGROUP\",\n      \"SubjectLogonId\": \"0x3e7\",\n      \"LogonProcessName\": \"Winlogon\"\n    }\n  }\n}",
            "event.sequence": 1,
            "event.code": "4611",
            "gulp.event_code": 4611,
            "event.duration": 1,
            "gulp.unmapped.Guid": "54849625-5478-4994-A5BA-3E3B0328C30D",
            "gulp.unmapped.Task": 12289,
            "gulp.unmapped.Keywords": "0x8020000000000000",
            "gulp.unmapped.SystemTime": "2016-06-29T15:24:36.686000Z",
            "winlog.record_id": "319457830",
            "process.pid": 768,
            "process.thread.id": 2764,
            "winlog.channel": "Security",
            "winlog.computer_name": "temporal",
            "user.id": "S-1-5-18",
            "user.name": "TEMPORAL$",
            "user.domain": "WORKGROUP",
            "winlog.logon.id": "0x3e7",
            "gulp.unmapped.LogonProcessName": "Winlogon"
        }
        """
        target_id = "172511ceb3c6c0ef9f6cbf1a10fcffc3"
        d = await GulpAPIQuery.query_single_id(token, TEST_OPERATION_ID, target_id)
        assert d["_id"] == target_id
        MutyLogger.get_instance().info(_test_query_single_id.__name__ + " succeeded!")

    async def _test_query_operations():
        guest_token = await GulpAPIUser.login("guest", "guest")
        assert guest_token
        operations = await GulpAPIQuery.query_operations(guest_token)
        assert operations and len(operations) == 1

        # create another operation (with no guest grants), with the guest user cannot see it
        admin_token = await GulpAPIUser.login("admin", "admin")
        assert admin_token
        try:
            await GulpAPIOperation.operation_delete(admin_token, "new_operation")
        except:
            pass
        op = await GulpAPIOperation.operation_create(admin_token, "new_operation")
        assert op and op["id"] == "new_operation"

        # ingest some data in this operation
        current_dir = os.path.dirname(os.path.realpath(__file__))
        samples_dir = os.path.join(current_dir, "../../samples/win_evtx")
        file_path = os.path.join(samples_dir, "Security_short_selected.evtx")
        await GulpAPIIngest.ingest_file(
            token=admin_token,
            file_path=file_path,
            operation_id="new_operation",
            context_name="new_context",
            plugin="win_evtx",
            req_id="new_req_id",
        )
        await _test_ingest_ws_loop(check_ingested=7, check_processed=7)

        # check that the guest user cannot see the new operation
        operations = await GulpAPIQuery.query_operations(guest_token)
        assert operations and len(operations) == 1

        # grant guest user
        await GulpAPIObjectACL.object_add_granted_user(
            token=admin_token,
            obj_id="new_operation",
            obj_type=COLLABTYPE_OPERATION,
            user_id="guest",
        )

        # guest token can now see the operation
        operations = await GulpAPIQuery.query_operations(guest_token)
        assert operations and len(operations) == 2

        # delete the new operation
        await GulpAPIOperation.operation_delete(admin_token, "new_operation")

    # login
    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token
    ingest_token = await GulpAPIUser.login("ingest", "ingest")
    assert ingest_token

    # ingest some data
    from tests.ingest.test_ingest import test_win_evtx
    await test_win_evtx()

    # test different queries
    await _test_query_gulp(guest_token)
    await _test_query_raw(guest_token)
    await _test_query_single_id(guest_token)
    await _test_query_operations()


