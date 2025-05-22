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
from gulp.api.rest.client.common import GulpAPICommon, _test_ingest_ws_loop, _test_init
from gulp.api.rest.client.ingest import GulpAPIIngest
from gulp.api.rest.client.note import GulpAPINote
from gulp.api.rest.client.object_acl import GulpAPIObjectACL
from gulp.api.rest.client.operation import GulpAPIOperation
from gulp.api.rest.client.query import GulpAPIQuery
from gulp.api.rest.client.user import GulpAPIUser
from gulp.api.rest.test_values import (
    TEST_CONTEXT_ID,
    TEST_HOST,
    TEST_INDEX,
    TEST_OPERATION_ID,
    TEST_REQ_ID,
    TEST_WS_ID,
)
from gulp.api.ws_api import (
    GulpQueryDonePacket,
    GulpQueryGroupMatchPacket,
    GulpWsAuthPacket,
)
from gulp.structs import GulpMappingParameters


@pytest.mark.asyncio
async def test_sigma_single_new():
    GulpAPICommon.get_instance().init(
        host=TEST_HOST, ws_id=TEST_WS_ID, req_id=TEST_REQ_ID, index=TEST_INDEX
    )

    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token
    ingest_token = await GulpAPIUser.login("ingest", "ingest")
    assert ingest_token

    current_dir = os.path.dirname(os.path.realpath(__file__))
    file_path = os.path.join(
        # current_dir, "sigma/Microsoft-Windows-Windows Defender%4Operational.evtx"
        current_dir, "sigma/Microsoft-Windows-Sysmon%4Operational.evtx"
    )

    if os.environ.get("INGEST_DATA") == "1":
        # ingest some data
        from tests.ingest.test_ingest import test_win_evtx
        await test_win_evtx(file_path=file_path, skip_checks=True)
        await asyncio.sleep(8)

    # read sigma
    sigma_path = os.path.join(
        current_dir, "sigma/windows/create_stream_hash/create_stream_hash_susp_ip_domains.yml"
        #current_dir, "sigma/windows/process_creation/proc_creation_win_powershell_cmdline_special_characters.yml"
        #current_dir, "sigma/win_defender_threat.yml"
    )
    sigma = await muty.file.read_file_async(sigma_path)
    
    _, host = TEST_HOST.split("://")
    ws_url = f"ws://{host}/ws"
    test_completed = False

    async with websockets.connect(ws_url) as ws:
        # connect websocket
        p: GulpWsAuthPacket = GulpWsAuthPacket(token=guest_token, ws_id=TEST_WS_ID)
        await ws.send(p.model_dump_json(exclude_none=True))

        # receive responses
        try:
            while True:
                response = await ws.recv()
                data = json.loads(response)

                if data["type"] == "ws_connected":
                    # run test
                    await GulpAPIQuery.query_sigma(
                        guest_token,
                        TEST_OPERATION_ID,
                        sigmas=[
                            sigma.decode(),
                        ],
                    )
                elif data["type"] == "query_done":
                    # query done
                    q_done_packet: GulpQueryDonePacket = (
                        GulpQueryDonePacket.model_validate(data["data"])
                    )
                    MutyLogger.get_instance().debug(
                        "query done, name=%s", q_done_packet.name
                    )
                    test_completed = True
                    break

                # ws delay
                await asyncio.sleep(0.1)

        except websockets.exceptions.ConnectionClosed as ex:
            MutyLogger.get_instance().exception(ex)

    assert test_completed
    MutyLogger.get_instance().info(test_sigma_single_new.__name__ + " succeeded!")

    # # try again
    # await GulpAPIQuery.query_sigma(
    #     guest_token,
    #     TEST_OPERATION_ID,
    #     src_ids=["e027a5f1254f620bc62f62ea7fd628437c303ccc"],
    #     sigmas=[
    #         sigma.decode(),
    #     ],
    # )
