#!/usr/bin/env python3
import asyncio
import json
import pytest
from gulp.api.rest.test_values import (
    TEST_HOST,
    TEST_INDEX,
    TEST_REQ_ID,
    TEST_WS_ID,
)
from gulp.api.ws_api import GulpWsAuthPacket
from tests.common import GulpAPICommon
import muty.file
import os
import websockets


@pytest.mark.asyncio
async def test_windows():
    gulp_api = GulpAPICommon(host=TEST_HOST, req_id=TEST_REQ_ID, ws_id=TEST_WS_ID)
    current_dir = os.path.dirname(os.path.realpath(__file__))
    test_plugin = "win_evtx"

    # reset first
    await gulp_api.reset_gulp_collab()

    # login admin
    guest_token = await gulp_api.login("guest", "guest")
    assert guest_token

    # read sigma
    sigma_match_all = await muty.file.read_file_async(
        os.path.join(current_dir, "sigma/match_all.yaml")
    )

    _, host = TEST_HOST.split("://")
    ws_url = f"ws://{host}/ws"
    async with websockets.connect(ws_url) as ws:
        # connect websocket
        p: GulpWsAuthPacket = GulpWsAuthPacket(token=guest_token, ws_id=TEST_WS_ID)
        await ws.send(p.model_dump_json(exclude_none=True))

        # query
        await gulp_api.query_sigma(
            guest_token, test_plugin, TEST_INDEX, [sigma_match_all.decode()]
        )

        # receive responses
        try:
            while True:
                response = await ws.recv()
                data = json.loads(response)

                # Process response
                print(f"received: {data}")

                await asyncio.sleep(0.1)
        except websockets.exceptions.ConnectionClosed:
            print("WebSocket connection closed")


if __name__ == "__main__":
    asyncio.run(test_windows())
