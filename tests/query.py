#!/usr/bin/env python3
import asyncio
import json
import pytest
from gulp.api.opensearch.query import (
    GulpQuery,
    GulpQueryAdditionalParameters,
    GulpQuerySigmaParameters,
)
from gulp.api.rest.test_values import (
    TEST_HOST,
    TEST_INDEX,
    TEST_REQ_ID,
    TEST_WS_ID,
)
from gulp.api.ws_api import GulpWsAuthPacket
from gulp.plugin import GulpPluginBase
from tests.common import GulpAPICommon
import muty.file
import os
import websockets


async def test_sigma(gulp_api: GulpAPICommon, token: str, plugin: str):
    # read sigma
    current_dir = os.path.dirname(os.path.realpath(__file__))
    sigma_match_all = await muty.file.read_file_async(
        os.path.join(current_dir, "sigma/match_all.yaml")
    )

    sigma_match_some = await muty.file.read_file_async(
        os.path.join(current_dir, "sigma/match_some.yaml")
    )

    sigma_match_some_more = await muty.file.read_file_async(
        os.path.join(current_dir, "sigma/match_some_more.yaml")
    )
    q_options = GulpQueryAdditionalParameters()
    q_options.sigma_parameters.plugin = plugin
    q_options.group = "test group"
    await gulp_api.query_sigma(
        token,
        TEST_INDEX,
        [
            sigma_match_some.decode(),
            # sigma_match_some_more.decode(),
            sigma_match_all.decode(),
        ],
        q_options,
    )


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

    # test sigma
    await test_sigma(gulp_api, guest_token, test_plugin)

    # sigma_match_combined = await muty.file.read_file_async(os.path.join(current_dir, "sigma/match_combined.yaml"))

    """
    # convert sigma rule/s to raw query
    mod = await GulpPluginBase.load("win_evtx")
    q_options = GulpQueryAdditionalParameters()
    if not q_options.sigma_parameters:
        q_options.sigma_parameters = GulpSigmaQueryParameters()
    sigmas = [
        sigma_match_some.decode(),
        sigma_match_some_more.decode(),
    ]

    q: list[GulpConvertedSigma] = mod.sigma_convert(
        sigmas,
        backend="opensearch",
        name=None,
        tags=None,
        pipeline=q_options.sigma_parameters.pipeline,
        output_format="dsl_lucene",
    )
    for qq in q:
        print(json.dumps(qq.q, indent=2))
        print("********")
    return
    """

    # query (98631 matches/notes)
    # await gulp_api.query_sigma(guest_token, test_plugin, TEST_INDEX, [sigma_match_all.decode()])

    # query (56 matches/notes)
    # await gulp_api.query_sigma(guest_token, test_plugin, TEST_INDEX, [sigma_match_some.decode()])

    # query (32 matches/notes)
    # await gulp_api.query_sigma(guest_token, test_plugin, TEST_INDEX, [sigma_match_some_more.decode()])

    # query (88 matches/notes)
    # await gulp_api.query_sigma(guest_token, test_plugin, TEST_INDEX, [sigma_match_combined.decode()])

    # TODO: websocket
    """
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
    """


if __name__ == "__main__":
    asyncio.run(test_windows())
