#!/usr/bin/env python3
import asyncio
import json
import pytest
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.opensearch.query import (
    GulpQueryAdditionalParameters,
)
from gulp.api.rest.test_values import (
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
from tests.common import GulpAPICommon
import muty.file
import os
import websockets
from muty.log import MutyLogger


# NOTE: assumes the test windows samples on an empty gulp index with
# ./test_scripts/test_ingest.py --path ./samples/win_evtx
# and the gulp server running on http://localhost:8080


@pytest.mark.asyncio
async def test_windows():
    async def _test_sigma_multi(gulp_api: GulpAPICommon, token: str, plugin: str):
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
                        q_options = GulpQueryAdditionalParameters()
                        q_options.sigma_parameters.plugin = plugin
                        q_options.group = "test group"
                        await gulp_api.query_sigma(
                            token,
                            TEST_INDEX,
                            [
                                sigma_match_some.decode(),
                                sigma_match_some_more.decode(),
                            ],
                            q_options,
                        )
                    elif data["type"] == "query_done":
                        # query done
                        q_done_packet: GulpQueryDonePacket = (
                            GulpQueryDonePacket.model_validate(data["data"])
                        )
                        MutyLogger.get_instance().debug(
                            "query done, name=%s", q_done_packet.name
                        )
                        if q_done_packet.name == "match_some_event_sequence_numbers":
                            assert q_done_packet.total_hits == 56
                        elif (
                            q_done_packet.name
                            == "match_some_more_event_sequence_numbers"
                        ):
                            assert q_done_packet.total_hits == 32
                        else:
                            raise ValueError(
                                f"unexpected query name: {q_done_packet.name}"
                            )
                    elif data["type"] == "query_group_match":
                        # query done
                        q_group_match_packet: GulpQueryGroupMatchPacket = (
                            GulpQueryGroupMatchPacket.model_validate(data["data"])
                        )
                        MutyLogger.get_instance().debug(
                            "query group match, name=%s", q_group_match_packet.name
                        )
                        if q_group_match_packet.name == "test group":
                            assert q_group_match_packet.total_hits == 88
                            test_completed = True
                        else:
                            raise ValueError(
                                f"unexpected query group name: {q_done_packet.name}"
                            )
                        break

                    # ws delay
                    await asyncio.sleep(0.1)

            except websockets.exceptions.ConnectionClosed:
                MutyLogger.get_instance().warning("WebSocket connection closed")

        assert test_completed
        MutyLogger.get_instance().info("test succeeded!")

    async def _test_sigma_single(gulp_api: GulpAPICommon, token: str, plugin: str):
        # read sigma
        current_dir = os.path.dirname(os.path.realpath(__file__))
        sigma_match_some = await muty.file.read_file_async(
            os.path.join(current_dir, "sigma/match_some.yaml")
        )

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
                        q_options = GulpQueryAdditionalParameters()
                        q_options.sigma_parameters.plugin = plugin
                        await gulp_api.query_sigma(
                            token,
                            TEST_INDEX,
                            [
                                sigma_match_some.decode(),
                            ],
                            q_options,
                        )
                    elif data["type"] == "query_done":
                        # query done
                        q_done_packet: GulpQueryDonePacket = (
                            GulpQueryDonePacket.model_validate(data["data"])
                        )
                        MutyLogger.get_instance().debug(
                            "query done, name=%s", q_done_packet.name
                        )
                        if q_done_packet.name == "match_some_event_sequence_numbers":
                            assert q_done_packet.total_hits == 56
                            test_completed = True
                        else:
                            raise ValueError(
                                f"unexpected query name: {q_done_packet.name}"
                            )
                        break

                    # ws delay
                    await asyncio.sleep(0.1)

            except websockets.exceptions.ConnectionClosed:
                MutyLogger.get_instance().warning("WebSocket connection closed")

        assert test_completed
        MutyLogger.get_instance().info("test succeeded!")

    async def _test_raw(gulp_api: GulpAPICommon, token: str, plugin: str):
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
                        q_options = GulpQueryAdditionalParameters()
                        q_options.name = "test_raw_query"
                        await gulp_api.query_raw(
                            token,
                            TEST_INDEX,
                            {
                                "query": {
                                    "match": {
                                        "event.code": "4907",
                                    },
                                },
                            },
                            flt=GulpQueryFilter(operation_ids=[TEST_OPERATION_ID]),
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
                            assert q_done_packet.total_hits == 6709
                            test_completed = True
                        else:
                            raise ValueError(
                                f"unexpected query name: {q_done_packet.name}"
                            )
                        break

                    # ws delay
                    await asyncio.sleep(0.1)

            except websockets.exceptions.ConnectionClosed:
                MutyLogger.get_instance().warning("WebSocket connection closed")

        assert test_completed
        MutyLogger.get_instance().info("test succeeded!")

    async def _test_raw_external_pre(gulp_api: GulpAPICommon, token: str, plugin: str):
        # run test
        q_options = GulpQueryAdditionalParameters()
        q_options.name = "test_raw_query_splunk"
        q_options.external_parameters.plugin = "splunk"
        q_options.external_parameters.uri = "http://localhost:8089"
        q_options.external_parameters.username = "admin"
        q_options.external_parameters.password = "Valerino74!"
        q_options.external_parameters.operation_id = TEST_OPERATION_ID
        q_options.external_parameters.context_name = "splunk_context"
        q_options.external_parameters.ingest_index = "test_idx"

        await gulp_api.query_raw(
            token,
            "_audit",
            'index="_audit"',
            flt=GulpQueryFilter(source=["audittrail"]),
            q_options=q_options,
        )

    async def _test_raw_external(gulp_api: GulpAPICommon, token: str, plugin: str):
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
                        q_options = GulpQueryAdditionalParameters()
                        q_options.name = "test_raw_query_splunk"
                        q_options.external_parameters.plugin = "splunk"
                        q_options.external_parameters.uri = "http://10.10.10.10:8089"
                        q_options.external_parameters.username = "admin"
                        q_options.external_parameters.password = "Valerino74!"

                        await gulp_api.query_raw(
                            token,
                            "_audit",
                            'index="_audit"',
                            flt=GulpQueryFilter(host=["383a1a58b2ea"]),
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
                            assert q_done_packet.total_hits == 6709
                            test_completed = True
                        else:
                            raise ValueError(
                                f"unexpected query name: {q_done_packet.name}"
                            )
                        break

                    # ws delay
                    await asyncio.sleep(0.1)

            except websockets.exceptions.ConnectionClosed:
                MutyLogger.get_instance().warning("WebSocket connection closed")

        assert test_completed
        MutyLogger.get_instance().info("test succeeded!")

    async def _test_gulp(gulp_api: GulpAPICommon, token: str, plugin: str):
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
                        q_options = GulpQueryAdditionalParameters()
                        q_options.name = "test_gulp_query"
                        await gulp_api.query_gulp(
                            token,
                            TEST_INDEX,
                            flt=GulpQueryFilter(operation_ids=[TEST_OPERATION_ID]),
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
                            assert q_done_packet.total_hits == 98631
                            test_completed = True
                        else:
                            raise ValueError(
                                f"unexpected query name: {q_done_packet.name}"
                            )
                        break

                    # ws delay
                    await asyncio.sleep(0.1)

            except websockets.exceptions.ConnectionClosed:
                MutyLogger.get_instance().warning("WebSocket connection closed")

        assert test_completed
        MutyLogger.get_instance().info("test succeeded!")

    # common stuff
    gulp_api = GulpAPICommon(host=TEST_HOST, req_id=TEST_REQ_ID, ws_id=TEST_WS_ID)
    test_plugin = "win_evtx"

    # reset first
    await gulp_api.reset_gulp_collab()

    # login admin
    guest_token = await gulp_api.login("guest", "guest")
    assert guest_token

    # test different queries
    await _test_raw_external_pre(gulp_api, guest_token, test_plugin)
    return

    await _test_gulp(gulp_api, guest_token, test_plugin)
    await _test_raw(gulp_api, guest_token, test_plugin)
    await _test_sigma_single(gulp_api, guest_token, test_plugin)
    await _test_sigma_multi(gulp_api, guest_token, test_plugin)


if __name__ == "__main__":
    asyncio.run(test_windows())
