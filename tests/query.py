#!/usr/bin/env python3
import asyncio
import json
import pytest
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.opensearch.query import (
    GulpQueryParameters,
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
from tests.api.db import GulpAPIDb
from tests.api.user import GulpAPIUser
from tests.api.common import GulpAPICommon
from tests.api.query import GulpAPIQuery
import muty.file
import os
import websockets
from muty.log import MutyLogger


@pytest.mark.asyncio
async def test_win_evtx():
    """
    NOTE: assumes the test windows samples in ./samples/win_evtx are ingested

    and the gulp server running on http://localhost:8080
    """

    async def _test_sigma_multi(token: str, plugin: str):
        # read sigmas
        current_dir = os.path.dirname(os.path.realpath(__file__))

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
                        q_options = GulpQueryParameters()
                        q_options.sigma_parameters.plugin = plugin
                        q_options.group = "test group"
                        await GulpAPIQuery.query_sigma(
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

    async def _test_sigma_stored(token: str):
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
                        q_options = GulpQueryParameters(group="test group")
                        await GulpAPIQuery.query_stored(
                            token,
                            TEST_INDEX,
                            ["test_stored_sigma_1", "test_stored_sigma_2"],
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

    async def _test_raw_stored(token: str):
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
                        await GulpAPIQuery.query_stored(
                            token,
                            TEST_INDEX,
                            ["test_stored_raw"],
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
                        if q_done_packet.name == "raw stored query":
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

    async def _test_sigma_single(token: str, plugin: str):
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
                        q_options = GulpQueryParameters()
                        q_options.sigma_parameters.plugin = plugin
                        await GulpAPIQuery.query_sigma(
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

    async def _test_raw(token: str):
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
                            TEST_INDEX,
                            {
                                "query": {
                                    "query_string": {
                                        "query": "event.code: 4907 AND (gulp.operation_id: %s)"
                                        % (TEST_OPERATION_ID),
                                    }
                                }
                            },
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

    async def _test_gulp(token: str):
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
                            assert q_done_packet.total_hits == 98632
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

    async def _test_single_id(token: str):
        d = await GulpAPIQuery.query_single_id(
            token, "d5ccd5f5ddfae05aea6e7e4a385be5fb", TEST_INDEX
        )
        assert d["_id"] == "d5ccd5f5ddfae05aea6e7e4a385be5fb"
        MutyLogger.get_instance().info("test succeeded!")

    GulpAPICommon.get_instance().init(
        host=TEST_HOST, ws_id=TEST_WS_ID, req_id=TEST_REQ_ID, index=TEST_INDEX
    )

    # reset first
    await GulpAPIDb.reset_collab_as_admin()
    test_plugin = "win_evtx"

    # login guest
    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token

    # test different queries
    await _test_raw_stored(guest_token)
    await _test_sigma_stored(guest_token)
    await _test_gulp(guest_token)
    await _test_raw(guest_token)
    await _test_sigma_single(guest_token, test_plugin)
    await _test_sigma_multi(guest_token, test_plugin)
    await _test_single_id(guest_token)


@pytest.mark.asyncio
async def test_elasticsearch():
    async def _test_sigma_external_multi_ingest(
        token: str,
    ):
        # read sigmas
        current_dir = os.path.dirname(os.path.realpath(__file__))

        sigma_match_some = await muty.file.read_file_async(
            os.path.join(current_dir, "sigma/match_some.yaml")
        )

        sigma_match_some_more = await muty.file.read_file_async(
            os.path.join(current_dir, "sigma/match_some_more.yaml")
        )

        ingest_index = "test_ingest_external_idx"
        ingest_token = await GulpAPIUser.login("ingest", "ingest")
        assert ingest_token

        # ensure the target index do not exists
        try:
            await GulpAPIDb.opensearch_delete_index(ingest_token, ingest_index)
        except:
            pass

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
                        q_options.name = "test_external_elasticsearch"
                        q_options.sigma_parameters.plugin = "elasticsearch"
                        q_options.group = "test group"
                        q_options.external_parameters.plugin = "elasticsearch"
                        q_options.external_parameters.uri = "http://localhost:9200"
                        q_options.external_parameters.username = "admin"
                        q_options.external_parameters.password = "Gulp1234!"
                        q_options.external_parameters.plugin_params.model_extra[
                            "is_elasticsearch"
                        ] = False  # we are querying gulp ....
                        q_options.fields = "*"
                        q_options.external_parameters.operation_id = TEST_OPERATION_ID
                        q_options.external_parameters.context_name = (
                            "elasticsearch_context"
                        )

                        # set ingest index
                        q_options.external_parameters.ingest_index = ingest_index

                        # also use additional windows mapping
                        q_options.external_parameters.plugin_params.additional_mapping_files = [
                            ("windows.json", "windows")
                        ]
                        await GulpAPIQuery.query_sigma(
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

        try:
            assert test_completed
        finally:
            await GulpAPIDb.opensearch_delete_index(ingest_token, ingest_index)
        MutyLogger.get_instance().info("test succeeded!")

    GulpAPICommon.get_instance().init(
        host=TEST_HOST, ws_id=TEST_WS_ID, req_id=TEST_REQ_ID, index=TEST_INDEX
    )

    # reset first
    await GulpAPIDb.reset_collab_as_admin()

    # login guest
    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token

    # TODO: better test, this uses gulp's opensearch .... should work, but better to be sure
    await _test_sigma_external_multi_ingest(guest_token)


@pytest.mark.asyncio
async def test_paid_plugins():
    import importlib

    m = importlib.import_module("gulp-paid-plugins.tests.query")
    assert await m.test_all()
