#!/usr/bin/env python3
import asyncio
import json
import os

import muty.file
import pytest
import pytest_asyncio
import websockets
from muty.log import MutyLogger

from gulp.api.collab.structs import GulpCollabFilter
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.opensearch.query import GulpQueryParameters
from gulp.api.rest.client.common import _test_init
from gulp.api.rest.client.note import GulpAPINote
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
    await _test_init()


@pytest.mark.asyncio
async def test_queries():
    """
    NOTE: assumes the test windows samples in ./samples/win_evtx are ingested

    and the gulp server running on http://localhost:8080
    """

    async def _test_sigma_group(token: str):
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
                        q_options.group = "test group"
                        await GulpAPIQuery.query_sigma(
                            token,
                            TEST_OPERATION_ID,
                            "win_evtx",
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
                            assert q_done_packet.total_hits == 3
                        elif (
                            q_done_packet.name
                            == "match_some_more_event_sequence_numbers"
                        ):
                            assert q_done_packet.total_hits == 2
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
                            assert q_group_match_packet.total_hits == 5

                            # check notes creation on postgres, there should be 5 nots with "test_group" in the tags
                            flt = GulpCollabFilter(tags=["test group"])
                            notes = await GulpAPINote.note_list(token, flt)
                            MutyLogger.get_instance().debug(
                                "notes: %s", json.dumps(notes, indent=2)
                            )
                            assert len(notes) == 5
                            test_completed = True
                        else:
                            raise ValueError(
                                f"unexpected query group name: {
                                    q_done_packet.name}"
                            )
                        break

                    # ws delay
                    await asyncio.sleep(0.1)

            except websockets.exceptions.ConnectionClosed as ex:
                MutyLogger.get_instance().exception(ex)

        assert test_completed
        MutyLogger.get_instance().info(_test_sigma_group.__name__ + " succeeded!")

    async def _test_sigma_single(token: str):
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
                        await GulpAPIQuery.query_sigma(
                            token,
                            TEST_OPERATION_ID,
                            plugin="win_evtx",
                            sigmas=[
                                sigma_match_some.decode(),
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
                        if q_done_packet.name == "match_some_event_sequence_numbers":
                            assert q_done_packet.total_hits == 3
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
        MutyLogger.get_instance().info(_test_sigma_single.__name__ + " succeeded!")

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
                            [
                                TEST_QUERY_RAW
                            ],
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
        target_id = "7f85775e0dfb388104693e3b938f0ef3"
        d = await GulpAPIQuery.query_single_id(token, TEST_OPERATION_ID, target_id)
        assert d["_id"] == target_id
        MutyLogger.get_instance().info(_test_query_single_id.__name__ + " succeeded!")

    # login
    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token
    ingest_token = await GulpAPIUser.login("ingest", "ingest")
    assert ingest_token

    # ingest some data
    from tests.ingest.test_ingest import test_win_evtx

    await test_win_evtx()

    # test different queries
    await _test_sigma_group(guest_token)
    await _test_sigma_single(guest_token)
    await _test_query_gulp(guest_token)
    await _test_query_raw(guest_token)
    await _test_query_single_id(guest_token)


# @pytest.mark.asyncio
# async def test_elasticsearch():
#     async def _test_sigma_external_multi_ingest():
#         # read sigmas
#         current_dir = os.path.dirname(os.path.realpath(__file__))

#         sigma_match_some = await muty.file.read_file_async(
#             os.path.join(current_dir, "sigma/match_some.yaml")
#         )

#         sigma_match_some_more = await muty.file.read_file_async(
#             os.path.join(current_dir, "sigma/match_some_more.yaml")
#         )

#         ingest_index = "test_ingest_external_idx"
#         ingest_token = await GulpAPIUser.login("ingest", "ingest")
#         assert ingest_token

#         # ensure the target index do not exists
#         try:
#             await GulpAPIDb.opensearch_delete_index(ingest_token, ingest_index)
#         except:
#             pass

#         token = ingest_token
#         _, host = TEST_HOST.split("://")
#         ws_url = f"ws://{host}/ws"
#         test_completed = False

#         async with websockets.connect(ws_url) as ws:
#             # connect websocket
#             p: GulpWsAuthPacket = GulpWsAuthPacket(token=token, ws_id=TEST_WS_ID)
#             await ws.send(p.model_dump_json(exclude_none=True))

#             # receive responses
#             try:
#                 while True:
#                     response = await ws.recv()
#                     data = json.loads(response)

#                     if data["type"] == "ws_connected":
#                         # run test
#                         q_options = GulpQueryParameters()
#                         q_options.name = "test_external_elasticsearch"
#                         q_options.sigma_parameters.plugin = "elasticsearch"
#                         q_options.group = "test group"
#                         q_options.external_parameters.plugin = "elasticsearch"
#                         q_options.external_parameters.uri = "http://localhost:9200"
#                         q_options.external_parameters.username = "admin"
#                         q_options.external_parameters.password = "Gulp1234!"
#                         q_options.external_parameters.plugin_params.custom_parameters[
#                             "is_elasticsearch"
#                         ] = False  # we are querying gulp ....
#                         q_options.fields = "*"
#                         q_options.external_parameters.operation_id = TEST_OPERATION_ID
#                         q_options.external_parameters.context_name = (
#                             "elasticsearch_context"
#                         )

#                         # set ingest index
#                         q_options.external_parameters.ingest_index = ingest_index

#                         # also use additional windows mapping
#                         q_options.external_parameters.plugin_params.additional_mapping_files = [
#                             ("windows.json", "windows")
#                         ]
#                         await GulpAPIQuery.query_sigma(
#                             token,
#                             TEST_INDEX,
#                             [
#                                 sigma_match_some.decode(),
#                                 sigma_match_some_more.decode(),
#                             ],
#                             q_options,
#                         )
#                     elif data["type"] == "query_done":
#                         # query done
#                         q_done_packet: GulpQueryDonePacket = (
#                             GulpQueryDonePacket.model_validate(data["data"])
#                         )
#                         MutyLogger.get_instance().debug(
#                             "query done, name=%s", q_done_packet.name
#                         )
#                         if q_done_packet.name == "match_some_event_sequence_numbers":
#                             assert q_done_packet.total_hits == 56
#                         elif (
#                             q_done_packet.name
#                             == "match_some_more_event_sequence_numbers"
#                         ):
#                             assert q_done_packet.total_hits == 32
#                         else:
#                             raise ValueError(
#                                 f"unexpected query name: {q_done_packet.name}"
#                             )
#                     elif data["type"] == "query_group_match":
#                         # query done
#                         q_group_match_packet: GulpQueryGroupMatchPacket = (
#                             GulpQueryGroupMatchPacket.model_validate(data["data"])
#                         )
#                         MutyLogger.get_instance().debug(
#                             "query group match, name=%s", q_group_match_packet.name
#                         )
#                         if q_group_match_packet.name == "test group":
#                             assert q_group_match_packet.total_hits == 88
#                             test_completed = True
#                         else:
#                             raise ValueError(
#                                 f"unexpected query group name: {
#                                     q_done_packet.name}"
#                             )
#                         break

#                     # ws delay
#                     await asyncio.sleep(0.1)

#             except websockets.exceptions.ConnectionClosed as ex:
#                 MutyLogger.get_instance().exception(ex)

#         try:
#             assert test_completed
#         finally:
#             await GulpAPIDb.opensearch_delete_index(ingest_token, ingest_index)
#         MutyLogger.get_instance().info("test succeeded!")

#     GulpAPICommon.get_instance().init(
#         host=TEST_HOST, ws_id=TEST_WS_ID, req_id=TEST_REQ_ID, index=TEST_INDEX
#     )

#     # reset first
#     await GulpAPIDb.reset_collab_as_admin()

#     # TODO: better test, this uses gulp's opensearch .... should work, but better to be sure
#     await _test_sigma_external_multi_ingest()
