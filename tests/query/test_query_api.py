#!/usr/bin/env python3
import asyncio
import json
import os
import time

import muty.file
import pytest
import pytest_asyncio
import websockets
from muty.log import MutyLogger

from gulp.api.collab.structs import GulpCollabFilter, GulpCollabType
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.opensearch.query import GulpQueryParameters
from gulp.api.rest.client.common import GulpAPICommon, _test_ingest_ws_loop, _test_init
from gulp.api.rest.client.db import GulpAPIDb
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
from gulp.structs import GulpMappingParameters, GulpPluginParameters

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
    # GulpAPICommon.get_instance().init(
    #     host=TEST_HOST, ws_id=TEST_WS_ID, req_id=TEST_REQ_ID, index=TEST_INDEX
    # )
    # admin_token = await GulpAPIUser.login("admin", "admin")
    # assert admin_token
    # await GulpAPIDb.postgres_reset_collab(admin_token, full_reset=False)


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
                            [
                                sigma_match_some.decode(),
                                sigma_match_some_more.decode(),
                            ],
                            mapping_parameters=GulpMappingParameters(
                                mapping_file="windows.json"
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
                            sigmas=[
                                sigma_match_some.decode(),
                            ],
                            mapping_parameters=GulpMappingParameters(
                                mapping_file="windows.json"
                            ),
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
            obj_type=GulpCollabType.OPERATION,
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
    await _test_sigma_group(guest_token)
    await _test_sigma_single(guest_token)
    await _test_query_gulp(guest_token)
    await _test_query_raw(guest_token)
    await _test_query_single_id(guest_token)
    await _test_query_operations()


@pytest.mark.asyncio
async def test_sigma_zip():
    """test sigma rule execution from zip file containing multiple rules
    
    validates proper processing of bulk sigma rules through websocket connection
    with enhanced timeout handling and connection stability measures
    """
    
    async def _test_sigma_zip_internal(token: str) -> None:
        """internal test implementation for sigma zip processing
        
        Args:
            token (str): authentication token for api access
            
        Raises:
            AssertionError: if test completion criteria not met
            TimeoutError: if processing stalls beyond configured thresholds
        """
        current_dir: str = os.path.dirname(os.path.realpath(__file__))
        sigma_zip_path: str = os.path.join(current_dir, "sigma/windows.zip")
        _, host = TEST_HOST.split("://")
        ws_url: str = f"ws://{host}/ws"
        test_completed: bool = False

        async def _ping_loop(websocket: websockets.WebSocketClientProtocol) -> None:
            """maintain websocket connection with regular pings
            
            Args:
                websocket: active websocket connection
                
            Note:
                runs until cancelled or connection error occurs
            """
            try:
                while True:
                    await asyncio.sleep(10)  # maintain 10s ping interval
                    try:
                        await websocket.ping()
                    except Exception as e:
                        MutyLogger.get_instance().error(f"ping error: {e}")
                        break
            except asyncio.CancelledError:
                pass

        try:
            async with websockets.connect(
                ws_url,
                ping_interval=15,  # more frequent keepalive
                ping_timeout=20,
                close_timeout=20,
                max_size=10_000_000,
            ) as ws:
                ping_task: asyncio.Task = asyncio.create_task(_ping_loop(ws))
                progress_timeout: int = 30  # seconds without progress
                message_timeout: int = 30  # seconds to wait for messages
                last_progress: float = time.time()

                try:
                    p: GulpWsAuthPacket = GulpWsAuthPacket(token=token, ws_id=TEST_WS_ID)
                    await ws.send(p.model_dump_json(exclude_none=True))
                    num_done: int = 0

                    # start query execution in separate task
                    query_task: asyncio.Task = asyncio.create_task(
                        GulpAPIQuery.query_sigma_zip(
                            token,
                            sigma_zip_path,
                            TEST_OPERATION_ID,
                            mapping_parameters=GulpMappingParameters(
                                mapping_file="windows.json"
                            ),
                            q_options=GulpQueryParameters(
                                group="test group",
                                note_parameters={"create_notes": False},
                            ),
                        )
                    )

                    # message processing loop with timeout safeguards and retry logic
                    while True:
                        retry_count: int = 0
                        max_retries: int = 5  # maximum consecutive timeout retries
                        base_backoff: float = 0.2  # initial backoff in seconds
                        
                        try:
                            # wait for message with timeout
                            response: str = await asyncio.wait_for(ws.recv(), timeout=message_timeout)
                            retry_count = 0  # reset retry counter on successful receive
                            data: dict = json.loads(response)

                            # handle query_done messages
                            if data["type"] == "query_done":
                                num_done += 1
                                last_progress = time.time()
                                q_done_packet: GulpQueryDonePacket = GulpQueryDonePacket.model_validate(data["data"])
                                
                                # log completion details
                                MutyLogger.get_instance().debug(
                                    f"query done: {q_done_packet.name}, matches: {q_done_packet.total_hits}, processed: {num_done}"
                                )

                                # check completion condition
                                if num_done == 1209:
                                    test_completed = True
                                    break

                            # apply progressive backpressure based on processing rate
                            if num_done % 10 == 0:
                                await asyncio.sleep(0.1)  # brief pause every 10 messages

                            # check for processing stall
                            if time.time() - last_progress > progress_timeout:
                                raise TimeoutError(f"no progress for {progress_timeout}s, last count: {num_done}")

                        except asyncio.TimeoutError:
                            retry_count += 1
                            MutyLogger.get_instance().warning(
                                f"no messages received for {message_timeout}s (retry {retry_count}/{max_retries})"
                            )

                            # apply exponential backoff with jitter
                            if retry_count <= max_retries:
                                backoff = base_backoff * (2 ** retry_count)
                                await asyncio.sleep(backoff)
                                continue  # retry message reception
                                
                            # handle query task status after max retries
                            if query_task.done():
                                if query_task.exception():
                                    MutyLogger.get_instance().error("query task failed with exception")
                                    break
                                MutyLogger.get_instance().info("query completed normally")
                                break
                                
                            # final timeout after retries exhausted
                            MutyLogger.get_instance().error("maximum timeout retries exceeded")
                            raise TimeoutError("message reception retries exhausted")

                finally:
                    # cleanup tasks with proper cancellation handling
                    ping_task.cancel()
                    await asyncio.gather(ping_task, return_exceptions=True)
                    
                    if not query_task.done():
                        query_task.cancel()
                        try:
                            await query_task
                        except asyncio.CancelledError:
                            pass

        except websockets.exceptions.ConnectionClosed as ex:
            MutyLogger.get_instance().error(f"websocket closed: {ex}")
            raise
        except Exception as ex:
            MutyLogger.get_instance().error(f"unexpected error: {ex}")
            raise

        assert test_completed, "test failed to process all expected sigma rules"
        MutyLogger.get_instance().info(
            f"{_test_sigma_zip_internal.__name__} completed successfully"
        )

    # test execution flow
    guest_token: str = await GulpAPIUser.login("guest", "guest")
    assert guest_token
    ingest_token: str = await GulpAPIUser.login("ingest", "ingest")
    assert ingest_token

    from tests.ingest.test_ingest import test_win_evtx_multiple
    await test_win_evtx_multiple()
    await _test_sigma_zip_internal(guest_token)
    

@pytest.mark.asyncio
async def test_sigma_convert():
    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token

    current_dir = os.path.dirname(os.path.realpath(__file__))
    sigma_zip_path = os.path.join(current_dir, "sigma/windows.zip")
    try:
        sigma_path = await muty.file.unzip(sigma_zip_path)
        rule_path = os.path.join(
            sigma_path,
            "create_remote_thread/create_remote_thread_win_susp_relevant_source_image.yml",
        )
        sigma: bytes = await muty.file.read_file_async(rule_path)
        mapping_parameters = GulpMappingParameters(mapping_file="windows.json")
        s = await GulpAPIQuery.sigma_convert(
            guest_token, sigma.decode(), mapping_parameters=mapping_parameters
        )
        # hackish but effective
        assert "process.executable:" in str(s)
        print(s)

        # now without mapping
        s = await GulpAPIQuery.sigma_convert(
            guest_token, sigma.decode(), mapping_parameters=None
        )
        assert "process.executable:" not in str(s)
        print(s)
        MutyLogger.get_instance().info(
            test_sigma_convert.__name__ + " succeeded!"
        )

    finally:
        muty.file.delete_file_or_dir(sigma_path)
