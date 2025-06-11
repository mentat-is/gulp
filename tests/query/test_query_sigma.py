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
from gulp.api.opensearch.query import GulpQueryParameters
from gulp.api.rest.client.common import GulpAPICommon, _ensure_test_operation
from gulp.api.rest.client.note import GulpAPINote
from gulp.api.rest.client.query import GulpAPIQuery
from gulp.api.rest.client.user import GulpAPIUser
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

@pytest_asyncio.fixture(scope="function", autouse=True)
async def _setup():
    if os.environ.get("INGEST_DATA", "1") == "1":
        await _ensure_test_operation()
    else:
        GulpAPICommon.get_instance().init(
            host=TEST_HOST, ws_id=TEST_WS_ID, req_id=TEST_REQ_ID, index=TEST_INDEX
        )

@pytest.mark.asyncio
async def test_sigma_group():
    if os.environ.get("INGEST_DATA", "1"):
        # use the default test data
        await _ensure_test_operation()
        from tests.ingest.test_ingest import test_win_evtx
        await test_win_evtx()

    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token

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
        p: GulpWsAuthPacket = GulpWsAuthPacket(token=guest_token, ws_id=TEST_WS_ID)
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
                        guest_token,
                        TEST_OPERATION_ID,
                        [
                            sigma_match_some.decode(),
                            sigma_match_some_more.decode(),
                        ],
                        q_options=q_options,
                        req_id="sigmareq"
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
                        notes = await GulpAPINote.note_list(guest_token, flt)
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
    MutyLogger.get_instance().info(test_sigma_group.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_sigma_single():
    async def _test_sigma_single_internal(token: str, path: str, expected_matches: int):
        sigma = await muty.file.read_file_async(path)
        
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
                        expected_hits = expected_matches
                        if q_done_packet.total_hits == expected_hits:
                            test_completed = True
                            break
                        else:
                            assert False, "expected hits: %d, got: %d, sigma=\n%s" % (expected_hits, q_done_packet.total_hits, sigma)

                    # ws delay
                    await asyncio.sleep(0.1)

            except websockets.exceptions.ConnectionClosed as ex:
                MutyLogger.get_instance().exception(ex)

        assert test_completed

    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token

    current_dir = os.path.dirname(os.path.realpath(__file__))
    file_path = os.path.join(
        # current_dir, "sigma/Microsoft-Windows-Windows Defender%4Operational.evtx"
        current_dir, "sigma/Microsoft-Windows-Sysmon%4Operational.evtx"
    )

    if os.environ.get("INGEST_DATA", "1") == "1":
        # ingest data is the default
        from tests.ingest.test_ingest import test_win_evtx
        await test_win_evtx(file_path=file_path, skip_checks=True)

    # read sigma
    # following matches are for Microsoft-Windows-Sysmon%4Operational.evtx on our sharepoint

    # current_dir, "sigma/windows/create_stream_hash/create_stream_hash_susp_ip_domains.yml" # 6
    # current_dir, "sigma/windows/create_stream_hash/create_stream_hash_file_sharing_domains_download_susp_extension.yml" # 1
    # current_dir, "sigma/windows/file/file_change/file_change_win_2022_timestomping.yml" # 4
    # current_dir, "sigma/windows/network_connection/net_connection_win_rdp_outbound_over_non_standard_tools.yml" # 0
    # current_dir, "sigma/windows/network_connection/net_connection_win_susp_initiated_uncommon_or_suspicious_locations.yml" # 3
    # current_dir, "sigma/windows/process_creation/proc_creation_win_powershell_cmdline_special_characters.yml" # 0
    # current_dir, "sigma/windows/process_creation/proc_creation_win_powershell_frombase64string.yml" # 1
    # current_dir, "sigma/windows/process_creation/proc_creation_win_renamed_binary_highly_relevant.yml" # 19
    # current_dir, "sigma/windows/process_creation/proc_creation_win_susp_execution_path.yml" # 10
    # current_dir, "sigma/windows/process_creation/proc_creation_win_susp_inline_win_api_access.yml" # 7
    # current_dir, "sigma/windows/process_creation/proc_creation_win_susp_parents.yml" # 1
    # current_dir, "sigma/windows/process_creation/proc_creation_win_susp_script_exec_from_env_folder.yml" # 0
    # current_dir, "sigma/windows/process_creation/proc_creation_win_susp_script_exec_from_temp.yml" # 5
    # current_dir, "sigma/windows/registry/registry_set/registry_set_renamed_sysinternals_eula_accepted.yml" # 7
    #current_dir, "sigma/win_defender_threat.yml"

    # rule 1
    sigma_path = os.path.join(
        current_dir, "sigma/windows/create_stream_hash/create_stream_hash_susp_ip_domains.yml"
    )    
    await _test_sigma_single_internal(guest_token, sigma_path, expected_matches=6)

    # rule 2
    sigma_path = os.path.join(
        current_dir, "sigma/windows/file/file_change/file_change_win_2022_timestomping.yml")
        
    await _test_sigma_single_internal(guest_token, sigma_path, expected_matches=4)

    # add other ?
    # ...

    # done
    MutyLogger.get_instance().info(test_sigma_single.__name__ + " succeeded!")

