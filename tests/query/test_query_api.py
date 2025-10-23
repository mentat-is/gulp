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
from gulp.api.opensearch.structs import GulpQueryParameters
from gulp_client.common import (
    GulpAPICommon,
    _ensure_test_operation,
    _test_ingest_ws_loop,
    _cleanup_test_operation,
)
from gulp_client.ingest import GulpAPIIngest
from gulp_client.note import GulpAPINote
from gulp_client.object_acl import GulpAPIObjectACL
from gulp_client.operation import GulpAPIOperation
from gulp_client.query import GulpAPIQuery
from gulp_client.user import GulpAPIUser
from gulp_client.test_values import (
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
    if os.getenv("SKIP_RESET") == "1":
        await _cleanup_test_operation()
    else:
        await _ensure_test_operation()


async def _login_and_ingest(
    user: str = "guest", password: str = "guest"
) -> tuple[str, str]:
    """
    login and (if SKIP_RESET not set) ingest some data

    Returns:
        tuple[str, str]: the token corresponding to (user,password), the ingest token
    """
    guest_token = await GulpAPIUser.login(user, password)
    assert guest_token
    skip_reset = os.getenv("SKIP_RESET", "0")
    if skip_reset == "0":
        # ingest some data
        from tests.ingest.test_ingest import test_win_evtx

        await test_win_evtx()

    ingest_token = await GulpAPIUser.login("ingest", "ingest")
    assert ingest_token
    return guest_token, ingest_token


async def _test_query_internal(q_type: str):
    guest_token, _ = await _login_and_ingest()

    MutyLogger.get_instance().info("ingested data, starting query")
    _, host = TEST_HOST.split("://")
    ws_url = f"ws://{host}/ws"
    test_completed = False
    async with websockets.connect(ws_url) as ws:
        # connect websocket
        p: GulpWsAuthPacket = GulpWsAuthPacket(token=guest_token, ws_id=TEST_WS_ID)
        await ws.send(p.model_dump_json(exclude_none=True))

        # receive responses
        q_name: str = None
        q_options = GulpQueryParameters(limit=2)
        current_dir: str = os.path.dirname(os.path.realpath(__file__))
        MutyLogger.get_instance().debug("current_dir=%s", current_dir)
        num_matches: int = 0
        query_group: str = None
        query_raw_dict = {
            "query": {
                "query_string": {
                    "query": "gulp.context_id: %s AND gulp.operation_id: %s"
                    % (TEST_CONTEXT_ID, TEST_OPERATION_ID),
                }
            }
        }

        try:
            while True:
                response = await ws.recv()
                data = json.loads(response)
                payload = data.get("payload", {})
                if data["type"] == "ws_connected":
                    # run test
                    if q_type == "gulp":
                        # query_gulp
                        q_name = "test_gulp_query"
                        q_options.name = q_name
                        num_matches: int = 7
                        await GulpAPIQuery.query_gulp(
                            guest_token,
                            TEST_OPERATION_ID,
                            flt=GulpQueryFilter(
                                operation_ids=[TEST_OPERATION_ID],
                                context_ids=[TEST_CONTEXT_ID],
                            ),
                            q_options=q_options,
                            req_id="req_test_gulp_query",
                        )
                    elif q_type == "raw":
                        q_name = "test_raw_query"
                        q_options.name = q_name
                        num_matches: int = 7
                        await GulpAPIQuery.query_raw(
                            guest_token,
                            TEST_OPERATION_ID,
                            [query_raw_dict],
                            q_options=q_options,
                            req_id="req_test_raw_query",
                        )
                    elif q_type == "query_raw_group":
                        num_matches: int = 140
                        q_name = "test_raw_group"
                        q_options.name = q_name
                        q_options.create_notes = True
                        q_options.group = "raw_group"
                        query_group = q_options.group
                        q_raw_array = []
                        for _ in range(20):
                            q_raw_array.append(query_raw_dict)
                        await GulpAPIQuery.query_raw(
                            guest_token,
                            TEST_OPERATION_ID,
                            q_raw_array,
                            q_options=q_options,
                            req_id="req_test_raw_group",
                        )
                    elif q_type == "sigma":
                        sigma_yml: str = await muty.file.read_file_async(
                            os.path.join(current_dir, "sigma/match_all.yaml")
                        )

                        ids: list[str] = ["64e7c3a4013ae243aa13151b5449aac884e36081"]
                        num_matches = 7
                        q_name = "Match All Events"
                        q_options.create_notes = True
                        await GulpAPIQuery.query_sigma(
                            guest_token,
                            TEST_OPERATION_ID,
                            [sigma_yml.decode("utf-8")],
                            src_ids=ids,
                            q_options=q_options,
                            req_id="req_test_sigma",
                        )
                    elif q_type == "query_sigma_group":
                        sigma_1_yml: str = await muty.file.read_file_async(
                            os.path.join(current_dir, "sigma/match_some.yaml")
                        )
                        sigma_2_yml: str = await muty.file.read_file_async(
                            os.path.join(current_dir, "sigma/match_some_more.yaml")
                        )

                        ids: list[str] = ["64e7c3a4013ae243aa13151b5449aac884e36081"]
                        num_matches: int = 5
                        q_name = "test_sigma_group"
                        q_options.create_notes = True
                        q_options.group = "sigma_group"
                        query_group = q_options.group
                        await GulpAPIQuery.query_sigma(
                            guest_token,
                            TEST_OPERATION_ID,
                            [sigma_1_yml.decode("utf-8"), sigma_2_yml.decode("utf-8")],
                            src_ids=ids,
                            q_options=q_options,
                            req_id="req_test_sigma_group",
                        )
                elif data["type"] == "query_group_match":
                    # query group match!
                    q_match_packet: GulpQueryGroupMatchPacket = (
                        GulpQueryGroupMatchPacket.model_validate(payload)
                    )
                    MutyLogger.get_instance().debug(
                        "query_group_match, group=%s, matches=%s",
                        q_match_packet.group,
                        q_match_packet.matches,
                    )
                    assert q_match_packet.group == query_group
                    test_completed = True
                    break
                elif data["type"] == "query_done":
                    # a single query is done
                    # query sigma group waits for query_group_match packets
                    q_done_packet: GulpQueryDonePacket = (
                        GulpQueryDonePacket.model_validate(payload)
                    )
                    MutyLogger.get_instance().debug(
                        "query done, name=%s", q_done_packet.q_name
                    )
                    if not query_group:
                        # not a group query
                        if q_done_packet.q_name == q_name:
                            assert q_done_packet.total_hits == num_matches
                            test_completed = True
                        else:
                            raise ValueError(
                                f"unexpected query name: {q_done_packet.q_name}, expected={q_name}"
                            )
                        break

                # ws delay
                await asyncio.sleep(0.1)

        except websockets.exceptions.ConnectionClosed as ex:
            MutyLogger.get_instance().exception(ex)
            raise

    assert test_completed


@pytest.mark.asyncio
async def test_query_gulp():
    await _test_query_internal("gulp")
    MutyLogger.get_instance().info(test_query_gulp.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_query_raw():
    await _test_query_internal("raw")
    MutyLogger.get_instance().info(test_query_raw.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_query_raw_preview():
    guest_token, _ = await _login_and_ingest()

    q_name = "test_raw_query"
    q_options = GulpQueryParameters(preview_mode=True)
    q_options.name = q_name
    d = await GulpAPIQuery.query_raw(
        guest_token,
        TEST_OPERATION_ID,
        [
            {
                "query": {
                    "query_string": {
                        "query": "gulp.context_id: %s AND gulp.operation_id: %s"
                        % (TEST_CONTEXT_ID, TEST_OPERATION_ID),
                    }
                }
            }
        ],
        q_options=q_options,
    )

    assert len(d["docs"]) == 7
    MutyLogger.get_instance().info(test_query_raw_preview.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_query_sigma():
    await _test_query_internal("sigma")
    MutyLogger.get_instance().info(test_query_sigma.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_query_sigma_group():
    await _test_query_internal("query_sigma_group")
    MutyLogger.get_instance().info(test_query_sigma_group.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_query_raw_group():
    await _test_query_internal("query_raw_group")
    MutyLogger.get_instance().info(test_query_raw_group.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_query_sigma_preview():
    guest_token, _ = await _login_and_ingest()

    current_dir: str = os.path.dirname(os.path.realpath(__file__))
    sigma_yml: str = await muty.file.read_file_async(
        os.path.join(current_dir, "sigma/match_all.yaml")
    )

    # q_name and create_notes are ignored
    q_name = "Match All Events"
    q_options = GulpQueryParameters(preview_mode=True)
    q_options.name = q_name
    q_options.create_notes = True
    d = await GulpAPIQuery.query_sigma(
        guest_token,
        TEST_OPERATION_ID,
        [sigma_yml.decode("utf-8")],
        q_options=q_options,
        req_id="req_test_sigma",
    )
    assert len(d["docs"]) == 7
    MutyLogger.get_instance().info(test_query_sigma_preview.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_query_single_id():
    guest_token, _ = await _login_and_ingest()

    target_id = "4905967cfcaf2abe0e28322ff085619d"
    d = await GulpAPIQuery.query_single_id(guest_token, TEST_OPERATION_ID, target_id)
    assert d["_id"] == target_id
    MutyLogger.get_instance().info(test_query_single_id.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_query_operations():
    # ingest some data first
    guest_token, ingest_token = await _login_and_ingest()

    operations = await GulpAPIQuery.query_operations(guest_token)
    assert operations and len(operations) == 1

    # create another operation (with no guest grants), with the guest user cannot see it
    try:
        await GulpAPIOperation.operation_delete(ingest_token, "new_operation")
    except:
        pass
    op = await GulpAPIOperation.operation_create(ingest_token, "new_operation")
    assert op and op["id"] == "new_operation"

    # ingest some data in this operation
    current_dir = os.path.dirname(os.path.realpath(__file__))
    samples_dir = os.path.join(current_dir, "../../samples/win_evtx")
    file_path = os.path.join(samples_dir, "Security_short_selected.evtx")
    await GulpAPIIngest.ingest_file(
        token=ingest_token,
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
        token=ingest_token,
        obj_id="new_operation",
        obj_type=COLLABTYPE_OPERATION,
        user_id="guest",
    )

    # guest token can now see the operation
    operations = await GulpAPIQuery.query_operations(guest_token)
    assert operations and len(operations) == 2

    # delete the new operation
    await GulpAPIOperation.operation_delete(ingest_token, "new_operation")


@pytest.mark.asyncio
async def test_query_gulp_export_json():
    # ingest some data first
    guest_token, ingest_token = await _login_and_ingest()
    current_dir = os.path.dirname(os.path.realpath(__file__))
    out_path = os.path.join(current_dir, "export.json")

    q_options: GulpQueryParameters = GulpQueryParameters()
    # q_options.limit = 500
    # q_options.total_limit = 500

    flt: GulpQueryFilter = GulpQueryFilter(time_range=(1467213874345999999, 0))

    path = await GulpAPIQuery.query_gulp_export_json(
        guest_token,
        TEST_OPERATION_ID,
        output_file_path=out_path,
        flt=flt,
        q_options=q_options,
    )
    assert path == out_path

    js = await muty.file.read_file_async(out_path)
    js = json.loads(js.decode("utf-8"))
    assert (
        len(js["docs"]) == 6
    )  # should match 6 entries, first is skipped by filter due to time_range
    MutyLogger.get_instance().info(test_query_gulp_export_json.__name__ + " succeeded!")
