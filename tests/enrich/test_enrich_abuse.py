#!/usr/bin/env python3
import asyncio
import json
import os

import muty.file
import pytest
import pytest_asyncio
import websockets
from gulp_client.common import (
    GulpAPICommon,
    _cleanup_test_operation,
    _ensure_test_operation,
)
from gulp_client.enrich import GulpAPIEnrich
from gulp_client.query import GulpAPIQuery
from gulp_client.test_values import (
    TEST_HOST,
    TEST_INDEX,
    TEST_OPERATION_ID,
    TEST_REQ_ID,
    TEST_WS_ID,
)
from gulp_client.user import GulpAPIUser
from muty.log import MutyLogger
from scipy import stats

from gulp.api.collab.stats import GulpRequestStats
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.ws_api import GulpQueryDonePacket, GulpWsAuthPacket
from gulp.plugin import GulpUpdateDocumentsStats
from gulp.structs import GulpPluginParameters

RAW_DATA: list[dict] = [
    {
        "@timestamp": "2019-07-01T00:00:00.000Z",
        "event.code": "test_event_code_1",
        "event.original": "some text",
        "gulp.context_id": "ac019b190bf8e15588812066161cf74137ab3e97",
        "gulp.source_id": "de65eb5d6bcac989815acd1adfa0afa183939eda",
        "gulp.operation_id": "test_operation",
        "event.sequence": 0,
        "url.full": "http://117.209.24.146:52152/bin.sh",
        "hash_md5": "1b109efade90ace7d953507adb1f1563",
        "host.hostname": "https://repubblica.it",
    },
    {
        "@timestamp": "2019-07-01T00:01:00.000Z",
        "event.code": "test_event_code_1",
        "event.original": "some text",
        "gulp.context_id": "ac019b190bf8e15588812066161cf74137ab3e97",
        "gulp.source_id": "de65eb5d6bcac989815acd1adfa0afa183939eda",
        "gulp.operation_id": "test_operation",
        "event.sequence": 0,
        "url.full": "http://notexistingdomain.tld/malware.exe",
        "hash_md5": "1b109efade90ace7d953507adb1f1563",
        "host.hostname": "https://repubblica.it",
    },
    {
        "@timestamp": "2019-07-01T00:02:00.000Z",
        "event.code": "test_event_code_2",
        "event.original": "some text",
        "gulp.context_id": "ac019b190bf8e15588812066161cf74137ab3e97",
        "gulp.source_id": "de65eb5d6bcac989815acd1adfa0afa183939eda",
        "gulp.operation_id": "test_operation",
        "event.sequence": 1,
        "url.full": "http://ip82-165-181-201.pbiaas.com:8080/i.sh",
        "hash_sha256": "5a699a644b98c662bb3eb67661a10d738e1b9ba4e1689f138e9dfc4d0f4b37c3",
        "source.ip": "3.162.112.100",
        "host.hostname": "https://repubblica.it",
    },
]


@pytest_asyncio.fixture(scope="function", autouse=True)
async def _setup():
    """
    this is called before any test, to initialize the environment
    """
    if os.getenv("SKIP_RESET", "0") == "1":
        await _cleanup_test_operation()
    else:
        await _ensure_test_operation()


@pytest.mark.asyncio
async def _test_enrich_abuse_documents_internal(query_type: str = "url"):

    if os.getenv("SKIP_RESET", "0") == "0":
        # ingest some data
        from tests.ingest.test_ingest import test_raw

        await test_raw(RAW_DATA)

    edit_token = await GulpAPIUser.login("editor", "editor")
    assert edit_token

    _, host = TEST_HOST.split("://")
    ws_url = f"ws://{host}/ws"
    test_completed = False

    async with websockets.connect(ws_url) as ws:
        # connect websocket
        p: GulpWsAuthPacket = GulpWsAuthPacket(token=edit_token, ws_id=TEST_WS_ID)
        await ws.send(p.model_dump_json(exclude_none=True))

        # receive responses
        try:
            while True:
                response = await ws.recv()
                data = json.loads(response)
                payload = data.get("payload", {})
                if data["type"] == "ws_connected":
                    # run test
                    plugin_params: GulpPluginParameters = GulpPluginParameters()
                    plugin_params.custom_parameters = {
                        "query_type": query_type,
                    }

                    if query_type is "url":
                        fields: dict = {
                            "url.full": None,
                        }
                    else:
                        # hash
                        fields: dict = {
                            "hash_md5": None,
                            "hash_sha256": None,
                        }

                    await GulpAPIEnrich.enrich_documents(
                        edit_token,
                        TEST_OPERATION_ID,
                        fields,
                        plugin="enrich_abuse",
                        plugin_params=plugin_params,
                        req_id="req_enrich_abuse",
                    )
                elif (
                    data["type"] == "stats_update"
                    and payload
                    and payload["obj"]["req_type"] == "enrich"
                ):
                    stats: GulpRequestStats = GulpRequestStats.from_dict(payload["obj"])
                    stats_data: GulpUpdateDocumentsStats = (
                        GulpUpdateDocumentsStats.model_validate(payload["obj"]["data"])
                    )
                    MutyLogger.get_instance().info("stats: %s", stats)

                    # query done
                    if query_type == "url":
                        updated = 2
                    else:
                        # hash
                        updated = 1
                    if stats.status == "done" and stats_data.updated == updated:
                        test_completed = True
                    else:
                        assert False, (
                            "enrich done, req_id=%s, expected updated=%d but received updated=%d",
                            stats.id,
                            updated,
                            stats_data.updated,
                        )
                    break

                # ws delay
                await asyncio.sleep(0.1)

        except websockets.exceptions.ConnectionClosed as ex:
            MutyLogger.get_instance().exception(ex)

    assert test_completed
    MutyLogger.get_instance().info(_test_enrich_abuse_documents_internal.__name__ + " succeeded, query_type=%s!" % (query_type))


@pytest.mark.asyncio
async def test_enrich_abuse_url_documents():
    await _test_enrich_abuse_documents_internal(query_type="url")

@pytest.mark.asyncio
async def test_enrich_abuse_hash_documents():
    await _test_enrich_abuse_documents_internal(query_type="hash")

@pytest.mark.asyncio
async def test_enrich_abuse_single_id():
    if os.getenv("SKIP_RESET", "0") == "0":
        # ingest some data
        from tests.ingest.test_ingest import test_raw

        await test_raw(RAW_DATA)

    doc_id: str = "bf16614415f8f86c709df23539796cd8"
    edit_token = await GulpAPIUser.login("editor", "editor")
    assert edit_token

    guest_token = await GulpAPIUser.login("guest", "guest")
    assert edit_token

    plugin_params: GulpPluginParameters = GulpPluginParameters()
    plugin_params.custom_parameters = {
        "query_type": "hash",
    }
    fields: dict = {
        "hash_md5": None,
        "hash_sha256": None,
        "hash_new_sha256": "5a699a644b98c662bb3eb67661a10d738e1b9ba4e1689f138e9dfc4d0f4b37c3",
    }
    
    # guest cannot enrich, verify that
    await GulpAPIEnrich.enrich_single_id(
        guest_token,
        TEST_OPERATION_ID,
        doc_id=doc_id,
        fields=fields,
        plugin="enrich_abuse",
        expected_status=401,
        plugin_params=plugin_params,
    )
    doc = await GulpAPIEnrich.enrich_single_id(
        edit_token,
        TEST_OPERATION_ID,
        doc_id=doc_id,
        fields=fields,
        plugin="enrich_abuse",
        plugin_params=plugin_params,
    )

    assert doc.get("gulp.enriched_enrich_abuse.hash_sha256") != None
    assert doc.get("gulp.enriched_enrich_abuse.hash_new_sha256") != None
    MutyLogger.get_instance().info(test_enrich_abuse_single_id.__name__ + " succeeded!")

