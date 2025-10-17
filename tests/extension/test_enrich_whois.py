#!/usr/bin/env python3
import asyncio
import json
import os

import muty.file
import pytest
import pytest_asyncio
from scipy import stats
import websockets
from muty.log import MutyLogger
from gulp.api.collab.stats import GulpRequestStats
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp_client.common import GulpAPICommon, _ensure_test_operation
from gulp_client.enrich import GulpAPIEnrich
from gulp_client.query import GulpAPIQuery
from gulp_client.user import GulpAPIUser
from gulp_client.test_values import (
    TEST_HOST,
    TEST_INDEX,
    TEST_OPERATION_ID,
    TEST_REQ_ID,
    TEST_WS_ID,
)
from gulp.api.ws_api import (
    GulpQueryDonePacket,
    GulpWsAuthPacket,
)
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
        "source.ip": "3.162.112.100",
        "host.hostname": "https://repubblica.it",
    },
    {
        "@timestamp": "2019-07-01T00:01:00.000Z",
        "event.code": "test_event_code_2",
        "event.original": "some text",
        "gulp.context_id": "ac019b190bf8e15588812066161cf74137ab3e97",
        "gulp.source_id": "de65eb5d6bcac989815acd1adfa0afa183939eda",
        "gulp.operation_id": "test_operation",
        "event.sequence": 1,
        "source.ip": "3.162.112.100",
        "host.hostname": "https://repubblica.it",
    },
    {
        "@timestamp": "2019-07-01T00:02:00.000Z",
        "event.code": "test_event_code_3",
        "event.original": "some text",
        "gulp.context_id": "ac019b190bf8e15588812066161cf74137ab3e97",
        "gulp.source_id": "de65eb5d6bcac989815acd1adfa0afa183939eda",
        "gulp.operation_id": "test_operation",
        "event.sequence": 2,
        "source.ip": "2001:4860:4801:53::3b",
        "host.hostname": "repubblica.it",
    },
    {
        "@timestamp": "2019-07-01T00:03:00.000Z",
        "event.code": "test_event_code_4",
        "event.original": "this is the original event text with ip 185.53.36.36, 104.18.4.215 and host=https://microsoft.com and 3.162.112.100 and ipv6=2001:4860:4801:53::3b and some invalid ipv6=ab12r3::ff:00:32 inside",
        "gulp.context_id": "ac019b190bf8e15588812066161cf74137ab3e97",
        "gulp.source_id": "de65eb5d6bcac989815acd1adfa0afa183939eda",
        "gulp.operation_id": "test_operation",
        "source.ip": "2001:4860:4801:53::3b",
        "event.sequence": 3,
    },
    {
        "@timestamp": "2019-07-01T00:04:00.000Z",
        "event.code": "test_event_code_5",
        "event.original": "ipv4=185.53.36.36,ipv4=104.18.4.215,host=https://microsoft.com,ip=3.162.112.100,ip=2001:4860:4801:53::3b,ip=ab12r3::ff:00:32",
        "gulp.context_id": "ac019b190bf8e15588812066161cf74137ab3e97",
        "gulp.source_id": "de65eb5d6bcac989815acd1adfa0afa183939eda",
        "gulp.operation_id": "test_operation",
        "source.ip": "2001:4860:4801:53::3b",
        "event.sequence": 4,
    },
    {
        "@timestamp": "2019-07-01T00:05:00.000Z",
        "event.code": "test_event_code_5",
        "event.original": '{"ip1": 185.53.36.36, "ip2":"104.18.4.215", "host":"https://microsoft.com", "ip3":"3.162.112.100", "ip4":"2001:4860:4801:53::3b", "ip5":"ab12r3::ff:00:32"}',
        "gulp.context_id": "ac019b190bf8e15588812066161cf74137ab3e97",
        "gulp.source_id": "de65eb5d6bcac989815acd1adfa0afa183939eda",
        "gulp.operation_id": "test_operation",
        "source.ip": "2001:4860:4801:53::3b",
        "event.sequence": 5,
    },
    {
        "@timestamp": "2019-07-01T00:06:00.000Z",
        "event.code": "test_event_code_6",
        "event.original": "[zonetransfer] Zoneid=3 185.53.36.36",
        "gulp.context_id": "ac019b190bf8e15588812066161cf74137ab3e97",
        "gulp.source_id": "de65eb5d6bcac989815acd1adfa0afa183939eda",
        "gulp.operation_id": "test_operation",
        "source.ip": "2001:4860:4801:53::3b",
        "event.sequence": 5,
    },
]


@pytest_asyncio.fixture(scope="function", autouse=True)
async def _setup():
    """
    this is called before any test, to initialize the environment
    """
    if os.getenv("SKIP_RESET", "0") == "1":
        GulpAPICommon.get_instance().init(
            host=TEST_HOST, ws_id=TEST_WS_ID, req_id=TEST_REQ_ID, index=TEST_INDEX
        )
    else:
        await _ensure_test_operation()


@pytest.mark.asyncio
async def test_enrich_whois_documents():

    edit_token = await GulpAPIUser.login("editor", "editor")
    assert edit_token

    if os.getenv("SKIP_RESET", "0") == "0":
        # ingest some data
        from tests.ingest.test_ingest import test_raw

        await test_raw(RAW_DATA)

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
                        "host_fields": ["source.ip", "host.hostname", "event.original"],
                    }
                    await GulpAPIEnrich.enrich_documents(
                        edit_token,
                        TEST_OPERATION_ID,
                        plugin="enrich_whois",
                        plugin_params=plugin_params,
                        req_id="req_enrich_whois",
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
                    if stats.status == "done" and stats_data.updated == 7:
                        test_completed = True
                    else:
                        assert False, (
                            "enrich done, req_id=%s, expected updated=7 but received updated=%d",
                            stats.id,
                            stats_data.updated,
                        )
                    break

                # ws delay
                await asyncio.sleep(0.1)

        except websockets.exceptions.ConnectionClosed as ex:
            MutyLogger.get_instance().exception(ex)

    assert test_completed
    MutyLogger.get_instance().info(test_enrich_whois_documents.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_enrich_whois_single():

    doc_id: str = "d1a00fb20c52958e0b69b63182c89bfa"
    edit_token = await GulpAPIUser.login("editor", "editor")
    assert edit_token

    guest_token = await GulpAPIUser.login("guest", "guest")
    assert edit_token

    # ingest some data
    from tests.ingest.test_ingest import test_raw

    await test_raw(RAW_DATA)

    plugin_params: GulpPluginParameters = GulpPluginParameters()
    plugin_params.custom_parameters = {
        "host_fields": ["source.ip", "host.hostname", "event.original"],
        "resolve_first_only": False,
    }

    # guest cannot enrich, verify that
    await GulpAPIEnrich.enrich_single_id(
        guest_token,
        TEST_OPERATION_ID,
        doc_id=doc_id,
        plugin="enrich_whois",
        expected_status=401,
        plugin_params=plugin_params,
    )
    doc = await GulpAPIEnrich.enrich_single_id(
        edit_token,
        TEST_OPERATION_ID,
        doc_id=doc_id,
        plugin="enrich_whois",
        plugin_params=plugin_params,
    )

    assert doc.get("gulp.enrich_whois.source_ip.unified_dump") != None
    assert doc.get("gulp.enrich_whois.event_original.unified_dump") != None
    MutyLogger.get_instance().info(test_enrich_whois_single.__name__ + " succeeded!")
