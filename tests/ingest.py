import asyncio
import json
import multiprocessing
import os
import token
import pytest
from muty.log import MutyLogger
import websockets
from gulp.api.collab.stats import GulpRequestStats
from gulp.api.collab.structs import GulpCollabFilter
from gulp.api.mapping.models import GulpMapping, GulpMappingField
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.rest.test_values import (
    TEST_CONTEXT_NAME,
    TEST_HOST,
    TEST_INDEX,
    TEST_OPERATION_ID,
    TEST_REQ_ID,
    TEST_WS_ID,
)
from gulp.api.ws_api import GulpWsAuthPacket
from gulp.structs import GulpPluginParameters
from tests.api.common import GulpAPICommon
from tests.api.query import GulpAPIQuery
from tests.api.user import GulpAPIUser
from tests.api.ingest import GulpAPIIngest
from tests.api.db import GulpAPIDb
import muty.file

RAW_DOCUMENTS_CHUNK = [
    {
        "@timestamp": "2019-07-01T00:00:00.000Z",
        "event.code": "test_event_code_1",
        "event.original": "some original event",
        "field1": "value1",
        "field2": "value2",
        "field3": 123,
    },
    {
        "@timestamp": "2019-07-01T00:01:00.000Z",
        "event.code": "test_event_code_2",
        "event.original": "some original event 3",
        "field4": "value3",
        "field5": "value4",
        "field6": 456,
    },
    {
        "@timestamp": "2019-07-01T00:02:00.000Z",
        "event.code": "test_event_code_3",
        "event.original": "some original event 3",
        "field7": "value5",
        "field8": "value6",
        "field9": 789,
    },
]


def _process_file_in_worker_process(
    host: str,
    ws_id: str,
    req_id: str,
    index: str,
    plugin: str,
    plugin_params: GulpPluginParameters,
    flt: GulpIngestionFilter,
    file_path: str,
    file_total: int,
):
    """
    process a file
    """

    async def _process_file_async():
        GulpAPICommon.get_instance().init(
            host=host, ws_id=ws_id, req_id=req_id, index=index
        )
        MutyLogger.get_instance().info(f"processing file: {file_path}")
        guest_token = await GulpAPIUser.login("guest", "guest")
        assert guest_token

        # ingest the file (guest cannot)
        await GulpAPIIngest.ingest_file(
            guest_token,
            file_path,
            TEST_OPERATION_ID,
            TEST_CONTEXT_NAME,
            TEST_INDEX,
            plugin,
            flt=flt,
            plugin_params=plugin_params,
            file_total=file_total,
            expected_status=401,
        )

        ingest_token = await GulpAPIUser.login("ingest", "ingest")
        assert ingest_token

        # ingest the file
        await GulpAPIIngest.ingest_file(
            ingest_token,
            file_path,
            TEST_OPERATION_ID,
            TEST_CONTEXT_NAME,
            TEST_INDEX,
            plugin,
            flt=flt,
            plugin_params=plugin_params,
            file_total=file_total,
            expected_status=200,
        )

    asyncio.run(_process_file_async())


async def _ws_loop(
    ingested: int, check_on_source_done: bool = False, processed: int = None
):
    _, host = TEST_HOST.split("://")
    ws_url = f"ws://{host}/ws"
    test_completed = False
    found_ingested=0
    found_processed=0

    async with websockets.connect(ws_url) as ws:
        # connect websocket
        p: GulpWsAuthPacket = GulpWsAuthPacket(token="monitor", ws_id=TEST_WS_ID)
        await ws.send(p.model_dump_json(exclude_none=True))

        # receive responses
        try:
            while True:
                response = await ws.recv()
                data = json.loads(response)
                if data["type"] == "stats_update":
                    # stats update
                    stats_packet = data["data"]["data"]
                    MutyLogger.get_instance().info(f"ingestion stats: {stats_packet}")

                    if stats_packet["status"] == "done":
                        # done
                        records_ingested = stats_packet.get("records_ingested", 0)
                        records_processed = stats_packet.get("records_processed", 0)
                        found_ingested=records_ingested
                        found_processed=records_processed
                        if records_ingested == ingested:
                            MutyLogger.get_instance().info(
                                "all %d records ingested!" % (ingested)
                            )
                            if processed:
                                # also check processed
                                if records_processed == processed:
                                    MutyLogger.get_instance().info(
                                        "all %d records processed!" % (processed)
                                    )
                                    test_completed = True
                            else:
                                # just check ingested
                                test_completed = True
                        break
                    elif (
                        stats_packet["status"] == "failed"
                        or stats_packet["status"] == "canceled"
                    ):
                        # failed
                        break
                elif data["type"] == "ingest_source_done":
                    # source done
                    if check_on_source_done:
                        source_done_packet = data["data"]
                        ingested = source_done_packet.get("docs_ingested", 0)
                        if ingested == ingested:
                            MutyLogger.get_instance().info(
                                "all %d records ingested!" % (ingested)
                            )
                            test_completed = True
                        break

                # ws delay
                await asyncio.sleep(0.1)

        except websockets.exceptions.ConnectionClosed:
            MutyLogger.get_instance().warning("WebSocket connection closed")

    MutyLogger.get_instance().info(f"found_ingested={found_ingested} (requested={ingested}), found_processed={found_processed} (requested={processed})")      
    assert test_completed
    MutyLogger.get_instance().info("test succeeded!")


async def _test_generic(
    files: list[str],
    plugin: str,
    check_ingested: int,
    check_processed: int = None,
    plugin_params: GulpPluginParameters = None,
    flt: GulpIngestionFilter = None,
):
    GulpAPICommon.get_instance().init(
        host=TEST_HOST, ws_id=TEST_WS_ID, req_id=TEST_REQ_ID, index=TEST_INDEX
    )
    await GulpAPIDb.reset_as_admin()

    # for each file, spawn a process using multiprocessing
    for file in files:
        p = multiprocessing.Process(
            target=_process_file_in_worker_process,
            args=(
                TEST_HOST,
                TEST_WS_ID,
                TEST_REQ_ID,
                TEST_INDEX,
                plugin,
                plugin_params,
                flt,
                file,
                len(files),
            ),
        )
        p.start()

    # wait for all processes to finish
    await _ws_loop(check_ingested, processed=check_processed)


@pytest.mark.asyncio
async def test_apache_access_clf():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [os.path.join(current_dir, "../samples/apache_clf/access.log")]
    await _test_generic(files, "apache_access_clf", 1311)


@pytest.mark.asyncio
async def test_apache_error_clf():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [os.path.join(current_dir, "../samples/apache_clf/error.log")]
    await _test_generic(files, "apache_error_clf", 1178)


@pytest.mark.asyncio
async def test_systemd_journal():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [os.path.join(current_dir, "../samples/systemd_journal/system.journal")]
    await _test_generic(files, "systemd_journal", 9243)


@pytest.mark.asyncio
async def test_win_reg():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [os.path.join(current_dir, "../samples/win_reg/NTUSER.DAT")]
    await _test_generic(files, "win_reg", 1206)


@pytest.mark.asyncio
async def test_eml():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [os.path.join(current_dir, "../samples/eml/sample.eml")]
    await _test_generic(files, "eml", 1)


@pytest.mark.asyncio
async def test_mbox():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [os.path.join(current_dir, "../samples/mbox/sample.mbox")]
    await _test_generic(files, "mbox", 16)


@pytest.mark.asyncio
async def test_pcap():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [os.path.join(current_dir, "../samples/pcap/220614_ip_flags_google.pcapng")]
    await _test_generic(files, "pcap", 58)


@pytest.mark.asyncio
async def test_teamviewer_regex_stacked():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [
        os.path.join(current_dir, "../samples/teamviewer/connections_incoming.txt")
    ]
    await _test_generic(files, "teamviewer_regex_stacked", 2)


@pytest.mark.asyncio
async def test_chrome_history():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [os.path.join(current_dir, "../samples/sqlite/chrome_history")]
    await _test_generic(files, "chrome_history_sqlite_stacked", 19)


@pytest.mark.asyncio
async def test_chrome_webdata():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [os.path.join(current_dir, "../samples/sqlite/chrome_webdata")]
    await _test_generic(files, "chrome_webdata_sqlite_stacked", 2, check_processed=1)


@pytest.mark.asyncio
async def test_pfsense():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [os.path.join(current_dir, "../samples/pfsense/filter.log")]
    await _test_generic(files, "pfsense", 61)

@pytest.mark.asyncio
async def test_win_evtx():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    samples_dir = os.path.join(current_dir, "../samples/win_evtx")
    files = muty.file.list_directory(samples_dir, recursive=True, files_only=True)
    await _test_generic(files, "win_evtx", 98632)


@pytest.mark.asyncio
async def test_csv_standalone_and_query_operations():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [os.path.join(current_dir, "../samples/mftecmd/sample_record.csv")]
    plugin_params = GulpPluginParameters(
        mappings={
            "test_mapping": GulpMapping(
                fields={"Created0x10": GulpMappingField(ecs="@timestamp")}
            )
        }
    )
    await _test_generic(files, "csv", 10, plugin_params=plugin_params)

    # test query operations
    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token
    ops = await GulpAPIQuery.query_operations(guest_token, TEST_INDEX)
    assert ops[0]["contexts"][0]["plugins"][0]["name"] == "csv"
    assert (
        ops[0]["contexts"][0]["plugins"][0]["sources"][0]["min_gulp.timestamp"]
        == 1258476898794248960
    )

    # test query max-min timestamp
    data = await GulpAPIQuery.query_max_min_per_field(guest_token, TEST_INDEX)
    assert data["buckets"][0]["*"]["doc_count"] == 10
    assert data["buckets"][0]["*"]["min_gulp.timestamp"] == 1258476898794248960


@pytest.mark.asyncio
async def test_csv_file_mapping():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [os.path.join(current_dir, "../samples/mftecmd/sample_record.csv")]
    plugin_params = GulpPluginParameters(
        mapping_file="mftecmd_csv.json", mapping_id="record"
    )
    await _test_generic(
        files, "csv", 44, check_processed=10, plugin_params=plugin_params
    )


@pytest.mark.asyncio
async def test_csv_stacked():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [os.path.join(current_dir, "../samples/mftecmd/sample_record.csv")]
    plugin_params = GulpPluginParameters(
        mappings={
            "test_mapping": GulpMapping(
                fields={"Created0x10": GulpMappingField(ecs="@timestamp")}
            )
        }
    )
    await _test_generic(files, "stacked_example", 10, plugin_params=plugin_params)

    # TODO: check documents (all documents duration set to 9999 and augmented=True set)


@pytest.mark.asyncio
async def test_raw():
    GulpAPICommon.get_instance().init(
        host=TEST_HOST, ws_id=TEST_WS_ID, req_id=TEST_REQ_ID, index=TEST_INDEX
    )
    current_dir = os.path.dirname(os.path.realpath(__file__))
    raw_chunk_path = os.path.join(current_dir, "raw_chunk.json")
    buf = await muty.file.read_file_async(raw_chunk_path)
    raw_chunk = json.loads(buf)

    await GulpAPIDb.reset_as_admin()

    ingest_token = await GulpAPIUser.login("ingest", "ingest")
    assert ingest_token

    # ingest raw chunk
    await GulpAPIIngest.ingest_raw(
        ingest_token,
        raw_chunk,
        TEST_OPERATION_ID,
        TEST_CONTEXT_NAME,
        TEST_INDEX,
    )

    # wait ws
    await _ws_loop(3, check_on_source_done=True)


@pytest.mark.asyncio
async def test_ingest_zip():
    GulpAPICommon.get_instance().init(
        host=TEST_HOST, ws_id=TEST_WS_ID, req_id=TEST_REQ_ID, index=TEST_INDEX
    )

    await GulpAPIDb.reset_as_admin()

    ingest_token = await GulpAPIUser.login("ingest", "ingest")
    assert ingest_token

    current_dir = os.path.dirname(os.path.realpath(__file__))
    test_zip = os.path.join(current_dir, "test_ingest_zip.zip")

    # ingest raw chunk
    await GulpAPIIngest.ingest_zip(
        ingest_token,
        test_zip,
        TEST_OPERATION_ID,
        TEST_CONTEXT_NAME,
        TEST_INDEX,
    )

    # wait ws
    await _ws_loop(13778, processed=13745)

@pytest.mark.asyncio
async def test_paid_plugins():
    import importlib
    m = importlib.import_module("gulp-paid-plugins.tests.ingest")
    assert await m.test_all()