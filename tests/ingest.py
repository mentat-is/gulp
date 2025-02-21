import asyncio
import json
import multiprocessing
import os
import platform
import random
import shutil
import string
import tempfile
from datetime import datetime, timedelta

import muty.crypto
import muty.file
import muty.string
import pytest
import pytest_asyncio
import websockets
from muty.log import MutyLogger

from gulp.api.collab.operation import GulpOperation
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
from gulp.api.ws_api import GulpWsAuthPacket, GulpWsIngestPacket
from gulp.structs import GulpPluginParameters
from tests.api.common import GulpAPICommon
from tests.api.db import GulpAPIDb
from tests.api.ingest import GulpAPIIngest
from tests.api.operation import GulpAPIOperation
from tests.api.query import GulpAPIQuery
from tests.api.user import GulpAPIUser


async def _init(login_admin_and_reset_operation: bool = True) -> None:
    """
    initialize the environment, automatically called before each test by the _setup() fixture

    :param login_admin_and_reset_operation: if True, login as admin and reset the operation
    """
    GulpAPICommon.get_instance().init(
        host=TEST_HOST, ws_id=TEST_WS_ID, req_id=TEST_REQ_ID, index=TEST_INDEX
    )
    if login_admin_and_reset_operation:
        await GulpAPIUser.login_admin_and_reset_operation(TEST_OPERATION_ID)


@pytest_asyncio.fixture(scope="function", autouse=True)
async def _setup():
    """
    this is called before any test, to initialize the environment
    """
    await _init()


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

        ingest_token = await GulpAPIUser.login("ingest", "ingest")
        assert ingest_token

        # ingest the file
        await GulpAPIIngest.ingest_file(
            ingest_token,
            file_path=file_path,
            operation_id=TEST_OPERATION_ID,
            context_name=TEST_CONTEXT_NAME,
            plugin=plugin,
            flt=flt,
            plugin_params=plugin_params,
            file_total=file_total,
        )

    asyncio.run(_process_file_async())


async def _ws_loop(
    check_ingested: int = None,
    check_processed: int = None,
    check_skipped: int = None,
    success: bool = None,
):
    """
    open a websocket and wait for the ingestion to complete, optionally enforcing check of the number of ingested/processed records

    :param check_ingested: if not None, check the number of ingested records
    :param check_processed: if not None, check the number of processed records
    :param check_skipped: if not None, check the number of skipped records
    :param success: if not None, check if the ingestion was successful
    """
    _, host = TEST_HOST.split("://")
    ws_url = f"ws://{host}/ws"
    test_completed = False
    records_ingested = 0
    records_processed = 0
    records_skipped = 0

    async with websockets.connect(ws_url) as ws:
        # connect websocket
        p: GulpWsAuthPacket = GulpWsAuthPacket(token="monitor", ws_id=TEST_WS_ID)
        await ws.send(p.model_dump_json(exclude_none=True))

        # receive responses
        try:
            while True:
                response = await ws.recv()
                data = json.loads(response)

                # wait for the stats update
                if data["type"] == "stats_update":
                    # stats update
                    stats_packet = data["data"]["data"]
                    MutyLogger.get_instance().info(f"ingestion stats: {stats_packet}")
                    records_ingested = stats_packet.get("records_ingested", 0)
                    records_processed = stats_packet.get("records_processed", 0)
                    records_skipped = stats_packet.get("records_skipped", 0)

                    # perform checks
                    skipped_test_succeeded = True
                    processed_test_succeeded = True
                    ingested_test_succeeded = True
                    success_test_succeeded = True
                    if check_ingested is not None:
                        if records_ingested == check_ingested:
                            MutyLogger.get_instance().info(
                                "all %d records ingested!" % (check_ingested)
                            )
                            ingested_test_succeeded = True
                        else:
                            ingested_test_succeeded = False

                    if check_processed is not None:
                        if records_processed == check_processed:
                            MutyLogger.get_instance().info(
                                "all %d records processed!" % (check_processed)
                            )
                            processed_test_succeeded = True
                        else:
                            processed_test_succeeded = False

                    if check_skipped is not None:

                        if records_skipped == check_skipped:
                            MutyLogger.get_instance().info(
                                "all %d records skipped!" % (check_skipped)
                            )
                            skipped_test_succeeded = True
                        else:
                            skipped_test_succeeded = False

                    if success is not None:
                        if stats_packet["status"] == "done":
                            MutyLogger.get_instance().info("success!")
                            success_test_succeeded = True
                        else:
                            success_test_succeeded = False

                    if (
                        ingested_test_succeeded
                        and processed_test_succeeded
                        and skipped_test_succeeded
                        and success_test_succeeded
                    ):
                        test_completed = True
                        break

                    # check for failed/canceled
                    if (
                        stats_packet["status"] == "failed"
                        or stats_packet["status"] == "canceled"
                    ):
                        break

                # ws delay
                await asyncio.sleep(0.1)

        except websockets.exceptions.ConnectionClosed as ex:
            MutyLogger.get_instance().exception(ex)

    MutyLogger.get_instance().info(
        f"found_ingested={records_ingested} (requested={check_ingested}), found_processed={
            records_processed} (requested={check_processed}), found_skipped={records_skipped} (requested={check_skipped})"
    )
    assert test_completed
    MutyLogger.get_instance().info("test succeeded!")


async def _test_generic(
    files: list[str],
    plugin: str,
    check_ingested: int,
    check_processed: int = None,
    plugin_params: GulpPluginParameters = None,
    flt: GulpIngestionFilter = None,
) -> str:
    """
    for each file, spawn a process using multiprocessing and perform ingestion with the selected plugin

    :param files: list of files to ingest
    :param plugin: plugin to use
    :param check_ingested: number of ingested records to check
    :param check_processed: number of processed records to check
    :param plugin_params: plugin parameters
    :param flt: ingestion filter
    """
    await _init(False)

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
    await _ws_loop(check_ingested=check_ingested, check_processed=check_processed)


# @pytest.mark.asyncio
# async def test_csv_standalone_and_query_operations():
#     current_dir = os.path.dirname(os.path.realpath(__file__))
#     files = [os.path.join(current_dir, "../samples/mftecmd/sample_record.csv")]
#     plugin_params = GulpPluginParameters(
#         mappings={
#             "test_mapping": GulpMapping(
#                 fields={"Created0x10": GulpMappingField(ecs="@timestamp")}
#             )
#         }
#     )
#     await _test_generic(files, "csv", 10, plugin_params=plugin_params)

#     # test query operations
#     guest_token = await GulpAPIUser.login("guest", "guest")
#     assert guest_token
#     ops = await GulpAPIQuery.query_operations(guest_token, TEST_INDEX)
#     assert ops[0]["contexts"][0]["plugins"][0]["name"] == "csv"
#     assert (
#         ops[0]["contexts"][0]["plugins"][0]["sources"][0]["min_gulp.timestamp"]
#         == 1258480498794248960
#     )

#     # test query max-min timestamp
#     data = await GulpAPIQuery.query_max_min_per_field(guest_token, TEST_INDEX)
#     assert data["buckets"][0]["*"]["doc_count"] == 10
#     assert data["buckets"][0]["*"]["min_gulp.timestamp"] == 1258480498794248960


#     # TODO: check documents (all documents duration set to 9999 and enriched=True set)


# @pytest.mark.asyncio
# async def test_all():
#     GulpAPICommon.get_instance().init(
#         host=TEST_HOST, ws_id=TEST_WS_ID, req_id=TEST_REQ_ID, index=TEST_INDEX
#     )
#     await GulpAPIDb.reset_all_as_admin()
#     await test_ingest_account()
#     await test_failed_upload()
#     await test_skipped_records()


@pytest.mark.asyncio
@pytest.mark.run(order=1)
async def test_ingest_account():
    """
    test ingest vs guest account (only ingest can ingest)
    """
    current_dir = os.path.dirname(os.path.realpath(__file__))
    samples_dir = os.path.join(current_dir, "../samples/win_evtx")
    file_path = os.path.join(samples_dir, "Security_short_selected.evtx")

    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token

    # ingest the file (guest cannot)
    await GulpAPIIngest.ingest_file(
        token=guest_token,
        file_path=file_path,
        operation_id=TEST_OPERATION_ID,
        context_name=TEST_CONTEXT_NAME,
        plugin="win_evtx",
        expected_status=401,
    )

    ingest_token = await GulpAPIUser.login("ingest", "ingest")
    assert ingest_token

    # ingest the file
    await GulpAPIIngest.ingest_file(
        token=ingest_token,
        file_path=file_path,
        operation_id=TEST_OPERATION_ID,
        context_name=TEST_CONTEXT_NAME,
        plugin="win_evtx",
    )

    await _ws_loop(check_ingested=7, check_processed=7)
    MutyLogger.get_instance().info(test_ingest_account.__name__ + " succeeded!")


@pytest.mark.asyncio
@pytest.mark.run(order=2)
async def test_failed_upload():
    """
    simulate a failed upload and reupload with resume after
    """
    current_dir = os.path.dirname(os.path.realpath(__file__))
    samples_dir = os.path.join(current_dir, "../samples/win_evtx")
    file_path = os.path.join(samples_dir, "Security_short_selected.evtx")

    ingest_token = await GulpAPIUser.login("ingest", "ingest")
    assert ingest_token

    # get full file size
    file_size = os.path.getsize(file_path)
    file_sha1 = await muty.crypto.hash_sha1_file(file_path)

    # copy to a temporary file, using a smaller size
    tmp_dir = os.path.join(tempfile.gettempdir(), "gulp")
    os.makedirs(tmp_dir, exist_ok=True)
    temp_file_path = os.path.join(tmp_dir, os.path.basename(file_path))
    with open(file_path, "rb") as f:
        with open(temp_file_path, "wb") as f2:
            f2.write(f.read(file_size - 100))
    try:
        # ingest the partial file, it will fail
        await GulpAPIIngest.ingest_file(
            token=ingest_token,
            file_path=temp_file_path,
            operation_id=TEST_OPERATION_ID,
            context_name=TEST_CONTEXT_NAME,
            plugin="win_evtx",
            file_sha1=file_sha1,
            total_file_size=file_size,
            expected_status=206,
        )

        # ingest the real file, starting from file_size - 100
        await GulpAPIIngest.ingest_file(
            token=ingest_token,
            file_path=file_path,
            operation_id=TEST_OPERATION_ID,
            context_name=TEST_CONTEXT_NAME,
            plugin="win_evtx",
            file_sha1=file_sha1,
            total_file_size=file_size,
            restart_from=file_size - 100,
        )

        await _ws_loop(check_ingested=7, check_processed=7)
    finally:
        shutil.rmtree(tmp_dir)
    MutyLogger.get_instance().info(test_failed_upload.__name__ + " succeeded!")


@pytest.mark.asyncio
@pytest.mark.run(order=3)
async def test_skipped_records():
    """
    simulate skipped records due to duplicate ingestion
    """
    current_dir = os.path.dirname(os.path.realpath(__file__))
    samples_dir = os.path.join(current_dir, "../samples/win_evtx")
    file_path = os.path.join(samples_dir, "Security_short_selected.evtx")

    ingest_token = await GulpAPIUser.login("ingest", "ingest")
    assert ingest_token

    # ingest first
    await GulpAPIIngest.ingest_file(
        token=ingest_token,
        file_path=file_path,
        operation_id=TEST_OPERATION_ID,
        context_name=TEST_CONTEXT_NAME,
        plugin="win_evtx",
    )
    await _ws_loop(check_ingested=7, check_processed=7)

    # ingest same file again, use another req_id
    await GulpAPIIngest.ingest_file(
        token=ingest_token,
        file_path=file_path,
        operation_id=TEST_OPERATION_ID,
        context_name=TEST_CONTEXT_NAME,
        plugin="win_evtx",
        req_id="req_id_2",
    )
    await _ws_loop(check_ingested=0, check_processed=7, check_skipped=7)
    MutyLogger.get_instance().info(test_skipped_records.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_win_evtx_multiple():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    samples_dir = os.path.join(current_dir, "../samples/win_evtx")
    files = muty.file.list_directory(samples_dir, recursive=True, files_only=True)
    await _test_generic(files, plugin="win_evtx", check_ingested=98633)
    MutyLogger.get_instance().info(test_win_evtx_multiple.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_ingest_ws_raw():
    def _generate_random_chunk(template_chunk: dict, size=1000):
        """
        randomize the given template chunk
        """
        base_timestamp = datetime(2019, 7, 1)
        result = []
        contexts = ["context1", "context2", "context3"]
        sources = ["source1", "source2", "source3"]

        for i in range(size):
            new_docs = []
            for doc in template_chunk:
                new_doc = {}
                for key, value in doc.items():
                    if key == "@timestamp":
                        # sequential timestamps
                        new_doc[key] = (
                            base_timestamp + timedelta(minutes=i)
                        ).isoformat() + ".000Z"
                    elif isinstance(value, str):
                        if key == "event.code":
                            new_doc[key] = (
                                f"event_code_{
                                random.randint(1, 1000)}"
                            )
                        else:
                            # leave gulp.operation_id as is
                            if key == "gulp.operation_id":
                                new_doc[key] = value
                            elif key == "gulp.context_id":
                                # use one of the contexts
                                new_doc[key] = contexts[random.randint(0, 2)]
                            elif key == "gulp.source_id":
                                # use one of the sources
                                new_doc[key] = sources[random.randint(0, 2)]
                            else:
                                # random string of similar length
                                new_doc[key] = "".join(
                                    random.choices(
                                        string.ascii_letters + string.digits,
                                        k=len(value),
                                    )
                                )
                    elif isinstance(value, int):
                        # Random integer between 0 and 1000
                        new_doc[key] = random.randint(0, 1000)
                new_docs.append(new_doc)
            result.extend(new_docs)
        return result

    current_dir = os.path.dirname(os.path.realpath(__file__))
    raw_chunk_path = os.path.join(current_dir, "raw_chunk.json")
    buf = await muty.file.read_file_async(raw_chunk_path)
    raw_chunk = json.loads(buf)

    ingest_token = await GulpAPIUser.login("ingest", "ingest")
    assert ingest_token

    _, host = TEST_HOST.split("://")
    ws_url = f"ws://{host}/ws_ingest_raw"
    test_completed = False

    async with websockets.connect(ws_url) as ws:
        # connect websocket
        p: GulpWsAuthPacket = GulpWsAuthPacket(
            token=ingest_token, ws_id=TEST_WS_ID + "_ingest_raw"
        )
        await ws.send(p.model_dump_json(exclude_none=True))

        # receive responses
        try:
            while True:
                response = await ws.recv()
                data = json.loads(response)
                if data["type"] == "ws_connected":
                    for i in range(2):
                        # send chunk
                        p: GulpWsIngestPacket = GulpWsIngestPacket(
                            docs=_generate_random_chunk(raw_chunk, size=1000),
                            index=TEST_INDEX,
                            operation_id=TEST_OPERATION_ID,
                            context_name=TEST_CONTEXT_NAME,
                            source="test_source",
                            req_id=TEST_REQ_ID,
                            ws_id=TEST_WS_ID,
                        )
                        await ws.send(p.model_dump_json(exclude_none=True))
                        await asyncio.sleep(0.1)

                    # TODO: check data, but should be ok ....
                    test_completed = True
                    break

                # ws delay
                await asyncio.sleep(0.1)

        except websockets.exceptions.ConnectionClosed as ex:
            MutyLogger.get_instance().exception(ex)

    assert test_completed
    await asyncio.sleep(10)
    op = await GulpAPIOperation.operation_get_by_id(
        token=ingest_token, operation_id=TEST_OPERATION_ID
    )
    assert op["doc_count"] == 6000
    MutyLogger.get_instance().info(test_ingest_ws_raw.__name__ + " succeeded!")


@pytest.mark.asyncio
@pytest.mark.run(order=4)
async def test_ingest_filter():
    """
    test ingestion filter
    """
    current_dir = os.path.dirname(os.path.realpath(__file__))
    samples_dir = os.path.join(current_dir, "../samples/win_evtx")
    file_path = os.path.join(samples_dir, "Security_short_selected.evtx")

    ingest_token = await GulpAPIUser.login("ingest", "ingest")
    assert ingest_token

    # ingest the file
    flt = GulpIngestionFilter(time_range=[0, 1467213874345999999])
    await GulpAPIIngest.ingest_file(
        token=ingest_token,
        file_path=file_path,
        operation_id=TEST_OPERATION_ID,
        context_name=TEST_CONTEXT_NAME,
        plugin="win_evtx",
        flt=flt,
    )

    await _ws_loop(check_ingested=1, check_processed=7)

    await GulpAPIUser.login_admin_and_reset_operation(TEST_OPERATION_ID)

    flt = GulpIngestionFilter(time_range=[1467213874345999999, 0])
    await GulpAPIIngest.ingest_file(
        token=ingest_token,
        file_path=file_path,
        operation_id=TEST_OPERATION_ID,
        context_name=TEST_CONTEXT_NAME,
        plugin="win_evtx",
        flt=flt,
    )

    await _ws_loop(check_ingested=6, check_processed=7)
    MutyLogger.get_instance().info(test_ingest_filter.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_csv_standalone():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [os.path.join(current_dir, "../samples/mftecmd/sample_record.csv")]
    plugin_params = GulpPluginParameters(
        mappings={
            "test_mapping": GulpMapping(
                fields={"Created0x10": GulpMappingField(ecs="@timestamp")}
            )
        }
    )
    await _test_generic(files, "csv", check_ingested=10, plugin_params=plugin_params)
    MutyLogger.get_instance().info(test_csv_standalone.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_csv_file_mapping():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [os.path.join(current_dir, "../samples/mftecmd/sample_record.csv")]
    plugin_params = GulpPluginParameters(
        mapping_file="mftecmd_csv.json", mapping_id="record"
    )
    await _test_generic(
        files, "csv", check_ingested=44, check_processed=10, plugin_params=plugin_params
    )
    MutyLogger.get_instance().info(test_csv_file_mapping.__name__ + " succeeded!")


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
    await _test_generic(
        files, "stacked_example", check_ingested=10, plugin_params=plugin_params
    )

    # check at least one document ...
    guest_token = await GulpAPIUser.login("guest", "guest")
    doc = await GulpAPIQuery.query_single_id(
        guest_token, TEST_OPERATION_ID, "d3bd618f59c8b001d77c6c8edc729b0a"
    )
    assert doc["event.duration"] == 9999
    assert doc["enriched"] == True
    MutyLogger.get_instance().info(test_csv_stacked.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_ingest_zip():
    ingest_token = await GulpAPIUser.login("ingest", "ingest")
    assert ingest_token

    current_dir = os.path.dirname(os.path.realpath(__file__))
    test_zip = os.path.join(current_dir, "test_ingest_zip.zip")

    # ingest raw chunk
    await GulpAPIIngest.ingest_zip(
        token=ingest_token,
        file_path=test_zip,
        operation_id=TEST_OPERATION_ID,
        context_name=TEST_CONTEXT_NAME,
    )

    # wait ws
    await _ws_loop(check_ingested=13779, check_processed=13745)
    MutyLogger.get_instance().info(test_ingest_zip.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_raw():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    raw_chunk_path = os.path.join(current_dir, "raw_chunk.json")
    buf = await muty.file.read_file_async(raw_chunk_path)
    raw_chunk = json.loads(buf)

    ingest_token = await GulpAPIUser.login("ingest", "ingest")
    assert ingest_token

    # ingest raw chunk with "raw" plugin
    await GulpAPIIngest.ingest_raw(
        token=ingest_token,
        raw_data=raw_chunk,
        operation_id=TEST_OPERATION_ID,
    )
    # wait ws
    await _ws_loop(check_ingested=3)  # , check_on_source_done=True)

    # ingest another
    for r in raw_chunk:
        # randomize event original
        r["event.original"] = muty.string.generate_unique()
    await GulpAPIIngest.ingest_raw(
        token=ingest_token,
        raw_data=raw_chunk,
        operation_id=TEST_OPERATION_ID,
    )

    MutyLogger.get_instance().info(test_raw.__name__ + " succeeded!")


@pytest.mark.skipif(
    platform.system() == "Darwin", reason="systemd journal tests not supported on macOS"
)
@pytest.mark.asyncio
async def test_systemd_journal():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [os.path.join(current_dir, "../samples/systemd_journal/system.journal")]
    await _test_generic(files, "systemd_journal", 9243)
    MutyLogger.get_instance().info(test_systemd_journal.__name__ + " succeeded!")


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
@pytest.mark.run("last")
async def test_paid_plugins():
    import importlib
    import sys

    current_dir = os.path.dirname(os.path.realpath(__file__))
    file_path = os.path.join(current_dir, "../../gulp-paid-plugins/tests/ingest.py")

    module_name = "paidplugins"
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    await module.test_all()
