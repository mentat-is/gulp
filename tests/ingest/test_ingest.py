import asyncio
import json
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

from gulp.api.mapping.models import GulpMapping, GulpMappingField
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp_client.common import (
    _ensure_test_operation,
    _test_ingest_generic,
    _test_ingest_ws_loop,
)
from gulp_client.ingest import GulpAPIIngest
from gulp_client.operation import GulpAPIOperation
from gulp_client.query import GulpAPIQuery
from gulp_client.user import GulpAPIUser
from gulp_client.test_values import (
    TEST_CONTEXT_NAME,
    TEST_HOST,
    TEST_INDEX,
    TEST_OPERATION_ID,
    TEST_REQ_ID,
    TEST_WS_ID,
)
from gulp.api.ws_api import GulpWsAuthPacket, GulpWsIngestPacket
from gulp.structs import GulpMappingParameters, GulpPluginParameters


@pytest_asyncio.fixture(scope="function", autouse=True)
async def _setup():
    """
    this is called before any test, to initialize the environment
    """
    await _ensure_test_operation()


@pytest.mark.asyncio
@pytest.mark.run(order=1)
async def test_ingest_account():
    """
    test guest account cannot ingest
    """
    current_dir = os.path.dirname(os.path.realpath(__file__))
    samples_dir = os.path.join(current_dir, "../../samples/win_evtx")
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
    MutyLogger.get_instance().info(test_ingest_account.__name__ + " succeeded!")


@pytest.mark.asyncio
@pytest.mark.run(order=2)
async def test_failed_upload():
    """
    simulate a failed upload and reupload with resume after
    """
    current_dir = os.path.dirname(os.path.realpath(__file__))
    samples_dir = os.path.join(current_dir, "../../samples/win_evtx")
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

        await _test_ingest_ws_loop(check_ingested=7, check_processed=7)
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
    samples_dir = os.path.join(current_dir, "../../samples/win_evtx")
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
    await _test_ingest_ws_loop(check_ingested=7, check_processed=7)

    # ingest same file again, use another req_id
    await GulpAPIIngest.ingest_file(
        token=ingest_token,
        file_path=file_path,
        operation_id=TEST_OPERATION_ID,
        context_name=TEST_CONTEXT_NAME,
        plugin="win_evtx",
        req_id="req_id_2",
    )
    await _test_ingest_ws_loop(check_ingested=0, check_processed=7, check_skipped=7)
    MutyLogger.get_instance().info(test_skipped_records.__name__ + " succeeded!")


@pytest.mark.asyncio
@pytest.mark.run(order=4)
async def test_ingest_filter():
    """
    test ingestion filter
    """
    current_dir = os.path.dirname(os.path.realpath(__file__))
    samples_dir = os.path.join(current_dir, "../../samples/win_evtx")
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

    await _test_ingest_ws_loop(check_ingested=1, check_processed=7)

    # ingest another part
    from gulp_client.common import _ensure_test_operation

    await _ensure_test_operation()
    flt = GulpIngestionFilter(time_range=[1467213874345999999, 0])
    await GulpAPIIngest.ingest_file(
        token=ingest_token,
        file_path=file_path,
        operation_id=TEST_OPERATION_ID,
        context_name=TEST_CONTEXT_NAME,
        plugin="win_evtx",
        flt=flt,
    )

    await _test_ingest_ws_loop(check_ingested=6, check_processed=7)
    MutyLogger.get_instance().info(test_ingest_filter.__name__ + " succeeded!")


@pytest.mark.asyncio
@pytest.mark.run(order=5)
async def test_raw(raw_data: list[dict] = None):
    """
    use raw_data for single test on such buffer
    """

    # TODO: only covers "raw gulpdocuments" plugin (raw)
    if raw_data:
        # use provided
        check_size = len(raw_data)
        buf = json.dumps(raw_data).encode("utf-8")
    else:
        current_dir = os.path.dirname(os.path.realpath(__file__))
        raw_chunk_path = os.path.join(current_dir, "raw_chunk.json")
        buf = await muty.file.read_file_async(raw_chunk_path)

    ingest_token = await GulpAPIUser.login("ingest", "ingest")
    assert ingest_token

    # ingest raw chunk with "raw" plugin
    await GulpAPIIngest.ingest_raw(
        token=ingest_token,
        raw_data=buf,
        operation_id=TEST_OPERATION_ID,
        # if we're passing raw_data (as in the enrich_whois test, this is the onlyu chunk)
        last=False,
    )
    if not raw_data:
        # ingest another (generate new random data)
        raw_chunk = json.loads(buf)
        MutyLogger.get_instance().debug("ingesting another chunk ...")
        for r in raw_chunk:
            # randomize event original
            r["event.original"] = muty.string.generate_unique()
        buf = json.dumps(raw_chunk).encode("utf-8")
        await GulpAPIIngest.ingest_raw(
            token=ingest_token,
            raw_data=buf,
            last=True,
            operation_id=TEST_OPERATION_ID,
        )
        check_size = 6  # plus the 3 above, we're using the same req_id

    await asyncio.sleep(10)
    op = await GulpAPIOperation.operation_get_by_id(
        ingest_token, TEST_OPERATION_ID, get_count=True
    )
    assert op["doc_count"] == check_size
    MutyLogger.get_instance().info(test_raw.__name__ + " succeeded!")


@pytest.mark.asyncio
@pytest.mark.run(order=6)
async def test_ws_raw():
    """
    tests websocket ingestion of raw data
    """

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
                            new_doc[key] = f"event_code_{random.randint(1, 1000)}"
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
        p: GulpWsAuthPacket = GulpWsAuthPacket(token=ingest_token, ws_id=TEST_WS_ID)
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
                            index=TEST_INDEX,
                            operation_id=TEST_OPERATION_ID,
                            context_name=TEST_CONTEXT_NAME,
                            source="test_source",
                            req_id=TEST_REQ_ID,
                            ws_id=TEST_WS_ID,
                        )
                        raw_data = json.dumps(
                            _generate_random_chunk(raw_chunk, size=1000)
                        ).encode("utf-8")

                        # send json then chunk
                        await ws.send(p.model_dump_json(exclude_none=True))
                        await ws.send(raw_data)
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
    MutyLogger.get_instance().info(test_ws_raw.__name__ + " succeeded!")


@pytest.mark.asyncio
@pytest.mark.run(order=7)
async def test_ingest_preview():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    samples_dir = os.path.join(current_dir, "../../samples/win_evtx")
    file_path = os.path.join(samples_dir, "Security_short_selected.evtx")

    ingest_token = await GulpAPIUser.login("ingest", "ingest")
    assert ingest_token

    # ingest the file
    d: dict = await GulpAPIIngest.ingest_file(
        token=ingest_token,
        file_path=file_path,
        operation_id=TEST_OPERATION_ID,
        context_name=TEST_CONTEXT_NAME,
        plugin="win_evtx",
        preview_mode=True,
    )
    assert len(d) == 7
    MutyLogger.get_instance().info(test_ingest_preview.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_ingest_offset():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    samples_dir = os.path.join(current_dir, "../../samples/win_evtx")
    file_path = os.path.join(samples_dir, "Security_short_selected.evtx")

    ingest_token = await GulpAPIUser.login("ingest", "ingest")
    assert ingest_token

    # ingest the file
    one_day_msec = 1000 * 60 * 60 * 24
    await GulpAPIIngest.ingest_file(
        token=ingest_token,
        file_path=file_path,
        operation_id=TEST_OPERATION_ID,
        context_name=TEST_CONTEXT_NAME,
        plugin="win_evtx",
        plugin_params=GulpPluginParameters(timestamp_offset_msec=one_day_msec),
    )
    await _test_ingest_ws_loop(check_ingested=7, check_processed=7)

    # get doc by id
    # source_id = "64e7c3a4013ae243aa13151b5449aac884e36081"
    target_id = "50edff98db7773ef04378ec20a47f622"
    doc = await GulpAPIQuery.query_single_id(ingest_token, TEST_OPERATION_ID, target_id)
    assert doc["_id"] == target_id
    assert doc["@timestamp"] == "2016-06-30T15:24:34.346000+00:00"
    assert doc["gulp.timestamp"] == 1467300274345999872  # 1467213874345999872 + 1 day
    MutyLogger.get_instance().info(test_ingest_offset.__name__ + " passed")


@pytest.mark.asyncio
async def test_win_evtx(file_path: str = None, skip_checks: bool = False):
    current_dir = os.path.dirname(os.path.realpath(__file__))
    samples_dir = os.path.join(current_dir, "../../samples/win_evtx")
    if file_path is None:
        file_path = os.path.join(samples_dir, "Security_short_selected.evtx")

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

    await _test_ingest_ws_loop(
        check_ingested=7, check_processed=7, skip_checks=skip_checks
    )
    MutyLogger.get_instance().info(test_win_evtx.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_win_evtx_multiple():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    samples_dir = os.path.join(current_dir, "../../samples/win_evtx")
    files = muty.file.list_directory(samples_dir, recursive=True, files_only=True)
    await _test_ingest_generic(files, plugin="win_evtx", check_ingested=98633)
    MutyLogger.get_instance().info(test_win_evtx_multiple.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_csv_standalone():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [os.path.join(current_dir, "../../samples/mftecmd/sample_record.csv")]
    plugin_params = GulpPluginParameters(
        mapping_parameters=GulpMappingParameters(
            mappings={
                "test_mapping": GulpMapping(
                    fields={"Created0x10": GulpMappingField(ecs="@timestamp")}
                )
            }
        )
    )
    await _test_ingest_generic(
        files, "csv", check_ingested=10, plugin_params=plugin_params
    )
    MutyLogger.get_instance().info(test_csv_standalone.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_csv_file_mapping():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [os.path.join(current_dir, "../../samples/mftecmd/sample_record.csv")]
    plugin_params = GulpPluginParameters(
        mapping_parameters=GulpMappingParameters(
            mapping_file="mftecmd_csv.json", mapping_id="record"
        )
    )
    await _test_ingest_generic(
        files, "csv", check_ingested=44, check_processed=10, plugin_params=plugin_params
    )
    MutyLogger.get_instance().info(test_csv_file_mapping.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_csv_stacked():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [os.path.join(current_dir, "../../samples/mftecmd/sample_record.csv")]
    plugin_params = GulpPluginParameters(
        mapping_parameters=GulpMappingParameters(
            mappings={
                "test_mapping": GulpMapping(
                    fields={"Created0x10": GulpMappingField(ecs="@timestamp")}
                )
            }
        )
    )
    await _test_ingest_generic(
        files, "stacked_example", check_ingested=10, plugin_params=plugin_params
    )

    # check at least one document ...
    guest_token = await GulpAPIUser.login("guest", "guest")
    doc = await GulpAPIQuery.query_single_id(
        guest_token, TEST_OPERATION_ID, "903bd0a1ecb33ce4b3fec4a5575c9085"
    )
    assert doc["event.duration"] == 9999
    assert doc["enriched"]
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
    await _test_ingest_ws_loop(check_ingested=13779, check_processed=13745)
    MutyLogger.get_instance().info(test_ingest_zip.__name__ + " succeeded!")


@pytest.mark.skipif(
    platform.system() == "Darwin", reason="systemd journal tests not supported on macOS"
)
@pytest.mark.asyncio
async def test_systemd_journal():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [os.path.join(current_dir, "../../samples/systemd_journal/system.journal")]
    await _test_ingest_generic(files, "systemd_journal", 9243)
    MutyLogger.get_instance().info(test_systemd_journal.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_win_reg():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [os.path.join(current_dir, "../../samples/win_reg/NTUSER.DAT")]
    await _test_ingest_generic(files, "win_reg", 1206)
    MutyLogger.get_instance().info(test_win_reg.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_eml():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [os.path.join(current_dir, "../../samples/eml/sample.eml")]
    await _test_ingest_generic(files, "eml", 1)
    MutyLogger.get_instance().info(test_eml.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_mbox():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [os.path.join(current_dir, "../../samples/mbox/sample.mbox")]
    await _test_ingest_generic(files, "mbox", 16)
    MutyLogger.get_instance().info(test_mbox.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_chrome_history():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [os.path.join(current_dir, "../../samples/sqlite/chrome_history")]
    await _test_ingest_generic(files, "chrome_history_sqlite_stacked", 19)
    MutyLogger.get_instance().info(test_chrome_history.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_chrome_webdata():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [os.path.join(current_dir, "../../samples/sqlite/chrome_webdata")]
    await _test_ingest_generic(
        files, "chrome_webdata_sqlite_stacked", 2, check_processed=1
    )
    MutyLogger.get_instance().info(test_chrome_webdata.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_pfsense():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [os.path.join(current_dir, "../../samples/pfsense/filter.log")]
    await _test_ingest_generic(files, "pfsense", 61)
    MutyLogger.get_instance().info(test_pfsense.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_pcap():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [
        os.path.join(current_dir, "../../samples/pcap/220614_ip_flags_google.pcapng")
    ]
    await _test_ingest_generic(files, "pcap", 58)
    MutyLogger.get_instance().info(test_pcap.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_teamviewer_regex_stacked():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [
        os.path.join(current_dir, "../../samples/teamviewer/connections_incoming.txt")
    ]
    await _test_ingest_generic(files, "teamviewer_regex_stacked", 2)
    MutyLogger.get_instance().info(
        test_teamviewer_regex_stacked.__name__ + " succeeded!"
    )


@pytest.mark.asyncio
async def test_apache_access_clf():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [os.path.join(current_dir, "../../samples/apache_clf/access.log")]
    await _test_ingest_generic(files, "apache_access_clf", 1311)
    MutyLogger.get_instance().info(test_apache_access_clf.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_apache_error_clf():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [os.path.join(current_dir, "../../samples/apache_clf/error.log")]
    await _test_ingest_generic(files, "apache_error_clf", 1178)
    MutyLogger.get_instance().info(test_apache_error_clf.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_iis_access():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [os.path.join(current_dir, "../../samples/iis_access/iis.log")]
    await _test_ingest_generic(files, "iis_access", 2)
    MutyLogger.get_instance().info(test_iis_access.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_iis_access_w3c():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [os.path.join(current_dir, "../../samples/iis_access_w3c/iis_w3c.log")]
    await _test_ingest_generic(files, "iis_access_w3c", 5)
    MutyLogger.get_instance().info(test_iis_access_w3c.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_iis_access_ncsa():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [os.path.join(current_dir, "../../samples/iis_access_ncsa/iis_ncsa.log")]
    await _test_ingest_generic(files, "iis_access_ncsa", 4)
    MutyLogger.get_instance().info(test_iis_access_ncsa.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_lin_syslog():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [os.path.join(current_dir, "../../samples/lin_syslog/syslog")]
    await _test_ingest_generic(files, "lin_syslog", 2548)
    MutyLogger.get_instance().info(test_lin_syslog.__name__ + " (syslog) succeeded!")

    # another file, reset first
    await _ensure_test_operation()
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [os.path.join(current_dir, "../../samples/lin_syslog/auth.log")]
    await _test_ingest_generic(files, "lin_syslog", 70)
    MutyLogger.get_instance().info(test_lin_syslog.__name__ + " (auth.log) succeeded!")


# @pytest.mark.asyncio
# disabled for now, too long (it works, anyway)
async def test_json_list():
    current_dir = os.path.dirname(os.path.realpath(__file__))

    # list
    files = [os.path.join(current_dir, "../../samples/json/million_logs.json")]
    mp = GulpMappingParameters(
        mappings={
            "test_mapping": GulpMapping(
                fields={"create_datetime": GulpMappingField(ecs="@timestamp")}
            )
        }
    )
    plugin_params = GulpPluginParameters(
        custom_parameters={"mode": "list"},
        mapping_parameters=mp,
    )
    await _test_ingest_generic(files, "json", 1000000, plugin_params=plugin_params)
    MutyLogger.get_instance().info(test_json.__name__ + " (list) succeeded!")


@pytest.mark.asyncio
async def test_zeek_conn_file_mapping():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    zeek_mapping = {
        "conn": {
            "path": [os.path.join(current_dir, "../../samples/zeek/conn.log")],
            "count": 34,
        },
        "dns": {
            "path": [os.path.join(current_dir, "../../samples/zeek/dns.log")],
            "count": 4,
        },
        "http": {
            "path": [os.path.join(current_dir, "../../samples/zeek/http.log")],
            "count": 5,
        },
        "file": {
            "path": [os.path.join(current_dir, "../../samples/zeek/files.log")],
            "count": 11,
        },
        "ftp": {
            "path": [os.path.join(current_dir, "../../samples/zeek/ftp.log")],
            "count": 4,
        },
        "ssl": {
            "path": [os.path.join(current_dir, "../../samples/zeek/ssl.log")],
            "count": 13,
        },
        "x509": {
            "path": [os.path.join(current_dir, "../../samples/zeek/x509.log")],
            "count": 7,
        },
        "smtp": {
            "path": [os.path.join(current_dir, "../../samples/zeek/smtp.log")],
            "count": 2,
        },
        "ssh": {
            "path": [os.path.join(current_dir, "../../samples/zeek/ssh.log")],
            "count": 5,
        },
        "pe": {
            "path": [os.path.join(current_dir, "../../samples/zeek/pe.log")],
            "count": 4,
        },
        "dhcp": {
            "path": [os.path.join(current_dir, "../../samples/zeek/dhcp.log")],
            "count": 4,
        },
        "ntp": {
            "path": [os.path.join(current_dir, "../../samples/zeek/ntp.log")],
            "count": 2,
        },
        "notice": {
            "path": [os.path.join(current_dir, "../../samples/zeek/notice.log")],
            "count": 6,
        },
        "dce_rpc": {
            "path": [os.path.join(current_dir, "../../samples/zeek/dce_rpc.log")],
            "count": 3,
        },
        "kerberos": {
            "path": [os.path.join(current_dir, "../../samples/zeek/kerberos.log")],
            "count": 5,
        },
        "smb_mapping": {
            "path": [os.path.join(current_dir, "../../samples/zeek/smb_mapping.log")],
            "count": 5,
        },
        "smb_files": {
            "path": [os.path.join(current_dir, "../../samples/zeek/smb_files.log")],
            "count": 7,
        },
        "ntlm": {
            "path": [os.path.join(current_dir, "../../samples/zeek/ntlm.log")],
            "count": 7,
        },
        "irc": {
            "path": [os.path.join(current_dir, "../../samples/zeek/irc.log")],
            "count": 11,
        },
        "ldap": {
            "path": [os.path.join(current_dir, "../../samples/zeek/ldap.log")],
            "count": 2,
        },
        "postgresql": {
            "path": [os.path.join(current_dir, "../../samples/zeek/postgresql.log")],
            "count": 3,
        },
        "quic": {
            "path": [os.path.join(current_dir, "../../samples/zeek/quic.log")],
            "count": 1,
        },
        "rdp": {
            "path": [os.path.join(current_dir, "../../samples/zeek/rdp.log")],
            "count": 1,
        },
        "traceroute": {
            "path": [os.path.join(current_dir, "../../samples/zeek/traceroute.log")],
            "count": 4,
        },
        "tunnel": {
            "path": [os.path.join(current_dir, "../../samples/zeek/tunnel.log")],
            "count": 8,
        },
        "known_certs": {
            "path": [os.path.join(current_dir, "../../samples/zeek/known_certs.log")],
            "count": 1,
        },
        "known_hosts": {
            "path": [os.path.join(current_dir, "../../samples/zeek/known_hosts.log")],
            "count": 5,
        },
        "known_services": {
            "path": [
                os.path.join(current_dir, "../../samples/zeek/known_services.log")
            ],
            "count": 20,
        },
        "software": {
            "path": [os.path.join(current_dir, "../../samples/zeek/software.log")],
            "count": 4,
        },
        "weird": {
            "path": [os.path.join(current_dir, "../../samples/zeek/weird.log")],
            "count": 2,
        },
    }
    total_doc_injested = 0
    for key, value in zeek_mapping.items():
        total_doc_injested += value["count"]
        await _test_zeek_ingest(value["path"], key, total_doc_injested)

    MutyLogger.get_instance().info(test_zeek_conn_file_mapping.__name__ + " succeeded!")


async def _test_zeek_ingest(files: list, mapping_id: str, check_ingested: int):
    plugin_params = GulpPluginParameters(
        custom_parameters={"mode": "line"},
        mapping_parameters=GulpMappingParameters(
            mapping_file="zeek.json", mapping_id=mapping_id
        ),
    )

    MutyLogger.get_instance().info(
        f"\n\n*********************\n START INGEST\n Mapping: {mapping_id}\n Ingest {check_ingested} doc \n*********************\n"
    )

    await _test_ingest_generic(
        files,
        "json",
        check_ingested=check_ingested,
        plugin_params=plugin_params,
    )

    MutyLogger.get_instance().info(
        f"\n\n*********************\n END INGEST\n Mapping: {mapping_id}\n Ingest {check_ingested} doc \n*********************\n"
    )


@pytest.mark.asyncio
async def test_json():
    # line
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [os.path.join(current_dir, "../../samples/json/jsonline.json")]
    mp = GulpMappingParameters(
        mappings={
            "test_mapping": GulpMapping(
                fields={"create_datetime": GulpMappingField(ecs="@timestamp")}
            )
        }
    )
    plugin_params = GulpPluginParameters(
        custom_parameters={"mode": "line"},
        mapping_parameters=mp,
    )
    await _test_ingest_generic(files, "json", 5, plugin_params=plugin_params)
    MutyLogger.get_instance().info(test_json.__name__ + " (line) succeeded!")

    # list
    await _ensure_test_operation()
    files = [os.path.join(current_dir, "../../samples/json/jsonlist.json")]
    plugin_params = GulpPluginParameters(
        custom_parameters={"mode": "list"}, mapping_parameters=mp
    )
    await _test_ingest_generic(files, "json", 5, plugin_params=plugin_params)
    MutyLogger.get_instance().info(test_json.__name__ + " (list) succeeded!")

    # dict
    await _ensure_test_operation()
    files = [os.path.join(current_dir, "../../samples/json/jsondict.json")]
    plugin_params = GulpPluginParameters(
        custom_parameters={"mode": "dict"}, mapping_parameters=mp
    )
    await _test_ingest_generic(files, "json", 5, plugin_params=plugin_params)
    MutyLogger.get_instance().info(test_json.__name__ + " (dict) succeeded!")


@pytest.mark.asyncio
async def test_mysql_error():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [os.path.join(current_dir, "../../samples/mysql_error/mysql_error.log")]
    await _test_ingest_generic(files, "mysql_error", 61)
    MutyLogger.get_instance().info(test_mysql_error.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_mysql_general():
    # TODO: broken
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = [
        os.path.join(current_dir, "../../samples/mysql_general/example.general.log")
    ]
    await _test_ingest_generic(files, "mysql_general", 4056)
    MutyLogger.get_instance().info(test_mysql_general.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_ingest_to_source(file_path: str = None, skip_checks: bool = False):
    # ingest first evtx first
    await test_win_evtx(file_path=file_path, skip_checks=skip_checks)
    source_id: str = "64e7c3a4013ae243aa13151b5449aac884e36081"

    # then another file
    ingest_token = await GulpAPIUser.login("ingest", "ingest")
    assert ingest_token
    current_dir = os.path.dirname(os.path.realpath(__file__))
    samples_dir = os.path.join(current_dir, "../../samples/win_evtx")
    file_path = os.path.join(samples_dir, "Application.evtx")
    await GulpAPIIngest.ingest_file_to_source(
        token=ingest_token,
        file_path=os.path.join(samples_dir, file_path),
        source_id=source_id,
        req_id="new_req",
    )

    await _test_ingest_ws_loop(
        check_ingested=6419, check_processed=6419, skip_checks=skip_checks
    )

    # now check operation total
    op = await GulpAPIOperation.operation_get_by_id(ingest_token, TEST_OPERATION_ID)
    assert op["doc_count"] == 6426
    MutyLogger.get_instance().info(test_ingest_to_source.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_ingest_file_local():
    # get working directory
    wrk_dir = os.getenv("GULP_WORKING_DIR", None)
    if not wrk_dir:
        home_path = os.path.expanduser("~")
        wrk_dir = os.path.join(home_path, ".config/gulp")
    ingest_local_dir = os.path.join(wrk_dir, "ingest_local")

    # get sample file path
    current_dir = os.path.dirname(os.path.realpath(__file__))
    samples_dir = os.path.join(current_dir, "../../samples/win_evtx")

    # copy to ingest_local
    src_path = os.path.join(samples_dir, "Security_short_selected.evtx")
    dest_path = os.path.join(ingest_local_dir, "Security_short_selected.evtx")
    await muty.file.copy_file_async(src_path, dest_path)
    MutyLogger.get_instance().info(f"copied {src_path} to {dest_path}")

    ingest_token = await GulpAPIUser.login("ingest", "ingest")
    assert ingest_token

    # ingest the file
    await GulpAPIIngest.ingest_file_local(
        token=ingest_token,
        file_path=dest_path,
        operation_id=TEST_OPERATION_ID,
        context_name=TEST_CONTEXT_NAME,
        plugin="win_evtx",
    )

    await _test_ingest_ws_loop(check_ingested=7, check_processed=7)
    MutyLogger.get_instance().info(test_ingest_file_local.__name__ + " succeeded!")


@pytest.mark.asyncio
async def test_ingest_file_local_to_source():
    # ingest first file
    await test_ingest_file_local()

    # get working directory
    source_id: str = "64e7c3a4013ae243aa13151b5449aac884e36081"
    wrk_dir = os.getenv("GULP_WORKING_DIR", None)
    if not wrk_dir:
        home_path = os.path.expanduser("~")
        wrk_dir = os.path.join(home_path, ".config/gulp")
    else:
        MutyLogger.get_instance().info(f"using GULP_WORKING_DIR={wrk_dir}")
    ingest_local_dir = os.path.join(wrk_dir, "ingest_local")

    # get sample file path
    current_dir = os.path.dirname(os.path.realpath(__file__))
    samples_dir = os.path.join(current_dir, "../../samples/win_evtx")

    # copy to ingest_local
    src_path = os.path.join(samples_dir, "Application.evtx")
    dest_path = os.path.join(ingest_local_dir, "Application.evtx")
    await muty.file.copy_file_async(src_path, dest_path)
    MutyLogger.get_instance().info(f"second file copied: {src_path} to {dest_path}")

    # ingest the file
    ingest_token = await GulpAPIUser.login("ingest", "ingest")
    assert ingest_token

    await GulpAPIIngest.ingest_file_local_to_source(
        token=ingest_token,
        file_path=dest_path,
        source_id=source_id,
        req_id="new_req_2",
    )

    await _test_ingest_ws_loop(check_ingested=6419, check_processed=6419)

    # now check operation total
    op = await GulpAPIOperation.operation_get_by_id(ingest_token, TEST_OPERATION_ID)
    assert op["doc_count"] == 6426

    MutyLogger.get_instance().info(
        test_ingest_file_local_to_source.__name__ + " succeeded!"
    )


@pytest.mark.asyncio
async def test_memprocfs_timeline_ingest():
    current_dir = os.path.dirname(os.path.realpath(__file__))

    files = [os.path.join(current_dir, "../../samples/memprocfs/timeline_all.txt")]

    plugin_params = GulpPluginParameters()
    await _test_ingest_generic(files, "mem_proc_fs", 77140, plugin_params=plugin_params)
    MutyLogger.get_instance().info(
        test_memprocfs_timeline_ingest.__name__ + " succeeded!"
    )


@pytest.mark.asyncio
async def test_memprocfs_web_ingest():
    current_dir = os.path.dirname(os.path.realpath(__file__))

    files = [os.path.join(current_dir, "../../samples/memprocfs/web.txt")]

    plugin_params = GulpPluginParameters()
    await _test_ingest_generic(
        files, "mem_proc_fs_web", 28, plugin_params=plugin_params
    )
    MutyLogger.get_instance().info(
        test_memprocfs_timeline_ingest.__name__ + " succeeded!"
    )


@pytest.mark.asyncio
async def test_memprocfs_ntfs_ingest():
    current_dir = os.path.dirname(os.path.realpath(__file__))

    files = [os.path.join(current_dir, "../../samples/memprocfs/ntfs_files.txt")]

    plugin_params = GulpPluginParameters()
    await _test_ingest_generic(
        files, "mem_proc_fs_ntfs", 58, plugin_params=plugin_params
    )
    MutyLogger.get_instance().info(
        test_memprocfs_timeline_ingest.__name__ + " succeeded!"
    )
