#!/usr/bin/env python3
import argparse
import asyncio
import json
import sys

import pytest
import pytest_asyncio
import websockets
from muty.log import MutyLogger

from gulp.api.opensearch.query import GulpQueryParameters
from gulp.api.rest.client.common import GulpAPICommon, _test_init
from gulp.api.rest.client.db import GulpAPIDb
from gulp.api.rest.client.query import GulpAPIQuery
from gulp.api.rest.client.user import GulpAPIUser
from gulp.api.rest.test_values import (
    TEST_HOST,
    TEST_OPERATION_ID,
    TEST_REQ_ID,
    TEST_WS_ID,
)
from gulp.api.ws_api import GulpQueryDonePacket, GulpWsAuthPacket
from gulp.structs import GulpPluginParameters

"""
example usage:

./test_scripts/query_external.py \
    --q 'sourcetype="WinEventLog:Security" Nome_applicazione="\\\\device\\\\harddiskvolume2\\\\program files\\\\intergraph smart licensing\\\\client\\\\islclient.exe"' \
    --plugin splunk --operation_id test_operation --reset --ingest \
    --plugin_params '{
        "custom_parameters":  {
            "uri": "http://localhost:8089",
            "username": "admin",
            "password": "Valerino74!",
            "index": "incidente_183651"
        },
        "override_chunk_size": 200,
        "additional_mapping_files": [[ "windows.json", "windows" ]]
}'
"""


@pytest_asyncio.fixture(scope="function", autouse=True)
async def _setup():
    """
    this is called before any test, to initialize the environment
    """
    await _test_init()


SPLUNK_RAW_Q = "EventCode=5156"  # all
SPLUNK_PLUGIN = "splunk"


def _parse_args():
    parser = argparse.ArgumentParser(
        description="tests gulp 'query_external' endpoint."
    )
    parser.add_argument(
        "--username",
        help="Gulp user name",
        default="ingest",
    )
    parser.add_argument(
        "--password",
        help="Gulp user password",
        default="ingest",
    )
    parser.add_argument("--host", default=TEST_HOST, help="Gulp host")
    parser.add_argument(
        "--operation_id",
        default=TEST_OPERATION_ID,
        help="Gulp operation_id",
    )
    parser.add_argument(
        "--plugin",
        default=SPLUNK_PLUGIN,
        help="Plugin to be used",
    )
    parser.add_argument("--ws_id", default=TEST_WS_ID, help="Websocket id")
    parser.add_argument("--req_id", default=TEST_REQ_ID, help="Request id")
    parser.add_argument(
        "--ingest", action="store_true", help="Ingest data", default=False
    )
    parser.add_argument(
        "--preview-mode",
        action="store_true",
        help="Preview mode (ignores --ingest)",
        default=False,
    )
    parser.add_argument("--q", default=SPLUNK_RAW_Q, help="Query to be used")
    parser.add_argument("--q_options", default=None, help="GulpQueryParameters as JSON")
    parser.add_argument(
        "--plugin_params",
        default=None,
        help="GulpPluginParameters as JSON",
        required=True,
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="reset gulp first",
        default=False,
    )
    return parser.parse_args()


async def query_external(args):

    MutyLogger.get_instance(name="query_external_test").debug("args=%s", args)

    GulpAPICommon.get_instance().init(
        host=args.host, ws_id=args.ws_id, req_id=args.req_id
    )
    if args.reset:
        # reset first
        MutyLogger.get_instance().info("resetting gulp !")
        token = await GulpAPIUser.login("admin", "admin")
        await GulpAPIDb.gulp_reset(token)

    # login
    token = await GulpAPIUser.login(args.username, args.password)
    _, ws_host = args.host.split("://")
    ws_url = f"ws://{ws_host}/ws"

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
                    # perform query
                    q_options = args.q_options or "{}"
                    # query
                    q_options: GulpQueryParameters = (
                        GulpQueryParameters.model_validate_json(args.q_options)
                        if args.q_options
                        else GulpQueryParameters()
                    )
                    MutyLogger.get_instance().debug(
                        "plugin_params=%s", json.loads(args.plugin_params)
                    )
                    plugin_params: GulpPluginParameters = (
                        GulpPluginParameters.model_validate_json(args.plugin_params)
                    )
                    q_options.name = "test_raw_query_splunk"
                    if not plugin_params.override_chunk_size:
                        plugin_params.override_chunk_size = 1000  # force if not set

                    if args.preview_mode:
                        args.ingest = False
                        q_options.preview_mode = True

                    # dump structs before running the query
                    MutyLogger.get_instance().debug("q_options=%s", q_options)

                    await GulpAPIQuery.query_external(
                        token,
                        args.operation_id,
                        q=[args.q],
                        plugin=args.plugin,
                        plugin_params=plugin_params,
                        q_options=q_options,
                        ingest=args.ingest,
                    )
                elif data["type"] == "query_done":
                    q_done_packet: GulpQueryDonePacket = (
                        GulpQueryDonePacket.model_validate(data["data"])
                    )
                    MutyLogger.get_instance().debug(
                        "query done, packet=%s", q_done_packet
                    )
                    if q_done_packet.name == q_options.name:
                        if q_done_packet.status != "done":
                            MutyLogger.get_instance().error(
                                f"query failed: {q_done_packet}"
                            )
                            break
                        MutyLogger.get_instance().info("DONE: %s" % (q_done_packet))
                    break

        except websockets.exceptions.ConnectionClosed as ex:
            MutyLogger.get_instance().exception(ex)


def main():
    args = _parse_args()
    return asyncio.run(query_external(args))


# add a main
if __name__ == "__main__":
    sys.exit(main())
