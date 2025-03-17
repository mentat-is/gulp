#!/usr/bin/env python3
import argparse
import asyncio
import json

import muty.file
import websockets
from muty.log import MutyLogger

from gulp.api.rest.client.common import GulpAPICommon
from gulp.api.rest.client.db import GulpAPIDb
from gulp.api.rest.client.user import GulpAPIUser
from gulp.api.rest.test_values import (
    TEST_HOST,
    TEST_INDEX,
    TEST_OPERATION_ID,
    TEST_REQ_ID,
    TEST_WS_ID,
)
from gulp.api.ws_api import GulpWsAuthPacket, GulpWsIngestPacket


def _chunk_array(arr: list, chunk_size: int) -> list:
    """
    split an array into chunks of specified size

    Args:
        arr: the array to split
        chunk_size: the size of each chunk

    Returns:
        list: a list of chunks (lists)
    """
    return [arr[i:i + chunk_size] for i in range(0, len(arr), chunk_size)]


async def _ingest_ws_raw(args) -> None:
    MutyLogger.get_instance("ingest_raw").info(
        "ingesting raw, args=%s ..." % (args))

    path = args.path
    reset = args.reset
    ws_id = args.ws_id
    req_id = args.req_id
    operation_id = args.operation_id
    host = args.host

    GulpAPICommon.get_instance().init(
        host=host, ws_id=ws_id, req_id=req_id
    )

    buf = await muty.file.read_file_async(path)
    raw_data = json.loads(buf)
    chunk_size = 200

    if reset:
        # reset first
        MutyLogger.get_instance().info("resetting gulp !")
        token = await GulpAPIUser.login("admin", "admin")
        await GulpAPIDb.gulp_reset(token)

    ingest_token = await GulpAPIUser.login("ingest", "ingest")

    _, host = host.split("://")
    ws_url = f"ws://{host}/ws_ingest_raw"

    async with websockets.connect(ws_url) as ws:
        # connect websocket
        p: GulpWsAuthPacket = GulpWsAuthPacket(
            token=ingest_token, ws_id=ws_id
        )
        await ws.send(p.model_dump_json(exclude_none=True))

        # receive responses
        while True:
            response = await ws.recv()
            data = json.loads(response)
            if data["type"] == "ws_connected":

                chunks = _chunk_array(raw_data, chunk_size)
                c = 0
                for chunk in chunks:
                    # send chunk
                    p: GulpWsIngestPacket = GulpWsIngestPacket(
                        docs=chunk,
                        index=operation_id,
                        operation_id=operation_id,
                        req_id=req_id,
                        ws_id=ws_id,
                    )
                    await ws.send(p.model_dump_json(exclude_none=True))
                    await asyncio.sleep(0.1)
                    c += 1
                    MutyLogger.get_instance().info("sent chunk %d of %d documents ..." % (c, len(chunk)))
                break

            # ws delay
            await asyncio.sleep(0.1)

        MutyLogger.get_instance().info("DONE!")


def parse_arguments() -> argparse.Namespace:
    """
    parse command line arguments for the anomaly generator

    Returns:
        argparse.Namespace: parsed command line arguments
    """
    parser = argparse.ArgumentParser(
        description="ingest raw data with the raw plugin using the /ingest_ws/raw api")
    parser.add_argument("--path", required=True,
                        help="path to the raw file with gulp json documents to be ingested")
    parser.add_argument("--host", default=TEST_HOST, help="Gulp host")
    parser.add_argument(
        "--operation_id",
        default=TEST_OPERATION_ID,
        help="Gulp operation_id",
    )
    parser.add_argument(
        "--req_id",
        default=TEST_REQ_ID,
        help="Gulp req_id",
    )
    parser.add_argument(
        "--ws_id",
        default=TEST_WS_ID,
        help="Gulp ws_id",
    )
    parser.add_argument("--reset", action="store_true",
                        help="reset gulp before ingesting", default=False)
    return parser.parse_args()


def main():
    args = parse_arguments()
    try:
        asyncio.run(_ingest_ws_raw(args))
        return 0
    except Exception as ex:
        MutyLogger.get_instance().exception(ex)
        return 1


if __name__ == "__main__":
    exit(main())
