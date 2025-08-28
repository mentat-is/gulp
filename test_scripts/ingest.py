#!/usr/bin/env python3
"""
script to test gulp ingestion api, simulates multiple client processes ingesting files in parallel

curl is used to send the files to the gulp ingestion api, to be as much close as possible to a real client.
"""

import argparse
import asyncio
import json
import logging
import os
import subprocess
import sys
from multiprocessing import Pool

import muty.crypto
import muty.file
import muty.string
import websockets
from muty.log import MutyLogger

from gulp_client.test_values import (
    TEST_CONTEXT_NAME,
    TEST_OPERATION_ID,
    TEST_REQ_ID,
    TEST_WS_ID,
)
from gulp.api.ws_api import GulpWsAuthPacket


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Spawn n curl processes in parallel for file ingestion."
    )
    parser.add_argument(
        "--username",
        help="user name",
        default="ingest",
    )
    parser.add_argument(
        "--password",
        help="user password",
        default="ingest",
    )
    parser.add_argument(
        "--path", help="File or directory path.", metavar="FILEPATH", required=True
    )
    parser.add_argument("--host", default="http://localhost:8080", help="Gulp host")
    parser.add_argument(
        "--operation_id",
        default=TEST_OPERATION_ID,
        help="Gulp operation_id",
    )
    parser.add_argument(
        "--context_name",
        default=TEST_CONTEXT_NAME,
        help="Gulp context_name",
    )
    parser.add_argument(
        "--plugin",
        default="win_evtx",
        help="Plugin to be used",
    )
    parser.add_argument("--ws_id", default=TEST_WS_ID, help="Websocket id")
    parser.add_argument("--req_id", default=TEST_REQ_ID, help="Request id")
    parser.add_argument(
        "--multi-req", action="store_true", help="Use multiple request ids"
    )
    parser.add_argument(
        "--sleep", action="store_true", help="Random sleep (1-3 sec) between requests"
    )
    parser.add_argument(
        "--flt",
        default=None,
        help="GulpIngestionFilter as JSON",
    )
    parser.add_argument(
        "--plugin_params",
        default=None,
        help="GulpPluginParameters as JSON, ignored if ingesting a zip file (use metadata.json)",
    )
    parser.add_argument(
        "--continue_offset",
        type=int,
        default=0,
        help="Offset to continue upload from",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="reset gulp first",
        default=False,
    )
    parser.add_argument(
        "--preview-mode",
        action="store_true",
        help="preview mode: no ingestion, no stats, no ws",
        default=False,
    )
    return parser.parse_args()


def _create_ingest_curl_command(file_path: str, file_total: int, args):
    def _create_payload(file_path, args, is_zip=False):
        payload = {"flt": json.loads(args.flt) if args.flt else {}}

        if not is_zip:
            payload["plugin_params"] = (
                json.loads(args.plugin_params) if args.plugin_params else {}
            )
            payload["original_file_path"] = file_path

        # add file sha1
        sha1_hash = asyncio.run(muty.crypto.hash_sha1_file(file_path))
        payload["file_sha1"] = sha1_hash

        return json.dumps(payload)

    def _get_common_headers(args, file_size=None):
        # create headers array with token, size, continue_offset
        headers = [
            ("-H", "content-type: multipart/form-data"),
            ("-H", f"token: {args.token or 'null'}"),
        ]
        if file_size:
            headers.extend(
                [
                    ("-H", f"size: {file_size}"),
                    ("-H", f"continue_offset: {args.continue_offset}"),
                ]
            )
        return headers

    is_zip = file_path and file_path.lower().endswith(".zip")
    preview_mode = args.preview_mode
    base_url = f"{args.host}"
    command = ["curl", "-v", "POST"]
    payload = _create_payload(file_path, args, is_zip)
    temp_file_path = None
    multi_request = args.multi_req
    if multi_request:
        req_id = f"{args.req_id}_{muty.string.generate_unique()}"
    else:
        req_id = args.req_id

    full_file_size = os.path.getsize(file_path)
    continue_offset = int(args.continue_offset)
    if args.continue_offset > 0:
        # handling restart using a truncated temp file
        MutyLogger.get_instance().info(
            "restarting %s from %d" % (file_path, continue_offset)
        )
        temp_file_path = "/tmp/%s" % (os.path.basename(file_path))
        with open(file_path, "rb") as f:
            f.seek(continue_offset)
            with open(temp_file_path, "wb") as tf:
                tf.write(f.read())
        file_path = temp_file_path

    upload_file_size = os.path.getsize(file_path)
    MutyLogger.get_instance().info(f"uploading size: {upload_file_size}")

    if is_zip:
        url = f"{base_url}/ingest_zip"
        params = f"operation_id={args.operation_id}&context_name={args.context_name}&ws_id={args.ws_id}&req_id={req_id}"
        file_type = "application/zip"
    else:
        url = f"{base_url}/ingest_file"
        params = f"operation_id={args.operation_id}&context_name={args.context_name}&plugin={
            args.plugin}&ws_id={args.ws_id}&req_id={req_id}&file_total={file_total}&preview_mode={preview_mode}"

        file_type = "application/octet-stream"

    command.extend(
        [
            f"{url}?{params}",
            *[
                item
                for pair in _get_common_headers(args, full_file_size)
                for item in pair
            ],
            "-F",
            f"payload={payload}; type=application/json",
            "-F",
            f"f=@{file_path};type={file_type}",
        ]
    )

    return command, temp_file_path


def _run_curl(file_path: str, file_total: int, args):
    MutyLogger.get_instance("test_ingest_worker-%d" % (os.getpid())).debug("_run_curl")

    command, tmp_file_path = _create_ingest_curl_command(file_path, file_total, args)
    if args.sleep:
        import random
        import time

        sleep_time = random.randint(1, 3)
        MutyLogger.get_instance().info(
            f"sleeping {sleep_time} seconds before ingesting {file_path}"
        )
        time.sleep(sleep_time)

    # copy file to a temporary location and truncate to args.continue_offset
    # print curl command line
    cmdline = " ".join(command)
    MutyLogger.get_instance().debug(f"CURL:\n{cmdline}")
    subprocess.run(command)

    if tmp_file_path:
        # remove temp file
        muty.file.delete_file_or_dir(tmp_file_path)


def _login(host, username, password, req_id, ws_id) -> str:
    MutyLogger.get_instance().info("logging in %s" % (username))
    login_command = [
        "curl",
        "-v",
        "-X",
        "POST",
        "-H",
        "Content-Type: application/json",
        "--data",
        json.dumps({"user_id": username, "password": password}),
        f"{host}/login?req_id={req_id}&ws_id={ws_id}",
    ]
    MutyLogger.get_instance().info(f"login command: {login_command}")
    login_response = subprocess.run(login_command, capture_output=True)
    if login_response.returncode != 0:
        MutyLogger.get_instance().error("login failed")
        sys.exit(1)
    MutyLogger.get_instance().debug(login_response.stdout)
    token = json.loads(login_response.stdout)["data"]["token"]
    return token


def _reset(host, req_id, ws_id):
    MutyLogger.get_instance().info("resetting gulp")
    admin_token = _login(host, "admin", "admin", req_id, ws_id)
    reset_command = [
        "curl",
        "-v",
        "-H",
        f"token: {admin_token}",
        "-X",
        "POST",
        f"{host}/gulp_reset?req_id={req_id}",
    ]
    MutyLogger.get_instance().info(f"reset command: {reset_command}")
    reset_response = subprocess.run(reset_command, capture_output=True)
    if reset_response.returncode != 0:
        MutyLogger.get_instance().error("reset failed")
        sys.exit(1)
    MutyLogger.get_instance().debug(reset_response.stdout)


def _ws_loop(host: str, token: str, ws_id: str):
    """
    consumes websocket data until ingestion is finished
    """

    async def _ws_loop_internal(host: str, token: str, ws_id: str):

        # connect to websocket
        MutyLogger.get_instance("ws_loop").info("ws loop running!")

        _, host = host.split("://")
        ws_url = f"ws://{host}/ws"
        async with websockets.connect(ws_url) as ws:
            # connect websocket
            p: GulpWsAuthPacket = GulpWsAuthPacket(token=token, ws_id=ws_id)
            await ws.send(p.model_dump_json(exclude_none=True))

            # receive responses
            try:
                while True:
                    response = await ws.recv()
                    data = json.loads(response)
                    if data["type"] == "stats_update":
                        # MutyLogger.get_instance().error(f"data: {data}")
                        d = data["data"]["data"]
                        if d["status"] != "ongoing":
                            MutyLogger.get_instance().info(f"stats: {d}")
                            break

                    # ws delay
                    await asyncio.sleep(0.1)

            except websockets.exceptions.ConnectionClosed as ex:
                MutyLogger.get_instance().exception(ex)

        MutyLogger.get_instance().info("ingestion finished!")

    try:
        asyncio.run(_ws_loop_internal(host, token, ws_id))
    except Exception as ex:
        MutyLogger.get_instance().exception(ex)
        raise


def main():
    MutyLogger.get_instance("test_ingest", level=logging.DEBUG)
    args = _parse_args()

    if args.reset:
        # reset first
        _reset(args.host, args.req_id, args.ws_id)

    # get an ingest token
    args.token = _login(
        args.host, args.username, args.password, args.req_id, args.ws_id
    )

    path = os.path.abspath(os.path.expanduser(args.path))
    if os.path.isdir(path):
        files = muty.file.list_directory(path, recursive=True, files_only=True)
    else:
        files = [path]
    MutyLogger.get_instance().info(f"files to ingest: {files}")

    # spawn curl processes
    with Pool() as pool:
        # run the loop
        pool.apply_async(
            _ws_loop, kwds={"host": args.host, "token": args.token, "ws_id": args.ws_id}
        )

        # run requests
        l = pool.starmap(_run_curl, [(file, len(files), args) for file in files])

        # wait for all processes to finish
        pool.close()
        pool.join()

    # done
    MutyLogger.get_instance().info("DONE!")


if __name__ == "__main__":
    main()
