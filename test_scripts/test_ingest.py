#!/usr/bin/env python3
"""
script to test gulp ingestion api, simulates multiple client processes ingesting files in parallel

curl is used to send the files to the gulp ingestion api, to be as much close as possible to a real client.
"""

import argparse
import json
import logging
import os
import subprocess
import sys
from multiprocessing import Pool

import muty.file
from muty.log import MutyLogger

from gulp.api.rest.test_values import (
    TEST_CONTEXT_NAME,
    TEST_INDEX,
    TEST_OPERATION_ID,
    TEST_REQ_ID,
    TEST_WS_ID,
)

"""
# win_evtx
# 98633 records, 1 record failed, 1 skipped, 98631 ingested
./test_scripts/test_ingest.py --path ./samples/win_evtx

# csv without mapping
# 10 records, 10 ingested
./test_scripts/test_ingest.py --path ./samples/mftecmd/sample_record.csv --plugin csv --plugin_params '{"mappings": { "test_mapping": { "timestamp_field": "Created0x10"}}}'

# csv with mapping
# 10 records, 44 ingested
./test_scripts/test_ingest.py --path ./samples/mftecmd/sample_record.csv --plugin csv --plugin_params '{"mapping_file": "mftecmd_csv.json", "mapping_id": "record"}'

# raw
# 3 ingested
./test_scripts/test_ingest.py --raw ./test_scripts/test_raw.json

# raw with mapping
# 3 ingested, record 1.field2 mapping changed
./test_scripts/test_ingest.py --raw ./test_scripts/test_raw.json --plugin_params '{ "mappings": { "test_mapping": { "fields": { "field2": { "ecs": [ "test.mapped", "test.another_mapped" ] } } } } }'

# zip (with metadata.json), win_evtx and csv with mappings
# 98750 ingested (98631 windows, 119 mftecmd, 44 record, 75 j)
# ./test_scripts/test_ingest.py --path ./test_scripts/test_ingest_zip.zip
"""


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
    parser.add_argument("--path", help="File or directory path.", metavar="FILEPATH")
    parser.add_argument(
        "--raw",
        help='a JSON file with raw data for the "raw" plugin, --path is ignored if this is set',
    )
    parser.add_argument("--host", default="localhost:8080", help="Gulp host")
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
        help="Plugin to be used, ignored if --raw is set or file is a zip",
    )
    parser.add_argument("--ws_id", default=TEST_WS_ID, help="Websocket id")
    parser.add_argument("--req_id", default=TEST_REQ_ID, help="Request id")
    parser.add_argument("--index", default=TEST_INDEX, help="Ingestion index")
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
        "--restart_from",
        type=int,
        default=0,
        help="Offset to continue upload from",
    )
    return parser.parse_args()


def _create_ingest_curl_command(file_path: str, file_total: int, raw: dict, args):
    def _create_payload(file_path, raw, args, is_zip=False):
        payload = {"flt": json.loads(args.flt) if args.flt else {}}

        if not is_zip:
            payload["plugin_params"] = (
                json.loads(args.plugin_params) if args.plugin_params else {}
            )
            payload["original_file_path"] = file_path
        if raw:
            payload["chunk"] = raw

        return json.dumps(payload)

    def _get_common_headers(args, file_size=None):
        headers = [
            ("-H", "content-type: multipart/form-data"),
            ("-H", f"token: {args.token or 'null'}"),
        ]
        if file_size:
            headers.extend(
                [
                    ("-H", f"size: {file_size}"),
                    ("-H", f"continue_offset: {args.restart_from}"),
                ]
            )
        return headers

    is_zip = file_path and file_path.lower().endswith(".zip")
    base_url = f"http://{args.host}"
    command = ["curl", "-v", "-X", "POST"]
    payload = _create_payload(file_path, raw, args, is_zip)

    if raw:
        # raw request
        url = f"{base_url}/ingest_raw"
        params = f"plugin=raw&operation_id={args.operation_id}&context_name={args.context_name}&source=raw_source&index={args.index}&ws_id={args.ws_id}&req_id={args.req_id}&token={args.token}"
        command.extend(
            [
                "-H",
                f"token: {args.token or 'null'}",
                f"{url}?{params}",
                "-H",
                "content-type: application/json",
                "-d",
                payload,
            ]
        )
    else:
        # file upload request
        file_size = os.path.getsize(file_path)

        if is_zip:
            url = f"{base_url}/ingest_zip"
            params = f"operation_id={args.operation_id}&context_name={args.context_name}&index={args.index}&ws_id={args.ws_id}&req_id={args.req_id}&token={args.token}"
            file_type = "application/zip"
        else:
            url = f"{base_url}/ingest_file"
            params = f"operation_id={args.operation_id}&context_name={args.context_name}&index={args.index}&plugin={args.plugin}&ws_id={args.ws_id}&req_id={args.req_id}&file_total={file_total}&token={args.token}"

            file_type = "application/octet-stream"

        command.extend(
            [
                f"{url}?{params}",
                *[
                    item
                    for pair in _get_common_headers(args, file_size)
                    for item in pair
                ],
                "-F",
                f"payload={payload}; type=application/json",
                "-F",
                f"f=@{file_path};type={file_type}",
            ]
        )

    return command


def _run_curl(file_path: str, file_total: int, raw: dict, args):
    command = _create_ingest_curl_command(file_path, file_total, raw, args)

    # print curl command line
    cmdline = " ".join(command)
    MutyLogger.get_instance("test_ingest_worker-%d" % (os.getpid())).debug(
        f"CURL:\n{cmdline}"
    )
    subprocess.run(command)


def main():
    MutyLogger.get_instance("test_ingest", level=logging.DEBUG)
    args = _parse_args()

    if args.path and args.raw:
        MutyLogger.get_instance().error("only one of --path or --raw can be set")
        sys.exit(1)
    if not args.path and not args.raw:
        MutyLogger.get_instance().error("either --path or --raw must be set")
        sys.exit(1)

    # login first
    login_command = [
        "curl",
        "-v",
        "-X",
        "PUT",
        f"http://{args.host}/login?user_id={args.username}&password={args.password}&req_id={args.req_id}&ws_id={args.ws_id}",
    ]
    MutyLogger.get_instance().info(f"login command: {login_command}")
    login_response = subprocess.run(login_command, capture_output=True)
    if login_response.returncode != 0:
        MutyLogger.get_instance().error("login failed")
        sys.exit(1)
    print(login_response.stdout)
    args.token = json.loads(login_response.stdout)["data"]["token"]

    if args.path:
        path = os.path.abspath(os.path.expanduser(args.path))
        if os.path.isdir(path):
            files = muty.file.list_directory(path, recursive=True, files_only=True)
        else:
            files = [path]
        raw = None
        MutyLogger.get_instance().info(f"files to ingest: {files}")
    else:
        # raw data is set, ignore path
        with open(args.raw) as f:
            raw = json.loads(f.read())
        files = None
        MutyLogger.get_instance().info("raw data loaded.")

    # spawn curl processes
    with Pool() as pool:
        if raw:
            l = pool.starmap(_run_curl, [(None, 1, raw, args)])
        else:
            l = pool.starmap(
                _run_curl, [(file, len(files), None, args) for file in files]
            )

        # wait for all processes to finish
        pool.close()
        pool.join()

    # done
    MutyLogger.get_instance().info("DONE!")


if __name__ == "__main__":
    main()
