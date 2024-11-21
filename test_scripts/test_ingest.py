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
import timeit
from multiprocessing import Pool

import muty.file
from muty.log import MutyLogger

"""
# win_evtx
./test_scripts/test_ingest.py --path ./samples/win_evtx

# csv with mapping
./test_scripts/test_ingest.py --path ./samples/mftecmd/sample_record.csv --plugin csv --plugin_params '{"mapping_file": "mftecmd_csv.json", "mapping_id": "record"}'

# raw
./test_scripts/test_ingest.py --raw ./test_scripts/test_raw.json

# raw with mapping
./test_scripts/test_ingest.py --raw ./test_scripts/test_raw.json --plugin_params '{ "mappings": { "test_mapping": { "fields": { "field2": { "ecs": [ "test.mapped", "test.another_mapped" ] } } } } }'
"""


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Spawn n curl processes in parallel for file ingestion."
    )
    parser.add_argument("--path", help="File or directory path", metavar="FILEPATH")
    parser.add_argument(
        "--raw",
        help='a JSON file with raw data for the "raw" plugin, --path is ignored if this is set',
        metavar="RAW_JSON",
    )
    parser.add_argument(
        "--host", default="localhost:8080", help="Gulp host", metavar="HOST:PORT"
    )
    parser.add_argument(
        "--operation",
        default="test_operation",
        help="Gulp operation_id",
        metavar="OPERATION_ID",
    )
    parser.add_argument(
        "--context",
        default="test_context",
        help="Gulp context_id",
        metavar="CONTEXT_ID",
    )
    parser.add_argument(
        "--plugin",
        default="win_evtx",
        help="Plugin to be used, ignored if --raw is set",
        metavar="PLUGIN",
    )
    parser.add_argument(
        "--ws_id", default="test_ws", help="Websocket id", metavar="WS_ID"
    )
    parser.add_argument(
        "--req_id", default="test_req", help="Request id", metavar="REQ_ID"
    )
    parser.add_argument(
        "--index", default="test_idx", help="Ingestion index", metavar="INDEX"
    )
    parser.add_argument(
        "--flt",
        default=None,
        help="GulpIngestionFilter as JSON",
        metavar="GULPINGESTIONFILTER",
    )
    parser.add_argument(
        "--plugin_params",
        default=None,
        help="GulpPluginParameters as JSON",
        metavar="GULPPLUGINPARAMETERS",
    )
    parser.add_argument("--token", default=None, help="Gulp token", metavar="TOKEN")
    parser.add_argument(
        "--restart_from",
        type=int,
        default=0,
        help="Offset to continue upload from",
        metavar="OFFSET",
    )
    return parser.parse_args()

def _create_curl_command(file_path: str, file_total: int, raw: dict, args):
    d = {
        "flt": json.loads(args.flt) if args.flt else None,
        "plugin_params": (
            json.loads(args.plugin_params) if args.plugin_params else None
        ),
    }
    if raw:
        d["chunk"] = raw
    else:
        d["original_file_path"] = file_path

    payload = json.dumps(d)

    command = [
        "curl",
        "-v",
        "-X",
        "PUT",
    ]
    if raw:
        # request is application/json
        command.extend(
            [
                f"http://{args.host}/ingest_raw?operation_id={args.operation}&context_id={args.context}&source=raw_source&index={args.index}&ws_id={args.ws_id}&req_id={args.req_id}",
                "-H",
                "content-type: application/json",
                "-d",
                payload,
            ]
        )
    else:
        # request is multipart/form-data
        file_size = os.path.getsize(file_path)
        command.extend(
            [
                f"http://{args.host}/ingest_file?operation_id={args.operation}&context_id={args.context}&index={args.index}&plugin={args.plugin}&ws_id={args.ws_id}&req_id={args.req_id}&file_total={file_total}",
                "-H",
                "content-type: multipart/form-data",
                "-H",
                f"size: {file_size}",
                "-H",
                f"continue_offset: {args.restart_from}",
                "-F",
                f"payload={payload}; type=application/json",
                "-F",
                f"f=@{file_path};type=application/octet-stream",
            ]
        )

    command.extend(["-H", f"token: {args.token or "null"}"])
    return command


def _run_curl(file_path: str, file_total: int, raw: dict, args):
    command = _create_curl_command(file_path, file_total, raw, args)
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
