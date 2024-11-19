#!/usr/bin/env python3
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


def parse_args():
    parser = argparse.ArgumentParser(
        description="Spawn n curl processes in parallel for file ingestion."
    )
    parser.add_argument("--path", required=True, help="File or directory path")
    parser.add_argument("--host", default="localhost:8080", help="Gulp host")
    parser.add_argument(
        "--operation", default="test_operation", help="Gulp operation_id"
    )
    parser.add_argument("--context", default="test_context", help="Gulp context_id")
    parser.add_argument("--plugin", default="win_evtx", help="Plugin to be used")
    parser.add_argument("--ws_id", default="test_ws", help="Websocket id")
    parser.add_argument("--req_id", default="test_req", help="Request id")
    parser.add_argument("--index", default="test_idx", help="Ingestion index")
    parser.add_argument("--flt", default=None, help="GulpIngestionFilter as JSON")
    parser.add_argument(
        "--plugin_params", default=None, help="GulpPluginParameters as JSON"
    )
    parser.add_argument("--token", default=None, help="Gulp token")
    parser.add_argument(
        "--restart_from", type=int, default=0, help="Offset to continue upload from"
    )
    return parser.parse_args()


def get_file_size(file_path):
    return os.path.getsize(file_path)


def create_curl_command(file_path, args):
    file_size = get_file_size(file_path)
    payload = json.dumps(
        {
            "flt": json.loads(args.flt) if args.flt else None,
            "plugin_params": (
                json.loads(args.plugin_params) if args.plugin_params else None
            ),
        }
    )
    command = [
        "curl",
        "-v",
        "-X",
        "PUT",
        f"http://{args.host}/ingest_file?operation_id={args.operation}&context_id={args.context}&index={args.index}&plugin={args.plugin}&ws_id={args.ws_id}&req_id={args.req_id}",
        "-H",
        f"token: {args.token or "null"}",
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
    if args.token:
        command.extend(["-H", f"token: {args.token}"])

    return command


def run_curl(file_path, args):
    command = create_curl_command(file_path, args)
    # print curl command line
    cmdline = " ".join(command)
    MutyLogger.get_instance("test_ingest_worker-%d" % (os.getpid())).debug(
        f"CURL:\n{cmdline}"
    )
    subprocess.run(command)


def main():
    MutyLogger.get_instance("test_ingest", level=logging.DEBUG)
    args = parse_args()
    path = os.path.abspath(os.path.expanduser(args.path))
    if os.path.isdir(path):
        files = muty.file.list_directory(path, recursive=True, files_only=True)
    else:
        files = [path]

    MutyLogger.get_instance().info(f"files to ingest: {files}")

    start = timeit.default_timer()
    with Pool() as pool:
        l = pool.starmap(run_curl, [(file, args) for file in files])
        # wait for all processes to finish
        pool.close()
        pool.join()
    end = timeit.default_timer()

    MutyLogger.get_instance().info(f"ingestion took {end-start:.2f} seconds")


if __name__ == "__main__":
    main()
