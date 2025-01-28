#!/usr/bin/env python3
"""
script to restart gulp by calling /restart_server endpoint with an admin token
"""
# add the project root directory to Python path
from gulp.api.rest.test_values import (
    TEST_HOST,
    TEST_REQ_ID,
    TEST_WS_ID,
)
import asyncio
import logging
import argparse
import sys
import os


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Restart gulp by calling /restart_server."
    )
    parser.add_argument(
        "--host", default="http://localhost:8080", help="Gulp host")
    return parser.parse_args()


async def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)  # Go up one level to /gulp
    sys.path.append(project_root)

    from tests.api.common import GulpAPICommon
    from tests.api.user import GulpAPIUser
    from tests.api.utility import GulpAPIUtility
    from muty.log import MutyLogger

    MutyLogger.get_instance("restart_gulp", level=logging.DEBUG)
    args = _parse_args()
    GulpAPICommon.get_instance().init(
        host=args.host,
        ws_id=TEST_WS_ID,
        req_id=TEST_REQ_ID,
    )
    token = await GulpAPIUser.login_admin()
    await GulpAPIUtility.restart_server(token)

    # done
    MutyLogger.get_instance().info("DONE!")


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
