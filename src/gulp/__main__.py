import argparse
import asyncio
import logging
import os
import sys
from multiprocessing import freeze_support

import art
from muty.log import MutyLogger

from gulp.api.rest_api import GulpRestServer

# just for quick testing from the command line
__RUN_TESTS__ = os.getenv("INTERNAL_TEST", "0") == "1"
if not __debug__:
    __RUN_TESTS__ = False


async def async_test():
    # from muty.jsend import JSendException, JSendResponseStatus

    # try:
    #     try:
    #         raise ValueError("test exception")
    #     except Exception as ex:
    #         raise JSendException("this is the jsend exceptiom", req_id="1234") from ex
    # except Exception as ex:
    #     # this will be logged
    #     MutyLogger.get_instance().exception(ex)
    #     print("-----")
    #     print(ex.to_string())
    pass


def main():
    """
    :return:
    """
    ver = GulpRestServer.get_instance().version_string()
    installation_dir = os.path.dirname(os.path.realpath(__file__))
    banner = art.text2art("(g)ULP", font="random")

    # parse args
    parser = argparse.ArgumentParser(
        description=banner,
        epilog="(generic) unified log parser\nversion: %s\ninstallation path: %s"
        % (ver, installation_dir),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--log-to-file",
        nargs=1,
        metavar=("file path"),
        help="also outputs log to this (rotating) file, default=stdout only.",
    )
    parser.add_argument(
        "--log-level",
        nargs=1,
        metavar=("level"),
        help='select log level, default="debug".',
        choices=["critical", "error", "warning", "info", "debug"],
        default=["debug"],
    )
    parser.add_argument(
        "--reset-collab",
        help="reset collaboration database on start (do not delete 'operation', 'users' and related tables to maintain existing owners and associations).",
        action="store_const",
        const=True,
        default=False,
    )
    parser.add_argument(
        "--reset-collab-full",
        help="same as --reset-collab, but perform a full collaboration database reset also deleting data on OpenSearch for the operations found in the 'operations' table.",
        action="store_const",
        const=True,
        default=False,
    )
    parser.add_argument(
        "--reset-operation",
        help="deletes operation data both on OpenSearch and on the collaboration database.",
        nargs=1,
        metavar=("operation_id"),
    )
    parser.add_argument(
        "--version",
        help="print version string and exits.",
        action="store_const",
        const=True,
        default=False,
    )
    args = parser.parse_args()

    # reconfigure logger
    lv = logging.getLevelNamesMapping()[args.log_level[0].upper()]
    logger_file_path = args.log_to_file[0] if args.log_to_file else None
    MutyLogger.get_instance(
        "gulp", logger_file_path=logger_file_path, level=lv)

    if __RUN_TESTS__:
        # test stuff
        asyncio.run(async_test())
        return 0

    # get params
    try:
        if args.version:
            # print version string and exit
            print(ver)
        else:
            reset_collab = 0
            if args.reset_collab:
                reset_collab = 1
            if args.reset_collab_full:
                reset_collab = 2
            # default
            print("%s\n%s" % (banner, ver))
            reset_operation = (
                args.reset_operation[0] if args.reset_operation is not None else None
            )
            GulpRestServer.get_instance().start(
                logger_file_path=logger_file_path,
                level=lv,
                reset_collab=reset_collab,
                reset_operation=reset_operation,
            )
    except Exception as ex:
        # print exception and exit
        MutyLogger.get_instance().exception(ex)
        return 1

    # done
    return 0


if __name__ == "__main__":
    freeze_support()  # this is needed for macos
    sys.exit(main())
