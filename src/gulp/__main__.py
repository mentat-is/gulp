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
        help="""deletes and recreate the collab database (useful when schema changes).
this also creates the database if it does not exist.

use with --delete-data to delete all documents on OpenSearch related to all the existing operations as well.
        """,
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--create",
        help="""(re)creates an operation on the collab database.

the specified operation will be deleted (if exists) and recreated.

use with --delete-data to delete data on OpenSearch as well.
""",
        nargs=1,
        metavar=("operation_id"),
    )
    parser.add_argument(
        "--delete-data",
        help="""to be used with --create or --reset-collab to ensure documents on OpenSearch are deleted if exists for one (--create) or all (--reset-collab) operations.
""",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--version",
        help="print version string and exits.",
        action="store_const",
        const=True,
        default=False,
    )
    args = parser.parse_args()
    print(". command line args (sys.argv):\n%s" % (sys.argv))
    print(". command line args (parsed):\n%s" % (args))

    # reconfigure logger
    lv = logging.getLevelNamesMapping()[args.log_level[0].upper()]
    logger_file_path = args.log_to_file[0] if args.log_to_file else None
    MutyLogger.get_instance("gulp", logger_file_path=logger_file_path, level=lv)

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
            reset_operation: str = None
            if args.create:
                reset_operation = args.create[0]

            # default
            print("%s\n%s" % (banner, ver))
            GulpRestServer.get_instance().start(
                logger_file_path=logger_file_path,
                level=lv,
                reset_collab=args.reset_collab,
                create_operation=reset_operation,
                delete_data=args.delete_data,
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
