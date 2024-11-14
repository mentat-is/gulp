import argparse
import asyncio
import logging
import os
import sys
from multiprocessing import freeze_support
import art

from gulp.utils import GulpLogger
from gulp.config import GulpConfig
from gulp.api.rest_api import GulpRestServer

# just for quick testing from the command line
__RUN_TESTS__ = os.getenv("INTERNAL_TEST", False)
if not __debug__:
    __RUN_TESTS__ = False

async def async_test():    
    if not __debug__:
        return
    l = 10
    batch_size = 3
    count = 0
    for i in range(0, l, batch_size):
        is_last = False
        if i + batch_size > l:
            batch_size = l - i
            is_last = True

        count += 1
        print(
            "running batch %d of %d tasks, total=%d, last=%r ..."
            % (count, batch_size, l, is_last)
        )

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
        help="reset collaboration database on start.",
        action="store_const",
        const=True,
        default=False,
    )
    parser.add_argument(
        "--reset-index",
        help="reset the given opensearch index.",
        nargs=1,
        metavar=("indexname"),
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
    log_file_path = args.log_to_file[0] if args.log_to_file else None
    GulpLogger.get_instance().reconfigure(log_file_path=log_file_path, level=lv)

    if __RUN_TESTS__:
        # test stuff
        asyncio.run(async_test())
        return
    
    # get params
    try:
        if args.version:
            # print version string and exit
            print(ver)
        else:
            # default
            print("%s\n%s" % (banner, ver))
            reset_collab = args.reset_collab
            reset_index = (
                args.reset_index[0] if args.reset_index is not None else None
            )
            GulpRestServer.get_instance().start(
                            log_file_path=log_file_path,
                            reset_collab=reset_collab,
                            reset_index=reset_index)
    except Exception as ex:
        # print exception and exit
        GulpLogger.get_instance().get_logger().exception(ex)
        sys.exit(1)

    # done
    sys.exit(0)


if __name__ == "__main__":
    freeze_support()  # this is needed for macos
    main()
