import argparse
import asyncio
import logging
import os
import sys
from multiprocessing import freeze_support
import art

from gulp.utils import logger
import gulp.api.rest_api as rest_api
import gulp.config as config
import gulp.utils
import muty.file

_logger = None
__RUN_TESTS__ = False
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

    sys.exit(0)


def test():
    """
    to test stuff in gulp environment, call it in main() as the first thing.
    """
    if not __debug__:
        return
    asyncio.run(async_test())


def main():
    """
    :return:
    """

    global _logger
    installation_dir = os.path.dirname(os.path.realpath(__file__))
    banner = art.text2art("(g)ULP", font="random")

    # parse args
    parser = argparse.ArgumentParser(
        description=banner,
        epilog="(generic) unified log parser\nversion: %s\ninstallation path: %s"
        % (gulp.utils.version_string(), installation_dir),
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
        "--bind-to",
        help='binds to this address/port, default="localhost 8080".',
        nargs=2,
        metavar=("address", "port"),
        default=["localhost", 8080],
    )
    parser.add_argument(
        "--reset-collab",
        help="reset collaboration database on start.",
        action="store_const",
        const=True,
        default=False,
    )
    parser.add_argument(
        "--reset-elastic",
        help="reset elasticsearch database on start (create index).",
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
    _logger = gulp.utils.configure_logger(
        log_to_file=log_file_path, level=lv, force_reconfigure=True
    )
    _logger.debug(
        "logger in main(): %s, lv=%d, level=%d" % (_logger, lv, _logger.level)
    )

    # initialize modules
    gulp.utils.init_modules(_logger)
    _logger.debug("gulp configuration: %s" % (config.config()))

    if __RUN_TESTS__:
        # test stuff
        test()
        asyncio.run(async_test())

    # initialize custom directories if needed
    asyncio.run(config.initialize_custom_directories())

    # get params
    try:
        if args.version:
            # print version string and exit
            print(gulp.utils.version_string())
        else:
            # default
            print("%s\n%s" % (banner, gulp.utils.version_string()))
            address = args.bind_to[0]
            port = int(args.bind_to[1])
            reset_collab = args.reset_collab
            elastic_index = (
                args.reset_elastic[0] if args.reset_elastic is not None else None
            )
            is_first_run = gulp.utils.check_first_run()
            if is_first_run:
                # first run, create index            
                elastic_index = "gulpidx"
                reset_collab = True
                logger().info(
                    "first run detected, creating default index: %s" % (elastic_index)
                )                
            rest_api.start_server(
                address,
                port,
                log_file_path,
                reset_collab,
                elastic_index,
                is_first_run
            )
    except Exception as ex:                        
        # print exception and exit
        _logger.exception(ex)
        sys.exit(1)

    # done
    sys.exit(0)


if __name__ == "__main__":
    freeze_support()  # this is needed for macos
    main()
