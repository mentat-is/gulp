import argparse
import asyncio
import logging
import os
import sys
from multiprocessing import freeze_support
import signal
import time

import art
from muty.log import MutyLogger

from gulp.api.server_api import GulpServer
from gulp.config import GulpConfig

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
    ver = GulpServer.get_instance().version_string()
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
        action="store_true",
        default=False,
        help="also outputs log (in addition to stdout) to $GULP_WORKING_DIR/logs/gulp.log (rotating every 4mb). Cannot be used with --log-to-syslog.",
    )
    parser.add_argument(
        "--log-to-syslog",
        help="also outputs log (in addition to stdout) to syslog, default address=(either /var/log or /var/run/syslog, depending on what is available), default facility=1 (LOG_LOCAL_0). Cannot be used with --log-to-file.",
        nargs="*",
        metavar=("address", "facility"),
        default=None,
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
        help="""deletes (if exists) and recreate the whole collab database: all the existing operations, their collab objects and data on Opensearch are deleted as well.
        """,
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--create",
        help="""deletes (if exists) and recreates the specified operation: all the related collab objects and data on Opensearch are deleted as well.
        """,
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
    parser.add_argument(
        "--stop",
        help="stop a running gulp instance (reads pidfile and sends SIGTERM)",
        action="store_true",
        default=False,
    )
    args = parser.parse_args()
    print(". command line args (sys.argv):\n%s" % (sys.argv))
    print(". command line args (parsed):\n%s" % (args))

    # reconfigure logger
    logs_path: str = GulpConfig.get_instance().path_logs()
    lv: str = logging.getLevelNamesMapping()[args.log_level[0].upper()]
    logger_file_path: str = (
        os.path.join(logs_path, "gulp.log") if args.log_to_file else None
    )

    # post-process log_to_syslog to always be a tuple of two elements (possibly None)
    log_to_syslog: tuple[str, str]
    if args.log_to_syslog and len(args.log_to_syslog) > 2:
        print(
            "ERROR: log_to_syslog can only have 0, 1 or 2 arguments, got %d"
            % len(args.log_to_syslog)
        )
        return 1

    if args.log_to_syslog is None:
        # no syslog logging
        log_to_syslog = None
    elif len(args.log_to_syslog) == 0:
        # no args means default syslog settings
        log_to_syslog = (None, None)
    elif len(args.log_to_syslog) == 1:
        log_to_syslog = (args.log_to_syslog[0], None)
    else:
        log_to_syslog = (args.log_to_syslog[0], args.log_to_syslog[1])
    print(". log_to_syslog:", log_to_syslog)
    MutyLogger.get_instance(
        "gulp", logger_file_path=logger_file_path, level=lv, log_to_syslog=log_to_syslog, reconfigure=True
    )
    if __RUN_TESTS__:
        # test stuff
        asyncio.run(async_test())
        return 0

    # get params
    try:
        if args.version:
            # print version string and exit
            print(ver)
        elif args.stop:
            # stop running instance using pidfile
            cfg = GulpConfig.get_instance()
            pidfile = os.path.join(cfg.path_working_dir(), "gulp.pid")
            if not os.path.exists(pidfile):
                print("No gulp pidfile found, is gulp running?")
                return 1
            try:
                with open(pidfile, "r", encoding="utf-8") as f:
                    pid = int(f.read().strip())
            except Exception as ex:
                print("Failed to read pidfile: %s" % (ex))
                return 1
            print("Stopping gulp pid=%d..." % (pid))
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                print("Process %d not found, removing stale pidfile." % (pid))
                try:
                    os.remove(pidfile)
                except Exception:
                    pass
                return 0
            except PermissionError as ex:
                print("Permission denied when killing pid %d: %s" % (pid, ex))
                return 1

            # wait for process to exit
            timeout = 10.0
            interval = 0.5
            waited = 0.0
            while waited < timeout:
                try:
                    os.kill(pid, 0)
                except OSError:
                    # process gone
                    break
                time.sleep(interval)
                waited += interval

            try:
                if waited >= timeout:
                    print("Process did not exit in time, sending SIGKILL...")
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except Exception:
                        pass
                # cleanup pidfile
                try:
                    os.remove(pidfile)
                except Exception:
                    pass
            except Exception:
                pass
            print("Stopped gulp (pid=%d)." % (pid))
            return 0
        else:
            create_operation: str = None
            if args.create:
                create_operation = args.create[0]

            # default
            print("%s\n%s" % (banner, ver))
            GulpServer.get_instance().start(
                logger_file_path=logger_file_path,
                level=lv,
                reset_collab=args.reset_collab,
                create_operation=create_operation,
                log_to_syslog=log_to_syslog,
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
