import argparse
import asyncio
import logging
import os
import sys
from multiprocessing import freeze_support
import signal
import time

import art

from gulp.api.server_api import GulpServer
from gulp.config import GulpConfig
from gulp.glug import Glug

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
    #     Glug.get_instance().exception(str(ex))
    #     print("-----")
    #     print(ex.to_string())
    pass


def _pid_from_pidfile_path(pidfile_path: str) -> int:
    """Extract the process id from a gulp pidfile path."""
    bare_file = os.path.basename(pidfile_path)
    return int(bare_file.removeprefix("gulp").removesuffix(".pid"))


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
        "--log-level",
        nargs=1,
        metavar=("level"),
        help='select log level, default="debug".',
        choices=["critical", "error", "warning", "info", "debug"],
        default=["debug"],
    )
    parser.add_argument(
        "--dev-mode",
        help="enables glug dev_mode: forces DEBUG level and unlocks extra diagnostic content in glug-migrated "
        "call sites, independently of --log-level. Togglable at runtime via Redis (see plans/log-revolution.md).",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--log-dir",
        help="base directory for glug's per-process JSON log files, written to "
        "<log-dir>/<hostname>/gulp-main.log or gulp-worker-<pid>.log, IN ADDITION to glug's always-on "
        "stdout output (human-readable, colorized) — never instead of it. Omit to keep stdout only; pass "
        "this when the JSON files themselves need to be persisted to disk too (e.g. for a log-shipping "
        "sidecar, or the future audit plugin, see plans/log-revolution.md).",
        nargs=1,
        metavar=("path"),
        default=None,
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
        help="stop all gulp running instances on this machine (reads pidfile and sends SIGTERM)",
        action="store_true",
        default=False,
    )
    args = parser.parse_args()
    print(". command line args (sys.argv):\n%s" % (sys.argv))
    print(". command line args (parsed):\n%s" % (args))

    lv: str = logging.getLevelNamesMapping()[args.log_level[0].upper()]
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
            # stop all gulp running instances on this machine
            cfg = GulpConfig.get_instance()
            import muty.file

            files = muty.file.list_directory(cfg.path_working_dir(), "gulp*.pid")
            if not files:
                print("No gulp pidfiles found, is gulp running?")
                return 1

            pids: list[tuple[str, int]] = []
            for f in files:
                try:
                    pids.append((f, _pid_from_pidfile_path(f)))
                except ValueError as ex:
                    print(
                        "Failed to parse pidfile %s, skipping: %s (removed)" % (f, ex)
                    )
                    os.remove(f)
                    continue
            if not pids:
                print("No valid gulp pidfiles found, is gulp running ?")
                return 1

            for t_p in pids:
                pidfile_path = t_p[0]
                pid = t_p[1]

                # close this pidwith SIGINT and wait for it to exit, if it doesn't exit in time, send SIGKILL
                print("Stopping (SIGINT) gulp pid=%d..." % (pid))
                try:
                    os.kill(pid, signal.SIGINT)
                except ProcessLookupError:
                    print("Process %d not found, it may have already exited..." % (pid))
                except PermissionError as ex:
                    print("Permission denied when killing pid %d: %s" % (pid, ex))

                # wait for process to exit
                timeout = 10.0
                interval = 0.5
                waited = 0.0
                while waited < timeout:
                    try:
                        os.kill(pid, 0)
                    except OSError:
                        # process gone
                        print("Process %d exited." % (pid))
                        break
                    time.sleep(interval)
                    waited += interval

                if waited >= timeout:
                    print("Process %d did not exit in time, sending SIGKILL..." % (pid))
                    try:
                        os.kill(pid, signal.SIGKILL)
                        print("Sent SIGKILL to process %d." % (pid))
                    except Exception:
                        pass

                # removing pidfile (best effort, it may have already been removed by the running process)
                try:
                    os.remove(pidfile_path)
                    print("Removed pidfile %s." % (pidfile_path))
                except Exception:
                    pass
                    """print(
                        "Failed to remove pidfile, it may have already been removed: %s"
                        % (pidfile_path)
                    )"""
            Glug.get_instance().info(f"stopped {len(pids)} gulp instances")
            return 0
        else:
            create_operation: str = None
            if args.create:
                create_operation = args.create[0]

            # default
            print("%s\n%s" % (banner, ver))
            GulpServer.get_instance().start(
                level=lv,
                reset_collab=args.reset_collab,
                create_operation=create_operation,
                log_dir=args.log_dir[0] if args.log_dir else None,
                dev_mode=args.dev_mode,
            )
    except Exception:
        # print exception and exit
        Glug.get_instance().exception("fatal error in main()")
        return 1

    # done
    return 0


if __name__ == "__main__":
    freeze_support()  # this is needed for macos
    sys.exit(main())
