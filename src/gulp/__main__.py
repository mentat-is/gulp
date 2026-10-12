import argparse
import asyncio
import logging
import os
import random
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


# bright ANSI foreground colors the startup banner/version are picked from
_BANNER_COLORS = ["91", "92", "93", "94", "95", "96"]


def _colored_banner(banner: str, ver: str, use_color: bool) -> str:
    """
    formats the startup banner and version string for stdout.

    Args:
        banner (str): the ascii-art banner.
        ver (str): the version string.
        use_color (bool): whether to emit ANSI color codes; callers should pass
            sys.stdout.isatty(), as the console formatter does.

    Returns:
        str: the banner in a random color, followed by the version in bold and in
            another color (both plain when use_color is False).
    """
    if not use_color:
        return "%s\n%s" % (banner, ver)
    banner_color, ver_color = random.sample(_BANNER_COLORS, 2)
    return "\x1b[%sm%s\x1b[0m\n\x1b[1;%sm%s\x1b[0m" % (
        banner_color,
        banner,
        ver_color,
        ver,
    )


def _describe_args(
    parser: argparse.ArgumentParser, argv: list[str] = None
) -> dict[str, str]:
    """
    describes the effective value of every command line option, for the startup debug log.

    Args:
        parser (argparse.ArgumentParser): the gulp argument parser.
        argv (list[str], optional): the arguments to parse. Defaults to None (sys.argv[1:]).

    Returns:
        dict[str, str]: option dest -> effective value, suffixed with " (default)" when
            the option was not given and its default applies.
    """
    args = parser.parse_args(argv)

    # argparse only fills in a default when the namespace lacks that dest, so a
    # namespace pre-filled with a sentinel tells "not given" apart from "given
    # with the default value" (e.g. an explicit --log-level debug)
    unset = object()
    probe = parser.parse_args(
        argv, namespace=argparse.Namespace(**{k: unset for k in vars(args)})
    )

    described: dict[str, str] = {}
    for k, v in vars(args).items():
        # nargs=1 options parse to a one-item list
        value = v[0] if isinstance(v, list) and len(v) == 1 else v
        described[k] = (
            f"{value!r} (default)" if getattr(probe, k) is unset else repr(value)
        )
    return described


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
        help='select log level, default="info". Given explicitly, it also takes precedence over '
        "a level saved by an extension plugin.",
        choices=["critical", "error", "warning", "info", "debug"],
        default=None,
    )
    parser.add_argument(
        "--log-file",
        help="also append every log line printed on stdout (same content, no colors) to this file, "
        "shared by the main process and every worker. gulp never rotates it: use an external tool "
        "such as logrotate. Omit to log to stdout only.",
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

    # None when --log-level is not given: the server applies INFO, and an extension
    # plugin keeping its own saved level can tell the two cases apart
    lv: int | None = (
        logging.getLevelNamesMapping()[args.log_level[0].upper()]
        if args.log_level
        else None
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

            # --version/--stop never reach this branch, no point logging them
            cli_options: dict[str, str] = {
                k: v
                for k, v in _describe_args(parser).items()
                if k not in ("version", "stop")
            }

            if lv is None:
                cli_options["log_level"] = "'info' (default)"

            # default
            print(_colored_banner(banner, ver, sys.stdout.isatty()))
            if not GulpServer.get_instance().start(
                level=lv,
                reset_collab=args.reset_collab,
                create_operation=create_operation,
                log_file=args.log_file[0] if args.log_file else None,
                cli_options=cli_options,
            ):
                # a required service is unreachable, already logged with a hint
                return 1
    except Exception:
        # print exception and exit
        Glug.get_instance().exception("fatal error in main()")
        return 1

    # done
    return 0


if __name__ == "__main__":
    freeze_support()  # this is needed for macos
    sys.exit(main())
