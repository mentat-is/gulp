import multiprocessing
import os
import platform
import signal
import subprocess
import sys
import uuid

import psutil
from importlib.metadata import PackageNotFoundError
from muty.log import MutyLogger
import muty.crypto
from importlib.metadata import version


def check_os(exclude: list = None):
    """
    Check the operating system and raise an exception if it is not supported.

    Args:
        exclude (list, optional): A list of operating systems to exclude. Defaults to None.

    Raises:
        RuntimeError: If the operating system is not supported.

    """
    if exclude is None:
        exclude = []

    if platform.system().lower() in exclude:
        raise RuntimeError("Unsupported operating system: %s" % (platform.system()))


def check_package_version(package_name: str, version_check: str = None) -> bool:
    """
    Check if a package is installed and optionally check its version.
    Supports exact match (==), version ranges (>=X.Y.Z,<=A.B.C),
    and comparisons (>, <, >=, <=).

    Args:
        package_name (str): The name of the package to check
        version_check (str, optional): Version requirement
    """

    def _parse_version(version_str: str) -> tuple:
        try:
            parts = version_str.strip().split(".")
            return tuple(int(x) for x in parts)
        except (ValueError, AttributeError):
            return tuple()

    try:
        pkg_version = version(package_name)
        if not version_check:
            return True

        pkg_tuple = _parse_version(pkg_version)
        if not pkg_tuple:
            return False

        version_check = version_check.strip()
        if "," in version_check:
            min_ver, max_ver = version_check.split(",")
            min_check, max_check = min_ver.strip(), max_ver.strip()

            meets_min = meets_max = True
            for check, comp in [(min_check, ">="), (max_check, "<=")]:
                if check.startswith(comp):
                    ver = check[2:].strip()
                    ver_tuple = _parse_version(ver)
                    if comp == ">=":
                        meets_min = ver_tuple and pkg_tuple >= ver_tuple
                    else:
                        meets_max = ver_tuple and pkg_tuple <= ver_tuple
            return meets_min and meets_max

        operators = {
            ">=": lambda x, y: x >= y,
            ">": lambda x, y: x > y,
            "<=": lambda x, y: x <= y,
            "<": lambda x, y: x < y,
        }

        for op, func in operators.items():
            if version_check.startswith(op):
                req_version = version_check[len(op) :].strip()
                req_tuple = _parse_version(req_version)
                return req_tuple and func(pkg_tuple, req_tuple)

        return pkg_version == version_check

    except (PackageNotFoundError, AttributeError, ValueError):
        return False


def check_and_install_package(package_name: str, version_check: str = None) -> None:
    """
    check if a package is installed and optionally check its version.

    if the package is not installed or has the wrong version, install it.

    Args:
        package_name (str): the name of the package to check
        version_check (str, optional): version requirement, accepts exact match (i.e. "1.0.0"), version ranges (i.e. ">=1.0.0,<2"), comparisons (i.e. ">1.0.0"). Defaults to None.
    """
    if not check_package_version(package_name, version_check):
        to_install = package_name
        if version_check:
            # check version
            if "," in version_check or any(
                op in version_check for op in [">=", ">", "<=", "<"]
            ):
                # version range or comparison
                to_install = f"{package_name}{version_check}"
            else:
                # exact version
                to_install = f"{package_name}=={version_check}"

        MutyLogger.get_instance().info(
            f"{package_name} not found or wrong version, installing {to_install} ..."
        )

        # install
        subprocess.check_call([sys.executable, "-m", "pip", "install", to_install])


def get_threads_per_core(logical=False) -> int:
    """! get number of threads per cpu core

    Args:
        logical (bool, optional): logical cores means the number of physical cores multiplied by the number of threads that can run on each core. Defaults to False.

    Returns:
        int: _description_
    """
    n = psutil.cpu_count() // psutil.cpu_count(logical=logical)
    return n


def multiprocessing_fixes():
    """! fixes for multiprocessing on macos and linux.

    namely, on macos, we need to set multiprocessing to use fork() instead of spawn().
    then we need to ignore SIGCHLD to avoid zombies on processes exit.
    """
    plat = platform.system().lower()
    MutyLogger.get_instance().info("running on %s ..." % (plat))
    if platform.system().lower() == "darwin":
        # by default, now python use spawn() on macos, and this would break gulp multiprocessing engine
        # anyway this is just for developing on macos, production will use linux.
        MutyLogger.get_instance().warning("setting multiprocessing to use fork!")
        multiprocessing.set_start_method("fork")

    # avoid zombies on processes exit
    signal.signal(signal.SIGCHLD, signal.SIG_IGN)


def _child_handler(signum, frame):
    """Reap zombie children"""
    try:
        while True:
            pid, _ = os.waitpid(-1, os.WNOHANG)
            if pid <= 0:
                break
    except ChildProcessError:
        pass


def respawn_current_process():
    """
    respawn the current process using fork() and execvp().
    """
    # setup proper signal handling for platform
    if platform.system() != "Darwin":
        signal.signal(signal.SIGCHLD, signal.SIG_IGN)
    else:
        signal.signal(signal.SIGCHLD, _child_handler)

    # First fork
    pid = os.fork()
    if pid > 0:
        os.kill(os.getpid(), signal.SIGKILL)

    # decouple from parent
    os.setsid()
    os.umask(0)

    # Second fork
    pid = os.fork()
    if pid > 0:
        # kill parent
        os.kill(os.getpid(), signal.SIGKILL)

    # child continues
    args = [sys.executable] + sys.argv
    os.execvp(sys.executable, args)


def get_machine_fingerprint(print_components: bool = False) -> bytes:
    """
    returns a fingerprint of the current system built from more stable characteristics:
    - uname fields
    - mac address
    - CPU info from /proc/cpuinfo (model/vendor)
    - BIOS/DMI fields from /sys/class/dmi/id

    Args:
        print_components (bool, optional): print fingerprint components on stdout, for debugging. Defaults to False.

    :return: 32 bytes hash
    """
    sy = platform.uname()
    mac = uuid.getnode()

    parts = [
        sy.system,
        sy.node,
        sy.machine,
        str(mac),
    ]

    # add CPU info (model name, vendor) from /proc/cpuinfo if available
    try:
        with open("/proc/cpuinfo", "r", encoding="utf-8", errors="ignore") as f:
            cpuinfo = f.read()
        model = ""
        vendor = ""
        for line in cpuinfo.splitlines():
            if not model and line.lower().startswith("model name"):
                model = line.split(":", 1)[1].strip()
            if not vendor and line.lower().startswith("vendor_id"):
                vendor = line.split(":", 1)[1].strip()
            if model and vendor:
                break
        parts.extend([model, vendor])
    except Exception:
        pass

    # add BIOS/DMI information from sysfs if available
    try:
        dmi_dir = "/sys/class/dmi/id"
        if os.path.isdir(dmi_dir):
            dmi_fields = (
                "bios_vendor",
                "board_name",
                "board_serial",
                "product_name",
                "product_serial",
                "product_uuid",
            )
            for fname in dmi_fields:
                fpath = os.path.join(dmi_dir, fname)
                if os.path.isfile(fpath):
                    try:
                        with open(fpath, "r", encoding="utf-8", errors="ignore") as f:
                            val = f.read().strip()
                        if val:
                            parts.append(val)
                    except Exception:
                        pass
    except Exception:
        pass

    fp = "|".join([p for p in parts if p is not None and p != ""])
    if print_components:
        print("[.] fingerprint components:\n%s" % (fp))
    return muty.crypto.hash_sha256(fp.encode("utf-8"), return_bytes=True)
