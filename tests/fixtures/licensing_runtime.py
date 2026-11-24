"""Run real licensing tools with simulated container identity files."""

import asyncio
import builtins
import io
import json
import os
from pathlib import Path
import runpy
import sys
from typing import Any, IO
from unittest.mock import patch

HOST_ID_PATH = "/run/gulp/host-machine-id"
RESULT_PREFIX = "LICENSING_RESULT="
_open = builtins.open
_exists = os.path.exists


def _open_identity(path: Any, *args: Any, **kwargs: Any) -> IO[Any]:
    """Redirect only machine identity reads; leave plugin loading untouched."""
    if path == HOST_ID_PATH:
        return _open(os.environ["TEST_HOST_MACHINE_ID_PATH"], *args, **kwargs)
    container_id = os.environ["TEST_CONTAINER_ID"]
    if path == "/proc/self/cgroup":
        return io.StringIO(f"0::/docker/{container_id}\n")
    if path == "/proc/self/mountinfo":
        return io.StringIO(f"/docker/containers/{container_id}/hostname\n")
    if path == "/etc/hostname":
        return io.StringIO(container_id[:12])
    return _open(path, *args, **kwargs)


def _identity_exists(path: str) -> bool:
    """Expose a Docker marker and the simulated read-only host mount."""
    if path == HOST_ID_PATH:
        return _exists(os.environ["TEST_HOST_MACHINE_ID_PATH"])
    if path == "/.dockerenv":
        return True
    return _exists(path)


async def _load_plugin(path: str) -> None:
    """Load and inspect a compiled plugin through the real gULP loader."""
    from gulp.plugin import GulpPluginBase, GulpPluginCacheMode

    plugin = await GulpPluginBase.load(
        path, cache_mode=GulpPluginCacheMode.IGNORE, run_post_init=False
    )
    try:
        print(
            RESULT_PREFIX
            + json.dumps(
                {
                    "name": plugin.name,
                    "display_name": plugin.display_name(),
                    "license": plugin.get_license(),
                    "valid": plugin.check_license(),
                }
            )
        )
    finally:
        await plugin.unload()


def main() -> None:
    """Run generation or plugin validation in a fresh simulated container."""
    mode, path = sys.argv[1:]
    with (
        patch("builtins.open", _open_identity),
        patch("os.path.exists", _identity_exists),
    ):
        if mode == "generate":
            sys.path.insert(0, str(Path(path).parent))
            sys.argv = [path, "--json", "--components"]
            runpy.run_path(path, run_name="__main__")
        else:
            asyncio.run(_load_plugin(path))


if __name__ == "__main__":
    main()
