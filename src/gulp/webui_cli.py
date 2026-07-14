"""Serve the bundled gULP web UI with pnpm."""

import argparse
import os
import shutil
from pathlib import Path

_SERVE_PACKAGE = "serve@14.2.5"


def bundled_webui_path() -> Path:
    """Return the installed compiled web UI directory."""
    return Path(__file__).with_name("webui")


def main(argv: list[str] | None = None) -> int:
    """Run the bundled web UI on a standalone static server."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", default=3000, type=int)
    args = parser.parse_args(argv)

    webui_path = bundled_webui_path()
    if not (webui_path / "index.html").is_file():
        parser.error(f"bundled web UI not found at {webui_path}")

    pnpm = shutil.which("pnpm")
    if not pnpm:
        parser.error("pnpm is required to serve the bundled web UI")

    os.execv(
        pnpm,
        [
            pnpm,
            "dlx",
            _SERVE_PACKAGE,
            "-s",
            str(webui_path),
            "-l",
            f"tcp://{args.host}:{args.port}",
            "--no-port-switching",
            "--no-clipboard",
        ],
    )
    return 0
