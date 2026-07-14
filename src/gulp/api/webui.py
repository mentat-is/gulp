"""Serve the packaged gULP web UI."""

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse


def add_webui_route(app: FastAPI, directory: Path) -> None:
    """Add a SPA fallback route when packaged UI assets are available."""
    directory = directory.resolve()
    index = directory / "index.html"
    if not index.is_file():
        return

    @app.get("/{path:path}", include_in_schema=False)
    async def webui(path: str) -> FileResponse:
        target = (directory / path).resolve()
        if target.is_relative_to(directory) and target.is_file():
            return FileResponse(target)
        return FileResponse(index)
