"""Discover installed UI manifests and resolve their public compiled assets."""

from pathlib import Path
from typing import Literal

import json5
from pydantic import BaseModel, ConfigDict, Field, model_validator

from gulp.config import GulpConfig
from muty.log import MutyLogger


class GulpUiPluginApiVersion(BaseModel):
    """UI API major and minimum minor required by a compiled plugin."""

    major: int = Field(ge=1)
    minor: int = Field(ge=0)


class GulpUiPluginMetadata(BaseModel):
    """A JSON-only activation manifest for an installed compiled UI plugin."""

    model_config = ConfigDict(extra="allow")

    id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
    filename: str = Field(min_length=1)
    display_name: str
    plugin: str
    extension: bool = True
    version: str
    desc: str = ""
    type: str | list[str] = Field(default_factory=list)
    targets: list[dict] = Field(default_factory=list)
    requires_ui_api: GulpUiPluginApiVersion
    build_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    assets: list[Literal["plugin.js", "plugin.css"]]
    entry_url: str = ""
    css_url: str | None = None

    @model_validator(mode="after")
    def validate_assets(self) -> "GulpUiPluginMetadata":
        """Require one JavaScript entry and reject duplicate asset declarations."""
        if "plugin.js" not in self.assets or len(set(self.assets)) != len(self.assets):
            raise ValueError("assets must include plugin.js and contain no duplicates")
        return self


def _asset_path(metadata: GulpUiPluginMetadata, asset: str) -> Path:
    """Resolve a declared asset while rejecting symlink and directory escapes."""
    root = (Path(GulpConfig.get_instance().path_working_dir()) / "plugins/ui").resolve()
    path = (root / metadata.id / metadata.build_hash / asset).resolve()
    if asset not in metadata.assets or not path.is_relative_to(root) or not path.is_file():
        raise FileNotFoundError("UI plugin asset not found")
    return path


def list_ui_plugins() -> list[GulpUiPluginMetadata]:
    """Read valid manifests, preferring the working directory over built-ins."""
    config = GulpConfig.get_instance()
    roots = [config.path_plugins_extra(), config.path_plugins_default()]
    packages: dict[str, GulpUiPluginMetadata] = {}
    filenames: set[str] = set()
    for root in roots:
        if not root:
            continue
        for manifest in sorted((Path(root) / "ui").glob("*.json")):
            if not manifest.is_file():
                continue
            try:
                metadata = GulpUiPluginMetadata.model_validate(json5.loads(manifest.read_text()))
                if metadata.id in packages or metadata.filename in filenames:
                    continue
                for asset in metadata.assets:
                    _asset_path(metadata, asset)
                prefix = f"/ui_plugin_asset/{metadata.id}/{metadata.build_hash}"
                # URLs always refer to this backend, never to manifest-supplied hosts.
                metadata.entry_url = f"{prefix}/plugin.js"
                metadata.css_url = f"{prefix}/plugin.css" if "plugin.css" in metadata.assets else None
                packages[metadata.id] = metadata
                filenames.add(metadata.filename)
            except (ValueError, OSError) as error:
                MutyLogger.get_instance().warning("Skipping UI manifest %s: %s", manifest, error)
    return list(packages.values())


def get_ui_plugin_asset(plugin_id: str, build_hash: str, asset: str) -> Path:
    """Return an asset only while its matching activation manifest is installed."""
    for metadata in list_ui_plugins():
        if metadata.id == plugin_id and metadata.build_hash == build_hash:
            return _asset_path(metadata, asset)
    raise FileNotFoundError("UI plugin asset not found")
