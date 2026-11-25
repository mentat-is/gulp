"""Validate isolated UI package selection, discovery and filesystem boundaries."""

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from gulp.api import ui_plugins

REPOSITORY = Path(__file__).resolve().parents[2]
BUILDER_PATH = REPOSITORY / "gulp-licensing/build_ui_plugins.py"
builder = None
if BUILDER_PATH.is_file():
    SPEC = importlib.util.spec_from_file_location("ui_builder", BUILDER_PATH)
    builder = importlib.util.module_from_spec(SPEC)
    SPEC.loader.exec_module(builder)


@pytest.fixture
def package_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Use a temporary package tree without initializing a real backend config."""
    config = SimpleNamespace(
        path_working_dir=lambda: str(tmp_path),
        path_plugins_extra=lambda: str(tmp_path / "plugins"),
        path_plugins_default=lambda: str(tmp_path / "default"),
    )
    monkeypatch.setattr(ui_plugins.GulpConfig, "get_instance", lambda: config)
    return tmp_path


def install_manifest(root: Path, **overrides: object) -> dict:
    """Create one complete package with JSON-only activation metadata."""
    manifest = {
        "id": "test-plugin", "filename": "Test.tsx", "display_name": "Test",
        "plugin": "test", "version": "1.0.0", "requires_ui_api": {"major": 1, "minor": 0},
        "build_hash": "a" * 64, "assets": ["plugin.js", "plugin.css"],
        **overrides,
    }
    directory = root / "plugins/ui" / manifest["id"] / manifest["build_hash"]
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "plugin.js").write_text("export default function Test() {}")
    (directory / "plugin.css").write_text(".test {}")
    metadata_dir = root / "plugins/ui"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    (metadata_dir / "test.json").write_text(json.dumps(manifest))
    return manifest


def test_json_only_manifest_gets_backend_owned_urls(package_root: Path) -> None:
    """Discovery requires compiled files, never a TSX source or client-supplied URL."""
    install_manifest(package_root, entry_url="https://untrusted.test/plugin.js")
    plugins = ui_plugins.list_ui_plugins()
    assert len(plugins) == 1
    assert plugins[0].entry_url == f"/ui_plugin_asset/test-plugin/{'a' * 64}/plugin.js"
    assert plugins[0].css_url.endswith("/plugin.css")
    assert ui_plugins.get_ui_plugin_asset("test-plugin", "a" * 64, "plugin.js") == (
        package_root / "plugins/ui/test-plugin" / ("a" * 64) / "plugin.js"
    )
    assert not list((package_root / "plugins/ui").glob("*.tsx"))


@pytest.mark.parametrize("asset", ["../secret", "plugin.js.map", "LICENSE.txt", "other.js"])
def test_undeclared_assets_are_rejected(package_root: Path, asset: str) -> None:
    """A valid id/hash cannot grant access to arbitrary sibling files."""
    install_manifest(package_root)
    with pytest.raises(FileNotFoundError):
        ui_plugins.get_ui_plugin_asset("test-plugin", "a" * 64, asset)


def test_symlinks_outside_bundle_root_are_rejected(package_root: Path) -> None:
    """A declared asset cannot escape through a filesystem symlink."""
    install_manifest(package_root)
    asset = package_root / "plugins/ui/test-plugin" / ("a" * 64) / "plugin.js"
    asset.unlink()
    secret = package_root / "secret.js"
    secret.write_text("private")
    asset.symlink_to(secret)
    assert ui_plugins.list_ui_plugins() == []


def test_incomplete_and_invalid_packages_do_not_hide_valid_packages(package_root: Path) -> None:
    """Discovery isolates malformed JSON and missing CSS from unrelated manifests."""
    install_manifest(package_root)
    directory = package_root / "plugins/ui"
    (directory / "broken.json").write_text("{broken")
    incomplete = json.loads((directory / "test.json").read_text())
    incomplete["id"] = "missing"
    (directory / "missing.json").write_text(json.dumps(incomplete))
    assert [p.id for p in ui_plugins.list_ui_plugins()] == ["test-plugin"]


def test_removing_manifest_disables_asset(package_root: Path) -> None:
    """Stored bundle files alone never enable a plugin."""
    install_manifest(package_root)
    (package_root / "plugins/ui/test.json").unlink()
    with pytest.raises(FileNotFoundError):
        ui_plugins.get_ui_plugin_asset("test-plugin", "a" * 64, "plugin.js")


def test_bundle_directory_with_json_suffix_is_not_a_manifest(package_root: Path) -> None:
    """Discovery scans only files when plugin-id directories share the JSON suffix."""
    install_manifest(package_root, id="bundle.json")
    assert [plugin.id for plugin in ui_plugins.list_ui_plugins()] == ["bundle.json"]


@pytest.mark.skipif(builder is None, reason="Internal licensing checkout is not installed")
def test_builder_selects_names_ids_and_all(tmp_path: Path) -> None:
    """Selection preserves entrypoint identities and deduplicates repeated names."""
    source = tmp_path / "Test.tsx"
    source.write_text("export default () => null")
    source.with_suffix(".tsx.json").write_text(json.dumps({
        "id": "custom-test", "display_name": "Test", "plugin": "test", "version": "1",
    }))
    available = builder.discover_plugins(tmp_path)
    assert len(builder.select_plugins(available, None)) == 1
    assert builder.select_plugins(available, ["custom-test", "Test", "Test.tsx"]) == available
    assert available[0][1]["filename"] == "Test.tsx"
    assert available[0][1]["requires_ui_api"] == {"major": 1, "minor": 0}
    with pytest.raises(ValueError, match="found 0"):
        builder.select_plugins(available, ["missing"])
    with pytest.raises(ValueError, match="No UI plugins"):
        builder.select_plugins(available, [])
    with pytest.raises(ValueError, match="found 2"):
        builder.select_plugins(available + available, ["custom-test"])


@pytest.mark.skipif(builder is None, reason="Internal licensing checkout is not installed")
def test_ui_merge_overwrites_matching_asset_paths_and_keeps_backend_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Identical compiled content replaces existing target files without replacing the tree."""
    source = tmp_path / "Test.tsx"
    source.write_text("export default () => null")
    source.with_suffix(".tsx.json").write_text(json.dumps({
        "id": "test", "display_name": "Test", "plugin": "test", "version": "1",
    }))
    selected = builder.discover_plugins(tmp_path)
    output = tmp_path / "customer"
    bytecode = output / "plugins/extension/keep.pyc"
    bytecode.parent.mkdir(parents=True)
    bytecode.write_bytes(b"existing licensed backend plugin")

    def compile_fixture(arguments: list[str], *, check: bool) -> None:
        """Provide stable compiler output to exercise exact destination-file replacement."""
        compiled = Path(arguments[3])
        compiled.mkdir()
        (compiled / "plugin.js").write_text("export default () => null;")

    monkeypatch.setattr(builder.subprocess, "run", compile_fixture)
    builder.build_plugins(selected, output, tmp_path)
    manifest = output / "plugins/ui/test.json"
    expected_manifest = manifest.read_bytes()
    metadata = json.loads(expected_manifest)
    asset = output / "plugins/ui/test" / metadata["build_hash"] / "plugin.js"
    expected_asset = asset.read_bytes()
    manifest.write_text("stale manifest")
    asset.write_text("stale asset")
    builder.build_plugins(selected, output, tmp_path)
    assert manifest.read_bytes() == expected_manifest
    assert asset.read_bytes() == expected_asset
    assert bytecode.read_bytes() == b"existing licensed backend plugin"
