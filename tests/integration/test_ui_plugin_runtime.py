"""Compile real UI bundles and exercise anonymous discovery/download on a live backend."""

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from types import SimpleNamespace
import uuid

import httpx
import pytest

REPOSITORY = Path(__file__).resolve().parents[2]
BUILDER = REPOSITORY / "gulp-licensing/build_ui_plugins.py"
FIXTURE = REPOSITORY / "tests/fixtures/ui_plugins"
pytestmark = [pytest.mark.integration, pytest.mark.skipif(not BUILDER.is_file(), reason="Internal licensing checkout is not installed")]


@pytest.fixture(scope="module")
def built_package(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, dict]:
    """Build a real React/CSS plugin with translations in a transitive dependency."""
    archived = os.getenv("GULP_UI_PREBUILT_PACKAGE")
    if archived:
        output = Path(archived)
        metadata = json.loads(next((output / "plugins/ui").glob("*.json")).read_text())
        return output, metadata
    root = tmp_path_factory.mktemp("ui-runtime")
    source = root / "sources"
    shutil.copytree(FIXTURE, source)
    manifest = source / "RuntimeLogin.tsx.json"
    metadata = json.loads(manifest.read_text())
    metadata["id"] += "-" + uuid.uuid4().hex
    manifest.write_text(json.dumps(metadata))
    output = root / "package"
    result = subprocess.run(
        [sys.executable, str(BUILDER), "--plugins-dir", str(source), "--out", str(output)],
        capture_output=True, text=True, timeout=90,
    )
    assert result.returncode == 0, result.stderr
    metadata = json.loads(next((output / "plugins/ui").glob("*.json")).read_text())
    return output, metadata


@pytest.fixture(scope="module")
def installed_package(built_package: tuple[Path, dict]) -> dict:
    """Install only this test's uniquely named package and clean it up afterward."""
    directory = os.getenv("GULP_UI_TEST_WORKING_DIR")
    if not directory:
        pytest.skip("Set GULP_UI_TEST_WORKING_DIR to the live backend's test working directory")
    output, metadata = built_package
    root = Path(directory)
    manifest = root / "plugins/ui" / f"{metadata['id']}.json"
    bundle = root / "plugins/ui" / metadata["id"]
    assert not manifest.exists() and not bundle.exists()
    manifest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(output / "plugins/ui" / metadata["id"], bundle)
    shutil.copy2(output / "plugins/ui" / manifest.name, manifest)
    try:
        yield metadata
    finally:
        manifest.unlink(missing_ok=True)
        shutil.rmtree(bundle)


def test_real_bundle_contains_transitive_locales_and_content_hash(built_package: tuple[Path, dict]) -> None:
    """Package identity covers compiled assets and the dependent component's locales."""
    output, metadata = built_package
    bundle = output / "plugins/ui" / metadata["id"] / metadata["build_hash"]
    digest = hashlib.sha256()
    for name in sorted(metadata["assets"]):
        digest.update(name.encode() + b"\0" + (bundle / name).read_bytes() + b"\0")
    assert digest.hexdigest() == metadata["build_hash"]
    assert {path.name for path in output.iterdir()} == {"plugins"}
    assert bundle.is_relative_to(output / "plugins/ui")
    source = (bundle / "plugin.js").read_text()
    assert "Portale tradotto" in source
    assert "@/" not in source
    assert "__gulpUiPluginHost" in source
    assert set(metadata["assets"]) == {"plugin.js", "plugin.css"}


async def test_compiled_ui_tree_is_served_by_real_backend_routes(
    built_package: tuple[Path, dict], monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Compile, discover and download the single UI tree through the actual ASGI router."""
    from fastapi import FastAPI
    from gulp.api.server.utility import router
    from gulp.config import GulpConfig

    output, metadata = built_package
    config = SimpleNamespace(
        path_working_dir=lambda: str(output),
        path_plugins_extra=lambda: str(output / "plugins"),
        path_plugins_default=lambda: str(output / "default"),
    )
    monkeypatch.setattr(GulpConfig, "get_instance", lambda: config)
    app = FastAPI()
    app.include_router(router)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://ui-runtime.test") as client:
        response = await client.get("/ui_plugin_list")
        assert response.status_code == 200
        assert response.headers["cache-control"] == "no-cache"
        plugin = next(item for item in response.json()["data"] if item["id"] == metadata["id"])
        for field, asset_name, content_type in (
            ("entry_url", "plugin.js", "text/javascript"), ("css_url", "plugin.css", "text/css"),
        ):
            asset = await client.get(plugin[field])
            assert asset.status_code == 200
            assert asset.headers["content-type"].startswith(content_type)
            assert "immutable" in asset.headers["cache-control"]
            path = output / "plugins/ui" / metadata["id"] / metadata["build_hash"] / asset_name
            assert asset.content == path.read_bytes()
        base = plugin["entry_url"].rsplit("/", 1)[0]
        assert (await client.get(base + "/secret.json")).status_code == 404
        manifest = output / "plugins/ui" / f"{metadata['id']}.json"
        original = manifest.read_bytes()
        try:
            manifest.unlink()
            assert (await client.get("/ui_plugin_list")).json()["data"] == []
            assert (await client.get(plugin["entry_url"])).status_code == 404
        finally:
            manifest.write_bytes(original)


async def test_anonymous_discovery_and_download(gulp_base_url: str, installed_package: dict) -> None:
    """The real backend serves JS/CSS before login and never exposes undeclared files."""
    async with httpx.AsyncClient(base_url=gulp_base_url) as client:
        response = await client.get("/ui_plugin_list")
        assert response.status_code == 200
        plugin = next(p for p in response.json()["data"] if p["id"] == installed_package["id"])
        assert response.headers["cache-control"] == "no-cache"
        for field, content_type in (("entry_url", "text/javascript"), ("css_url", "text/css")):
            asset = await client.get(plugin[field])
            assert asset.status_code == 200
            assert asset.headers["content-type"].startswith(content_type)
            assert "immutable" in asset.headers["cache-control"]
            assert asset.content
        for name in ("source.tsx", "plugin.js.map", "secret.json"):
            assert (await client.get(plugin["entry_url"].rsplit("/", 1)[0] + "/" + name)).status_code == 404
        assert (await client.get(plugin["entry_url"].replace(plugin["build_hash"], "b" * 64))).status_code == 404


async def test_sdk_lists_runtime_packages_without_login(gulp_base_url: str, installed_package: dict) -> None:
    """The existing SDK wrapper preserves runtime metadata without requiring login."""
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        plugins = await client.plugins.list_ui()
    plugin = next(item for item in plugins if item["id"] == installed_package["id"])
    assert plugin["requires_ui_api"] == {"major": 1, "minor": 0}
    assert plugin["entry_url"].endswith("/plugin.js")


def test_names_file_and_failed_build_are_atomic(tmp_path: Path) -> None:
    """A names file selects entrypoints and a later compile failure publishes nothing."""
    source = tmp_path / "sources"
    shutil.copytree(FIXTURE, source)
    names = tmp_path / "names.txt"
    names.write_text("# customer selection\n\nRuntimeLogin\nRuntimeLogin\n")
    output = tmp_path / "selected"
    command = [sys.executable, str(BUILDER), "--plugins-dir", str(source), "--out", str(output)]
    selected = subprocess.run(command + ["--plugins-file", str(names)], capture_output=True, text=True, timeout=90)
    assert selected.returncode == 0, selected.stderr
    assert len(list((output / "plugins/ui").glob("*.json"))) == 1
    (source / "Broken.tsx").write_text("export default function ( { syntax error")
    (source / "Broken.tsx.json").write_text(json.dumps({
        "display_name": "Broken", "plugin": "broken", "version": "1",
    }))
    failed_output = tmp_path / "failed"
    failed = subprocess.run(command[:command.index("--out")] + ["--out", str(failed_output)], capture_output=True, text=True, timeout=90)
    assert failed.returncode != 0
    assert not failed_output.exists()
    assert len(list((output / "plugins/ui").glob("*.json"))) == 1

    existing = {path.relative_to(output): path.read_bytes() for path in output.rglob("*") if path.is_file()}
    failed_existing = subprocess.run(command, capture_output=True, text=True, timeout=90)
    assert failed_existing.returncode != 0
    assert {path.relative_to(output): path.read_bytes() for path in output.rglob("*") if path.is_file()} == existing


@pytest.mark.skipif(
    not BUILDER.with_name("build_paid_plugins.py").is_file(),
    reason="Internal backend package builder is not installed",
)
def test_ui_build_merges_into_backend_package_and_overwrites_selected_files(tmp_path: Path) -> None:
    """The real BE/UI CLIs share one output, preserving unrelated files on UI rebuild."""
    output = tmp_path / "customer"
    backend = subprocess.run(
        [sys.executable, str(BUILDER.with_name("build_paid_plugins.py")),
         "--customer_id", "ui-merge-test", "--fingerprint", "a" * 64,
         "--plugins", str(REPOSITORY / "tests/fixtures/plugins/enrich_update_marker.py"),
         "--out", str(output)],
        capture_output=True, text=True, timeout=30,
    )
    assert backend.returncode == 0, backend.stderr
    bytecode = output / "plugins/enrich_update_marker.pyc"
    original_bytecode = bytecode.read_bytes()
    mapping = output / "mapping_files/custom.json"
    mapping.parent.mkdir()
    mapping.write_text('{"preserve": true}')

    command = [sys.executable, str(BUILDER), "--plugins-dir", str(FIXTURE),
               "--plugin", "RuntimeLogin", "--out", str(output)]
    first = subprocess.run(command, capture_output=True, text=True, timeout=90)
    assert first.returncode == 0, first.stderr
    assert bytecode.read_bytes() == original_bytecode
    manifest = next((output / "plugins/ui").glob("*.json"))
    metadata = json.loads(manifest.read_text())
    asset = output / "plugins/ui" / metadata["id"] / metadata["build_hash"] / "plugin.js"
    assert "Portale tradotto" in asset.read_text()

    # Keep another valid UI package outside this rebuild's selected plugin set.
    shutil.copytree(output / "plugins/ui" / metadata["id"], output / "plugins/ui/unselected")
    (output / "plugins/ui/unselected.json").write_text(json.dumps({
        **metadata, "id": "unselected", "filename": "Unselected.tsx",
    }))
    preserved = {path.relative_to(output): path.read_bytes() for path in output.rglob("*")
                 if path.is_file() and path != manifest}
    manifest.write_text('{"stale": true}')
    rebuilt = subprocess.run(command, capture_output=True, text=True, timeout=90)
    assert rebuilt.returncode == 0, rebuilt.stderr
    assert all((output / path).read_bytes() == content for path, content in preserved.items())
    updated = json.loads(manifest.read_text())
    assert updated["id"] == metadata["id"]
    assert updated["version"] == metadata["version"]
    rebuilt_bundle = output / "plugins/ui" / updated["id"] / updated["build_hash"]
    digest = hashlib.sha256()
    for name in sorted(updated["assets"]):
        digest.update(name.encode() + b"\0" + (rebuilt_bundle / name).read_bytes() + b"\0")
    assert digest.hexdigest() == updated["build_hash"]
    assert "Portale tradotto" in (rebuilt_bundle / "plugin.js").read_text()


def test_browser_runtime_login(installed_package: dict) -> None:
    """Exercise the actual UI and native ESM loader in a temporary Chromium browser."""
    if not os.getenv("GULP_UI_BROWSER_URL") or not os.getenv("GULP_PLAYWRIGHT_MODULE"):
        pytest.skip("Set GULP_UI_BROWSER_URL and GULP_PLAYWRIGHT_MODULE for browser coverage")
    result = subprocess.run(
        ["node", str(FIXTURE / "browser.cjs")], capture_output=True, text=True, timeout=90,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_paid_bundles_in_browser(installed_package: dict) -> None:
    """Import prebuilt paid packages and exercise actual transform registration disposal."""
    directory = os.getenv("GULP_UI_PAID_BROWSER_PACKAGE")
    if not directory or not os.getenv("GULP_UI_BROWSER_URL") or not os.getenv("GULP_PLAYWRIGHT_MODULE"):
        pytest.skip("Set GULP_UI_PAID_BROWSER_PACKAGE and browser variables for paid coverage")
    output = Path(directory)
    root = Path(os.environ["GULP_UI_TEST_WORKING_DIR"])
    installed: list[tuple[Path, Path]] = []
    try:
        for source in sorted((output / "plugins/ui").glob("*.json")):
            metadata = json.loads(source.read_text())
            manifest = root / "plugins/ui" / source.name
            bundle = root / "plugins/ui" / metadata["id"]
            assert not manifest.exists() and not bundle.exists()
            shutil.copytree(output / "plugins/ui" / metadata["id"], bundle)
            installed.append((manifest, bundle))
            shutil.copy2(source, manifest)
        result = subprocess.run(
            ["node", str(FIXTURE / "paid_browser.cjs")], capture_output=True, text=True, timeout=90,
        )
        assert result.returncode == 0, result.stdout + result.stderr
    finally:
        for manifest, bundle in installed:
            manifest.unlink(missing_ok=True)
            shutil.rmtree(bundle)
