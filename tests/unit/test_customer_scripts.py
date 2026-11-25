"""Exercise real installer/update entrypoints with isolated Docker command results."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[2]
SCRIPTS = REPOSITORY / "scripts"
pytestmark = [
    pytest.mark.unit,
    pytest.mark.skipif(not (SCRIPTS / "update.sh").is_file(), reason="Private customer scripts are unavailable"),
]

DOCKER_FIXTURE = r'''
import json
import os
import subprocess
import sys

args = sys.argv[1:]
with open(os.environ["TEST_DOCKER_LOG"], "a") as log:
    log.write(json.dumps(args) + "\n")
if args == ["info"]:
    sys.exit(0)
if args[:2] == ["image", "inspect"]:
    sys.exit(1 if args[-1] == os.environ.get("TEST_MISSING_IMAGE") else 0)
if args[0] == "inspect":
    print("/etc/machine-id" if "/run/gulp/host-machine-id" in args[2] else
          os.environ.get("TEST_MOUNT_SOURCE", os.environ["GULP_WORKING_DIR"]))
    sys.exit(0)
if args[0] != "compose":
    sys.exit("Unexpected Docker command: " + repr(args))
if args[1:] == ["version"]:
    sys.exit(subprocess.run([os.environ["TEST_REAL_DOCKER"], *args]).returncode)
commands = {"config", "run", "up", "ps", "exec", "pull"}
index = next(i for i, arg in enumerate(args) if arg in commands)
command, tail = args[index], args[index + 1:]
if command == "config":
    sys.exit(subprocess.run([os.environ["TEST_REAL_DOCKER"], *args]).returncode)
if command == "pull":
    sys.exit(int(os.environ.get("TEST_PULL_FAILURE", "0")))
if command == "ps":
    if "--quiet" in tail and not os.environ.get("TEST_NO_INSTALL"):
        print(tail[-1] + "-fixture")
    sys.exit(0)
if command == "run":
    entrypoint = tail[tail.index("--entrypoint") + 1]
    if entrypoint == "python3":
        code = tail[tail.index("-c") + 1]
        sys.exit(subprocess.run([sys.executable, "-c", code, os.environ["GULP_WORKING_DIR"]]).returncode)
    if entrypoint == "/bin/sh":
        sys.exit(0)
if command == "exec":
    print(json.dumps({"fingerprint": "a" * 64, "method": "docker_host_machine_id"}))
    sys.exit(0)
if command == "up":
    sys.exit(0)
sys.exit("Unexpected Compose command: " + repr(args))
'''


@pytest.fixture
def package(tmp_path: Path) -> tuple[Path, dict[str, str], Path]:
    """Copy private scripts to a path with spaces and proxy only Docker mutations."""
    root = tmp_path / "customer package"
    root.mkdir()
    for name in ("install.sh", "update.sh", "docker-compose-docker-prod.yml"):
        shutil.copy2(SCRIPTS / name, root / name)
    (root / ".env").write_text(
        "GULP_IMAGE=gulp-core:fixture\nGULP_UI_IMAGE=gulp-ui:fixture\nPYTHON_VERSION=3.13\n"
        "POSTGRES_USER=test\nPOSTGRES_PASSWORD=test\nPOSTGRES_DB=test\n"
        "OPENSEARCH_USER=test\nOPENSEARCH_PASSWORD=test\nREDIS_PASSWORD=test\n"
        "SEAWEEDFS_ACCESS_KEY=test\nSEAWEEDFS_SECRET_KEY=test\nSFTPD_USER=test\nSFTPD_PASSWORD=test\n"
    )
    for name in ("generate_fingerprint.py", "fingerprint.py", "gulp_cfg.json", "customer.license"):
        (root / name).write_text("preserve me")
    (root / "plugins/ui").mkdir(parents=True)
    binary_dir = tmp_path / "bin"
    binary_dir.mkdir()
    docker = binary_dir / "docker"
    docker.write_text(f"#!{sys.executable}\n{DOCKER_FIXTURE}")
    docker.chmod(0o755)
    log = tmp_path / "docker.jsonl"
    environment = {key: value for key, value in os.environ.items()
                   if not key.startswith(("GULP_", "COMPOSE_", "TEST_"))}
    environment.update({
        "PATH": f"{binary_dir}:{os.environ['PATH']}", "TEST_DOCKER_LOG": str(log),
        "TEST_REAL_DOCKER": shutil.which("docker"),
    })
    return root, environment, log


def _run(
    package: tuple[Path, dict[str, str], Path], script: str = "update.sh", *arguments: str,
) -> subprocess.CompletedProcess[str]:
    """Invoke from another directory to verify package-relative working-dir selection."""
    root, environment, _ = package
    return subprocess.run(["bash", str(root / script), *arguments], cwd=root.parent,
                          env=environment, capture_output=True, text=True, timeout=30)


def _commands(package: tuple[Path, dict[str, str], Path]) -> list[list[str]]:
    """Read recorded Docker calls, including delegated real Compose probes."""
    log = package[2]
    return [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []


def _runtime_package(root: Path) -> Path:
    """Install one valid manifest and compiled JS with an embedded locale dictionary."""
    bundle = root / "plugins/ui/test" / ("a" * 64)
    bundle.mkdir(parents=True)
    asset = bundle / "plugin.js"
    asset.write_text('export const locales={it:{title:"Prova"}}; export default ()=>null;')
    (root / "plugins/ui/test.json").write_text(json.dumps({
        "id": "test", "filename": "Test.tsx", "display_name": "Test", "plugin": "test",
        "version": "1.0.0", "requires_ui_api": {"major": 1, "minor": 0},
        "build_hash": "a" * 64, "assets": ["plugin.js"],
    }))
    return asset


@pytest.mark.parametrize("offline", [False, True])
def test_updater_replaces_only_applications_and_preserves_package(
    package: tuple[Path, dict[str, str], Path], offline: bool,
) -> None:
    """Both images are checked before targeted recreation; offline mode never pulls."""
    root, _, _ = package
    _runtime_package(root)
    original = {file: file.read_bytes() for file in root.rglob("*") if file.is_file()}
    result = _run(package, "update.sh", *(["--no-pull"] if offline else []))
    assert result.returncode == 0, result.stderr
    commands = _commands(package)
    pulls = [args for args in commands if "pull" in args]
    assert len(pulls) == (0 if offline else 1)
    if pulls:
        assert pulls[0][-3:] == ["pull", "gulp", "gulp-ui"]
    inspections = [args for args in commands if args[:2] == ["image", "inspect"]]
    assert {"gulp-core:fixture", "gulp-ui:fixture"} <= {args[-1] for args in inspections}
    updates = [args for args in commands if "up" in args]
    assert len(updates) == 1
    assert updates[0][updates[0].index("up"):] == [
        "up", "--detach", "--no-deps", "--no-build", "--pull", "never",
        "--force-recreate", "--wait", "--wait-timeout", "120", "gulp", "gulp-ui",
    ]
    assert all(commands.index(args) < commands.index(updates[0]) for args in inspections + pulls)
    assert all(file.read_bytes() == content for file, content in original.items())
    assert not any("down" in args or "run" in args or "exec" in args for args in commands)


@pytest.mark.parametrize("failure,arguments", [
    ({"TEST_PULL_FAILURE": "1"}, []),
    ({"TEST_MISSING_IMAGE": "gulp-ui:fixture"}, ["--no-pull"]),
    ({"TEST_NO_INSTALL": "1"}, []),
    ({"TEST_MOUNT_SOURCE": "/another/package"}, []),
])
def test_update_failure_never_recreates_containers(
    package: tuple[Path, dict[str, str], Path], failure: dict[str, str], arguments: list[str],
) -> None:
    """Missing images, failed downloads and wrong installations fail before mutation."""
    package[1].update(failure)
    result = _run(package, "update.sh", *arguments)
    assert result.returncode != 0
    assert not any("up" in args for args in _commands(package))


@pytest.mark.parametrize("with_plugin", [True, False])
@pytest.mark.parametrize("offline", [False, True])
def test_installer_accepts_runtime_packages_without_a_ui_build(
    package: tuple[Path, dict[str, str], Path], with_plugin: bool, offline: bool,
) -> None:
    """Bootstrap pulls both apps unless offline and accepts the unified UI tree."""
    if with_plugin:
        _runtime_package(package[0])
    result = _run(package, "install.sh", *(["--no-pull"] if offline else []))
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["method"] == "docker_host_machine_id"
    commands = _commands(package)
    pulls = [args for args in commands if "pull" in args]
    assert len(pulls) == (0 if offline else 1)
    if pulls:
        assert pulls[0][-3:] == ["pull", "gulp", "gulp-ui"]
        assert commands.index(pulls[0]) < next(i for i, args in enumerate(commands) if "run" in args)
    assert any("up" in args for args in commands)
    assert not any("--no-recreate" in args for args in commands)
    assert any(args[0] == "inspect" and "/run/gulp/host-machine-id" in args[2]
               for args in commands)


def test_installer_generates_fingerprint_before_customer_plugin_delivery(
    package: tuple[Path, dict[str, str], Path],
) -> None:
    """An empty instance yields its fingerprint before a later compiled delivery."""
    first = _run(package, "install.sh")
    assert first.returncode == 0, first.stderr
    fingerprint = json.loads(first.stdout)["fingerprint"]
    assert len(fingerprint) == 64
    assert list((package[0] / "plugins/ui").iterdir()) == []
    _runtime_package(package[0])
    delivered = _run(package, "install.sh", "--no-pull")
    assert delivered.returncode == 0, delivered.stderr
    assert json.loads(delivered.stdout)["fingerprint"] == fingerprint
    assert len([args for args in _commands(package) if "pull" in args]) == 1


def test_installer_failed_pull_does_not_start_bootstrap_containers(
    package: tuple[Path, dict[str, str], Path],
) -> None:
    """A failed application image download aborts before permissions or startup."""
    package[1]["TEST_PULL_FAILURE"] = "1"
    result = _run(package, "install.sh")
    assert result.returncode != 0
    assert not any("run" in args or "up" in args or "exec" in args for args in _commands(package))


@pytest.mark.parametrize("invalid", ["legacy", "missing-js", "missing-css", "escape"])
def test_installer_rejects_invalid_ui_packages_before_startup(
    package: tuple[Path, dict[str, str], Path], invalid: str,
) -> None:
    """Legacy metadata, absent assets and escaping symlinks cannot silently disable UI plugins."""
    root = package[0]
    asset = _runtime_package(root)
    manifest = root / "plugins/ui/test.json"
    if invalid == "legacy":
        manifest.write_text('{"filename":"Test.tsx","plugin":"test"}')
    elif invalid == "missing-js":
        asset.unlink()
    elif invalid == "missing-css":
        metadata = json.loads(manifest.read_text())
        metadata["assets"].append("plugin.css")
        manifest.write_text(json.dumps(metadata))
    else:
        asset.unlink()
        outside = root / "outside.js"
        outside.write_text("private")
        asset.symlink_to(outside)
    result = _run(package, "install.sh")
    assert result.returncode != 0
    assert "Invalid runtime UI package" in result.stderr
    assert "complete compiled package under plugins/ui" in result.stderr
    assert not any("up" in args or "exec" in args for args in _commands(package))


def test_update_help_does_not_require_an_installation(
    package: tuple[Path, dict[str, str], Path],
) -> None:
    """Usage and argument errors are handled before any Docker or package access."""
    (package[0] / ".env").unlink()
    result = _run(package, "update.sh", "--help")
    assert result.returncode == 0
    assert "--no-pull" in result.stdout
    result = _run(package, "update.sh", "--unknown")
    assert result.returncode != 0
    assert "Unknown argument" in result.stderr
    assert _commands(package) == []
