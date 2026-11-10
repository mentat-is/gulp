"""Exercise browser history routes against the UI's production nginx config."""

from http.client import HTTPConnection
from pathlib import Path
import subprocess
import time
from uuid import uuid4


def _get(port: int, path: str) -> tuple[int, bytes]:
    """Request a UI path with the Host header used for an IP-based deployment."""
    connection = HTTPConnection("127.0.0.1", port, timeout=2)
    try:
        connection.request("GET", path, headers={"Host": "192.168.1.1"})
        response = connection.getresponse()
        return response.status, response.read()
    finally:
        connection.close()


def test_ui_nginx_serves_direct_routes_and_static_files(tmp_path: Path) -> None:
    """Deep links load the app, while missing compiled assets retain 404."""
    repository = Path(__file__).resolve().parents[2]
    ui_directory = repository / "gulp-ui"
    assert "COPY nginx.conf /etc/nginx/conf.d/default.conf" in (
        ui_directory / "Dockerfile"
    ).read_text()

    index = b"<!doctype html><div id='root'>UI routing test</div>"
    (tmp_path / "index.html").write_bytes(index)
    static_directory = tmp_path / "static" / "js"
    static_directory.mkdir(parents=True)
    (static_directory / "app.js").write_bytes(b"console.log('asset')")

    container_name = f"gulp-ui-routing-{uuid4().hex}"
    try:
        subprocess.run(
            [
                "docker",
                "run",
                "--detach",
                "--rm",
                "--name",
                container_name,
                "--publish",
                "127.0.0.1::80",
                "nginx:alpine",
            ],
            capture_output=True,
            text=True,
            check=True,
            timeout=30,
        )
        subprocess.run(
            [
                "docker",
                "cp",
                str(ui_directory / "nginx.conf"),
                f"{container_name}:/etc/nginx/conf.d/default.conf",
            ],
            capture_output=True,
            text=True,
            check=True,
            timeout=10,
        )
        subprocess.run(
            ["docker", "cp", f"{tmp_path}/.", f"{container_name}:/usr/share/nginx/html"],
            capture_output=True,
            text=True,
            check=True,
            timeout=10,
        )
        subprocess.run(
            ["docker", "exec", container_name, "nginx", "-t"],
            capture_output=True,
            text=True,
            check=True,
            timeout=10,
        )
        subprocess.run(
            ["docker", "exec", container_name, "nginx", "-s", "reload"],
            capture_output=True,
            text=True,
            check=True,
            timeout=10,
        )
        port_result = subprocess.run(
            ["docker", "port", container_name, "80/tcp"],
            capture_output=True,
            text=True,
            check=True,
            timeout=10,
        )
        port = int(port_result.stdout.strip().rsplit(":", 1)[1])

        deadline = time.monotonic() + 10
        while True:
            try:
                status, body = _get(port, "/operations/test-operation")
                if (status, body) == (200, index):
                    break
            except OSError:
                pass
            if time.monotonic() >= deadline:
                raise AssertionError("nginx did not load the SPA route config in time")
            time.sleep(0.1)

        assert _get(port, "/") == (200, index)
        assert _get(port, "/detached/table?view=1") == (200, index)
        assert _get(port, "/static/js/app.js") == (200, b"console.log('asset')")
        assert _get(port, "/static/js/missing.js")[0] == 404
    finally:
        subprocess.run(
            ["docker", "rm", "--force", container_name],
            capture_output=True,
            check=False,
            timeout=10,
        )
