import socket
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from gulp.api.server_api import GulpServer
from gulp.config import GulpConfig
from gulp.glug import Glug


def _closed_port() -> int:
    """returns a local port nothing is listening on"""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def listener():
    """a local listening socket, standing in for a reachable service"""
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    s.listen()
    yield s.getsockname()[1]
    s.close()


@pytest.fixture
def http_server():
    """starts a local HTTP server answering every GET with the given status, returns its port"""
    servers: list[HTTPServer] = []

    def start(status: int) -> int:
        class _Handler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                self.send_response(status)
                self.end_headers()

            def log_message(self, *args):
                pass

        srv = HTTPServer(("127.0.0.1", 0), _Handler)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        servers.append(srv)
        return srv.server_address[1]

    yield start
    for srv in servers:
        srv.shutdown()
        srv.server_close()


@pytest.fixture
def disconnecting_server():
    """a port that accepts TCP connections and drops them unanswered, like OpenSearch while booting"""
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    s.listen()
    stop = threading.Event()

    def serve():
        s.settimeout(0.1)
        while not stop.is_set():
            try:
                conn, _ = s.accept()
            except OSError:
                continue
            conn.recv(1024)
            conn.close()

    t = threading.Thread(target=serve, daemon=True)
    t.start()
    yield s.getsockname()[1]
    stop.set()
    t.join()
    s.close()


@pytest.fixture
def logged(monkeypatch):
    """records the (level, message, hint) of every error/critical Glug call"""
    calls: list[tuple[str, str, str | None]] = []
    glug = Glug.get_instance()
    monkeypatch.setattr(
        glug, "error", lambda msg, hint=None, **_: calls.append(("error", msg, hint))
    )
    monkeypatch.setattr(
        glug,
        "critical",
        lambda msg, hint=None, **_: calls.append(("critical", msg, hint)),
    )
    return calls


def _use_urls(monkeypatch, postgres: int, opensearch: int, redis: int) -> None:
    """points GulpConfig at local ports, ignoring any GULP_*_URL in the environment"""
    for env_var in ("GULP_POSTGRES_URL", "GULP_OPENSEARCH_URL", "GULP_REDIS_URL"):
        monkeypatch.delenv(env_var, raising=False)
    cfg = object.__new__(GulpConfig)
    cfg._config = {
        "postgres_url": f"postgresql+psycopg://postgres:secret@127.0.0.1:{postgres}/gulp",
        "opensearch_url": f"http://127.0.0.1:{opensearch}",
        "redis_url": f"redis://127.0.0.1:{redis}",
    }
    monkeypatch.setattr(GulpConfig, "get_instance", classmethod(lambda cls: cfg))


@pytest.mark.unit
@pytest.mark.parametrize("opensearch_status", [200, 401])
def test_required_services_all_reachable(
    monkeypatch, listener, http_server, logged, opensearch_status
):
    # a 401 still means OpenSearch is serving requests (the probe sends no credentials)
    _use_urls(monkeypatch, listener, http_server(opensearch_status), listener)

    assert GulpServer._check_required_services(timeout_sec=1.0) is True
    assert logged == []


@pytest.mark.unit
def test_required_services_opensearch_booting_drops_connection(
    monkeypatch, listener, disconnecting_server, logged
):
    _use_urls(monkeypatch, listener, disconnecting_server, listener)

    assert GulpServer._check_required_services(timeout_sec=1.0) is False
    errors = [c for c in logged if c[0] == "error"]
    assert len(errors) == 1
    _, msg, hint = errors[0]
    assert "OpenSearch" in msg and "not ready yet" in msg
    assert "still starting up" in hint
    assert "required services unreachable: OpenSearch" in logged[-1][1]


@pytest.mark.unit
def test_required_services_opensearch_503_is_not_ready(
    monkeypatch, listener, http_server, logged
):
    _use_urls(monkeypatch, listener, http_server(503), listener)

    assert GulpServer._check_required_services(timeout_sec=1.0) is False
    assert "HTTP 503" in [c for c in logged if c[0] == "error"][0][1]


@pytest.mark.unit
def test_required_services_missing_database_logs_hint(
    monkeypatch, listener, http_server, logged
):
    pg_port = _closed_port()
    _use_urls(monkeypatch, pg_port, http_server(200), listener)

    assert GulpServer._check_required_services(timeout_sec=1.0) is False

    errors = [c for c in logged if c[0] == "error"]
    assert len(errors) == 1
    _, msg, hint = errors[0]
    assert f"PostgreSQL is not reachable at 127.0.0.1:{pg_port}" in msg
    assert "postgres_url" in hint and "GULP_POSTGRES_URL" in hint
    # credentials embedded in the URL must never reach the log
    assert "secret" not in msg and "secret" not in hint

    assert logged[-1][0] == "critical"
    assert "required services unreachable: PostgreSQL" in logged[-1][1]
