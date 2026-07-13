"""Unit checks for sigma_zip_big_harness helpers."""

import sys
from types import SimpleNamespace

import pytest

from tests.integration import test_stress
from tests.integration.test_stress import (
    SigmaZipHarnessError,
    _count_operation_notes,
    _round_robin_instance_url,
    _clear_query_artifacts,
    _should_print_received_count,
    _wait_request_stats_progress,
)
from test_scripts import sigma_zip_big_harness
from test_scripts.sigma_zip_big_harness import (
    _assert_service_containers_alive,
    _expected_values,
    _gulp_concurrency_config_line,
    _instance_urls,
    _monitor_container_names,
    _parse_args,
    _service_container_snapshot,
    _split_urls,
)


class _FakeCountPlugins:
    def __init__(self, count: int) -> None:
        self.count = count
        self.calls: list[tuple[str, str]] = []

    async def object_count(self, obj_type: str, *, operation_id: str) -> dict[str, int]:
        self.calls.append((obj_type, operation_id))
        return {"count": self.count}


class _FakeCountClient:
    def __init__(self, count: int) -> None:
        self.plugins = _FakeCountPlugins(count)


class _FakePlugins:
    def __init__(self, stats: list[dict[str, object]]) -> None:
        self._stats = stats

    async def request_get(self, _req_id: str) -> dict[str, object]:
        return self._stats.pop(0)


class _FakeStatsClient:
    def __init__(self, stats: list[dict[str, object]]) -> None:
        self.plugins = _FakePlugins(stats)


class _FakeBulkDeletePlugins:
    def __init__(self, missing_obj_types: set[str] | None = None) -> None:
        self.calls: list[tuple[str, str, dict[str, object]]] = []
        self._missing_obj_types = missing_obj_types or set()

    async def object_delete_bulk(
        self,
        operation_id: str,
        obj_type: str,
        flt: dict[str, object],
    ) -> dict[str, int]:
        from gulp_sdk import NotFoundError

        self.calls.append((operation_id, obj_type, flt))
        if obj_type in self._missing_obj_types:
            raise NotFoundError(
                "not found", 404, {"__error": {"name": "ObjectNotFound"}}
            )
        return {"deleted": 0}


class _FakeBulkDeleteClient:
    def __init__(self, missing_obj_types: set[str] | None = None) -> None:
        self.plugins = _FakeBulkDeletePlugins(missing_obj_types)


@pytest.mark.unit
def test_received_count_debug_sampling() -> None:
    assert _should_print_received_count(1, 1149)
    assert _should_print_received_count(1000, 1149)
    assert _should_print_received_count(1149, 1149)
    assert not _should_print_received_count(999, 1149)


@pytest.mark.unit
def test_small_sigma_expected_values() -> None:
    assert _expected_values(big=False) == (98633, 14, 15)


@pytest.mark.unit
def test_gulp_concurrency_config_line_preserves_zero_query_ceiling(
    tmp_path, monkeypatch
) -> None:
    cfg_path = tmp_path / "gulp_cfg.json"
    cfg_path.write_text(
        """
        {
          "workers_max": 1,
          "workers_concurrency_max": 2,
          "opensearch_pool_maxsize": 4,
          "postgres_pool_size": 5,
          "postgres_max_overflow": 6
        }
        """,
        encoding="utf-8",
    )
    monkeypatch.setattr(sigma_zip_big_harness, "_gulp_config_path", lambda: cfg_path)

    assert "workers_concurrency_max=2" in _gulp_concurrency_config_line()
    assert "postgres_max_overflow=6" in _gulp_concurrency_config_line()


@pytest.mark.unit
def test_gulp_concurrency_config_line_preserves_zero_postgres_overflow(
    tmp_path, monkeypatch
) -> None:
    cfg_path = tmp_path / "gulp_cfg.json"
    cfg_path.write_text(
        """
        {
          "workers_max": 1,
          "workers_concurrency_max": 2,
          "postgres_max_overflow": 0
        }
        """,
        encoding="utf-8",
    )
    monkeypatch.setattr(sigma_zip_big_harness, "_gulp_config_path", lambda: cfg_path)

    line = _gulp_concurrency_config_line()

    assert "postgres_pool_size=2" in line
    assert "postgres_max_overflow=0" in line


@pytest.mark.unit
@pytest.mark.asyncio
async def test_query_only_reset_bulk_deletes_notes_and_request_stats() -> None:
    client = _FakeBulkDeleteClient()

    await _clear_query_artifacts(client, "op-a")

    assert client.plugins.calls == [
        ("op-a", "note", {"operation_ids": ["op-a"]}),
        ("op-a", "request_stats", {"operation_ids": ["op-a"]}),
    ]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_query_only_reset_ignores_missing_artifacts() -> None:
    client = _FakeBulkDeleteClient({"note", "request_stats"})

    await _clear_query_artifacts(client, "op-a")

    assert client.plugins.calls == [
        ("op-a", "note", {"operation_ids": ["op-a"]}),
        ("op-a", "request_stats", {"operation_ids": ["op-a"]}),
    ]


@pytest.mark.unit
def test_instance_urls_use_base_plus_additional_urls() -> None:
    urls = _instance_urls(
        "http://localhost:8080",
        _split_urls("http://localhost:8100, http://localhost:8101"),
        3,
    )

    assert urls == [
        "http://localhost:8080",
        "http://localhost:8100",
        "http://localhost:8101",
    ]


@pytest.mark.unit
def test_instance_urls_requires_declared_count() -> None:
    with pytest.raises(ValueError, match="additional URL"):
        _instance_urls("http://localhost:8080", ["http://localhost:8100"], 3)


@pytest.mark.unit
def test_monitor_container_names_can_be_disabled(monkeypatch) -> None:
    monkeypatch.setenv("GULP_SIGMA_ZIP_MONITOR_CONTAINERS", "")

    assert _monitor_container_names() == []


@pytest.mark.unit
def test_service_container_snapshot_parses_docker_inspect(monkeypatch) -> None:
    calls: list[list[str]] = []

    def _fake_run(cmd, **kwargs):
        calls.append(list(cmd))
        if cmd[:2] == ["docker", "version"]:
            return SimpleNamespace(returncode=0, stdout="24.0\n", stderr="")
        return SimpleNamespace(
            returncode=0,
            stdout="/gulp-os01-1 2026-07-06T10:00:00Z\n",
            stderr="",
        )

    monkeypatch.setattr(sigma_zip_big_harness.subprocess, "run", _fake_run)

    assert _service_container_snapshot(["gulp-os01-1"]) == {
        "gulp-os01-1": "2026-07-06T10:00:00Z"
    }
    assert calls[1][:3] == ["docker", "inspect", "--format"]


@pytest.mark.unit
def test_service_container_restart_detection(monkeypatch) -> None:
    monkeypatch.setattr(
        sigma_zip_big_harness,
        "_service_container_snapshot",
        lambda _names: {"gulp-os01-1": "new-start"},
    )

    with pytest.raises(RuntimeError, match="gulp-os01-1"):
        _assert_service_containers_alive({"gulp-os01-1": "old-start"})


@pytest.mark.unit
def test_parse_args_defaults_to_all_users_concurrent(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "sigma_zip_big_harness.py",
            "--users",
            "4",
            "--instances",
            "1",
            "--small-sigmas",
        ],
    )

    args = _parse_args()

    assert args.concurrency == 4


@pytest.mark.unit
def test_parse_args_supports_small_sigmas(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "sigma_zip_big_harness.py",
            "--users",
            "1",
            "--instances",
            "1",
            "--small-sigmas",
        ],
    )

    args = _parse_args()

    assert args.small_sigmas is True


@pytest.mark.unit
def test_parse_args_rejects_multi_user_big_sigma_without_override(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "sigma_zip_big_harness.py",
            "--users",
            "2",
            "--instances",
            "1",
        ],
    )

    with pytest.raises(SystemExit):
        _parse_args()


@pytest.mark.unit
def test_parse_args_allows_multi_user_big_sigma_with_override(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "sigma_zip_big_harness.py",
            "--users",
            "2",
            "--instances",
            "1",
            "--allow-heavy-query-run",
        ],
    )

    args = _parse_args()

    assert args.allow_heavy_query_run is True


@pytest.mark.unit
def test_parse_args_supports_query_only_operation_ids(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "sigma_zip_big_harness.py",
            "--query-only",
            "--operation-id",
            "op-a,op-b",
            "--small-sigmas",
        ],
    )

    args = _parse_args()

    assert args.query_only is True
    assert args.users == 2
    assert args.operation_ids == ["op-a", "op-b"]


@pytest.mark.unit
def test_parse_args_shared_query_only_reuses_one_operation_id(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "sigma_zip_big_harness.py",
            "--query-only",
            "--shared-operation",
            "--users",
            "3",
            "--operation-id",
            "op1",
            "--small-sigmas",
        ],
    )

    args = _parse_args()

    assert args.shared_operation is True
    assert args.operation_ids == ["op1", "op1", "op1"]


@pytest.mark.unit
def test_worker_credentials_default_to_user_id() -> None:
    assert sigma_zip_big_harness._worker_user_id(0) == "user1"
    assert sigma_zip_big_harness._worker_password("user1", None) == "Aa1!user1"
    assert sigma_zip_big_harness._worker_password("user1", "pw") == "pw"


@pytest.mark.unit
def test_parse_args_query_only_respects_equals_users(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "sigma_zip_big_harness.py",
            "--query-only",
            "--users=1",
            "--operation-id",
            "op-a,op-b",
            "--small-sigmas",
        ],
    )

    with pytest.raises(SystemExit):
        _parse_args()


@pytest.mark.unit
def test_ingest_requests_are_split_by_flattened_operation_file_index() -> None:
    urls = ["http://localhost:8080", "http://localhost:8100", "http://localhost:8101"]

    selected = [
        _round_robin_instance_url(urls, operation_index, file_index, 24)
        for operation_index in range(10)
        for file_index in range(24)
    ]

    assert selected[:6] == [
        "http://localhost:8080",
        "http://localhost:8100",
        "http://localhost:8101",
        "http://localhost:8080",
        "http://localhost:8100",
        "http://localhost:8101",
    ]
    assert {url: selected.count(url) for url in urls} == {
        "http://localhost:8080": 80,
        "http://localhost:8100": 80,
        "http://localhost:8101": 80,
    }


@pytest.mark.unit
def test_query_requests_are_split_by_operation_index() -> None:
    urls = ["http://localhost:8080", "http://localhost:8100", "http://localhost:8101"]

    selected = [
        _round_robin_instance_url(urls, operation_index, 0, 1)
        for operation_index in range(10)
    ]

    assert selected == [
        "http://localhost:8080",
        "http://localhost:8100",
        "http://localhost:8101",
        "http://localhost:8080",
        "http://localhost:8100",
        "http://localhost:8101",
        "http://localhost:8080",
        "http://localhost:8100",
        "http://localhost:8101",
        "http://localhost:8080",
    ]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_worker_defers_cleanup_to_harness_finally(monkeypatch) -> None:
    observed: dict[str, object] = {}

    async def _fake_run(*args, **kwargs):
        observed.update(kwargs)
        return {"operation_id": kwargs["operation_id"]}

    monkeypatch.setattr(
        sigma_zip_big_harness,
        "_run_query_sigma_zip_big_matches_and_notes",
        _fake_run,
    )

    result = await sigma_zip_big_harness._worker(
        "http://localhost:8080",
        "user-a",
        "pw",
        7,
        "op-a",
        ["http://localhost:8080", "http://localhost:8100"],
    )

    assert result["success"] is True
    assert observed["cleanup"] is False


@pytest.mark.unit
@pytest.mark.asyncio
async def test_worker_can_use_small_sigma_set(monkeypatch) -> None:
    observed: dict[str, object] = {}

    async def _fake_run(*args, **kwargs):
        observed.update(kwargs)
        return {"operation_id": kwargs["operation_id"], "status": "done"}

    monkeypatch.setattr(
        sigma_zip_big_harness,
        "_run_query_sigma_zip_big_matches_and_notes",
        _fake_run,
    )

    result = await sigma_zip_big_harness._worker(
        "http://localhost:8080",
        "user-a",
        "pw",
        7,
        "op-a",
        ["http://localhost:8080"],
        big=False,
    )

    assert result["success"] is True
    assert observed["big"] is False


@pytest.mark.unit
@pytest.mark.asyncio
async def test_worker_can_use_query_only(monkeypatch) -> None:
    observed: dict[str, object] = {}

    async def _fake_run(*args, **kwargs):
        observed.update(kwargs)
        return {"operation_id": kwargs["operation_id"], "status": "done"}

    monkeypatch.setattr(
        sigma_zip_big_harness,
        "_run_query_sigma_zip_big_matches_and_notes",
        _fake_run,
    )

    result = await sigma_zip_big_harness._worker(
        "http://localhost:8080",
        "admin",
        "admin",
        0,
        "op-a",
        ["http://localhost:8080"],
        query_only=True,
    )

    assert result["success"] is True
    assert observed["query_only"] is True


@pytest.mark.unit
@pytest.mark.asyncio
async def test_worker_partial_totals_can_succeed(monkeypatch) -> None:
    async def _fake_run(*args, **kwargs):
        return {
            "operation_id": kwargs["operation_id"],
            "status": "done",
            "records_ingested": 12,
            "completed_queries": 3,
            "total_hits": 7,
            "notes": 7,
        }

    monkeypatch.setattr(
        sigma_zip_big_harness,
        "_run_query_sigma_zip_big_matches_and_notes",
        _fake_run,
    )

    result = await sigma_zip_big_harness._worker(
        "http://localhost:8080",
        "user-a",
        "pw",
        7,
        "op-a",
        ["http://localhost:8080", "http://localhost:8100"],
    )

    assert result["success"] is True
    assert result["status"] == "done"
    assert result["records_ingested"] == 12
    assert result["completed_queries"] == 3
    assert result["total_hits"] == 7
    assert result["notes"] == 7
    assert "error" not in result


@pytest.mark.unit
@pytest.mark.asyncio
async def test_run_deletes_operations_after_final_status_output(
    monkeypatch, capsys
) -> None:
    class _FakeGulpClient:
        def __init__(
            self,
            base_url: str,
            token: str | None = None,
            ws_auto_connect: bool = True,
        ) -> None:
            self.base_url = base_url.rstrip("/")
            self.token = token or "token"

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args) -> None:
            return None

    async def _fake_run_flow(*args, **kwargs):
        return {
            "operation_id": kwargs["operation_id"],
            "status": "done",
            "completed_queries": 1,
        }

    events: list[str] = []

    async def _fake_delete_op(*args, **kwargs) -> None:
        events.append("delete_op")
        return None

    monkeypatch.setitem(
        sys.modules, "gulp_sdk", SimpleNamespace(GulpClient=_FakeGulpClient)
    )
    monkeypatch.setattr(
        sigma_zip_big_harness, "_login_ready", lambda *args, **kwargs: _noop()
    )
    monkeypatch.setattr(
        sigma_zip_big_harness,
        "_create_harness_worker_users",
        lambda *args, **kwargs: _return(args[1]),
    )
    monkeypatch.setattr(
        sigma_zip_big_harness,
        "_create_named_worker_operation",
        lambda *args: _return("op-a"),
    )
    monkeypatch.setattr(
        sigma_zip_big_harness,
        "_run_query_sigma_zip_big_matches_and_notes",
        _fake_run_flow,
    )
    monkeypatch.setattr(sigma_zip_big_harness, "_delete_op", _fake_delete_op)
    monkeypatch.setattr(
        sigma_zip_big_harness, "_cleanup_worker_users", lambda *args: _noop()
    )

    rc = await sigma_zip_big_harness._run(
        SimpleNamespace(
            base_url="http://localhost:8080",
            admin_user="admin",
            admin_password="admin",
            users=1,
            worker_password="pw",
            cleanup=True,
            instance_urls=["http://localhost:8080"],
        )
    )

    out = capsys.readouterr().out
    assert rc == 0
    assert "expected_total_hits=73464" in out
    assert "status=done" in out
    assert events == ["delete_op"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_run_always_deletes_operations_but_cleanup_controls_users(
    monkeypatch,
) -> None:
    class _FakeGulpClient:
        def __init__(
            self,
            base_url: str,
            token: str | None = None,
            ws_auto_connect: bool = True,
        ) -> None:
            self.base_url = base_url.rstrip("/")
            self.token = token or "token"

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args) -> None:
            return None

    deleted_operations: list[str] = []
    cleaned_users: list[str] = []

    async def _fake_run_flow(*args, **kwargs):
        return {"operation_id": kwargs["operation_id"], "status": "done"}

    async def _fake_delete_op(_client, operation_id: str) -> None:
        deleted_operations.append(operation_id)

    async def _fake_cleanup_users(_client, users: list[str]) -> None:
        cleaned_users.extend(users)

    monkeypatch.setitem(
        sys.modules, "gulp_sdk", SimpleNamespace(GulpClient=_FakeGulpClient)
    )
    monkeypatch.setattr(
        sigma_zip_big_harness, "_login_ready", lambda *args, **kwargs: _noop()
    )
    monkeypatch.setattr(
        sigma_zip_big_harness,
        "_create_harness_worker_users",
        lambda *args, **kwargs: _return(args[1]),
    )
    monkeypatch.setattr(
        sigma_zip_big_harness,
        "_create_named_worker_operation",
        lambda *args: _return("op-a"),
    )
    monkeypatch.setattr(
        sigma_zip_big_harness,
        "_run_query_sigma_zip_big_matches_and_notes",
        _fake_run_flow,
    )
    monkeypatch.setattr(sigma_zip_big_harness, "_delete_op", _fake_delete_op)
    monkeypatch.setattr(
        sigma_zip_big_harness, "_cleanup_worker_users", _fake_cleanup_users
    )

    rc = await sigma_zip_big_harness._run(
        SimpleNamespace(
            base_url="http://localhost:8080",
            admin_user="admin",
            admin_password="admin",
            users=1,
            worker_password="pw",
            cleanup=False,
            instance_urls=["http://localhost:8080"],
        )
    )

    assert rc == 0
    assert deleted_operations == ["op-a"]
    assert cleaned_users == []


@pytest.mark.unit
@pytest.mark.asyncio
async def test_run_keep_operations_keeps_worker_users(monkeypatch) -> None:
    class _FakeGulpClient:
        def __init__(
            self,
            base_url: str,
            token: str | None = None,
            ws_auto_connect: bool = True,
        ) -> None:
            self.base_url = base_url.rstrip("/")
            self.token = token or "token"

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args) -> None:
            return None

    deleted_operations: list[str] = []
    cleaned_users: list[str] = []

    async def _fake_run_flow(*args, **kwargs):
        return {"operation_id": kwargs["operation_id"], "status": "done"}

    async def _fake_delete_op(_client, operation_id: str) -> None:
        deleted_operations.append(operation_id)

    async def _fake_cleanup_users(_client, users: list[str]) -> None:
        cleaned_users.extend(users)

    monkeypatch.setitem(
        sys.modules, "gulp_sdk", SimpleNamespace(GulpClient=_FakeGulpClient)
    )
    monkeypatch.setattr(
        sigma_zip_big_harness, "_login_ready", lambda *args, **kwargs: _noop()
    )
    monkeypatch.setattr(
        sigma_zip_big_harness,
        "_create_harness_worker_users",
        lambda *args, **kwargs: _return(args[1]),
    )
    monkeypatch.setattr(
        sigma_zip_big_harness,
        "_create_named_worker_operation",
        lambda *args: _return("op-a"),
    )
    monkeypatch.setattr(
        sigma_zip_big_harness,
        "_run_query_sigma_zip_big_matches_and_notes",
        _fake_run_flow,
    )
    monkeypatch.setattr(sigma_zip_big_harness, "_delete_op", _fake_delete_op)
    monkeypatch.setattr(
        sigma_zip_big_harness, "_cleanup_worker_users", _fake_cleanup_users
    )

    rc = await sigma_zip_big_harness._run(
        SimpleNamespace(
            base_url="http://localhost:8080",
            admin_user="admin",
            admin_password="admin",
            users=1,
            worker_password="pw",
            cleanup=True,
            keep_operations=True,
            query_only=False,
            instance_urls=["http://localhost:8080"],
            concurrency=1,
            small_sigmas=True,
        )
    )

    assert rc == 0
    assert deleted_operations == []
    assert cleaned_users == []


@pytest.mark.unit
@pytest.mark.asyncio
async def test_run_query_only_reuses_operations_without_cleanup(monkeypatch) -> None:
    class _FakeOperations:
        async def get(self, operation_id: str) -> SimpleNamespace:
            return SimpleNamespace(id=operation_id, user_id="owner-a")

    class _FakeGulpClient:
        def __init__(
            self,
            base_url: str,
            token: str | None = None,
            ws_auto_connect: bool = True,
        ) -> None:
            self.base_url = base_url.rstrip("/")
            self.token = token or "token"
            self.operations = _FakeOperations()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args) -> None:
            return None

    observed: dict[str, object] = {}
    deleted_operations: list[str] = []

    async def _fake_run_flow(*args, **kwargs):
        observed["args"] = args
        observed.update(kwargs)
        return {"operation_id": kwargs["operation_id"], "status": "done"}

    async def _unexpected_create_users(*args, **kwargs):
        raise AssertionError("query-only must not create users")

    async def _fake_delete_op(_client, operation_id: str) -> None:
        deleted_operations.append(operation_id)

    monkeypatch.setitem(
        sys.modules, "gulp_sdk", SimpleNamespace(GulpClient=_FakeGulpClient)
    )
    monkeypatch.setattr(
        sigma_zip_big_harness, "_login_ready", lambda *args, **kwargs: _noop()
    )
    monkeypatch.setattr(
        sigma_zip_big_harness, "_create_harness_worker_users", _unexpected_create_users
    )
    monkeypatch.setattr(
        sigma_zip_big_harness,
        "_run_query_sigma_zip_big_matches_and_notes",
        _fake_run_flow,
    )
    monkeypatch.setattr(sigma_zip_big_harness, "_delete_op", _fake_delete_op)

    rc = await sigma_zip_big_harness._run(
        SimpleNamespace(
            base_url="http://localhost:8080",
            admin_user="admin",
            admin_password="admin",
            users=1,
            worker_password="pw",
            cleanup=True,
            keep_operations=False,
            query_only=True,
            operation_ids=["op-a"],
            instance_urls=["http://localhost:8080"],
            concurrency=1,
            small_sigmas=True,
        )
    )

    assert rc == 0
    assert observed["query_only"] is True
    assert observed["args"][1:3] == ("owner-a", "pw")
    assert deleted_operations == []


@pytest.mark.unit
@pytest.mark.asyncio
async def test_run_limits_worker_concurrency(monkeypatch) -> None:
    class _FakeGulpClient:
        def __init__(
            self,
            base_url: str,
            token: str | None = None,
            ws_auto_connect: bool = True,
        ) -> None:
            self.base_url = base_url.rstrip("/")
            self.token = token or "token"

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args) -> None:
            return None

    active = 0
    max_active = 0

    async def _fake_run_flow(*args, **kwargs):
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        await sigma_zip_big_harness.asyncio.sleep(0)
        active -= 1
        return {"operation_id": kwargs["operation_id"], "status": "done"}

    monkeypatch.setitem(
        sys.modules, "gulp_sdk", SimpleNamespace(GulpClient=_FakeGulpClient)
    )
    monkeypatch.setattr(
        sigma_zip_big_harness, "_login_ready", lambda *args, **kwargs: _noop()
    )
    monkeypatch.setattr(
        sigma_zip_big_harness,
        "_create_harness_worker_users",
        lambda *args, **kwargs: _return(args[1]),
    )
    monkeypatch.setattr(
        sigma_zip_big_harness,
        "_create_named_worker_operation",
        lambda *args: _return("op-a"),
    )
    monkeypatch.setattr(
        sigma_zip_big_harness,
        "_run_query_sigma_zip_big_matches_and_notes",
        _fake_run_flow,
    )
    monkeypatch.setattr(sigma_zip_big_harness, "_delete_op", lambda *args: _noop())
    monkeypatch.setattr(
        sigma_zip_big_harness, "_cleanup_worker_users", lambda *args: _noop()
    )

    rc = await sigma_zip_big_harness._run(
        SimpleNamespace(
            base_url="http://localhost:8080",
            admin_user="admin",
            admin_password="admin",
            users=3,
            worker_password="pw",
            cleanup=True,
            instance_urls=["http://localhost:8080", "http://localhost:8100"],
            concurrency=1,
        )
    )

    assert rc == 0
    assert max_active == 1


@pytest.mark.unit
@pytest.mark.asyncio
async def test_wait_request_stats_progress_uses_polled_stats(monkeypatch) -> None:
    async def _no_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(test_stress.asyncio, "sleep", _no_sleep)
    client = _FakeStatsClient(
        [
            {"status": "ongoing", "data": {"completed_queries": 1}},
            {"status": "done", "data": {"completed_queries": 2}},
        ]
    )

    stats = await _wait_request_stats_progress(
        client,
        "req-a",
        lambda item: int((item.get("data") or {}).get("completed_queries", 0)) >= 2,
        label="query",
    )

    assert stats["status"] == "done"


async def _noop() -> None:
    return None


async def _return(value: object) -> object:
    return value


@pytest.mark.unit
@pytest.mark.asyncio
async def test_count_operation_notes_uses_generic_object_count() -> None:
    client = _FakeCountClient(3)

    assert await _count_operation_notes(client, "op-a") == 3
    assert client.plugins.calls == [("note", "op-a")]
