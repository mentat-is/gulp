#!/usr/bin/env python3
"""Run the BIG_SIGMAS sigma-zip stress flow as N created users."""

import argparse
import asyncio
import contextlib
import multiprocessing
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import json5

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.integration.test_stress import (  # noqa: E402
    SigmaZipHarnessError,
    _cleanup_worker_users,
    _delete_op,
    _login_ready,
    _run_query_sigma_zip_big_matches_and_notes,
    _short_error,
    _wait_created,
)


_EXPECTED_RECORDS_INGESTED = 98633
_EXPECTED_COMPLETED_QUERIES = 1149
_EXPECTED_TOTAL_HITS = 73464
_SMALL_EXPECTED_COMPLETED_QUERIES = 14
_SMALL_EXPECTED_TOTAL_HITS = 15
_HEAVY_QUERY_USER_THRESHOLD = 1
_PROGRESS_INTERVAL_SEC = 30.0
_DEFAULT_MONITOR_CONTAINERS = ("gulp-os01-1", "gulp-postgres-1", "gulp-redis-1")


def _expected_values(big: bool) -> tuple[int, int, int]:
    """Return expected records, queries, and hits for the selected sigma set."""
    return (
        _EXPECTED_RECORDS_INGESTED,
        _EXPECTED_COMPLETED_QUERIES if big else _SMALL_EXPECTED_COMPLETED_QUERIES,
        _EXPECTED_TOTAL_HITS if big else _SMALL_EXPECTED_TOTAL_HITS,
    )


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def _split_urls(value: str | None) -> list[str]:
    if not value:
        return []
    return [url.strip() for url in value.split(",") if url.strip()]


def _split_values(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _utc_now() -> datetime:
    """Return the current UTC timestamp."""
    return datetime.now(timezone.utc)


def _format_dt(value: datetime) -> str:
    """Format a UTC timestamp for harness output."""
    return value.isoformat(timespec="seconds")


def _worker_user_id(worker_id: int) -> str:
    """Return the deterministic harness user id for a zero-based worker id."""
    return f"user{worker_id + 1}"


def _worker_password(user_id: str, override: str | None) -> str:
    """Return the password for a deterministic harness user."""
    return override or user_id


def _gulp_config_path() -> Path:
    """Return the gulp configuration file path used by local test runs."""
    working_dir = Path(os.getenv("GULP_WORKING_DIR", "~/.config/gulp")).expanduser()
    return working_dir / "gulp_cfg.json"


def _gulp_concurrency_config_line() -> str:
    """Return a compact local gulp concurrency/parallelism config summary."""
    cfg = json5.loads(_gulp_config_path().read_text(encoding="utf-8"))
    configured_processes = int(cfg.get("workers_max", 0) or 0)
    resolved_processes = configured_processes or multiprocessing.cpu_count()
    raw_query_concurrency_max = cfg.get(
        "query_concurrency_max", cfg.get("query_concurrency", 4)
    )
    query_concurrency_max = (
        int(cfg.get("query_concurrency", 4) or 4)
        if raw_query_concurrency_max is None
        else int(raw_query_concurrency_max)
    )
    workers_concurrency_max = int(cfg.get("workers_concurrency_max", 16) or 16)
    postgres_pool_size = int(
        cfg.get("postgres_pool_size",10) or 10 
    )
    postgres_max_overflow = int(cfg.get("postgres_max_overflow", 0))
    return (
        f"[sigma-zip-big] gulp_config workers_max={configured_processes} "
        f"workers_max_resolved={resolved_processes} "
        f"workers_concurrency_max={workers_concurrency_max} "
        f"query_concurrency_max={query_concurrency_max} "
        f"opensearch_pool_maxsize={int(cfg.get('opensearch_pool_maxsize', 10) or 10)} "
        f"postgres_pool_size={postgres_pool_size} "
        f"postgres_max_overflow={postgres_max_overflow}"
    )


def _instance_urls(base_url: str, extra_urls: list[str] | None, instances: int) -> list[str]:
    urls = [base_url, *(extra_urls or [])]
    if instances < 1:
        raise ValueError("--instances must be >= 1")
    if len(urls) != instances:
        raise ValueError(
            f"--instances={instances} requires {instances - 1} additional URL(s), got {len(urls) - 1}"
        )
    return urls


def _monitor_container_names() -> list[str]:
    """Return service containers the harness should watch for restarts."""
    value = os.getenv("GULP_SIGMA_ZIP_MONITOR_CONTAINERS")
    if value is None:
        return list(_DEFAULT_MONITOR_CONTAINERS)
    return _split_values(value)


def _docker_cmd() -> list[str] | None:
    """Return a docker command prefix that works in host or devcontainer runs."""
    for cmd in (["docker"], ["sudo", "docker"]):
        try:
            res = subprocess.run(
                [*cmd, "version", "--format", "{{.Server.Version}}"],
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
        except (FileNotFoundError, subprocess.TimeoutExpired):
            continue
        if res.returncode == 0:
            return cmd
    return None


def _service_container_snapshot(containers: list[str]) -> dict[str, str]:
    """Return container start timestamps, or an empty dict when unavailable."""
    if not containers:
        return {}
    cmd = _docker_cmd()
    if cmd is None:
        return {}
    try:
        res = subprocess.run(
            [*cmd, "inspect", "--format", "{{.Name}} {{.State.StartedAt}}", *containers],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {}
    if res.returncode != 0:
        return {}
    snapshot: dict[str, str] = {}
    for line in res.stdout.splitlines():
        name, _, started_at = line.partition(" ")
        if name and started_at:
            snapshot[name.lstrip("/")] = started_at
    return snapshot


def _assert_service_containers_alive(initial: dict[str, str]) -> None:
    """Fail the harness if a monitored service container restarted."""
    if not initial:
        return
    current = _service_container_snapshot(list(initial))
    restarted = [
        name
        for name, started_at in initial.items()
        if current.get(name) != started_at
    ]
    if restarted:
        raise RuntimeError(
            "monitored service container restarted: " + ", ".join(sorted(restarted))
        )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create N users and run test_stress.py::test_query_sigma_zip_big_matches_and_notes "
            "with BIG_SIGMAS semantics, one operation per user."
        )
    )
    parser.add_argument(
        "--base-url",
        default=os.getenv("GULP_BASE_URL", "http://localhost:8080"),
        help="Gulp server URL (env: GULP_BASE_URL)",
    )
    parser.add_argument(
        "--instances",
        type=int,
        default=int(os.getenv("GULP_SIGMA_ZIP_INSTANCES", "1")),
        help="Number of Gulp instances to spread workers across (env: GULP_SIGMA_ZIP_INSTANCES)",
    )
    parser.add_argument(
        "--instance-url",
        action="append",
        default=[],
        help=(
            "Additional Gulp instance URL. Repeat it or pass comma-separated URLs "
            "(env: GULP_SIGMA_ZIP_INSTANCE_URLS)."
        ),
    )
    parser.add_argument(
        "--admin-user",
        default=os.getenv("GULP_TEST_USER", "admin"),
        help="Admin user used to create/delete workers (env: GULP_TEST_USER)",
    )
    parser.add_argument(
        "--admin-password",
        default=os.getenv("GULP_TEST_PASSWORD", "admin"),
        help="Admin password (env: GULP_TEST_PASSWORD)",
    )
    parser.add_argument(
        "--users",
        type=int,
        default=int(os.getenv("GULP_SIGMA_ZIP_USERS", "10")),
        help="Number of created users/concurrent flows (env: GULP_SIGMA_ZIP_USERS, default: 10)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=int(os.getenv("GULP_SIGMA_ZIP_CONCURRENCY", "0")),
        help=(
            "Maximum worker flows to run at once. Default 0 means all users "
            "(env: GULP_SIGMA_ZIP_CONCURRENCY)."
        ),
    )
    parser.add_argument(
        "--worker-password",
        default=os.getenv("GULP_SIGMA_ZIP_USER_PASSWORD", ""),
        help=(
            "Password assigned to every created user. Default empty means each "
            "user's password matches its id, e.g. user1/user1 "
            "(env: GULP_SIGMA_ZIP_USER_PASSWORD)"
        ),
    )
    parser.add_argument(
        "--small-sigmas",
        action="store_true",
        default=_env_bool("GULP_SIGMA_ZIP_SMALL", False),
        help=(
            "Use the small sigma zip query set after full EVTX ingest "
            "(env: GULP_SIGMA_ZIP_SMALL)"
        ),
    )
    parser.add_argument(
        "--allow-heavy-query-run",
        action="store_true",
        default=_env_bool("GULP_SIGMA_ZIP_ALLOW_HEAVY", False),
        help=(
            "Allow multi-user runs with the full 1149-query sigma set "
            "(env: GULP_SIGMA_ZIP_ALLOW_HEAVY)"
        ),
    )
    parser.add_argument(
        "--query-only",
        action="store_true",
        default=_env_bool("GULP_SIGMA_ZIP_QUERY_ONLY", False),
        help=(
            "Reuse existing operations: delete notes/request_stats, then run only "
            "the sigma query phase as each operation owner; --worker-password "
            "must match those users (env: GULP_SIGMA_ZIP_QUERY_ONLY)"
        ),
    )
    parser.add_argument(
        "--shared-operation",
        action="store_true",
        default=_env_bool("GULP_SIGMA_ZIP_SHARED_OPERATION", False),
        help=(
            "Use one operation for all users. For --query-only, one "
            "--operation-id is enough and is reused by every user "
            "(env: GULP_SIGMA_ZIP_SHARED_OPERATION)"
        ),
    )
    parser.add_argument(
        "--progress-interval",
        type=float,
        default=float(os.getenv("GULP_SIGMA_ZIP_PROGRESS_INTERVAL", str(_PROGRESS_INTERVAL_SEC))),
        help=(
            "Seconds between live aggregate count lines while workers run "
            "(env: GULP_SIGMA_ZIP_PROGRESS_INTERVAL, default: 30)"
        ),
    )
    parser.add_argument(
        "--operation-id",
        action="append",
        default=[],
        help=(
            "Existing operation ID for --query-only. Repeat it or pass comma-separated "
            "IDs (env: GULP_SIGMA_ZIP_OPERATION_IDS)."
        ),
    )
    parser.add_argument(
        "--cleanup",
        action=argparse.BooleanOptionalAction,
        default=_env_bool("GULP_SIGMA_ZIP_CLEANUP", True),
        help="Delete created users at the end; operations are always deleted at the end (env: GULP_SIGMA_ZIP_CLEANUP, default: true)",
    )
    parser.add_argument(
        "--keep-operations",
        action="store_true",
        default=_env_bool("GULP_SIGMA_ZIP_KEEP_OPERATIONS", False),
        help="Keep created operations for later --query-only runs (env: GULP_SIGMA_ZIP_KEEP_OPERATIONS)",
    )
    args = parser.parse_args()
    if args.users < 1:
        parser.error("--users must be >= 1")
    env_urls = _split_urls(os.getenv("GULP_SIGMA_ZIP_INSTANCE_URLS"))
    cli_urls = [url for value in args.instance_url for url in _split_urls(value)]
    env_ops = _split_values(os.getenv("GULP_SIGMA_ZIP_OPERATION_IDS"))
    cli_ops = [op for value in args.operation_id for op in _split_values(value)]
    args.operation_ids = cli_ops or env_ops
    if args.query_only:
        if not args.operation_ids:
            parser.error("--query-only requires at least one --operation-id")
        users_was_default = (
            os.getenv("GULP_SIGMA_ZIP_USERS") is None
            and not any(arg == "--users" or arg.startswith("--users=") for arg in sys.argv)
        )
        if args.shared_operation and len(args.operation_ids) == 1:
            args.operation_ids = args.operation_ids * args.users
        elif users_was_default:
            args.users = len(args.operation_ids)
        elif len(args.operation_ids) != args.users:
            parser.error("--query-only requires one --operation-id per user")
    try:
        args.instance_urls = _instance_urls(args.base_url, cli_urls or env_urls, args.instances)
    except ValueError as exc:
        parser.error(str(exc))
    if args.concurrency == 0:
        args.concurrency = args.users
    if args.concurrency < 1:
        parser.error("--concurrency must be >= 1, or 0 for all users")
    if args.progress_interval < 1:
        parser.error("--progress-interval must be >= 1")
    if (
        not args.small_sigmas
        and args.users > _HEAVY_QUERY_USER_THRESHOLD
        and not args.allow_heavy_query_run
    ):
        parser.error(
            "full sigma runs submit 1149 queries per user; use --small-sigmas for "
            "staged validation or pass --allow-heavy-query-run when the target "
            "environment is ready"
        )
    return args


async def _operation_counts(client: Any, operation_ids: list[str]) -> dict[str, int]:
    """Poll backend request stats and note counts for the given operations."""
    totals = {
        "records_ingested": 0,
        "completed_queries": 0,
        "total_hits": 0,
        "notes": 0,
        "poll_errors": 0,
    }
    for operation_id in dict.fromkeys(operation_ids):
        try:
            stats_list = await client.plugins.request_list(operation_id)
        except Exception:
            totals["poll_errors"] += 1
            stats_list = []
        for stats in stats_list:
            data = stats.get("data") or {}
            totals["records_ingested"] += int(data.get("records_ingested", 0) or 0)
            totals["completed_queries"] += int(data.get("completed_queries", 0) or 0)
            totals["total_hits"] += int(data.get("total_hits", 0) or 0)
        try:
            counted = await client.plugins.object_count("note", operation_id=operation_id)
            totals["notes"] += int(counted.get("count", 0) or 0)
        except Exception:
            totals["poll_errors"] += 1
    return totals


async def _wait_operation_searchable(
    client: Any,
    operation_id: str,
    expected_docs: int,
    *,
    timeout_sec: float = 120.0,
) -> int:
    """Wait until OpenSearch exposes the expected shared-operation document count."""
    deadline = time.monotonic() + timeout_sec
    last_observed = -1
    while True:
        qr = await client.queries.query_raw(
            operation_id=operation_id,
            q=[{"query": {"match_all": {}}}],
            q_options={
                "preview_mode": True,
                "preview_mode_max_records": 1,
                "name": "sigma_zip_visibility",
            },
        )
        observed = int((qr.get("data") or {}).get("total_hits", 0) or 0)
        if observed != last_observed:
            last_observed = observed
            print(
                f"[sigma-zip-big] visibility operation={operation_id} "
                f"docs={observed} expected={expected_docs}",
                flush=True,
            )
        if observed >= expected_docs:
            return observed
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"operation {operation_id} searchable docs={observed}, expected={expected_docs}"
            )
        await asyncio.sleep(1.0)


async def _reset_operation_query_artifacts(client: Any, operation_id: str) -> None:
    """Delete query-created artifacts once before a shared query-only run."""
    for obj_type in ("note", "request_stats"):
        try:
            await client.plugins.object_delete_bulk(
                operation_id,
                obj_type,
                {"operation_ids": [operation_id]},
            )
        except Exception:
            pass


async def _live_progress_reporter(
    base_url: str,
    token: str,
    operation_ids: list[str],
    done: asyncio.Event,
    *,
    started: float,
    interval_sec: float,
    service_snapshot: dict[str, str],
) -> None:
    """Print periodic aggregate backend counts until the run finishes."""
    from gulp_sdk import GulpClient

    async with GulpClient(base_url, token=token, ws_auto_connect=False) as client:
        while True:
            try:
                await asyncio.wait_for(done.wait(), timeout=interval_sec)
                return
            except TimeoutError:
                pass

            _assert_service_containers_alive(service_snapshot)
            totals = await _operation_counts(client, operation_ids)
            unique_operations = len(dict.fromkeys(operation_ids))
            expected_polls = unique_operations * 2
            if totals["poll_errors"] >= expected_polls:
                print(
                    f"[sigma-zip-big] live elapsed={round(time.monotonic() - started, 2)}s "
                    f"operations={len(operation_ids)} unavailable=true",
                    flush=True,
                )
                continue
            print(
                f"[sigma-zip-big] live elapsed={round(time.monotonic() - started, 2)}s "
                f"operations={len(operation_ids)} "
                f"records_ingested={totals['records_ingested']} "
                f"completed_queries={totals['completed_queries']} "
                f"total_hits={totals['total_hits']} "
                f"notes={totals['notes']} "
                f"poll_errors={totals['poll_errors']}",
                flush=True,
            )


async def _worker(
    base_url: str,
    user: str,
    password: str,
    worker_id: int,
    operation_id: str,
    instance_urls: list[str],
    big: bool = True,
    query_only: bool = False,
    context_name: str | None = None,
    expected_multiplier: int = 1,
    ingest_only: bool = False,
    reset_query_state: bool = True,
    verify_notes: bool = True,
) -> dict[str, Any]:
    result: dict[str, Any] = {"worker_id": worker_id, "user_id": user, "success": False}
    started = time.monotonic()
    try:
        result.update(
            await _run_query_sigma_zip_big_matches_and_notes(
                base_url,
                user,
                password,
                big=big,
                cleanup=False,
                operation_id=operation_id,
                instance_urls=instance_urls,
                operation_index=worker_id,
                query_only=query_only,
                context_name=context_name,
                expected_multiplier=expected_multiplier,
                ingest_only=ingest_only,
                reset_query_state=reset_query_state,
                verify_notes=verify_notes,
            )
        )
        result["status"] = result.get("status", "done")
        result["success"] = result["status"] == "done" or (
            ingest_only
            and int(result.get("records_ingested", 0) or 0)
            == int(result.get("expected_records_ingested", 0) or 0)
        )
    except SigmaZipHarnessError as exc:
        result.update(exc.totals)
        result["success"] = False
        result["status"] = result.get("status", "failed")
        result["error"] = _short_error(exc)
    except Exception as exc:
        result["status"] = "error"
        result["error"] = _short_error(exc)
    result["total_secs"] = round(time.monotonic() - started, 2)
    return result


async def _create_named_worker_operation(
    base_url: str,
    user: str,
    password: str,
    name: str,
    granted_user_ids: list[str] | None = None,
) -> str:
    """Create a deterministic operation name as a worker user."""
    from gulp_sdk import GulpClient

    async with GulpClient(base_url) as client:
        await _login_ready(client, user, password)
        response = await client._request(
            "POST",
            "/operation_create",
            params={"name": name, "set_default_grants": True},
        )
        op_data = response.get("data", {})
        op_id = str(op_data["id"])
        for granted_user_id in dict.fromkeys(granted_user_ids or []):
            await client.acl.add_granted_user(op_id, "operation", granted_user_id)
        await _wait_created(client.operations.get, op_id, label="operation")
        return op_id


async def _create_harness_worker_users(
    admin_client: Any,
    user_ids: list[str],
    passwords: list[str],
    permission: list[str],
) -> list[str]:
    """Recreate deterministic harness users."""
    await _cleanup_worker_users(admin_client, user_ids)
    created: list[str] = []
    try:
        for user_id, password in zip(user_ids, passwords):
            await admin_client.users.create(
                user_id=user_id,
                password=password,
                permission=permission,
            )
            created.append(user_id)
            await _wait_created(admin_client.users.get, user_id, label="user")
    except Exception:
        await _cleanup_worker_users(admin_client, created)
        raise
    return user_ids


async def _run(args: argparse.Namespace) -> int:
    from gulp_sdk import GulpClient

    start_time = _utc_now()
    started = time.monotonic()
    workload_started = started
    worker_users: list[str] = []
    operation_ids: list[str] = []
    worker_urls = [args.instance_urls[i % len(args.instance_urls)] for i in range(args.users)]
    worker_concurrency = getattr(args, "concurrency", args.users)
    big = not getattr(args, "small_sigmas", False)
    query_only = getattr(args, "query_only", False)
    keep_operations = getattr(args, "keep_operations", False)
    shared_operation = getattr(args, "shared_operation", False)
    expected_records, expected_queries, expected_hits = _expected_values(big)
    run_suffix = f"{int(started * 1000) & 0xFFFF:04x}"
    print(
        f"[sigma-zip-big] start users={args.users} instances={len(args.instance_urls)} "
        f"concurrency={worker_concurrency} query_only={query_only} "
        f"started={_format_dt(start_time)}",
        flush=True,
    )
    print(_gulp_concurrency_config_line(), flush=True)
    async with GulpClient(args.base_url) as admin_client:
        await _login_ready(admin_client, args.admin_user, args.admin_password)
        configured_worker_password = args.worker_password or None
        if query_only:
            operation_ids = list(args.operation_ids)
            for i, operation_id in enumerate(operation_ids):
                op = await admin_client.operations.get(operation_id)
                owner = getattr(op, "user_id", None) or getattr(op, "owner_id", None)
                worker_users.append(_worker_user_id(i) if shared_operation else str(owner))
        else:
            worker_users = [f"{_worker_user_id(i)}{run_suffix}" for i in range(args.users)]
            await _create_harness_worker_users(
                admin_client,
                worker_users,
                [_worker_password(user, configured_worker_password) for user in worker_users],
                permission=["read", "edit", "ingest", "delete"],
            )
        try:
            if not query_only:
                if shared_operation:
                    op_id = await _create_named_worker_operation(
                        worker_urls[0],
                        worker_users[0],
                        _worker_password(worker_users[0], configured_worker_password),
                        f"op1_{run_suffix}",
                        ["guest", "admin", *worker_users],
                    )
                    operation_ids = [op_id] * args.users
                else:
                    operation_ids = await asyncio.gather(
                        *[
                            _create_named_worker_operation(
                                worker_urls[i],
                                user,
                                _worker_password(user, configured_worker_password),
                                f"op{i + 1}_{run_suffix}",
                                ["guest", "admin", user],
                            )
                            for i, user in enumerate(worker_users)
                        ]
                    )
            print(
                "[sigma-zip-big] targets "
                + " ".join(
                    f"worker={i} user={worker_users[i]} "
                    f"password={_worker_password(worker_users[i], configured_worker_password)} "
                    f"operation={operation_ids[i]} url={worker_urls[i]}"
                    for i in range(args.users)
                ),
                flush=True,
            )
            workload_start_time = _utc_now()
            workload_started = time.monotonic()
            semaphore = asyncio.Semaphore(worker_concurrency)

            async def _run_worker(
                i: int,
                *,
                phase_query_only: bool = query_only,
                ingest_only: bool = False,
                expected_multiplier: int = 1,
                reset_query_state: bool = True,
                verify_notes: bool = True,
            ) -> dict[str, Any]:
                async with semaphore:
                    return await _worker(
                        worker_urls[i],
                        worker_users[i],
                        _worker_password(worker_users[i], configured_worker_password),
                        i,
                        operation_ids[i],
                        args.instance_urls,
                        big=big,
                        query_only=phase_query_only,
                        context_name=f"{worker_users[i]}_sdk_sigma_zip_context",
                        expected_multiplier=expected_multiplier,
                        ingest_only=ingest_only,
                        reset_query_state=reset_query_state,
                        verify_notes=verify_notes,
                    )

            async def _run_task_set(tasks: list[asyncio.Task[dict[str, Any]]], label: str) -> list[dict[str, Any]]:
                task_results: list[dict[str, Any]] = []
                pending: set[asyncio.Task[dict[str, Any]]] = set(tasks)
                try:
                    while pending:
                        wait_for: set[asyncio.Task[Any]] = set(pending)
                        if progress_task is not None:
                            wait_for.add(progress_task)
                        done_tasks, _ = await asyncio.wait(
                            wait_for, return_when=asyncio.FIRST_COMPLETED
                        )
                        if progress_task in done_tasks:
                            progress_task.result()
                        for completed_task in done_tasks.intersection(pending):
                            pending.remove(completed_task)
                            result = await completed_task
                            task_results.append(result)
                            passed_count = sum(1 for r in task_results if r.get("success"))
                            failed_count = len(task_results) - passed_count
                            total_records_ingested = sum(
                                int(r.get("records_ingested", 0) or 0) for r in task_results
                            )
                            total_completed_queries = sum(
                                int(r.get("completed_queries", 0) or 0) for r in task_results
                            )
                            total_hits = sum(
                                int(r.get("total_hits", 0) or 0) for r in task_results
                            )
                            print(
                                f"[sigma-zip-big] progress phase={label} "
                                f"completed={len(task_results)}/{args.users} "
                                f"passed={passed_count} failed={failed_count} "
                                f"records_ingested={total_records_ingested} "
                                f"completed_queries={total_completed_queries} "
                                f"total_hits={total_hits}",
                                flush=True,
                            )
                except Exception:
                    for task in pending:
                        task.cancel()
                    await asyncio.gather(*pending, return_exceptions=True)
                    raise
                return task_results

            results: list[dict[str, Any]]
            progress_done = asyncio.Event()
            service_snapshot = _service_container_snapshot(_monitor_container_names())
            if service_snapshot:
                print(
                    "[sigma-zip-big] monitor_containers "
                    + ",".join(sorted(service_snapshot)),
                    flush=True,
                )
            progress_task: asyncio.Task[None] | None = None
            progress_task = asyncio.create_task(
                _live_progress_reporter(
                    args.base_url,
                    admin_client.token,
                    list(dict.fromkeys(operation_ids)),
                    progress_done,
                    started=workload_started,
                    interval_sec=float(getattr(args, "progress_interval", _PROGRESS_INTERVAL_SEC)),
                    service_snapshot=service_snapshot,
                )
            )
            try:
                if shared_operation and not query_only:
                    ingest_results = await _run_task_set(
                        [
                            asyncio.create_task(
                                _run_worker(i, phase_query_only=False, ingest_only=True)
                            )
                            for i in range(args.users)
                        ],
                        "ingest",
                    )
                    if any(not r.get("success") for r in ingest_results):
                        results = ingest_results
                    else:
                        await _wait_operation_searchable(
                            admin_client,
                            operation_ids[0],
                            expected_records * args.users,
                        )
                        query_results = await _run_task_set(
                            [
                                asyncio.create_task(
                                    _run_worker(
                                        i,
                                        phase_query_only=True,
                                        expected_multiplier=args.users,
                                        reset_query_state=False,
                                        verify_notes=False,
                                    )
                                )
                                for i in range(args.users)
                            ],
                            "query",
                        )
                        records_by_worker = {
                            int(r["worker_id"]): int(r.get("records_ingested", 0) or 0)
                            for r in ingest_results
                        }
                        for result in query_results:
                            result["records_ingested"] = records_by_worker.get(
                                int(result["worker_id"]), 0
                            )
                            result["expected_records_ingested"] = expected_records
                        results = query_results
                else:
                    if shared_operation and query_only:
                        await _reset_operation_query_artifacts(
                            admin_client, operation_ids[0]
                        )
                    results = await _run_task_set(
                        [
                            asyncio.create_task(
                                _run_worker(
                                    i,
                                    expected_multiplier=args.users if shared_operation else 1,
                                    reset_query_state=not shared_operation,
                                    verify_notes=not shared_operation,
                                )
                            )
                            for i in range(args.users)
                        ],
                        "query" if query_only else "full",
                    )
            finally:
                progress_done.set()
                try:
                    await asyncio.wait_for(progress_task, timeout=5)
                except TimeoutError:
                    progress_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await progress_task
            passed = [r for r in results if r.get("success")]
            failed = [r for r in results if not r.get("success")]
            total_records_ingested = sum(int(r.get("records_ingested", 0) or 0) for r in results)
            total_completed_queries = sum(int(r.get("completed_queries", 0) or 0) for r in results)
            total_hits = sum(int(r.get("total_hits", 0) or 0) for r in results)
            expected_total_hits = expected_hits * args.users * (args.users if shared_operation else 1)
            expected_total_notes = expected_total_hits
            observed_total_notes = 0
            shared_notes_ok = True
            if shared_operation:
                while not failed:
                    observed_counts = await _operation_counts(
                        admin_client, list(dict.fromkeys(operation_ids))
                    )
                    observed_total_notes = observed_counts["notes"]
                    if observed_total_notes >= expected_total_notes:
                        break
                    await asyncio.sleep(1.0)
                if failed:
                    observed_counts = await _operation_counts(
                        admin_client, list(dict.fromkeys(operation_ids))
                    )
                    observed_total_notes = observed_counts["notes"]
                shared_notes_ok = observed_total_notes == expected_total_notes
            end_time = _utc_now()
            print(
                f"[sigma-zip-big] done passed={len(passed)} failed={len(failed)} "
                f"users={args.users} instances={len(args.instance_urls)} "
                f"shared_operation={shared_operation} cleanup={args.cleanup} "
                f"started={_format_dt(start_time)} ended={_format_dt(end_time)} "
                f"workload_started={_format_dt(workload_start_time)} "
                f"setup_elapsed={round(workload_started - started, 2)}s "
                f"workload_elapsed={round(time.monotonic() - workload_started, 2)}s "
                f"elapsed={round(time.monotonic() - started, 2)}s "
                f"records_ingested={total_records_ingested} "
                f"expected_records_ingested={expected_records * args.users} "
                f"completed_queries={total_completed_queries} "
                f"expected_completed_queries={expected_queries * args.users} "
                f"total_hits={total_hits} "
                f"expected_total_hits={expected_total_hits} "
                f"notes={observed_total_notes if shared_operation else total_hits} "
                f"expected_notes={expected_total_notes}"
            )
            for result in sorted(results, key=lambda item: int(item["worker_id"])):
                status = "PASS" if result.get("success") else "FAIL"
                print(
                    f"  {status} worker={result['worker_id']} user={result['user_id']} "
                    f"status={result.get('status', '?')} "
                    f"url={worker_urls[result['worker_id']]} "
                    f"query_url={result.get('query_url', '?')} "
                    f"operation={result.get('operation_id', '?')} total={result.get('total_secs')}s "
                    f"records_ingested={result.get('records_ingested', '?')} "
                    f"expected_records_ingested={result.get('expected_records_ingested', expected_records)} "
                    f"completed_queries={result.get('completed_queries', '?')} "
                    f"expected_completed_queries={result.get('expected_completed_queries', expected_queries)} "
                    f"total_hits={result.get('total_hits', '?')} "
                    f"expected_total_hits={result.get('expected_total_hits', expected_hits)} "
                    f"notes={result.get('notes', '?')} "
                    f"ws_stats={result.get('ws_stats_update_count', '?')} "
                    f"ws_query_done={result.get('ws_query_done_count', '?')} "
                    f"ws_collab_create={result.get('ws_collab_create_count', '?')} "
                    f"error={result.get('error', '')}"
                )
            return 1 if failed or not shared_notes_ok else 0
        finally:
            if not query_only and not keep_operations:
                for operation_id in dict.fromkeys(operation_ids):
                    try:
                        await _delete_op(admin_client, operation_id)
                    except Exception as exc:
                        print(
                            f"[sigma-zip-big] cleanup operation failed "
                            f"operation={operation_id} error={_short_error(exc)}",
                            flush=True,
                        )
                        pass
            if args.cleanup and not query_only and not keep_operations:
                await _cleanup_worker_users(admin_client, worker_users)


def main() -> int:
    return asyncio.run(_run(_parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
