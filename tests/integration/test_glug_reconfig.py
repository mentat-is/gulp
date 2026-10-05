"""
Integration coverage for gulp.glug.reconfig: the Redis-backed mechanism that
broadcasts level/dev_mode/log_dir changes live to every gulp process, and that
lets a process started after a change still pick up the current state.

Real subprocesses are used (not just multiple Glug instances in-process)
because Glug is a per-process singleton by design (mirrors how main/worker
gulp processes each own exactly one Glug instance) — the only faithful way
to exercise "two processes react live" and "a process born later still
converges" is to actually run separate Python processes against real Redis.
"""

import asyncio
import json
import logging
import multiprocessing
import os
import socket
import time
from pathlib import Path
from uuid import uuid4

import pytest

from gulp.api.redis_api import GulpRedis


def _channel_and_state_key(suffix: str) -> tuple[str, str]:
    return f"gulp:test:glug:reconfig:{suffix}", f"gulp:test:glug:state:{suffix}"


async def _redis_available(server_id: str) -> bool:
    try:
        GulpRedis.get_instance().initialize(server_id=server_id, main_process=False)
        await GulpRedis.get_instance().client().ping()
        return True
    except Exception:
        return False


def _live_reconfig_worker(
    log_dir: str, server_id: str, channel: str, state_key: str, ready_flag: str, result_path: str
) -> None:
    """
    runs in its own process: creates a Glug instance, syncs to any persisted
    state, starts the reconfig subscriber, signals readiness, then waits
    (polling its own in-process Glug) for dev_mode to flip to True.
    """

    async def run() -> None:
        from gulp.glug import reconfig
        from gulp.glug.core import Glug

        reconfig.RECONFIG_CHANNEL = channel
        reconfig.STATE_KEY = state_key

        GulpRedis.get_instance().initialize(server_id=server_id, main_process=False)
        client = GulpRedis.get_instance().client()

        Glug.get_instance(
            name=f"glug-reconfig-worker-{uuid4().hex}",
            log_dir=log_dir,
            level=logging.INFO,
            dev_mode=False,
            process_role="worker",
        )
        await reconfig.sync_from_persisted_state()
        await reconfig.start_subscriber()

        await client.incr(ready_flag)

        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and not Glug.get_instance().is_dev_mode():
            await asyncio.sleep(0.05)

        Path(result_path).write_text(
            json.dumps({"dev_mode": Glug.get_instance().is_dev_mode()})
        )
        await reconfig.stop_subscriber()
        await GulpRedis.get_instance().shutdown()

    asyncio.run(run())


def _late_joiner_worker(
    log_dir: str, server_id: str, channel: str, state_key: str, result_path: str
) -> None:
    """
    runs in its own process, started only AFTER publish_reconfig() already
    ran: it must converge to the persisted state via sync_from_persisted_state()
    alone, without ever seeing the original pub/sub message.
    """

    async def run() -> None:
        from gulp.glug import reconfig
        from gulp.glug.core import Glug

        reconfig.RECONFIG_CHANNEL = channel
        reconfig.STATE_KEY = state_key

        GulpRedis.get_instance().initialize(server_id=server_id, main_process=False)

        Glug.get_instance(
            name=f"glug-reconfig-late-{uuid4().hex}",
            log_dir=log_dir,
            level=logging.INFO,
            dev_mode=False,
            process_role="worker",
            # GulpRedis.initialize() above already logged through a bare instance
            reconfigure=True,
        )
        await reconfig.sync_from_persisted_state()

        glug = Glug.get_instance()
        glug.info("late joiner is logging")
        Path(result_path).write_text(
            json.dumps(
                {
                    "dev_mode": glug.is_dev_mode(),
                    "level": glug._level,
                    "log_dir": glug._log_dir,
                }
            )
        )
        await GulpRedis.get_instance().shutdown()

    asyncio.run(run())


def _live_log_dir_worker(
    server_id: str, channel: str, state_key: str, ready_flag: str, result_path: str
) -> None:
    """
    runs in its own process, started with stdout only: waits for a published
    log_dir to open its JSON files, logs one line into them, then waits for
    log_dir=None to close them again.
    """

    async def run() -> None:
        from gulp.glug import reconfig
        from gulp.glug.core import Glug

        reconfig.RECONFIG_CHANNEL = channel
        reconfig.STATE_KEY = state_key

        GulpRedis.get_instance().initialize(server_id=server_id, main_process=False)
        client = GulpRedis.get_instance().client()

        Glug.get_instance(
            name=f"glug-reconfig-logdir-{uuid4().hex}",
            level=logging.INFO,
            process_role="worker",
            # GulpRedis.initialize() above already logged through a bare instance
            reconfigure=True,
        )
        await reconfig.sync_from_persisted_state()
        await reconfig.start_subscriber()
        await client.incr(ready_flag)

        async def wait_for(predicate) -> bool:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                if predicate():
                    return True
                await asyncio.sleep(0.05)
            return False

        glug = Glug.get_instance()
        opened = await wait_for(lambda: glug._log_dir is not None)
        glug.info(f"json line from pid {os.getpid()}")
        await client.incr(ready_flag)
        closed = await wait_for(lambda: glug._log_dir is None)
        glug.info("after the json files were closed")

        Path(result_path).write_text(
            json.dumps({"opened": opened, "closed": closed, "pid": os.getpid()})
        )
        await reconfig.stop_subscriber()
        await GulpRedis.get_instance().shutdown()

    asyncio.run(run())


async def _wait_counter(key: str, target: int, timeout: float = 15) -> None:
    client = GulpRedis.get_instance().client()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        count = await client.get(key)
        if count and int(count) >= target:
            return
        await asyncio.sleep(0.1)
    pytest.fail(f"{key} never reached {target}")


@pytest.mark.integration
async def test_publish_reconfig_reaches_running_processes_live(tmp_path: Path):
    suffix = uuid4().hex
    server_id = f"test-glug-live-{suffix}"
    channel, state_key = _channel_and_state_key(suffix)
    ready_flag = f"gulp:test:glug:ready:{suffix}"

    if not await _redis_available(server_id):
        pytest.skip("Redis is not available through current gULP config")

    from gulp.glug import reconfig

    reconfig.RECONFIG_CHANNEL = channel
    reconfig.STATE_KEY = state_key

    ctx = multiprocessing.get_context("spawn")
    result_paths = [tmp_path / f"result_{i}.json" for i in range(2)]
    procs = [
        ctx.Process(
            target=_live_reconfig_worker,
            args=(
                str(tmp_path / f"logs_{i}"),
                server_id,
                channel,
                state_key,
                ready_flag,
                str(result_paths[i]),
            ),
        )
        for i in range(2)
    ]
    for p in procs:
        p.start()

    try:
        client = GulpRedis.get_instance().client()
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            count = await client.get(ready_flag)
            if count and int(count) >= len(procs):
                break
            await asyncio.sleep(0.1)
        else:
            pytest.fail("workers never signaled readiness")

        # give the subscribers a moment to be fully in their get_message() loop
        await asyncio.sleep(0.2)
        await reconfig.publish_reconfig(dev_mode=True)

        for p in procs:
            p.join(timeout=15)
            assert not p.is_alive(), "worker did not exit in time"
            assert p.exitcode == 0
    finally:
        for p in procs:
            if p.is_alive():
                p.terminate()

    for path in result_paths:
        data = json.loads(path.read_text())
        assert data["dev_mode"] is True


@pytest.mark.integration
async def test_process_started_after_publish_reads_persisted_state(tmp_path: Path):
    suffix = uuid4().hex
    server_id = f"test-glug-late-{suffix}"
    channel, state_key = _channel_and_state_key(suffix)

    if not await _redis_available(server_id):
        pytest.skip("Redis is not available through current gULP config")

    from gulp.glug import reconfig

    reconfig.RECONFIG_CHANNEL = channel
    reconfig.STATE_KEY = state_key

    # publish BEFORE any consuming process exists, simulating a runtime
    # change that happens while no worker is subscribed yet
    await reconfig.publish_reconfig(level="DEBUG", dev_mode=True)

    ctx = multiprocessing.get_context("spawn")
    result_path = tmp_path / "result.json"
    proc = ctx.Process(
        target=_late_joiner_worker,
        args=(str(tmp_path / "logs"), server_id, channel, state_key, str(result_path)),
    )
    proc.start()
    proc.join(timeout=15)

    assert not proc.is_alive()
    assert proc.exitcode == 0
    data = json.loads(result_path.read_text())
    assert data["dev_mode"] is True


@pytest.mark.integration
async def test_publish_log_dir_opens_and_closes_json_files_live(tmp_path: Path):
    suffix = uuid4().hex
    server_id = f"test-glug-logdir-{suffix}"
    channel, state_key = _channel_and_state_key(suffix)
    ready_flag = f"gulp:test:glug:ready:{suffix}"

    if not await _redis_available(server_id):
        pytest.skip("Redis is not available through current gULP config")

    from gulp.glug import reconfig

    reconfig.RECONFIG_CHANNEL = channel
    reconfig.STATE_KEY = state_key

    ctx = multiprocessing.get_context("spawn")
    result_paths = [tmp_path / f"result_{i}.json" for i in range(2)]
    procs = [
        ctx.Process(
            target=_live_log_dir_worker,
            args=(server_id, channel, state_key, ready_flag, str(result_paths[i])),
        )
        for i in range(2)
    ]
    for p in procs:
        p.start()

    log_dir = tmp_path / "logs"
    try:
        await _wait_counter(ready_flag, len(procs))
        await asyncio.sleep(0.2)
        await reconfig.publish_reconfig(log_dir=str(log_dir))

        # each process wrote its line, then the files are closed again
        await _wait_counter(ready_flag, 2 * len(procs))
        await reconfig.publish_reconfig(log_dir=None)

        for p in procs:
            p.join(timeout=15)
            assert not p.is_alive(), "worker did not exit in time"
            assert p.exitcode == 0
    finally:
        for p in procs:
            if p.is_alive():
                p.terminate()
        await GulpRedis.get_instance().client().delete(state_key, ready_flag)

    results = [json.loads(path.read_text()) for path in result_paths]
    assert all(r["opened"] and r["closed"] for r in results)

    log_subdir = log_dir / socket.gethostname()
    for r in results:
        json_file = log_subdir / f"gulp-worker-{r['pid']}.jsonl"
        messages = [json.loads(line)["message"] for line in json_file.read_text().splitlines()]
        # only what was logged while log_dir was on
        assert messages == [f"json line from pid {r['pid']}"]
        assert (log_subdir / f"gulp-worker-{r['pid']}-exceptions.jsonl").exists()


@pytest.mark.integration
async def test_persisted_state_merges_fields_and_reset_clears_it(tmp_path: Path):
    suffix = uuid4().hex
    server_id = f"test-glug-merge-{suffix}"
    channel, state_key = _channel_and_state_key(suffix)

    if not await _redis_available(server_id):
        pytest.skip("Redis is not available through current gULP config")

    from gulp.glug import reconfig

    reconfig.RECONFIG_CHANNEL = channel
    reconfig.STATE_KEY = state_key

    log_dir = str(tmp_path / "logs")
    try:
        # two separate publishes: the second must not drop what the first set
        await reconfig.publish_reconfig(level="WARNING", dev_mode=True)
        await reconfig.publish_reconfig(log_dir=log_dir)
        assert await reconfig.read_persisted_state() == {
            "level": "WARNING",
            "dev_mode": True,
            "log_dir": log_dir,
        }

        ctx = multiprocessing.get_context("spawn")
        result_path = tmp_path / "result.json"
        proc = ctx.Process(
            target=_late_joiner_worker,
            args=(None, server_id, channel, state_key, str(result_path)),
        )
        proc.start()
        proc.join(timeout=15)
        assert proc.exitcode == 0
        data = json.loads(result_path.read_text())
        assert data == {"dev_mode": True, "level": logging.WARNING, "log_dir": log_dir}
        assert list((Path(log_dir) / socket.gethostname()).glob("gulp-worker-*.jsonl"))

        await reconfig.reset_persisted_state()
        assert await reconfig.read_persisted_state() == {}
    finally:
        await GulpRedis.get_instance().client().delete(state_key)
