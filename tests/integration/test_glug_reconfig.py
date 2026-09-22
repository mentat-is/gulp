"""
Integration coverage for gulp.glug.reconfig: the Redis-backed mechanism that
broadcasts level/dev_mode changes live to every gulp process, and that lets
a process started after a change still pick up the current state.

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
        )
        await reconfig.sync_from_persisted_state()

        Path(result_path).write_text(
            json.dumps({"dev_mode": Glug.get_instance().is_dev_mode()})
        )
        await GulpRedis.get_instance().shutdown()

    asyncio.run(run())


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
