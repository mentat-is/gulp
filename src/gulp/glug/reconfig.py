"""
runtime reconfiguration of Glug (level/dev_mode) across all gulp processes
(main + every worker), via a dedicated Redis pub/sub channel plus a
persisted state key.

Deliberately uses ONLY the raw client returned by
`GulpRedis.get_instance().client()` (a plain `redis.asyncio.Redis`), never
`src/gulp/api/redis_api.py`'s own pub/sub machinery: that machinery is wired
for `GulpWsData` websocket messages and only runs in the main process, while
glug reconfiguration is a rare, tiny, main+worker broadcast unrelated to
websocket routing — reusing it would mean forcing a foreign message format
onto a core, widely shared file for a single rare toggle. `process.py`
already uses this same raw-client pattern for its spawn-key INCR/EXPIRE.
"""

import asyncio
import logging

import orjson

from gulp.api.redis_api import GulpRedis
from gulp.glug.core import Glug

RECONFIG_CHANNEL = "gulp:glug:reconfig"
STATE_KEY = "gulp:glug:state"

_subscriber_task: asyncio.Task | None = None


async def publish_reconfig(level: str = None, dev_mode: bool = None) -> None:
    """
    broadcasts a level/dev_mode change to every gulp process (main + workers).

    the new state is persisted to STATE_KEY *before* publishing, so a
    process started after this call still picks up the current state on its
    next sync_from_persisted_state() call instead of missing the "bare"
    pub/sub message entirely.

    Args:
        level (str, optional): new logging level name (e.g. "DEBUG"), or None to leave unchanged.
        dev_mode (bool, optional): new dev_mode flag, or None to leave unchanged.
    """
    payload = orjson.dumps({"level": level, "dev_mode": dev_mode})
    client = GulpRedis.get_instance().client()
    await client.set(STATE_KEY, payload)
    await client.publish(RECONFIG_CHANNEL, payload)


async def read_persisted_state() -> dict:
    """
    Returns:
        dict: {"level": str|None, "dev_mode": bool|None} last published, or {} if nothing ever was.
    """
    client = GulpRedis.get_instance().client()
    raw = await client.get(STATE_KEY)
    if not raw:
        return {}
    return orjson.loads(raw)


def _apply_state(state: dict) -> None:
    level_name = state.get("level")
    level = getattr(logging, level_name.upper()) if level_name else None
    Glug.apply_reconfig(level=level, dev_mode=state.get("dev_mode"))


async def sync_from_persisted_state() -> None:
    """
    aligns this process's Glug instance to the last-published state.

    call once right after creating this process's Glug instance (main at
    FastAPI startup, each worker at spawn), so a process born after a
    runtime reconfigure still starts with the right level/dev_mode instead
    of the value it was launched with.
    """
    state = await read_persisted_state()
    if state:
        _apply_state(state)


async def start_subscriber() -> None:
    """
    starts a background task that applies live level/dev_mode changes
    published on RECONFIG_CHANNEL to this process's Glug instance.

    idempotent: calling it again in the same process while a subscriber is
    already running is a no-op.
    """
    global _subscriber_task
    if _subscriber_task is not None and not _subscriber_task.done():
        return

    client = GulpRedis.get_instance().client()
    pubsub = client.pubsub()
    await pubsub.subscribe(RECONFIG_CHANNEL)

    async def _loop() -> None:
        try:
            while True:
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=1.0
                )
                if message:
                    try:
                        _apply_state(orjson.loads(message["data"]))
                    except Exception:
                        Glug.get_instance().error(
                            "failed to apply glug reconfig message", exc_info=True
                        )
        finally:
            await pubsub.unsubscribe(RECONFIG_CHANNEL)
            await pubsub.close()

    _subscriber_task = asyncio.create_task(_loop(), name="glug-reconfig-subscriber")


async def stop_subscriber() -> None:
    """cancels the background reconfig subscriber task, if running (e.g. on process shutdown)."""
    global _subscriber_task
    if _subscriber_task is None:
        return
    _subscriber_task.cancel()
    try:
        await _subscriber_task
    except asyncio.CancelledError:
        pass
    _subscriber_task = None
