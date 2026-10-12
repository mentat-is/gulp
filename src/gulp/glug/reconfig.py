"""
runtime reconfiguration of Glug (level/dev_mode/JSON files) across all gulp
processes (main + every worker), via a dedicated Redis pub/sub channel plus
a persisted state hash.

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

# publish_reconfig() sentinel for "leave log_dir unchanged", since None already
# means "turn the JSON files off"
_UNCHANGED = object()


async def publish_reconfig(
    level: str = None, dev_mode: bool = None, log_dir=_UNCHANGED
) -> None:
    """
    broadcasts a level/dev_mode/log_dir change to every gulp process (main + workers).

    the changed fields are merged into the persisted STATE_KEY hash *before*
    publishing, so a process started after this call still picks up the
    current state on its next sync_from_persisted_state() call instead of
    missing the "bare" pub/sub message entirely. Fields left unchanged here
    keep their previously persisted value.

    the core never calls this: it is the entry point for an extension plugin
    that manages logging (see docs/logging.md, "Runtime reconfiguration").

    Args:
        level (str, optional): new logging level name (e.g. "DEBUG"), or None to leave unchanged.
        dev_mode (bool, optional): new dev_mode flag, or None to leave unchanged.
        log_dir (str | None, optional): base directory for the per-process JSON files, None to
            turn them off, omitted to leave them unchanged.
    """
    fields: dict = {}
    if level is not None:
        fields["level"] = level
    if dev_mode is not None:
        fields["dev_mode"] = dev_mode
    if log_dir is not _UNCHANGED:
        fields["log_dir"] = log_dir
    if not fields:
        return

    client = GulpRedis.get_instance().client()
    await client.hset(
        STATE_KEY, mapping={k: orjson.dumps(v) for k, v in fields.items()}
    )
    await client.publish(RECONFIG_CHANNEL, orjson.dumps(fields))


async def read_persisted_state() -> dict:
    """
    Returns:
        dict: the fields ("level", "dev_mode", "log_dir") published so far, each only if it
            ever was; {} if nothing was published since the last reset_persisted_state().
    """
    client = GulpRedis.get_instance().client()
    raw: dict = await client.hgetall(STATE_KEY)
    return {
        (k.decode() if isinstance(k, bytes) else k): orjson.loads(v)
        for k, v in raw.items()
    }


async def reset_persisted_state() -> None:
    """
    forgets every published change. The main process calls it at startup, before
    spawning workers: the command line alone applies until a plugin publishes again,
    so a removed plugin cannot leave dev_mode or the JSON files on from a previous run.
    """
    await GulpRedis.get_instance().client().delete(STATE_KEY)


def _apply_state(state: dict) -> None:
    level_name = state.get("level")
    level = getattr(logging, level_name.upper()) if level_name else None
    kwargs = {"level": level, "dev_mode": state.get("dev_mode")}
    if "log_dir" in state:
        kwargs["log_dir"] = state["log_dir"]
    Glug.apply_reconfig(**kwargs)


async def sync_from_persisted_state() -> None:
    """
    aligns this process's Glug instance to the last-published state.

    call once right after creating a worker's Glug instance, so a worker born
    after a runtime reconfigure (e.g. respawned after maxtasksperchild) still
    starts with the right level/dev_mode/JSON files instead of the values it
    was launched with.
    """
    state = await read_persisted_state()
    if state:
        _apply_state(state)


async def start_subscriber() -> None:
    """
    starts a background task that applies live level/dev_mode/log_dir changes
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
