"""Internal event models, subscriptions, and Redis-backed dispatch."""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

import muty.string
import muty.time
import orjson
from muty.log import MutyLogger
from pydantic import BaseModel, ConfigDict, Field


class GulpCoreEvent(StrEnum):
    """Event types emitted by the gULP core."""

    USER_LOGIN = "user_login"
    USER_LOGOUT = "user_logout"
    SOURCE_FINISHED = "ingest_source_done"
    OPERATION_DELETED = "operation_deleted"
    CHUNK_POST_INGEST = "chunk_post_ingest"
    CHUNK_PRE_INGEST = "chunk_pre_ingest"


class GulpEvent(BaseModel):
    """One internal event delivered to subscribed listeners."""

    model_config = ConfigDict(extra="forbid")

    event_id: str
    event: str
    timestamp_msec: int
    data: dict = Field(default_factory=dict)
    user_id: str | None = None
    operation_id: str | None = None
    req_id: str | None = None


class GulpEventReply(BaseModel):
    """Optional listener reply that may replace data and stop dispatch."""

    result: dict | None = None
    stop: bool = False


class GulpEventResult(BaseModel):
    """Result of a waited event dispatch."""

    event_id: str
    event: str
    handlers: list[str] = Field(default_factory=list)
    result: dict = Field(default_factory=dict)
    stopped: bool = False


class GulpEventError(Exception):
    """Raised when a waited event cannot be delivered or handled."""


class GulpIngestSourceDoneEventData(BaseModel):
    """Payload emitted when ingestion of one source reaches a terminal state."""

    plugin: str
    user_id: str
    operation_id: str
    req_id: str
    file_path: str
    status: str
    plugin_params: dict
    errors: list[str] = Field(default_factory=list)
    records_ingested: int = 0
    records_failed: int = 0
    records_skipped: int = 0
    records_processed: int = 0


class GulpChunkPrePostIngestEventData(BaseModel):
    """Payload emitted immediately before or after ingesting one chunk."""

    chunk: list[dict]
    index: str
    operation_id: str
    req_id: str
    ws_id: str | None = None
    user_id: str
    plugin: str
    flt: Any = None


@dataclass
class _Listener:
    name: str
    instance: Any
    event_types: list[str]


class _Events:
    """Main-process listener registry and Redis event bridge."""

    CHUNK_PRE_INGEST = 1 << 0
    CHUNK_POST_INGEST = 1 << 1
    _CUSTOM_EVENT_RE = re.compile(r"^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)+$")

    def __init__(self) -> None:
        self._listeners: dict[str, _Listener] = {}
        self._chunk_mask = 0
        self._tasks: set[asyncio.Task] = set()

    @staticmethod
    def _event_name(event_type: GulpCoreEvent | str) -> str:
        if isinstance(event_type, GulpCoreEvent):
            return event_type.value
        if not isinstance(event_type, str) or not event_type:
            raise ValueError("event type must be a GulpCoreEvent or non-empty string")
        if event_type in {event.value for event in GulpCoreEvent}:
            return event_type
        if not _Events._CUSTOM_EVENT_RE.fullmatch(event_type):
            raise ValueError(
                "custom event types must be lowercase namespaced strings such as my_plugin.event_name"
            )
        return event_type

    @staticmethod
    def _listener_name(listener: Any) -> str:
        name = getattr(listener, "name", None)
        if not isinstance(name, str) or not name:
            raise ValueError("event listeners must expose a non-empty name")
        callback = getattr(listener, "on_event", None)
        if not callable(callback):
            raise ValueError("event listeners must implement on_event(event)")
        return name

    @staticmethod
    def _ensure_main_process() -> None:
        from gulp.process import GulpProcess

        if not GulpProcess.get_instance().is_main_process():
            raise RuntimeError("event subscriptions are main-process-only")

    def subscribe(self, listener: Any, event_types: list[GulpCoreEvent | str]) -> None:
        """Subscribe a named listener to one or more event types."""
        self._ensure_main_process()
        if not event_types:
            raise ValueError("event subscriptions require at least one event type")

        name = self._listener_name(listener)
        normalized = [self._event_name(event_type) for event_type in event_types]
        current = self._listeners.get(name)
        if current and current.instance is not listener:
            raise ValueError(f"event listener name already registered: {name}")

        if current is None:
            current = _Listener(name=name, instance=listener, event_types=[])
            self._listeners[name] = current
        for event_type in normalized:
            if event_type not in current.event_types:
                current.event_types.append(event_type)
        self._refresh_chunk_mask()

    def unsubscribe(self, listener: Any) -> None:
        """Unsubscribe a listener; repeated calls are harmless."""
        self._ensure_main_process()
        name = getattr(listener, "name", None)
        current = self._listeners.get(name)
        if current and current.instance is listener:
            del self._listeners[name]
            self._refresh_chunk_mask()

    def _refresh_chunk_mask(self) -> None:
        mask = 0
        for listener in self._listeners.values():
            if GulpCoreEvent.CHUNK_PRE_INGEST.value in listener.event_types:
                mask |= self.CHUNK_PRE_INGEST
            if GulpCoreEvent.CHUNK_POST_INGEST.value in listener.event_types:
                mask |= self.CHUNK_POST_INGEST
        self._chunk_mask = mask

        try:
            task = asyncio.get_running_loop().create_task(self._store_chunk_mask(mask))
        except RuntimeError:
            return
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _store_chunk_mask(self, mask: int) -> None:
        try:
            from gulp.api.redis_api import GulpRedis

            redis_client = GulpRedis.get_instance()
            if not redis_client.server_id:
                return
            await redis_client.client().set(
                self._mask_key(redis_client.server_id), mask
            )
        except Exception:
            MutyLogger.get_instance().exception("failed to update event listener mask")

    @staticmethod
    def _mask_key(server_id: str) -> str:
        return f"gulp:events:listener_mask:{server_id}"

    async def listener_mask(self) -> int:
        """Return the current source-level chunk listener mask."""
        from gulp.api.redis_api import GulpRedis
        from gulp.process import GulpProcess

        if GulpProcess.get_instance().is_main_process():
            return self._chunk_mask
        redis_client = GulpRedis.get_instance()
        raw = await redis_client.client().get(self._mask_key(redis_client.server_id))
        return int(raw or 0)

    async def emit(
        self,
        event_type: GulpCoreEvent | str,
        data: dict | None = None,
        *,
        wait: bool = False,
        broadcast: bool = False,
        timeout: float = 30.0,
        user_id: str | None = None,
        operation_id: str | None = None,
        req_id: str | None = None,
    ) -> GulpEventResult | None:
        """Publish an event, optionally waiting for main-process listeners."""
        event_name = self._event_name(event_type)
        if data is None:
            data = {}
        if not isinstance(data, dict):
            raise TypeError("event data must be a dictionary")
        if wait and broadcast:
            raise ValueError("waited events cannot be broadcast")
        if timeout <= 0:
            raise ValueError("event timeout must be greater than zero")

        event = GulpEvent(
            event_id=muty.string.generate_unique(),
            event=event_name,
            timestamp_msec=muty.time.now_msec(),
            data=data,
            user_id=user_id,
            operation_id=operation_id,
            req_id=req_id,
        )
        response_key = f"gulp:events:response:{event.event_id}" if wait else None
        if not wait:
            try:
                envelope = self._envelope(
                    event, wait=False, broadcast=broadcast, response_key=None
                )
                subscribers = await self._publish_local(envelope)
                if subscribers < 1:
                    MutyLogger.get_instance().warning(
                        "dropping event %s because no main-process subscriber is available",
                        event_name,
                    )
            except Exception:
                MutyLogger.get_instance().exception(
                    "failed to publish event %s", event_name
                )
            return None

        try:
            async with asyncio.timeout(timeout):
                envelope = self._envelope(
                    event,
                    wait=True,
                    broadcast=False,
                    response_key=response_key,
                )
                subscribers = await self._publish_local(envelope)
                if subscribers < 1:
                    raise GulpEventError(
                        f"event {event_name} has no main-process subscriber"
                    )
                return await self._wait_for_response(response_key)
        except TimeoutError as ex:
            await self._delete_response(response_key)
            raise GulpEventError(
                f"event {event_name} timed out after {timeout:g} seconds"
            ) from ex
        except GulpEventError:
            await self._delete_response(response_key)
            raise
        except Exception as ex:
            await self._delete_response(response_key)
            raise GulpEventError(f"failed to emit event {event_name}: {ex}") from ex

    def _envelope(
        self,
        event: GulpEvent,
        *,
        wait: bool,
        broadcast: bool,
        response_key: str | None,
    ) -> dict:
        return {
            "route_target_type": "event",
            "origin_server_id": self._server_id(),
            "payload": event.model_dump(),
            "wait": wait,
            "broadcast": broadcast,
            "response_key": response_key,
        }

    @staticmethod
    def _server_id() -> str:
        from gulp.api.redis_api import GulpRedis

        server_id = GulpRedis.get_instance().server_id
        if not server_id:
            raise RuntimeError("Redis is not initialized")
        return server_id

    async def _publish_local(self, envelope: dict) -> int:
        from gulp.api.redis_api import GulpRedis

        redis_client = GulpRedis.get_instance()
        result = await redis_client.publish(
            envelope, channel=redis_client.worker_to_main_channel()
        )
        return int(result or 0)

    async def _wait_for_response(self, response_key: str) -> GulpEventResult:
        from gulp.api.redis_api import GulpRedis

        redis_client = GulpRedis.get_instance()
        while True:
            raw = await redis_client.client().get(response_key)
            if raw is None:
                await asyncio.sleep(0.01)
                continue
            await self._delete_response(response_key)
            try:
                response = orjson.loads(raw)
                if not isinstance(response, dict) or not isinstance(
                    response.get("ok"), bool
                ):
                    raise ValueError("missing response status")
                if not response["ok"]:
                    raise GulpEventError(
                        str(response.get("error") or "listener failed")
                    )
                return GulpEventResult.model_validate(response.get("result"))
            except GulpEventError:
                raise
            except Exception as ex:
                raise GulpEventError("malformed event response") from ex

    async def _delete_response(self, response_key: str | None) -> None:
        if not response_key:
            return
        try:
            from gulp.api.redis_api import GulpRedis

            await GulpRedis.get_instance().client().delete(response_key)
        except Exception:
            pass

    async def _dispatch(self, event: GulpEvent, *, wait: bool) -> GulpEventResult:
        listeners = [
            listener
            for listener in self._listeners.values()
            if event.event in listener.event_types
        ]
        current_data = event.data
        handlers: list[str] = []
        stopped = False

        for listener in listeners:
            handlers.append(listener.name)
            current_event = event.model_copy(update={"data": current_data})
            try:
                reply = await listener.instance.on_event(current_event)
                if reply is None:
                    continue
                if isinstance(reply, dict):
                    current_data = reply
                    continue
                if isinstance(reply, GulpEventReply):
                    if reply.result is not None:
                        current_data = reply.result
                    if reply.stop:
                        stopped = True
                        break
                    continue
                raise TypeError(
                    f"listener {listener.name} returned unsupported type {type(reply).__name__}"
                )
            except Exception as ex:
                if wait:
                    raise GulpEventError(
                        f"event {event.event} listener {listener.name} failed: {ex}"
                    ) from ex
                MutyLogger.get_instance().exception(
                    "event %s listener %s failed", event.event, listener.name
                )

        return GulpEventResult(
            event_id=event.event_id,
            event=event.event,
            handlers=handlers,
            result=current_data,
            stopped=stopped,
        )

    async def _handle_redis_message(
        self,
        message: dict,
        *,
        redis_channel: str | None,
        origin_server_id: str,
    ) -> None:
        """Dispatch one event envelope received by the Redis broker."""
        from gulp.api.redis_api import GulpRedis, _MAIN_REDIS_CHANNEL

        redis_client = GulpRedis.get_instance()
        broadcast = bool(message.get("broadcast"))
        wait = bool(message.get("wait"))
        response_key = message.get("response_key")
        local_channel = redis_client.worker_to_main_channel()

        if redis_channel == _MAIN_REDIS_CHANNEL:
            if not broadcast:
                MutyLogger.get_instance().warning(
                    "dropping non-broadcast event received on the cluster channel"
                )
                return
            if origin_server_id == redis_client.server_id and message.get(
                "origin_processed"
            ):
                return
            try:
                event = GulpEvent.model_validate(message.get("payload"))
                await self._dispatch(event, wait=False)
            except Exception:
                MutyLogger.get_instance().exception(
                    "failed to dispatch broadcast event"
                )
            return

        if redis_channel != local_channel:
            MutyLogger.get_instance().warning(
                "dropping event received on unexpected Redis channel %s", redis_channel
            )
            return

        try:
            event = GulpEvent.model_validate(message.get("payload"))
            result = await self._dispatch(event, wait=wait)
            if wait:
                await self._write_response(
                    response_key, {"ok": True, "result": result.model_dump()}
                )
        except Exception as ex:
            if wait:
                await self._write_response(
                    response_key, {"ok": False, "error": str(ex)}
                )
            else:
                MutyLogger.get_instance().exception("failed to dispatch event")
            return

        if broadcast:
            forwarded = dict(message)
            forwarded["route_target_type"] = "event"
            forwarded["origin_server_id"] = origin_server_id
            forwarded["origin_processed"] = True
            try:
                subscribers = await redis_client.publish(
                    forwarded, channel=_MAIN_REDIS_CHANNEL
                )
                if int(subscribers or 0) < 1:
                    MutyLogger.get_instance().warning(
                        "broadcast event %s had no Redis subscribers", event.event
                    )
            except Exception:
                MutyLogger.get_instance().exception(
                    "failed to broadcast event %s", event.event
                )

    @staticmethod
    async def _write_response(response_key: str | None, response: dict) -> None:
        if not response_key:
            MutyLogger.get_instance().warning("waited event is missing a response key")
            return
        from gulp.api.redis_api import GulpRedis

        await GulpRedis.get_instance().client().set(
            response_key, orjson.dumps(response), ex=300
        )

    async def drain(self) -> None:
        """Wait for pending mask writes and Redis event callbacks."""
        tasks = list(self._tasks)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        try:
            from gulp.api.redis_api import GulpRedis

            await GulpRedis.get_instance().drain_pubsub_handlers()
        except Exception:
            MutyLogger.get_instance().exception("failed to drain event dispatch")

    def _reset_for_tests(self) -> None:
        """Clear process-local listeners without touching Redis."""
        self._listeners.clear()
        self._chunk_mask = 0
        for task in self._tasks:
            task.cancel()
        self._tasks.clear()


events = _Events()


__all__ = [
    "GulpCoreEvent",
    "GulpEvent",
    "GulpEventError",
    "GulpEventReply",
    "GulpEventResult",
    "events",
]
