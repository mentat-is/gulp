"""
This module provides a singleton class `GulpRedis` for managing asynchronous Redis operations, including WebSocket metadata storage, pub/sub messaging, and a task queue.
"""

import asyncio
import hashlib
import math
import os
import time
import zlib
from typing import Annotated, Any, Callable, Optional
from urllib.parse import urlparse

import muty.string
import orjson
import redis.asyncio as redis
from muty.log import MutyLogger
from pydantic import BaseModel, Field
from redis.asyncio.client import PubSub
from redis.exceptions import ConnectionError as RedisConnectionError, ResponseError
from gulp.api.prometheus_api import GulpMetrics
from gulp.config import GulpConfig


class GulpWsMetadata(BaseModel):
    """
    Metadata for a WebSocket connection stored in Redis
    """

    ws_id: Annotated[str, Field(description="The websocket id")]
    server_id: Annotated[
        str, Field(description="The server instance id that owns this websocket")
    ]
    types: Annotated[
        list[str],
        Field(
            description="List of GulpWsData.type this websocket is interested in, default is an empty list(all)",
            default_factory=list,
        ),
    ]
    operation_ids: Annotated[
        list[str],
        Field(
            description="List of `operation_id` this websocket is interested in, default is an empty list(all)",
            default_factory=list,
        ),
    ]
    socket_type: Annotated[
        str, Field(description="The socket type (default, ingest, client_data)")
    ]


class TaskQueueFullError(RuntimeError):
    """Raised when task admission control rejects a new queued task."""

    def __init__(
        self,
        task_type: str,
        queue_depth: int,
        queue_limit: int,
        scope: str = "task_type",
        retry_after_msec: int = 1000,
        work_units: int = 1,
    ):
        super().__init__(
            f"task queue full for scope={scope!r} task_type={task_type!r}: depth={queue_depth}, limit={queue_limit}, work_units={work_units}"
        )
        self.task_type = task_type
        self.queue_depth = queue_depth
        self.queue_limit = queue_limit
        self.scope = scope
        self.retry_after_msec = retry_after_msec
        self.work_units = max(1, int(work_units))


class IpRateLimitError(RuntimeError):
    """Raised when per-IP admission control rejects an unauthenticated request."""

    def __init__(
        self,
        scope: str,
        ip: str,
        count: int,
        limit: int,
        window_sec: int,
        retry_after_msec: int,
    ):
        super().__init__(
            f"IP rate limit exceeded for scope={scope!r} ip={ip!r}: count={count}, limit={limit}, window_sec={window_sec}"
        )
        self.scope = scope
        self.ip = ip
        self.count = count
        self.limit = limit
        self.window_sec = window_sec
        self.retry_after_msec = retry_after_msec


# module-local Redis channel names (class-level constants removed; use GulpRedisChannel as canonical source)
_MAIN_REDIS_CHANNEL = "gulpredis"
_CLIENT_DATA_REDIS_CHANNEL = "gulpredis:client_data"


class GulpRedis:
    """
    Singleton class to manage Redis connections and operations.
    """

    _instance: "GulpRedis" = None
    # redis set key to track known task types (members are task_type strings)
    TASK_TYPES_SET = "gulp:queue:types"
    # redis stream key prefix for task streams (one stream per task_type)
    STREAM_TASK_PREFIX = "gulp:stream:tasks"
    # redis stream prefix used to retain failed or malformed tasks for inspection
    STREAM_TASK_DLQ_PREFIX = "gulp:stream:tasks:dead"
    # redis hash/lock prefixes used to track task execution lifecycle.
    # By default lifecycle keys use req_id. Multi-task request producers may set
    # __task_id__ to keep duplicate suppression scoped to one queued stream entry.
    TASK_LIFECYCLE_PREFIX = "gulp:task:lifecycle"
    TASK_EXECUTION_LOCK_PREFIX = "gulp:task:execution_lock"
    TASK_SIDE_EFFECT_LOCK_PREFIX = "gulp:task:side_effect_lock"
    TASK_ACTIVE_USER_HASH = "gulp:task:active:user"
    TASK_ACTIVE_OPERATION_HASH = "gulp:task:active:operation"
    IP_RATE_LIMIT_PREFIX = "gulp:rate:ip"
    TASK_TERMINAL_STATUSES = {"succeeded", "dead_lettered", "canceled"}
    # consumer group name for task streams
    STREAM_CONSUMER_GROUP = "gulp:stream:group:tasks"
    # redis key for broadcasting aggregate websocket queue utilization from main to workers
    WS_QUEUE_UTILIZATION_KEY = "gulp:ws:queue_utilization"
    # how often (seconds) the main process updates the utilization key
    WS_QUEUE_UTILIZATION_UPDATE_INTERVAL: float = 1.0
    # redis key prefixes used by /ws_ingest_raw worker streams and companion keys
    WS_INGEST_STREAM_PREFIX = "gulp:stream:ws_ingest"
    WS_INGEST_BYTES_PREFIX = "gulp:stream:ws_ingest:bytes"
    WS_INGEST_DONE_PREFIX = "gulp:ws_ingest:done"
    WS_INGEST_ORPHAN_CLEANUP_INTERVAL: float = 30.0
    # interval (ms) used by dispatchers to refresh live task stream leases
    TASK_LEASE_REFRESH_INTERVAL_MS: int = 20_000
    # idle time (ms) after which stale pending stream entries can be reclaimed
    TASK_AUTOCLAIM_IDLE_MS: int = 60_000
    # retention for terminal task lifecycle records
    TASK_LIFECYCLE_TTL_SEC: int = 86_400
    TASK_ACTIVE_USER_MAX: int = 0
    TASK_ACTIVE_OPERATION_MAX: int = 0
    # hard cap for queued task backlog before admission control rejects new work
    STREAM_TASK_MAXLEN: int = 10_000
    # heartbeat key prefix and TTL for node liveness detection
    HEARTBEAT_KEY_PREFIX: str = "gulp:heartbeat"
    HEARTBEAT_TTL: int = 30  # seconds — key expires if not refreshed
    HEARTBEAT_INTERVAL: float = 10.0  # seconds between heartbeat updates
    # TTL for websocket metadata and lookup keys (seconds)
    WS_KEY_TTL: int = 300  # 5 minutes, refreshed periodically by live connections

    def __init__(self):
        self._redis: redis.Redis = None
        self._pubsub: PubSub = None
        self._subscriber_task: asyncio.Task = None
        self._subscriber_callback: Callable[[dict], Any] | None = None
        self._pubsub_lock: asyncio.Lock = asyncio.Lock()
        self.server_id: str = None
        # list of roles assigned to this server instance (e.g. ['ingest', 'query'])
        self._instance_roles: list[str] = []
        # simple task queue (list) key
        self._task_queue_key: str = "gulp:queue:tasks"
        # ttl for stored large payloads (seconds)
        self.PUBLISH_LARGE_PAYLOAD_TTL: int = (
            5 * 60
        )  # 5 min, it should be consumed quickly
        self._publish_rate_limit: int = 100  # msgs per second

        # Token-bucket for publish backpressure (replaces sliding-window sleep behavior)
        # - `_token_bucket_rate` : refill rate (tokens/sec) — defaults to `_publish_rate_limit`
        # - `_token_bucket_capacity` : max burst capacity (tokens)
        # - `_token_bucket_tokens` : current available tokens (float)
        # - `_token_bucket_last` : last refill timestamp
        self._token_bucket_rate: float = float(self._publish_rate_limit)
        self._token_bucket_capacity: float = max(
            1.0, float(self._publish_rate_limit) * 2.0
        )
        self._token_bucket_tokens: float = float(self._token_bucket_capacity)
        self._token_bucket_last: float = time.monotonic()

        # timestamp of last aggregate utilization update (main process only)
        self._last_utilization_update: float = 0.0
        # timestamp of last heartbeat update (main process only)
        self._last_heartbeat_update: float = 0.0
        # timestamp of last raw websocket ingest orphan cleanup (main process only)
        self._last_ws_ingest_orphan_cleanup: float = 0.0
        # timestamp of last stale task autoclaim sweep (main process only)
        self._last_autoclaim_time: float = 0.0
        # cached utilization value for workers (read from Redis)
        self._cached_queue_utilization: float = 0.0
        self._cached_queue_utilization_time: float = 0.0
        # semaphore to limit concurrent message handler tasks in subscriber loop
        self._handler_semaphore: asyncio.Semaphore = asyncio.Semaphore(16)
        # track in-flight handler tasks for cleanup
        self._handler_tasks: set[asyncio.Task] = set()

    def __new__(cls):
        """
        Create a new instance of the class.
        """
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def get_instance(cls) -> "GulpRedis":
        """
        returns the singleton instance of the Redis client.

        Returns:
            GulpRedis: The singleton instance of the Redis client.
        """
        if not cls._instance:
            cls._instance = cls()
        return cls._instance

    def client(self) -> redis.Redis:
        """
        Get the underlying Redis (async) client.

        Returns:
            redis.Redis: The Redis (async) client instance.

        Throws:
            RuntimeError: If initialize() has not been called yet.
        """
        if not self._redis:
            raise RuntimeError("initialize() must be called first!")
        return self._redis

    def initialize(self, server_id: str, main_process: bool = True) -> None:
        """
        Initialize Redis pub/sub connections.

        Args:
            server_id (str): The unique server instance ID
        """
        self.server_id = server_id

        # determine instance roles from configuration
        self._instance_roles = GulpConfig.get_instance().instance_roles()

        # load configurable Redis constants
        cfg = GulpConfig.get_instance()
        GulpRedis.TASK_LEASE_REFRESH_INTERVAL_MS = (
            cfg.redis_task_lease_refresh_interval_ms()
        )
        GulpRedis.TASK_AUTOCLAIM_IDLE_MS = cfg.redis_task_autoclaim_idle_ms()
        GulpRedis.TASK_LIFECYCLE_TTL_SEC = cfg.redis_task_lifecycle_ttl_sec()
        GulpRedis.TASK_ACTIVE_USER_MAX = cfg.redis_task_active_user_max()
        GulpRedis.TASK_ACTIVE_OPERATION_MAX = cfg.redis_task_active_operation_max()
        GulpRedis.STREAM_TASK_MAXLEN = cfg.redis_stream_task_maxlen()
        GulpRedis.HEARTBEAT_TTL = cfg.redis_heartbeat_ttl()
        GulpRedis.HEARTBEAT_INTERVAL = cfg.redis_heartbeat_interval()
        GulpRedis.WS_KEY_TTL = cfg.redis_ws_key_ttl()

        # create redis client
        url: str = GulpConfig.get_instance().redis_url()
        max_connections = cfg.redis_max_connections()
        redis_kwargs: dict[str, Any] = {"decode_responses": False}
        if max_connections > 0:
            redis_kwargs["max_connections"] = max_connections
        self._redis = redis.Redis.from_url(url, **redis_kwargs)
        url_parts = urlparse(url)
        MutyLogger.get_instance().info(
            "client %s connected to Redis at %s:%d (max_connections=%s)",
            self._redis,
            url_parts.hostname,
            url_parts.port,
            max_connections if max_connections > 0 else "unlimited",
        )

        if main_process:
            # create pub/sub connection
            self._pubsub = self._redis.pubsub()

            MutyLogger.get_instance().info(
                "initialized Redis pub/sub for server_id=%s", server_id
            )

    async def shutdown(self) -> None:
        """
        close Redis connections and cleanup.
        """
        from gulp.process import GulpProcess

        main_process = GulpProcess.get_instance().is_main_process()

        if main_process:
            try:
                await self.unsubscribe()
            except Exception:
                MutyLogger.get_instance().exception(
                    "error closing pubsub during shutdown!"
                )

        # close redis client
        try:
            await self._redis.close()
            MutyLogger.get_instance().info(
                "Redis client %s connection closed, redis shutdown DONE!, main_process=%r",
                self._redis,
                main_process,
            )
        except Exception as ex:
            MutyLogger.get_instance().exception("error closing redis client!")

    def _get_ws_metadata_key(self, ws_id: str) -> str:
        """Get Redis key for websocket metadata."""
        return f"gulp:ws:metadata:{self.server_id}:{ws_id}"

    def _get_ws_metadata_key_for_server(self, server_id: str, ws_id: str) -> str:
        """Get Redis key for websocket metadata owned by a specific server."""
        return f"gulp:ws:metadata:{server_id}:{ws_id}"

    def _get_ws_lookup_key(self, ws_id: str) -> str:
        """Get Redis key for quick ws_id to server_id lookup."""
        return f"gulp:ws:lookup:{ws_id}"

    async def ws_register(
        self,
        ws_id: str,
        types: list[str] = None,
        operation_ids: list[str] = None,
        socket_type: str = "default",
    ) -> bool:
        """
        Register a websocket in Redis.

        Args:
            ws_id (str): The websocket id
            types (list[str], optional): List of message types this ws is interested in
            operation_ids (list[str], optional): List of operation_ids this ws is interested in
            socket_type (str): The socket type
            ttl (int): Time to live in seconds

        Returns:
            bool: True if registered successfully
        """
        metadata = GulpWsMetadata(
            ws_id=ws_id,
            server_id=self.server_id,
            types=types or [],
            operation_ids=operation_ids or [],
            socket_type=socket_type,
        )

        # store metadata
        metadata_key = self._get_ws_metadata_key(ws_id)
        await self._redis.set(
            metadata_key,
            orjson.dumps(metadata.model_dump()),
            ex=self.WS_KEY_TTL,
        )

        # store ws_id -> server_id lookup mapping
        lookup_key = self._get_ws_lookup_key(ws_id)
        await self._redis.set(lookup_key, self.server_id.encode(), ex=self.WS_KEY_TTL)

        MutyLogger.get_instance().debug(
            "registered websocket: ws_id=%s, server_id=%s",
            ws_id,
            self.server_id,
        )
        return True

    async def ws_unregister(self, ws_id: str) -> bool:
        """
        Unregister a websocket from Redis.

        Args:
            ws_id (str): The websocket id

        Returns:
            bool: True if unregistered successfully
        """
        deleted_total = 0
        try:
            # Delete only this server's metadata. A reconnect with the same
            # ws_id may already be owned by a different server.
            try:
                metadata_key = self._get_ws_metadata_key(ws_id)
                deleted = await self._redis.delete(metadata_key)
                if isinstance(deleted, int):
                    deleted_total += deleted
            except Exception:
                MutyLogger.get_instance().exception("failed to delete ws metadata key")

            # Delete the lookup only if it still points to this server. This
            # prevents a stale disconnect from erasing a newer remote owner.
            lookup_key = self._get_ws_lookup_key(ws_id)
            try:
                owner = await self._redis.get(lookup_key)
                owner_server_id = owner.decode() if isinstance(owner, bytes) else owner
                if owner_server_id == self.server_id:
                    deleted = await self._redis.delete(lookup_key)
                    if isinstance(deleted, int):
                        deleted_total += deleted
            except Exception:
                MutyLogger.get_instance().exception("failed to delete ws lookup key")

        except Exception:
            MutyLogger.get_instance().exception(
                "error during ws_unregister scan/delete"
            )

        MutyLogger.get_instance().debug(
            "unregistered websocket: ws_id=%s, deleted=%d",
            ws_id,
            deleted_total,
        )
        return deleted_total > 0

    async def ws_refresh_ttl(self, ws_id: str) -> None:
        """
        Refresh the TTL on websocket metadata and lookup keys for a live connection.

        Args:
            ws_id (str): The websocket id
        """
        try:
            metadata_key = self._get_ws_metadata_key(ws_id)
            lookup_key = self._get_ws_lookup_key(ws_id)
            pipe = self._redis.pipeline(transaction=False)
            pipe.expire(metadata_key, self.WS_KEY_TTL)
            pipe.expire(lookup_key, self.WS_KEY_TTL)
            await pipe.execute()
        except Exception:
            MutyLogger.get_instance().exception(
                "error refreshing TTL for ws_id=%s", ws_id
            )

    async def ws_refresh_all_ttls(self) -> None:
        """
        Refresh TTLs for all websocket keys owned by this server.
        Called periodically by the main process.
        """
        pattern = f"gulp:ws:metadata:{self.server_id}:*"
        try:
            cursor = 0
            while True:
                cursor, keys = await self._redis.scan(
                    cursor=cursor, match=pattern, count=200
                )
                if keys:
                    pipe = self._redis.pipeline(transaction=False)
                    for key in keys:
                        pipe.expire(key, self.WS_KEY_TTL)
                        # also refresh the lookup key
                        # extract ws_id from metadata key: gulp:ws:metadata:{server_id}:{ws_id}
                        try:
                            key_str = key.decode() if isinstance(key, bytes) else key
                            ws_id = key_str.rsplit(":", 1)[-1]
                            lookup_key = self._get_ws_lookup_key(ws_id)
                            pipe.expire(lookup_key, self.WS_KEY_TTL)
                        except Exception:
                            pass
                    await pipe.execute()
                if cursor == 0:
                    break
        except Exception:
            MutyLogger.get_instance().exception("error refreshing ws key TTLs")

    async def ws_get_server(self, ws_id: str) -> str:
        """
        Get the server_id that owns a websocket.

        Args:
            ws_id (str): The websocket id

        Returns:
            str: The server_id or None if not found
        """
        lookup_key = self._get_ws_lookup_key(ws_id)
        server_id: bytes = await self._redis.get(lookup_key)
        return server_id.decode() if server_id else None

    async def ws_get_metadata(self, ws_id: str) -> Optional[GulpWsMetadata]:
        """
        Get websocket metadata.

        Args:
            ws_id (str): The websocket id

        Returns:
            Optional[GulpWsMetadata]: The metadata or None if not found
        """
        server_id = await self.ws_get_server(ws_id)
        if not server_id:
            return None

        metadata_key = self._get_ws_metadata_key_for_server(server_id, ws_id)
        data = await self._redis.get(metadata_key)

        if data:
            return GulpWsMetadata.model_validate(orjson.loads(data))
        return None

    def worker_to_main_channel(self, server_id: str | None = None) -> str:
        """Return the targeted pub/sub channel used to hand messages to a server's main process."""
        target_server_id = server_id or self.server_id
        if not target_server_id:
            return _MAIN_REDIS_CHANNEL
        return f"{_MAIN_REDIS_CHANNEL}:server:{target_server_id}"

    @staticmethod
    def _pubsub_route_metric_labels(message: dict, channel: str) -> tuple[str, str]:
        """Return low-cardinality route labels for Redis pub/sub metrics."""
        route_target = message.get("route_target_type")
        if route_target not in {"worker_to_main", "broadcast", "client_data"}:
            route_target = "unknown"

        if channel == _CLIENT_DATA_REDIS_CHANNEL:
            channel_scope = "client_data"
        elif channel.startswith(f"{_MAIN_REDIS_CHANNEL}:server:"):
            channel_scope = "server"
        elif channel.startswith(f"{_MAIN_REDIS_CHANNEL}:group"):
            channel_scope = "group"
        elif channel == _MAIN_REDIS_CHANNEL:
            channel_scope = "main"
        else:
            channel_scope = "other"

        return route_target, channel_scope

    def _pubsub_channels(self) -> list[str]:
        """Return the channels the main-process subscriber must keep attached to."""
        channels = [_MAIN_REDIS_CHANNEL, _CLIENT_DATA_REDIS_CHANNEL]
        if self.server_id:
            channels.append(self.worker_to_main_channel(self.server_id))
        return channels

    def _has_active_pubsub_subscriptions(self, pubsub: PubSub | None = None) -> bool:
        """Return whether the pubsub object currently tracks any active subscriptions."""
        active_pubsub = pubsub or self._pubsub
        if active_pubsub is None:
            return False

        channels = getattr(active_pubsub, "channels", None) or {}
        patterns = getattr(active_pubsub, "patterns", None) or {}
        return bool(channels or patterns)

    def _ensure_pubsub(self) -> PubSub:
        """Create a pubsub object lazily when the main process subscribes."""
        if self._pubsub is None:
            self._pubsub = self._redis.pubsub()
        return self._pubsub

    async def publish(self, message: dict, channel: str | None = None) -> None:
        """
        Publish a message on redis pubsub.

        Args:
            message (dict): The message to publish (must be a GulpWsData dict)
            channel (str|None): Redis pubsub channel name to publish to. If None, uses main channel.
        """
        # determine redis channel name
        channel_to_use = channel or _MAIN_REDIS_CHANNEL

        # serialize message
        payload = orjson.dumps(message)
        compression_threshold: int = (
            GulpConfig.get_instance().redis_compression_threshold() * 1024
        )
        pubsub_max_chunk_size: int = (
            GulpConfig.get_instance().redis_pubsub_max_chunk_size() * 1024
        )

        await self._apply_publish_backpressure()

        if GulpConfig.get_instance().prometheus_enabled():
            # update Prometheus counters
            try:
                route_target, channel_scope = self._pubsub_route_metric_labels(
                    message, channel_to_use
                )
                GulpMetrics.redis_publish_total.inc()
                GulpMetrics.redis_publish_bytes_total.inc(len(payload))
                GulpMetrics.redis_publish_by_route_total.labels(
                    route_target=route_target,
                    channel_scope=channel_scope,
                ).inc()
                GulpMetrics.redis_publish_bytes_by_route_total.labels(
                    route_target=route_target,
                    channel_scope=channel_scope,
                ).inc(len(payload))
                if ":server:" in channel_to_use:
                    GulpMetrics.redis_targeted_publish_total.inc()
            except Exception:
                pass

        # if payload is too large, compress, chunk and store it in Redis then publish a small pointer
        try:
            if len(payload) > compression_threshold:
                # decide whether to compress payload based on configuration
                compress_payload = GulpConfig.get_instance().redis_compression_enabled()

                if compress_payload:
                    # compress the serialized payload to reduce storage and network size
                    try:
                        stored_bytes = zlib.compress(payload)
                        compressed_flag = True
                    except Exception:
                        MutyLogger.get_instance().exception(
                            "failed to compress large payload, falling back to direct publish"
                        )
                        await self._redis.publish(channel_to_use, payload)
                        return
                else:
                    # store uncompressed bytes
                    stored_bytes = payload
                    compressed_flag = False

                # split bytes into chunks
                chunks = [
                    stored_bytes[i : i + pubsub_max_chunk_size]
                    for i in range(0, len(stored_bytes), pubsub_max_chunk_size)
                ]
                num_chunks = len(chunks)

                # include server_id in base key to prevent cross-node chunk retrieval
                base_key = (
                    f"gulp:pub:payload:{self.server_id}:{muty.string.generate_unique()}"
                )
                chunk_keys = [f"{base_key}:{i}" for i in range(num_chunks)]

                # pointer metadata to publish on pubsub
                pointer_message = dict(message)
                pointer_message["payload"] = {
                    "__pointer_key": base_key,
                    "__chunks": num_chunks,
                    "__compressed": compressed_flag,
                    "__orig_len": len(payload),
                    "__server_id": self.server_id,  # track which server stored the chunks
                }
                pointer_bytes = orjson.dumps(pointer_message)

                # use a pipelined SET+EX calls followed by PUBLISH instead of Lua to avoid per-message
                # Lua script compilation overhead. Pipelines give similar atomicity for our use case
                # (all commands executed together) and are faster in practice.
                try:
                    pipe = self._redis.pipeline(transaction=True)
                    for i, c in enumerate(chunks):
                        pipe.set(chunk_keys[i], c, ex=self.PUBLISH_LARGE_PAYLOAD_TTL)
                    pipe.publish(channel_to_use, pointer_bytes)
                    await pipe.execute()

                    if GulpConfig.get_instance().prometheus_enabled():
                        # update Prometheus counter for chunked publishes
                        try:
                            GulpMetrics.redis_chunked_publish_total.inc()
                        except Exception:
                            pass
                    MutyLogger.get_instance().warning(
                        "published large %s message stored in %d chunks base=%s total_size=%d bytes (stored=%d)",
                        "compressed" if compressed_flag else "uncompressed",
                        num_chunks,
                        base_key,
                        len(payload),
                        len(stored_bytes),
                    )
                    return
                except Exception:
                    MutyLogger.get_instance().exception(
                        "pipeline store/publish failed for chunked payload, attempting non-transactional fallback"
                    )
                    # fallback: best-effort non-transactional pipeline (may succeed on partial failures)
                    try:
                        pipe = self._redis.pipeline(transaction=False)
                        for i, c in enumerate(chunks):
                            pipe.set(
                                chunk_keys[i], c, ex=self.PUBLISH_LARGE_PAYLOAD_TTL
                            )
                        pipe.publish(channel_to_use, pointer_bytes)
                        await pipe.execute()
                        MutyLogger.get_instance().warning(
                            "published large %s message stored (fallback non-atomic) in %d chunks base=%s",
                            "compressed" if compressed_flag else "uncompressed",
                            num_chunks,
                            base_key,
                        )
                        return
                    except Exception:
                        MutyLogger.get_instance().exception(
                            "failed to store chunked payload in Redis (pipeline fallback), falling back to direct publish"
                        )

        except Exception:
            MutyLogger.get_instance().exception("error handling large publish payload")

        # default: publish inline
        await self._redis.publish(channel_to_use, payload)

    async def _apply_publish_backpressure(self) -> None:
        """Token-bucket rate limiter to avoid flooding Redis pubsub.

        Replenishes tokens at `_token_bucket_rate` per second up to
        `_token_bucket_capacity`. Each publish consumes one token. If no
        token is available the coroutine waits (non-busy) until enough
        tokens accumulate. This provides smooth steady-state rate
        limiting with bounded bursts.
        """
        now = time.monotonic()

        # refill tokens based on elapsed time
        elapsed = now - self._token_bucket_last
        if elapsed > 0:
            self._token_bucket_tokens = min(
                self._token_bucket_capacity,
                self._token_bucket_tokens + elapsed * self._token_bucket_rate,
            )
            self._token_bucket_last = now

        # check aggregate websocket queue utilization and apply end-to-end throttling
        try:
            depth = await self._get_queue_utilization()
            throttle_threshold = GulpConfig.get_instance().ws_throttle_threshold()
            if depth > throttle_threshold:
                # progressive delay up to ws_throttle_max_delay() based on pressure above threshold
                max_delay = GulpConfig.get_instance().ws_throttle_max_delay()
                pressure_above = (depth - throttle_threshold) / (
                    1.0 - throttle_threshold
                )
                delay = max_delay * pressure_above
                MutyLogger.get_instance().warning(
                    "publish backpressure throttling: agg_queue_util=%.2f threshold=%.2f delay=%.3fs",
                    depth,
                    throttle_threshold,
                    delay,
                )
                await asyncio.sleep(delay)
        except Exception:
            # best-effort: if we can't inspect socket queues, proceed with normal token-bucket
            MutyLogger.get_instance().exception(
                "failed to evaluate aggregate queue utilization for backpressure"
            )

        # if token available, consume and return immediately
        if self._token_bucket_tokens >= 1.0:
            self._token_bucket_tokens -= 1.0
            return

        # compute time to wait until next token is available
        needed = 1.0 - self._token_bucket_tokens
        wait_time = (
            needed / self._token_bucket_rate if self._token_bucket_rate > 0 else 0.01
        )

        # wait (in small increments to be responsive to wakeups)
        # cap single sleep to 0.1s to remain responsive under contention
        while self._token_bucket_tokens < 1.0:
            await asyncio.sleep(min(wait_time, 0.1))
            now = time.monotonic()
            elapsed = now - self._token_bucket_last
            if elapsed > 0:
                self._token_bucket_tokens = min(
                    self._token_bucket_capacity,
                    self._token_bucket_tokens + elapsed * self._token_bucket_rate,
                )
                self._token_bucket_last = now
            # recompute remaining wait_time
            needed = max(0.0, 1.0 - self._token_bucket_tokens)
            wait_time = (
                needed / self._token_bucket_rate
                if self._token_bucket_rate > 0
                else 0.01
            )

        # consume token and continue
        self._token_bucket_tokens -= 1.0

    async def _get_queue_utilization(self) -> float:
        """Get aggregate websocket queue utilization.

        In the main process, reads directly from GulpConnectedSockets.
        In worker processes, reads the shared Redis key published by the main process.

        Returns:
            float: utilization between 0.0 and 1.0
        """
        from gulp.process import GulpProcess

        if GulpProcess.get_instance().is_main_process():
            from gulp.api.ws_api import GulpConnectedSockets

            return GulpConnectedSockets.get_instance().aggregate_queue_utilization()

        return self._cached_queue_utilization

    async def _update_queue_utilization_key(self) -> None:
        """Write the current aggregate websocket queue utilization to Redis.

        Called periodically from the main process subscriber loop so that
        worker processes can read the value for end-to-end backpressure.
        """
        now = time.monotonic()
        if (
            now - self._last_utilization_update
            < self.WS_QUEUE_UTILIZATION_UPDATE_INTERVAL
        ):
            return

        self._last_utilization_update = now
        try:
            from gulp.api.ws_api import GulpConnectedSockets

            depth = GulpConnectedSockets.get_instance().aggregate_queue_utilization()
            # SET with short TTL so stale values expire if main process dies
            await self._redis.set(
                self.WS_QUEUE_UTILIZATION_KEY,
                str(depth),
                ex=10,
            )
        except Exception:
            # best-effort
            pass

    async def _update_heartbeat(self) -> None:
        """Update the heartbeat key for this server instance.

        Called periodically from the main process subscriber loop so that
        other nodes can detect liveness.
        """
        now = time.monotonic()
        if now - self._last_heartbeat_update < self.HEARTBEAT_INTERVAL:
            return

        self._last_heartbeat_update = now
        try:
            key = f"{self.HEARTBEAT_KEY_PREFIX}:{self.server_id}"
            await self._redis.set(key, b"1", ex=self.HEARTBEAT_TTL)
        except Exception:
            # best-effort
            pass

    async def is_server_alive(self, server_id: str) -> bool:
        """Check if a server instance is alive by checking its heartbeat key.

        Args:
            server_id (str): The server instance ID to check.

        Returns:
            bool: True if the server's heartbeat key exists.
        """
        try:
            key = f"{self.HEARTBEAT_KEY_PREFIX}:{server_id}"
            return bool(await self._redis.exists(key))
        except Exception:
            return False

    async def cleanup_orphaned_ws_ingest_streams(self, limit: int = 100) -> int:
        """Delete raw ingest websocket streams whose owner heartbeat has expired."""
        cleaned = 0
        pattern = f"{self.WS_INGEST_STREAM_PREFIX}:*"
        try:
            async for raw_key in self._redis.scan_iter(match=pattern, count=200):
                key = raw_key.decode() if isinstance(raw_key, bytes) else str(raw_key)
                suffix = key.removeprefix(f"{self.WS_INGEST_STREAM_PREFIX}:")
                if suffix == key or suffix.startswith("bytes:"):
                    continue

                try:
                    owner_server_id, ws_id = suffix.split(":", 1)
                except ValueError:
                    continue

                if not owner_server_id or not ws_id:
                    continue

                if await self.is_server_alive(owner_server_id):
                    continue

                keys_to_delete = [
                    key,
                    f"{self.WS_INGEST_BYTES_PREFIX}:{owner_server_id}:{ws_id}",
                    f"{self.WS_INGEST_DONE_PREFIX}:{owner_server_id}:{ws_id}",
                ]
                await self._redis.delete(*keys_to_delete)
                cleaned += 1
                MutyLogger.get_instance().warning(
                    "cleaned orphaned raw websocket ingest stream: owner_server_id=%s ws_id=%s",
                    owner_server_id,
                    ws_id,
                )
                if cleaned >= limit:
                    break
        except Exception:
            MutyLogger.get_instance().exception(
                "error cleaning orphaned raw websocket ingest streams"
            )
        return cleaned

    async def _periodic_ws_ingest_orphan_cleanup(self) -> None:
        """Periodically reap raw ingest streams owned by dead server instances."""
        now = time.monotonic()
        if (
            now - self._last_ws_ingest_orphan_cleanup
            < self.WS_INGEST_ORPHAN_CLEANUP_INTERVAL
        ):
            return

        self._last_ws_ingest_orphan_cleanup = now
        await self.cleanup_orphaned_ws_ingest_streams()

    async def _periodic_ws_ttl_refresh(self) -> None:
        """Periodically refresh TTLs on all ws keys owned by this server.

        Runs at half the WS_KEY_TTL interval to ensure keys don't expire
        while connections are still alive.
        """
        now = time.monotonic()
        # refresh at half the TTL interval
        refresh_interval = self.WS_KEY_TTL / 2.0
        if not hasattr(self, "_last_ws_ttl_refresh"):
            self._last_ws_ttl_refresh = 0.0
        if now - self._last_ws_ttl_refresh < refresh_interval:
            return

        self._last_ws_ttl_refresh = now
        try:
            await self.ws_refresh_all_ttls()
        except Exception:
            # best-effort
            pass

    async def unsubscribe(self) -> None:
        """
        unsubscribe from the redis pubsub channel
        """
        async with self._pubsub_lock:
            task = self._subscriber_task
            pubsub = self._pubsub
            channels = self._pubsub_channels()

            self._subscriber_task = None
            self._subscriber_callback = None
            self._pubsub = None

        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        if not pubsub:
            MutyLogger.get_instance().debug(
                "unsubscribe requested with no active pubsub, server_id=%s",
                self.server_id,
            )
            return

        try:
            if self._has_active_pubsub_subscriptions(pubsub):
                await pubsub.unsubscribe(*channels)
        except Exception:
            MutyLogger.get_instance().exception("error unsubscribing pubsub channels")

        try:
            await pubsub.close()
        except Exception:
            MutyLogger.get_instance().exception(
                "error closing pubsub after unsubscribe"
            )

        MutyLogger.get_instance().info(
            "unsubscribed from channels: %s, server_id=%s",
            channels,
            self.server_id,
        )

    async def subscribe(
        self,
        callback: Callable[[dict], Any],
    ) -> None:
        """
        subscribe to the redis pubsub channel

        Args:
            callback: Async function fun(d: dict) to call with each message dict
        """
        # subscribe to main channel and client_data channel;
        async with self._pubsub_lock:
            if self._subscriber_task and self._subscriber_task.done():
                self._subscriber_task = None

            if self._subscriber_task:
                MutyLogger.get_instance().warning(
                    "subscribe requested while subscriber loop is already running, server_id=%s",
                    self.server_id,
                )
                return

            self._subscriber_callback = callback
            pubsub = self._ensure_pubsub()
            channels = self._pubsub_channels()

            if not self._has_active_pubsub_subscriptions(pubsub):
                await pubsub.subscribe(*channels)

            task = asyncio.create_task(
                self._subscriber_loop("subscriber", callback),
                name=f"gulpredis-sub-{self.server_id}",
            )
            self._subscriber_task = task

        MutyLogger.get_instance().info(
            "subscribed to channels: %s, server_id=%s",
            channels,
            self.server_id,
        )

    async def _recreate_pubsub(self) -> None:
        """Recreate pubsub connection and resubscribe after errors."""
        if GulpConfig.get_instance().prometheus_enabled():
            # update Prometheus counter for pubsub reconnects
            try:
                GulpMetrics.redis_pubsub_reconnect_total.inc()
            except Exception:
                pass

        async with self._pubsub_lock:
            if self._subscriber_callback is None:
                MutyLogger.get_instance().debug(
                    "skipping pubsub recreation because subscriber is shutting down"
                )
                return

            old_pubsub = self._pubsub
            self._pubsub = self._redis.pubsub()
            channels = self._pubsub_channels()

            try:
                if old_pubsub:
                    await old_pubsub.close()
            except Exception:
                MutyLogger.get_instance().exception(
                    "error closing pubsub before recreate"
                )

            await self._pubsub.subscribe(*channels)

        MutyLogger.get_instance().warning(
            "pubsub connection recreated and resubscribed: %s", channels
        )

    async def _subscriber_loop(
        self,
        channel_name: str,
        callback: Callable[[dict], Any],
    ) -> None:
        """
        Internal loop for processing Redis pub/sub messages from multiple channels.

        Args:
            channel_name (str): Descriptive name for logging (e.g., "subscriber")
            callback: Function to call with each message
        """
        MutyLogger.get_instance().debug(
            "starting subscriber loop for channels: %s, %s",
            _MAIN_REDIS_CHANNEL,
            _CLIENT_DATA_REDIS_CHANNEL,
        )

        try:
            backoff: float = 1.0
            while True:
                try:
                    current_task = asyncio.current_task()
                    if self._subscriber_task is not current_task:
                        MutyLogger.get_instance().debug(
                            "stopping stale subscriber loop for server_id=%s",
                            self.server_id,
                        )
                        return

                    pubsub = self._pubsub
                    if pubsub is None or not self._has_active_pubsub_subscriptions(
                        pubsub
                    ):
                        MutyLogger.get_instance().warning(
                            "subscriber loop stopping because no pubsub subscriptions are active, server_id=%s",
                            self.server_id,
                        )
                        return

                    message: dict = await pubsub.get_message(
                        ignore_subscribe_messages=True, timeout=0.25
                    )
                    if message:
                        actual_channel = message.get("channel")
                        if isinstance(actual_channel, bytes):
                            actual_channel = actual_channel.decode("utf-8")

                        try:
                            d: dict = orjson.loads(message["data"])
                            d["__redis_channel__"] = actual_channel
                            # dispatch as concurrent task to avoid head-of-line blocking
                            await self._handler_semaphore.acquire()
                            # calls _handle_pubsub_message releasing the lock in the end
                            task = asyncio.create_task(
                                self._guarded_callback(callback, d, actual_channel)
                            )
                            self._handler_tasks.add(task)
                            task.add_done_callback(self._handler_tasks.discard)
                        except Exception as ex:
                            if GulpConfig.get_instance().prometheus_enabled():
                                # update Prometheus counter for subscriber callback errors
                                try:
                                    GulpMetrics.redis_subscriber_errors_total.inc()
                                except Exception:
                                    pass
                            MutyLogger.get_instance().exception(
                                "ERROR in subscriber callback for channel %s: %s",
                                actual_channel,
                                ex,
                            )
                            # Continue processing instead of terminating the loop

                    backoff = 1.0

                    # periodically publish aggregate queue utilization for worker backpressure
                    await self._update_queue_utilization_key()

                    # periodically refresh heartbeat and ws key TTLs
                    await self._update_heartbeat()
                    await self._periodic_ws_ttl_refresh()
                    await self._periodic_ws_ingest_orphan_cleanup()

                except asyncio.CancelledError:
                    MutyLogger.get_instance().debug("subscriber loop cancelled")
                    raise
                except RuntimeError as ex:
                    if "pubsub connection not set" not in str(ex).lower():
                        raise

                    if self._subscriber_task is not asyncio.current_task():
                        MutyLogger.get_instance().debug(
                            "stale subscriber loop exiting after pubsub detach, server_id=%s",
                            self.server_id,
                        )
                        return

                    if self._subscriber_callback is None:
                        MutyLogger.get_instance().info(
                            "subscriber loop exiting because shutdown detached pubsub, server_id=%s",
                            self.server_id,
                        )
                        return

                    MutyLogger.get_instance().warning(
                        "redis pubsub lost its subscribed connection, recreating: %s",
                        ex,
                    )
                    try:
                        await self._recreate_pubsub()
                    except Exception:
                        MutyLogger.get_instance().exception("failed to recreate pubsub")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2.0, 30.0)
                except (RedisConnectionError, asyncio.IncompleteReadError) as ex:
                    MutyLogger.get_instance().warning(
                        "redis pubsub connection error, will reconnect: %s", ex
                    )
                    try:
                        await self._recreate_pubsub()
                    except Exception:
                        MutyLogger.get_instance().exception("failed to recreate pubsub")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2.0, 30.0)
                except Exception as ex:
                    MutyLogger.get_instance().exception(
                        "error in subscriber loop: %s", ex
                    )
                    await asyncio.sleep(1.0)

        except asyncio.CancelledError:
            MutyLogger.get_instance().info(
                "subscriber loop stopped for channels: %s, %s",
                _MAIN_REDIS_CHANNEL,
                _CLIENT_DATA_REDIS_CHANNEL,
            )
            # wait for in-flight handler tasks to finish
            if self._handler_tasks:
                await asyncio.gather(*self._handler_tasks, return_exceptions=True)

    async def _guarded_callback(
        self, callback: Callable[[dict], Any], d: dict, channel: str
    ) -> None:
        """Run callback under semaphore guard, releasing on completion."""
        try:
            await callback(d)
        except Exception as ex:
            if GulpConfig.get_instance().prometheus_enabled():
                # update Prometheus counter for subscriber callback errors
                try:
                    GulpMetrics.redis_subscriber_errors_total.inc()
                except Exception:
                    pass
            MutyLogger.get_instance().exception(
                "ERROR in subscriber callback for channel %s: %s",
                channel,
                ex,
            )
        finally:
            self._handler_semaphore.release()

    def task_queue_key(self) -> str:
        """Return the Redis key used for the task queue."""
        return self._task_queue_key

    async def _task_stream_keys(self, task_type: str | None = None) -> list[str]:
        """Return the task stream keys relevant to this instance."""
        keys: list[str] = []
        if self._instance_roles:
            keys.extend(
                f"{GulpRedis.STREAM_TASK_PREFIX}:{r}" for r in self._instance_roles
            )
        else:
            try:
                types = await self._redis.smembers(GulpRedis.TASK_TYPES_SET)
                if types:
                    for t in types:
                        if isinstance(t, bytes):
                            t = t.decode()
                        keys.append(f"{GulpRedis.STREAM_TASK_PREFIX}:{t}")
            except Exception:
                MutyLogger.get_instance().exception(
                    "failed to read known task types from %s", GulpRedis.TASK_TYPES_SET
                )

        if task_type:
            stream_key = f"{GulpRedis.STREAM_TASK_PREFIX}:{task_type}"
            if stream_key not in keys:
                keys.append(stream_key)
        return keys

    async def task_queue_depth(self, task_type: str | None = None) -> int:
        """Return the total queued task depth across all known task streams."""
        depth = 0
        for stream_key in await self._task_stream_keys(task_type):
            try:
                depth += int(await self._redis.xlen(stream_key))
            except Exception:
                MutyLogger.get_instance().exception(
                    "failed to read task queue depth for %s", stream_key
                )
        return depth

    @staticmethod
    def _redis_stream_id_msec(stream_id: str | bytes | None) -> int | None:
        """Extract the millisecond timestamp from a Redis stream entry id."""
        if not stream_id:
            return None
        if isinstance(stream_id, bytes):
            stream_id = stream_id.decode()
        try:
            return int(str(stream_id).split("-", 1)[0])
        except (TypeError, ValueError):
            return None

    @classmethod
    def _redis_stream_age_msec(
        cls,
        stream_id: str | bytes | None,
        now_msec: int,
    ) -> int:
        """Return a non-negative age for a Redis stream id."""
        entry_msec = cls._redis_stream_id_msec(stream_id)
        if entry_msec is None:
            return 0
        return max(0, now_msec - entry_msec)

    async def task_metrics_snapshot(self) -> dict[str, Any]:
        """Return task queue metrics for Prometheus collection and diagnostics."""
        snapshot: dict[str, Any] = {
            "task_types": {},
            "active": {
                "user": 0,
                "operation": 0,
            },
            "running": {},
        }
        now_msec = int(time.time() * 1000)

        try:
            types = await self._redis.smembers(GulpRedis.TASK_TYPES_SET)
        except Exception:
            MutyLogger.get_instance().exception(
                "failed to read known task types for metrics"
            )
            types = set()

        for t in types:
            task_type = t.decode() if isinstance(t, bytes) else str(t)
            stream_key = f"{GulpRedis.STREAM_TASK_PREFIX}:{task_type}"
            dead_letter_stream = f"{GulpRedis.STREAM_TASK_DLQ_PREFIX}:{task_type}"
            metrics = {
                "queued": 0,
                "pending": 0,
                "dead_lettered": 0,
                "oldest_queued_age_msec": 0,
                "oldest_pending_age_msec": 0,
            }
            try:
                metrics["queued"] = int(await self._redis.xlen(stream_key))
                if metrics["queued"] > 0:
                    oldest = await self._redis.xrange(
                        stream_key,
                        min="-",
                        max="+",
                        count=1,
                    )
                    if oldest:
                        metrics["oldest_queued_age_msec"] = self._redis_stream_age_msec(
                            oldest[0][0], now_msec
                        )
            except Exception:
                MutyLogger.get_instance().exception(
                    "failed to collect task stream depth for %s", stream_key
                )
            try:
                pending_info = await self._redis.xpending(
                    stream_key,
                    GulpRedis.STREAM_CONSUMER_GROUP,
                )
                if isinstance(pending_info, dict):
                    metrics["pending"] = int(pending_info.get("pending", 0) or 0)
                    if metrics["pending"] > 0:
                        metrics["oldest_pending_age_msec"] = (
                            self._redis_stream_age_msec(
                                pending_info.get("min"),
                                now_msec,
                            )
                        )
            except ResponseError as ex:
                if "NOGROUP" not in str(ex):
                    MutyLogger.get_instance().exception(
                        "failed to collect task pending depth for %s", stream_key
                    )
            except Exception:
                MutyLogger.get_instance().exception(
                    "failed to collect task pending depth for %s", stream_key
                )
            try:
                metrics["dead_lettered"] = int(
                    await self._redis.xlen(dead_letter_stream)
                )
            except Exception:
                MutyLogger.get_instance().exception(
                    "failed to collect task dead-letter depth for %s",
                    dead_letter_stream,
                )
            snapshot["task_types"][task_type] = metrics

        for scope, counter_hash in (
            ("user", GulpRedis.TASK_ACTIVE_USER_HASH),
            ("operation", GulpRedis.TASK_ACTIVE_OPERATION_HASH),
        ):
            try:
                values = await self._redis.hvals(counter_hash)
                total = 0
                for value in values:
                    if isinstance(value, bytes):
                        value = value.decode()
                    total += max(0, int(value or 0))
                snapshot["active"][scope] = total
            except Exception:
                MutyLogger.get_instance().exception(
                    "failed to collect active task reservations for scope=%s",
                    scope,
                )

        try:
            async for lifecycle_key in self._redis.scan_iter(
                match=f"{GulpRedis.TASK_LIFECYCLE_PREFIX}:*"
            ):
                lifecycle = self._decode_redis_mapping(
                    await self._redis.hgetall(lifecycle_key)
                )
                status = lifecycle.get("status")
                task_type = lifecycle.get("task_type") or "unknown"
                if status != "running":
                    continue
                server_id = lifecycle.get("server_id") or "unknown"
                running_by_server = snapshot["running"].setdefault(server_id, {})
                running_by_server[task_type] = running_by_server.get(task_type, 0) + 1
        except Exception:
            MutyLogger.get_instance().exception(
                "failed to collect running task lifecycle metrics"
            )

        return snapshot

    def _task_envelope(
        self,
        task: dict,
        stream_name: str,
        msg_id: str,
        consumer_name: str | None = None,
    ) -> dict:
        """Attach Redis stream metadata to a task payload."""
        if isinstance(stream_name, bytes):
            stream_name = stream_name.decode()
        if isinstance(msg_id, bytes):
            msg_id = msg_id.decode()
        envelope = dict(task)
        envelope["__redis_stream__"] = stream_name
        envelope["__redis_message_id__"] = msg_id
        envelope["__redis_consumer_group__"] = GulpRedis.STREAM_CONSUMER_GROUP
        envelope["__redis_consumer_name__"] = consumer_name or self.server_id
        return envelope

    @staticmethod
    def _task_lifecycle_key(task_id: str) -> str:
        """Return the Redis key for a task lifecycle hash."""
        return f"{GulpRedis.TASK_LIFECYCLE_PREFIX}:{task_id}"

    @staticmethod
    def _task_execution_lock_key(task_id: str) -> str:
        """Return the Redis key for a task execution lock."""
        return f"{GulpRedis.TASK_EXECUTION_LOCK_PREFIX}:{task_id}"

    @staticmethod
    def _task_side_effect_lock_key(task_id: str) -> str:
        """Return the Redis key for a task side-effect lease."""
        return f"{GulpRedis.TASK_SIDE_EFFECT_LOCK_PREFIX}:{task_id}"

    @staticmethod
    def _task_lifecycle_id(task: dict) -> str | None:
        """Return the internal lifecycle id for one queued task."""
        return task.get("__task_id__") or task.get("task_id") or task.get("req_id")

    @staticmethod
    def _decode_redis_mapping(raw: dict | None) -> dict[str, str]:
        """Decode a Redis hash returned with byte keys/values into strings."""
        if not raw:
            return {}
        decoded: dict[str, str] = {}
        for key, value in raw.items():
            if isinstance(key, bytes):
                key = key.decode()
            if isinstance(value, bytes):
                value = value.decode()
            decoded[str(key)] = str(value)
        return decoded

    @staticmethod
    def _task_active_status(status: str | None) -> bool:
        """Return whether a lifecycle status should count against active limits."""
        return status in {"queued", "running"}

    @staticmethod
    def _task_admission_retry_after_msec() -> int:
        """Return wait hint for temporary queue admission rejections."""
        return 1000

    @staticmethod
    def _task_type_from_stream(stream_name: str | bytes | None) -> str:
        """Infer a task type from a task stream key for metrics/logging."""
        if not stream_name:
            return "unknown"
        if isinstance(stream_name, bytes):
            stream_name = stream_name.decode()
        return str(stream_name).rsplit(":", 1)[-1] or "unknown"

    def _record_task_transition(
        self,
        action: str,
        task_type: str | None,
        outcome: str,
        amount: int = 1,
    ) -> None:
        """Increment the task lifecycle transition counter when Prometheus is on."""
        try:
            if not GulpConfig.get_instance().prometheus_enabled():
                return
            GulpMetrics.redis_task_transition_total.labels(
                action=action,
                task_type=str(task_type or "unknown"),
                outcome=outcome,
            ).inc(amount)
        except Exception:
            MutyLogger.get_instance().exception(
                "failed to record task transition metric action=%s task_type=%s outcome=%s",
                action,
                task_type,
                outcome,
            )

    async def _record_task_execution_duration(
        self,
        task: dict,
        outcome: str,
        finished_at_msec: int | None = None,
    ) -> None:
        """Observe task execution duration from claim to terminal outcome."""
        try:
            if not GulpConfig.get_instance().prometheus_enabled():
                return

            task_id = self._task_lifecycle_id(task)
            if not task_id:
                return
            lifecycle = self._decode_redis_mapping(
                await self._redis.hgetall(self._task_lifecycle_key(task_id))
            )
            started_at = lifecycle.get("started_at_msec")
            if not started_at:
                return
            if finished_at_msec is None:
                finished_at_msec = int(time.time() * 1000)
            duration_seconds = max(
                0,
                (finished_at_msec - int(started_at)) / 1000,
            )
            GulpMetrics.redis_task_execution_duration_seconds.labels(
                task_type=str(task.get("task_type") or "unknown"),
                outcome=outcome,
            ).observe(duration_seconds)
        except Exception:
            MutyLogger.get_instance().exception(
                "failed to record task execution duration metric task_type=%s req_id=%s outcome=%s",
                task.get("task_type"),
                task.get("req_id"),
                outcome,
            )

    def _task_admission_counters(self, task: dict) -> list[tuple[str, str, int, str]]:
        """Return active admission counters relevant to a task."""
        counters: list[tuple[str, str, int, str]] = []
        user_id = task.get("user_id")
        if user_id and GulpRedis.TASK_ACTIVE_USER_MAX > 0:
            counters.append(
                (
                    GulpRedis.TASK_ACTIVE_USER_HASH,
                    str(user_id),
                    GulpRedis.TASK_ACTIVE_USER_MAX,
                    "user",
                )
            )

        operation_id = task.get("operation_id")
        if operation_id and GulpRedis.TASK_ACTIVE_OPERATION_MAX > 0:
            counters.append(
                (
                    GulpRedis.TASK_ACTIVE_OPERATION_HASH,
                    str(operation_id),
                    GulpRedis.TASK_ACTIVE_OPERATION_MAX,
                    "operation",
                )
            )
        return counters

    @staticmethod
    def _positive_int(value: Any, default: int = 0) -> int:
        """Return value as a non-negative integer, or default on invalid input."""
        try:
            return max(0, int(value))
        except (TypeError, ValueError):
            return default

    @classmethod
    def _query_clause_count(cls, value: Any) -> int:
        """Return a rough count of query DSL clauses for admission estimation."""
        if isinstance(value, dict):
            count = 1
            for child in value.values():
                count += cls._query_clause_count(child)
            return count
        if isinstance(value, list):
            return sum(cls._query_clause_count(child) for child in value)
        return 0

    @classmethod
    def _query_work_units(cls, task_type: str, params: dict) -> int:
        """Estimate query task cost from query count, result window, and options."""
        total_num_queries = params.get("total_num_queries")
        query_count = cls._positive_int(total_num_queries)
        queries = params.get("queries")
        if query_count <= 0 and isinstance(queries, list):
            query_count = len(queries)
        query_count = max(1, query_count)

        q_options = (
            params.get("q_options") if isinstance(params.get("q_options"), dict) else {}
        )
        limit = cls._positive_int(q_options.get("limit"), default=1000) or 1000
        total_limit = cls._positive_int(q_options.get("total_limit"))
        result_window = total_limit if total_limit > 0 else limit
        window_units = max(1, math.ceil(result_window / 5_000))

        option_units = 0
        if q_options.get("highlight_results"):
            option_units += 1
        if q_options.get("create_notes") or q_options.get("group"):
            option_units += 1
        if task_type.startswith("external_query") or params.get("plugin"):
            option_units += 1

        clause_count = 0
        if isinstance(queries, list):
            clause_count = sum(cls._query_clause_count(q) for q in queries)
        clause_units = max(0, math.ceil(max(0, clause_count - 20) / 20))

        return max(1, (query_count * window_units) + option_units + clause_units)

    @classmethod
    def _ingest_work_units(cls, params: dict) -> int:
        """Estimate ingest task cost from file count, declared size, and storage work."""
        file_count = cls._positive_int(params.get("file_total"), default=1) or 1
        size_bytes = 0
        for key in ("file_size", "size", "upload_size", "declared_size"):
            size_bytes = cls._positive_int(params.get(key))
            if size_bytes > 0:
                break

        payload = (
            params.get("payload") if isinstance(params.get("payload"), dict) else {}
        )
        if size_bytes <= 0:
            for key in ("file_size", "size", "original_size", "declared_size"):
                size_bytes = cls._positive_int(payload.get(key))
                if size_bytes > 0:
                    break

        if size_bytes <= 0:
            file_path = params.get("file_path") or params.get("local_file_path")
            if file_path:
                try:
                    size_bytes = max(0, os.path.getsize(str(file_path)))
                except OSError:
                    size_bytes = 0

        size_units = max(1, math.ceil(size_bytes / (128 * 1024 * 1024)))
        store_file_units = 0
        plugin_params = payload.get("plugin_params")
        if isinstance(plugin_params, dict) and plugin_params.get("store_file"):
            store_file_units = 1
        return max(1, file_count, size_units + store_file_units)

    @classmethod
    def _filter_clause_count(cls, value: Any) -> int:
        """Return a rough count of filter predicates for admission estimation."""
        if value in (None, {}, []):
            return 0
        if isinstance(value, dict):
            return len(value) + sum(cls._filter_clause_count(v) for v in value.values())
        if isinstance(value, list):
            return sum(cls._filter_clause_count(v) for v in value)
        return 1

    @classmethod
    def _rebase_work_units(cls, params: dict) -> int:
        """Estimate rebase cost from field count and filter selectivity."""
        fields = params.get("fields")
        field_units = len(fields) if isinstance(fields, list) and fields else 1
        flt = params.get("flt")
        filter_clauses = cls._filter_clause_count(flt)
        if filter_clauses <= 0:
            filter_units = 4
        else:
            filter_units = max(1, min(4, math.ceil(filter_clauses / 4)))
        return max(1, field_units + filter_units)

    @classmethod
    def _enrich_work_units(cls, params: dict) -> int:
        """Estimate enrich/update cost from action, fields, and filter breadth."""
        action = params.get("action")
        fields = params.get("fields")
        remove_fields = params.get("remove_fields")
        data = params.get("data")
        tags = params.get("tags")

        field_units = 1
        if isinstance(fields, dict) and fields:
            field_units = len(fields)
        elif isinstance(remove_fields, list) and remove_fields:
            field_units = len(remove_fields)
        elif isinstance(data, dict) and data:
            field_units = len(data)
        elif isinstance(tags, list) and tags:
            field_units = len(tags)

        flt = params.get("flt")
        filter_clauses = cls._filter_clause_count(flt)
        filter_units = (
            4 if filter_clauses <= 0 else max(1, min(4, math.ceil(filter_clauses / 4)))
        )
        plugin_units = 1 if action == "enrich_documents" or params.get("plugin") else 0
        return max(1, field_units + filter_units + plugin_units)

    @classmethod
    def _task_work_units(cls, task: dict) -> int:
        """Estimate how many admission units a task should reserve."""
        explicit_units = task.get("__task_work_units__") or task.get("work_units")
        if explicit_units is not None:
            try:
                return max(1, int(explicit_units))
            except (TypeError, ValueError):
                return 1

        params = task.get("params") if isinstance(task.get("params"), dict) else {}
        task_type = str(task.get("task_type") or "")
        if task_type.startswith(("query", "external_query")):
            return cls._query_work_units(task_type, params)
        if task_type.startswith("ingest"):
            return cls._ingest_work_units(params)
        if task_type.startswith("rebase"):
            return cls._rebase_work_units(params)
        if task_type.startswith("enrich"):
            return cls._enrich_work_units(params)

        return 1

    async def _reserve_task_admission(self, task: dict, work_units: int) -> None:
        """Reserve active-task admission capacity for a new unique request."""
        task_id = self._task_lifecycle_id(task)
        lifecycle: dict[str, str] = {}
        if task_id:
            lifecycle = self._decode_redis_mapping(
                await self._redis.hgetall(self._task_lifecycle_key(task_id))
            )
            if self._task_active_status(lifecycle.get("status")):
                return

        counters = self._task_admission_counters(task)
        for counter_hash, counter_key, limit, scope in counters:
            raw_depth = await self._redis.hget(counter_hash, counter_key)
            if isinstance(raw_depth, bytes):
                raw_depth = raw_depth.decode()
            depth = int(raw_depth or 0)
            if depth + work_units > limit:
                raise TaskQueueFullError(
                    str(task.get("task_type") or "unknown"),
                    depth,
                    limit,
                    scope=scope,
                    retry_after_msec=self._task_admission_retry_after_msec(),
                    work_units=work_units,
                )

        if not task_id:
            return

        reservation_fields: dict[str, str] = {}
        for counter_hash, counter_key, _, scope in counters:
            await self._redis.hincrby(counter_hash, counter_key, work_units)
            reservation_fields[f"active_{scope}_hash"] = counter_hash
            reservation_fields[f"active_{scope}_key"] = counter_key
            reservation_fields[f"active_{scope}_units"] = str(work_units)

        if reservation_fields:
            reservation_fields["active_reserved"] = "1"
            await self._redis.hset(
                self._task_lifecycle_key(task_id),
                mapping=reservation_fields,
            )
            await self._redis.expire(
                self._task_lifecycle_key(task_id),
                GulpRedis.TASK_LIFECYCLE_TTL_SEC,
            )

    async def _release_task_admission(self, task: dict) -> None:
        """Release active-task admission capacity for a terminal request."""
        task_id = self._task_lifecycle_id(task)
        if not task_id:
            return

        lifecycle_key = self._task_lifecycle_key(task_id)
        lifecycle = self._decode_redis_mapping(await self._redis.hgetall(lifecycle_key))
        if lifecycle.get("active_reserved") != "1":
            return

        fields_to_clear = ["active_reserved"]
        for scope in ("user", "operation"):
            counter_hash = lifecycle.get(f"active_{scope}_hash")
            counter_key = lifecycle.get(f"active_{scope}_key")
            if not counter_hash or not counter_key:
                continue
            units = int(lifecycle.get(f"active_{scope}_units") or 1)
            depth = await self._redis.hincrby(counter_hash, counter_key, -units)
            if depth <= 0:
                await self._redis.hdel(counter_hash, counter_key)
            fields_to_clear.extend(
                [
                    f"active_{scope}_hash",
                    f"active_{scope}_key",
                    f"active_{scope}_units",
                ]
            )

        await self._redis.hdel(lifecycle_key, *fields_to_clear)

    @classmethod
    def _ip_rate_limit_key(cls, scope: str, ip: str) -> str:
        """Return the Redis key for a per-IP rate limit bucket."""
        ip_hash = hashlib.sha256((ip or "unknown").encode()).hexdigest()
        return f"{cls.IP_RATE_LIMIT_PREFIX}:{scope}:{ip_hash}"

    async def check_ip_rate_limit(
        self,
        scope: str,
        ip: str,
        limit: int,
        window_sec: int,
    ) -> None:
        """Increment and enforce a fixed-window per-IP request limit."""
        if limit <= 0 or window_sec <= 0:
            return

        key = self._ip_rate_limit_key(scope, ip)
        count = int(await self._redis.incr(key))
        if count == 1:
            await self._redis.expire(key, window_sec)

        ttl = int(await self._redis.ttl(key))
        if ttl < 0:
            await self._redis.expire(key, window_sec)
            ttl = window_sec

        if count > limit:
            raise IpRateLimitError(
                scope=scope,
                ip=ip,
                count=count,
                limit=limit,
                window_sec=window_sec,
                retry_after_msec=max(1, ttl) * 1000,
            )

    @staticmethod
    def _task_lock_ttl_ms() -> int:
        """Return execution lock TTL in milliseconds."""
        return max(
            1_000,
            GulpRedis.TASK_AUTOCLAIM_IDLE_MS
            + GulpRedis.TASK_LEASE_REFRESH_INTERVAL_MS,
        )

    @staticmethod
    def _task_side_effect_lock_ttl_ms() -> int:
        """Return side-effect lease TTL in milliseconds."""
        return max(300_000, GulpRedis._task_lock_ttl_ms() * 3)

    def _task_lock_value(self, task: dict) -> str:
        """Return the lock owner value for a dequeued task."""
        return "%s:%s:%s" % (
            self.server_id,
            task.get("__redis_stream__", ""),
            task.get("__redis_message_id__", ""),
        )

    async def _task_execution_lock_matches(self, task: dict) -> bool:
        """Return whether the current execution lock belongs to this task."""
        current = await self._task_execution_lock_owner(task)
        return current == self._task_lock_value(task)

    async def _task_execution_lock_owner(self, task: dict) -> str | None:
        """Return the current execution lock owner for this task."""
        task_id = self._task_lifecycle_id(task)
        if not task_id:
            return self._task_lock_value(task)
        current = await self._redis.get(self._task_execution_lock_key(task_id))
        if isinstance(current, bytes):
            current = current.decode()
        return current

    async def _task_side_effect_lock_matches(self, task: dict) -> bool:
        """Return whether the current side-effect lease belongs to this task."""
        task_id = self._task_lifecycle_id(task)
        if not task_id:
            return True
        current = await self._redis.get(self._task_side_effect_lock_key(task_id))
        if isinstance(current, bytes):
            current = current.decode()
        return current == self._task_lock_value(task)

    async def _task_lifecycle_hset(
        self,
        task: dict,
        status: str,
        extra: dict[str, Any] | None = None,
    ) -> None:
        """Update lifecycle hash fields for a task with a req_id."""
        task_id = self._task_lifecycle_id(task)
        if not task_id:
            return

        now_msec = int(time.time() * 1000)
        mapping: dict[str, Any] = {
            "task_id": task_id,
            "req_id": task.get("req_id", ""),
            "task_type": task.get("task_type", ""),
            "operation_id": task.get("operation_id", ""),
            "status": status,
            "updated_at_msec": now_msec,
            "server_id": self.server_id,
            "stream": task.get("__redis_stream__", ""),
            "message_id": task.get("__redis_message_id__", ""),
        }
        if extra:
            mapping.update(extra)
        await self._redis.hset(
            self._task_lifecycle_key(task_id),
            mapping={k: v for k, v in mapping.items() if v is not None},
        )
        await self._redis.expire(
            self._task_lifecycle_key(task_id),
            GulpRedis.TASK_LIFECYCLE_TTL_SEC,
        )

    async def _task_lifecycle_status(self, task: dict) -> str | None:
        """Return the current lifecycle status for a queued task."""
        task_id = self._task_lifecycle_id(task)
        if not task_id:
            return None
        lifecycle = self._decode_redis_mapping(
            await self._redis.hgetall(self._task_lifecycle_key(task_id))
        )
        return lifecycle.get("status")

    async def task_record_queued(self, task: dict) -> str:
        """Record that a task is queued unless it is already terminal."""
        task_id = self._task_lifecycle_id(task)
        if not task_id:
            self._record_task_transition(
                "record_queued",
                task.get("task_type"),
                "untracked",
            )
            return "untracked"

        lifecycle = self._decode_redis_mapping(
            await self._redis.hgetall(self._task_lifecycle_key(task_id))
        )
        status = lifecycle.get("status")
        if status in GulpRedis.TASK_TERMINAL_STATUSES:
            self._record_task_transition(
                "record_queued",
                task.get("task_type"),
                status,
            )
            return status
        await self._task_lifecycle_hset(task, "queued")
        self._record_task_transition(
            "record_queued",
            task.get("task_type"),
            "queued",
        )
        return "queued"

    async def task_claim_execution(self, task: dict) -> str:
        """
        Claim a task for execution by req_id.

        Returns:
            str: "claimed", "duplicate_running", "terminal", or "untracked".
        """
        task_id = self._task_lifecycle_id(task)
        if not task_id:
            self._record_task_transition(
                "claim",
                task.get("task_type"),
                "untracked",
            )
            return "untracked"

        lifecycle_key = self._task_lifecycle_key(task_id)
        lock_key = self._task_execution_lock_key(task_id)
        lifecycle = self._decode_redis_mapping(await self._redis.hgetall(lifecycle_key))
        if lifecycle.get("status") in GulpRedis.TASK_TERMINAL_STATUSES:
            self._record_task_transition(
                "claim",
                task.get("task_type"),
                "terminal",
            )
            return "terminal"
        lifecycle_owner = lifecycle.get("server_id")
        if (
            task.get("__redis_autoclaimed__")
            and lifecycle.get("status") == "running"
            and lifecycle_owner
            and lifecycle_owner != self.server_id
            and await self.is_server_alive(lifecycle_owner)
        ):
            MutyLogger.get_instance().warning(
                "not claiming autoclaimed task because lifecycle owner is still alive: task_type=%s req_id=%s owner=%s",
                task.get("task_type"),
                task.get("req_id"),
                lifecycle_owner,
            )
            self._record_task_transition(
                "claim",
                task.get("task_type"),
                "duplicate_running",
            )
            return "duplicate_running"

        lock_value = self._task_lock_value(task)
        acquired = await self._redis.set(
            lock_key,
            lock_value,
            nx=True,
            px=self._task_lock_ttl_ms(),
        )
        if not acquired:
            existing_lock = await self._redis.get(lock_key)
            if isinstance(existing_lock, bytes):
                existing_lock = existing_lock.decode()
            if existing_lock != lock_value:
                self._record_task_transition(
                    "claim",
                    task.get("task_type"),
                    "duplicate_running",
                )
                return "duplicate_running"

        side_effect_lock_key = self._task_side_effect_lock_key(task_id)
        side_effect_acquired = await self._redis.set(
            side_effect_lock_key,
            lock_value,
            nx=True,
            px=self._task_side_effect_lock_ttl_ms(),
        )
        if not side_effect_acquired:
            existing_side_effect_lock = await self._redis.get(side_effect_lock_key)
            if isinstance(existing_side_effect_lock, bytes):
                existing_side_effect_lock = existing_side_effect_lock.decode()
            if existing_side_effect_lock != lock_value:
                MutyLogger.get_instance().warning(
                    "not claiming task because side-effect lease is owned by another worker: task_type=%s req_id=%s",
                    task.get("task_type"),
                    task.get("req_id"),
                )
                await self._task_release_execution_lock(task)
                self._record_task_transition(
                    "claim",
                    task.get("task_type"),
                    "duplicate_running",
                )
                return "duplicate_running"

        await self._task_lifecycle_hset(
            task,
            "running",
            extra={
                "started_at_msec": int(time.time() * 1000),
                "lock": lock_value,
                "side_effect_lock": lock_value,
                "side_effect_lock_ttl_msec": self._task_side_effect_lock_ttl_ms(),
            },
        )
        self._record_task_transition(
            "claim",
            task.get("task_type"),
            "claimed",
        )
        return "claimed"

    async def task_mark_succeeded(self, task: dict) -> None:
        """Mark a task lifecycle as succeeded and release its execution lock."""
        task_id = self._task_lifecycle_id(task)
        if not task_id:
            self._record_task_transition(
                "succeed",
                task.get("task_type"),
                "untracked",
            )
            return
        status = await self._task_lifecycle_status(task)
        if status in GulpRedis.TASK_TERMINAL_STATUSES:
            MutyLogger.get_instance().warning(
                "not marking task succeeded because lifecycle is already terminal: task_type=%s req_id=%s status=%s",
                task.get("task_type"),
                task.get("req_id"),
                status,
            )
            self._record_task_transition(
                "succeed",
                task.get("task_type"),
                "terminal",
            )
            await self._task_release_execution_lock(task)
            await self._task_release_side_effect_lock(task)
            return
        lock_owner = await self._task_execution_lock_owner(task)
        lock_value = self._task_lock_value(task)
        if lock_owner is not None and lock_owner != lock_value:
            MutyLogger.get_instance().warning(
                "not marking task succeeded because execution lock is owned by another worker: task_type=%s req_id=%s",
                task.get("task_type"),
                task.get("req_id"),
            )
            self._record_task_transition(
                "succeed",
                task.get("task_type"),
                "lock_mismatch",
            )
            return
        if lock_owner is None:
            MutyLogger.get_instance().warning(
                "marking task succeeded after execution lock expired: task_type=%s req_id=%s",
                task.get("task_type"),
                task.get("req_id"),
            )
        finished_at_msec = int(time.time() * 1000)
        await self._record_task_execution_duration(
            task,
            "succeeded",
            finished_at_msec=finished_at_msec,
        )
        await self._task_lifecycle_hset(
            task,
            "succeeded",
            extra={"finished_at_msec": finished_at_msec},
        )
        await self._release_task_admission(task)
        await self._task_release_execution_lock(task)
        await self._task_release_side_effect_lock(task)
        self._record_task_transition(
            "succeed",
            task.get("task_type"),
            "succeeded",
        )

    async def task_mark_canceled(
        self,
        task: dict,
        reason: str = "request canceled",
    ) -> None:
        """Mark a task lifecycle as canceled and release queued reservations."""
        task_id = self._task_lifecycle_id(task)
        if not task_id:
            self._record_task_transition(
                "cancel",
                task.get("task_type"),
                "untracked",
            )
            return

        lifecycle = self._decode_redis_mapping(
            await self._redis.hgetall(self._task_lifecycle_key(task_id))
        )
        task_payload = {**lifecycle, **{k: v for k, v in task.items() if v is not None}}
        finished_at_msec = int(time.time() * 1000)
        await self._task_lifecycle_hset(
            task_payload,
            "canceled",
            extra={
                "finished_at_msec": finished_at_msec,
                "last_error": reason,
            },
        )
        await self._release_task_admission(task_payload)
        await self._task_release_execution_lock(task_payload)
        await self._task_release_side_effect_lock(task_payload)
        self._record_task_transition(
            "cancel",
            task_payload.get("task_type"),
            "canceled",
        )

    async def _task_release_execution_lock(self, task: dict) -> None:
        """Release a task execution lock if it exists."""
        task_id = self._task_lifecycle_id(task)
        if task_id and await self._task_execution_lock_matches(task):
            await self._redis.delete(self._task_execution_lock_key(task_id))

    async def _task_release_side_effect_lock(self, task: dict) -> None:
        """Release a task side-effect lease if it belongs to this task."""
        task_id = self._task_lifecycle_id(task)
        if task_id and await self._task_side_effect_lock_matches(task):
            await self._redis.delete(self._task_side_effect_lock_key(task_id))

    async def _ack_and_delete_task(self, stream_name: str, msg_id: str) -> None:
        """Acknowledge and delete one task stream entry."""
        try:
            pipe = self._redis.pipeline(transaction=False)
            pipe.xack(stream_name, GulpRedis.STREAM_CONSUMER_GROUP, msg_id)
            pipe.xdel(stream_name, msg_id)
            await pipe.execute()
            self._record_task_transition(
                "ack_delete",
                self._task_type_from_stream(stream_name),
                "success",
            )
        except ResponseError as ex:
            if "NOGROUP" in str(ex):
                try:
                    await self._redis.xdel(stream_name, msg_id)
                    self._record_task_transition(
                        "ack_delete",
                        self._task_type_from_stream(stream_name),
                        "nogroup_xdel",
                    )
                except Exception:
                    MutyLogger.get_instance().exception(
                        "failed to xdel %s %s after missing consumer group",
                        stream_name,
                        msg_id,
                    )
                    self._record_task_transition(
                        "ack_delete",
                        self._task_type_from_stream(stream_name),
                        "nogroup_xdel_error",
                    )
            else:
                MutyLogger.get_instance().exception(
                    "failed to ack/delete task %s %s: %s", stream_name, msg_id, ex
                )
                self._record_task_transition(
                    "ack_delete",
                    self._task_type_from_stream(stream_name),
                    "error",
                )
        except Exception as ex:
            MutyLogger.get_instance().exception(
                "failed to ack/delete task %s %s: %s", stream_name, msg_id, ex
            )
            self._record_task_transition(
                "ack_delete",
                self._task_type_from_stream(stream_name),
                "error",
            )

    async def task_ack_delete(self, task: dict) -> None:
        """Acknowledge and delete a dequeued task using its Redis envelope metadata."""
        stream_name = task.get("__redis_stream__")
        msg_id = task.get("__redis_message_id__")
        if not stream_name or not msg_id:
            MutyLogger.get_instance().warning(
                "task_ack_delete called without Redis envelope metadata: task_type=%s req_id=%s",
                task.get("task_type"),
                task.get("req_id"),
            )
            self._record_task_transition(
                "ack_delete",
                task.get("task_type"),
                "missing_metadata",
            )
            return
        await self._ack_and_delete_task(stream_name, msg_id)

    async def task_refresh_lease(self, task: dict) -> bool:
        """
        Refresh the pending-entry idle timer for a live task.

        Redis Streams use pending-entry idle time for XAUTOCLAIM. A long-running
        task must periodically reset that idle timer so another dispatcher does
        not reclaim and execute it while it is still alive.
        """
        stream_name = task.get("__redis_stream__")
        msg_id = task.get("__redis_message_id__")
        consumer_name = task.get("__redis_consumer_name__") or self.server_id
        if not stream_name or not msg_id:
            MutyLogger.get_instance().warning(
                "task_refresh_lease called without Redis envelope metadata: task_type=%s req_id=%s",
                task.get("task_type"),
                task.get("req_id"),
            )
            return False

        try:
            claimed = await self._redis.xclaim(
                stream_name,
                GulpRedis.STREAM_CONSUMER_GROUP,
                consumer_name,
                min_idle_time=0,
                message_ids=[msg_id],
            )
            if not claimed:
                MutyLogger.get_instance().warning(
                    "task lease refresh found no pending entry: stream=%s msg_id=%s task_type=%s req_id=%s",
                    stream_name,
                    msg_id,
                    task.get("task_type"),
                    task.get("req_id"),
                )
                return False
            task_id = self._task_lifecycle_id(task)
            if task_id:
                now_msec = int(time.time() * 1000)
                await self._redis.hset(
                    self._task_lifecycle_key(task_id),
                    mapping={
                        "status": "running",
                        "updated_at_msec": now_msec,
                        "server_id": self.server_id,
                        "stream": stream_name,
                        "message_id": msg_id,
                    },
                )
                await self._redis.expire(
                    self._task_lifecycle_key(task_id),
                    GulpRedis.TASK_LIFECYCLE_TTL_SEC,
                )
                await self._redis.pexpire(
                    self._task_execution_lock_key(task_id),
                    self._task_lock_ttl_ms(),
                )
                await self._redis.pexpire(
                    self._task_side_effect_lock_key(task_id),
                    self._task_side_effect_lock_ttl_ms(),
                )
            return True
        except ResponseError as ex:
            if "NOGROUP" not in str(ex):
                MutyLogger.get_instance().exception(
                    "failed to refresh task lease for %s %s: %s",
                    stream_name,
                    msg_id,
                    ex,
                )
            return False
        except Exception as ex:
            MutyLogger.get_instance().exception(
                "failed to refresh task lease for %s %s: %s",
                stream_name,
                msg_id,
                ex,
            )
            return False

    async def _dead_letter_task(
        self,
        stream_name: str,
        msg_id: str,
        reason: str,
        raw_payload: bytes | bytearray | str | None = None,
        task_payload: dict | None = None,
    ) -> None:
        """Store a malformed or unsupported task in a dead-letter stream."""
        if isinstance(stream_name, bytes):
            stream_name = stream_name.decode()
        if isinstance(msg_id, bytes):
            msg_id = msg_id.decode()
        task_type = "unknown"
        if isinstance(task_payload, dict):
            task_type = str(task_payload.get("task_type") or task_type)
        elif isinstance(raw_payload, (bytes, bytearray, str)):
            try:
                task_payload = orjson.loads(raw_payload)
                if isinstance(task_payload, dict):
                    task_type = str(task_payload.get("task_type") or task_type)
            except Exception:
                task_payload = None

        dead_letter_stream = f"{GulpRedis.STREAM_TASK_DLQ_PREFIX}:{task_type}"
        dead_letter = {
            "stream_name": stream_name,
            "message_id": msg_id,
            "reason": reason,
            "task": task_payload,
            "raw": (
                raw_payload.decode()
                if isinstance(raw_payload, (bytes, bytearray))
                else raw_payload
            ),
            "server_id": self.server_id,
        }

        try:
            await self._redis.xadd(
                dead_letter_stream,
                {"data": orjson.dumps(dead_letter)},
            )
            self._record_task_transition(
                "dead_letter",
                task_type,
                "written",
            )
        except Exception:
            MutyLogger.get_instance().exception(
                "failed to write task %s %s to dead-letter stream %s",
                stream_name,
                msg_id,
                dead_letter_stream,
            )
            self._record_task_transition(
                "dead_letter",
                task_type,
                "write_error",
            )
        finally:
            await self._ack_and_delete_task(stream_name, msg_id)

    async def task_fail_dead_letter(self, task: dict, reason: str) -> str:
        """
        Handle a valid task execution failure by moving it to dead-letter.

        Returns:
            str: "dead_letter" when written, "terminal" when already terminal,
            or "ignored" when the task cannot be finalized.
        """
        stream_name = task.get("__redis_stream__")
        msg_id = task.get("__redis_message_id__")
        if not stream_name or not msg_id:
            MutyLogger.get_instance().warning(
                "task_fail_dead_letter called without Redis envelope metadata: task_type=%s req_id=%s",
                task.get("task_type"),
                task.get("req_id"),
            )
            self._record_task_transition(
                "failure",
                task.get("task_type"),
                "missing_metadata",
            )
            return "ignored"
        status = await self._task_lifecycle_status(task)
        if status in GulpRedis.TASK_TERMINAL_STATUSES:
            MutyLogger.get_instance().warning(
                "not dead-lettering task because lifecycle is already terminal: task_type=%s req_id=%s status=%s",
                task.get("task_type"),
                task.get("req_id"),
                status,
            )
            self._record_task_transition(
                "failure",
                task.get("task_type"),
                "terminal",
            )
            await self.task_ack_delete(task)
            await self._task_release_execution_lock(task)
            await self._task_release_side_effect_lock(task)
            return "terminal"
        if task.get("req_id") and not await self._task_execution_lock_matches(task):
            MutyLogger.get_instance().warning(
                "not dead-lettering task because execution lock is owned by another worker: task_type=%s req_id=%s",
                task.get("task_type"),
                task.get("req_id"),
            )
            self._record_task_transition(
                "failure",
                task.get("task_type"),
                "lock_mismatch",
            )
            return "ignored"

        task_payload = {k: v for k, v in task.items() if not k.startswith("__redis_")}
        task_payload["__task_last_error__"] = reason
        finished_at_msec = int(time.time() * 1000)
        task_payload["__task_last_failed_at_msec__"] = finished_at_msec
        await self._record_task_execution_duration(
            task_payload,
            "dead_letter",
            finished_at_msec=finished_at_msec,
        )
        await self._task_lifecycle_hset(
            task_payload,
            "dead_lettered",
            extra={
                "finished_at_msec": finished_at_msec,
                "last_error": reason,
            },
        )
        await self._release_task_admission(task_payload)
        await self._dead_letter_task(
            stream_name,
            msg_id,
            f"task failed: {reason}",
            task_payload=task_payload,
        )
        await self._task_release_execution_lock(task)
        await self._task_release_side_effect_lock(task)
        self._record_task_transition(
            "failure",
            task_payload.get("task_type"),
            "dead_letter",
        )
        return "dead_letter"

    async def task_dead_letter(self, task: dict, reason: str) -> bool:
        """Move a dequeued task directly to the dead-letter stream."""
        stream_name = task.get("__redis_stream__")
        msg_id = task.get("__redis_message_id__")
        if not stream_name or not msg_id:
            MutyLogger.get_instance().warning(
                "task_dead_letter called without Redis envelope metadata: task_type=%s req_id=%s",
                task.get("task_type"),
                task.get("req_id"),
            )
            self._record_task_transition(
                "dead_letter",
                task.get("task_type"),
                "missing_metadata",
            )
            return False
        status = await self._task_lifecycle_status(task)
        if status in GulpRedis.TASK_TERMINAL_STATUSES:
            MutyLogger.get_instance().warning(
                "not dead-lettering task because lifecycle is already terminal: task_type=%s req_id=%s status=%s",
                task.get("task_type"),
                task.get("req_id"),
                status,
            )
            self._record_task_transition(
                "dead_letter",
                task.get("task_type"),
                "terminal",
            )
            await self.task_ack_delete(task)
            await self._task_release_execution_lock(task)
            await self._task_release_side_effect_lock(task)
            return False
        if task.get("req_id") and not await self._task_execution_lock_matches(task):
            MutyLogger.get_instance().warning(
                "not dead-lettering task because execution lock is owned by another worker: task_type=%s req_id=%s",
                task.get("task_type"),
                task.get("req_id"),
            )
            self._record_task_transition(
                "dead_letter",
                task.get("task_type"),
                "lock_mismatch",
            )
            return False

        task_payload = {k: v for k, v in task.items() if not k.startswith("__redis_")}
        task_payload["__task_last_error__"] = reason
        finished_at_msec = int(time.time() * 1000)
        task_payload["__task_last_failed_at_msec__"] = finished_at_msec
        await self._record_task_execution_duration(
            task_payload,
            "dead_letter",
            finished_at_msec=finished_at_msec,
        )
        await self._task_lifecycle_hset(
            task_payload,
            "dead_lettered",
            extra={
                "finished_at_msec": finished_at_msec,
                "last_error": reason,
            },
        )
        await self._release_task_admission(task_payload)
        await self._dead_letter_task(
            stream_name,
            msg_id,
            reason,
            task_payload=task_payload,
        )
        await self._task_release_execution_lock(task)
        await self._task_release_side_effect_lock(task)
        self._record_task_transition(
            "dead_letter",
            task_payload.get("task_type"),
            "direct",
        )
        return True

    async def task_enqueue(self, task: dict) -> None:
        """
        Enqueue a task onto the Redis stream queue.

        Args:
            task (dict): JSON-serializable task with fields like
              {"task_type": str, "operation_id": str, "user_id": str, "ws_id": str, "req_id": str, "params": dict}
        """
        payload: bytes = orjson.dumps(task)

        # tasks must have a task_type; push only to the type-specific stream
        ttype = task.get("task_type")
        if not ttype:
            MutyLogger.get_instance().error(
                "task missing 'task_type', not enqueued: %s", task
            )
            self._record_task_transition("enqueue", "unknown", "missing_task_type")
            return

        # stream key per task type
        stream_key = f"{GulpRedis.STREAM_TASK_PREFIX}:{ttype}"
        admission_reserved = False
        try:
            work_units = self._task_work_units(task)
            if GulpRedis.STREAM_TASK_MAXLEN > 0:
                queue_depth = await self.task_queue_depth(ttype)
                if queue_depth >= GulpRedis.STREAM_TASK_MAXLEN:
                    self._record_task_transition(
                        "enqueue",
                        ttype,
                        "queue_full",
                    )
                    raise TaskQueueFullError(
                        ttype,
                        queue_depth,
                        GulpRedis.STREAM_TASK_MAXLEN,
                        retry_after_msec=self._task_admission_retry_after_msec(),
                        work_units=work_units,
                    )
            await self._reserve_task_admission(task, work_units)
            admission_reserved = True
            # add to stream under field 'data'
            await self._redis.xadd(stream_key, {"data": payload})
            # record known task type
            await self._redis.sadd(GulpRedis.TASK_TYPES_SET, ttype)
            await self.task_record_queued(task)
            self._record_task_transition("enqueue", ttype, "queued")
        except TaskQueueFullError:
            raise
        except Exception:
            if admission_reserved:
                await self._release_task_admission(task)
            # best-effort: log and continue
            MutyLogger.get_instance().exception(
                "failed to push task into role-specific stream '%s'", stream_key
            )
            self._record_task_transition("enqueue", ttype, "error")

        MutyLogger.get_instance().debug(
            "enqueued task_type=%s on %s, task=%s",
            ttype,
            stream_key,
            muty.string.make_shorter(str(task), max_len=260),
        )

    async def task_dequeue_batch(self, max_items: int) -> list[dict]:
        """
        Read up to max_items tasks from the Redis stream queue.

        Args:
            max_items (int): Maximum number of tasks to pop.

        Returns:
            list[dict]: Decoded task dicts.
        """
        items: list[dict] = []

        # determine stream keys to poll (per-type streams)
        keys: list[str] = await self._task_stream_keys()

        if not keys:
            return []

        # ensure consumer group exists for each stream
        for stream in keys:
            try:
                # create group starting from the beginning so existing entries are consumable
                await self._redis.xgroup_create(
                    stream, GulpRedis.STREAM_CONSUMER_GROUP, id="0", mkstream=True
                )
            except Exception:
                # ignore if group exists or other recoverable errors
                pass

        # build streams dict for xreadgroup: {stream: '>'}
        streams_dict = {k: ">" for k in keys}

        try:
            # read up to max_items across streams (non-blocking)
            entries = await self._redis.xreadgroup(
                GulpRedis.STREAM_CONSUMER_GROUP,
                self.server_id,
                streams=streams_dict,
                count=max_items,
                block=None,
                noack=False,
            )
        except Exception as ex:
            MutyLogger.get_instance().exception("error reading from streams: %s", ex)
            return []

        # entries: list of (stream, [(id, {field: value}), ...])
        for stream_name, msgs in entries:
            for msg_id, fields in msgs:
                raw = (
                    fields.get(b"data")
                    if isinstance(fields, dict)
                    else fields.get("data")
                )
                if raw is None:
                    await self._dead_letter_task(
                        stream_name,
                        msg_id,
                        "missing data field",
                        raw_payload=None,
                    )
                    continue
                try:
                    # raw may be bytes
                    if isinstance(raw, bytes):
                        d = orjson.loads(raw)
                    else:
                        d = orjson.loads(raw)
                    if not isinstance(d, dict):
                        raise TypeError("task payload must decode to dict")
                    items.append(
                        self._task_envelope(d, stream_name, msg_id, self.server_id)
                    )
                except Exception as ex:
                    MutyLogger.get_instance().error(
                        "invalid task payload in stream %s: %s", stream_name, ex
                    )
                    await self._dead_letter_task(
                        stream_name,
                        msg_id,
                        f"invalid task payload: {ex}",
                        raw_payload=raw,
                    )

        return items

    async def task_pop_blocking(self, timeout: int = 5) -> Optional[dict]:
        """
        Blocking read for a single task from the Redis stream queue.

        Args:
            timeout (int): Seconds to wait before returning None if queue is empty.

        Returns:
            Optional[dict]: Decoded task dict or None if timed out.
        """
        try:
            # build stream keys to read from
            keys: list[str] = await self._task_stream_keys()

            if not keys:
                return None

            # ensure consumer group exists for each stream
            for stream in keys:
                try:
                    # create group starting from the beginning so existing entries are consumable
                    await self._redis.xgroup_create(
                        stream, GulpRedis.STREAM_CONSUMER_GROUP, id="0", mkstream=True
                    )
                except Exception:
                    pass

            streams_dict = {k: ">" for k in keys}
            # block is in milliseconds
            block_ms = int(timeout * 1000) if timeout else 0
            res = await self._redis.xreadgroup(
                GulpRedis.STREAM_CONSUMER_GROUP,
                self.server_id,
                streams=streams_dict,
                count=1,
                block=block_ms,
                noack=False,
            )
            if not res:
                return None

            # pick first message found
            for stream_name, msgs in res:
                for msg_id, fields in msgs:
                    raw = (
                        fields.get(b"data")
                        if isinstance(fields, dict)
                        else fields.get("data")
                    )
                    try:
                        d = orjson.loads(raw)
                        if not isinstance(d, dict):
                            raise TypeError("task payload must decode to dict")
                        return self._task_envelope(
                            d, stream_name, msg_id, self.server_id
                        )
                    except Exception as ex:
                        MutyLogger.get_instance().error(
                            "invalid task payload on XREADGROUP, skipping: %s", ex
                        )
                        await self._dead_letter_task(
                            stream_name,
                            msg_id,
                            f"invalid task payload: {ex}",
                            raw_payload=raw,
                        )
                        continue
            return None
        except asyncio.CancelledError:
            raise
        except Exception as ex:
            MutyLogger.get_instance().exception("error in task_pop_blocking: %s", ex)
            await asyncio.sleep(0.5)
            return None

    async def task_autoclaim_stale(self) -> list[dict]:
        """
        Reclaim stale pending stream tasks whose consumer has stopped refreshing them.

        Returns:
            list[dict]: Reclaimed task envelopes ready for dispatch.
        """
        reclaimed: list[dict] = []
        keys = await self._task_stream_keys()
        for stream in keys:
            try:
                await self._redis.xgroup_create(
                    stream, GulpRedis.STREAM_CONSUMER_GROUP, id="0", mkstream=True
                )
            except Exception:
                pass

            try:
                _, claimed_msgs, _ = await self._redis.xautoclaim(
                    stream,
                    GulpRedis.STREAM_CONSUMER_GROUP,
                    self.server_id,
                    min_idle_time=GulpRedis.TASK_AUTOCLAIM_IDLE_MS,
                    start_id="0-0",
                    count=50,
                )
            except ResponseError as ex:
                if "NOGROUP" in str(ex):
                    continue
                MutyLogger.get_instance().exception(
                    "error autoclaiming stale tasks from %s: %s", stream, ex
                )
                continue
            except Exception as ex:
                MutyLogger.get_instance().exception(
                    "error autoclaiming stale tasks from %s: %s", stream, ex
                )
                continue

            for msg_id, fields in claimed_msgs:
                raw = (
                    fields.get(b"data")
                    if isinstance(fields, dict)
                    else fields.get("data")
                )
                if raw is None:
                    await self._dead_letter_task(
                        stream,
                        msg_id,
                        "missing data field in autoclaimed message",
                        raw_payload=None,
                    )
                    continue
                try:
                    task = orjson.loads(raw)
                    if not isinstance(task, dict):
                        raise TypeError("task payload must decode to dict")
                    envelope = self._task_envelope(task, stream, msg_id, self.server_id)
                    envelope["__redis_autoclaimed__"] = True
                    reclaimed.append(envelope)
                except Exception as ex:
                    MutyLogger.get_instance().error(
                        "invalid task payload in autoclaimed message %s/%s: %s",
                        stream,
                        msg_id,
                        ex,
                    )
                    await self._dead_letter_task(
                        stream,
                        msg_id,
                        f"invalid task payload in autoclaimed message: {ex}",
                        raw_payload=raw,
                    )

            if claimed_msgs:
                task_type = self._task_type_from_stream(stream)
                self._record_task_transition(
                    "autoclaim",
                    task_type,
                    "reclaimed",
                    amount=len(claimed_msgs),
                )
                MutyLogger.get_instance().warning(
                    "autoclaimed %d stale task(s) from %s",
                    len(claimed_msgs),
                    stream,
                )

        return reclaimed

    async def task_purge_by_filter(
        self, operation_id: str | None = None, req_id: str | None = None
    ) -> int:
        """
        Remove queued tasks matching the provided filter from the Redis list queue.

        Args:
            operation_id (str|None): If set, only remove tasks for this operation_id.
            req_id (str|None): If set, only remove tasks for this req_id.

        Returns:
            int: Number of removed tasks.
        """
        # purge matching tasks from all type-specific streams known in TASK_TYPES_SET
        removed_total: int = 0

        try:
            types = await self._redis.smembers(GulpRedis.TASK_TYPES_SET)
        except Exception:
            types = None

        if not types:
            return 0

        for t in types:
            if isinstance(t, bytes):
                t = t.decode()
            stream = f"{GulpRedis.STREAM_TASK_PREFIX}:{t}"
            try:
                entries = await self._redis.xrange(stream, min="-", max="+")
            except Exception:
                entries = None
            if not entries:
                continue

            removed: int = 0
            ids_to_del: list[str] = []
            for msg_id, fields in entries:
                raw = (
                    fields.get(b"data")
                    if isinstance(fields, dict)
                    else fields.get("data")
                )
                if raw is None:
                    continue
                try:
                    d = orjson.loads(raw)
                except Exception:
                    # skip malformed
                    continue

                cond_ok = True
                if operation_id is not None and d.get("operation_id") != operation_id:
                    cond_ok = False
                if req_id is not None and d.get("req_id") != req_id:
                    cond_ok = False

                if cond_ok:
                    await self._release_task_admission(d)
                    await self.task_mark_canceled(
                        d,
                        reason="request canceled while queued",
                    )
                    ids_to_del.append(msg_id)
                    removed += 1

            if ids_to_del:
                try:
                    pipe = self._redis.pipeline(transaction=False)
                    pipe.xack(stream, GulpRedis.STREAM_CONSUMER_GROUP, *ids_to_del)
                    pipe.xdel(stream, *ids_to_del)
                    await pipe.execute()
                    MutyLogger.get_instance().debug(
                        "purged %d task(s) from %s", removed, stream
                    )
                    removed_total += removed
                except Exception:
                    MutyLogger.get_instance().exception(
                        "failed to xdel ids on %s", stream
                    )

        return removed_total

    async def cleanup_redis(self) -> int:
        """
        Destroy all keys under the `gulp:*` namespace used by this application.

        Returns:
            int: Number of keys removed.
        """
        total_deleted = 0
        cursor = 0
        pattern = "gulp:*"
        try:
            # iterate over keys matching the gulp prefix and delete them in batches
            while True:
                cursor, keys = await self._redis.scan(
                    cursor=cursor, match=pattern, count=200
                )
                if keys:
                    try:
                        # redis.delete accepts varargs of keys
                        deleted = await self._redis.delete(*keys)
                        if isinstance(deleted, int):
                            total_deleted += deleted
                    except Exception:
                        MutyLogger.get_instance().exception(
                            "failed to delete key batch during cleanup"
                        )
                if cursor == 0:
                    break
        except Exception:
            MutyLogger.get_instance().exception(
                "error during cleanup_redis scan/delete"
            )

        MutyLogger.get_instance().info("cleanup_redis removed %d keys", total_deleted)
        return total_deleted

    async def cleanup_consumers_for_server(self, server_id: str) -> None:
        """
        Remove this server's consumer entries from all task streams' consumer groups.

        For each known task-type stream, attempt to remove the consumer named
        `server_id` from the `STREAM_CONSUMER_GROUP`.

        Args:
            server_id (str): The server instance ID whose consumers should be removed.
        """
        try:
            types = await self._redis.smembers(GulpRedis.TASK_TYPES_SET)
        except Exception:
            types = None

        if not types:
            return

        for t in types:
            if isinstance(t, bytes):
                t = t.decode()
            stream = f"{GulpRedis.STREAM_TASK_PREFIX}:{t}"
            try:
                # remove this consumer from the group; returns number of pending messages
                try:
                    await self._redis.xgroup_delconsumer(
                        stream, GulpRedis.STREAM_CONSUMER_GROUP, server_id
                    )
                except Exception:
                    # ignore failures (group may not exist yet)
                    pass

                # inspect group info: if group exists and has zero consumers and zero pending,
                # destroy the group and cleanup stream
                try:
                    groups = await self._redis.xinfo_groups(stream)
                except Exception:
                    groups = None

                if groups:
                    for g in groups:
                        # g is a dict-like mapping returned by redis-py
                        name = (
                            g.get(b"name")
                            if isinstance(g, dict) and b"name" in g
                            else g.get("name")
                        )
                        if (
                            name
                            and (name.decode() if isinstance(name, bytes) else name)
                            == GulpRedis.STREAM_CONSUMER_GROUP
                        ):
                            consumers = (
                                g.get(b"consumers")
                                if isinstance(g, dict) and b"consumers" in g
                                else g.get("consumers")
                            )
                            pending = (
                                g.get(b"pending")
                                if isinstance(g, dict) and b"pending" in g
                                else g.get("pending")
                            )
                            try:
                                consumers = int(consumers)
                            except Exception:
                                consumers = 0
                            try:
                                pending = int(pending)
                            except Exception:
                                pending = 0

                            if consumers == 0 and pending == 0:
                                try:
                                    await self._redis.xgroup_destroy(
                                        stream, GulpRedis.STREAM_CONSUMER_GROUP
                                    )
                                except Exception:
                                    pass
                            break
            except Exception:
                MutyLogger.get_instance().exception(
                    "error cleaning consumer for stream %s", stream
                )
