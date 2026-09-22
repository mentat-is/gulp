"""
This module provides a singleton class `GulpRedis` for managing asynchronous Redis operations, including WebSocket metadata storage, pub/sub messaging, and a task queue.
"""

import asyncio
import hashlib
import time
import json
import zlib
from typing import Annotated, Any, Callable, Optional
from urllib.parse import urlparse

import muty.string
import orjson
import redis.asyncio as redis
from pydantic import BaseModel, Field
from redis.asyncio.client import PubSub
from redis.exceptions import ConnectionError as RedisConnectionError, ResponseError
from gulp.api.prometheus_api import GulpMetrics
from gulp.config import GulpConfig
from gulp.glug import Glug


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
    data: Annotated[
        dict[str, Any],
        Field(
            description="Negotiated client capabilities and connection metadata.",
            default_factory=dict,
        ),
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
    # request-scoped streams created by batched enqueueing
    TASK_BATCH_STREAMS_SET = "gulp:queue:batch_streams"
    # redis stream key prefix for task streams
    STREAM_TASK_PREFIX = "gulp:stream:tasks"
    IP_RATE_LIMIT_PREFIX = "gulp:rate:ip"
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
    # idle time (ms) after which stale pending stream entries can be dropped
    TASK_AUTOCLAIM_IDLE_MS: int = 60_000
    # hard cap for queued task backlog before admission control rejects new work
    STREAM_TASK_MAXLEN: int = 10_000
    # compression threshold and max chunk size for pub/sub messages (bytes)
    PUBSUB_COMPRESSION_THRESHOLD: int = 256 * 1024
    PUBSUB_MAX_CHUNK_SIZE: int = 128 * 1024
    # heartbeat key prefix and TTL for node liveness detection
    HEARTBEAT_KEY_PREFIX: str = "gulp:heartbeat"
    HEARTBEAT_TTL: int = 30  # seconds — key expires if not refreshed
    HEARTBEAT_INTERVAL: float = 10.0  # seconds between heartbeat updates
    # TTL for websocket metadata and lookup keys (seconds)
    WS_KEY_TTL: int = 300  # 5 minutes, refreshed periodically by live connections
    QUERY_CHUNK_ACK_TTL: int = 300

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
        # Rotate the first task stream polled so one busy type cannot starve others.
        self._task_stream_cursor: int = 0
        # timestamp of last raw websocket ingest orphan cleanup (main process only)
        self._last_ws_ingest_orphan_cleanup: float = 0.0
        # timestamp of last stale pending task cleanup (main process only)
        self._last_stale_pending_cleanup_time: float = 0.0
        # cached utilization value for workers (read from Redis)
        self._cached_queue_utilization: float = 0.0
        self._cached_queue_utilization_time: float = 0.0
        # semaphore to limit concurrent message handler tasks in subscriber loop
        self._handler_semaphore: asyncio.Semaphore = asyncio.Semaphore(16)
        # track in-flight handler tasks for cleanup
        self._handler_tasks: set[asyncio.Task[None]] = set()
        # Preserve Redis reception order within one websocket request while
        # retaining concurrent callback processing across independent requests.
        self._request_handler_tails: dict[
            tuple[str, str], asyncio.Task[None]
        ] = {}

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
        GulpRedis.STREAM_TASK_MAXLEN = cfg.redis_stream_task_maxlen()

        # create redis client
        url: str = GulpConfig.get_instance().redis_url()
        max_connections = cfg.redis_max_connections()
        redis_kwargs: dict[str, Any] = {"decode_responses": False}
        if max_connections > 0:
            redis_kwargs["max_connections"] = max_connections
        self._redis = redis.Redis.from_url(url, **redis_kwargs)
        url_parts = urlparse(url)
        Glug.get_instance().info(
            f"client {self._redis} connected to Redis at "
            f"{url_parts.hostname}:{url_parts.port} "
            f"(max_connections={max_connections if max_connections > 0 else 'unlimited'})"
        )

        if main_process:
            # create pub/sub connection
            self._pubsub = self._redis.pubsub()

            Glug.get_instance().info(f"initialized Redis pub/sub for server_id={server_id}")

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
                Glug.get_instance().exception("error closing pubsub during shutdown")

        # close redis client
        try:
            await self._redis.close()
            Glug.get_instance().info(
                f"redis client {self._redis} connection closed, shutdown complete, "
                f"main_process={main_process!r}"
            )
        except Exception as ex:
            Glug.get_instance().exception("error closing redis client")

    def _get_ws_metadata_key(self, ws_id: str) -> str:
        """Get Redis key for websocket metadata."""
        return f"gulp:ws:metadata:{self.server_id}:{ws_id}"

    def _get_ws_metadata_key_for_server(self, server_id: str, ws_id: str) -> str:
        """Get Redis key for websocket metadata owned by a specific server."""
        return f"gulp:ws:metadata:{server_id}:{ws_id}"

    def _get_ws_lookup_key(self, ws_id: str) -> str:
        """Get Redis key for quick ws_id to server_id lookup."""
        return f"gulp:ws:lookup:{ws_id}"

    @classmethod
    def _get_query_chunk_ack_key(
        cls,
        ws_id: str,
        req_id: str,
        query_name: str,
        stage: str,
    ) -> str:
        """Return a bounded-cardinality Redis key for query chunk progress."""
        identity = "\0".join((ws_id, req_id, query_name, stage))
        digest = hashlib.sha256(identity.encode()).hexdigest()
        return f"gulp:query:chunk_ack:{digest}"

    async def ws_register(
        self,
        ws_id: str,
        types: list[str] = None,
        operation_ids: list[str] = None,
        socket_type: str = "default",
        data: dict[str, Any] = None,
    ) -> bool:
        """
        Register a websocket in Redis.

        Args:
            ws_id (str): The websocket id
            types (list[str], optional): List of message types this ws is interested in
            operation_ids (list[str], optional): List of operation_ids this ws is interested in
            socket_type (str): The socket type
            data (dict[str, Any], optional): Negotiated client capabilities.

        Returns:
            bool: True if registered successfully
        """
        metadata = GulpWsMetadata(
            ws_id=ws_id,
            server_id=self.server_id,
            types=types or [],
            operation_ids=operation_ids or [],
            socket_type=socket_type,
            data=data or {},
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

        Glug.get_instance().debug(
            f"registered websocket: ws_id={ws_id}, server_id={self.server_id}"
        )
        return True

    async def query_chunk_ack_record(
        self,
        ws_id: str,
        req_id: str,
        query_name: str,
        chunk_num: int,
        stage: str,
    ) -> None:
        """Record the greatest acknowledged chunk for one request stage."""
        key = self._get_query_chunk_ack_key(ws_id, req_id, query_name, stage)
        current = await self._redis.get(key)
        current_num = int(current) if current is not None else -1
        if chunk_num > current_num:
            await self._redis.set(key, str(chunk_num), ex=self.QUERY_CHUNK_ACK_TTL)
        else:
            await self._redis.expire(key, self.QUERY_CHUNK_ACK_TTL)

    async def query_chunk_ack_get(
        self,
        ws_id: str,
        req_id: str,
        query_name: str,
        stage: str,
    ) -> int | None:
        """Return the greatest acknowledged chunk for one request stage."""
        key = self._get_query_chunk_ack_key(ws_id, req_id, query_name, stage)
        value = await self._redis.get(key)
        if value is None:
            return None
        return int(value)

    async def query_chunk_ack_clear(
        self,
        ws_id: str,
        req_id: str,
        query_name: str,
    ) -> None:
        """Delete accepted and durable progress after terminal completion."""
        await self._redis.delete(
            self._get_query_chunk_ack_key(ws_id, req_id, query_name, "accepted"),
            self._get_query_chunk_ack_key(ws_id, req_id, query_name, "durable"),
        )

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
                Glug.get_instance().exception("failed to delete ws metadata key")

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
                Glug.get_instance().exception("failed to delete ws lookup key")

        except Exception:
            Glug.get_instance().exception("error during ws_unregister scan/delete")

        Glug.get_instance().debug(
            f"unregistered websocket: ws_id={ws_id}, deleted={deleted_total}"
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
            Glug.get_instance().exception(f"error refreshing TTL for ws_id={ws_id}")

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
            Glug.get_instance().exception("error refreshing ws key TTLs")

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
        if route_target not in {"worker_to_main", "broadcast", "client_data", "event"}:
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

    async def publish(self, message: dict, channel: str | None = None) -> int:
        """
        Publish a message on redis pubsub.

        Args:
            message (dict): The message to publish (must be a GulpWsData dict)
            channel (str|None): Redis pubsub channel name to publish to. If None, uses main channel.
        """
        # determine redis channel name
        channel_to_use = channel or _MAIN_REDIS_CHANNEL
        try:
            payload = orjson.dumps(message)
        except Exception:
            Glug.get_instance().exception(
                "failed to serialize message with orjson, trying with json... "
                "anyway, it is invalid data, should be checked",
                detail=message,
            )
            payload = json.dumps(message).encode()

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
            if len(payload) > self.PUBSUB_COMPRESSION_THRESHOLD:
                try:
                    stored_bytes = zlib.compress(payload)
                    compressed_flag = True
                except Exception:
                    Glug.get_instance().exception(
                        "failed to compress large payload, falling back to direct publish"
                    )
                    return int(await self._redis.publish(channel_to_use, payload))

                # split bytes into chunks
                chunks = [
                    stored_bytes[i : i + self.PUBSUB_MAX_CHUNK_SIZE]
                    for i in range(0, len(stored_bytes), self.PUBSUB_MAX_CHUNK_SIZE)
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
                    results = await pipe.execute()

                    if GulpConfig.get_instance().prometheus_enabled():
                        # update Prometheus counter for chunked publishes
                        try:
                            GulpMetrics.redis_chunked_publish_total.inc()
                        except Exception:
                            pass
                    # routine handling of an oversized payload, not an anomaly — DEBUG,
                    # not WARNING (the fallback path below, taken only when the primary
                    # pipeline fails, is the one that actually deserves a WARNING)
                    Glug.get_instance().debug(
                        f"published large "
                        f"{'compressed' if compressed_flag else 'uncompressed'} message "
                        f"stored in {num_chunks} chunks base={base_key} "
                        f"total_size={len(payload)} bytes (stored={len(stored_bytes)})"
                    )
                    return int(results[-1] or 0)
                except Exception:
                    Glug.get_instance().exception(
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
                        results = await pipe.execute()
                        Glug.get_instance().warning(
                            f"published large "
                            f"{'compressed' if compressed_flag else 'uncompressed'} message "
                            f"stored (fallback non-atomic) in {num_chunks} chunks base={base_key}"
                        )
                        return int(results[-1] or 0)
                    except Exception:
                        Glug.get_instance().exception(
                            "failed to store chunked payload in Redis (pipeline fallback), falling back to direct publish"
                        )

        except Exception:
            Glug.get_instance().exception("error handling large publish payload")

        # default: publish inline
        return int(await self._redis.publish(channel_to_use, payload))

    async def drain_pubsub_handlers(self) -> None:
        """Wait for the currently in-flight pub/sub callbacks to finish."""
        tasks = list(self._handler_tasks)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

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
            throttle_threshold = 0.7
            if depth > throttle_threshold:
                max_delay = 0.2
                pressure_above = (depth - throttle_threshold) / (
                    1.0 - throttle_threshold
                )
                delay = max_delay * pressure_above
                Glug.get_instance().warning(
                    f"publish backpressure throttling: agg_queue_util={depth:.2f} "
                    f"threshold={throttle_threshold:.2f} delay={delay:.3f}s"
                )
                await asyncio.sleep(delay)
        except Exception:
            # best-effort: if we can't inspect socket queues, proceed with normal token-bucket
            Glug.get_instance().exception(
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
            from gulp.events import events

            key = f"{self.HEARTBEAT_KEY_PREFIX}:{self.server_id}"
            await self._redis.set(key, b"1", ex=self.HEARTBEAT_TTL)
            await self._redis.expire(
                events._mask_key(self.server_id), self.HEARTBEAT_TTL
            )
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
                Glug.get_instance().warning(
                    f"cleaned orphaned raw websocket ingest stream: "
                    f"owner_server_id={owner_server_id} ws_id={ws_id}"
                )
                if cleaned >= limit:
                    break
        except Exception:
            Glug.get_instance().exception(
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
            Glug.get_instance().debug(
                f"unsubscribe requested with no active pubsub, server_id={self.server_id}"
            )
            return

        try:
            if self._has_active_pubsub_subscriptions(pubsub):
                await pubsub.unsubscribe(*channels)
        except Exception:
            Glug.get_instance().exception("error unsubscribing pubsub channels")

        try:
            await pubsub.close()
        except Exception:
            Glug.get_instance().exception("error closing pubsub after unsubscribe")

        Glug.get_instance().info(
            f"unsubscribed from channels: {channels}, server_id={self.server_id}"
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
                Glug.get_instance().warning(
                    "subscribe requested while subscriber loop is already running, "
                    f"server_id={self.server_id}"
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

        Glug.get_instance().info(
            f"subscribed to channels: {channels}, server_id={self.server_id}"
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
                Glug.get_instance().debug(
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
                Glug.get_instance().exception("error closing pubsub before recreate")

            await self._pubsub.subscribe(*channels)

        Glug.get_instance().warning(f"pubsub connection recreated and resubscribed: {channels}")

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
        Glug.get_instance().debug(
            f"starting subscriber loop for channels: {_MAIN_REDIS_CHANNEL}, "
            f"{_CLIENT_DATA_REDIS_CHANNEL}"
        )

        try:
            backoff: float = 1.0
            while True:
                try:
                    current_task = asyncio.current_task()
                    if self._subscriber_task is not current_task:
                        Glug.get_instance().debug(
                            f"stopping stale subscriber loop for server_id={self.server_id}"
                        )
                        return

                    pubsub = self._pubsub
                    if pubsub is None or not self._has_active_pubsub_subscriptions(
                        pubsub
                    ):
                        Glug.get_instance().warning(
                            "subscriber loop stopping because no pubsub subscriptions "
                            f"are active, server_id={self.server_id}"
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
                            # Dispatch requests independently while preserving the
                            # Redis reception order inside one websocket request.
                            await self._schedule_handler_callback(
                                callback,
                                d,
                                actual_channel,
                            )
                        except Exception as ex:
                            if GulpConfig.get_instance().prometheus_enabled():
                                # update Prometheus counter for subscriber callback errors
                                try:
                                    GulpMetrics.redis_subscriber_errors_total.inc()
                                except Exception:
                                    pass
                            Glug.get_instance().exception(
                                f"ERROR in subscriber callback for channel {actual_channel}: {ex}"
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
                    Glug.get_instance().debug("subscriber loop cancelled")
                    raise
                except RuntimeError as ex:
                    if "pubsub connection not set" not in str(ex).lower():
                        raise

                    if self._subscriber_task is not asyncio.current_task():
                        Glug.get_instance().debug(
                            f"stale subscriber loop exiting after pubsub detach, "
                            f"server_id={self.server_id}"
                        )
                        return

                    if self._subscriber_callback is None:
                        Glug.get_instance().info(
                            f"subscriber loop exiting because shutdown detached pubsub, "
                            f"server_id={self.server_id}"
                        )
                        return

                    Glug.get_instance().warning(
                        f"redis pubsub lost its subscribed connection, recreating: {ex}"
                    )
                    try:
                        await self._recreate_pubsub()
                    except Exception:
                        Glug.get_instance().exception("failed to recreate pubsub")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2.0, 30.0)
                except (RedisConnectionError, asyncio.IncompleteReadError) as ex:
                    Glug.get_instance().warning(
                        f"redis pubsub connection error, will reconnect: {ex}"
                    )
                    try:
                        await self._recreate_pubsub()
                    except Exception:
                        Glug.get_instance().exception("failed to recreate pubsub")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2.0, 30.0)
                except Exception as ex:
                    Glug.get_instance().exception(f"error in subscriber loop: {ex}")
                    await asyncio.sleep(1.0)

        except asyncio.CancelledError:
            Glug.get_instance().info(
                f"subscriber loop stopped for channels: {_MAIN_REDIS_CHANNEL}, "
                f"{_CLIENT_DATA_REDIS_CHANNEL}"
            )
        finally:
            # wait for in-flight handler tasks to finish
            if self._handler_tasks:
                await asyncio.gather(*self._handler_tasks, return_exceptions=True)
            self._request_handler_tails.clear()

    @staticmethod
    def _request_handler_key(message: dict) -> tuple[str, str] | None:
        """Return the websocket request identity that requires FIFO delivery."""
        ws_id = message.get("ws_id")
        req_id = message.get("req_id")
        if not isinstance(ws_id, str) or not ws_id:
            return None
        if not isinstance(req_id, str) or not req_id:
            return None
        return ws_id, req_id

    async def _schedule_handler_callback(
        self,
        callback: Callable[[dict], Any],
        message: dict,
        channel: str,
    ) -> asyncio.Task[None]:
        """Schedule one bounded callback after its request predecessor."""
        await self._handler_semaphore.acquire()
        request_key = self._request_handler_key(message)
        predecessor = (
            self._request_handler_tails.get(request_key) if request_key else None
        )
        try:
            task = asyncio.create_task(
                self._guarded_callback(
                    callback,
                    message,
                    channel,
                    predecessor,
                )
            )
        except BaseException:
            self._handler_semaphore.release()
            raise

        self._handler_tasks.add(task)
        if request_key:
            self._request_handler_tails[request_key] = task
        task.add_done_callback(
            lambda completed, key=request_key: self._handler_callback_done(
                completed,
                key,
            )
        )
        return task

    def _handler_callback_done(
        self,
        task: asyncio.Task[None],
        request_key: tuple[str, str] | None,
    ) -> None:
        """Release completed task references without deleting a newer chain tail."""
        self._handler_tasks.discard(task)
        if request_key and self._request_handler_tails.get(request_key) is task:
            self._request_handler_tails.pop(request_key, None)

    async def _guarded_callback(
        self,
        callback: Callable[[dict], Any],
        d: dict,
        channel: str,
        predecessor: asyncio.Task[None] | None = None,
    ) -> None:
        """Run a callback after its request predecessor and release its slot."""
        try:
            if predecessor:
                await asyncio.gather(predecessor, return_exceptions=True)
            await callback(d)
        except Exception as ex:
            if GulpConfig.get_instance().prometheus_enabled():
                # update Prometheus counter for subscriber callback errors
                try:
                    GulpMetrics.redis_subscriber_errors_total.inc()
                except Exception:
                    pass
            Glug.get_instance().exception(
                f"ERROR in subscriber callback for channel {channel}: {ex}"
            )
        finally:
            self._handler_semaphore.release()

    def task_queue_key(self) -> str:
        """Return the Redis key used for the task queue."""
        return self._task_queue_key

    async def task_known_types(self) -> list[str]:
        """Return task types this instance is configured to consume."""
        if self._instance_roles:
            return sorted(self._instance_roles)
        types = await self._redis.smembers(GulpRedis.TASK_TYPES_SET)
        return sorted(t.decode() if isinstance(t, bytes) else t for t in types)

    async def _task_stream_keys(
        self,
        task_type: str | None = None,
        task_types: list[str] | None = None,
    ) -> list[str]:
        """Return the task stream keys relevant to this instance."""
        keys: list[str] = []
        try:
            known_types = await self.task_known_types()
            if task_types is not None:
                known_types = [known for known in known_types if known in task_types]
            if task_type is not None:
                known_types = [known for known in known_types if known == task_type]
            keys.extend(
                f"{GulpRedis.STREAM_TASK_PREFIX}:{known}" for known in known_types
            )
            batch_streams = await self._redis.smembers(GulpRedis.TASK_BATCH_STREAMS_SET)
            keys.extend(
                stream
                for raw_stream in batch_streams
                if (
                    stream := (
                        raw_stream.decode()
                        if isinstance(raw_stream, bytes)
                        else raw_stream
                    )
                )
                and self._task_type_from_stream(stream) in known_types
            )
        except Exception:
            Glug.get_instance().exception("failed to read task streams")

        if task_type:
            stream_key = f"{GulpRedis.STREAM_TASK_PREFIX}:{task_type}"
            if stream_key not in keys:
                keys.append(stream_key)
        return sorted(set(keys))

    async def task_queue_depth(self, task_type: str | None = None) -> int:
        """Return the total queued task depth across all known task streams."""
        depth = 0
        for stream_key in await self._task_stream_keys(task_type):
            try:
                depth += int(await self._redis.xlen(stream_key))
            except Exception:
                Glug.get_instance().exception(f"failed to read task queue depth for {stream_key}")
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
        }
        now_msec = int(time.time() * 1000)

        try:
            types = await self._redis.smembers(GulpRedis.TASK_TYPES_SET)
        except Exception:
            Glug.get_instance().exception("failed to read known task types for metrics")
            types = set()

        for t in types:
            task_type = t.decode() if isinstance(t, bytes) else str(t)
            metrics = {
                "queued": 0,
                "pending": 0,
                "oldest_queued_age_msec": 0,
                "oldest_pending_age_msec": 0,
            }
            for stream_key in await self._task_stream_keys(task_type):
                try:
                    queued = int(await self._redis.xlen(stream_key))
                    metrics["queued"] += queued
                    if queued > 0:
                        oldest = await self._redis.xrange(
                            stream_key,
                            min="-",
                            max="+",
                            count=1,
                        )
                        if oldest:
                            age = self._redis_stream_age_msec(oldest[0][0], now_msec)
                            metrics["oldest_queued_age_msec"] = max(
                                metrics["oldest_queued_age_msec"], age
                            )
                except Exception:
                    Glug.get_instance().exception(
                        f"failed to collect task stream depth for {stream_key}"
                    )
                try:
                    pending_info = await self._redis.xpending(
                        stream_key,
                        GulpRedis.STREAM_CONSUMER_GROUP,
                    )
                    if isinstance(pending_info, dict):
                        pending = int(pending_info.get("pending", 0) or 0)
                        metrics["pending"] += pending
                        if pending > 0:
                            age = self._redis_stream_age_msec(
                                pending_info.get("min"), now_msec
                            )
                            metrics["oldest_pending_age_msec"] = max(
                                metrics["oldest_pending_age_msec"], age
                            )
                except ResponseError as ex:
                    if "NOGROUP" not in str(ex):
                        Glug.get_instance().exception(
                            f"failed to collect task pending depth for {stream_key}"
                        )
                except Exception:
                    Glug.get_instance().exception(
                        f"failed to collect task pending depth for {stream_key}"
                    )
            snapshot["task_types"][task_type] = metrics

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
        suffix = str(stream_name).removeprefix(f"{GulpRedis.STREAM_TASK_PREFIX}:")
        return suffix.split(":", 1)[0] or "unknown"

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
            Glug.get_instance().exception(
                f"failed to record task transition metric action={action} "
                f"task_type={task_type} outcome={outcome}"
            )

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

    async def task_mark_canceled(
        self,
        task: dict,
        reason: str = "request canceled",
    ) -> None:
        """Record a cancellation transition; PostgreSQL owns cancel state."""
        self._record_task_transition(
            "cancel",
            task.get("task_type"),
            "canceled",
        )

    async def _ack_and_delete_task(self, stream_name: str, msg_id: str) -> None:
        """Acknowledge and delete one task stream entry."""
        try:
            pipe = self._redis.pipeline(transaction=False)
            pipe.xack(stream_name, GulpRedis.STREAM_CONSUMER_GROUP, msg_id)
            pipe.xdel(stream_name, msg_id)
            await pipe.execute()
            await self._remove_empty_batch_stream(stream_name)
            self._record_task_transition(
                "ack_delete",
                self._task_type_from_stream(stream_name),
                "success",
            )
        except ResponseError as ex:
            if "NOGROUP" in str(ex):
                try:
                    await self._redis.xdel(stream_name, msg_id)
                    await self._remove_empty_batch_stream(stream_name)
                    self._record_task_transition(
                        "ack_delete",
                        self._task_type_from_stream(stream_name),
                        "nogroup_xdel",
                    )
                except Exception:
                    Glug.get_instance().exception(
                        f"failed to xdel {stream_name} {msg_id} after missing consumer group"
                    )
                    self._record_task_transition(
                        "ack_delete",
                        self._task_type_from_stream(stream_name),
                        "nogroup_xdel_error",
                    )
            else:
                Glug.get_instance().exception(
                    f"failed to ack/delete task {stream_name} {msg_id}: {ex}"
                )
                self._record_task_transition(
                    "ack_delete",
                    self._task_type_from_stream(stream_name),
                    "error",
                )
        except Exception as ex:
            Glug.get_instance().exception(f"failed to ack/delete task {stream_name} {msg_id}: {ex}")
            self._record_task_transition(
                "ack_delete",
                self._task_type_from_stream(stream_name),
                "error",
            )

    async def _remove_empty_batch_stream(self, stream_name: str) -> None:
        """Atomically unregister a drained request stream."""
        if stream_name.count(":") <= GulpRedis.STREAM_TASK_PREFIX.count(":") + 1:
            return
        await self._redis.eval(
            "if redis.call('XLEN', KEYS[1]) == 0 then "
            "redis.call('SREM', KEYS[2], KEYS[1]); "
            "return redis.call('DEL', KEYS[1]) end return 0",
            2,
            stream_name,
            GulpRedis.TASK_BATCH_STREAMS_SET,
        )

    async def task_ack_delete(self, task: dict) -> None:
        """Acknowledge and delete a dequeued task using its Redis envelope metadata."""
        stream_name = task.get("__redis_stream__")
        msg_id = task.get("__redis_message_id__")
        if not stream_name or not msg_id:
            Glug.get_instance().warning(
                "task_ack_delete called without Redis envelope metadata: "
                f"task_type={task.get('task_type')} req_id={task.get('req_id')}"
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
        task must periodically reset that idle timer so stale cleanup does not
        drop it while it is still alive.
        """
        stream_name = task.get("__redis_stream__")
        msg_id = task.get("__redis_message_id__")
        consumer_name = task.get("__redis_consumer_name__") or self.server_id
        if not stream_name or not msg_id:
            Glug.get_instance().warning(
                "task_refresh_lease called without Redis envelope metadata: "
                f"task_type={task.get('task_type')} req_id={task.get('req_id')}"
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
                Glug.get_instance().warning(
                    f"task lease refresh found no pending entry: stream={stream_name} "
                    f"msg_id={msg_id} task_type={task.get('task_type')} "
                    f"req_id={task.get('req_id')}"
                )
                return False
            return True
        except ResponseError as ex:
            if "NOGROUP" not in str(ex):
                Glug.get_instance().exception(
                    f"failed to refresh task lease for {stream_name} {msg_id}: {ex}"
                )
            return False
        except Exception as ex:
            Glug.get_instance().exception(
                f"failed to refresh task lease for {stream_name} {msg_id}: {ex}"
            )
            return False

    async def _drop_task(
        self,
        stream_name: str,
        msg_id: str,
        reason: str,
    ) -> None:
        """Drop an invalid queued task entry."""
        if isinstance(stream_name, bytes):
            stream_name = stream_name.decode()
        if isinstance(msg_id, bytes):
            msg_id = msg_id.decode()
        Glug.get_instance().warning(f"dropping queued task {stream_name} {msg_id}: {reason}")
        self._record_task_transition(
            "drop",
            self._task_type_from_stream(stream_name),
            "invalid",
        )
        await self._ack_and_delete_task(stream_name, msg_id)

    async def task_mark_failed(self, task: dict, reason: str) -> str:
        """
        Drop a failed task stream entry.

        Returns:
            str: "failed" when finalized or "ignored" when metadata is missing.
        """
        stream_name = task.get("__redis_stream__")
        msg_id = task.get("__redis_message_id__")
        if not stream_name or not msg_id:
            Glug.get_instance().warning(
                "task_mark_failed called without Redis envelope metadata: "
                f"task_type={task.get('task_type')} req_id={task.get('req_id')}"
            )
            self._record_task_transition(
                "failure",
                task.get("task_type"),
                "missing_metadata",
            )
            return "ignored"
        await self.task_ack_delete(task)
        self._record_task_transition(
            "failure",
            task.get("task_type"),
            "failed",
        )
        return "failed"

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
            Glug.get_instance().error("task missing 'task_type', not enqueued", detail=task)
            self._record_task_transition("enqueue", "unknown", "missing_task_type")
            return

        # stream key per task type
        stream_key = f"{GulpRedis.STREAM_TASK_PREFIX}:{ttype}"
        try:
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
                    )
            # add to stream under field 'data'
            await self._redis.xadd(stream_key, {"data": payload})
            # record known task type
            await self._redis.sadd(GulpRedis.TASK_TYPES_SET, ttype)
            self._record_task_transition("enqueue", ttype, "queued")
        except TaskQueueFullError:
            raise
        except Exception:
            # best-effort: log and continue
            Glug.get_instance().exception(
                f"failed to push task into role-specific stream '{stream_key}'"
            )
            self._record_task_transition("enqueue", ttype, "error")

        Glug.get_instance().debug(
            f"enqueued task_type={ttype} on {stream_key}", detail=task
        )

    async def task_enqueue_many(self, tasks: list[dict]) -> None:
        """Atomically enqueue one request with a stable-stream wake-up task."""
        if not tasks:
            return

        task_types = {task.get("task_type") for task in tasks}
        if None in task_types or len(task_types) != 1:
            raise ValueError("batched enqueue requires one non-empty task_type")

        task_type = task_types.pop()
        req_ids = {task.get("req_id") for task in tasks}
        if None in req_ids or len(req_ids) != 1:
            raise ValueError("batched enqueue requires one non-empty req_id")
        req_id = req_ids.pop()
        stable_stream = f"{GulpRedis.STREAM_TASK_PREFIX}:{task_type}"
        request_stream = (
            f"{GulpRedis.STREAM_TASK_PREFIX}:{task_type}:"
            f"{hashlib.sha256(str(req_id).encode()).hexdigest()}"
            if len(tasks) > 1
            else None
        )
        queue_depth = await self.task_queue_depth(task_type)
        if (
            GulpRedis.STREAM_TASK_MAXLEN > 0
            and queue_depth + len(tasks) > GulpRedis.STREAM_TASK_MAXLEN
        ):
            self._record_task_transition("enqueue", task_type, "queue_full")
            raise TaskQueueFullError(
                task_type,
                queue_depth,
                GulpRedis.STREAM_TASK_MAXLEN,
                retry_after_msec=self._task_admission_retry_after_msec(),
                work_units=len(tasks),
            )

        pipe = self._redis.pipeline(transaction=True)
        # The dispatcher already blocks on the stable type stream. Publishing
        # the first task there wakes it without waiting for another discovery
        # cycle; only the remaining work needs a request-scoped rotating stream.
        pipe.xadd(stable_stream, {"data": orjson.dumps(tasks[0])})
        if request_stream:
            for task in tasks[1:]:
                pipe.xadd(request_stream, {"data": orjson.dumps(task)})
        pipe.sadd(GulpRedis.TASK_TYPES_SET, task_type)
        if request_stream:
            pipe.sadd(GulpRedis.TASK_BATCH_STREAMS_SET, request_stream)
        try:
            await pipe.execute()
        except Exception:
            Glug.get_instance().exception(
                f"failed to enqueue {len(tasks)} task_type={task_type} messages"
            )
            self._record_task_transition("enqueue", task_type, "error")
            raise

        for _ in tasks:
            self._record_task_transition("enqueue", task_type, "queued")
        Glug.get_instance().debug(
            f"enqueued {len(tasks)} task_type={task_type} messages with stable wake "
            f"stream={stable_stream} request_stream={request_stream}"
        )

    async def task_dequeue_batch(
        self,
        max_items: int,
        task_types: list[str] | None = None,
    ) -> list[dict]:
        """
        Read up to max_items tasks from the Redis stream queue.

        Args:
            max_items (int): Maximum number of tasks to pop.
            task_types: Optional task types to read.

        Returns:
            list[dict]: Decoded task dicts.
        """
        if max_items <= 0:
            return []

        items: list[dict] = []

        # determine stream keys to poll (per-type streams)
        keys: list[str] = await self._task_stream_keys(task_types=task_types)

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

        start = self._task_stream_cursor % len(keys)
        ordered_keys = keys[start:] + keys[:start]
        self._task_stream_cursor = (start + 1) % len(keys)

        # Redis applies COUNT to each stream, not to the combined response. Read
        # streams separately so this method never claims more than max_items.
        for index, stream in enumerate(ordered_keys):
            remaining = max_items - len(items)
            if remaining <= 0:
                break
            streams_left = len(ordered_keys) - index
            count = max(1, (remaining + streams_left - 1) // streams_left)
            try:
                entries = await self._redis.xreadgroup(
                    GulpRedis.STREAM_CONSUMER_GROUP,
                    self.server_id,
                    streams={stream: ">"},
                    count=count,
                    block=None,
                    noack=False,
                )
            except Exception as ex:
                if isinstance(ex, ResponseError) and "NOGROUP" in str(ex):
                    continue
                Glug.get_instance().exception(f"error reading from stream {stream}: {ex}")
                continue
            items.extend(await self._decode_task_entries(entries))

        return items

    async def _decode_task_entries(self, entries: list) -> list[dict]:
        """Decode claimed Redis stream entries and drop malformed payloads."""
        items: list[dict] = []
        for stream_name, msgs in entries:
            for msg_id, fields in msgs:
                raw = fields.get(b"data", fields.get("data"))
                if raw is None:
                    await self._drop_task(stream_name, msg_id, "missing data field")
                    continue
                try:
                    task = orjson.loads(raw)
                    if not isinstance(task, dict):
                        raise TypeError("task payload must decode to dict")
                    items.append(
                        self._task_envelope(task, stream_name, msg_id, self.server_id)
                    )
                except Exception as ex:
                    Glug.get_instance().exception(
                        f"invalid task payload in stream {stream_name}: {ex}"
                    )
                    await self._drop_task(
                        stream_name, msg_id, f"invalid task payload: {ex}"
                    )
        return items

    async def task_pop_blocking(
        self,
        timeout: int = 5,
        task_types: list[str] | None = None,
    ) -> Optional[dict]:
        """
        Blocking read for a single task from the Redis stream queue.

        Args:
            timeout (int): Seconds to wait before returning None if queue is empty.
            task_types: Optional task types to read.

        Returns:
            Optional[dict]: Decoded task dict or None if timed out.
        """
        try:
            # build stream keys to read from
            keys: list[str] = await self._task_stream_keys(task_types=task_types)

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

            start = self._task_stream_cursor % len(keys)
            deadline = time.monotonic() + timeout if timeout > 0 else None
            index = start
            while deadline is None or time.monotonic() < deadline:
                stream = keys[index]
                index = (index + 1) % len(keys)
                self._task_stream_cursor = index
                remaining = (
                    0.25
                    if deadline is None
                    else min(0.25, max(0.001, deadline - time.monotonic()))
                )
                res = await self._redis.xreadgroup(
                    GulpRedis.STREAM_CONSUMER_GROUP,
                    self.server_id,
                    streams={stream: ">"},
                    count=1,
                    block=max(1, int(remaining * 1000)),
                    noack=False,
                )
                items = await self._decode_task_entries(res)
                if items:
                    return items[0]
            return None
        except asyncio.CancelledError:
            raise
        except Exception as ex:
            if isinstance(ex, ResponseError) and "NOGROUP" in str(ex):
                return None
            Glug.get_instance().exception(f"error in task_pop_blocking: {ex}")
            await asyncio.sleep(0.5)
            return None

    async def task_drop_stale_pending(self) -> list[dict]:
        """
        Drop stale pending stream tasks whose consumer stopped refreshing them.

        Returns:
            list[dict]: Task envelopes that were dropped as abandoned.
        """
        dropped: list[dict] = []
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
                Glug.get_instance().exception(
                    f"error cleaning stale pending tasks from {stream}: {ex}"
                )
                continue
            except Exception as ex:
                Glug.get_instance().exception(
                    f"error cleaning stale pending tasks from {stream}: {ex}"
                )
                continue

            for msg_id, fields in claimed_msgs:
                raw = (
                    fields.get(b"data")
                    if isinstance(fields, dict)
                    else fields.get("data")
                )
                if raw is None:
                    await self._drop_task(
                        stream,
                        msg_id,
                        "missing data field in stale pending message",
                    )
                    continue
                try:
                    task = orjson.loads(raw)
                    if not isinstance(task, dict):
                        raise TypeError("task payload must decode to dict")
                    envelope = self._task_envelope(task, stream, msg_id, self.server_id)
                    await self.task_ack_delete(envelope)
                    dropped.append(envelope)
                except Exception as ex:
                    Glug.get_instance().exception(
                        f"invalid task payload in stale pending message {stream}/{msg_id}: {ex}"
                    )
                    await self._drop_task(
                        stream,
                        msg_id,
                        f"invalid task payload in stale pending message: {ex}",
                    )

            if claimed_msgs:
                task_type = self._task_type_from_stream(stream)
                self._record_task_transition(
                    "stale_pending",
                    task_type,
                    "dropped",
                    amount=len(claimed_msgs),
                )
                Glug.get_instance().warning(
                    f"dropped {len(claimed_msgs)} stale pending task(s) from {stream}"
                )

        return dropped

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
        # purge matching tasks from every known task stream
        removed_total: int = 0
        for stream in await self._task_stream_keys():
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
                    await self._remove_empty_batch_stream(stream)
                    Glug.get_instance().debug(f"purged {removed} task(s) from {stream}")
                    removed_total += removed
                except Exception:
                    Glug.get_instance().exception(f"failed to xdel ids on {stream}")

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
                        Glug.get_instance().exception(
                            "failed to delete key batch during cleanup"
                        )
                if cursor == 0:
                    break
        except Exception:
            Glug.get_instance().exception("error during cleanup_redis scan/delete")

        Glug.get_instance().info(f"cleanup_redis removed {total_deleted} keys")
        return total_deleted

    async def cleanup_consumers_for_server(self, server_id: str) -> None:
        """
        Remove this server's empty consumers from task stream groups.

        Consumers with pending entries stay intact so stale-task recovery can
        claim their messages after the lease expires.

        Args:
            server_id (str): The server instance ID whose consumers should be removed.
        """
        for stream in await self._task_stream_keys():
            try:
                # Inspect pending state before deleting; DELCONSUMER would strand
                # messages still owned by this consumer.
                try:
                    consumers_info = await self._redis.xinfo_consumers(
                        stream, GulpRedis.STREAM_CONSUMER_GROUP
                    )
                except Exception:
                    # Keep the consumer when Redis cannot prove it is empty.
                    consumers_info = []

                for consumer in consumers_info:
                    name = consumer.get("name", consumer.get(b"name"))
                    name = name.decode() if isinstance(name, bytes) else name
                    if name != server_id:
                        continue
                    pending = consumer.get("pending", consumer.get(b"pending", 0))
                    if int(pending or 0) > 0:
                        Glug.get_instance().warning(
                            f"retaining task consumer {server_id} on {stream} "
                            f"with {pending} pending entries"
                        )
                        break
                    await self._redis.xgroup_delconsumer(
                        stream, GulpRedis.STREAM_CONSUMER_GROUP, server_id
                    )
                    break

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
                Glug.get_instance().exception(f"error cleaning consumer for stream {stream}")

    async def cleanup_dead_task_consumers(self) -> int:
        """Remove zero-pending task consumers whose server heartbeat expired."""
        removed = 0
        for stream in await self._task_stream_keys():
            try:
                consumers = await self._redis.xinfo_consumers(
                    stream, GulpRedis.STREAM_CONSUMER_GROUP
                )
            except Exception:
                continue

            for consumer in consumers:
                name = consumer.get("name", consumer.get(b"name"))
                name = name.decode() if isinstance(name, bytes) else name
                pending = consumer.get("pending", consumer.get(b"pending", 0))
                if (
                    not name
                    or int(pending or 0) > 0
                    or await self.is_server_alive(name)
                ):
                    continue
                try:
                    await self._redis.xgroup_delconsumer(
                        stream, GulpRedis.STREAM_CONSUMER_GROUP, name
                    )
                    removed += 1
                except Exception:
                    Glug.get_instance().exception(
                        f"failed to remove dead task consumer {name} from {stream}"
                    )
        return removed
