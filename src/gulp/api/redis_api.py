"""
This module provides a singleton class `GulpRedis` for managing asynchronous Redis operations, including WebSocket metadata storage, pub/sub messaging, and a task queue.
"""

import asyncio
import time
import zlib
from collections import deque
from typing import Annotated, Any, Callable, Optional
from urllib.parse import urlparse

import muty.string
import orjson
import redis.asyncio as redis
from muty.log import MutyLogger
from pydantic import BaseModel, Field
from redis.asyncio.client import PubSub
from redis.exceptions import ConnectionError as RedisConnectionError

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


class GulpRedis:
    """
    Singleton class to manage Redis connections and operations.
    """

    _instance: "GulpRedis" = None
    CHANNEL = "gulpredis"
    # dedicated channel for client_data cross-instance routing
    CLIENT_DATA_CHANNEL = "gulpredis:client_data"
    # redis set key to track known task types (members are task_type strings)
    TASK_TYPES_SET = "gulp:queue:types"
    # redis stream key prefix for task streams (one stream per task_type)
    STREAM_TASK_PREFIX = "gulp:stream:tasks"
    # consumer group name for task streams
    STREAM_CONSUMER_GROUP = "gulp:stream:group:tasks"

    def __init__(self):
        self._redis: redis.Redis = None
        self._pubsub: PubSub = None
        self._subscriber_task: asyncio.Task = None
        self.server_id: str = None
        # list of roles assigned to this server instance (e.g. ['ingest', 'query'])
        self._instance_roles: list[str] = []
        # simple task queue (list) key
        self._task_queue_key: str = "gulp:queue:tasks"
        # ttl for stored large payloads (seconds)
        self.PUBLISH_LARGE_PAYLOAD_TTL: int = 5 * 60  # 5 min, it should be consumed quickly
        # sliding window for publish rate limiting
        self._publish_times: deque[float] = deque()
        self._publish_rate_limit: int = 100  # msgs per second

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

        # create redis client
        url: str = GulpConfig.get_instance().redis_url()
        self._redis = redis.Redis.from_url(url, decode_responses=False)
        url_parts = urlparse(url)
        MutyLogger.get_instance().info(
            "client %s connected to Redis at %s:%d",
            self._redis,
            url_parts.hostname,
            url_parts.port,
        )

        if main_process:
            # create pub/sub connection
            self._pubsub = self._redis.pubsub()

            MutyLogger.get_instance().info(
                "initialized Redis pub/sub for server_id=%s", server_id
            )

    async def shutdown(self, main_process: bool = True) -> None:
        """
        close Redis connections and cleanup.
        """
        if main_process:
            # cancel all subscriber tasks
            try:
                if self._subscriber_task:
                    # only main process have subscriber task
                    self._subscriber_task.cancel()
                    await asyncio.wait_for(self._subscriber_task, timeout=5.0)
                    MutyLogger.get_instance().debug("cancelled subscriber task!")
            except asyncio.TimeoutError:
                MutyLogger.get_instance().warning("timeout cancelling subscriber task!")
            except Exception as ex:
                MutyLogger.get_instance().exception("error cancelling subscriber task!")

            # close pubsub
            try:
                await self._pubsub.close()
                MutyLogger.get_instance().debug("pubsub closed!")
            except Exception as ex:
                MutyLogger.get_instance().exception("error closing pubsub!")

        # close redis client
        try:
            await self._redis.close()
            MutyLogger.get_instance().info(
                "Redis client %s connection closed", self._redis
            )
        except Exception as ex:
            MutyLogger.get_instance().exception("error closing redis client!")

    def _get_ws_metadata_key(self, ws_id: str) -> str:
        """Get Redis key for websocket metadata."""
        return f"gulp:ws:metadata:{self.server_id}:{ws_id}"

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
        )

        # store ws_id -> server_id lookup mapping
        lookup_key = self._get_ws_lookup_key(ws_id)
        await self._redis.set(lookup_key, self.server_id.encode())

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
        lookup_key = self._get_ws_lookup_key(ws_id)

        deleted_total = 0
        try:
            # delete any metadata keys for this ws_id across all server instances
            pattern = f"gulp:ws:metadata:*:{ws_id}"
            cursor = 0
            keys_to_delete: list[str] = []
            while True:
                cursor, keys = await self._redis.scan(cursor=cursor, match=pattern, count=200)
                if keys:
                    keys_to_delete.extend(keys)
                if cursor == 0:
                    break

            if keys_to_delete:
                try:
                    deleted = await self._redis.delete(*keys_to_delete)
                    if isinstance(deleted, int):
                        deleted_total += deleted
                except Exception:
                    MutyLogger.get_instance().exception("failed to delete ws metadata keys")

            # delete the lookup mapping ws_id -> server_id
            try:
                deleted = await self._redis.delete(lookup_key)
                if isinstance(deleted, int):
                    deleted_total += deleted
            except Exception:
                MutyLogger.get_instance().exception("failed to delete ws lookup key")

        except Exception:
            MutyLogger.get_instance().exception("error during ws_unregister scan/delete")

        MutyLogger.get_instance().debug(
            "unregistered websocket: ws_id=%s, deleted=%d",
            ws_id,
            deleted_total,
        )
        return deleted_total > 0

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
        metadata_key = self._get_ws_metadata_key(ws_id)
        data = await self._redis.get(metadata_key)

        if data:
            return GulpWsMetadata.model_validate(orjson.loads(data))
        return None

    async def publish(self, message: dict) -> None:
        """
        Publish a message on redis pubsub.

        Args:
            message (dict): The message to publish (must be a GulpWsData dict)
        """
        # serialize message
        payload = orjson.dumps(message)
        compression_threshold: int = GulpConfig.get_instance().redis_compression_threshold() * 1024
        pubsub_max_chunk_size: int = GulpConfig.get_instance().redis_pubsub_max_chunk_size() * 1024

        await self._apply_publish_backpressure()

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
                        MutyLogger.get_instance().exception("failed to compress large payload, falling back to direct publish")
                        await self._redis.publish(GulpRedis.CHANNEL, payload)
                        return
                else:
                    # store uncompressed bytes
                    stored_bytes = payload
                    compressed_flag = False

                # split bytes into chunks
                chunks = [stored_bytes[i : i + pubsub_max_chunk_size] for i in range(0, len(stored_bytes), pubsub_max_chunk_size)]
                num_chunks = len(chunks)

                # include server_id in base key to prevent cross-node chunk retrieval
                base_key = f"gulp:pub:payload:{self.server_id}:{muty.string.generate_unique()}"
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

                # prepare lua script to atomically set all chunk keys with TTL and publish the pointer
                # ARGV layout: chunk1..chunkN, pointer_json, ttl_seconds, channel
                lua_lines = [
                    "local n = #KEYS\n",
                    "local ttl = tonumber(ARGV[n+2])\n",
                    "for i=1,n do\n",
                    "  redis.call('SET', KEYS[i], ARGV[i], 'EX', ttl)\n",
                    "end\n",
                    "redis.call('PUBLISH', ARGV[n+3], ARGV[n+1])\n",
                    "return 1\n",
                ]
                lua = "".join(lua_lines)
                try:
                    # build args: chunk bytes..., pointer_bytes, ttl, channel
                    args = []
                    for c in chunks:
                        args.append(c)
                    args.append(pointer_bytes)
                    args.append(str(self.PUBLISH_LARGE_PAYLOAD_TTL))
                    args.append(GulpRedis.CHANNEL)

                    # execute lua atomically
                    await self._redis.eval(lua, len(chunk_keys), *chunk_keys, *args)
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
                        "failed to atomically store/publish chunked payload, falling back to per-key store"
                    )
                    # fallback: attempt per-key store then publish pointer (best effort)
                    try:
                        for i, c in enumerate(chunks):
                            await self._redis.set(chunk_keys[i], c, ex=self.PUBLISH_LARGE_PAYLOAD_TTL)
                        await self._redis.publish(GulpRedis.CHANNEL, pointer_bytes)
                        MutyLogger.get_instance().warning(
                            "published large %s message stored (fallback) in %d chunks base=%s",
                            "compressed" if compressed_flag else "uncompressed",
                            num_chunks,
                            base_key,
                        )
                        return
                    except Exception:
                        MutyLogger.get_instance().exception(
                            "failed to store chunked payload in Redis, falling back to direct publish"
                        )

        except Exception:
            MutyLogger.get_instance().exception("error handling large publish payload")

        # default: publish inline
        await self._redis.publish(GulpRedis.CHANNEL, payload)

    async def _apply_publish_backpressure(self) -> None:
        """Simple sliding-window rate limiter to avoid flooding Redis pubsub."""
        now = time.monotonic()
        self._publish_times.append(now)
        cutoff = now - 1.0
        while self._publish_times and self._publish_times[0] < cutoff:
            self._publish_times.popleft()

        if len(self._publish_times) > self._publish_rate_limit:
            # sleep just enough to drop under limit
            await asyncio.sleep(0.01)

    async def unsubscribe(self) -> None:
        """
        unsubscribe from the redis pubsub channel
        """
        # cancel the subscriber task if present
        if self._subscriber_task:
            self._subscriber_task.cancel()
            try:
                await self._subscriber_task
            except asyncio.CancelledError:
                pass

        # unsubscribe from both channels
        try:
            await self._pubsub.unsubscribe(GulpRedis.CHANNEL, GulpRedis.CLIENT_DATA_CHANNEL)
        except Exception:
            # ignore errors during unsubscribe
            pass

        MutyLogger.get_instance().info(
            "unsubscribed from channels: %s, %s, server_id=%s",
            GulpRedis.CHANNEL,
            GulpRedis.CLIENT_DATA_CHANNEL,
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
        # subscribe to both channels using a single pubsub subscription
        await self._pubsub.subscribe(GulpRedis.CHANNEL, GulpRedis.CLIENT_DATA_CHANNEL)

        task = asyncio.create_task(
            self._subscriber_loop("multiple", callback),
            name=f"gulpredis-sub-{self.server_id}",
        )
        self._subscriber_task = task

        MutyLogger.get_instance().info(
            "subscribed to channels: %s, %s, server_id=%s",
            GulpRedis.CHANNEL,
            GulpRedis.CLIENT_DATA_CHANNEL,
            self.server_id,
        )

    async def _recreate_pubsub(self) -> None:
        """Recreate pubsub connection and resubscribe after errors."""
        try:
            if self._pubsub:
                try:
                    await self._pubsub.close()
                except Exception:
                    pass
        except Exception:
            MutyLogger.get_instance().exception("error closing pubsub before recreate")

        self._pubsub = self._redis.pubsub()
        await self._pubsub.subscribe(GulpRedis.CHANNEL, GulpRedis.CLIENT_DATA_CHANNEL)

        MutyLogger.get_instance().warning(
            "pubsub connection recreated and resubscribed: %s, %s", GulpRedis.CHANNEL, GulpRedis.CLIENT_DATA_CHANNEL
        )

    async def _subscriber_loop(
        self,
        channel_name: str,
        callback: Callable[[dict], Any],
    ) -> None:
        """
        Internal loop for processing Redis pub/sub messages from multiple channels.

        Args:
            channel_name (str): Descriptive name for logging (e.g., "multiple")
            callback: Function to call with each message
        """
        MutyLogger.get_instance().debug(
            "starting subscriber loop for channels: %s, %s",
            GulpRedis.CHANNEL,
            GulpRedis.CLIENT_DATA_CHANNEL,
        )

        try:
            backoff: float = 1.0
            while True:
                try:
                    message: dict = await self._pubsub.get_message(
                        ignore_subscribe_messages=True, timeout=1.0
                    )
                    if message:
                        actual_channel = message.get("channel")
                        if isinstance(actual_channel, bytes):
                            actual_channel = actual_channel.decode("utf-8")

                        try:
                            d: dict = orjson.loads(message["data"])
                            d["__redis_channel__"] = actual_channel
                            await callback(d)
                        except Exception as ex:
                            MutyLogger.get_instance().exception(
                                "ERROR in subscriber callback for channel %s: %s",
                                actual_channel,
                                ex,
                            )
                            raise ex

                    backoff = 1.0
                    await asyncio.sleep(0.05)

                except asyncio.CancelledError:
                    MutyLogger.get_instance().debug("subscriber loop cancelled")
                    raise
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
                GulpRedis.CHANNEL,
                GulpRedis.CLIENT_DATA_CHANNEL,
            )

    def task_queue_key(self) -> str:
        """Return the Redis key used for the task queue."""
        return self._task_queue_key

    async def task_enqueue(self, task: dict) -> None:
        """
        Enqueue a task onto the Redis list queue.

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
            return

        # stream key per task type
        stream_key = f"{GulpRedis.STREAM_TASK_PREFIX}:{ttype}"
        try:
            # add to stream under field 'data'
            await self._redis.xadd(stream_key, {"data": payload})
            # record known task type
            await self._redis.sadd(GulpRedis.TASK_TYPES_SET, ttype)
        except Exception:
            # best-effort: log and continue
            MutyLogger.get_instance().exception(
                "failed to push task into role-specific stream '%s'", stream_key
            )

        MutyLogger.get_instance().debug(
            "enqueued task_type=%s on %s, task=%s", ttype, stream_key, muty.string.make_shorter(str(task),max_len=260))

    async def task_dequeue_batch(self, max_items: int) -> list[dict]:
        """
        Pop up to max_items tasks from the Redis list queue.

        Args:
            max_items (int): Maximum number of tasks to pop.

        Returns:
            list[dict]: Decoded task dicts.
        """
        items: list[dict] = []

        # determine stream keys to poll (per-type streams)
        keys: list[str] = []
        if self._instance_roles:
            for r in self._instance_roles:
                keys.append(f"{GulpRedis.STREAM_TASK_PREFIX}:{r}")
        else:
            # get all known task types from redis set
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
        ids_to_del: dict[str, list[str]] = {}
        for stream_name, msgs in entries:
            for msg_id, fields in msgs:
                raw = fields.get(b"data") if isinstance(fields, dict) else fields.get("data")
                if raw is None:
                    continue
                try:
                    # raw may be bytes
                    if isinstance(raw, bytes):
                        d = orjson.loads(raw)
                    else:
                        d = orjson.loads(raw)
                    items.append(d)
                    ids_to_del.setdefault(stream_name, []).append(msg_id)
                except Exception as ex:
                    MutyLogger.get_instance().error("invalid task payload in stream %s: %s", stream_name, ex)

        # acknowledge and delete read entries to emulate destructive pop semantics
        # batch xack/xdel operations in a single pipeline to reduce round trips
        if ids_to_del:
            try:
                pipe = self._redis.pipeline(transaction=False)
                for stream_name, ids in ids_to_del.items():
                    try:
                        pipe.xack(stream_name, GulpRedis.STREAM_CONSUMER_GROUP, *ids)
                        pipe.xdel(stream_name, *ids)
                    except Exception:
                        # if building pipeline entries fails for this stream, log and continue
                        MutyLogger.get_instance().exception("failed to queue xack/xdel in pipeline for %s", stream_name)
                # execute all ack/del commands together
                await pipe.execute()
            except Exception:
                MutyLogger.get_instance().exception("failed to execute pipeline for xack/xdel")

        return items

    async def task_pop_blocking(self, timeout: int = 5) -> Optional[dict]:
        """
        Blocking pop for a single task from the Redis list queue.

        Args:
            timeout (int): Seconds to wait before returning None if queue is empty.

        Returns:
            Optional[dict]: Decoded task dict or None if timed out.
        """
        try:
            # build stream keys to read from
            keys: list[str] = []
            if self._instance_roles:
                for r in self._instance_roles:
                    keys.append(f"{GulpRedis.STREAM_TASK_PREFIX}:{r}")
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
                    raw = fields.get(b"data") if isinstance(fields, dict) else fields.get("data")
                    try:
                        d = orjson.loads(raw)
                        # acknowledge and delete entry to emulate destructive pop
                        try:
                            # batch ack + del in a small pipeline for this single message
                            pipe = self._redis.pipeline(transaction=False)
                            pipe.xack(stream_name, GulpRedis.STREAM_CONSUMER_GROUP, msg_id)
                            pipe.xdel(stream_name, msg_id)
                            await pipe.execute()
                        except Exception:
                            MutyLogger.get_instance().exception("failed to xack/xdel %s %s", stream_name, msg_id)

                        return d
                    except Exception as ex:
                        MutyLogger.get_instance().error("invalid task payload on XREADGROUP, skipping: %s", ex)
                        continue
            return None
        except asyncio.CancelledError:
            raise
        except Exception as ex:
            MutyLogger.get_instance().exception("error in task_pop_blocking: %s", ex)
            await asyncio.sleep(0.5)
            return None

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
                entries = await self._redis.xrange(stream, min='-', max='+')
            except Exception:
                entries = None
            if not entries:
                continue

            removed: int = 0
            ids_to_del: list[str] = []
            for msg_id, fields in entries:
                raw = fields.get(b"data") if isinstance(fields, dict) else fields.get("data")
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
                    ids_to_del.append(msg_id)
                    removed += 1

            if ids_to_del:
                try:
                    await self._redis.xdel(stream, *ids_to_del)
                    MutyLogger.get_instance().debug("purged %d task(s) from %s", removed, stream)
                    removed_total += removed
                except Exception:
                    MutyLogger.get_instance().exception("failed to xdel ids on %s", stream)

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
                cursor, keys = await self._redis.scan(cursor=cursor, match=pattern, count=200)
                if keys:
                    try:
                        # redis.delete accepts varargs of keys
                        deleted = await self._redis.delete(*keys)
                        if isinstance(deleted, int):
                            total_deleted += deleted
                    except Exception:
                        MutyLogger.get_instance().exception("failed to delete key batch during cleanup")
                if cursor == 0:
                    break
        except Exception:
            MutyLogger.get_instance().exception("error during cleanup_redis scan/delete")

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
                    await self._redis.xgroup_delconsumer(stream, GulpRedis.STREAM_CONSUMER_GROUP, server_id)
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
                        name = g.get(b'name') if isinstance(g, dict) and b'name' in g else g.get('name')
                        if name and (name.decode() if isinstance(name, bytes) else name) == GulpRedis.STREAM_CONSUMER_GROUP:
                            consumers = g.get(b'consumers') if isinstance(g, dict) and b'consumers' in g else g.get('consumers')
                            pending = g.get(b'pending') if isinstance(g, dict) and b'pending' in g else g.get('pending')
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
                                    await self._redis.xgroup_destroy(stream, GulpRedis.STREAM_CONSUMER_GROUP)
                                except Exception:
                                    pass
                            break
            except Exception:
                MutyLogger.get_instance().exception("error cleaning consumer for stream %s", stream)
