"""
This module provides a singleton class `GulpRedis` for managing asynchronous Redis operations, including WebSocket metadata storage, pub/sub messaging, and a task queue.
"""

import asyncio
from typing import Annotated, Any, Callable, Optional
from urllib.parse import urlparse

import orjson
import redis.asyncio as redis
from muty.log import MutyLogger
from pydantic import BaseModel, Field
from redis.asyncio.client import PubSub
import muty.string
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
            description="List of GulpWsData.type this websocket is interested in, default is an empty list(all)"
        ),
    ] = []
    operation_ids: Annotated[
        list[str],
        Field(
            description="List of `operation_id` this websocket is interested in, default is an empty list(all)"
        ),
    ] = []
    socket_type: Annotated[
        str, Field(description="The socket type (default, ingest, client_data)")
    ]


class GulpRedis:
    """
    Singleton class to manage Redis connections and operations.
    """

    _instance: "GulpRedis" = None
    CHANNEL = "gulpredis"

    def __init__(self):
        self._redis: redis.Redis = None
        self._pubsub: PubSub = None
        self._subscriber_task: asyncio.Task = None
        self.server_id: str = None
        # simple task queue (list) key
        self._task_queue_key: str = "gulp:queue:tasks"

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

    def initialize(self, server_id: str, main_process: bool = True) -> None:
        """
        Initialize Redis pub/sub connections.

        Args:
            server_id (str): The unique server instance ID
        """
        self.server_id = server_id

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
        metadata_key = self._get_ws_metadata_key(ws_id)
        lookup_key = self._get_ws_lookup_key(ws_id)

        deleted = await self._redis.delete(metadata_key, lookup_key)

        MutyLogger.get_instance().debug(
            "unregistered websocket: ws_id=%s, deleted=%d",
            ws_id,
            deleted,
        )
        return deleted > 0

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
        payload = orjson.dumps(message)
        await self._redis.publish(GulpRedis.CHANNEL, payload)

    async def unsubscribe(self) -> None:
        """
        unsubscribe from the redis pubsub channel
        """
        if self._subscriber_task:
            await self._pubsub.unsubscribe(GulpRedis.CHANNEL)
            MutyLogger.get_instance().info(
                "unsubscribed from channel: %s, server_id=%s",
                GulpRedis.CHANNEL,
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
        await self._pubsub.subscribe(GulpRedis.CHANNEL)

        task = asyncio.create_task(
            self._subscriber_loop(GulpRedis.CHANNEL, callback),
            name=f"gulpredis-sub-{self.server_id}",
        )
        self._subscriber_task = task

        MutyLogger.get_instance().info(
            "subscribed to channel: %s, server_id=%s", GulpRedis.CHANNEL, self.server_id
        )

    async def _subscriber_loop(
        self,
        channel: str,
        callback: Callable[[dict], Any],
    ) -> None:
        """
        Internal loop for processing Redis pub/sub messages.

        Args:
            channel (str): The channel name
            callback: Function to call with each message
        """
        MutyLogger.get_instance().debug(
            "starting subscriber loop for channel: %s", channel
        )

        try:
            while True:
                try:
                    # get message and call callback
                    message: dict = await self._pubsub.get_message(
                        ignore_subscribe_messages=True, timeout=1.0
                    )
                    if message:
                        MutyLogger.get_instance().debug(
                            "---> received message on channel %s: %s",
                            channel,
                            muty.string.make_shorter(str(message), max_len=260),
                        )
                        d: dict = orjson.loads(message["data"])
                        await callback(d)

                    # yield control
                    await asyncio.sleep(0.001)

                except asyncio.CancelledError:
                    MutyLogger.get_instance().debug(
                        "subscriber loop cancelled for %s", channel
                    )
                    raise
                except Exception as ex:
                    MutyLogger.get_instance().error(
                        "error in subscriber loop for %s: %s", channel, ex
                    )
                    await asyncio.sleep(1.0)

        except asyncio.CancelledError:
            MutyLogger.get_instance().info(
                "subscriber loop stopped for channel: %s", channel
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
        await self._redis.rpush(self._task_queue_key, payload)
        MutyLogger.get_instance().debug(
            "enqueued task_type=%s on %s", task.get("task_type"), self._task_queue_key
        )

    async def task_dequeue_batch(self, max_items: int) -> list[dict]:
        """
        Pop up to max_items tasks from the Redis list queue.

        Args:
            max_items (int): Maximum number of tasks to pop.

        Returns:
            list[dict]: Decoded task dicts.
        """
        items: list[dict] = []
        # non-blocking LPOP up to max_items
        for _ in range(max_items):
            data: bytes | None = await self._redis.lpop(self._task_queue_key)
            if not data:
                break
            try:
                items.append(orjson.loads(data))
            except Exception as ex:
                MutyLogger.get_instance().error(
                    "invalid task payload, skipping: %s", ex
                )
        if items:
            MutyLogger.get_instance().debug(
                "dequeued %d task(s) from %s", len(items), self._task_queue_key
            )
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
            res = await self._redis.blpop(self._task_queue_key, timeout=timeout)
            if not res:
                return None
            # res is (key, value)
            _, raw = res
            try:
                d = orjson.loads(raw)
                return d
            except Exception as ex:
                MutyLogger.get_instance().error(
                    "invalid task payload on BLPOP, skipping: %s", ex
                )
                return None
        except asyncio.CancelledError:
            # propagate cancellations cleanly
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
        # fetch all current items
        all_items: list[bytes] = await self._redis.lrange(self._task_queue_key, 0, -1)
        if not all_items:
            return 0

        keep_raw: list[bytes] = []
        removed: int = 0
        for raw in all_items:
            try:
                d = orjson.loads(raw)
            except Exception:
                # keep malformed entries to avoid accidental data loss
                keep_raw.append(raw)
                continue

            cond_ok = True
            if operation_id is not None and d.get("operation_id") != operation_id:
                cond_ok = False
            if req_id is not None and d.get("req_id") != req_id:
                cond_ok = False

            if cond_ok:
                # mark for removal by not keeping it
                removed += 1
            else:
                keep_raw.append(raw)

        if removed == 0:
            return 0

        # rebuild the list with items to keep
        pipe = self._redis.pipeline()
        pipe.delete(self._task_queue_key)
        if keep_raw:
            pipe.rpush(self._task_queue_key, *keep_raw)
        await pipe.execute()
        MutyLogger.get_instance().debug(
            "purged %d task(s) from %s", removed, self._task_queue_key
        )
        return removed
