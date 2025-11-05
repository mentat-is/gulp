import asyncio
from typing import Annotated, Any, Callable, Optional
from urllib.parse import urlparse

import orjson
import redis.asyncio as redis
from fastapi import WebSocket, WebSocketDisconnect
from muty.log import MutyLogger
from pydantic import BaseModel, Field
from redis.asyncio.client import PubSub

from gulp.config import GulpConfig


class GulpWsMetadata(BaseModel):
    """
    Metadata for a WebSocket connection stored in Redis
    """
    ws_id: Annotated[str, Field(description="The websocket id")]
    server_id: Annotated[str, Field(description="The server instance id that owns this websocket")]
    types: Annotated[list[str], Field(description="List of GulpWsData.type this websocket is interested in, default is an empty list(all)")] = []
    operation_ids: Annotated[list[str], Field(description="List of `operation_id` this websocket is interested in, default is an empty list(all)")] = []
    socket_type: Annotated[str, Field(description="The socket type (default, ingest, client_data)")]


class GulpRedis:
    """
    Singleton class to manage Redis connections and operations.
    """

    _instance: "GulpRedis" = None

    def __init__(self):
        self._redis: redis.Redis = None
        self._pubsub: PubSub = None
        self._subscriber_tasks: dict[str, asyncio.Task] = {}
        self._server_id: str = None

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

    def initialize(self, server_id: str) -> None:
        """
        Initialize Redis pub/sub connections.
        
        Args:
            server_id (str): The unique server instance ID
        """
        self._server_id = server_id
        
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
        
        # create pub/sub connection
        self._pubsub = self._redis.pubsub()
        
        MutyLogger.get_instance().info(
            "initialized Redis pub/sub for server_id=%s", server_id
        )
    
    async def shutdown(self) -> None:
        """
        Close Redis connections and cleanup.
        """
        logger = MutyLogger.get_instance()
        
        # cancel all subscriber tasks
        for channel, task in list(self._subscriber_tasks.items()):
            try:
                task.cancel()
                await asyncio.wait_for(task, timeout=5.0)
                logger.debug("cancelled subscriber task for %s", channel)
            except asyncio.TimeoutError:
                logger.warning("timeout cancelling subscriber for %s", channel)
            except Exception as ex:
                logger.error("error cancelling subscriber for %s: %s", channel, ex)
        
        self._subscriber_tasks.clear()
        
        # close pubsub
        if self._pubsub:
            try:
                await self._pubsub.close()
                self._pubsub = None
            except Exception as ex:
                logger.error("error closing pubsub: %s", ex)
        
        # close redis client
        if self._redis:
            try:
                await self._redis.close()
                logger.info("Redis client %s connection closed", self._redis)
                self._redis = None
            except Exception as ex:
                logger.error("error closing redis client: %s", ex)
        
        self._initialized = False
    
    def _get_ws_metadata_key(self, ws_id: str) -> str:
        """Get Redis key for websocket metadata."""
        return f"gulp:ws:metadata:{self._server_id}:{ws_id}"
    
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
            server_id=self._server_id,
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
        
        # store lookup mapping
        lookup_key = self._get_ws_lookup_key(ws_id)
        await self._redis.set(lookup_key, self._server_id.encode())
        
        MutyLogger.get_instance().debug(
            "registered websocket: ws_id=%s, server_id=%s",
            ws_id,
            self._server_id,
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
    
    async def ws_get_server(self, ws_id: str) -> Optional[str]:
        """
        Get the server_id that owns a websocket.
        
        Args:
            ws_id (str): The websocket id
            
        Returns:
            Optional[str]: The server_id or None if not found
        """
        lookup_key = self._get_ws_lookup_key(ws_id)
        server_id = await self._redis.get(lookup_key)
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
        
        metadata_key = f"gulp:ws:metadata:{server_id}:{ws_id}"
        data = await self._redis.get(metadata_key)
        
        if data:
            return GulpWsMetadata.model_validate(orjson.loads(data))
        return None
    
    async def ws_keepalive(self, ws_id: str, ttl: int = 3600) -> bool:
        """
        Refresh TTL for a websocket registration.
        
        Args:
            ws_id (str): The websocket id
            ttl (int): Time to live in seconds
            
        Returns:
            bool: True if refreshed successfully
        """
        metadata_key = self._get_ws_metadata_key(ws_id)
        lookup_key = self._get_ws_lookup_key(ws_id)
        
        # refresh both keys
        await self._redis.expire(metadata_key, ttl)
        await self._redis.expire(lookup_key, ttl)
        return True

    def _get_worker_channel(self) -> str:
        """Get the Redis channel for worker->main communication."""
        return f"gulp:ws:worker:{self._server_id}"
    
    def _get_broadcast_channel(self) -> str:
        """Get the Redis channel for instance<->instance broadcast."""
        return "gulp:ws:broadcast"
    
    async def publish_from_worker(self, message: dict) -> None:
        """
        Publish a message from worker to main process (same instance).
        
        Args:
            message (dict): The message to publish (must be a GulpWsData dict)
        """
        channel = self._get_worker_channel()
        payload = orjson.dumps(message)
        await self._redis.publish(channel, payload)
        
    async def publish_broadcast(self, message: dict) -> None:
        """
        Publish a message to all server instances (cross-instance broadcast).
        
        Args:
            message (dict): The message to publish (must be a GulpWsData dict)
        """
        channel = self._get_broadcast_channel()
        payload = orjson.dumps(message)
        await self._redis.publish(channel, payload)
    
    async def subscribe_worker_channel(
        self,
        callback: Callable[[dict], Any],
    ) -> None:
        """
        Subscribe to worker->main messages (main process only).
        
        Args:
            callback: Async function to call with each message dict
        """
        channel = self._get_worker_channel()
        await self._pubsub.subscribe(channel)
        
        task = asyncio.create_task(
            self._subscriber_loop(channel, callback),
            name=f"redis-sub-worker-{self._server_id}"
        )
        self._subscriber_tasks[channel] = task
        
        MutyLogger.get_instance().info(
            "subscribed to worker channel: %s", channel
        )
    
    async def subscribe_broadcast_channel(
        self,
        callback: Callable[[dict], Any],
    ) -> None:
        """
        Subscribe to broadcast messages from all instances (all processes).
        
        Args:
            callback: Async function to call with each message dict
        """
        channel = self._get_broadcast_channel()
        await self._pubsub.subscribe(channel)
        
        task = asyncio.create_task(
            self._subscriber_loop(channel, callback),
            name="redis-sub-broadcast"
        )
        self._subscriber_tasks[channel] = task
        
        MutyLogger.get_instance().info(
            "subscribed to broadcast channel: %s", channel
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
        logger = MutyLogger.get_instance()
        logger.debug("starting subscriber loop for channel: %s", channel)
        
        try:
            while True:
                try:
                    message = await self._pubsub.get_message(
                        ignore_subscribe_messages=True,
                        timeout=1.0
                    )
                    
                    if message and message["type"] == "message":
                        try:
                            data = orjson.loads(message["data"])
                            # call callback (which might be async)
                            result = callback(data)
                            if asyncio.iscoroutine(result):
                                await result
                        except Exception as ex:
                            logger.error(
                                "error processing message from %s: %s",
                                channel,
                                ex
                            )
                    
                    # yield control
                    await asyncio.sleep(0.001)
                    
                except asyncio.CancelledError:
                    logger.debug("subscriber loop cancelled for %s", channel)
                    raise
                except Exception as ex:
                    logger.error("error in subscriber loop for %s: %s", channel, ex)
                    await asyncio.sleep(1.0)
                    
        except asyncio.CancelledError:
            logger.info("subscriber loop stopped for channel: %s", channel)
        finally:
            # unsubscribe on exit
            try:
                await self._pubsub.unsubscribe(channel)
            except Exception:
                pass