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
    CHANNEL = "gulpredis"

    def __init__(self):
        self._redis: redis.Redis = None
        self._pubsub: PubSub = None
        self._subscriber_task: asyncio.Task = None
        self.server_id: str = None

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
        
        # create pub/sub connection
        self._pubsub = self._redis.pubsub()
        
        MutyLogger.get_instance().info(
            "initialized Redis pub/sub for server_id=%s", server_id
        )
    
    async def shutdown(self) -> None:
        """
        Close Redis connections and cleanup.
        """        
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
            MutyLogger.get_instance().error("error cancelling subscriber task: %s", ex)
    
        # close pubsub
        if self._pubsub:
            try:
                await self._pubsub.close()
            except Exception as ex:
                MutyLogger.get_instance().error("error closing pubsub: %s", ex)
        
        # close redis client
        if self._redis:
            try:
                await self._redis.close()
                MutyLogger.get_instance().info("Redis client %s connection closed", self._redis)
            except Exception as ex:
                MutyLogger.get_instance().error("error closing redis client: %s", ex)
        
    
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
            name=f"gulpredis-sub-{self.server_id}"
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
        logger = MutyLogger.get_instance()
        logger.debug("starting subscriber loop for channel: %s", channel)
        
        try:
            while True:
                try:
                    # get message and call callback
                    message: dict = await self._pubsub.get_message(
                        ignore_subscribe_messages=True,
                        timeout=1.0
                    )                    
                    if message:
                        MutyLogger.get_instance().debug(
                            "---> received message on channel %s: %s",
                            channel,
                            message,
                        )
                        d: dict = orjson.loads(message["data"])                        
                        await callback(d)
                    
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