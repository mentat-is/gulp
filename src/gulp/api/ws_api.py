import asyncio
import collections
import os
import queue
import random
import threading
import time
from enum import StrEnum
from multiprocessing.managers import SyncManager
from queue import Empty, Queue
from typing import Any, Optional

import muty
import muty.time
from fastapi import WebSocket, WebSocketDisconnect
from fastapi.websockets import WebSocketState
from muty.log import MutyLogger
from muty.pydantic import autogenerate_model_example_by_class
from pydantic import BaseModel, ConfigDict, Field

from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.opensearch.structs import GulpDocument
from gulp.api.rest.test_values import (
    TEST_CONTEXT_ID,
    TEST_INDEX,
    TEST_OPERATION_ID,
    TEST_REQ_ID,
    TEST_SOURCE_ID,
    TEST_WS_ID,
)
from gulp.config import GulpConfig
from gulp.structs import GulpPluginParameters


class GulpWsType(StrEnum):
    # the type of the websocket
    WS_DEFAULT = "default"
    WS_INGEST = "ingest"
    WS_CLIENT_DATA = "client_data"


# data types for the websocket
WSDATA_ERROR = "ws_error"
WSDATA_CONNECTED = "ws_connected"
WSDATA_STATS_UPDATE = "stats_update"
WSDATA_COLLAB_UPDATE = "collab_update"
WSDATA_USER_LOGIN = "user_login"
WSDATA_USER_LOGOUT = "user_logout"
WSDATA_DOCUMENTS_CHUNK = "docs_chunk"
WSDATA_COLLAB_DELETE = "collab_delete"
WSDATA_INGEST_SOURCE_DONE = "ingest_source_done"
WSDATA_QUERY_DONE = "query_done"  # this is sent in the end of each individual query
WSDATA_QUERY_GROUP_DONE = "query_group_done"  # this is sent in the end of the query task, being it single or group(i.e. sigma) query
WSDATA_ENRICH_DONE = "enrich_done"
WSDATA_TAG_DONE = "tag_done"
WSDATA_QUERY_GROUP_MATCH = "query_group_match"
WSDATA_REBASE_DONE = "rebase_done"
WSDATA_CLIENT_DATA = "client_data"
WSDATA_SOURCE_FIELDS_CHUNK = "source_fields_chunk"
WSDATA_NEW_SOURCE = "new_source"
WSDATA_NEW_CONTEXT = "new_context"
WSDATA_PROGRESS = "progress"
WSDATA_GENERIC = "generic"

# special token used to monitor also logins
WSTOKEN_MONITOR = "monitor"


class WsQueueFullException(Exception):
    """Exception raised when queue is full after retries"""

    pass


class GulpUserLoginLogoutPacket(BaseModel):
    """
    Represents a user login or logout event.
    """

    model_config = ConfigDict(
        json_schema_extra={"examples": [{"user_id": "admin", "login": True}]}
    )
    user_id: str = Field(..., description="The user ID.")
    login: bool = Field(
        ..., description="If the event is a login event, either it is a logout."
    )
    ip: Optional[str] = Field(
        None,
        description="The IP address of the user, if available.",
    )


class GulpWsAcknowledgedPacket(BaseModel):
    """
    Represents a connection acknowledged event on the websocket.
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [{"ws_id": "the_ws_id", "token": "the_user_token"}]
        }
    )
    ws_id: str = Field(..., description="The WebSocket ID.")
    token: str = Field(..., description="The user token.")


class GulpSourceFieldsChunkPacket(BaseModel):
    """
    Represents fields type mapping for a source
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "operation_id": "the_operation_id",
                    "source_id": "the_source_id",
                    "context_id": "the_context_id",
                    "fields": {
                        "gulp.operation_id": "keyword",
                        "gulp.source_id": "keyword",
                        "gulp.timestamp": "long",
                    },
                    "last": False,
                }
            ]
        }
    )

    operation_id: str = Field(
        ...,
        description="the operation ID.",
    )
    source_id: str = Field(
        ...,
        description="the source ID.",
    )
    context_id: str = Field(
        ...,
        description="the context ID.",
    )
    fields: dict = Field(
        ...,
        description="mappings for the fields in a source.",
    )
    last: bool = Field(
        False,
        description="if this is the last chunk.",
    )


class GulpCollabDeletePacket(BaseModel):
    """
    Represents a delete collab event.
    """

    model_config = ConfigDict(json_schema_extra={"examples": [{"id": "the id"}]})
    id: str = Field(..., description="The collab object ID.")


class GulpQueryGroupMatchPacket(BaseModel):
    """
    Represents a query group match.

    once receiving a query group match, the client should query notes with tag=group name
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "name": "the query group name",
                    "total_hits": 100,
                }
            ]
        }
    )
    name: str = Field(..., description="The query group name.")
    total_hits: int = Field(..., description="The total number of hits.")


class GulpQueryDonePacket(BaseModel):
    """
    Represents a query done event on the websocket.
    """

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "status": GulpRequestStatus.DONE,
                    "name": "the query name",
                    "total_hits": 100,
                    "errors": None,
                }
            ]
        },
    )
    name: Optional[str] = Field(None, description="The query name.")
    status: GulpRequestStatus = Field(
        ..., description="The status of the query operation (done/failed)."
    )
    errors: Optional[list[str]] = Field([], description="The error message/s, if any.")
    total_hits: Optional[int] = Field(
        None, description="The total number of hits for the query."
    )


class GulpProgressPacket(BaseModel):
    """
    Used to signal progress on the websocket.
    """

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "current": 50,
                    "total": 100,
                    "msg": "Progressing ....",
                }
            ]
        },
    )
    current: int = Field(0, description="The current progress.")
    done: Optional[bool] = Field(
        False, description="If the progress is done, i.e. reached the total."
    )
    total: Optional[int] = Field(0, description="The total to be reached.")
    msg: Optional[str] = Field(
        None, description="An optional message to display with the progress."
    )


class GulpIngestSourceDonePacket(BaseModel):
    """
    to signal on the websocket that a source ingestion is done
    """

    model_config = ConfigDict(
        # solves the issue of not being able to populate fields using field name instead of alias
        populate_by_name=True,
        json_schema_extra={
            "examples": [
                {
                    "source_id": TEST_SOURCE_ID,
                    "context_id": TEST_CONTEXT_ID,
                    "req_id": TEST_REQ_ID,
                    "num_docs": 100,
                    "status": GulpRequestStatus.DONE,
                }
            ]
        },
    )
    source_id: str = Field(
        description="The source ID in the collab database.",
        alias="gulp.source_id",
    )
    context_id: str = Field(
        ...,
        description="The context ID in the collab database.",
        alias="gulp.context_id",
    )
    docs_ingested: int = Field(
        ..., description="The number of documents ingested in this source."
    )
    docs_skipped: int = Field(
        ..., description="The number of documents skipped in this source."
    )
    docs_failed: int = Field(
        ..., description="The number of documents failed in this source."
    )
    req_id: str = Field(..., description="The request ID.")
    status: GulpRequestStatus = Field(..., description="The request status.")


class GulpCollabCreateUpdatePacket(BaseModel):
    """
    Represents a create or update event.
    """

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "data": {
                        "id": "the id",
                        "name": "the name",
                        "type": "note",
                        "something": "else",
                    },
                    "bulk": True,
                    "bulk_size": 100,
                    "type": "note",
                    "created": True,
                }
            ]
        },
    )
    data: list | dict = Field(..., description="The created or updated data.")
    bulk: Optional[bool] = Field(
        default=False,
        description="If the event is a bulk event (data is a list instead of dict).",
    )
    type: Optional[str] = Field(
        None,
        description="Type of the event (i.e. one of the COLLABTYPE strings).",
    )
    bulk_size: Optional[int] = Field(None, description="The size of the bulk event.")
    last: Optional[bool] = Field(
        True,
        description="indicates the last chunk.",
    )
    created: Optional[bool] = Field(
        default=False, description="If the event is a create event."
    )


class GulpWsError(StrEnum):
    """
    Used for "error_code" in GulpWsErrorPacket
    """

    OBJECT_NOT_FOUND = "Not Found"
    MISSING_PERMISSION = "Forbidden"
    ERROR_GENERIC = "Error"


class GulpRebaseDonePacket(BaseModel):
    """
    Represents a rebase done event on the websocket.
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "src_index": TEST_INDEX,
                    "dest_index": "destination_index",
                    "status": GulpRequestStatus.DONE,
                    "result": "error message or successful rebase result",
                }
            ]
        }
    )
    operation_id: str = Field(..., description="The operation ID.")
    src_index: str = Field(..., description="The source index.")
    dest_index: str = Field(..., description="The destination index.")
    status: "GulpRequestStatus" = Field(
        ..., description="The status of the rebase operation (done/failed)."
    )
    result: Optional[str | dict] = Field(
        None, description="the error message, or successful rebase result."
    )


class GulpWsErrorPacket(BaseModel):
    """
    Represents an error on the websocket
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "error": "error message",
                    "error_code": GulpWsError.OBJECT_NOT_FOUND,
                    "data": {"id": "the id"},
                }
            ]
        }
    )
    error: str = Field(..., description="error on the websocket.")
    error_code: Optional[str] = Field(None, description="optional error code.")
    data: Optional[dict] = Field(None, description="optional error data")


class GulpClientDataPacket(BaseModel):
    """
    arbitrary client data sent to gulp by UI, routed to all connected websockets
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "data": {"something": "12345"},
                }
            ]
        }
    )
    data: dict = Field(..., description="arbitrary data")


class GulpWsIngestPacket(BaseModel):
    """
    Represents packets sent to /ws_ingest to ingest raw documents.
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "operation_id": TEST_OPERATION_ID,
                    "ws_id": TEST_WS_ID,
                    "flt": autogenerate_model_example_by_class(GulpIngestionFilter),
                    "plugin": "raw",
                    "plugin_params": autogenerate_model_example_by_class(
                        GulpPluginParameters
                    ),
                }
            ]
        },
    )

    index: str = Field(
        ...,
        description="the Gulp index to ingest into.",
    )
    operation_id: str = Field(
        ...,
        description="the operation ID.",
    )
    ws_id: str = Field(
        ...,
        description="id of the websocket to stream ingested data to.",
    )
    req_id: str = (Field(..., description="id of the request"),)
    flt: Optional[GulpIngestionFilter] = Field(
        GulpIngestionFilter(),
        description="optional filter to apply for ingestion.",
    )
    plugin: Optional[str] = Field(
        "raw",
        description="plugin to be used for ingestion (default='raw').",
    )
    plugin_params: Optional[GulpPluginParameters] = Field(
        None,
        description="optional plugin parameters",
    )


class GulpWsAuthPacket(BaseModel):
    """
    Parameters for authentication on the websocket
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "token": "token_admin",
                    "ws_id": TEST_WS_ID,
                    "operation_id": [TEST_OPERATION_ID],
                    "type": [WSDATA_DOCUMENTS_CHUNK],
                }
            ]
        }
    )
    token: str = Field(
        ...,
        description="""user token.

        - `monitor` is a special token, reserved for internal use.
    """,
    )
    ws_id: Optional[str] = Field(
        None, description="the WebSocket ID, leave empty to autogenerate."
    )
    operation_ids: Optional[list[str]] = Field(
        None,
        description="the `operation_id`/s this websocket is registered to receive data for, defaults to `None` (all).",
    )
    types: Optional[list[str]] = Field(
        None,
        description="the `GulpWsData.type`/s this websocket is registered to receive, defaults to `None` (all).",
    )


class GulpDocumentsChunkPacket(BaseModel):
    """
    Represents a chunk of GulpDocument dictionaries returned by a query or sent during ingestion.

    may include extra fields depending on the source.

    """

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "docs": [
                        autogenerate_model_example_by_class(GulpDocument),
                    ],
                    "num_docs": 1,
                    "chunk_number": 1,
                    "total_hits": 100,
                    "name": "the query name",
                    "last": False,
                    "search_after": ["value_1", "value_2"],
                }
            ]
        },
    )

    docs: list[dict] = Field(
        ...,
        description="the documents in a query or ingestion chunk.",
    )
    num_docs: int = Field(
        ...,
        description="the number of documents in this chunk.",
    )
    chunk_number: Optional[int] = Field(
        0,
        description="the chunk number (may not be available)",
    )
    total_hits: Optional[int] = Field(
        0,
        description="for query only: the total number of hits for the query related to this chunk.",
    )
    name: Optional[str] = Field(
        None,
        description="for query only: the query name related to this chunk.",
    )
    last: Optional[bool] = Field(
        False,
        description="for query only: is this the last chunk of a query response ?",
    )
    search_after: Optional[list] = Field(
        None,
        description="for query only: to use in `QueryAdditionalParameters.search_after` to request the next chunk in a paged query.",
    )
    enriched: Optional[bool] = Field(
        False,
        description="if the documents have been enriched.",
    )


class GulpWsData(BaseModel):
    """
    data carried by the websocket ui<->gulp
    """

    model_config = ConfigDict(
        # solves the issue of not being able to populate fields using field name instead of alias
        populate_by_name=True,
    )
    timestamp: int = Field(
        ..., description="The timestamp of the data.", alias="@timestamp"
    )
    type: str = Field(..., description="The type of data carried by the websocket.")
    ws_id: Optional[str] = Field(
        None,
        description="The target WebSocket ID, may be None only if `internal` is set.",
    )
    user_id: Optional[str] = Field(None, description="The user who issued the request.")
    req_id: Optional[str] = Field(None, description="The request ID.")
    operation_id: Optional[str] = Field(
        None,
        description="The operation this data belongs to.",
        alias="gulp.operation_id",
    )
    private: Optional[bool] = Field(
        False,
        description="If the data is private, only the websocket `ws_id` receives it.",
    )
    data: Optional[Any] = Field(None, description="The data carried by the websocket.")
    internal: Optional[bool] = Field(
        False,
        description="Used internally (i.e. to broadcast internal events to plugins).",
    )


class WsQueueMessagePool:
    """
    message pool for the ws to reduce memory pressure and gc
    """

    def __init__(self, maxsize: int = 1000):
        # preallocate message pool
        self._pool = collections.deque(maxlen=maxsize)

    def clear(self):
        """
        clear the message pool
        """
        self._pool.clear()

    def get(self) -> dict:
        """
        get a preallocated message from the pool

        Returns:
            dict: the message
        """
        try:
            msg = self._pool.popleft()
            return msg
        except IndexError:
            return {}

    def put(self, msg: dict):
        """
        put a message back into the pool

        Args:
            msg (dict): the message to put back
        """
        # clear and put back
        msg.clear()
        self._pool.append(msg)


class GulpConnectedSocket:
    """
    represents a connected websocket.

    this class manages the lifecycle of a websocket connection including:
    - message queue handling
    - adaptive rate limiting
    - performance monitoring
    - connection state management
    - cleanup operations
    """

    # class constants for configuration
    BACKPRESSURE_THRESHOLD: int = 1000
    BACKPRESSURE_DELAY: float = 0.1
    QUEUE_SIZE_HIGH_THRESHOLD: int = 500
    QUEUE_SIZE_MEDIUM_THRESHOLD: int = 200
    MAX_ADAPTIVE_DELAY: float = 0.5
    METRICS_LOG_INTERVAL: int = 1000
    COUNTER_RESET_INTERVAL: int = 10000
    YIELD_CONTROL_INTERVAL: int = 100
    YIELD_CONTROL_DELAY: float = 0.01

    def __init__(
        self,
        ws: WebSocket,
        ws_id: str,
        types: list[str] = None,
        operation_ids: list[str] = None,
        socket_type: GulpWsType = GulpWsType.WS_DEFAULT,
    ):
        """
        Initializes the ConnectedSocket object.

        Args:
            ws (WebSocket): The WebSocket object.
            ws_id (str): The WebSocket ID.
            types (list[str], optional): The types of data this websocket is interested in. Defaults to None (all).
            operation_ids (list[str], optional): The operation/s this websocket is interested in. Defaults to None (all).
            socket_type (GulpWsType, optional): The type of the websocket. Defaults to GulpWsType.WS_DEFAULT.
        """
        self.ws = ws
        self.ws_id = ws_id
        self._msg_pool = WsQueueMessagePool()
        self.types = types
        self.operation_ids = operation_ids
        self.send_task = None
        self.receive_task = None
        self.socket_type = socket_type
        self._cleaned_up = False

        # each socket has its own asyncio queue, consumed by its own task
        self.q = asyncio.Queue()

    async def run_loop(self) -> None:
        """
        Runs the websocket loop with optimized task management and cleanup
        """
        tasks: list[asyncio.Task] = []
        try:
            # Create tasks with names for better debugging
            self.send_task = asyncio.create_task(
                self._send_loop(), name=f"send_loop-{self.ws_id}"
            )
            self.receive_task = asyncio.create_task(
                self._receive_loop(), name=f"receive_loop-{self.ws_id}"
            )
            tasks.extend([self.send_task, self.receive_task])

            # Wait for first task to complete
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)

            # Process completed task
            task = done.pop()
            try:
                await task
            except WebSocketDisconnect as ex:
                MutyLogger.get_instance().debug(
                    f"websocket {self.ws_id} disconnected: {ex}"
                )
                raise
            except Exception as ex:
                MutyLogger.get_instance().error(f"error in {task.get_name()}: {ex}")
                raise

        finally:
            # ensure cleanup happens even if cancelled
            await self.cleanup(tasks)

    async def cleanup(self, tasks: list[asyncio.Task] = None) -> None:
        """
        ensure websocket is cleaned up when canceled/closing
        """
        if self._cleaned_up:
            # already cleaned up
            return

        MutyLogger.get_instance().debug(
            "---> cleanup, ensuring ws cleanup for ws=%s, ws_id=%s"
            % (self.ws, self.ws_id)
        )
        # empty the queue
        await self._flush_queue()

        # clear the msg pool
        self._msg_pool.clear()

        if tasks:
            # clear tasks
            await asyncio.shield(self._cleanup_tasks(tasks))

        self._cleaned_up = True

    async def _flush_queue(self) -> None:
        """
        flushes the websocket queue.
        """
        try:
            # drain queue quickly and efficiently
            while self.q.qsize() != 0:
                try:
                    self.q.get_nowait()
                    self.q.task_done()
                except asyncio.QueueEmpty:
                    break

            await self.q.join()
            MutyLogger.get_instance().debug(
                f"---> queue flushed for ws_id={self.ws_id}"
            )
        except Exception as ex:
            MutyLogger.get_instance().warning(f"error flushing queue: {ex}")

    @staticmethod
    def is_alive(ws_id: str) -> bool:
        """
        check if a websocket is alive

        Args:
            ws_id (str): The WebSocket ID.

        Returns:
            bool: True if the websocket is alive, False otherwise.
        """
        from gulp.process import GulpProcess

        if GulpConfig.get_instance().debug_ignore_missing_ws():
            return True

        return ws_id in GulpProcess.get_instance().shared_ws_list

    async def _cleanup_tasks(self, tasks: list[asyncio.Task]) -> None:
        """
        clean up tasks with proper cancellation handling

        Args:
            tasks (list): the tasks to clean up
        """
        logger = MutyLogger.get_instance()

        for task in tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    logger.debug(f"task {task.get_name()} cancelled successfully")
                except WebSocketDisconnect:
                    logger.debug(f"task {task.get_name()} disconnected during cleanup")
                except Exception as ex:
                    logger.error(f"error cleaning up {task.get_name()}: {str(ex)}")

        # remove from global ws list
        from gulp.process import GulpProcess

        try:
            GulpProcess.get_instance().shared_ws_list.remove(self.ws_id)
            logger.debug(f"removed ws_id={self.ws_id} from shared_ws_list")
        except ValueError:
            logger.debug(f"ws_id={self.ws_id} not found in shared_ws_list")

    async def _receive_loop(self) -> None:
        """
        continuously receives messages to detect disconnection
        """
        # track time to periodically yield control
        last_yield_time = time.monotonic()

        while True:
            # raise exception if websocket is disconnected
            self.validate_connection()

            try:
                # use a shorter timeout to ensure we can yield control regularly
                await asyncio.wait_for(self.ws.receive_json(), timeout=1.0)

                # yield control periodically even if receiving messages quickly
                current_time = time.monotonic()
                if current_time - last_yield_time > 0.1:
                    await asyncio.sleep(
                        self.YIELD_CONTROL_DELAY
                    )  # brief yield to handle control frames
                    last_yield_time = current_time

            except asyncio.TimeoutError:
                # no message received, good time to yield control
                await asyncio.sleep(self.YIELD_CONTROL_DELAY)
                continue

    async def put_message(self, msg: dict) -> None:
        """
        Puts a message into the websocket queue.

        Args:
            msg (dict): The message to put.
        """
        if self.q.qsize() > self.BACKPRESSURE_THRESHOLD:
            # backpressure
            await asyncio.sleep(self.BACKPRESSURE_DELAY)

        # use a slot from the pool
        pooled_msg = self._msg_pool.get()
        pooled_msg.update(msg)
        await self.q.put(pooled_msg)

    async def send_json(self, msg: dict, delay: float) -> None:
        """
        Sends a JSON message to the websocket.

        Args:
            msg (dict): The message to send.
            delay (float): The delay to wait before sending the message.
        """
        await self.ws.send_json(msg)

        # return msg to pool
        self._msg_pool.put(msg)

        # rate limit
        await asyncio.sleep(delay)

    def _calculate_adaptive_delay(self, queue_size: int, base_delay: float) -> float:
        """
        calculates adaptive delay based on queue size

        args:
            queue_size (int): current queue size
            base_delay (float): base delay configuration

        returns:
            float: calculated delay in seconds
        """
        if queue_size > self.QUEUE_SIZE_HIGH_THRESHOLD:
            # exponential backoff based on queue size
            return min(base_delay * (queue_size / 100), self.MAX_ADAPTIVE_DELAY)
        return base_delay

    def _get_queue_timeout(self, queue_size: int) -> float:
        """
        determines appropriate queue timeout based on load

        args:
            queue_size (int): current queue size

        returns:
            float: timeout value in seconds
        """
        return 0.05 if queue_size > self.QUEUE_SIZE_MEDIUM_THRESHOLD else 0.1

    def validate_connection(self) -> None:
        """
        validates that the websocket is still connected

        raises:
            WebSocketDisconnect: if the websocket is not connected
        """
        if self.ws.client_state != WebSocketState.CONNECTED:
            raise WebSocketDisconnect("client disconnected")

    async def _process_queue_message(self) -> bool:
        """
        processes a single message from the queue

        returns:
            bool: true if message was processed, false if timeout occurred
        """
        queue_size = self.q.qsize()
        timeout = self._get_queue_timeout(queue_size)

        try:
            # get message with timeout
            item = await asyncio.wait_for(self.q.get(), timeout=timeout)

            # check state before sending
            current_task = asyncio.current_task()
            if current_task and current_task.cancelled():
                raise asyncio.CancelledError()

            # raise exception if websocket is disconnected
            self.validate_connection()

            # connection is verified, send the message
            await self.send_json(
                item,
                self._calculate_adaptive_delay(
                    queue_size, GulpConfig.get_instance().ws_rate_limit_delay()
                ),
            )
            self.q.task_done()
            return True

        except asyncio.TimeoutError:
            # check for cancellation during timeout
            current_task = asyncio.current_task()
            if current_task and current_task.cancelled():
                raise asyncio.CancelledError()
            return False

    def _log_processing_metrics(self, message_count: int, start_time: float) -> None:
        """
        logs processing metrics for monitoring

        args:
            message_count (int): number of messages processed
            start_time (float): start time reference
        """
        elapsed: float = time.monotonic() - start_time
        queue_size: int = self.q.qsize()

        if elapsed > 0:
            rate: float = message_count / elapsed
            MutyLogger.get_instance().info(
                f"websocket {self.ws_id} processing at {rate:.1f} msg/s, queue_size={queue_size}"
            )
        else:
            MutyLogger.get_instance().info(
                f"websocket {self.ws_id} processed {message_count} messages, queue_size={queue_size}"
            )

    async def _send_loop(self) -> None:
        """
        processes outgoing messages with adaptive rate limiting and monitoring
        """
        logger = MutyLogger.get_instance()

        try:
            logger.debug(f"---> starting send loop for ws_id={self.ws_id}")

            # tracking variables for metrics
            message_count = 0
            start_time = time.monotonic()
            yield_counter = 0

            while True:
                # process a single message
                message_processed = await self._process_queue_message()

                if message_processed:
                    message_count += 1
                    yield_counter += 1

                    # log metrics periodically
                    if message_count % self.METRICS_LOG_INTERVAL == 0:
                        self._log_processing_metrics(message_count, start_time)

                    # reset counters periodically
                    if message_count % self.COUNTER_RESET_INTERVAL == 0:
                        start_time = time.monotonic()
                        message_count = 0

                    # yield control periodically to allow ping handling
                    if yield_counter % self.YIELD_CONTROL_INTERVAL == 0:
                        yield_counter = 0
                        await asyncio.sleep(self.YIELD_CONTROL_DELAY)

        except asyncio.CancelledError:
            logger.warning(f"---> send loop canceled for ws_id={self.ws_id}")
            raise

        except WebSocketDisconnect as ex:
            logger.warning(f"---> websocket disconnected: ws_id={self.ws_id}")
            logger.exception(ex)
            raise

        except Exception as ex:
            logger.error(f"---> error in send loop: ws_id={self.ws_id}")
            logger.exception(ex)
            raise

    def __str__(self):
        return f"ConnectedSocket(ws_id={self.ws_id}, types={self.types}, operations={self.operation_ids})"


class GulpConnectedSockets:
    """
    singleton class to hold all connected sockets
    """

    _instance: "GulpConnectedSockets" = None

    def __init__(self):
        pass

    def __new__(cls):
        """
        Create a new instance of the class.
        """
        if not cls._instance:
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance

    @classmethod
    def get_instance(cls) -> "GulpConnectedSockets":
        """
        Get the singleton instance of the class.

        Returns:
            GulpConnectedSockets: the singleton instance
        """
        if not cls._instance:
            cls._instance = cls()
        return cls._instance

    def _initialize(self):
        self._initialized: bool = True
        self._sockets: dict[str, GulpConnectedSocket] = {}

        # for faster lookup, maintain a mapping from ws_id to socket id
        self._ws_id_map: dict[str, str] = {}
        self._logger = MutyLogger.get_instance()

    def add(
        self,
        ws: WebSocket,
        ws_id: str,
        types: list[str] = None,
        operation_ids: list[str] = None,
        socket_type: GulpWsType = GulpWsType.WS_DEFAULT,
    ) -> GulpConnectedSocket:
        """
        Adds a websocket to the connected sockets list.

        Args:
            ws (WebSocket): The WebSocket object.
            ws_id (str): The WebSocket ID.
            types (list[str], optional): The types of data this websocket is interested in. Defaults to None (all)
            operation_ids (list[str], optional): The operations this websocket is interested in. Defaults to None (all)
            socket_type (GulpWsType, optional): The type of the websocket. Defaults to GulpWsType.WS_DEFAULT.
        Returns:
            ConnectedSocket: The ConnectedSocket object.
        """
        # create connected socket instance
        connected_socket = GulpConnectedSocket(
            ws=ws,
            ws_id=ws_id,
            types=types,
            operation_ids=operation_ids,
            socket_type=socket_type,
        )

        # store socket reference
        socket_id = str(id(ws))
        self._sockets[socket_id] = connected_socket
        self._ws_id_map[ws_id] = socket_id

        # add to global shared list
        from gulp.process import GulpProcess

        GulpProcess.get_instance().shared_ws_list.append(ws_id)

        self._logger.debug(
            f"---> added connected ws: {ws}, id_str={socket_id}, ws_id={ws_id}, len={len(self._sockets)}"
        )

        return connected_socket

    async def remove(self, ws: WebSocket, flush: bool = True) -> None:
        """
        Removes a websocket from the connected sockets list

        Args:
            ws (WebSocket): The WebSocket object.
            flush (bool, optional): If the queue should be flushed. Defaults to True.

        """
        socket_id = str(id(ws))
        self._logger.debug(f"---> remove, ws={ws}, id_str={socket_id}")

        connected_socket = self._sockets.get(socket_id)
        if not connected_socket:
            self._logger.warning(
                f"remove(): no websocket found for ws={ws}, id_str={socket_id}, len={len(self._sockets)}"
            )
            return

        # flush queue if requested
        if flush:
            await connected_socket.cleanup()

        # remove from global ws list
        from gulp.process import GulpProcess

        ws_id = connected_socket.ws_id
        while ws_id in GulpProcess.get_instance().shared_ws_list:
            GulpProcess.get_instance().shared_ws_list.remove(ws_id)

        # remove from internal maps
        del self._sockets[socket_id]
        if ws_id in self._ws_id_map:
            del self._ws_id_map[ws_id]

        self._logger.debug(
            f"---> removed connected ws={ws}, id={socket_id}, ws_id={ws_id}, len={len(self._sockets)}"
        )

    def find(self, ws_id: str) -> GulpConnectedSocket:
        """
        Finds a ConnectedSocket object by its ID.

        Args:
            ws_id (str): The WebSocket ID.

        Returns:
            ConnectedSocket: The ConnectedSocket object or None if not found.
        """
        # use fast lookup via mapping
        socket_id = self._ws_id_map.get(ws_id)
        if socket_id:
            return self._sockets.get(socket_id)

        return None

    async def cancel_all(self) -> None:
        """
        cancels all active websocket tasks
        """
        # create a copy to avoid modification during iteration
        sockets_to_cancel = list(self._sockets.values())

        for connected_socket in sockets_to_cancel:
            self._logger.warning(
                f"---> canceling ws {connected_socket.ws}, ws_id={connected_socket.ws_id}"
            )
            if connected_socket.receive_task:
                connected_socket.receive_task.cancel()
            if connected_socket.send_task:
                connected_socket.send_task.cancel()

        self._logger.debug(
            f"all active websockets cancelled, count={len(sockets_to_cancel)}"
        )

    async def _route_message(
        self, data: GulpWsData, client_ws: GulpConnectedSocket
    ) -> bool:
        """
        determines if a message should be routed to the target websocket
        and sends it if appropriate

        Args:
            data (GulpWsData): the data to route
            client_ws (GulpConnectedSocket): the target websocket

        Returns:
            bool: True if message was routed, False otherwise
        """

        async def _route(data: GulpWsData, client_ws: GulpConnectedSocket):
            """put message in the target websocket queue"""
            message = data.model_dump(
                exclude_none=True, exclude_defaults=True, by_alias=True
            )

            await client_ws.put_message(message)

        # skip if not a default socket
        if client_ws.socket_type != GulpWsType.WS_DEFAULT:
            return False

        # check type filter
        if client_ws.types and data.type not in client_ws.types:
            return False

        # check operation filter
        if client_ws.operation_ids and data.operation_id not in client_ws.operation_ids:
            return False

        # handle private messages
        if data.private and data.type != WSDATA_USER_LOGIN:
            if client_ws.ws_id != data.ws_id:
                return False

        # check message distribution rules
        broadcast_types = {
            WSDATA_COLLAB_UPDATE,
            WSDATA_COLLAB_DELETE,
            WSDATA_NEW_CONTEXT,
            WSDATA_NEW_SOURCE,
            WSDATA_INGEST_SOURCE_DONE,
            WSDATA_REBASE_DONE,
            WSDATA_USER_LOGIN,
            WSDATA_USER_LOGOUT,
        }

        if data.type not in broadcast_types and client_ws.ws_id != data.ws_id:
            return False

        # all checks passed, send the message
        await _route(data, client_ws)
        return True

    async def _process_internal_message(self, data: GulpWsData) -> None:
        """
        processes internal messages that do not require routing

        Args:
            data (GulpWsData): The internal message to process.
        """
        # handle internal messages here
        # for now, just log it
        # self._logger.debug(f"processing internal message: {data}")
        from gulp.plugin import GulpInternalEventsManager

        await GulpInternalEventsManager.get_instance().broadcast_event(
            data.type, data.data
        )

    async def broadcast_message(
        self, data: GulpWsData, skip_list: list[str] = None
    ) -> None:
        """
        broadcasts message to appropriate connected websockets

        Args:
            data (GulpWsData): The message to broadcast.
            skip_list (list[str], optional): The list of websocket IDs to skip. Defaults to None.
        """
        if data.internal:
            # this is an internal message, skip routing and process directly
            await self._process_internal_message(data)
            return

        # create copy to avoid modification during iteration
        socket_items = list(self._sockets.items())

        # track dead sockets for cleanup
        dead_sockets = []

        for socket_id, connected_socket in socket_items:
            try:
                if skip_list and socket_id in skip_list:
                    continue

                await self._route_message(data, connected_socket)

            except Exception as ex:
                self._logger.warning(
                    f"Failed to broadcast to socket {socket_id}: {str(ex)}"
                )
                dead_sockets.append(socket_id)

        # clean up dead sockets
        for socket_id in dead_sockets:
            self._logger.warning(f"removing dead socket {socket_id}")
            self._sockets.pop(socket_id, None)


class GulpWsSharedQueue:
    """
    Singleton class to manage a shared websocket queue between processes.
    Provides methods for adding data to the queue and processing messages.
    """

    _instance: "GulpWsSharedQueue" = None

    # Class constants
    MAX_QUEUE_SIZE = 1000
    QUEUE_TIMEOUT = 30
    MAX_RETRIES = 3

    # Queue processing constants
    MIN_BATCH_SIZE = 10
    MAX_BATCH_SIZE = 100
    BATCH_TIMEOUT = 0.1
    PROCESSING_YIELD_INTERVAL = 0.1
    SUB_BATCH_SIZE = 20

    def __init__(self):
        pass

    def __new__(cls):
        """
        Create a new instance of the class.
        """
        if not cls._instance:
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance

    @classmethod
    def get_instance(cls) -> "GulpWsSharedQueue":
        """
        Returns the singleton instance.

        Returns:
            GulpWsSharedQueue: The singleton instance.
        """
        if not cls._instance:
            cls._instance = cls()
        return cls._instance

    def _initialize(self):
        self._initialized: bool = True
        self._shared_q: Queue = None
        self._fill_task: asyncio.Task = None
        self._last_queue_warning: int = 0  # just for metrics...

    def set_queue(self, q: Queue) -> None:
        """
        Sets the shared queue.

        Args:
            q (Queue): The shared queue.
        """
        from gulp.process import GulpProcess

        if GulpProcess.get_instance().is_main_process():
            raise RuntimeError("set_queue() must be called in a worker process")

        MutyLogger.get_instance().debug(
            "setting shared ws queue in worker process: q=%s" % (q)
        )
        self._shared_q = q

    async def init_queue(self, mgr: SyncManager) -> Queue:
        """
        to be called by the MAIN PROCESS, initializes the shared multiprocessing queue: the same queue must then be passed to the worker processes

        Args:
            q (Queue): The shared queue created by the multiprocessing manager in the main process
        """
        from gulp.process import GulpProcess

        if not GulpProcess.get_instance().is_main_process():
            raise RuntimeError("init_queue() must be called in the main process")

        if self._shared_q:
            # close first
            await self.close()

        MutyLogger.get_instance().debug("re/initializing shared ws queue ...")
        self._shared_q = mgr.Queue()
        self._fill_task = asyncio.create_task(self._fill_ws_queues_from_shared_queue())

        return self._shared_q

    def _update_queue_metrics(self, queue_size: int, sleep_time: float) -> None:
        """
        update queue metrics for monitoring

        args:
            queue_size (int): current queue size
            sleep_time (float): applied sleep time
        """
        # log periodic warnings if queue remains consistently full
        now = time.time()
        load_ratio = queue_size / self.MAX_QUEUE_SIZE

        # log warning at most once per minute
        if load_ratio > 0.9 and (now - self._last_queue_warning) > 60:
            MutyLogger.get_instance().warning(
                f"high queue pressure: size={queue_size}/{self.MAX_QUEUE_SIZE} "
                f"({load_ratio:.1%}), applying {sleep_time:.3f}s delay"
            )
            self._last_queue_warning = now

    def _apply_backpressure_strategy(self, queue_size: int) -> float:
        """
        apply appropriate backpressure based on queue size.

        throttles throughput by increasing sleep time as queue fills up,
        but never drops messages.

        args:
            queue_size (int): current queue size

        returns:
            float: sleep time in seconds to apply
        """
        # determine load level
        load_ratio = queue_size / self.MAX_QUEUE_SIZE

        # exponential backpressure as load increases
        if load_ratio > 0.95:  # critical load
            # max 2 second delay at absolute maximum load
            return min(2.0, 0.5 * (load_ratio**2))

        elif load_ratio > 0.8:  # heavy load
            # between ~0.1s and ~0.5s delay
            return min(0.5, 0.15 * load_ratio)

        elif load_ratio > 0.6:  # moderate load
            # gentle slowdown
            return min(0.15, 0.05 * load_ratio)

        else:  # normal load
            # minimal delay
            return 0.01

    async def _process_message_batch(self, messages: list[GulpWsData]) -> None:
        """
        Process a batch of messages with retry logic.

        Args:
            messages (list): List of messages to process
        """
        logger = MutyLogger.get_instance()

        # process in smaller sub-batches to yield control
        sub_batch_size = max(1, min(self.SUB_BATCH_SIZE, len(messages)))
        for i in range(0, len(messages), sub_batch_size):
            sub_batch = messages[i : i + sub_batch_size]
            for msg in sub_batch:
                await self._process_single_message(msg, logger)

            # yield control between sub-batches
            await asyncio.sleep(0.01)

    async def _process_single_message(self, msg: GulpWsData, logger: MutyLogger):
        """Process a single message with retry logic"""
        retries = 0
        while retries < self.MAX_RETRIES:
            try:
                cws = GulpConnectedSockets.get_instance().find(msg.ws_id)
                if cws or msg.internal:
                    await GulpConnectedSockets.get_instance().broadcast_message(msg)
                break
            except Exception as e:
                retries += 1
                if retries == self.MAX_RETRIES:
                    logger.error(
                        f"Failed to process message after {self.MAX_RETRIES} retries: {e}"
                    )
                await asyncio.sleep(0.1 * retries)

    async def _fill_ws_queues_from_shared_queue(self):
        """
        Processes messages from shared queue with guaranteed delivery
        """
        from gulp.api.rest_api import GulpRestServer

        logger = MutyLogger.get_instance()
        logger.debug("Starting queue processing task...")

        try:
            messages: list[GulpWsData] = []
            current_batch_size = self.MAX_BATCH_SIZE
            last_yield_time = time.monotonic()

            while not GulpRestServer.get_instance().is_shutdown():
                # Yield control periodically
                if time.monotonic() - last_yield_time > self.PROCESSING_YIELD_INTERVAL:
                    await asyncio.sleep(0.01)
                    last_yield_time = time.monotonic()

                shared_queue_size = self._shared_q.qsize()

                # apply backpressure strategy based on queue load
                if shared_queue_size > self.MAX_QUEUE_SIZE * 0.6:
                    sleep_time = self._apply_backpressure_strategy(shared_queue_size)
                    self._update_queue_metrics(shared_queue_size, sleep_time)
                    await asyncio.sleep(sleep_time)

                    # increase batch size under heavy load to clear backlog faster
                    if shared_queue_size > self.MAX_QUEUE_SIZE * 0.95:
                        current_batch_size = (
                            self.MAX_BATCH_SIZE
                        )  # force maximum batch size
                    else:
                        # adjust batch size dynamically based on load
                        current_batch_size = self._calculate_batch_size(
                            shared_queue_size
                        )
                else:
                    # normal load - use standard batch size calculation
                    current_batch_size = self._calculate_batch_size(shared_queue_size)

                # collect batch of messages
                await self._collect_message_batch(messages, current_batch_size)

                # process collected messages
                if messages:
                    await self._process_message_batch(messages)
                    messages.clear()

                # Adaptive sleep based on queue size
                sleep_time = (
                    0.05 if shared_queue_size > self.MAX_QUEUE_SIZE * 0.5 else 0.1
                )
                await asyncio.sleep(sleep_time)

        except asyncio.CancelledError:
            logger.info("Queue processing cancelled")
        except Exception as e:
            logger.error(f"Queue processing error: {e}")
            raise
        finally:
            # process remaining messages
            for msg in messages:
                try:
                    cws = GulpConnectedSockets.get_instance().find(msg.ws_id)
                    if cws or msg.internal:
                        await GulpConnectedSockets.get_instance().broadcast_message(msg)
                except Exception as e:
                    logger.error(f"Cleanup error: {e}")

            logger.info("Queue processing task completed")

    def _calculate_batch_size(self, queue_size: int) -> int:
        """Calculate optimal batch size based on queue load"""
        if queue_size > self.MAX_QUEUE_SIZE * 0.8:
            return self.MAX_BATCH_SIZE
        elif queue_size < self.MAX_QUEUE_SIZE * 0.2:
            return self.MIN_BATCH_SIZE
        else:
            # linear interpolation between min and max batch sizes
            load_factor = (queue_size - self.MAX_QUEUE_SIZE * 0.2) / (
                self.MAX_QUEUE_SIZE * 0.6
            )
            return int(
                self.MIN_BATCH_SIZE
                + load_factor * (self.MAX_BATCH_SIZE - self.MIN_BATCH_SIZE)
            )

    async def _collect_message_batch(
        self, messages: list[GulpWsData], batch_size: int
    ) -> None:
        """
        Collect a batch of messages from the queue

        Args:
            messages (list): List to store collected messages
            batch_size (int): Target batch size
        """
        batch_start = time.monotonic()

        while (
            len(messages) < batch_size
            and (time.monotonic() - batch_start) < self.BATCH_TIMEOUT
        ):
            try:
                entry = self._shared_q.get_nowait()
                messages.append(entry)
                self._shared_q.task_done()
            except Empty:
                break

    async def close(self) -> None:
        """
        Closes the shared multiprocessing queue (flushes it first).

        Returns:
            None
        """
        if not self._shared_q:
            return

        logger = MutyLogger.get_instance()
        logger.debug("Closing shared WS queue...")
        success: bool = False

        try:
            # flush queue with timeout protection
            try:
                await asyncio.wait_for(self._flush_queue(), timeout=10.0)
            except asyncio.TimeoutError:
                logger.warning("Queue flush operation timed out")

            # cancel processing task with proper handling
            if self._fill_task:
                try:
                    cancelled = self._fill_task.cancel()
                    if cancelled:
                        # wait for task with timeout
                        await asyncio.wait_for(self._fill_task, timeout=5.0)
                        logger.debug("Queue processing task cancelled successfully")
                    else:
                        logger.debug("Queue processing task was already completed")
                except asyncio.TimeoutError:
                    logger.warning("Task cancellation timed out")
                except Exception as e:
                    logger.error(f"Error during task cancellation: {e}")

            # release resources
            self._shared_q = None
            success = True
            logger.debug(f"Shared WS queue closed (success={success})")

        except Exception as e:
            logger.error(f"Failed to close shared queue: {e}")

    async def _flush_queue(self) -> None:
        """Flush all items from the queue"""
        if not self._shared_q:
            return

        counter = 0
        while True:
            try:
                self._shared_q.get_nowait()
                self._shared_q.task_done()
                counter += 1
            except Empty:
                break

        self._shared_q.join()
        MutyLogger.get_instance().debug(f"Flushed {counter} messages from shared queue")

    def put_internal_event(self, msg: str, params: dict = None) -> None:
        """
        Puts a GulpInternalEvent (not websocket related) into the shared queue.

        this is used to send internal events from worker processes to be processed by the main process

        Args:
            msg (str): The message type.
            params (dict, optional): The parameters for the message.
        """
        wsd = GulpWsData(
            timestamp=muty.time.now_msec(),
            type=msg,
            data=params,
            internal=True,
        )
        self._shared_q.put(wsd)

    async def put(
        self,
        type: str,
        ws_id: str,
        user_id: str,
        operation_id: str = None,
        req_id: str = None,
        data: Any = None,
        private: bool = False,
    ) -> None:
        """
        adds data to the shared queue with retry logic and backpressure handling.

        this method attempts to put a message into the queue and will retry if the
        queue is full, with progressive backoff between retries.

        args:
            type (str): the type of data being queued
            ws_id (str): the websocket id that will receive the message
            user_id (str): the user id associated with this message
            operation_id (Optional[str]): the operation id if applicable
            req_id (Optional[str]): the request id if applicable
            data (Optional[Any]): the payload data
            private (bool): whether this message is private to the specified ws_id

        raises:
            WebSocketDisconnect: if websocket is not connected for DOCUMENTS_CHUNK type
            WsQueueFullException: if queue is full after all retries
        """

        # verify websocket is alive for document chunks - this allows interrupting
        # lengthy processes if the websocket is no longer connected
        if type == WSDATA_DOCUMENTS_CHUNK:
            if not GulpConnectedSocket.is_alive(ws_id):
                raise WebSocketDisconnect("websocket '%s' is not connected!" % (ws_id))

        # create the message
        wsd = GulpWsData(
            timestamp=muty.time.now_msec(),
            type=type,
            operation_id=operation_id,
            ws_id=ws_id,
            user_id=user_id,
            req_id=req_id,
            private=private,
            data=data,
        )

        # attempt to add with exponential backoff
        logger = MutyLogger.get_instance()

        for retry in range(self.MAX_RETRIES):
            try:
                self._shared_q.put(wsd, block=True, timeout=self.QUEUE_TIMEOUT)
                # MutyLogger.get_instance().debug(f"added {type} message to queue for ws {ws_id}")
                return
            except queue.Full:
                # exponential backoff with jitter
                backoff_time = min(0.1 * (2**retry), 1.0) * (
                    0.8 + 0.4 * random.random()
                )

                # log the attempt with more consistent formatting
                logger.warning(
                    f"queue full for ws {ws_id}, attempt {retry+1}/{self.MAX_RETRIES}, "
                    f"backoff for {backoff_time:.2f}s"
                )

                # wait before retrying
                # TODO: maybe this should be the reason to turn this async ? hopefully we do not hit this often....
                await asyncio.sleep(backoff_time)

        # all retries failed
        queue_size = self._shared_q.qsize() if self._shared_q else "unknown"
        logger.error(
            f"failed to add {type} message to queue for ws {ws_id} after "
            f"{self.MAX_RETRIES} attempts (queue size: {queue_size})"
        )
        raise WsQueueFullException(f"queue full for ws {ws_id}")
