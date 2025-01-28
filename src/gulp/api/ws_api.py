import asyncio
import collections
from enum import StrEnum
from multiprocessing.managers import SyncManager
import os
from queue import Empty, Queue
import threading
import time
from typing import Any, Optional
from muty.pydantic import autogenerate_model_example_by_class
import muty
from fastapi import WebSocket, WebSocketDisconnect
from muty.log import MutyLogger
from pydantic import BaseModel, ConfigDict, Field
import queue
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.opensearch.structs import GulpDocument
from gulp.api.collab.structs import GulpCollabType, GulpRequestStatus
from gulp.api.rest.test_values import (
    TEST_CONTEXT_ID,
    TEST_INDEX,
    TEST_OPERATION_ID,
    TEST_REQ_ID,
    TEST_SOURCE_ID,
    TEST_WS_ID,
)
from gulp.config import GulpConfig
import asyncio
from fastapi.websockets import WebSocketState
import muty.time

from gulp.structs import GulpPluginParameters


class GulpWsType(StrEnum):
    # the type of the websocket
    WS_DEFAULT = "default"
    WS_INGEST = "ingest"
    WS_CLIENT_DATA = "client_data"


class GulpWsQueueDataType(StrEnum):
    """
    The type of data into the websocket queue.
    """

    # GulpWsErrorPacket
    WS_ERROR = "ws_error"
    # GulpWsAcknowledgedPacket
    WS_CONNECTED = "ws_connected"
    # GulpCollabCreateUpdatePacket
    STATS_UPDATE = "stats_update"
    # GulpCollabCreateUpdatePacket
    COLLAB_UPDATE = "collab_update"
    # GulpUserLoginLogoutPacket
    USER_LOGIN = "user_login"
    # GulpUserLoginLogoutPacket
    USER_LOGOUT = "user_logout"
    # a GulpDocumentsChunk to indicate a chunk of documents during ingest or query operation
    DOCUMENTS_CHUNK = "docs_chunk"
    # GulpCollabDeletePacket
    COLLAB_DELETE = "collab_delete"
    # GulpIngestSourceDonePacket to indicate a source has been ingested
    INGEST_SOURCE_DONE = "ingest_source_done"
    # GulpQueryDonePacket
    QUERY_DONE = "query_done"
    # GulpQueryDonePacket
    ENRICH_DONE = "enrich_done"
    # GulpQueryGroupMatch
    QUERY_GROUP_MATCH = "query_group_match"
    # GulpRebaseDonePacket
    REBASE_DONE = "rebase_done"
    # GulpClientDataPacket
    CLIENT_DATA = "client_data"


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


class GulpWsAcknowledgedPacket(BaseModel):
    """
    Represents a connection acknowledged event on the websocket.
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [{"ws_id": "the_ws_id", "token": "the_user_token"}]
        }
    )
    token: str = Field(..., description="The user token.")


class GulpCollabDeletePacket(BaseModel):
    """
    Represents a delete collab event.
    """

    model_config = ConfigDict(
        json_schema_extra={"examples": [{"id": "the id"}]})
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
        json_schema_extra={
            "examples": [
                {
                    "req_id": TEST_REQ_ID,
                    "status": GulpRequestStatus.DONE,
                    "name": "the query name",
                    "total_hits": 100,
                    "error": None,
                }
            ]
        }
    )
    name: Optional[str] = Field(None, description="The query name.")
    status: GulpRequestStatus = Field(
        ..., description="The status of the query operation (done/failed)."
    )
    error: Optional[str] = Field(
        None, description="The error message, if any.")
    total_hits: Optional[int] = Field(
        None, description="The total number of hits for the query."
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
        json_schema_extra={
            "examples": [
                {
                    "data": {
                        "id": "the id",
                        "name": "the name",
                        "type": GulpCollabType.NOTE,
                        "something": "else",
                    },
                    "bulk": True,
                    "bulk_size": 100,
                    "type": GulpCollabType.NOTE,
                    "created": True,
                }
            ]
        }
    )
    data: list | dict = Field(..., description="The created or updated data.")
    bulk: Optional[bool] = Field(
        default=False, description="If the event is a bulk event (data is a list instead of dict)."
    )
    type: Optional[str] = Field(
        None, description="Type of the event (may be a GulpCollabType or an arbitrary string)."
    )
    bulk_size: Optional[int] = Field(
        None, description="The size of the bulk event.")
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
                    "docs": [autogenerate_model_example_by_class(GulpDocument)],
                    "index": TEST_INDEX,
                    "operation_id": TEST_OPERATION_ID,
                    "context_name": "context_name",
                    "source": "source_name",
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

    docs: list[dict] = Field(
        ...,
        description="the GulpDocument dictionaries to be ingested.",
    )
    index: str = Field(
        ...,
        description="the Gulp index to ingest into.",
    )
    operation_id: str = Field(
        ...,
        description="the operation ID.",
    )
    context_name: str = Field(
        ...,
        description="name of the context to associate data with.",
    )
    source: str = Field(...,
                        description="name of the source to associate data with.")
    ws_id: str = Field(
        ...,
        description="id of the websocket to stream ingest data to.",
    )
    req_id: str = (Field(..., description="id of the request"),)
    flt: Optional[GulpIngestionFilter] = Field(
        0,
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
                    "type": [GulpWsQueueDataType.DOCUMENTS_CHUNK],
                }
            ]
        }
    )
    token: str = Field(
        ...,
        description="""user token. 
        
        - `monitor` is a special token used to also monitor users login/logout.
    """,
    )
    ws_id: str = Field(..., description="the WebSocket ID.")
    operation_ids: Optional[list[str]] = Field(
        None,
        description="the `operation_id`/s this websocket is registered to receive data for, defaults to `None` (all).",
    )
    types: Optional[list[GulpWsQueueDataType]] = Field(
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
    type: GulpWsQueueDataType = Field(
        ..., description="The type of data carried by the websocket."
    )
    ws_id: str = Field(..., description="The WebSocket ID.")
    user_id: Optional[str] = Field(
        None, description="The user who issued the request.")
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
    data: Optional[Any] = Field(
        None, description="The data carried by the websocket.")


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
    Represents a connected websocket.
    """

    def __init__(
        self,
        ws: WebSocket,
        ws_id: str,
        types: list[GulpWsQueueDataType] = None,
        operation_ids: list[str] = None,
        socket_type: GulpWsType = GulpWsType.WS_DEFAULT,
    ):
        """
        Initializes the ConnectedSocket object.

        Args:
            ws (WebSocket): The WebSocket object.
            ws_id (str): The WebSocket ID.
            types (list[GulpWsQueueDataType], optional): The types of data this websocket is interested in. Defaults to None (all).
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
                self._send_loop(), name=f"send_loop_{self.ws_id}"
            )
            self.receive_task = asyncio.create_task(
                self._receive_loop(), name=f"receive_loop_{self.ws_id}"
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
                MutyLogger.get_instance().error(
                    f"error in {task.get_name()}: {ex}")
                raise

        finally:
            # ensure cleanup happens even if cancelled
            MutyLogger.get_instance().debug("ensuring ws cleanup!")
            self._msg_pool.clear()

            # empty the queue
            await self.flush_queue()

            # clear tasks
            await asyncio.shield(self._cleanup_tasks(tasks))

    async def flush_queue(self) -> None:
        """
        flushes the websocket queue.
        """
        while self.q.qsize() != 0:
            try:
                self.q.get_nowait()
                self.q.task_done()
            except Empty:
                break
        await self.q.join()
        MutyLogger.get_instance().debug("ws_id %s (id=%s) queue flushed!" %
                                        (self.ws_id, id(str(self.ws))))

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

        if ws_id in GulpProcess.get_instance().shared_ws_list:
            return True
        return False

    async def _cleanup_tasks(self, tasks: list[asyncio.Task]) -> None:
        """
        clean up tasks with proper cancellation handling

        Args:
            tasks (list): the tasks to clean up
        """
        for task in tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except WebSocketDisconnect:
                    pass
                except Exception as ex:
                    MutyLogger.get_instance().error(
                        f"error cleaning up {task.get_name()}: {ex}"
                    )

        # remove from global ws list
        from gulp.process import GulpProcess

        try:
            GulpProcess.get_instance().shared_ws_list.remove(self.ws_id)
        except ValueError:
            pass

    async def _receive_loop(self) -> None:
        """
        continuously receives messages to detect disconnection
        """
        while True:
            if self.ws.client_state != WebSocketState.CONNECTED:
                raise WebSocketDisconnect("client disconnected")

            # this will raise WebSocketDisconnect when client disconnects
            await self.ws.receive_json()

            # TODO: handle incoming messages from the client

    async def put_message(self, msg: dict) -> None:
        """
        Puts a message into the websocket queue.

        Args:
            msg (dict): The message to put.
        """
        if self.q.qsize() > 1000:
            # backpressure
            await asyncio.sleep(0.1)

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

    async def _send_loop(self) -> None:
        """
        send messages from the ws queue to the websocket
        """
        try:
            MutyLogger.get_instance().debug(
                f'starting ws "{self.ws_id}" loop ...')
            ws_delay = GulpConfig.get_instance().ws_rate_limit_delay()

            while True:
                try:
                    # single message processing with timeout
                    item = await asyncio.wait_for(self.q.get(), timeout=0.1)

                    # check state before sending
                    if asyncio.current_task().cancelled():
                        raise asyncio.CancelledError()
                    if self.ws.client_state != WebSocketState.CONNECTED:
                        raise WebSocketDisconnect("client disconnected")

                    # send
                    await self.send_json(item, ws_delay)
                    self.q.task_done()

                except asyncio.TimeoutError:
                    if asyncio.current_task().cancelled():
                        raise asyncio.CancelledError()
                    continue

        except asyncio.CancelledError:
            MutyLogger.get_instance().debug(
                f'ws "{self.ws_id}" send loop cancelled')
            raise
        except WebSocketDisconnect as ex:
            MutyLogger.get_instance().debug(
                f'ws "{self.ws_id}" disconnected: {ex}')
            raise
        except Exception as ex:
            MutyLogger.get_instance().error(f'ws "{self.ws_id}" error: {ex}')
            raise

    def __str__(self):
        return f"ConnectedSocket(ws_id={self.ws_id}, registered_types={self.types}, registered_operations={self.operation_ids})"


class GulpConnectedSockets:
    """
    singleton class to hold all connected sockets
    """

    def __init__(self):
        raise RuntimeError("call get_instance() instead")

    @classmethod
    def get_instance(cls) -> "GulpConnectedSockets":
        """
        returns the singleton instance
        """
        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls)
            cls._instance._initialize()

        return cls._instance

    def _initialize(self):
        if not hasattr(self, "_initialized"):
            self._initialized = True
            self._sockets: dict[str, GulpConnectedSocket] = {}

    def add(
        self,
        ws: WebSocket,
        ws_id: str,
        types: list[GulpWsQueueDataType] = None,
        operation_ids: list[str] = None,
        socket_type: GulpWsType = GulpWsType.WS_DEFAULT,
    ) -> GulpConnectedSocket:
        """
        Adds a websocket to the connected sockets list.

        Args:
            ws (WebSocket): The WebSocket object.
            ws_id (str): The WebSocket ID.
            types (list[GulpWsQueueDataType], optional): The types of data this websocket is interested in. Defaults to None (all)
            operation_ids (list[str], optional): The operations this websocket is interested in. Defaults to None (all)
            socket_type (GulpWsType, optional): The type of the websocket. Defaults to GulpWsType.WS_DEFAULT.
        Returns:
            ConnectedSocket: The ConnectedSocket object.
        """
        wws = GulpConnectedSocket(
            ws=ws,
            ws_id=ws_id,
            types=types,
            operation_ids=operation_ids,
            socket_type=socket_type,
        )
        self._sockets[str(id(ws))] = wws

        # also add to the global list
        from gulp.process import GulpProcess

        GulpProcess.get_instance().shared_ws_list.append(ws_id)
        MutyLogger.get_instance().debug(
            f"added connected ws: {wws}, len={len(self._sockets)}"
        )
        return wws

    async def remove(self, ws: WebSocket, flush: bool = True) -> None:
        """
        Removes a websocket from the connected sockets list

        Args:
            ws (WebSocket): The WebSocket object.
            flush (bool, optional): If the queue should be flushed. Defaults to True.

        """
        id_str = str(id(ws))
        cws = self._sockets.get(id_str, None)
        if not cws:
            MutyLogger.get_instance().warning(
                f"remove(): no websocket found for ws_id={
                    id_str}, len={len(self._sockets)}"
            )
            return

        # flush queue first
        if flush:
            await cws.flush_queue()

        # remove from global ws list
        from gulp.process import GulpProcess

        while cws.ws_id in GulpProcess.get_instance().shared_ws_list:
            GulpProcess.get_instance().shared_ws_list.remove(cws.ws_id)

        del self._sockets[id_str]

        MutyLogger.get_instance().debug(
            f"removed connected ws, id={id_str}, len={len(self._sockets)}"
        )

    def find(self, ws_id: str) -> GulpConnectedSocket:
        """
        Finds a ConnectedSocket object by its ID.

        Args:
            ws_id (str): The WebSocket ID.

        Returns:
            ConnectedSocket: The ConnectedSocket object or None if not found.
        """
        for _, v in self._sockets.items():
            if v.ws_id == ws_id:
                return v

        MutyLogger.get_instance().warning(
            f"no websocket found for ws_id={ws_id}, len={len(self._sockets)}"
        )
        return None

    async def cancel_all(self) -> None:
        """
        Waits for all active websockets to close.
        """
        # cancel all tasks
        for _, cws in self._sockets.items():
            MutyLogger.get_instance().debug("canceling ws %s..." % (cws.ws_id))
            cws.receive_task.cancel()
            cws.send_task.cancel()

        MutyLogger.get_instance().debug(
            "all active websockets closed, len=%d!" % (len(self._sockets))
        )

    async def _route_message(
        self, data: GulpWsData, client_ws: GulpConnectedSocket
    ) -> None:
        """
        routes the message to the target websocket

        the message is routed only if:

        - client ws is not an ingest ws AND
        - message is not CLIENT_DATA (they are always routed) AND
        - the message type is in the target websocket types AND
        - the message operation_id is in the target websocket operation_ids AND
        - private messages are only routed to the target websocket
        - collab updates are only relayed to other websockets

        Args:
            data (GulpWsData): The data to route.
            client_ws (ConnectedSocket): The target websocket.

        """
        if client_ws.socket_type != GulpWsType.WS_DEFAULT:
            return

        # if types is set, only route if it matches
        if client_ws.types and data.type not in client_ws.types:
            # MutyLogger.get_instance().warning(f"skipping entry type={data.type} for ws_id={client_ws.ws_id}, types={client_ws.types}")
            return

        # if operation_id is set, only route if it matches
        if client_ws.operation_ids and data.operation_id not in client_ws.operation_ids:
            # MutyLogger.get_instance().warning(f"skipping entry type={data.type} for ws_id={client_ws.ws_id}, operation_ids={client_ws.operation_ids}")
            return

        # private messages are only routed to the target websocket (except login, always public)
        if (
            data.private and data.type != GulpWsQueueDataType.USER_LOGIN
        ) and client_ws.ws_id != data.ws_id:
            # MutyLogger.get_instance().warning(f"skipping private entry type={data.type} for ws_id={client_ws.ws_id}")
            return

        if data.type not in [
            GulpWsQueueDataType.COLLAB_UPDATE,
            GulpWsQueueDataType.COLLAB_DELETE,
            GulpWsQueueDataType.REBASE_DONE,
            GulpWsQueueDataType.USER_LOGIN,
            GulpWsQueueDataType.USER_LOGOUT,
        ]:
            # for data types not in this list, only relay to the ws_id of the client
            if client_ws.ws_id != data.ws_id:
                # MutyLogger.get_instance().warning(f"skipping entry type={data.type} for ws_id={client_ws.ws_id}")
                return

        # send the message
        message = data.model_dump(
            exclude_none=True, exclude_defaults=True, by_alias=True
        )
        await client_ws.put_message(message)

    async def broadcast_data(self, d: GulpWsData, skip_list: list[str] = None) -> None:
        """
        broadcasts data to all connected websockets

        Args:
            d (GulpWsData): The data to broadcast.
            skip_list (list[str], optional): The list of websocket IDs to skip. Defaults to None.
        """
        # Create copy of sockets dict
        socket_items = list(self._sockets.items())

        for ws_id, cws in socket_items:
            try:
                if skip_list and ws_id in skip_list:
                    continue

                await self._route_message(d, cws)

            except Exception as ex:
                MutyLogger.get_instance().warning(
                    f"Failed to broadcast to {ws_id}: {str(ex)}"
                )
                # remove dead socket
                self._sockets.pop(ws_id, None)
                continue


class GulpSharedWsQueue:
    """
    singleton class to manage adding data to the shared websocket queue
    """

    def __init__(self):
        raise RuntimeError("call get_instance() instead")

    @classmethod
    def get_instance(cls) -> "GulpSharedWsQueue":
        """
        Returns the singleton instance.

        Returns:
            GulpSharedWsDataQueue: The singleton instance.
        """
        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        if not hasattr(self, "_initialized"):
            self._initialized = True
            self._shared_q: Queue = None
            self._fill_task: asyncio.Task = None
            self.MAX_QUEUE_SIZE = 1000
            self.QUEUE_TIMEOUT = 30
            self.MAX_RETRIES = 3

    def set_queue(self, q: Queue) -> None:
        """
        Sets the shared queue.

        Args:
            q (Queue): The shared queue.
        """
        from gulp.process import GulpProcess

        if GulpProcess.get_instance().is_main_process():
            raise RuntimeError(
                "set_queue() must be called in a worker process")

        MutyLogger.get_instance().debug(
            "setting shared ws queue in worker process: q=%s" % (q)
        )
        self._shared_q = q

    def init_queue(self, mgr: SyncManager) -> Queue:
        """
        to be called by the MAIN PROCESS, initializes the shared multiprocessing queue: the same queue must then be passed to the worker processes

        Args:
            q (Queue): The shared queue created by the multiprocessing manager in the main process
        """
        from gulp.process import GulpProcess

        if not GulpProcess.get_instance().is_main_process():
            raise RuntimeError(
                "init_queue() must be called in the main process")

        if self._shared_q:
            # close first
            MutyLogger.get_instance().debug("closing shared ws queue ...")
            self.close()

        MutyLogger.get_instance().debug("re/initializing shared ws queue ...")
        self._shared_q = mgr.Queue()
        self._fill_task = asyncio.create_task(
            self._fill_ws_queues_from_shared_queue())

        return self._shared_q

    async def _fill_ws_queues_from_shared_queue(self):
        """
        Processes messages from shared queue with guaranteed delivery
        """
        from gulp.api.rest_api import GulpRestServer

        BATCH_SIZE = 100
        BATCH_TIMEOUT = 0.1
        MAX_RETRIES = 3
        MAX_QUEUE_SIZE = 10000
        
        logger = MutyLogger.get_instance()
        logger.debug("Starting queue processing task...")

        try:
            messages = []
            while not GulpRestServer.get_instance().is_shutdown():
                # Backpressure check
                if self._shared_q.qsize() > MAX_QUEUE_SIZE:
                    await asyncio.sleep(0.1)
                    continue

                batch_start = time.monotonic()

                # Collect messages
                while (len(messages) < BATCH_SIZE and 
                    (time.monotonic() - batch_start) < BATCH_TIMEOUT):
                    try:
                        entry = self._shared_q.get_nowait()
                        messages.append(entry)
                        self._shared_q.task_done()
                    except Empty:
                        break

                # Process collected messages
                for msg in messages:
                    retries = 0
                    while retries < MAX_RETRIES:
                        try:
                            cws = GulpConnectedSockets.get_instance().find(msg.ws_id)
                            if cws:
                                await GulpConnectedSockets.get_instance().broadcast_data(msg)
                            break
                        except Exception as e:
                            retries += 1
                            if retries == MAX_RETRIES:
                                logger.error(f"Failed to process message after {MAX_RETRIES} retries: {e}")
                            await asyncio.sleep(0.1 * retries)

                messages.clear()
                await asyncio.sleep(0)

        except asyncio.CancelledError:
            logger.info("Queue processing cancelled")
        except Exception as e:
            logger.error(f"Queue processing error: {e}")
            raise
        finally:
            # Process remaining messages
            while messages:
                try:
                    msg = messages.pop()
                    cws = GulpConnectedSockets.get_instance().find(msg.ws_id)
                    if cws:
                        await GulpConnectedSockets.get_instance().broadcast_data(msg)
                except Exception as e:
                    logger.error(f"Cleanup error: {e}")

    def close(self) -> None:
        """
        Closes the shared multiprocessing queue (flushes it first).

        Returns:
            None
        """
        MutyLogger.get_instance().debug("closing shared ws queue ...")

        # flush queue first
        while self._shared_q.qsize() != 0:
            try:
                self._shared_q.get_nowait()
                self._shared_q.task_done()
            except Exception:
                pass

        self._shared_q.join()
        MutyLogger.get_instance().debug("shared queue flush done.")

        # and also kill the task
        if self._fill_task:
            b = self._fill_task.cancel()
            MutyLogger.get_instance().debug(
                "cancelling shared queue fill task done: %r" % (b)
            )

    def _cleanup_stale_messages(self):
        """
        remove old messages if queue size exceeds limit
        """
        cleaned = 0
        while self._shared_q.qsize() > self.MAX_QUEUE_SIZE:
            try:
                self._shared_q.get_nowait()
                cleaned += 1
            except queue.Empty:
                break
        if cleaned > 0:
            MutyLogger.get_instance().info(
                f"cleaned {cleaned} stale messages from queue"
            )

    def put(
        self,
        type: GulpWsQueueDataType,
        ws_id: str,
        user_id: str,
        operation_id: str = None,
        req_id: str = None,
        data: Any = None,
        private: bool = False,
    ) -> None:
        """
        Adds data to the shared queue.

        Args:
            type (GulpWsQueueDataType): The type of data.
            user (str): The user.
            ws_id (str): The WebSocket ID.
            operation (str, optional): The operation.
            req_id (str, optional): The request ID. Defaults to None.
            data (any, optional): The data. Defaults to None.
            private (bool, optional): If the data is private. Defaults to False.
        """
        if type == GulpWsQueueDataType.DOCUMENTS_CHUNK:
            # allow to interrupt lenghty processes if the websocket is dead
            if not GulpConnectedSocket.is_alive(ws_id):
                raise WebSocketDisconnect(
                    "websocket '%s' is not connected!" % (ws_id))

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
        retries = 0
        while retries < self.MAX_RETRIES:
            try:
                self._shared_q.put(wsd, block=True, timeout=self.QUEUE_TIMEOUT)
                return
            except queue.Full:
                MutyLogger.get_instance().warning(
                    f"queue full for ws {ws_id}, attempt {
                        retries + 1}/{self.MAX_RETRIES}"
                )
                self._cleanup_stale_messages()
                retries += 1

        # if we get here, all retries failed
        MutyLogger.get_instance().error(
            f"failed to add message to queue for ws {
                ws_id} after {self.MAX_RETRIES} attempts"
        )
        raise WsQueueFullException(f"queue full for ws {ws_id}")
