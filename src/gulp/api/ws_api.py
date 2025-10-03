import asyncio
import collections
from datetime import time
import queue
from enum import StrEnum
from multiprocessing.managers import SyncManager
from queue import Empty, Queue
from typing import Any, Optional, Annotated

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
from gulp.config import GulpConfig
from gulp.structs import GulpPluginParameters
import time


class GulpWsType(StrEnum):
    # the type of the websocket
    WS_DEFAULT = "default"
    WS_INGEST = "ingest"
    WS_CLIENT_DATA = "client_data"


# data types for the websocket
WSDATA_ERROR = "ws_error"
WSDATA_CONNECTED = "ws_connected"  # whenever a websocket connection is established
WSDATA_COLLAB_CREATE = (
    "collab_create"  # GulpCollabCreatePacket, whenever a collab object is created
)
WSDATA_COLLAB_UPDATE = (
    "collab_update"  # GulpCollabUpdatePacket, whenever a collab object is updated
)
WSDATA_COLLAB_DELETE = (
    "collab_delete"  # GulpCollabDeletePacket, whenever a collab object is deleted
)
WSDATA_QUERY_GROUP_MATCH = "query_group_match"  # GulpQueryGroupMatchPacket, this is sent to indicate a query group match, i.e. a query group that matched some queries
WSDATA_INGEST_SOURCE_DONE = "ingest_source_done"  # GulpIngestSourceDonePacket, this is sent in the end of an ingestion operation, one per source
WSDATA_PROGRESS_REBASE = (
    "progress_rebase"  # GulpProgressPacket.type type for rebase operations
)

WSDATA_USER_LOGIN = "user_login"
WSDATA_USER_LOGOUT = "user_logout"
WSDATA_DOCUMENTS_CHUNK = "docs_chunk"
WSDATA_CLIENT_DATA = "client_data"
WSDATA_SOURCE_FIELDS_CHUNK = "source_fields_chunk"
WSDATA_GENERIC = "generic"

# the following data types sent on the websocket are to be used to track status
WSDATA_QUERY_DONE = "query_done"  # this is sent in the end of a query operation, one per single query (i.e. a sigma zip query may generate multiple single queries, called a query group)
WSDATA_QUERY_GROUP_DONE = "query_group_done"  # this is sent in the end of the query task, being it single or group(i.e. sigma) query
WSDATA_ENRICH_DONE = "enrich_done"  # this is sent in the end of an enrichment operation
WSDATA_TAG_DONE = "tag_done"  # this is sent in the end of a tag operation


# special token used to monitor also logins
WSTOKEN_MONITOR = "monitor"

SHARED_MEMORY_KEY_ACTIVE_SOCKETS = "active_sockets"


class GulpWsData(BaseModel):
    """
    data carried by the websocket ui<->gulp
    """

    model_config = ConfigDict(
        # solves the issue of not being able to populate fields using field name instead of alias
        populate_by_name=True,
    )
    timestamp: Annotated[
        int, Field(description="The timestamp of the data.", alias="@timestamp")
    ]
    type: Annotated[
        str,
        Field(
            description="The type of data carried by the websocket, see WSDATA_* constants.",
        ),
    ]
    private: Annotated[
        bool,
        Field(
            description="If the data is private, only the websocket `ws_id` receives it. Ignored if `internal` is set.",
        ),
    ] = False
    internal: Annotated[
        bool,
        Field(
            description="set to broadcast internal events to plugins registered through `GulpPluginBase.register_internal_events_callback()`.",
        ),
    ] = False
    payload: Annotated[
        Optional[Any], Field(description="The data carried by the websocket.")
    ] = None
    ws_id: Annotated[
        Optional[str],
        Field(
            description="The target WebSocket ID, ignored if `internal` is set. if None, the message is broadcasted to all connected websockets.",
        ),
    ] = None
    user_id: Annotated[
        Optional[str],
        Field(
            description="The user who issued the request, ignored if `internal` is set.",
        ),
    ] = None
    req_id: Annotated[
        Optional[str],
        Field(description="The request ID, ignored if `internal` is set."),
    ] = None
    operation_id: Annotated[
        Optional[str],
        Field(
            description="The operation this data belongs to, ignored if `internal` is set.",
            alias="gulp.operation_id",
        ),
    ] = None


class GulpCollabCreatePacket(BaseModel):
    """
    Represents a create event (WsData.type = WSDATA_COLLAB_CREATE).
    """

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "obj": {
                        "id": "the id",
                        "name": "the name",
                        "type": "note",
                        "something": "else",
                    },
                    "bulk": True,
                    "last": False,
                }
            ]
        },
    )
    obj: Annotated[
        list[dict] | dict,
        Field(
            description="The created object (or bulk of objects): the object `type` is `obj.type` or `obj[0].type` for bulk objects.",
        ),
    ]
    bulk: Annotated[
        bool,
        Field(
            description="indicates if `obj` is a bulk of objects (list) or a single object (dict).",
        ),
    ] = False
    last: Annotated[
        bool,
        Field(
            description="for bulk operations, indicates if this is the last chunk of a bulk operation.",
        ),
    ] = False
    total_size: Annotated[
        int,
        Field(
            description="for bulk operations, indicates the total size of the bulk operation, if known.",
        ),
    ] = 0
    bulk_size: Annotated[
        int,
        Field(
            description="for bulk operations, indicates the size of this bulk chunk.",
        ),
    ] = 0


class GulpCollabUpdatePacket(BaseModel):
    """
    Represents an update event (WsData.type = WSDATA_COLLAB_UPDATE).
    """

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "obj": {
                        "id": "the id",
                        "name": "the name",
                        "type": "note",
                        "something": "else",
                    }
                }
            ]
        },
    )
    obj: Annotated[
        dict, Field(description="The updated object: the object `type` is `obj.type`.")
    ]


class GulpCollabDeletePacket(BaseModel):
    """
    Represents a delete collab event (WsData.type = WSDATA_COLLAB_DELETE).
    """

    model_config = ConfigDict(json_schema_extra={"examples": [{"id": "the id"}]})
    id: Annotated[str, Field(description="The deleted collab object ID.")]


class WsQueueFullException(Exception):
    """Exception raised when queue is full after retries"""


class GulpUserLoginLogoutPacket(BaseModel):
    """
    Represents a user login or logout event.
    """

    model_config = ConfigDict(
        json_schema_extra={"examples": [{"user_id": "admin", "login": True}]}
    )
    user_id: Annotated[str, Field(description="The user ID.")]
    login: Annotated[
        bool, Field(description="If the event is a login event, either it is a logout.")
    ]
    ip: Annotated[
        Optional[str],
        Field(
            None,
            description="The IP address of the user, if available.",
        ),
    ] = None


class GulpWsAcknowledgedPacket(BaseModel):
    """
    Represents a connection acknowledged event on the websocket.
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [{"ws_id": "the_ws_id", "token": "the_user_token"}]
        }
    )
    ws_id: Annotated[str, Field(description="The WebSocket ID.")]
    token: Annotated[str, Field(description="The user token.")]


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

    operation_id: Annotated[
        str,
        Field(
            description="the operation ID.",
        ),
    ]
    source_id: Annotated[
        str,
        Field(
            description="the source ID.",
        ),
    ]
    context_id: Annotated[
        str,
        Field(
            description="the context ID.",
        ),
    ]
    fields: Annotated[
        dict,
        Field(
            description="mappings for the fields in a source.",
        ),
    ]
    last: Annotated[
        bool,
        Field(
            description="if this is the last chunk.",
        ),
    ] = False


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
    q_group: Annotated[str, Field(description="The query group name.")]
    q_matched: Annotated[int, Field(description="The number of queries that matched.")]
    q_total: Annotated[
        int, Field(description="The total number of queries in the group.")
    ]


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
    name: Annotated[str, Field(description="The query name.")]
    status: Annotated[
        str, Field(description="The status of the query operation (done/failed).")
    ]
    errors: Annotated[list[str], Field(description="The error message/s, if any.")] = []
    total_hits: Annotated[
        int, Field(description="The total number of hits for the query.")
    ] = 0


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
                    "msg": "some_msg",
                }
            ]
        },
    )
    current: Annotated[int, Field(description="The current progress.")] = 0
    done: Annotated[
        bool, Field(description="If the progress is done, i.e. reached the total.")
    ] = False
    total: Annotated[int, Field(description="The total to be reached.")] = 0
    type: Annotated[
        Optional[str],
        Field(description="progress type, see PROGRESS_* constants."),
    ] = None
    data: Annotated[
        dict, Field(description="Optional extra data to send with the progress.")
    ] = {}
    canceled: Annotated[
        bool, Field(description="If the request has been canceled.")
    ] = False


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
                    "source_id": "fabae8858452af6c2acde7f90786b3de3a928289",
                    "context_id": "66d98ed55d92b6b7382ffc77df70eda37a6efaa1",
                    "req_id": "test_req",
                    "num_docs": 100,
                    "status": GulpRequestStatus.DONE,
                }
            ]
        },
    )
    source_id: Annotated[
        str,
        Field(
            description="The source ID in the collab database.",
            alias="gulp.source_id",
        ),
    ]
    context_id: Annotated[
        str,
        Field(
            description="The context ID in the collab database.",
            alias="gulp.context_id",
        ),
    ]
    status: Annotated[
        str, Field(description="The source ingestion status (failed, done, canceled).")
    ]
    records_ingested: Annotated[
        int, Field(description="The number of documents ingested in this source.")
    ] = 0
    records_skipped: Annotated[
        int, Field(description="The number of documents skipped in this source.")
    ] = 0
    records_failed: Annotated[
        int, Field(description="The number of documents failed in this source.")
    ] = 0


class GulpCollabGenericNotifyPacket(BaseModel):
    """
    Represents a generic notify
    """

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "data": {"value1": "vvv", "value2": 12345},
                    "type": "my_custom_type",
                }
            ]
        },
    )
    type: Annotated[
        str,
        Field(
            description="An arbitrary application specific notify type.",
        ),
    ]
    data: Annotated[str, Field(description="Some optional arbitrary data.")] = {}


class GulpWsError(StrEnum):
    """
    Used for "error_code" in GulpWsErrorPacket
    """

    OBJECT_NOT_FOUND = "Not Found"
    MISSING_PERMISSION = "Forbidden"
    ERROR_GENERIC = "Error"


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
    error: Annotated[str, Field(description="error on the websocket.")]
    error_code: Annotated[Optional[str], Field(description="optional error code.")] = (
        None
    )
    data: Annotated[dict, Field(description="optional error data")] = {}


class GulpClientDataPacket(BaseModel):
    """
    arbitrary client data sent to gulp by UI, routed to all connected websockets
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "data": {"something": "12345"},
                    "operation_id": "test_operation",
                    "target_user_ids": ["admin", "user"],
                }
            ]
        }
    )
    data: Annotated[dict, Field(description="arbitrary data")]
    operation_id: Annotated[
        Optional[str],
        Field(
            description="the operation ID this data belongs to: if not set, the data is broadcast to all connected websockets.",
        ),
    ] = None
    target_user_ids: Annotated[
        Optional[list[str]],
        Field(
            description="optional list of user IDs to send this data to only: if not set, data is broadcast to all connected websockets.",
        ),
    ] = None


class GulpWsIngestPacket(BaseModel):
    """
    Represents packets sent to /ws_ingest to ingest raw documents.
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "operation_id": "test_operation",
                    "ws_id": "test_ws",
                    "flt": autogenerate_model_example_by_class(GulpIngestionFilter),
                    "plugin": "raw",
                    "plugin_params": autogenerate_model_example_by_class(
                        GulpPluginParameters
                    ),
                }
            ]
        },
    )

    index: Annotated[
        str,
        Field(
            description="the Gulp index to ingest into.",
        ),
    ]
    operation_id: Annotated[
        str,
        Field(
            description="the operation ID.",
        ),
    ]
    ws_id: Annotated[
        str,
        Field(
            description="id of the websocket to stream ingested data to.",
        ),
    ]
    req_id: Annotated[str, Field(description="id of the request")]

    flt: Annotated[
        Optional[GulpIngestionFilter],
        Field(
            description="optional filter to apply for ingestion.",
        ),
    ] = GulpIngestionFilter()
    plugin: Annotated[
        str,
        Field(
            description="plugin to be used for ingestion (default='raw').",
        ),
    ] = "raw"
    plugin_params: Annotated[
        GulpPluginParameters,
        Field(
            description="optional plugin parameters",
        ),
    ] = None


class GulpWsAuthPacket(BaseModel):
    """
    Parameters for authentication on the websocket
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "token": "token_admin",
                    "ws_id": "test_ws",
                    "operation_id": ["test_operation"],
                    "type": [WSDATA_DOCUMENTS_CHUNK],
                }
            ]
        }
    )
    token: Annotated[
        str,
        Field(
            description="""user token.

        - `monitor` is a special token, reserved for internal use.
    """,
        ),
    ]
    ws_id: Annotated[
        Optional[str],
        Field(description="the WebSocket ID, leave empty to autogenerate."),
    ] = None
    operation_ids: Annotated[
        Optional[list[str]],
        Field(
            description="the `operation_id`/s this websocket is registered to receive data for, defaults to `None` (all).",
        ),
    ] = None
    types: Annotated[
        Optional[list[str]],
        Field(
            description="the `GulpWsData.type`/s this websocket is registered to receive, defaults to `None` (all).",
        ),
    ] = None


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

    docs: Annotated[
        list[dict],
        Field(
            description="the documents in a query or ingestion chunk.",
        ),
    ]
    num_docs: Annotated[
        int,
        Field(
            description="the number of documents in this chunk.",
        ),
    ] = 0
    chunk_number: Annotated[
        int,
        Field(
            description="the chunk number (may not be available)",
        ),
    ] = 0
    total_hits: Annotated[
        int,
        Field(
            description="for query only: the total number of hits for the query related to this chunk.",
        ),
    ] = 0
    name: Annotated[
        Optional[str],
        Field(
            description="for query only: the query name related to this chunk.",
        ),
    ] = None
    last: Annotated[
        bool,
        Field(
            description="for query only: is this the last chunk of a query response ?",
        ),
    ] = False
    enriched: Annotated[
        bool,
        Field(
            description="if the documents have been enriched.",
        ),
    ] = False


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

    YIELD_CONTROL_INTERVAL: int = 100  # yield control every 100 messages
    YIELD_CONTROL_DELAY: float = 0.01  # yield control delay in seconds

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

            # process completed task
            task = done.pop()
            try:
                await task
            except WebSocketDisconnect as ex:
                MutyLogger.get_instance().debug(
                    "websocket %s disconnected: %s", self.ws_id, ex
                )
                raise
            except Exception as ex:
                MutyLogger.get_instance().error("error in %s: %s", task.get_name(), ex)
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
            "---> cleanup, ensuring ws cleanup for ws=%s, ws_id=%s", self.ws, self.ws_id
        )

        # empty the queue
        await self._flush_queue()

        # clear the msg pool
        self._msg_pool.clear()

        if tasks:
            # clear tasks
            await asyncio.shield(self._cleanup_tasks(tasks))

        self._cleaned_up = True
        MutyLogger.get_instance().debug(
            "---> GulpConnectedSocket.cleanup() DONE, ws=%s, ws_id=%s",
            self.ws,
            self.ws_id,
        )

    async def _flush_queue(self) -> None:
        """
        flushes the websocket queue.
        """
        try:
            # drain queue
            while self.q.qsize() != 0:
                try:
                    self.q.get_nowait()
                    self.q.task_done()
                except asyncio.QueueEmpty:
                    break

            await self.q.join()
            MutyLogger.get_instance().debug(
                "---> queue flushed for ws_id=%s", self.ws_id
            )
        except Exception as ex:
            MutyLogger.get_instance().warning("error flushing queue: %s", ex)

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

        active_sockets: list[str] = GulpProcess.get_instance().shared_memory_get(
            SHARED_MEMORY_KEY_ACTIVE_SOCKETS
        )
        if not active_sockets:
            return False
        return ws_id in active_sockets

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
                    MutyLogger.get_instance().debug(
                        "task %s cancelled successfully", task.get_name()
                    )
                except WebSocketDisconnect:
                    MutyLogger.get_instance().debug(
                        "task %s disconnected during cleanup", task.get_name()
                    )
                except Exception as ex:
                    MutyLogger.get_instance().error(
                        "error cleaning up %s: %s", task.get_name(), str(ex)
                    )

        # remove from global ws list
        from gulp.process import GulpProcess

        MutyLogger.get_instance().debug(
            "removing ws_id=%s from active sockets shared list ...", self.ws_id
        )
        GulpProcess.get_instance().shared_memory_remove_from_list(
            SHARED_MEMORY_KEY_ACTIVE_SOCKETS, self.ws_id
        )

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
        get and send a message from the queue to the websocket with timeout and cancellation handling

        returns:
            bool: true if message was processed, false if timeout occurred
        """
        try:
            # get message with timeout
            item = await asyncio.wait_for(self.q.get(), timeout=0.1)

            # check state before sending
            current_task = asyncio.current_task()
            if current_task and current_task.cancelled():
                raise asyncio.CancelledError()

            # raise exception if websocket is disconnected
            self.validate_connection()

            # connection is verified, send the message
            await self.ws.send_json(item)
            await asyncio.sleep(GulpConfig.get_instance().ws_rate_limit_delay())

            self.q.task_done()
            return True

        except asyncio.TimeoutError:
            # no messages, check for cancellation during timeout
            current_task = asyncio.current_task()
            if current_task and current_task.cancelled():
                raise asyncio.CancelledError()
            return False

    async def _receive_loop(self) -> None:
        """
        continuously receives messages to detect disconnection
        """
        while True:
            # raise exception if websocket is disconnected
            self.validate_connection()

            try:
                # use a shorter timeout to ensure we can yield control regularly
                await asyncio.wait_for(self.ws.receive_json(), timeout=1.0)
            except asyncio.TimeoutError:
                # no message received
                continue
            finally:
                await asyncio.sleep(GulpConnectedSocket.YIELD_CONTROL_DELAY)

    async def _send_loop(self) -> None:
        """
        processes outgoing messages (filled by the shared queue) and sends them to the websocket
        """
        logger = MutyLogger.get_instance()

        try:
            logger.debug(f"---> starting send loop for ws_id={self.ws_id}")

            # tracking variables for metrics
            message_count: int = 0
            yield_counter: int = 0

            while True:
                # process message from the websocket queue (filled by the shared queue)
                message_processed = await self._process_queue_message()

                if message_processed:
                    message_count += 1
                    yield_counter += 1

                    # yield control periodically
                    if yield_counter % self.YIELD_CONTROL_INTERVAL == 0:
                        yield_counter = 0
                        await asyncio.sleep(self.YIELD_CONTROL_DELAY)

        except asyncio.CancelledError:
            logger.warning("---> send loop canceled for ws_id=%s", self.ws_id)
            raise

        except WebSocketDisconnect as ex:
            logger.warning("---> websocket disconnected: ws_id=%s", self.ws_id)
            logger.exception(ex)
            raise

        except Exception as ex:
            logger.error("---> error in send loop: ws_id=%s", self.ws_id)
            logger.exception(ex)
            raise

    def __str__(self):
        return f"ConnectedSocket(ws_id={self.ws_id}, types={self.types}, operation_ids={self.operation_ids})"


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

        GulpProcess.get_instance().shared_memory_add_to_list(
            SHARED_MEMORY_KEY_ACTIVE_SOCKETS, ws_id
        )
        self._logger.debug(
            "---> added connected ws: %s, id_str=%s, ws_id=%s, len=%d",
            ws,
            socket_id,
            ws_id,
            len(self._sockets),
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
        self._logger.debug("---> remove, ws=%s, id_str=%s", ws, socket_id)

        connected_socket = self._sockets.get(socket_id)
        if not connected_socket:
            self._logger.warning(
                "remove(): no websocket found for ws=%s, id_str=%s, len=%d",
                ws,
                socket_id,
                len(self._sockets),
            )

            return

        # flush queue if requested
        if flush:
            await connected_socket.cleanup()

        # remove from global ws list
        from gulp.process import GulpProcess

        ws_id = connected_socket.ws_id
        ws_list: list[str] = GulpProcess.get_instance().shared_memory_get(
            SHARED_MEMORY_KEY_ACTIVE_SOCKETS
        )
        while True:
            if ws_id in ws_list:
                GulpProcess.get_instance().shared_memory_remove_from_list(
                    SHARED_MEMORY_KEY_ACTIVE_SOCKETS, ws_id
                )
            else:
                break

            # next
            ws_list = GulpProcess.get_instance().shared_memory_get(
                SHARED_MEMORY_KEY_ACTIVE_SOCKETS
            )

        # remove from internal maps
        del self._sockets[socket_id]
        if ws_id in self._ws_id_map:
            del self._ws_id_map[ws_id]

        self._logger.debug(
            "---> removed connected ws=%s, id=%s, ws_id=%s, len=%d",
            ws,
            socket_id,
            ws_id,
            len(self._sockets),
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
                "---> canceling ws=%s, ws_id=%s",
                connected_socket.ws,
                connected_socket.ws_id,
            )
            if connected_socket.receive_task:
                connected_socket.receive_task.cancel()
            if connected_socket.send_task:
                connected_socket.send_task.cancel()

        self._logger.debug(
            "---> all active websockets cancelled, count=%d", len(sockets_to_cancel)
        )

    def num_connected_sockets(self, default_sockets_only: bool = True) -> int:
        """
        returns the number of currently connected sockets

        Args:
            default_sockets_only (bool): if True, counts only WS_DEFAULT sockets (always fixed 1 per client)

        Returns:
            int: the number of connected sockets
        """
        if default_sockets_only:
            return sum(
                1
                for s in self._sockets.values()
                if s.socket_type == GulpWsType.WS_DEFAULT
            )

        return len(self._sockets)

    async def _route_message(
        self, data: GulpWsData, target_ws: GulpConnectedSocket
    ) -> bool:
        """
        determines if a message should be routed to the target websocket and sends it if appropriate

        the message is routed if:
        - the target_ws is of type WS_DEFAULT
        - the target_ws types is None or includes data.type
        - the target_ws operation_ids is None or includes data.operation_id
        - if data.private is True, message is routed if the target_ws.ws_id matches data.ws_id
        - if data.type is not in the broadcast_types, message is routed if either data.ws_id is None (broadcast to all) or matches target_ws.ws_id

        Args:
            data (GulpWsData): the data to route
            target_ws (GulpConnectedSocket): the target websocket

        Returns:
            bool: True if message was routed, False otherwise
        """

        # route only to WS_DEFAULT sockets: these are the sockets used by the UI for receiving most of the data (collab objects and chunks), corresponding to the ws_id in most of the API calls
        if target_ws.socket_type != GulpWsType.WS_DEFAULT:
            return False

        # check type filter
        if target_ws.types and data.type not in target_ws.types:
            return False

        # check operation filter
        if target_ws.operation_ids and data.operation_id not in target_ws.operation_ids:
            return False

        # handle private messages
        if data.private and data.type != WSDATA_USER_LOGIN:
            if target_ws.ws_id != data.ws_id:
                return False

        # check message distribution rules (broadcast only the specified types or if ws_id matches)
        if data.type not in GulpWsSharedQueue.get_instance().broadcast_types and (
            data.ws_id and target_ws.ws_id != data.ws_id
        ):
            return False

        # all checks passed, send the message
        message = data.model_dump(
            exclude_none=True, exclude_defaults=True, by_alias=True
        )
        # MutyLogger.get_instance().debug(
        #     "routing message to ws_id=%s: %s", target_ws.ws_id, message
        # )
        await target_ws.q.put(message)
        return True

    async def _route_internal_message(self, data: GulpWsData) -> None:
        """
        route message NOT to connected clients BUT to the plugins which registered for it

        Args:
            data (GulpWsData): The internal message to process.
        """
        # self._logger.debug(f"processing internal message: {data}")
        from gulp.plugin import GulpInternalEventsManager

        # MutyLogger.get_instance().debug(
        #     "routing internal message: type=%s, data=%s", data.type, data.payload
        # )
        await GulpInternalEventsManager.get_instance().broadcast_event(
            data.type,
            data=data.payload,
            user_id=data.user_id,
            operation_id=data.operation_id,
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
        # MutyLogger.get_instance().debug(
        #     "******************* broadcasting message: type=%s, data=%s, skip_list=%s",
        #     data.type,
        #     data.payload,
        #     skip_list,
        # )

        if data.internal:
            # this is an internal message, route to plugins
            # MutyLogger.get_instance().debug(
            #     "broadcast_message(): routing internal message: type=%s, data=%s",
            #     data.type,
            #     data.payload,
            # )
            await self._route_internal_message(data)
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
                    "failed to broadcast to socket %s: %s", socket_id, ex
                )
                dead_sockets.append(socket_id)

        # clean up dead sockets
        for socket_id in dead_sockets:
            self._logger.warning("removing dead socket %s", socket_id)
            self._sockets.pop(socket_id, None)


class GulpWsSharedQueue:
    """
    Singleton class to manage a shared websocket queue between processes.
    Provides methods for adding data to the queue and processing messages.
    """

    _instance: "GulpWsSharedQueue" = None

    MAX_QUEUE_SIZE = 1000
    QUEUE_TIMEOUT = 30
    MAX_RETRIES = 3
    BATCH_SIZE = 100
    PROCESSING_YIELD_INTERVAL = 0.1
    PROCESSING_YIELD_CONTROL_DELAY = 0.01

    def __init__(self):
        # these are the fixed broadcast types that are always sent to all connected websockets
        self.broadcast_types: list[str] = {
            WSDATA_COLLAB_CREATE,
            WSDATA_COLLAB_UPDATE,
            WSDATA_COLLAB_DELETE,
            WSDATA_INGEST_SOURCE_DONE,
            WSDATA_USER_LOGIN,
            WSDATA_USER_LOGOUT,
            WSDATA_GENERIC,
        }
        self._initialized: bool = True
        self._shared_q: Queue = None
        self._fill_task: asyncio.Task = None
        self._last_queue_warning: int = 0  # just for metrics...

    def __new__(cls):
        """
        Create a new instance of the class.
        """
        if not cls._instance:
            cls._instance = super().__new__(cls)
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

    def add_broadcast_type(self, t: str) -> None:
        """
        Adds a new type to the broadcast types list: this type will be broadcast according to the rules defined in GulpConnectedSockets.

        Args:
            type (str): The type to add.
        """
        if t not in self._broadcast_types:
            self._broadcast_types.append(t)

    def remove_broadcast_type(self, t: str) -> None:
        """
        Removes a type from the broadcast types list.

        Args:
            type (str): The type to remove.
        """
        if t in self._broadcast_types:
            self._broadcast_types.remove(t)

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
            "setting shared ws queue in worker process: q=%s(%s)" % (q, type(q))
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

        MutyLogger.get_instance().debug("initializing shared ws queue ...")
        self._shared_q = mgr.Queue(GulpWsSharedQueue.MAX_QUEUE_SIZE)
        self._fill_task = asyncio.create_task(self._fill_ws_queues_from_shared_queue())

        return self._shared_q

    async def _fill_ws_queues_from_shared_queue(self):
        """
        process shared queue and dispatch messages to the target websockets
        """
        from gulp.api.rest_api import GulpRestServer

        MutyLogger.get_instance().debug("starting shared queue processing task...")

        messages: list[GulpWsData] = []
        last_yield_time: GulpWsIngestPacket = time.monotonic()
        try:

            while not GulpRestServer.get_instance().is_shutdown():
                # collect batch of messages
                messages.clear()
                while len(messages) < GulpWsSharedQueue.BATCH_SIZE:
                    try:
                        entry = self._shared_q.get_nowait()
                        messages.append(entry)
                        self._shared_q.task_done()
                    except Empty:
                        break

                # dispatch
                for msg in messages:
                    # MutyLogger.get_instance().debug(
                    #     "shared queue processing message: type=%s, ws_id=%s, user_id=%s, operation_id=%s, internal=%s",
                    #     msg.type,
                    #     msg.ws_id,
                    #     msg.user_id,
                    #     msg.operation_id,
                    #     msg.internal,
                    # )
                    cws = GulpConnectedSockets.get_instance().find(msg.ws_id)
                    if cws or msg.internal:
                        await GulpConnectedSockets.get_instance().broadcast_message(msg)

                # yield control between batches
                if (
                    time.monotonic() - last_yield_time
                    > GulpWsSharedQueue.PROCESSING_YIELD_INTERVAL
                ):
                    await asyncio.sleep(
                        GulpWsSharedQueue.PROCESSING_YIELD_CONTROL_DELAY
                    )
                    last_yield_time = time.monotonic()

        except asyncio.CancelledError:
            MutyLogger.get_instance().warning("queue processing cancelled")
        except Exception as e:
            MutyLogger.get_instance().error("queue processing error: %s", e)
            raise
        finally:
            # process remaining messages
            for msg in messages:
                try:
                    cws = GulpConnectedSockets.get_instance().find(msg.ws_id)
                    if cws or msg.internal:
                        await GulpConnectedSockets.get_instance().broadcast_message(msg)
                except Exception as e:
                    MutyLogger.get_instance().error("shared queue cleanup error: %s", e)

            MutyLogger.get_instance().info("shared queue processing task completed")

    async def close(self) -> None:
        """
        Closes the shared multiprocessing queue (flushes it first).

        Returns:
            None
        """
        if not self._shared_q:
            return

        logger = MutyLogger.get_instance()
        logger.debug("closing shared WS queue...")
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

    def put_internal_event(
        self,
        t: str,
        user_id: str = None,
        operation_id: str = None,
        req_id: str = None,
        data: dict = None,
    ) -> None:
        """
        Puts a GulpInternalEvent (not websocket related) into the shared queue.

        this is used to broadcast internal events to plugins via the GulpInternalEventsManager

        Args:
            t (str): The message type (i.e. GulpInternalEventsManager.EVENT_INGEST)
            user_id (str, optional): the user id associated with this event. Defaults to None.
            operation_id (str, optional): the operation id if applicable. Defaults to None.
            req_id (str, optional): the request id originating the event, if applicable. Defaults to None.
            data (dict, optional): event data. Defaults to None.
        """
        wsd = GulpWsData(
            timestamp=muty.time.now_msec(),
            type=t,
            user_id=user_id,
            req_id=req_id,
            operation_id=operation_id,
            payload=data,
            internal=True,
        )
        self._shared_q.put(wsd)

    async def put(
        self,
        t: str,
        user_id: str,
        ws_id: str = None,
        operation_id: str = None,
        req_id: str = None,
        data: Any = None,
        private: bool = False,
    ) -> None:
        """
        adds data to the shared queue

        this method attempts to put a message into the queue and will retry if the
        queue is full, with progressive backoff between retries.

        args:
            type (str): the type of data being queued
            user_id (str): the user id associated with this message
            ws_id (str, optional): the websocket id that will receive the message. if None, the message is broadcasted to all connected websockets
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
        if t == WSDATA_DOCUMENTS_CHUNK:
            if not ws_id or not GulpConnectedSocket.is_alive(ws_id):
                raise WebSocketDisconnect("websocket '%s' is not connected!", ws_id)

        # create the message
        wsd = GulpWsData(
            timestamp=muty.time.now_msec(),
            type=t,
            operation_id=operation_id,
            ws_id=ws_id,
            user_id=user_id,
            req_id=req_id,
            private=private,
            payload=data,
        )

        # attempt to add with exponential backoff
        for retry in range(GulpWsSharedQueue.MAX_RETRIES):
            try:
                self._shared_q.put(
                    wsd, block=True, timeout=GulpWsSharedQueue.QUEUE_TIMEOUT
                )
                # MutyLogger.get_instance().debug(
                #     "added type=%s message to queue for ws=%s", t, ws_id
                # )
                # MutyLogger.get_instance().debug(
                #     "queued message in the shared_q: %s", wsd
                # )
                return
            except queue.Full:
                # exponential backoff
                backoff_time: int = retry

                # log the attempt
                MutyLogger.get_instance().error(
                    "***** queue full (size=%d) for ws_id=%s, attempt %d/%d, backoff_time=%ds",
                    self._shared_q.qsize(),
                    ws_id,
                    retry + 1,
                    GulpWsSharedQueue.MAX_RETRIES,
                    backoff_time,
                )

                # wait before retrying
                await asyncio.sleep(backoff_time)

        # all retries failed
        raise WsQueueFullException(
            f"queue full, size={self._shared_q.qsize()}, for ws_id={ws_id} after {GulpWsSharedQueue.MAX_RETRIES} attempts!"
        )

    async def put_generic_notify(
        self,
        t: str,
        user_id: str,
        req_id: str,
        ws_id: str = None,
        d: dict = None,
        operation_id: str = None,
    ) -> None:
        """
        a shortcut method to send generic notify messages to the queue, to be routed among connected sockets

        args:
            t (str): the custom notify type
            user_id (str): the user id associated with this message
            req_id (str): the request id
            ws_id (str, optional): the websocket id that will receive the message. if None, the message is broadcasted to all connected websockets
            d (dict, optional): the payload data. Defaults to None.
            operation_id (Optional[str]): the operation id if applicable
        """

        if not d:
            d = {}

        p: GulpCollabGenericNotifyPacket = GulpCollabGenericNotifyPacket(type=t, data=d)
        d = p.model_dump(exclude_none=True)
        await self.put(
            t=WSDATA_GENERIC,
            user_id=user_id,
            ws_id=ws_id,
            operation_id=operation_id,
            req_id=req_id,
            data=d,
        )

    async def put_progress(
        self,
        msg: str,
        user_id: str,
        req_id: str,
        ws_id: str = None,
        total: int = 0,
        current: int = 0,
        d: dict = None,
        done: bool = False,
        operation_id: str = None,
    ) -> None:
        """
        a shortcut method to send GulpProgressPacket messages to the queue

        args:
            msg (str): the progress message
            user_id (str): the user id associated with this message
            req_id (str, optional): the request id
            ws_id (str, optional): the websocket id that will receive the message. if None, the message is broadcasted to all connected websockets
            total (int, optional): the total number of items to process
            current (int, optional): the current number of processed items
            d (dict, optional): the progress payload data if any. Defaults to None.
            done (bool, optional): whether the operation is done. Defaults to False.
            operation_id (Optional[str]): the operation id if applicable
        """
        p = GulpProgressPacket(
            total=total,
            current=current,
            done=done,
            msg=msg,
            data=d or {},
        )
        wsq = GulpWsSharedQueue.get_instance()
        await wsq.put(
            t=WSDATA_PROGRESS,
            ws_id=ws_id,
            user_id=user_id,
            req_id=req_id,
            operation_id=operation_id,
            data=p.model_dump(exclude_none=True),
        )
