import asyncio
import os
import queue
import time
from enum import StrEnum
from multiprocessing.managers import SyncManager
from queue import Empty, Queue
from threading import Lock
from typing import Annotated, Any, Optional

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
WSDATA_STATS_CREATE = (
    "stats_create"  # GulpRequestStats, whenever a stats object is created
)
WSDATA_STATS_UPDATE = (
    "stats_update"  # GulpRequestStats, whenever a stats object is updated
)
WSDATA_USER_LOGIN = "user_login"  # GulpUserAccessPacket
WSDATA_USER_LOGOUT = "user_logout"  # GulpUserAccessPacket
WSDATA_DOCUMENTS_CHUNK = "docs_chunk"  # GulpDocumentsChunkPacket
WSDATA_INGEST_SOURCE_DONE = "ingest_source_done"  # GulpIngestSourceDonePacket, this is sent in the end of an ingestion operation, one per source
WSDATA_REBASE_DONE = "rebase_done"  # GulpUpdateDocumentsStats, sent when a rebase operation is done (broadcasted to all websockets)
WSDATA_QUERY_GROUP_MATCH = "query_group_match"  # GulpQueryGroupMatchPacket, this is sent to indicate a query group match, i.e. a query group that matched some queries
WSDATA_CLIENT_DATA = "client_data"  # arbitrary content, to be routed to all connected websockets (used by the ui to communicate with other connected clients with its own protocol)
WSDATA_SOURCE_FIELDS_CHUNK = "source_fields_chunk"
WSDATA_QUERY_DONE = "query_done"  # this is sent in the end of a query operation, one per single query (i.e. a sigma zip query may generate multiple single queries, called a query group)
WSDATA_QUERY_GROUP_DONE = "query_group_done"  # this is sent in the end of the query task, being it single or group(i.e. sigma) query

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
            description="The created GulpCollabObject (or bulk of objects): the object `type` is `obj.type` or `obj[0].type` for bulk objects.",
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
        dict,
        Field(
            description="The updated GulpCollabObject: the object `type` is `obj.type`."
        ),
    ]


class GulpCollabDeletePacket(BaseModel):
    """
    Represents a delete collab event (WsData.type = WSDATA_COLLAB_DELETE).
    """

    model_config = ConfigDict(json_schema_extra={"examples": [{"id": "the id"}]})
    id: Annotated[str, Field(description="The deleted collab object ID.")]


class WsQueueFullException(Exception):
    """Exception raised when queue is full after retries"""


class GulpUserAccessPacket(BaseModel):
    """
    Represents a user login or logout event.
    """

    model_config = ConfigDict(
        json_schema_extra={"examples": [{"user_id": "admin", "login": True}]}
    )
    user_id: Annotated[str, Field(description="The user ID.")]
    login: Annotated[
        bool, Field(description="If the event is a login event, either it is a logout.")
    ]=True
    req_id: Annotated[Optional[str], Field(description="The request ID, if any.")] = None
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
    group: Annotated[str, Field(description="The query group name.")]
    matches: Annotated[dict, Field(description="The matched queries.")]
    color: Annotated[
        Optional[str],
        Field(
            None,
            description="The color associated with the query group, if any.",
        ),
    ] = None
    glyph_id: Annotated[
        Optional[str],
        Field(
            None,
            description="The glyph associated with the query group, if any.",
        ),
    ] = None


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
    q_name: Annotated[str, Field(description="The query name.")]
    q_group: Annotated[
        Optional[str], Field(description="The query group name, if any.")
    ] = None
    status: Annotated[
        str, Field(description="The status of the query operation (done/failed).")
    ]
    errors: Annotated[list[str], Field(description="The error message/s, if any.")] = []
    total_hits: Annotated[
        int, Field(description="The total number of hits for the query.")
    ] = 0


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
    status: Annotated[
        str, Field(description="The source ingestion status (failed, done, canceled).")
    ]
    source_id: Annotated[
        Optional[str],
        Field(
            description="The source ID in the collab database (this may be None if the plugin dynamically created a source).",
            alias="gulp.source_id",
        ),
    ] = None
    context_id: Annotated[
        Optional[str],
        Field(
            description="The context ID in the collab database (this may be None if the plugin dynamically created a context).",
            alias="gulp.context_id",
        ),
    ] = None
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
    last: Annotated[
        bool,
        Field(
            description="set to True to indicate the last packet of a stream.",
        ),
    ] = False


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
                    "types": [WSDATA_DOCUMENTS_CHUNK],
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
    req_id: Annotated[
        Optional[str], Field(description="the request ID, if any.")
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
    chunk_size: Annotated[
        int,
        Field(
            description="the chunk size (number of documents)",
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
        self.types = types
        self.operation_ids = operation_ids
        self.socket_type = socket_type
        self._tasks: list[asyncio.Task] = []
        # each socket has its own asyncio queue, consumed by its own task
        self.q = asyncio.Queue()

    async def run_loop(self) -> None:
        """
        Runs the websocket loop with optimized task management and cleanup
        """
        # create tasks with names for better debugging
        send_task: asyncio.Task = asyncio.create_task(
            self._send_loop(), name=f"send_loop-{self.ws_id}"
        )
        receive_task: asyncio.Task = asyncio.create_task(
            self._receive_loop(), name=f"receive_loop-{self.ws_id}"
        )
        self._tasks.extend([send_task, receive_task])

        # Wait for first task to complete
        done, _ = await asyncio.wait(self._tasks, return_when=asyncio.FIRST_EXCEPTION)

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

    async def cleanup(self) -> None:
        """
        ensure websocket is cleaned up when canceled/closing
        """
        MutyLogger.get_instance().debug(
            "---> cleanup, ensuring ws cleanup for ws=%s, ws_id=%s", self.ws, self.ws_id
        )

        if self._tasks:
            # clear tasks
            await asyncio.shield(self._cleanup_tasks())

        # empty the queue
        await self._flush_queue()

        MutyLogger.get_instance().debug(
            "---> GulpConnectedSocket.cleanup() DONE, ws=%s, ws_id=%s",
            self.ws,
            self.ws_id,
        )

    async def _flush_queue(self) -> None:
        """
        flushes the websocket queue.
        """
        MutyLogger.get_instance().debug(
            "---> flushing queue for ws=%s, ws_id=%s", self.ws, self.ws_id
        )

        try:
            # drain any currently enqueued items without blocking
            drained: int = 0
            while True:
                try:
                    self.q.get_nowait()
                    try:
                        self.q.task_done()
                    except Exception:
                        # defensive: task tracking may misbehave; continue
                        pass
                    drained += 1
                except asyncio.QueueEmpty:
                    break

            MutyLogger.get_instance().debug(
                "---> drained %d queued items for ws=%s, ws_id=%s",
                drained,
                self.ws,
                self.ws_id,
            )

            # attempt a bounded wait for any remaining in-flight tasks to complete
            try:
                await asyncio.wait_for(self.q.join(), timeout=2.0)
                MutyLogger.get_instance().debug(
                    "---> queue.join() completed for ws=%s, ws_id=%s",
                    self.ws,
                    self.ws_id,
                )
            except asyncio.TimeoutError:
                # gather diagnostics to help root-cause the hang
                unfinished = getattr(self.q, "_unfinished_tasks", None)
                try:
                    qsize = self.q.qsize()
                except Exception:
                    qsize = -1
                MutyLogger.get_instance().warning(
                    "queue.join() timed out for ws=%s, ws_id=%s: unfinished=%r, qsize=%d",
                    self.ws,
                    self.ws_id,
                    unfinished,
                    qsize,
                )

                # force-drain remaining items to ensure we don't hang indefinitely
                forced = 0
                while True:
                    try:
                        self.q.get_nowait()
                        try:
                            self.q.task_done()
                        except Exception:
                            pass
                        forced += 1
                    except asyncio.QueueEmpty:
                        break

                MutyLogger.get_instance().warning(
                    "forced-drained %d items for ws=%s, ws_id=%s",
                    forced,
                    self.ws,
                    self.ws_id,
                )
        except Exception as ex:
            MutyLogger.get_instance().exception(
                "error flushing queue for ws=%s, ws_id=%s: %s", self.ws, self.ws_id, ex
            )

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
        active_sockets: list[str] = GulpProcess.get_instance().shared_memory_get(
            SHARED_MEMORY_KEY_ACTIVE_SOCKETS
        )
        ignore: bool = GulpConfig.get_instance().debug_ignore_missing_ws()
        # MutyLogger.get_instance().debug("checking if ws_id=%s is alive: debug_ignore_missing_ws=%r, active_sockets=%s", ws_id, ignore, active_sockets)

        if ignore:
            return True
        
        if active_sockets is None:
            return False
        
        return ws_id in active_sockets

    async def _cleanup_tasks(self) -> None:
        """
        clean up tasks with proper cancellation handling
        """
        for task in self._tasks:
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

    async def _ws_rate_limit_delay(self) -> float:
        """
        get the rate limit delay for the websocket

        returns:
            float: the rate limit delay in seconds
        """
        adaptive_rate_limit = GulpConfig.get_instance().ws_adaptive_rate_limit()
        if not adaptive_rate_limit:
            return GulpConfig.get_instance().ws_rate_limit_delay()

        base_delay: float = GulpConfig.get_instance().ws_rate_limit_delay()
        connected_sockets: int = (
            GulpConnectedSockets.get_instance().num_connected_sockets()
        )

        # reduce delay for fewer sockets, increase for many
        if connected_sockets < 10:
            return base_delay * 0.5
        elif connected_sockets > 50:
            return base_delay * 2
        return base_delay

    async def _process_queue_message(self) -> bool:
        """
        get and send a message from the queue to the websocket with timeout and cancellation handling

        returns:
            bool: true if message was processed, false if timeout occurred
        """
        item = None
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
            # MutyLogger.get_instance().debug(
            #     "---> SENT message to ws_id=%s, type=%s, content=%s", self.ws_id, item.get("type"), item
            # )
            await asyncio.sleep(GulpConfig.get_instance().ws_rate_limit_delay())
            return True
        except asyncio.TimeoutError:
            # no messages, check for cancellation during timeout
            current_task = asyncio.current_task()
            if current_task and current_task.cancelled():
                raise asyncio.CancelledError()
            return False
        finally:
            if item:
                self.q.task_done()

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
            except asyncio.CancelledError:
                self._logger.warning(
                    "---> _receive loop canceled for ws_id=%s", self.ws_id
                )
                raise
            except WebSocketDisconnect:
                raise

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
        self._sockets: dict[str, GulpConnectedSocket] = {}
        self._logger = MutyLogger.get_instance()

    async def add(
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
        if ws_id in self._sockets:
            self._logger.warning(
                "add(): websocket with ws_id=%s already exists, disconnect existing and overwriting", ws_id
            )
            # disconnect existing socket
            connected_socket = self._sockets.get(ws_id)
            await connected_socket.ws.close(code=1001, reason="overwriting, another connection with same ws_id=%s established!" % (ws_id))
            await self.remove(ws_id)
            await connected_socket.cleanup()

        # create connected socket instance
        connected_socket = GulpConnectedSocket(
            ws=ws,
            ws_id=ws_id,
            types=types,
            operation_ids=operation_ids,
            socket_type=socket_type,
        )

        # store socket reference
        self._sockets[ws_id] = connected_socket

        # add to global shared list
        from gulp.process import GulpProcess

        GulpProcess.get_instance().shared_memory_add_to_list(
            SHARED_MEMORY_KEY_ACTIVE_SOCKETS, ws_id
        )

        self._logger.debug(
            "---> added connected ws: %s, ws_id=%s, len=%d",
            ws,
            ws_id,
            len(self._sockets),
        )
        return connected_socket

    async def remove(self, ws_id: str) -> None:
        """
        Removes a websocket from the connected sockets list

        Args:
            ws_id (str): The WebSocket ID.
            flush (bool, optional): If the queue should be flushed. Defaults to True.

        """
        self._logger.debug("---> remove, ws_id=%s", ws_id)

        connected_socket = self._sockets.get(ws_id)
        if not connected_socket:
            self._logger.warning(
                "remove(): no websocket ws_id=%s found, num connected sockets=%d",
                ws_id,
                len(self._sockets),
            )
            return

        # remove from list
        del self._sockets[ws_id]

        # remove from global ws list
        from gulp.process import GulpProcess

        GulpProcess.get_instance().shared_memory_remove_from_list(
            SHARED_MEMORY_KEY_ACTIVE_SOCKETS, ws_id
        )

        self._logger.debug(
            "---> removed connected ws=%s, ws_id=%s, num connected sockets=%d",
            connected_socket.ws,
            ws_id,
            len(self._sockets),
        )

    def get(self, ws_id: str) -> GulpConnectedSocket:
        """
        Finds a ConnectedSocket object by its ID.

        Args:
            ws_id (str): The WebSocket ID.

        Returns:
            GulpConnectedSocket: The ConnectedSocket object or None if not found.
        """
        # use fast lookup via mapping
        return self._sockets.get(ws_id)

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
        try:
            await target_ws.q.put(message)
        except Exception as ex:
            self._logger.exception(
                "failed to route message to ws_id=%s: %s", target_ws.ws_id, ex
            )
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

        # collect routing tasks
        tasks = []
        ws_ids = []
        for ws_id, connected_socket in socket_items:
            if skip_list and ws_id in skip_list:
                continue
            tasks.append(self._route_message(data, connected_socket))
            ws_ids.append(ws_id)

        if tasks:
            # execute concurrently
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # check for exceptions
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    ws_id = ws_ids[i]
                    self._logger.warning(
                        "failed to broadcast to (local) ws_id=%s: %s, removing dead socket",
                        ws_id,
                        result,
                    )
                    self._sockets.pop(ws_id, None)


class GulpWsSharedQueue:
    """
    Singleton class to manage shared queues shards for receiving data through workers and broadcasting across connected websockets.
    Provides methods for adding data to the queue and processing messages.
    """

    _instance: "GulpWsSharedQueue" = None

    MAX_QUEUE_SIZE = 20000
    QUEUE_TIMEOUT = 30
    YIELD_CONTROL_DELAY = 0.01

    def __init__(self):
        # these are the fixed broadcast types that are always sent to all connected websockets
        # keep as a set for fast membership checks
        self.broadcast_types: set[str] = {
            WSDATA_COLLAB_CREATE,
            WSDATA_COLLAB_UPDATE,
            WSDATA_COLLAB_DELETE,
            WSDATA_REBASE_DONE,
            WSDATA_INGEST_SOURCE_DONE,
            WSDATA_USER_LOGIN,
            WSDATA_USER_LOGOUT,
        }

        # internal state
        self._initialized: bool = True
        # _shared_q is a list of multiprocessing.Queue instances (shards)
        self._shared_q: list[Queue] = None
        # when sharded, store a list of asyncio tasks processing each shard
        self._fill_tasks: list[asyncio.Task] | None = None
        # round-robin counter + lock for shard selection
        self._rr_counter: int = 0
        self._rr_lock: Lock = Lock()

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
        # ensure we operate on the broadcast_types set
        if t not in self.broadcast_types:
            self.broadcast_types.add(t)

    def remove_broadcast_type(self, t: str) -> None:
        """
        Removes a type from the broadcast types list.

        Args:
            type (str): The type to remove.
        """
        if t in self.broadcast_types:
            self.broadcast_types.remove(t)

    def set_queue(self, q: list[Queue]) -> None:
        """
        to be called by workers to set the shared queues

        Args:
            q (list[Queue]): The shared queue(s) created by the multiprocessing manager in the main process
        """
        from gulp.process import GulpProcess

        if GulpProcess.get_instance().is_main_process():
            raise RuntimeError("set_queue() must be called in a worker process")

        MutyLogger.get_instance().debug(
            "setting shared ws queues in worker process: q=%s", q
        )
        self._shared_q = q

    async def init_queue(self, mgr: SyncManager) -> list[Queue]:
        """
        to be called by the MAIN PROCESS, initializes the shared multiprocessing queue: the same queue must then be passed to the worker processes

        Args:
            q (Queue): The shared queue created by the multiprocessing manager in the main process
        """
        from gulp.process import GulpProcess

        if not GulpProcess.get_instance().is_main_process():
            raise RuntimeError("init_queue() must be called in the main process")

        MutyLogger.get_instance().debug("initializing shared ws queue(s) ...")

        # decide on number of shards; prefer config if available
        num_shards = GulpConfig.get_instance().ws_queue_num_shards()
        auto: bool = False
        if not num_shards or num_shards < 1:
            auto = True
            # dynamic calculation based on cpu count (2x cores) with a sensible cap
            cpu_count = os.cpu_count() or 1
            num_shards = max(1, min(cpu_count * 2, 32))

        # create multiple queues (shards) to reduce contention; even single-shard becomes a list
        queues: list[Queue] = [
            mgr.Queue(GulpWsSharedQueue.MAX_QUEUE_SIZE) for _ in range(num_shards)
        ]
        self._shared_q = queues

        # create a processing task per shard
        self._fill_tasks = [
            asyncio.create_task(self._process_shared_queue(q)) for q in queues
        ]
        MutyLogger.get_instance().debug(
            "initialized %d shared ws queues (auto=%r)", num_shards, auto
        )
        return queues

    async def _process_shared_queue(self, q: Queue):
        """
        process a single shared queue (a shard) and dispatch messages to the target websockets

        This method runs one processing loop for the given queue, continuously retrieving messages and broadcasting them to connected websockets.

        """
        from gulp.api.server_api import GulpRestServer
        from gulp.process import GulpProcess

        async def _process_message(msg: GulpWsData) -> None:
            """
            process a single message from the queue, called by gather() below
            """
            cws = GulpConnectedSockets.get_instance().get(msg.ws_id)
            if cws or msg.internal:
                await GulpConnectedSockets.get_instance().broadcast_message(msg)


        MutyLogger.get_instance().debug(
            "starting shared queue processing task for shard %s...", q
        )

        try:
            batch_counter: int = 0

            # use the event loop executor to block on manager.Queue.get() without busy-polling
            loop = asyncio.get_running_loop()
            while not GulpRestServer.get_instance().is_shutdown():
                # get batch size
                base_batch_size = GulpConfig.get_instance().ws_queue_batch_size()

                try:
                    # blockingly wait for the first message in a thread to avoid busy-wait
                    first_msg = await loop.run_in_executor(
                        GulpProcess.get_instance().thread_pool, q.get
                    )
                    q.task_done()

                except Exception as e:
                    # if something unexpected happens while blocking, log and continue
                    MutyLogger.get_instance().error(
                        "error getting message from shard %s: %s", q, e
                    )
                    await asyncio.sleep(GulpWsSharedQueue.YIELD_CONTROL_DELAY)
                    continue

                # decide batch size, use a small multiplier when backlog is large
                queue_size = q.qsize()
                if queue_size > 1000:
                    batch_size = base_batch_size * 2
                else:
                    batch_size = base_batch_size

                # start with the first item and try to drain additional items without blocking
                messages = [first_msg]
                for _ in range(batch_size - 1):
                    try:
                        msg = q.get_nowait()
                        try:
                            q.task_done()
                        except Exception:
                            pass
                        messages.append(msg)
                    except Empty:
                        break

                # process batch concurrently
                start_time: float = time.time()
                await asyncio.gather(
                    *[_process_message(msg) for msg in messages],
                    return_exceptions=True,
                )
                elapsed: float = time.time() - start_time

                batch_counter += 1
                if elapsed > 0.1:
                    MutyLogger.get_instance().warning(
                        "batch processing took %.3f seconds for %d messages",
                        elapsed,
                        len(messages),
                    )

                # log queue size periodically
                if batch_counter % 100 == 0:
                    try:
                        MutyLogger.get_instance().debug(
                            "processed %d batches for shard %s, current queue size: %d",
                            batch_counter,
                            q,
                            queue_size,
                        )
                    except Exception:
                        pass

        except asyncio.CancelledError:
            MutyLogger.get_instance().warning(
                "queue processing cancelled for shard %s", q
            )
        except Exception as e:
            MutyLogger.get_instance().error(
                "queue processing error for shard %s: %s", q, e
            )
            raise
        finally:
            MutyLogger.get_instance().info(
                "shared queue processing task completed for shard %s", q
            )

    async def close(self) -> None:
        """
        closes the shared multiprocessing queues, flushing first

        Returns:
            None
        """
        logger = MutyLogger.get_instance()
        logger.debug("closing shared WS queue(s)...")

        try:
            # flush queues with timeout protection
            try:
                await asyncio.wait_for(self._flush_queue(), timeout=10.0)
            except asyncio.TimeoutError:
                logger.warning("queue flush operation timed out")

            # cancel processing tasks with proper handling
            if self._fill_tasks:
                for t in list(self._fill_tasks):
                    try:
                        cancelled = t.cancel()
                        if cancelled:
                            await asyncio.wait_for(t, timeout=5.0)
                            logger.debug("Queue processing task cancelled successfully")
                        else:
                            logger.debug("Queue processing task was already completed")
                    except asyncio.TimeoutError:
                        logger.warning("Task cancellation timed out")
                    except Exception as e:
                        logger.error("Error during task cancellation: %s", e)

            # release resources
            self._shared_q = None
            self._fill_tasks = None
            logger.debug("Shared WS queue(s) closed")

        except Exception as e:
            logger.error("Failed to close shared queue(s): %s", e)

    async def _flush_queue(self) -> None:
        """Flush all items from the queue(s)"""
        counter = 0

        for q in self._shared_q:
            while True:
                try:
                    q.get_nowait()
                    try:
                        q.task_done()
                    except Exception:
                        pass
                    counter += 1
                except Empty:
                    break
            try:
                q.join()
            except Exception:
                # some manager queue implementations may not implement join
                pass

        MutyLogger.get_instance().debug(
            f"flushed {counter} messages from shared queue(s)"
        )

    async def _put_internal(self, wsd: GulpWsData) -> None:
        """
        put gulp-internal message in the shared queues
        """

        # choose the target queue (round robin)
        with self._rr_lock:
            shard_index = self._rr_counter
            self._rr_counter = (self._rr_counter + 1) % len(self._shared_q)
        target_queue = self._shared_q[shard_index]

        # attempt to add with exponential backoff
        max_retries = GulpConfig.get_instance().ws_queue_max_retries()
        backoff_cap = GulpConfig.get_instance().ws_queue_backoff_cap()
        for retry in range(max_retries):
            try:
                target_queue.put(wsd, block=False)
                return
            except queue.Full:
                # exponential backoff with cap
                backoff_time: float = min(backoff_cap, 2**retry)

                # log the attempt
                qsize = target_queue.qsize()
                MutyLogger.get_instance().error(
                    "***** queue %s full (size=%d) for ws_id=%s, attempt %d/%d, backoff_time=%.1fs",
                    target_queue,
                    qsize,
                    wsd.ws_id,
                    retry + 1,
                    max_retries,
                    backoff_time,
                )

                # wait before retrying
                await asyncio.sleep(backoff_time)

        # all retries failed
        final_size = target_queue.qsize()
        raise WsQueueFullException(
            f"queue {target_queue} full, size={final_size}, for ws_id={wsd.ws_id} after {max_retries} attempts!"
        )

    async def put_internal_event(
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
        await self._put_internal(wsd)

    async def put(
        self,
        t: str,
        user_id: str,
        ws_id: str = None,
        operation_id: str = None,
        req_id: str = None,
        d: Any = None,
        private: bool = False,
    ) -> None:
        """
        adds data to the shared queue

        args:
            type (str): the type of data being queued
            user_id (str): the user id associated with this message
            ws_id (str, optional): the websocket id that will receive the message. if None, the message is broadcasted to all connected websockets
            operation_id (Optional[str]): the operation id if applicable
            req_id (Optional[str]): the request id if applicable
            d (Optional[Any]): the payload data
            private (bool): whether this message is private to the specified ws_id
        raises:
            WebSocketDisconnect: if websocket is not connected for DOCUMENTS_CHUNK type
            WsQueueFullException: if queue is full after all retries
        """
        # verify websocket is alive
        if not ws_id or not GulpConnectedSocket.is_alive(ws_id):
            raise WebSocketDisconnect("websocket '%s' is not connected!" % (ws_id))

        # create the message
        wsd = GulpWsData(
            timestamp=muty.time.now_msec(),
            type=t,
            operation_id=operation_id,
            ws_id=ws_id,
            user_id=user_id,
            req_id=req_id,
            private=private,
            payload=d,
        )
        await self._put_internal(wsd)
