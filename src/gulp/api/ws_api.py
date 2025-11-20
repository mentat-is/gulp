import asyncio
import os
import queue
import time
import zlib
from enum import StrEnum
from multiprocessing.managers import SyncManager
from queue import Empty, Queue
from threading import Lock
from typing import Annotated, Any, Literal, Optional

import muty
import muty.string
import muty.time
import orjson
from fastapi import WebSocket, WebSocketDisconnect
from fastapi.websockets import WebSocketState
from gitdb.fun import chunk_size
from muty.log import MutyLogger
from muty.pydantic import autogenerate_model_example_by_class
from pydantic import BaseModel, ConfigDict, Field

from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.opensearch.structs import GulpDocument
from gulp.api.redis_api import GulpRedis
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


class GulpRedisChannel(StrEnum):
    """
    Redis pubsub channels for websocket communication
    """

    BROADCAST = "broadcast"
    WORKER_TO_MAIN = "worker_to_main"


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
    type: Annotated[str, Field(description="The deleted collab object type.")]
    id: Annotated[str, Field(description="The deleted collab object ID.")]


class redis_brokerueueFullException(Exception):
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
    ] = True
    req_id: Annotated[Optional[str], Field(description="The request ID, if any.")] = (
        None
    )
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
    req_id: Annotated[Optional[str], Field(description="the request ID, if any.")] = (
        None
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

    # yield interval used to avoid starving the event loop
    YIELD_CONTROL_INTERVAL: int = 100
    # sleep duration when yielding control to the event loop
    YIELD_CONTROL_DELAY: float = 0.01
    # threshold for logging queue pressure warnings
    QUEUE_PRESSURE_THRESHOLD: float = 0.8
    # interval between queue pressure logs to avoid flooding
    QUEUE_PRESSURE_LOG_INTERVAL: float = 30.0
    # interval between overflow logs
    QUEUE_OVERFLOW_LOG_INTERVAL: float = 10.0

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
        from gulp.process import GulpProcess

        self.ws = ws
        self.ws_id = ws_id
        self.types = types
        self._terminated: bool = False
        self.operation_ids = operation_ids
        self.socket_type = socket_type
        self._cleaned_up = False
        self._server_id = GulpProcess.get_instance().server_id
        self._tasks: list[asyncio.Task] = []
        # store queue capacity for quick reference in telemetry
        self._queue_capacity: int = GulpConfig.get_instance().ws_queue_max_size()
        # each socket has its own asyncio queue, consumed by its own task with bounded size
        self.q = asyncio.Queue(maxsize=self._queue_capacity)
        # track dropped message count for observability
        self._dropped_messages: int = 0
        # remember queue high watermark for diagnostics
        self._queue_high_watermark: int = 0
        # throttle repeated overflow logs
        self._last_overflow_log_ts: float = 0.0
        # throttle repeated pressure logs
        self._last_pressure_log_ts: float = 0.0

    async def enqueue_message(self, message: dict) -> bool:
        """
        enqueue a websocket message without blocking the caller

        Args:
            message (dict): payload ready for serialization
        Returns:
            bool: True when the message has been enqueued, False if dropped
        Throws:
            RuntimeError: propagated when queue operations fail unexpectedly
        """
        try:
            # prefer non-blocking enqueue to avoid stalling broadcast fan-out
            self.q.put_nowait(message)
            self._record_queue_pressure()
            return True
        except asyncio.QueueFull:
            # handle overflow by dropping the oldest entry and logging the event
            MutyLogger.get_instance().exception("**** ws_id=%s QUEUE FULL ****", self.ws_id)
            return await self._handle_queue_overflow(message)

    def _record_queue_pressure(self) -> None:
        """
        track queue utilization and log when approaching capacity

        Args:
            None
        Returns:
            None
        Throws:
            RuntimeError: not raised explicitly but kept for interface parity
        """
        current_size: int = self.q.qsize()
        if current_size > self._queue_high_watermark:
            # update high watermark for later inspection
            self._queue_high_watermark = current_size

        if not self._queue_capacity:
            return

        utilization: float = current_size / self._queue_capacity
        if utilization < self.QUEUE_PRESSURE_THRESHOLD:
            return

        now: float = time.monotonic()
        if now - self._last_pressure_log_ts < self.QUEUE_PRESSURE_LOG_INTERVAL:
            return

        self._last_pressure_log_ts = now
        MutyLogger.get_instance().warning(
            "ws queue high pressure ws_id=%s utilization=%.2f capacity=%d high_watermark=%d",
            self.ws_id,
            utilization,
            self._queue_capacity,
            self._queue_high_watermark,
        )

    async def _handle_queue_overflow(self, message: dict) -> bool:
        """
        drop the oldest message to make room for a new one when the queue is full

        Args:
            message (dict): payload that could not be queued due to overflow
        Returns:
            bool: True if the new message was eventually enqueued, False otherwise
        Throws:
            RuntimeError: propagated when queue operations fail unexpectedly
        """
        self._dropped_messages += 1
        dropped_type: Optional[str] = None

        try:
            # remove the oldest entry to reclaim space before inserting the new payload
            dropped_item = self.q.get_nowait()
            if isinstance(dropped_item, dict):
                dropped_type = dropped_item.get("type")
            self.q.task_done()
        except asyncio.QueueEmpty:
            dropped_type = None

        try:
            self.q.put_nowait(message)
            self._log_queue_overflow(dropped_type, True)
            return True
        except asyncio.QueueFull:
            MutyLogger.get_instance().exception("**** ws_id=%s QUEUE FULL ****", self.ws_id)
            self._log_queue_overflow(dropped_type, False)
            return False

    def _log_queue_overflow(self, dropped_type: Optional[str], recovered: bool) -> None:
        """
        log overflow information with throttling to avoid noisy logs

        Args:
            dropped_type (Optional[str]): discarded payload type when known
            recovered (bool): indicates if the new payload entered the queue
        Returns:
            None
        Throws:
            RuntimeError: not raised explicitly but reserved for consistency
        """
        now: float = time.monotonic()
        if now - self._last_overflow_log_ts < self.QUEUE_OVERFLOW_LOG_INTERVAL:
            return

        self._last_overflow_log_ts = now
        MutyLogger.get_instance().warning(
            "ws queue overflow ws_id=%s dropped=%d last_type=%s recovered=%s capacity=%d high_watermark=%d",
            self.ws_id,
            self._dropped_messages,
            dropped_type,
            recovered,
            self._queue_capacity,
            self._queue_high_watermark,
        )

    async def _flush_queue(self) -> None:
        """
        flushes the websocket queue and send the terminator message.
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

            # put terminator
            await self.q.put(None)
        except Exception as ex:
            MutyLogger.get_instance().exception(
                "error flushing queue for ws=%s, ws_id=%s: %s", self.ws, self.ws_id, ex
            )

    async def cleanup(self) -> None:
        """
        ensure websocket is cleaned up when canceled/closing
        """
        MutyLogger.get_instance().debug(
            "---> cleanup, ensuring ws cleanup for ws=%s, ws_id=%s", self.ws, self.ws_id
        )

        # empty the queue
        await self._flush_queue()

        if self._tasks:
            # clear tasks
            await asyncio.shield(self.cleanup_tasks())

        MutyLogger.get_instance().debug(
            "---> GulpConnectedSocket.cleanup() DONE, ws=%s, ws_id=%s",
            self.ws,
            self.ws_id,
        )

    @staticmethod
    async def is_alive(ws_id: str) -> bool:
        """
        check if a websocket is alive by querying Redis

        Args:
            ws_id (str): The WebSocket ID.

        Returns:
            bool: True if the websocket is alive, False otherwise.
        """
        if GulpConfig.get_instance().debug_ignore_missing_ws():
            return True

        connected_sockets = GulpConnectedSockets.get_instance()

        # check local registry first to avoid unnecessary redis round-trips
        if connected_sockets.get(ws_id):
            return True

        cached_server: Optional[str] = connected_sockets.get_cached_server(ws_id)
        if cached_server:
            return True

        redis_client = GulpRedis.get_instance()
        server_id = await redis_client.ws_get_server(ws_id)
        connected_sockets.cache_server(ws_id, server_id)
        return server_id is not None

    async def cleanup_tasks(self) -> None:
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

        # unregister from Redis
        from gulp.api.redis_api import GulpRedis

        MutyLogger.get_instance().debug(
            "unregistering ws_id=%s from Redis ...", self.ws_id
        )
        try:
            await GulpRedis.get_instance().ws_unregister(self.ws_id)
        except Exception as ex:
            MutyLogger.get_instance().error(
                "error unregistering ws_id=%s: %s", self.ws_id, ex
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
            if not item:
                self._terminated = True
                return True

            # check state before sending
            current_task = asyncio.current_task()
            if current_task and current_task.cancelled():
                raise asyncio.CancelledError()

            # raise exception if websocket is disconnected
            self.validate_connection()

            # connection is verified, send the message
            await self.ws.send_json(item)
            # MutyLogger.get_instance().debug(
            #     "---> SENT message to ws_id=%s, type=%s, content=%s",
            #     self.ws_id,
            #     item.get("type"),
            #     muty.string.make_shorter(str(item), max_len=260),
            # )
            await asyncio.sleep(await self._ws_rate_limit_delay())
            return True
        except asyncio.CancelledError:
            MutyLogger.get_instance().warning(
                "---> _process_queue_message canceled for ws_id=%s", self.ws_id
            )
            raise
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
            if self._terminated:
                MutyLogger.get_instance().debug(
                    "---> _receive loop terminator found, terminating ws_id=%s",
                    self.ws_id,
                )
                break

            try:
                # use a shorter timeout to ensure we can yield control regularly
                await asyncio.wait_for(self.ws.receive_json(), timeout=1.0)
            except asyncio.TimeoutError:
                # no message received
                continue
            except asyncio.CancelledError:
                MutyLogger.get_instance().warning(
                    "---> _receive loop canceled for ws_id=%s", self.ws_id
                )
                raise
            except WebSocketDisconnect:
                raise

    async def _send_loop(self) -> None:
        """
        processes outgoing messages (filled by the shared queue) and sends them to the websocket
        """
        try:
            MutyLogger.get_instance().debug(
                f"---> starting send loop for ws_id={self.ws_id}"
            )

            # tracking variables for metrics
            message_count: int = 0
            yield_counter: int = 0

            while True:
                # process message from the websocket queue (filled by the shared queue)
                message_processed = await self._process_queue_message()
                if self._terminated:
                    MutyLogger.get_instance().debug(
                        "---> _send loop terminator found, terminating ws_id=%s",
                        self.ws_id,
                    )
                    break

                if message_processed:
                    message_count += 1
                    yield_counter += 1

                    # yield control periodically
                    if yield_counter % self.YIELD_CONTROL_INTERVAL == 0:
                        yield_counter = 0
                        await asyncio.sleep(self.YIELD_CONTROL_DELAY)

        except asyncio.CancelledError:
            MutyLogger.get_instance().warning(
                "---> send loop canceled for ws_id=%s", self.ws_id
            )
            raise

        except WebSocketDisconnect as ex:
            MutyLogger.get_instance().warning(
                "---> websocket disconnected: ws_id=%s", self.ws_id
            )
            MutyLogger.get_instance().exception(ex)
            raise

        except Exception as ex:
            MutyLogger.get_instance().error(
                "---> error in send loop: ws_id=%s", self.ws_id
            )
            MutyLogger.get_instance().exception(ex)
            raise

    def __str__(self):
        return f"ConnectedSocket(ws_id={self.ws_id}, types={self.types}, operation_ids={self.operation_ids})"

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
        done, pending = await asyncio.wait(
            self._tasks, return_when=asyncio.FIRST_EXCEPTION
        )

        # MutyLogger.get_instance().debug("---> wait returned for ws_id=%s, done=%s, pending=%s", self.ws_id, done, pending)
        for d in done:
            MutyLogger.get_instance().debug(
                "---> completed task %s for ws_id=%s", d.get_name(), self.ws_id
            )
        if pending:
            for t in pending:
                MutyLogger.get_instance().debug(
                    "---> cancelling pending task %s for ws_id=%s",
                    t.get_name(),
                    self.ws_id,
                )
                t.cancel()
                try:
                    await t
                except asyncio.CancelledError:
                    MutyLogger.get_instance().debug(
                        "pending task %s cancelled successfully", t.get_name()
                    )


class GulpConnectedSockets:
    """
    singleton class to hold all connected sockets for an instance of the Gulp server (NOT cluster-wide).
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
        # cache websocket ownership lookups to reduce Redis load
        self._ws_server_cache: dict[str, tuple[str, float]] = {}
        # ttl for cache entries, for quick invalidation
        self._ws_server_cache_ttl: float = 1.0 # GulpConfig.get_instance().ws_server_cache_ttl()

    def get_cached_server(self, ws_id: str) -> Optional[str]:
        """
        return cached websocket ownership if the entry is still valid

        Args:
            ws_id (str): websocket identifier
        Returns:
            Optional[str]: cached server id or None if absent/expired
        Throws:
            RuntimeError: not raised explicitly but kept for parity
        """
        cache_entry = self._ws_server_cache.get(ws_id)
        if not cache_entry:
            return None

        server_id, expires_at = cache_entry
        if expires_at < time.monotonic():
            self._ws_server_cache.pop(ws_id, None)
            return None

        return server_id

    def cache_server(self, ws_id: str, server_id: Optional[str]) -> None:
        """
        store websocket ownership information locally to avoid extra Redis hits

        Args:
            ws_id (str): websocket identifier
            server_id (Optional[str]): owning server id if known
        Returns:
            None
        Throws:
            RuntimeError: not raised explicitly but included for consistency
        """
        if not ws_id:
            return

        if not server_id:
            self._ws_server_cache.pop(ws_id, None)
            return

        expires_at: float = time.monotonic() + self._ws_server_cache_ttl
        self._ws_server_cache[ws_id] = (server_id, expires_at)

    def invalidate_cached_server(self, ws_id: str) -> None:
        """
        remove a websocket ownership cache entry

        Args:
            ws_id (str): websocket identifier
        Returns:
            None
        Throws:
            RuntimeError: not raised explicitly but declared for uniformity
        """
        if not ws_id:
            return

        self._ws_server_cache.pop(ws_id, None)

    async def add(
        self,
        ws: WebSocket,
        ws_id: str,
        types: list[str] = None,
        operation_ids: list[str] = None,
        socket_type: GulpWsType = GulpWsType.WS_DEFAULT,
    ) -> GulpConnectedSocket:
        """
        Adds a websocket to the connected sockets list and registers in Redis.

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
            # disconnect the existing socket with the same ID
            MutyLogger.get_instance().warning(
                "add(): websocket with ws_id=%s already exists, disconnecting existing socket",
                ws_id,
            )
            existing_socket = self._sockets[ws_id]
            await existing_socket.cleanup()
            del self._sockets[ws_id]

        # create connected socket instance
        connected_socket = GulpConnectedSocket(
            ws=ws,
            ws_id=ws_id,
            types=types,
            operation_ids=operation_ids,
            socket_type=socket_type,
        )

        # store local socket reference
        self._sockets[ws_id] = connected_socket

        # register in Redis
        redis_client = GulpRedis.get_instance()
        await redis_client.ws_register(
            ws_id=ws_id,
            types=types,
            operation_ids=operation_ids,
            socket_type=socket_type.value,
        )

        MutyLogger.get_instance().debug(
            "---> added connected ws: %s, ws_id=%s, len=%d",
            ws,
            ws_id,
            len(self._sockets),
        )
        return connected_socket

    async def remove(self, ws_id: str) -> None:
        """
        Removes a websocket from the connected sockets list and unregisters from Redis

        Args:
            ws_id (str): The WebSocket ID.
        """
        MutyLogger.get_instance().debug("---> remove, ws_id=%s", ws_id)

        connected_socket = self._sockets.get(ws_id)
        if not connected_socket:
            MutyLogger.get_instance().warning(
                "remove(): no websocket ws_id=%s found, num connected sockets=%d",
                ws_id,
                len(self._sockets),
            )
            return

        # remove from internal map
        ws_id = connected_socket.ws_id
        del self._sockets[ws_id]

        # unregister from Redis
        redis_client = GulpRedis.get_instance()
        await redis_client.ws_unregister(ws_id)
        
        # drop cached ownership so workers do not rely on stale entries
        self.invalidate_cached_server(ws_id)

        MutyLogger.get_instance().debug(
            "---> removed connected ws=%s, ws_id=%s, num connected sockets=%d",
            connected_socket.ws,
            ws_id,
            len(self._sockets),
        )

    def get(self, ws_id: str) -> GulpConnectedSocket:
        """
        gets a ConnectedSocket object by its ID.

        Args:
            ws_id (str): The WebSocket ID.

        Returns:
            GulpConnectedSocket: The ConnectedSocket object or None if not found.
        """
        return self._sockets.get(ws_id)

    async def cancel_all(self) -> None:
        """
        cancels all active websocket tasks
        """
        # create a copy to avoid modification during iteration
        sockets_to_cancel = list(self._sockets.values())

        for connected_socket in sockets_to_cancel:
            MutyLogger.get_instance().warning(
                "---> canceling ws=%s, ws_id=%s",
                connected_socket.ws,
                connected_socket.ws_id,
            )
            await connected_socket.cleanup_tasks()

        MutyLogger.get_instance().debug(
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
        if data.type not in GulpRedisBroker.get_instance().broadcast_types and (
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
            await target_ws.enqueue_message(message)
        except Exception as ex:
            MutyLogger.get_instance().exception(
                "failed to route message to ws_id=%s: %s", target_ws.ws_id, ex
            )
        return True

    async def _route_internal_message(self, data: GulpWsData) -> None:
        """
        route message NOT to connected clients BUT to the plugins which registered for it

        Args:
            data (GulpWsData): The internal message to process.
        """
        # MutyLogger.get_instance().debug(f"processing internal message: {data}")
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
        Broadcasts message to appropriate connected websockets (local and cross-instance).

        Args:
            data (GulpWsData): The message to broadcast.
            skip_list (list[str], optional): The list of websocket IDs to skip. Defaults to None.
        """
        if data.internal:
            # this is an internal message, route to plugins
            await self._route_internal_message(data)
            return

        # for broadcast types, publish to Redis so all instances receive it
        if data.type in GulpRedisBroker.get_instance().broadcast_types:
            redis_client = GulpRedis.get_instance()
            message_dict = data.model_dump(exclude_none=True)
            await redis_client.publish(message_dict)
            # NOTE: we'll still process locally below, the broadcast ensures other instances get it

        # route to local connected websockets
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
                    MutyLogger.get_instance().warning(
                        "failed to broadcast to (local) ws_id=%s: %s, removing dead socket",
                        ws_id,
                        result,
                    )
                    self._sockets.pop(ws_id, None)


class GulpRedisBroker:
    """
    Singleton class to manage Redis pub/sub for worker->main and instance<->instance communication.
    Provides methods for publishing data and handling broadcast messages.
    """

    _instance: "GulpRedisBroker" = None

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

    def __new__(cls):
        """
        Create a new instance of the class.
        """
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def get_instance(cls) -> "GulpRedisBroker":
        """
        Returns the singleton instance.

        Returns:
            GulpRedisBroker: The singleton instance.
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

    async def initialize(self) -> None:
        """
        Initialize Redis pub/sub subscriptions

        NOTE: must be called in the main process.
        """
        redis_client = GulpRedis.get_instance()

        # subscribe to worker->main channel
        await redis_client.subscribe(self._handle_pubsub_message)
        MutyLogger.get_instance().info("GulpRedisBroker initialized!")

    async def shutdown(self) -> None:
        """
        Deinitialize Redis pub/sub subscriptions

        NOTE: must be called in the main process.
        """
        redis_client = GulpRedis.get_instance()

        # unsubscribe from worker->main channel
        await redis_client.unsubscribe()
        MutyLogger.get_instance().info("GulpRedisBroker uninitialized!")

    async def _handle_pubsub_message(self, message: dict) -> None:
        """
        Handle a message from Redis pub/sub.

        Args:
            message (dict): The message dictionary (GulpWsData)
        """
        channel: str = message.pop("__channel__", None)
        server_id: str = message.pop("__server_id__", None)
        redis_client = GulpRedis.get_instance()

        # if this message is a pointer to a stored payload, try to fetch it and
        # replace the message dict with the full stored payload
        try:
            pl = message.get("payload")
            if isinstance(pl, dict) and pl.get("__pointer_key"):
                ptr = pl.get("__pointer_key")
                chunk_server_id = pl.get("__server_id")
                
                # only retrieve chunks if:
                # 1) we are the server that stored them (chunk_server_id == our server_id), OR
                # 2) the message is explicitly targeted to a ws_id we own
                should_retrieve = False
                if chunk_server_id == redis_client.server_id:
                    # we stored these chunks, we should retrieve them
                    should_retrieve = True
                else:
                    # check if the message targets a websocket we own
                    target_ws_id = message.get("ws_id")
                    if target_ws_id:
                        owner_server = await redis_client.ws_get_server(target_ws_id)
                        if owner_server == redis_client.server_id:
                            should_retrieve = True
                
                if not should_retrieve:
                    # this node should not retrieve chunks - another node will handle it
                    # log at debug level to avoid flooding logs
                    MutyLogger.get_instance().debug(
                        "skipping chunk retrieval for ptr=%s (chunk_server_id=%s, our_server_id=%s)",
                        ptr,
                        chunk_server_id,
                        redis_client.server_id,
                    )
                    # leave message as-is with pointer, don't process further
                    return
                
                try:
                    # If pointer contains __chunks, try atomic multi-GET+DEL for all chunk keys
                    chunks = pl.get("__chunks")
                    compressed_flag = pl.get("__compressed", False)
                    if chunks and int(chunks) > 0:
                        # build chunk keys: base:0 .. base:N-1
                        keys = [f"{ptr}:{i}" for i in range(int(chunks))]

                        # lua: get all keys, if any missing return nil, else delete all and return concatenated value
                        lua_multi = (
                            "local n = #KEYS\n"
                            "local vals = {}\n"
                            "for i=1,n do local v = redis.call('GET', KEYS[i]); if not v then return nil end; vals[i]=v end\n"
                            "for i=1,n do redis.call('DEL', KEYS[i]) end\n"
                            "return table.concat(vals)\n"
                        )
                        stored = await redis_client._redis.eval(
                            lua_multi, len(keys), *keys
                        )

                        if stored:
                            try:
                                data_bytes = stored
                                if compressed_flag:
                                    try:
                                        data_bytes = zlib.decompress(data_bytes)
                                    except Exception:
                                        MutyLogger.get_instance().exception(
                                            "failed to decompress stored chunks for base=%s",
                                            ptr,
                                        )
                                        data_bytes = None

                                if data_bytes:
                                    full_msg = orjson.loads(data_bytes)
                                    message = full_msg
                                else:
                                    MutyLogger.get_instance().warning(
                                        "failed to decode stored chunks for base=%s",
                                        ptr,
                                    )
                            except Exception:
                                MutyLogger.get_instance().exception(
                                    "failed to decode stored large payload for base=%s",
                                    ptr,
                                )
                        else:
                            MutyLogger.get_instance().warning(
                                "large payload chunked pointer missing in Redis for base=%s",
                                ptr,
                            )
                    else:
                        # single-key pointer handling: atomic GET+DEL for single key
                        lua = (
                            "local v = redis.call('GET', KEYS[1]);"
                            "if v then redis.call('DEL', KEYS[1]) end; return v"
                        )
                        stored = await redis_client._redis.eval(lua, 1, ptr)                        
                        if stored:
                            try:
                                full_msg = orjson.loads(stored)
                                message = full_msg
                            except Exception:
                                MutyLogger.get_instance().exception(
                                    "failed to decode stored large payload for key=%s",
                                    ptr,
                                )
                        else:
                            MutyLogger.get_instance().warning(
                                "large payload pointer missing in Redis for key=%s", ptr
                            )
                except Exception:
                    MutyLogger.get_instance().exception(
                        "error fetching and deleting large payload pointer %s from Redis",
                        ptr,
                    )
        except Exception:
            MutyLogger.get_instance().exception("error while resolving payload pointer")

        if (
            server_id
            and server_id == redis_client.server_id
            and channel == GulpRedisChannel.BROADCAST.value
        ):
            # avoid processing our own broadcast messages
            return

        try:
            wsd = GulpWsData.model_validate(message)
            if channel == GulpRedisChannel.WORKER_TO_MAIN.value:
                await self._process_message(wsd)
            elif channel == GulpRedisChannel.BROADCAST.value:
                # only process if we have this websocket or it's a broadcast type
                server_id = await redis_client.ws_get_server(wsd.ws_id)

                # check if this message is for this instance (we have the websocket) or if it's a broadcast type
                if (
                    server_id == redis_client.server_id
                    or wsd.type in self.broadcast_types
                ):
                    await self._process_message(wsd)
        except Exception as ex:
            MutyLogger.get_instance().error("error processing pubsub message: %s", ex)

    async def _process_message(self, msg: GulpWsData) -> None:
        """
        Process a single message and route to appropriate websockets.

        Args:
            msg (GulpWsData): The message to process
        """
        cws = GulpConnectedSockets.get_instance().get(msg.ws_id)
        if cws or msg.internal:
            await GulpConnectedSockets.get_instance().broadcast_message(msg)

    async def _put_common(self, wsd: GulpWsData) -> None:
        """
        Publish message to Redis (or process directly).

        Args:
            wsd (GulpWsData): The message to publish
        """
        from gulp.process import GulpProcess

        redis_client = GulpRedis.get_instance()
        message_dict = wsd.model_dump(exclude_none=True)

        # determine if this should be broadcast to all instances or just local
        if wsd.type in self.broadcast_types or wsd.internal:
            # process here
            if GulpProcess.get_instance().is_main_process():
                await self._process_message(wsd)
            else:
                message_dict["__channel__"] = GulpRedisChannel.WORKER_TO_MAIN.value
                await redis_client.publish(message_dict)

            # then broadcast to all other instances
            message_dict["__channel__"] = GulpRedisChannel.BROADCAST.value
            message_dict["__server_id__"] = redis_client.server_id
            await redis_client.publish(message_dict)
        elif GulpProcess.get_instance().is_main_process():
            # main process: directly process locally
            await self._process_message(wsd)
        else:
            # worker process: publish to main process of this instance
            message_dict["__channel__"] = GulpRedisChannel.WORKER_TO_MAIN.value
            await redis_client.publish(message_dict)

    async def put_internal_event(
        self,
        t: str,
        user_id: str = None,
        operation_id: str = None,
        req_id: str = None,
        data: dict = None,
    ) -> None:
        """
        called by internal components and plugins to publish internal events.

        this is used to broadcast internal events via the GulpInternalEventsManager

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
        await self._put_common(wsd)

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
        called by workers, publishes data to the main process via Redis pub/sub.

        args:
            t (str): the type of data being published
            user_id (str): the user id associated with this message
            ws_id (str, optional): the websocket id that will receive the message. if None, the message is broadcasted to all connected websockets
            operation_id (Optional[str]): the operation id if applicable
            req_id (Optional[str]): the request id if applicable
            d (Optional[Any]): the payload data
            private (bool): whether this message is private to the specified ws_id
        raises:
            WebSocketDisconnect: if websocket is not connected
        """

        # verify websocket is alive (check Redis registry)

        if not await GulpConnectedSocket.is_alive(ws_id):
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
            payload=d,
        )
        await self._put_common(wsd)
