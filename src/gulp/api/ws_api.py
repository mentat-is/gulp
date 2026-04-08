import asyncio
import os
import queue
import time
import zlib
import copy
from enum import StrEnum
from multiprocessing.managers import SyncManager
from queue import Empty, Queue
from threading import Lock
from typing import Annotated, Any, Literal, Optional

import hashlib
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

from gulp.api.prometheus_api import GulpMetrics
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.opensearch.structs import GulpDocument
from gulp.api.redis_api import GulpRedis, _MAIN_REDIS_CHANNEL
from gulp.config import GulpConfig
from gulp.structs import GulpPluginParameters


class GulpWsType(StrEnum):
    # the type of the websocket
    WS_DEFAULT = "default" # default websocket for logged in users, used for most interactions including collab updates, stats updates, user login/logout, etc. (basically all except ingest and inter-ui communications)
    WS_INGEST = "ingest" # websocket type for /ws_raw_ingest_api
    WS_CLIENT_DATA = "client_data" # websocket type for inter-ui communications


# data types for the websocket
WSDATA_ERROR = "ws_error" # GulpWsErrorPacket, for reporting errors on the websocket
WSDATA_CONNECTED = "ws_connected"  # GulpWsAcknoledgedPacket, whenever a websocket connection is established
WSDATA_COLLAB_CREATE = (
    "collab_create"  # GulpCollabCreatePacket, whenever a collab object without a specified `ws_data_type` is created (note,highlight,link,...)
)
WSDATA_COLLAB_UPDATE = (
    "collab_update"  # GulpCollabUpdatePacket, whenever a collab object without a specified `ws_data_type` is updated (note,highlight,link,...)
)
WSDATA_COLLAB_DELETE = (
    "collab_delete"  # GulpCollabDeletePacket, whenever a collab object without a specified `ws_data_type` is deleted (note,highlight,link,...)
)
WSDATA_STATS_CREATE = (
    "stats_create"  # GulpRequestStats, whenever a stats object is created
)
WSDATA_STATS_UPDATE = (
    "stats_update"  # GulpRequestStats, whenever a stats object is updated
)
WSDATA_USER_LOGIN = "user_login"  # GulpUserAccessPacket, whenever a user is logged in
WSDATA_USER_LOGOUT = "user_logout"  # GulpUserAccessPacketv, whenever a user is logged out
WSDATA_DOCUMENTS_CHUNK = "docs_chunk"  # GulpDocumentsChunkPacket, whenever a chunk of GulpDocuments is transmitted over the ws during ingestion or query
WSDATA_INGEST_SOURCE_DONE = "ingest_source_done"  # GulpIngestSourceDonePacket, this is sent in the end of an ingestion operation, one per source
WSDATA_INGEST_RAW_PROGRESS = "ingest_raw_progress"  # GulpIngestRawProgress, this is sent in the end of an ingestion operation for realtime ingestion using `/ingest_raw` API or `/ws_ingest_raw` websocket
WSDATA_REBASE_DONE = "rebase_done"  # GulpUpdateDocumentsStats, sent when a rebase operation is done via `/opensearch_rebase_by_query` API
WSDATA_QUERY_GROUP_MATCH = "query_group_match"  # GulpQueryGroupMatchPacket, this is sent to indicate a query group match, i.e. a query group that matched some queries when `GulpQueryParameters.group` has been set
WSDATA_QUERY_DONE = "query_done"  # GulpQueryDonePacket, this is sent in the end of a query operation, one per single query (i.e. a sigma zip query may generate multiple single queries, called a query group)
WSDATA_CLIENT_DATA = "client_data"  # arbitrary content, to be routed to all connected websockets having type=`WS_CLIENT_DATA`: this is used by the UI clients to communicate data each other via an ui-specific protocol, the backend just routes the packets.

class GulpMessageRoutingTarget(StrEnum):
    """
    message routing targets
    """

    BROADCAST = "broadcast"  # message to be broadcasted across gulp instances
    WORKER_TO_MAIN = "worker_to_main"  # messages from worker processes to main process (local)
    CLIENT_DATA = "client_data"  # messages from the ws_client_data websocket (ui-to-ui)

    def redis_channel_name(self) -> str:
        """Return the actual Redis pubsub channel name for this routing type

        - CLIENT_DATA -> "gulpredis:client_data"
        - all others -> "gulpredis"
        """
        if self is GulpMessageRoutingTarget.CLIENT_DATA:
            return "gulpredis:client_data"
        return "gulpredis"


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
    propagate_internal: Annotated[
        bool,
        Field(
            description="if true and `internal` is set, the internal event will be propagated to all other instances instead of being handled in the main process of the current instance only.",
        ),
    ] = False
    # explicitly carry a routing/channel marker so pubsub handlers
    # can make routing decisions without inspecting payload internals
    route_target_type: Annotated[
        Optional[str],
        Field(description="One of the GulpRedisChannel types, to determine the routing target (broadcast, internal, ...)"),
    ] = None
    origin_server_id: Annotated[
        Optional[str],
        Field(description="The ID of the server that originated the message."),
    ] = None
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

    # optional Redis key used for worker->main internal event request/response.
    # when set, main process writes the internal event dispatch result to this key.
    response_key: Annotated[
        Optional[str],
        Field(
            description="Optional Redis key used to write back internal event responses.",
            alias="__response_key__",
        ),
    ] = None

    def check_type_in_broadcast_types(self) -> bool:
        """
        Determine whether this message's type should be treated as a broadcast-type.

        Rationale:
        - Some messages carry the actual object "type" inside the payload (typically
          under `payload["obj"]` or within the first element of a bulk list). For
          example, collab objects embed their object `type` (e.g. "note") inside
          `payload.obj` rather than using `GulpWsData.type`.
        - This method therefore "digs" into `payload` to look for an object-level
          `type` and checks that against the broker's `broadcast_types` set. If an
          object-level type is found and is present in `broadcast_types`, the
          message is considered broadcastable.
        - If the payload does not contain an object `type`, we fall back to
          checking `self.type`.

        Returns:
            bool: True if the message should be broadcasted, False otherwise.
        """
        payload: dict = self.payload if isinstance(self.payload, dict) else {}
        payload_data: dict = payload.get("obj", {})
        if self.type == WSDATA_COLLAB_DELETE:
            # special case for delete, as payload is not under "obj"
            payload_data = payload

        if isinstance(payload_data, dict):
            # single
            payload_data_type = payload_data.get("type", None) # i.e. note
        elif isinstance(payload_data, list):
            # bulk
            payload_data_type = payload_data[0].get("type", None) if len(payload_data) > 0 else None

        if payload_data_type and payload_data_type in GulpRedisBroker.get_instance().broadcast_types:
            # MutyLogger.get_instance().debug(
            #     "GulpWsData.check_type_in_broadcast_types(): payload_data_type=%s is in broadcast types",
            #     payload_data_type,
            # )
            return True
        
        t: str = self.type        

        # resort to plain data type
        if t in GulpRedisBroker.get_instance().broadcast_types:
            # MutyLogger.get_instance().debug(
            #     "GulpWsData.check_type_in_broadcast_types(): type=%s is in broadcast types",
            #     self.type,
            # )
            return True
        return False

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
    req_id: Annotated[str, Field(description="The request ID.")]
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


class GulpIngestRawProgress(BaseModel):
    "to signal on the websocket that ingest raw progress for realtime"

    model_config = ConfigDict(
        populate_by_name=True,
        json_schema_extra={
            "examples": [
                {
                    "last": True,
                    "status": GulpRequestStatus.DONE,
                }
            ]
        },
    )

    status: Annotated[
        str, Field(description="The status of the query operation (done/failed).")
    ] = GulpRequestStatus.DONE
    last: Annotated[
        bool,
        Field(
            description="set to True to indicate the last packet of a stream.",
        ),
    ]


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
            description="""access token from login API.
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
    data: Annotated[
        Optional[dict],
        Field(
            description="optional arbitrary data to be associated with this websocket connection.",
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

    def __init__(
        self,
        ws: WebSocket,
        ws_id: str,
        types: list[str] = None,
        operation_ids: list[str] = None,
        socket_type: GulpWsType = GulpWsType.WS_DEFAULT,
        data: dict = None,
        user_id: str = None,
    ):
        """
        Initializes the ConnectedSocket object.

        Args:
            ws (WebSocket): The WebSocket object.
            ws_id (str): The WebSocket ID.
            types (list[str], optional): The types of data this websocket is interested in. Defaults to None (all).
            operation_ids (list[str], optional): The operation/s this websocket is interested in. Defaults to None (all).
            socket_type (GulpWsType, optional): The type of the websocket. Defaults to GulpWsType.WS_DEFAULT.
            data (dict, optional): Optional arbitrary data to be associated with this websocket connection. Defaults to None.
            user_id (str, optional): The user ID associated with this websocket connection. Defaults to None.
        """
        from gulp.process import GulpProcess

        self.ws = ws
        self.ws_id = ws_id
        self.types = types
        self._data = data or {}
        self._terminated: bool = False
        self.operation_ids = operation_ids
        self.socket_type = socket_type
        self.user_id = user_id
        self._cleaned_up = False
        self._server_id = GulpProcess.get_instance().server_id
        self._tasks: list[asyncio.Task] = []
        # store queue capacity for quick reference in telemetry
        self._queue_capacity: int = GulpConfig.get_instance().ws_queue_max_size()
        # each socket has its own asyncio queue, consumed by its own task with bounded size
        self.q = asyncio.Queue(maxsize=self._queue_capacity)
        # remember queue high watermark for diagnostics
        self._queue_high_watermark: int = 0
        # throttle repeated pressure logs
        self._last_pressure_log_ts: float = 0.0

    async def enqueue_message(self, message: dict) -> bool:
        """
        enqueue a websocket message with backpressure handling

        Uses a combination of:
        1. Pre-enqueue throttle delay based on queue pressure
        2. Blocking enqueue with timeout to give slow clients time to catch up
        3. Disconnection on timeout (client considered unresponsive)

        Args:
            message (dict): payload ready for serialization
        Returns:
            bool: True when the message has been enqueued, False if client should be disconnected
        Throws:
            RuntimeError: propagated when queue operations fail unexpectedly
        """
        # apply pre-enqueue throttle delay if queue is under pressure
        throttle_delay = self._calculate_throttle_delay()
        if throttle_delay > 0:
            await asyncio.sleep(throttle_delay)

        try:
            # blocking enqueue with timeout
            timeout: float = GulpConfig.get_instance().ws_enqueue_timeout()
            await asyncio.wait_for(self.q.put(message), timeout=timeout)
            self._record_queue_pressure()
            return True
        except asyncio.TimeoutError:
            # client cannot keep up after generous timeout - disconnect
            MutyLogger.get_instance().error(
                "ws_id=%s ENQUEUE TIMEOUT after %.2fs - client unresponsive, will disconnect",
                self.ws_id,
                timeout,
            )
            return False
        except asyncio.QueueFull:
            # should not happen with blocking put, but handle defensively
            MutyLogger.get_instance().exception(
                "**** ws_id=%s QUEUE FULL (unexpected with blocking) ****", self.ws_id
            )
            return False

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

    def _calculate_throttle_delay(self) -> float:
        """
        calculate intake throttle delay based on current queue pressure

        Returns:
            float: delay in seconds (0 if no throttling needed)
        """
        if not self._queue_capacity:
            return 0.0

        utilization: float = self.q.qsize() / self._queue_capacity
        throttle_threshold: float = GulpConfig.get_instance().ws_throttle_threshold()

        if utilization < throttle_threshold:
            return 0.0

        # progressive delay from 0 to max_delay based on utilization above threshold
        max_delay: float = GulpConfig.get_instance().ws_throttle_max_delay()
        pressure_above_threshold = (utilization - throttle_threshold) / (
            1.0 - throttle_threshold
        )
        return max_delay * pressure_above_threshold

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
        if GulpConfig.get_instance().ws_ignore_missing():
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
        processes outgoing messages and sends them to the websocket
        """
        try:
            MutyLogger.get_instance().debug(
                f"---> starting send loop for ws_id={self.ws_id}"
            )

            # tracking variables for metrics
            message_count: int = 0
            yield_counter: int = 0

            while True:
                # process message from the websocket queue
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
        # ttl for cache entries
        self._ws_server_cache_ttl: float = GulpConfig.get_instance().ws_server_cache_ttl()
        
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

    def aggregate_queue_utilization(self) -> float:
        """Return aggregate websocket queue utilization across all connected sockets.

        Returns a float between 0.0 and 1.0 representing (total_enqueued / total_capacity).
        Returns 0.0 if there are no tracked sockets or total capacity is zero.
        """
        total_enqueued = 0
        total_capacity = 0
        for sock in self._sockets.values():
            try:
                total_enqueued += sock.q.qsize()
                total_capacity += sock._queue_capacity or 0
            except Exception:
                # defensive: ignore sockets that misreport
                continue

        if total_capacity == 0:
            return 0.0
        return min(1.0, total_enqueued / total_capacity)

    def cache_server(self, ws_id: str, server_id: Optional[str]) -> None:
        """
        store websocket ownership information locally to avoid extra Redis hits

        - positive entries cache the owning `server_id` for `_ws_server_cache_ttl` seconds
        - negative entries (server_id is None) are *also cached* for a short period to
          avoid repeated Redis lookups for non-existent ws_ids

        Args:
            ws_id (str): websocket identifier
            server_id (Optional[str]): owning server id if known (None for not found)
        Returns:
            None
        """
        if not ws_id:
            return

        # negative caching: avoid repeated Redis round-trips for missing ws_ids
        if not server_id:
            neg_ttl = min(30.0, max(1.0, self._ws_server_cache_ttl / 10.0))
            expires_at: float = time.monotonic() + neg_ttl
            # store explicit None as server id to indicate a negative cache entry
            self._ws_server_cache[ws_id] = (None, expires_at)
            return

        # positive cache entry
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
        data: dict = None,
        user_id: str = None,
    ) -> GulpConnectedSocket:
        """
        Adds a websocket to the connected sockets list and registers in Redis.

        Args:
            ws (WebSocket): The WebSocket object.
            ws_id (str): The WebSocket ID.
            types (list[str], optional): The types of data this websocket is interested in. Defaults to None (all)
            operation_ids (list[str], optional): The operations this websocket is interested in. Defaults to None (all)
            socket_type (GulpWsType, optional): The type of the websocket. Defaults to GulpWsType.WS_DEFAULT.
            data (dict, optional): Optional arbitrary data to be associated with this websocket connection. Defaults to None.
            user_id (str, optional): The user ID associated with this websocket connection. Defaults to None.
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
            # del self._sockets[ws_id] # cleanup causes the websocket loop to terminate, and the loop already calls remove()

        # create connected socket instance
        connected_socket = GulpConnectedSocket(
            ws=ws,
            ws_id=ws_id,
            types=types,
            operation_ids=operation_ids,
            socket_type=socket_type,
            data=data,
            user_id=user_id,
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

        if GulpConfig.get_instance().prometheus_enabled():
            # update Prometheus gauge
            try:
                GulpMetrics.ws_connected_sockets.set(len(self._sockets))
            except Exception:
                pass

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

        if GulpConfig.get_instance().prometheus_enabled():
            # update Prometheus gauge
            try:
                GulpMetrics.ws_connected_sockets.set(len(self._sockets))
            except Exception:
                pass

    def sockets(self) -> dict[str, GulpConnectedSocket]:
        """
        gets all connected sockets.

        Returns:
            dict[str, GulpConnectedSocket]: A dictionary of WebSocket ID to ConnectedSocket objects.
        """
        return self._sockets
        
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
        
    async def route_message_to_local_websockets(
        self, wsd: GulpWsData, wsd_dict: dict, skip_list: list[str] = None
    ) -> None:
        """
        route message only to local connected websockets, without publishing to Redis

        Args:
            wsd (GulpWsData): The message to route.
            wsd_dict (dict): The message as a dictionary (pre-serialized for efficiency).
            skip_list (list[str], optional): The list of websocket IDs to skip. Defaults to None.
        """
        socket_items = list(self._sockets.items())

        # collect routing tasks
        tasks = []
        ws_ids = []
        for ws_id, cws in socket_items:
            if skip_list and ws_id in skip_list:
                continue

            """
            the message is routed if:
            - the target_ws is of type WS_DEFAULT
            - the target_ws types is None or includes data.type
            - the target_ws operation_ids is None or includes data.operation_id
            - if data.private is True, message is routed if the target_ws.ws_id matches data.ws_id unless it is a login
            """
            # route only to WS_DEFAULT sockets: these are the sockets used by the UI for receiving most of the data (collab objects and chunks), corresponding to the ws_id in most of the API calls
            if cws.socket_type != GulpWsType.WS_DEFAULT:
                # MutyLogger.get_instance().warning("***** data.ws_id=%s, not routing cws.socket_type=%s",wsd.ws_id, cws.socket_type)
                continue

            # check type filter
            if cws.types and wsd.type not in cws.types:
                # MutyLogger.get_instance().warning("***** data.ws_id=%s, not routing data.type=%s", wsd.ws_id, wsd.type)
                continue

            # check operation filter
            if cws.operation_ids and wsd.operation_id not in cws.operation_ids:
                # MutyLogger.get_instance().warning("***** wsd.ws_id=%s, not routing data.operation_id=%s", wsd.ws_id, wsd.operation_id)
                continue

            # check if this must be broadcasted
            # is_bt_type: bool = data.check_type_in_broadcast_types()

            # handle private messages, do not route them unless it is a login message
            if wsd.ws_id and wsd.private and wsd.type != WSDATA_USER_LOGIN:
                if cws.ws_id != wsd.ws_id:
                    # MutyLogger.get_instance().warning("***** wsd.ws_id=%s, not routing private message to wsd.ws_id=%s", wsd.ws_id, cws.ws_id)
                    continue

            tasks.append(cws.enqueue_message(wsd_dict))
            ws_ids.append(ws_id)

        if tasks:
            # execute concurrently
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # check for exceptions
            for i, result in enumerate(results):
                if isinstance(result, Exception) or result is False:
                    ws_id = ws_ids[i]
                    MutyLogger.get_instance().warning(
                        "failed to broadcast to local ws, ws_id=%s: %s, removing dead socket",
                        ws_id,
                        result,
                    )
                    self.remove(ws_id)

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
            #WSDATA_COLLAB_CREATE,
            #WSDATA_COLLAB_UPDATE,
            #WSDATA_COLLAB_DELETE,
            WSDATA_REBASE_DONE,
            WSDATA_INGEST_SOURCE_DONE,
            WSDATA_USER_LOGIN,
            WSDATA_USER_LOGOUT,
            "note",
            "link",
            "highlight",
            "context",
            "source",
            "operation",
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
        redis_client = GulpRedis.get_instance()
        
        # extract Redis channel name (added by subscriber loop)
        redis_channel: str = message.pop("__redis_channel__", None)
        # and the message routing target and origin
        route_target_type: str = message.pop("route_target_type")
        origin_server_id: str = message.pop("origin_server_id")

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
                # 
                # NOTE: for BROADCAST messages, all instances need the payload so we use
                # GET instead of GET+DEL to allow multiple reads, relying on TTL for cleanup
                should_retrieve = False
                use_getdel = True  # whether to delete chunks after retrieval
                
                if chunk_server_id == redis_client.server_id:
                    # we stored these chunks, we should retrieve them
                    should_retrieve = True
                elif (
                    (redis_channel == GulpMessageRoutingTarget.BROADCAST.redis_channel_name()
                    or (redis_channel and redis_channel.startswith(_MAIN_REDIS_CHANNEL + ":group")))
                    and route_target_type == GulpMessageRoutingTarget.BROADCAST.value
                ):
                    # broadcast messages (including group-partitioned channels): retrieve without deleting (multiple instances need it)
                    should_retrieve = True
                    use_getdel = False
                else:
                    # check if the message targets a websocket we own
                    target_ws_id = message.get("ws_id")
                    if target_ws_id:
                        owner_server = await redis_client.ws_get_server(target_ws_id)
                        if owner_server == redis_client.server_id:
                            should_retrieve = True

                if not should_retrieve:
                    # this node should not retrieve chunks - another node will handle it
                    if GulpConfig.get_instance().prometheus_enabled():
                        # track skipped pointer resolutions for non-owners
                        try:
                            GulpMetrics.ws_payload_pointer_resolve_total.labels(
                                outcome="skipped_not_owner"
                            ).inc()
                        except Exception:
                            pass
                    MutyLogger.get_instance().debug(
                        "skipping chunk retrieval for ptr=%s (chunk_server_id=%s, our_server_id=%s, redis_channel=%s)",
                        ptr,
                        chunk_server_id,
                        redis_client.server_id,
                        redis_channel,
                    )
                    # leave message as-is with pointer, don't process further
                    return

                try:
                    # If pointer contains __chunks, retrieve all chunk keys
                    chunks = pl.get("__chunks")
                    compressed_flag = pl.get("__compressed", False)
                    if chunks and int(chunks) > 0:
                        keys = [f"{ptr}:{i}" for i in range(int(chunks))]

                        if use_getdel:
                            # Atomic GET+DEL for exclusive retrieval (non-broadcast messages)
                            lua_multi = (
                                "local n = #KEYS\n"
                                "local vals = {}\n"
                                "for i=1,n do local v = redis.call('GET', KEYS[i]); if not v then return nil end; vals[i]=v end\n"
                                "for i=1,n do redis.call('DEL', KEYS[i]) end\n"
                                "return table.concat(vals)\n"
                            )
                        else:
                            # GET-only for broadcast messages (multiple instances need it, TTL handles cleanup)
                            lua_multi = (
                                "local n = #KEYS\n"
                                "local vals = {}\n"
                                "for i=1,n do local v = redis.call('GET', KEYS[i]); if not v then return nil end; vals[i]=v end\n"
                                "return table.concat(vals)\n"
                            )

                        stored = None
                        for attempt in range(3):
                            try:
                                stored = await redis_client._redis.eval(
                                    lua_multi, len(keys), *keys
                                )
                                break
                            except Exception as ex:
                                if attempt == 2:
                                    raise ex
                                await asyncio.sleep(0.2 * (attempt + 1))

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
                                    if GulpConfig.get_instance().prometheus_enabled():
                                        # track successful pointer resolutions
                                        try:
                                            GulpMetrics.ws_payload_pointer_resolve_total.labels(
                                                outcome="resolved"
                                            ).inc()
                                        except Exception:
                                            pass
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
                            if GulpConfig.get_instance().prometheus_enabled():
                                # track missing pointer data for chunked payloads
                                try:
                                    GulpMetrics.ws_payload_pointer_resolve_total.labels(
                                        outcome="missing"
                                    ).inc()
                                except Exception:
                                    pass
                            MutyLogger.get_instance().warning(
                                "large payload chunked pointer missing in Redis for base=%s",
                                ptr,
                            )
                    else:
                        # single-key pointer handling
                        if use_getdel:
                            # Atomic GET+DEL for single key
                            lua = (
                                "local v = redis.call('GET', KEYS[1]);"
                                "if v then redis.call('DEL', KEYS[1]) end; return v"
                            )
                        else:
                            # GET-only for broadcast
                            lua = "return redis.call('GET', KEYS[1])"
                        
                        stored = None
                        for attempt in range(3):
                            try:
                                stored = await redis_client._redis.eval(lua, 1, ptr)
                                break
                            except Exception as ex:
                                if attempt == 2:
                                    raise ex
                                await asyncio.sleep(0.2 * (attempt + 1))
                        if stored:
                            try:
                                full_msg = orjson.loads(stored)
                                message = full_msg
                                if GulpConfig.get_instance().prometheus_enabled():
                                    try:
                                        # track successful pointer resolutions for single-key pointers
                                        GulpMetrics.ws_payload_pointer_resolve_total.labels(
                                            outcome="resolved"
                                        ).inc()
                                    except Exception:
                                        pass
                            except Exception:
                                MutyLogger.get_instance().exception(
                                    "failed to decode stored large payload for key=%s",
                                    ptr,
                                )
                        else:
                            if GulpConfig.get_instance().prometheus_enabled():
                                # track missing pointer data for single-key payloads
                                try:
                                    GulpMetrics.ws_payload_pointer_resolve_total.labels(
                                        outcome="missing"
                                    ).inc()
                                except Exception:
                                    pass
                            MutyLogger.get_instance().warning(
                                "large payload pointer missing in Redis for key=%s", ptr
                            )
                except Exception:
                    if GulpConfig.get_instance().prometheus_enabled():
                        # track errors during pointer resolution (e.g. Redis issues) separately from missing data
                        try:                            
                            GulpMetrics.ws_payload_pointer_resolve_total.labels(
                                outcome="error"
                            ).inc()
                        except Exception:
                            pass
                    MutyLogger.get_instance().exception(
                        "error fetching and deleting large payload pointer %s from Redis",
                        ptr,
                    )
        except Exception:
            MutyLogger.get_instance().exception("error while resolving payload pointer")

        # Route based on routing target
        try:
            # validate into model once and reuse for all cases that need it
            wsd = GulpWsData.model_validate(message)

            if route_target_type == GulpMessageRoutingTarget.WORKER_TO_MAIN.value:
                if wsd.internal and (wsd.propagate_internal or origin_server_id == redis_client.server_id):                
                    # internal messages:
                    # we process internal messages only if they are marked as internal and either the message is 
                    # for this server instance or is to be propagated across instances
                    # internal event from worker: dispatch to plugins
                    from gulp.plugin import GulpInternalEventsManager
                    result = await GulpInternalEventsManager.get_instance().dispatch_internal_event(
                        wsd.type,
                        data=wsd.payload,
                        user_id=wsd.user_id,
                        operation_id=wsd.operation_id,
                    )

                    # if requested by caller, write back the result for synchronous usage via GulpRedisBroker.put_internal_wait()
                    if wsd.response_key:
                        try:
                            # keep key short-lived; caller deletes after reading
                            await redis_client._redis.set(
                                wsd.response_key,
                                orjson.dumps(result or []),
                                ex=300,
                            )
                        except Exception:
                            MutyLogger.get_instance().exception(
                                "error writing internal event response for key=%s",
                                wsd.response_key,
                            )
                        return
                else:
                    # this also just sends message to single ws_id if ws_id is set
                    await GulpConnectedSockets.get_instance().route_message_to_local_websockets(wsd, message)

            elif route_target_type == GulpMessageRoutingTarget.BROADCAST.value:
                # messages to be broadcast across instances
                # here we process only if the current server is DIFFERENT from the origin server, to avoid duplicate processing of our own broadcast messages (we already routed to local sockets in _put_common)
                if origin_server_id == redis_client.server_id:
                    # we already routed this message to our local sockets in _put_common()
                    return
                await GulpConnectedSockets.get_instance().route_message_to_local_websockets(wsd, message)

            elif route_target_type == GulpMessageRoutingTarget.CLIENT_DATA.value:
                # Client-data messages (UI <-> UI)
                # skip messages originating from this server instance
                if origin_server_id == redis_client.server_id:
                    return
                await self._route_message_to_local_client_data_websockets(wsd, message)
                return

        except Exception as ex:
            MutyLogger.get_instance().error("error processing pubsub message: %s", ex)

    async def _route_message_to_local_client_data_websockets(self, wsd: GulpWsData, wsd_dict: dict) -> None:
        """
        route message to connected WS_CLIENT_DATA websockets (ui<->ui communication through the backend)

        Args:
            wsd (GulpWsData): The client_data message
            wsd_dict (dict): The message as a dictionary (pre-serialized for efficiency).
        """
        MutyLogger.get_instance().debug(
            "processing cross-instance client_data: ws_id=%s, operation_id=%s",
            wsd.ws_id, wsd.operation_id
        )
        s = GulpConnectedSockets.get_instance()
        
        # collect routing tasks
        tasks = []
        ws_ids = []
        socket_items = list(s.sockets().items())
        for ws_id, cws in socket_items:
            # skip if not client_data socket
            if cws.socket_type != GulpWsType.WS_CLIENT_DATA:
                continue

            if wsd.ws_id and cws.ws_id != wsd.ws_id:
                # if message has ws_id, only route to matching ws_id
                continue

            # filter by operation_id if set (empty list means accept all)
            if (
                wsd.operation_id
                and cws.operation_ids
                and wsd.operation_id not in cws.operation_ids
            ):
                continue

            # filter by target_user_ids if present in payload
            # check if recipient's user matches target list
            pl = wsd.payload or {}
            target_user_ids = []
            if isinstance(pl, dict):
                target_user_ids = list(pl.get("target_user_ids", []))
            if target_user_ids and cws.user_id not in target_user_ids:
                continue
            tasks.append(cws.enqueue_message(wsd_dict))
            ws_ids.append(ws_id)

        if tasks:
            # execute concurrently
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # check for exceptions
            for i, result in enumerate(results):
                if isinstance(result, Exception) or result is False:
                    ws_id = ws_ids[i]
                    MutyLogger.get_instance().warning(
                        "failed to broadcast to local ws, ws_id=%s: %s, removing dead socket",
                        ws_id,
                        result,
                    )
                    self.remove(ws_id)



                if isinstance(result, Exception):
                    ws_id = ws_ids[i]
                    MutyLogger.get_instance().warning(
                        "failed to broadcast to local client_data ws, ws_id=%s: %s, removing dead socket",
                        ws_id,
                        result,
                    )
                    self._sockets.pop(ws_id, None)
                else:                    
                    """MutyLogger.get_instance().debug(
                        "routed ws_client_data to ws_id=%s", cws.ws_id
                    )"""
                    pass

    async def _put_common(self, wsd: GulpWsData) -> None:
        """
        Publish message to Redis (or process directly).

        Args:
            wsd (GulpWsData): The message to publish
        """
        from gulp.process import GulpProcess

        redis_client = GulpRedis.get_instance()

        # Routing policy
        # - workers ALWAYS publish to main process using WORKER_TO_MAIN
        # - main process decides final routing based on explicit `channel` metadata
        is_main: bool = GulpProcess.get_instance().is_main_process()

        if not is_main or wsd.internal:
            # we're running in a worker or it's an internal message: always forward to main process
            wsd.route_target_type = GulpMessageRoutingTarget.WORKER_TO_MAIN.value
            wsd.origin_server_id = redis_client.server_id
            await redis_client.publish(wsd.model_dump(exclude_none=True), channel=redis_client.worker_to_main_channel())            
            return

        # BROADCAST messages: route locally and publish so other instances receive them
        is_broadcast = wsd.check_type_in_broadcast_types()
        if is_broadcast or wsd.ws_id:
            # route to local websockets
            wsd.origin_server_id = redis_client.server_id
            wsd_dict = wsd.model_dump(exclude_none=True)
            await GulpConnectedSockets.get_instance().route_message_to_local_websockets(wsd, wsd_dict)

            if is_broadcast:
                # publish to Redis so all gulp instances will receive it as well
                wsd_dict["route_target_type"] = GulpMessageRoutingTarget.BROADCAST.value
                await redis_client.publish(wsd_dict, channel=GulpMessageRoutingTarget.BROADCAST.redis_channel_name())
        return

    async def _wait_internal_response(
        self,
        response_key: str,
        timeout: float | None = None,
        poll_interval: float = 0.05,
    ) -> dict:
        """Wait for an internal event response dict written by the main process.

        Args:
            response_key (str): Redis key where main process writes the response.
            timeout (float | None): Max seconds to wait. None waits forever.
            poll_interval (float): Sleep interval between Redis polls.

        Returns:
            dict: The response dictionary.

        Raises:
            TimeoutError: If timeout expires before a response is available.
        """
        redis_client = GulpRedis.get_instance()
        start = time.monotonic()

        while True:
            raw = await redis_client._redis.get(response_key)
            if raw is not None:
                # best effort cleanup of rendezvous key once consumed
                try:
                    await redis_client._redis.delete(response_key)
                except Exception:
                    pass

                try:
                    parsed = orjson.loads(raw)
                except Exception:
                    MutyLogger.get_instance().warning(
                        "invalid internal event response payload for key=%s",
                        response_key,
                    )
                    return {}
                return parsed if isinstance(parsed, dict) else {"result": parsed}

            if timeout is not None and timeout >= 0:
                if (time.monotonic() - start) >= timeout:
                    raise TimeoutError(
                        f"timeout waiting for internal event response on key={response_key}"
                    )

            await asyncio.sleep(poll_interval)

    async def put_internal_event(
        self,
        t: str,
        user_id: str = None,
        operation_id: str = None,
        req_id: str = None,
        data: dict = None,
        propagate: bool=False
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
            propagate (bool, optional): whether to propagate the event to other instances. Defaults to False.
        """
        wsd = GulpWsData(
            timestamp=muty.time.now_msec(),
            type=t,
            user_id=user_id,
            req_id=req_id,
            operation_id=operation_id,
            payload=data,
            internal=True,
            propagate_internal=propagate,
        )
        await self._put_common(wsd)

    async def put_internal_event_wait(
        self,
        t: str,
        user_id: str = None,
        operation_id: str = None,
        req_id: str = None,
        data: dict = None,
        timeout: float | None = None,
    ) -> "GulpInternalEventResult":
        """Publish an internal event and wait for the main-process response dict.

        The message is processed in the main process like `put_internal_event`, but this
        method additionally waits for a response dictionary produced after processing.

        Args:
            t (str): Internal event type.
            user_id (str, optional): Event user id.
            operation_id (str, optional): Event operation id.
            req_id (str, optional): Event request id.
            data (dict, optional): Event payload.
            timeout (float | None): Wait timeout in seconds. None waits forever.

        Returns:
            GulpInternalEventResult: The result of the internal event processing, as returned by the plugins that handled the event (if any), or None
        """
        response_key = f"gulp:internal:wait:{muty.string.generate_unique()}"

        wsd = GulpWsData(
            timestamp=muty.time.now_msec(),
            type=t,
            user_id=user_id,
            req_id=req_id,
            operation_id=operation_id,
            payload=data,
            internal=True,
            response_key=response_key,
        )
        await self._put_common(wsd)

        # wait for the response
        res = await self._wait_internal_response(response_key=response_key, timeout=timeout)
        if res:
            try:
                from gulp.plugin import GulpInternalEventResult
                return GulpInternalEventResult.model_validate(res)
            except Exception as e:
                MutyLogger.get_instance().exception(e)
        return None
    
    async def put(
        self,
        t: str,
        user_id: str,
        ws_id: str = None,
        operation_id: str = None,
        req_id: str = None,
        d: Any = None,
        private: bool = False,
        force_ignore_missing_ws: bool = False,
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
            force_ignore_missing_ws (bool): whether to ignore missing websocket and not raise an error
        raises:
            WebSocketDisconnect: if websocket is not connected
        """

        # verify websocket is alive (check Redis registry)

        if not force_ignore_missing_ws and ws_id:
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
