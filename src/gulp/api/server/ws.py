"""
Websocket implementation for the GULP (Generic Unstructured Language Processing) API.

This module provides websocket endpoints for real-time bidirectional communication between
clients and the GULP server. It supports three main connection types:
1. Default websocket (/ws) - Generic websocket connection for data exchange
2. Ingest websocket (/ws_ingest_raw) - Specialized for streaming ingestion of raw data
3. Client data websocket (/ws_client_data) - For routing UI data between connected clients

The websocket protocol follows a simple pattern:
1. Authentication via token and establishment of a connection identified by ws_id
2. Two-way communication with rate limiting to prevent overloading
3. Support for broadcasting events to other connected clients

Key components:
- GulpAPIWebsocket: Main class handling websocket connections, authentication, and messaging
- WsIngestRawWorker: Background worker for processing raw ingestion requests
- Support for user permissions and authentication

The module integrates with the rest of the GULP API, allowing for real-time updates
during ingestion operations, collaboration features, and inter-client communication.

"""

import asyncio
import time
from typing import Annotated, Awaitable, Callable, Optional

import muty.log
import muty.string
import muty.time
import orjson
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from fastapi.websockets import WebSocketState
from muty.log import MutyLogger
from pydantic import BaseModel, Field

from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.stats import GulpIngestionStats, GulpRequestStats, RequestStatsType
from gulp.api.collab.structs import (
    GulpRequestStatus,
    GulpUserPermission,
    MissingPermission,
)
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.redis_api import GulpRedis
from gulp.api.server_api import GulpServer
from gulp.api.ws_api import (
    WSDATA_CLIENT_DATA,
    WSDATA_CONNECTED,
    WSDATA_ERROR,
    WSDATA_USER_LOGIN,
    GulpClientDataPacket,
    GulpConnectedSocket,
    GulpConnectedSockets,
    GulpRedisBroker,
    GulpUserAccessPacket,
    GulpWsAcknowledgedPacket,
    GulpWsAuthPacket,
    GulpWsData,
    GulpWsError,
    GulpWsErrorPacket,
    GulpWsIngestPacket,
    GulpWsType,
)
from gulp.config import GulpConfig
from gulp.plugin import GulpPluginBase, GulpPluginCacheMode
from gulp.process import GulpProcess
from gulp.structs import ObjectNotFound

router = APIRouter()

# default TTL for idle streams (seconds) — stream deleted only after this much inactivity
STREAM_TTL = 3600


class InternalWsIngestPacket(BaseModel):
    """
    holds data for the ws ingest worker
    """

    user_id: Annotated[str, Field(description="the user id")]
    index: Annotated[str, Field(description="the index to ingest into")]
    data: Annotated[
        GulpWsIngestPacket, Field(description="a GulpWsIngestPacket dictionary")
    ]
    raw_data: Annotated[
        bytes, Field(description="raw data received from the websocket")
    ]


class WsIngestRawWorker:
    """
    a gulp worker task to handle websocket raw ingestion, running in a worker process

    1 ws -> 1 task in a worker process
    """
    # Stream lifecycle (per websocket):
    # - Main process creates a `stream_key` for this websocket:
    #     `gulp:stream:ws_ingest:{server_id}:{ws_id}`
    # - Main process `await pool_worker.put(packet)` performs `XADD` into the stream.
    # - A worker process consumes messages with `XREADGROUP` from a consumer group
    #   (`gulp:ws_ingest:group`) using a unique consumer name `{server_id}:{ws_id}`.
    # - When the main process wants the worker to stop it appends a sentinel
    #   message `{"terminate": True}` to the stream; the worker ACKs/DEL and
    #   sets a done key `gulp:ws_ingest:done:{server_id}:{ws_id}` before exiting.
    # - The main process waits for the done key, then deletes the stream and done key.

    def __init__(self, ws: GulpConnectedSocket):
        # use Redis streams per-websocket for cross-instance processing
        self._cws = ws
        redis = GulpRedis.get_instance()
        self._server_id = redis.server_id
        self._stream_key = f"gulp:stream:ws_ingest:{self._server_id}:{ws.ws_id}"
        self._consumer_group = "gulp:ws_ingest:group"
        self._consumer_name = f"{self._server_id}:{ws.ws_id}"
        self._done_key = f"gulp:ws_ingest:done:{self._server_id}:{ws.ws_id}"
        # ttl for orphaned streams (seconds).
        self._stream_ttl = STREAM_TTL

    @staticmethod
    async def _process_loop(stream_key: str, consumer_group: str, consumer_name: str, done_key: str):
        """
        runs in a worker process,
        loop for the ws_ingest_raw worker, processes packets from the queue for this websocket
        """
        MutyLogger.get_instance().debug(
            "ws ingest _process_loop started for stream=%s consumer=%s",
            stream_key,
            consumer_name,
        )

        redis = GulpRedis.get_instance().client()

        try:
            async with GulpCollab.get_instance().session() as sess:
                stats: GulpRequestStats = None
                mod: GulpPluginBase = None

                try:
                    while True:
                        # blocking read from stream via consumer group
                        try:
                            entries = await redis.xreadgroup(
                                consumer_group,
                                consumer_name,
                                streams={stream_key: ">"},
                                count=1,
                                block=5000,
                                noack=False,
                            )
                        except Exception as ex:
                            MutyLogger.get_instance().exception(
                                "error during xreadgroup: %s", ex
                            )
                            await asyncio.sleep(0.5)
                            continue

                        if not entries:
                            # timeout, loop again
                            continue

                        # process entries
                        ids_to_ack = []
                        ids_to_del = []

                        for stream_name, msgs in entries:
                            for msg_id, fields in msgs:
                                # fields keys are bytes when decode_responses=False
                                # expect 'data' (JSON) and optionally 'raw' (bytes)
                                meta_raw = fields.get(b"data") or fields.get("data")
                                raw_bytes = fields.get(b"raw") or fields.get("raw")

                                # normalize memoryview to bytes
                                try:
                                    if isinstance(meta_raw, memoryview):
                                        meta_raw = meta_raw.tobytes()
                                    if isinstance(raw_bytes, memoryview):
                                        raw_bytes = raw_bytes.tobytes()
                                except Exception:
                                    pass

                                if not isinstance(meta_raw, (bytes, bytearray, str)):
                                    MutyLogger.get_instance().exception(
                                        "invalid meta packet type in stream %s id=%s: %r",
                                        stream_name,
                                        msg_id,
                                        type(meta_raw),
                                    )
                                    ids_to_ack.append((stream_name, msg_id))
                                    ids_to_del.append((stream_name, msg_id))
                                    continue

                                try:
                                    d = orjson.loads(meta_raw)
                                except Exception as ex:
                                    MutyLogger.get_instance().exception(
                                        "invalid meta packet in stream %s id=%s: %s",
                                        stream_name,
                                        msg_id,
                                        ex,
                                    )
                                    # acknowledge and delete malformed message
                                    ids_to_ack.append((stream_name, msg_id))
                                    ids_to_del.append((stream_name, msg_id))
                                    continue

                                # attach raw bytes if present
                                if raw_bytes is not None:
                                    d["raw_data"] = raw_bytes

                                # termination sentinel
                                if isinstance(d, dict) and d.get("terminate"):
                                    # ack + del
                                    ids_to_ack.append((stream_name, msg_id))
                                    ids_to_del.append((stream_name, msg_id))
                                    # set done key and exit
                                    pipe = redis.pipeline(transaction=False)
                                    for s, mid in ids_to_ack:
                                        pipe.xack(s, consumer_group, mid)
                                    for s, mid in ids_to_del:
                                        pipe.xdel(s, mid)
                                    await pipe.execute()
                                    await GulpRedis.get_instance().client().set(done_key, "1")
                                    MutyLogger.get_instance().debug(
                                        "ws ingest _process_loop received terminate for stream=%s",
                                        stream_key,
                                    )
                                    return

                                # normal packet
                                packet = None
                                try:
                                    packet = InternalWsIngestPacket.model_validate(d)
                                except Exception as ex:
                                    MutyLogger.get_instance().exception(
                                        "error validating packet from stream %s id=%s: %s",
                                        stream_name,
                                        msg_id,
                                        ex,
                                    )
                                    ids_to_ack.append((stream_name, msg_id))
                                    ids_to_del.append((stream_name, msg_id))
                                    continue

                                # process packet safely per-packet
                                try:
                                    # on first iteration, create stats and load plugin
                                    if not stats:
                                        stats, _ = await GulpRequestStats.create_or_get_existing(
                                            sess,
                                            packet.data.req_id,
                                            packet.user_id,
                                            packet.data.operation_id,
                                            ws_id=packet.data.ws_id,
                                            req_type=RequestStatsType.REQUEST_TYPE_RAW_INGESTION,
                                            never_expire=True,
                                            data=GulpIngestionStats().model_dump(exclude_none=True),
                                        )

                                    if not mod:
                                        mod = await GulpPluginBase.load(packet.data.plugin)

                                    await mod.ingest_raw(
                                        sess,
                                        stats,
                                        packet.user_id,
                                        packet.data.req_id,
                                        packet.data.ws_id,
                                        packet.index,
                                        packet.data.operation_id,
                                        packet.raw_data,
                                        flt=packet.data.flt,
                                        plugin_params=packet.data.plugin_params,
                                        last=packet.data.last,
                                    )
                                    await mod.update_final_stats_and_flush(flt=packet.data.flt)

                                    # track last and cleanup if needed
                                    if packet.data.last:
                                        # finalize plugin and stats
                                        try:
                                            mod._last_raw_chunk = True
                                        except Exception:
                                            pass
                                        await mod.update_final_stats_and_flush(flt=packet.data.flt)
                                        await mod.unload()

                                except Exception as ex:
                                    MutyLogger.get_instance().exception(
                                        "error processing packet from stream %s id=%s: %s",
                                        stream_name,
                                        msg_id,
                                        ex,
                                    )
                                    # continue after logging

                                # always ack+del the message to avoid reprocessing
                                ids_to_ack.append((stream_name, msg_id))
                                ids_to_del.append((stream_name, msg_id))

                        # batch ack + del
                        if ids_to_ack:
                            pipe = redis.pipeline(transaction=False)
                            for s, mid in ids_to_ack:
                                pipe.xack(s, consumer_group, mid)
                            for s, mid in ids_to_del:
                                pipe.xdel(s, mid)
                            try:
                                await pipe.execute()
                            except Exception as ex:
                                MutyLogger.get_instance().exception(
                                    "error acknowledging/deleting messages: %s", ex
                                )

                finally:
                    # ensure done_key is set so the main process can detect worker exit
                    try:
                        await GulpRedis.get_instance().client().set(done_key, "1")
                    except Exception:
                        pass

        except Exception as exxx:
            MutyLogger.get_instance().exception(
                "outer exception in ws ingest _process_loop: %s", exxx
            )

        MutyLogger.get_instance().debug("ws ingest _process_loop done for stream=%s", stream_key)

    async def start(self) -> None:
        """
        starts the worker, which will run in a task in a separate process
        1 ws -> 1 task in a worker process
        """
        MutyLogger.get_instance().debug("starting ws ingest worker ...")

        # ensure consumer group exists and then run _process_loop in a separate process
        redis = GulpRedis.get_instance().client()
        try:
            try:
                await redis.xgroup_create(self._stream_key, self._consumer_group, id="0", mkstream=True)
            except Exception:
                # ignore if group exists or other create-time race
                pass
            # do NOT set expire here — TTL will be set on first data `put()` and refreshed thereafter
        except Exception:
            MutyLogger.get_instance().exception("error creating consumer group for %s", self._stream_key)

        await GulpServer.get_instance().spawn_worker_task(
            WsIngestRawWorker._process_loop,
            self._stream_key,
            self._consumer_group,
            self._consumer_name,
            self._done_key,
        )

    async def stop(self) -> None:
        """
        stops the worker (puts an empty message in the queue to signal the worker to exit)
        waits until the worker has finished processing.
        """
        MutyLogger.get_instance().debug(
            "stopping ws ingest worker for ws=%s (will push terminate to stream=%s) ...",
            self._cws.ws_id,
            self._stream_key,
        )
        redis = GulpRedis.get_instance().client()
        try:
            # push termination sentinel as meta field
            await redis.xadd(self._stream_key, {"data": orjson.dumps({"terminate": True})})
        except Exception:
            MutyLogger.get_instance().exception("error pushing terminate sentinel to %s", self._stream_key)

        # wait for worker to set done key
        try:
            while not await redis.exists(self._done_key):
                await asyncio.sleep(0.1)
        except Exception:
            MutyLogger.get_instance().exception("error waiting for done key %s", self._done_key)

        # cleanup stream and done key
        try:
            # destroy consumer group to avoid leaving groups behind
            try:
                await redis.xgroup_destroy(self._stream_key, self._consumer_group)
            except Exception:
                # ignore if group does not exist or destroy fails
                pass
            await redis.delete(self._stream_key)
            await redis.delete(self._done_key)
        except Exception:
            MutyLogger.get_instance().exception("error cleaning up stream %s", self._stream_key)

        MutyLogger.get_instance().debug("stopped ws ingest worker for ws=%s !", self._cws.ws_id)

    async def put(self, packet: InternalWsIngestPacket):
        """
        puts a packet in the worker queue

        Args:
            packet (InternalWsIngestPacket): the packet to put in the queue
        """
        try:
            payload = packet.model_dump(exclude_none=True)
            # separate raw bytes from JSON-serializable metadata
            raw_bytes = None
            if "raw_data" in payload and isinstance(payload["raw_data"], (bytes, bytearray)):
                raw_bytes = payload.pop("raw_data")

            data_json = orjson.dumps(payload)
            # push two fields: 'data' (JSON) and 'raw' (binary)
            fields = {"data": data_json}
            if raw_bytes is not None:
                fields["raw"] = raw_bytes

            await GulpRedis.get_instance().client().xadd(self._stream_key, fields)
            # refresh TTL to indicate recent activity
            try:
                await GulpRedis.get_instance().client().expire(self._stream_key, self._stream_ttl)
            except Exception:
                MutyLogger.get_instance().exception("error refreshing expire on stream %s", self._stream_key)
        except Exception as ex:
            MutyLogger.get_instance().exception("error xadd to %s: %s", self._stream_key, ex)


class GulpAPIWebsocket:
    """
    handles gulp websocket connections

    we subclass starlette's WebSocketEndpoint to have better control on websocket termination, etc...
    """

    @staticmethod
    async def _authenticate_websocket(
        websocket: WebSocket,
        params: GulpWsAuthPacket,
        required_permission: Optional[GulpUserPermission] = None,
    ) -> tuple[str, str, str]:
        """
        authenticates a websocket connection

        Args:
            websocket (WebSocket): the websocket to authenticate
            params (GulpWsAuthPacket): the authentication parameters
            required_permission (Optional[GulpUserPermission]): permission required for this connection
            notify_login (bool): whether to notify login events on this connection
        Returns:
            Tuple[str, str, str]: ws_id, req_id, user_id

        Raises:
            ObjectNotFound: if user not found
            MissingPermission: if user lacks required permissions
        """
        # autogenerate ws_id if not provided
        if not params.ws_id:
            params.ws_id = muty.string.generate_unique()
            MutyLogger.get_instance().warning(
                "empty ws_id, auto-generated: %s", params.ws_id
            )
        if not params.req_id:
            params.req_id = muty.string.generate_unique()
            MutyLogger.get_instance().warning(
                "empty req_id, auto-generated: %s", params.req_id
            )            
        user_id: str = None

        # authenticate user
        async with GulpCollab.get_instance().session() as sess:
            s = await GulpUserSession.check_token(
                sess, params.token, required_permission or GulpUserPermission.READ
            )
            user_id = s.user.id
        
        return params.ws_id, params.req_id, user_id

    @staticmethod
    async def _send_error_response(
        websocket: WebSocket, ex: Exception, ws_id: str, error_type: GulpWsError
    ) -> None:
        """
        sends an error response to the client

        Args:
            websocket (WebSocket): the websocket to send the error to
            ex (Exception): the exception that occurred
            ws_id (str): the websocket id
            error_type (GulpWsError): the type of error
        """
        p = GulpWsErrorPacket(error=str(ex), error_code=error_type.name)
        wsd = GulpWsData(
            timestamp=muty.time.now_msec(),
            type=WSDATA_ERROR,
            ws_id=ws_id,
            payload=p.model_dump(exclude_none=True),
        )
        await websocket.send_json(wsd.model_dump(exclude_none=True, by_alias=True))

    @staticmethod
    async def _send_connection_ack(
        websocket: WebSocket, ws_id: str, req_id: str, token: str, user_id: Optional[str]
    ) -> None:
        """
        sends connection acknowledgment to the client

        Args:
            websocket (WebSocket): the websocket to send the acknowledgment to
            ws_id (str): the websocket id
            req_id (str): the request id
            token (str): the auth token
            user_id (Optional[str]): the user id
        """
        p = GulpWsData(
            timestamp=muty.time.now_msec(),
            type=WSDATA_CONNECTED,
            ws_id=ws_id,
            req_id=req_id,
            user_id=user_id,
            payload=GulpWsAcknowledgedPacket(token=token, ws_id=ws_id, req_id=req_id).model_dump(
                exclude_none=True
            ),
        )
        await websocket.send_json(p.model_dump(exclude_none=True))

    @staticmethod
    async def _handle_websocket(
        websocket: WebSocket,
        run_loop_fn: Callable[[GulpConnectedSocket, str], Awaitable[None]],
        socket_type: GulpWsType = None,
        permission: GulpUserPermission = None,
        notify_login: bool = False
    ) -> None:
        """
        generic websocket handler that follows the common pattern for all ws endpoints

        Args:
            websocket (WebSocket): the websocket connection
            run_loop_fn (Callable): the main loop function for this socket type
            socket_type (GulpWsType): the type of socket connection
            permission (Optional[GulpUserPermission]): required permission for this endpoint
            notify_login (bool): whether to notify login events on this connection
        """
        if not socket_type:
            socket_type = GulpWsType.WS_DEFAULT

        gulp_ws: GulpConnectedSocket = None
        ws_id: str = None
        try:
            # accept connection and get auth params
            await websocket.accept()
            js = await websocket.receive_json()
            params = GulpWsAuthPacket.model_validate(js)

            # authenticate user            
            ws_id, req_id, user_id = await GulpAPIWebsocket._authenticate_websocket(
                websocket, params, permission
            )

            # connection accepted, log and create socket
            MutyLogger.get_instance().debug("socket type=%s accepted for ws_id=%s, req_id=%s", socket_type, ws_id, req_id)

            gulp_ws = await GulpConnectedSockets.get_instance().add(
                websocket,
                ws_id,
                types=params.types,
                operation_ids=params.operation_ids,
                socket_type=socket_type,
                data=params.data,
                user_id=user_id,
            )

            # acknowledge connection
            await GulpAPIWebsocket._send_connection_ack(
                websocket, ws_id, req_id, params.token, user_id
            )
            if notify_login:
                # notify login event
                p = GulpUserAccessPacket(user_id=user_id, login=True, ip=websocket.client.host, req_id=req_id)
                redis = GulpRedisBroker.get_instance()
                await redis.put(
                    WSDATA_USER_LOGIN,
                    user_id=user_id,
                    ws_id=ws_id,
                    req_id=req_id,
                    d=p.model_dump(exclude_none=True, exclude_defaults=True),
                )

            # run the appropriate loop function
            await run_loop_fn(gulp_ws, user_id)

        except ObjectNotFound as ex:
            # user not found
            await GulpAPIWebsocket._send_error_response(
                websocket, ex, params.ws_id, GulpWsError.OBJECT_NOT_FOUND
            )
        except MissingPermission as ex:
            # user has no permission
            await GulpAPIWebsocket._send_error_response(
                websocket, ex, params.ws_id, GulpWsError.MISSING_PERMISSION
            )
        except WebSocketDisconnect as ex:
            MutyLogger.get_instance().debug(f"websocket disconnected: {ex}")
        except Exception as ex:
            MutyLogger.get_instance().exception(ex)
        finally:
            # cleanup
            if gulp_ws:
                try:
                    await GulpConnectedSockets.get_instance().remove(gulp_ws.ws_id)
                    await gulp_ws.cleanup()
                except Exception as ex:
                    MutyLogger.get_instance().exception(
                        "error during ws cleanup: %s", ex
                    )

            # close websocket gracefully if still connected
            if websocket.client_state == WebSocketState.CONNECTED:
                try:
                    await websocket.close()
                except:
                    MutyLogger.get_instance().exception("error closing websocket")
                    pass
            MutyLogger.get_instance().debug("socket %s, type=%s closed!", ws_id, socket_type)

    @router.websocket("/ws")
    @staticmethod
    async def ws_handler(websocket: WebSocket):
        """
        handles the websocket connection

        the websocket protocol is really simple:

        1. client sends a json request with GulpWsAuthParameters
        2. server accepts the connection and checks the token and ws_id
        3. on error, server sends a GulpWsErrorPacket and closes the connection. on success, it sends a GulpWsAcknowledgedPacket and starts the main loop.
        4. server sends messages to the client on the connected `ws_id` and also broadcastis Collab objects and events to the other connected websockets.

        Args:
            websocket (WebSocket): The websocket object.
        """

        async def run_loop(ws: GulpConnectedSocket, user_id: str) -> None:
            await ws.run_loop()

        await GulpAPIWebsocket._handle_websocket(
            websocket,
            run_loop,
            notify_login=True
        )

    @router.websocket("/ws_ingest_raw")
    @staticmethod
    async def ws_ingest_raw_handler(websocket: WebSocket):
        """
        a websocket endpoint specific for ingestion

        1. client (i.e. an agent, or a bridge) sends a json request with GulpWsAuthParameters
        2. Gulp accepts the connection and checks the token (needs INGEST permission) and ws_id
        3. on error, server sends a GulpWsErrorPacket and closes the connection. on success, it sends a GulpWsAcknowledgedPacket and starts the main loop.
        4. client streams GulpWsIngestPackets to gulp on the given ws_id: each contains the ws_id on which the UI is connected, the plugin to be used (optional, default "raw"), the GulpPluginParameters (optional), followed by the raw data.
        5. Gulp ingests the raw data and finally streams it on GulpWsIngestPacket.ws_id as it would normally when using the HTTP API.
        6. when the client sends a GulpWsIngestPacket with `last=true`, the ingestion is finalized, the stats are closed and the websocket connection is closed.

        # tracking progress

        the flow is the same as `ingest_file`.

                Args:
            websocket (WebSocket): The websocket object.
        """
        await GulpAPIWebsocket._handle_websocket(
            websocket,
            GulpAPIWebsocket.ws_ingest_run_loop,
            socket_type=GulpWsType.WS_INGEST,
            permission=GulpUserPermission.INGEST,
        )

    @staticmethod
    async def ws_ingest_run_loop(ws: GulpConnectedSocket, user_id: str) -> None:
        """
        main loop for the ingest websocket connection:

        1. a worker is started in a worker process's task
        2. the main loop receives data from the websocket and puts it in the worker queue (shared between the main process and the worker)
        3. WsIngestRawWorker._process_loop processes the queue

        Args:
            ws (GulpConnectedSocket): the websocket connection
            user_id (str): the user id
        """
        # starts a worker in the pool for this ws
        pool_worker = WsIngestRawWorker(ws)
        await pool_worker.start()
        index: str = None

        # loop until done
        try:
            while True:
                # raise WebSocketDisconnect when client disconnects
                ws.validate_connection()

                try:
                    # get dict and data from websocket
                    js = await ws.ws.receive_json()
                    ingest_packet = GulpWsIngestPacket.model_validate(js)
                    raw_data: bytes = await ws.ws.receive_bytes()
                    # MutyLogger.get_instance().debug(
                    #     "ws_ingest received packet: req_id=%s, ws_id=%s, operation_id=%s, plugin=%s, flt=%s, last=%s, raw_data_len=%s",
                    #     ingest_packet.req_id,
                    #     ingest_packet.ws_id,
                    #     ingest_packet.operation_id,
                    #     ingest_packet.plugin,
                    #     ingest_packet.flt,
                    #     ingest_packet.last,
                    #     len(raw_data),
                    # )
                    # get operation if we don't have it
                    if not index:
                        # get operation if we don't have it
                        async with GulpCollab.get_instance().session() as sess:
                            op = await GulpOperation.get_by_id(
                                sess,
                                ingest_packet.operation_id,
                                throw_if_not_found=False,
                            )
                            if not op or op.id != ingest_packet.operation_id:
                                # missing or wrong operation, abort
                                msg = (
                                    "operation not found or operation_id mismatch! (packet.operation_id=%s)"
                                    % (ingest_packet.operation_id)
                                )
                                MutyLogger.get_instance().error(msg)
                                p = GulpWsErrorPacket(
                                    error=msg,
                                    error_code=GulpWsError.OBJECT_NOT_FOUND.name,
                                )
                                await GulpRedisBroker.get_instance().put(
                                    WSDATA_ERROR,
                                    user_id,
                                    ws_id=ingest_packet.ws_id,
                                    d=p.model_dump(exclude_none=True),
                                )
                                raise ObjectNotFound(msg)

                            # got operation's index
                            index = op.index

                    # package data for worker
                    packet = InternalWsIngestPacket(
                        user_id=user_id,
                        index=index,
                        data=ingest_packet,
                        raw_data=raw_data,
                    )

                    # and put in the worker queue (async xadd to Redis)
                    await pool_worker.put(packet)
                    if ingest_packet.last:
                        # last packet, break the loop
                        MutyLogger.get_instance().debug(
                            "ws_ingest received last packet for ws_id=%s, exiting loop", ws.ws_id
                        )
                        break

                except WebSocketDisconnect:
                    MutyLogger.get_instance().exception(
                        "websocket %s disconnected!", ws.ws_id
                    )
                    break

                except Exception as ex:
                    MutyLogger.get_instance().exception(ex)
                    break
        except Exception as ex:
            MutyLogger.get_instance().exception(ex)
        finally:
            await pool_worker.stop()

    @router.websocket("/ws_client_data")
    @staticmethod
    async def ws_client_data_handler(websocket: WebSocket):
        """
        a websocket endpoint specific for data sent by UI to be routed among connected websockets

        1. client sends a json request with GulpWsAuthParameters
        2. server accepts the connection and checks the token and ws_id
        3. on error, server sends a GulpWsErrorPacket and closes the connection. on success, it sends a GulpWsAcknowledgedPacket and starts the main loop.
        4. client streams GulpWsClientData which is routed to other clients connected to /ws_client_data

        Args:
            websocket (WebSocket): The websocket object.
        """
        await GulpAPIWebsocket._handle_websocket(
            websocket,
            GulpAPIWebsocket.ws_client_data_run_loop,
            socket_type=GulpWsType.WS_CLIENT_DATA,
        )

    @staticmethod
    async def ws_client_data_receive_loop(
        ws: GulpConnectedSocket, user_id: str
    ) -> None:
        """
        receives client ui data

        Args:
            ws (GulpConnectedSocket): the websocket connection
            user_id (str): the user id
        """
        while True:
            # raise WebSocketDisconnect when client disconnects
            ws.validate_connection()

            try:
                # this will raise WebSocketDisconnect when client disconnects
                js: dict = await ws.ws.receive_json()
                client_ui_data = GulpClientDataPacket.model_validate(js)

                data = GulpWsData(
                    timestamp=muty.time.now_msec(),
                    type=WSDATA_CLIENT_DATA,
                    ws_id=ws.ws_id,
                    user_id=user_id,
                    operation_id=client_ui_data.operation_id,
                    payload=client_ui_data.model_dump(exclude_none=True),
                )

                # route to connected client_data websockets
                s = GulpConnectedSockets.get_instance()

                for _, cws in s.sockets().items():
                    if (
                        ws.ws_id == cws.ws_id
                        or cws.socket_type != GulpWsType.WS_CLIENT_DATA
                        # filter by operation_id if set (empty list means accept all)
                        or (
                            client_ui_data.operation_id
                            and cws.operation_ids
                            and client_ui_data.operation_id not in cws.operation_ids
                        )
                        # filter by user_id if set
                        or (
                            client_ui_data.target_user_ids
                            and user_id not in client_ui_data.target_user_ids
                        )
                    ):
                        # skip this ws
                        continue
                    try:
                        # use enqueue_message to properly handle queuing and connection state
                        await cws.enqueue_message(data.model_dump(exclude_none=True))
                    except Exception as ex:
                        MutyLogger.get_instance().error(
                            f"error enqueuing data to ws_id={cws.ws_id}: {ex}"
                        )
                # publish to other instances via dedicated client_data Redis channel
                try:
                    redis_client = GulpRedis.get_instance()
                    msg = data.model_dump(exclude_none=True)
                    msg["__channel__"] = "client_data"
                    msg["__server_id__"] = redis_client.server_id
                    msg["__sender_ws_id__"] = ws.ws_id
                    # publish to dedicated channel
                    MutyLogger.get_instance().debug(
                        "publishing client_data to Redis: ws_id=%s, operation_id=%s",
                        ws.ws_id, client_ui_data.operation_id
                    )
                    await redis_client.client().publish(
                        GulpRedis.CLIENT_DATA_CHANNEL, orjson.dumps(msg)
                    )
                except Exception as ex:
                    MutyLogger.get_instance().warning(
                        "failed to publish client_data to Redis for ws_id=%s: %s", ws.ws_id, ex
                    )
            except WebSocketDisconnect as ex:
                MutyLogger.get_instance().warning(
                    "---> ws_client_data websocket %s disconnected: ex", ws.ws_id, ex
                )
                raise
            except asyncio.CancelledError:
                MutyLogger.get_instance().warning(
                    "---> ws_client_data receive loop canceled for ws_id=%s", ws.ws_id
                )
                raise
            except Exception as ex:
                MutyLogger.get_instance().error("---> ws_client_data error for ws_id=%s, in receive loop: %s", ws.ws_id, ex)
                raise
            
    @staticmethod
    async def ws_client_data_run_loop(ws: GulpConnectedSocket, user_id: str) -> None:
        """
        main loop for the client_data websocket connection

        Args:
            ws (GulpConnectedSocket): the websocket connection
            user_id (str): the user id

        Throws:
            WebSocketDisconnect: when the client disconnects
            Exception: for any unexpected errors during processing
        """
        # create tasks with names for better debugging
        send_task: asyncio.Task = asyncio.create_task(
            ws._send_loop(), name=f"client_data-send_loop-{ws.ws_id}"
        )
        receive_task: asyncio.Task = asyncio.create_task(
            GulpAPIWebsocket.ws_client_data_receive_loop(ws, user_id),
            name=f"client_data-receive_loop-{ws.ws_id}",
        )
        ws._tasks.extend([send_task, receive_task])

        # wait for first task to complete
        done, pending = await asyncio.wait(ws._tasks, return_when=asyncio.FIRST_EXCEPTION)
        
        # cancel all pending tasks to ensure clean shutdown
        MutyLogger.get_instance().debug("ws_id=%s done=%s, pending=%s", ws.ws_id, done, pending)
        if pending:
            for t in pending:
                MutyLogger.get_instance().debug(
                    "cancelling pending task %s for ws_id=%s",
                    t.get_name(),
                    ws.ws_id,
                )
                t.cancel()
                try:
                    await t
                except asyncio.CancelledError:
                    MutyLogger.get_instance().debug(
                        "pending task %s cancelled successfully", t.get_name()
                    )
        
        # check if any completed task raised an exception
        task = done.pop()
        try:
            await task
        except WebSocketDisconnect as ex:
            MutyLogger.get_instance().debug(f"websocket {ws.ws_id} disconnected: {ex}")
            raise
        except Exception as ex:
            MutyLogger.get_instance().error(f"error in {task.get_name()}: {ex}")
            raise
        MutyLogger.get_instance().debug("ws_id=%s ws_client_data_run_loop exiting!",ws.ws_id)
