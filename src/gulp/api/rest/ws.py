import asyncio
import time
from multiprocessing import Queue
from typing import Awaitable, Callable, Optional

import muty.jsend
import muty.list
import muty.log
import muty.os
import muty.string
import muty.time
import muty.uploadfile
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from fastapi.websockets import WebSocketState
from muty.log import MutyLogger
from pydantic import BaseModel, Field

from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.stats import GulpRequestStats
from gulp.api.collab.structs import GulpUserPermission, MissingPermission
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.rest_api import GulpRestServer
from gulp.api.ws_api import (
    GulpClientDataPacket,
    GulpConnectedSocket,
    GulpConnectedSockets,
    GulpWsAcknowledgedPacket,
    GulpWsAuthPacket,
    GulpWsData,
    GulpWsError,
    GulpWsErrorPacket,
    GulpWsIngestPacket,
    GulpWsQueueDataType,
    GulpWsSharedQueue,
    GulpWsType,
)
from gulp.plugin import GulpPluginBase, GulpPluginCacheMode
from gulp.process import GulpProcess
from gulp.structs import ObjectNotFound

router = APIRouter()


class InternalWsIngestPacket(BaseModel):
    """
    holds data for the ws ingest worker
    """

    user_id: str = Field(..., description="the user id")
    index: str = Field(..., description="the index to ingest into")
    data: GulpWsIngestPacket = Field(...,
                                     description="a GulpWsIngestPacket dictionary")


class WsIngestRawWorker:
    """
    a gulp worker to handle websocket raw ingestion
    """

    def __init__(self, ws: GulpConnectedSocket):
        # use gulp's multiprocessing manager to create a process-shareable queue
        self._input_queue = GulpProcess.get_instance().mp_manager.Queue()
        self._cws = ws

    @staticmethod
    async def _process_loop(input_queue: Queue):
        """
        loop for the ingest worker, processes packets from the queue

        Args:
            input_queue (Queue): the input queue
        """
        MutyLogger.get_instance().debug(
            "ws ingest _process_loop started, input_queue=%s" % (input_queue)
        )

        async with GulpCollab.get_instance().session() as sess:
            stats: GulpRequestStats = None

            while True:
                packet: InternalWsIngestPacket = input_queue.get()
                if packet is None:
                    break

                if not stats:
                    # create a stats that never expire
                    stats: GulpRequestStats = await GulpRequestStats.create(
                        sess,
                        user_id=packet.user_id,
                        req_id=packet.data.req_id,
                        ws_id=packet.data.ws_id,
                        operation_id=packet.data.operation_id,
                        context_id=None,
                        never_expire=True,
                    )

                try:
                    mod: GulpPluginBase = None
                    MutyLogger.get_instance().debug("_ws_ingest_process_internal")

                    # load plugin, force caching so it will be loaded first time only
                    mod: GulpPluginBase = await GulpPluginBase.load(
                        packet.data.plugin, cache_mode=GulpPluginCacheMode.FORCE
                    )

                    # process docs using plugin
                    await mod.ingest_raw(
                        sess,
                        user_id=packet.user_id,
                        req_id=packet.data.req_id,
                        ws_id=packet.data.ws_id,
                        index=packet.index,
                        stats=stats,
                        operation_id=packet.data.operation_id,
                        chunk=packet.data.docs,
                        flt=packet.data.flt,
                        plugin_params=packet.data.plugin_params,
                    )

                except Exception as ex:
                    MutyLogger.get_instance().exception(ex)
                    # just append error
                    d = dict(
                        error=ex,
                    )
                    await stats.update(
                        sess, d, ws_id=packet.data.ws_id, user_id=packet.user_id
                    )

                finally:
                    if mod:
                        await mod.unload()

            MutyLogger.get_instance().debug("ws ingest _process_loop done")

    async def start(self) -> None:
        """
        starts the worker, which will run in a task in a separate process
        """

        async def worker_coro():
            await GulpProcess.get_instance().process_pool.apply(
                WsIngestRawWorker._process_loop, args=(self._input_queue,)
            )

        # run _process_loop in a separate process
        MutyLogger.get_instance().debug("starting ws ingest worker ...")
        await GulpRestServer.get_instance().spawn_bg_task(worker_coro())

    async def stop(self):
        """
        stops the worker
        """
        MutyLogger.get_instance().debug("stopping ws ingest worker pool ...")
        self._input_queue.put(None)

    def put(self, packet: InternalWsIngestPacket):
        """
        puts a packet in the worker queue


        """
        # MutyLogger.get_instance().debug("putting packet in ws ingest worker")
        self._input_queue.put(packet)


class GulpAPIWebsocket:
    """
    handles gulp websocket connections

    we subclass starlette's WebSocketEndpoint to have better control on websocket termination, etc...
    """

    MAX_MESSAGES_PER_SECOND = 10
    RATE_LIMIT_WINDOW_SECONDS = 1.0
    BACKPRESSURE_DELAY = 0.5  # seconds to wait when rate limit is hit

    @staticmethod
    async def _authenticate_websocket(
        websocket: WebSocket,
        params: GulpWsAuthPacket,
        required_permission: Optional[GulpUserPermission] = None
    ) -> tuple[str, Optional[str]]:
        """
        authenticates a websocket connection

        Args:
            websocket (WebSocket): the websocket to authenticate
            params (GulpWsAuthPacket): the authentication parameters
            required_permission (Optional[GulpUserPermission]): permission required for this connection

        Returns:
            Tuple[str, Optional[str]]: ws_id and user_id (None for monitor connections)

        Raises:
            ObjectNotFound: if user not found
            MissingPermission: if user lacks required permissions
        """
        # autogenerate ws_id if not provided
        if not params.ws_id:
            params.ws_id = muty.string.generate_unique()
            MutyLogger.get_instance().warning(
                f"empty ws_id, auto-generated: {params.ws_id}"
            )

        user_id = None
        # special case for monitor token
        if params.token.lower() == "monitor" and not required_permission:
            return params.ws_id, user_id

        # authenticate normal user
        async with GulpCollab.get_instance().session() as sess:
            s = await GulpUserSession.check_token(
                sess, params.token, required_permission or GulpUserPermission.READ
            )
            user_id = s.user_id

        return params.ws_id, user_id

    @staticmethod
    async def _send_error_response(
        websocket: WebSocket,
        ex: Exception,
        ws_id: str,
        error_type: GulpWsError
    ) -> None:
        """
        sends an error response to the client

        Args:
            websocket (WebSocket): the websocket to send the error to
            ex (Exception): the exception that occurred
            ws_id (str): the websocket id
            error_type (GulpWsError): the type of error
        """
        p = GulpWsErrorPacket(
            error=str(ex), error_code=error_type.name
        )
        wsd = GulpWsData(
            timestamp=muty.time.now_msec(),
            type=GulpWsQueueDataType.WS_ERROR,
            ws_id=ws_id,
            data=p.model_dump(exclude_none=True),
        )
        await websocket.send_json(wsd.model_dump(exclude_none=True, by_alias=True))

    @staticmethod
    async def _send_connection_ack(
        websocket: WebSocket,
        ws_id: str,
        token: str,
        user_id: Optional[str]
    ) -> None:
        """
        sends connection acknowledgment to the client

        Args:
            websocket (WebSocket): the websocket to send the acknowledgment to
            ws_id (str): the websocket id
            token (str): the auth token
            user_id (Optional[str]): the user id
        """
        p = GulpWsData(
            timestamp=muty.time.now_msec(),
            type=GulpWsQueueDataType.WS_CONNECTED,
            ws_id=ws_id,
            user_id=user_id,
            data=GulpWsAcknowledgedPacket(
                token=token, ws_id=ws_id
            ).model_dump(exclude_none=True),
        )
        await websocket.send_json(p.model_dump(exclude_none=True))

    @staticmethod
    async def _handle_websocket(
        websocket: WebSocket,
        socket_type: GulpWsType,
        permission: Optional[GulpUserPermission],
        run_loop_fn: Callable[[GulpConnectedSocket, str], Awaitable[None]],
    ) -> None:
        """
        generic websocket handler that follows the common pattern for all ws endpoints

        Args:
            websocket (WebSocket): the websocket connection
            socket_type (GulpWsType): the type of socket connection
            permission (Optional[GulpUserPermission]): required permission for this endpoint
            run_loop_fn (Callable): the main loop function for this socket type
        """
        ws = None
        try:
            # accept connection and get auth params
            await websocket.accept()
            js = await websocket.receive_json()
            params = GulpWsAuthPacket.model_validate(js)

            # authenticate user
            ws_id, user_id = await GulpAPIWebsocket._authenticate_websocket(
                websocket, params, permission
            )

            # connection accepted, log and create socket
            logger = MutyLogger.get_instance()
            logger.debug(f"{socket_type} accepted for ws_id={ws_id}")

            ws = GulpConnectedSockets.get_instance().add(
                websocket, ws_id, params.types, params.operation_ids, socket_type=socket_type
            )

            # acknowledge connection
            await GulpAPIWebsocket._send_connection_ack(
                websocket, ws_id, params.token, user_id
            )

            # run the appropriate loop function
            await run_loop_fn(ws, user_id)

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
            if ws:
                try:
                    await GulpConnectedSockets.get_instance().remove(websocket)
                except Exception as ex:
                    MutyLogger.get_instance().error(
                        f"error during ws cleanup: {ex}")
                del ws

            # close websocket gracefully if still connected
            if (websocket.client_state == WebSocketState.CONNECTED):
                try:
                    await websocket.close()
                except:
                    pass

    @staticmethod
    async def _apply_rate_limiting(
        message_count: int,
        last_window_start: float,
        max_messages: int = MAX_MESSAGES_PER_SECOND,
        window_seconds: float = RATE_LIMIT_WINDOW_SECONDS
    ) -> tuple[int, float, bool]:
        """
        applies rate limiting logic

        Args:
            message_count (int): current message count
            last_window_start (float): timestamp of the start of the current window
            max_messages (int): maximum messages allowed per window
            window_seconds (float): window duration in seconds

        Returns:
            Tuple[int, float, bool]: updated message count, window start time, and whether to apply delay
        """
        current_time = time.time()
        time_diff = current_time - last_window_start
        apply_delay = False

        # reset counter for new time window
        if time_diff >= window_seconds:
            message_count = 0
            last_window_start = current_time

        # check if exceeding rate limit
        if message_count >= max_messages:
            apply_delay = True

        return message_count, last_window_start, apply_delay

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
            GulpWsType.WS_DEFAULT,
            None,
            run_loop
        )

    @router.websocket("/ws_ingest_raw")
    @staticmethod
    async def ws_ingest_raw_handler(websocket: WebSocket):
        """
        a websocket endpoint specific for ingestion

        1. client sends a json request with GulpWsAuthParameters
        2. server accepts the connection and checks the token (needs INGEST permission) and ws_id
        3. on error, server sends a GulpWsErrorPacket and closes the connection. on success, it sends a GulpWsAcknowledgedPacket and starts the main loop.
        4. client streams GulpWsIngestPackets over the websocket, the server replies ingested data on GulpWsIngestPacket.ws_id as it would be a normal ingestion using the http API.

        Args:
            websocket (WebSocket): The websocket object.
        """
        await GulpAPIWebsocket._handle_websocket(
            websocket,
            GulpWsType.WS_INGEST,
            GulpUserPermission.INGEST,
            GulpAPIWebsocket.ws_ingest_run_loop
        )

    @staticmethod
    async def ws_ingest_run_loop(ws: GulpConnectedSocket, user_id: str) -> None:
        """
        main loop for the ingest websocket connection:

        1. a worker is started in a worker process's task
        2. the main loop receives data from the websocket and puts it in the worker (shared) queue
        3. the worker queue is processed using the plugin indicated in the GulpWsIngestPacket

        Args:
            ws (GulpConnectedSocket): the websocket connection
            user_id (str): the user id
        """
        worker_pool = WsIngestRawWorker(ws)
        await worker_pool.start()

        # rate limiting state
        message_count = 0
        last_window_start = time.time()
        # higher limits for ingestion endpoint
        max_messages = (
            GulpAPIWebsocket.MAX_MESSAGES_PER_SECOND * 2
        )  # higher than ws_client_data
        window_seconds = GulpAPIWebsocket.RATE_LIMIT_WINDOW_SECONDS

        try:
            async with GulpCollab.get_instance().session() as sess:
                operation: GulpOperation = None

                while True:
                    # raise WebSocketDisconnect when client disconnects
                    ws.validate_connection()

                    # apply rate limiting
                    message_count, last_window_start, apply_delay = await GulpAPIWebsocket._apply_rate_limiting(
                        message_count, last_window_start, max_messages, window_seconds
                    )

                    if apply_delay:
                        # apply rate limiting
                        MutyLogger.get_instance().warning(
                            f"ingest rate limit hit for ws_id={ws.ws_id}. applying backpressure."
                        )
                        # backpressure delay
                        await asyncio.sleep(GulpAPIWebsocket.BACKPRESSURE_DELAY)
                        message_count = 0
                        last_window_start = time.time()
                        continue

                    message_count += 1

                    # Regular message processing
                    try:
                        # get packet from ws
                        js = await ws.ws.receive_json()
                        ingest_packet = GulpWsIngestPacket.model_validate(js)

                        # check operation, if not the same, get it (sort of cache)
                        if not operation or operation.id != ingest_packet.operation_id:
                            # this may be not necessary, since the operation should never change in the same ws connection....
                            operation = await GulpOperation.get_by_id(
                                sess,
                                ingest_packet.operation_id,
                                throw_if_not_found=False,
                            )
                        if not operation:
                            # missing operation, abort
                            MutyLogger.get_instance().error(
                                "operation %s not found!" % (
                                    ingest_packet.operation_id)
                            )
                            p = GulpWsErrorPacket(
                                error="operation %s not found!"
                                % (ingest_packet.operation_id),
                                error_code=GulpWsError.OBJECT_NOT_FOUND.name,
                            )
                            await GulpWsSharedQueue.get_instance().put(
                                type=GulpWsQueueDataType.WS_ERROR,
                                ws_id=ingest_packet.ws_id,
                                user_id=user_id,
                                data=p.model_dump(exclude_none=True),
                            )
                            break

                        # package data for worker
                        packet = InternalWsIngestPacket(
                            user_id=user_id, index=operation.index, data=ingest_packet
                        )

                        # and put in the worker queue
                        worker_pool.put(packet)

                    except WebSocketDisconnect as ex:
                        MutyLogger.get_instance().error(
                            f"websocket {ws.ws_id} disconnected: {ex}"
                        )
                        break

                    except Exception as ex:
                        message_count -= 1  # Don't count errors against rate limit

                        MutyLogger.get_instance().error(
                            f"Error processing ingest message: {ex}"
                        )

        finally:
            await worker_pool.stop()

    @router.websocket("/ws_client_data")
    @staticmethod
    async def ws_client_data_handler(websocket: WebSocket):
        """
        a websocket endpoint specific for data sent by UI to be routed among connected websockets

        1. client sends a json request with GulpWsAuthParameters
        2. server accepts the connection and checks the token and ws_id
        3. on error, server sends a GulpWsErrorPacket and closes the connection. on success, it sends a GulpWsAcknowledgedPacket and starts the main loop.
        4. client streams GulpWsClientData which is routed to all other clients connected to /ws_client_data

        Args:
            websocket (WebSocket): The websocket object.
        """
        await GulpAPIWebsocket._handle_websocket(
            websocket,
            GulpWsType.WS_CLIENT_DATA,
            None,
            GulpAPIWebsocket.ws_client_data_run_loop
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
        # Rate limiting state
        message_count = 0
        last_window_start = time.time()

        while True:
            # raise WebSocketDisconnect when client disconnects
            ws.validate_connection()

            # apply rate limiting
            message_count, last_window_start, apply_delay = await GulpAPIWebsocket._apply_rate_limiting(
                message_count, last_window_start
            )

            if apply_delay:
                MutyLogger.get_instance().warning(
                    f"rate limit hit for ws_id={ws.ws_id}. applying backpressure."
                )
                # apply backpressure
                await asyncio.sleep(GulpAPIWebsocket.BACKPRESSURE_DELAY)

                # reset counters after backpressure
                message_count = 0
                last_window_start = time.time()
                continue

            # increment counter for this message
            message_count += 1

            # normal message processing
            try:
                # this will raise WebSocketDisconnect when client disconnects
                js: dict = await ws.ws.receive_json()
                client_ui_data = GulpClientDataPacket.model_validate(js)

                data = GulpWsData(
                    timestamp=muty.time.now_msec(),
                    type=GulpWsQueueDataType.CLIENT_DATA,
                    ws_id=ws.ws_id,
                    user_id=user_id,
                    data=client_ui_data.model_dump(exclude_none=True),
                )

                # route to all connected client_data websockets
                s = GulpConnectedSockets.get_instance()
                for _, cws in s._sockets.items():
                    if (
                        ws.ws_id == cws.ws_id
                        or cws.socket_type != GulpWsType.WS_CLIENT_DATA
                    ):
                        # skip this ws
                        continue
                    try:
                        await cws.ws.send_json(data.model_dump(exclude_none=True))
                    except Exception as ex:
                        MutyLogger.get_instance().error(
                            f"error sending data to ws_id={cws.ws_id}: {ex}"
                        )
            except WebSocketDisconnect as ex:
                MutyLogger.get_instance().warning(
                    f"websocket {ws.ws_id} disconnected: {ex}"
                )
                break
            except Exception as ex:
                # Don't count errors against rate limit
                message_count -= 1
                MutyLogger.get_instance().error(f"error in receive loop: {ex}")

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
        tasks: list[asyncio.Task[None]] = []
        try:
            # create tasks with names for better debugging
            ws.receive_task = asyncio.create_task(
                GulpAPIWebsocket.ws_client_data_receive_loop(ws, user_id),
                name=f"receive_loop_{ws.ws_id}",
            )
            tasks.append(ws.receive_task)

            # wait for first task to complete
            done_set: set[asyncio.Task[None]]
            done_set, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)

            # process completed task
            if done_set:
                task = done_set.pop()
                try:
                    await task
                except WebSocketDisconnect as ex:
                    MutyLogger.get_instance().error(
                        f"websocket {ws.ws_id} disconnected: {ex}"
                    )
                    raise
                except Exception as ex:
                    MutyLogger.get_instance().error(
                        f"error in {task.get_name()}: {ex}")
                    raise

        finally:
            # ensure cleanup happens even if cancelled
            await ws.cleanup(tasks)
