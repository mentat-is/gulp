import asyncio
from typing import override

import muty.jsend
import muty.list
import muty.log
import muty.os
import muty.string
import muty.time
import muty.uploadfile
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from starlette.endpoints import WebSocketEndpoint

import gulp.config as config
from gulp.api.collab.structs import GulpUserPermission
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.ws_api import ConnectedSocket, GulpConnectedSockets, WsParameters
from gulp.utils import GulpLogger

class WsRouter:
    """
    handles rest entrypoint/s for the websocket
    """
    @staticmethod
    def create_router() -> APIRouter:
        """
        Returns this module api-router, to add it to the main router

        Returns:
            APIRouter: The APIRouter instance
        """

        router = APIRouter()
        @router.websocket_route("/ws")
        class WebSocketHandler(WebSocketEndpoint):
            """
            the websocket protocol is really simple:

            1. client sends a json request { "token": ..., "ws_id": ...}
            2. server checks the token and ws_id, and accepts the connection
            3. server sends messages to the client with the same ws_id (plus broadcasting CollabObj objects to the other connected websockets)
            """
            @override
            def __init__(self, scope, receive, send) -> None:
                self._ws: ConnectedSocket = None
                self._cancel_event: asyncio.Event = None
                self._consumer_task: asyncio.Task = None

                super().__init__(scope, receive, send)

            @override
            async def on_connect(self, websocket: WebSocket) -> None:
                GulpLogger.get_instance().debug("awaiting accept ...")
                super().on_connect(websocket)

                try:
                    js = await websocket.receive_json()
                    params = WsParameters.model_validate_json(js)
                    await GulpUserSession.check_token_permission(params.token, GulpUserPermission.READ)
                except Exception as ex:
                    GulpLogger.get_instance().error("ws rejected: %s" % (ex))
                    return

                # connection is ok
                GulpLogger.get_instance().debug("ws accepted for ws_id=%s!" % (params.ws_id))
                ws = GulpConnectedSockets().add(websocket, params.ws_id, params.type, params.operation)
                self._ws = ws
                self._cancel_event = asyncio.Event()

                # start the consumer task to send data to the websocket as it arrives in the queue (via calls GulpSharedWsDataQueue.add_data())
                self._consumer_task = asyncio.create_task(self.send_data_loop())
                GulpLogger.get_instance().debug("created consumer task for ws_id=%s!" % (params.ws_id))

            @override
            async def on_disconnect(self, websocket: WebSocket, close_code: int) -> None:
                GulpLogger.get_instance().debug("on_disconnect, close_code=%d" % (close_code))
                if self._consumer_task is not None:
                    GulpLogger.get_instance().debug("canceling consumer task ...")
                    self._consumer_task.cancel()

                # remove websocket from active list and close it
                await GulpConnectedSockets().remove(websocket)

            async def _read_items(self, q: asyncio.Queue):
                """
                Reads WsData items from the websocket's asyncio queue.

                Args:
                    q (asyncio.Queue): The asyncio queue.

                Yields:
                    Any: The item read from the queue.            
                """
                # GulpLogger.get_instance().debug("reading items from queue ...")
                while True:
                    item = await q.get()
                    q.task_done()
                    yield item

            async def send_data_loop(self) -> None:
                """
                Sends data to the websocket as it arrives, infinitely looping until the websocket disconnects.

                Raises:
                    WebSocketDisconnect: If the websocket disconnects.
                """
                GulpLogger.get_instance().debug('starting ws "%s" loop ...' % (self._ws.ws_id))
                async for item in self._read_items(self._ws.q):
                    try:
                        # send
                        await self._ws.ws.send_json(item)

                        # rate limit
                        ws_delay = config.ws_rate_limit_delay()
                        await asyncio.sleep(ws_delay)

                    except WebSocketDisconnect as ex:
                        GulpLogger.get_instance().exception("ws disconnected: %s" % (ex))
                        break
                    except Exception as ex:
                        GulpLogger.get_instance().exception("ws error: %s" % (ex))
                        break
                    except asyncio.CancelledError as ex:
                        GulpLogger.get_instance().exception("ws cancelled: %s" % (ex))
                        break
        
        return router
