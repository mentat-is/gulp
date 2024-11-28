import asyncio
from typing import override

from fastapi.websockets import WebSocketState
import muty.jsend
import muty.list
import muty.log
import muty.os
import muty.string
import muty.time
import muty.uploadfile
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from muty.log import MutyLogger
from starlette.endpoints import WebSocketEndpoint

from gulp.api.collab.structs import GulpUserPermission
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.ws_api import ConnectedSocket, GulpConnectedSockets, GulpWsAuthParameters
from gulp.config import GulpConfig


router = APIRouter()


class GulpAPIWebsocket:
    """
    handles gulp websocket connections

    we subclass starlette's WebSocketEndpoint to have better control on websocket termination, etc...
    """

    @router.websocket("/ws")
    @staticmethod
    async def ws_handler(websocket: WebSocket):
        """
        handles the websocket connection

        the websocket protocol is really simple:

        1. client sends a json request { "token": ..., "ws_id": ...}
        2. server checks the token and ws_id, and accepts the connection
        3. server sends messages to the client with the same ws_id (plus broadcasting CollabObj objects to the other connected websockets)

        Args:
            websocket (WebSocket): The websocket object.
        """
        ws = None
        try:
            await websocket.accept()
            js = await websocket.receive_json()
            params = GulpWsAuthParameters.model_validate(js)
            async with GulpCollab.get_instance().session() as sess:
                await GulpUserSession.check_token(
                    sess, params.token, GulpUserPermission.READ
                )

            MutyLogger.get_instance().debug(f"ws accepted for ws_id={params.ws_id}")
            ws = GulpConnectedSockets.get_instance().add(
                websocket, params.ws_id, params.type, params.operation_id
            )

            # blocks until exception/disconnect
            await ws.run_loop()

        except WebSocketDisconnect as ex:
            MutyLogger.get_instance().warning(f"webSocket disconnected: {ex}")
        except Exception as ex:
            MutyLogger.get_instance().error(f"ws error: {ex}")
        finally:
            if ws:
                try:
                    await GulpConnectedSockets.get_instance().remove(websocket)
                    if websocket.client_state == WebSocketState.CONNECTED:
                        # close gracefully
                        await websocket.close()
                except Exception as ex:
                    MutyLogger.get_instance().error(f"error during ws cleanup: {ex}")

    @router.websocket("/ws")
    @staticmethod
    async def ws_handler(websocket: WebSocket):
        """
        handles the websocket connection

        the websocket protocol is really simple:

        1. client sends a json request { "token": ..., "ws_id": ...}
        2. server checks the token and ws_id, and accepts the connection
        3. server sends messages to the client with the same ws_id (plus broadcasting CollabObj objects to the other connected websockets)

        Args:
            websocket (WebSocket): The websocket object.
        """
        ws = None
        try:
            await websocket.accept()
            js = await websocket.receive_json()
            params = GulpWsAuthParameters.model_validate(js)
            async with GulpCollab.get_instance().session() as sess:
                await GulpUserSession.check_token(
                    sess, params.token, GulpUserPermission.READ
                )

            MutyLogger.get_instance().debug(f"ws accepted for ws_id={params.ws_id}")
            ws = GulpConnectedSockets.get_instance().add(
                websocket, params.ws_id, params.type, params.operation_id
            )

            # blocks until exception/disconnect
            await ws.run_loop()

        except WebSocketDisconnect as ex:
            MutyLogger.get_instance().warning(f"webSocket disconnected: {ex}")
        except Exception as ex:
            MutyLogger.get_instance().error(f"ws error: {ex}")
        finally:
            if ws:
                try:
                    await GulpConnectedSockets.get_instance().remove(websocket)
                    if websocket.client_state == WebSocketState.CONNECTED:
                        # close gracefully
                        await websocket.close()
                except Exception as ex:
                    MutyLogger.get_instance().error(f"error during ws cleanup: {ex}")
