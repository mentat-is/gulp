import asyncio
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

from gulp.api.collab.context import GulpContext
from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.source import GulpSource
from gulp.api.collab.structs import GulpUserPermission, MissingPermission
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.ws_api import (
    GulpConnectedSocket,
    GulpConnectedSockets,
    GulpWsAcknowledgedPacket,
    GulpWsAuthPacket,
    GulpWsData,
    GulpWsError,
    GulpWsErrorPacket,
    GulpWsIngestPacket,
    GulpWsQueueDataType,
)
from gulp.plugin import GulpPluginBase
from gulp.structs import ObjectNotFound


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

        1. client sends a json request with GulpWsAuthParameters
        2. server accepts the connection and checks the token and ws_id
        3. on error, server sends a GulpWsErrorPacket and closes the connection. on success, it sends a GulpWsAcknowledgedPacket and starts the main loop.
        4. server sends messages to the client on the connected `ws_id` and also broadcastis Collab objects and events to the other connected websockets.

        Args:
            websocket (WebSocket): The websocket object.
        """
        ws = None
        try:
            await websocket.accept()
            js = await websocket.receive_json()
            params = GulpWsAuthPacket.model_validate(js)
            user_id = None
            if params.token.lower() != "monitor":
                # "monitor" skips token check, for internal usage only
                async with GulpCollab.get_instance().session() as sess:
                    s = await GulpUserSession.check_token(
                        sess, params.token, GulpUserPermission.READ
                    )
                    user_id = s.user_id

            MutyLogger.get_instance().debug(
                f"ws accepted for ws_id={params.ws_id}")
            ws = GulpConnectedSockets.get_instance().add(
                websocket, params.ws_id, params.types, params.operation_ids
            )

            # aknowledge connection
            p = GulpWsData(
                timestamp=muty.time.now_msec(),
                type=GulpWsQueueDataType.WS_CONNECTED,
                ws_id=params.ws_id,
                user_id=user_id,
                data=GulpWsAcknowledgedPacket(token=params.token).model_dump(
                    exclude_none=True
                ),
            )
            await websocket.send_json(p.model_dump(exclude_none=True))

            # blocks until exception/disconnect
            await ws.run_loop()
        except ObjectNotFound as ex:
            # user not found
            p = GulpWsErrorPacket(
                error=str(ex), error_code=GulpWsError.OBJECT_NOT_FOUND.name
            )
            wsd = GulpWsData(
                timestamp=muty.time.now_msec(),
                type=GulpWsQueueDataType.WS_ERROR,
                ws_id=params.ws_id,
                data=p.model_dump(exclude_none=True),
            )
            await websocket.send_json(wsd.model_dump(exclude_none=True, by_alias=True))
        except MissingPermission as ex:
            # user has no permission
            p = GulpWsErrorPacket(
                error=str(ex), error_code=GulpWsError.MISSING_PERMISSION.name
            )
            wsd = GulpWsData(
                timestamp=muty.time.now_msec(),
                type=GulpWsQueueDataType.WS_ERROR,
                ws_id=params.ws_id,
                data=p.model_dump(exclude_none=True),
            )
            await websocket.send_json(wsd.model_dump(exclude_none=True, by_alias=True))
        except WebSocketDisconnect as ex:
            MutyLogger.get_instance().exception(ex)
        except Exception as ex:
            MutyLogger.get_instance().exception(ex)
        finally:
            if ws:
                try:
                    await GulpConnectedSockets.get_instance().remove(websocket)
                except Exception as ex:
                    MutyLogger.get_instance().error(
                        f"error during ws cleanup: {ex}")
            if websocket.client_state == WebSocketState.CONNECTED:
                # close gracefully
                await websocket.close()

    @router.websocket("/ws_ingest_raw")
    @staticmethod
    async def ws_ingest_handler(websocket: WebSocket):
        """
        a websocket endpoint specific for ingestion

        1. client sends a json request with GulpWsAuthParameters
        2. server accepts the connection and checks the token (needs INGEST permission) and ws_id
        3. on error, server sends a GulpWsErrorPacket and closes the connection. on success, it sends a GulpWsAcknowledgedPacket and starts the main loop.
        4. client streams GulpWsIngestPackets over the websocket, the server replies ingested data on GulpWsIngestPacket.ws_id as it would be a normal ingestion using the http API.

        Args:
            websocket (WebSocket): The websocket object.
        """

        ws = None
        try:
            await websocket.accept()
            js = await websocket.receive_json()
            params = GulpWsAuthPacket.model_validate(js)
            user_id = None
            async with GulpCollab.get_instance().session() as sess:
                s = await GulpUserSession.check_token(
                    sess, params.token, GulpUserPermission.INGEST
                )
                user_id = s.user_id

            MutyLogger.get_instance().debug(
                f"ws_ingest accepted for ws_id={params.ws_id}")
            ws = GulpConnectedSockets.get_instance().add(
                websocket, params.ws_id
            )

            # aknowledge connection
            p = GulpWsData(
                timestamp=muty.time.now_msec(),
                type=GulpWsQueueDataType.WS_CONNECTED,
                ws_id=params.ws_id,
                user_id=user_id,
                data=GulpWsAcknowledgedPacket(token=params.token).model_dump(
                    exclude_none=True
                ),
            )
            await websocket.send_json(p.model_dump(exclude_none=True))

            # blocks until exception/disconnect
            await GulpAPIWebsocket.run_loop_ingest(ws, user_id)
        except ObjectNotFound as ex:
            # user not found
            p = GulpWsErrorPacket(
                error=str(ex), error_code=GulpWsError.OBJECT_NOT_FOUND.name
            )
            wsd = GulpWsData(
                timestamp=muty.time.now_msec(),
                type=GulpWsQueueDataType.WS_ERROR,
                ws_id=params.ws_id,
                data=p.model_dump(exclude_none=True),
            )
            await websocket.send_json(wsd.model_dump(exclude_none=True, by_alias=True))
        except MissingPermission as ex:
            # user has no permission
            p = GulpWsErrorPacket(
                error=str(ex), error_code=GulpWsError.MISSING_PERMISSION.name
            )
            wsd = GulpWsData(
                timestamp=muty.time.now_msec(),
                type=GulpWsQueueDataType.WS_ERROR,
                ws_id=params.ws_id,
                data=p.model_dump(exclude_none=True),
            )
            await websocket.send_json(wsd.model_dump(exclude_none=True, by_alias=True))
        except WebSocketDisconnect as ex:
            MutyLogger.get_instance().exception(ex)
        except Exception as ex:
            MutyLogger.get_instance().exception(ex)
        finally:
            if ws:
                try:
                    await GulpConnectedSockets.get_instance().remove(websocket)
                except Exception as ex:
                    MutyLogger.get_instance().error(
                        f"error during ws_ingest cleanup: {ex}")
            if websocket.client_state == WebSocketState.CONNECTED:
                # close gracefully
                await websocket.close()

    @staticmethod
    async def receive_ingest_loop(ws: GulpConnectedSocket, user_id: str) -> None:
        """
        receives ingest packets from the client and processes them

        Args:
            ws (GulpConnectedSocket): the websocket connection
            user_id (str): the user id
        """
        async with GulpCollab.get_instance().session() as sess:
            while True:
                if ws.ws.client_state != WebSocketState.CONNECTED:
                    raise WebSocketDisconnect("client disconnected")

                # this will raise WebSocketDisconnect when client disconnects
                js: dict = await ws.ws.receive_json()
                ingest_packet = GulpWsIngestPacket.model_validate(js)

                # load plugin
                mod: GulpPluginBase = await GulpPluginBase.load(
                    ingest_packet.plugin)

                # create (and associate) context and source on the collab db, if they do not exist
                operation: GulpOperation = await GulpOperation.get_by_id(sess, ingest_packet.operation_id)
                ctx: GulpContext = await operation.add_context(
                    sess, user_id=user_id, name=ingest_packet.context_name
                )
                src: GulpSource = await ctx.add_source(
                    sess,
                    user_id=user_id,
                    name=ingest_packet.source,
                )

                # ingest
                await mod.ingest_raw(
                    sess,
                    user_id=user_id,
                    req_id=ingest_packet.req_id,
                    ws_id=ingest_packet.ws_id,
                    index=ingest_packet.index,
                    operation_id=ingest_packet.operation_id,
                    context_id=ctx.id,
                    source_id=src.id,
                    chunk=ingest_packet.docs,
                    flt=ingest_packet.flt,
                    plugin_params=ingest_packet.plugin_params,
                )

    @staticmethod
    async def run_loop_ingest(ws: GulpConnectedSocket, user_id: str) -> None:
        """
        main loop for the ingest websocket connection

        Args:
            ws (GulpConnectedSocket): the websocket connection
            user_id (str): the user id
        """
        tasks: list[asyncio.Task] = []
        try:
            # Create tasks with names for better debugging
            ws.receive_task = asyncio.create_task(
                GulpAPIWebsocket.receive_ingest_loop(ws, user_id), name=f"receive_loop_{ws.ws_id}"
            )
            tasks.extend([ws.receive_task])

            # Wait for first task to complete
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)

            # Process completed task
            task = done.pop()
            try:
                await task
            except WebSocketDisconnect as ex:
                MutyLogger.get_instance().debug(
                    f"websocket {ws.ws_id} disconnected: {ex}"
                )
                raise
            except Exception as ex:
                MutyLogger.get_instance().error(
                    f"error in {task.get_name()}: {ex}")
                raise

        finally:
            # ensure cleanup happens even if cancelled
            await asyncio.shield(ws._cleanup_tasks(tasks))
