import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from enum import IntEnum, StrEnum, auto
from queue import Empty, Queue
from typing import override

import muty.crypto
import muty.file
import muty.jsend
import muty.list
import muty.log
import muty.os
import muty.string
import muty.time
import muty.uploadfile
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field
from starlette.endpoints import WebSocketEndpoint

from gulp.api import collab_api
from gulp.api.collab.user_session import GulpUserSession


import gulp.api.rest_api as rest_api
import gulp.config as config
import gulp.defs
import gulp.utils
from gulp.api.collab.structs import GulpUserPermission
from gulp.utils import logger

_app: APIRouter = APIRouter()
_connected_ws: dict[str, "ConnectedWs"] = {}
_global_shared_q: Queue = None


class WsQueueDataType(StrEnum):
    """
    The type of data into the websocket queue.
    """

    STATS_UPDATE = "stats_update"
    COLLAB_UPDATE = "collab_update"
    QUERY_DONE = "query_done"
    REBASE_DONE = "rebase_done"
    CHUNK = "chunk"


class WsParameters(BaseModel):
    token: str = Field(..., description="user token")
    operation: list[str] = Field(
        None,
        description="The operation/s on which this websocket is registered to receive data for, defaults to None(=all).",
    )
    types: list[WsQueueDataType] = Field(
        None,
        description="The types of data this websocket is interested in, defaults to None(=all).",
    )
    
    @staticmethod
    def from_dict(d: dict) -> "WsParameters":
        return WsParameters(**d)


class ConnectedWs(BaseModel):
    """
    a connected and active websocket
    """
    class Config:
        arbitrary_types_allowed = True
        
    ws: WebSocket = Field(..., description="The WebSocket instance.")
    ws_id: str = Field(..., description="The WebSocket ID.")
    q: asyncio.Queue = Field(
        ..., description="The asyncio queue associated with this websocket."
    )
    user: str = Field(..., description="user associated with this websocket.")
    params: WsParameters = Field(
        ..., description="creation parameters for this websocket."
    )


class WsData(BaseModel):
    """
    data carried by the websocket
    """

    type: WsQueueDataType = Field(
        ..., description="The type of data carried by the websocket."
    )
    operation: str = Field(..., description="The operation this data belongs to.")
    user: str = Field(..., description="The user who issued the request.")
    ws_id: str = Field(..., description="The WebSocket ID.")
    req_id: str = Field(..., description="The request ID.")
    private: bool = Field(
        False,
        description="If the data is private(=only ws with ws_id=ws_id can receive it).",
    )
    timestamp: int = Field(None, description="The timestamp of the data.")
    data: dict = Field(None, description="The data carried by the websocket.")

    def __init__(self, **kwargs):
        if "timestamp" not in kwargs:
            kwargs["timestamp"] = muty.time.now_nsec()
        super().__init__(**kwargs)

    def to_dict(self) -> dict:
        """
        Returns the object as a dictionary.

        Returns:
            dict: The object as a dictionary.
        """
        return self.model_dump()

    @staticmethod
    def from_dict(d: dict) -> "WsData":
        """
        Creates a WsData object from a dictionary.

        Args:
            d (dict): The dictionary.

        Returns:
            WsData: The WsData object.
        """
        return WsData(**d)


def init(ws_queue: Queue, main_process: bool = False):
    """
    Initializes the websocket API.

    Args:
        logger (logging.Logger): The logger object to be used for logging.
        ws_queue (Queue): proxy queue for websocket messages.
        main_process (bool, optional): If the current process is the main process. Defaults to False (to perform initialization in worker process, must be False).
    """
    global _global_shared_q
    _global_shared_q = ws_queue
    if main_process:
        # start the asyncio queue fill task to keep the asyncio queue filled from the ws queue
        asyncio.create_task(_fill_ws_queues_from_shared_queue(ws_queue))
    logger().debug("ws initialized!")


def shared_queue_close(q: Queue) -> None:
    """
    Closes the shared multiprocessing queue (flushes it first).

    Returns:
        None
    """
    # flush queue first
    while q.qsize() != 0:
        try:
            q.get_nowait()
            q.task_done()
        except Exception:
            pass

    q.join()


async def _fill_ws_queues_from_shared_queue(q: Queue):
    """
    runs continously (in the main process) to walk through the queued data in the multiprocessing shared queue and fill each connected websocket asyncio queue
    """

    # uses an executor to run the blocking get() call in a separate thread
    logger().debug("starting asyncio queue fill task ...")
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as pool:
        while True:
            if rest_api.is_shutdown():
                break

            # logger().debug("running ws_q.get in executor ...")
            try:
                # get a WsData entry from the shared multiprocessing queue
                d: dict = await loop.run_in_executor(pool, q.get, True, 1)
                q.task_done()
                entry = WsData.from_dict(d)

                # find the websocket associated with this entry
                cws = _find_target_ws(entry.ws_id)
                if cws is None:
                    # no websocket found for this entry, skip (this WsData entry will be lost)
                    logger().warning(
                        "no websocket found for ws_id=%s, skipping entry!"
                        % (entry.ws_id)
                    )
                    continue

                # broadcast
                for _, cws in _connected_ws.items():
                    if cws.types is not None:
                        # check types
                        if not entry.type in cws.types:
                            logger().warning(
                                "skipping entry type=%s for ws_id=%s, cws.types=%s"
                                % (entry.type, cws.ws_id, cws.types)
                            )
                            continue
                    if cws.operation is not None:
                        # check operation/s
                        if not entry.operation in cws.operation:
                            logger().warning(
                                "skipping entry type=%s for ws_id=%s, cws.operation=%s"
                                % (entry.type, cws.ws_id, cws.operation)
                            )
                            continue

                    if cws.ws_id == entry.ws_id:
                        # always relay to the ws async queue for the target websocket
                        await cws.q.put(d)
                    else:
                        # not target websocket
                        if entry.private:
                            # do not broadcast
                            logger().warning(
                                "skipping entry type=%s for ws_id=%s, private=True"
                                % (entry.type, cws.ws_id)
                            )
                            continue

                        # only relay collab updates to other ws
                        if entry.type not in [WsQueueDataType.COLLAB_UPDATE]:
                            continue

                        await cws.q.put(d)
            except Empty:
                await asyncio.sleep(1)
                continue


def shared_queue_add_data(
    type: WsQueueDataType,
    operation: str,
    user: str,
    ws_id: str,
    req_id: str,
    data: dict = None,
) -> None:
    """
    Adds data to the shared multiprocessing queue.

    Args:
        t (WsQueueDataType): The type of data.
        operation (str): The operation this data belongs to.
        username (str): The user who issued the request.
        ws_id (str): The WebSocket ID.
        req_id (str): The request ID.
        data (dict, optional): The data to add. Defaults to None.
    """
    global _global_shared_q
    wsd = WsData(
        type=type, operation=operation, user=user, ws_id=ws_id, req_id=req_id, data=data
    )
    logger().debug("adding entry type=%s to ws_id=%s queue..." % (wsd.type, wsd.ws_id))
    _global_shared_q.put(wsd.to_dict())


async def _check_ws_parameters(ws: WebSocket) -> WsParameters:
    """
    Check WebSocket parameters.

    Args:
        ws (WebSocket): The WebSocket object.

    Returns:
        WsParameters

    Raises:
        gulp.defs.InvalidArgument: If the 'ws_id' parameter is missing in the JSON request.
        gulp.defs.ObjectNotFound: If the user associated with the token is not found/expired.
        gulp.defs.MissingPermission: If the user does not have the required permissions.
    """
    # receive json request
    js = await ws.receive_json()
    params = WsParameters.from_dict(js)

    # check token
    u, _ = await GulpUserSession.check_token(params.token, GulpUserPermission.READ)

    # also check if we have monitor permission
    if u.is_monitor():
        js["is_monitor"] = True

    # check ws_id, req_id
    ws_id = js.get("ws_id", None)
    if ws_id is None:
        raise gulp.defs.InvalidArgument("ws_id is required.")

    return js


async def wait_all_connected_ws_close() -> None:
    """
    waits for all active websockets to close
    """
    global _connected_ws
    while len(_connected_ws) > 0:
        logger().debug("waiting for active websockets to close ...")
        await asyncio.sleep(1)
    logger().debug("all active websockets closed!")


def _find_target_ws(ws_id: str) -> ConnectedWs:
    """
    Finds a ConnectedWs object by its ID.

    Args:
        ws_id (str): The WebSocket ID.

    Returns:
        ConnectedWs: The ConnectedWs object.
    """
    global _connected_ws
    for _, v in _connected_ws.items():
        if v.ws_id == ws_id:
            return v
    return None


def _add_connected_ws(ws: WebSocket, params: dict) -> ConnectedWs:
    """
    Adds a WebSocket to the set of active WebSockets.

    Args:
        ws (WebSocket): The WebSocket to add.
        params (dict): The parameters to add along with the WebSocket.
    Returns:
        ConnectedWs: The ConnectedWs object.
    """
    global _connected_ws
    id_str = str(id(ws))

    # create a new ConnectedWs object, with its own queue, and add it to the dict
    q = asyncio.Queue()
    is_monitor: bool = params.get("is_monitor", False)
    types: list[WsQueueDataType] = params.get("types", None)
    ws_id: str = params["ws_id"]
    cws = ConnectedWs(ws, ws_id, q, is_monitor=is_monitor, types=types)

    # _connected_ws is a dict where keys are the ws object internal python id
    _connected_ws[id_str] = cws
    logger().debug("added connected ws %s: %s" % (id_str, cws))
    return cws


async def _remove_connected_ws(ws: WebSocket) -> str:
    """
    Removes a WebSocket from the set of active WebSockets (also flush the queue).

    Args:
        ws (WebSocket): The WebSocket to remove.
    Returns:
        str: The ID of the WebSocket.
    """
    global _connected_ws
    id_str = str(id(ws))
    logger().debug("removing connected ws, id=%s" % (id_str))
    cws = _connected_ws.get(id_str, None)
    if cws is None:
        return id_str

    # flush queue first
    q = cws.q
    while q.qsize() != 0:
        try:
            q.get_nowait()
            q.task_done()
        except Exception:
            pass

    await q.join()
    logger().debug("queue flush done for ws id=%s" % (id_str))
    del _connected_ws[id_str]
    return id_str


@_app.websocket_route("/ws")
class WebSocketHandler(WebSocketEndpoint):
    """
    the websocket protocol is really simple:

    1. client sends a json request { "token": ..., "ws_id": ...}
    2. server checks the token and ws_id, and accepts the connection
    3. server sends messages to the client with the same ws_id (plus broadcasting CollabObj objects to the other connected websockets)
    """

    def __init__(self, scope, receive, send) -> None:
        self.cws = None
        self.cancel_event = None
        self.consumer_task = None
        super().__init__(scope, receive, send)

    async def on_connect(self, websocket: WebSocket) -> None:
        logger().debug("awaiting accept ...")

        await websocket.accept()
        try:
            ws_parameters = await _check_ws_parameters(websocket)
        except Exception as ex:
            logger().error("ws rejected: %s" % (ex))
            return

        # connection is ok
        logger().debug("ws accepted for ws_id=%s!" % (ws_parameters["ws_id"]))
        self.cws = _add_connected_ws(websocket, ws_parameters)
        self.cancel_event = asyncio.Event()
        self.consumer_task = asyncio.create_task(self.send_data())
        logger().debug("created consumer task for ws_id=%s!" % (ws_parameters["ws_id"]))

    async def on_disconnect(self, websocket: WebSocket, close_code: int) -> None:
        logger().debug("on_disconnect")
        if self.consumer_task is not None:
            logger().debug("cancelling consumer task ...")
            self.consumer_task.cancel()

        # remove websocket from active list and close it
        await _remove_connected_ws(websocket)

    async def _read_items(self, q: asyncio.Queue):
        # logger().debug("reading items from queue ...")
        while True:
            item = await q.get()
            q.task_done()
            yield item

    async def send_data(self) -> None:
        logger().debug('starting ws "%s" loop ...' % (self.cws.ws_id))
        async for item in self._read_items(self.cws.q):
            try:
                # send
                await self.cws.ws.send_json(item)

                # rate limit
                ws_delay = config.ws_rate_limit_delay()
                await asyncio.sleep(ws_delay)

            except WebSocketDisconnect as ex:
                logger().exception("ws disconnected: %s" % (ex))
                break
            except Exception as ex:
                logger().exception("ws error: %s" % (ex))
                break
            except asyncio.CancelledError as ex:
                logger().exception("ws cancelled: %s" % (ex))
                break


def router() -> APIRouter:
    """
    Returns this module api-router, to add it to the main router

    Returns:
        APIRouter: The APIRouter instance
    """
    global _app
    return _app
