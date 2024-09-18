import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from enum import IntEnum
from queue import Empty, Queue

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
from starlette.endpoints import WebSocketEndpoint
from gulp.utils import logger
import gulp.api.rest_api as rest_api
import gulp.config as config
import gulp.defs
import gulp.plugin
import gulp.utils
from gulp.api.collab.base import GulpUserPermission

_app: APIRouter = APIRouter()
_ws_q: Queue = None


class WsQueueDataType(IntEnum):
    """
    The type of data into the websocket queue.
    """

    INGESTION_STATS_CREATE = (
        1  # data: GulpStats with type=GulpCollabType.STATS_INGESTION
    )
    INGESTION_STATS_UPDATE = (
        2  # data: GulpStats with type=GulpCollabType.STATS_INGESTION
    )
    COLLAB_CREATE = 3  # data: {"collabs": [ CollabObj, CollabObj, ... ]}
    COLLAB_UPDATE = 4  # data: CollabObj
    COLLAB_DELETE = 5  # data: CollabObj
    QUERY_RESULT = 6  # data: QueryResult
    QUERY_DONE = 7  # data: { "status": GulpRequestStatus, "combined_hits": 12345 } to represent total hits for a request (which may execute more than one query)
    SIGMA_GROUP_RESULT = 8  # data: { "sigma_group_results": [ "...", "..." ] }
    INGESTION_CHUNK = 9  # data: { "events": [ GulpDocument, GulpDocument, ... ]}
    QUERY_STATS_CREATE = 10  # data: GulpStats with type=GulpCollabType.STATS_QUERY
    QUERY_STATS_UPDATE = 11  # data: GulpStats with type=GulpCollabType.STATS_QUERY
    INGESTION_DONE=12 # data: { "src_file"": "...", "context": "..." }
    REBASE_DONE=13 # data: { "status": GulpRequestStatus, "error": str (on error only), "index": "...", "dest_index": "...", "result": { ... } }


class ConnectedWs:
    """
    to identify active websockets

    Args:
        ws (WebSocket): The WebSocket instance.
        ws_id (str): The WebSocket ID.
        q (asyncio.Queue): The asyncio queue.
        is_monitor (bool, optional): If the WebSocket is a monitor socket. Defaults to False.
        types (list[WsQueueDataType], optional): The types of data the WebSocket is interested in. Defaults to None (all)
    """

    def __init__(
        self,
        ws: WebSocket,
        ws_id: str,
        q: asyncio.Queue,
        is_monitor: bool = False,
        types: list[WsQueueDataType] = None,
    ):
        # the websocket instance itself
        self.ws: WebSocket = ws
        # the same ws_id passed to REST API, to identify the websocket to send data to
        self.ws_id: str = ws_id
        # websocket queue to send data to
        self.q: asyncio.Queue = q
        # if the websocket is a monitor socket (user has MONITOR permission)
        self.is_monitor: bool = is_monitor
        # types of data this websocket is interested in (empty=all)
        self.types: list[WsQueueDataType] = types if types is not None else []

    def __str__(self) -> str:
        return "ConnectedWs(ws_id=%s, is_monitor=%r, types=%s)" % (
            self.ws_id,
            self.is_monitor,
            self.types,
        )


class WsData:
    """
    data carried by the websocket

    t: one of WsQueueDataType values
    ws_id: the websocket id
    data: the data itself
    username: the username of the user who sent the data
    timestamp: the timestamp of the data
    """

    def __init__(
        self,
        t: WsQueueDataType,
        ws_id: str,
        req_id: str,
        data: dict = None,
        username: str = None,
        timestamp: int = None,
    ):
        self.type: WsQueueDataType = t
        self.data: dict = data if data is not None else {}
        self.username: str = username
        self.req_id: str = req_id
        self.ws_id: str = ws_id
        if timestamp is None:
            # set
            self.timestamp: int = muty.time.now_nsec()
        else:
            # take from parameters (used by from_dict)
            self.timestamp: int = timestamp

    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "data": self.data,
            "req_id": self.req_id,
            "username": self.username,
            "timestamp": self.timestamp,
            "ws_id": self.ws_id,
        }

    @staticmethod
    def from_dict(d: dict) -> "WsData":
        return WsData(
            t=d["type"],
            ws_id=d["ws_id"],
            req_id=d["req_id"],
            data=d["data"],
            username=d["username"],
            timestamp=d["timestamp"],
        )

    def __str__(self) -> str:
        return (
            "WsData(type=%s, data=%s, username=%s, ws_id=%s, req_id=%s, timestamp=%d)"
            % (
                self.type,
                self.data,
                self.username,
                self.ws_id,
                self.req_id,
                self.timestamp,
            )
        )


_connected_ws: dict[str, ConnectedWs] = {}


def init(ws_queue: Queue, main_process: bool = False):
    """
    Initializes the websocket API.

    Args:
        logger (logging.Logger): The logger object to be used for logging.
        ws_queue (Queue): proxy queue for websocket messages.
        main_process (bool, optional): If the current process is the main process. Defaults to False (to perform initialization in worker process, must be False).
    """
    global _ws_q
    _ws_q = ws_queue
    if main_process:
        # start the asyncio queue fill task to keep the asyncio queue filled from the ws queue
        asyncio.create_task(_fill_ws_queues_from_shared_queue(logger, ws_queue))

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


async def _fill_ws_queues_from_shared_queue(l: logging.Logger, q: Queue):
    """
    fills each connected websocket queue from the shared ws queue
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
                # get an entry from the shared multiprocessing queue
                d: dict = await loop.run_in_executor(pool, q.get, True, 1)
                q.task_done()

                # check for which ws it is
                entry = WsData.from_dict(d)
                cws = _find_connected_ws_by_id(entry.ws_id)
                my_ws_id: str = None
                if cws is not None:
                    my_ws_id = cws.ws_id
                    # check if the entry type is in the types list for the found socket
                    if len(cws.types) > 0 and entry.type not in cws.types:
                        logger().debug(
                            "skipping data for websocket %s, not in entry.types!"
                            % (cws)
                        )
                        continue

                    # put it in the ws async queue for the selected websocket
                    await cws.q.put(d)
                    logger().debug("data put in %s queue, type=%s!" % (cws, entry.type))

                    # rely collab data (including sigma group results, which may be interesting to all) to all other sockets
                    for _, cws in _connected_ws.items():
                        if cws.ws_id != my_ws_id and entry.type not in [
                            WsQueueDataType.INGESTION_STATS_CREATE,
                            WsQueueDataType.INGESTION_STATS_UPDATE,
                            WsQueueDataType.QUERY_STATS_CREATE,
                            WsQueueDataType.QUERY_STATS_UPDATE,
                            WsQueueDataType.INGESTION_CHUNK,
                            WsQueueDataType.QUERY_DONE,
                            WsQueueDataType.QUERY_RESULT,
                        ]:
                            # but skip private data
                            is_private = False
                            if entry.data is not None:
                                is_private = entry.data.get("private", False)
                            if not is_private:
                                await cws.q.put(d)
                                logger().debug("data put in %s queue (RELY)!" % (cws))
                            else:
                                logger().debug("skipping private data for %s!" % (cws))
            except Empty:
                await asyncio.sleep(1)
                continue


def shared_queue_add_data(
    t: WsQueueDataType, req_id: str, data: dict, username: str = None, ws_id: str = None
) -> None:
    """
    adds an entry to the ws queue

    Args:
        t (WsQueueDataType): The type of data to be added to the queue.
        req_id (str): The request ID.
        data (dict): The data to be added to the queue.
        username (str, optional): The username. Defaults to None.
        ws_id (str, optional): The websocket id. Defaults to None.
    """
    if ws_id is not None and len(ws_id) > 0:
        wsd = WsData(
            t=t,
            ws_id=ws_id,
            req_id=req_id,
            data=data,
            username=username,
        )

        global _ws_q
        logger().debug(
            "adding entry type=%s to ws_id=%s queue..." % (wsd.type, wsd.ws_id)
        )
        _ws_q.put(wsd.to_dict())


async def _check_ws_parameters(ws: WebSocket) -> dict:
    """
    Check WebSocket parameters.

    Args:
        ws (WebSocket): The WebSocket object.

    Returns:
        dict: The JSON request parameters.

    Raises:
        gulp.defs.InvalidArgument: If the 'ws_id' parameter is missing in the JSON request.
        gulp.defs.ObjectNotFound: If the user associated with the token is not found/expired.
        gulp.defs.MissingPermission: If the user does not have the required permissions.
    """
    import gulp.api.collab_api as collab_api
    from gulp.api.collab.session import UserSession

    # receive json request
    js = await ws.receive_json()

    # check token
    u, _ = await UserSession.check_token(
        await collab_api.collab(), js["token"], GulpUserPermission.READ
    )

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


def _find_connected_ws_by_id(ws_id: str) -> ConnectedWs:
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
