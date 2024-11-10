import asyncio
from concurrent.futures import ThreadPoolExecutor
from enum import StrEnum
from queue import Empty, Queue
from typing import Any, Optional

from fastapi import WebSocket
import muty
from pydantic import BaseModel, Field

from gulp.utils import GulpLogger


class WsQueueDataType(StrEnum):
    """
    The type of data into the websocket queue.
    """

    STATS_UPDATE = "stats_update"
    COLLAB_UPDATE = "collab_update" # an array of GulpCollabObject (note, story, highlight, link)
    QUERY_DONE = "query_done"
    REBASE_DONE = "rebase_done"
    DOCS_CHUNK = "docs_chunk"  # a GulpDocumentsChunk to indicate a chunk of documents during ingest or query operation


class WsParameters(BaseModel):
    """
    Parameters for the websocket.
    """

    token: str = Field(..., description="user token")
    ws_id: str = Field(..., description="The WebSocket ID.")
    operation: Optional[list[str]] = Field(
        None,
        description="The operation/s this websocket is registered to receive data for, defaults to None(=all).",
    )
    type: Optional[list[WsQueueDataType]] = Field(
        None,
        description="The WsData.type this websocket is registered to receive, defaults to None(=all).",
    )


class GulpDocumentsChunk(BaseModel):
    """
    Represents a chunk of GulpDocument dictionaries returned by a query or sent during ingestion.

    may include extra fields depending on the source.
    """

    class Config:
        extra = "allow"

    docs: list[dict] = Field(
        ...,
        description="the documents in a query or ingestion chunk.",
    )
    chunk_number: int = Field(
        ...,
        description="the chunk number.",
    )
    total_hits: Optional[int] = Field(
        0,
        description="the total number of hits for the query related to this chunk (may not be available).",
    )
    last: Optional[bool] = Field(
        False,
        description="is this the last chunk of a query response ? (may not be available).",
    )


class WsData(BaseModel):
    """
    data carried by the websocket
    """

    timestamp: int = Field(..., description="The timestamp of the data.")
    type: WsQueueDataType = Field(
        ..., description="The type of data carried by the websocket."
    )
    ws_id: str = Field(..., description="The WebSocket ID.")
    user_id: str = Field(..., description="The user who issued the request.")
    req_id: Optional[str] = Field(None, description="The request ID.")
    operation: Optional[str] = Field(
        None, description="The operation this data belongs to."
    )
    private: Optional[bool] = Field(
        False,
        description="If the data is private(=only ws=ws_id can receive it).",
    )
    data: Optional[Any] = Field(None, description="The data carried by the websocket.")


class ConnectedSocket:
    """
    Represents a connected websocket.
    """

    def __init__(
        self,
        ws: WebSocket,
        ws_id: str,
        type: list[WsQueueDataType] = None,
        operation: list[str] = None,
    ):
        """
        Initializes the ConnectedSocket object.

        Args:
            ws (WebSocket): The WebSocket object.
            ws_id (str): The WebSocket ID.
            type (list[WsQueueDataType], optional): The types of data this websocket is interested in. Defaults to None (all).
            operation (list[str], optional): The operations this websocket is interested in. Defaults to None (all).
        """
        self.ws = ws
        self.ws_id = ws_id
        self.type = type
        self.operation = operation

        # each socket has its own asyncio queue
        self.q = asyncio.Queue()

    def __str__(self):
        return f"ConnectedSocket(ws_id={self.ws_id}, registered_types={self.type}, registered_operations={self.operation})"


class GulpConnectedSockets:
    """
    singleton class to hold all connected sockets
    """

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, "_initialized"):
            self._initialized = True
            self._sockets: dict[str, ConnectedSocket] = {}

    def add(
        self,
        ws: WebSocket,
        ws_id: str,
        type: list[WsQueueDataType] = None,
        operation: list[str] = None,
    ) -> ConnectedSocket:
        """
        Adds a websocket to the connected sockets list.

        Args:
            ws (WebSocket): The WebSocket object.
            ws_id (str): The WebSocket ID.
            type (list[WsQueueDataType], optional): The types of data this websocket is interested in. Defaults to None (all)
            operation (list[str], optional): The operations this websocket is interested in. Defaults to None (all)

        Returns:
            ConnectedSocket: The ConnectedSocket object.
        """
        ws = ConnectedSocket(ws=ws, ws_id=ws_id, type=type, operation=operation)
        self._sockets[str(id(ws))] = ws
        GulpLogger.get_instance().debug(f"added connected ws {id(ws)}: {ws}")
        return ws

    async def remove(self, ws: WebSocket, flush: bool = True) -> None:
        """
        Removes a websocket from the connected sockets list

        Args:
            ws (WebSocket): The WebSocket object.
            flush (bool, optional): If the queue should be flushed. Defaults to True.

        """
        id_str = str(id(ws))
        cws = self._sockets.get(id_str, None)
        if cws is None:
            GulpLogger.get_instance().warning(f"no websocket found for ws_id={id_str}")
            return

        # flush queue first
        if flush:
            q = cws.q
            while q.qsize() != 0:
                try:
                    q.get_nowait()
                    q.task_done()
                except Exception:
                    pass

            await q.join()
            GulpLogger.get_instance().debug(f"queue flush done for ws id={id_str}")

        GulpLogger.get_instance().debug(f"removed connected ws, id={id_str}")
        del self._sockets[id_str]

    def find(self, ws_id: str) -> ConnectedSocket:
        """
        Finds a ConnectedSocket object by its ID.

        Args:
            ws_id (str): The WebSocket ID.

        Returns:
            ConnectedSocket: The ConnectedSocket object or None if not found.
        """
        for _, v in self._sockets.items():
            if v.ws_id == ws_id:
                return v

        GulpLogger.get_instance().warning(f"no websocket found for ws_id={ws_id}")
        return None

    async def wait_all_close(self) -> None:
        """
        Waits for all active websockets to close.
        """
        while len(self._sockets) > 0:
            GulpLogger.get_instance().debug(
                "waiting for active websockets to close ..."
            )
            await asyncio.sleep(1)
        GulpLogger.get_instance().debug("all active websockets closed!")

    async def close_all(self) -> None:
        """
        Closes all active websockets.
        """
        for _, cws in self._sockets.items():
            await self.remove(cws.ws, flush=True)
        GulpLogger.get_instance().debug("all active websockets closed!")

    async def broadcast_data(self, d: WsData):
        """
        broadcasts data to all connected websockets

        Args:
            d (WsData): The data to broadcast.
        """
        for _, cws in self._sockets.items():
            if cws.type:
                # check types
                if not d.type in cws.type:
                    GulpLogger.get_instance().warning(
                        "skipping entry type=%s for ws_id=%s, cws.types=%s"
                        % (d.type, cws.ws_id, cws.type)
                    )
                    continue
            if cws.operation:
                # check operation/s
                if not d.operation in cws.operation:
                    GulpLogger.get_instance().warning(
                        "skipping entry type=%s for ws_id=%s, cws.operation=%s"
                        % (d.type, cws.ws_id, cws.operation)
                    )
                    continue

            if cws.ws_id == d.ws_id:
                # always relay to the ws async queue for the target websocket
                await cws.q.put(d)
            else:
                # not the target websocket
                if d.private:
                    # do not broadcast private data
                    GulpLogger.get_instance().warning(
                        "skipping entry type=%s for ws_id=%s, private=True"
                        % (d.type, cws.ws_id)
                    )
                    continue

                # only relay collab updates to other ws
                if d.type not in [WsQueueDataType.COLLAB_UPDATE]:
                    continue

                await cws.q.put(d)

class GulpSharedWsQueue:
    """
    singleton class to manage adding data to the shared websocket queue
    """

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        raise RuntimeError("call get_instance() instead")

    @classmethod
    def get_instance(cls) -> "GulpSharedWsQueue":
        """
        Returns the singleton instance.

        Returns:
            GulpSharedWsDataQueue: The singleton instance.
        """
        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        if not hasattr(self, "_initialized"):
            self._initialized = True
            self._shared_q: Queue = None

    @classmethod
    def get_instance(cls) -> "GulpSharedWsQueue":
        """
        Returns the singleton instance.

        Returns:
            GulpSharedWsDataQueue: The singleton instance.
        """
        return cls()

    def init_in_worker_process(self, q: Queue):
        """
        Initializes the shared queue in a worker process.

        Args:
            q (Queue): The shared queue from worker's process initializer
        """
        self._shared_q = q

    def init_in_main_process(self):
        """
        Initializes the shared queue in the main process.
        """
        # in the main process, initialize the shared queue and start the asyncio queue fill task
        self._shared_q: Queue = Queue()
        asyncio.create_task(self._fill_ws_queues_from_shared_queue())

    async def _fill_ws_queues_from_shared_queue(self):
        """
        runs continously (in the main process) to walk through the queued data in the multiprocessing shared queue and fill each connected websocket asyncio queue
        """

        # uses an executor to run the blocking get() call in a separate thread
        from gulp.api import rest_api

        GulpLogger.get_instance().debug("starting asyncio queue fill task ...")
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as pool:
            while True:
                if rest_api.is_shutdown():
                    break

                # GulpLogger.get_instance().debug("running ws_q.get in executor ...")
                try:
                    # get a WsData entry from the shared multiprocessing queue
                    d: dict = await loop.run_in_executor(
                        pool, self._shared_q.get, True, 1
                    )
                    self._shared_q.task_done()
                    entry = WsData.model_validate(d)

                    # find the websocket associated with this entry
                    cws = GulpConnectedSockets().find(entry.ws_id)
                    if not cws:
                        # no websocket found for this entry, skip (this WsData entry will be lost)
                        continue

                    # broadcast
                    await GulpConnectedSockets().broadcast_data(entry)

                except Empty:
                    # let's not busy wait...
                    await asyncio.sleep(1)
                    continue

    def close(self) -> None:
        """
        Closes the shared multiprocessing queue (flushes it first).

        Returns:
            None
        """
        # flush queue first
        while self._shared_q.qsize() != 0:
            try:
                self._shared_q.get_nowait()
                self._shared_q.task_done()
            except Exception:
                pass

        self._shared_q.join()

    def put(
        self,
        type: WsQueueDataType,
        ws_id: str,
        user_id: str,
        operation: str = None,
        req_id: str = None,
        data: Any = None,
        private: bool = False,
    ) -> None:
        """
        Adds data to the shared queue.

        Args:
            type (WsQueueDataType): The type of data.
            user (str): The user.
            ws_id (str): The WebSocket ID.
            operation (str, optional): The operation.
            req_id (str, optional): The request ID. Defaults to None.
            data (any, optional): The data. Defaults to None.
            private (bool, optional): If the data is private. Defaults to False.
        """
        wsd = WsData(
            timestamp=muty.time.now_msec(),
            type=type,
            operation=operation,
            ws_id=ws_id,
            user_id=user_id,
            req_id=req_id,
            private=private,
            data=data,
        )
        GulpLogger.get_instance().debug(
            "adding entry type=%s to ws_id=%s queue..." % (wsd.type, wsd.ws_id)
        )
        # TODO: try and see if it works without serializing...
        self._shared_q.put(wsd.model_dump(exclude_none=True))
