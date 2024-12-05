import asyncio
import collections
from enum import StrEnum
from multiprocessing.managers import SyncManager
from queue import Empty, Queue
from typing import Any, Optional, override

import muty
from fastapi import WebSocket, WebSocketDisconnect
from muty.log import MutyLogger
from muty.pydantic import autogenerate_model_example
from pydantic import BaseModel, ConfigDict, Field

from gulp.api.collab.structs import GulpCollabType, GulpRequestStatus
from gulp.api.rest.test_values import (
    TEST_CONTEXT_ID,
    TEST_REQ_ID,
    TEST_SOURCE_ID,
)
from gulp.config import GulpConfig
import asyncio
from fastapi.websockets import WebSocketState


class GulpUserLoginLogoutPacket(BaseModel):
    """
    Represents a user login or logout event.
    """

    user_id: str = Field(..., description="The user ID.")
    login: bool = Field(
        ..., description="If the event is a login event, either it is a logout."
    )


class GulpCollabDeletePacket(BaseModel):
    """
    Represents a delete collab event.
    """

    id: str = Field(..., description="The collab ID.")


class GulpIngestSourceDonePacket(BaseModel):
    """
    to signal on the websocket that a source ingestion is done
    """

    model_config = ConfigDict(
        # solves the issue of not being able to populate fields using field name instead of alias
        populate_by_name=True,
    )
    source_id: str = Field(
        description="The source ID in the collab database.",
        alias="gulp.source_id",
        example=TEST_SOURCE_ID,
    )
    context_id: str = Field(
        ...,
        description="The context ID in the collab database.",
        alias="gulp.context_id",
        example=TEST_CONTEXT_ID,
    )
    req_id: str = Field(..., description="The request ID.", example=TEST_REQ_ID)
    status: GulpRequestStatus = Field(
        ..., description="The request status.", example=GulpRequestStatus.DONE
    )

    @override
    @classmethod
    def model_json_schema(cls, *args, **kwargs):
        return autogenerate_model_example(cls, *args, **kwargs)


class GulpCollabCreateUpdatePacket(BaseModel):
    """
    Represents a create or update event.
    """

    data: list | dict = Field(..., description="The created or updated data.")
    bulk: Optional[bool] = Field(
        default=False, description="If the event is a bulk event (data is a list)."
    )
    bulk_type: Optional[GulpCollabType] = Field(
        None, description="The type of the bulk event."
    )
    created: Optional[bool] = Field(
        default=False, description="If the event is a create event."
    )


class GulpWsError(StrEnum):
    """
    Used for "error_code" in GulpWsErrorPacket
    """

    OBJECT_NOT_FOUND = "Not Found"
    MISSING_PERMISSION = "Forbidden"


class GulpRebaseDonePacket(BaseModel):
    """
    Represents a rebase done event on the websocket.
    """

    src_index: str = Field(..., description="The source index.")
    dest_index: str = Field(..., description="The destination index.")
    status: "GulpRequestStatus" = Field(
        ..., description="The status of the rebase operation (done/failed)."
    )
    result: Optional[str | dict] = Field(
        None, description="the error message, or successful rebase result."
    )


class GulpWsErrorPacket(BaseModel):
    """
    Represents an error on the websocket
    """

    error: str = Field(..., description="error on the websocket.")
    error_code: Optional[str] = Field(None, description="optional error code.")
    data: Optional[dict] = Field(None, description="optional error data")


class GulpWsQueueDataType(StrEnum):
    """
    The type of data into the websocket queue.
    """

    # GulpWsErrorPacket
    WS_ERROR = "ws_error"
    # GulpCollabCreateUpdatePacket
    STATS_UPDATE = "stats_update"
    # GulpCollabCreateUpdatePacket
    COLLAB_UPDATE = "collab_update"
    # GulpUserLoginLogoutPacket
    USER_LOGIN = "user_login"
    # GulpUserLoginLogoutPacket
    USER_LOGOUT = "user_logout"
    # a GulpDocumentsChunk to indicate a chunk of documents during ingest or query operation
    DOCUMENTS_CHUNK = "docs_chunk"
    # GulpCollabDeletePacket
    COLLAB_DELETE = "collab_delete"
    # GulpIngestSourceDonePacket to indicate a source has been ingested
    INGEST_SOURCE_DONE = "ingest_source_done"
    QUERY_DONE = "query_done"
    # GulpRebaseDonePacket
    REBASE_DONE = "rebase_done"


class GulpWsAuthParameters(BaseModel):
    """
    Parameters for authentication on the websocket
    """

    token: str = Field(
        ...,
        description="""user token. 
        
        - `monitor` is a special token used to also monitor users login/logout.
    """,
    )
    ws_id: str = Field(..., description="the WebSocket ID.")
    operation_id: Optional[list[str]] = Field(
        None,
        description="the `operation_id`/s this websocket is registered to receive data for, defaults to `None` (all).",
    )
    type: Optional[list[GulpWsQueueDataType]] = Field(
        None,
        description="the `GulpWsData.type`/s this websocket is registered to receive, defaults to `None` (all).",
    )


class GulpDocumentsChunk(BaseModel):
    """
    Represents a chunk of GulpDocument dictionaries returned by a query or sent during ingestion.

    may include extra fields depending on the source.

    """

    model_config = ConfigDict(extra="allow")

    docs: list[dict] = Field(
        ...,
        description="the documents in a query or ingestion chunk.",
    )
    num_docs: int = Field(
        ...,
        description="the number of documents in this chunk.",
    )
    chunk_number: Optional[int] = Field(
        0,
        description="the chunk number (may not be available)",
    )
    total_hits: Optional[int] = Field(
        0,
        description="the total number of hits for the query related to this chunk (may not be available).",
    )
    last: Optional[bool] = Field(
        False,
        description="is this the last chunk of a query response ? (may not be available).",
    )
    search_after: Optional[dict] = Field(
        None,
        description="to use in `QueryAdditionalParameters.search_after` to request the next chunk in a paged query.",
    )


class GulpWsData(BaseModel):
    """
    data carried by the websocket ui<->gulp
    """

    model_config = ConfigDict(
        # solves the issue of not being able to populate fields using field name instead of alias
        populate_by_name=True,
    )
    timestamp: int = Field(
        ..., description="The timestamp of the data.", alias="@timestamp"
    )
    type: GulpWsQueueDataType = Field(
        ..., description="The type of data carried by the websocket."
    )
    ws_id: str = Field(..., description="The WebSocket ID.")
    user_id: Optional[str] = Field(None, description="The user who issued the request.")
    req_id: Optional[str] = Field(None, description="The request ID.")
    operation_id: Optional[str] = Field(
        None,
        description="The operation this data belongs to.",
        alias="gulp.operation_id",
    )
    private: Optional[bool] = Field(
        False,
        description="If the data is private, only the websocket `ws_id` receives it.",
    )
    data: Optional[Any] = Field(None, description="The data carried by the websocket.")


class WsQueueMessagePool:
    """
    message pool for the ws to reduce memory pressure and gc
    """

    def __init__(self, maxsize: int = 1000):
        # preallocate message pool
        self._pool = collections.deque(maxlen=maxsize)

    def get(self) -> dict:
        """
        get a preallocated message from the pool

        Returns:
            dict: the message
        """
        try:
            msg = self._pool.popleft()
            return msg
        except IndexError:
            return {}

    def put(self, msg: dict):
        """
        put a message back into the pool

        Args:
            msg (dict): the message to put back
        """
        # clear and put back
        msg.clear()
        self._pool.append(msg)


class ConnectedSocket:
    """
    Represents a connected websocket.
    """

    def __init__(
        self,
        ws: WebSocket,
        ws_id: str,
        types: list[GulpWsQueueDataType] = None,
        operation_ids: list[str] = None,
    ):
        """
        Initializes the ConnectedSocket object.

        Args:
            ws (WebSocket): The WebSocket object.
            ws_id (str): The WebSocket ID.
            types (list[GulpWsQueueDataType], optional): The types of data this websocket is interested in. Defaults to None (all).
            operation_ids (list[str], optional): The operation/s this websocket is interested in. Defaults to None (all).
        """
        self.ws = ws
        self.ws_id = ws_id
        self._msg_pool = WsQueueMessagePool()
        self.types = types
        self.operation_ids = operation_ids
        self.send_task = None
        self.receive_task = None

        # each socket has its own asyncio queue, consumed by its own task
        self.q = asyncio.Queue()

    async def run_loop(self) -> None:
        """
        Runs the websocket loop with optimized task management and cleanup
        """
        tasks: list[asyncio.Task] = []
        try:
            # Create tasks with names for better debugging
            self.send_task = asyncio.create_task(
                self._send_loop(), name=f"send_loop_{self.ws_id}"
            )
            self.receive_task = asyncio.create_task(
                self._receive_loop(), name=f"receive_loop_{self.ws_id}"
            )
            tasks.extend([self.send_task, self.receive_task])

            # Wait for first task to complete
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)

            # Process completed task
            task = done.pop()
            try:
                await task
            except WebSocketDisconnect as ex:
                MutyLogger.get_instance().debug(
                    f"websocket {self.ws_id} disconnected: {ex}"
                )
                raise
            except Exception as ex:
                MutyLogger.get_instance().error(f"error in {task.get_name()}: {ex}")
                raise

        finally:
            # ensure cleanup happens even if cancelled
            await asyncio.shield(self._cleanup_tasks(tasks))

    async def _cleanup_tasks(self, tasks: list[asyncio.Task]) -> None:
        """
        clean up tasks with proper cancellation handling

        Args:
            tasks (list): the tasks to clean up
        """
        for task in tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except WebSocketDisconnect:
                    pass
                except Exception as ex:
                    MutyLogger.get_instance().error(
                        f"error cleaning up {task.get_name()}: {ex}"
                    )

    async def _receive_loop(self) -> None:
        """
        continuously receives messages to detect disconnection
        """
        while True:
            if self.ws.client_state != WebSocketState.CONNECTED:
                raise WebSocketDisconnect("client disconnected")

            # this will raise WebSocketDisconnect when client disconnects
            await self.ws.receive_json()

    async def put_message(self, msg: dict) -> None:
        """
        Puts a message into the websocket queue.

        Args:
            msg (dict): The message to put.
        """
        if self.q.qsize() > 1000:
            # backpressure
            await asyncio.sleep(0.1)

        # use a slot from the pool
        pooled_msg = self._msg_pool.get()
        pooled_msg.update(msg)
        await self.q.put(pooled_msg)

    async def send_json(self, msg: dict, delay: float) -> None:
        """
        Sends a JSON message to the websocket.

        Args:
            msg (dict): The message to send.
            delay (float): The delay to wait before sending the message.
        """
        await self.ws.send_json(msg)

        # return msg to pool
        self._msg_pool.put(msg)

        # rate limit
        await asyncio.sleep(delay)

    async def _send_loop(self) -> None:
        """
        send messages from the ws queue to the websocket
        """
        try:
            MutyLogger.get_instance().debug(f'starting ws "{self.ws_id}" loop ...')
            ws_delay = GulpConfig.get_instance().ws_rate_limit_delay()

            while True:
                try:
                    # single message processing with timeout
                    item = await asyncio.wait_for(self.q.get(), timeout=0.1)

                    # check state before sending
                    if asyncio.current_task().cancelled():
                        raise asyncio.CancelledError()
                    if self.ws.client_state != WebSocketState.CONNECTED:
                        raise WebSocketDisconnect("client disconnected")

                    # send
                    await self.send_json(item, ws_delay)
                    self.q.task_done()

                except asyncio.TimeoutError:
                    if asyncio.current_task().cancelled():
                        raise asyncio.CancelledError()
                    continue

        except asyncio.CancelledError:
            MutyLogger.get_instance().debug(f'ws "{self.ws_id}" send loop cancelled')
            raise
        except WebSocketDisconnect as ex:
            MutyLogger.get_instance().debug(f'ws "{self.ws_id}" disconnected: {ex}')
            raise
        except Exception as ex:
            MutyLogger.get_instance().error(f'ws "{self.ws_id}" error: {ex}')
            raise

    def __str__(self):
        return f"ConnectedSocket(ws_id={self.ws_id}, registered_types={self.types}, registered_operations={self.operation_ids})"


class GulpConnectedSockets:
    """
    singleton class to hold all connected sockets
    """

    def __init__(self):
        raise RuntimeError("call get_instance() instead")

    @classmethod
    def get_instance(cls) -> "GulpConnectedSockets":
        """
        returns the singleton instance
        """
        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls)
            cls._instance._initialize()

        return cls._instance

    def _initialize(self):
        if not hasattr(self, "_initialized"):
            self._initialized = True
            self._sockets: dict[str, ConnectedSocket] = {}

    def add(
        self,
        ws: WebSocket,
        ws_id: str,
        type: list[GulpWsQueueDataType] = None,
        operation_id: list[str] = None,
    ) -> ConnectedSocket:
        """
        Adds a websocket to the connected sockets list.

        Args:
            ws (WebSocket): The WebSocket object.
            ws_id (str): The WebSocket ID.
            type (list[GulpWsQueueDataType], optional): The types of data this websocket is interested in. Defaults to None (all)
            operation_id (list[str], optional): The operations this websocket is interested in. Defaults to None (all)

        Returns:
            ConnectedSocket: The ConnectedSocket object.
        """
        wws = ConnectedSocket(
            ws=ws, ws_id=ws_id, types=type, operation_ids=operation_id
        )
        self._sockets[str(id(ws))] = wws
        MutyLogger.get_instance().debug(f"added connected ws: {wws}")
        return wws

    async def remove(self, ws: WebSocket, flush: bool = True) -> None:
        """
        Removes a websocket from the connected sockets list

        Args:
            ws (WebSocket): The WebSocket object.
            flush (bool, optional): If the queue should be flushed. Defaults to True.

        """
        id_str = str(id(ws))
        cws = self._sockets.get(id_str, None)
        if not cws:
            MutyLogger.get_instance().warning(f"no websocket found for ws_id={id_str}")
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
            MutyLogger.get_instance().debug(f"queue flush done for ws id={id_str}")

        MutyLogger.get_instance().debug(f"removed connected ws, id={id_str}")
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

        MutyLogger.get_instance().warning(f"no websocket found for ws_id={ws_id}")
        return None

    async def cancel_all(self) -> None:
        """
        Waits for all active websockets to close.
        """
        # cancel all tasks
        for _, cws in self._sockets.items():
            MutyLogger.get_instance().debug("canceling ws %s..." % (cws.ws_id))
            cws.receive_task.cancel()
            cws.send_task.cancel()
            await cws
        MutyLogger.get_instance().debug(
            "all active websockets closed, len=%d!" % (len(self._sockets))
        )

    async def _route_message(
        self, data: GulpWsData, client_ws: ConnectedSocket
    ) -> None:
        """
        routes the message to the target websocket

        the message is routed only if:

        - the message type is in the target websocket types AND
        - the message operation_id is in the target websocket operation_ids AND
        - private messages are only routed to the target websocket
        - collab updates are only relayed to other websockets

        Args:
            data (GulpWsData): The data to route.
            client_ws (ConnectedSocket): The target websocket.

        """
        # if types is set, only route if it matches
        if client_ws.types and data.type not in client_ws.types:
            MutyLogger.get_instance().warning(
                f"skipping entry type={data.type} for ws_id={client_ws.ws_id}, types={client_ws.types}"
            )
            return

        # if operation_id is set, only route if it matches
        if client_ws.operation_ids and data.operation_id not in client_ws.operation_ids:
            MutyLogger.get_instance().warning(
                f"skipping entry type={data.type} for ws_id={client_ws.ws_id}, operation_ids={client_ws.operation_ids}"
            )
            return

        # private messages are only routed to the target websocket
        if data.private and client_ws.ws_id != data.ws_id:
            MutyLogger.get_instance().warning(
                f"skipping private entry type={data.type} for ws_id={client_ws.ws_id}"
            )
            return

        # send the message
        message = data.model_dump(
            exclude_none=True, exclude_defaults=True, by_alias=True
        )
        await client_ws.put_message(message)

    async def broadcast_data(self, d: GulpWsData):
        """
        broadcasts data to all connected websockets

        Args:
            d (GulpWsData): The data to broadcast.
        """
        for _, cws in self._sockets.items():
            await self._route_message(d, cws)


class GulpSharedWsQueue:
    """
    singleton class to manage adding data to the shared websocket queue
    """

    _fill_task: asyncio.Task = None

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

    def set_queue(self, q: Queue) -> None:
        """
        Sets the shared queue.

        Args:
            q (Queue): The shared queue.
        """
        from gulp.process import GulpProcess

        if GulpProcess.get_instance().is_main_process():
            raise RuntimeError("set_queue() must be called in a worker process")

        MutyLogger.get_instance().debug(
            "setting shared ws queue in worker process: q=%s" % (q)
        )
        self._shared_q = q

    def init_queue(self, mgr: SyncManager) -> Queue:
        """
        to be called by the MAIN PROCESS, initializes the shared multiprocessing queue: the same queue must then be passed to the worker processes

        Args:
            q (Queue): The shared queue created by the multiprocessing manager in the main process
        """
        from gulp.process import GulpProcess

        if not GulpProcess.get_instance().is_main_process():
            raise RuntimeError("init_queue() must be called in the main process")

        if self._shared_q:
            # close first
            MutyLogger.get_instance().debug("closing shared ws queue ...")
            self.close()

        MutyLogger.get_instance().debug("re/initializing shared ws queue ...")
        self._shared_q = mgr.Queue()
        self._fill_task = asyncio.create_task(self._fill_ws_queues_from_shared_queue())

        return self._shared_q

    async def _fill_ws_queues_from_shared_queue(self):
        """
        runs continously (in the main process) to walk through the queued data in the multiprocessing shared queue and fill each connected websocket asyncio queue
        """

        # uses an executor to run the blocking get() call in a separate thread
        from gulp.api.rest_api import GulpRestServer

        MutyLogger.get_instance().debug("starting asyncio queue fill task ...")

        try:
            while not GulpRestServer.get_instance().is_shutdown():
                # process messages in batches (100 per batch)
                messages = []

                # quick non-blocking check for messages
                while len(messages) < 100:
                    try:
                        entry = self._shared_q.get_nowait()
                        messages.append(entry)
                        self._shared_q.task_done()
                    except Empty:
                        break

                if messages:
                    # Process batch of messages
                    for entry in messages:
                        cws = GulpConnectedSockets.get_instance().find(entry.ws_id)
                        if cws:
                            await GulpConnectedSockets.get_instance().broadcast_data(
                                entry
                            )

                    # small delay between batches
                    await asyncio.sleep(0.01)
                else:
                    # no messages, short sleep
                    await asyncio.sleep(0.1)

            MutyLogger.get_instance().debug("asyncio queue fill task terminated!")

        except Exception as ex:
            MutyLogger.get_instance().error(f"error in fill task: {ex}")
            raise

    def close(self) -> None:
        """
        Closes the shared multiprocessing queue (flushes it first).

        Returns:
            None
        """
        MutyLogger.get_instance().debug("closing shared ws queue ...")
        # flush queue first
        while self._shared_q.qsize() != 0:
            try:
                self._shared_q.get_nowait()
                self._shared_q.task_done()
            except Exception:
                pass

        self._shared_q.join()
        MutyLogger.get_instance().debug("shared queue flush done.")

        # and also kill the task
        if self._fill_task:
            b = self._fill_task.cancel()
            MutyLogger.get_instance().debug(
                "cancelling shared queue fill task done: %r" % (b)
            )

    def put(
        self,
        type: GulpWsQueueDataType,
        ws_id: str,
        user_id: str,
        operation_id: str = None,
        req_id: str = None,
        data: Any = None,
        private: bool = False,
    ) -> None:
        """
        Adds data to the shared queue.

        Args:
            type (GulpWsQueueDataType): The type of data.
            user (str): The user.
            ws_id (str): The WebSocket ID.
            operation (str, optional): The operation.
            req_id (str, optional): The request ID. Defaults to None.
            data (any, optional): The data. Defaults to None.
            private (bool, optional): If the data is private. Defaults to False.
        """
        wsd = GulpWsData(
            timestamp=muty.time.now_msec(),
            type=type,
            operation_id=operation_id,
            ws_id=ws_id,
            user_id=user_id,
            req_id=req_id,
            private=private,
            data=data,
        )
        if self._shared_q and ws_id:
            MutyLogger.get_instance().debug(
                "adding entry type=%s to ws_id=%s queue..." % (type, ws_id)
            )
            self._shared_q.put(wsd)
