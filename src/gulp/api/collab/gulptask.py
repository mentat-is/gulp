"""
a lightweight task object for Gulp, allowing to decouple task processing from the main request handling.

NOTE: this is currently a workaround until a proper task queue is implemented (e.g. Celery, RabbitMQ, etc...).
"""

from typing import override, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession
import os
from muty.log import MutyLogger
from sqlalchemy import ForeignKey, Insert, String, LargeBinary, insert
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.dialects.postgresql import JSONB
import muty.crypto
from gulp.api.collab.structs import COLLABTYPE_TASK, GulpCollabBase, GulpUserPermission


class GulpTask(GulpCollabBase, type=COLLABTYPE_TASK):
    """
    a lightweight task object for Gulp, allowing to decouple task processing from the main request handling.
    """

    ws_id: Mapped[str] = mapped_column(
        String,
        doc="id of the websocket connection this entry is associated with",
    )
    operation_id: Mapped[str] = mapped_column(
        ForeignKey(
            "operation.id",
            ondelete="CASCADE",
        ),
        doc="id of the operation this source is associated with",
    )
    req_id: Mapped[str] = mapped_column(
        doc="id of the request this entry is associated with",
    )
    task_type: Mapped[str] = mapped_column(
        String, doc="type of the task, e.g. 'ingest', 'query', etc..."
    )
    params: Mapped[dict] = mapped_column(
        MutableDict.as_mutable(JSONB),
        doc="task parameters, depending on the task type",
    )
    raw_data: Mapped[Optional[bytes]] = mapped_column(
        LargeBinary, nullable=True, doc="raw data associated with the task."
    )
    pid: Mapped[int] = mapped_column(doc="the process id starting the task")

    @override
    @classmethod
    def example(cls) -> dict:
        d = super().example()
        d.update(
            {
                "user_id": "user_id",
                "ws_id": "websocket_id",
                "operation_id": "operation_id",
                "req_id": "request_id",
                "task_type": "ingest",
                "pid": 12345,
                "params": {
                    "param1": "value1",
                    "param2": 42,
                },
            }
        )
        return d

    @classmethod
    async def enqueue_bulk(
        cls,
        task_type: str,
        operation_id: str,
        user_id: str,
        ws_id: str,
        req_id: str,
        params_list: list[dict],
        raw_data_list: Optional[list[bytes]] = None,
        sess: Optional[AsyncSession] = None,
    ) -> None:
        """
        enqueue multiple new tasks in the database, to be processed later by a worker.

        Args:
            task_type (str): the type of the task, e.g. 'ingest',
            operation_id (str): the operation id the task is associated with.
            user_id (str): the user id the task is associated with, must be already verified
            ws_id (str): the websocket id the task is associated with.
            req_id (str): the request id the task is associated with.
            params_list (list[dict]): a list of parameters of the tasks, depending on the task type.
            raw_data_list (list[bytes], optional): a list of raw data associated with the tasks, if any.
            sess (AsyncSession, optional): the database session to use: if not provided, a new session will be created. Defaults to None.
        Raises:
            any exception raised by the underlying create method.
        """

        if raw_data_list:
            if len(raw_data_list) != len(params_list):
                raise ValueError("raw_data_list must be of same length as params_list")

        batch: list[dict] = []
        i: int = 0
        for p in params_list:
            # enqueue ingestion task on collab
            object_data: dict[str, Any] = {
                "ws_id": ws_id,
                "operation_id": operation_id,
                "req_id": req_id,
                "task_type": task_type,
                "params": p,
                "pid": os.getpid(),
                "raw_data": raw_data_list[i] if raw_data_list else None,
            }
            obj_id: str = muty.crypto.hash_xxh128(str(object_data))
            task_dict = GulpTask.build_base_object_dict(
                object_data=object_data, user_id=user_id, obj_id=obj_id
            )
            batch.append(task_dict)
            i += 1

        # add batch
        stmt: Insert = insert(GulpTask).values(batch)
        stmt = stmt.on_conflict_do_nothing(index_elements=["id"])
        stmt = stmt.returning(GulpTask.id)
        res = await sess.execute(stmt)
        await sess.commit()
        inserted_ids = [row[0] for row in res]
        MutyLogger.get_instance().info(
            "written %d tasks on collab db" % len(inserted_ids)
        )

    @classmethod
    async def enqueue(
        cls,
        task_type: str,
        operation_id: str,
        user_id: str,
        ws_id: str,
        req_id: str,
        params: dict,
        raw_data: Optional[bytes] = None,
        signal_task_enqueued: bool = False,
        sess: Optional[AsyncSession] = None,
    ) -> None:
        """
        enqueue a new task in the database, to be processed later by a worker.

        Args:
            task_type (str): the type of the task, e.g. 'ingest',
            operation_id (str): the operation id the task is associated with.
            user_id (str): the user id the task is associated with, must be already verified
            ws_id (str): the websocket id the task is associated with.
            req_id (str): the request id the task is associated with.
            params (dict): the parameters of the task, depending on the task type.
            raw_data (bytes, optional): the raw data associated with the task, if any.
            signal_task_enqueued (bool): if True, the ws_id will be set on the task, so that a notification will be sent to the client when the task is created. Defaults to False.
            sess (AsyncSession, optional): the database session to use: if not provided, a new session will be created. Defaults to None.
        Raises:
            any exception raised by the underlying create method.
        """

        # enqueue ingestion task on collab
        object_data: dict[str, Any] = {
            "ws_id": ws_id,
            "operation_id": operation_id,
            "req_id": req_id,
            "task_type": task_type,
            "params": params,
            "pid": os.getpid(),
        }
        MutyLogger.get_instance().debug(
            "queueing task_type=%s, object_data=%s" % (task_type, object_data)
        )
        await cls.create(
            token=None,
            user_id=user_id,  # permission already checked
            ws_id=ws_id if signal_task_enqueued else None,  # only signal if requested
            req_id=req_id,
            object_data=object_data,
            sess=sess,
            raw_data=raw_data,
        )

    @override
    @classmethod
    async def create(
        cls,
        token: str,
        ws_id: str,
        req_id: str,
        object_data: dict,
        permission: list[GulpUserPermission] = None,
        obj_id: str = None,
        private: bool = True,
        operation_id: str = None,
        user_id: str = None,
        sess: AsyncSession = None,
        **kwargs,
    ) -> dict:
        """
        disabled, use enqueue method to create tasks.
        """
        raise TypeError("use enqueue method to create tasks")
