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
from gulp.api.collab.structs import COLLABTYPE_TASK, GulpCollabBase
import muty.string


class GulpTask(GulpCollabBase, type=COLLABTYPE_TASK):
    """
    a lightweight task object for Gulp, allowing to decouple task processing from the main request handling.
    """

    task_type: Mapped[str] = mapped_column(
        String, doc="type of the task, e.g. 'ingest', 'query', etc..."
    )
    params: Mapped[dict] = mapped_column(
        MutableDict.as_mutable(JSONB),
        doc="task parameters, depending on the task type",
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
        sess: AsyncSession,
        task_type: str,
        operation_id: str,
        user_id: str,
        ws_id: str,
        req_id: str,
        params_list: list[dict],
        raw_data_list: Optional[list[bytes]] = None,
    ) -> None:
        """
        enqueue multiple new tasks, of the same type, to be processed later by a worker.

        Args:
            sess (AsyncSession): the database session to use.
            task_type (str): type of the tasks, e.g. 'ingest',
            operation_id (str): the operation id the tasks are associated with.
            user_id (str): the user id the tasks are associated with, must be already verified
            ws_id (str): the websocket id the tasks are associated with.
            req_id (str): the request id the tasks are associated with.
            params_list (list[dict]): a list of parameters for each task
            raw_data_list (list[bytes], optional): a list of raw data for each tasks, if any (must be in the same order as params_list). Defaults to None.
        """

        if raw_data_list:
            if len(raw_data_list) != len(params_list):
                raise ValueError("raw_data_list must be of same length as params_list")

        batch: list[dict] = []
        i: int = 0
        for p in params_list:
            # build each task object
            task_dict = GulpTask.build_object_dict(
                user_id=user_id,
                operation_id=operation_id,
                ws_id=ws_id,
                req_id=req_id,
                task_type=task_type,
                pid=os.getpid(),
                raw_data=raw_data_list[i] if raw_data_list else None,
            )
            batch.append(task_dict)
            i += 1

        # enquueue tasks in bulk
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
        sess: AsyncSession,
        task_type: str,
        operation_id: str,
        user_id: str,
        ws_id: str,
        req_id: str,
        params: dict,
        raw_data: Optional[bytes] = None,
    ) -> None:
        """
        enqueue a new task in the database, to be processed later by a worker.

        Args:
            sess (AsyncSession): the database session to use.
            task_type (str): the type of the task, e.g. 'ingest',
            operation_id (str): the operation id the task is associated with.
            user_id (str): the user id the task is associated with, must be already verified
            ws_id (str): the websocket id the task is associated with.
            req_id (str): the request id the task is associated with.
            params (dict): the parameters of the task
            raw_data (bytes, optional): the raw data associated with the task, if any.

        Raises:
            any exception raised by the underlying create method.
        """

        MutyLogger.get_instance().debug(
            "queueing task_type=%s, ws_id=%s, req_id=%s, params=%s"
            % (task_type, ws_id, req_id, muty.string.make_shorter(str(params), 100))
        )
        try:
            await GulpTask.create_internal(
                sess,
                user_id,
                operation_id=operation_id,
                req_id=req_id,
                task_type=task_type,
                pid=os.getpid(),
                params=params,
                extra_object_data={
                    "ws_id": ws_id,
                    "req_id": req_id,
                },  # avoids clash with main ws_id, req_id params
            )
        except Exception as ex:
            await sess.rollback()
            raise ex

    @override
    @classmethod
    async def create(
        cls,
        *args,
        **kwargs,
    ) -> dict:
        """
        disabled, use enqueue method to create tasks.
        """
        raise TypeError("use enqueue or enqueue_bulk methods to create tasks")
