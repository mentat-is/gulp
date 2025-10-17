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
        String, doc="type of the task, e.g. 'ingest', etc..."
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
    async def enqueue(
        cls,
        sess: AsyncSession,
        task_type: str,
        operation_id: str,
        user_id: str,
        ws_id: str,
        req_id: str,
        params: dict,
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
            )
        except Exception as ex:
            await sess.rollback()
            raise

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
        raise TypeError("use enqueue to create tasks")
