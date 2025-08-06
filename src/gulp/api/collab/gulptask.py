"""
a lightweight task object for Gulp, allowing to decouple task processing from the main request handling.

NOTE: this is currently a workaround until a proper task queue is implemented (e.g. Celery, RabbitMQ, etc...).
"""

from git import Optional
from typing import override
from sqlalchemy import ForeignKey, String, LargeBinary
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.dialects.postgresql import JSONB

from gulp.api.collab.structs import COLLABTYPE_TASK, GulpCollabBase


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
        String,
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
                "params": {
                    "param1": "value1",
                    "param2": 42,
                },
            }
        )
        return d
