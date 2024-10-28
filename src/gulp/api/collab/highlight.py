from typing import Optional, Union, override
from sqlalchemy import BIGINT, ForeignKey, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.collab.structs import GulpCollabObject, GulpCollabType, T
from gulp.utils import logger


class GulpHighlight(GulpCollabObject):
    """
    an highlight in the gulp collaboration system
    """

    __tablename__ = GulpCollabType.HIGHLIGHT.value

    time_range: Mapped[tuple[int, int]] = mapped_column(
        JSONB,
        doc="The time range of the highlight, in nanoseconds from unix epoch.",
    )
    log_file_path: Mapped[Optional[str]] = mapped_column(
        String, default=None, doc="The associated log file path or name."
    )

    __mapper_args__ = {
        "polymorphic_identity": GulpCollabType.HIGHLIGHT.value,
    }

    @override
    @classmethod
    async def create(
        cls,
        id: str,
        owner: str,
        operation: str,
        time_range: tuple[int, int],
        log_file_path: str,
        glyph: str = None,
        tags: list[str] = None,
        title: str = None,
        private: bool = False,
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
        commit: bool = True,
        **kwargs,
    ) -> T:
        args = {
            "operation": operation,
            "time_range": time_range,
            "log_file_path": log_file_path,
            "glyph": glyph,
            "tags": tags,
            "title": title,
            "private": private,
        }
        return await super()._create(
            id,
            owner,
            ws_id,
            req_id,
            sess,
            commit,
            **args,
        )
