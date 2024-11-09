from typing import Optional
from sqlalchemy import String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from gulp.api.collab.structs import GulpCollabObject, GulpCollabType, T


class GulpHighlight(GulpCollabObject, type=GulpCollabType.HIGHLIGHT):
    """
    an highlight in the gulp collaboration system
    """

    time_range: Mapped[tuple[int, int]] = mapped_column(
        JSONB,
        doc="The time range of the highlight, in nanoseconds from unix epoch.",
    )
    log_file_path: Mapped[Optional[str]] = mapped_column(
        String, default=None, doc="The associated log file path or name."
    )

    @classmethod
    async def create(
        cls,
        token: str,
        id: str,
        operation: str,
        time_range: tuple[int, int],
        log_file_path: str,
        glyph: str = None,
        tags: list[str] = None,
        title: str = None,
        private: bool = False,
        ws_id: str = None,
        req_id: str = None,
        **kwargs,        
    ) -> T:
        """
        Create a new highlight object

        Args:
            token: the token of the user creating the object, for access check
            id: the id of the highlight
            operation: the operation associated with the highlight
            time_range: the time range of the highlight [start, end] in nanoseconds from unix epoch
            log_file_path: the log file path associated with the highlight
            glyph: the glyph associated with the highlight
            tags: the tags associated with the highlight
            title: the title of the highlight
            private: whether the highlight is private
            ws_id: the websocket id
            req_id: the request id
            kwargs: additional arguments

        Returns:
            the created highlight object
        """
        args = {
            "operation": operation,
            "time_range": time_range,
            "log_file_path": log_file_path,
            "glyph": glyph,
            "tags": tags,
            "title": title,
            "private": private,
            **kwargs,
        }
        return await super()._create(
            id,
            token=token,            
            ws_id=ws_id,
            req_id=req_id,
            **args,
        )
