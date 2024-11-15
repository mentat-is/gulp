from typing import Optional, override
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

    @override
    def __init__(self, *args, **kwargs):
        # initializes the base class
        super().__init__(*args, type=GulpCollabType.HIGHLIGHT, **kwargs)

    @classmethod
    async def create(
        cls,
        token: str,
        operation_id: str,
        time_range: tuple[int, int],
        log_file_path: str,
        glyph_id: str = None,
        color: str = None,
        tags: list[str] = None,
        title: str = None,
        description: str = None,
        private: bool = False,
        ws_id: str = None,
        req_id: str = None,
        **kwargs,        
    ) -> T:
        """
        Create a new highlight object on the collab database.

        Args:
            token(str): the token of the user creating the object, for access check
            operation_id(str): the id of the operation associated with the highlight
            time_range(tuple[int, int]): the time range of the highlight (start, end, in nanoseconds from unix epoch)
            log_file_path(str): the associated log file path or source name
            glyph_id(str, optional): the id of the glyph associated with the highlight
            color(str, optional): the color associated with the highlight (default: green)
            tags(list[str], optional): the tags associated with the highlight
            title(str, optional): the title of the highlight
            description(str, optional): the description of the highlight
            private(bool, optional): whether the highlight is private
            ws_id(str, optional): the websocket id
            req_id(str, optional): the request id
            kwargs: additional arguments

        Returns:
            the created highlight object
        """
        args = {
            "operation_id": operation_id,
            "time_range": time_range,
            "log_file_path": log_file_path,
            "glyph_id": glyph_id,
            "color": color or "green",
            "tags": tags,
            "title": title,
            "description": description,
            "private": private,
            **kwargs,
        }
        # id is automatically generated
        return await super()._create(
            token=token,            
            ws_id=ws_id,
            req_id=req_id,
            **args,
        )
