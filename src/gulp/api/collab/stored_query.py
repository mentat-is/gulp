from typing import Optional, Union, override
from sqlalchemy import BIGINT, Boolean, ForeignKey, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.collab.structs import GulpCollabBase, GulpCollabObject, GulpCollabType, T
from gulp.utils import GulpLogger


class GulpStoredQuery(GulpCollabBase, type=GulpCollabType.STORED_QUERY):
    """
    a stored query in the gulp collaboration system
    """
    dsl: Mapped[dict] = mapped_column(
        JSONB,
        doc="The query in OpenSearch DSL format, ready to be used by the opensearch query api.",
    )
    sigma: Mapped[Optional[bool]] = mapped_column(
        Boolean,
        default=False,
        doc="Whether the query is a sigma query.",
    )
    text: Mapped[Optional[str]] = mapped_column(
        String, doc="The text of the query in the original format.",
        default=None,
    )
    description: Mapped[Optional[str]] = mapped_column(
        String, doc="Query description.",
        default=None,
    )
    
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
        token: str = None,
        ws_id: str = None,
        req_id: str = None,
        **kwargs,        
    ) -> T:
        """
        Create a new highlight object

        Args:
            id: the id of the highlight
            owner: the owner of the highlight
            operation: the operation associated with the highlight
            time_range: the time range of the highlight
            log_file_path: the log file path associated with the highlight
            glyph: the glyph associated with the highlight
            tags: the tags associated with the highlight
            title: the title of the highlight
            private: whether the highlight is private
            token: the token of the user
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
            owner,
            token=token,            
            ws_id=ws_id,
            req_id=req_id,
            **args,
        )
