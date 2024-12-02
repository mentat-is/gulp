from typing import Optional, override

from sqlalchemy import ForeignKey, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.structs import GulpCollabObject, GulpCollabType, T


class GulpHighlight(GulpCollabObject, type=GulpCollabType.HIGHLIGHT):
    """
    an highlight in the gulp collaboration system
    """

    time_range: Mapped[tuple[int, int]] = mapped_column(
        JSONB,
        doc="The time range of the highlight, in nanoseconds from unix epoch.",
    )
    source_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("source.id", ondelete="CASCADE"), doc="The associated GulpSource id."
    )

    # @classmethod
    # async def create(
    #     cls,
    #     sess: AsyncSession,
    #     user_id: str,
    #     operation_id: str,
    #     ws_id: str,
    #     req_id: str,
    #     time_range: tuple[int, int],
    #     source_id: str,
    #     glyph_id: str = None,
    #     color: str = None,
    #     tags: list[str] = None,
    #     name: str = None,
    #     description: str = None,
    #     private: bool = False,
    # ) -> T:
    #     """
    #     Create a new highlight object on the collab database.

    #     Args:
    #         sess (AsyncSession): The database session.
    #         user_id (str): The ID of the user creating the object.
    #         operation_id (str): The ID of the operation associated with the highlight.
    #         ws_id (str): The ID of the workspace associated with the highlight.
    #         req_id (str): The ID of the request associated with the highlight.
    #         time_range (tuple[int, int]): The time range of the highlight, in nanoseconds from unix epoch.
    #         source_id (str): The associated GulpSource id.
    #         glyph_id (str, optional): The associated GulpGlyph id. Defaults to None.
    #         color (str, optional): The color of the highlight. Defaults to "green"
    #         tags (list[str], optional): The tags associated with the highlight. Defaults to None.
    #         name (str, optional): The name of the highlight. Defaults to None.
    #         description (str, optional): The description of the highlight. Defaults to None.
    #         private (bool, optional): Whether the highlight is private or not. Defaults to False.
    #         **kwargs: Arbitrary keyword arguments.

    #     Returns:
    #         the created highlight object
    #     """
    #     object_data = {
    #         "operation_id": operation_id,
    #         "time_range": time_range,
    #         "source_id": source_id,
    #         "glyph_id": glyph_id,
    #         "color": color or "green",
    #         "tags": tags,
    #         "name": name,
    #         "description": description,
    #         "private": private,
    #     }

    #     return await super()._create(
    #         sess,
    #         object_data,
    #         owner_id=user_id,
    #         ws_id=ws_id,
    #         req_id=req_id,
    #     )
