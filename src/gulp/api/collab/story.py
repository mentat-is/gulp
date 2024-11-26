
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from gulp.api.collab.structs import GulpCollabObject, GulpCollabType, T
from gulp.api.opensearch.structs import GulpBasicDocument


class GulpStory(GulpCollabObject, type=GulpCollabType.STORY):
    """
    a story in the gulp collaboration system
    """

    docs: Mapped[list[GulpBasicDocument]] = mapped_column(
        JSONB, doc="One or more documents associated with the story."
    )

    @classmethod
    async def create(
        cls,
        sess: AsyncSession,
        user_id: str,
        operation_id: str,
        ws_id: str,
        req_id: str,
        name: str,
        documents: list[GulpBasicDocument],
        color: str = None,
        description: str = None,
        glyph_id: str = None,
        tags: list[str] = None,
        private: bool = False,
    ) -> T:
        """
        Create a new story object on the collab database.

        Args:
            sess (AsyncSession): The database session.
            user_id (str): The ID of the user creating the object.
            operation_id (str): The ID of the operation associated with the story.
            ws_id (str): websocket id
            req_id (str): The ID of the request associated with the story.
            name (str): The display name for the story.
            documents (list[GulpBasicDocument]): The documents associated with the story.
            color (str): The color of the story.
            description (str): The description of the story.
            glyph_id (str): The ID of the glyph associated with the story.
            tags (list[str]): The tags associated with the story.
            private (bool): Whether the story is private.
        Returns:
            the created story object
        """
        object_data = {
            "operation": operation_id,
            "documents": documents,
            "glyph_id": glyph_id,
            "color": color or "blue",
            "tags": tags,
            "name": name,
            "description": description,
            "private": private,
        }
        # id is automatically generated
        return await super()._create(
            sess,
            object_data,
            user_id=user_id,
            ws_id=ws_id,
            req_id=req_id,
            operation_id=operation_id,
            private=private,
        )
