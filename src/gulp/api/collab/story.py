from typing import Optional, override
from sqlalchemy import String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from gulp.api.collab.structs import GulpCollabType, GulpCollabObject, T
from gulp.api.opensearch.structs import GulpAssociatedDocument


class GulpStory(GulpCollabObject, type=GulpCollabType.STORY):
    """
    a story in the gulp collaboration system
    """

    documents: Mapped[list[GulpAssociatedDocument]] = mapped_column(
        JSONB, doc="One or more events associated with the story."
    )
    text: Mapped[Optional[str]] = mapped_column(
        String, default=None, doc="The description of the story."
    )

    @classmethod
    async def create(
        cls,
        id: str,
        owner: str,
        operation: str,
        documents: list[GulpAssociatedDocument],
        text: str = None,
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
        Create a new story object

        Args:
            id(str): the id of the story
            owner(str): the owner of the story
            operation(str): the operation associated with the story
            documents(list[GulpAssociatedDocument]): the events associated with the story
            text(str): the description of the story
            glyph(str): the glyph associated with the story
            tags(list[str]): the tags associated with the story
            title(str): the title of the story
            private(bool): whether the story is private
            token(str): the token of the user creating the object, for access check
            ws_id(str): the websocket id
            req_id(str): the request id
            kwargs: additional arguments
        
        Returns:
            the created story object    
        """
        args = {
            "operation": operation,
            "documents": documents,
            "glyph": glyph,
            "tags": tags,
            "title": title,
            "text": text,
            "private": private,
        }
        return await super()._create(
            id,
            owner,
            token=token,
            ws_id=ws_id,
            req_id=req_id,
            **args,
        )
