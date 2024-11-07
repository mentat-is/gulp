from typing import Union, override
from sqlalchemy import Index, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.collab.structs import GulpCollabObject, GulpCollabType, T
from gulp.api.opensearch.structs import GulpAssociatedDocument, GulpDocument
from gulp.utils import GulpLogger


class GulpLink(GulpCollabObject, type=GulpCollabType.LINK):
    """
    a link in the gulp collaboration system
    """

    # the source event
    document_from: Mapped[str] = mapped_column(String, doc="The source document.")
    # target events
    documents: Mapped[list[GulpAssociatedDocument]] = mapped_column(
        JSONB, doc="One or more target documents."
    )

    @classmethod
    async def create(
        cls,
        id: str,
        owner: str,
        operation: str,
        document_from: str,
        documents: list[GulpAssociatedDocument],
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
        Create a new link object

        Args:
            id(str): the id of the link
            owner(str): the owner of the link
            operation(str): the operation associated with the link
            document_from(str): the source document
            documents(list[GulpAssociatedDocument]): the target documents
            glyph(str): the glyph associated with the link
            tags(list[str]): the tags associated with the link
            title(str): the title of the link
            private(bool): whether the link is private
            token(str): the token of the user creating the object, for access check
            ws_id(str): the websocket id
            req_id(str): the request id
            kwargs: additional arguments
            
        Returns:
            the created link object
        """
        args = {
            "operation": operation,
            "document_from": document_from,
            "documents": documents,
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
