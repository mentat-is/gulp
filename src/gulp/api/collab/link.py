from typing import Union, override
from sqlalchemy import Index, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.collab.structs import GulpCollabObject, GulpCollabType, T
from gulp.api.opensearch.structs import GulpBasicDocument, GulpDocument
from gulp.utils import GulpLogger


class GulpLink(GulpCollabObject, type=GulpCollabType.LINK):
    """
    a link in the gulp collaboration system
    """

    # the source event
    document_from: Mapped[str] = mapped_column(String, doc="The source document.")
    # target events
    documents: Mapped[list[GulpBasicDocument]] = mapped_column(
        JSONB, doc="One or more target documents."
    )

    @override
    def __init__(self, *args, **kwargs):
        # initializes the base class
        super().__init__(*args, type=GulpCollabType.LINK, **kwargs)

    @classmethod
    async def create(
        cls,
        token: str,
        operation: str,
        document_from: str,
        documents: list[GulpBasicDocument],
        glyph: str = None,
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
        Create a new link object on the collab database.

        Args:
            token(str): the token of the user creating the object, for access check
            operation(str): the id of the operation associated with the link
            document_from(str): the source document
            documents(list[GulpBasicDocument]): the target documents
            glyph(str, optional): the id of the glyph associated with the link
            color(str, optional): the color associated with the link (default: red)
            tags(list[str], optional): the tags associated with the link
            title(str, optional): the title of the link
            description(str, optional): the description of the link
            private(bool, optional): whether the link is private
            ws_id(str, optional): the websocket id
            req_id(str, optional): the request id

        Returns:
            the created link object
        """
        args = {
            "operation": operation,
            "document_from": document_from,
            "documents": documents,
            "glyph": glyph,
            "color": color or "red",
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
