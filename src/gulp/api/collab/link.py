from typing import override
from sqlalchemy import ForeignKey, String, JSONB
from sqlalchemy.orm import Mapped, mapped_column
from gulp.api.collab.structs import (
    GulpCollabObject,
)
from gulp.api.elastic.structs import GulpAssociatedDocument, GulpDocument
from gulp.utils import logger


class GulpLink(GulpCollabObject):
    """
    a link in the gulp collaboration system
    """

    __tablename__ = "link"

    # the source event
    document_from: Mapped[str] = mapped_column(String, doc="The source document.")
    # target events
    documents: Mapped[list[GulpAssociatedDocument]] = mapped_column(
        JSONB, doc="One or more target documents."
    )

    __mapper_args__ = {
        "polymorphic_identity": "link",
    }

    @override
    def __init__(
        self,
        id: str,
        user: str,
        operation: str,
        document_from: str,
        documents: list[GulpDocument],
        **kwargs,
    ) -> None:
        """
        Initialize a GulpLink object.
        Args:
            id (str): The unique identifier for the link.
            user (str): The user associated with the link.
            operation (str): The operation type for the link.
            document_from (str): The source document for the link.
            documents (list[GulpDocument]): A list of GulpDocument objects associated with the link.
            **kwargs: Additional keyword arguments passed to the GulpCollabObject initializer.
        Returns:
            None
        """
        super().__init__(id, GulpCollabObject.LINK, user, operation, **kwargs)
        self.document_from = document_from
        self.documents = documents
        logger().debug(
            "---> GulpLink: document_from=%s, documents=%s" % (document_from, documents)
        )
