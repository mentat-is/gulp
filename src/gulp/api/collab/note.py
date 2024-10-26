from typing import Optional, override
from sqlalchemy import ForeignKey, String, JSONB
from sqlalchemy.orm import Mapped, mapped_column
from gulp.api.collab.structs import (
    GulpCollabObject,
    GulpCollabType,
)
from gulp.api.elastic.structs import GulpAssociatedDocument, GulpDocument
from gulp.utils import logger


class GulpNote(GulpCollabObject):
    """
    a note in the gulp collaboration system
    """

    __tablename__ = GulpCollabType.NOTE

    context: Mapped[str] = mapped_column(
        ForeignKey("context.id", ondelete="CASCADE"),
        doc="The context associated with the note.",
    )
    log_file_path: Mapped[str] = mapped_column(
        String, doc="The log file path associated with the note."
    )
    documents: Mapped[Optional[list[GulpAssociatedDocument]]] = mapped_column(
        JSONB, doc="One or more documents associated with the note."
    )
    text: Mapped[str] = mapped_column(String, doc="The text of the note.")

    __mapper_args__ = {
        f"polymorphic_identity": {GulpCollabType.NOTE},
    }

    @override
    def __init__(
        self,
        id: str,
        user: str,
        operation: str,
        context: str,
        log_file_path: str,
        documents: list[GulpDocument],
        text: str,
        **kwargs,
    ) -> None:
        """
        Initialize a GulpNote instance.
        Args:
            id (str): The unique identifier for the note.
            user (str): The user associated with the note.
            operation (str): The operation performed on the note.
            context (str): The context in which the note is created.
            log_file_path (str): The path to the log file.
            documents (list[GulpDocument]): A list of associated GulpDocument objects.
            text (str): The text content of the note.
            **kwargs: Additional keyword arguments passed to the GulpCollabObject initializer.
        """
        super().__init__(id, GulpCollabType.NOTE, user, operation, **kwargs)
        self.context = context
        self.log_file_path = log_file_path
        self.documents = documents
        self.text = text
        logger().debug(
            "---> GulpNote: context=%s, log_file_path=%s, documents=%s, text=%s"
            % (context, log_file_path, documents, text)
        )
