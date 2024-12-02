from typing import Optional

from sqlalchemy import ForeignKey, PrimaryKeyConstraint, String
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.collab.structs import (
    GulpCollabBase,
    GulpCollabType,
    T,
)


class GulpSource(GulpCollabBase, type=GulpCollabType.SOURCE):
    """
    Represents a source of data being processed by the gulp system.

    it has always associated a context and an operation, and the tuple composed by the three is unique.
    """

    operation_id: Mapped[str] = mapped_column(
        ForeignKey("operation.id", ondelete="CASCADE"),
        doc="The ID of the operation associated with the context.",
        primary_key=True,
    )
    context_id: Mapped[str] = mapped_column(
        ForeignKey("context.id", ondelete="CASCADE"),
        doc="The ID of the context associated with this source.",
        primary_key=True,
    )
    name: Mapped[str] = mapped_column(
        String, doc="The name of the source (i.e. log file name/path)."
    )
    color: Mapped[Optional[str]] = mapped_column(
        String, default="purple", doc="The color of the context."
    )
    glyph_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("glyph.id", ondelete="SET NULL"),
        default=None,
        doc="The glyph associated with the context.",
    )

    # composite primary key and contraints for operation_id and context_id (a source is unique for each operation and context)
    # __table_args__ = (PrimaryKeyConstraint("operation_id", "context_id", "id"),)
