"""
gulp source representation module.

this module provides the `GulpSource` class, which represents a source of data
being processed by the gulp system. a source is always associated with an operation
and a context, forming a unique tuple.

the source entity is a fundamental part of the collaboration data model, linking
operations and contexts with the actual datasource, and providing additional metadata like color.
"""
from typing import Optional

from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.mutable import MutableDict
from gulp.api.collab.structs import GulpCollabBase, GulpCollabType


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
    color: Mapped[Optional[str]] = mapped_column(
        String, default="purple", doc="The color of the context."
    )
    plugin: Mapped[Optional[str]] = mapped_column(
        String,
        default=None,
        doc="plugin used for ingestion.",
    )
    plugin_params: Mapped[Optional[dict]] = mapped_column(
        MutableDict.as_mutable(JSONB),
        default_factory=dict,
        doc="plugin parameters used for ingestion (mapping, ...).",
    )
