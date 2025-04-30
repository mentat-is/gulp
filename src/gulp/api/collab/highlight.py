"""
This module defines the GulpHighlight class, which represents highlights in the gulp collaboration system.

Highlights are time-based annotations associated with a gulp source, allowing users to mark specific
temporal sections of content for collaboration purposes.

The module provides:
- GulpHighlight: A class for creating, storing, and manipulating highlight objects
- Integration with SQLAlchemy for database persistence
- Support for time ranges specified in nanoseconds
- Optional association with source objects through foreign key relationships
"""

from typing import Optional

from sqlalchemy import ARRAY, BIGINT, ForeignKey
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.orm import Mapped, mapped_column

from gulp.api.collab.structs import COLLABTYPE_HIGHLIGHT, GulpCollabObject


class GulpHighlight(GulpCollabObject, type=COLLABTYPE_HIGHLIGHT):
    """
    an highlight in the gulp collaboration system
    """

    time_range: Mapped[tuple[int, int]] = mapped_column(
        MutableList.as_mutable(ARRAY(BIGINT)),
        doc="The time range of the highlight, in nanoseconds from unix epoch.",
    )
    source_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("source.id", ondelete="CASCADE"), doc="The associated GulpSource id."
    )

    @classmethod
    def example(cls) -> dict:
        d = super().example()
        d["time_range"] = [0, 1000000]
        d["source_id"] = "source_id"
        return d
