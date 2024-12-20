from typing import Optional

from sqlalchemy import ARRAY, ForeignKey, Integer
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.ext.mutable import MutableList
from gulp.api.collab.structs import GulpCollabObject, GulpCollabType


class GulpHighlight(GulpCollabObject, type=GulpCollabType.HIGHLIGHT):
    """
    an highlight in the gulp collaboration system
    """

    time_range: Mapped[tuple[int, int]] = mapped_column(
        MutableList.as_mutable(ARRAY(Integer)),
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
