from typing import Optional
from sqlalchemy import BIGINT, ForeignKey, Index, String, ARRAY, Boolean, JSONB
from sqlalchemy.orm import Mapped, mapped_column
from gulp.api.collab.structs import COLLAB_MAX_NAME_LENGTH, GulpCollabType
from gulp.api.collab_api import CollabBase


class CollabHighlight(CollabBase):
    """
    an highlight in the gulp collaboration system
    """

    __tablename__ = "highlight"
    
    id: Mapped[int] = mapped_column(ForeignKey("collab_obj.id"), primary_key=True)
    time_start: Mapped[int] = mapped_column(BIGINT)
    time_end: Mapped[int] = mapped_column(BIGINT)
    source: Mapped[Optional[str]] = mapped_column(String, default=None)

    __mapper_args__ = {
        "polymorphic_identity": "highlight",
    }    
