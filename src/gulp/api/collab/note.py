from typing import Optional
from sqlalchemy import BIGINT, ForeignKey, Index, String, ARRAY, Boolean, JSONB
from sqlalchemy.orm import Mapped, mapped_column
from gulp.api.collab.base import GulpCollabObject
from gulp.api.collab.structs import COLLAB_MAX_NAME_LENGTH, GulpCollabType
from gulp.api.collab_api import CollabBase


class GulpCollabNote(GulpCollabObject):
    """
    a note in the gulp collaboration system
    """

    __tablename__ = "note"
    
    # the following must be refactored for specific tables
    id: Mapped[int] = mapped_column(ForeignKey("collab_obj.id"), primary_key=True)
    context: Mapped[Optional[str]] = mapped_column(
        ForeignKey("context.id", ondelete="CASCADE"), default=None
    )
    source: Mapped[Optional[str]] = mapped_column(String, default=None)
    events: Mapped[Optional[list[dict]]] = mapped_column(JSONB, default=None)
    text: Mapped[Optional[str]] = mapped_column(String, default=None)

    __mapper_args__ = {
        "polymorphic_identity": "note",
    }    
