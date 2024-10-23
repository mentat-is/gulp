from typing import Optional
from sqlalchemy import BIGINT, ForeignKey, Index, String, ARRAY, Boolean, JSONB
from sqlalchemy.orm import Mapped, mapped_column
from gulp.api.collab.structs import COLLAB_MAX_NAME_LENGTH, GulpCollabType
from gulp.api.collab_api import CollabBase


class CollabStory(CollabBase):
    """
    a story in the gulp collaboration system
    """

    __tablename__ = "story"
    
    # the following must be refactored for specific tables
    id: Mapped[int] = mapped_column(ForeignKey("collab_obj.id"), primary_key=True)
    events: Mapped[Optional[list[dict]]] = mapped_column(JSONB)
    text: Mapped[Optional[str]] = mapped_column(String, default=None)

    __mapper_args__ = {
        "polymorphic_identity": "story",
    }    
