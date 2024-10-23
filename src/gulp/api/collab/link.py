from typing import Optional
from sqlalchemy import BIGINT, ForeignKey, Index, String, ARRAY, Boolean, JSONB
from sqlalchemy.orm import Mapped, mapped_column
from gulp.api.collab.structs import COLLAB_MAX_NAME_LENGTH, GulpCollabType
from gulp.api.collab_api import CollabBase


class CollabLink(CollabBase):
    """
    a link in the gulp collaboration system
    """

    __tablename__ = "link"
    id: Mapped[int] = mapped_column(ForeignKey("collab_obj.id"), primary_key=True)
    
    # the source event
    event_from: Mapped[Optional[str]] = mapped_column(String)
    # target events
    events: Mapped[Optional[list[dict]]] = mapped_column(JSONB)

    __mapper_args__ = {
        "polymorphic_identity": "link",
    }