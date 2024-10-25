from typing import override
from sqlalchemy import ForeignKey, String, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio.engine import AsyncEngine
from sqlalchemy.orm import Mapped, mapped_column

from gulp.api.collab.base import CollabBase, GulpCollabFilter
from gulp.api.collab.user import User
from gulp.api.collab.structs import COLLAB_MAX_NAME_LENGTH, GulpCollabType
from gulp.defs import ObjectAlreadyExists, ObjectNotFound
from gulp.utils import logger


class UserData(CollabBase):
    """
    defines data associated with an user
    """

    __tablename__ = "user_data"
    id: Mapped[str] = mapped_column(
        ForeignKey("collab_base.id"),
        primary_key=True,
        doc="The unique identifier (name) of the user data.",
    )
    user: Mapped[str] = mapped_column(
        ForeignKey("user.id", ondelete="CASCADE"),
        doc="The user associated with the data.",
    )
    data: Mapped[dict] = mapped_column(JSONB, doc="The data.")

    __mapper_args__ = {
        "polymorphic_identity": "user_data",
    }

    @override
    def __init__(self, id: str, user: str, data: dict) -> None:
        """
        Initialize a UserData instance.

        Args:
            id (str): The identifier for the user data.
            user (str): The user associated with the data.
            data (dict): The data.
        """
        super().__init__(id, GulpCollabType.USER_DATA)
        self.user = user
        self.data = data
        logger().debug("---> UserData: user=%s, data=%s" % (user, data))
