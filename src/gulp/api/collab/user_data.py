from typing import override
from sqlalchemy import ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from gulp.api.collab.structs import (
    GulpCollabBase,
    GulpCollabType,
)
from gulp.utils import logger


class GulpUserData(GulpCollabBase):
    """
    defines data associated with an user
    """

    __tablename__ = "user_data"
    user = relationship("GulpUser", back_populates="user_data")
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
        super().__init__(id, GulpCollabType.USER_DATA, user)
        self.user = user
        self.data = data
        logger().debug("---> UserData: user=%s, data=%s" % (user, data))
