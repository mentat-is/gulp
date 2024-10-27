from typing import override
from sqlalchemy import ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column, relationship

from gulp.api.collab.structs import GulpCollabBase, GulpCollabFilter, GulpCollabType, T
from gulp.utils import logger


class GulpUserData(GulpCollabBase):
    """
    defines data associated with an user
    """

    __tablename__ = GulpCollabType.USER_DATA
    __mapper_args__ = {
        f"polymorphic_identity": {GulpCollabType.USER_DATA},
    }
    data: Mapped[dict] = mapped_column(
        JSONB, doc="The data to be associated with user."
    )

    @override
    def _init(self, id: str, user: "GulpUser", data: dict, **kwargs) -> None:
        """
        Initialize a UserData instance.

        Args:
            id (str): The identifier for the user data.
            user (GulpUser): The user associated with the data.
            data (dict): The data.
            **kwargs: Additional keyword arguments.
        """
        super().__init__(id, GulpCollabType.USER_DATA, user)
        self.data = data
        logger().debug("---> UserData: user=%s, data=%s" % (user, data))
