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
    user = relationship("GulpUser", back_populates="user_data")
    data: Mapped[dict] = mapped_column(
        JSONB, doc="The data to be associated with user."
    )

    __mapper_args__ = {
        f"polymorphic_identity": {GulpCollabType.USER_DATA},
    }

    @override
    def __init__(self, id: str, user: "GulpUser", data: dict) -> None:
        """
        Initialize a UserData instance.

        Args:
            id (str): The identifier for the user data.
            user (GulpUser): The user associated with the data.
            data (dict): The data.
        """
        super().__init__(id, GulpCollabType.USER_DATA, user.id)
        self.user = user
        self.data = data
        logger().debug("---> UserData: user=%s, data=%s" % (user, data))

    @classmethod
    @override
    async def create(
        cls,
        id: str,
        user: str | "GulpUser",
        sess: AsyncSession = None,
        commit: bool = True,
        **kwargs,
    ) -> T:
        if isinstance(user, str):
            from gulp.api.collab.user import GulpUser

            user = await GulpUser.get_one(GulpCollabFilter(id=[user]), sess)
        return await super().create(id, user, sess, commit, **kwargs)
