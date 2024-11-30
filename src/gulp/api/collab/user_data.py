from sqlalchemy import ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from gulp.api.collab.structs import GulpCollabBase, GulpCollabType, T


class GulpUserData(GulpCollabBase, type=GulpCollabType.USER_DATA):
    """
    defines data associated with an user
    """

    user_id: Mapped[str] = mapped_column(
        ForeignKey("user.id", ondelete="CASCADE"),
        doc="The user ID associated with this data.",
        unique=True,
    )
    data: Mapped[dict] = mapped_column(
        JSONB, doc="The data to be associated with user."
    )

    @classmethod
    async def create(
        cls,
        sess: AsyncSession,
        user_id: str,
        data: dict,
    ) -> T:
        """
        creates a data object associated with an user

        Args:
            sess (AsyncSession): The database session.
            user_id (str): The ID of the user associated with the data.
            data (dict): The data to be associated with the user.
        Returns:
            T: The created user data entry.
        """
        object_data = {
            "data": data,
        }
        return await super()._create(
            sess,
            object_data,
            owner_id=user_id,
        )
