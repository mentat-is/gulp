from typing import Union, override
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

    __tablename__ = GulpCollabType.USER_DATA.value
    __mapper_args__ = {
        "polymorphic_identity": GulpCollabType.USER_DATA.value,
    }
    user_id: Mapped[str] = mapped_column(
        ForeignKey("user.id", ondelete="CASCADE"),
        doc="The user ID associated with this data.",
        unique=True,
    )
    user: Mapped["GulpUser"] = relationship(
        "GulpUser",
        back_populates="user_data",
        foreign_keys="[GulpUser.user_data_id]",
        cascade="all,delete-orphan",
        single_parent=True,
        uselist=False,
    )
    data: Mapped[dict] = mapped_column(
        JSONB, doc="The data to be associated with user."
    )

    @override
    @classmethod
    async def create(
        cls,
        id: str,
        owner: str,
        data: dict,
        ws_id: str = None,
        req_id: str = None,
        **kwargs,
    ) -> T:
        args = {
            "data": data,
        }
        return await super()._create(
            id,
            GulpCollabType.USER_DATA,
            owner,
            ws_id,
            req_id,
            **args,
        )
