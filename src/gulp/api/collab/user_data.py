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
    @classmethod
    async def create(
        cls,
        id: str,
        user: str | "GulpUser",
        data: dict,
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
        commit: bool = True,
        **kwargs,
    ) -> T:
        args = {
            "data": data,
        }
        return await super()._create(
            id,
            user,
            ws_id,
            req_id,
            sess,
            commit,
            **args,
        )
