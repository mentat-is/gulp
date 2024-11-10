from typing import Union, override
from sqlalchemy import ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column, relationship

from gulp.api.collab.structs import GulpCollabBase, GulpCollabFilter, GulpCollabType, T
from gulp.utils import GulpLogger


class GulpUserData(GulpCollabBase, type=GulpCollabType.USER_DATA):
    """
    defines data associated with an user
    """

    user_id: Mapped[str] = mapped_column(
        ForeignKey("user.id", ondelete="CASCADE"),
        doc="The user ID associated with this data.",
        unique=True,
    )
    user: Mapped["GulpUser"] = relationship(
        "GulpUser",
        back_populates="user_data",
        foreign_keys=[user_id],
        single_parent=True,
        uselist=False,
        innerjoin=True,
    )
    data: Mapped[dict] = mapped_column(
        JSONB, doc="The data to be associated with user."
    )

    @override
    def __init__(self, *args, **kwargs):
        # initializes the base class
        super().__init__(*args,  type=GulpCollabType.USER_DATA, **kwargs)

    @classmethod    
    async def create(
        cls,
        token: str,
        data: dict,
        ws_id: str = None,
        req_id: str = None,
        **kwargs,
    ) -> T:
        """
        Asynchronously creates a new user data entry.
        Args:
            token (str): The authentication token, for permission check.
            data (dict): The data to be stored in the user data entry.
            ws_id (str, optional): The websocket ID. Defaults to None.
            req_id (str, optional): The request ID. Defaults to None.
            **kwargs: Additional keyword arguments.
        Returns:
            T: The created user data entry.
        """        
        args = {
            "data": data,
        }
        return await super()._create(
            # id is automatically generated
            token=token,
            ws_id=ws_id,
            req_id=req_id,
            **args,
        )
