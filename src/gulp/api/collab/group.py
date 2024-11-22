from typing import Optional, override

from sqlalchemy import ARRAY, ForeignKey, String
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.types import Enum as SQLEnum

from gulp.api.collab.structs import (
    GulpCollabBase,
    GulpCollabType,
    GulpUserPermission,
    T,
)
from gulp.api.collab.user import GulpUser
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab


class GulpUserGroup(GulpCollabBase, type=GulpCollabType.USER_GROUP):
    """
    Represents an user group in the gulp system.
    """

    title: Mapped[Optional[str]] = mapped_column(String, doc="The name of the group.")

    users: Mapped[Optional[list[GulpUser]]] = relationship(
        "GulpUser",
        back_populates="group",
        cascade="all, delete-orphan",
        foreign_keys="GulpUser.group_id",
        doc="The users associated with the group.",
    )
    
    glyph_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("glyph.id", ondelete="SET NULL"),
        doc="The glyph associated with the group.",
        default=None,
    )
    permission: Mapped[Optional[list[GulpUserPermission]]] = mapped_column(
        MutableList.as_mutable(ARRAY(SQLEnum(GulpUserPermission))),
        default_factory=lambda: [GulpUserPermission.READ],
        doc="One or more permissions of the user.",
    )

    @override
    def __init__(self, *args, **kwargs):
        # initializes the base class
        super().__init__(*args, type=GulpCollabType.USER_GROUP, **kwargs)

    @classmethod
    async def create(
        cls,
        token: str,
        name: str,
        permission: list[GulpUserPermission] = [GulpUserPermission.READ],
        glyph_id: str = None,
        ws_id: str = None,
        req_id: str = None,
        **kwargs,
    ) -> T:
        """
        Creates a new user group.

        Args:
            token (str): The token of the user creating the group.
            name (str): The name of the group.
            permission (list[GulpUserPermission]): The permission of the group.
            glyph_id (str, optional): The glyph id associated with the group.
            ws_id (str, optional): The websocket id
            req_id (str, optional): The request id
            kwargs: Additional arguments.

        Returns:
            The created user group.
        """
        # only admin can create a group
        await GulpUserSession.check_token_permission(token, [GulpUserPermission.ADMIN])
        if GulpUserPermission.READ not in permission:
            # ensure that all groups have read permission
            permission.append(GulpUserPermission.READ)

        args = {
            "title": name,
            "permission": permission,
            "glyph_id": glyph_id,
        }

        # autogenerate id
        return await super()._create(
            token=token,
            id=None,
            required_permission=[GulpUserPermission.ADMIN],
            ws_id=ws_id,
            req_id=req_id,
            ensure_eager_load=True,
            **args,
        )

    async def add_user(self, user_id: str):
        """
        Adds a user to the group.

        Args:
            user_id (str): The user id to add to the group.
        """
        async with GulpCollab.get_instance().session() as sess:
            user = await GulpUser.get_one_by_id(user_id, sess=sess)
            sess.add(user)
            user.group_id = self.id
            await sess.commit()

    async def remove_user(self, user_id: str):
        """
        Removes a user from the group.

        Args:
            user_id (str): The user id to remove from the group.
        """
        async with GulpCollab.get_instance().session() as sess:
            user = await GulpUser.get_one_by_id(user_id, sess=sess)
            sess.add(user)
            user.group_id = None
            await sess.commit()

    def is_admin(self) -> bool:
        """
        Checks if the group is an admin group.

        Returns:
            bool: True if the group is an admin group, False otherwise.
        """
        return GulpUserPermission.ADMIN in self.permission

    def has_permission(self, permission: list[GulpUserPermission]) -> bool:
        """
        Checks if the group has a permission.

        Args:
            permission (list[GulpUserPermission]): The permission/s to check

        Returns:
            bool: True if the group has the permission/s, False otherwise.
        """
        if GulpUserPermission.ADMIN in self.permission:
            return True

        granted = all([p in self.permission for p in permission])
        return granted
