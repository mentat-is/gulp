from typing import Optional

from muty.log import MutyLogger
from sqlalchemy import ARRAY, Column, ForeignKey, String, Table
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.types import Enum as SQLEnum

from gulp.api.collab.structs import (
    GulpCollabBase,
    GulpCollabType,
    GulpUserPermission,
    T,
)


class GulpUserAssociations:
    # multiple users can be associated with a group
    table = Table(
        "user_associations",
        GulpCollabBase.metadata,
        Column("user_id", ForeignKey("user.id", ondelete="CASCADE"), primary_key=True),
        Column(
            "group_id",
            ForeignKey("user_group.id", ondelete="CASCADE"),
            primary_key=True,
        ),
    )


class GulpUserGroup(GulpCollabBase, type=GulpCollabType.USER_GROUP):
    """
    Represents an user group in the gulp system.
    """

    name: Mapped[Optional[str]] = mapped_column(String, doc="The name of the group.")
    users: Mapped[list["GulpUser"]] = relationship(
        "GulpUser",
        secondary=GulpUserAssociations.table,
        lazy="selectin",
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

    @classmethod
    async def create(
        cls,
        sess: AsyncSession,
        owner_id: str,
        name: str,
        permission: list[GulpUserPermission] = [GulpUserPermission.READ],
        glyph_id: str = None,
    ) -> T:
        """
        Creates a new user group.

        Args:
            sess (AsyncSession): The database session.
            owner_id (str): The ID of the user creating the group.
            name (str): The name of the group.
            permission (list[GulpUserPermission], optional): The permission of the user. Defaults to [GulpUserPermission.READ].
            glyph_id (str): The ID of the glyph associated with the group.

        Returns:
            The created user group.
        """

        object_data = {
            "name": name,
            "permission": permission,
            "glyph_id": glyph_id,
        }

        # autogenerate id
        return await super()._create(sess, object_data, owner_id=owner_id)

    async def add_user(self, sess: AsyncSession, user_id: str) -> None:
        """
        Adds a user to the group.

        Args:
            sess (AsyncSession): The session to use.
            user_id (str): The user id to add to the group.
        """
        from gulp.api.collab.user import GulpUser

        user = await GulpUser.get_by_id(sess, id=user_id)
        if user not in self.users:
            self.users.append(user)
            await sess.commit()
            await sess.refresh(user)
            MutyLogger.get_instance().info(
                "Adding user %s to group %s" % (user_id, self.id)
            )
        else:
            MutyLogger.get_instance().info(
                "User %s already in group %s" % (user_id, self.id)
            )

    async def remove_user(self, sess: AsyncSession, user_id: str) -> None:
        """
        Removes a user from the group.

        Args:
            sess (AsyncSession): The session to use.
            user_id (str): The user id to remove from the group.
        """
        from gulp.api.collab.user import GulpUser

        user = await GulpUser.get_by_id(sess, id=user_id)
        if user in self.users:
            self.users.remove(user)
            await sess.commit()
            await sess.refresh(self)
            MutyLogger.get_instance().info(
                "Removing user %s from group %s" % (user_id, self.id)
            )
        else:
            MutyLogger.get_instance().info(
                "User %s not in group %s" % (user_id, self.id)
            )

    def is_admin(self) -> bool:
        """
        Checks if the group is an admin group.

        Returns:
            bool: True if the group is an admin group, False otherwise.
        """
        return GulpUserPermission.ADMIN in self.permission

    def has_user(self, user_id: str) -> bool:
        """
        Checks if the group has a user.

        Args:
            sess (AsyncSession): The session to use.
            user_id (str): The user id to check.

        Returns:
            bool: True if the group has the user, False otherwise.
        """
        return any([u.id == user_id for u in self.users])

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
