"""
Module for user group management in the Gulp system.

This module provides classes for managing user groups, their associations with users,
and permission handling. It includes the GulpUserGroup class, which represents a user
group with functionality to add/remove users and check permissions.

The module defines:
- GulpUserAssociations: Handles the many-to-many relationship between users and groups
- GulpUserGroup: Represents a user group with associated users and permissions
- ADMINISTRATORS_GROUP_ID: Constant identifier for the administrators group (which is automatically created)

User groups can have various permissions defined in GulpUserPermission, and users can
be added to or removed from groups. The module supports checking if a user is part of a
group and if a group has specific permissions.

"""

from typing import TYPE_CHECKING, Optional, override

from muty.log import MutyLogger
from sqlalchemy import ARRAY, Column, ForeignKey, Table
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.types import Enum as SQLEnum

from gulp.api.collab.structs import COLLABTYPE_USER_GROUP, GulpCollabBase, GulpUserPermission
from gulp.structs import ObjectAlreadyExists, ObjectNotFound

if TYPE_CHECKING:
    from gulp.api.collab.user import GulpUser


ADMINISTRATORS_GROUP_ID = "administrators"


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


class GulpUserGroup(GulpCollabBase, type=COLLABTYPE_USER_GROUP):
    """
    Represents an user group in the gulp system.
    """

    users: Mapped[list["GulpUser"]] = relationship(
        "GulpUser",
        secondary=GulpUserAssociations.table,
        lazy="selectin",
    )
    permission: Mapped[Optional[list[GulpUserPermission]]] = mapped_column(
        MutableList.as_mutable(ARRAY(SQLEnum(GulpUserPermission))),
        default_factory=lambda: [GulpUserPermission.READ],
        doc="One or more permissions of the user.",
    )

    @override
    @classmethod
    def example(cls) -> dict:
        d = super().example()
        d["permission"] = [GulpUserPermission.READ]
        return d

    @classmethod
    def example_nested(cls) -> dict:
        from gulp.api.collab.user import GulpUser

        d = cls.example()
        d["users"] = [GulpUser.example()]
        return d

    async def add_user(
        self, sess: AsyncSession, user_id: str, raise_if_already_exists: bool = True
    ) -> None:
        """
        Adds a user to the group.

        Args:
            sess (AsyncSession): The session to use (it will be committed)
            user_id (str): The user id to add to the group.
            raise_if_already_exists (bool): If True, raises an error if the user is already in the group.
        """
        from gulp.api.collab.user import GulpUser

        try:
            await self.__class__.acquire_advisory_lock(sess, self.id)
            user = await GulpUser.get_by_id(sess, obj_id=user_id)
            existing_users = [u.id for u in self.users]
            if user_id not in existing_users:
                # add user to the group
                self.users.append(user)
                await sess.commit()
                MutyLogger.get_instance().info(
                    "adding user %s to group %s" % (user_id, self.id)
                )
            else:
                MutyLogger.get_instance().info(
                    "user %s already in group %s" % (user_id, self.id)
                )
                if raise_if_already_exists:
                    raise ObjectAlreadyExists(
                        "user %s already in group %s" % (user_id, self.id)
                    )
        except Exception as e:
            await sess.rollback()
            raise e

    async def remove_user(
        self, sess: AsyncSession, user_id: str, raise_if_not_found: bool = True
    ) -> None:
        """
        Removes a user from the group.

        Args:
            sess (AsyncSession): The session to use.
            user_id (str): The user id to remove from the group.
            raise_if_not_found (bool): If True, raises an error if the user is not in the group.
        """
        from gulp.api.collab.user import GulpUser
        try:
            await self.__class__.acquire_advisory_lock(sess, self.id)
            user = await GulpUser.get_by_id(sess, obj_id=user_id)
            existing_users = [u.id for u in self.users]
            if user_id in existing_users:
                # remove user from the group
                self.users.remove(user)
                await sess.commit()
                MutyLogger.get_instance().info(
                    "removing user %s from group %s" % (user_id, self.id)
                )
            else:
                MutyLogger.get_instance().info(
                    "user %s not in group %s" % (user_id, self.id)
                )
                if raise_if_not_found:
                    raise ObjectNotFound("User %s not in group %s" % (user_id, self.id))
        except Exception as e:
            await sess.rollback()
            raise e

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
