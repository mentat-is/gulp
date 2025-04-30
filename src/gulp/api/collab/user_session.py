"""
This module provides classes for handling user sessions in the Gulp application.

The `GulpUserSession` class represents a user session for a logged-in user, maintaining
the relationship between sessions and users, and providing mechanisms for session validation,
permission checking, and access control.

Key features include:
- Session management with expiration handling
- Permission-based access control for resources
- Special handling for admin sessions
- Token validation with configurable permission requirements

This module works in conjunction with the user management system to provide
authentication and authorization services throughout the application.
"""

from typing import TYPE_CHECKING, Optional, override

from muty.log import MutyLogger
from sqlalchemy import BIGINT, ForeignKey
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column, relationship

from gulp.api.collab.structs import (
    COLLABTYPE_USER_SESSION,
    GulpCollabBase,
    GulpUserPermission,
    MissingPermission,
    T,
)
from gulp.config import GulpConfig
from gulp.structs import ObjectNotFound

if TYPE_CHECKING:
    from gulp.api.collab.user import GulpUser


class GulpUserSession(GulpCollabBase, type=COLLABTYPE_USER_SESSION):
    """
    Represents a user session (logged user).
    """

    user_id: Mapped[str] = mapped_column(
        ForeignKey("user.id", ondelete="CASCADE"),
        doc="The user ID associated with the session.",
        unique=True,
    )

    user: Mapped["GulpUser"] = relationship(
        "GulpUser",
        foreign_keys=[user_id],
        uselist=False,
        lazy="joined",
    )
    time_expire: Mapped[Optional[int]] = mapped_column(
        BIGINT,
        default=0,
        doc="The time when the session expires, in milliseconds from unix epoch.",
    )

    @override
    @classmethod
    def example(cls) -> dict:
        d = super().example()
        d["user_id"] = "user_id"
        d["time_expire"] = 0
        return d

    @classmethod
    async def create(
        cls,
        *args,
        **kwargs,
    ) -> T:
        """
        uninmplemented, use GulpUser.login() to create a session.
        """
        raise NotImplementedError("use GulpUser.login() to create a session.")

    @staticmethod
    async def _get_admin_session(sess: AsyncSession) -> "GulpUserSession":
        """
        Get an admin session, for debugging purposes only

        Args:
            sess (AsyncSession): The database session to use.

        Returns:
            GulpUserSession: The admin session object.
        """
        try:
            from gulp.api.collab.user import GulpUser
            await GulpUserSession.acquire_advisory_lock(sess, "admin")

            # the "admin" user always exists
            admin_user: GulpUser = await GulpUser.get_by_id(sess, obj_id="admin")
            if admin_user.session:
                # already exists
                return admin_user.session

            # create a new permanent admin session
            object_data = {"user_id": admin_user.id, "time_expire": 0}
            admin_session: GulpUserSession = await GulpUserSession._create_internal(
                sess,
                object_data=object_data,
                owner_id=admin_user.id,
            )
            # MutyLogger.get_instance().debug("created new admin session: %s" % (admin_session.to_dict()))
            return admin_session
        finally:
            await GulpUserSession.release_advisory_lock(sess, "admin")

    @staticmethod
    async def check_token(
        sess: AsyncSession,
        token: str,
        permission: list[GulpUserPermission] | GulpUserPermission = None,
        obj: Optional[GulpCollabBase] = None,
        throw_on_no_permission: bool = True,
        enforce_owner: bool = False,
    ) -> "GulpUserSession":
        """
        Check if the user represented by token is logged in and has the required permissions.

        - if both permission and obj are None, the function will return the user session without checking permissions.
        - if user is an admin, the function will always grant access.
        - first, if permission is provided, the function will check if the user has the required permission/s.
        - then, if obj is provided, the function will check the user permissions against the object to access it.
            - check GulpUser.check_object_access() for details.

        Args:
            sess (AsyncSession, optional): The database session to use. Defaults to None.
            token (str): The token representing the user's session.
            permission (list[GulpUserPermission]|GulpUserPermission, optional): The permission(s) required to access the object. Defaults to None.
            obj (Optional[GulpCollabBase], optional): The object to check the permissions against, for access. Defaults to None.
            throw_on_no_permission (bool, optional): If True, raises an exception if the user does not have the required permissions (or if he's not logged on). Defaults to True.
            enforce_owner (bool, optional): If True, the user must be the owner of the object to access it (or administrator). Defaults to False.

        Returns:
            GulpUserSession: The user session object (includes GulpUser object).

        Raises:
            MissingPermission: If the user does not have the required permissions.
        """
        MutyLogger.get_instance().debug(
            "---> check_token_permission: token=%s, permission=%s, sess=%s ..."
            % (token, permission, sess)
        )
        if not permission:
            # assume read permission if not provided
            permission = [GulpUserPermission.READ]

        if isinstance(permission, GulpUserPermission):
            # allow single permission as string
            permission = [permission]

        if GulpConfig.get_instance().debug_allow_any_token_as_admin():
            return await GulpUserSession._get_admin_session(sess)

        try:
            user_session: GulpUserSession = await GulpUserSession.get_by_id(
                sess, obj_id=token, throw_if_not_found=throw_on_no_permission
            )
            # MutyLogger.get_instance().debug("got user session for token %s: %s" % (token, user_session.to_dict()))
        except ObjectNotFound as ex:
            raise MissingPermission('token "%s" not logged in' % (token)) from ex

        if user_session.user.is_admin():
            # admin user can access any object and always have permission
            return user_session

        if not obj:
            # check if the user have the required permission (owner always have permission)
            if user_session.user.has_permission(permission):
                # access granted
                return user_session

            if throw_on_no_permission:
                raise MissingPermission(
                    f"User {user_session.user_id} does not have the required permissions {permission} to perform this operation."
                )
            return None

        # check if the user has access
        if user_session.user.check_object_access(
            obj,
            throw_on_no_permission=throw_on_no_permission,
            enforce_owner=enforce_owner,
        ):
            # check if the user have the required permission (owner always have permission)
            if user_session.user.has_permission(permission) or obj.is_owner(
                user_session.user.id
            ):
                # access granted
                return user_session

        if throw_on_no_permission:
            raise MissingPermission(
                f"User {user_session.user_id} does not have the required permissions {permission} to perform this operation, obj={obj.id if obj else None}, obj_owner={obj.owner_user_id if obj else None}."
            )
        return None
