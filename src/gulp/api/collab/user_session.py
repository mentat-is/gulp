from typing import TYPE_CHECKING, Optional, override

import muty.string
import muty.time
from muty.log import MutyLogger
from sqlalchemy import BIGINT, ForeignKey, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column, relationship, selectinload, joinedload

from gulp.api.collab.structs import (
    GulpCollabBase,
    GulpCollabType,
    GulpUserPermission,
    MissingPermission,
    T,
)
from gulp.config import GulpConfig
from gulp.structs import ObjectNotFound

if TYPE_CHECKING:
    from gulp.api.collab.user import GulpUser


class GulpUserSession(GulpCollabBase, type=GulpCollabType.USER_SESSION):
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
        from gulp.api.collab.user import GulpUser

        # the "admin" user always exists
        admin_user: GulpUser = await GulpUser.get_by_id(sess, id="admin")
        if admin_user.session:
            # already exists
            return admin_user.session
        else:
            # create a new admin session
            object_data = {"user_id": admin_user.id, "time_expire": 0}
            admin_session: GulpUserSession = await GulpUserSession._create(
                sess,
                object_data=object_data,
                owner_id=admin_user.id,
            )
            MutyLogger.get_instance().warning(
                "created new admin session: %s" % (admin_session.to_dict())
            )
            return admin_session

    @staticmethod
    async def check_token(
        sess: AsyncSession,
        token: str,
        permission: list[GulpUserPermission] | GulpUserPermission = None,
        obj: Optional[GulpCollabBase] = None,
        throw_on_no_permission: bool = True,
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
            throw_on_no_permission (bool, optional): If True, raises an exception if the user does not have the required permissions. Defaults to True.

        Returns:
            GulpUserSession: The user session object (includes GulpUser object).

        Raises:
            MissingPermission: If the user does not have the required permissions.
        """
        # MutyLogger.get_instance().debug("---> check_token_permission: token=%s, permission=%s, sess=%s ..." % (token, permission, sess))
        if isinstance(permission, GulpUserPermission):
            # allow single permission as string
            permission = [permission]

        if GulpConfig.get_instance().debug_allow_any_token_as_admin():
            return await GulpUserSession._get_admin_session(sess)

        try:
            user_session: GulpUserSession = await GulpUserSession.get_by_id(
                sess, id=token, throw_if_not_found=throw_on_no_permission
            )
        except ObjectNotFound as ex:
            raise ObjectNotFound('token "%s" not logged in' % (token))

        if not obj and not permission:
            # no permission or object provided, just return the session
            return user_session

        if user_session.user.is_admin():
            # admin user has all permissions, always allowed
            return user_session

        granted = False

        # check if the user have the required permission first
        if user_session.user.has_permission(permission):
            granted = True

        if obj:
            # check the user permissions against the object
            if user_session.user.check_object_access(
                obj,
                throw_on_no_permission=throw_on_no_permission,
            ):
                granted = True
            else:
                # user cannot access the object
                granted = False

        if granted:
            return user_session

        if throw_on_no_permission:
            raise MissingPermission(
                f"User {user_session.user_id} does not have the required permissions {permission} to perform this operation, obj={obj.id if obj else None}."
            )
        return None
