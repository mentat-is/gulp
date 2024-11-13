from typing import Optional, Union, override
import muty.crypto
import muty.string
import muty.time
from sqlalchemy import BIGINT, ForeignKey
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column, relationship
from gulp.api.collab.structs import (
    GulpCollabFilter,
    GulpCollabBase,
    T,
    GulpCollabType,
    GulpUserPermission,
    MissingPermission,
    WrongUsernameOrPassword,
)

from gulp.utils import GulpLogger
from gulp.config import GulpConfig

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
        back_populates="session",
        foreign_keys=[user_id],
        single_parent=True,
        uselist=False,
        innerjoin=True,
    )
    time_expire: Mapped[Optional[int]] = mapped_column(
        BIGINT,
        default=0,
        doc="The time when the session expires, in milliseconds from unix epoch.",
    )

    @override
    def __init__(self, *args, **kwargs):
        # initializes the base class
        GulpLogger.get_instance().debug("---> GulpUserSession.__init__: args=%s, kwargs=%s ..." % (args, kwargs))
        super().__init__(*args,  type=GulpCollabType.USER_SESSION, **kwargs)

    @classmethod
    async def create(
        cls,
        *args, **kwargs,
    ) -> T:
        """
        uninmplemented, use GulpUser.login() to create a session.
        """
        raise NotImplementedError("use GulpUser.login() to create a session.")

    @staticmethod
    async def get_by_token(token: str, sess: AsyncSession = None) -> "GulpUserSession":
        """
        Asynchronously retrieves a logged user session by token.
        Args:
            token (str): The token of the user session to retrieve.
            sess (AsyncSession, optional): An optional asynchronous session object. Defaults to None.
        Returns:
            T: the user session object.
        Raises:
            ObjectNotFound: if the user session is not found.
        """
        GulpLogger.get_instance().debug("---> get_by_token: token=%s, sess=%s ..." % (token, sess))
        if GulpConfig.get_instance().debug_allow_any_token_as_admin():
            # return an admin session
            from gulp.api.collab.user import GulpUser

            # the "admin" user always exists
            admin_user: GulpUser = await GulpUser.get_one_by_id(
                id="admin", sess=sess, throw_if_not_found=False)
            if admin_user.session:
                # already exists
                GulpLogger.get_instance().debug(
                    "debug_allow_any_token_as_admin, reusing existing admin session: %s" % (admin_user.session)
                )
                return admin_user.session
            else:
                # create a new admin session
                admin_session: GulpUserSession = await GulpUserSession._create(
                    id=admin_user.id, user_id=admin_user.id, user=admin_user, ensure_eager_load=True
                )
                admin_user.session = admin_session
                GulpLogger.get_instance().debug(
                    "debug_allow_any_token_as_admin, created new admin session: %s" % (admin_session)
                )                
                return await admin_session

        # default, get a session if exists
        s:GulpUserSession = await GulpUserSession.get_one_by_id(id=token, sess=sess)
        return s

    @staticmethod
    async def check_token_permission(
        token: str,
        permission: list[GulpUserPermission] = [GulpUserPermission.READ],
        sess: AsyncSession = None,
        throw_on_no_permission: bool = True) -> "GulpUserSession":
        """
        Check if the user represented by token has the required permissions.

        Args:
            token (str): The token representing the user's session.
            permission (list[GulpUserPermission], optional): A list of required permissions. Defaults to [GulpUserPermission.READ].
            sess (AsyncSession, optional): The database session to use. Defaults to None.
            throw_on_no_permission (bool, optional): If True, raises an exception if the user does not have the required permissions. Defaults to True.
        
        Returns:
            GulpUserSession: The user session object.

        Raises:
            MissingPermission: If the user does not have the required permissions.
        """
        # get session
        GulpLogger.get_instance().debug("---> check_token_permission: token=%s, permission=%s, sess=%s ..." % (token, permission, sess))
        user_session: GulpUserSession = await GulpUserSession.get_by_token(token, sess=sess)
        GulpLogger.get_instance().debug("---> check_token_permission: user_session=%s ..." % (user_session))        
        
        from gulp.api.collab.user import GulpUser
        u: GulpUser = user_session.user
        if u.has_permission(permission):
            return user_session
        
        if throw_on_no_permission:
            raise MissingPermission(
                f"User {user_session.user_id} does not have the required permissions {permission} to perform this operation."
            )
        return None
            
