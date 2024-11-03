from typing import Optional, Union, override
import muty.crypto
import muty.string
import muty.time
from sqlalchemy import BIGINT, ForeignKey
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column, relationship
from gulp.api.collab_api import session
from gulp.api.collab.structs import (
    GulpCollabFilter,
    GulpCollabBase,
    T,
    GulpCollabType,
    WrongUsernameOrPassword,
)
import gulp.config as config
from gulp.utils import logger


class GulpUserSession(GulpCollabBase, type=GulpCollabType.SESSION):
    """
    Represents a user session (logged user).
    """

    user_id: Mapped[str] = mapped_column(
        ForeignKey("user.id"),
        doc="The user ID associated with the session.",
        unique=True,
    )

    user: Mapped["GulpUser"] = relationship(
        "GulpUser",
        back_populates="session",
        foreign_keys="[GulpUser.session_id]",
        single_parent=True,
        uselist=False,
    )
    time_expire: Mapped[Optional[int]] = mapped_column(
        BIGINT,
        default=0,
        doc="The time when the session expires, in milliseconds from unix epoch.",
    )

    @classmethod
    async def create(
        cls,
        id: str,
        owner: str,
        token: str = None,
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
        **kwargs,
    ) -> T:
        raise NotImplementedError("use GulpUser.login() to create a session.")

    @classmethod
    async def get_by_user(
        cls,
        user: Union[str, "GulpUser"],
        sess: AsyncSession = None,
        throw_if_not_found: bool = True,
    ) -> T:
        """
        Asynchronously retrieves a logged user session by user.
        Args:
            user (str | GulpUser): The user object or username of the user session to retrieve.
            sess (AsyncSession, optional): An optional asynchronous session object. Defaults to None.
            throw_if_not_found (bool, optional): Whether to raise an exception if the user session is not found. Defaults to True.
        Returns:
            T: the user session object.
        Raises:
            ObjectNotFound: if the user session is not found.
        """
        if isinstance(user, str):
            from gulp.api.collab.user import GulpUser

            return await GulpUser.get_one_by_id(user, sess, throw_if_not_found)
        return user.session

    @classmethod
    async def get_by_token(cls, token: str, sess: AsyncSession = None) -> T:
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
        if config.debug_allow_any_token_as_admin():
            # return an admin session
            from gulp.api.collab.user import GulpUser

            # the "admin" user always exists
            admin_user: GulpUser = await GulpUser.get_one_by_id(
                "admin", sess, throw_if_not_found=False
            )
            if admin_user.session_id:
                # already exists
                logger().debug(
                    "debug_allow_any_token_as_admin, reusing existing admin session"
                )
                return admin_user.session
            else:
                # create a new admin session
                token = muty.string.generate_unique()
                admin_session = await GulpUserSession._create(
                    token, admin_user.id, user_id=admin_user.id, user=admin_user, 
                )
                admin_user.session_id = token
                admin_user.session = admin_session
                logger().debug(
                    "debug_allow_any_token_as_admin, created new admin session"
                )
                return admin_session

        # default, get if exists
        s = GulpUserSession.get_one_by_id(token, sess)
        return s
