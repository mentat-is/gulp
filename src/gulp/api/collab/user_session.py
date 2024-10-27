from typing import Optional, Union, override
import muty.crypto
import muty.string
import muty.time
from sqlalchemy import BIGINT, ForeignKey
from sqlalchemy import event
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
from gulp.defs import ObjectNotFound
from gulp.utils import logger


class GulpUserSession(GulpCollabBase):
    """
    Represents a user session (logged user).
    """

    __tablename__ = GulpCollabType.SESSION.value
    time_expire: Mapped[Optional[int]] = mapped_column(
        BIGINT,
        default=0,
        doc="The time when the session expires, in milliseconds from unix epoch.",
    )

    __mapper_args__ = {
        "polymorphic_identity": GulpCollabType.SESSION.value,
    }

    @classmethod
    async def __create(
        cls,
        user: Union[str, "GulpUser"],
        password: str,
        ws_id: str = None,
        req_id: str = None,
        **kwargs,
    ) -> T:

        sess = await session()
        async with sess:
            if isinstance(user, str):
                from gulp.api.collab.user import GulpUser

                user = await GulpUser.get_one(
                    GulpCollabFilter(id=[user], type=[GulpCollabType.USER]), sess
                )

            # check if user has a session already, if so invalidate
            existing_session = await cls.get_by_user(
                user, sess, throw_if_not_found=False
            )
            if existing_session:
                # delete previous session (ORM will take care of the relationship)
                logger().debug("deleting previous session for user=%s" % (user))
                existing_session.user.session = None
                existing_session.user.session_id = None
                sess.add(existing_session.user)

            # check password
            if user.pwd_hash != muty.crypto.hash_sha256(password):
                raise WrongUsernameOrPassword("wrong password for user=%s" % (user))

            # create new session
            token = muty.string.generate_unique()
            new_session: GulpUserSession = await super()._create(
                token,
                user,
                ws_id=ws_id,
                req_id=req_id,
                sess=sess,
                commit=False,
                **kwargs,
            )
            if config.debug_no_token_expiration():
                new_session.time_expire = 0
            else:
                if user.is_admin():
                    new_session.time_expire = (
                        muty.time.now_msec() + config.token_admin_ttl() * 1000
                    )
                else:
                    new_session.time_expire = (
                        muty.time.now_msec() + config.token_ttl() * 1000
                    )

            user.session_id = token
            user.session = new_session
            sess.add(user)
            await sess.commit()
            return new_session

    @classmethod
    async def login(
        cls, user: str, password: str, ws_id: str = None, req_id: str = None
    ) -> T:
        """
        Asynchronously logs in a user and creates a session (=obtain token).
        Args:
            user (str): The username of the user to log in.
            password (str): The password of the user to log in.
        Returns:
            T: the user session object.
        """

        logger().debug("---> logging in user=%s ..." % (user))
        return await cls.__create(user, password, ws_id=ws_id, req_id=req_id)

    @classmethod
    async def logout(cls, token: str, ws_id: str = None, req_id: str = None) -> None:
        """
        Logs out the specified user by deleting their session.
        Args:
            user (str): The username of the user to log out.
            sess (AsyncSession, optional): The asynchronous session to use for the operation (will be committed). Defaults to None.
        Returns:
            None
        """
        logger().debug("---> logging out token=%s ..." % (token))
        await cls.delete(token, ws_id=ws_id, req_id=req_id)

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
            c: GulpUser = await GulpUser.get_one_by_id(
                "admin", sess, throw_if_not_found=False
            )
            if c.session:
                # already exists
                logger().debug(
                    "debug_allow_any_token_as_admin, reusing existing admin session"
                )
                return c.session
            else:
                # create a new admin session
                token = muty.string.generate_unique()
                s = await super().create(token, c.user, sess)
                c.session_id = token
                c.session = s
                logger().debug(
                    "debug_allow_any_token_as_admin, created new admin session"
                )
                return await super().create(token, c.user, sess)

        c = GulpUserSession.get_one_by_id(token, sess)
        return c
