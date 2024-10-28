from typing import Optional, Union, override

import muty.crypto
import muty.time
import muty.string
from sqlalchemy import BIGINT, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship, declared_attr, remote
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.types import Enum as SQLEnum
from gulp.api.collab.structs import (
    GulpCollabBase,
    GulpCollabFilter,
    GulpCollabType,
    GulpUserPermission,
    T,
    WrongUsernameOrPassword,
)
from gulp.api.collab_api import session
from gulp.utils import logger
from gulp import config


class GulpUser(GulpCollabBase):
    """
    Represents a user in the system.
    """

    __tablename__ = GulpCollabType.USER.value
    pwd_hash: Mapped[str] = mapped_column(String)
    permission: Mapped[Optional[GulpUserPermission]] = mapped_column(
        SQLEnum(GulpUserPermission), default=GulpUserPermission.READ
    )
    glyph: Mapped[Optional[str]] = mapped_column(
        ForeignKey("glyph.id", ondelete="SET NULL"), default=None
    )
    email: Mapped[Optional[str]] = mapped_column(String, default=None)
    time_last_login: Mapped[Optional[int]] = mapped_column(BIGINT, default=0)
    session_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("session.id", ondelete="SET NULL"), default=None, unique=True
    )
    session: Mapped[Optional["GulpUserSession"]] = relationship(
        "GulpUserSession",
        back_populates="user",
        foreign_keys=[session_id],
        default=None,
    )

    user_data_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("user_data.id", ondelete="SET NULL"), default=None, unique=True
    )
    user_data: Mapped[Optional["GulpUserData"]] = relationship(
        "GulpUserData",
        back_populates="user",
        foreign_keys=[user_data_id],
        uselist=False,
        default=None,
    )
    __mapper_args__ = {
        "polymorphic_identity": GulpCollabType.USER.value,
    }

    @classmethod
    async def create(
        cls,
        id: str,
        password: str,
        permission: GulpUserPermission = GulpUserPermission.READ,
        owner: str = None,
        email: str = None,
        glyph: str = None,
        ws_id: str = None,
        req_id: str = None,
        **kwargs,
    ) -> T:
        args = {
            "pwd_hash": muty.crypto.hash_sha256(password),
            "permission": permission,
            "email": email,
            "glyph": glyph,
        }
        return await super()._create(
            id,
            GulpCollabType.USER,
            owner,
            ws_id,
            req_id,
            **args,
        )

    @override
    @classmethod
    async def update_by_id(
        cls,
        id: str,
        d: dict,
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
        throw_if_not_found: bool = True,
    ) -> T:
        # if d is a dict and have "password", hash it (password update)
        pwd_changed = False
        if isinstance(d, dict) and "password" in d:
            d["pwd_hash"] = muty.crypto.hash_sha256(d["password"])
            del d["password"]
            pwd_changed = True

        sess = await session()
        async with sess:
            user: GulpUser = await super().update_by_id(
                id,
                d,
                ws_id=ws_id,
                req_id=req_id,
                sess=sess,
                throw_if_not_found=throw_if_not_found,
            )
            if pwd_changed and user.session:
                # invalidate (delete) the session if the password was changed
                logger().debug(
                    "password changed, deleting session for user=%s" % (user.id)
                )
                sess.add(user.session)
                user.session_id = None
                user.session = None

            # commit in the end
            sess.add(user)
            await sess.commit()
            return user

    @override
    @classmethod
    async def delete_by_id(
        cls,
        id: str,
        ws_id: str = None,
        req_id: str = None,
        throw_if_not_found: bool = True,
        sess: AsyncSession = None,
    ) -> None:
        if id == "admin":
            raise ValueError("cannot delete the default admin user")
        await super().delete_by_id(id, ws_id, req_id, throw_if_not_found, sess)

    def is_admin(self) -> bool:
        """
        Check if the user has admin permission.

        Returns:
            bool: True if the user has admin permission, False otherwise.
        """
        return bool(self.permission & GulpUserPermission.ADMIN)

    def logged_in(self) -> bool:
        """
        check if the user is logged in

        Returns:
            bool: True if the user has an active session (logged in)
        """
        return self.session is not None

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
        from gulp.api.collab.user_session import GulpUserSession

        logger().debug("---> logging in user=%s ..." % (user))

        sess = await session()
        async with sess:
            user = await cls.get_one(
                GulpCollabFilter(id=[user], type=[GulpCollabType.USER]), sess
            )

            # check if user has a session already, if so invalidate
            existing_session: GulpUserSession = await GulpUserSession.get_by_user(
                user, sess, throw_if_not_found=False
            )
            if existing_session:
                # zero-out the session_id and session fields in the user, the ORM will take care of the rest
                logger().debug("deleting previous session for user=%s" % (user))
                user.session = None
                user.session_id = None
                sess.add(user)  # keep track of the change

            # check password
            if user.pwd_hash != muty.crypto.hash_sha256(password):
                raise WrongUsernameOrPassword("wrong password for user=%s" % (user))

            # create new session
            token = muty.string.generate_unique()
            new_session: GulpUserSession = await super()._create(
                token,
                GulpCollabType.SESSION,
                user,
                ws_id=ws_id,
                req_id=req_id,
                sess=sess,
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
            sess.add(new_session)
            await sess.commit()  # this will also delete the previous session from above, if needed
            return new_session

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
        from gulp.api.collab.user_session import GulpUserSession

        await GulpUserSession.delete_by_id(token, ws_id=ws_id, req_id=req_id)

    def has_permissions(self, permission: list[GulpUserPermission] | list[str]) -> bool:
        """
        Check if the user has the specified permission.

        Args:
            permission (list[GulpUserPermission] | list[str]): The permission(s) to check.

        Returns:
            bool: True if the user has the specified permissions, False otherwise.
        """
        if isinstance(permission[0], str):
            permission = [GulpUserPermission[p] for p in permission]

        if self.is_admin():
            return True

        return bool(self.permission & sum(permission))
