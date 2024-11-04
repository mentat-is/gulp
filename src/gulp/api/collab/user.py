from typing import Optional, Union, override

import muty.crypto
import muty.time
import muty.string
from sqlalchemy import ARRAY, BIGINT, ForeignKey, String
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


class GulpUser(GulpCollabBase, type=GulpCollabType.USER):
    """
    Represents a user in the system.
    """

    pwd_hash: Mapped[str] = mapped_column(String)
    permission: Mapped[Optional[list[GulpUserPermission]]] = mapped_column(
        ARRAY(SQLEnum(GulpUserPermission)), default_factory=lambda: [GulpUserPermission.READ]
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
    @classmethod
    async def create(
        cls,        
        id: str,
        password: str,
        permission: list[GulpUserPermission] = [GulpUserPermission.READ],
        owner: str = None,
        email: str = None,
        glyph: str = None,
        token: str = None,
        ws_id: str = None,
        req_id: str = None,
        **kwargs,
    ) -> T:  
              
        if GulpUserPermission.READ not in permission:
            # ensure that all users have read permission
            permission.append(GulpUserPermission.READ)

        args = {
            "pwd_hash": muty.crypto.hash_sha256(password),
            "permission": permission,
            "email": email,
            "glyph": glyph,
        }
        owner = id
        return await super()._create(
            id,
            owner,
            token=token,
            required_permission=[GulpUserPermission.ADMIN],
            ws_id=ws_id,
            req_id=req_id,
            **args,
        )

    @override
    @classmethod
    async def update_by_id(
        cls,
        id: str,
        d: dict,
        token: str=None,
        permission: list[GulpUserPermission] = [GulpUserPermission.EDIT],
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
        throw_if_not_found: bool = True,
        **kwargs,
    ) -> T:
        check_permission_args={}
        if 'permission' in d:
            # changing permission, only admin can do it, standard users cannot change their own permission too
            if GulpUserPermission.READ not in d["permission"]:
                d["permission"].append(GulpUserPermission.READ)
            check_permission_args['allow_owner'] = False
        
        if 'password' in d:
            # only admin can change password to other users
            check_permission_args['permission'] = [GulpUserPermission.ADMIN]

        # check token
        GulpCollabBase.check_object_permission_by_id(id, token, **check_permission_args)

        # if d is a dict and have "password", hash it (password update)
        pwd_changed = False
        if "password" in d:
            d["pwd_hash"] = muty.crypto.hash_sha256(d["password"])
            del d["password"]
            pwd_changed = True

        # already checked token above, we can skip check here
        sess = await session()
        async with sess:
            user: GulpUser = await super().update_by_id(
                id,
                d,
                ws_id=ws_id,
                req_id=req_id,
                sess=sess,
                throw_if_not_found=throw_if_not_found,
                **kwargs,
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
        token: str = None,
        permission: list[GulpUserPermission] = [GulpUserPermission.DELETE],
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
        throw_if_not_found: bool = True,
    ) -> None:
        if id == "admin":
            raise ValueError("cannot delete the default admin user")
        await super().delete_by_id(id, token, permission, ws_id, req_id, sess, throw_if_not_found)

    def is_admin(self) -> bool:
        """
        Check if the user has admin permission.

        Returns:
            bool: True if the user has admin permission, False otherwise.
        """
        return GulpUserPermission.ADMIN in self.permission

    def logged_in(self) -> bool:
        """
        check if the user is logged in

        Returns:
            bool: True if the user has an active session (logged in)
        """
        return self.session is not None

    @staticmethod
    async def login(
        user: str, password: str, ws_id: str = None, req_id: str = None
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
            u: GulpUser = await GulpUser.get_one_by_id(user, sess=sess)
            # check if user has a session already, if so invalidate
            if u.session:
                logger().debug("resetting previous session for user=%s" % (user))
                u.session = None
                u.session_id = None
                sess.add(u)  # keep track of the change

            # check password
            if u.pwd_hash != muty.crypto.hash_sha256(password):
                raise WrongUsernameOrPassword("wrong password for user=%s" % (user))

            # create new session
            token = muty.string.generate_unique()
            new_session: GulpUserSession = await GulpUserSession._create(
                token,
                u.id,
                ws_id=ws_id,
                req_id=req_id,
                sess=sess,
                user_id=u.id,
                user=u,
            )
            if config.debug_no_token_expiration():
                new_session.time_expire = 0
            else:
                if u.is_admin():
                    new_session.time_expire = (
                        muty.time.now_msec() + config.token_admin_ttl() * 1000
                    )
                else:
                    new_session.time_expire = (
                        muty.time.now_msec() + config.token_ttl() * 1000
                    )

            u.session_id = token
            u.session = new_session
            u.time_last_login = muty.time.now_msec()
            sess.add(u)
            sess.add(new_session)
            await sess.commit()  # this will also delete the previous session from above, if needed
            return new_session

    @staticmethod
    async def logout(token: str, ws_id: str = None, req_id: str = None) -> None:
        """
        Logs out the specified user by deleting their session.
        Args:
            token (str): The token of the user to log out.
            ws_id (str, optional): The websocket ID. Defaults to None.
            req_id (str, optional): The request ID. Defaults to None.
        Returns:
            None
        """
        logger().debug("---> logging out token=%s ..." % (token))
        from gulp.api.collab.user_session import GulpUserSession

        await GulpUserSession.delete_by_id(token, ws_id=ws_id, req_id=req_id)

    def has_permission(self, permission: list[GulpUserPermission]) -> bool:
        """
        Check if the user has the specified permission.

        Args:
            permission (list[GulpUserPermission] | list[str]): The permission(s) to check.

        Returns:
            bool: True if the user has the specified permissions, False otherwise.
        """
        if self.is_admin():
            return True

        # check if all permissions are present
        return all([p in self.permission for p in permission])
