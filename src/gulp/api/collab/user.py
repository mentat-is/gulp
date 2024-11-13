from typing import Optional, override

import muty.crypto
import muty.time
import muty.string
from sqlalchemy import ARRAY, BIGINT, ForeignKey, String, select
from sqlalchemy.orm import Mapped, mapped_column, relationship, joinedload
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.types import Enum as SQLEnum
from gulp.api.collab.structs import (
    GulpCollabBase,
    GulpCollabType,
    GulpUserPermission,
    T,
    WrongUsernameOrPassword,
)
from gulp.utils import GulpLogger
from gulp.config import GulpConfig
from gulp.api.collab_api import GulpCollab
class GulpUser(GulpCollabBase, type=GulpCollabType.USER):
    """
    Represents a user in the system.
    """

    pwd_hash: Mapped[str] = mapped_column(String)
    permission: Mapped[Optional[list[GulpUserPermission]]] = mapped_column(
         MutableList.as_mutable(ARRAY(SQLEnum(GulpUserPermission))), 
         default_factory=lambda: [GulpUserPermission.READ]
    )
    glyph: Mapped[Optional[str]] = mapped_column(
        ForeignKey("glyph.id", ondelete="SET NULL"), default=None
    )
    email: Mapped[Optional[str]] = mapped_column(String, default=None)
    time_last_login: Mapped[Optional[int]] = mapped_column(BIGINT, default=0)
    session: Mapped[Optional["GulpUserSession"]] = relationship(
        "GulpUserSession",
        back_populates="user",
        cascade="all,delete-orphan",
        default=None,
        foreign_keys="[GulpUserSession.user_id]",
    )

    user_data: Mapped[Optional["GulpUserData"]] = relationship(
        "GulpUserData",
        back_populates="user",
        cascade="all,delete-orphan",
        uselist=False,
        default=None,
        foreign_keys="[GulpUserData.user_id]",
    )   
    
    @override
    def __init__(self, *args, **kwargs):
        # initializes the base class
        super().__init__(*args, type=GulpCollabType.USER, **kwargs)

    @classmethod
    async def create(
        cls,        
        token: str,
        id: str,
        password: str,
        permission: list[GulpUserPermission] = [GulpUserPermission.READ],
        email: str = None,
        glyph: str = None,
        ws_id: str = None,
        req_id: str = None,
        **kwargs,
    ) -> T:  
        """
        Create a new user object on the collab database.

        Args:
            token: The token of the user creating the object, for permission check (needs ADMIN permission).
            id: The unique identifier of the user(=username).
            password: The password of the user.
            permission: The permission of the user.
            email: The email of the user.
            glyph: The glyph associated with the user.
            ws_id: The websocket id.
            req_id: The request id.            
            kwargs: Additional keyword arguments: "init" is used to create the default admin user and skip token check.

        Returns:
            The created user object.
        """
        if GulpUserPermission.READ not in permission:
            # ensure that all users have read permission
            permission.append(GulpUserPermission.READ)

        args = {
            "pwd_hash": muty.crypto.hash_sha256(password),
            "permission": permission,
            "email": email,
            "glyph": glyph,

            # force owner to the user itself
            "owner_id": id,
        }

        if "init" in kwargs:
            # "init" internal flag is used to create the default admin user and skip token check
            token=None
            kwargs.pop("init")

        return await super()._create(
            token=token,
            id=id,
            required_permission=[GulpUserPermission.ADMIN],
            ws_id=ws_id,
            req_id=req_id,
            ensure_eager_load=True,            
            **args,
        )

    @override
    @classmethod
    async def update_by_id(
        cls,
        token: str,
        id: str,
        d: dict,
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
            # so we set allow_owner to False explicitly here, so only admin can pass check_token_against_object_by_id
            if GulpUserPermission.READ not in d["permission"]:
                # ensure read permission is always present
                d["permission"].append(GulpUserPermission.READ)
            check_permission_args['allow_owner'] = False
        
        if 'password' in d:
            # only admin can change password to other users
            check_permission_args['permission'] = [GulpUserPermission.ADMIN]

        # if d is a dict and have "password", hash it (password update)
        pwd_changed = False
        if "password" in d:
            d["pwd_hash"] = muty.crypto.hash_sha256(d["password"])
            del d["password"]
            pwd_changed = True

        sess = GulpCollab.get_instance().session()
        async with sess:
            user: GulpUser = await super().update_by_id(
                token=token,
                id=id,
                d=d,
                ws_id=ws_id,
                req_id=req_id,
                sess=sess,
                throw_if_not_found=throw_if_not_found,
                **kwargs,
            )
            if pwd_changed and user.session:
                # invalidate (delete) the session if the password was changed
                GulpLogger.get_logger().debug(
                    "password changed, deleting session for user=%s" % (user.id)
                )
                sess.add(user.session)
                user.session = None

            # commit in the end
            sess.add(user)
            await sess.commit()
            return user

    @override
    @classmethod
    async def delete_by_id(
        cls,
        token: str,
        id: str,
        permission: list[GulpUserPermission] = [GulpUserPermission.DELETE],
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
        throw_if_not_found: bool = True,
    ) -> None:
        if id == "admin":
            raise ValueError("cannot delete the default admin user")
        await super().delete_by_id(
            token=token,
            id=id,
            permission=permission,
            ws_id=ws_id,
            req_id=req_id,
            sess=sess,
            throw_if_not_found=throw_if_not_found)
            

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
    ) -> tuple["GulpUser", "GulpUserSession"]:
        """
        Asynchronously logs in a user and creates a session (=obtain token).
        Args:
            user (str): The username of the user to log in.
            password (str): The password of the user to log in.
        Returns:
            tuple[GulpUser, GulpUserSession]: The updated user and the session object.
        """
        from gulp.api.collab.user_session import GulpUserSession

        GulpLogger.get_logger().debug("---> logging in user=%s ..." % (user))

        sess = GulpCollab.get_instance().session()
        async with sess:
            u: GulpUser = await GulpUser.get_one_by_id(id=user, sess=sess)
            # check if user has a session already, if so invalidate
            if u.session:
                GulpLogger.get_logger().debug("resetting previous session for user=%s" % (user))
                u.session = None
                sess.add(u)  # keep track of the change

            # check password
            if u.pwd_hash != muty.crypto.hash_sha256(password):
                raise WrongUsernameOrPassword("wrong password for user=%s" % (user))

            # create new session (will auto-generate a token)
            new_session: GulpUserSession = await GulpUserSession._create(
                ws_id=ws_id,
                req_id=req_id,
                sess=sess,
                user_id=u.id,
                user=u,
            )
            if GulpConfig.get_instance().debug_no_token_expiration():
                new_session.time_expire = 0
            else:
                # setup session expiration
                if u.is_admin():
                    new_session.time_expire = (
                        muty.time.now_msec() + GulpConfig.get_instance().token_admin_ttl() * 1000
                    )
                else:
                    new_session.time_expire = (
                        muty.time.now_msec() + GulpConfig.get_instance().token_ttl() * 1000
                    )

            # update user with new session and write the new session object itself
            u.session = new_session
            u.time_last_login = muty.time.now_msec()
            sess.add(u)
            sess.add(new_session)
            await sess.commit()  # this will also delete the previous session from above, if needed
            return u, new_session

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
        GulpLogger.get_logger().debug("---> logging out token=%s ..." % (token))
        from gulp.api.collab.user_session import GulpUserSession

        await GulpUserSession.delete_by_id(token=token, id=token, ws_id=ws_id, req_id=req_id)

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
