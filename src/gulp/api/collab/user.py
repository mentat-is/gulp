from typing import TYPE_CHECKING, Optional, override

import muty.crypto
import muty.string
import muty.time
from muty.log import MutyLogger
from sqlalchemy import ARRAY, BIGINT, ForeignKey, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.types import Enum as SQLEnum
from sqlalchemy.ext.mutable import MutableDict

from gulp.api.collab.structs import (
    GulpCollabBase,
    GulpCollabType,
    GulpUserPermission,
    MissingPermission,
    T,
    WrongUsernameOrPassword,
)
from gulp.api.collab.user_group import GulpUserAssociations
from gulp.api.ws_api import GulpUserLoginLogoutPacket, GulpWsQueueDataType
from gulp.config import GulpConfig

if TYPE_CHECKING:
    from gulp.api.collab.user_group import GulpUserGroup
    from gulp.api.collab.user_session import GulpUserSession


class GulpUser(GulpCollabBase, type=GulpCollabType.USER):
    """
    Represents a user in the system.
    """

    pwd_hash: Mapped[str] = mapped_column(
        String, doc="The hashed password of the user."
    )
    groups: Mapped[list["GulpUserGroup"]] = relationship(
        "GulpUserGroup",
        secondary=GulpUserAssociations.table,
        back_populates="users",
        lazy="selectin",
    )
    permission: Mapped[Optional[list[GulpUserPermission]]] = mapped_column(
        MutableList.as_mutable(ARRAY(SQLEnum(GulpUserPermission))),
        default_factory=lambda: [GulpUserPermission.READ],
        doc="One or more permissions of the user.",
    )
    email: Mapped[Optional[str]] = mapped_column(
        String, default=None, doc="The email of the user.", unique=True
    )
    time_last_login: Mapped[Optional[int]] = mapped_column(
        BIGINT,
        default=0,
        doc="The time of the last login, in milliseconds from the unix epoch.",
    )
    session: Mapped[Optional["GulpUserSession"]] = relationship(
        "GulpUserSession",
        back_populates="user",
        cascade="all,delete-orphan",
        default=None,
        foreign_keys="[GulpUserSession.user_id]",
    )

    user_data: Mapped[Optional[dict]] = mapped_column(
        MutableDict.as_mutable(JSONB), default_factory=dict, doc="Arbitrary user data."
    )

    @override
    @classmethod
    def example(cls) -> dict:
        from gulp.api.collab.user_group import GulpUserGroup
        from gulp.api.collab.user_session import GulpUserSession

        d = super().example()
        d.update(
            {
                "pwd_hash": "hashed_password",
                "groups": [GulpUserGroup.example()],
                "permission": ["READ"],
                "email": "user@mail.com",
                "time_last_login": 1234567890,
                "session": GulpUserSession.example(),
                "user_data": {"key": "value", "key2": 1234},
            }
        )
        return d

    @classmethod
    async def create(
        cls,
        sess: AsyncSession,
        user_id: str,
        password: str,
        permission: list[GulpUserPermission] = [GulpUserPermission.READ],
        email: str = None,
        glyph_id: str = None,
    ) -> T:
        """
        Create a new user object on the collab database (can only be called by an admin).

        Args:
            sess (AsyncSession): The database session.
            user_id (str): The ID of the user to create.
            password (str): The password of the user to create.
            permission (list[GulpUserPermission], optional): The permission of the user to create. Defaults to [GulpUserPermission.READ].
            email (str, optional): The email of the user to create. Defaults to None.
            glyph_id (str, optional): The glyph ID of the user to create. Defaults to None.

        Returns:
            The created user object.
        """
        if GulpUserPermission.READ not in permission:
            # ensure that all users have read permission
            permission.append(GulpUserPermission.READ)

        object_data = {
            "pwd_hash": muty.crypto.hash_sha256(password),
            "permission": permission,
            "email": email,
            "glyph_id": glyph_id,
            "user_data": {},
        }

        # set user_id to username (user owns itself)
        return await super()._create(
            sess, id=user_id, object_data=object_data, owner_id=user_id
        )

    @override
    async def update(
        self,
        sess: AsyncSession,
        d: dict,
        user_session: "GulpUserSession",
    ) -> None:
        """
        updates the user object with the specified data, checking for permission and password changes.

        Args:
            sess (AsyncSession): The database session.
            d (dict): The data to update.
            user_session (GulpUserSession): The user session object.

        Raises:
            MissingPermission: If the user does not have the required permission.
        """

        # special checks for permission and password
        #
        # - only admin can change permission
        # - only admin can change password to other users
        # - changing password will invalidate the session
        if "permission" in d:
            if not user_session.user.is_admin():
                # only admin can change permission
                raise MissingPermission(
                    "only admin can change permission, session_user_id=%s"
                    % (user_session.user_id)
                )

        if "password" in d:
            if not user_session.user.is_admin() and user_session.user.id != self.id:
                # only admin can change password to other users
                raise MissingPermission(
                    "only admin can change password to other users, user_id=%s, session_user_id=%s"
                    % (self.id, user_session.user_id)
                )

        # checks ok, update user
        if "password" in d:
            d["pwd_hash"] = muty.crypto.hash_sha256(d["password"])
            del d["password"]
        if "permission" in d:
            # ensure that all users have read permission
            if GulpUserPermission.READ not in d["permission"]:
                d["permission"].append(GulpUserPermission.READ)

        # update
        await super().update(sess, d)

        # invalidate session for the user
        MutyLogger.get_instance().warning(
            "updated user, invalidating session for user_id=%s" % (self.id)
        )

        await sess.delete(user_session)
        await sess.flush()

    def is_admin(self) -> bool:
        """
        Check if the user has admin permission (or is in an admin group).

        Returns:
            bool: True if the user has admin permission, False otherwise.
        """
        admin = GulpUserPermission.ADMIN in self.permission
        if admin:
            return admin

        if self.groups:
            # also check if the user is in an admin group
            for group in self.groups:
                if group.is_admin():
                    return True

        return False

    def logged_in(self) -> bool:
        """
        check if the user is logged in

        Returns:
            bool: True if the user has an active session (logged in)
        """
        return self.session is not None

    @staticmethod
    async def login(
        sess: AsyncSession, user_id: str, password: str, ws_id: str, req_id: str
    ) -> "GulpUserSession":
        """
        Asynchronously logs in a user and creates a session (=obtain token).
        Args:
            user (str): The username of the user to log in.
            password (str): The password of the user to log in.
            ws_id (str): The websocket ID.
            req_id (str): The request ID.

        Returns:
            GulpUserSession: The created session object.
        """
        from gulp.api.collab.user_session import GulpUserSession

        u: GulpUser = await GulpUser.get_by_id(sess, user_id, with_for_update=True)
        if u.session:
            # check if user has a session already, if so invalidate
            MutyLogger.get_instance().warning(
                "user %s was already logged in, resetting..." % (user_id)
            )
            await sess.delete(u.session)
            await sess.flush()
            await sess.refresh(u)

        # check password
        if u.pwd_hash != muty.crypto.hash_sha256(password):
            raise WrongUsernameOrPassword("wrong password for user_id=%s" % (user_id))

        # get expiration time
        if GulpConfig.get_instance().debug_no_token_expiration():
            time_expire = None
        else:
            # setup session expiration
            if u.is_admin():
                time_expire = (
                    muty.time.now_msec()
                    + GulpConfig.get_instance().token_admin_ttl() * 1000
                )
            else:
                time_expire = (
                    muty.time.now_msec() + GulpConfig.get_instance().token_ttl() * 1000
                )

        # create new session
        p = GulpUserLoginLogoutPacket(user_id=u.id, login=True)
        object_data = {
            "user_id": u.id,
            "time_expire": time_expire,
        }
        if GulpConfig.get_instance().is_integration_test():
            # for integration tests, this api will return a fixed token based on the user_id
            # (the user must anyway log in first)
            token_id = "token_" + user_id
            MutyLogger.get_instance().warning(
                "using fixed token %s for integration test" % (token_id)
            )
        else:
            # autogenerated
            token_id = None
            
        new_session: GulpUserSession = await GulpUserSession._create(
            sess,
            object_data,
            id=token_id,
            ws_id=ws_id,
            owner_id=u.id,
            ws_queue_datatype=GulpWsQueueDataType.USER_LOGIN,
            ws_data=p.model_dump(),
            req_id=req_id,
        )

        # update user with new session and write the new session object itself
        u.session = new_session
        u.time_last_login = muty.time.now_msec()
        sess.add(u)
        await sess.commit()
        await sess.refresh(new_session)
        MutyLogger.get_instance().info(
            "user %s logged in, token=%s" % (u.id, new_session.id)
        )
        return new_session

    @staticmethod
    async def logout(
        sess: AsyncSession, s: "GulpUserSession", ws_id: str, req_id: str
    ) -> None:
        """
        Logs out the specified user by deleting the session.

        Args:
            sess (AsyncSession): The session to use.
            s: the GulpUserSession to log out
            ws_id (str): The websocket ID.
            req_id (str): The request ID.
        Returns:
            None
        """
        async with sess:
            MutyLogger.get_instance().info(
                "logging out token=%s, user=%s" % (s.id, s.user_id)
            )
            p = GulpUserLoginLogoutPacket(user_id=s.user_id, login=False)
            await s.delete(
                sess=sess,
                user_id=s.user_id,
                ws_id=ws_id,
                req_id=req_id,
                ws_queue_datatype=GulpWsQueueDataType.USER_LOGOUT,
                ws_data=p.model_dump(),
            )

    def has_permission(self, permission: list[GulpUserPermission]) -> bool:
        """
        Check if the user has the specified permission

        Args:
            permission (list[GulpUserPermission]): The permission(s) to check.

        Returns:
            bool: True if the user has the specified permissions, False otherwise.
        """
        if self.is_admin():
            return True

        has_permission = all([p in self.permission for p in permission])
        if has_permission:
            return True
        if self.groups:
            for group in self.groups:
                if group.has_permission(permission):
                    return True
        return False

    def check_object_access(
        self,
        obj: GulpCollabBase,
        enforce_owner: bool = False,
        throw_on_no_permission: bool = False,
    ) -> bool:
        """
        Check if the user has READ permission to access the specified object.

        the user has permission to access the object if:

        - no granted users or groups are set (everyone has access)
        - the user is an admin
        - the user is the owner of the object
        - the user is in the granted groups of the object
        - the user is in the granted users of the object

        Args:
            obj (GulpCollabBase): The object to check against.
            enforce_owner (bool, optional): Whether to enforce that the user is the owner of the object (or administrator). Defaults to False.
            throw_on_no_permission (bool, optional): Whether to throw an exception if the user does not have permission. Defaults to False.
        Returns:
            bool: True if the user has permission to access the object, False otherwise.
        """
        if self.is_admin():
            # admin is always granted
            # MutyLogger.get_instance().debug("allowing access to admin")
            return True

        # check if the user is the owner of the object
        if obj.is_owner(self.id):
            # MutyLogger.get_instance().debug("allowing access to object owner")
            return True

        if enforce_owner:
            if throw_on_no_permission:
                raise MissingPermission(
                    f"User {self.id} is not the owner of the object {obj.id}."
                )
            return False

        if not obj.granted_user_group_ids and not obj.granted_user_ids:
            # no granted users or groups, allow access
            # MutyLogger.get_instance().debug("allowing access to object without granted users or groups")
            return True

        # check if the user is in the granted groups
        if obj.granted_user_group_ids:
            for group in self.groups:
                if group.id in obj.granted_user_group_ids:
                    # MutyLogger.get_instance().debug("allowing access to granted group")
                    return True

        # check if the user is in the granted users
        if obj.granted_user_ids and self.id in obj.granted_user_ids:
            # MutyLogger.get_instance().debug("allowing access to granted user")
            return True

        if throw_on_no_permission:
            raise MissingPermission(
                f"User {self.id} does not have the required permissions to access the object {obj.id}."
            )
        MutyLogger.get_instance().debug(
            f"User {self.id} does not have the required permissions to access the object {obj.id}, granted_user_ids={obj.granted_user_ids}, granted_group_ids={obj.granted_user_group_ids}, requestor_user_id={self.id}"
        )
        return False
