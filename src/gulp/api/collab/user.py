from typing import Optional

import muty.time
from sqlalchemy import ARRAY, BIGINT, ForeignKey, Integer, String, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio.engine import AsyncEngine
from sqlalchemy.orm import Mapped, mapped_column

import gulp.api.collab_api as collab_api
from gulp.api.collab.base import (
    CollabBase,
    GulpCollabFilter,
    GulpUserPermission,
    MissingPermission,
)
from gulp.api.collab.session import UserSession
from gulp.defs import ObjectAlreadyExists, ObjectNotFound


class User(CollabBase):
    """
    Represents a user in the system.
    """

    __tablename__ = "users"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, init=False)
    name: Mapped[str] = mapped_column(String(32), unique=True)
    pwd_hash: Mapped[str] = mapped_column(String(128))
    glyph_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("glyph.id", ondelete="SET NULL"), default=None, nullable=True
    )
    email: Mapped[Optional[str]] = mapped_column(
        String(128), default=None, nullable=True
    )
    time_last_login: Mapped[int] = mapped_column(BIGINT, default=0)
    permission: Mapped[GulpUserPermission] = mapped_column(
        Integer, default=GulpUserPermission.READ
    )
    session_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("sessions.id", ondelete="SET NULL"), default=None, nullable=True
    )
    user_data: Mapped[list[int]] = mapped_column(
        ARRAY(Integer), default=None, nullable=True
    )

    def is_admin(self) -> bool:
        """
        Check if the user has admin permission.

        Returns:
            bool: True if the user has admin permission, False otherwise.
        """
        return bool(self.permission & GulpUserPermission.ADMIN)

    def is_monitor(self) -> bool:
        """
        Check if the user has monitor permission.
        """
        return bool(self.permission & GulpUserPermission.MONITOR)

    def logged_in(self) -> bool:
        """
        check if the user is logged in

        Returns:
            bool: True if the user has an active session (logged in)
        """
        return self.session_id is not None

    def has_permission(self, permission: GulpUserPermission) -> bool:
        """
        Check if the user has the specified permission.

        Args:
            permission (GulpUserPermission): The permission to check.

        Returns:
            bool: True if the user has the specified permission, False otherwise.
        """
        return bool(self.permission & permission)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "pwd_hash": self.pwd_hash,
            "glyph_id": self.glyph_id,
            "email": self.email,
            "time_last_login": self.time_last_login,
            "session_id": self.session_id,
            "logged_in": self.logged_in(),
            "user_data": self.user_data,
            "permission": self.permission,
        }

    @staticmethod
    async def get(engine: AsyncEngine, flt: GulpCollabFilter = None) -> list["User"]:
        """
        Retrieves a list of users based on the provided filter.

        Args:
            engine (AsyncEngine): The database engine.
            flt (GulpCollabFilter, optional): The filter (name, id, limit, offset). Defaults to None (=get all users).

        Returns:
            list[User]: A list of User objects matching the filter.

        Raises:
            ObjectNotFound: If no user is found.
        """

        collab_api.logger().debug("---> get: flt=%s" % (flt))

        # make a select() query depending on the filter
        q = select(User)
        if flt is not None:
            # check each part of flt and build the query
            if flt.id is not None:
                q = q.where(User.id.in_(flt.id))

            if flt.name is not None:
                q = q.where(User.name.in_(flt.name))

            if flt.limit is not None:
                q = q.limit(flt.limit)
            if flt.offset is not None:
                q = q.offset(flt.offset)

        async with AsyncSession(engine) as sess:
            res = await sess.execute(q)
            users = res.scalars().all()
            if len(users) == 0:
                raise ObjectNotFound("no user found")
            collab_api.logger().info("users retrieved: %s ..." % (users))
            return users

    @staticmethod
    async def create(
        engine: AsyncEngine,
        username: str,
        password: str,
        email: str = None,
        permission: GulpUserPermission = GulpUserPermission.READ,
        glyph_id: int = None,
    ) -> "User":
        """
        Create a new user in the Gulp system (admin only)

        Args:
            engine (AsyncEngine): The database engine.
            username (str): The username of the new user.
            password (str): The password of the new user.
            email (str, optional): The email address of the new user. Defaults to None.
            permission (GulpUserPermission, optional): The permission level of the new user. Defaults to GulpUserPermission.READ.
            glyph_id (int, optional): The glyph ID of the new user. Defaults to None.
        Returns:
            User: The newly created user object.
        """
        collab_api.logger().debug("---> create: username=%s" % (username))
        async with AsyncSession(engine, expire_on_commit=False) as sess:
            # check if user already exists
            q = select(User).where(User.name == username)
            res = await sess.execute(q)
            if res.scalar_one_or_none() is not None:
                raise ObjectAlreadyExists("user %s already exists" % (username))

            # create
            h = muty.crypto.hash_sha256(password)
            u = User(
                name=username,
                pwd_hash=h,
                email=email,
                permission=permission,
                glyph_id=glyph_id,
                user_data=[],
            )
            sess.add(u)
            await sess.commit()
            collab_api.logger().info("user created: %s ..." % (u))
            return u

    @staticmethod
    async def delete(engine: AsyncEngine, user_id: int) -> None:
        """
        Deletes a user from the system

        Args:
            engine (AsyncEngine): The database engine.
            user_id: The username to be deleted.

        Raises:
            MissingPermission
            ObjectNotFound
        """

        collab_api.logger().debug("---> delete: user_id=%d" % (user_id))

        async with AsyncSession(engine) as sess:
            q = select(User).where(User.id == user_id)
            res = await sess.execute(q)
            user = res.scalar_one_or_none()
            if user is None:
                raise ObjectNotFound("user id %d does not exist!" % (user_id))
            await sess.delete(user)
            await sess.commit()
            collab_api.logger().info("user deleted: %s ..." % (user_id))

    async def update_user_data(self, sess: AsyncSession) -> None:
        """
        updates user.user_data in the underlying table

        NOTE: sess must be committed manually after calling this method.

        Args:
            engine (AsyncEngine): The database engine.
            sess (AsyncSession): The database session.
            new_user_data (list[int]): The new user data to be set.

        Returns:
            None
        """
        if self.user_data is None:
            self.user_data = []

        q = update(User).where(User.id == self.id).values(user_data=self.user_data)
        await sess.execute(q)

    @staticmethod
    async def update(
        engine: AsyncEngine,
        requestor_token: str,
        user_id: int = None,
        password: str = None,
        email: str = None,
        permission: GulpUserPermission = None,
        glyph_id: int = None,
    ) -> "User":
        """
        Update user information.

        Args:
            engine (AsyncEngine): The database engine.
            requestor_token (str): The requestor user token
            user_id (int): id of the user to update (if None, it is derived from token. either, token must be an admin token if user_id != token.user_id)
            password (str, optional): The new password. Defaults to None.
            email (str, optional): The new email. Defaults to None.
            permission (GulpUserPermission, optional): The new permission level (needs admin). Defaults to None.
            glyph_id (int, optional): The new glyph ID. Defaults to None.

        Returns:
            User: The updated user object.

        Raises:
            MissingPermission: If the user does not have permission to update the specified user or permission level.
        """

        collab_api.logger().debug(
            "---> update: token=%s, user_id=%s" % (requestor_token, user_id)
        )

        # check if the requestor has the right to update the user
        user = await User.check_token_owner(
            engine, requestor_token, user_id, permission
        )
        if user_id is None:
            user_id = user.id

        async with AsyncSession(engine, expire_on_commit=False) as sess:
            q = select(User).where(User.id == user_id).with_for_update()
            res = await sess.execute(q)
            user = res.scalar_one_or_none()
            if user is None:
                raise ObjectNotFound("user %s not found!" % (user_id))

            if password is not None:
                h = muty.crypto.hash_sha256(password)
                user.pwd_hash = h
            if permission is not None:
                user.permission = permission
            if glyph_id is not None:
                user.glyph_id = glyph_id
            if email is not None:
                user.email = email

            sess.add(user)
            await sess.commit()
            collab_api.logger().info("user updated: %s ..." % (user))
            return user

    @staticmethod
    async def login(
        engine: AsyncEngine, username: str, password: str
    ) -> tuple["User", UserSession]:
        """
        Logs in a user with the specified username and password.

        Args:
            engine (AsyncEngine): The database engine.
            username (str): The username of the user.
            password (str): The password of the user.

        Returns:
            tuple[User, UserSession]: A tuple containing the logged-in user and the user session.
        """
        collab_api.logger().debug("---> login: username=%s" % (username))
        user, session = await UserSession.create(engine, username, password)
        return user, session

    @staticmethod
    async def logout(engine: AsyncEngine, token: str) -> int:
        """
        Logout the user by deleting the collab session associated with the given token.

        Args:
            engine (AsyncEngine): The database engine.
            token (str): The user token.
        Returns:
            int: The number of deleted sessions.
        raises:
            ObjectNotFound: If the token does not exist.
        """
        collab_api.logger().debug("---> logout: token=%s" % (token))
        return await UserSession.delete(engine, token)

    @staticmethod
    async def check_token_owner(
        engine: AsyncEngine,
        requestor_token: str,
        user_id: int = None,
        permission: GulpUserPermission = None,
    ) -> "User":
        """
        Checks if the requestor token is the owner of the specified user ID (is the user's token or it is an admin token)

        Args:
            engine (AsyncEngine): The database engine.
            requestor_token (str): The token of the requestor.
            user_id (int, optional): The ID of the user to check ownership for. Defaults to None (use user from requestor's token).
            permission (GulpUserPermission, optional): setting this to any value other than None forces the requestor to be checked for ADMIN rights. Defaults to None.

        Raises:
            MissingPermission: If the requestor token does not have the required permission.

        Returns:
            User: The user object.
        """

        requestor, _ = await UserSession.check_token(engine, requestor_token)
        req_user: User = requestor
        if permission is not None and not req_user.is_admin():
            raise MissingPermission(
                "token %s does not have permission to change this user permission"
                % (req_user)
            )

        if user_id is None:
            # use token's user_id (default)
            user_id = req_user.id

        if user_id != req_user.id and not req_user.is_admin():
            # if user_id is set, it must be the same as token's user_id (or token must be an admin token)
            raise MissingPermission(
                "%s (userid=%d) does not have permission to access user_id=%d"
                % (req_user.name, req_user.id, user_id)
            )
        return req_user
