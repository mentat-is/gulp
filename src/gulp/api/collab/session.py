import muty.crypto
import muty.string
import muty.time
from sqlalchemy import BIGINT, ForeignKey, String, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio.engine import AsyncEngine
from sqlalchemy.orm import Mapped, mapped_column

from gulp.utils import logger
import gulp.config as config
from gulp.api.collab.base import (
    CollabBase,
    GulpCollabFilter,
    GulpUserPermission,
    MissingPermission,
    SessionExpired,
    WrongUsernameOrPassword,
)
from gulp.api.collab.collabobj import CollabObj
from gulp.defs import ObjectNotFound


class UserSession(CollabBase):
    """
    Represents a user session (logged user).

    Attributes:
        id (int): The session ID.
        user_id (int): The ID of the user associated with the session.
        token (str): The session token.
        time_expire (int): The expiration time of the session.
        data (Optional[dict]): Additional data associated with the session.
    """

    __tablename__ = "sessions"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, init=False)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    token: Mapped[str] = mapped_column(String(128), unique=True)
    time_expire: Mapped[int] = mapped_column(BIGINT, default=0)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "user_id": self.user_id,
            "token": self.token,
            "time_expire": self.time_expire,
        }

    @staticmethod
    async def check_expired(engine: AsyncEngine, user_session: "UserSession") -> None:
        """
        Check if the user session has expired and delete the token if it has.

        Args:
            engine (AsyncEngine): The database engine.
            user_session (UserSession): The user session object.

        Raises:
            SessionExpired: If the session token has expired.
        """
        if not config.debug_no_token_expiration():
            if user_session.time_expire > 0:
                if muty.time.now_msec() > user_session.time_expire:
                    # delete this token
                    await UserSession.delete(engine, user_session.token)
                    raise SessionExpired(
                        "session token %s expired !" % (user_session.token)
                    )

    @staticmethod
    async def get_by_user_id(
        engine: AsyncEngine, user_id: int, **kwargs
    ) -> "UserSession":
        """
        Retrieves a user session based on the provided user ID.

        Args:
            engine (AsyncEngine): The async engine used for database operations.
            user_id (int): The ID of the er associated with the session.
            kwargs (dict): Additional keyword arguments.

        Returns:
            User: The user session object.

        Raises:
            ObjectNotFound: If the session token is not found.
        """

        logger().debug("---> get: user_id=%s" % (user_id))

        async with AsyncSession(engine) as sess:
            q = select(UserSession).where(UserSession.user_id == user_id)
            res = await sess.execute(q)
            user_session: UserSession = res.scalar_one_or_none()
            if user_session is None:
                raise ObjectNotFound("session not found for user_id=%d !" % (user_id))

            logger().debug(
                "get: token for user_id %d=%s" % (user_id, user_session.token)
            )

            # check if token is expired
            await UserSession.check_expired(engine, user_session)

            # token exists and is not expired
            return user_session

    @staticmethod
    async def get(engine: AsyncEngine, token: str, **kwargs) -> "UserSession":
        """
        Retrieves a user session based on the provided token.

        Args:
            engine (AsyncEngine): The async engine used for database operations.
            token (str): The token associated with the user session.
            kwargs (dict): Additional keyword arguments.

        Returns:
            UserSession: The user session object.

        Raises:
            ObjectNotFound: If the session token is not found.
        """

        logger().debug("---> get: token=%s" % (token))

        async with AsyncSession(engine) as sess:
            q = select(UserSession).where(UserSession.token == token)
            res = await sess.execute(q)
            user_session: UserSession = res.scalar_one_or_none()
            if user_session is None:
                raise ObjectNotFound("session token %s not found !" % (token))
            logger().debug(
                "get: user_session for token %s=%s" % (token, user_session)
            )

            # check if token is expired
            await UserSession.check_expired(engine, user_session)

            # token exists and is not expired
            return user_session

    @staticmethod
    async def _create_internal(
        engine: AsyncEngine, sess: AsyncSession, user, impersonated: bool = False
    ) -> "UserSession":
        # check if session exists
        q = select(UserSession).where(UserSession.user_id == user.id).with_for_update()
        res = await sess.execute(q)
        user_session = res.scalar_one_or_none()

        # create new session token
        token = muty.string.generate_unique()
        if user.is_admin() or impersonated:
            time_expire = config.token_admin_ttl()
        else:
            time_expire = config.token_ttl()

        if time_expire > 0:
            time_expire = time_expire * 1000 + muty.time.now_msec()
            logger().warning(
                "token %s created for user %s, time_expire=%s"
                % (token, user.name, time_expire)
            )

        # update sessions table
        if user_session is not None:
            # already exist
            logger().debug(
                "user %s session already exists, update token ..." % (user.name)
            )
            user_session.token = token
            user_session.time_expire = time_expire
        else:
            # create
            user_session = UserSession(
                user_id=user.id, token=token, time_expire=time_expire
            )
            logger().debug("created new user session: %s" % (user_session))

        # update user table with last login time and session id
        user.time_last_login = muty.time.now_msec()
        sess.add(user_session)
        await sess.flush()
        # logger().debug('session id(POST-FLUSH)=%d' % (user_session.id))

        # set session_id in user to indicate login
        user.session_id = user_session.id
        sess.add(user)
        await sess.commit()

        # done
        logger().info(
            "---> create: user %s logged in, session=%s" % (user, user_session)
        )

        return user_session

    @staticmethod
    async def create(
        engine: AsyncEngine,
        username: str,
        password: str,
        allow_any_password: bool = False,
        **kwargs,
    ) -> tuple[any, "UserSession"]:
        """
        Creates a new user session.

        Args:
            engine (AsyncEngine): The database engine.
            username (str): The username of the user.
            password (str): The password of the user.
            allow_any_password (bool): for debugging only
            kwargs (dict): Additional keyword arguments.

        Returns:
            tuple['User', 'UserSession']: A tuple containing the created User and UserSession objects.
        """
        from gulp.api.collab.user import User

        if not __debug__:
            allow_any_password = False

        logger().debug("---> create: username=%s" % (username))
        async with AsyncSession(engine, expire_on_commit=False) as sess:
            h = muty.crypto.hash_sha256(password)

            # get user
            if allow_any_password:
                # debugging only
                q = select(User).where((User.name == username)).with_for_update()
                res = await sess.execute(q)
            else:
                q = (
                    select(User)
                    .where((User.name == username) & (User.pwd_hash == h))
                    .with_for_update()
                )
                res = await sess.execute(q)
            user = res.scalar_one_or_none()
            if user is None:
                raise WrongUsernameOrPassword(
                    "user %s not found or wrong password" % (username)
                )

            s = await UserSession._create_internal(engine, sess, user)
            return user, s

    @staticmethod
    async def delete(engine: AsyncEngine, token: str, **kwargs) -> None:
        """
        Delete a user session with the given token.

        Args:
            engine (AsyncEngine): The async engine used for database operations.
            token (str): The token of the session to be deleted.
            kwargs (dict): Additional keyword arguments.
        Raises:
            ObjectNotFound: If the session with the given token is not found.

        """
        logger().debug("---> delete: token=%s" % (token))
        if config.debug_allow_any_token_as_admin():
            # use admin token
            _, s = await UserSession._get_admin(engine)
            token = s.token

        async with AsyncSession(engine) as sess:
            q = select(UserSession).where(UserSession.token == token)
            res = await sess.execute(q)
            session = res.scalar_one_or_none()
            if session is None:
                raise ObjectNotFound("token %s does not exist!" % (token))

            await sess.delete(session)
            await sess.commit()
            logger().info(
                "session deleted for token=%s, user_id=%d ..."
                % (token, session.user_id)
            )

    @staticmethod
    async def _get_admin(engine: AsyncEngine) -> tuple[any, "UserSession"]:
        """
        this is only used for debugging when "debug_allow_any_token_as_admin" is set

        @return: a tuple with admin user and its session (existing or created)

        """
        from gulp.api.collab.user import User

        logger().debug("---> get_admin")
        # get user
        async with AsyncSession(engine, expire_on_commit=False) as sess:
            q = select(User).where(User.name == "admin")
            res = await sess.execute(q)
            user = res.scalar_one_or_none()
            if user is None:
                raise ObjectNotFound("admin user not found !")

        user_session: UserSession = None
        async with AsyncSession(engine) as sess:
            q = select(UserSession).where(UserSession.user_id == user.id)
            res = await sess.execute(q)
            user_session: UserSession = res.scalar_one_or_none()

        if user_session is not None:
            # found session
            return user, user_session

        # create session
        user, user_session = await UserSession.create(
            engine, user.name, "admin", allow_any_password=True
        )
        return user, user_session

    @staticmethod
    async def impersonate(
        engine: AsyncEngine, token: str, user_id: int
    ) -> "UserSession":
        """
        Impersonate a user by creating a new session token for the given user ID.

        NOTE: the given user token will be invalidated if user_id is logged in.

        Args:
            engine (AsyncEngine): The async engine used for database operations.
            token: Authentication token (must be admin)
            user_id (int): The ID of the user to be impersonated.

        Returns:
            UserSession: The new user session object.
        """
        from gulp.api.collab.user import User

        logger().debug("---> impersonate: user_id=%s" % (user_id))
        await UserSession.check_token(engine, token, GulpUserPermission.ADMIN)

        async with AsyncSession(engine, expire_on_commit=False) as sess:
            q = select(User).where(User.id == user_id).with_for_update()
            res = await sess.execute(q)
            user = res.scalar_one_or_none()
            if user is None:
                raise ObjectNotFound("user %d not found!" % (user_id))

            return await UserSession._create_internal(
                engine, sess, user, impersonated=True
            )

    @staticmethod
    async def check_token(
        engine: AsyncEngine,
        token: str,
        requested_permission: GulpUserPermission = GulpUserPermission.READ,
        obj_id: int = None,
        **kwargs,
    ) -> tuple[any, "UserSession"]:
        """
        Checks the validity of a token and verifies the user's permissions (also checks for token expiration).

        Args:
            engine (AsyncEngine): The database engine.
            token (str): The token to be checked.
            requested_permission (GulpUserPermission, optional): The requested permission level. Defaults to GulpUserPermission.READ.
            obj_id (int, optional): The object to be checked. Defaults to None.
            kwargs (dict): Additional keyword arguments.
        Returns:
            tuple[User, UserSession]: A tuple containing the User and UserSession objects.

        Raises:
            ObjectNotFound: If the user associated with the token is not found/expired.
            MissingPermission: If the user does not have the required permissions.
        """
        from gulp.api.collab.user import User

        logger().debug("---> check_token: token=%s" % (token))
        if config.debug_allow_any_token_as_admin():
            # always return the admin user and its token (for testing purposes)
            u, s = await UserSession._get_admin(engine)
            return u, s

        # get session
        user_session = await UserSession.get(engine, token)

        # get user
        async with AsyncSession(engine) as sess:
            res = await sess.execute(
                select(User).where((User.id == user_session.user_id))
            )

            user = res.scalar_one_or_none()
            if user is None:
                raise ObjectNotFound(
                    "user %d not found, token=%s !" % (user_session.user_id, token)
                )
        user_permission_name = GulpUserPermission(user.permission).name
        requested_permission_name = GulpUserPermission(requested_permission).name

        # check permissions
        is_owner = False
        if obj_id is not None:
            # get object by id
            obj = await CollabObj.get(engine, GulpCollabFilter(id=[obj_id]))
            obj = obj[0]

            is_owner = obj.owner_user_id == user.id
            is_admin = user.is_admin()
            is_delete = (
                requested_permission & GulpUserPermission.DELETE
                == GulpUserPermission.DELETE
            )
            is_edit = (
                requested_permission & GulpUserPermission.EDIT
                == GulpUserPermission.EDIT
            )
            has_delete = (
                (
                    user.permission & GulpUserPermission.DELETE
                    == GulpUserPermission.DELETE
                )
                or is_admin
                or is_owner
            )
            has_edit = (
                (user.permission & GulpUserPermission.EDIT == GulpUserPermission.EDIT)
                or is_admin
                or is_owner
            )
            if (is_delete and not has_delete) or (is_edit and not has_edit):
                raise MissingPermission(
                    "user=%d(%s) has no permission to delete or edit object=%d (not owner, not admin, user permission=%s(%d), requested=%s(%d))"
                    % (
                        user.id,
                        user.name,
                        obj_id,
                        user_permission_name,
                        user.permission,
                        requested_permission_name,
                        requested_permission,
                    )
                )

        if (
            (user.permission & requested_permission != requested_permission)
            and not user.is_admin()
            and not is_owner
        ):
            raise MissingPermission(
                "user=%d(%s) do not have the requested permission: user permission=%s(%d), requested=%s(%d))"
                % (
                    user.id,
                    user.name,
                    user_permission_name,
                    user.permission,
                    requested_permission_name,
                    requested_permission,
                )
            )

        return user, user_session
