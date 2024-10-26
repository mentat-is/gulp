from typing import Optional, override
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
    WrongUsernameOrPassword,
)
import gulp.config as config
from gulp.utils import logger


class GulpUserSession(GulpCollabBase):
    """
    Represents a user session (logged user).
    """

    __tablename__ = GulpCollabType.SESSION
    user_id: Mapped[str] = mapped_column(
        str, ForeignKey("user.id", ondelete="CASCADE"), nullable=False
    )
    user: Mapped["GulpUser"] = relationship("User", back_populates="session")
    time_expire: Mapped[Optional[int]] = mapped_column(
        BIGINT,
        default=0,
        doc="The time when the session expires, in milliseconds from unix epoch.",
    )

    __mapper_args__ = {
        f"polymorphic_identity": {GulpCollabType.SESSION},
    }

    @override
    def _init(self, id: str, user: "GulpUser", **kwargs) -> None:
        """
        Initialize a GulpUserSession.
        Args:
            id (str): The token identifier for the session.
            user (GulpUser): The user associated with the session.
            **kwargs: Additional keyword arguments.
        Returns:
            None
        """

        from gulp.api.collab.user import GulpUser

        # id is the token
        u: GulpUser = user
        super().__init__(id, GulpCollabType.SESSION, u.id)
        self.user = u
        self.user_id = u.id
        if config.debug_no_token_expiration():
            self.time_expire = 0
        else:
            if u.is_admin():
                self.time_expire = (
                    muty.time.now_msec() + config.token_admin_ttl() * 1000
                )
            else:
                self.time_expire = muty.time.now_msec() + config.token_ttl() * 1000
        logger().debug(
            "---> GulpUserSession: token=%s, user=%s, time_expire=%s"
            % (id, self.user, self.time_expire)
        )

    @classmethod
    @override
    async def create(
        cls,
        id: str,
        user: str | "GulpUser",
        sess: AsyncSession = None,
        commit: bool = True,
        **kwargs,
    ) -> T:
        raise NotImplementedError(
            "Direct calls to create() are not allowed. Use login() instead."
        )

    @classmethod
    async def login(cls, user: str, password: str, sess: AsyncSession = None) -> T:
        """
        Asynchronously logs in a user and creates a session.
        Args:
            user (str): The username of the user to log in.
            password (str): The password of the user to log in.
            sess (AsyncSession, optional): An optional asynchronous session object. Defaults to None.
        Returns:
            T: the user session object.
        """

        logger().debug("---> logging in user=%s ..." % (user))
        return await cls.__create(user, password, sess)

    @classmethod
    async def logout(cls, token: str, sess: AsyncSession = None) -> None:
        """
        Logs out the specified user by deleting their session.
        Args:
            user (str): The username of the user to log out.
            sess (AsyncSession, optional): The asynchronous session to use for the operation. Defaults to None.
        Returns:
            None
        """
        logger().debug("---> logging out token=%s ..." % (token))
        await cls.delete(token, sess=sess)

    @classmethod
    async def __create(
        cls,
        user: str | "GulpUser",
        password: str,
        sess: AsyncSession = None,
        commit: bool = True,
        **kwargs,
    ) -> T:
        if isinstance(user, str):
            # get user object
            from gulp.api.collab.user import GulpUser

            user: GulpUser = await GulpUser.get_one(GulpCollabFilter(id=[user]), sess)

        # check if user has a session already, if so invalidate
        if user.session:
            # delete previous session
            logger().debug("deleting previous session for user=%s" % (user))
            await GulpUserSession.delete(user.session.id, sess, commit=False)
            user.session = None

        # check password
        if user.pwd_hash != muty.crypto.hash_sha256(password):
            raise WrongUsernameOrPassword("wrong password for user=%s" % (user))

        # create new session
        token = muty.string.generate_unique()
        return await super().create(token, user, sess, commit, **kwargs)

    @classmethod
    async def get_by_token(cls, token: str, sess: AsyncSession = None) -> T:
        """
        Asynchronously retrieves a user session by token.
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

            c: GulpUser = await super().get_one(
                GulpCollabFilter(id=["admin"], type=[GulpCollabType.USER]),
                sess,
                throw_if_not_found=False,
            )
            if c.session:
                # already exists
                return c.session
            else:
                # create a new admin session
                token = muty.string.generate_unique()
                return await super().create(token, c.user, sess)

        return await super().get_one(
            GulpCollabFilter(id=[token], type=[GulpCollabType.SESSION]), sess
        )
