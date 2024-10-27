from typing import Optional, Union, override

import muty.crypto
import muty.time
from sqlalchemy import BIGINT, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship, declared_attr
from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.collab.structs import (
    GulpCollabBase,
    GulpCollabType,
    GulpUserPermission,
    T,
)
from gulp.api.collab_api import session
from gulp.utils import logger


class GulpUser(GulpCollabBase):
    """
    Represents a user in the system.
    """

    __tablename__ = GulpCollabType.USER.value
    pwd_hash: Mapped[str] = mapped_column(String)
    permission: Mapped[Optional[GulpUserPermission]] = mapped_column(
        String, default=GulpUserPermission.READ
    )
    glyph: Mapped[Optional[str]] = mapped_column(
        ForeignKey("glyph.id", ondelete="SET NULL"), default=None
    )
    email: Mapped[Optional[str]] = mapped_column(String, default=None)
    time_last_login: Mapped[Optional[int]] = mapped_column(BIGINT, default=0)
    session_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("session.id", ondelete="SET NULL"), default=None
    )
    __mapper_args__ = {
        "polymorphic_identity": GulpCollabType.USER.value,
    }

    @declared_attr
    def session(self):
        return relationship(
            "GulpUserSession",
            back_populates="user",
            cascade="all, delete-orphan",
        )

    @declared_attr
    def user_data(self):
        return relationship(
            "GulpUserData", back_populates="user", cascade="all, delete-orphan"
        )

    @override
    @classmethod
    async def create(
        cls,
        id: str,
        user: Union[str, "GulpUser"],
        password: str,
        permission: GulpUserPermission = GulpUserPermission.READ,
        email: str = None,
        glyph: str = None,
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
        commit: bool = True,
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
            user,
            ws_id,
            req_id,
            sess,
            commit,
            **args,
        )

    @override
    @classmethod
    async def update(
        cls,
        id: str,
        d: dict | T,
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
        commit: bool = True,
        throw_if_not_found: bool = True,
    ) -> T:
        # if d is a dict and have "password", hash it (password update)
        pwd_changed = False
        if isinstance(d, dict) and "password" in d:
            d["pwd_hash"] = muty.crypto.hash_sha256(d["password"])
            del d["password"]
            pwd_changed = True

        if sess is None:
            sess = await session()
        commit = False

        c = await super().update(
            id, d, ws_id, req_id, sess, commit, throw_if_not_found=throw_if_not_found
        )
        if pwd_changed and c.session:
            # invalidate (delete) the session if the password was changed
            logger().debug("password changed, deleting session for user=%s" % (c.id))
            c.session.user = None
            c.session = None
            sess.add(c)

        # commit in the end
        await sess.commit()
        return c

    @override
    @classmethod
    async def delete(
        cls,
        id: str,
        ws_id: str = None,
        req_id: str = None,
        throw_if_not_found: bool = True,
        sess: AsyncSession = None,
        commit: bool = True,
    ) -> None:
        if id == "admin":
            raise ValueError("cannot delete the default admin user")
        await super().delete(id, ws_id, req_id, throw_if_not_found, sess, commit)

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
