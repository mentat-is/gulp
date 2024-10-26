from typing import Optional, override

import muty.crypto
import muty.time
from sqlalchemy import BIGINT, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.collab.structs import (
    GulpCollabBase,
    GulpCollabType,
    GulpUserPermission,
    T,
)


class GulpUser(GulpCollabBase):
    """
    Represents a user in the system.
    """

    __tablename__ = GulpCollabType.USER
    pwd_hash: Mapped[str] = mapped_column(String)
    permission: Mapped[Optional[GulpUserPermission]] = mapped_column(
        String, default=GulpUserPermission.READ
    )
    glyph: Mapped[Optional[str]] = mapped_column(
        ForeignKey("glyph.id", ondelete="SET NULL"), default=None
    )
    email: Mapped[Optional[str]] = mapped_column(String, default=None)
    time_last_login: Mapped[Optional[int]] = mapped_column(BIGINT, default=0)
    user_data = relationship(
        "GulpUserData", back_populates="user", cascade="all, delete-orphan"
    )
    session = relationship(
        "GulpUserSession", back_populates="user", cascade="all, delete-orphan"
    )

    """
    session: Mapped[Optional[str]] = mapped_column(
        ForeignKey("session.name", ondelete="SET NULL"), default=None
    )
    """
    __mapper_args__ = {
        f"polymorphic_identity": {GulpCollabType.USER},
    }

    @override
    def _init(
        self,
        id: str,
        user: str,
        password: str,
        permission: GulpUserPermission = GulpUserPermission.READ,
        email: str = None,
        glyph: str = None,
        **kwargs,
    ) -> None:
        """
        Initialize a new Gulp user.
        Args:
            id (str): The user identifier (name).
            user (str): who created the user.
            password (str): The password of the user (will be hashed before storing).
            permission (GulpUserPermission, optional): The permission level of the user. Defaults to GulpUserPermission.READ.
            email (str, optional): The email address of the user. Defaults to None.
            glyph (str, optional): The glyph associated with the user. Defaults to None.
        Returns:
            None
        """
        super().__init__(id, GulpCollabType.USER, user)
        self.pwd_hash = muty.crypto.hash_sha256(password)
        self.permission = permission
        self.email = email
        self.glyph = glyph

    @override
    @classmethod
    async def update(
        cls,
        obj_id: str,
        d: dict | T,
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

        c = await super().update(obj_id, d, sess, commit, throw_if_not_found)
        if pwd_changed and c.session:
            # invalidate the session if the password was changed
            from gulp.api.collab.user_session import GulpUserSession

            await GulpUserSession.delete(c.session.id)

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
