from typing import Optional, override

import muty.crypto
import muty.time
from sqlalchemy import ARRAY, BIGINT, ForeignKey, Integer, String, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio.engine import AsyncEngine
from sqlalchemy.orm import Mapped, mapped_column, relationship

from gulp.api.collab.structs import (
    GulpCollabBase,
    GulpCollabFilter,
    GulpCollabType,
    GulpUserPermission,
    MissingPermission,
)

from gulp.defs import ObjectAlreadyExists, ObjectNotFound
from gulp.utils import logger


class GulpUser(GulpCollabBase):
    """
    Represents a user in the system.
    """

    __tablename__ = "user"
    id: Mapped[str] = mapped_column(
        ForeignKey("collab_base.id"),
        primary_key=True,
        doc="The user name.",
    )
    pwd_hash: Mapped[str] = mapped_column(String)
    permission: Mapped[Optional[GulpUserPermission]] = mapped_column(
        String, default=GulpUserPermission.READ
    )
    glyph: Mapped[Optional[str]] = mapped_column(
        ForeignKey("glyph.id", ondelete="SET NULL"), default=None
    )
    email: Mapped[Optional[str]] = mapped_column(String, default=None)
    time_last_login: Mapped[Optional[int]] = mapped_column(BIGINT, default=0)
    user_data = relationship("GulpUserData", back_populates="user")
    session = relationship("GulpUserSession", back_populates="user")

    """
    session: Mapped[Optional[str]] = mapped_column(
        ForeignKey("session.name", ondelete="SET NULL"), default=None
    )
    """

    @override
    def __init__(
        self,
        username: str,
        password: str,
        permission: GulpUserPermission = GulpUserPermission.READ,
        email: str = None,
        glyph: str = None,
    ) -> None:
        """
        Initialize a new Gulp user.
        Args:
            username (str): The username of the user.
            password (str): The password of the user.
            permission (GulpUserPermission, optional): The permission level of the user. Defaults to GulpUserPermission.READ.
            email (str, optional): The email address of the user. Defaults to None.
            glyph (str, optional): The glyph associated with the user. Defaults to None.
        Returns:
            None
        """
        super().__init__(username, GulpCollabType.USER)
        self.pwd_hash = muty.crypto.hash_sha256(password)
        self.permission = permission
        self.email = email
        self.glyph = glyph

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

    def has_permission(self, permission: GulpUserPermission | str) -> bool:
        """
        Check if the user has the specified permission.

        Args:
            permission (GulpUserPermission|str): The permission to check.

        Returns:
            bool: True if the user has the specified permission, False otherwise.
        """
        if isinstance(permission, str):
            try:
                permission = GulpUserPermission[permission]
            except KeyError:
                raise ValueError(f"Invalid permission: {permission}")

        return bool(self.permission & permission)
