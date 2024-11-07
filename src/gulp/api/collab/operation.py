from typing import Optional, Union, override

from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.collab.structs import GulpCollabBase, GulpCollabType, T, GulpUserPermission
from gulp.utils import GulpLogger


class GulpOperation(GulpCollabBase, type=GulpCollabType.OPERATION):
    """
    Represents an operation in the gulp system.
    """

    index: Mapped[Optional[str]] = mapped_column(
        String(),
        default=None,
        doc="The opensearch index to associate the operation with.",
    )
    description: Mapped[Optional[str]] = mapped_column(
        String(), default=None, doc="The description of the operation."
    )
    glyph: Mapped[Optional[str]] = mapped_column(
        ForeignKey("glyph.id", ondelete="SET NULL"),
        default=None,
        doc="The glyph associated with the operation.",
    )

    @override
    @classmethod
    async def create(
        cls,
        id: str,
        owner: str,
        index: str = None,
        glyph: str = None,
        description: str = None,
        token: str = None,
        **kwargs,
    ) -> T:
        """
        Create a new operation object.

        Args:
            id: The unique identifier of the operation.
            owner: The owner of the operation.
            index: The opensearch index to associate the operation with.
            glyph: The glyph associated with the operation.
            description: The description of the operation.
            token: The token of the user creating the object, for permission check (needs ADMIN permission).
            kwargs: Arbitrary keyword arguments.

        Returns:
            The created operation object.
        """
        args = {
            "index": index,
            "description": description,
            "glyph": glyph,
            **kwargs,
        }
        return await super()._create(
            id,
            owner,
            token=token,
            required_permission=[GulpUserPermission.ADMIN],
            **args,
        )
