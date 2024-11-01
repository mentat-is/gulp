import base64
from typing import Union, override
import muty.file
from sqlalchemy import ForeignKey, LargeBinary
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.collab.structs import GulpCollabBase, GulpCollabType, T, GulpUserPermission
from gulp.utils import logger


class GulpGlyph(GulpCollabBase, type=GulpCollabType.GLYPH):
    """
    Represents a glyph object.

    Attributes:
        id (int): The unique identifier of the glyph (name).
        img (bytes): The image data of the glyph as binary blob.
    """

    img: Mapped[bytes] = mapped_column(
        LargeBinary, doc="The image data of the glyph as binary blob."
    )
    @override
    def __repr__(self) -> str:
        return super().__repr__() + f" img={self.img[:10]}[...]"

    @classmethod
    async def create(
        cls,
        id: str,
        owner: str,
        img: bytes | str,
        token: str = None,
        **kwargs,
    ) -> T:
        """
        Create a new glyph object.
        Args:
            id (str): The unique identifier of the glyph (name).
            owner (str): The owner of the glyph.
            img (bytes | str): The image data of the glyph as binary blob or file path.
            token (str): The token of the user creating the glyph.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            T: The created glyph object
        """
        if isinstance(img, str):
            # load from file path
            img = await muty.file.read_file_async(img)

        args = {"img": img,
                **kwargs}
        return await super()._create(
            id,
            owner,
            token=token,
            required_permission=[GulpUserPermission.ADMIN],
            **args,
        )

    @override
    def to_dict(self, **kwargs) -> dict:
        """
        Convert the object to a dictionary representation.
        Args:
            **kwargs: Arbitrary keyword arguments.
        Returns:
            dict: A dictionary representation of the object, including base64 encoded "img".
        """
        d = super().to_dict(**kwargs)
        d["img"] = base64.b64encode(self.img).decode()
        return d
