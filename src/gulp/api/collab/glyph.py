import base64
from typing import Union, override
import muty.file
from sqlalchemy import ForeignKey, LargeBinary
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.collab.structs import GulpCollabBase, GulpCollabType, T
from gulp.utils import logger


class GulpGlyph(GulpCollabBase):
    """
    Represents a glyph object.

    Attributes:
        id (int): The unique identifier of the glyph (name).
        img (bytes): The image data of the glyph as binary blob.
    """

    __tablename__ = GulpCollabType.GLYPH.value
    img: Mapped[bytes] = mapped_column(
        LargeBinary, doc="The image data of the glyph as binary blob."
    )

    __mapper_args__ = {
        "polymorphic_identity": GulpCollabType.GLYPH.value,
    }

    @override
    def __repr__(self) -> str:
        return super().__repr__() + f" img={self.img[:10]}[...]"

    @override
    @classmethod
    async def create(
        cls,
        id: str,
        owner: str,
        img: bytes | str,
        ws_id: str = None,
        req_id: str = None,
        **kwargs,
    ) -> T:
        if isinstance(img, str):
            # load from file path
            img = await muty.file.read_file_async(img)

        args = {"img": img}
        return await super()._create(
            id,
            GulpCollabType.GLYPH,
            owner,
            ws_id,
            req_id,
            **args,
        )

    @override
    def to_dict(self, *args, **kwargs) -> dict:
        """
        Convert the object to a dictionary representation.
        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        Returns:
            dict: A dictionary representation of the object, including base64 encoded "img".
        """
        d = super().to_dict(*args, **kwargs)
        d["img"] = base64.b64encode(self.img).decode()
        return d
