import base64
from typing import override

import muty.file
from sqlalchemy import LargeBinary, String
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from gulp.api.collab.structs import (
    GulpCollabBase,
    GulpCollabType,
    T,
)


class GulpGlyph(GulpCollabBase, type=GulpCollabType.GLYPH):
    """
    Represents a glyph object.
    """

    name: Mapped[str] = mapped_column(String, doc="Display name for the glyph.")
    img: Mapped[bytes] = mapped_column(
        LargeBinary, doc="The image data of the glyph as binary blob."
    )

    @override
    @classmethod
    def example(cls) -> dict:
        d = super().example()
        d["name"] = "glyph_name"
        d["img"] = "base64_image_data"
        return d

    @override
    def __repr__(self) -> str:
        return super().__repr__() + f" img={self.img[:10]}[...]"

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
