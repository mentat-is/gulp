"""
Module for representing and manipulating glyphs in the Gulp collaboration system.

This module defines the GulpGlyph class which represents a glyph object in the system.
Glyphs are stored with their binary image data and can be converted to dictionaries
with base64-encoded image data for serialization purposes.

Classes:
    GulpGlyph: A class representing a glyph with image data.
"""

import base64
from typing import Optional, override

from sqlalchemy import LargeBinary
from sqlalchemy.orm import Mapped, mapped_column

from gulp.api.collab.structs import COLLABTYPE_GLYPH, GulpCollabBase


class GulpGlyph(GulpCollabBase, type=COLLABTYPE_GLYPH):
    """
    Represents a glyph object.
    """

    img: Optional[Mapped[bytes]] = mapped_column(
        LargeBinary, doc="The image data of the glyph as binary blob."
    )

    @override
    @classmethod
    def example(cls) -> dict:
        d = super().example()
        d["img"] = "base64_image_data"
        return d

    @override
    def __repr__(self) -> str:
        return super().__repr__() + f" img={self.img[:10]}[...]"

    @override
    def to_dict(
        self,
        nested: bool = False,
        hybrid_attributes: bool = False,
        exclude: list[str] | None = None,
        exclude_none: bool = False,
    ) -> dict:
        d = super().to_dict(
            nested=nested,
            hybrid_attributes=hybrid_attributes,
            exclude=exclude,
            exclude_none=exclude_none,
        )
        if self.img:
            d["img"] = base64.b64encode(self.img).decode()
        return d
