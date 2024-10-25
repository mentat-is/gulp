import base64
from typing import override

from sqlalchemy import ForeignKey, LargeBinary
from sqlalchemy.orm import Mapped, mapped_column

from gulp.api.collab.structs import (
    GulpCollabBase,
    GulpCollabType,
)
from gulp.utils import logger


class GulpGlyph(GulpCollabBase):
    """
    Represents a glyph object.

    Attributes:
        id (int): The unique identifier of the glyph (name).
        img (bytes): The image data of the glyph as binary blob.
    """

    __tablename__ = "glyph"
    id: Mapped[str] = mapped_column(
        ForeignKey("collab_base.id"),
        primary_key=True,
        doc="The unique identifier (name) of the glyph.",
    )
    img: Mapped[bytes] = mapped_column(
        LargeBinary, doc="The image data of the glyph as binary blob."
    )

    __mapper_args__ = {
        "polymorphic_identity": "glyph",
    }

    @override
    def __init__(self, id: str, img: bytes | str) -> None:
        """
        Initialize a GulpGlyph instance.
        Args:
            id (str): The identifier for the glyph.
            img (bytes | str): The image data as bytes or a file path as a string.
        Raises:
            FileNotFoundError: If the file path provided in img does not exist.
            IOError: If there is an error reading the file.
        """

        super().__init__(id, GulpCollabType.GLYPH)
        if isinstance(img, str):
            # load from file path
            with open(img, "rb") as f:
                img = f.read()

        self.img = img
        logger().debug("---> GulpGlyph: img=%s..." % (img[0:8]))

    @override
    def to_dict(self, *args, **kwargs) -> dict:
        """
        Convert the object to a dictionary representation.
        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        Returns:
            dict: A dictionary representation of the object, including a base64 encoded image.
        """
        d = super().to_dict(*args, **kwargs)
        d["img"] = base64.b64encode(self.img).decode()
        return d
