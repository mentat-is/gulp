import base64
from typing import Optional, Union, override
import muty.file
from sqlalchemy import ForeignKey, LargeBinary, String
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.collab.structs import GulpCollabBase, GulpCollabType, T, GulpUserPermission
from gulp.utils import GulpLogger


class GulpGlyph(GulpCollabBase, type=GulpCollabType.GLYPH):
    """
    Represents a glyph object.
    """
    title: Mapped[Optional[str]] = mapped_column(
        String, doc="Display name for the glyph."
    )    
    img: Mapped[bytes] = mapped_column(
        LargeBinary, doc="The image data of the glyph as binary blob."
    )
    @override
    def __repr__(self) -> str:
        return super().__repr__() + f" img={self.img[:10]}[...]"

    @override
    def __init__(self, *args, **kwargs):
        # initializes the base class
        super().__init__(*args, type=GulpCollabType.GLYPH, **kwargs)

    @classmethod
    async def create(
        cls,
        token: str,
        id: str,
        img: bytes | str,
        title: str = None,
        **kwargs,
    ) -> T:
        """
        Create a new glyph object on the collab database.

        Args:
            token (str): The token of the user creating the object, for permission check (needs EDIT permission).
            id (str): The name of the glyph (must be unique and not containing spaces)
            img (bytes | str): The image data of the glyph as binary blob or file path.
            title (str, optional): The display name for the glyph. Defaults to id.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            T: The created glyph object
        """
        if isinstance(img, str):
            # load from file path
            img = await muty.file.read_file_async(img)

        args = {"img": img, "title": title or id,
                **kwargs}
        return await super()._create(
            token=token,
            id=id,
            required_permission=[GulpUserPermission.EDIT],
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
        d = super().to_dict(**kwargs, exclude_none=True)
        d["img"] = base64.b64encode(self.img).decode()
        return d
