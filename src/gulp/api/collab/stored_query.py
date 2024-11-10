from typing import Optional, Union, override
from sqlalchemy import BIGINT, Boolean, ForeignKey, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.collab.structs import GulpCollabBase, GulpCollabObject, GulpCollabType, T, GulpUserPermission
from gulp.utils import GulpLogger
from sigma.rule import SigmaRule

class GulpStoredQuery(GulpCollabBase, type=GulpCollabType.STORED_QUERY):
    """
    a stored query in the gulp collaboration system
    """
    title: Mapped[str] = mapped_column(
        String, doc="The query display name.",
    )
    sigma: Mapped[Optional[bool]] = mapped_column(
        Boolean,
        doc="Whether the query is a sigma query.",
    )
    text: Mapped[Optional[str]] = mapped_column(
        String, doc="The text of the query in the original format, stringified.",
    )
    description: Mapped[Optional[str]] = mapped_column(
        String, doc="The description of the query.",
    )
    glyph: Mapped[Optional[str]] = mapped_column(
        String, doc="ID of a glyph to associate with the query.",
    )
    
    @override
    def __init__(self, *args, **kwargs):
        # initializes the base class
        super().__init__(*args, type=GulpCollabType.STORED_QUERY, **kwargs)

    @classmethod
    async def create(
        cls,
        token: str,
        text: str,
        sigma: bool = False,
        title: str = None,
        description: str = None,
        glyph: str = None,
        **kwargs,
    ) -> T:
        """
        Create a new stored query object on the collab database.

        Args:
            token(str): the token of the user creating the object, for access check (needs EDIT permission)
            id(str): the id of the stored query
            text(str): the text of the query in the original format, stringified. Defaults to None.
            sigma(bool, optional): whether the query is a sigma query. Defaults to False.
            title(str, optional): the title of the query. Defaults to None.
            description(str, optional): the description of the query. Defaults to None.
            glyph(str, optional): the ID of a glyph to associate with the query. Defaults to None.
            kwargs: additional arguments

        Returns:
            the created stored query object
        """
        args = {
            "sigma": sigma,
            "text": text,
            "title": title,
            "description": description,
            "glyph": glyph,
            **kwargs,
        }
        if sigma:
            # take from sigma rule
            r = SigmaRule.from_yaml(text)
            id = r.id
        else:
            # autogenerate
            id = None  

        return await super()._create(
            token=token,
            id = id,
            required_permission=[GulpUserPermission.EDIT],
            **args,
        )
