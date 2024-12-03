from typing import Optional, override

from sigma.rule import SigmaRule
from sqlalchemy import ARRAY, String
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from gulp.api.collab.structs import (
    GulpCollabBase,
    GulpCollabType,
    T,
)


class GulpStoredQuery(GulpCollabBase, type=GulpCollabType.STORED_QUERY):
    """
    a stored query in the gulp collaboration system
    """

    name: Mapped[str] = mapped_column(
        String,
        doc="The query display name.",
    )
    text: Mapped[str] = mapped_column(
        String,
        doc="The query in its original format, as string.",
    )
    tags: Mapped[Optional[list[str]]] = mapped_column(
        ARRAY(String),
        doc="The tags associated with the query.",
    )
    description: Mapped[Optional[str]] = mapped_column(
        String,
        doc="The description of the query.",
    )
    glyph_id: Mapped[Optional[str]] = mapped_column(
        String,
        doc="ID of a glyph to associate with the query.",
    )
    converted: Mapped[Optional[str]] = mapped_column(
        String,
        doc="The query converted in a format suitable for the target, as string.",
        default=None,
    )

    @override
    @classmethod
    def example(cls) -> dict:
        d = super().example()
        d.update(
            {
                "name": "query_name",
                "text": "the_query_as_text",
                "tags": ["tag1", "tag2"],
                "description": "query_description",
                "glyph_id": "glyph_id",
                "converted": "the_converted_query_as_text",
            }
        )
        return d
    @classmethod
    async def create(
        cls,
        sess: AsyncSession,
        user_id: str,
        name: str,
        text: str,
        converted: any = None,
        tags: list[str] = None,
        description: str = None,
        glyph_id: str = None,
    ) -> T:
        """
        Create a new stored query object on the collab database.

        Args:
            sess (AsyncSession): The database session.
            user_id (str): The ID of the user creating the object.
            name (str): The query display name.
            text (str): The query in its original format, as string.
            converted (any, optional): The query converted in a format suitable for the target, as string. Defaults to None.
            tags (list[str], optional): The tags associated with the query. Defaults to None.
            description (str, optional): The description of the query. Defaults to None.
            glyph_id (str, optional): ID of a glyph to associate with the query. Defaults to None.

        Returns:
            the created stored query object
        """
        object_data = {
            "name": name,
            "text": text,
            "converted": converted,
            "tags": tags,
            "description": description,
            "glyph_id": glyph_id,
        }
        if "sigma" in tags:
            # take id from sigma rule
            r = SigmaRule.from_yaml(text)
            id = r.id
        else:
            # autogenerate
            id = None

        return await super()._create(
            sess,
            object_data,
            owner_id=user_id,
            id=id,
        )
