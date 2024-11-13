from typing import Optional, override
from sqlalchemy import ARRAY, Boolean, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from gulp.api.collab.structs import GulpCollabBase, GulpCollabType, T, GulpUserPermission
from sigma.rule import SigmaRule

class GulpStoredQuery(GulpCollabBase, type=GulpCollabType.STORED_QUERY):
    """
    a stored query in the gulp collaboration system
    """
    title: Mapped[str] = mapped_column(
        String, doc="The query display name.",
    )
    text: Mapped[str] = mapped_column(
        String, doc="The query in its original format, as string.",
    )
    tags: Mapped[Optional[list[str]]] = mapped_column(
        ARRAY(String),
        doc="The tags associated with the query.",
    )
    description: Mapped[Optional[str]] = mapped_column(
        String, doc="The description of the query.",
    )
    glyph: Mapped[Optional[str]] = mapped_column(
        String, doc="ID of a glyph to associate with the query.",
    )
    converted: Mapped[Optional[str]] = mapped_column(
        String, doc="If present, the query converted in the native format, as string.",
        default=None,
    )
    
    @override
    def __init__(self, *args, **kwargs):
        # initializes the base class
        super().__init__(*args, type=GulpCollabType.STORED_QUERY, **kwargs)

    @classmethod
    async def create(
        cls,
        token: str,
        title: str,
        text: str,
        converted: any = None,
        tags: list[str] = None,
        description: str = None,
        glyph: str = None,
        **kwargs,
    ) -> T:
        """
        Create a new stored query object on the collab database.

        Args:
            token(str): the token of the user creating the object, for access check (needs EDIT permission)
            title(str, optional): the title of the query. Defaults to None.
            text(str): the text of the query in the original format, stringified. Defaults to None.
            converted(any, optional): the converted query, if any. Defaults to None.
            tags(list[str], optional): the tags associated with the query. Defaults to None.
                for sigma rules, use "sigma" tag to store the query with id = rule.id
            description(str, optional): the description of the query. Defaults to None.
            glyph(str, optional): the ID of a glyph to associate with the query. Defaults to None.
            kwargs: additional arguments

        Returns:
            the created stored query object
        """
        args = {
            "title": title,
            "text": text,
            "converted": converted,
            "tags": tags,
            "description": description,
            "glyph": glyph,
            **kwargs,
        }
        if "sigma" in tags:
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
