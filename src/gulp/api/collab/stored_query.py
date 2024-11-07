from typing import Optional, Union, override
from sqlalchemy import BIGINT, Boolean, ForeignKey, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.collab.structs import GulpCollabBase, GulpCollabObject, GulpCollabType, T, GulpUserPermission
from gulp.utils import GulpLogger


class GulpStoredQuery(GulpCollabBase, type=GulpCollabType.STORED_QUERY):
    """
    a stored query in the gulp collaboration system
    """
    dsl: Mapped[dict] = mapped_column(
        JSONB,
        doc="The query in OpenSearch DSL format, ready to be used by the opensearch query api.",
    )
    sigma: Mapped[Optional[bool]] = mapped_column(
        Boolean,
        default=False,
        doc="Whether the query is a sigma query.",
    )
    text: Mapped[Optional[str]] = mapped_column(
        String, doc="The text of the query in the original format.",
        default=None,
    )
    description: Mapped[Optional[str]] = mapped_column(
        String, doc="Query description.",
        default=None,
    )
    
    
    @classmethod
    async def create(
        cls,
        id: str,
        dsl: dict,
        sigma: bool = False,
        text: str = None,
        description: str = None,        
        token: str = None,
        **kwargs,
    ) -> T:
        """
        Create a new stored query object

        Args:
            id(str): the id of the stored query
            dsl(dict): the query in OpenSearch DSL format
            sigma(bool, optional): whether the query is a sigma query. Defaults to False.
            text(str, optional): the text of the query in the original format. Defaults to None.
            description(str, optional): query description. Defaults to None.
            token(str, optional): the token of the user creating the object, for access check (needs EDIT permission)
            kwargs: additional arguments

        Returns:
            the created stored query object
        """
        args = {
            "dsl": dsl,
            "sigma": sigma,
            "text": text,
            "description": description,
            **kwargs,
        }
        return await super()._create(
            id,
            token=token,
            required_permission=[GulpUserPermission.EDIT],
            **args,
        )
