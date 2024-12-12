from enum import StrEnum
from typing import Optional, override
from muty.pydantic import autogenerate_model_example_by_class
from pydantic import BaseModel
from sigma.rule import SigmaRule
from sqlalchemy import ARRAY, Boolean, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.ext.mutable import MutableList
from gulp.api.collab.structs import (
    GulpCollabBase,
    GulpCollabType,
    T,
)
from gulp.api.opensearch.query import (
    GulpQueryAdditionalParameters,
    GulpQuerySigmaParameters,
)


class GulpStoredQuery(GulpCollabBase, type=GulpCollabType.STORED_QUERY):
    """
    a stored query in the gulp collaboration system
    """

    q: Mapped[str] = (
        mapped_column(
            String,
            doc="a query as string: may be YAML, JSON string, text, depending on the query type.",
        ),
    )
    s_options: Mapped[Optional[GulpQuerySigmaParameters]] = mapped_column(
        JSONB,
        doc="""
this must be set if this is a sigma query.
""",
    )
    tags: Mapped[Optional[list[str]]] = mapped_column(
        ARRAY(String),
        doc="The tags associated with the query.",
    )
    q_groups: Mapped[Optional[list[str]]] = mapped_column(
        MutableList.as_mutable(ARRAY(String)),
        doc="Groups associated with the query.",
    )
    external_plugin: Mapped[Optional[str]] = mapped_column(
        String,
        default=None,
        doc="the external plugin name to use for this query, if this is an external query.",
    )

    @override
    @classmethod
    def example(cls) -> dict:
        d = super().example()
        d["q"] = ["example query"]
        d["q_options"] = autogenerate_model_example_by_class(
            GulpQueryAdditionalParameters
        )
        d["q_groups"] = ["group1", "group2"]
        d["external_plugin"] = None
        d["tags"] = ["example", "tag"]
        return d
