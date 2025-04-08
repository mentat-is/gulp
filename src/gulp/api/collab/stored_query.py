"""
This module defines the GulpStoredQuery class for the gulp collaboration system.

The GulpStoredQuery class extends GulpCollabBase to represent stored queries
that can be shared and reused within the system. Stored queries can be in various
formats such as YAML (like Sigma rules) or JSON strings, and can be associated
with tags, groups, and specific plugins for processing.

Key features:
- Stores query strings in various formats
- Supports tagging for organization
- Associates queries with groups for access control
- Provides plugin integration for query processing
- Includes customizable plugin parameters

The class is integrated with SQLAlchemy for database persistence and includes
type hints for better code completeness.
"""

from typing import Optional, override
from sqlalchemy import ARRAY, Boolean, String
from sqlalchemy.ext.mutable import MutableList, MutableDict
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import JSONB
from gulp.api.collab.structs import GulpCollabBase, GulpCollabType


class GulpStoredQuery(GulpCollabBase, type=GulpCollabType.STORED_QUERY):
    """
    a stored query in the gulp collaboration system
    """

    q: Mapped[str] = mapped_column(
        String,
        doc="a query as string, i.e. YAML (i.e. sigma), JSON string, ...",
    )
    sigma: Mapped[Optional[bool]] = mapped_column(
        Boolean,
        doc="is the query a sigma rule ?",
    )
    tags: Mapped[Optional[list[str]]] = mapped_column(
        MutableList.as_mutable(ARRAY(String)),
        default_factory=list,
        doc="The tags associated with the query.",
    )
    q_groups: Mapped[Optional[list[str]]] = mapped_column(
        MutableList.as_mutable(ARRAY(String)),
        default_factory=list,
        doc="Groups associated with the query.",
    )

    @override
    @classmethod
    def example(cls) -> dict:
        d = super().example()
        d["q"] = ["example query"]
        d["tags"] = ["example", "tag"]
        d["q_groups"] = ["group1", "group2"]
        return d
