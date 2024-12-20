from typing import Optional, override
from muty.pydantic import autogenerate_model_example_by_class
from sqlalchemy import ARRAY, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.ext.mutable import MutableList, MutableDict
from gulp.api.collab.structs import (
    GulpCollabBase,
    GulpCollabType,
)
from gulp.api.mapping.models import GulpMapping
from gulp.structs import GulpPluginParameters
from gulp.api.opensearch.query import GulpQuerySigmaParameters
from sqlalchemy.dialects.postgresql import JSONB


class GulpStoredQuery(GulpCollabBase, type=GulpCollabType.STORED_QUERY):
    """
    a stored query in the gulp collaboration system
    """

    s_options: Mapped[Optional[GulpQuerySigmaParameters]] = mapped_column(
        MutableDict.as_mutable(JSONB),
        doc="""
this must be set if this is a sigma query.
""",
    )
    tags: Mapped[Optional[list[str]]] = mapped_column(
        MutableList.as_mutable(ARRAY(String)),
        doc="The tags associated with the query.",
    )
    q_groups: Mapped[Optional[list[str]]] = mapped_column(
        MutableList.as_mutable(ARRAY(String)),
        doc="Groups associated with the query.",
    )
    plugin_params: Mapped[Optional[GulpPluginParameters]] = mapped_column(
        MutableDict.as_mutable(JSONB),
        doc="Custom plugin parameters.",
    )
    q: Mapped[str] = mapped_column(
        String,
        doc="a query as string: may be YAML, JSON string, text, depending on the query type.",
    )

    external_plugin: Mapped[Optional[str]] = mapped_column(
        String,
        default=None,
        doc="the external plugin name to use for this query, if this is an external query.",
    )

    @override
    @classmethod
    def example(cls) -> dict:
        from gulp.api.opensearch.query import GulpQuerySigmaParameters

        d = super().example()
        d["q"] = ["example query"]
        d["s_options"] = autogenerate_model_example_by_class(GulpQuerySigmaParameters)
        d["tags"] = ["example", "tag"]
        d["q_groups"] = ["group1", "group2"]
        d["external_plugin"] = "test"
        d["plugin_params"] = {
            "mapping_file": "mftecmd_csv.json",
            "mappings": autogenerate_model_example_by_class(GulpMapping),
            "mapping_id": "record",
            "my_custom_param": "example",
        }
        return d
