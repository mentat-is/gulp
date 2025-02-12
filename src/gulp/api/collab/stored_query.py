from typing import Optional, override

from sqlalchemy import ARRAY, String
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.orm import Mapped, mapped_column

from gulp.api.collab.structs import GulpCollabBase, GulpCollabType


class GulpStoredQuery(GulpCollabBase, type=GulpCollabType.STORED_QUERY):
    """
    a stored query in the gulp collaboration system
    """

    tags: Mapped[Optional[list[str]]] = mapped_column(
        MutableList.as_mutable(ARRAY(String)),
        doc="The tags associated with the query.",
    )
    q_groups: Mapped[Optional[list[str]]] = mapped_column(
        MutableList.as_mutable(ARRAY(String)),
        doc="Groups associated with the query.",
    )
    q: Mapped[str] = mapped_column(
        String,
        doc="a query as string: may be YAML (for sigma) or JSON string, text, depending on the query type.",
    )
    plugin: Mapped[str] = mapped_column(
        String,
        doc="If q is a sigma YAML, this is the plugin implementing `sigma_convert` to be used for conversion.",
    )

    @override
    @classmethod
    def example(cls) -> dict:
        d = super().example()
        d["q"] = ["example query"]
        d["plugin"] = "win_evtx"
        d["tags"] = ["example", "tag"]
        d["q_groups"] = ["group1", "group2"]
        return d
