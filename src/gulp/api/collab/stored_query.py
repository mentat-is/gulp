from typing import Optional, override

from sqlalchemy import ARRAY, String
from sqlalchemy.ext.mutable import MutableList, MutableDict
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from gulp.api.collab.structs import GulpCollabBase, GulpCollabType


class GulpStoredQuery(GulpCollabBase, type=GulpCollabType.STORED_QUERY):
    """
    a stored query in the gulp collaboration system
    """

    q: Mapped[str] = mapped_column(
        String,
        doc="a query as string, i.e. YAML (i.e. sigma), JSON string, ...",
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
    plugin: Mapped[Optional[str]] = mapped_column(
        String,
        default=None,
        doc="If q is a sigma YAML, this is the plugin implementing `sigma_convert` to be used for conversion.",
    )
    plugin_params: Mapped[Optional[dict]] = mapped_column(
        MutableDict.as_mutable(JSONB),
        default_factory=dict,
        doc="Parameters to be passed to the plugin.",)

    @override
    @classmethod
    def example(cls) -> dict:
        d = super().example()
        d["q"] = ["example query"]
        
        d["plugin"] = "win_evtx"
        d["tags"] = ["example", "tag"]
        d["q_groups"] = ["group1", "group2"]
        return d
