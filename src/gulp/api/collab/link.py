from typing import override
from sqlalchemy import ARRAY, String
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.ext.mutable import MutableList
from gulp.api.collab.structs import GulpCollabObject, GulpCollabType


class GulpLink(GulpCollabObject, type=GulpCollabType.LINK):
    """
    a link in the gulp collaboration system
    """

    # the source document
    doc_id_from: Mapped[str] = mapped_column(String, doc="The source document ID.")
    # target documents
    doc_ids: Mapped[list[str]] = mapped_column(
        MutableList.as_mutable(ARRAY(String)),
        default_factory=list,
        doc="One or more target document IDs.",
    )

    @override
    @classmethod
    def example(cls) -> dict:
        d = super().example()
        d["doc_id_from"] = "source_doc_id"
        d["doc_ids"] = ["target_doc_id"]
        return d
