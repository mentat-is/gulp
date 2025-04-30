"""
Link module for the gulp collaboration system.

This module defines the GulpLink class, which represents a link between documents
in the collaboration system. A link connects a source document to one or more target
documents, enabling relationships and references between content.

Links are fundamental to establishing connections in the collaboration graph, allowing
users to create associations between related pieces of information.
"""
from typing import override

from sqlalchemy import ARRAY, String
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.orm import Mapped, mapped_column

from gulp.api.collab.structs import COLLABTYPE_LINK, GulpCollabObject


class GulpLink(GulpCollabObject, type=COLLABTYPE_LINK):
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
