from typing import override
from sqlalchemy import ARRAY, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.ext.mutable import MutableList
from gulp.api.collab.structs import GulpCollabObject, GulpCollabType
from gulp.api.opensearch.structs import GulpBasicDocument


class GulpStory(GulpCollabObject, type=GulpCollabType.STORY):
    """
    a story in the gulp collaboration system
    """

    doc_ids: Mapped[list[str]] = mapped_column(
        MutableList.as_mutable(ARRAY(String)),
        default_factory=list,
        doc="one or more document IDs associated with the story.",
    )

    @override
    @classmethod
    def example(cls) -> dict:
        d = super().example()
        d["doc_ids"] = ["doc_id1", "doc_id2"]
        return d
