"""
Module for handling story objects in the GULP collaborative system.

A story in GULP represents a collection of document references, providing
a way to group and organize related documents within the collaboration system.

This module defines the GulpStory class which inherits from GulpCollabObject
and is specifically typed as a STORY in the collaboration type system.

"""
from typing import override

from sqlalchemy import ARRAY, String
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.orm import Mapped, mapped_column

from gulp.api.collab.structs import GulpCollabObject, GulpCollabType


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
