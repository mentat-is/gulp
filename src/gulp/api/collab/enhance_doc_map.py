from typing import Annotated, Optional, override

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import ARRAY, ForeignKey, Integer, String
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.orm import Mapped, mapped_column

from gulp.api.collab.structs import COLLABTYPE_ENHANCE_DOCUMENT_MAP, GulpCollabBase

class GulpEnhanceDocumentMap(GulpCollabBase, type=COLLABTYPE_ENHANCE_DOCUMENT_MAP):
    """
    """
    gulp_event_code: Mapped[int] = mapped_column(Integer, doc="The `gulp.event_code` to enhance.")
    plugin: Mapped[str] = mapped_column(String, doc="The plugin to enhance the `gulp.event_code` in.")
    color: Mapped[Optional[str]] = mapped_column(String, doc="The color to use for the given `gulp.event_code`.")
    glyph_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("glyph.id", ondelete="SET NULL"),
        doc="The glyph ID associated with the object.",
    )

    @override
    @classmethod
    def example(cls) -> dict:
        d = super().example()
        d["gulp_event_code"] = 123456
        d["plugin"] = "example_plugin"
        d["color"] = "#ff0000"
        d["glyph_id"] = "example_glyph_id"
        return d
