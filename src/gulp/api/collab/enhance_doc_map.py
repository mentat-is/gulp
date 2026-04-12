from typing import Optional, override

from sqlalchemy import ForeignKey, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import Mapped, mapped_column

from gulp.api.collab.structs import COLLABTYPE_ENHANCE_DOCUMENT_MAP, GulpCollabBase


class GulpEnhanceDocumentMap(GulpCollabBase, type=COLLABTYPE_ENHANCE_DOCUMENT_MAP):
    """
    Maps a set of key-value criteria on a GulpDocument to a visual enhancement
    (color and/or glyph) for a specific plugin.

    ``match_criteria`` is a dict of field-name -> criteria-value pairs. All entries
    must match (AND semantics) for the enhancement to be applied by the UI.

    Each criterion value can be:
    - A simple value (string, number, boolean) for exact equality match.
    - A dict with operator keys for numeric comparisons:
      - ``"eq"``: exact equality
      - ``"gte"``: greater than or equal
      - ``"lte"``: less than or equal
      - Multiple operators can be combined in one dict (e.g., ``{"gte": 100, "lte": 200}`` for range).

    Example criteria:
    ```python
    {
        "gulp.event_code": {"eq": 4624},  # exact match
        "status": "active",  # simple string match
        "severity_level": {"gte": 5, "lte": 10},  # numeric range
        "request_count": {"gte": 100},  # greater than or equal
    }
    ```
    """
    match_criteria: Mapped[dict] = mapped_column(
        MutableDict.as_mutable(JSONB),
        doc="A dict mapping GulpDocument field names to criteria values. Values can be simple values (for exact match) or dicts with operator keys ('eq', 'gte', 'lte') for numeric comparisons. All entries must match (AND semantics).",
    )
    plugin: Mapped[str] = mapped_column(String, doc="The plugin whose documents this entry applies to.")
    color: Mapped[Optional[str]] = mapped_column(String, doc="The CSS hex color to apply when criteria match.")
    glyph_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("glyph.id", ondelete="SET NULL"),
        doc="The glyph ID to apply when criteria match.",
    )

    @override
    @classmethod
    def example(cls) -> dict:
        d = super().example()
        d["match_criteria"] = {
            "gulp.event_code": {"eq": 4624},
            "winlog.provider_name": "Microsoft-Windows-Security-Auditing",
            "severity_level": {"gte": 7, "lte": 10},
        }
        d["plugin"] = "win_evtx"
        d["color"] = "#ff0000"
        d["glyph_id"] = "example_glyph_id"
        return d
