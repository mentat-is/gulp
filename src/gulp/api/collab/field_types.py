"""
DB model for deduplicated field types used by sources.
"""
import json
from typing import Optional, Tuple

import muty.crypto
import muty.time
from muty.log import MutyLogger
from sqlalchemy import BIGINT, String, delete, exists, insert, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import Mapped, mapped_column

from gulp.api.collab.structs import COLLABTYPE_FIELD_TYPES, GulpCollabBase


class GulpFieldTypesEntry(GulpCollabBase, type=COLLABTYPE_FIELD_TYPES):
    """A deduplicated store for field types.

    Entries are keyed by an SHA1 hash of the canonical JSON serialization of the
    field types dict, so identical field types share the same row. This model
    derives from GulpCollabBase to follow existing collab objects semantics.
    """

    __tablename__ = "field_types_entries"

    # only store field_types content here; other common collab fields are inherited
    field_types: Mapped[dict] = mapped_column(
        MutableDict.as_mutable(JSONB), default_factory=dict, doc="The field types dict"
    )

    @classmethod
    async def _compute_id_from_field_types(cls, field_types: dict) -> str:
        # canonicalize mapping to JSON then hash it
        s = json.dumps(field_types, sort_keys=True, separators=(",", ":"))
        return muty.crypto.hash_sha1(s)

    @classmethod
    async def create_if_not_exists(
        cls, sess: AsyncSession, field_types: dict, user_id: Optional[str] = None
    ) -> Tuple["GulpFieldTypesEntry", bool]:
        """Create a field types entry if not exists. Returns (instance, created_bool)."""
        fp_id = await cls._compute_id_from_field_types(field_types)
        existing = await cls.get_by_id(sess, fp_id, throw_if_not_found=False)
        if existing:
            return existing, False

        # use create_internal to ensure proper collab semantics (user, ws notification, etc)
        obj: GulpFieldTypesEntry = await cls.create_internal(
            sess,
            user_id=user_id,
            name=f"field_types_{fp_id}",
            obj_id=fp_id,
            field_types=field_types,
        )
        MutyLogger.get_instance().debug("field types %s created by %s", obj.id, user_id)
        return obj, True

    @classmethod
    async def delete_orphaned(cls, sess: AsyncSession) -> int:
        """Delete field types entries with no referencing source_field_types and return deleted count."""
        # delete field_types_entries where not exists (select 1 from source_field_types where source_field_types.field_types_id = field_types_entries.id)
        from gulp.api.collab.source_field_types import GulpSourceFieldTypes

        delete_stmt = delete(cls).where(
            ~exists(
                select(GulpSourceFieldTypes.id).where(GulpSourceFieldTypes.field_types_id == cls.id)
            )
        )
        res = await sess.execute(delete_stmt)
        deleted = res.rowcount or 0
        if deleted:
            await sess.commit()
            MutyLogger.get_instance().debug("deleted %d orphan field types entries", deleted)
        return deleted
