"""
DB model for deduplicated mapping parameters used by sources.
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

from gulp.api.collab.structs import COLLABTYPE_MAPPING_PARAMETERS, GulpCollabBase


class GulpMappingParametersEntry(GulpCollabBase, type=COLLABTYPE_MAPPING_PARAMETERS):
    """A deduplicated store for mapping parameters.

    Entries are keyed by an SHA1 hash of the canonical JSON serialization of the
    mapping parameters dict, so identical mappings share the same row. This model
    derives from GulpCollabBase to follow existing collab objects semantics.
    """

    __tablename__ = "mapping_parameters"

    # only store mapping content here; other common collab fields are inherited
    mapping: Mapped[dict] = mapped_column(
        MutableDict.as_mutable(JSONB), default_factory=dict, doc="The mapping dict"
    )

    @classmethod
    async def _compute_id_from_mapping(cls, mapping: dict) -> str:
        # canonicalize mapping to JSON then hash it
        s = json.dumps(mapping, sort_keys=True, separators=(",", ":"))
        return muty.crypto.hash_sha1(s)

    @classmethod
    async def create_if_not_exists(
        cls, sess: AsyncSession, mapping: dict, user_id
    ) -> Tuple["GulpMappingParametersEntry", bool]:
        """Create a mapping parameters entry if not exists. Returns (instance, created_bool)."""
        mp_id = await cls._compute_id_from_mapping(mapping)
        existing = await cls.get_by_id(sess, mp_id, throw_if_not_found=False)
        if existing:
            return existing, False

        # use create_internal to ensure proper collab semantics (user, ws notification, etc)
        mp = await cls.create_internal(
            sess,
            user_id=user_id,
            name=f"mapping_{mp_id}",
            obj_id=mp_id,
            mapping=mapping,
        )
        MutyLogger.get_instance().debug("mapping parameters %s created by %s", mp.id, user_id)
        return mp, True

    @classmethod
    async def delete_orphaned(cls, sess: AsyncSession) -> int:
        """Delete mapping entries with no referencing sources and return deleted count."""
        # delete mapping_parameters where not exists (select 1 from source where source.mapping_parameters_id = mapping_parameters.id)
        from gulp.api.collab.source import GulpSource
        await cls.acquire_advisory_lock(sess, "__del_orphaned_mapping_parameters__")                
        delete_stmt = delete(cls).where(
            ~exists(select(GulpSource.id).where(GulpSource.mapping_parameters_id == cls.id))
        )
        res = await sess.execute(delete_stmt)
        deleted = res.rowcount or 0
        if deleted:
            await sess.commit()
            MutyLogger.get_instance().debug("deleted %d orphan mapping parameters", deleted)
        return deleted
