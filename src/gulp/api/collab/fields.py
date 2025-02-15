from typing import override

import muty.crypto
import muty.log
import muty.time
from muty.log import MutyLogger
from sqlalchemy import ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import Mapped, mapped_column

from gulp.api.collab.structs import GulpCollabBase, GulpCollabType, T


class GulpSourceFields(GulpCollabBase, type=GulpCollabType.SOURCE_FIELDS):
    """
    represents the fields mapping for a source, as returned by GulpOpenSearch.datastream_get_mapping_by_src
    """

    operation_id: Mapped[str] = mapped_column(
        ForeignKey("operation.id", ondelete="CASCADE"),
        doc="The associable operation.",
    )
    context_id: Mapped[str] = mapped_column(
        ForeignKey("context.id", ondelete="CASCADE"),
        doc="The associated context.",
    )
    source_id: Mapped[str] = mapped_column(
        ForeignKey("source.id", ondelete="CASCADE"),
        doc="The associated source.",
    )
    fields: Mapped[dict] = mapped_column(
        MutableDict.as_mutable(JSONB),
        default_factory=dict,
        doc="The fields mapping.",
    )

    @override
    def to_dict(
        self, nested=False, hybrid_attributes=False, exclude=None, exclude_none=False
    ):
        # override to have 'gulpesque' keys
        d = super().to_dict(nested, hybrid_attributes, exclude, exclude_none)
        if "operation_id" in d:
            d["gulp.operation_id"] = d.pop("operation_id")
        if "context_id" in d:
            d["gulp.context_id"] = d.pop("context_id")
        if "source_id" in d:
            d["gulp.source_id"] = d.pop("source_id")
        return d

    @classmethod
    async def create(
        cls,
        sess: AsyncSession,
        user_id: str,
        operation_id: str,
        context_id: str,
        source_id: str,
        fields: dict = None,
    ) -> T:
        """
        """
        obj_id = muty.crypto.hash_xxh128(
            f"{operation_id}{context_id}{source_id}"
        )

        MutyLogger.get_instance().debug(
            "---> create: id=%s, operation_id=%s, context_id=%s, source_id=%s, # of fields=%d",
            obj_id,
            operation_id,
            context_id,
            source_id,
            len(fields),
        )

        # acquire an advisory lock
        lock_id = muty.crypto.hash_xxh64_int(obj_id)
        await GulpCollabBase.acquire_advisory_lock(sess, lock_id)

        # check if the stats already exist
        s: GulpSourceFields = await cls.get_by_id(
            sess, id=obj_id, throw_if_not_found=False
        )
        if s:
            # update existing
            MutyLogger.get_instance().debug(
                "---> update: id=%s, operation_id=%s, context_id=%s, source_id=%s, # of fields=%d",
                obj_id,
                operation_id,
                context_id,
                source_id,
                len(fields),
            )
            s.fields = fields
            await sess.commit()
            return s

        object_data = {
            "operation_id": operation_id,
            "context_id": context_id,
            "source_id": source_id,
            "fields": fields,
        }
        return await super()._create(
            sess,
            object_data=object_data,
            id=obj_id,
            owner_id=user_id,
            private=False
        )
