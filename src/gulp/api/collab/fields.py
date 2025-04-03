"""
This module defines the `GulpSourceFields` class, which represents the fields mapping for a data source.

The `GulpSourceFields` class inherits from `GulpCollabBase` and is used to store and manage
field mappings as returned by GulpOpenSearch.datastream_get_mapping_by_src. It maintains relationships
with operations, contexts, and sources through foreign keys.

Classes:
    GulpSourceFields: SQLAlchemy model representing field mappings for a source.

The module implements functionality for:
- Creating and updating field mappings
- Converting field mappings to dictionaries with "gulpesque" keys
- Acquiring advisory locks to prevent race conditions during creation
"""

from typing import override

import muty.crypto
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

    @override
    @classmethod
    # pylint: disable=arguments-differ
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
        Create a new GulpSourceFields object.

        If an object with the same operation_id, context_id, and source_id already exists,
        it will be updated instead.

        Args:
            sess (AsyncSession): The SQLAlchemy session.
            user_id (str): The ID of the user creating the object.
            operation_id (str): The ID of the operation.
            context_id (str): The ID of the context.
            source_id (str): The ID of the source.
            fields (dict, optional): The fields mapping. Defaults to None.

        Returns:
            GulpSourceFields: The created or updated GulpSourceFields object.
        Raises:
            Exception: If the object cannot be created or updated.
        """
        obj_id = muty.crypto.hash_xxh128(f"{operation_id}{context_id}{source_id}")

        MutyLogger.get_instance().debug(
            "---> create: id=%s, operation_id=%s, context_id=%s, source_id=%s, # of fields=%d",
            obj_id,
            operation_id,
            context_id,
            source_id,
            len(fields),
        )

        try:
            await GulpSourceFields.acquire_advisory_lock(sess, obj_id)

            # check if the the source fields entry already exists
            s: GulpSourceFields = await cls.get_by_id(
                sess, obj_id=obj_id, throw_if_not_found=False
            )
            if s:
                # update existing
                MutyLogger.get_instance().debug(
                    "---> create source fields, update: id=%s, operation_id=%s, context_id=%s, source_id=%s, # of fields=%d",
                    obj_id,
                    operation_id,
                    context_id,
                    source_id,
                    len(fields),
                )
                s.fields = fields
                await sess.commit()
                return s

            # create new
            object_data = {
                "operation_id": operation_id,
                "context_id": context_id,
                "source_id": source_id,
                "fields": fields,
            }
            return await super()._create_internal(
                sess,
                object_data=object_data,
                obj_id=obj_id,
                owner_id=user_id,
                private=False,
            )
        finally:
            await GulpSourceFields.release_advisory_lock(sess, obj_id)
