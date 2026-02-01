"""
This module defines the `GulpSourceFieldTypes` class, which represents the types of each field in the corresponding data source.
"""

from typing import override

import muty.crypto
from muty.log import MutyLogger
from sqlalchemy import ForeignKey
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from gulp.api.collab.structs import COLLABTYPE_SOURCE_FIELD_TYPES, GulpCollabBase, T


class GulpSourceFieldTypes(GulpCollabBase, type=COLLABTYPE_SOURCE_FIELD_TYPES):
    """
    associates a GulpSource with its field types: represents the field types present in a source on OpenSearch, as returned by GulpOpenSearch.datastream_get_field_types_by_src
    """

    operation_id: Mapped[str] = mapped_column(
        ForeignKey("operation.id", ondelete="CASCADE"),
        doc="The associable operation.",
    )
    context_id: Mapped[str] = mapped_column(
        ForeignKey("context.id", ondelete="CASCADE"),
        doc="The associated GulpContext, if any.",
        nullable=True,
    )
    source_id: Mapped[str] = mapped_column(
        ForeignKey("source.id", ondelete="CASCADE"),
        doc="The associated GulpSource, if any.",
        nullable=True,
    )
    # deduplicated reference to `field_types_entries` table storing the actual dicts
    field_types_id: Mapped[str] = mapped_column(
        ForeignKey("field_types_entries.id", ondelete="SET NULL"),
        doc="Reference to a GulpFieldTypesEntry id",
        nullable=True,
    )

    async def expand_field_types(self, sess: AsyncSession) -> dict:
        """Return the actual field types dict for this source.

        - If `field_types_id` is present, fetch the corresponding entry and return it.
        - Otherwise return an empty dict.
        """
        if self.field_types_id:
            from gulp.api.collab.field_types_entry import GulpFieldTypesEntry

            entry = await GulpFieldTypesEntry.get_by_id(
                sess, self.field_types_id, throw_if_not_found=False
            )
            if entry:
                return entry.field_types

        return {}

    @override
    def to_dict(
        self,
        nested: bool = False,
        hybrid_attributes: bool = False,
        exclude: list[str] | None = None,
        exclude_none: bool = True,
    ) -> dict:
        # override to have 'gulpesque' keys
        d = super().to_dict(
            nested=nested,
            hybrid_attributes=hybrid_attributes,
            exclude=exclude,
            exclude_none=exclude_none,
        )
        if "operation_id" in d:
            d["gulp.operation_id"] = d.pop("operation_id")
        if "context_id" in d:
            d["gulp.context_id"] = d.pop("context_id")
        if "source_id" in d:
            d["gulp.source_id"] = d.pop("source_id")
        return d

    @override
    @classmethod
    async def create(cls, *args, **kwargs):
        raise TypeError("Use create_or_update_source_field_types instead")

    @classmethod
    async def create_or_update_source_field_types(
        cls,
        sess: AsyncSession,
        user_id: str,
        operation_id: str,
        context_id: str,
        source_id: str,
        field_types: dict,
    ) -> dict:
        """
        Creates (or updates an existing) a new GulpSourceFieldTypes object.

        If an object with the same operation_id, context_id, and source_id already exists,
        it will be updated instead.

        Args:
            sess (AsyncSession): The SQLAlchemy session.
            user_id (str): The ID of the user creating the object.
            operation_id (str): The ID of the operation.
            context_id (str): The ID of the context.
            source_id (str): The ID of the source.
            field_types (dict): The field->type mappings, i.e.  {"field1": "type", "field2": "type", ...}

        Returns:
            dict: The created or updated GulpSourceFieldTypes object as a dictionary.
        Raises:
            Exception: If the object cannot be created or updated.
        """
        obj_id = muty.crypto.hash_xxh128(f"{operation_id}{context_id}{source_id}")

        await GulpSourceFieldTypes.acquire_advisory_lock(sess, obj_id)

        # check if the the source fields entry already exists
        src_field_types: GulpSourceFieldTypes = await cls.get_by_id(
            sess, obj_id, throw_if_not_found=False
        )
        # create or get deduplicated entry for these field_types
        from gulp.api.collab.field_types_entry import GulpFieldTypesEntry

        entry, created = await GulpFieldTypesEntry.create_if_not_exists(
            sess, field_types, user_id
        )

        if src_field_types:
            # already exists, update it to point to the deduplicated entry
            MutyLogger.get_instance().debug(
                "---> updating source_field_types: id=%s, operation_id=%s, context_id=%s, source_id=%s, # of fields=%d",
                obj_id,
                operation_id,
                context_id,
                source_id,
                len(field_types),
            )
            # prefer writing the reference id; keep legacy field_types column untouched for now
            src_field_types.field_types_id = entry.id
            await src_field_types.update(sess)
            # return expanded dict representation
            d = src_field_types.to_dict()
            d["field_types"] = entry.field_types
            return d

        MutyLogger.get_instance().debug(
            "---> create source_field_types: id=%s, operation_id=%s, context_id=%s, source_id=%s, # of fieldtypes=%d",
            obj_id,
            operation_id,
            context_id,
            source_id,
            len(field_types),
        )

        # create new, store only the reference to the deduplicated entry
        obj: GulpSourceFieldTypes = await cls.create_internal(
            sess,
            user_id,
            operation_id=operation_id,
            obj_id=obj_id,
            private=False,
            context_id=context_id,
            source_id=source_id,
            field_types_id=entry.id,
        )
        d = obj.to_dict()
        d["field_types"] = entry.field_types
        return d
