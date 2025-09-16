"""
This module defines the `GulpSourceFieldTypes` class, which represents the types of each field in the corresponding data source.
"""

from typing import override

import muty.crypto
from muty.log import MutyLogger
from sqlalchemy import ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import Mapped, mapped_column

from gulp.api.collab.structs import COLLABTYPE_SOURCE_FIELD_TYPES, GulpCollabBase, T


class GulpSourceFieldTypes(GulpCollabBase, type=COLLABTYPE_SOURCE_FIELD_TYPES):
    """
    represents the field types present in a source on OpenSearch, as returned by GulpOpenSearch.datastream_get_field_types_by_src
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
    field_types: Mapped[dict] = mapped_column(
        MutableDict.as_mutable(JSONB),
        default_factory=dict,
        doc="""
a dict representing the type of each field ingested in this source.

{
    "field1": "type",
    "field2": "type",
    ...
}
""",
    )

    @override
    def to_dict(
        nested: bool = False,
        hybrid_attributes: bool = False,
        exclude: list[str] | None = None,
        exclude_none: bool = False,
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
        raise TypeError("Use create_source_fields instead")

    @classmethod
    async def create_source_field_types(
        cls,
        sess: AsyncSession,
        user_id: str,
        operation_id: str,
        context_id: str,
        source_id: str,
        field_types: dict,
    ) -> T:
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
            GulpSourceFieldTypes: The created or updated GulpSourceFields object.
        Raises:
            Exception: If the object cannot be created or updated.
        """
        obj_id = muty.crypto.hash_xxh128(f"{operation_id}{context_id}{source_id}")

        MutyLogger.get_instance().debug(
            "---> create: id=%s, operation_id=%s, context_id=%s, source_id=%s, # of fieldtypes=%d",
            obj_id,
            operation_id,
            context_id,
            source_id,
            len(field_types),
        )

        try:
            await GulpSourceFieldTypes.acquire_advisory_lock(sess, obj_id)

            # check if the the source fields entry already exists
            src_field_types: GulpSourceFieldTypes = await cls.get_by_id(
                sess, obj_id=obj_id, throw_if_not_found=False
            )
            if src_field_types:
                # update existing
                MutyLogger.get_instance().debug(
                    "---> updating source_field_types: id=%s, operation_id=%s, context_id=%s, source_id=%s, # of fields=%d",
                    obj_id,
                    operation_id,
                    context_id,
                    source_id,
                    len(field_types),
                )
                src_field_types.field_types = field_types
                await src_field_types.update(sess)
                return src_field_types

            # create new
            object_data = {
                "context_id": context_id,
                "source_id": source_id,
                "field_types": field_types,
            }
            await cls.create_internal(
                sess,
                user_id,
                operation_id=operation_id,
                private=False,
                **object_data,
            )
            await sess.commit()
        except Exception as e:
            await sess.rollback()
            raise e
