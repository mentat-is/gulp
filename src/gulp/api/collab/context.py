"""
This module defines the GulpContext class, which represents a context object in the GULP system.

A context in GULP terms is used to group a set of data coming from the same host. It's always
associated with an operation, and the tuple composed by the two is unique.

The GulpContext class provides functionality for:
- Creating context and source IDs using cryptographic hashing
- Adding sources to a context with proper locking and permissions handling
- Managing the relationship between contexts, operations, and sources

Classes:
    GulpContext: A class representing a context object within the collaboration system
"""

from typing import Optional

import muty.crypto
from muty.log import MutyLogger
from sqlalchemy import ForeignKey, String
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column, relationship
from gulp.api.collab.source import GulpSource
from gulp.api.collab.structs import COLLABTYPE_CONTEXT, GulpCollabBase
from gulp.api.ws_api import WSDATA_NEW_SOURCE
from gulp.structs import GulpMappingParameters


class GulpContext(GulpCollabBase, type=COLLABTYPE_CONTEXT):
    """
    Represents a context object

    in gulp terms, a context is used to group a set of data coming from the same host.

    it has always associated an operation, and the tuple composed by the two is unique.
    """

    operation_id: Mapped[str] = mapped_column(
        ForeignKey("operation.id", ondelete="CASCADE"),
        doc="The ID of the operation associated with the context.",
        primary_key=True,
    )
    # multiple sources can be associated with a context
    sources: Mapped[Optional[list[GulpSource]]] = relationship(
        "GulpSource",
        cascade="all, delete-orphan",
        uselist=True,
        lazy="selectin",
        foreign_keys=[GulpSource.context_id],
        doc="The source/s associated with the context.",
    )

    color: Mapped[Optional[str]] = mapped_column(
        String, doc="The color of the context."
    )

    @staticmethod
    def make_context_id_key(operation_id: str, context_name: str) -> str:
        """
        Make a key for the context_id.

        Args:
            operation_id (str): The operation id.
            context_id (str): The context name.

        Returns:
            str: The key.
        """
        return muty.crypto.hash_sha1("%s%s" % (operation_id, context_name.lower()))

    @staticmethod
    def make_source_id_key(operation_id: str, context_id: str, source_name: str) -> str:
        """
        Make a key for the source_id.

        Args:
            operation_id (str): The operation id.
            context_id (str): The context id.
            source_name (str): The source name.

        Returns:
            str: The key.
        """
        return muty.crypto.hash_sha1(
            "%s%s%s" % (operation_id, context_id, source_name.lower())
        )

    async def add_source(
        self,
        sess: AsyncSession,
        user_id: str,
        name: str,
        ws_id: str = None,
        req_id: str = None,
        src_id: str = None,
        color: str = None,
        plugin: str = None,
        mapping_parameters: GulpMappingParameters = None,
        glyph_id: str = None,
    ) -> tuple[GulpSource, bool]:
        """
        Add a source to the context.

        Args:
            sess (AsyncSession): The session to use.
            user_id (str): The id of the user adding the source.
            name (str): The name of the source (may be file name, path, etc...)
            ws_id (str, optional): The websocket id to stream NEW_SOURCE to. Defaults to None.
            req_id (str, optional): The request id. Defaults to None.
            src_id (str, optional): The id of the source. If not provided, a new id will be generated from name.
            color (str, optional): The color of the source
            plugin (str, optional): The plugin to use for the source. Defaults to None.
            mapping_parameters (GulpMappingParameters, optional): The mapping parameters for the source. Defaults to None (ignored if plugin is None).
            glyph_id (str, optional): The glyph id for the source. Defaults to None ("file").
        Returns:
            tuple(GulpSource, bool): The source added (or already existing) and a flag indicating if the source was added
        """
        # consider just the last part of the name if it's a path
        bare_name = name.split("/")[-1]
        if not src_id:
            # create a new source id
            src_id = GulpContext.make_source_id_key(
                self.operation_id, self.id, bare_name
            )

        try:
            await GulpSource.acquire_advisory_lock(sess, src_id)

            # check if source already exists
            src: GulpSource = await GulpSource.get_by_id(
                sess, obj_id=src_id, throw_if_not_found=False
            )
            if src:
                # MutyLogger.get_instance().debug(f"source {src.id}, name={name} already exists in context {self.id}.")
                return src, False

            # MutyLogger.get_instance().warning("creating new source: %s, id=%s", name, src_id)

            # create new source and link it to context
            object_data = {
                "operation_id": self.operation_id,
                "context_id": self.id,
                "name": name,
                "color": color,
                "glyph_id": glyph_id or "file",  # default glyph is 'file'
            }
            if plugin and mapping_parameters:
                object_data["plugin"] = plugin

                # ensure sigma mappings are stored in the mapping parameters if set, to avoid having to reload them from file
                from gulp.api.opensearch.sigma import get_sigma_mappings

                sigma_mappings = await get_sigma_mappings(mapping_parameters)
                if sigma_mappings:
                    mapping_parameters.sigma_mappings = sigma_mappings

                object_data["mapping_parameters"] = mapping_parameters.model_dump(
                    exclude_none=True
                )

            # pylint: disable=protected-access
            src = await GulpSource.create_internal(
                sess,
                object_data,
                obj_id=src_id,
                owner_id=user_id,
                ws_data_type=WSDATA_NEW_SOURCE if ws_id else None,
                ws_id=ws_id,
                req_id=req_id,
                commit=False,
                private=False,
            )

            MutyLogger.get_instance().debug(
                "context %s granted_user_ids=%s, granted_group_ids=%s"
                % (
                    self.id,
                    self.granted_user_ids,
                    self.granted_user_group_ids,
                )
            )
            # TODO: at the moment, keep sources public (ACL checks are only done operation-wide)
            # add same grants to the source as the context
            # for u in self.granted_user_ids:
            #     await src.add_user_grant(sess, u, commit=False)
            # for g in self.granted_user_group_ids:
            #     await src.add_group_grant(sess, g, commit=False)

            # finally commit the session
            await sess.commit()
            await sess.refresh(self)
            MutyLogger.get_instance().debug(
                f"source {src.id}, name={name} added to context {self.id}, src={src}"
            )
            return src, True
        except Exception as e:
            await sess.rollback()
            raise e
