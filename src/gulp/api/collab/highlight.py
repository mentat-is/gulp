"""
This module defines the GulpHighlight class, which represents highlights in the gulp collaboration system.

Highlights are time-based annotations associated with a gulp source, allowing users to mark specific
temporal sections of content for collaboration purposes.

The module provides:
- GulpHighlight: A class for creating, storing, and manipulating highlight objects
- Integration with SQLAlchemy for database persistence
- Support for time ranges specified in nanoseconds
- Optional association with source objects through foreign key relationships
"""

from typing import Optional
import muty
from muty.log import MutyLogger

from sqlalchemy import ARRAY, BIGINT, ForeignKey, Insert
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from gulp.api.collab.structs import COLLABTYPE_HIGHLIGHT, GulpCollabBase
from gulp.api.ws_api import (
    WSDATA_COLLAB_CREATE,
    GulpCollabCreatePacket,
    GulpWsSharedQueue,
)


class GulpHighlight(GulpCollabBase, type=COLLABTYPE_HIGHLIGHT):
    """
    an highlight in the gulp collaboration system
    """

    time_range: Mapped[tuple[int, int]] = mapped_column(
        MutableList.as_mutable(ARRAY(BIGINT)),
        doc="The time range of the highlight, in nanoseconds from unix epoch.",
    )

    @classmethod
    def example(cls) -> dict:
        d = super().example()
        d["time_range"] = [0, 1000000]
        return d

    @staticmethod
    async def bulk_create_for_documents_and_send_to_ws(
        sess: AsyncSession,
        operation_id: str,
        user_id: str,
        ws_id: str,
        req_id: str,
        docs: list[dict],
        last: bool = False,
    ) -> int:
        """
        creates a Highlight on collab for each document in the list, using bulk insert, then streams the chunk of Highlights to the websocket

        NOTE: this is meant for internal usage, access checks should be done before calling this method

        Args:
            sess (AsyncSession): the database session
            operation_id (str): the operation id the highlights belong to
            user_id (str): the user id creating the highlights
            ws_id (str): the websocket id
            req_id (str): the request id
            docs (list[dict]): the list of dict that highligh information (name, time_range, color, tags ecc..)

            last (bool, optional): whether this is the last batch of highligths to be created. Defaults to False.
        Returns:
            the number of highligths created
        """

        if not docs:
            MutyLogger.get_instance().warning(
                "no documents provided, no highligths created"
            )
            return 0

        highligths: list[dict] = []
        MutyLogger.get_instance().info(
            "creating a bulk of %d highligths ..." % len(docs)
        )

        for doc in docs:
            object_data: dict = GulpHighlight.build_object_dict(
                user_id,
                name=doc["name"],
                operation_id=operation_id,
                glyph_id=doc.get("glyph_id", None),
                description=doc["decription"],
                tags=doc["tags"],
                color=doc["color"],
                private=False,
                time_range=doc["time_range"],
            )
            obj_id = muty.crypto.hash_xxh128(str(object_data))
            object_data["id"] = obj_id
            highligths.append(object_data)

        # bulk insert (handles duplicates)
        stmt: Insert = insert(GulpHighlight).values(highligths)
        stmt = stmt.on_conflict_do_nothing(index_elements=["id"])
        stmt = stmt.returning(GulpHighlight.id)
        res = await sess.execute(stmt)
        await sess.commit()
        inserted_ids = [row[0] for row in res]
        MutyLogger.get_instance().info(
            "written %d highligths on collab db", len(inserted_ids)
        )

        # send on ws, only keep the ones inserted (to avoid sending duplicates, but we always send something if "last" is set)
        inserted_highligths: list[dict] = [
            item for item in highligths if item["id"] in inserted_ids
        ]
        if inserted_highligths or last:
            p: GulpCollabCreatePacket = GulpCollabCreatePacket(
                obj=inserted_highligths,
                bulk=True,
                last=last,
                bulk_size=len(inserted_highligths),
                total_size=len(highligths),
            )
            wsq = GulpWsSharedQueue.get_instance()
            await wsq.put(
                WSDATA_COLLAB_CREATE,
                user_id,
                ws_id=ws_id,
                operation_id=operation_id,
                req_id=req_id,
                d=p.model_dump(exclude_none=True),
            )
            MutyLogger.get_instance().debug(
                "sent (inserted) highligths on the websocket %s: highligths=%d,inserted=%d,last=%r (if 'highligths' > 'inserted', duplicate highligths were skipped!)"
                % (ws_id, len(highligths), len(inserted_highligths), last)
            )
        return len(inserted_highligths)
