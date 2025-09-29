from typing import override, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession
import os
from muty.log import MutyLogger
from sqlalchemy import ForeignKey, Insert, String, LargeBinary, insert
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.dialects.postgresql import JSONB
from gulp.api.collab.structs import (
    COLLABTYPE_QUERY_GROUP_MATCH,
    GulpCollabBase,
    GulpCollabFilter,
    GulpUserPermission,
)
from gulp.api.ws_api import (
    WSDATA_QUERY_GROUP_MATCH,
    GulpCollabCreatePacket,
    GulpQueryGroupMatchPacket,
    GulpWsSharedQueue,
)


class GulpQueryGroupMatch(GulpCollabBase, type=COLLABTYPE_QUERY_GROUP_MATCH):
    """
    for a query identified by operation_id and req_id, track how many queries in the group have matched.
    """

    q_group: Mapped[str] = mapped_column(
        String,
        doc="the query group identifier",
    )
    q_must_match: Mapped[int] = mapped_column(
        doc="number of queries that must match in the group to trigger a match",
    )
    operation_id: Mapped[str] = mapped_column(
        ForeignKey(
            "operation.id",
            ondelete="CASCADE",
        ),
        doc="id of the operation this source is associated with",
    )
    q_req_id: Mapped[str] = mapped_column(
        ForeignKey("request_stats.id", ondelete="CASCADE"),
        doc="id of the request this entry is associated with",
    )
    q_matched: Mapped[int] = mapped_column(
        default=0,
        doc="number of queries that currently match in the group",
    )
    allow_partial: Mapped[Optional[bool]] = mapped_column(
        default=False,
        doc="if True, allow partial matches (i.e. current_matches < total_matches)",
    )

    @override
    @classmethod
    def example(cls) -> dict:
        d = super().example()
        d.update(
            {
                "operation_id": "op_1234567890abcdef",
                "req_id": "req_1234567890abcdef",
                "q_must_match": 3,
                "q_matched": 1,
                "allow_partial": False,
            }
        )
        return d

    @classmethod
    async def query_group_start(
        cls,
        sess: AsyncSession,
        q_group: str,
        operation_id: str,
        user_id: str,
        req_id: str,
        q_must_match: int,
        allow_partial: bool = False,
    ) -> dict:
        """
        to be called when starting a query with q_group set

        Args:
            sess (AsyncSession): the database session to use
            q_group (str): the query group identifier
            operation_id (str): the operation id
            user_id (str): caller user id
            req_id (str): request id of the query
            q_must_match (int): the number of queries that must match in the group to trigger
            allow_partial (bool): if True, allow partial matches (i.e. q_matched < q_must_match)

        Returns:
            dict: the created entry
        """
        d: GulpQueryGroupMatch = await cls.create_internal(
            sess,
            user_id,
            operation_id=operation_id,
            q_group=q_group,
            q_req_id=req_id,
            q_must_match=q_must_match,
            allow_partial=allow_partial,
        )
        return d.to_dict()

    @classmethod
    async def query_group_add_match(
        cls,
        sess: AsyncSession,
        operation_id: str,
        q_group: str,
        user_id: str,
        ws_id: str,
        req_id: str,
    ) -> None:
        obj: GulpQueryGroupMatch = await GulpQueryGroupMatch.get_first_by_filter(
            sess,
            GulpCollabFilter(
                operation_ids=[operation_id], q_req_id=req_id, q_group=q_group
            ),
        )
        try:
            current_matches = obj.q_matched + 1
            await GulpQueryGroupMatch.acquire_advisory_lock(sess, obj.id)
            await super().update(sess, q_matched=current_matches)
            MutyLogger.get_instance().info(
                "adding a match to query group %s !", obj.q_group
            )

            if current_matches >= obj.q_must_match:
                # signal match
                MutyLogger.get_instance().info(
                    "query group %s has matched (current_matches=%d, total_matches=%d)",
                    obj.q_group,
                    current_matches,
                    obj.q_must_match,
                )
                p = GulpQueryGroupMatchPacket(
                    q_group=obj.q_group,
                    q_matched=current_matches,
                    q_total=obj.q_must_match,
                )
                wsq = GulpWsSharedQueue.get_instance()
                await wsq.put(
                    WSDATA_QUERY_GROUP_MATCH,
                    user_id,
                    ws_id=ws_id,
                    operation_id=operation_id,
                    req_id=req_id,
                    data=p.model_dump(exclude_none=True),
                )
                # then we can delete this entry
                await sess.delete(obj)

            await sess.commit()
        except Exception:
            MutyLogger.get_instance().exception(
                "failed to update query group match for query_id=%s, operation_id=%s",
                req_id,
                operation_id,
            )
            await sess.rollback()

    @override
    @classmethod
    async def create(
        cls,
        *args,
        **kwargs,
    ) -> dict:
        """
        disabled, use the query_group_start method to create query group matches.
        """
        raise TypeError("use query_group_start method to create query group matches")

    @override
    @classmethod
    async def update(*args, **kwargs) -> None:
        """
        disabled, query group matches can only be updated via the update_matches method.
        """
        raise TypeError("use add_match method to update query group matches")
