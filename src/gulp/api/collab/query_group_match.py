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


class GulpQueryGroupMatch(GulpCollabBase, type=COLLABTYPE_QUERY_GROUP_MATCH):
    """
    for a query identified by operation_id and req_id, track how many queries in the group have matched.
    """

    total_matches: Mapped[int] = mapped_column(
        doc="number of queries that must match in the group to trigger a match",
    )
    current_matches: Mapped[int] = mapped_column(
        doc="number of queries that currently match in the group",
    )
    operation_id: Mapped[str] = mapped_column(
        ForeignKey(
            "operation.id",
            ondelete="CASCADE",
        ),
        doc="id of the operation this source is associated with",
    )
    req_id: Mapped[str] = mapped_column(
        ForeignKey("request_stats.id", ondelete="CASCADE"),
        doc="id of the request this entry is associated with",
    )
    allow_partial: Mapped[Optional[bool]] = mapped_column(
        doc="if True, allow partial matches (i.e. current_matches < total_matches)",
    )

    @override
    @classmethod
    def example(cls) -> dict:
        d = super().example()
        d.update(
            {
                "total_matches": 3,
                "current_matches": 1,
            }
        )
        return d

    @classmethod
    async def query_group_start(
        cls,
        operation_id: str,
        user_id: str,
        req_id: str,
        total_matches: int,
        allow_partial: bool = False,
    ) -> dict:
        """
        to be called when starting a new query group, creates a new entry in the table.

        Args:
            operation_id (str): the operation id of the query group
            user_id (str): the user id of the user creating the query group
            req_id (str): the request id of the query group
            total_matches (int): the number of queries that must match in the group to trigger a

        Returns:
            dict: the created entry
        """
        object_data: dict[str, Any] = {
            "operation_id": operation_id,
            "req_id": req_id,
            "total_matches": total_matches,
            "current_matches": 0,
            "allow_partial": allow_partial,
        }
        d = await cls.create(
            token=None,  # this is an internal request
            user_id=user_id,
            ws_id=None,
            req_id=req_id,
            object_data=object_data,
        )
        return d

    @classmethod
    async def add_match(
        cls,
        operation_id: str,
        req_id: str,
        matches: int = 1,
        sess: AsyncSession = None,
    ) -> None:
        """
        update the current_matches count for a query group.

        Args:
            operation_id (str): the operation id of the query group
            req_id (str): the request id of the query group
            matches (int): the number of new matches to add, default is 1
        Returns:
            None
        """

        async def _update_internal(sess: AsyncSession) -> None:
            obj: GulpQueryGroupMatch = await GulpQueryGroupMatch.get_by_filter(
                sess, GulpCollabFilter(operation_ids=[operation_id], req_id=[req_id])
            )
            d = {
                "current_matches": obj.current_matches + matches,
            }
            await obj.update(sess, d)
            pass

        if not sess:
            async with GulpCollab.get_instance().session() as sess:
                await _update_internal(sess)
        else:
            await _update_internal(sess)

    @classmethod
    async def query_group_done(
        cls,
        operation_id: str,
        req_id: str,
        sess: AsyncSession = None,
    ) -> bool:
        """ "
        to be called after all queries in the group have been processed, to check if the group has matched.

        Args:
            operation_id (str): the operation id of the query group
            req_id (str): the request id of the query group
        Returns:
            bool: True if the query group has matched, False otherwise
        """

        async def _update_internal(sess: AsyncSession) -> None:
            obj: GulpQueryGroupMatch = await GulpQueryGroupMatch.get_by_filter(
                sess, GulpCollabFilter(operation_ids=[operation_id], req_id=[req_id])
            )
            if obj.allow_partial:
                return obj.current_matches > 0
            else:
                return obj.current_matches >= obj.total_matches

        if not sess:
            async with GulpCollab.get_instance().session() as sess:
                await _update_internal(sess)
        else:
            await _update_internal(sess)

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

    @override
    async def update_by_id(*args, **kwargs) -> None:
        """
        disabled, query group matches can only be updated via the update_matches method.
        """
        raise TypeError("use update_matches method to update query group matches")
