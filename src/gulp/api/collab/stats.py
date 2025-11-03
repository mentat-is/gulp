"""
The stats module provides functionality for tracking and managing request statistics in Gulp.

This module defines classes for managing collaborative request statistics, including:
- RequestCanceledError: Raised when a request is aborted
- SourceCanceledError: Raised when a source is aborted
- GulpRequestStats: Main class for tracking statistics of ingestion operations

GulpRequestStats maintains metrics such as processed/failed sources, processed/ingested records,
and request status. It handles creation, updating, and finalization of statistics with
appropriate database locks to prevent race conditions.

The module also provides utilities for:
- Query completion notification
- Stats expiration management
- Error handling and aggregation
- Status determination based on processing state

This is a core component of the Gulp collaborative API, allowing monitoring of
long-running ingestion processes.

"""

from enum import StrEnum
from typing import Annotated, Optional, Union, override

import muty.time
import muty.log
from muty.log import MutyLogger
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import ARRAY, BIGINT, ForeignKey, Index, Integer, String, Boolean
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.mutable import MutableList, MutableDict
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import Enum as SQLEnum

from gulp.api.collab.structs import (
    COLLABTYPE_REQUEST_STATS,
    GulpCollabBase,
    GulpCollabFilter,
    GulpRequestStatus,
    GulpUserPermission,
    T,
)
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.ws_api import (
    WSDATA_COLLAB_DELETE,
    WSDATA_QUERY_DONE,
    WSDATA_STATS_CREATE,
    WSDATA_STATS_UPDATE,
    GulpQueryDonePacket,
    GulpRedisBroker,
)
from gulp.config import GulpConfig


class RequestCanceledError(Exception):
    """
    Raised when a request is aborted (by API or in case of too many failures).
    """


class SourceCanceledError(Exception):
    """
    Raised when a source is aborted (by API or in case of too many failures).
    """


class RequestStatsType(StrEnum):
    """
    types of request stats
    """

    REQUEST_TYPE_INGESTION = "ingest"
    REQUEST_TYPE_QUERY = "query"
    REQUEST_TYPE_EXTERNAL_QUERY = "ext_query"
    REQUEST_TYPE_ENRICHMENT = "enrich"
    REQUEST_TYPE_REBASE = "rebase"
    REQUEST_TYPE_GENERIC = "generic"


class GulpIngestionStats(BaseModel):
    """
    Represents the ingestion statistics
    """

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "source_total": 2,
                    "source_processed": 1,
                    "source_failed": 0,
                    "records_processed": 100,
                    "records_ingested": 90,
                    "records_skipped": 5,
                    "records_failed": 5,
                },
            ]
        },
    )

    source_total: Annotated[
        int, Field(description="Number of sources in this request.")
    ] = 1
    source_processed: Annotated[
        int, Field(description="Number of processed sources in this request.")
    ] = 0
    source_failed: Annotated[
        int, Field(description="Number of failed sources in this request.")
    ] = 0
    records_processed: Annotated[
        int, Field(description="Number of processed records (includes all sources).")
    ] = 0
    records_ingested: Annotated[
        int,
        Field(
            description="Number of ingested records (includes all sources, may be different than processed, i.e. failed/skipped/extra-generated documents)",
        ),
    ] = 0
    records_skipped: Annotated[
        int,
        Field(
            description="Number of skipped(=not ingested because duplicated) records (includes all sources)",
        ),
    ] = 0
    records_failed: Annotated[
        int, Field(description="Number of failed records (includes all sources)")
    ] = 0


class GulpQueryStats(BaseModel):
    """
    Represents the query statistics
    """

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {"total_hits": 100, "q_group": "my_query_group"},
            ]
        },
    )

    total_hits: Annotated[
        int, Field(description="Total number of hits for this query.")
    ] = 0
    num_queries: Annotated[int, Field(description="Number of queries executed.")] = 1
    completed_queries: Annotated[
        int, Field(description="Number of queries completed so far.")
    ] = 0
    q_group: Annotated[
        Optional[str], Field(description="The query group this query belongs to.")
    ] = None


class GulpUpdateDocumentsStats(BaseModel):
    """
    Represents the rebase/enrich statistics
    """

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {"total_hits": 100, "updated": 80, "errors": ["error1", "error2"]},
            ]
        },
    )

    total_hits: Annotated[
        int, Field(description="Number of documents to be update.")
    ] = 0
    updated: Annotated[
        int, Field(description="Number of documents effectively updated.")
    ] = 0
    flt: Annotated[
        Optional[GulpQueryFilter],
        Field(description="The filter used for the operation."),
    ] = None
    plugin: Annotated[
        Optional[str], Field(description="The plugin used for the operation.")
    ] = None


class GulpRequestStats(GulpCollabBase, type=COLLABTYPE_REQUEST_STATS):
    """
    Represents the statistics for a request (the `req_id` parameter passed to API is the id of the GulpRequestStats)
    """

    operation_id: Mapped[str] = mapped_column(
        ForeignKey("operation.id", ondelete="CASCADE"),
        nullable=True,
        doc="The operation associated with the stats.",
    )
    status: Mapped[str] = mapped_column(
        String,
        default=GulpRequestStatus.ONGOING.value,
        doc="The status of the stats (done, ongoing, failed, canceled).",
    )
    req_type: Mapped[str] = mapped_column(
        String,
        default=RequestStatsType.REQUEST_TYPE_INGESTION,
        doc="The type of request stats (ingestion, query, rebase, enrich, generic).",
    )
    time_expire: Mapped[Optional[int]] = mapped_column(
        BIGINT,
        default=0,
        doc="The timestamp when the stats will expire, in milliseconds from the unix epoch.",
    )
    time_finished: Mapped[Optional[int]] = mapped_column(
        BIGINT,
        default=0,
        doc="The timestamp when the stats were completed, in milliseconds from the unix epoch.",
    )
    errors: Mapped[Optional[list[str]]] = mapped_column(
        MutableList.as_mutable(ARRAY(String)),
        default_factory=list,
        doc="A list of errors encountered during the operation.",
    )
    data: Mapped[Optional[dict]] = mapped_column(
        MutableDict.as_mutable(JSONB),
        default_factory=dict,
        doc="Additional data associated with the stats (GulpQueryStats, GulpIngestionStats)",
    )
    __table_args__ = (Index("idx_stats_operation", "operation_id"),)

    @override
    @classmethod
    async def create(
        cls,
        *args,
        **kwargs,
    ) -> dict:
        raise TypeError("use GulpRequestStats.create_or_get_existing() instead")

    @classmethod
    async def create_or_get_existing(
        cls,
        sess: AsyncSession,
        req_id: str,
        user_id: str,
        operation_id: str,
        req_type: RequestStatsType = RequestStatsType.REQUEST_TYPE_INGESTION,
        ws_id: str = None,
        never_expire: bool = False,
        data: dict = None,
    ) -> tuple[T, bool]:
        """
        creates (or get an existing) GulpRequestStats object on the collab database

        Args:
            sess (AsyncSession): The database session to use.
            req_id (str): The request ID (=id of the stats): if a stats with this ID already exists, its expire time and status are updated and the stats object is returned instead of creating a new one.
            user_id (str): The user ID creating the stats.
            operation_id (str): The operation associated with the stats
            req_type (RequestStatsType, optional): The type of request stats. Defaults to RequestStatsType.REQUEST_TYPE_INGESTION.
            ws_id (str, optional): The websocket ID to notify WSDATA_STATS_CREATE to. Defaults to None.
            never_expire (bool, optional): If True, the stats will never expire. Defaults to False.
            data (dict, optional): Additional data to store and initialize the stats, depending on req_type, i.e. GulpIngestionStats.model_dump(). Defaults to None.
        Returns:
            tuple[GulpRequestStats, bool]: The created or existing stats object and a boolean indicating if it was created (True) or already existed (False).
        """

        MutyLogger.get_instance().debug(
            "---> create_stats: req_id=%s, operation_id=%s, user_id=%s, stats_type=%s",
            req_id,
            operation_id,
            user_id,
            req_type,
        )

        # determine expiration time
        time_expire: int = 0
        time_updated: int = muty.time.now_msec()
        if not never_expire:
            # set expiration time based on config
            msecs_to_expiration: int = GulpConfig.get_instance().stats_ttl() * 1000

            if msecs_to_expiration > 0:
                time_expire = time_updated + msecs_to_expiration
            # MutyLogger.get_instance().debug("now=%s, setting stats %s time_expire to %s", time_updated, req_id, time_expire)

        await GulpRequestStats.acquire_advisory_lock(sess, req_id)

        # check if the stats already exists
        stats: GulpRequestStats = await cls.get_by_id(
            sess, obj_id=req_id, throw_if_not_found=False
        )
        if stats:
            MutyLogger.get_instance().debug(
                "---> create_stats: req_id=%s, already existing, updating...",
                req_id,
            )
            if stats.status != GulpRequestStatus.ONGOING.value:
                MutyLogger.get_instance().warning(
                    "existing stats %s is already finished with status=%s, not updating status or expiration time",
                    req_id,
                    stats.status,
                )
                return stats, False
            
            # update existing stats as ongoing, and update time_expire if needed
            stats.status = GulpRequestStatus.ONGOING.value
            stats.time_updated = time_updated
            stats.time_finished = 0
            if time_expire > 0:
                stats.time_expire = time_expire
            await stats.update(sess, ws_id=ws_id, user_id=user_id)
            return stats, False

        # create new
        stats = await GulpRequestStats.create_internal(
            sess,
            user_id,
            operation_id=operation_id,
            private=True,  # stats are private
            obj_id=req_id,  # id is the request id
            ws_id=ws_id,
            status=GulpRequestStatus.ONGOING.value,
            time_expire=time_expire,
            req_type=req_type.value,
            time_updated=time_updated,
            time_finished=0,
            errors=[],
            data=data,
            ws_data_type=WSDATA_STATS_CREATE,
        )
        return stats, True

    @staticmethod
    async def is_canceled(sess: AsyncSession, req_id: str) -> bool:
        """
        check if the request is canceled

        Args:
            sess(AsyncSession): collab database session
            req_id(str): the request id
        Returns:
            bool: True if the request is canceled, False otherwise
        """
        stats: GulpRequestStats = await GulpRequestStats.get_by_id(
            sess, req_id, throw_if_not_found=False
        )
        if stats and stats.status == GulpRequestStatus.CANCELED.value:
            MutyLogger.get_instance().warning("request %s is canceled!", req_id)
            return True
        return False

    async def set_finished(
        self,
        sess: AsyncSession,
        status: GulpRequestStatus = None,
        data: dict = None,
        time_expire: int = 0,
        user_id: str = None,
        ws_id: str = None,
        errors: list[str] = None,
    ) -> dict:
        """
        set the stats as finished (done, failed, canceled)

        Args:
            sess(AsyncSession): collab database session
            status(GulpRequestStatus, optional): the status to set the stats to. Defaults to None (keep current status).
            data(dict, optional): additional data to store in the stats. Defaults to None.
            time_expire(int, optional): the time when the stats will expire, in milliseconds from
                the unix epoch. If 0, the expiration time is not updated. Defaults to 0.
            user_id(str, optional): the user id issuing the request
            ws_id(str, optional): the websocket id to notify WS_STATS_UPDATE to
            errors(list[str], optional): list of errors to add. Defaults to None.
        Returns:
            dict: the updated stats
        """
        await GulpRequestStats.acquire_advisory_lock(sess, self.id)

        if status:
            # force this status
            self.status = status.value
        self.time_finished = muty.time.now_msec()
        if errors:
            # add errors
            self.errors.extend(errors or [])
        if time_expire:
            self.time_expire = time_expire
        if data:
            self.data = data

        MutyLogger.get_instance().info(
            "**FINISHED** status=%s, elapsed_time=%ds, stats=%s",
            self.status,
            (self.time_finished - self.time_created)
            // 1000,  # time elapsed in seconds
            self,
        )
        return await self.update(
            sess, ws_id=ws_id, user_id=user_id, ws_data_type=WSDATA_STATS_UPDATE
        )

    async def set_canceled(
        self,
        sess: AsyncSession,
        expire_now: bool = False,
        data: dict = None,
        user_id: str = None,
        ws_id: str = None,
    ) -> dict:
        """
        set the stats as canceled

        Args:
            sess(AsyncSession): collab database session
            expire_now(bool, optional): if True, the stats will expire immediately. Defaults to False.
            data(dict, optional): additional data to store in the stats. Defaults to None.
            user_id(str, optional): the user id issuing the request
            ws_id(str, optional): the websocket id to notify COLLAB_UPDATE to
        Returns:
            dict: the updated stats
        """
        if expire_now:
            time_expire: int = muty.time.now_msec()
        else:
            # default expires in 1 minutes
            time_expire: int = muty.time.now_msec() + 60 * 1000

        MutyLogger.get_instance().warning(
            "setting request %s as CANCELED, expires at %s", self.id, time_expire
        )
        await self.set_finished(
            sess,
            status=GulpRequestStatus.CANCELED,
            time_expire=time_expire,
            data=data,
            user_id=user_id,
            ws_id=ws_id,
        )
        return self.to_dict()

    @staticmethod
    async def purge_ongoing_requests():
        """
        delete all ongoing stats (status="ongoing")
        """
        async with GulpCollab.get_instance().session() as sess:
            flt = GulpCollabFilter(status=GulpRequestStatus.ONGOING.value)
            deleted = await GulpRequestStats.delete_by_filter(
                sess, flt, throw_if_not_found=False
            )
            # use lazy % formatting for logging to defer string interpolation
            MutyLogger.get_instance().info("deleted %d ongoing stats", deleted)
            return deleted

    async def update_ingestion_stats(
        self,
        sess: AsyncSession,
        user_id: str = None,
        ws_id: str = None,
        records_ingested: int = 0,
        records_skipped: int = 0,
        records_processed: int = 0,
        records_failed: int = 0,
        errors: list[str | Exception] = None,
        status: GulpRequestStatus = None,
        source_finished: bool = False,
        set_expiration: bool = False,
    ) -> dict:
        """
        update the ingestion stats

        once all sources are marked as finished (source_finished=True), the request is marked as DONE/FAILED (if all sources failed)

        Args:
            sess(AsyncSession): collab database session
            user_id(str, optional): the user id issuing the request (ignored if ws_id is not set)
            ws_id(str, optional): the websocket id to notify WS_STATS_UPDATE to (ignored if not set)
            ingested(int, optional): number of ingested records to add. Defaults to 0.
            skipped(int, optional): number of skipped records to add. Defaults to 0.
            processed(int, optional): number of processed records to add. Defaults to 0.
            failed(int, optional): number of failed records to add. Defaults to 0.
            errors(list[str|Exception], optional): list of errors to add. Defaults to None.
            status(GulpRequestStatus, optional): set forcefully the status to this value (only on finish). Defaults to None.
            source_finished(bool, optional): if True, marks one source as finished. Defaults to False.
            set_expiration(bool, optional): if True, sets the expiration time so the request will expire then (meant to be used when the request is finished and was set to "never_expire", i.e. ws_raw requests). Defaults to False.
        Returns:
            dict: the updated stats
        """
        # more than one process may be working on this request (multiple ingestion with the same req_id)
        await GulpRequestStats.acquire_advisory_lock(sess, self.id)
        await sess.refresh(self)
        if self.status != GulpRequestStatus.ONGOING.value:
            MutyLogger.get_instance().warning(
                "UPDATE IGNORED! request %s is already done/failed/canceled, status=%s",
                self.id,
                self.status,
            )
            await sess.commit()  # release the lock
            return self.to_dict()

        # update
        errs: list[str] = []
        if errors:
            for e in errors:
                if isinstance(e, Exception):
                    e = muty.log.exception_to_string(e)
                if e not in self.errors:
                    self.errors.append(e)

        d: GulpIngestionStats = (
            GulpIngestionStats.model_validate(self.data) or GulpIngestionStats()
        )
        d.records_ingested += records_ingested
        d.records_skipped += records_skipped
        d.records_processed += records_processed
        d.records_failed += records_failed

        if source_finished or set_expiration:
            # this request is done, compute status value
            d.source_processed += 1
            if status and status in [GulpRequestStatus.FAILED.value, GulpRequestStatus.CANCELED.value,]:
                # if the request w as canceled or failed, mark one more source as failed
                d.source_failed += 1

            if d.source_processed >= d.source_total:
                MutyLogger.get_instance().debug(
                    "all sources processed for request %s, processed=%d, total=%d",
                    self.id,
                    d.source_processed,
                    d.source_total,
                )
                # request is finished
                self.time_finished = muty.time.now_msec()
                if d.source_failed == d.source_total:
                    # if all sources failed, mark the request as failed
                    MutyLogger.get_instance().error(
                        "all sources failed for request %s, marking as FAILED",
                        self.id,
                    )
                    self.status = GulpRequestStatus.FAILED.value
                else:
                    self.status = GulpRequestStatus.DONE.value
                if status:
                    # force set status
                    self.status = status.value

            if set_expiration:
                # set expiration time based on config
                MutyLogger.get_instance().debug(
                    "setting expiration after completion of request %s", self.id
                )
                msecs_to_expiration: int = (
                    GulpConfig.get_instance().stats_ttl() * 1000
                )
                if msecs_to_expiration > 0:
                    self.time_expire = muty.time.now_msec() + msecs_to_expiration

        self.data = d.model_dump()
        if self.status != GulpRequestStatus.ONGOING.value:
            MutyLogger.get_instance().info(
                "**FINISHED** status=%s, elapsed_time=%ds, stats=%s",
                self.status,
                (self.time_finished - self.time_created) // 1000,
                self,
            )

        return await super().update(
            sess, ws_id=ws_id, user_id=user_id, ws_data_type=WSDATA_STATS_UPDATE
        )
    async def update_updatedocuments_stats(
        self,
        sess: AsyncSession,
        user_id: str = None,
        ws_id: str = None,
        total_hits: int = 0,
        updated: int = 0,
        flt: GulpQueryFilter = None,
        errors: list[str] = None,
        last: bool = False,
    ) -> dict:
        """
        update the rebase/enrich stats counters

        Args:
            sess(AsyncSession): collab database session
            user_id(str, optional): the user id issuing the request (ignored if ws_id is not set)
            ws_id(str, optional): the websocket id to notify WS_STATS_UPDATE to (ignored if not set)
            total_hits(int, optional): number of documents found to be updated. Defaults to 0.
            updated(int, optional): number of documents effectively updated. Defaults to 0.
            flt(GulpQueryFilter, optional): the filter used for the operation. Defaults to None.
            errors(list[str], optional): list of errors to add. Defaults to None.
            last(bool, optional): if True, this is the last update and the stats can be finalized if needed. Defaults to False.
        Returns:
            dict: the updated stats
        """
        await GulpRequestStats.acquire_advisory_lock(sess, self.id)
        await sess.refresh(self)

        # update
        d: GulpUpdateDocumentsStats = (
            GulpUpdateDocumentsStats.model_validate(self.data)
            or GulpUpdateDocumentsStats()
        )
        d.updated += updated
        d.total_hits = total_hits
        if errors:
            for e in errors:
                if e not in self.errors:
                    self.errors.append(e)
        if flt:
            d.flt = GulpQueryFilter.model_validate(flt.model_dump())
        if last:
            # last update, finalize stats
            if self.status != GulpRequestStatus.CANCELED.value:
                if d.updated < d.total_hits and self.errors:
                    self.status = GulpRequestStatus.FAILED.value
                else:
                    self.status = GulpRequestStatus.DONE.value
            self.time_finished = muty.time.now_msec()
            MutyLogger.get_instance().info(
                "**FINISHED** status=%s, elapsed_time=%ds, stats=%s",
                self.status,
                (self.time_finished - self.time_created) // 1000,
                self,
            )
        self.data = d.model_dump()
        return await super().update(
            sess, ws_id=ws_id, user_id=user_id, ws_data_type=WSDATA_STATS_UPDATE
        )

    async def update_query_stats(
        self,
        sess: AsyncSession,
        user_id: str = None,
        ws_id: str = None,
        hits: int = 0,
        inc_completed: int = 1,
        errors: list[str] = None,
    ) -> dict:
        """
        update the query stats

        once the number of completed queries is equal to num_queries, the request is marked as DONE (or FAILED if there are errors)

        Args:
            sess(AsyncSession): collab database session
            user_id(str, optional): the user id issuing the request (ignored if ws_id is not set)
            ws_id(str, optional): the websocket id to notify WS_STATS_UPDATE to (ignored if not set)
            hits(int, optional): number of hits to add. Defaults to 0.
            inc_completed(int, optional): number of completed queries to add. Defaults to 1.
            errors(list[str], optional): list of errors to add. Defaults to None.
        Returns:
            dict: the updated stats
        """
        await GulpRequestStats.acquire_advisory_lock(sess, self.id)
        await sess.refresh(self)

        # update
        d: GulpQueryStats = (
            GulpQueryStats.model_validate(self.data) or GulpQueryStats()
        )

        d.total_hits += hits
        if errors:
            for e in errors:
                if e not in self.errors:
                    self.errors.append(e)
        if self.status != GulpRequestStatus.CANCELED.value:
            if inc_completed:
                # some queries completed
                d.completed_queries += inc_completed
                MutyLogger.get_instance().info(
                    "**QUERY REQ=%s num queries completed=%d/%d**, current hits=%d",
                    self.id,
                    d.completed_queries,
                    d.num_queries,
                    d.total_hits,
                )

            # if the query has not been canceled
            if d.completed_queries >= d.num_queries:
                # the whole req is finished
                self.time_finished = muty.time.now_msec()
                self.status = (
                    GulpRequestStatus.DONE.value
                    if not self.errors
                    else GulpRequestStatus.FAILED.value
                )
                # get time elapsed in seconds
                MutyLogger.get_instance().info(
                    "**FINISHED** status=%s, elapsed_time=%ds, stats=%s",
                    self.status,
                    (self.time_finished - self.time_created) // 1000,
                    self,
                )

        self.data = d.model_dump()
        return await super().update(
            sess, ws_id=ws_id, user_id=user_id, ws_data_type=WSDATA_STATS_UPDATE
        )
