"""
The stats module provides functionality for tracking and managing request statistics in Gulp.

This module defines classes for managing collaborative request statistics, including:
- RequestCanceledError: Raised when a request is aborted
- SourceCanceledError: Raised when a source is aborted
- PreviewDone: Raised when a preview is completed during ingestion
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
from typing import Optional, Union, override

import muty.time
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
from gulp.api.ws_api import (
    WSDATA_COLLAB_DELETE,
    WSDATA_QUERY_DONE,
    GulpQueryDonePacket,
    GulpWsSharedQueue,
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
    REQUEST_TYPE_ENRICHMENT = "enrich"
    REQUEST_TYPE_REBASE = "rebase"
    REQUEST_TYPE_GENERIC = "generic"


class PreviewDone(Exception):
    """
    Raised when a preview is done on ingestion
    """

    def __init__(self, message: str, processed: int = 0):
        """
        Initialize the PreviewDone exception.

        Args:
            message (str): The message describing the preview completion.
            processed (int, optional): The number of records processed in the preview. Defaults to 0.
        """
        super().__init__(message)
        self.processed = processed


class GulpIngestionStats(BaseModel):
    """
    Represents the ingestion statistics
    """

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {},
            ]
        },
    )

    source_total: int = Field(1, description="Number of sources in this request.")
    source_processed: int = Field(
        0, description="Number of processed sources in this request."
    )
    source_failed: int = Field(
        0, description="Number of failed sources in this request."
    )
    records_processed: int = Field(
        0, description="Number of processed records (includes all sources)."
    )
    records_ingested: int = Field(
        0,
        description="Number of ingested records (includes all sources, may be different than processed, i.e. failed/skipped/extra-generated documents)",
    )
    records_skipped: int = Field(
        0,
        description="Number of skipped(=not ingested because duplicated) records (includes all sources)",
    )
    records_failed: int = Field(
        0, description="Number of failed records (includes all sources)"
    )


class GulpQueryStats(BaseModel):
    """
    Represents the query statistics
    """

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {},
            ]
        },
    )

    total_hits: int = Field(0, description="Total number of hits for this query.")
    q_group: Optional[str] = Field(
        None, description="The query group this query belongs to."
    )


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
        doc="The type of request stats (ingestion, query, enrichment, generic).",
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
        raise TypeError("use GulpRequestStats.create_stats() instead of create()")

    @classmethod
    async def create_stats(
        cls,
        sess: AsyncSession,
        req_id: str,
        user_id: str,
        operation_id: str,
        req_type: RequestStatsType = RequestStatsType.REQUEST_TYPE_INGESTION,
        ws_id: str = None,
        never_expire: bool = False,
        data: dict = None,
        **kwargs,
    ) -> T:
        """
        create a new GulpRequestStats object on the collab database (or update an existing one if the req_id already exists).

        NOTE: session is committed inside this method.

        Args:
            sess (AsyncSession): The database session to use.
            req_id (str): The request ID (=id of the stats): if a stats with this ID already exists, its expire time and status are updated and returned instead of creating a new one.
            user_id (str): The user ID creating the stats.
            operation_id (str): The operation associated with the stats
            req_type (RequestStatsType, optional): The type of request stats. Defaults to RequestStatsType.REQUEST_TYPE_INGESTION.
            ws_id (str, optional): The websocket ID to notify WSDATA_COLLAB_CREATE to. Defaults to None.
            never_expire (bool, optional): If True, the stats will never expire. Defaults to False.
            data
            **kwargs: Additional data to associate with the stats.
        Returns:
            T: The created (or retrieved) stats.
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

        try:
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

                # update existing stats as ongoing, and update time_expire if needed
                stats.status = GulpRequestStatus.ONGOING.value
                stats.time_updated = time_updated
                stats.time_finished = 0
                if time_expire > 0:
                    stats.time_expire = time_expire
                await stats.update(sess, ws_id=ws_id, user_id=user_id)
                return stats

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
                **kwargs,
            )
            return stats

        except Exception as e:
            await sess.rollback()
            raise e

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

    async def set_canceled(
        self,
        sess: AsyncSession,
        expire_now: bool = False,
        user_id: str = None,
        ws_id: str = None,
        **kwargs,
    ) -> dict:
        """
        set the stats as canceled

        Args:
            sess(AsyncSession): collab database session
            expire_now(bool, optional): if True, the stats will expire immediately. Defaults to False.
            user_id(str, optional): the user id issuing the request
            ws_id(str, optional): the websocket id to notify COLLAB_UPDATE to
            **kwargs: additional arguments to pass to the update method
        Returns:
            dict: the updated stats
        """
        if expire_now:
            time_expire: int = muty.time.now_msec()
        else:
            # default expires in 1 minutes
            time_expire: int = muty.time.now_msec() + 60 * 1000

        # cancel
        self.status = GulpRequestStatus.CANCELED.value
        self.time_expire = time_expire
        self.time_finished = muty.time.now_msec()
        return await self.update(sess, user_id=user_id, ws_id=ws_id, **kwargs)

    @staticmethod
    async def purge_ongoing_requests():
        """
        delete all ongoing stats (status="ongoing")
        """
        sess: AsyncSession = None
        try:
            async with GulpCollab.get_instance().session() as sess:
                flt = GulpCollabFilter(status=GulpRequestStatus.ONGOING.value)
                deleted = await GulpRequestStats.delete_by_filter(
                    sess, flt, throw_if_not_found=False
                )
                # use lazy % formatting for logging to defer string interpolation
                MutyLogger.get_instance().info("deleted %d ongoing stats", deleted)
                return deleted
        except Exception as e:
            await sess.rollback()
            raise e

    async def update_ingestion_stats(
        self,
        sess: AsyncSession,
        user_id: str = None,
        ws_id: str = None,
        ingested: int = 0,
        skipped: int = 0,
        processed: int = 0,
        failed: int = 0,
        errors: list[str | Exception] = None,
        status: GulpRequestStatus = None,
        source_finished: bool = False,
        set_expiration: bool = False,
    ) -> dict:
        """
        update the ingestion stats

        Args:
            sess(AsyncSession): collab database session
            user_id(str, optional): the user id issuing the request
            ws_id(str, optional): the websocket id to notify COLLAB_UPDATE to
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
        try:
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
                        e = str(e)
                    if e not in errs:
                        errs.append(e)
            for e in errs:
                if e not in self.errors:
                    self.errors.append(e)

            d: GulpIngestionStats = (
                GulpIngestionStats.model_validate(self.data) or GulpIngestionStats()
            )
            d.records_ingested += ingested
            d.records_skipped += skipped
            d.records_processed += processed
            d.records_failed += failed

            if source_finished or set_expiration:
                # this request is done, compute status value
                d.source_processed += 1
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

            return await super().update(sess, ws_id=ws_id, user_id=user_id)
        except Exception as e:
            await sess.rollback()
            raise e

    async def update_query_stats(
        self,
        sess: AsyncSession,
        user_id: str = None,
        ws_id: str = None,
        ingested: int = 0,
        skipped: int = 0,
        processed: int = 0,
        failed: int = 0,
        errors: list[str | Exception] = None,
        status: GulpRequestStatus = GulpRequestStatus.ONGOING,
    ) -> dict:
        """ """
        try:
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
                        e = str(e)
                    if e not in errs:
                        errs.append(e)
            self.errors.extend(errs)
            self.status = status.value
            d: GulpIngestionStats = GulpIngestionStats.model_validate(self.data or {})
            d.records_ingested += ingested
            d.records_skipped += skipped
            d.records_processed += processed
            d.records_failed += failed
            self.data = d.model_dump()
            return await super().update(sess, ws_id=ws_id, user_id=user_id)
        except Exception as e:
            await sess.rollback()
            raise e
