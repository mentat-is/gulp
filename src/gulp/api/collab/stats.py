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

from typing import Optional, Union, override

import muty.time
from muty.log import MutyLogger
from sqlalchemy import ARRAY, BIGINT, ForeignKey, Index, Integer, String
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.mutable import MutableList
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
    WSDATA_STATS_UPDATE,
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


class PreviewDone(Exception):
    """
    Raised when a preview is done on ingestion
    """


class GulpRequestStats(GulpCollabBase, type=COLLABTYPE_REQUEST_STATS):
    """
    Represents the statistics for an ingestion operation.
    """

    operation_id: Mapped[str] = mapped_column(
        ForeignKey("operation.id", ondelete="CASCADE"),
        nullable=True,
        doc="The operation associated with the stats.",
    )
    context_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("context.id", ondelete="CASCADE"),
        doc="The context associated with the stats.",
        nullable=True,
    )
    source_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("source.id", ondelete="CASCADE"),
        doc="The source associated with the stats.",
        nullable=True,
    )
    status: Mapped[str] = mapped_column(
        String,
        default=GulpRequestStatus.ONGOING.value,
        doc="The status of the stats (done, ongoing, ...).",
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
        doc="The errors that occurred during processing.",
    )
    source_processed: Mapped[Optional[int]] = mapped_column(
        Integer, default=0, doc="The number of sources processed."
    )
    source_total: Mapped[Optional[int]] = mapped_column(
        Integer, default=0, doc="The total number of sources to be processed."
    )
    source_failed: Mapped[Optional[int]] = mapped_column(
        Integer, default=0, doc="The number of sources that failed."
    )
    records_failed: Mapped[Optional[int]] = mapped_column(
        Integer, default=0, doc="The number of records that failed."
    )
    records_skipped: Mapped[Optional[int]] = mapped_column(
        Integer, default=0, doc="The number of records that were skipped."
    )
    records_processed: Mapped[Optional[int]] = mapped_column(
        Integer, default=0, doc="The number of records that were processed."
    )
    records_ingested: Mapped[Optional[int]] = mapped_column(
        Integer,
        default=0,
        doc="The number of records that were ingested (may be more than records_processed: a single record may originate more than one record to be ingested).",
    )
    total_hits: Mapped[Optional[int]] = mapped_column(
        Integer,
        default=0,
        doc="The total number of hits for the query (used for search requests).",
    )
    __table_args__ = (Index("idx_stats_operation", "operation_id"),)

    @override
    def to_dict(
        self, nested=False, hybrid_attributes=False, exclude=None, exclude_none=False
    ):
        """
        convert object to dictionary with 'gulpesque' keys.

        Args:
            nested (bool): whether to include nested objects. Defaults to False.
            hybrid_attributes (bool): whether to include hybrid attributes. Defaults to False.
            exclude (list, optional): list of attributes to exclude. Defaults to None.
            exclude_none (bool): whether to exclude None values. Defaults to False.

        Returns:
            dict: dictionary representation of the object
        """
        # override to have 'gulpesque' keys
        d = super().to_dict(nested, hybrid_attributes, exclude, exclude_none)

        # convert keys to gulp namespaced format
        for key in ["operation_id", "context_id", "source_id"]:
            if key in d:
                d[f"gulp.{key}"] = d.pop(key)

        return d

    @staticmethod
    async def delete_pending():
        """
        delete all ongoing stats that are not completed.
        """
        async with GulpCollab.get_instance().session() as sess:
            flt = GulpCollabFilter(status=["ongoing"])
            deleted = await GulpRequestStats.delete_by_filter(
                sess, flt, throw_if_not_found=False
            )
            MutyLogger.get_instance().info("deleted %d pending stats" % (deleted))
            return deleted

    @classmethod
    @override
    async def create(
        cls,
        token: str,
        ws_id: str,
        req_id: str,
        object_data: dict,
        permission: list[GulpUserPermission] = None,
        obj_id: str = None,
        private: bool = True,
        operation_id: str = None,
        **kwargs,
    ) -> T:
        """
        Create new (or get an existing) GulpRequestStats object on the collab database.

        NOTE: session is committed after the operation

        Args:
            token (str): The token of the user creating the stats, ignored
            ws_id (str): The websocket ID.
            req_id (str): The request ID.
            object_data (dict): The data to create the stats with, pass None to use the below default:
                - source_total (int, optional): The total number of sources to be processed by the request to which this stats belong. Defaults to 1.
                - source_id (str, optional): The source associated with the stats. Defaults to None.
                - context_id (str): The context associated with the stats. Defaults to None if not present (i.e. for queries).
                - never_expire (bool, optional): Whether the stats should never expire, ignoring the configuration. Defaults to False.
            permission (list[GulpUserPermission], optional): ignored
            obj_id (str, optional): ignored, req_id is used
            private (bool, optional): ignored
            operation_id (str): The operation associated with the stats
            **kwargs: Additional keyword arguments.
                - sess: AsyncSession (mandatory)
                - user_id: str (mandatory)
        Returns:
            T: The created stats.
        """
        if not object_data:
            object_data = {}

        source_total: int = object_data.get("source_total", 1)
        source_id: str = object_data.get("source_id", None)
        context_id: str = object_data.get("context_id", None)
        never_expire: bool = object_data.get("never_expire", False)
        sess: AsyncSession = kwargs["sess"]
        user_id: str = kwargs["user_id"]

        MutyLogger.get_instance().debug(
            "---> create stats: id=%s, operation_id=%s, context_id=%s, source_id=%s, source_total=%d, sess=%s, user_id=%s",
            req_id,
            operation_id,
            context_id,
            source_id,
            source_total,
            sess,
            user_id,
        )

        # determine expiration time
        time_expire: int = 0
        time_updated = muty.time.now_msec()
        if not never_expire:
            msecs_to_expiration = GulpConfig.get_instance().stats_ttl() * 1000

            if msecs_to_expiration > 0:
                time_expire = time_updated + msecs_to_expiration
            # MutyLogger.get_instance().debug("now=%s, setting stats %s time_expire to %s", time_updated, req_id, time_expire)

        try:
            await GulpRequestStats.acquire_advisory_lock(sess, req_id)

            # check if the stats already exists
            s: GulpRequestStats = await cls.get_by_id(
                sess, obj_id=req_id, throw_if_not_found=False
            )
            if s:
                # update existing stats
                if s.status != GulpRequestStatus.CANCELED.value:
                    s.status = GulpRequestStatus.ONGOING.value
                s.time_updated = time_updated
                s.time_finished = 0
                if time_expire > 0:
                    s.time_expire = time_expire

                await sess.commit()
                return s

            # create new
            object_data = {
                "time_expire": time_expire,
                "operation_id": operation_id,
                "context_id": context_id,
                "source_id": source_id,
                "source_total": source_total,
            }
            return await super()._create_internal(
                sess,
                object_data=object_data,
                obj_id=req_id,
                ws_id=ws_id,
                owner_id=user_id,
                ws_queue_datatype=WSDATA_STATS_UPDATE,
                req_id=req_id,
            )
        finally:
            await GulpRequestStats.release_advisory_lock(sess, req_id)

    async def cancel(
        self,
        sess: AsyncSession,
    ):
        """
        Cancel the stats.

        Args:
            sess (AsyncSession): The database session to use.
        """
        # expires in 5 minutes, allow any loop to finish
        time_expire = muty.time.now_msec() + 60 * 1000 * 5

        # cancel
        d: dict = {
            "status": GulpRequestStatus.CANCELED,
            "time_expire": time_expire,
            "completed": "1",
            "time_finished": muty.time.now_msec(),
        }

        await super().update(sess, d=d)

    @classmethod
    async def update_by_id(
        cls,
        token,
        obj_id: str,
        ws_id: str,
        req_id: str,
        d: dict = None,
        permission: list[GulpUserPermission] = None,
        **kwargs,
    ) -> dict:
        """
        same as base class update_by_id, but without checking token

        Args:
            token (str): The token of the user updating the stats (ignored)
            obj_id (str): The ID of the object to update.
            ws_id (str): The websocket ID.
            req_id (str): The request ID.
            d (dict, optional): The data to update the object with. Defaults to None.
            permission (list[GulpUserPermission], optional): The permissions of the user (ignored).
            **kwargs: Additional keyword arguments.
                - sess: AsyncSession (mandatory)
                - user_id: str (mandatory)

        Returns:
            dict: The updated object as a dictionary.
        """

        # get insance
        sess = kwargs["sess"]
        user_id = kwargs["user_id"]
        try:
            await GulpRequestStats.acquire_advisory_lock(sess, obj_id)
            s: GulpRequestStats = await cls.get_by_id(sess, obj_id)
            dd = await s.update(sess, d=d, ws_id=ws_id, user_id=user_id)
            return dd
        finally:
            await GulpRequestStats.release_advisory_lock(sess, obj_id)

    @override
    async def update(
        self,
        sess: AsyncSession,
        d: dict,
        ws_id: str = None,
        user_id: str = None,
        ws_queue_datatype: str = WSDATA_STATS_UPDATE,  # provide default
        ws_data: dict = None,  # keep for super().update
        req_id: str = None,  # keep for super().update,
    ) -> dict:
        """
        update the stats with improved locking strategy to prevent deadlocks.

        Args:
            sess (AsyncSession): the database session to use.
            d (dict): the dictionary of values to update:
                source_processed (int, optional): the number of sources processed. defaults to 0.
                source_failed (int, optional): the number of sources that failed. defaults to 0.
                records_failed (int, optional): the number of records that failed. defaults to 0.
                records_skipped (int, optional): the number of records that were skipped. defaults to 0.
                records_processed (int, optional): the number of records that were processed. defaults to 0.
                records_ingested (int, optional): the number of records that were ingested. defaults to 0.
                error (str | Exception | list[str], optional): the error message or exception that occurred. defaults to none.
                status (GulpRequestStatus, optional): force a specific status. defaults to none.
                source_id (str, optional): update the source id associated with the stats. defaults to none.
            ws_id (str, optional): the websocket id. defaults to none.
            user_id (str, optional): the user id updating the stats. defaults to none.
            ws_queue_datatype (str, optional): the websocket queue data type for notification. defaults to WSDATA_STATS_UPDATE.
            ws_data (dict, optional): additional data for the websocket message. defaults to none.
            req_id (str, optional): the request id for the websocket message. defaults to none.

        Returns:
            dict: the updated stats as a dictionary.

        Raises:
            OperationalError: if locking fails after retries.
        """
        log = MutyLogger.get_instance()
        updated_data: dict = (
            {}
        )  # dictionary to hold changes for super().update if needed, though current super().update ignores 'd'

        try:
            # acquire lock for the duration of the update
            await self.__class__.acquire_advisory_lock(sess, self.id)

            # get latest data
            await sess.refresh(self)

            # check if already completed or canceled
            if self.status in [
                GulpRequestStatus.CANCELED.value,
                GulpRequestStatus.DONE.value,
            ]:
                log.warning(
                    "request %s is already done or canceled, status=%s! update ignored.",
                    self.id,
                    self.status,
                )
                return self.to_dict()  # return current state

            # apply updates from d
            self.source_processed += d.get("source_processed", 0)
            self.source_failed += d.get("source_failed", 0)
            self.records_failed += d.get("records_failed", 0)
            self.records_skipped += d.get("records_skipped", 0)
            self.records_processed += d.get("records_processed", 0)
            self.records_ingested += d.get("records_ingested", 0)
            if "source_id" in d:
                self.source_id = d["source_id"]

            # process errors
            error: Union[Exception, str, list[str]] = d.get("error")
            if error:
                if not self.errors:
                    self.errors = []  # ensure list exists

                new_errors: list[str] = []
                if isinstance(error, Exception):
                    log.exception(error)  # log the full exception
                    error_str = str(error)
                    if error_str not in self.errors:
                        new_errors.append(error_str)
                elif isinstance(error, str):
                    if error not in self.errors:
                        new_errors.append(error)
                elif isinstance(error, list):
                    for e in error:
                        e_str = str(e)  # ensure it's a string
                        if e_str not in self.errors:
                            new_errors.append(e_str)

                if new_errors:
                    self.errors.extend(new_errors)
                    # mark errors as modified for sqlalchemy mutable tracking (explicit assignment is needed)
                    self.errors = self.errors  # type: ignore

            # log update details
            log.debug("---> update stats (pre): %s" % (self))
            if error:
                log.error("---> update stats error: id=%s, error=%s", self.id, error)

            # determine status
            is_completed: bool = False
            determined_status: GulpRequestStatus = None

            # check if all sources are processed
            if self.source_processed >= self.source_total:  # use >= for safety
                log.debug(
                    'source_processed: %d >= source_total: %d, request "%s" processing complete.',
                    self.source_processed,
                    self.source_total,
                    self.id,
                )
                is_completed = True
                # default to done, then check for failure conditions
                determined_status = GulpRequestStatus.DONE

                # condition 1: all sources failed
                if self.source_failed >= self.source_total:
                    log.error(
                        'source_failed: %d >= source_total: %d, setting request "%s" to failed.',
                        self.source_failed,
                        self.source_total,
                        self.id,
                    )
                    determined_status = GulpRequestStatus.FAILED
                # condition 2: some sources processed, but nothing ingested (implies failure)
                elif self.records_processed > 0 and self.records_ingested == 0:
                    log.warning(
                        'records_processed: %d > 0 but records_ingested: 0, setting request "%s" to failed.',
                        self.records_processed,
                        self.id,
                    )
                    determined_status = GulpRequestStatus.FAILED
                # condition 3: marked done, but actually no records processed and some failed
                elif (
                    determined_status == GulpRequestStatus.DONE
                    and self.records_processed == 0
                    and self.records_failed > 0
                ):
                    log.warning(
                        'status was done, but records_processed: 0 and records_failed: %d > 0, setting request "%s" to failed.',
                        self.records_failed,
                        self.id,
                    )
                    determined_status = GulpRequestStatus.FAILED

            # apply the determined status if one was found
            if determined_status:
                self.status = determined_status.value

            # apply forced status
            # this overrides any automatically determined status
            forced_status: str = d.get("status")
            if forced_status:
                log.warning(
                    'applying forced status "%s" to request "%s"',
                    forced_status,
                    self.id,
                )
                self.status = forced_status

            if self.status in [
                GulpRequestStatus.FAILED.value,
                GulpRequestStatus.DONE.value,
                GulpRequestStatus.CANCELED.value,
            ]:
                # forced completion
                is_completed = True

            # handle completion
            if is_completed:
                self.time_finished = muty.time.now_msec()
                log.info(
                    'request "%s" **COMPLETED** with status=%s, total time: %d seconds',
                    self.id,
                    self.status,
                    (
                        (self.time_finished - self.time_created) / 1000
                        if self.time_created
                        else -1
                    ),
                )

            # --- call parent update ---
            # note: the parent's update handles the commit and websocket notification.
            # it uses the current state of 'self', so passing d=None is correct if parent doesn't need incremental changes.
            updated_dict: dict = await super().update(
                sess,
                d=updated_data,  # pass empty dict or specific fields if parent needs them
                ws_id=ws_id,
                user_id=user_id,
                ws_queue_datatype=ws_queue_datatype,
                ws_data=ws_data,  # pass through ws_data
                req_id=req_id or self.id,  # pass through req_id or use self.id
            )
            return updated_dict

        finally:
            # ensure lock is always released
            await self.__class__.release_advisory_lock(sess, self.id)

    @staticmethod
    async def finalize_query_stats(
        sess: AsyncSession,
        req_id: str,
        ws_id: str,
        user_id: str,
        q_name: str = None,
        hits: int = 0,
        ws_queue_datatype: str = WSDATA_QUERY_DONE,
        errors: list[str] = None,
        send_query_done: bool = True,
        num_queries: int = 0,
        q_group: str = None,
    ) -> dict:
        """
        sets the final status of a query stats

        Args:
            sess(AsyncSession): collab database session
            req_id(str): the request id
            ws_id(str): the websocket id
            user_id(str): the user id
            q_name(str, optional): the query name (default: None)
            hits(int, optiona): the number of hits (default: 0)
            ws_queue_datatype(str, optional): the websocket queue data type (default: WSDATA_QUERY_DONE)
            errors(list[str], optional): the list of errors (default: None)
            send_query_done(bool, optional): whether to send the query done packet to the websocket (default: True)
            num_queries(int, optional): the number of queries performed (default: 0)
            q_group(str, optional): the query group (default: None)

        Returns:
            dict: the updated stats as a dictionary
        """
        try:
            await GulpRequestStats.acquire_advisory_lock(sess, req_id)
            stats: GulpRequestStats = await GulpRequestStats.get_by_id(
                sess, req_id, throw_if_not_found=False
            )
            dd: dict = {}
            if stats and stats.status != GulpRequestStatus.CANCELED.value:
                # mark as completed
                stats.status = GulpRequestStatus.DONE.value
                stats.time_finished = muty.time.now_msec()

                # add any errors
                if errors:
                    if not stats.errors:
                        stats.errors = errors
                    else:
                        stats.errors.extend(errors)

                if stats.errors:
                    stats.status = GulpRequestStatus.FAILED.value
                stats.total_hits = hits

                MutyLogger.get_instance().debug(
                    "update_query_stats id=%s, with status=%s, hits=%d"
                    % (stats.id, stats.status, hits)
                )
                dd = stats.to_dict(exclude_none=True)
                await sess.commit()

        finally:
            await GulpRequestStats.release_advisory_lock(sess, req_id)

        if not send_query_done:
            return dd

        # inform the websocket
        MutyLogger.get_instance().debug(
            f"sending query done packet, datatype={ws_queue_datatype}, errors={errors}"
        )
        p = GulpQueryDonePacket(
            status=dd.get("status", GulpRequestStatus.DONE.value),
            errors=errors or [],
            total_hits=hits,
            queries=num_queries,
            name=q_name,
            group=q_group,
        )

        GulpWsSharedQueue.get_instance().put(
            type=ws_queue_datatype,
            ws_id=ws_id,
            user_id=user_id,
            req_id=req_id,
            data=p.model_dump(exclude_none=True),
        )

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
            MutyLogger.get_instance().warning(f"request {req_id} is canceled!")
            return True
        return False
