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

import asyncio
from typing import Optional, Union, override

import muty.crypto
import muty.time
import sqlalchemy
from muty.log import MutyLogger
from sqlalchemy import ARRAY, BIGINT, ForeignKey, Index, Integer, String, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import Enum as SQLEnum

from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.structs import (GulpCollabBase, GulpCollabType,
                                     GulpRequestStatus, T, GulpUserPermission)
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.ws_api import (GulpQueryDonePacket, GulpWsQueueDataType,
                             GulpWsSharedQueue)
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


class GulpRequestStats(GulpCollabBase, type=GulpCollabType.REQUEST_STATS):
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
    status: Mapped[GulpRequestStatus] = mapped_column(
        SQLEnum(GulpRequestStatus),
        default=GulpRequestStatus.ONGOING,
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
    # TODO: consider to remove this column and convert "status" to a String column instead, to ease comparison
    completed: Mapped[Optional[str]] = mapped_column(
        String,
        default="0",
        doc="to easily filter against completion: '0' indicates requests still running, '1' indicates completed (done, canceled or failed)",
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
        **kwargs
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
                s.status = GulpRequestStatus.ONGOING
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
                ws_queue_datatype=GulpWsQueueDataType.STATS_UPDATE,
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
        },

        await super().update(
            sess,
            d=d
        )

    @override
    async def delete(
        self,
        sess,
        ws_id=None,
        user_id=None,
        ws_queue_datatype=GulpWsQueueDataType.COLLAB_DELETE,
        ws_data=None,
        req_id=None,
    ):
        raise NotImplementedError("Stats will be deleted by the system automatically.")

    @classmethod
    async def update_by_id(
        cls,
        token,
        obj_id: str,
        ws_id: str,
        req_id: str,
        d: dict = None,
        permission: list[GulpUserPermission] = None,
        **kwargs
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
        ws_id: str=None,
        user_id: str=None,
        ws_queue_datatype: "GulpWsQueueDataType" = None,
        ws_data: dict = None,
        req_id: str = None,
    ) -> dict:
        """
        update the stats with improved locking strategy to prevent deadlocks.

        Args:
            sess (AsyncSession): the database session to use.
            d (dict): the dictionary of values to update:
                source_processed (int): the number of sources processed.
                source_failed (int): the number of sources that failed.
                records_failed (int): the number of records that failed.
                records_skipped (int): the number of records that were skipped.
                records_processed (int): the number of records that were processed.
                records_ingested (int): the number of records that were ingested.
                error (str|Exception|list[str]): the error message or exception that occurred.
                status (GulpRequestStatus): the status of the stats.
            ws_id (str): the websocket id.
            user_id (str): the user id updating the stats.
            ws_queue_datatype (GulpWsQueueDataType): ignored
            ws_data (dict): ignored
            req_id (str): ignored
        
        Returns:
            dict: the updated stats as a dictionary.
        
        Raises:
            OperationalError: if locking fails after retries.
        """
        try:
            await self.__class__.acquire_advisory_lock(sess, self.id)

            # get latest data
            await sess.refresh(self)
        finally:
            await self.__class__.release_advisory_lock(sess, self.id)

        # update counters from the provided data
        self.source_processed += d.get("source_processed", 0)
        self.source_failed += d.get("source_failed", 0)
        self.records_failed += d.get("records_failed", 0)
        self.records_skipped += d.get("records_skipped", 0)
        self.records_processed += d.get("records_processed", 0)
        self.records_ingested += d.get("records_ingested", 0)
        self.source_id = d.get("source_id", self.source_id)

        # process any errors provided in the update
        error: Union[Exception, str, list[str]] = d.get("error", None)
        if error:
            if not self.errors:
                self.errors = []

            # handle different error types
            if isinstance(error, Exception):
                MutyLogger.get_instance().exception(error)
                error_str = str(error)
                if error_str not in self.errors:
                    self.errors.append(error_str)
            elif isinstance(error, str):
                if error not in self.errors:
                    self.errors.append(error)
            elif isinstance(error, list):
                for e in error:
                    if e not in self.errors:
                        self.errors.append(e)

        # log update details
        msg: str = f"---> update: ws_id={ws_id}, d={d}"
        if error:
            MutyLogger.get_instance().error(msg)
        else:
            MutyLogger.get_instance().debug(msg)

        # check if all sources processed
        if self.source_processed == self.source_total:
            MutyLogger.get_instance().debug(
                'source_processed: %d == source_total: %d, setting request "%s" to DONE'
                % (self.source_processed, self.source_total, self.id)
            )
            if self.records_processed > 0 and self.records_ingested == 0:
                self.status = GulpRequestStatus.FAILED
            else:
                self.status = GulpRequestStatus.DONE

        # check if all sources failed
        if self.source_failed == self.source_total:
            MutyLogger.get_instance().error(
                'source_failed: %d == source_total: %d, setting request "%s" to FAILED'
                % (self.source_failed, self.source_total, self.id)
            )
            self.status = GulpRequestStatus.FAILED

        # edge case for DONE but records processing failed
        if self.status == GulpRequestStatus.DONE:
            if self.records_processed == 0 and self.records_failed > 0:
                self.status = GulpRequestStatus.FAILED

        # handle completed status and update completion metrics
        if self.status in [
            GulpRequestStatus.CANCELED,
            GulpRequestStatus.FAILED,
            GulpRequestStatus.DONE,
        ]:
            # mark as complete and record finishing time
            self.time_finished = muty.time.now_msec()
            self.completed = "1"

            # log completion information
            MutyLogger.get_instance().info(
                'REQUEST TIME INFO: request "%s" COMPLETED with status=%s, TOTAL TIME: %d seconds'
                % (
                    self.id,
                    self.status,
                    (self.time_finished - self.time_created) / 1000,
                )
            )

        # apply forced status if provided
        forced_status: GulpRequestStatus = d.get("status", None)
        if forced_status:
            self.status = forced_status

        # pass to parent's update method
        dd = await super().update(
            sess,
            d=None,
            ws_id=ws_id,
            user_id=user_id,
            ws_queue_datatype=GulpWsQueueDataType.STATS_UPDATE,
            req_id=self.id,
        )
        return dd

    @staticmethod
    async def finalize_query_stats(
        sess: AsyncSession,
        req_id: str,
        ws_id: str,
        user_id: str,
        q_name: str = None,
        hits: int = 0,
        ws_queue_datatype: GulpWsQueueDataType = GulpWsQueueDataType.QUERY_DONE,
        errors: list[str] = None,
        send_query_done: bool = True,
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
            ws_queue_datatype(GulpWsQueueDataType, optional): the websocket queue data type (default: GulpWsQueueDataType.QUERY_DONE)
            errors(list[str], optional): the list of errors (default: None)
            send_query_done(bool, optional): whether to send the query done packet to the websocket (default: True)
        """
        try:
            await GulpRequestStats.acquire_advisory_lock(sess, req_id)
            stats: GulpRequestStats = await GulpRequestStats.get_by_id(
                sess, req_id, throw_if_not_found=False
            )
            status: GulpRequestStatus = GulpRequestStatus.DONE

            dd: dict={}
            if stats and stats.status != GulpRequestStatus.CANCELED:
                # mark as completed
                stats.completed = "1"
                stats.time_finished = muty.time.now_msec()

                # determine status based on hits
                if hits >= 1:
                    stats.status = GulpRequestStatus.DONE
                else:
                    stats.status = GulpRequestStatus.FAILED

                # add any errors
                if errors:
                    if not stats.errors:
                        stats.errors = errors
                    else:
                        stats.errors.extend(errors)

                stats.total_hits = hits
                MutyLogger.get_instance().debug(f"update_query_stats: {stats}")
                status = stats.status
                dd = await stats.update(
                    sess,
                    d={},
                )
        finally:
            await GulpRequestStats.release_advisory_lock(sess, req_id)
                    
        if not send_query_done:
            return dd

        # inform the websocket
        MutyLogger.get_instance().debug(
            f"sending query done packet, errors={errors}"
        )
        p = GulpQueryDonePacket(
            status=status,
            errors=errors or [],
            total_hits=hits,
            name=q_name,
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
        if stats and stats.status == GulpRequestStatus.CANCELED:
            MutyLogger.get_instance().warning(
                f"request {req_id} is canceled!"
            )
            return True
        return False
