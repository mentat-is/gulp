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
from sqlalchemy import ARRAY, BIGINT, ForeignKey, Index, Integer, String, Boolean
from sqlalchemy.dialects.postgresql import JSONB
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
    GulpCollabCreateUpdatePacket,
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


class GulpRequestStats(GulpCollabBase, type=COLLABTYPE_REQUEST_STATS):
    """
    Represents the statistics for an operation.
    """

    operation_id: Mapped[str] = mapped_column(
        ForeignKey("operation.id", ondelete="CASCADE"),
        nullable=True,
        doc="The operation associated with the stats.",
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
    req_type: Mapped[Optional[str]] = mapped_column(
        String,
        default=RequestStatsType.REQUEST_TYPE_INGESTION,
        doc="The type of request stats (ingestion, query, enrichment, generic).",
    )
    data: Mapped[Optional[dict]] = mapped_column(
        MutableList.as_mutable(JSONB),
        default_factory=dict,
        doc="Additional data associated with the stats.",
    )
    __table_args__ = (Index("idx_stats_operation", "operation_id"),)

    @override
    @classmethod
    async def create(
        cls,
        *args,
        **kwargs,
    ) -> dict:
        """
        disabled, use create_or_get instead
        """
        raise TypeError("use GulpRequestStats.create_or_get() instead of create()")

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
        for key in ["operation_id"]:
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
    async def create_or_get(
        cls,
        sess: AsyncSession,
        req_id: str,
        user_id: str,
        ws_id: str,
        operation_id: str,
        stats_type: RequestStatsType = RequestStatsType.REQUEST_TYPE_INGESTION,
        never_expire: bool = False,
    ) -> T:
        """
        Create new (or get an existing) GulpRequestStats object on the collab database.

        NOTE: session is committed inside this method.

        Args:
            sess (AsyncSession): The database session to use.
            req_id (str): The request ID (=id of the stats): if a stats with this ID already exists, its expire time and status are updated and returned instead of creating a new one.
            user_id (str): The user ID creating the stats.
            operation_id (str): The operation associated with the stats
            ws_id (str): The websocket ID to notify the creation of the stats.
            stats_type (RequestStatsType, optional): The type of request stats. Defaults to RequestStatsType.REQUEST_TYPE_INGESTION.
            data (dict, optional): Additional data to associate with the stats. Defaults to None.

        Returns:
            T: The created (or retrieved) stats.
        """
        if not object_data:
            object_data = {}

        MutyLogger.get_instance().debug(
            "---> create/get stats: req_id=%s, operation_id=%s, sess=%s, user_id=%s, stats_type=%s",
            req_id,
            operation_id,
            sess,
            user_id,
            stats_type,
        )

        # determine expiration time
        time_expire: int = 0
        time_updated = muty.time.now_msec()
        if not never_expire:
            # set expiration time based on config
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
                # update existing stats as ongoing, and update time_expire if needed
                s.status = GulpRequestStatus.ONGOING.value
                s.time_updated = time_updated
                s.time_finished = 0
                if time_expire > 0:
                    s.time_expire = time_expire

                await sess.commit()
                await sess.refresh(s)

                # notify the websocket
                data = s.to_dict(exclude_none=True)
                p = GulpCollabCreateUpdatePacket(obj=data, created=True)
                wsq = GulpWsSharedQueue.get_instance()
                await wsq.put(
                    WSDATA_STATS_UPDATE,
                    ws_id=ws_id,
                    user_id=s.user_id,
                    operation_id=s.operation_id,
                    req_id=req_id,
                    data=p.model_dump(exclude_none=True, exclude_defaults=True),
                    private=True,
                )
                return s

            # create new
            object_data = {
                "time_expire": time_expire,
                "operation_id": operation_id,
                "req_type": stats_type.value,
                "data": {},
            }
            return await super().create_internal(
                sess,
                object_data=object_data,
                obj_id=req_id,
                ws_id=ws_id,
                owner_id=user_id,
                ws_data_type=WSDATA_STATS_UPDATE,
                req_id=req_id,
            )
        except Exception as e:
            await sess.rollback()
            raise e

    async def update_query_stats(
        self,
        sess: AsyncSession,
        user_id: str,
        hits: int,
        errors: list[str] = None,
        ws_id: str = None,
    ) -> dict:
        try:
            # acquire lock and get the latest data
            await self.__class__.acquire_advisory_lock(sess, self.id)
            await sess.refresh(self)

            # update stats
            data: dict = self.data or {}
            data["records_ingested"] = data.get("records_ingested", 0) + ingested
            data["records_skipped"] = data.get("records_skipped", 0) + skipped
            data["records_processed"] = data.get("records_processed", 0) + processed
            data["records_failed"] = data.get("records_failed", 0) + failed
            if errors:
                if "error" not in data or not data["error"]:
                    data["error"] = []
                for e in errors:
                    e_str = str(e)
                    if e_str not in data["error"]:
                        data["error"].append(e_str)
            self.data = data  # mark as modified
            updated_dict: dict = await self.update(
                sess,
                ws_id=ws_id,
                user_id=user_id,
                ws_data_type=WSDATA_STATS_UPDATE,
            )
            return updated_dict
        finally:
            # commit the transaction to release the lock
            await sess.commit()

    async def update_enrichment_stats(
        self,
        sess: AsyncSession,
        user_id: str,
        total_hits: int,
        enriched: int,
        ws_id: str = None,
        status: GulpRequestStatus = None,
        send_progress: bool = True,
    ) -> dict:
        try:
            # acquire lock and get the latest data
            await self.__class__.acquire_advisory_lock(sess, self.id)
            await sess.refresh(self)

            # update stats
            data: dict = self.data or {}
            data["current_enriched"] = data.get("enriched", 0) + enriched
            data["total_hits"] = total_hits
            if status:
                self.status = status.value
            self.data = data  # mark as modified for the ORM
            updated_dict: dict = await self.update(
                sess,
                ws_id=ws_id,
                user_id=user_id,
                ws_data_type=WSDATA_STATS_UPDATE,
            )
            return updated_dict
        finally:
            # commit the transaction to release the lock
            await sess.commit()

    async def update_ingestion_stats(
        self,
        sess: AsyncSession,
        user_id: str,
        ingested: int = 0,
        skipped: int = 0,
        processed: int = 0,
        failed: int = 0,
        errors: list[str] = None,
        status: GulpRequestStatus = None,
        ws_id: str = None,
    ) -> dict:
        try:
            # acquire lock and get the latest data
            await self.__class__.acquire_advisory_lock(sess, self.id)
            await sess.refresh(self)

            # update stats
            data: dict = self.data or {}
            data["records_ingested"] = data.get("records_ingested", 0) + ingested
            data["records_skipped"] = data.get("records_skipped", 0) + skipped
            data["records_processed"] = data.get("records_processed", 0) + processed
            data["records_failed"] = data.get("records_failed", 0) + failed
            if status:
                self.status = status.value
            if errors:
                if "error" not in data or not data["error"]:
                    data["error"] = []
                for e in errors:
                    e_str = str(e)
                    if e_str not in data["error"]:
                        data["error"].append(e_str)
            self.data = data  # mark as modified
            updated_dict: dict = await self.update(
                sess,
                ws_id=ws_id,
                user_id=user_id,
                ws_data_type=WSDATA_STATS_UPDATE,
            )
            return updated_dict
        finally:
            # commit the transaction to release the lock
            await sess.commit()

    @override
    async def update_running_stats(
        self,
        sess: AsyncSession,
        user_id: str,
        d: dict,
        ws_id: str = None,
    ) -> dict:
        """ """
        log = MutyLogger.get_instance()
        should_update: bool = True

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
                # return current state: we still need to commit the transaction in finally block, to release the lock
                should_update = False
                return self.to_dict()

            # apply updates from d
            time_expire: int = d.get("time_expire", 0)
            if time_expire > 0:
                # update time_expire if provided
                self.time_expire = time_expire

            data: dict = d.get("data", {})
            self.source_processed += data.get("source_processed", 0)
            self.source_failed += data.get("source_failed", 0)
            self.records_failed += data.get("records_failed", 0)
            self.records_skipped += data.get("records_skipped", 0)
            self.records_processed += data.get("records_processed", 0)
            self.records_ingested += data.get("records_ingested", 0)
            self.total_hits = d.get("total_hits", 0)

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
            log.debug(
                "---> update stats (pre): %s, ws_data_type=%s, ws_id=%s"
                % (self, ws_data_type, ws_id)
            )
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
                    'request "%s" **COMPLETED** with status=%s, total time: %d seconds, ws_data_type=%s, ws_id=%s',
                    self.id,
                    self.status,
                    (
                        (self.time_finished - self.time_created) / 1000
                        if self.time_created
                        else -1
                    ),
                    ws_data_type,
                    ws_id,
                )

            # --- call parent update ---
            # note: the parent's update handles the commit and websocket notification.
            # it uses the current state of 'self', so passing d=None is correct if parent doesn't need incremental changes.
            updated_dict: dict = await super().update(
                sess,
                d=None,
                ws_id=ws_id,
                user_id=user_id,
                ws_data_type=ws_data_type,
                ws_data=ws_data,  # pass through ws_data
                req_id=req_id or self.id,  # pass through req_id or use self.id
            )
            return updated_dict

        except Exception as e:
            await sess.rollback()
            raise e
        finally:
            if not should_update:
                # if we didn't update, we still need to release the lock
                await sess.commit()

    @staticmethod
    async def finalize(
        sess: AsyncSession,
        req_id: str,
        ws_id: str,
        user_id: str,
        errors: list[str] = None,
        hits: int = 0,
    ) -> dict:
        """
        sets the final status of a (generic) stats:

        - if errors is not empty/None, the status is set to failed, otherwise to done.
        - if the stats was already canceled, it is not modified.

        Args:
            sess(AsyncSession): collab database session
            req_id(str): the request id
            ws_id(str): the websocket id
            user_id(str): the user id
            errors(list[str], optional): the list of errors (default: None)

        Returns:
            dict: the updated stats as a dictionary or None if stats not found
        """
        status: str = GulpRequestStatus.DONE
        try:
            await GulpRequestStats.acquire_advisory_lock(sess, req_id)
            stats: GulpRequestStats = await GulpRequestStats.get_by_id(
                sess, req_id, throw_if_not_found=False
            )
            if not stats:
                return None

            dd: dict = {}
            if stats.status != GulpRequestStatus.CANCELED.value:
                # mark as completed
                if errors:
                    status = GulpRequestStatus.FAILED
                else:
                    status = GulpRequestStatus.DONE
                object_data = {
                    "status": status,
                    "data": {
                        "error": errors or [],
                        "total_hits": hits,
                    },
                }
                dd = await stats.update(sess, object_data, user_id=user_id, ws_id=ws_id)
            else:
                # already canceled, just return current state
                dd = stats.to_dict(exclude_none=True)

        except Exception as e:
            await sess.rollback()
            raise e

        return dd

    @staticmethod
    async def finalize_query_stats(
        sess: AsyncSession,
        req_id: str,
        ws_id: str,
        user_id: str,
        q_name: str = None,
        hits: int = 0,
        ws_data_type: str = WSDATA_QUERY_DONE,
        errors: list[str] = None,
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
            ws_data_type(str, optional): the websocket queue data type (default: WSDATA_QUERY_DONE)
            errors(list[str], optional): the list of errors (default: None)
            send_query_done(bool, optional): whether to send the query done packet to the websocket (default: True)
            num_queries(int, optional): the number of queries performed (default: 0)
            q_group(str, optional): the query group (default: None)

        Returns:
            dict: the updated stats as a dictionary (empty if stats not found)
        """
        dd: dict = await GulpRequestStats.finalize(
            sess, req_id, ws_id, user_id, errors=errors, hits=hits
        )
        if not dd:
            return {}

        # inform the websocket
        MutyLogger.get_instance().debug(
            f"sending query done packet, datatype={ws_data_type}, errors={errors}"
        )
        p = GulpQueryDonePacket(
            status=dd.get("status", GulpRequestStatus.DONE.value),
            errors=errors or [],
            total_hits=hits,
            queries=num_queries,
            name=q_name,
            group=q_group,
        )
        wsq = GulpWsSharedQueue.get_instance()
        await wsq.put(
            type=ws_data_type,
            ws_id=ws_id,
            operation_id=dd.get("operation_id"),
            user_id=user_id,
            req_id=req_id,
            data=p.model_dump(exclude_none=True),
        )
        return dd

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
        sess: AsyncSession = kwargs["sess"]
        user_id: str = kwargs["user_id"]
        try:
            await GulpRequestStats.acquire_advisory_lock(sess, obj_id)
            s: GulpRequestStats = await cls.get_by_id(sess, obj_id)
            dd = await s.update(sess, d=d, ws_id=ws_id, user_id=user_id)
            return dd
        except Exception as e:
            await sess.rollback()
            raise e

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
            "time_finished": muty.time.now_msec(),
        }

        await super().update(sess, d=d)
