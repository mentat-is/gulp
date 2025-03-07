from typing import Optional, Union, override

import muty.crypto
import muty.log
import muty.time
from muty.log import MutyLogger
from sqlalchemy import ARRAY, BIGINT, ForeignKey, Index, Integer, String
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import Enum as SQLEnum

from gulp.api.collab.structs import GulpCollabBase, GulpCollabType, GulpRequestStatus, T
from gulp.api.ws_api import GulpQueryDonePacket, GulpWsQueueDataType, GulpWsSharedQueue
from gulp.config import GulpConfig


class RequestCanceledError(Exception):
    """
    Raised when a request is aborted (by API or in case of too many failures).
    """

    pass


class SourceCanceledError(Exception):
    """
    Raised when a source is aborted (by API or in case of too many failures).
    """

    pass


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
    async def _update_existing_stats(
        cls,
        sess: AsyncSession,
        stats: "GulpRequestStats",
        time_updated: int,
        time_expire: int,
    ) -> "GulpRequestStats":
        """
        update existing stats object.

        Args:
            sess (AsyncSession): database session
            stats (GulpRequestStats): existing stats object
            time_updated (int): current timestamp
            time_expire (int): expiration timestamp

        Returns:
            GulpRequestStats: updated stats object
        """
        # update existing stats
        stats.status = GulpRequestStatus.ONGOING
        stats.time_updated = time_updated
        stats.time_finished = 0

        if time_expire > 0:
            stats.time_expire = time_expire

        await sess.commit()
        return stats

    @staticmethod
    def _calculate_expiration_time(never_expire: bool) -> int:
        """
        calculate the expiration time for stats.

        Args:
            never_expire (bool): whether the stats should never expire

        Returns:
            int: expiration time in milliseconds since epoch
        """
        if never_expire:
            return 0

        time_updated = muty.time.now_msec()
        msecs_to_expiration = GulpConfig.get_instance().stats_ttl() * 1000

        if msecs_to_expiration > 0:
            # MutyLogger.get_instance().debug("now=%s, setting stats %s time_expire to %s", time_updated, req_id, time_updated + time_expire)
            return time_updated + msecs_to_expiration

        return 0

    @classmethod
    async def create(
        cls,
        sess: AsyncSession,
        user_id: str,
        req_id: str,
        ws_id: str,
        operation_id: str,
        context_id: str,
        source_id: str = None,
        source_total: int = 1,
        never_expire: bool = False,
    ) -> T:
        """
        Create new (or get an existing) GulpRequestStats object on the collab database.

        Args:
            sess (AsyncSession): The database session to use.
            user_id (str): The user ID creating the stats.
            req_id (str): The request ID (= the id of the stats)
            ws_id (str): The websocket ID.
            operation_id (str): The operation associated with the stats
            source_id (str, optional): The source associated with the stats. Defaults to None.
            context_id (str): The context associated with the stats
            source_total (int, optional): The total number of sources to be processed by the request to which this stats belong. Defaults to 1.
            never_expire (bool, optional): Whether the stats should never expire, ignoring the configuration. Defaults to False.
        Returns:
            T: The created stats.
        """
        MutyLogger.get_instance().debug(
            "---> create: id=%s, operation_id=%s, context_id=%s, source_id=%s, source_total=%d",
            req_id,
            operation_id,
            context_id,
            source_id,
            source_total,
        )

        # acquire an advisory lock
        lock_id = muty.crypto.hash_xxh64_int(req_id)
        await GulpCollabBase.acquire_advisory_lock(sess, lock_id)

        # determine expiration time
        time_expire = cls._calculate_expiration_time(never_expire)
        time_updated = muty.time.now_msec()

        # check if the stats already exist
        s: GulpRequestStats = await cls.get_by_id(
            sess, id=req_id, throw_if_not_found=False
        )
        if s:
            # update existing stats
            return await cls._update_existing_stats(sess, s, time_updated, time_expire)

        object_data = {
            "time_expire": time_expire,
            "operation_id": operation_id,
            "context_id": context_id,
            "source_id": source_id,
            "source_total": source_total,
        }
        return await super()._create(
            sess,
            object_data=object_data,
            id=req_id,
            ws_id=ws_id,
            owner_id=user_id,
            ws_queue_datatype=GulpWsQueueDataType.STATS_UPDATE,
            req_id=req_id,
        )

    async def cancel(
        self,
        sess: AsyncSession,
        user_id: str,
    ):
        """
        Cancel the stats.

        Args:
            sess (AsyncSession): The database session to use.
            user_id (str): The user ID who cancels the stats.
        """
        # expires in 5 minutes, allow any loop to finish
        time_expire = muty.time.now_msec() + 60 * 1000 * 5
        return await super().update(
            sess,
            {
                "status": GulpRequestStatus.CANCELED,
                "time_expire": time_expire,
                "completed": "1",
                "time_finished": muty.time.now_msec(),
            },
            ws_id=None,
            user_id=user_id,
            req_id=self.id,
            ws_queue_datatype=GulpWsQueueDataType.STATS_UPDATE,
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
        sess: AsyncSession,
        id: str,
        user_id: str,
        ws_id: str,
        req_id: str,
        d: dict = None,
        updated_instance: T = None,
    ) -> dict:
        """
        same as base class update_by_id, but without checking token

        Args:
            sess (AsyncSession): The database session to use.
            id (str): The ID of the object to update.
            user_id (str): The user ID updating the object.
            ws_id (str): The websocket ID.
            req_id (str): The request ID.
            d (dict, optional): The data to update the object with. Defaults to None.
            updated_instance (T, optional): An already updated instance of the object. Defaults to None.

        Returns:
            dict: The updated object as a dictionary.

        Raises:
            ValueError: If both d and updated_instance are provided.
            MissingPermissionError: If the user does not have permission to update the object.
        """
        if d and updated_instance:
            raise ValueError("only one of d or updated_instance should be provided")

        n: GulpCollabBase = await cls.get_by_id(sess, id, with_for_update=True)
        try:
            await n.update(
                sess,
                d=d,
                ws_id=ws_id,
                user_id=user_id,
            )
        except:
            pass
        return n.to_dict(exclude_none=True)

    async def _refresh_and_lock(self, sess: AsyncSession) -> None:
        """
        refresh stats from db and acquire advisory lock.

        Args:
            sess (AsyncSession): database session
        """
        lock_id = muty.crypto.hash_xxh64_int(self.id)
        await GulpCollabBase.acquire_advisory_lock(sess, lock_id)
        await sess.refresh(self)

    def _update_counters(self, d: dict) -> None:
        """
        update counter fields from input dictionary.

        Args:
            d (dict): input data with counter updates
        """
        self.source_processed += d.get("source_processed", 0)
        self.source_failed += d.get("source_failed", 0)
        self.records_failed += d.get("records_failed", 0)
        self.records_skipped += d.get("records_skipped", 0)
        self.records_processed += d.get("records_processed", 0)
        self.records_ingested += d.get("records_ingested", 0)
        self.source_id = d.get("source_id", self.source_id)

    def _process_errors(
        self, error: Optional[Union[Exception, str, list[str]]]
    ) -> None:
        """
        process and store errors.

        Args:
            error (Exception|str|list[str], optional): error information to process
        """
        if not error:
            return

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

    def _handle_status_updates(self, d: dict) -> Optional[GulpRequestStatus]:
        """
        handle explicit status updates from input data.

        Args:
            d (dict): input data with possible status update

        Returns:
            GulpRequestStatus: forced status if provided in update
        """
        status: GulpRequestStatus = d.get("status", None)
        if status:
            self.status = status
        return status

    def _determine_status_based_on_state(self) -> None:
        """
        determine the status based on processing state and counts.
        """
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

    def _handle_completed_status(self) -> None:
        """
        handle finalization of the request when status indicates completion.
        """
        if self.status not in [
            GulpRequestStatus.CANCELED,
            GulpRequestStatus.FAILED,
            GulpRequestStatus.DONE,
        ]:
            return

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

    @override
    async def update(
        self,
        sess: AsyncSession,
        d: dict,
        ws_id: str,
        user_id: str,
    ) -> None:
        """
        Update the stats.

        Args:
            sess (AsyncSession): The database session to use.
            d (dict): The dictionary of values to update:
                source_processed (int): The number of sources processed.
                source_failed (int): The number of sources that failed.
                records_failed (int): The number of records that failed.
                records_skipped (int): The number of records that were skipped.
                records_processed (int): The number of records that were processed.
                records_ingested (int): The number of records that were ingested.
                error (str|Exception|list[str]): The error message or exception that occurred.
                status (GulpRequestStatus): The status of the stats.

            ws_id (str): The websocket ID.
            user_id (str): The user ID updating the stats.

        """
        # refresh stats and acquire lock
        await self._refresh_and_lock(sess)

        # update counters
        self._update_counters(d)

        # process errors if any
        self._process_errors(d.get("error", None))

        # handle status updates
        forced_status = self._handle_status_updates(d)

        # log update details
        msg = f"---> update: ws_id={ws_id}, d={d}"
        if "error" in d and d["error"]:
            MutyLogger.get_instance().error(msg)
        else:
            MutyLogger.get_instance().debug(msg)

        # determine status based on processing state
        self._determine_status_based_on_state()

        # handle completed status and update completion metrics
        self._handle_completed_status()

        # update the instance (will update websocket too)
        if forced_status:
            self.status = forced_status

        await super().update(
            sess,
            d=None,
            ws_id=ws_id,
            user_id=user_id,
            ws_queue_datatype=GulpWsQueueDataType.STATS_UPDATE,
            req_id=self.id,
            updated_instance=self,
        )

    @staticmethod
    async def _update_stats_for_query_completion(
        stats: "GulpRequestStats", hits: int, errors: Optional[list[str]]
    ) -> None:
        """
        update stats object for query completion.

        Args:
            stats (GulpRequestStats): stats object to update
            hits (int): number of hits
            errors (list[str], optional): list of errors
        """
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

    @staticmethod
    def _send_query_done_notification(
        ws_id: str,
        user_id: str,
        req_id: str,
        status: GulpRequestStatus,
        errors: Optional[list[str]],
        hits: int,
        q_name: Optional[str],
        ws_queue_datatype: GulpWsQueueDataType,
    ) -> None:
        """
        send query done notification through websocket.

        Args:
            ws_id (str): websocket id
            user_id (str): user id
            req_id (str): request id
            status (GulpRequestStatus): final status
            errors (list[str], optional): list of errors
            hits (int): number of hits
            q_name (str, optional): query name
            ws_queue_datatype (GulpWsQueueDataType): websocket queue data type
        """
        MutyLogger.get_instance().debug(f"sending query done packet, errors={errors}")

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
    ) -> None:
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
        stats: GulpRequestStats = await GulpRequestStats.get_by_id(
            sess, req_id, throw_if_not_found=False
        )
        status: GulpRequestStatus = GulpRequestStatus.DONE

        if stats and stats.status != GulpRequestStatus.CANCELED:
            await GulpRequestStats._update_stats_for_query_completion(
                stats, hits, errors
            )
            status = stats.status
            await sess.commit()

        if send_query_done:
            # inform the websocket
            GulpRequestStats._send_query_done_notification(
                ws_id, user_id, req_id, status, errors, hits, q_name, ws_queue_datatype
            )
