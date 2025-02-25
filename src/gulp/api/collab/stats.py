from typing import Optional, override

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
        # override to have 'gulpesque' keys
        d = super().to_dict(nested, hybrid_attributes, exclude, exclude_none)
        if "operation_id" in d:
            d["gulp.operation_id"] = d.pop("operation_id")
        if "context_id" in d:
            d["gulp.context_id"] = d.pop("context_id")
        if "source_id" in d:
            d["gulp.source_id"] = d.pop("source_id")
        return d

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

        time_expire: int = 0
        time_updated = muty.time.now_msec()
        if not never_expire:
            # configure expiration
            time_expire = GulpConfig.get_instance().stats_ttl() * 1000
            if time_expire > 0:
                time_expire = time_updated + time_expire
                # MutyLogger.get_instance().debug("now=%s, setting stats %s time_expire to %s", time_updated, req_id, time_expire)

        # check if the stats already exist
        s: GulpRequestStats = await cls.get_by_id(
            sess, id=req_id, throw_if_not_found=False
        )
        if s:
            # update existing stats
            s.status = GulpRequestStatus.ONGOING
            s.time_updated = time_updated
            s.time_finished = 0
            if time_expire > 0:
                # add new time expire
                s.time_expire = time_expire
            await sess.commit()
            return s

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
        # refresh stats from db first, use advisory lock here which is more
        # efficient than row-level lock with with_for_update
        lock_id = muty.crypto.hash_xxh64_int(self.id)
        await GulpCollabBase.acquire_advisory_lock(sess, lock_id)
        await sess.refresh(self)

        """
        if self.status in [
            GulpRequestStatus.CANCELED,
            GulpRequestStatus.FAILED,
            GulpRequestStatus.DONE,
        ]:
            # nothing to do, request is already done
            MutyLogger.get_instance().warning(
                'not updating request "%s" (already done/failed/canceled), status=%s'
                % (self.id, self.status)
            )
            raise RequestCanceledError()
        """

        # update
        self.source_processed += d.get("source_processed", 0)
        self.source_failed += d.get("source_failed", 0)
        self.records_failed += d.get("records_failed", 0)
        self.records_skipped += d.get("records_skipped", 0)
        self.records_processed += d.get("records_processed", 0)
        self.records_ingested += d.get("records_ingested", 0)
        self.source_id = d.get("source_id", self.source_id)

        error = d.get("error", None)
        if error:
            if not self.errors:
                self.errors = []
            if isinstance(error, Exception):
                MutyLogger.get_instance().exception(error)
                error = str(error)
                if error not in self.errors:
                    self.errors.append(error)
            elif isinstance(error, str):
                # MutyLogger.get_instance().error(f"PRE-COMMIT: str error={error}")
                if error not in self.errors:
                    self.errors.append(error)
            elif isinstance(error, list[str]):
                # MutyLogger.get_instance().error(f"PRE-COMMIT: list error={error}")
                for e in error:
                    if e not in self.errors:
                        self.errors.append(e)

        status: GulpRequestStatus = d.get("status", None)
        forced_status: GulpRequestStatus = status
        if status:
            self.status = status

        msg = f"---> update: ws_id={ws_id}, d={d}"
        if error:
            MutyLogger.get_instance().error(msg)
        else:
            MutyLogger.get_instance().debug(msg)

        if self.source_processed == self.source_total:
            MutyLogger.get_instance().debug(
                'source_processed: %d == source_total: %d, setting request "%s" to DONE'
                % (self.source_processed, self.source_total, self.id)
            )
            if self.records_processed > 0 and self.records_ingested == 0:
                # if some records were processed but none were ingested, set to FAILED
                self.status = GulpRequestStatus.FAILED
            else:
                self.status = GulpRequestStatus.DONE

        if self.source_failed == self.source_total:
            MutyLogger.get_instance().error(
                'source_failed: %d == source_total: %d, setting request "%s" to FAILED'
                % (self.source_failed, self.source_total, self.id)
            )
            self.status = GulpRequestStatus.FAILED

        if self.status == GulpRequestStatus.DONE:
            # if no records were processed and some failed, set to FAILED
            if self.records_processed == 0 and self.records_failed > 0:
                self.status = GulpRequestStatus.FAILED

        if self.status in [
            GulpRequestStatus.CANCELED,
            GulpRequestStatus.FAILED,
            GulpRequestStatus.DONE,
        ]:
            # print the time it took to complete the request, in seconds
            self.time_finished = muty.time.now_msec()
            MutyLogger.get_instance().info(
                'REQUEST TIME INFO: request "%s" COMPLETED with status=%s, TOTAL TIME: %d seconds'
                % (
                    self.id,
                    self.status,
                    (self.time_finished - self.time_created) / 1000,
                )
            )
            self.completed = "1"

        # update the instance (will update websocket too)
        if forced_status:
            # use the forced status
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
            # set completed
            stats.completed = "1"
            stats.time_finished = muty.time.now_msec()
            if hits >= 1:
                stats.status = GulpRequestStatus.DONE
            else:
                stats.status = GulpRequestStatus.FAILED
            if errors:
                if not stats.errors:
                    stats.errors = errors
                else:
                    stats.errors.extend(errors)
            status = stats.status
            stats.total_hits = hits
            MutyLogger.get_instance().debug("update_query_stats: %s" % (stats))
            await sess.commit()

        if send_query_done:
            # inform the websocket
            MutyLogger.get_instance().debug(
                "sending query done packet, errors=%s" % (errors)
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
