from typing import Optional, Tuple, override

import muty.crypto
import muty.log
import muty.time
from muty.log import MutyLogger
from sqlalchemy import ARRAY, BIGINT, ForeignKey, Index, Integer, String, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import Enum as SQLEnum

from gulp.api.collab.structs import GulpCollabBase, GulpCollabType, GulpRequestStatus, T
from gulp.api.ws_api import GulpWsQueueDataType
from gulp.config import GulpConfig


class RequestCanceledError(Exception):
    """
    Raised when a request is aborted (by API or in case of too many failures).
    """

    pass


class GulpIngestionStats(GulpCollabBase, type=GulpCollabType.INGESTION_STATS):
    """
    Represents the statistics for an ingestion operation.
    """

    operation_id: Mapped[str] = mapped_column(
        ForeignKey("operation.id", ondelete="CASCADE"),
        doc="The operation associated with the stats.",
    )
    context_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("context.id", ondelete="CASCADE"),
        doc="The context associated with the stats.",
    )
    status: Mapped[GulpRequestStatus] = mapped_column(
        SQLEnum(GulpRequestStatus),
        default=GulpRequestStatus.ONGOING,
        doc="The status of the stats (done, ongoing, ...).",
    )
    time_expire: Mapped[Optional[int]] = mapped_column(
        BIGINT, default=0, doc="The timestamp when the stats will expire."
    )
    time_finished: Mapped[Optional[int]] = mapped_column(
        BIGINT, default=0, doc="The timestamp when the stats were completed."
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
        source_total: int = 1,
    ) -> T:
        """
        Create new (or get an existing) GulpIngestionStats object on the collab database.

        Args:
            sess (AsyncSession): The database session to use.
            user_id (str): The user ID creating the stats.
            req_id (str): The request ID (= the id of the stats)
            ws_id (str): The websocket ID.
            operation_id (str): The operation associated with the stats
            context_id (str): The context associated with the stats
            source_total (int, optional): The total number of sources to be processed by the request to which this stats belong. Defaults to 1.

        Returns:
            T: The created stats.
        """
        MutyLogger.get_instance().debug(
            "---> create_or_get: id=%s, operation_id=%s, context_id=%s, source_total=%d",
            req_id,
            operation_id,
            context_id,
            source_total,
        )

        # acquire an advisory lock
        lock_id = muty.crypto.hash_xxh64_int(req_id)
        await sess.execute(
            text("SELECT pg_advisory_xact_lock(:lock_id)"), {"lock_id": lock_id}
        )

        # check if the stats already exist
        s: GulpIngestionStats = await cls.get_by_id(
            sess, id=req_id, throw_if_not_found=False
        )
        if s:
            return s

        # configure expiration
        time_expire = GulpConfig.get_instance().stats_ttl() * 1000
        if time_expire > 0:
            now = muty.time.now_msec()
            time_expire = muty.time.now_msec() + time_expire
            MutyLogger.get_instance().debug(
                'now=%s, setting stats "%s".time_expire to %s', now, id, time_expire
            )

        object_data = {
            "time_expire": time_expire,
            "operation_id": operation_id,
            "context_id": context_id,
            "source_total": source_total,
        }
        return await super()._create(
            sess,
            object_data=object_data,
            id=req_id,
            ws_id=ws_id,
            user_id=user_id,
            ws_queue_datatype=GulpWsQueueDataType.STATS_UPDATE,
            req_id=req_id,
        )
        return s

    async def cancel(
        self,
        sess: AsyncSession,
        ws_id: str,
        user_id: str,
    ):
        """
        Cancel the stats.

        Args:
            sess (AsyncSession): The database session to use.
            ws_id (str): The websocket ID.
            user_id (str): The user ID who cancels the stats.
        """
        return await super().update(
            sess,
            {
                "status": GulpRequestStatus.CANCELED,
            },
            ws_id=ws_id,
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

    @override
    async def update(
        self,
        sess: AsyncSession,
        d: dict,
        ws_id: str,
        user_id: str,
        **kwargs,
    ) -> None:
        """
        Update the stats.

        Args:
            sess (AsyncSession): The database session to use.
            d (dict): The dictionary of values to update.
            ws_id (str): The websocket ID.
            user_id (str): The user ID updating the stats.
            kwargs: Additional keyword arguments.

        Returns:
            T: The updated stats.
        """
        if self.status in [
            GulpRequestStatus.CANCELED,
            GulpRequestStatus.FAILED,
        ]:
            # already processed, nothing to do
            MutyLogger.get_instance().warning(
                "request already set to %s, nothing to do" % (self.status)
            )
            return self

        # get stats from db first
        await sess.refresh(self, with_for_update=True)
    
        # update
        self.source_processed += d.get("source_processed", 0)
        self.source_failed += d.get("source_failed", 0)
        self.records_failed += d.get("records_failed", 0)
        self.records_skipped += d.get("records_skipped", 0)
        self.records_processed += d.get("records_processed", 0)
        self.records_ingested += d.get("records_ingested", 0)
        error = d.get("error", None)
        if error:
            if not self.errors:
                self.errors = []
            if isinstance(error, Exception):
                # MutyLogger.get_instance().error(f"PRE-COMMIT: ex error={error}")
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

        status = d.get("status", None)
        if status:
            self.status = status

        msg = f"---> update: ws_id={ws_id}, d={d}, kwargs={kwargs}"
        if error:
            MutyLogger.get_instance().error(msg)
        else:
            MutyLogger.get_instance().debug(msg)

        if self.source_processed == self.source_total:
            MutyLogger.get_instance().debug(
                'source_processed == source_total, setting request "%s" to DONE'
                % (self.id)
            )
            self.status = GulpRequestStatus.DONE

        if self.source_failed == self.source_total:
            MutyLogger.get_instance().error(
                'source_failed == source_total, setting request "%s" to FAILED'
                % (self.id)
            )
            self.status = GulpRequestStatus.FAILED

        # check threshold
        failure_threshold = GulpConfig.get_instance().ingestion_evt_failure_threshold()
        if (
            failure_threshold > 0
            and self.type == GulpCollabType.INGESTION_STATS
            and (
                self.records_failed >= failure_threshold
                or self.records_skipped >= failure_threshold
            )
        ):
            # too many failures, abort
            MutyLogger.get_instance().error(
                "TOO MANY FAILURES req_id=%s (failed=%d, threshold=%d), aborting ingestion!"
                % (self.id, self.source_failed, failure_threshold)
            )
            self.status = GulpRequestStatus.CANCELED

        if self.status == GulpRequestStatus.DONE:
            # if no records were processed and some failed, set to FAILED
            if self.records_processed == 0 and self.records_failed > 0:
                self.status = GulpRequestStatus.FAILED

        if self.status in [
            GulpRequestStatus.CANCELED,
            GulpRequestStatus.FAILED,
            GulpRequestStatus.DONE,
        ]:
            self.time_finished = muty.time.now_msec()
            MutyLogger.get_instance().debug(
                'request "%s" COMPLETED with status=%s' % (self.id, self.status)
            )
            # print the time it took to complete the request, in seconds
            MutyLogger.get_instance().debug(
                "*** TOTAL TIME TOOK by request %s: %s seconds ***"
                % (self.id, (self.time_finished - self.time_created) / 1000)
            )

        # update the instance (will update websocket too)
        await super().update(
            sess,
            d=None,
            ws_id=ws_id,
            user_id=user_id,
            ws_queue_datatype=GulpWsQueueDataType.STATS_UPDATE,
            req_id=self.id,
            updated_instance=self,
            **kwargs,
        )

        if self.status == GulpRequestStatus.CANCELED:
            MutyLogger.get_instance().error('request "%s" set to CANCELED' % (self.id))
            raise RequestCanceledError()
