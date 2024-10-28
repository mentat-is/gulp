from typing import Optional, Union, override
import muty.log
import muty.time
from sqlalchemy import BIGINT, ForeignKey, Index, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import Enum as SQLEnum
from gulp import config
from gulp.api.collab.structs import GulpCollabType, GulpRequestStatus, GulpCollabBase, T
from gulp.utils import logger


class GulpStatsBase(GulpCollabBase):
    """
    Represents the base class for statistics
    the id of the stats corresponds to the request "req_id" (unique per request).
    """

    operation: Mapped[Optional[str]] = mapped_column(
        ForeignKey("operation.id", ondelete="CASCADE"),
        default=None,
        doc="The operation associated with the stats.",
    )
    context: Mapped[str] = mapped_column(
        ForeignKey("context.id", ondelete="CASCADE"),
        default=None,
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

    __mapper_args__ = {
        "polymorphic_identity": "stats_base",
    }
    __abstract__ = True

    def __init__(self, *args, **kwargs):
        if type(self) is GulpStatsBase:
            raise TypeError("GulpStatsBase cannot be instantiated directly")

    @override
    @classmethod
    async def update_by_id(
        cls,
        id: str,
        d: dict | T,
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
        throw_if_not_found: bool = True,
    ) -> T:
        raise NotImplementedError(
            "update_by_id @classmethod not implemented, use instance method instead"
        )

    @override
    @classmethod
    async def delete_by_id(
        cls,
        id: str,
        sess: AsyncSession = None,
        throw_if_not_found: bool = True,
    ) -> None:
        raise NotImplementedError("delete_by_id @classmethod not implemented")

    @override
    @classmethod
    async def _create(
        cls,
        id: str,
        owner: str,
        operation: str = None,
        context: str = None,
        ws_id: str = None,
        req_id: str = None,
        **kwargs,
    ) -> T:

        # configure expiration
        time_expire = config.stats_ttl() * 1000
        if time_expire > 0:
            time_expire = muty.time.now_msec() + time_expire

        args = {
            "operation": operation,
            "context": context,
            "time_expire": time_expire,
            **kwargs,
        }
        return await super()._create(
            id,
            GulpCollabType.STATS_INGESTION,
            owner,
            ws_id,
            req_id,
            **args,
        )

    async def create_or_get(
        cls,
        id: str,
        owner: str,
        operation: str = None,
        context: str = None,
        sess: AsyncSession = None,
        **kwargs,
    ) -> T:
        """
        Create new or get an existing GulpStats record.

        Args:
            id (str): The unique identifier of the stats.
            owner (str): The owner of the stats.
            operation (str, optional): The operation associated with the stats. Defaults to None.
            context (str, optional): The context associated with the stats. Defaults to None.
            sess (AsyncSession, optional): The database session. Defaults to None.
            kwargs: Additional keyword arguments.
        Returns:
            GulpStats: The created CollabStats object.
        """
        if cls.__class__ is GulpStatsBase:
            raise NotImplementedError(
                "GulpStatsBase is an abstract class, use GulpIngestionStats or GulpQueryStats."
            )
        existing = await cls.get_one_by_id(id, sess=sess, throw_if_not_found=False)
        if existing:
            return existing

        # create new
        stats = await cls._create(
            id=id, owner=owner, operation=operation, context=context, **kwargs
        )
        return stats


class GulpIngestionStats(GulpStatsBase):
    """
    Represents the statistics for an ingestion operation.
    """

    __tablename__ = GulpCollabType.STATS_INGESTION.value
    errors: Mapped[Optional[dict]] = mapped_column(
        JSONB, default=None, doc="The errors that occurred during processing."
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

    __mapper_args__ = {
        "polymorphic_identity": GulpCollabType.STATS_INGESTION.value,
    }

    __table_args__ = (Index("idx_stats_operation", "operation"),)

    def _reset_buffer(self):
        self._buffer = {
            "errors": {},
            "source_processed": 0,
            "source_total": 0,
            "source_failed": 0,
            "records_failed": 0,
            "records_skipped": 0,
            "records_processed": 0,
        }

    def __init__(self, *args, **kwargs):
        self._reset_buffer()
        super().__init__(*args, **kwargs)

    @override
    @classmethod
    async def _create(
        cls,
        id: str,
        owner: str,
        operation: str = None,
        context: str = None,
        source_total: int = 0,
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
        **kwargs,
    ) -> T:
        args = {"source_total": source_total}
        return await super()._create(
            id,
            owner,
            operation,
            context,
            ws_id,
            req_id,
            sess,
            **args,
        )

    def _update_buffered(
        self,
        errors: dict[str, list[str]] = None,
        source_processed: int = 0,
        source_total: int = 0,
        source_failed: int = 0,
        records_failed: int = 0,
        records_skipped: int = 0,
        records_processed: int = 0,
    ) -> None:
        """
        Updates the buffered statistics with the provided values.
        """
        self.buffer["source_processed"] += source_processed
        self.buffered["source_total"] += source_total
        self.buffered["source_failed"] += source_failed
        self.buffered["records_failed"] += records_failed
        self.buffered["records_skipped"] += records_skipped
        self.buffered["records_processed"] += records_processed
        if errors:
            for k, v in errors:
                if k not in self.buffer["errors"]:
                    self.buffer["errors"][k] = []
                for e in v:
                    if e not in self.buffer["errors"][k]:
                        self.buffered["errors"][k].append(e)

    async def update(
        self,
        status: GulpRequestStatus = GulpRequestStatus.ONGOING,
        errors: dict[str, list[str]] = None,
        source_processed: int = 0,
        source_total: int = 0,
        source_failed: int = 0,
        records_failed: int = 0,
        records_skipped: int = 0,
        records_processed: int = 0,
        ws_id: str = None,
        throw_if_not_found: bool = True,
    ) -> None:
        """
        Asynchronously updates the status and statistics of a Gulp request.
        Args:
            status (GulpRequestStatus, optional): The current status of the request. Defaults to GulpRequestStatus.ONGOING.
            errors (dict[str, list[str]], optional): A dictionary of errors encountered during processing. Defaults to None.
            source_processed (int, optional): The number of sources processed. Defaults to 0.
            source_total (int, optional): The total number of sources. Defaults to 0.
            source_failed (int, optional): The number of sources that failed. Defaults to 0.
            records_failed (int, optional): The number of records that failed. Defaults to 0.
            records_skipped (int, optional): The number of records that were skipped. Defaults to 0.
            records_processed (int, optional): The number of records that were processed. Defaults to 0.
            ws_id (str, optional): The workspace ID. Defaults to None.
            throw_if_not_found (bool, optional): Whether to throw an exception if the request is not found. Defaults to True.
        Returns:
            None
        """

        # update buffer and status
        self._update_buffered(
            errors=errors,
            source_processed=source_processed,
            source_total=source_total,
            source_failed=source_failed,
            records_failed=records_failed,
            records_skipped=records_skipped,
            records_processed=records_processed,
        )
        self.status = status
        done: bool = False

        # check threshold
        threshold = config.stats_update_threshold()
        failure_threshold = config.ingestion_evt_failure_threshold()

        if (
            failure_threshold > 0
            and self.type == GulpCollabType.STATS_INGESTION
            and self.source_failed >= failure_threshold
        ):
            # too many failures, abort
            logger().error(
                "TOO MANY FAILURES REQ_ID=%s (failed=%d, threshold=%d), aborting ingestion!"
                % (self.id, self.source_failed, failure_threshold)
            )
            self.status = GulpRequestStatus.FAILED

        if status in [
            GulpRequestStatus.CANCELED,
            GulpRequestStatus.FAILED,
            GulpRequestStatus.DONE,
        ]:
            self.time_finished = muty.time.now_msec()
            done = True
            logger().debug("request finished, setting final status: %s" % (self))

        if self.source_processed % threshold == 0 or done:
            # write on db
            del self.buffer
            await super().update_by_id(
                self.id,
                self.to_dict(),
                ws_id,
                self.id,
                throw_if_not_found=throw_if_not_found,
            )
            self._reset_buffer()
