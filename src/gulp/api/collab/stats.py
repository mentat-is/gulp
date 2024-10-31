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
from gulp.api.collab_api import session
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
        self._buffer = {}
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
        ws_id: str = None,
        req_id: str = None,
        throw_if_not_found: bool = True,
    ) -> None:
        raise NotImplementedError("delete_by_id @classmethod not implemented")

    @override
    @classmethod
    async def _create(
        cls,
        id: str,
        type: GulpCollabType,
        owner: str,
        ws_id: str = None,
        req_id: str = None,
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

    @staticmethod
    async def create_or_get(
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
            id (str): The unique identifier of the stats (= "req_id" from the request)
            owner (str): The owner of the stats.
            operation (str, optional): The operation associated with the stats. Defaults to None.
            context (str, optional): The context associated with the stats. Defaults to None.
            sess (AsyncSession, optional): The database session. Defaults to None.
            kwargs: Additional keyword arguments.
        Returns:
            GulpStats: The created CollabStats object.
        """
        existing = await GulpIngestionStats.get_one_by_id(
            id, sess=sess, throw_if_not_found=False
        )
        if existing:
            return existing

        # create new
        stats = await GulpIngestionStats._create(
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
            "errors": [],
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
        error: str | list[str] | Exception = None,
        source_processed: int = 0,
        source_failed: int = 0,
        records_failed: int = 0,
        records_skipped: int = 0,
        records_processed: int = 0,
    ) -> None:
        """
        Updates the buffered statistics with the provided values.
        """
        self._buffer["source_processed"] += source_processed
        self._buffer["source_failed"] += source_failed
        self._buffer["records_failed"] += records_failed
        self._buffer["records_skipped"] += records_skipped
        self._buffer["records_processed"] += records_processed
        if error:
            if isinstance(error, Exception):
                error = str(error)
            if error not in self._buffer["errors"]:
                self._buffer["errors"].append(error)

    @classmethod
    async def cancel_by_id(cls, id: str, ws_id: str = None) -> None:
        """
        Cancels a running request.

        Args:
            id (str): The request ID.
            ws_id (str, optional): The workspace ID. Defaults to None.
        """
        await cls.update_by_id(
            id,
            status=GulpRequestStatus.CANCELED,
            ws_id=ws_id,
            throw_if_not_found=False,
        )

    async def cancel(self, ws_id: str = None) -> None:
        """
        Camcels the current request.
        Args:
            ws_id (str, optional): The workspace ID. Defaults to None.
        Returns:
            None
        """
        await self.update(
            status=GulpRequestStatus.CANCELED,
            ws_id=ws_id,
            throw_if_not_found=False,
        )

    async def update(
        self,
        status: GulpRequestStatus = GulpRequestStatus.ONGOING,
        error: str | Exception = None,
        source_processed: int = 0,
        source_failed: int = 0,
        records_failed: int = 0,
        records_skipped: int = 0,
        records_processed: int = 0,
        ws_id: str = None,
        throw_if_not_found: bool = True,
        force_flush: bool = False,
    ) -> None:
        """
        Asynchronously updates the status and statistics of a Gulp request.
        Args:
            status (GulpRequestStatus, optional): The current status of the request. Defaults to GulpRequestStatus.ONGOING.
            errors (dict[str, list[str]], optional): A dictionary of errors encountered during processing. Defaults to None.
            source_processed (int, optional): The number of sources processed. Defaults to 0.
            source_failed (int, optional): The number of sources that failed. Defaults to 0.
            records_failed (int, optional): The number of records that failed. Defaults to 0.
            records_skipped (int, optional): The number of records that were skipped. Defaults to 0.
            records_processed (int, optional): The number of records that were processed. Defaults to 0.
            ws_id (str, optional): The workspace ID. Defaults to None.
            throw_if_not_found (bool, optional): Whether to throw an exception if the request is not found. Defaults to True.
            force_flush (bool, optional): Whether to force the flush of the buffered statistics. Defaults to False.
        Returns:
            None
        """

        # update buffer and status
        self._update_buffered(
            error=error,
            source_processed=source_processed,
            source_failed=source_failed,
            records_failed=records_failed,
            records_skipped=records_skipped,
            records_processed=records_processed,
        )
        self.status = status

        if self.source_processed == self.source_total:
            logger().debug(
                "source_processed == source_total, setting status to DONE: %s" % (self)
            )
            self.status = GulpRequestStatus.DONE

        # check threshold
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
            force_flush = True
            logger().debug("request is finished: %s" % (self))

        if force_flush:
            # time to update on the storage
            async with await session() as sess:
                # be sure to read the latest version from db
                sess.add(self)
                await sess.refresh(self)

                # add buffered values to the instance
                self.source_processed += self._buffer["source_processed"]
                self.source_failed += self._buffer["source_failed"]
                self.records_failed += self._buffer["records_failed"]
                self.records_skipped += self._buffer["records_skipped"]
                self.records_processed += self._buffer["records_processed"]
                for e in self._buffer["errors"]:
                    if e not in self.errors:
                        self.errors.append(e)

                # update the instance
                await super().update_by_id(
                    self.id,
                    self.to_dict(),
                    ws_id,
                    self.id,
                    sess,
                    throw_if_not_found=throw_if_not_found,
                )

                # TODO: update ws

                self._reset_buffer()
