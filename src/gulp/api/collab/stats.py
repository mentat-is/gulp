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
    Represents the base class for statistics for ingestion or query operations.
    the id of the stats corresponds to the request "req_id" (unique per request).
    """

    operation: Mapped[Optional[str]] = mapped_column(
        ForeignKey("operation.name", ondelete="CASCADE"),
        default=None,
        doc="The operation associated with the stats.",
    )
    context: Mapped[str] = mapped_column(
        ForeignKey("context.name", ondelete="CASCADE"),
        default=None,
        doc="The context associated with the stats.",
    )
    status: Mapped[GulpRequestStatus] = mapped_column(
        String,
        default=GulpRequestStatus.ONGOING.value,
        doc="The status of the stats (done, ongoing, ...).",
    )
    time_expire: Mapped[Optional[int]] = mapped_column(
        BIGINT, default=0, doc="The timestamp when the stats will expire."
    )
    time_finished: Mapped[Optional[int]] = mapped_column(
        BIGINT, default=0, doc="The timestamp when the stats were completed."
    )

    # index for operation
    __table_args__ = (Index("idx_operation", "operation"),)

    __mapper_args__ = {
        "polymorphic_identity": "stats_base",
    }
    __abstract__ = True

    def __init__(self, *args, **kwargs):
        if type(self) is GulpStatsBase:
            raise TypeError("GulpStatsBase cannot be instantiated directly")

    @override
    @classmethod
    async def update(
        cls,
        id: str,
        d: dict | T,
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
        commit: bool = True,
        throw_if_not_found: bool = True,
    ) -> T:
        raise NotImplementedError("update method not implemented")

    @override
    @classmethod
    async def delete(
        cls,
        id: str,
        sess: AsyncSession = None,
        commit: bool = True,
        throw_if_not_found: bool = True,
    ) -> None:
        raise NotImplementedError("delete method not implemented")

    @override
    @classmethod
    async def _create(
        cls,
        id: str,
        user: Union[str, "GulpUser"],
        operation: str = None,
        context: str = None,
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
        commit: bool = True,
        **kwargs,
    ) -> T:

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
            user,
            ws_id,
            req_id,
            sess,
            commit,
            **args,
        )

    async def create_or_get(
        cls,
        id: str,
        user: Union[str, "GulpUser"],
        operation: str = None,
        context: str = None,
        sess: AsyncSession = None,
        **kwargs,
    ) -> T:
        """
        Create new or get a GulpStats record.

        Args:
            id (str): The unique identifier of the stats.
            user (str | GulpUser): The user associated with the stats.
            operation (str, optional): The operation associated with the stats. Defaults to None.
            context (str, optional): The context associated with the stats. Defaults to None.
            sess (AsyncSession, optional): The database session. Defaults to None.
            commit (bool, optional): Whether to commit the transaction. Defaults to True.
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
            id=id, user=user, operation=operation, context=context, **kwargs
        )
        return stats


class GulpQueryStats(GulpStatsBase):
    """
    Represents the statistics for a query operation.

    TODO: is this really needed ?
    """

    __tablename__ = GulpCollabType.STATS_QUERY.value

    __mapper_args__ = {
        "polymorphic_identity": GulpCollabType.STATS_QUERY.value,
    }

    async def update(
        self,
        status: GulpRequestStatus = None,
        ws_id: str = None,
        sess: AsyncSession = None,
        commit: bool = True,
        throw_if_not_found: bool = True,
    ) -> None:
        # for query stats, we write directly
        await super().update(
            self.id,
            self.to_dict(),
            ws_id,
            self.id,
            sess,
            commit,
            throw_if_not_found,
        )


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
        user: Union[str, "GulpUser"],
        operation: str = None,
        context: str = None,
        source_total: int = 0,
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
        commit: bool = True,
        **kwargs,
    ) -> T:
        args = {"source_total": source_total}
        return await super()._create(
            id,
            user,
            operation,
            context,
            ws_id,
            req_id,
            sess,
            commit,
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
    ):
        """
        Updates the buffered statistics with the provided values.
        Parameters:
        errors (dict[str, list[str]], optional): A dictionary of errors where the key is a string (the source)
                                                    and the vue is a list of error messages. Defaults to None.
        source_processed (int, optional): The number of sources processed. Defaults to 0.
        source_total (int, optional): The total number of sources. Defaults to 0.
        source_failed (int, optional): The number of sources that failed. Defaults to 0.
        records_failed (int, optional): The number of records that failed. Defaults to 0.
        records_skipped (int, optional): The number of records that were skipped. Defaults to 0.
        records_processed (int, optional): The number of records that were processed. Defaults to 0.
        Returns:
        None
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
        sess: AsyncSession = None,
        commit: bool = True,
        throw_if_not_found: bool = True,
    ) -> None:

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
        self.status = status.value
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
            logger().debug("request finished, setting final status: %s" % (self))

        if self.source_processed % threshold == 0 or done:
            # write on db
            del self.buffer
            await super().update(
                self.id,
                self.to_dict(),
                ws_id,
                self.id,
                sess,
                commit,
                throw_if_not_found,
            )
            self._reset_buffer()
