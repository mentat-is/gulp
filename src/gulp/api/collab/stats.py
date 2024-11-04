from typing import Optional, Union, override
import muty.log
import muty.time
from opensearchpy import Field
from pydantic import BaseModel
from sqlalchemy import BIGINT, ForeignKey, Index, Integer, String, ARRAY
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import Enum as SQLEnum
from gulp import config
from gulp.api.collab.structs import (
    GulpCollabType,
    GulpRequestStatus,
    GulpCollabBase,
    T,
    GulpUserPermission,
)
from gulp.api.collab_api import session
from gulp.utils import logger
from dotwiz import DotWiz


class RequestCanceledError(Exception):
    """
    Raised when a request is aborted (by API or in case of too many failures).
    """
    pass
class GulpStatsBase(GulpCollabBase, type="stats_base", abstract=True):
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

    def __init__(self, *args, **kwargs):
        if type(self) is GulpStatsBase:
            raise TypeError("GulpStatsBase cannot be instantiated directly")

    @override
    @classmethod
    async def update_by_id(
        cls,
        id: str,
        d: dict,
        token: str = None,
        permission: list[GulpUserPermission] = [GulpUserPermission.EDIT],
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
        throw_if_not_found: bool = True,
        **kwargs,
    ) -> T:
        raise NotImplementedError(
            "update_by_id @classmethod not implemented, use instance method instead"
        )

    @override
    @classmethod
    async def delete_by_id(
        cls,
        id: str,
        token: str = None,
        permission: list[GulpUserPermission] = [GulpUserPermission.DELETE],
        ws_id: str = None,
        req_id: str = None,
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
        ws_id: str = None,
        sess: AsyncSession = None,
        ensure_eager_load: bool=True,
        **kwargs,
    ) -> T:
        """
        Asynchronously creates a new GulpStats subclass instance
        Args:
            id (str): The unique identifier for the instance.
            owner (str): The owner of the instance.
            ws_id (str, optional): The websocket ID. Defaults to None.
            sess (AsyncSession, optional): The asynchronous session. Defaults to None.
            ensure_eager_load (bool, optional): Whether to ensure eager loading of the instance. Defaults to True.
            **kwargs: Additional keyword arguments.
        Keyword Args:
            operation (str, optional): The operation. Defaults to None.
            context (str, optional): The context of the operation. Defaults to None.
        Returns:
            T: The created instance.
        """

        operation: str = kwargs.get("operation", None)
        context: str = kwargs.get("context", None)

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
            owner,
            ws_id=ws_id,
            req_id=id,
            sess=sess,
            ensure_eager_load=ensure_eager_load,
            **args,
        )

    @classmethod
    async def _create_or_get(
        cls,
        id: str,
        owner: str,
        operation: str = None,
        context: str = None,
        sess: AsyncSession = None,
        ensure_eager_load: bool=True,
        **kwargs,
    ) -> T:
        existing = await cls.get_one_by_id(id, sess=sess, throw_if_not_found=False)
        if existing:
            return existing

        # create new
        stats = await cls._create(
            id=id,
            owner=owner,
            operation=operation,
            context=context,
            ensure_eager_load=ensure_eager_load,
            **kwargs,
        )
        return stats


class GulpIngestionStats(GulpStatsBase, type=GulpCollabType.STATS_INGESTION.value):
    """
    Represents the statistics for an ingestion operation.
    """

    errors: Mapped[Optional[list[str]]] = mapped_column(
        ARRAY(String), default=None, doc="The errors that occurred during processing."
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
    __table_args__ = (Index("idx_stats_operation", "operation"),)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    async def create_or_get(
        cls,
        id: str,
        owner: str,
        operation: str = None,
        context: str = None,
        source_total: int = 0,
        sess: AsyncSession = None,
        **kwargs,
    ) -> T:
        """
        Create new or get an existing GulpIngestionStats record.

        Args:
            id (str): The unique identifier of the stats (= "req_id" from the request)
            owner (str): The owner of the stats.
            operation (str, optional): The operation associated with the stats. Defaults to None.
            context (str, optional): The context associated with the stats. Defaults to None.
            sess (AsyncSession, optional): The database session. Defaults to None.
            kwargs: Additional keyword arguments.
        Keyword Args:
            source_total (int, optional): The total number of sources to be processed. Defaults to 0.
        Returns:
            GulpIngestionStats: The created CollabStats object.
        """
        return await cls._create_or_get(
            id,
            owner,
            operation,
            context,
            sess,
            source_total=source_total,
            **kwargs,
        )


    @classmethod
    async def cancel_by_id(cls, id: str, ws_id: str = None) -> None:
        """
        Cancels a running request.

        Args:
            id (str): The request ID.
            ws_id (str, optional): The websocket ID. Defaults to None.
        """
        await cls.update_by_id(
            id,
            {"status": GulpRequestStatus.CANCELED},
            ws_id=ws_id,
            req_id=id,
            throw_if_not_found=False,
        )

    async def cancel(self, ws_id: str = None) -> None:
        """
        Camcels the current request.
        Args:
            ws_id (str, optional): The websocket ID. Defaults to None.
        Returns:
            None
        """
        await self.update(
            {"status": GulpRequestStatus.CANCELED},
            ws_id=ws_id,
            req_id=self.id,
            throw_if_not_found=False
        )

    @override
    async def update(
        self,
        d: dict=None,
        ws_id: str = None,
        throw_if_not_found: bool = True,
        error: str | Exception | list[str] = None,
        status: GulpRequestStatus = None,
        source_processed: int = 0,
        source_failed: int = 0,
        records_failed: int = 0,
        records_skipped: int = 0,
        records_processed: int = 0,
        records_ingested: int = 0,
        **kwargs,
    ) -> T:
        """
        Asynchronously updates the status and statistics of a Gulp request.
        Args:
            d (dict): unused
            ws_id (str, optional): The websocket ID. Defaults to None.
            throw_if_not_found (bool, optional): If set, an exception is raised if the request is not found. Defaults to True.
            error (str|Exception|list[str], optional): The error/s to append. Defaults to None.
            status (GulpRequestStatus, optional): The status of the request. Defaults to None.
            source_processed (int, optional): The number of sources processed. Defaults to 0.
            source_failed (int, optional): The number of sources that failed. Defaults to 0.
            records_failed (int, optional): The number of records that failed. Defaults to 0.
            records_skipped (int, optional): The number of records that were skipped. Defaults to 0.
            records_processed (int, optional): The number of records that were processed. Defaults to 0.
            records_ingested (int, optional): The number of records that were ingested. Defaults to 0.
            **kwargs: Additional keyword arguments.
        Returns:
            T: The updated instance.
        Raises:
            RequestAbortError: If the request is aborted.
        """
        if status:
            self.status = status

        if self.source_processed == self.source_total:
            logger().debug(
                "source_processed == source_total, setting status to DONE: %s" % (self)
            )
            self.status = GulpRequestStatus.DONE
        
        if self.source_failed == self.source_total:
            logger().debug(
                "source_failed == source_total, setting status to FAILED: %s" % (self)
            )
            self.status = GulpRequestStatus.FAILED

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
            self.status = GulpRequestStatus.CANCELED

        if status in [
            GulpRequestStatus.CANCELED,
            GulpRequestStatus.FAILED,
            GulpRequestStatus.DONE,
        ]:
            self.time_finished = muty.time.now_msec()
            logger().debug("request is finished: %s" % (self))

        async with await session() as sess:
            # be sure to read the latest version from db
            sess.add(self)
            await sess.refresh(self)

            # add buffered values to the instance
            self.source_processed += source_processed
            self.source_failed += source_failed
            self.records_failed += records_failed
            self.records_skipped += records_skipped
            self.records_processed += records_processed
            self.records_ingested += records_ingested
            if error:
                if isinstance(error, Exception):
                    error = str(error)
                    if error not in self.errors:
                        self.errors.append(error)
                elif isinstance(error, str):
                    if error not in self.errors:
                        self.errors.append(error)
                elif isinstance(error, list[str]):
                    for e in error:
                        if e not in self.errors:
                            self.errors.append(e)

            # update the instance
            await super().update(
                self.to_dict(),
                ws_id=ws_id,
                req_id=self.id,
                throw_if_not_found=throw_if_not_found,
                **kwargs,
            )

        if ws_id:
            # TODO: update ws
            pass

        if status == GulpRequestStatus.CANCELED:
            logger().error("request canceled: %s" % (self))
            raise RequestCanceledError()
