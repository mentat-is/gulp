import inspect
from typing import Optional, Tuple, override

import muty.log
import muty.time
from muty.log import MutyLogger
from sqlalchemy import ARRAY, BIGINT, ForeignKey, Index, Integer, String
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import Enum as SQLEnum
from sqlalchemy_dlock.asyncio import create_async_sadlock

from gulp.api.collab.structs import (
    GulpCollabBase,
    GulpCollabType,
    GulpRequestStatus,
    GulpUserPermission,
    T,
)
from gulp.api.collab_api import GulpCollab
from gulp.api.ws_api import GulpSharedWsQueue, WsQueueDataType
from gulp.config import GulpConfig


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

    operation_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("operation.id", ondelete="CASCADE"),
        nullable=True,
        default=None,
        doc="The operation associated with the stats.",
    )
    context_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("context.id", ondelete="CASCADE"),
        nullable=True,
        default=None,
        doc="The context associated with the stats.",
    )
    source_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("source.id", ondelete="CASCADE"),
        nullable=True,
        default=None,
        doc="The source associated with the stats.",
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
        # initializes the base class
        if type(self) is GulpStatsBase:
            raise TypeError("GulpStatsBase cannot be instantiated directly")
        super().__init__(*args, **kwargs)

    @override
    @classmethod
    async def update_by_id(
        cls,
        token: str,
        id: str,
        d: dict,
        permission: list[GulpUserPermission] = [GulpUserPermission.EDIT],
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
        throw_if_not_found: bool = True,
        **kwargs,
    ) -> T:

        # Check if the caller is 'cancel_by_id'
        stack = inspect.stack()
        callers = [frame.function for frame in stack]
        if "cancel_by_id" not in callers:
            raise NotImplementedError(
                "update_by_id @classmethod can only be called from 'cancel_by_id'"
            )

        return await super().update_by_id(
            token=token,
            id=id,
            d=d,
            permission=permission,
            ws_id=ws_id,
            req_id=req_id,
            sess=sess,
            throw_if_not_found=throw_if_not_found,
            ws_queue_datatype=WsQueueDataType.STATS_UPDATE,
            **kwargs,
        )

    @override
    @classmethod
    async def delete_by_id(
        cls,
        token: str,
        id: str,
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
        token: str = None,
        id: str = None,
        required_permission: list[GulpUserPermission] = [GulpUserPermission.READ],
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
        ensure_eager_load: bool = False,
        eager_load_depth: int = 3,
        **kwargs,
    ) -> T:
        """
        Asynchronously creates a new GulpStats subclass instance
        Args:
            token (str, optional): The authentication token. Defaults to None (unused)
            id (str): The unique identifier for the stats (req_id)
            required_permission (list[GulpUserPermission], optional): The required permission. Defaults to [GulpUserPermission.READ] (unused)
            ws_id (str, optional): The websocket ID. Defaults to None.
            req_id (str, optional): The request ID. Defaults to None (unused)
            sess (AsyncSession, optional): The asynchronous session. Defaults to None.
            ensure_eager_load (bool, optional): Whether to ensure eager loading of the instance. Defaults to True.
            eager_load_depth (int, optional): The depth of eager loading. Defaults to 3.
            **kwargs: Additional keyword arguments.
        Keyword Args:
            operation (str, optional): The operation. Defaults to None.
            context (str, optional): The context of the operation. Defaults to None.
        Returns:
            T: The created instance.
        """
        MutyLogger.get_instance().debug(
            "--->_create: id=%s, ws_id=%s, ensure_eager_load=%s, kwargs=%s",
            id,
            ws_id,
            ensure_eager_load,
            kwargs,
        )
        operation_id: str = kwargs.get("operation_id", None)
        context_id: str = kwargs.get("context_id", None)
        source_id: str = kwargs.get("source_id", None)

        # configure expiration
        time_expire = GulpConfig.get_instance().stats_ttl() * 1000
        if time_expire > 0:
            now = muty.time.now_msec()
            time_expire = muty.time.now_msec() + time_expire
            MutyLogger.get_instance().debug(
                'now=%s, setting stats "%s".time_expire to %s', now, id, time_expire
            )

        args = {
            "operation_id": operation_id,
            "context_id": context_id,
            "source_id": source_id,
            "time_expire": time_expire,
            **kwargs,
        }
        return await super()._create(
            id=id,
            ws_id=ws_id,
            req_id=id,
            sess=sess,
            ensure_eager_load=ensure_eager_load,
            eager_load_depth=eager_load_depth,
            ws_queue_datatype=WsQueueDataType.STATS_UPDATE,
            **args,
        )

    @classmethod
    async def _create_or_get(
        cls,
        id: str,
        operation_id: str = None,
        context_id: str = None,
        source_id: str = None,
        sess: AsyncSession = None,
        ensure_eager_load: bool = True,
        eager_load_depth: int = 3,
        **kwargs,
    ) -> Tuple[T, bool]:
        MutyLogger.get_instance().debug(
            f"--->_create_or_get: id={id}, operation_id={operation_id}, context_id={context_id},source_id={source_id}, sess={sess}, ensure_eager_load={ensure_eager_load},kwargs={kwargs}"
        )
        existing = await cls.get_one_by_id(id, sess=sess, throw_if_not_found=False)
        if existing:
            return existing, False

        # create new
        stats = await cls._create(
            id=id,
            operation_id=operation_id,
            context_id=context_id,
            source_id=source_id,
            ensure_eager_load=ensure_eager_load,
            eager_load_depth=eager_load_depth,
            **kwargs,
        )
        return stats, True


class GulpIngestionStats(GulpStatsBase, type=GulpCollabType.INGESTION_STATS):
    """
    Represents the statistics for an ingestion operation.
    """

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
    def __init__(self, *args, **kwargs):
        # initializes the base class
        super().__init__(*args, type=GulpCollabType.INGESTION_STATS, **kwargs)

    @classmethod
    async def create_or_get(
        cls,
        id: str,
        operation_id: str,
        context_id: str,
        source_id: str,
        **kwargs,
    ) -> Tuple[T, bool]:
        """
        Create new or get an existing GulpIngestionStats object on the collab database.

        if the stats already exists (id matches) but source_id is different, stats refers to a multi-source
        ingestion and source_total (and other relevant fields) are updated.

        Args:
            id (str): The unique identifier of the stats (= "req_id" of the request)
            operation_id (str): The operation associated with the stats
            context_id (str): The context associated with the stats
            source_id (str): The source associated with the stats
            kwargs: Additional keyword arguments.

        Returns:
            a tuple (GulpIngestionStats, bool): The created or retrieved instance and a boolean indicating if the instance was created.
        """
        MutyLogger.get_instance().debug(
            "---> create_or_get: id=%s, operation_id=%s, context_id=%s, kwargs=%s",
            id,
            operation_id,
            context_id,
            kwargs,
        )

        lock_key=id
        async with GulpCollab.get_instance().session() as sess:
            async with create_async_sadlock(sess, lock_key) as lock:
                assert lock.locked
                s: GulpIngestionStats
                s, created = await cls._create_or_get(
                    id=id,
                    operation_id=operation_id,
                    context_id=context_id,
                    source_id=source_id,
                    sess=sess,
                    source_total=1,
                    **kwargs,
                )
            assert not lock.locked
            
            if not created and s.source_id != source_id:
                # already exists but source is different: source total must be updated
                sess.add(s)
                await sess.refresh(s)
                s.source_total += 1
                s.status = GulpRequestStatus.PENDING
                s.time_finished = 0
                await sess.commit()

            return s, created

    @classmethod
    async def cancel_by_id(cls, token: str, req_id: str, ws_id: str = None) -> None:
        """
        Cancels a running request.

        Args:
            token(str): The authentication token.
            req_id (str): The request ID.
            ws_id (str, optional): The websocket ID. Defaults to None.
        """
        MutyLogger.get_instance().debug(
            "---> cancel_by_id: id=%s, ws_id=%s", req_id, ws_id
        )
        await cls.update_by_id(
            token=token,
            id=req_id,
            d={"status": GulpRequestStatus.CANCELED},
            permission=[GulpUserPermission.READ],
            ws_id=ws_id,
            req_id=req_id,
            throw_if_not_found=False,
        )

    @override
    async def update(
        self,
        token: str = None,
        d: dict = None,
        permission: list[GulpUserPermission] = None,
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
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
            status (GulpRequestStatus, optional): The status to set (either, it )
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
        if self.status in [
            GulpRequestStatus.CANCELED,
            GulpRequestStatus.FAILED,
        ]:
            # already processed, nothing to do
            MutyLogger.get_instance().warning(
                "request already set to %s, nothing to do" % (self.status)
            )
            return self

        msg = f"---> update: ws_id={ws_id}, throw_if_not_found={throw_if_not_found}, error={error}, status={status}, source_processed={source_processed}, source_failed={source_failed}, records_failed={records_failed}, records_skipped={records_skipped}, records_processed={records_processed}, records_ingested={records_ingested}, kwargs={kwargs}"
        if error:
            MutyLogger.get_instance().error(msg)
        else:
            MutyLogger.get_instance().debug(msg)

        sess = GulpCollab.get_instance().session()
        async with sess:
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

            # MutyLogger.get_instance().debug(f"PRE-COMMIT: source_processed={self.source_processed}, source_failed={self.source_failed}, records_failed={self.records_failed}, records_skipped={self.records_skipped}, records_processed={self.records_processed}, records_ingested={self.records_ingested}, errors={self.errors}")
            if status:
                self.status = status

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
            failure_threshold = (
                GulpConfig.get_instance().ingestion_evt_failure_threshold()
            )
            if (
                failure_threshold > 0
                and self.type == GulpCollabType.INGESTION_STATS
                and self.records_failed >= failure_threshold
                or self.records_skipped >= failure_threshold
            ):
                # too many failures, abort
                MutyLogger.get_instance().error(
                    "TOO MANY FAILURES req_id=%s (failed=%d, threshold=%d), aborting ingestion!"
                    % (self.id, self.source_failed, failure_threshold)
                )
                self.status = GulpRequestStatus.CANCELED

            if self.status in [
                GulpRequestStatus.CANCELED,
                GulpRequestStatus.FAILED,
                GulpRequestStatus.DONE,
            ]:
                self.time_finished = muty.time.now_msec()
                MutyLogger.get_instance().debug(
                    'request "%s" COMPLETED with status=%s' % (self.id, self.status)
                )

            # update the instance (will update websocket too)
            await super().update(
                token=None,  # no token needed
                d=self.to_dict(),
                ws_id=ws_id,
                req_id=self.id,
                sess=sess,
                throw_if_not_found=throw_if_not_found,
                ws_queue_datatype=WsQueueDataType.STATS_UPDATE,
                **kwargs,
            )
            await sess.commit()

            if self.status == GulpRequestStatus.CANCELED:
                MutyLogger.get_instance().error(
                    'request "%s" set to CANCELED' % (self.id)
                )
                raise RequestCanceledError()
        return self
