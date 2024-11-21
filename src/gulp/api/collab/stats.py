import inspect
from typing import Optional, Tuple, override

import muty.crypto
import muty.log
import muty.time
import xxhash
from muty.log import MutyLogger
from sqlalchemy import ARRAY, BIGINT, ForeignKey, Index, Integer, String, select, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import Enum as SQLEnum

from gulp.api.collab.structs import (
    GulpCollabBase,
    GulpCollabType,
    GulpRequestStatus,
    GulpUserPermission,
    T,
)
from gulp.api.collab_api import GulpCollab
from gulp.api.ws_api import WsQueueDataType
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
    def __init__(self, *args, **kwargs):
        # initializes the base class
        super().__init__(*args, type=GulpCollabType.INGESTION_STATS, **kwargs)

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
        Asynchronously creates a new GulpIngestionStats subclass instance

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
        # configure expiration
        time_expire = GulpConfig.get_instance().stats_ttl() * 1000
        if time_expire > 0:
            now = muty.time.now_msec()
            time_expire = muty.time.now_msec() + time_expire
            MutyLogger.get_instance().debug(
                'now=%s, setting stats "%s".time_expire to %s', now, id, time_expire
            )

        args = {
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
    async def create_or_get(
        cls,
        id: str,
        operation_id: str,
        context_id: str,
        sess: AsyncSession = None,
        ensure_eager_load: bool = True,
        eager_load_depth: int = 3,
        source_total: int = 1,
        **kwargs,
    ) -> Tuple[T, bool]:
        """
        Create new or get an existing GulpIngestionStats object on the collab database.

        Args:
            id (str): The unique identifier of the stats (= "req_id" of the request)
            operation_id (str): The operation associated with the stats
            context_id (str): The context associated with the stats
            source_total (int, optional): The total number of sources to be processed by the request to which this stats belong. Defaults to 1.
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

        async with GulpCollab.get_instance().session() as sess:
            # acquire an advisory lock
            lock_id = muty.crypto.hash_xxh64_int(id)
            await sess.execute(
                text("SELECT pg_advisory_xact_lock(:lock_id)"), {"lock_id": lock_id}
            )

            # check if the stats already exist
            s: GulpIngestionStats
            s = await cls.get_one_by_id(
                id, sess=sess, throw_if_not_found=False
            )
            if s:
                return s, False

            try:
                # not exist, create
                s: GulpIngestionStats
                s = await cls._create(
                    id=id,
                    sess=sess,
                    operation_id=operation_id,
                    context_id=context_id,
                    ensure_eager_load=ensure_eager_load,
                    eager_load_depth=eager_load_depth,
                    source_total=source_total,
                    **kwargs,
                )
            except Exception as ex:
                MutyLogger.get_instance().error(
                    "create_or_get: error creating stats: %s" % ex
                )
                raise ex

            return s, True

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
            # get stats from db first
            stats = await sess.get(GulpIngestionStats, self.id, with_for_update=True)

            # update
            stats.source_processed += source_processed
            stats.source_failed += source_failed
            stats.records_failed += records_failed
            stats.records_skipped += records_skipped
            stats.records_processed += records_processed
            stats.records_ingested += records_ingested
            if error:
                if not stats.errors:
                    stats.errors = []
                if isinstance(error, Exception):
                    # MutyLogger.get_instance().error(f"PRE-COMMIT: ex error={error}")
                    error = str(error)
                    if error not in stats.errors:
                        stats.errors.append(error)
                elif isinstance(error, str):
                    # MutyLogger.get_instance().error(f"PRE-COMMIT: str error={error}")
                    if error not in stats.errors:
                        stats.errors.append(error)
                elif isinstance(error, list[str]):
                    # MutyLogger.get_instance().error(f"PRE-COMMIT: list error={error}")
                    for e in error:
                        if e not in stats.errors:
                            stats.errors.append(e)

            # MutyLogger.get_instance().debug(f"PRE-COMMIT: source_processed={self.source_processed}, source_failed={self.source_failed}, records_failed={self.records_failed}, records_skipped={self.records_skipped}, records_processed={self.records_processed}, records_ingested={self.records_ingested}, errors={self.errors}")
            if status:
                stats.status = status

            if stats.source_processed == stats.source_total:
                MutyLogger.get_instance().debug(
                    'source_processed == source_total, setting request "%s" to DONE'
                    % (stats.id)
                )
                stats.status = GulpRequestStatus.DONE

            if stats.source_failed == stats.source_total:
                MutyLogger.get_instance().error(
                    'source_failed == source_total, setting request "%s" to FAILED'
                    % (stats.id)
                )
                stats.status = GulpRequestStatus.FAILED

            # check threshold
            failure_threshold = (
                GulpConfig.get_instance().ingestion_evt_failure_threshold()
            )
            if (
                failure_threshold > 0
                and stats.type == GulpCollabType.INGESTION_STATS
                and stats.records_failed >= failure_threshold
                or stats.records_skipped >= failure_threshold
            ):
                # too many failures, abort
                MutyLogger.get_instance().error(
                    "TOO MANY FAILURES req_id=%s (failed=%d, threshold=%d), aborting ingestion!"
                    % (stats.id, stats.source_failed, failure_threshold)
                )
                stats.status = GulpRequestStatus.CANCELED

            if stats.status in [
                GulpRequestStatus.CANCELED,
                GulpRequestStatus.FAILED,
                GulpRequestStatus.DONE,
            ]:
                stats.time_finished = muty.time.now_msec()
                MutyLogger.get_instance().debug(
                    'request "%s" COMPLETED with status=%s' % (self.id, self.status)
                )
                # print the time it took to complete the request, in seconds
                MutyLogger.get_instance().debug(
                    "*** TOTAL TIME TOOK by request %s: %s seconds ***"
                    % (self.id, (stats.time_finished - stats.time_created) / 1000)
                )

            # update the instance (will update websocket too)
            await super().update(
                token=None,  # no token needed
                d=None,  # we pass the already updated stats in updated_instance
                ws_id=ws_id,
                req_id=self.id,
                sess=sess,
                throw_if_not_found=throw_if_not_found,
                ws_queue_datatype=WsQueueDataType.STATS_UPDATE,
                updated_instance=stats,
                **kwargs,
            )
            await sess.commit()

            if stats.status == GulpRequestStatus.CANCELED:
                MutyLogger.get_instance().error(
                    'request "%s" set to CANCELED' % (self.id)
                )
                raise RequestCanceledError()
        return stats
