import muty.log
import muty.time
from sqlalchemy import (
    BIGINT,
    JSON,
    Boolean,
    ForeignKey,
    Index,
    Integer,
    String,
    and_,
    delete,
    or_,
    select,
    update,
)
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio.engine import AsyncEngine
from sqlalchemy.orm import Mapped, mapped_column

import gulp.api.collab_api as collab_api
import gulp.api.rest.ws as ws_api
import gulp.config as config
from gulp.api.collab.base import (
    CollabBase,
    GulpCollabFilter,
    GulpCollabType,
    GulpRequestStatus,
)
from gulp.api.rest.ws import WsQueueDataType
from gulp.defs import ObjectNotFound


class TmpQueryStats:
    """
    global stats for all queries processed during a request
    """

    def __init__(self):
        self.queries_total = 0
        self.queries_processed = 0
        self.matches_total = 0

    def to_dict(self):
        return self.__dict__

    def __repr__(self) -> str:
        return f"TmpQueryStats(queries_total={self.queries_total}, queries_processed={self.queries_processed}, matches_total={self.matches_total})"


class TmpIngestStats:
    """
    Represents the statistics for a single file during ingestion.
    """

    def __init__(self, src_file: str):
        self.src_file: str = src_file
        # cannot process the event (event is not valid at all and cannot be accessed)
        self.ev_failed: int = 0
        # event skipped (e.g. due to duplicates)
        self.ev_skipped: int = 0
        self.ev_processed: int = 0
        self.tot_processed: int = 0
        self.tot_failed: int = 0
        self.tot_skipped: int = 0
        self.ingest_errors: list[str] = []
        # probably file format is wrong
        self.parser_failed: int = 0
        
    def to_dict(self):
        return self.__dict__

    def __repr__(self) -> str:
        return f"""TmpIngestStats(src_file={self.src_file}, ev_failed={self.ev_failed}, ev_skipped={self.ev_skipped}, ev_processed={self.ev_processed}
            tot_processed={self.tot_processed}, tot_failed={self.tot_failed}, tot_skipped={self.tot_skipped}, ingest_errors={self.ingest_errors}, parser_failed={self.parser_failed})
            """

    def update(
        self,
        processed: int = 0,
        failed: int = 0,
        skipped: int = 0,
        ingest_errors: list[str | Exception] = None,
        parser_failed: int=0,
        ex_full_traceback: bool = False,
    ) -> "TmpIngestStats":
        """
        Updates(=adds to) the statistics of the TmpIngestStats object.

        Args:
            processed (int): The number of processed and ingested items (may be less than actual processed records, due to skipped/errors).
            failed (int): The number of items that failed.
            skipped (int): The number of items skipped.
            ingest_errors (list[str | Exception], optional): A list of errors encountered during processing.
                Defaults to None.
            parser_failed (int): number of parser failures (=cannot parse the file/events at all)
            ex_full_traceback (bool, optional): Whether to include the full traceback in the error messages.
                Defaults to False.

        Returns:
            TmpIngestStats: The updated TmpIngestStats object.
        """
        if parser_failed:
            self.parser_failed += parser_failed
        if failed:
            self.ev_failed += failed
            self.tot_failed += failed
        if processed:
            self.tot_processed += processed
            self.ev_processed += processed
        if skipped:
            self.ev_skipped += skipped
            self.ev_processed -= skipped
            self.tot_skipped += skipped
            self.tot_processed -= skipped
        if ingest_errors is not None:
            for e in ingest_errors:
                if isinstance(e, Exception):
                    error = muty.log.exception_to_string(
                        e, with_full_traceback=ex_full_traceback
                    )
                else:
                    error = e
                if error not in self.ingest_errors:
                    self.ingest_errors.append(error)
        return self

    def reset(self) -> "TmpIngestStats":
        """
        Resets the statistics of the GulpStats object.
        """
        self.ev_failed = 0
        self.ev_skipped = 0
        self.ev_processed = 0
        self.parser_failed = 0
        self.ingest_errors = []
        return self


class GulpStats(CollabBase):
    """
    Represents the statistics for an ingestion or query operation

    Attributes:
        id (int): The unique identifier of the stats.
        type (GulpCollabType): The type of the stats.
        req_id (str): The request ID.
        time_created (int): The timestamp when the stats were created.
        operation_id (int): The ID of the operation associated with the stats.
        client_id (int): The ID of the client associated with the stats.
        context (str): The context of the stats.
        status (GulpRequestStatus): The status of the stats.
        time_expire (int): The timestamp when the stats will expire.
        time_update (int): The timestamp of the last update to the stats.
        time_end (int): The timestamp when the stats were completed.
        ev_failed (int): The number of events that could not be parsed at all in the request (ingestion only).
        ev_skipped (int): The number of events that were skipped in the request (ingestion only).
        ev_processed (int): The total number of events that were processed (=ingested, may be more than the actual records) in the request (ingestion only).
        parser_failed (int): The number of parser failures in the request (=the whole source failed, during plugin's parser initialization stage) (ingestion only).
        files_processed (int): The number of files that were processed (ingestion only).
        files_total (int): The total number of files to be processed (ingestion only).
        queries_total (int): Total number of queries requested (query only).
        queries_processed (int): Total number of queries processed so far (query only).
        matches_total(int): Total number of matches found (query only).
        ingest_errors (dict): The errors that occurred during ingestion (ingestion only).
        current_src_file (str): The current source file being processed (ingestion only).
    """

    __tablename__ = "stats"
    __table_args__ = (Index("idx_stats_operation_id", "operation_id"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, init=False)
    type: Mapped[GulpCollabType] = mapped_column(Integer)
    req_id: Mapped[str] = mapped_column(String(128), unique=True)
    status: Mapped[GulpRequestStatus] = mapped_column(
        Integer, default=GulpRequestStatus.ONGOING
    )
    operation_id: Mapped[int] = mapped_column(
        ForeignKey("operation.id", ondelete="CASCADE"), default=None, nullable=True
    )
    client_id: Mapped[int] = mapped_column(
        ForeignKey("clients.id", ondelete="CASCADE"), default=None, nullable=True
    )
    context: Mapped[str] = mapped_column(String, default=None, nullable=True)
    time_created: Mapped[int] = mapped_column(BIGINT, default=0)
    time_expire: Mapped[int] = mapped_column(BIGINT, default=0)
    time_update: Mapped[int] = mapped_column(BIGINT, default=0)
    time_end: Mapped[int] = mapped_column(BIGINT, default=0)
    ev_failed: Mapped[int] = mapped_column(Integer, default=0)
    ev_skipped: Mapped[int] = mapped_column(Integer, default=0)
    ev_processed: Mapped[int] = mapped_column(Integer, default=0)
    parser_failed: Mapped[int] = mapped_column(Integer, default=False)
    files_processed: Mapped[int] = mapped_column(Integer, default=0)
    files_total: Mapped[int] = mapped_column(Integer, default=0)
    queries_total: Mapped[int] = mapped_column(Integer, default=0)
    queries_processed: Mapped[int] = mapped_column(Integer, default=0)
    matches_total: Mapped[int] = mapped_column(Integer, default=0)
    ingest_errors: Mapped[dict] = mapped_column(JSON, default=None, nullable=True)
    current_src_file: Mapped[str] = mapped_column(String, default=None, nullable=True)

    def to_dict(self) -> dict:
        """
        Converts the GulpStats object to a dictionary.

        Returns:
            dict: A dictionary representation of the GulpStats object.
        """
        d = {
            "id": self.id,
            "type": self.type,
            "req_id": self.req_id,
            "operation_id": self.operation_id,
            "client_id": self.client_id,
            "context": self.context,
            "status": self.status,
            "time_created": self.time_created,
            "time_expire": self.time_expire,
            "time_update": self.time_update,
            "time_end": self.time_end,
            "ev_failed": self.ev_failed,
            "ev_skipped": self.ev_skipped,
            "ev_processed": self.ev_processed,
            "files_processed": self.files_processed,
            "files_total": self.files_total,
            "queries_total": self.queries_total,
            "queries_processed": self.queries_processed,
            "matches_total": self.matches_total,
            "ingest_errors": self.ingest_errors,
            "current_src_file": self.current_src_file,
            "parser_failed": self.parser_failed,
        }
        return d

    def __repr__(self) -> str:
        return f"GulpStats(id={self.id}, type={self.type}, req_id={self.req_id}, operation_id={self.operation_id}, \
            client_id={self.client_id}, context={self.context}, status={self.status}, time_created={self.time_created}, \
            time_expire={self.time_expire}, time_update={self.time_update}, time_end={self.time_end}, \
            ev_failed={self.ev_failed}, ev_skipped={self.ev_skipped}, ev_processed={self.ev_processed}, parser_failed={self.parser_failed}, \
            files_processed={self.files_processed}, files_total={self.files_total}, \
            queries_total={self.queries_total}, queries_processed={self.queries_processed}, matches_total={self.matches_total}, \
            ingest_errors={self.ingest_errors}, \
            current_src_file={self.current_src_file})"

    @staticmethod
    async def delete(engine: AsyncEngine, flt: GulpCollabFilter = None) -> int:
        """
        Delete collab stats based on the provided filter.

        Args:
            engine (AsyncEngine): The database engine.
            flt (GulpCollabFilter, optional): The filter to apply. Defaults to None. available filters: id, req_id, operation_id, client_id, type, context, status, time_created_start, time_created_end, limit, offset.

        Returns:
            int: The number of collab stats deleted.
        """
        async with AsyncSession(engine, expire_on_commit=False) as sess:
            q = select(GulpStats.id)
            if flt is not None:
                if flt.id is not None:
                    q = q.where(GulpStats.id.in_(flt.id))
                if flt.req_id is not None:
                    q = q.where(GulpStats.req_id.in_(flt.req_id))
                if flt.operation_id is not None:
                    q = q.where(GulpStats.operation_id.in_(flt.operation_id))
                if flt.client_id is not None:
                    q = q.where(GulpStats.client_id.in_(flt.client_id))
                if flt.type is not None:
                    q = q.where(GulpStats.type.in_(flt.type))
                if flt.context is not None:
                    qq = [GulpStats.context.ilike(x) for x in flt.context]
                    q = q.filter(or_(*qq))
                if flt.status is not None:
                    q = q.where(GulpStats.status.in_(flt.status))
                if flt.time_created_start is not None:
                    q = q.where(GulpStats.time_created >= flt.time_created_start)
                if flt.time_created_end is not None:
                    q = q.where(GulpStats.time_created <= flt.time_created_end)
                if flt.limit is not None:
                    q = q.limit(flt.limit)
                if flt.offset is not None:
                    q = q.offset(flt.offset)

            qq = delete(GulpStats).where(GulpStats.id.in_(q))
            res = await sess.execute(qq)
            await sess.commit()
            if res.rowcount == 0:
                raise ObjectNotFound("no stats found for the given filter: %s" % (flt))

            return res.rowcount

    @staticmethod
    async def delete_by_id(engine: AsyncEngine, stats_id: int) -> None:
        """
        Delete collab stats by ID

        Args:
            engine (AsyncEngine): The database engine.
            stats_id (int): The ID of the stats to delete.

        Raises:
            ObjectNotFound: If no stats are found

        """
        async with AsyncSession(engine, expire_on_commit=False) as sess:
            q = delete(GulpStats).where(GulpStats.id == stats_id)
            res = await sess.execute(q)
            await sess.commit()
            if res.rowcount == 0:
                raise ObjectNotFound("stats id=%d NOT found" % (stats_id))

    @staticmethod
    async def set_canceled(engine: AsyncEngine, req_id: str) -> "GulpStats":
        """
        Set the status of the stats with the specified request ID to canceled.

        Args:
            engine (AsyncEngine): The async database engine.
            req_id (str): The request ID.

        Returns:
            GulpStats: The updated GulpStats object.
        """
        async with AsyncSession(engine, expire_on_commit=False) as sess:
            q = select(GulpStats).where(GulpStats.req_id == req_id).with_for_update()
            res = await sess.execute(q)
            stats = res.scalar_one_or_none()
            if stats is None:
                raise ObjectNotFound("no stats found for req_id=%s" % (req_id))

            stats.status = GulpRequestStatus.CANCELED
            stats.time_end = muty.time.now_msec()
            sess.add(stats)
            await sess.commit()
            return stats

    @staticmethod
    async def delete_expired(engine: AsyncEngine) -> None:
        """
        Delete expired stats from the database.

        Args:
            engine (AsyncEngine): The database engine.

        Returns:
            None
        """
        collab_api.logger().debug("---> delete_expired")
        now = muty.time.now_msec()
        async with AsyncSession(
            engine,
        ) as sess:
            q = delete(GulpStats).where(
                and_(
                    GulpStats.time_expire > 0,
                    now > GulpStats.time_expire,
                )
            )
            res = await sess.execute(q)
            await sess.commit()
            if res.rowcount > 0:
                collab_api.logger().info(
                    "---> delete_expired: deleted %d expired stats" % (res.rowcount)
                )

    @staticmethod
    async def create(
        engine: AsyncEngine,
        t: GulpCollabType,
        req_id: str,
        ws_id: str,
        operation_id: int = None,
        client_id: int = None,
        context: str = None,
        total_files: int = 0,
        **kwargs,
    ) -> tuple[bool, "GulpStats"]:
        """
        Creates a new GulpStats record.

        Args:
            engine (AsyncEngine): The database engine.
            t (GulpCollabType): The type of the stats.
            req_id (str): The request ID.
            ws_id (str): The websocket ID.
            operation_id (int, optional): The operation ID.
            client_id (int, optional): The client ID.
            context (str, optional): The context.
            total_files (int, optional): The total number of files (ingestion stats only, ignored either). Defaults to 0.
            kwargs: Additional keyword arguments, may contain the following:
            - "stats_ttl" (int) for testing stats expiration (same as config "stats_ttl")
        Returns:
            tuple[bool, 'GulpStats']: A tuple containing a boolean indicating if the stats were created successfully (false if it already exists) and the created/existing GulpStats object.
        """
        collab_api.logger().debug(
            "---> create: t=%s, req_id=%s, operation_id=%s, client_id=%s, context=%s, total_files=%s"
            % (t, req_id, operation_id, client_id, context, total_files)
        )

        # take a chance to cleanup expired stats
        await GulpStats.delete_expired(engine)

        # create stats record
        async with AsyncSession(engine, expire_on_commit=False) as sess:
            # check exists
            q = select(GulpStats).where(GulpStats.req_id == req_id).with_for_update()
            res = await sess.execute(q)
            stats = res.scalar_one_or_none()
            if stats is not None:
                collab_api.logger().warning("---> STATS ALREADY EXISTS: %s" % (stats))
                return False, stats

            # create stats record
            now = muty.time.now_msec()
            ttl = config.stats_ttl()

            test_ttl = kwargs.get("stats_ttl", None)
            time_expire = None
            if test_ttl is not None:
                ttl = test_ttl
            if ttl > 0:
                time_expire = ttl * 1000 + now
                collab_api.logger().warning(
                    "this stat will expire at %d" % (time_expire)
                )

            stats = GulpStats(
                type=t,
                req_id=req_id,
                operation_id=operation_id,
                client_id=client_id,
                context=context,
                files_total=total_files,
                time_expire=time_expire,
                time_created=now,
            )

            stats.ingest_errors = {}
            stats.status = GulpRequestStatus.ONGOING
            sess.add(stats)
            await sess.commit()
            collab_api.logger().info("---> create: created stats=%s" % (stats))

            # add to ws queue
            ws_api.shared_queue_add_data(
                (
                    WsQueueDataType.INGESTION_STATS_CREATE
                    if t == GulpCollabType.STATS_INGESTION
                    else WsQueueDataType.QUERY_STATS_CREATE
                ),
                req_id,
                stats.to_dict(),
                ws_id=ws_id,
            )

            return True, stats

    def update_internal(
        self,
        fs: TmpIngestStats = None,
        qs: TmpQueryStats = None,
        file_done: bool = False,
        status: GulpRequestStatus = None,
    ) -> bool:
        """
        updates the ingest stats with new file stats and status.

        Args:
            fs (TmpIngestStats, optional): The file stats to add
            qs (TmpQueryStats, optional): The query stats to add
            file_done (bool, optional): Indicates whether the (current) file processing is done. Defaults to False.
            status (GulpRequestStatus, optional): The status of the stats. Defaults to None.
        Returns:
            bool: True if a database update is needed
        """
        # collab_api.logger().debug("file_done=%d, status=%s" % (file_done, status))
        now = muty.time.now_msec()
        finished: bool = False

        if self.status != GulpRequestStatus.ONGOING:
            # only update stats when ongoing
            return False

        self.time_update = now
        if status is not None:
            # forced status
            self.status = status

        # check for canceled or failed (or file done, for ingestion stats)
        if file_done or status in [
            GulpRequestStatus.CANCELED,
            GulpRequestStatus.FAILED,
        ]:
            if self.type == GulpCollabType.STATS_INGESTION:
                # this file is done
                self.files_processed += 1

        # update totals
        if self.type == GulpCollabType.STATS_INGESTION and fs is not None:
            # ingestion stats
            self.current_src_file = fs.src_file
            self.ev_processed += fs.ev_processed
            self.parser_failed += fs.parser_failed
            self.ev_skipped += fs.ev_skipped
            self.ev_failed += fs.ev_failed

            # update errors
            if len(fs.ingest_errors) > 0:
                if self.ingest_errors.get(fs.src_file, None) is None:
                    # create key
                    self.ingest_errors[fs.src_file] = []
                for e in fs.ingest_errors:
                    if e not in self.ingest_errors[fs.src_file]:
                        self.ingest_errors[fs.src_file].append(e)

            if self.files_processed >= self.files_total:
                # all files are processed
                self.files_processed = self.files_total
                finished = True
        else:
            # query stats
            self.queries_total = qs.queries_total
            self.queries_processed = qs.queries_processed
            self.matches_total = qs.matches_total

            if self.queries_processed >= self.queries_total:
                # all queries are processed
                # self.queries_processed = self.queries_total
                finished = True

        if finished:
            # request finished, set final status
            collab_api.logger().debug("request finished, setting final status")
            self.time_end = now
            self.status = GulpRequestStatus.DONE

            """
            # NOTE: we prefer not to do this ... a request is done and MAY have errors, but it is not failed.
            if self.ev_failed > 0 or (len(self.ingest_errors) > 0 or (f is not None and len(f.errors) > 0)):
                collab_api.logger().debug('finished, setting status to FAILED')
                self.status = GulpRequestStatus.FAILED
            """

        # collab_api.logger().error(self.ingest_errors)
        return True

    @staticmethod
    async def update(
        engine: AsyncEngine,
        req_id: str,
        ws_id: str,
        fs: TmpIngestStats = None,
        qs: TmpQueryStats = None,
        file_done: bool = False,
        force: bool = False,
        new_status: GulpRequestStatus = None,
    ) -> tuple[GulpRequestStatus, "GulpStats"]:
        """
        Update the GulpStats object identified by "req_id" with new file and/or query stats

        Args:
            engine (AsyncEngine): The async database engine.
            req_id (str): The request ID the stat to update refer to.
            ws_id (str): The websocket ID
            fs (FileStats, optional): The file stats to be added on the current stats, on db.
            qs (TmpQueryStats, optional): The query stats to be added on the current stats, on db. Defaults to None.
            file_done (bool, optional): Indicates if the file processing is done. Defaults to False.
            force (bool, optional): Indicates if the update should be forced (ignore threshold check). Defaults to False.
            new_status (GulpRequestStatus, optional): The new status to set. Defaults to None.
        Returns:
            tuple[GulpRequestStatus, GulpStats]: A tuple containing the updated status and the updated GulpStats object (or None if it is not yet time to update/force not set).
        """
        if new_status is not None:
            status = new_status
        else:
            status = GulpRequestStatus.ONGOING

        if qs is None and fs is None:
            # nothing to update
            return status, None

        if fs is not None:
            # check threshold (we update stats every N events processed, or forced)
            stats_update_threshold = config.stats_update_threshold()
            if (fs.tot_processed % stats_update_threshold == 0) or force:
                # check failure threshold (too many failures=abort)
                ingestion_evt_failure_threshold = (
                    config.ingestion_evt_failure_threshold()
                )
                if (
                    ingestion_evt_failure_threshold > 0
                    and (fs.tot_failed + fs.tot_skipped)
                    >= ingestion_evt_failure_threshold
                ):
                    # too many failures, abort
                    collab_api.logger().error(
                        "TOO MANY FAILURES REQ_ID=%s (failed+skipped=%d, threshold=%d), aborting current file ingestion: %s!"
                        % (
                            req_id,
                            fs.tot_failed + fs.tot_skipped,
                            ingestion_evt_failure_threshold,
                            fs.src_file if fs is not None else None,
                        )
                    )
                    status = GulpRequestStatus.FAILED
                    file_done = True
            else:
                # not yet time to update db
                return status, None
        else:
            # query stats
            stats_update_threshold = 100
            if qs.queries_processed % stats_update_threshold != 0 and not force:
                # not yet time to update db
                return status, None

        # update stats record
        # collab_api.logger().debug('---> update, req_id=%s, fs=%s, qr=%s, file_done=%s force=%s' % (
        #    req_id, fs, qr, file_done, force))
        async with AsyncSession(engine, expire_on_commit=False) as sess:
            # get fresh stats from db
            q = select(GulpStats).where(GulpStats.req_id == req_id).with_for_update()
            res = await sess.execute(q)
            the_stats: GulpStats = res.scalar_one_or_none()
            collab_api.logger().debug(
                "---> update: engine=%s, req_id=%s, got stats=%s"
                % (engine, req_id, the_stats)
            )

            # update stats totals
            do_update = the_stats.update_internal(
                fs=fs, qs=qs, file_done=file_done, status=status
            )
            if not do_update:
                # no update needed
                return the_stats.status, None

            now = muty.time.now_msec()
            the_stats.time_update = now
            if the_stats.status == GulpRequestStatus.DONE:
                the_stats.time_end = now

            if fs is not None:
                # reset fs for the next call
                fs = fs.reset()

            # rewrite on db
            ss = the_stats.to_dict()
            q = update(GulpStats).where(GulpStats.req_id == req_id).values(ss)
            await sess.execute(q)

            sess.add(the_stats)
            await sess.commit()

            collab_api.logger().debug("---> update: updated stats=%s" % (the_stats))

            # add to ws queue
            ws_api.shared_queue_add_data(
                (
                    WsQueueDataType.INGESTION_STATS_UPDATE
                    if the_stats.type == GulpCollabType.STATS_INGESTION
                    else WsQueueDataType.QUERY_STATS_UPDATE
                ),
                req_id,
                ss,
                ws_id=ws_id,
            )
            if file_done:
                # signal file done
                ws_api.shared_queue_add_data(
                    WsQueueDataType.INGESTION_DONE,
                    req_id,
                    {
                        "src_file": the_stats.current_src_file,
                        "context": the_stats.context,
                    },
                    ws_id=ws_id,
                )

            return status, the_stats

    @staticmethod
    async def get(engine: AsyncEngine, flt: GulpCollabFilter) -> list["GulpStats"]:
        """
        Retrieves collab stats based on the provided filter (id, req_id, operation, client, type, context, status, start time, end time, limit, offset).

        Args:
            engine (AsyncEngine): The async engine used for database operations.
            flt (GulpCollabFilter): The filter to apply to the query.

        Returns:
            list['GulpStats']: A list of collab stats matching the filter.

        Raises:
            ObjectNotFound: If no stats are found based on the filter.
        """
        collab_api.logger().debug("---> get: flt=%s" % (flt))

        # make a select() query depending on the filter
        q = select(GulpStats)
        if flt is not None:
            # check each part of flt and build the query
            if flt.id is not None:
                q = q.where(GulpStats.id.in_(flt.id))
            if flt.req_id is not None:
                q = q.where(GulpStats.req_id.in_(flt.req_id))
            if flt.operation_id is not None:
                q = q.where(GulpStats.operation_id.in_(flt.operation_id))
            if flt.client_id is not None:
                q = q.where(GulpStats.client_id.in_(flt.client_id))
            if flt.type is not None:
                q = q.where(GulpStats.type.in_(flt.type))
            if flt.context is not None:
                qq = [GulpStats.context.ilike(x) for x in flt.context]
                q = q.filter(or_(*qq))
            if flt.status is not None:
                q = q.where(GulpStats.status.in_(flt.status))
            if flt.time_created_start is not None:
                q = q.where(GulpStats.time_created >= flt.time_created_start)
            if flt.time_created_end is not None:
                q = q.where(GulpStats.time_created <= flt.time_created_end)
            if flt.limit is not None:
                q = q.limit(flt.limit)
            if flt.offset is not None:
                q = q.offset(flt.offset)

        async with AsyncSession(engine) as sess:
            res = await sess.execute(q)
            stats = res.scalars().all()
            if len(stats) == 0:
                raise ObjectNotFound("no stats found for the given filter: %s" % (flt))
            collab_api.logger().info(
                "---> get: found %d stats: %s" % (len(stats), stats)
            )
            return stats
