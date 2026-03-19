"""
Prometheus metrics integration for gULP.

Provides a GulpMetrics singleton that owns all custom metric objects,
a setup function to attach the instrumentator to FastAPI, and a
background gauge collection loop for the main process.
"""

import asyncio
import os
import shutil
import tempfile
import time

from muty.log import MutyLogger

from gulp.config import GulpConfig

# ── Ensure PROMETHEUS_MULTIPROC_DIR is set before importing prometheus_client ──
# Workers write counters (OpenSearch ingestion, Redis publish, etc.) so
# multiprocess mode is required for correct metric aggregation at /metrics.
_prom_dir = os.environ.get("PROMETHEUS_MULTIPROC_DIR")
if _prom_dir:
    os.makedirs(_prom_dir, exist_ok=True)
else:
    _prom_dir = tempfile.mkdtemp(prefix="gulp_prometheus_")
    MutyLogger.get_instance(name="gulp").debug(
        "created Prometheus multiprocess temp dir: %s", _prom_dir
    )
    os.environ["PROMETHEUS_MULTIPROC_DIR"] = _prom_dir

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Info,
    generate_latest,
    multiprocess,
)


class GulpMetrics:
    """Singleton holding all Prometheus metric objects for gULP."""

    _instance: "GulpMetrics" = None
    # directory where prometheus_client writes per-process metric files;
    # retained so cleanup_prometheus() can remove it on shutdown.
    _multiproc_dir: str = _prom_dir

    @classmethod
    def cleanup_multiproc_dir(cls) -> None:
        """Delete the multiprocess metrics dir if it was auto-created by us."""
        if cls._multiproc_dir and os.path.isdir(cls._multiproc_dir):
            MutyLogger.get_instance().debug(
                "cleaning up Prometheus multiprocess temp dir: %s", cls._multiproc_dir
            )
            if cls._multiproc_dir.startswith(tempfile.gettempdir()):
                shutil.rmtree(cls._multiproc_dir, ignore_errors=True)
            cls._multiproc_dir = None

    # ── Redis pub/sub ──
    redis_publish_total = Counter(
        "gulp_redis_publish_total",
        "Total messages published to Redis pub/sub",
    )
    redis_publish_bytes_total = Counter(
        "gulp_redis_publish_bytes_total",
        "Total bytes published to Redis pub/sub",
    )
    redis_chunked_publish_total = Counter(
        "gulp_redis_chunked_publish_total",
        "Messages that required chunking before publish",
    )
    redis_subscriber_errors_total = Counter(
        "gulp_redis_subscriber_errors_total",
        "Errors in the Redis subscriber loop",
    )
    redis_pubsub_reconnect_total = Counter(
        "gulp_redis_pubsub_reconnect_total",
        "Number of Redis pub/sub reconnection attempts",
    )
    redis_targeted_publish_total = Counter(
        "gulp_redis_targeted_publish_total",
        "Messages published to per-server Redis pub/sub channels",
    )

    # ── Multi-node routing counters ──
    ws_remote_forward_total = Counter(
        "gulp_ws_remote_forward_total",
        "Messages forwarded to a websocket owned by a different server",
    )
    ws_broadcast_dedup_dropped_total = Counter(
        "gulp_ws_broadcast_dedup_dropped_total",
        "Broadcast messages dropped due to cross-node deduplication",
    )
    ws_payload_pointer_resolve_total = Counter(
        "gulp_ws_payload_pointer_resolve_total",
        "Chunked payload pointer resolve outcomes",
        ["outcome"],
    )

    # ── Redis gauges (updated by collection loop) ──
    redis_token_bucket_tokens = Gauge(
        "gulp_redis_token_bucket_tokens",
        "Current available tokens in publish backpressure bucket",
        multiprocess_mode="livesum",
    )
    redis_stream_depth = Gauge(
        "gulp_redis_stream_depth",
        "Number of messages in a Redis stream",
        ["stream"],
        multiprocess_mode="livesum",
    )
    redis_stream_pending = Gauge(
        "gulp_redis_stream_pending",
        "Number of pending (unacknowledged) messages in a Redis stream consumer group",
        ["stream"],
        multiprocess_mode="livesum",
    )
    redis_cluster_live_servers = Gauge(
        "gulp_redis_cluster_live_servers",
        "Number of currently alive gULP server instances (heartbeat keys present)",
        multiprocess_mode="livesum",
    )
    redis_pubsub_channel_subscribers = Gauge(
        "gulp_redis_pubsub_channel_subscribers",
        "Number of Redis pub/sub subscribers per channel",
        ["channel"],
        multiprocess_mode="livesum",
    )

    # ── WebSocket ──
    ws_connected_sockets = Gauge(
        "gulp_ws_connected_sockets",
        "Number of currently connected websockets",
        multiprocess_mode="livesum",
    )
    ws_queue_utilization = Gauge(
        "gulp_ws_queue_utilization",
        "Aggregate websocket queue utilization (0.0-1.0)",
        multiprocess_mode="livesum",
    )

    # ── OpenSearch ──
    opensearch_bulk_docs_total = Counter(
        "gulp_opensearch_bulk_docs_total",
        "Total documents successfully bulk-ingested into OpenSearch",
    )
    opensearch_bulk_errors_total = Counter(
        "gulp_opensearch_bulk_errors_total",
        "Total documents that failed bulk ingestion into OpenSearch",
    )
    opensearch_bulk_skipped_total = Counter(
        "gulp_opensearch_bulk_skipped_total",
        "Total documents skipped (duplicates) during bulk ingestion",
    )

    # ── PostgreSQL connection pool ──
    postgres_pool_size = Gauge(
        "gulp_postgres_pool_size",
        "PostgreSQL connection pool configured size",
        multiprocess_mode="livesum",
    )
    postgres_pool_checked_out = Gauge(
        "gulp_postgres_pool_checked_out",
        "PostgreSQL connections currently checked out (in use)",
        multiprocess_mode="livesum",
    )
    postgres_pool_idle = Gauge(
        "gulp_postgres_pool_idle",
        "PostgreSQL connections currently idle in the pool",
        multiprocess_mode="livesum",
    )
    postgres_pool_overflow = Gauge(
        "gulp_postgres_pool_overflow",
        "PostgreSQL connections currently open above pool_size (overflow)",
        multiprocess_mode="livesum",
    )

    # ── Workers ──
    worker_process_count = Gauge(
        "gulp_worker_process_count",
        "Number of active worker processes",
        multiprocess_mode="livesum",
    )
    # These gauges are written only by the main process collection loop,
    # so livesum is correct.  The explicit `worker_pid` label already
    # distinguishes individual workers; no internal pid label is needed.
    worker_memory_rss_bytes = Gauge(
        "gulp_worker_memory_rss_bytes",
        "Resident set size (RSS) in bytes of a worker process",
        ["worker_pid"],
        multiprocess_mode="livesum",
    )
    worker_cpu_percent = Gauge(
        "gulp_worker_cpu_percent",
        "CPU utilization percentage of a worker process",
        ["worker_pid"],
        multiprocess_mode="livesum",
    )
    worker_tasks_spawned_total = Counter(
        "gulp_worker_tasks_spawned_total",
        "Total worker tasks dispatched to the process pool",
    )
    main_process_memory_rss_bytes = Gauge(
        "gulp_main_process_memory_rss_bytes",
        "Resident set size (RSS) in bytes of the main server process",
        multiprocess_mode="livesum",
    )
    main_process_cpu_percent = Gauge(
        "gulp_main_process_cpu_percent",
        "CPU utilization percentage of the main server process",
        multiprocess_mode="livesum",
    )
    main_process_open_fds = Gauge(
        "gulp_main_process_open_fds",
        "Number of open file descriptors in the main server process",
        multiprocess_mode="livesum",
    )

    # ── System-wide resource gauges ──
    system_cpu_percent = Gauge(
        "gulp_system_cpu_percent",
        "System-wide CPU utilization percentage",
        multiprocess_mode="livesum",
    )
    system_memory_used_percent = Gauge(
        "gulp_system_memory_used_percent",
        "System-wide memory utilization percentage",
        multiprocess_mode="livesum",
    )

    # ── Server info ──
    server_uptime_seconds = Gauge(
        "gulp_server_uptime_seconds",
        "Server uptime in seconds",
        multiprocess_mode="livesum",
    )
    server_info = Info(
        "gulp_server",
        "gULP server information",
    )

    def __init__(self):
        self._collect_task: asyncio.Task = None
        self._start_time: float = time.monotonic()
        self._known_worker_pids: set[str] = set()
        self._worker_psutil_procs: dict[int, object] = {}
        self._main_psutil_proc = None
        self._system_cpu_warmed_up: bool = False

    def __new__(cls):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def get_instance(cls) -> "GulpMetrics":
        if not cls._instance:
            cls._instance = cls()
        return cls._instance

    async def start_collect_loop(self) -> None:
        """Start the background gauge collection task (main process only)."""
        if self._collect_task is not None:
            return
        self._collect_task = asyncio.create_task(
            self._collect_gauges_loop(),
            name="gulp-prometheus-collect",
        )

    async def stop_collect_loop(self) -> None:
        """Stop the background gauge collection task."""
        if self._collect_task is not None:
            self._collect_task.cancel()
            try:
                await self._collect_task
            except asyncio.CancelledError:
                pass
            self._collect_task = None

    async def _collect_gauges_loop(self) -> None:
        """Periodically update Gauge metrics by querying live system state."""
        interval = GulpConfig.get_instance().prometheus_collect_interval()
        while True:
            try:
                await self._collect_gauges()
            except asyncio.CancelledError:
                raise
            except Exception:
                MutyLogger.get_instance().exception(
                    "error collecting Prometheus gauge metrics"
                )
            await asyncio.sleep(interval)

    async def _collect_gauges(self) -> None:
        """Single collection pass — update all gauge values."""
        from gulp.api.redis_api import GulpRedis
        from gulp.api.ws_api import GulpConnectedSockets
        from gulp.process import GulpProcess

        # ── Server uptime ──
        self.server_uptime_seconds.set(time.monotonic() - self._start_time)

        # ── WebSocket gauges ──
        try:
            sockets = GulpConnectedSockets.get_instance()
            self.ws_connected_sockets.set(sockets.num_connected_sockets(default_sockets_only=False))
            self.ws_queue_utilization.set(sockets.aggregate_queue_utilization())
        except Exception:
            pass

        # ── Redis token bucket ──
        try:
            redis_inst = GulpRedis.get_instance()
            self.redis_token_bucket_tokens.set(redis_inst._token_bucket_tokens)
        except Exception:
            pass

        # ── Redis stream depths ──
        try:
            redis_client = GulpRedis.get_instance().client()
            # task streams
            task_types = await redis_client.smembers(GulpRedis.TASK_TYPES_SET)
            for tt in task_types:
                tt_str = tt.decode() if isinstance(tt, bytes) else tt
                stream_key = f"{GulpRedis.STREAM_TASK_PREFIX}:{tt_str}"
                try:
                    length = await redis_client.xlen(stream_key)
                    self.redis_stream_depth.labels(stream=tt_str).set(length)
                except Exception:
                    pass
                try:
                    pending_info = await redis_client.xpending(
                        stream_key, GulpRedis.STREAM_CONSUMER_GROUP
                    )
                    pending_count = pending_info.get("pending", 0) if isinstance(pending_info, dict) else 0
                    self.redis_stream_pending.labels(stream=tt_str).set(pending_count)
                except Exception:
                    pass

            # cluster liveness from heartbeat keys
            try:
                live_servers = 0
                async for _ in redis_client.scan_iter(match=f"{GulpRedis.HEARTBEAT_KEY_PREFIX}:*"):
                    live_servers += 1
                self.redis_cluster_live_servers.set(live_servers)
            except Exception:
                pass

            # subscriber counts for key pub/sub channels
            try:
                channels = ["gulpredis", "gulpredis:client_data"]
                numsub = await redis_client.pubsub_numsub(*channels)
                if isinstance(numsub, (list, tuple)):
                    for row in numsub:
                        if not isinstance(row, (list, tuple)) or len(row) != 2:
                            continue
                        ch, subs = row
                        ch_str = ch.decode() if isinstance(ch, bytes) else str(ch)
                        try:
                            self.redis_pubsub_channel_subscribers.labels(channel=ch_str).set(int(subs))
                        except Exception:
                            pass
            except Exception:
                pass
        except Exception:
            pass

        # ── PostgreSQL pool stats ──
        try:
            from gulp.api.collab_api import GulpCollab
            collab = GulpCollab.get_instance()
            if collab._engine is not None:
                pool = collab._engine.pool
                self.postgres_pool_size.set(pool.size())
                self.postgres_pool_checked_out.set(pool.checkedout())
                self.postgres_pool_idle.set(pool.checkedin())
                self.postgres_pool_overflow.set(pool.overflow())
        except Exception:
            pass

        # ── Worker process count + resource utilization ──
        try:
            import psutil
            proc = GulpProcess.get_instance()
            if proc.process_pool is not None and hasattr(proc.process_pool, 'processes'):
                alive_pids = set()
                for worker_proc in list(proc.process_pool.processes.keys()):
                    if worker_proc.is_alive() and worker_proc.pid:
                        pid = worker_proc.pid
                        alive_pids.add(str(pid))
                        try:
                            ps = self._worker_psutil_procs.get(pid)
                            if ps is None:
                                ps = psutil.Process(pid)
                                ps.cpu_percent()  # warmup: first call always returns 0.0
                                self._worker_psutil_procs[pid] = ps
                            mem = ps.memory_info()
                            self.worker_memory_rss_bytes.labels(worker_pid=str(pid)).set(mem.rss)
                            self.worker_cpu_percent.labels(worker_pid=str(pid)).set(
                                ps.cpu_percent(interval=None)
                            )
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            pass
                self.worker_process_count.set(len(alive_pids))

                # remove stale label series for dead workers
                for label_pid in list(self._known_worker_pids - alive_pids):
                    try:
                        self.worker_memory_rss_bytes.remove(label_pid)
                    except Exception:
                        pass
                    try:
                        self.worker_cpu_percent.remove(label_pid)
                    except Exception:
                        pass
                    self._worker_psutil_procs.pop(int(label_pid), None)
                self._known_worker_pids = alive_pids
            else:
                self.worker_process_count.set(0)
        except Exception:
            pass

        # ── Main process resource utilization ──
        try:
            import psutil
            if self._main_psutil_proc is None:
                self._main_psutil_proc = psutil.Process(os.getpid())
                self._main_psutil_proc.cpu_percent()  # warmup: first call always returns 0.0
            main_ps = self._main_psutil_proc
            mem = main_ps.memory_info()
            self.main_process_memory_rss_bytes.set(mem.rss)
            self.main_process_cpu_percent.set(main_ps.cpu_percent(interval=None))
            self.main_process_open_fds.set(main_ps.num_fds())
        except Exception:
            pass

        # ── System-wide resource utilization ──
        try:
            import psutil
            if not self._system_cpu_warmed_up:
                psutil.cpu_percent()  # warmup: first call always returns 0.0
                self._system_cpu_warmed_up = True
            self.system_cpu_percent.set(psutil.cpu_percent(interval=None))
            self.system_memory_used_percent.set(psutil.virtual_memory().percent)
        except Exception:
            pass


def _metrics_endpoint():
    """Generate Prometheus metrics output, aggregating all processes."""
    from starlette.responses import Response

    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)
    data = generate_latest(registry)
    return Response(content=data, media_type="text/plain; version=0.0.4; charset=utf-8")


def setup_prometheus(app) -> None:
    """
    Attach Prometheus instrumentation to a FastAPI app.

    Call this AFTER middleware is configured but BEFORE routers are added.
    """
    if not GulpConfig.get_instance().prometheus_enabled():
        return

    MutyLogger.get_instance().info("Enabling Prometheus metrics endpoint...")

    from prometheus_fastapi_instrumentator import Instrumentator

    endpoint: str = "/metrics"

    # auto-instrument HTTP requests (latency histogram + request counter by method/path/status)
    instrumentator = Instrumentator(
        should_group_status_codes=True,
        should_ignore_untemplated=True,
        excluded_handlers=[endpoint],
    )
    instrumentator.instrument(app)

    # register custom /metrics endpoint that aggregates multiprocess data
    app.add_api_route(
        endpoint,
        _metrics_endpoint,
        methods=["GET"],
        tags=["monitoring"],
    )

    MutyLogger.get_instance().info(
        "Prometheus metrics enabled at %s", endpoint
    )


def cleanup_prometheus() -> None:
    """Clean up multiprocess temp directory on shutdown."""
    GulpMetrics.cleanup_multiproc_dir()
