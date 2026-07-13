"""
generic objects functions
"""
from collections import Counter
import sys
from collections import defaultdict
from contextlib import contextmanager, asynccontextmanager
import gc
from typing import Any
import tracemalloc
import psutil
import time
import os
from muty.log import MutyLogger
import asyncio

BYTES_PER_KB = 1024
BYTES_PER_MB = 1024 * 1024


def _format_bytes(bytes_val: int) -> str:
    """Format bytes with units while preserving raw value"""
    mb_val = bytes_val / BYTES_PER_MB
    return f"{bytes_val:,} bytes ({mb_val:.2f} MB)"


def close_object(closeable: Any):
    """
    calls close on the given object
    :param closeable: object implementing close()
    :return:
    """
    if closeable and hasattr(closeable, "close"):
        # call close
        closeable.close()


def _format_memory_stats(
    mem_diff: int,
    peak_traced: int,
    vm_stats: Any,
    mem_end: int,
    gc_diff: dict,
    compare_stats: list
) -> list[str]:
    """Format memory statistics into log lines"""
    logger_lines = []
    logger_lines.append(f"[.] PID: {os.getpid()}")
    logger_lines.append(f"[.] Memory Usage: {_format_bytes(mem_diff)}")
    logger_lines.append(f"[.] Peak Memory: {_format_bytes(peak_traced)}")
    logger_lines.append(f"[.] System Memory: {
                        vm_stats.percent}% used ({_format_bytes(vm_stats.used)})")
    logger_lines.append(f"[.] Process Memory: {_format_bytes(mem_end)}")

    logger_lines.append("[.] Top 5 Memory Allocations:")
    for stat in compare_stats[:5]:
        logger_lines.append(f"{stat} ({_format_bytes(stat.size)})")

    if gc_diff:
        logger_lines.append("[.] Object Count Changes:")
        for k, v in sorted(gc_diff.items(), key=lambda x: x[1], reverse=True)[:5]:
            logger_lines.append(f"  {k}: +{v}")

    return logger_lines


async def _gather_memory_stats_async():
    """Gather memory statistics asynchronously"""
    loop = asyncio.get_running_loop()
    process = psutil.Process(os.getpid())

    await loop.run_in_executor(None, gc.collect)
    mem_info = await loop.run_in_executor(None, lambda: process.memory_info().rss)
    gc_stats = await loop.run_in_executor(
        None,
        lambda: Counter(str(o.__class__) for o in gc.get_objects())
    )
    return mem_info, gc_stats


def _gather_memory_stats():
    """Gather memory statistics synchronously"""
    process = psutil.Process(os.getpid())
    gc.collect()
    return process.memory_info().rss, Counter(str(o.__class__) for o in gc.get_objects())


@asynccontextmanager
async def track_memory_async():
    """
    Async context manager to track memory usage and log statistics
    """
    mem_start, gc_stats_start = await _gather_memory_stats_async()
    tracemalloc.start()
    snapshot1 = tracemalloc.take_snapshot()

    try:
        yield
    finally:
        snapshot2 = tracemalloc.take_snapshot()
        mem_end, gc_stats_end = await _gather_memory_stats_async()

        stats = _format_memory_stats(
            mem_diff=mem_end - mem_start,
            peak_traced=tracemalloc.get_traced_memory()[1],
            vm_stats=psutil.virtual_memory(),
            mem_end=mem_end,
            gc_diff={k: gc_stats_end[k] - gc_stats_start[k]
                     for k in gc_stats_end
                     if gc_stats_end[k] - gc_stats_start[k] > 0},
            compare_stats=snapshot2.compare_to(snapshot1, 'lineno')
        )

        for line in stats:
            print(line)
        tracemalloc.stop()


@contextmanager
def track_memory():
    """
    Context manager to track memory usage and log statistics        
    """
    mem_start, gc_stats_start = _gather_memory_stats()
    tracemalloc.start()
    snapshot1 = tracemalloc.take_snapshot()

    try:
        yield
    finally:
        snapshot2 = tracemalloc.take_snapshot()
        mem_end, gc_stats_end = _gather_memory_stats()

        stats = _format_memory_stats(
            mem_diff=mem_end - mem_start,
            peak_traced=tracemalloc.get_traced_memory()[1],
            vm_stats=psutil.virtual_memory(),
            mem_end=mem_end,
            gc_diff={k: gc_stats_end[k] - gc_stats_start[k]
                     for k in gc_stats_end
                     if gc_stats_end[k] - gc_stats_start[k] > 0},
            compare_stats=snapshot2.compare_to(snapshot1, 'lineno')
        )

        for line in stats:
            print(line)
        tracemalloc.stop()


def trigger_gc_and_get_stats(get_full_objects_stats: bool = True) -> dict:
    """
    triggers a gc collection and returns a comprehensive dictionary of garbage collection stats, including memory before/after gc

    Args:
        get_full_objects_stats (bool): whether to include detailed object allocation stats, default True

    Returns:
        dict: dictionary of garbage collection stats
    """
    def _gc_stats(get_full_objects_stats: bool = True) -> dict:
        stats = {
            "collections": [gc.get_count()[i] for i in range(3)],
            "objects_tracked": len(gc.get_objects()),
            "garbage": len(gc.garbage),
            "objects_detail": _get_object_allocation_stats() if get_full_objects_stats else {}
        }
        return stats

    def _get_memory_info() -> dict:
        mem = psutil.Process(os.getpid()).memory_info()
        return {
            "shared_mb": mem.shared / (1024 * 1024),
            "shared": mem.shared,
            "rss_mb": mem.rss / (1024 * 1024),
            "rss": mem.rss,
            "vms_mb": mem.vms / (1024 * 1024),
            "vms": mem.vms
        }

    def _get_object_allocation_stats() -> dict:
        type_counts = defaultdict(int)
        type_sizes = defaultdict(int)
        source_locations = defaultdict(list)

        for obj in gc.get_objects():
            obj_type = type(obj).__name__
            type_counts[obj_type] += 1
            type_sizes[obj_type] += sys.getsizeof(obj, 0)

            # Try to get frame info for object creation
            try:
                if hasattr(obj, '__traceback__'):
                    frames = inspect.getinnerframes(obj.__traceback__)
                    if frames:
                        frame = frames[-1]
                        source_locations[obj_type].append({
                            'file': frame.filename,
                            'line': frame.lineno,
                            'function': frame.function
                        })
            except:
                pass

        return {
            "by_type": {
                t: {
                    "count": count,
                    "total_size_bytes": type_sizes[t],
                    "avg_size_bytes": type_sizes[t] // count,
                    # Limit to 5 examples
                    "source_samples": source_locations[t][:5]
                }
                for t, count in type_counts.items()
            }
        }

    # collect stats before
    before_stats = _gc_stats(get_full_objects_stats)
    before_mem = _get_memory_info()
    start_time = time.perf_counter()

    # Run collection
    gc.collect()

    # collect stats after
    end_time = time.perf_counter()
    after_stats = _gc_stats(get_full_objects_stats)
    after_mem = _get_memory_info()

    result = {
        "before": {
            "memory": before_mem,
            "gc_stats": before_stats
        },
        "after": {
            "memory": after_mem,
            "gc_stats": after_stats
        },
        "delta": {
            "rss": before_mem["rss"] - after_mem["rss"],
            "rss_mb": before_mem["rss_mb"] - after_mem["rss_mb"],
            "vms": before_mem["vms"] - after_mem["vms"],
            "vms_mb": before_mem["vms_mb"] - after_mem["vms_mb"],
            "shared": before_mem["shared"] - after_mem["shared"],
            "shared_mb": before_mem["shared_mb"] - after_mem["shared_mb"],
            "objects": before_stats["objects_tracked"] - after_stats["objects_tracked"],
        },
        "collection_time_ms": (end_time - start_time) * 1000
    }
    return result
