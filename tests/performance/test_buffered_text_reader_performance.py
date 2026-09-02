"""Opt-in benchmark for the shared chunk-buffered asynchronous text reader."""

import asyncio
import hashlib
import statistics
import time
from pathlib import Path

import aiofiles
import pytest

import muty.file


_SAMPLE_SIZE = 100_000
_WARMUP_SIZE = 1_000
_REPETITIONS = 3
_FULL_SAMPLE_EVENTS = 14_726_712
_SAMPLE_PATH = Path("samples/apache_clf/ipplusonline.ipplus.energy.access.log")


async def _read_with_aiofiles(path: Path, limit: int) -> tuple[float, str, int]:
    """Measure the former one-thread-pool-dispatch-per-line implementation."""
    digest = hashlib.sha256()
    count = 0
    started = time.perf_counter()
    async with aiofiles.open(path, "r", encoding="utf8") as source:
        async for line in source:
            digest.update(line.encode("utf8"))
            count += 1
            if count == limit:
                break
    return time.perf_counter() - started, digest.hexdigest(), count


async def _read_buffered(path: Path, limit: int) -> tuple[float, str, int]:
    """Measure fixed-size reads plus incremental decoding and line splitting."""
    digest = hashlib.sha256()
    count = 0
    started = time.perf_counter()
    async for line in muty.file.iter_text_file_lines(
        path, encoding="utf8", newline=None
    ):
        digest.update(line.encode("utf8"))
        count += 1
        if count == limit:
            break
    return time.perf_counter() - started, digest.hexdigest(), count


@pytest.mark.performance
def test_buffered_text_reader_performance() -> None:
    """The shared reader preserves output and uses at most half the legacy time."""
    if not _SAMPLE_PATH.is_file():
        pytest.skip(f"Apache performance sample not found: {_SAMPLE_PATH}")

    baseline_warmup = asyncio.run(
        _read_with_aiofiles(_SAMPLE_PATH, _WARMUP_SIZE)
    )
    optimized_warmup = asyncio.run(_read_buffered(_SAMPLE_PATH, _WARMUP_SIZE))
    if baseline_warmup[2] < _WARMUP_SIZE:
        pytest.skip(
            f"Apache performance sample has {baseline_warmup[2]} records; "
            f"{_SAMPLE_SIZE} required"
        )
    assert baseline_warmup[1:] == optimized_warmup[1:]

    timings: dict[str, list[float]] = {"baseline": [], "optimized": []}
    digests: set[str] = set()
    counts: set[int] = set()
    readers = {
        "baseline": _read_with_aiofiles,
        "optimized": _read_buffered,
    }
    for repetition in range(_REPETITIONS):
        order = ("baseline", "optimized")
        if repetition % 2:
            order = tuple(reversed(order))
        for mode in order:
            elapsed, digest, count = asyncio.run(
                readers[mode](_SAMPLE_PATH, _SAMPLE_SIZE)
            )
            timings[mode].append(elapsed)
            digests.add(digest)
            counts.add(count)

    if counts != {_SAMPLE_SIZE}:
        pytest.skip(
            f"Apache performance sample has {max(counts, default=0)} records; "
            f"{_SAMPLE_SIZE} required"
        )

    baseline_median = statistics.median(timings["baseline"])
    optimized_median = statistics.median(timings["optimized"])
    ratio = optimized_median / baseline_median
    improvement = (1.0 - ratio) * 100.0
    events_per_second = _SAMPLE_SIZE / optimized_median
    projected_minutes = _FULL_SAMPLE_EVENTS / events_per_second / 60.0

    print(
        "buffered text reader benchmark: "
        f"baseline_median={baseline_median:.3f}s, "
        f"optimized_median={optimized_median:.3f}s, "
        f"events_per_second={events_per_second:.1f}, "
        f"improvement={improvement:.2f}%, "
        f"projected_full_read={projected_minutes:.1f}min"
    )

    assert len(digests) == 1, "baseline and buffered logical bytes differ"
    assert ratio <= 0.50, (
        f"optimized median {optimized_median:.3f}s exceeds 50% of "
        f"baseline {baseline_median:.3f}s"
    )
