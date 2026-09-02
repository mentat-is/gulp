"""Opt-in benchmark for one-pass timestamp normalization."""

import datetime
import hashlib
import re
import statistics
import time
from collections.abc import Callable
from pathlib import Path

import pytest

from gulp.api.opensearch.structs import GulpDocument, GulpNormalizedTimestamp

_SAMPLE_SIZE = 100_000
_WARMUP_SIZE = 1_000
_REPETITIONS = 3
_FULL_SAMPLE_EVENTS = 14_726_712
_SAMPLE_PATH = Path("samples/apache_clf/ipplusonline.ipplus.energy.access.log")
_APACHE_FORMAT = "%d/%b/%Y:%H:%M:%S %z"
_REGEX_FORMAT = "%Y/%m/%d-%H:%M:%S"
_APACHE_TIMESTAMP = re.compile(r"\[(?P<timestamp>[^\]]+)\]")


def _load_timestamp_inputs(limit: int) -> list[tuple[str, str]]:
    """Load real Apache timestamps in Apache and custom-regex formats."""
    values: list[tuple[str, str]] = []
    with _SAMPLE_PATH.open("r", encoding="utf8") as sample:
        for line in sample:
            match = _APACHE_TIMESTAMP.search(line)
            if not match:
                continue
            raw = match.group("timestamp")
            if len(values) % 2:
                parsed = datetime.datetime.strptime(raw, _APACHE_FORMAT)
                values.append((parsed.strftime(_REGEX_FORMAT), _REGEX_FORMAT))
            else:
                values.append((raw, _APACHE_FORMAT))
            if len(values) == limit:
                break
    return values


def _legacy_normalize(raw: str, timestamp_format: str) -> GulpNormalizedTimestamp:
    """Reproduce strptime -> isoformat -> ensure_timestamp from before point 2."""
    intermediate = datetime.datetime.strptime(raw, timestamp_format).isoformat()
    iso8601, nanos, invalid = GulpDocument.ensure_timestamp(intermediate)
    return GulpNormalizedTimestamp(iso8601, nanos, invalid)


def _one_pass_normalize(raw: str, timestamp_format: str) -> GulpNormalizedTimestamp:
    return GulpDocument.normalize_timestamp(
        raw,
        timestamp_format=timestamp_format,
        timestamp_naive_timezone="utc",
    )


def _measure(
    values: list[tuple[str, str]],
    normalize: Callable[[str, str], GulpNormalizedTimestamp],
) -> tuple[float, str]:
    digest = hashlib.sha256()
    started = time.perf_counter()
    normalized_values = [normalize(raw, fmt) for raw, fmt in values]
    elapsed = time.perf_counter() - started
    for normalized in normalized_values:
        digest.update(
            f"{normalized.iso8601}|{normalized.nanos}|{normalized.invalid}\n".encode()
        )
    return elapsed, digest.hexdigest()


@pytest.mark.performance
def test_timestamp_normalization_performance() -> None:
    """One-pass Apache/regex normalization uses at most 40% of legacy time."""
    if not _SAMPLE_PATH.is_file():
        pytest.skip(f"Apache performance sample not found: {_SAMPLE_PATH}")

    values = _load_timestamp_inputs(_SAMPLE_SIZE)
    if len(values) < _SAMPLE_SIZE:
        pytest.skip(
            f"Apache performance sample has {len(values)} timestamps; "
            f"{_SAMPLE_SIZE} required"
        )

    _measure(values[:_WARMUP_SIZE], _legacy_normalize)
    _measure(values[:_WARMUP_SIZE], _one_pass_normalize)

    timings: dict[str, list[float]] = {"baseline": [], "optimized": []}
    digests: set[str] = set()
    modes = {
        "baseline": _legacy_normalize,
        "optimized": _one_pass_normalize,
    }
    for repetition in range(_REPETITIONS):
        order = ("baseline", "optimized")
        if repetition % 2:
            order = tuple(reversed(order))
        for mode in order:
            elapsed, digest = _measure(values, modes[mode])
            timings[mode].append(elapsed)
            digests.add(digest)

    baseline_median = statistics.median(timings["baseline"])
    optimized_median = statistics.median(timings["optimized"])
    ratio = optimized_median / baseline_median
    improvement = (1.0 - ratio) * 100.0
    events_per_second = _SAMPLE_SIZE / optimized_median
    projected_minutes = _FULL_SAMPLE_EVENTS / events_per_second / 60.0

    print(
        "timestamp normalization benchmark: "
        f"baseline_median={baseline_median:.3f}s, "
        f"optimized_median={optimized_median:.3f}s, "
        f"events_per_second={events_per_second:.1f}, "
        f"improvement={improvement:.2f}%, "
        f"projected_full_normalization={projected_minutes:.1f}min"
    )

    assert len(digests) == 1, "legacy and one-pass timestamp results differ"
    assert ratio <= 0.40, (
        f"optimized median {optimized_median:.3f}s exceeds 40% of "
        f"baseline {baseline_median:.3f}s"
    )
