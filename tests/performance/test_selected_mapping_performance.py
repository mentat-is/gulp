"""Opt-in performance coverage for the selected-mapping fallback cache."""

import asyncio
import hashlib
import re
import statistics
import time
from collections.abc import Callable
from pathlib import Path

import orjson
import pytest

from gulp.api.mapping.models import GulpMapping, GulpMappingFile
from gulp.plugin import GulpPluginBase
from gulp.plugins.apache_access_clf import Plugin
from gulp.structs import GulpPluginParameters

_SAMPLE_SIZE = 100_000
_WARMUP_SIZE = 1_000
_REPETITIONS = 3
_FULL_SAMPLE_EVENTS = 14_726_712
_SAMPLE_PATH = Path("samples/apache_clf/ipplusonline.ipplus.energy.access.log")
_MAPPING_PATH = Path("src/gulp/mapping_files/apache_access_clf.json")
_DATE_FORMAT = "%d/%b/%Y:%H:%M:%S %z"
_PATTERN = re.compile(
    r"\s+".join(
        [
            r"(?P<host>\S+)",
            r"\S+",
            r"(?P<user>\S+)",
            r"\[(?P<datetime>(?P<date>.*?)(?= ) (?P<timezone>.*?))\]",
            r"\"(?P<request_method>.*?) (?P<path>.*?)(?P<request_version> HTTP\/.*)?\"",
            r"(?P<status>[0-9]+)",
            r"(?P<size>\S+)",
            r"\"(?P<referrer>.*)\"",
            r"\"(?P<agent>.*)\"",
        ]
    )
    + r".*\Z"
)


def _selected_mapping_with_allocation(plugin: GulpPluginBase) -> GulpMapping:
    """Reproduce the pre-optimization mapping lookup for the A/B baseline."""
    return plugin._mappings.get(plugin._mapping_id, GulpMapping())


def _load_lines(limit: int) -> list[str]:
    lines: list[str] = []
    with _SAMPLE_PATH.open("r", encoding="utf8") as sample:
        for line in sample:
            if line.strip():
                lines.append(line.strip())
                if len(lines) == limit:
                    break
    return lines


def _build_plugin() -> Plugin:
    mapping_file = GulpMappingFile.model_validate_json(_MAPPING_PATH.read_bytes())
    plugin = Plugin(str(Path("src/gulp/plugins/apache_access_clf.py")), __name__)
    plugin._mappings = mapping_file.mappings
    plugin._mapping_id = "default"
    plugin._plugin_params = GulpPluginParameters()
    plugin._operation_id = "selected-mapping-benchmark"
    plugin._context_id = "benchmark-context"
    plugin._source_id = "benchmark-source"
    plugin._file_path = str(_SAMPLE_PATH)
    return plugin


async def _parse_and_digest(
    plugin: Plugin,
    lines: list[str],
    selected_mapping: Callable[[GulpPluginBase], GulpMapping],
) -> tuple[float, str]:
    digest = hashlib.sha256()
    elapsed = 0.0
    original = GulpPluginBase.selected_mapping
    GulpPluginBase.selected_mapping = selected_mapping
    try:
        for record_idx, line in enumerate(lines):
            started = time.perf_counter()
            doc = await plugin._record_to_gulp_document(
                line,
                record_idx,
                regex=_PATTERN,
                date_format=_DATE_FORMAT,
            )
            elapsed += time.perf_counter() - started
            assert doc is not None
            digest.update(
                orjson.dumps(
                    doc.model_dump(by_alias=True),
                    option=orjson.OPT_SORT_KEYS,
                )
            )
    finally:
        GulpPluginBase.selected_mapping = original
    return elapsed, digest.hexdigest()


@pytest.mark.performance
def test_selected_mapping_performance() -> None:
    """The cached fallback keeps output stable and reduces Apache parse time 20%."""
    if not _SAMPLE_PATH.is_file():
        pytest.skip(f"Apache performance sample not found: {_SAMPLE_PATH}")

    lines = _load_lines(_SAMPLE_SIZE)
    if len(lines) < _SAMPLE_SIZE:
        pytest.skip(
            f"Apache performance sample has {len(lines)} records; {_SAMPLE_SIZE} required"
        )

    plugin = _build_plugin()
    optimized = GulpPluginBase.selected_mapping

    # Warm both paths before measuring and alternate their order across repetitions.
    asyncio.run(
        _parse_and_digest(
            plugin, lines[:_WARMUP_SIZE], _selected_mapping_with_allocation
        )
    )
    asyncio.run(_parse_and_digest(plugin, lines[:_WARMUP_SIZE], optimized))

    timings: dict[str, list[float]] = {"baseline": [], "optimized": []}
    digests: set[str] = set()
    modes = {
        "baseline": _selected_mapping_with_allocation,
        "optimized": optimized,
    }
    for repetition in range(_REPETITIONS):
        order = ("baseline", "optimized")
        if repetition % 2:
            order = tuple(reversed(order))
        for mode in order:
            elapsed, digest = asyncio.run(_parse_and_digest(plugin, lines, modes[mode]))
            timings[mode].append(elapsed)
            digests.add(digest)

    baseline_median = statistics.median(timings["baseline"])
    optimized_median = statistics.median(timings["optimized"])
    ratio = optimized_median / baseline_median
    improvement = (1.0 - ratio) * 100.0
    events_per_second = _SAMPLE_SIZE / optimized_median
    projected_minutes = _FULL_SAMPLE_EVENTS / events_per_second / 60.0

    print(
        "selected_mapping benchmark: "
        f"baseline_median={baseline_median:.3f}s, "
        f"optimized_median={optimized_median:.3f}s, "
        f"events_per_second={events_per_second:.1f}, "
        f"improvement={improvement:.2f}%, "
        f"projected_full_parse={projected_minutes:.1f}min"
    )

    assert len(digests) == 1, "baseline and optimized documents differ"
    assert ratio <= 0.80, (
        f"optimized median {optimized_median:.3f}s exceeds 80% of "
        f"baseline {baseline_median:.3f}s"
    )
