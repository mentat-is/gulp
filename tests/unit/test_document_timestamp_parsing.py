"""Document timestamp parsing regression coverage."""

import pytest
import muty.time

from gulp.api.opensearch.structs import GulpDocument


@pytest.mark.unit
@pytest.mark.parametrize(
    ("value", "expected_nanos"),
    [
        ("1999-04-23T22:22:00+00:00", 924_906_120_000_000_000),
        ("2004-08-18T00:00:00+00:00", 1_092_787_200_000_000_000),
    ],
)
def test_ensure_timestamp_handles_nanoseconds_on_both_sides_of_1e18(
    value: str, expected_nanos: int
) -> None:
    timestamp, nanos, invalid = GulpDocument.ensure_timestamp(value)

    assert muty.time.nanos_from_unix_epoch_to_iso8601(expected_nanos) == value
    assert timestamp == value
    assert nanos == expected_nanos
    assert invalid is False
