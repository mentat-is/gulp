"""Unit tests for query-history ordering and legacy-data handling."""

from copy import deepcopy
from types import SimpleNamespace

import pytest

from gulp.api.collab.user import GulpUser


@pytest.mark.unit
@pytest.mark.asyncio
async def test_query_history_is_newest_first_without_mutating_user_data() -> None:
    """Order timestamps and ties while preserving stored insertion order."""
    history = [
        {"marker": "same_timestamp_older", "q": {}, "timestamp_msec": 300},
        {
            "marker": "legacy_payload",
            "query": {"query": {"match_all": {}}},
            "query_options": {},
            "timestamp_msec": 100,
        },
        {"marker": "same_timestamp_newer", "q": {}, "timestamp_msec": 300},
        {"marker": "missing_timestamp_older", "query": {}},
        {"marker": "invalid_timestamp_newer", "q": {}, "timestamp_msec": "bad"},
    ]
    stored_snapshot = deepcopy(history)
    user = SimpleNamespace(user_data={"query_history": history})

    result = await GulpUser.get_query_history(user)

    assert [entry["marker"] for entry in result] == [
        "same_timestamp_newer",
        "same_timestamp_older",
        "legacy_payload",
        "invalid_timestamp_newer",
        "missing_timestamp_older",
    ]
    assert result is not history
    assert user.user_data["query_history"] == stored_snapshot


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize("user_data", [None, {}, {"query_history": "invalid"}])
async def test_query_history_handles_missing_or_invalid_storage(
    user_data: dict | None,
) -> None:
    """Return an empty history when the optional user-data value is unavailable."""
    user = SimpleNamespace(user_data=user_data)

    assert await GulpUser.get_query_history(user) == []
