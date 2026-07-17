"""Stacked plugin lower-plugin selection regressions."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gulp.plugin import GulpPluginBase, GulpPluginCacheMode, GulpPluginType
from gulp.structs import GulpPluginParameters


class _IngestionPlugin(GulpPluginBase):
    """Minimal ingestion plugin used to exercise stack setup."""

    def display_name(self) -> str:
        return "test"

    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION


@pytest.mark.unit
def test_stacked_plugin_parameter_is_not_empty() -> None:
    """A lower-plugin override must count as supplied plugin parameters."""
    params = GulpPluginParameters(stacked_plugin="csv_pro")

    print("stacked plugin parameters:", params.model_dump(exclude_none=True))
    assert not params.is_empty()


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("stacked_plugin", "nested", "expected"),
    [(None, False, "csv"), ("csv_pro", False, "csv_pro"), ("csv_pro", True, "csv")],
)
async def test_setup_stacked_plugin_selects_configured_lower(
    monkeypatch, stacked_plugin, nested, expected
) -> None:
    """Only the outer stacked plugin may replace its declared lower plugin."""
    upper = _IngestionPlugin("/tmp/upper.py", "upper")
    upper._plugin_params = GulpPluginParameters(stacked_plugin=stacked_plugin)
    if nested:
        upper._upper_instance = object()

    lower = SimpleNamespace(name=expected, type=lambda: GulpPluginType.INGESTION)
    load = AsyncMock(return_value=lower)
    monkeypatch.setattr(GulpPluginBase, "load", load)

    result = await upper.setup_stacked_plugin("csv")

    print("selected lower plugin:", result.name)
    load.assert_awaited_once_with(
        expected,
        extension=False,
        cache_mode=GulpPluginCacheMode.DEFAULT,
    )
    assert result is lower
    assert upper._lower_instance is lower
    assert lower._upper_instance is upper
