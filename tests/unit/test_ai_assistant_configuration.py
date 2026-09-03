"""Configuration resolution coverage for the open AI assistant plugin."""

from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from gulp.plugins.extension import ai_assistant


class _Scalars:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def scalars(self):
        return _Scalars(self._rows)


class _Session:
    def __init__(self, rows):
        self.execute = AsyncMock(return_value=_Result(rows))


class _Collab:
    def __init__(self, session):
        self._session = session

    @asynccontextmanager
    async def session(self):
        yield self._session


def _plugin():
    return ai_assistant.Plugin(ai_assistant.__file__, "ai_assistant_test")


def _new_obj(default: bool, key: str) -> dict:
    return {
        "plugin": "ai_assistant",
        "default": default,
        "configuration": [
            {
                "name": "default_model",
                "type": "str",
                "default_value": "fallback-model",
                "value": "selected-model",
            },
            {
                "name": "api_base_url",
                "type": "str",
                "default_value": "https://api.example/v1",
            },
            {
                "name": "openrouter_key",
                "type": "str",
                "default_value": "fallback-key",
                "value": key,
                "is_secret": True,
            },
        ],
    }


@pytest.mark.unit
def test_ai_configuration_values_prefer_value_over_default():
    values = ai_assistant.Plugin._configuration_values(_new_obj(True, "secret"))
    assert values == {
        "default_model": "selected-model",
        "api_base_url": "https://api.example/v1",
        "openrouter_key": "secret",
    }


@pytest.mark.unit
@pytest.mark.asyncio
async def test_ai_reads_only_new_default_profile(monkeypatch):
    rows = [
        SimpleNamespace(id="secondary", obj=_new_obj(False, "secondary-key")),
        SimpleNamespace(id="primary", obj=_new_obj(True, "primary-key")),
    ]
    session = _Session(rows)
    get_by_id = AsyncMock()
    create_internal = AsyncMock()
    monkeypatch.setattr(
        ai_assistant.GulpCollab,
        "get_instance",
        staticmethod(lambda: _Collab(session)),
    )
    monkeypatch.setattr(
        ai_assistant.SharedObjectPlugin.GulpSharedObject, "get_by_id", get_by_id
    )
    monkeypatch.setattr(
        ai_assistant.SharedObjectPlugin.GulpSharedObject,
        "create_internal",
        create_internal,
    )

    plugin = _plugin()
    await plugin._refresh_configuration_from_db()

    assert plugin._configuration["openrouter_key"] == "primary-key"
    get_by_id.assert_not_awaited()
    create_internal.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_ai_falls_back_to_legacy_db_without_writing(monkeypatch):
    session = _Session([])
    legacy = SimpleNamespace(
        obj_type="plugin_configuration",
        obj={
            "default_model": "legacy-model",
            "api_base_url": "https://legacy.example/v1",
            "openrouter_key": "legacy-key",
        }
    )
    get_by_id = AsyncMock(return_value=legacy)
    create_internal = AsyncMock()
    monkeypatch.setattr(
        ai_assistant.GulpCollab,
        "get_instance",
        staticmethod(lambda: _Collab(session)),
    )
    monkeypatch.setattr(
        ai_assistant.SharedObjectPlugin.GulpSharedObject, "get_by_id", get_by_id
    )
    monkeypatch.setattr(
        ai_assistant.SharedObjectPlugin.GulpSharedObject,
        "create_internal",
        create_internal,
    )

    plugin = _plugin()
    await plugin._refresh_configuration_from_db()

    assert plugin._configuration["default_model"] == "legacy-model"
    assert plugin._configuration["openrouter_key"] == "legacy-key"
    create_internal.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_ai_uses_file_only_when_database_has_no_configuration(monkeypatch):
    session = _Session([])
    monkeypatch.setattr(
        ai_assistant.GulpCollab,
        "get_instance",
        staticmethod(lambda: _Collab(session)),
    )
    monkeypatch.setattr(
        ai_assistant.SharedObjectPlugin.GulpSharedObject,
        "get_by_id",
        AsyncMock(return_value=None),
    )
    file_config = {
        "default_model": "file-model",
        "api_base_url": "https://file.example/v1",
        "openrouter_key": "file-key",
    }
    config = SimpleNamespace(get=MagicMock(return_value=file_config))
    monkeypatch.setattr(
        ai_assistant.GulpConfig,
        "get_instance",
        staticmethod(lambda: config),
    )

    plugin = _plugin()
    await plugin._refresh_configuration_from_db()

    assert plugin._configuration == file_config
    config.get.assert_called_once_with("ai_configuration", {})


@pytest.mark.unit
def test_ai_core_does_not_import_paid_configuration_models():
    source = Path(ai_assistant.__file__).read_text(encoding="utf-8")
    assert "plugin_manager" not in source
    assert "PluginConfigurationObject" not in source
