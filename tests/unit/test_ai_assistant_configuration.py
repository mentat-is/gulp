"""Regression coverage for both assistants' named DB parameters."""

from copy import deepcopy
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest


@pytest.mark.unit
def test_named_parameters_set_llm_configuration(
    ai_assistant_module: ModuleType, ai_configuration: dict
) -> None:
    """The nested defaults supply the key, model and URL without mutation."""
    plugin = object.__new__(ai_assistant_module.Plugin)
    original = deepcopy(ai_configuration)
    # Top-level fields must not override the canonical parameter list.
    ai_configuration["openrouter_key"] = "wrong-key"
    ai_configuration["default_model"] = "wrong-model"
    ai_configuration["api_base_url"] = "http://wrong.invalid"

    assert plugin._set_configuration(ai_configuration) is True
    plugin._validate_configuration()
    assert plugin._api_key == "test-secret"
    assert (
        getattr(plugin, "_model", getattr(plugin, "_default_model", None))
        == "test-model"
    )
    assert plugin._api_base_url == "http://127.0.0.1:1"
    assert ai_configuration["configuration"] == original["configuration"]
    assert plugin._set_configuration(deepcopy(ai_configuration)) is False


@pytest.mark.unit
@pytest.mark.parametrize("name", ["openrouter_key", "default_model", "api_base_url"])
@pytest.mark.parametrize("value", [None, "", "  ", 123])
def test_invalid_required_defaults_are_rejected(
    ai_assistant_module: ModuleType, ai_configuration: dict, name: str, value: object
) -> None:
    """Invalid required values fail locally without exposing credentials."""
    plugin = object.__new__(ai_assistant_module.Plugin)
    for parameter in ai_configuration["configuration"]:
        if parameter["name"] == name:
            parameter["default_value"] = value
    plugin._set_configuration(ai_configuration)

    with pytest.raises(ValueError, match=f"ai_configuration/{name}") as error:
        plugin._validate_configuration()
    assert "test-secret" not in str(error.value)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("openrouter_key", "sk-or-your-openrouter-api-key"),
        ("default_model", "your_default_model_here"),
    ],
)
def test_placeholder_defaults_are_rejected(
    ai_assistant_module: ModuleType, ai_configuration: dict, name: str, value: str
) -> None:
    """Previously seeded placeholders cannot reach the LLM API."""
    plugin = object.__new__(ai_assistant_module.Plugin)
    for parameter in ai_configuration["configuration"]:
        if parameter["name"] == name:
            parameter["default_value"] = value
    plugin._set_configuration(ai_configuration)
    with pytest.raises(ValueError, match=f"ai_configuration/{name}"):
        plugin._validate_configuration()


@pytest.mark.unit
def test_missing_parameter_does_not_reuse_previous_key(
    ai_assistant_module: ModuleType, ai_configuration: dict
) -> None:
    """Removing the key from DB configuration clears the cached credential."""
    plugin = object.__new__(ai_assistant_module.Plugin)
    plugin._set_configuration(ai_configuration)
    updated = deepcopy(ai_configuration)
    updated["configuration"] = [
        parameter
        for parameter in updated["configuration"]
        if parameter["name"] != "openrouter_key"
    ]
    plugin._set_configuration(updated)
    assert plugin._api_key is None
    with pytest.raises(ValueError, match="ai_configuration/openrouter_key"):
        plugin._validate_configuration()


@pytest.mark.unit
@pytest.mark.parametrize("parameters", [None, {}, ["invalid"], [{}]])
def test_malformed_parameter_list_is_rejected(
    ai_assistant_module: ModuleType, parameters: object
) -> None:
    """Malformed DB settings report a clear configuration error."""
    plugin = object.__new__(ai_assistant_module.Plugin)
    with pytest.raises(ValueError, match="ai_configuration/configuration"):
        plugin._set_configuration({"configuration": parameters})


@pytest.mark.unit
def test_seed_configuration_requires_credentials(
    ai_assistant_module: ModuleType,
) -> None:
    """Initial settings are editable metadata and never a usable fake key."""
    plugin = object.__new__(ai_assistant_module.Plugin)
    plugin._set_configuration(deepcopy(ai_assistant_module._DEFAULT_CONFIGURATION))
    assert plugin._api_key is None
    with pytest.raises(ValueError, match="ai_configuration/openrouter_key"):
        plugin._validate_configuration()


@pytest.fixture
def configuration_sources(
    ai_assistant_module: ModuleType,
    ai_configuration: dict,
    monkeypatch: pytest.MonkeyPatch,
) -> SimpleNamespace:
    """Provide independent DB and file sources without external I/O."""
    session = object()

    @asynccontextmanager
    async def sessions() -> AsyncIterator[object]:
        yield session

    read = AsyncMock(return_value=SimpleNamespace(obj=deepcopy(ai_configuration)))

    async def create(**kwargs: Any) -> SimpleNamespace:
        stored = SimpleNamespace(obj=deepcopy(kwargs["obj"]))
        read.return_value = stored
        return stored

    file_get = Mock(
        return_value={
            "openrouter_key": "file-key",
            "default_model": "file-model",
            "api_base_url": "http://file.invalid/v1",
        }
    )
    created = AsyncMock(side_effect=create)
    monkeypatch.setattr(
        ai_assistant_module.GulpCollab,
        "get_instance",
        lambda: SimpleNamespace(session=sessions),
    )
    monkeypatch.setattr(
        ai_assistant_module.GulpConfig,
        "get_instance",
        lambda: SimpleNamespace(get=file_get),
    )
    shared_object = ai_assistant_module.SharedObjectPlugin.GulpSharedObject
    monkeypatch.setattr(shared_object, "get_by_id", read)
    monkeypatch.setattr(shared_object, "create_internal", created)
    return SimpleNamespace(read=read, create=created, file_get=file_get)


async def _startup_refresh(plugin: Any) -> None:
    """Exercise the startup source choice for either plugin."""
    if hasattr(plugin, "_send_message"):
        await plugin._refresh_configuration_from_db(create_if_missing=True)
    else:
        await plugin._refresh_configuration_from_db()


@pytest.mark.unit
async def test_existing_db_configuration_wins_over_file(
    ai_assistant_module: ModuleType, configuration_sources: SimpleNamespace
) -> None:
    """Startup reads an existing DB row without consulting the file."""
    plugin = object.__new__(ai_assistant_module.Plugin)
    await _startup_refresh(plugin)
    plugin._validate_configuration()
    assert plugin._api_key == "test-secret"
    configuration_sources.file_get.assert_not_called()
    configuration_sources.create.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.parametrize("with_url", [True, False])
async def test_missing_db_configuration_uses_file(
    ai_assistant_module: ModuleType,
    configuration_sources: SimpleNamespace,
    with_url: bool,
) -> None:
    """Base uses the file directly; Pro persists a nested copy at startup."""
    sources = configuration_sources
    sources.read.return_value = None
    if not with_url:
        del sources.file_get.return_value["api_base_url"]
    original = deepcopy(sources.file_get.return_value)
    plugin = object.__new__(ai_assistant_module.Plugin)
    await _startup_refresh(plugin)
    plugin._validate_configuration()
    assert plugin._api_key == "file-key"
    assert (
        getattr(plugin, "_model", getattr(plugin, "_default_model", None))
        == "file-model"
    )
    assert plugin._api_base_url == (
        "http://file.invalid/v1" if with_url else "https://openrouter.ai/api/v1"
    )
    sources.file_get.assert_called_once_with("ai_configuration", {})
    assert sources.file_get.return_value == original
    if hasattr(plugin, "_send_message"):
        sources.create.assert_awaited_once()
        arguments = sources.create.await_args.kwargs
        assert arguments["obj"] == plugin._configuration
        assert arguments["private"] is True
        assert arguments["obj_type"] == "plugin_configuration"
        assert arguments["obj_id"] == "ai_configuration"
    else:
        sources.create.assert_not_awaited()


@pytest.mark.unit
async def test_missing_db_and_file_leave_unconfigured_defaults(
    ai_assistant_module: ModuleType, configuration_sources: SimpleNamespace
) -> None:
    """Only Pro persists defaults when no configured source exists."""
    sources = configuration_sources
    sources.read.return_value = None
    sources.file_get.return_value = {}
    plugin = object.__new__(ai_assistant_module.Plugin)
    await _startup_refresh(plugin)
    assert plugin._configuration == ai_assistant_module._DEFAULT_CONFIGURATION
    assert plugin._api_key is None
    with pytest.raises(ValueError, match="ai_configuration/openrouter_key"):
        plugin._validate_configuration()
    if hasattr(plugin, "_send_message"):
        sources.create.assert_awaited_once()
        assert (
            sources.create.await_args.kwargs["obj"]
            == ai_assistant_module._DEFAULT_CONFIGURATION
        )
    else:
        sources.create.assert_not_awaited()


@pytest.mark.unit
async def test_incomplete_db_configuration_does_not_fall_back_to_file(
    ai_assistant_module: ModuleType, configuration_sources: SimpleNamespace
) -> None:
    """An existing DB object is authoritative, including an empty object."""
    sources = configuration_sources
    sources.read.return_value = SimpleNamespace(obj={})
    plugin = object.__new__(ai_assistant_module.Plugin)
    await _startup_refresh(plugin)
    assert plugin._api_key is None
    with pytest.raises(ValueError, match="ai_configuration/openrouter_key"):
        plugin._validate_configuration()
    sources.file_get.assert_not_called()
    sources.create.assert_not_awaited()


@pytest.mark.unit
async def test_missing_db_during_requests_uses_only_the_allowed_source(
    ai_assistant_module: ModuleType, configuration_sources: SimpleNamespace
) -> None:
    """Base falls back on refresh; Pro cannot import the file during requests."""
    sources = configuration_sources
    plugin = object.__new__(ai_assistant_module.Plugin)
    await _startup_refresh(plugin)
    sources.read.return_value = None
    if hasattr(plugin, "_send_message"):
        with pytest.raises(ValueError, match="shared object .* not found"):
            await plugin._refresh_configuration_from_db()
        sources.file_get.assert_not_called()
    else:
        await plugin._refresh_configuration_from_db()
        plugin._validate_configuration()
        assert plugin._api_key == "file-key"
    sources.create.assert_not_awaited()


@pytest.mark.unit
async def test_pro_imports_file_only_when_creating_the_db_row(
    ai_assistant_module: ModuleType, configuration_sources: SimpleNamespace
) -> None:
    """File edits affect base fallback but cannot override Pro's seeded DB row."""
    sources = configuration_sources
    sources.read.return_value = None
    plugin = object.__new__(ai_assistant_module.Plugin)
    await _startup_refresh(plugin)
    sources.file_get.return_value["openrouter_key"] = "changed-file-key"
    await plugin._refresh_configuration_from_db()
    if hasattr(plugin, "_send_message"):
        assert plugin._api_key == "file-key"
        sources.file_get.assert_called_once()
        sources.create.assert_awaited_once()
    else:
        assert plugin._api_key == "changed-file-key"
        assert sources.file_get.call_count == 2
        sources.create.assert_not_awaited()


@pytest.mark.unit
async def test_db_errors_do_not_silently_enable_file_fallback(
    ai_assistant_module: ModuleType, configuration_sources: SimpleNamespace
) -> None:
    """Failure to query the DB is different from an absent configuration."""
    sources = configuration_sources
    sources.read.side_effect = RuntimeError("database unavailable")
    plugin = object.__new__(ai_assistant_module.Plugin)
    with pytest.raises(RuntimeError, match="database unavailable"):
        await _startup_refresh(plugin)
    sources.file_get.assert_not_called()
    sources.create.assert_not_awaited()
