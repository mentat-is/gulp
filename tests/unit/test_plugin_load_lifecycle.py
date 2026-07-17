"""Dynamic plugin load cleanup regressions."""

import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gulp import plugin as plugin_module
from gulp.api import server_api
from gulp.plugin import GulpPluginBase, GulpPluginCacheMode, GulpPluginType


@pytest.mark.unit
@pytest.mark.asyncio
async def test_failed_extension_post_init_rolls_back_process_state(monkeypatch):
    app = SimpleNamespace(router=SimpleNamespace(routes=["core-route"]))
    broker = SimpleNamespace(broadcast_types={"core-type"})
    module_names: list[str] = []

    class _FailedPlugin:
        name = "failed_extension"

        def __init__(self, module_name: str):
            self.module_name = module_name
            self.unload = AsyncMock()

        def depends_on(self):
            return []

        async def post_init(self, **_kwargs):
            app.router.routes.append("failed-route")
            broker.broadcast_types.add("failed-type")
            raise RuntimeError("post_init failed")

    def _load_module(module_name: str, _path: str):
        module_names.append(module_name)
        module = ModuleType(module_name)
        module.Plugin = lambda _path, name, **_kwargs: _FailedPlugin(name)
        sys.modules[module_name] = module
        return module

    config = SimpleNamespace(
        plugin_disabled=lambda: [],
        plugin_cache_enabled=lambda: False,
    )
    process = SimpleNamespace(is_main_process=lambda: True)
    monkeypatch.setattr(
        GulpPluginBase,
        "path_from_plugin",
        staticmethod(lambda *_args, **_kwargs: "/tmp/failed_extension.py"),
    )
    monkeypatch.setattr(
        plugin_module.muty.dynload,
        "load_dynamic_module_from_file",
        _load_module,
    )
    monkeypatch.setattr(
        plugin_module.GulpConfig,
        "get_instance",
        staticmethod(lambda: config),
    )
    monkeypatch.setattr(
        plugin_module.GulpProcess,
        "get_instance",
        staticmethod(lambda: process),
    )
    monkeypatch.setattr(
        server_api.GulpServer,
        "get_instance",
        staticmethod(lambda: SimpleNamespace(_app=app)),
    )
    monkeypatch.setattr(
        plugin_module.GulpRedisBroker,
        "get_instance",
        staticmethod(lambda: broker),
    )
    monkeypatch.setattr(plugin_module.events, "drain", AsyncMock())

    with pytest.raises(RuntimeError, match="post_init failed"):
        await GulpPluginBase.load(
            "failed_extension",
            extension=True,
            cache_mode=GulpPluginCacheMode.IGNORE,
        )

    print("failed plugin module:", module_names[0])
    assert module_names[0] not in sys.modules
    assert app.router.routes == ["core-route"]
    assert broker.broadcast_types == {"core-type"}


@pytest.mark.unit
@pytest.mark.asyncio
async def test_extension_metadata_load_skips_runtime_initialization(monkeypatch):
    module_names: list[str] = []
    plugin = SimpleNamespace(
        name="metadata_extension",
        depends_on=lambda: [],
        post_init=AsyncMock(),
    )

    def _load_module(module_name: str, _path: str):
        module_names.append(module_name)
        module = ModuleType(module_name)
        module.Plugin = lambda *_args, **_kwargs: plugin
        sys.modules[module_name] = module
        return module

    monkeypatch.setattr(
        GulpPluginBase,
        "path_from_plugin",
        staticmethod(lambda *_args, **_kwargs: "/tmp/metadata_extension.py"),
    )
    monkeypatch.setattr(
        plugin_module.muty.dynload,
        "load_dynamic_module_from_file",
        _load_module,
    )
    monkeypatch.setattr(
        plugin_module.GulpConfig,
        "get_instance",
        staticmethod(
            lambda: SimpleNamespace(
                plugin_disabled=lambda: [],
                plugin_cache_enabled=lambda: False,
            )
        ),
    )

    loaded = await GulpPluginBase.load(
        "metadata_extension",
        extension=True,
        cache_mode=GulpPluginCacheMode.IGNORE,
        run_post_init=False,
    )

    print("metadata extension post_init calls:", plugin.post_init.await_count)
    assert loaded is plugin
    plugin.post_init.assert_not_awaited()
    sys.modules.pop(module_names[0], None)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_filtered_plugin_listing_unloads_without_post_init(monkeypatch):
    listed_plugin = SimpleNamespace(name="other", unload=AsyncMock())
    load = AsyncMock(return_value=listed_plugin)
    monkeypatch.setattr(GulpPluginBase, "load", load)
    monkeypatch.setattr(
        plugin_module.muty.file,
        "list_directory_async",
        AsyncMock(side_effect=[["/tmp/other.py"], [], [], []]),
    )
    monkeypatch.setattr(
        plugin_module.GulpConfig,
        "get_instance",
        staticmethod(
            lambda: SimpleNamespace(
                path_plugins_extra=lambda: "/tmp/extra",
                path_plugins_default=lambda: "/tmp/default",
            )
        ),
    )

    result = await GulpPluginBase.list_plugins(name="wanted")

    print("filtered plugin listing result:", result)
    assert result == []
    load.assert_awaited_once_with(
        "/tmp/other.py",
        extension=False,
        cache_mode=GulpPluginCacheMode.IGNORE,
        run_post_init=False,
    )
    listed_plugin.unload.assert_awaited_once_with()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_direct_plugin_load_unloads_after_initialization_failure(monkeypatch):
    class UpperPlugin(GulpPluginBase):
        def display_name(self):
            return "upper"

        def type(self):
            return GulpPluginType.INGESTION

    lower = SimpleNamespace(
        _initialize=AsyncMock(side_effect=RuntimeError("initialize failed")),
        unload=AsyncMock(),
    )
    monkeypatch.setattr(GulpPluginBase, "load", AsyncMock(return_value=lower))
    upper = UpperPlugin("/tmp/upper.py", "upper")

    with pytest.raises(RuntimeError, match="initialize failed"):
        await upper.load_plugin_direct("lower", sess=None, stats=None)

    print("direct plugin unloaded after initialization failure")
    lower.unload.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_parent_unload_releases_stacked_plugin_after_failure(monkeypatch):
    class UpperPlugin(GulpPluginBase):
        def display_name(self):
            return "upper"

        def type(self):
            return GulpPluginType.INGESTION

    lower_module = "gulp.plugins.lower-test"
    upper_module = "gulp.plugins.upper-test"
    lower = SimpleNamespace(module_name=lower_module, unload=AsyncMock())
    upper = UpperPlugin("/tmp/upper.py", upper_module)
    upper._lower_instance = lower
    monkeypatch.setitem(sys.modules, lower_module, ModuleType(lower_module))
    monkeypatch.setitem(sys.modules, upper_module, ModuleType(upper_module))
    monkeypatch.setattr(
        plugin_module.GulpProcess,
        "get_instance",
        staticmethod(lambda: SimpleNamespace(is_main_process=lambda: False)),
    )

    await upper.unload()

    print("stacked plugin unloaded with parent")
    lower.unload.assert_awaited_once()
    assert upper._lower_instance is None
    assert upper_module not in sys.modules


@pytest.mark.unit
@pytest.mark.asyncio
async def test_final_stats_failure_still_unloads_plugin():
    class Plugin(GulpPluginBase):
        def display_name(self):
            return "plugin"

        def type(self):
            return GulpPluginType.INGESTION

    plugin = Plugin("/tmp/plugin.py", "plugin")
    plugin.update_final_stats_and_flush = AsyncMock(
        side_effect=RuntimeError("stats failed")
    )
    plugin.unload = AsyncMock()

    with pytest.raises(RuntimeError, match="stats failed"):
        await plugin.update_final_stats_and_unload()

    print("plugin unloaded after final stats failure")
    plugin.unload.assert_awaited_once_with()
