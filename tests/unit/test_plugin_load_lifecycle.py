"""Dynamic plugin load cleanup regressions."""

import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gulp import plugin as plugin_module
from gulp.api import server_api
from gulp.plugin import GulpPluginBase, GulpPluginCacheMode


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
