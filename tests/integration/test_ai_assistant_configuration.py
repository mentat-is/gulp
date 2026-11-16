"""Read real DB settings and verify both assistants' outgoing HTTP requests."""

from copy import deepcopy
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from aiohttp import web
from sqlalchemy import update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from gulp.api.collab_api import GulpCollab
from gulp.config import GulpConfig


@pytest.mark.integration
@pytest.mark.parametrize("with_file_configuration", [True, False])
async def test_db_configuration_refresh_and_authorization(
    ai_assistant_module: ModuleType,
    ai_configuration: dict,
    gulp_base_url: str,
    monkeypatch: pytest.MonkeyPatch,
    with_file_configuration: bool,
) -> None:
    """DB updates reach the model, URL and Bearer header without live LLM calls."""
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{gulp_base_url}/docs")
        assert response.status_code == 200

    requests = []

    async def completion(request: web.Request) -> web.Response:
        payload = await request.json()
        requests.append((request.path, request.headers.get("Authorization"), payload))
        if payload["stream"]:
            return web.Response(
                text='data: {"choices":[{"delta":{"content":"verified"}}]}\n'
                "data: [DONE]\n",
                content_type="text/event-stream",
            )
        return web.json_response(
            {"choices": [{"message": {"role": "assistant", "content": "verified"}}]}
        )

    application = web.Application()
    application.router.add_post("/{version}/chat/completions", completion)
    runner = web.AppRunner(application)
    await runner.setup()
    config = GulpConfig.get_instance()
    engine = create_async_engine(config.postgres_url(), poolclass=NullPool)
    try:
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        base_url = f"http://127.0.0.1:{runner.addresses[0][1]}"
        file_settings = {
            "openrouter_key": "file-test-key",
            "default_model": "file-test-model",
            "api_base_url": f"{base_url}/file",
        }
        if with_file_configuration:
            monkeypatch.setitem(config.config(), "ai_configuration", file_settings)
        else:
            monkeypatch.delitem(config.config(), "ai_configuration", raising=False)
        object_id = f"test_ai_config_{uuid4().hex}"
        monkeypatch.setattr(ai_assistant_module, "_CONFIG_OBJECT_ID", object_id)
        broker = SimpleNamespace(put=AsyncMock())
        monkeypatch.setattr(
            ai_assistant_module.GulpRedisBroker, "get_instance", lambda: broker
        )
        GulpCollab.init_mappers()
        shared_object = ai_assistant_module.SharedObjectPlugin.GulpSharedObject
        plugin = object.__new__(ai_assistant_module.Plugin)

        async def assert_completion(version: str, key: str, model: str) -> None:
            """Verify the selected settings on a real local HTTP request."""
            if hasattr(plugin, "_send_message"):
                result = await plugin._send_message(
                    [ai_assistant_module.ChatMessage(role="user", content="test")],
                    model=plugin._default_model,
                )
                assert result["choices"][0]["message"]["content"] == "verified"
            else:
                broker.put.reset_mock()
                await plugin._call_ai(
                    operation_id="test_operation",
                    ws_id="test-websocket",
                    req_id=f"test-ai-{uuid4().hex}",
                    user_id="admin",
                    prompt="test",
                )
                assert [call.kwargs["t"] for call in broker.put.await_args_list] == [
                    "ai_assistant_stream",
                    "ai_assistant_done",
                ]
            path, authorization, payload = requests[-1]
            assert path == f"/{version}/chat/completions"
            assert authorization == f"Bearer {key}"
            assert payload["model"] == model
            assert payload["messages"][0]["content"] == "test"

        async with engine.connect() as connection:
            transaction = await connection.begin()
            try:
                sessions = async_sessionmaker(
                    connection,
                    expire_on_commit=False,
                    join_transaction_mode="create_savepoint",
                )
                monkeypatch.setattr(
                    GulpCollab,
                    "get_instance",
                    lambda: SimpleNamespace(session=sessions),
                )
                # Base never creates the missing row; Pro seeds only at startup.
                if hasattr(plugin, "_send_message"):
                    await plugin._refresh_configuration_from_db(create_if_missing=True)
                else:
                    await plugin._refresh_configuration_from_db()
                assert plugin._api_key == (
                    "file-test-key" if with_file_configuration else None
                )
                async with sessions() as session:
                    seeded = await shared_object.get_by_id(
                        session, object_id, throw_if_not_found=False
                    )
                    if hasattr(plugin, "_send_message"):
                        assert seeded is not None
                        values = {
                            parameter["name"]: parameter["default_value"]
                            for parameter in seeded.obj["configuration"]
                        }
                        assert values["openrouter_key"] == (
                            "file-test-key" if with_file_configuration else None
                        )
                        assert values["default_model"] == (
                            "file-test-model" if with_file_configuration else None
                        )
                        assert seeded.obj_type == "plugin_configuration"
                        assert seeded.is_private() is True
                    else:
                        assert seeded is None
                if with_file_configuration:
                    plugin._validate_configuration()
                    await assert_completion("file", "file-test-key", "file-test-model")
                if not hasattr(plugin, "_send_message"):
                    async with sessions() as session:
                        await shared_object.create_internal(
                            session,
                            user_id="admin",
                            obj_id=object_id,
                            name="AI test configuration",
                            obj=deepcopy(ai_configuration),
                            obj_type="plugin_configuration",
                            private=True,
                        )

                for version in ("v1", "v2"):
                    configuration = deepcopy(ai_configuration)
                    values = {
                        "default_model": f"model-{version}",
                        "openrouter_key": f"test-key-{version}",
                        "api_base_url": f"{base_url}/{version}",
                    }
                    for parameter in configuration["configuration"]:
                        parameter["default_value"] = values[parameter["name"]]
                    async with sessions() as session:
                        await session.execute(
                            update(shared_object)
                            .where(shared_object.id == object_id)
                            .values(obj=configuration)
                        )
                        await session.commit()

                    assert await plugin._refresh_configuration_from_db() is True
                    assert await plugin._refresh_configuration_from_db() is False
                    plugin._validate_configuration()
                    await assert_completion(
                        version, f"test-key-{version}", f"model-{version}"
                    )
                expected_requests = 3 if with_file_configuration else 2
                assert len(requests) == expected_requests

                # A DB row takes precedence even when its values are incomplete.
                async with sessions() as session:
                    await session.execute(
                        update(shared_object)
                        .where(shared_object.id == object_id)
                        .values(obj={"configuration": []})
                    )
                    await session.commit()
                await plugin._refresh_configuration_from_db()
                with pytest.raises(ValueError, match="ai_configuration/openrouter_key"):
                    plugin._validate_configuration()
                assert plugin._api_key is None
                assert len(requests) == expected_requests
            finally:
                # Even create_internal() commits stay inside this transaction.
                await transaction.rollback()
            async with connection.begin():
                async with sessions() as session:
                    assert (
                        await shared_object.get_by_id(
                            session, object_id, throw_if_not_found=False
                        )
                        is None
                    )
    finally:
        await engine.dispose()
        await runner.cleanup()
