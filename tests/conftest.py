"""Pytest configuration and fixtures."""

import importlib.util
import os
import sys
from copy import deepcopy
from pathlib import Path
from types import ModuleType
import pytest
import httpx
from unittest.mock import AsyncMock, MagicMock


@pytest.fixture(scope="session", params=["basic", "pro"])
def ai_assistant_module(request: pytest.FixtureRequest) -> ModuleType:
    """Load assistant source once; paid-plugin coverage is optional."""
    if request.param == "basic":
        from gulp.plugins.extension import ai_assistant

        return ai_assistant

    path = (
        Path(__file__).parents[1]
        / "gulp-paid-plugins/src/gulp-paid-plugins/plugins/extension/ai_assistant_pro.py"
    )
    if not path.is_file():
        pytest.skip("gulp-paid-plugins checkout is not available")
    name = "_gulp_test_ai_assistant_pro_configuration"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def ai_configuration(ai_assistant_module: ModuleType) -> dict:
    """Return independent nested settings with fake credentials."""
    configuration = deepcopy(ai_assistant_module._DEFAULT_CONFIGURATION)
    values = {
        "default_model": "test-model",
        "api_base_url": "http://127.0.0.1:1",
        "openrouter_key": "test-secret",
    }
    for parameter in configuration["configuration"]:
        parameter["default_value"] = values[parameter["name"]]
    return configuration


# Mark test types
def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "unit: Unit tests (no external dependencies)")
    config.addinivalue_line("markers", "integration: Integration tests (requires live Gulp server)")
    config.addinivalue_line("markers", "e2e: End-to-end tests")


@pytest.fixture
def gulp_base_url() -> str:
    """Gulp server base URL from environment or default."""
    return os.getenv("GULP_BASE_URL", "http://localhost:8080")


@pytest.fixture
def gulp_token() -> str:
    """Test token from environment or default."""
    return os.getenv("GULP_TEST_TOKEN", "")


@pytest.fixture
def gulp_test_user() -> str:
    """Integration username/user_id for login tests."""
    return os.getenv("GULP_TEST_USER", "admin")


@pytest.fixture
def gulp_test_password() -> str:
    """Integration password for login tests."""
    return os.getenv("GULP_TEST_PASSWORD", "admin")


@pytest.fixture
async def mock_http_client() -> AsyncMock:
    """Mock httpx.AsyncClient for testing."""
    return AsyncMock(spec=httpx.AsyncClient)


@pytest.fixture
def jsend_success_response():
    """Fixture: JSend success response template."""
    return {
        "status": "success",
        "req_id": "test-req-id",
        "timestamp_msec": 1234567890,
        "data": {},
    }


@pytest.fixture
def jsend_error_response():
    """Fixture: JSend error response template."""
    return {
        "status": "error",
        "req_id": "test-req-id",
        "timestamp_msec": 1234567890,
        "data": "Error message",
    }
