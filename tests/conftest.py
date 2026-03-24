"""Pytest configuration and fixtures."""

import os
import pytest
import httpx
from unittest.mock import AsyncMock, MagicMock

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
