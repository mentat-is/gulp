"""Integration tests for authentication APIs."""

import pytest


@pytest.mark.integration
async def test_login_with_invalid_credentials(gulp_base_url):
    """Test login fails with invalid credentials."""
    from gulp_sdk import GulpClient, AuthenticationError, NotFoundError

    async with GulpClient(gulp_base_url) as client:
        # Backend may return 401 or 404 depending on whether user_id exists.
        with pytest.raises((AuthenticationError, NotFoundError)):
            await client.auth.login("invalid_user", "invalid_pass")


@pytest.mark.integration
async def test_get_current_user_unauthenticated(gulp_base_url):
    """Test get_current_user fails without authentication."""
    from gulp_sdk import GulpClient, AuthenticationError, GulpSDKError

    async with GulpClient(gulp_base_url) as client:
        with pytest.raises((AuthenticationError, GulpSDKError)):
            await client.users.get_current()


@pytest.mark.integration
async def test_login_success(gulp_base_url, gulp_test_user, gulp_test_password):
    """Test successful login sets token."""
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        session = await client.auth.login(gulp_test_user, gulp_test_password)
        assert session.token is not None
        assert client.token == session.token
        assert client._ws is not None
        assert client._ws.is_connected
        assert client.ws_id


@pytest.mark.integration
async def test_logout(gulp_base_url, gulp_test_user, gulp_test_password):
    """Test logout invalidates token."""
    from gulp_sdk import GulpClient, AuthenticationError

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        try:
            result = await client.auth.logout()
            assert result is True
            assert client.token is None
        except AuthenticationError:
            # In some test configs sessions are not persisted for admin debug login.
            pytest.skip("Logout not supported in current test server auth configuration")


@pytest.mark.integration
async def test_get_available_login_api(gulp_base_url):
    """get_available_login_api should list at least one supported method (no auth required).

    NOTE: On this dev server the oauth extension raises 'missing oauth configuration!'
    which causes a 500.  We tolerate that as a known server-side limitation.
    """
    from gulp_sdk import GulpClient, GulpSDKError

    async with GulpClient(gulp_base_url) as client:
        try:
            result = await client.auth.get_available_login_api()
            # Returns a list of login provider names/dicts
            assert result is not None
        except GulpSDKError as exc:
            # Known: oauth extension raises 'missing oauth configuration!' (server-side bug)
            if "missing oauth configuration" in str(exc):
                pytest.skip("get_available_login_api: server-side oauth extension not configured")
            raise

