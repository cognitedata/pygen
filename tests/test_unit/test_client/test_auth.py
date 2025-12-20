"""Tests for OAuth2 authentication."""

from datetime import datetime, timedelta

import httpx
import pytest
import respx

from cognite.pygen._client.auth import OAuth2ClientCredentials
from cognite.pygen._client.exceptions import OAuth2Error


class TestOAuth2ClientCredentials:
    """Tests for OAuth2ClientCredentials class."""

    @pytest.fixture
    def token_url(self) -> str:
        """Token endpoint URL."""
        return "https://example.com/oauth2/token"

    @pytest.fixture
    def mock_token_response(self) -> dict[str, str | int]:
        """Mock successful token response."""
        return {
            "access_token": "mock_access_token_12345",
            "token_type": "Bearer",
            "expires_in": 3600,
        }

    @pytest.fixture
    def credentials(self, token_url: str) -> OAuth2ClientCredentials:
        """Create OAuth2ClientCredentials instance."""
        return OAuth2ClientCredentials(
            token_url=token_url,
            client_id="test_client_id",
            client_secret="test_client_secret",
            scopes=["https://api.example.com/.default"],
            audience="https://api.example.com",
        )

    @respx.mock
    def test_get_headers_success(
        self, credentials: OAuth2ClientCredentials, token_url: str, mock_token_response: dict[str, str | int]
    ) -> None:
        """Test successful token acquisition and header generation."""
        route = respx.post(token_url).mock(return_value=httpx.Response(200, json=mock_token_response))

        header_key, header_value = credentials.authorization_header()

        assert header_key == "Authorization"
        assert header_value == "Bearer mock_access_token_12345"
        assert route.called

        # Verify the request payload
        request = route.calls.last.request
        # Parse form data from request body
        body = request.content.decode("utf-8")
        assert "grant_type=client_credentials" in body
        assert "client_id=test_client_id" in body
        assert "client_secret=test_client_secret" in body
        assert "scope=https%3A%2F%2Fapi.example.com%2F.default" in body
        assert "audience=https%3A%2F%2Fapi.example.com" in body

    @respx.mock
    def test_get_headers_without_optional_params(
        self, token_url: str, mock_token_response: dict[str, str | int]
    ) -> None:
        """Test token acquisition without optional parameters."""
        credentials = OAuth2ClientCredentials(
            token_url=token_url,
            client_id="test_client_id",
            client_secret="test_client_secret",
        )

        route = respx.post(token_url).mock(return_value=httpx.Response(200, json=mock_token_response))

        header_key, header_value = credentials.authorization_header()

        assert header_key == "Authorization"
        assert header_value == "Bearer mock_access_token_12345"

        # Verify the request payload doesn't include scope or audience
        request = route.calls.last.request
        body = request.content.decode("utf-8")
        assert "scope=" not in body
        assert "audience=" not in body

    @respx.mock
    def test_token_caching(
        self, credentials: OAuth2ClientCredentials, token_url: str, mock_token_response: dict[str, str | int]
    ) -> None:
        """Test that token is cached and not requested again if still valid."""
        route = respx.post(token_url).mock(return_value=httpx.Response(200, json=mock_token_response))

        # First call should request token
        header1 = credentials.authorization_header()
        assert route.call_count == 1

        # Second call should use cached token
        header2 = credentials.authorization_header()
        assert route.call_count == 1  # Still only one call
        assert header1 == header2

    @respx.mock
    def test_token_refresh_when_expired(
        self, credentials: OAuth2ClientCredentials, token_url: str, mock_token_response: dict[str, str | int]
    ) -> None:
        """Test that token is refreshed when it expires."""
        route = respx.post(token_url).mock(return_value=httpx.Response(200, json=mock_token_response))

        # First call
        credentials.authorization_header()
        assert route.call_count == 1

        # Manually expire the token
        credentials._token_expiry = datetime.now() - timedelta(seconds=1)

        # Second call should refresh
        credentials.authorization_header()
        assert route.call_count == 2

    @respx.mock
    def test_token_refresh_within_margin(
        self, credentials: OAuth2ClientCredentials, token_url: str, mock_token_response: dict[str, str | int]
    ) -> None:
        """Test that token is refreshed when within refresh margin."""
        route = respx.post(token_url).mock(return_value=httpx.Response(200, json=mock_token_response))

        # Set refresh margin to 600 seconds (10 minutes)
        credentials.refresh_margin = 600

        # First call
        credentials.authorization_header()
        assert route.call_count == 1

        # Set token to expire in 500 seconds (within margin)
        credentials._token_expiry = datetime.now() + timedelta(seconds=500)

        # Second call should refresh
        credentials.authorization_header()
        assert route.call_count == 2

    @respx.mock
    def test_http_error(self, credentials: OAuth2ClientCredentials, token_url: str) -> None:
        """Test handling of HTTP errors during token request."""
        respx.post(token_url).mock(return_value=httpx.Response(401, text="Unauthorized"))

        with pytest.raises(OAuth2Error) as exc_info:
            credentials.authorization_header()

        assert "Token request failed with status 401" in str(exc_info.value)
        assert "Unauthorized" in str(exc_info.value)

    @respx.mock
    def test_request_error(self, credentials: OAuth2ClientCredentials, token_url: str) -> None:
        """Test handling of request errors during token request."""
        respx.post(token_url).mock(side_effect=httpx.ConnectError("Connection failed"))

        with pytest.raises(OAuth2Error) as exc_info:
            credentials.authorization_header()

        assert "Token request failed" in str(exc_info.value)

    @respx.mock
    def test_missing_access_token(self, credentials: OAuth2ClientCredentials, token_url: str) -> None:
        """Test handling of response missing access_token field."""
        respx.post(token_url).mock(
            return_value=httpx.Response(200, json={"token_type": "Bearer"})  # Missing access_token
        )

        with pytest.raises(OAuth2Error) as exc_info:
            credentials.authorization_header()

        assert "missing access_token field" in str(exc_info.value)

    @respx.mock
    def test_default_expiry(self, credentials: OAuth2ClientCredentials, token_url: str) -> None:
        """Test that default expiry is used when expires_in is missing."""
        respx.post(token_url).mock(
            return_value=httpx.Response(
                200,
                json={
                    "access_token": "test_token",
                    "token_type": "Bearer",
                    # Missing expires_in
                },
            )
        )

        credentials.authorization_header()

        # Should have set expiry to default (1 hour from now)
        assert credentials._token_expiry is not None
        time_until_expiry = (credentials._token_expiry - datetime.now()).total_seconds()
        assert 3590 < time_until_expiry < 3610  # Around 1 hour (with small tolerance)

    @respx.mock
    def test_multiple_scopes(self, token_url: str, mock_token_response: dict[str, str | int]) -> None:
        """Test token request with multiple scopes."""
        credentials = OAuth2ClientCredentials(
            token_url=token_url,
            client_id="test_client_id",
            client_secret="test_client_secret",
            scopes=["scope1", "scope2", "scope3"],
        )

        route = respx.post(token_url).mock(return_value=httpx.Response(200, json=mock_token_response))

        credentials.authorization_header()

        # Verify scopes are joined with spaces
        request = route.calls.last.request
        body = request.content.decode("utf-8")
        assert "scope=scope1+scope2+scope3" in body

    @respx.mock
    def test_context_manager(self, token_url: str, mock_token_response: dict[str, str | int]) -> None:
        """Test context manager usage."""
        respx.post(token_url).mock(return_value=httpx.Response(200, json=mock_token_response))

        with OAuth2ClientCredentials(
            token_url=token_url,
            client_id="test_client_id",
            client_secret="test_client_secret",
        ) as credentials:
            header_key, header_value = credentials.authorization_header()
            assert header_key == "Authorization"
            assert header_value == "Bearer mock_access_token_12345"

        # Client should be closed after context exit
        assert credentials._http_client.is_closed

    @respx.mock
    def test_thread_safety(
        self, credentials: OAuth2ClientCredentials, token_url: str, mock_token_response: dict[str, str | int]
    ) -> None:
        """Test that token refresh is thread-safe."""
        route = respx.post(token_url).mock(return_value=httpx.Response(200, json=mock_token_response))

        # Simulate concurrent access
        import threading

        results: list[tuple[str, str]] = []

        def get_token() -> None:
            results.append(credentials.authorization_header())

        threads = [threading.Thread(target=get_token) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All threads should get the same token
        assert len(results) == 10
        assert all(r == results[0] for r in results)

        # Token should only be requested once (or a few times due to race conditions)
        # but definitely not 10 times
        assert route.call_count < 5

    @respx.mock
    def test_refresh_if_needed_explicit(
        self, credentials: OAuth2ClientCredentials, token_url: str, mock_token_response: dict[str, str | int]
    ) -> None:
        """Test explicit refresh_if_needed call."""
        route = respx.post(token_url).mock(return_value=httpx.Response(200, json=mock_token_response))

        # Should refresh since no token exists
        credentials.refresh_if_needed()
        assert route.call_count == 1
        assert credentials._access_token == "mock_access_token_12345"

        # Should not refresh again
        credentials.refresh_if_needed()
        assert route.call_count == 1
