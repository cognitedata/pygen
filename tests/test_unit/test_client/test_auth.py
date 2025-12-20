"""Tests for OAuth2 authentication."""

from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import httpx
import pytest

from cognite.pygen._client.auth import OAuth2ClientCredentials
from cognite.pygen._client.exceptions import OAuth2Error


class TestOAuth2ClientCredentials:
    """Tests for OAuth2ClientCredentials class."""

    @pytest.fixture
    def mock_token_response(self):
        """Mock successful token response."""
        return {
            "access_token": "mock_access_token_12345",
            "token_type": "Bearer",
            "expires_in": 3600,
        }

    @pytest.fixture
    def credentials(self):
        """Create OAuth2ClientCredentials instance."""
        return OAuth2ClientCredentials(
            token_url="https://example.com/oauth2/token",
            client_id="test_client_id",
            client_secret="test_client_secret",
            scopes=["https://api.example.com/.default"],
            audience="https://api.example.com",
        )

    @patch("httpx.Client.post")
    def test_get_headers_success(self, mock_post, credentials, mock_token_response):
        """Test successful token acquisition and header generation."""
        mock_response = Mock()
        mock_response.json.return_value = mock_token_response
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        headers = credentials.get_headers()

        assert headers == {"Authorization": "Bearer mock_access_token_12345"}
        mock_post.assert_called_once()

        # Verify the request payload
        call_args = mock_post.call_args
        assert call_args[0][0] == "https://example.com/oauth2/token"
        data = call_args[1]["data"]
        assert data["grant_type"] == "client_credentials"
        assert data["client_id"] == "test_client_id"
        assert data["client_secret"] == "test_client_secret"
        assert data["scope"] == "https://api.example.com/.default"
        assert data["audience"] == "https://api.example.com"

    @patch("httpx.Client.post")
    def test_get_headers_without_optional_params(self, mock_post, mock_token_response):
        """Test token acquisition without optional parameters."""
        credentials = OAuth2ClientCredentials(
            token_url="https://example.com/oauth2/token",
            client_id="test_client_id",
            client_secret="test_client_secret",
        )

        mock_response = Mock()
        mock_response.json.return_value = mock_token_response
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        headers = credentials.get_headers()

        assert headers == {"Authorization": "Bearer mock_access_token_12345"}

        # Verify the request payload doesn't include scope or audience
        call_args = mock_post.call_args
        data = call_args[1]["data"]
        assert "scope" not in data
        assert "audience" not in data

    @patch("httpx.Client.post")
    def test_token_caching(self, mock_post, credentials, mock_token_response):
        """Test that token is cached and not requested again if still valid."""
        mock_response = Mock()
        mock_response.json.return_value = mock_token_response
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        # First call should request token
        headers1 = credentials.get_headers()
        assert mock_post.call_count == 1

        # Second call should use cached token
        headers2 = credentials.get_headers()
        assert mock_post.call_count == 1  # Still only one call
        assert headers1 == headers2

    @patch("httpx.Client.post")
    def test_token_refresh_when_expired(self, mock_post, credentials, mock_token_response):
        """Test that token is refreshed when it expires."""
        mock_response = Mock()
        mock_response.json.return_value = mock_token_response
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        # First call
        credentials.get_headers()
        assert mock_post.call_count == 1

        # Manually expire the token
        credentials._token_expiry = datetime.now() - timedelta(seconds=1)

        # Second call should refresh
        credentials.get_headers()
        assert mock_post.call_count == 2

    @patch("httpx.Client.post")
    def test_token_refresh_within_margin(self, mock_post, credentials, mock_token_response):
        """Test that token is refreshed when within refresh margin."""
        mock_response = Mock()
        mock_response.json.return_value = mock_token_response
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        # Set refresh margin to 600 seconds (10 minutes)
        credentials.refresh_margin = 600

        # First call
        credentials.get_headers()
        assert mock_post.call_count == 1

        # Set token to expire in 500 seconds (within margin)
        credentials._token_expiry = datetime.now() + timedelta(seconds=500)

        # Second call should refresh
        credentials.get_headers()
        assert mock_post.call_count == 2

    @patch("httpx.Client.post")
    def test_http_error(self, mock_post, credentials):
        """Test handling of HTTP errors during token request."""
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        mock_post.return_value = mock_response
        mock_post.return_value.raise_for_status.side_effect = httpx.HTTPStatusError(
            "401 Client Error", request=Mock(), response=mock_response
        )

        with pytest.raises(OAuth2Error) as exc_info:
            credentials.get_headers()

        assert "Token request failed with status 401" in str(exc_info.value)
        assert "Unauthorized" in str(exc_info.value)

    @patch("httpx.Client.post")
    def test_request_error(self, mock_post, credentials):
        """Test handling of request errors during token request."""
        mock_post.side_effect = httpx.RequestError("Connection failed")

        with pytest.raises(OAuth2Error) as exc_info:
            credentials.get_headers()

        assert "Token request failed" in str(exc_info.value)

    @patch("httpx.Client.post")
    def test_missing_access_token(self, mock_post, credentials):
        """Test handling of response missing access_token field."""
        mock_response = Mock()
        mock_response.json.return_value = {"token_type": "Bearer"}  # Missing access_token
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        with pytest.raises(OAuth2Error) as exc_info:
            credentials.get_headers()

        assert "missing access_token field" in str(exc_info.value)

    @patch("httpx.Client.post")
    def test_default_expiry(self, mock_post, credentials):
        """Test that default expiry is used when expires_in is missing."""
        mock_response = Mock()
        mock_response.json.return_value = {
            "access_token": "test_token",
            "token_type": "Bearer",
            # Missing expires_in
        }
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        credentials.get_headers()

        # Should have set expiry to default (1 hour from now)
        assert credentials._token_expiry is not None
        time_until_expiry = (credentials._token_expiry - datetime.now()).total_seconds()
        assert 3590 < time_until_expiry < 3610  # Around 1 hour (with small tolerance)

    @patch("httpx.Client.post")
    def test_multiple_scopes(self, mock_post, mock_token_response):
        """Test token request with multiple scopes."""
        credentials = OAuth2ClientCredentials(
            token_url="https://example.com/oauth2/token",
            client_id="test_client_id",
            client_secret="test_client_secret",
            scopes=["scope1", "scope2", "scope3"],
        )

        mock_response = Mock()
        mock_response.json.return_value = mock_token_response
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        credentials.get_headers()

        # Verify scopes are joined with spaces
        call_args = mock_post.call_args
        data = call_args[1]["data"]
        assert data["scope"] == "scope1 scope2 scope3"

    @patch("httpx.Client.post")
    def test_context_manager(self, mock_post, mock_token_response):
        """Test context manager usage."""
        mock_response = Mock()
        mock_response.json.return_value = mock_token_response
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        with OAuth2ClientCredentials(
            token_url="https://example.com/oauth2/token",
            client_id="test_client_id",
            client_secret="test_client_secret",
        ) as credentials:
            headers = credentials.get_headers()
            assert headers == {"Authorization": "Bearer mock_access_token_12345"}

        # Client should be closed after context exit
        assert credentials._http_client.is_closed

    @patch("httpx.Client.post")
    def test_thread_safety(self, mock_post, credentials, mock_token_response):
        """Test that token refresh is thread-safe."""
        mock_response = Mock()
        mock_response.json.return_value = mock_token_response
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        # Simulate concurrent access
        import threading

        results = []

        def get_token():
            results.append(credentials.get_headers())

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
        assert mock_post.call_count < 5

    def test_close(self, credentials):
        """Test close method."""
        # Client should be open initially
        assert not credentials._http_client.is_closed

        credentials.close()

        # Client should be closed after calling close
        assert credentials._http_client.is_closed

    @patch("httpx.Client.post")
    def test_refresh_if_needed_explicit(self, mock_post, credentials, mock_token_response):
        """Test explicit refresh_if_needed call."""
        mock_response = Mock()
        mock_response.json.return_value = mock_token_response
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        # Should refresh since no token exists
        credentials.refresh_if_needed()
        assert mock_post.call_count == 1
        assert credentials._access_token == "mock_access_token_12345"

        # Should not refresh again
        credentials.refresh_if_needed()
        assert mock_post.call_count == 1
