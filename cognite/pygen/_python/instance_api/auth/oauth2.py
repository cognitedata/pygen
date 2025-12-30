"""OAuth2 authentication support."""

from datetime import datetime, timedelta
from threading import Lock

import httpx
from pydantic import BaseModel, ValidationError

from cognite.pygen._python.instance_api.auth.credentials import Credentials
from cognite.pygen._python.instance_api.exceptions import OAuth2Error


class _TokenResponse(BaseModel):
    access_token: str
    expires_in: int = 3600


class OAuth2ClientCredentials(Credentials):
    """
    OAuth2 Client Credentials flow authentication.

    This implements the OAuth2 Client Credentials grant type (RFC 6749 Section 4.4).
    It automatically handles token refresh when tokens expire.

    Args:
        token_url: OAuth2 token endpoint URL
        client_id: OAuth2 client ID
        client_secret: OAuth2 client secret
        scopes: List of OAuth2 scopes to request
        audience: Optional audience parameter for the token request
        refresh_margin: Time in seconds before expiry to refresh the token (default: 300)

    """

    def __init__(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        scopes: list[str] | None = None,
        audience: str | None = None,
        refresh_margin: int = 300,
    ) -> None:
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.scopes = scopes or []
        self.audience = audience
        self.refresh_margin = refresh_margin

        # Token state
        self._access_token: str | None = None
        self._token_expiry: datetime | None = None
        self._lock = Lock()

        # HTTP client for token requests
        self._http_client = httpx.Client(timeout=30.0)

    def authorization_header(self) -> tuple[str, str]:
        """
        Get authentication headers with a valid access token.

        Automatically refreshes the token if needed before returning headers.

        Returns:
            Dictionary containing the Authorization header with Bearer token.

        Raises:
            OAuth2Error: If token acquisition fails.
        """
        self.refresh_if_needed()
        if not self._access_token:
            raise OAuth2Error("Failed to acquire access token")
        return "Authorization", f"Bearer {self._access_token}"

    def refresh_if_needed(self) -> None:
        """
        Refresh the access token if it is expired or about to expire.

        This method is thread-safe and will only refresh once if called concurrently.
        """
        with self._lock:
            if self._needs_refresh():
                self._refresh_token()

    def _needs_refresh(self) -> bool:
        """Check if token needs to be refreshed."""
        if self._access_token is None or self._token_expiry is None:
            return True

        # Refresh if we're within the refresh margin of expiry
        time_until_expiry = (self._token_expiry - datetime.now()).total_seconds()
        return time_until_expiry < self.refresh_margin

    def _refresh_token(self) -> None:
        """
        Refresh the access token using OAuth2 client credentials flow.

        Raises:
            OAuth2Error: If token refresh fails.
        """
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        if self.scopes:
            data["scope"] = " ".join(self.scopes)
        if self.audience:
            data["audience"] = self.audience

        try:
            response = self._http_client.post(self.token_url, data=data)
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise OAuth2Error(f"Token request failed with status {e.response.status_code}: {e.response.text}") from e
        except httpx.RequestError as e:
            raise OAuth2Error(f"Token request failed: {e}") from e

        try:
            token_data = _TokenResponse.model_validate_json(response.text)
        except ValidationError as e:
            raise OAuth2Error(f"Invalid token response from server: {e}") from e
        self._access_token = token_data.access_token

        self._token_expiry = datetime.now() + timedelta(seconds=token_data.expires_in)

    def close(self) -> None:
        """Close the HTTP client used for token requests."""
        self._http_client.close()

    def __enter__(self) -> "OAuth2ClientCredentials":
        """Context manager support."""
        return self

    def __exit__(self, *args) -> None:
        """Context manager support - closes HTTP client."""
        self.close()
