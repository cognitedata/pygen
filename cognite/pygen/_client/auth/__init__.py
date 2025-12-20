"""Authentication support for Pygen client."""

from cognite.pygen._client.auth.credentials import Credentials
from cognite.pygen._client.auth.oauth2 import OAuth2ClientCredentials, OAuth2Error

__all__ = ["Credentials", "OAuth2ClientCredentials", "OAuth2Error"]
