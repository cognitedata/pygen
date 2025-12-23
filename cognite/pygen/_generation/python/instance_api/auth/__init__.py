"""Authentication support for Pygen client."""

from cognite.pygen._generation.python.instance_api.auth.credentials import Credentials
from cognite.pygen._generation.python.instance_api.auth.oauth2 import OAuth2ClientCredentials

__all__ = ["Credentials", "OAuth2ClientCredentials"]
