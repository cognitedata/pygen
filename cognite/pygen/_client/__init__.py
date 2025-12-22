"""Pygen v2 Client Core.

This module provides the internal HTTP client for interacting with
CDF Data Modeling API. This client is used internally by Pygen for
fetching data models and is not intended for direct use by end users.
"""

from cognite.pygen._client.auth import Credentials, OAuth2ClientCredentials
from cognite.pygen._client.config import PygenClientConfig
from cognite.pygen._client.core import PygenClient
from cognite.pygen._client.resources import ContainersAPI, DataModelsAPI, Page, SpacesAPI, ViewsAPI

__all__ = [
    "ContainersAPI",
    "Credentials",
    "DataModelsAPI",
    "OAuth2ClientCredentials",
    "Page",
    "PygenClient",
    "PygenClientConfig",
    "SpacesAPI",
    "ViewsAPI",
]
