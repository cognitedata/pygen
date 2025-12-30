"""Pygen Client Core.

This module provides the main PygenClient class for interacting with
CDF Data Modeling API.
"""

import sys
from typing import Literal

from cognite.pygen._client.resources import ContainersAPI, DataModelsAPI, SpacesAPI, ViewsAPI
from cognite.pygen._python.instance_api.config import PygenClientConfig
from cognite.pygen._python.instance_api.http_client import HTTPClient

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class PygenClient:
    """Client for CDF Data Modeling API.

    This client provides access to data modeling resources like spaces,
    data models, views, and containers.

    The client is designed as an internal utility for Pygen and is not
    intended for direct use by end users.

    Attributes:
        spaces: API for managing spaces.
        data_models: API for managing data models.
        views: API for managing views.
        containers: API for managing containers.
    """

    def __init__(
        self,
        config: PygenClientConfig,
        max_retries: int = 10,
    ) -> None:
        """Initialize the Pygen client.

        Args:
            config: Configuration for the client including URL, project, and credentials.
            max_retries: Maximum number of retries for failed requests. Default is 10.
        """
        self.config = config
        self._http_client = HTTPClient(config, max_retries=max_retries)

        # Initialize resource APIs as attributes (not properties)
        self.spaces = SpacesAPI(self._http_client)
        self.data_models = DataModelsAPI(self._http_client)
        self.views = ViewsAPI(self._http_client)
        self.containers = ContainersAPI(self._http_client)

    def __enter__(self) -> Self:
        """Enter context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object | None,
    ) -> Literal[False]:
        """Exit context manager and close the HTTP client."""
        self.close()
        return False

    def close(self) -> None:
        """Close the client and release resources.

        This method should be called when the client is no longer needed
        to properly close HTTP connections.
        """
        self._http_client.session.close()
