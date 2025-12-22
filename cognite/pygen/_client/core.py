"""Pygen Client Core.

This module provides the main PygenClient class for interacting with
CDF Data Modeling API.
"""

import sys
from typing import Literal

from cognite.pygen._client.config import PygenClientConfig
from cognite.pygen._client.http_client import HTTPClient
from cognite.pygen._client.resources import ContainersAPI, DataModelsAPI, SpacesAPI, ViewsAPI

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

    Example:
        >>> from cognite.pygen._client import PygenClient
        >>> from cognite.pygen._client.config import PygenClientConfig
        >>> from cognite.pygen._client.auth import OAuth2ClientCredentials
        >>>
        >>> credentials = OAuth2ClientCredentials(
        ...     token_url="https://login.microsoftonline.com/tenant/oauth2/v2.0/token",
        ...     client_id="your-client-id",
        ...     client_secret="your-secret",
        ...     scopes=["https://bluefield.cognitedata.com/.default"],
        ... )
        >>> config = PygenClientConfig(
        ...     cdf_url="https://bluefield.cognitedata.com",
        ...     project="my-project",
        ...     credentials=credentials,
        ... )
        >>> with PygenClient(config) as client:
        ...     # List all spaces
        ...     for space in client.spaces.list():
        ...         print(space.space)
        ...
        ...     # Retrieve a specific data model
        ...     from cognite.pygen._client.models import DataModelReference
        ...     ref = DataModelReference(space="my_space", external_id="my_model", version="v1")
        ...     models = client.data_models.retrieve([ref], inline_views=True)

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
        self._config = config
        self._http_client = HTTPClient(config, max_retries=max_retries)

        # Initialize resource APIs
        self._spaces = SpacesAPI(self._http_client)
        self._data_models = DataModelsAPI(self._http_client)
        self._views = ViewsAPI(self._http_client)
        self._containers = ContainersAPI(self._http_client)

    @property
    def spaces(self) -> SpacesAPI:
        """API for managing CDF spaces."""
        return self._spaces

    @property
    def data_models(self) -> DataModelsAPI:
        """API for managing CDF data models."""
        return self._data_models

    @property
    def views(self) -> ViewsAPI:
        """API for managing CDF views."""
        return self._views

    @property
    def containers(self) -> ContainersAPI:
        """API for managing CDF containers."""
        return self._containers

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
