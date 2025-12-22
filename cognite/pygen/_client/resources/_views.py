"""Views API resource client.

This module provides the ViewsAPI class for managing CDF views.
"""

from collections.abc import Iterator, Sequence

from cognite.pygen._client.http_client import HTTPClient
from cognite.pygen._client.models import ViewReference, ViewRequest, ViewResponse

from ._base import BaseResourceAPI, Page, ResourceLimits


class ViewsAPI:
    """API client for CDF View resources.

    Views define the structure and properties that can be queried on nodes and edges.
    They provide a schema layer on top of containers.

    Example:
        >>> from cognite.pygen._client import PygenClient
        >>> client = PygenClient(config)
        >>> # List all views
        >>> for view in client.views.list():
        ...     print(f"{view.space}:{view.external_id}")
        >>> # Retrieve specific views
        >>> ref = ViewReference(space="my_space", external_id="my_view", version="v1")
        >>> views = client.views.retrieve([ref])
    """

    def __init__(self, http_client: HTTPClient, limits: ResourceLimits | None = None) -> None:
        """Initialize the Views API.

        Args:
            http_client: The HTTP client to use for API requests.
            limits: Configuration for API endpoint limits. Uses defaults if not provided.
        """
        self._api = BaseResourceAPI[ViewReference, ViewResponse](
            http_client=http_client,
            endpoint="/models/views",
            reference_cls=ViewReference,
            request_cls=ViewRequest,
            response_cls=ViewResponse,
            limits=limits,
        )

    def create(self, items: Sequence[ViewRequest]) -> list[ViewResponse]:
        """Create or update views.

        Args:
            items: A sequence of request objects defining the views to create/update.

        Returns:
            A list of the created/updated view objects.
        """
        return self._api.create(items)

    def retrieve(self, references: Sequence[ViewReference]) -> list[ViewResponse]:
        """Retrieve specific views by their references.

        Args:
            references: A sequence of reference objects identifying the views to retrieve.

        Returns:
            A list of view objects. Views that don't exist are not included.
        """
        return self._api.retrieve(references)

    def delete(self, references: Sequence[ViewReference]) -> list[ViewReference]:
        """Delete views by their references.

        Args:
            references: A sequence of reference objects identifying the views to delete.

        Returns:
            A list of references to the deleted views.
        """
        return self._api.delete(references)

    def iterate(
        self,
        params: dict | None = None,
    ) -> Page[ViewResponse]:
        """Fetch a single page of views.

        Args:
            params: Query parameters for the request. Supported parameters:
                - cursor: Cursor for pagination
                - limit: Maximum number of items per page (default: 1000)
                - space: Filter by space
                - includeGlobal: Whether to include global views (default: False)
                - allVersions: Whether to return all versions (default: False)
                - includeInheritedProperties: Whether to include inherited properties (default: True)

        Returns:
            A Page containing the views and the cursor for the next page.
        """
        return self._api.iterate(params)

    def list(
        self,
        params: dict | None = None,
    ) -> Iterator[ViewResponse]:
        """List all views, handling pagination automatically.

        This method lazily iterates over all views, fetching pages as needed.

        Args:
            params: Query parameters for the request. Supported parameters:
                - space: Filter by space
                - includeGlobal: Whether to include global views (default: False)
                - limit: Maximum total number of items to return
                - allVersions: Whether to return all versions (default: False)
                - includeInheritedProperties: Whether to include inherited properties (default: True)

        Yields:
            ViewResponse objects from the API.
        """
        return self._api.list(params)
