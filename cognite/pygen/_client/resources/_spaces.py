"""Spaces API resource client.

This module provides the SpacesAPI class for managing CDF spaces.
"""

from collections.abc import Iterator, Sequence

from cognite.pygen._client.http_client import HTTPClient
from cognite.pygen._client.models import SpaceReference, SpaceRequest, SpaceResponse

from ._base import BaseResourceAPI, Page, ResourceLimits


class SpacesAPI:
    """API client for CDF Space resources.

    Spaces are containers that organize data modeling resources like
    data models, views, and containers.

    Example:
        >>> from cognite.pygen._client import PygenClient
        >>> client = PygenClient(config)
        >>> # List all spaces
        >>> for space in client.spaces.list():
        ...     print(space.space)
        >>> # Retrieve specific spaces
        >>> spaces = client.spaces.retrieve([SpaceReference(space="my_space")])
        >>> # Create a space
        >>> created = client.spaces.create([SpaceRequest(space="new_space", name="New Space")])
        >>> # Delete a space
        >>> deleted = client.spaces.delete([SpaceReference(space="my_space")])
    """

    def __init__(self, http_client: HTTPClient, limits: ResourceLimits | None = None) -> None:
        """Initialize the Spaces API.

        Args:
            http_client: The HTTP client to use for API requests.
            limits: Configuration for API endpoint limits. Uses defaults if not provided.
        """
        self._api = BaseResourceAPI[SpaceReference, SpaceResponse](
            http_client=http_client,
            endpoint="/models/spaces",
            reference_cls=SpaceReference,
            request_cls=SpaceRequest,
            response_cls=SpaceResponse,
            limits=limits,
        )

    def create(self, items: Sequence[SpaceRequest]) -> list[SpaceResponse]:
        """Create or update spaces.

        Args:
            items: A sequence of request objects defining the spaces to create/update.

        Returns:
            A list of the created/updated space objects.
        """
        return self._api.create(items)

    def retrieve(self, references: Sequence[SpaceReference]) -> list[SpaceResponse]:
        """Retrieve specific spaces by their references.

        Args:
            references: A sequence of reference objects identifying the spaces to retrieve.

        Returns:
            A list of space objects. Spaces that don't exist are not included.
        """
        return self._api.retrieve(references)

    def delete(self, references: Sequence[SpaceReference]) -> list[SpaceReference]:
        """Delete spaces by their references.

        Args:
            references: A sequence of reference objects identifying the spaces to delete.

        Returns:
            A list of references to the deleted spaces.
        """
        return self._api.delete(references)

    def iterate(
        self,
        params: dict | None = None,
    ) -> Page[SpaceResponse]:
        """Fetch a single page of spaces.

        Args:
            params: Query parameters for the request. Supported parameters:
                - cursor: Cursor for pagination
                - limit: Maximum number of items per page (default: 1000)
                - includeGlobal: Whether to include global spaces (default: False)

        Returns:
            A Page containing the spaces and the cursor for the next page.
        """
        return self._api.iterate(params)

    def list(
        self,
        params: dict | None = None,
    ) -> Iterator[SpaceResponse]:
        """List all spaces, handling pagination automatically.

        This method lazily iterates over all spaces, fetching pages as needed.

        Args:
            params: Query parameters for the request. Supported parameters:
                - includeGlobal: Whether to include global spaces (default: False)
                - limit: Maximum total number of items to return

        Yields:
            SpaceResponse objects from the API.
        """
        return self._api.list(params)
