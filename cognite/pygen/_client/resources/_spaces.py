"""Spaces API resource client.

This module provides the SpacesAPI class for managing CDF spaces.
"""

from collections.abc import Sequence

from cognite.pygen._client.models import SpaceReference, SpaceRequest, SpaceResponse
from cognite.pygen._generation.python.instance_api.http_client import HTTPClient, SuccessResponse

from ._base import BaseResourceAPI, Page, ReferenceResponseItems, ResourceLimits


class SpacesAPI(BaseResourceAPI[SpaceReference, SpaceRequest, SpaceResponse]):
    """API client for CDF Space resources.

    Spaces are containers that organize data modeling resources like
    data models, views, and containers.

    """

    def __init__(self, http_client: HTTPClient, limits: ResourceLimits | None = None) -> None:
        """Initialize the Spaces API.

        Args:
            http_client: The HTTP client to use for API requests.
            limits: Configuration for API endpoint limits. Uses defaults if not provided.
        """
        super().__init__(http_client, "/models/spaces", limits)

    def _page_response(self, response: SuccessResponse) -> Page[SpaceResponse]:
        return Page[SpaceResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ReferenceResponseItems[SpaceReference]:
        return ReferenceResponseItems[SpaceReference].model_validate_json(response.body)

    def create(self, items: Sequence[SpaceRequest]) -> list[SpaceResponse]:
        """Create or update spaces.

        Args:
            items: A sequence of request objects defining the spaces to create/update.

        Returns:
            A list of the created/updated space objects.
        """
        return self._create(items)

    def retrieve(self, references: Sequence[SpaceReference]) -> list[SpaceResponse]:
        """Retrieve specific spaces by their references.

        Args:
            references: A sequence of reference objects identifying the spaces to retrieve.

        Returns:
            A list of space objects. Spaces that don't exist are not included.
        """
        return self._retrieve(references)

    def delete(self, references: Sequence[SpaceReference]) -> list[SpaceReference]:
        """Delete spaces by their references.

        Args:
            references: A sequence of reference objects identifying the spaces to delete.

        Returns:
            A list of references to the deleted spaces.
        """
        return self._delete(references)

    def iterate(
        self,
        include_global: bool = False,
        limit: int = 10,
        cursor: str | None = None,
    ) -> Page[SpaceResponse]:
        """Fetch a single page of spaces.

        Args:
            include_global: Whether to include global spaces (default: False)
            limit: Maximum number of items per page (default: 1000)
            cursor: Cursor for pagination (default: None)

        Returns:
            A Page containing the spaces and the cursor for the next page.
        """
        return self._iterate(limit, cursor, {"includeGlobal": include_global})

    def list(
        self,
        include_global: bool = False,
        limit: int | None = 10,
    ) -> list[SpaceResponse]:
        """List all spaces, handling pagination automatically.

        This method lazily iterates over all spaces, fetching pages as needed.

        Args:
            include_global: Whether to include global spaces (default: False)
            limit: Maximum number of items to retrieve (default: None, meaning no limit)

        Returns:
            A list of all space objects.

        """
        return self._list(limit, {"includeGlobal": include_global})
