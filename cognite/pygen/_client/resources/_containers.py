"""Containers API resource client.

This module provides the ContainersAPI class for managing CDF containers.
"""

from collections.abc import Sequence

from cognite.pygen._client.http_client import HTTPClient, SuccessResponse
from cognite.pygen._client.models import ContainerReference, ContainerRequest, ContainerResponse

from ._base import BaseResourceAPI, Page, ReferenceResponseItems, ResourceLimits


class ContainersAPI(BaseResourceAPI[ContainerReference, ContainerRequest, ContainerResponse]):
    """API client for CDF Container resources.

    Containers define the storage schema for nodes and edges.
    They specify the properties and their types that can be stored.

    """

    def __init__(self, http_client: HTTPClient, limits: ResourceLimits | None = None) -> None:
        """Initialize the Containers API.

        Args:
            http_client: The HTTP client to use for API requests.
            limits: Configuration for API endpoint limits. Uses defaults if not provided.
        """
        super().__init__(http_client, "/models/containers", limits)

    def _page_response(self, response: SuccessResponse) -> Page[ContainerResponse]:
        return Page[ContainerResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ReferenceResponseItems[ContainerReference]:
        return ReferenceResponseItems[ContainerReference].model_validate_json(response.body)

    def create(self, items: Sequence[ContainerRequest]) -> list[ContainerResponse]:
        """Create or update containers.

        Args:
            items: A sequence of request objects defining the containers to create/update.

        Returns:
            A list of the created/updated container objects.
        """
        return self._create(items)

    def retrieve(self, references: Sequence[ContainerReference]) -> list[ContainerResponse]:
        """Retrieve specific containers by their references.

        Args:
            references: A sequence of reference objects identifying the containers to retrieve.

        Returns:
            A list of container objects. Containers that don't exist are not included.
        """
        return self._retrieve(references)

    def delete(self, references: Sequence[ContainerReference]) -> list[ContainerReference]:
        """Delete containers by their references.

        Args:
            references: A sequence of reference objects identifying the containers to delete.

        Returns:
            A list of references to the deleted containers.
        """
        return self._delete(references)

    def iterate(
        self,
        space: str | None = None,
        include_global: bool = False,
        limit: int = 10,
        cursor: str | None = None,
    ) -> Page[ContainerResponse]:
        """Fetch a single page of containers.

        Args:
            space: Filter by space (default: None, meaning all spaces)
            include_global: Whether to include global containers (default: False)
            limit: Maximum number of items per page (default: 10)
            cursor: Cursor for pagination (default: None)

        Returns:
            A Page containing the containers and the cursor for the next page.
        """
        return self._iterate(limit, cursor, {"space": space, "includeGlobal": include_global})

    def list(
        self,
        space: str | None = None,
        include_global: bool = False,
        limit: int | None = 10,
    ) -> list[ContainerResponse]:
        """List all containers, handling pagination automatically.

        This method fetches all containers, handling pagination transparently.

        Args:
            space: Filter by space (default: None, meaning all spaces)
            include_global: Whether to include global containers (default: False)
            limit: Maximum number of items to retrieve (default: None, meaning no limit)

        Returns:
            A list of all container objects.
        """
        return self._list(limit, {"space": space, "includeGlobal": include_global})
