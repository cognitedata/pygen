"""Containers API resource client.

This module provides the ContainersAPI class for managing CDF containers.
"""

from __future__ import annotations

import builtins
from collections.abc import Iterator, Sequence

from cognite.pygen._client.http_client import HTTPClient
from cognite.pygen._client.models import ContainerReference, ContainerRequest, ContainerResponse

from ._base import BaseResourceAPI, Page


class ContainersAPI:
    """API client for CDF Container resources.

    Containers define the storage schema for nodes and edges.
    They specify the properties and their types that can be stored.

    Example:
        >>> from cognite.pygen._client import PygenClient
        >>> client = PygenClient(config)
        >>> # List all containers
        >>> for container in client.containers.list():
        ...     print(f"{container.space}:{container.external_id}")
        >>> # Retrieve specific containers
        >>> ref = ContainerReference(space="my_space", external_id="my_container")
        >>> containers = client.containers.retrieve([ref])
    """

    def __init__(self, http_client: HTTPClient) -> None:
        """Initialize the Containers API.

        Args:
            http_client: The HTTP client to use for API requests.
        """
        self._api = BaseResourceAPI[ContainerReference, ContainerResponse](
            http_client=http_client,
            endpoint="/models/containers",
            reference_cls=ContainerReference,
            request_cls=ContainerRequest,
            response_cls=ContainerResponse,
        )

    def iterate(
        self,
        *,
        space: str | None = None,
        cursor: str | None = None,
        limit: int = 100,
        include_global: bool = False,
    ) -> Page[ContainerResponse]:
        """Fetch a single page of containers.

        Args:
            space: Filter by space. If None, returns containers from all spaces.
            cursor: Cursor for pagination. If None, starts from the beginning.
            limit: Maximum number of items to return per page. Default is 100.
            include_global: Whether to include global containers. Default is False.

        Returns:
            A Page containing the containers and the cursor for the next page.
        """
        return self._api.iterate(
            space=space,
            cursor=cursor,
            limit=limit,
            include_global=include_global,
        )

    def list(
        self,
        *,
        space: str | None = None,
        include_global: bool = False,
        limit: int | None = None,
    ) -> Iterator[ContainerResponse]:
        """List all containers, handling pagination automatically.

        This method lazily iterates over all containers, fetching pages as needed.

        Args:
            space: Filter by space. If None, returns containers from all spaces.
            include_global: Whether to include global containers. Default is False.
            limit: Maximum total number of items to return. If None, returns all items.

        Yields:
            ContainerResponse objects from the API.
        """
        return self._api.list(
            space=space,
            include_global=include_global,
            limit=limit,
        )

    def retrieve(self, references: Sequence[ContainerReference]) -> builtins.list[ContainerResponse]:
        """Retrieve specific containers by their references.

        Args:
            references: A sequence of reference objects identifying the containers to retrieve.

        Returns:
            A list of container objects. Containers that don't exist are not included.
        """
        return self._api.retrieve(references)

    def create(self, items: Sequence[ContainerRequest]) -> builtins.list[ContainerResponse]:
        """Create or update containers.

        Args:
            items: A sequence of request objects defining the containers to create/update.

        Returns:
            A list of the created/updated container objects.
        """
        return self._api.create(items)

    def delete(self, references: Sequence[ContainerReference]) -> builtins.list[ContainerReference]:
        """Delete containers by their references.

        Args:
            references: A sequence of reference objects identifying the containers to delete.

        Returns:
            A list of references to the deleted containers.
        """
        return self._api.delete(references)
