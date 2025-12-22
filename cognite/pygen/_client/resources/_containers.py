"""Containers API resource client.

This module provides the ContainersAPI class for managing CDF containers.
"""

from __future__ import annotations

from collections.abc import Iterator

from pydantic import TypeAdapter

from cognite.pygen._client.http_client import HTTPClient, RequestMessage
from cognite.pygen._client.models import ContainerReference, ContainerRequest, ContainerResponse

from ._base import BaseResourceAPI, Page


class ContainersAPI(BaseResourceAPI[ContainerReference, ContainerRequest, ContainerResponse]):
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
        super().__init__(http_client)
        self._response_type_adapter = TypeAdapter(list[ContainerResponse])
        self._reference_type_adapter = TypeAdapter(list[ContainerReference])
        self._request_type_adapter = TypeAdapter(list[ContainerRequest])

    @property
    def _endpoint(self) -> str:
        return "/models/containers"

    @property
    def _response_adapter(self) -> TypeAdapter[list[ContainerResponse]]:
        return self._response_type_adapter

    @property
    def _reference_adapter(self) -> TypeAdapter[list[ContainerReference]]:
        return self._reference_type_adapter

    @property
    def _request_adapter(self) -> TypeAdapter[list[ContainerRequest]]:
        return self._request_type_adapter

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
        params: dict[str, str | int | bool] = {"limit": limit}
        if cursor is not None:
            params["cursor"] = cursor
        if space is not None:
            params["space"] = space
        if include_global:
            params["includeGlobal"] = True

        request = RequestMessage(
            endpoint_url=self._make_url(),
            method="GET",
            parameters=params,
        )

        result = self._http_client.request_with_retries(request)
        response = result.get_success_or_raise()

        return self._parse_list_response(response)

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
        cursor: str | None = None
        count = 0
        page_limit = min(limit, 1000) if limit is not None else 1000

        while True:
            page = self.iterate(
                space=space,
                cursor=cursor,
                limit=page_limit,
                include_global=include_global,
            )

            for item in page.items:
                yield item
                count += 1
                if limit is not None and count >= limit:
                    return

            if page.cursor is None:
                break
            cursor = page.cursor
