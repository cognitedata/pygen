"""Base classes for resource clients.

This module provides the base infrastructure for resource-specific clients
that handle CRUD operations for CDF Data Modeling API resources.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Generic, TypeVar

from pydantic import TypeAdapter

from cognite.pygen._client.http_client import HTTPClient, RequestMessage, SuccessResponse
from cognite.pygen._client.models import ReferenceObject, ResponseResource

# Type variables for resource classes
T_Reference = TypeVar("T_Reference", bound=ReferenceObject)
T_Request = TypeVar("T_Request")
T_Response = TypeVar("T_Response", bound=ResponseResource)


@dataclass
class Page(Generic[T_Response]):
    """A page of results from a paginated API response.

    Attributes:
        items: The list of items in this page.
        cursor: The cursor for the next page, or None if this is the last page.
    """

    items: list[T_Response]
    cursor: str | None


class BaseResourceAPI(ABC, Generic[T_Reference, T_Request, T_Response]):
    """Abstract base class for resource-specific API clients.

    This class provides common CRUD operations for CDF Data Modeling resources.
    Subclasses must implement the abstract properties and methods to define
    resource-specific behavior.
    """

    def __init__(self, http_client: HTTPClient) -> None:
        """Initialize the resource API.

        Args:
            http_client: The HTTP client to use for API requests.
        """
        self._http_client = http_client

    @property
    @abstractmethod
    def _endpoint(self) -> str:
        """The API endpoint path for this resource (e.g., '/models/spaces')."""
        ...

    @property
    @abstractmethod
    def _response_adapter(self) -> TypeAdapter[list[T_Response]]:
        """Type adapter for deserializing response items."""
        ...

    @property
    @abstractmethod
    def _reference_adapter(self) -> TypeAdapter[list[T_Reference]]:
        """Type adapter for serializing reference objects."""
        ...

    @property
    @abstractmethod
    def _request_adapter(self) -> TypeAdapter[list[T_Request]]:
        """Type adapter for serializing request objects."""
        ...

    def _make_url(self) -> str:
        """Create the full URL for this resource endpoint."""
        return self._http_client.config.create_api_url(self._endpoint)

    def iterate(
        self,
        *,
        space: str | None = None,
        cursor: str | None = None,
        limit: int = 100,
        include_global: bool = False,
    ) -> Page[T_Response]:
        """Fetch a single page of resources.

        Args:
            space: Filter by space. If None, returns resources from all spaces.
            cursor: Cursor for pagination. If None, starts from the beginning.
            limit: Maximum number of items to return per page. Default is 100.
            include_global: Whether to include global resources. Default is False.

        Returns:
            A Page containing the items and the cursor for the next page.
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

    def _parse_list_response(self, response: SuccessResponse) -> Page[T_Response]:
        """Parse a list/iterate response into a Page."""
        body = response.body_json
        items_data = body.get("items", [])
        next_cursor = body.get("nextCursor")

        items = self._response_adapter.validate_python(items_data)
        return Page(items=items, cursor=next_cursor)

    def list(
        self,
        *,
        space: str | None = None,
        include_global: bool = False,
        limit: int | None = None,
    ) -> Iterator[T_Response]:
        """List all resources, handling pagination automatically.

        This method lazily iterates over all resources, fetching pages as needed.

        Args:
            space: Filter by space. If None, returns resources from all spaces.
            include_global: Whether to include global resources. Default is False.
            limit: Maximum total number of items to return. If None, returns all items.

        Yields:
            Resource objects from the API.
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

    def retrieve(self, references: Sequence[T_Reference]) -> list[T_Response]:
        """Retrieve specific resources by their references.

        Args:
            references: A sequence of reference objects identifying the resources to retrieve.

        Returns:
            A list of resource objects. Resources that don't exist are not included.
        """
        if not references:
            return []

        body = {"items": self._reference_adapter.dump_python(list(references), mode="json", by_alias=True)}

        request = RequestMessage(
            endpoint_url=f"{self._make_url()}/byids",
            method="POST",
            body_content=body,
        )

        result = self._http_client.request_with_retries(request)
        response = result.get_success_or_raise()

        body_json = response.body_json
        items_data = body_json.get("items", [])
        return self._response_adapter.validate_python(items_data)

    def create(self, items: Sequence[T_Request]) -> list[T_Response]:
        """Create or update resources.

        Args:
            items: A sequence of request objects defining the resources to create/update.

        Returns:
            A list of the created/updated resource objects.
        """
        if not items:
            return []

        body = {"items": self._request_adapter.dump_python(list(items), mode="json", by_alias=True)}

        request = RequestMessage(
            endpoint_url=self._make_url(),
            method="POST",
            body_content=body,
        )

        result = self._http_client.request_with_retries(request)
        response = result.get_success_or_raise()

        body_json = response.body_json
        items_data = body_json.get("items", [])
        return self._response_adapter.validate_python(items_data)

    def delete(self, references: Sequence[T_Reference]) -> list[T_Reference]:
        """Delete resources by their references.

        Args:
            references: A sequence of reference objects identifying the resources to delete.

        Returns:
            A list of references to the deleted resources.
        """
        if not references:
            return []

        body = {"items": self._reference_adapter.dump_python(list(references), mode="json", by_alias=True)}

        request = RequestMessage(
            endpoint_url=f"{self._make_url()}/delete",
            method="POST",
            body_content=body,
        )

        result = self._http_client.request_with_retries(request)
        response = result.get_success_or_raise()

        body_json = response.body_json
        items_data = body_json.get("items", [])
        return self._reference_adapter.validate_python(items_data)
