"""Base classes for resource clients.

This module provides the base infrastructure for resource-specific clients
that handle CRUD operations for CDF Data Modeling API resources.
"""

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from functools import cached_property
from typing import Any, Generic

from pydantic import BaseModel, TypeAdapter

from cognite.pygen._client.http_client import HTTPClient, RequestMessage, SuccessResponse
from cognite.pygen._client.models import (
    T_APIResource,
    T_Reference,
    T_ResponseResource,
)


@dataclass(frozen=True)
class ResourceLimits:
    """Configuration for API endpoint limits.

    These limits control the maximum number of items per request
    for each type of operation.

    Attributes:
        create: Maximum items per create request. Default is 100.
        retrieve: Maximum items per retrieve request. Default is 100.
        delete: Maximum items per delete request. Default is 100.
        list: Maximum items per list/iterate request. Default is 1000.
    """

    create: int = 100
    retrieve: int = 100
    delete: int = 100
    list: int = 1000


class Page(BaseModel, Generic[T_ResponseResource]):
    """A page of results from a paginated API response.

    Attributes:
        items: The list of items in this page.
        cursor: The cursor for the next page, or None if this is the last page.
    """

    items: list[T_ResponseResource]
    cursor: str | None = None


class BaseResourceAPI(Generic[T_Reference, T_ResponseResource]):
    """Generic resource API for CDF Data Modeling resources.

    This class provides common CRUD operations for CDF Data Modeling resources.
    It is designed to be used via composition, not inheritance.
    """

    def __init__(
        self,
        http_client: HTTPClient,
        endpoint: str,
        reference_cls: type[T_Reference],
        request_cls: type[T_APIResource],
        response_cls: type[T_ResponseResource],
        limits: ResourceLimits | None = None,
    ) -> None:
        """Initialize the resource API.

        Args:
            http_client: The HTTP client to use for API requests.
            endpoint: The API endpoint path for this resource (e.g., '/models/spaces').
            reference_cls: The class for reference objects.
            request_cls: The class for request objects.
            response_cls: The class for response objects.
            limits: Configuration for API endpoint limits. Uses defaults if not provided.
        """
        self._http_client = http_client
        self._endpoint = endpoint
        self._response_cls = response_cls
        self._reference_cls = reference_cls
        self._request_cls = request_cls
        self._limits = limits or ResourceLimits()

    @cached_property
    def _request_adapter(self) -> TypeAdapter[list[T_APIResource]]:
        """TypeAdapter for serializing request objects."""
        return TypeAdapter(list[self._request_cls])

    @cached_property
    def _response_adapter(self) -> TypeAdapter[list[T_ResponseResource]]:
        """TypeAdapter for deserializing response objects."""
        return TypeAdapter(list[self._response_cls])

    @cached_property
    def _reference_adapter(self) -> TypeAdapter[list[T_Reference]]:
        """TypeAdapter for serializing/deserializing reference objects."""
        return TypeAdapter(list[self._reference_cls])

    def _make_url(self) -> str:
        """Create the full URL for this resource endpoint."""
        return self._http_client.config.create_api_url(self._endpoint)

    def _chunk_items(self, items: Sequence[Any], chunk_size: int) -> Iterator[list[Any]]:
        """Split items into chunks of specified size.

        Args:
            items: The items to chunk.
            chunk_size: Maximum items per chunk.

        Yields:
            Lists of items, each with at most chunk_size elements.
        """
        items_list = list(items)
        for i in range(0, len(items_list), chunk_size):
            yield items_list[i : i + chunk_size]

    def create(self, items: Sequence[T_APIResource]) -> list[T_ResponseResource]:
        """Create or update resources.

        Args:
            items: A sequence of request objects defining the resources to create/update.

        Returns:
            A list of the created/updated resource objects.
        """
        if not items:
            return []

        all_responses: list[T_ResponseResource] = []

        for chunk in self._chunk_items(items, self._limits.create):
            body = {"items": self._request_adapter.dump_python(chunk, mode="json", by_alias=True)}

            request = RequestMessage(
                endpoint_url=self._make_url(),
                method="POST",
                body_content=body,
            )

            result = self._http_client.request_with_retries(request)
            response = result.get_success_or_raise()

            body_json = response.body_json
            items_data = body_json.get("items", [])
            all_responses.extend(self._response_adapter.validate_python(items_data))

        return all_responses

    def retrieve(
        self,
        references: Sequence[T_Reference],
        params: dict[str, Any] | None = None,
    ) -> list[T_ResponseResource]:
        """Retrieve specific resources by their references.

        Args:
            references: A sequence of reference objects identifying the resources to retrieve.
            params: Additional query parameters to include in the request.

        Returns:
            A list of resource objects. Resources that don't exist are not included.
        """
        if not references:
            return []

        # Convert params to the expected type
        request_params: dict[str, str | int | float | bool] | None = None
        if params:
            request_params = {k: v for k, v in params.items() if v is not None}

        all_responses: list[T_ResponseResource] = []

        for chunk in self._chunk_items(references, self._limits.retrieve):
            body = {"items": self._reference_adapter.dump_python(chunk, mode="json", by_alias=True)}

            request = RequestMessage(
                endpoint_url=f"{self._make_url()}/byids",
                method="POST",
                body_content=body,
                parameters=request_params,
            )

            result = self._http_client.request_with_retries(request)
            response = result.get_success_or_raise()

            body_json = response.body_json
            items_data = body_json.get("items", [])
            all_responses.extend(self._response_adapter.validate_python(items_data))

        return all_responses

    def delete(self, references: Sequence[T_Reference]) -> list[T_Reference]:
        """Delete resources by their references.

        Args:
            references: A sequence of reference objects identifying the resources to delete.

        Returns:
            A list of references to the deleted resources.
        """
        if not references:
            return []

        all_deleted: list[T_Reference] = []

        for chunk in self._chunk_items(references, self._limits.delete):
            body = {"items": self._reference_adapter.dump_python(chunk, mode="json", by_alias=True)}

            request = RequestMessage(
                endpoint_url=f"{self._make_url()}/delete",
                method="POST",
                body_content=body,
            )

            result = self._http_client.request_with_retries(request)
            response = result.get_success_or_raise()

            body_json = response.body_json
            items_data = body_json.get("items", [])
            all_deleted.extend(self._reference_adapter.validate_python(items_data))

        return all_deleted

    def iterate(self, params: dict[str, Any] | None = None) -> Page[T_ResponseResource]:
        """Fetch a single page of resources.

        Args:
            params: Query parameters for the request. Supported parameters depend on
                the resource type but typically include:
                - cursor: Cursor for pagination
                - limit: Maximum number of items (defaults to list limit)
                - space: Filter by space
                - includeGlobal: Whether to include global resources

        Returns:
            A Page containing the items and the cursor for the next page.
        """
        request_params: dict[str, str | int | float | bool] = {}

        if params:
            for key, value in params.items():
                if value is not None:
                    request_params[key] = value

        # Apply default limit if not specified
        if "limit" not in request_params:
            request_params["limit"] = self._limits.list

        request = RequestMessage(
            endpoint_url=self._make_url(),
            method="GET",
            parameters=request_params if request_params else None,
        )

        result = self._http_client.request_with_retries(request)
        response = result.get_success_or_raise()

        return self._parse_list_response(response)

    def list(self, params: dict[str, Any] | None = None) -> Iterator[T_ResponseResource]:
        """List all resources, handling pagination automatically.

        This method lazily iterates over all resources, fetching pages as needed.

        Args:
            params: Query parameters for the request. Supported parameters depend on
                the resource type but typically include:
                - space: Filter by space
                - includeGlobal: Whether to include global resources
                - limit: Maximum total number of items to return

                Note: cursor is managed automatically.

        Yields:
            Resource objects from the API.
        """
        params = dict(params) if params else {}

        # Extract total limit if specified
        total_limit = params.pop("limit", None)

        cursor: str | None = None
        count = 0
        page_limit = min(total_limit, self._limits.list) if total_limit is not None else self._limits.list

        while True:
            page_params = dict(params)
            page_params["limit"] = page_limit
            if cursor is not None:
                page_params["cursor"] = cursor

            page = self.iterate(page_params)

            for item in page.items:
                yield item
                count += 1
                if total_limit is not None and count >= total_limit:
                    return

            if page.cursor is None:
                break
            cursor = page.cursor

    def _parse_list_response(self, response: SuccessResponse) -> Page[T_ResponseResource]:
        """Parse a list/iterate response into a Page."""
        body = response.body_json
        items_data = body.get("items", [])
        next_cursor = body.get("nextCursor")

        items = self._response_adapter.validate_python(items_data)
        return Page[T_ResponseResource](items=items, cursor=next_cursor)
