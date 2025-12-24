"""Instance API for view-specific operations.

This module contains the InstanceAPI base class that provides methods for
querying instances (nodes/edges) through a specific view.
"""

from __future__ import annotations

# Use List for type annotations to avoid confusion with the list() method in this class
from builtins import list as List
from collections.abc import Iterator, Sequence
from typing import Any, Generic, Literal, TypeVar, overload

from pydantic import JsonValue, TypeAdapter

from cognite.pygen._generation.python.instance_api.http_client import (
    HTTPClient,
    RequestMessage,
)
from cognite.pygen._generation.python.instance_api.models import (
    InstanceId,
    InstanceList,
    T_Instance,
    T_InstanceList,
    T_InstanceWrite,
    ViewReference,
)
from cognite.pygen._generation.python.instance_api.models.filters import Filter
from cognite.pygen._generation.python.instance_api.models.query import (
    PropertySort,
    UnitConversion,
)
from cognite.pygen._generation.python.instance_api.models.responses import ListResponse

# Default limits
DEFAULT_LIMIT = 25
DEFAULT_CHUNK_SIZE = 1000
MAX_LIMIT = 1000
SEARCH_LIMIT = 1000


# Type variable for the list type constructor
T_List = TypeVar("T_List", bound=InstanceList[Any])


class InstanceAPI(Generic[T_InstanceWrite, T_Instance, T_InstanceList]):
    """Generic resource API for CDF Data Modeling view-specific operations.

    This class provides methods for querying instances (nodes or edges) through
    a specific view. It supports filtering, sorting, pagination, and full-text search.

    The InstanceAPI is designed to be subclassed or used via composition to create
    view-specific APIs with proper type hints.

    Args:
        http_client: The HTTP client to use for API requests.
        view_ref: Reference to the view for querying instances.
        instance_type: The type of instances to query ("node" or "edge").
        list_cls: The class to use for creating instance lists (defaults to InstanceList).

    Example:
        >>> from cognite.pygen._generation.python.instance_api import InstanceAPI
        >>> api = InstanceAPI(http_client, view_ref, "node")
        >>> for page in api.iterate(limit=100):
        ...     for instance in page.items:
        ...         print(instance.external_id)
    """

    ENDPOINT = "/models/instances"
    LIST_ENDPOINT = "/models/instances/list"
    SEARCH_ENDPOINT = "/models/instances/search"

    def __init__(
        self,
        http_client: HTTPClient,
        view_ref: ViewReference,
        instance_type: Literal["node", "edge"],
        list_cls: type[T_InstanceList] | None = None,
    ) -> None:
        """Initialize the resource API.

        Args:
            http_client: The HTTP client to use for API requests.
            view_ref: Reference to the view for querying instances.
            instance_type: The type of instances to query ("node" or "edge").
            list_cls: The class to use for creating instance lists.
        """
        self._http_client = http_client
        self._view_ref = view_ref
        self._instance_type = instance_type
        self._list_cls = list_cls or InstanceList  # type: ignore[assignment]

    @property
    def _sources_body(self) -> List[dict[str, Any]]:
        """Build the sources body for the API request."""
        return [
            {
                "source": self._view_ref.model_dump(by_alias=True),
            }
        ]

    def iterate(
        self,
        filter: Filter | None = None,
        sort: PropertySort | Sequence[PropertySort] | None = None,
        limit: int | None = None,
        chunk_size: int | None = None,
        cursor: str | None = None,
        include_typing: bool = False,
        include_debug: bool = False,
        units: UnitConversion | Sequence[UnitConversion] | None = None,
    ) -> Iterator[ListResponse[T_InstanceList]]:
        """Iterate over instances in the view with automatic pagination.

        This method yields pages of instances, handling pagination automatically.
        It uses the advancedListInstance API endpoint.

        Args:
            filter: Filter to apply to the query. Use the filter data classes
                from the filters module to build complex filters.
            sort: Sort order for the results. Can be a single PropertySort or
                a sequence of PropertySort for multi-level sorting.
            limit: Maximum total number of instances to return across all pages.
                If None, iterates through all matching instances.
            chunk_size: Number of instances to fetch per API request (page size).
                Defaults to 1000 (the API maximum). Use smaller values for
                memory-constrained environments.
            cursor: Initial cursor for resuming pagination from a previous point.
            include_typing: If True, includes type information for direct relations
                in the response.
            include_debug: If True, includes debug information (query timing, etc.)
                in each response page.
            units: Unit conversion configuration for numeric properties with units.
                Can be a single UnitConversion or a sequence for multiple properties.

        Yields:
            ListResponse containing a page of instances and optional debug information.

        Example:
            >>> # Iterate through all instances
            >>> for page in api.iterate():
            ...     for instance in page.items:
            ...         process(instance)

            >>> # Iterate with filter and limit
            >>> from cognite.pygen._generation.python.instance_api.models.filters import EqualsFilterData
            >>> filter = {"equals": EqualsFilterData(
            ...     property=["my_space", "MyView/v1", "status"],
            ...     value="active"
            ... )}
            >>> for page in api.iterate(filter=filter, limit=100):
            ...     print(f"Got {len(page.items)} items")
        """
        total_fetched = 0
        current_cursor = cursor
        effective_chunk_size = min(chunk_size or DEFAULT_CHUNK_SIZE, MAX_LIMIT)

        while True:
            # Calculate how many items to fetch in this request
            if limit is not None:
                remaining = limit - total_fetched
                if remaining <= 0:
                    break
                request_limit = min(remaining, effective_chunk_size)
            else:
                request_limit = effective_chunk_size

            response = self._list_request(
                filter=filter,
                sort=sort,
                limit=request_limit,
                cursor=current_cursor,
                include_typing=include_typing,
                include_debug=include_debug,
                units=units,
            )

            total_fetched += len(response.items)
            yield response

            # Check if we've reached the end
            if response.next_cursor is None or len(response.items) < request_limit:
                break

            current_cursor = response.next_cursor

    def list(
        self,
        filter: Filter | None = None,
        sort: PropertySort | Sequence[PropertySort] | None = None,
        limit: int = DEFAULT_LIMIT,
        include_typing: bool = False,
        include_debug: bool = False,
        units: UnitConversion | Sequence[UnitConversion] | None = None,
    ) -> T_InstanceList:
        """List instances in the view.

        This is a convenience method that wraps iterate() and collects all results
        into a single list. For large result sets, consider using iterate() instead
        to process results in chunks.

        Args:
            filter: Filter to apply to the query. Use the filter data classes
                from the filters module to build complex filters.
            sort: Sort order for the results. Can be a single PropertySort or
                a sequence of PropertySort for multi-level sorting.
            limit: Maximum number of instances to return. Defaults to 25.
                Set to -1 or None to return all matching instances.
            include_typing: If True, includes type information for direct relations.
            include_debug: If True, includes debug information. Note: only the
                debug info from the last page is preserved when collecting results.
            units: Unit conversion configuration for numeric properties with units.

        Returns:
            A list of instances matching the query.

        Example:
            >>> # List first 10 instances
            >>> instances = api.list(limit=10)

            >>> # List with sorting
            >>> sort = PropertySort(
            ...     property=["my_space", "MyView/v1", "created_at"],
            ...     direction="descending"
            ... )
            >>> recent = api.list(sort=sort, limit=5)
        """
        effective_limit = None if limit == -1 else limit

        all_items: List[T_Instance] = []
        for page in self.iterate(
            filter=filter,
            sort=sort,
            limit=effective_limit,
            include_typing=include_typing,
            include_debug=include_debug,
            units=units,
        ):
            all_items.extend(page.items)

        return self._list_cls(all_items)  # type: ignore[return-value]

    def search(
        self,
        query: str | None = None,
        properties: str | Sequence[str] | None = None,
        filter: Filter | None = None,
        sort: PropertySort | Sequence[PropertySort] | None = None,
        limit: int = DEFAULT_LIMIT,
        include_typing: bool = False,
        include_debug: bool = False,
        units: UnitConversion | Sequence[UnitConversion] | None = None,
    ) -> T_InstanceList:
        """Search for instances using full-text search.

        This method uses the searchInstances API endpoint to perform full-text
        search on text properties. It can be combined with filters for more
        precise results.

        Args:
            query: The search query string. The query is matched against the
                searchable text properties of the view.
            properties: The properties to search in. If None, searches all
                searchable text properties. Can be a single property name or
                a sequence of property names.
            filter: Additional filter to apply after the search. Use this to
                narrow down search results.
            sort: Sort order for the results. Can be a single PropertySort or
                a sequence of PropertySort for multi-level sorting.
            limit: Maximum number of instances to return. Defaults to 25.
                Maximum is 1000 for search operations.
            include_typing: If True, includes type information for direct relations.
            include_debug: If True, includes debug information (query timing, etc.).
            units: Unit conversion configuration for numeric properties with units.

        Returns:
            A list of instances matching the search query.

        Example:
            >>> # Simple text search
            >>> results = api.search(query="important document")

            >>> # Search specific properties with filter
            >>> results = api.search(
            ...     query="error",
            ...     properties=["title", "description"],
            ...     filter={"equals": EqualsFilterData(
            ...         property=["my_space", "MyView/v1", "status"],
            ...         value="active"
            ...     )},
            ...     limit=50
            ... )
        """
        effective_limit = min(limit, SEARCH_LIMIT)

        # Build request body
        body = self._build_search_body(
            query=query,
            properties=properties,
            filter=filter,
            sort=sort,
            limit=effective_limit,
            include_typing=include_typing,
            include_debug=include_debug,
            units=units,
        )

        request = RequestMessage(
            endpoint_url=self._http_client.config.create_api_url(self.SEARCH_ENDPOINT),
            method="POST",
            body_content=body,
        )

        result = self._http_client.request_with_retries(request)
        success = result.get_success_or_raise()

        response = self._parse_list_response(success.body)
        return response.items  # type: ignore[return-value]

    def _list_request(
        self,
        filter: Filter | None = None,
        sort: PropertySort | Sequence[PropertySort] | None = None,
        limit: int = DEFAULT_CHUNK_SIZE,
        cursor: str | None = None,
        include_typing: bool = False,
        include_debug: bool = False,
        units: UnitConversion | Sequence[UnitConversion] | None = None,
    ) -> ListResponse[T_InstanceList]:
        """Execute a single list request to the API.

        Args:
            filter: Filter to apply.
            sort: Sort order.
            limit: Number of items to fetch.
            cursor: Pagination cursor.
            include_typing: Include type information.
            include_debug: Include debug information.
            units: Unit conversions.

        Returns:
            ListResponse with items and optional cursor/debug info.
        """
        body = self._build_list_body(
            filter=filter,
            sort=sort,
            limit=limit,
            cursor=cursor,
            include_typing=include_typing,
            include_debug=include_debug,
            units=units,
        )

        request = RequestMessage(
            endpoint_url=self._http_client.config.create_api_url(self.LIST_ENDPOINT),
            method="POST",
            body_content=body,
        )

        result = self._http_client.request_with_retries(request)
        success = result.get_success_or_raise()

        return self._parse_list_response(success.body)

    def _build_list_body(
        self,
        filter: Filter | None = None,
        sort: PropertySort | Sequence[PropertySort] | None = None,
        limit: int = DEFAULT_CHUNK_SIZE,
        cursor: str | None = None,
        include_typing: bool = False,
        include_debug: bool = False,
        units: UnitConversion | Sequence[UnitConversion] | None = None,
    ) -> dict[str, JsonValue]:
        """Build the request body for list operations."""
        body: dict[str, Any] = {
            "instanceType": self._instance_type,
            "sources": self._sources_body,
            "limit": limit,
            "includeTyping": include_typing,
        }

        if filter is not None:
            body["filter"] = self._serialize_filter(filter)

        if sort is not None:
            body["sort"] = self._serialize_sort(sort)

        if cursor is not None:
            body["cursor"] = cursor

        if include_debug:
            body["includeDebug"] = True

        if units is not None:
            body["targetUnits"] = self._serialize_units(units)

        return body  # type: ignore[return-value]

    def _build_search_body(
        self,
        query: str | None = None,
        properties: str | Sequence[str] | None = None,
        filter: Filter | None = None,
        sort: PropertySort | Sequence[PropertySort] | None = None,
        limit: int = DEFAULT_LIMIT,
        include_typing: bool = False,
        include_debug: bool = False,
        units: UnitConversion | Sequence[UnitConversion] | None = None,
    ) -> dict[str, JsonValue]:
        """Build the request body for search operations."""
        body: dict[str, Any] = {
            "instanceType": self._instance_type,
            "view": self._view_ref.model_dump(by_alias=True),
            "limit": limit,
            "includeTyping": include_typing,
        }

        if query is not None:
            body["query"] = query

        if properties is not None:
            if isinstance(properties, str):
                body["properties"] = [properties]
            else:
                body["properties"] = List(properties)

        if filter is not None:
            body["filter"] = self._serialize_filter(filter)

        if sort is not None:
            body["sort"] = self._serialize_sort(sort)

        if include_debug:
            body["includeDebug"] = True

        if units is not None:
            body["targetUnits"] = self._serialize_units(units)

        return body  # type: ignore[return-value]

    def _serialize_filter(self, filter: Filter) -> dict[str, Any]:
        """Serialize a filter to API format."""
        from cognite.pygen._generation.python.instance_api.models.filters import FilterAdapter

        return FilterAdapter.dump_python(filter, by_alias=True, exclude_none=True)

    def _serialize_sort(self, sort: PropertySort | Sequence[PropertySort]) -> List[dict[str, Any]]:
        """Serialize sort specification to API format."""
        if isinstance(sort, PropertySort):
            sort_list = [sort]
        else:
            sort_list = List(sort)

        return [s.model_dump(by_alias=True, exclude_none=True) for s in sort_list]

    def _serialize_units(self, units: UnitConversion | Sequence[UnitConversion]) -> List[dict[str, Any]]:
        """Serialize unit conversions to API format."""
        if isinstance(units, UnitConversion):
            units_list = [units]
        else:
            units_list = List(units)

        return [u.model_dump(by_alias=True) for u in units_list]

    def _parse_list_response(self, body: str) -> ListResponse[T_InstanceList]:
        """Parse the response from list/search operations.

        Args:
            body: The JSON response body.

        Returns:
            ListResponse with parsed items.
        """
        # We use TypeAdapter here to handle the generic type properly
        adapter = TypeAdapter(ListResponse[self._list_cls])  # type: ignore[name-defined]
        return adapter.validate_json(body)

    # Placeholder methods for future implementation (Task 3b)

    @overload
    def _retrieve(
        self,
        id: str | InstanceId | tuple[str, str],
        space: str | None = None,
    ) -> T_Instance | None: ...

    @overload
    def _retrieve(
        self,
        id: List[str | InstanceId | tuple[str, str]],
        space: str | None = None,
    ) -> T_InstanceList: ...

    def _retrieve(
        self,
        id: str | InstanceId | tuple[str, str] | List[str | InstanceId | tuple[str, str]],
        space: str | None = None,
    ) -> T_Instance | T_InstanceList | None:
        """Retrieve instances by ID (placeholder for Task 3b)."""
        raise NotImplementedError("retrieve() will be implemented in Task 3b")

    def _aggregate(self) -> None:
        """Aggregate instances (placeholder for Task 3b)."""
        raise NotImplementedError("aggregate() will be implemented in Task 3b")

    def _sync(self) -> None:
        """Sync instances (placeholder for future implementation)."""
        raise NotImplementedError("sync() is not yet implemented")

    def _query(self) -> None:
        """Query instances (placeholder for future implementation)."""
        raise NotImplementedError("query() is not yet implemented")
