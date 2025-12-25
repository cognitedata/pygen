"""Instance API for view-specific operations.

This module contains the InstanceAPI base class that provides methods for
querying instances (nodes/edges) through a specific view.
"""

from collections.abc import Sequence
from typing import Any, Generic, Literal, overload

from pydantic import BaseModel, JsonValue, TypeAdapter

from cognite.pygen._generation.python.instance_api.http_client import (
    HTTPClient,
    RequestMessage,
)
from cognite.pygen._generation.python.instance_api.models import (
    InstanceId,
    T_Instance,
    T_InstanceList,
    ViewReference,
)
from cognite.pygen._generation.python.instance_api.models.filters import Filter, FilterAdapter
from cognite.pygen._generation.python.instance_api.models.query import (
    DebugParameters,
    PropertySort,
    UnitConversion,
)
from cognite.pygen._generation.python.instance_api.models.responses import ListResponse, Page


class InstanceAPI(Generic[T_Instance, T_InstanceList]):
    """Generic resource API for CDF Data Modeling view-specific operations.

    This class provides methods for querying instances (nodes or edges) through
    a specific view. It supports filtering, sorting, pagination, and full-text search.

    The InstanceAPI is designed to be subclassed to create
    view-specific APIs with proper type hints.

    Args:
        http_client: The HTTP client to use for API requests.
        view_ref: Reference to the view for querying instances.
        instance_type: The type of instances to query ("node" or "edge").
        list_cls: The class to use for creating instance lists (defaults to InstanceList).
    """

    _ENDPOINT = "/models/instances"
    _LIST_ENDPOINT = f"{_ENDPOINT}/list"
    _SEARCH_ENDPOINT = f"{_ENDPOINT}/search"
    _LIST_LIMIT = 1000
    _SEARCH_LIMIT = 1000
    _DEFAULT_LIST_LIMIT = 25

    def __init__(
        self,
        http_client: HTTPClient,
        view_ref: ViewReference,
        instance_type: Literal["node", "edge"],
        list_cls: type[T_InstanceList],
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
        self._list_cls = list_cls
        # Create TypeAdapters at init time to avoid mypy errors with dynamic types
        self._page_adapter: TypeAdapter[Page[T_InstanceList]] = TypeAdapter(Page[list_cls])  # type: ignore[valid-type]
        self._list_response_adapter: TypeAdapter[ListResponse[T_InstanceList]] = TypeAdapter(
            ListResponse[list_cls]  # type: ignore[valid-type]
        )

    def _iterate(
        self,
        include_typing: bool = False,
        target_units: UnitConversion | Sequence[UnitConversion] | None = None,
        debug: DebugParameters | None = None,
        cursor: str | None = None,
        limit: int = _DEFAULT_LIST_LIMIT,
        sort: PropertySort | Sequence[PropertySort] | None = None,
        filter: Filter | None = None,
    ) -> Page[T_InstanceList]:
        """Iterate over instances in the view with automatic pagination.

        This method yields pages of instances, handling pagination automatically.
        It uses the advancedListInstance API endpoint.

        Args:
            include_typing: If True, includes type information for direct relations
                in the response.
            target_units: Unit conversion configuration for numeric properties with units.
                Can be a single UnitConversion or a sequence for multiple properties.
            debug: Return query debug notices.
            cursor: Initial cursor for resuming pagination from a previous point.
            limit: Maximum total number of instances to return across all pages.
            sort: Sort order for the results. Can be a single PropertySort or
                a sequence of PropertySort for multi-level sorting.
            filter: Filter to apply to the query. Use the filter data classes
                from the filters module to build complex filters.

        Returns:
            Page containing a page of instances and optional debug information.

        """
        if not (0 < limit <= self._LIST_LIMIT):
            raise ValueError(f"Limit must be between 1 and {self._LIST_LIMIT}, got {limit}.")

        body = self._build_read_body(
            view_key="sources",
            filter=filter,
            sort=sort,
            limit=limit,
            cursor=cursor,
            include_typing=include_typing,
            debug=debug,
            target_units=target_units,
        )

        request = RequestMessage(
            endpoint_url=self._http_client.config.create_api_url(self._LIST_ENDPOINT),
            method="POST",
            body_content=body,
        )
        result = self._http_client.request_with_retries(request)
        success = result.get_success_or_raise()
        return self._page_adapter.validate_json(success.body)

    def _search(
        self,
        query: str | None = None,
        properties: str | Sequence[str] | None = None,
        target_units: UnitConversion | Sequence[UnitConversion] | None = None,
        filter: Filter | None = None,
        include_typing: bool = False,
        sort: PropertySort | Sequence[PropertySort] | None = None,
        operator: Literal["and", "or"] = "or",
        limit: int = _DEFAULT_LIST_LIMIT,
    ) -> ListResponse[T_InstanceList]:
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
            target_units: Unit conversion configuration for numeric properties with units.

        Returns:
            A list of instances matching the search query.
        """
        if not (0 < limit <= self._SEARCH_LIMIT):
            raise ValueError(f"Limit must be between 1 and {self._SEARCH_LIMIT}, got {limit}.")

        # Build request body
        body = self._build_read_body(
            view_key="view",
            query=query,
            properties=properties,
            filter=filter,
            sort=sort,
            limit=limit,
            include_typing=include_typing,
            target_units=target_units,
            operator=operator,
        )

        request = RequestMessage(
            endpoint_url=self._http_client.config.create_api_url(self._SEARCH_ENDPOINT),
            method="POST",
            body_content=body,
        )

        result = self._http_client.request_with_retries(request)
        success = result.get_success_or_raise()
        return self._list_response_adapter.validate_json(success.body)

    def _build_read_body(
        self,
        view_key: Literal["sources", "view"],
        limit: int,
        query: str | None = None,
        properties: str | Sequence[str] | None = None,
        filter: Filter | None = None,
        sort: PropertySort | Sequence[PropertySort] | None = None,
        cursor: str | None = None,
        include_typing: bool | None = None,
        debug: DebugParameters | None = None,
        target_units: UnitConversion | Sequence[UnitConversion] | None = None,
        operator: Literal["and", "or"] | None = None,
    ) -> dict[str, JsonValue]:
        """Build the request body for list operations."""
        body: dict[str, Any] = {
            "instanceType": self._instance_type,
            "limit": limit,
            "includeTyping": include_typing,
        }
        if view_key == "view":
            body["view"] = self._view_ref.dump(camel_case=True, include_type=True)
        else:
            source: dict[str, Any] = {
                "source": self._view_ref.dump(camel_case=True, include_type=True),
            }
            if target_units is not None:
                source["targetUnits"] = self._serialize_model(target_units)
            body["sources"] = [source]
        if query is not None:
            body["query"] = query
        if properties is not None:
            if isinstance(properties, str):
                body["properties"] = [properties]
            else:
                body["properties"] = list(properties)
        if filter is not None:
            body["filter"] = FilterAdapter.dump_python(filter, by_alias=True, exclude_none=True)

        if sort is not None:
            body["sort"] = self._serialize_model(sort)

        if cursor is not None:
            body["cursor"] = cursor

        if debug is not None:
            body["debug"] = debug.model_dump(by_alias=True, exclude_none=True)

        if target_units is not None and view_key == "view":
            body["targetUnits"] = self._serialize_model(target_units)
        if operator is not None:
            body["operator"] = operator.upper()
        return body  # type: ignore[return-value]

    @staticmethod
    def _serialize_model(model: BaseModel | Sequence[BaseModel]) -> list[dict[str, Any]]:
        """Serialize sort specification to API format."""
        if isinstance(model, BaseModel):
            model_list = [model]
        else:
            model_list = list(model)

        return [model.model_dump(by_alias=True, exclude_none=True) for model in model_list]

    @overload
    def _retrieve(
        self,
        id: str | InstanceId | tuple[str, str],
        space: str | None = None,
    ) -> T_Instance | None: ...

    @overload
    def _retrieve(
        self,
        id: list[str | InstanceId | tuple[str, str]],
        space: str | None = None,
    ) -> T_InstanceList: ...

    def _retrieve(
        self,
        id: str | InstanceId | tuple[str, str] | list[str | InstanceId | tuple[str, str]],
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

    def _list(
        self,
        include_typing: bool = False,
        target_units: UnitConversion | Sequence[UnitConversion] | None = None,
        debug: DebugParameters | None = None,
        limit: int | None = _DEFAULT_LIST_LIMIT,
        sort: PropertySort | Sequence[PropertySort] | None = None,
        filter: Filter | None = None,
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
            debug: Return query debug notices.
            target_units: Unit conversion configuration for numeric properties with units.

        Returns:
            A list of instances matching the query.

        """
        all_items = self._list_cls()
        next_cursor: str | None = None
        total = 0
        while True:
            page_limit = self._LIST_LIMIT if limit is None else min(limit - total, self._LIST_LIMIT)
            page = self._iterate(
                include_typing=include_typing,
                target_units=target_units,
                debug=debug,
                cursor=next_cursor,
                limit=page_limit,
                sort=sort,
                filter=filter,
            )
            all_items.extend(page.items)
            total += len(page.items)
            if page.next_cursor is None or (limit is not None and total >= limit):
                break
            next_cursor = page.next_cursor
        return all_items
