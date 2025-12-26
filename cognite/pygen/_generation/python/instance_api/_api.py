"""Instance API for view-specific operations.

This module contains the InstanceAPI base class that provides methods for
querying instances (nodes/edges) through a specific view.
"""

import concurrent.futures
from collections.abc import Callable, Sequence
from typing import Any, Generic, Literal, TypeVar, overload

from pydantic import BaseModel, JsonValue, TypeAdapter

from cognite.pygen._generation.python.instance_api.http_client import (
    HTTPClient,
    HTTPResult,
    RequestMessage,
    SuccessResponse,
)
from cognite.pygen._generation.python.instance_api.models import (
    AggregatedValue,
    AggregateResult,
    Aggregation,
    AggregationGroup,
    AggregationLiteral,
    Avg,
    Count,
    InstanceId,
    Max,
    MetricAggregation,
    Min,
    Sum,
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
from cognite.pygen._generation.python.instance_api.models.responses import (
    ListResponse,
    Page,
    RetrieveResponse,
)
from cognite.pygen._utils.collection import chunker_sequence

T = TypeVar("T")


class InstanceAPI(Generic[T_Instance, T_InstanceList]):
    """Generic resource API for CDF Data Modeling view-specific operations.

    This class provides methods for querying instances (nodes or edges) through
    a specific view. It supports filtering, sorting, pagination, full-text search,
    retrieve by ID, and aggregations.

    The InstanceAPI is designed to be subclassed to create
    view-specific APIs with proper type hints.

    Args:
        http_client: The HTTP client to use for API requests.
        view_ref: Reference to the view for querying instances.
        instance_type: The type of instances to query ("node" or "edge").
        list_cls: The class to use for creating instance lists.
        retrieve_executor: Optional thread pool executor for parallel retrieve operations.
            If not provided, retrieve operations will run sequentially.
    """

    _ENDPOINT = "/models/instances"
    _LIST_ENDPOINT = f"{_ENDPOINT}/list"
    _SEARCH_ENDPOINT = f"{_ENDPOINT}/search"
    _RETRIEVE_ENDPOINT = f"{_ENDPOINT}/byids"
    _AGGREGATE_ENDPOINT = f"{_ENDPOINT}/aggregate"
    _LIST_LIMIT = 1000
    _SEARCH_LIMIT = 1000
    _RETRIEVE_LIMIT = 1000
    _AGGREGATE_LIMIT = 10000
    _DEFAULT_LIST_LIMIT = 25

    def __init__(
        self,
        http_client: HTTPClient,
        view_ref: ViewReference,
        instance_type: Literal["node", "edge"],
        list_cls: type[T_InstanceList],
        retrieve_executor: concurrent.futures.ThreadPoolExecutor | None = None,
    ) -> None:
        """Initialize the resource API.

        Args:
            http_client: The HTTP client to use for API requests.
            view_ref: Reference to the view for querying instances.
            instance_type: The type of instances to query ("node" or "edge").
            list_cls: The class to use for creating instance lists.
            retrieve_executor: Optional thread pool executor for parallel retrieve operations.
        """
        self._http_client = http_client
        self._view_ref = view_ref
        self._instance_type = instance_type
        self._list_cls = list_cls
        self._retrieve_executor = retrieve_executor
        # Create TypeAdapters at init time to avoid mypy errors with dynamic types
        self._page_adapter: TypeAdapter[Page[T_InstanceList]] = TypeAdapter(Page[list_cls])  # type: ignore[valid-type]
        self._list_response_adapter: TypeAdapter[ListResponse[T_InstanceList]] = TypeAdapter(
            ListResponse[list_cls]  # type: ignore[valid-type]
        )
        self._retrieve_response_adapter: TypeAdapter[RetrieveResponse[T_InstanceList]] = TypeAdapter(
            RetrieveResponse[list_cls]  # type: ignore[valid-type]
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
        include_typing: bool = False,
    ) -> T_Instance | None: ...

    @overload
    def _retrieve(
        self,
        id: list[str | InstanceId | tuple[str, str]],
        space: str | None = None,
        include_typing: bool = False,
    ) -> T_InstanceList: ...

    def _retrieve(
        self,
        id: str | InstanceId | tuple[str, str] | list[str | InstanceId | tuple[str, str]],
        space: str | None = None,
        include_typing: bool = False,
    ) -> T_Instance | T_InstanceList | None:
        """Retrieve instances by ID.

        This method retrieves instances by their external IDs using the byExternalIds
        API endpoint. It supports both single and batch retrieval.

        Args:
            id: Instance identifier(s). Can be:
                - A string external_id (requires space parameter)
                - An InstanceId object
                - A tuple of (space, external_id)
                - A list of any of the above
            space: Default space to use when id is a string. Required when
                providing string external_ids without explicit space.
            include_typing: If True, includes type information for direct relations.

        Returns:
            For single id: The instance if found, None otherwise.
            For list of ids: A list of found instances.

        Raises:
            ValueError: If space is not provided when using string external_ids.

        Examples:
            Retrieve a single instance:

            >>> instance = api._retrieve("my-instance", space="my-space")

            Retrieve multiple instances:

            >>> instances = api._retrieve(
            ...     [("space1", "id1"), ("space2", "id2")]
            ... )
        """
        # Normalize input to list
        is_single = not isinstance(id, list)
        id_list: list[str | InstanceId | tuple[str, str]] = [id] if is_single else id  # type: ignore[list-item, assignment]

        if not id_list:
            return self._list_cls() if not is_single else None

        # Convert all ids to InstanceId objects
        instance_ids = [self._to_instance_id(item, space) for item in id_list]

        # Retrieve instances in chunks (potentially in parallel)
        all_items = self._list_cls()
        if self._retrieve_executor is not None:
            # Parallel execution
            results = self._execute_in_parallel(
                instance_ids,
                self._RETRIEVE_LIMIT,
                self._retrieve_executor,
                lambda chunk: self._retrieve_chunk(chunk, include_typing),
            )
            for result in results:
                if isinstance(result, SuccessResponse):
                    response = self._retrieve_response_adapter.validate_json(result.body)
                    all_items.extend(response.items)
                else:
                    result.get_success_or_raise()  # Raises appropriate error
        else:
            # Sequential execution
            for chunk in chunker_sequence(instance_ids, self._RETRIEVE_LIMIT):
                result = self._retrieve_chunk(chunk, include_typing)
                success = result.get_success_or_raise()
                response = self._retrieve_response_adapter.validate_json(success.body)
                all_items.extend(response.items)

        if is_single:
            return all_items[0] if all_items else None
        return all_items

    def _retrieve_chunk(
        self,
        instance_ids: list[InstanceId],
        include_typing: bool,
    ) -> HTTPResult:
        """Retrieve a chunk of instances by their IDs.

        Args:
            instance_ids: List of InstanceId objects to retrieve.
            include_typing: Whether to include typing information.

        Returns:
            HTTPResult from the API call.
        """
        items = [
            {
                "instanceType": self._instance_type,
                "space": item.space,
                "externalId": item.external_id,
            }
            for item in instance_ids
        ]

        source: dict[str, Any] = {
            "source": self._view_ref.dump(camel_case=True, include_type=True),
        }

        body: dict[str, JsonValue] = {
            "items": items,  # type: ignore[dict-item]
            "sources": [source],
            "includeTyping": include_typing,
        }

        request = RequestMessage(
            endpoint_url=self._http_client.config.create_api_url(self._RETRIEVE_ENDPOINT),
            method="POST",
            body_content=body,
        )
        return self._http_client.request_with_retries(request)

    def _to_instance_id(
        self,
        item: str | InstanceId | tuple[str, str],
        default_space: str | None,
    ) -> InstanceId:
        """Convert various input types to InstanceId.

        Args:
            item: The item to convert.
            default_space: Default space to use if item is a string.

        Returns:
            InstanceId object.

        Raises:
            ValueError: If space cannot be determined.
        """
        if isinstance(item, InstanceId):
            return item
        if isinstance(item, tuple):
            return InstanceId(
                instance_type=self._instance_type,
                space=item[0],
                external_id=item[1],
            )
        if isinstance(item, str):
            if default_space is None:
                raise ValueError("space parameter is required when retrieving by external_id string")
            return InstanceId(
                instance_type=self._instance_type,
                space=default_space,
                external_id=item,
            )
        raise TypeError(f"Unsupported type for id: {type(item)}")

    @classmethod
    def _execute_in_parallel(
        cls,
        items: list[T],
        chunk_size: int,
        executor: concurrent.futures.ThreadPoolExecutor,
        task_fn: Callable[[list[T]], HTTPResult],
    ) -> list[HTTPResult]:
        """Execute a task function in parallel on chunked items.

        Args:
            items: List of items to process.
            chunk_size: Maximum size of each chunk.
            executor: Thread pool executor to use.
            task_fn: Function to execute on each chunk.

        Returns:
            List of HTTPResult objects from all tasks.
        """
        futures = [executor.submit(task_fn, chunk) for chunk in chunker_sequence(items, chunk_size)]
        return [future.result() for future in concurrent.futures.as_completed(futures)]

    def _aggregate(
        self,
        aggregate: Aggregation | AggregationLiteral | Sequence[Aggregation | AggregationLiteral],
        group_by: str | Sequence[str] | None = None,
        properties: str | Sequence[str] | None = None,
        query: str | None = None,
        search_properties: str | Sequence[str] | None = None,
        filter: Filter | None = None,
        limit: int = _AGGREGATE_LIMIT,
    ) -> AggregateResult:
        """Aggregate instances in the view.

        This method performs aggregations on instances using the CDF aggregate
        API endpoint. It supports various aggregation types (count, sum, avg, min, max)
        and optional grouping.

        Args:
            aggregate: The aggregation(s) to perform. Can be:
                - A single Aggregation object (Count, Sum, Avg, Min, Max)
                - A string literal ("count", "sum", "avg", "min", "max")
                - A sequence of aggregations
            group_by: Property or properties to group results by. Results will be
                organized into groups based on unique combinations of these values.
            properties: Property or properties to aggregate on. Required for sum, avg,
                min, max aggregations when using string literals.
            query: Search query for full-text search filtering before aggregation.
            search_properties: Properties to search in when using query.
            filter: Filter to apply before aggregation.
            limit: Maximum number of groups to return when using group_by.
                Defaults to 10000.

        Returns:
            AggregateResult containing the aggregated values. When using group_by,
            results are in the `grouped` field; otherwise they're in the `items` field.

        Raises:
            ValueError: If properties are required but not provided.

        Examples:
            Count all instances:

            >>> result = api._aggregate("count")

            Get average of a property:

            >>> result = api._aggregate(Avg(property="temperature"))

            Group by a property and count:

            >>> result = api._aggregate("count", group_by="category")
        """
        # Normalize aggregations
        aggregations = self._normalize_aggregations(aggregate, properties)

        # Build request body
        body = self._build_aggregate_body(
            aggregations=aggregations,
            group_by=group_by,
            query=query,
            search_properties=search_properties,
            filter=filter,
            limit=limit,
        )

        request = RequestMessage(
            endpoint_url=self._http_client.config.create_api_url(self._AGGREGATE_ENDPOINT),
            method="POST",
            body_content=body,
        )
        result = self._http_client.request_with_retries(request)
        success = result.get_success_or_raise()
        return self._parse_aggregate_response(success.body, group_by is not None)

    def _normalize_aggregations(
        self,
        aggregate: Aggregation | AggregationLiteral | Sequence[Aggregation | AggregationLiteral],
        properties: str | Sequence[str] | None,
    ) -> list[Aggregation]:
        """Normalize aggregation inputs to a list of Aggregation objects.

        Args:
            aggregate: The aggregation(s) to normalize.
            properties: Properties to use for string literal aggregations.

        Returns:
            List of Aggregation objects.

        Raises:
            ValueError: If properties are required but not provided.
        """
        AGGREGATION_CLASSES: dict[str, type[MetricAggregation]] = {
            "count": Count,
            "sum": Sum,
            "avg": Avg,
            "min": Min,
            "max": Max,
        }

        # Normalize to list
        if isinstance(aggregate, str | MetricAggregation):
            agg_list: list[Aggregation | AggregationLiteral] = [aggregate]
        else:
            agg_list = list(aggregate)

        # Normalize properties
        if isinstance(properties, str):
            prop_list: list[str] = [properties]
        elif properties is not None:
            prop_list = list(properties)
        else:
            prop_list = []

        # Convert string literals to Aggregation objects
        result: list[Aggregation] = []
        for agg in agg_list:
            if isinstance(agg, Count | Sum | Avg | Min | Max):
                result.append(agg)
            elif isinstance(agg, str):
                if agg == "count" and not prop_list:
                    # Count without properties counts externalId
                    result.append(Count(property="externalId"))
                elif not prop_list:
                    raise ValueError(f"Cannot aggregate '{agg}' without specifying properties")
                else:
                    agg_class = AGGREGATION_CLASSES.get(agg)
                    if agg_class is None:
                        raise ValueError(f"Unknown aggregation type: {agg}")
                    for prop in prop_list:
                        result.append(agg_class(property=prop))  # type: ignore[arg-type]

        return result

    def _build_aggregate_body(
        self,
        aggregations: list[Aggregation],
        group_by: str | Sequence[str] | None,
        query: str | None,
        search_properties: str | Sequence[str] | None,
        filter: Filter | None,
        limit: int,
    ) -> dict[str, JsonValue]:
        """Build the request body for aggregate operations."""
        body: dict[str, Any] = {
            "instanceType": self._instance_type,
            "view": self._view_ref.dump(camel_case=True, include_type=True),
            "aggregates": [agg.model_dump(by_alias=True, exclude_none=True) for agg in aggregations],
            "limit": limit,
        }

        if group_by is not None:
            if isinstance(group_by, str):
                body["groupBy"] = [group_by]
            else:
                body["groupBy"] = list(group_by)

        if query is not None:
            body["query"] = query

        if search_properties is not None:
            if isinstance(search_properties, str):
                body["properties"] = [search_properties]
            else:
                body["properties"] = list(search_properties)

        if filter is not None:
            body["filter"] = FilterAdapter.dump_python(filter, by_alias=True, exclude_none=True)

        return body  # type: ignore[return-value]

    def _parse_aggregate_response(self, body: str, has_groups: bool) -> AggregateResult:
        """Parse the aggregate response from the API.

        Args:
            body: The response body JSON string.
            has_groups: Whether the response contains grouped results.

        Returns:
            AggregateResult with the parsed data.
        """
        import json

        data = json.loads(body)

        if has_groups:
            # Response format with groupBy contains instanceGroups with group and aggregates
            groups = []
            for group_data in data.get("instanceGroups", []):
                aggregates = [
                    AggregatedValue(
                        aggregate=agg.get("aggregate"),
                        property=agg.get("property"),
                        value=agg.get("value"),
                    )
                    for agg in group_data.get("aggregates", [])
                ]
                groups.append(
                    AggregationGroup(
                        group=group_data.get("group", {}),
                        aggregates=aggregates,
                    )
                )
            return AggregateResult(grouped=groups)
        else:
            # Response format without groupBy contains items with aggregate, property, and value
            items = [
                AggregatedValue(
                    aggregate=item.get("aggregate"),
                    property=item.get("property"),
                    value=item.get("value"),
                )
                for item in data.get("items", [])
            ]
            return AggregateResult(items=items)

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
                Set to None to return all matching instances.
            include_typing: If True, includes type information for direct relations.
            debug: Return query debug notices.
            target_units: Unit conversion configuration for numeric properties with units.

        Returns:
            A list of instances matching the query.

        """
        if limit is not None and limit <= 0:
            raise ValueError("Limit must be a positive integer or None for no limit.")
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
