from collections.abc import Sequence
from datetime import date, datetime
from typing import Literal, overload

from cognite.pygen._python.instance_api._api import InstanceAPI
from cognite.pygen._python.instance_api.http_client import HTTPClient
from cognite.pygen._python.instance_api.models import (
    Aggregation,
    InstanceId,
    PropertySort,
    ViewReference,
)
from cognite.pygen._python.instance_api.models.responses import (
    AggregateResponse,
    Page,
)

from ._data_class import (
    ProductNode,
    ProductNodeFilter,
    ProductNodeList,
)


def _create_property_ref(view_ref: ViewReference, property_name: str) -> list[str]:
    """Create a property reference for filtering."""
    return [view_ref.space, f"{view_ref.external_id}/{view_ref.version}", property_name]


class ProductNodeApi(InstanceAPI[ProductNode, ProductNodeList]):
    """API for ProductNode instances with type-safe filter methods."""

    def __init__(self, http_client: HTTPClient) -> None:
        view_ref = ViewReference(space="pygen_example", external_id="ProductNode", version="v1")
        super().__init__(http_client, view_ref, "node", ProductNodeList)

    @overload
    def retrieve(
        self,
        id: str | InstanceId | tuple[str, str],
        space: str | None = None,
    ) -> ProductNode | None: ...

    @overload
    def retrieve(
        self,
        id: list[str | InstanceId | tuple[str, str]],
        space: str | None = None,
    ) -> ProductNodeList: ...

    def retrieve(
        self,
        id: str | InstanceId | tuple[str, str] | list[str | InstanceId | tuple[str, str]],
        space: str | None = None,
    ) -> ProductNode | ProductNodeList | None:
        """Retrieve ProductNode instances by ID.

        Args:
            id: Instance identifier(s). Can be a string, InstanceId, tuple, or list of these.
            space: Default space to use when id is a string.

        Returns:
            For single id: The ProductNode if found, None otherwise.
            For list of ids: A ProductNodeList of found instances.
        """
        return self._retrieve(id, space)

    def aggregate(
        self,
        aggregate: Aggregation | Sequence[Aggregation],
        group_by: str | Sequence[str] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        min_quantity: int | None = None,
        max_quantity: int | None = None,
        active: bool | None = None,
        min_created_date: date | None = None,
        max_created_date: date | None = None,
        min_updated_timestamp: datetime | None = None,
        max_updated_timestamp: datetime | None = None,
        category: str | InstanceId | tuple[str, str] | list[str | InstanceId | tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
    ) -> AggregateResponse:
        """Aggregate instances.

        Args:
            aggregate: Aggregation(s) to perform.
            group_by: Property or properties to group by.
            name: Filter by exact name or list of values.
            name_prefix: Filter by name prefix.
            description: Filter by exact description or list of values.
            description_prefix: Filter by description prefix.
            min_price: Minimum price (inclusive).
            max_price: Maximum price (inclusive).
            min_quantity: Minimum quantity (inclusive).
            max_quantity: Maximum quantity (inclusive).
            active: Filter by active.
            min_created_date: Minimum created date (inclusive).
            max_created_date: Maximum created date (inclusive).
            min_updated_timestamp: Minimum updated timestamp (inclusive).
            max_updated_timestamp: Maximum updated timestamp (inclusive).
            category: Filter by category relation.
            external_id_prefix: Filter by external ID prefix.
            space: Filter by space.

        Returns:
            AggregateResponse with aggregated values.
        """
        filter_ = ProductNodeFilter("and")
        filter_.name.equals_or_in(name)
        filter_.name.prefix(name_prefix)
        filter_.description.equals_or_in(description)
        filter_.description.prefix(description_prefix)
        filter_.price.greater_than_or_equals(min_price)
        filter_.price.less_than_or_equals(max_price)
        filter_.quantity.greater_than_or_equals(min_quantity)
        filter_.quantity.less_than_or_equals(max_quantity)
        filter_.active.equals(active)
        filter_.created_date.greater_than_or_equals(min_created_date)
        filter_.created_date.less_than_or_equals(max_created_date)
        filter_.updated_timestamp.greater_than_or_equals(min_updated_timestamp)
        filter_.updated_timestamp.less_than_or_equals(max_updated_timestamp)
        filter_.category.equals_or_in(category)
        filter_.external_id.prefix(external_id_prefix)
        filter_.space.equals_or_in(space)
        return self._aggregate(aggregate=aggregate, group_by=group_by, filter=filter_.as_filter())

    def search(
        self,
        query: str | None = None,
        properties: str | Sequence[str] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        min_quantity: int | None = None,
        max_quantity: int | None = None,
        active: bool | None = None,
        min_created_date: date | None = None,
        max_created_date: date | None = None,
        min_updated_timestamp: datetime | None = None,
        max_updated_timestamp: datetime | None = None,
        category: str | InstanceId | tuple[str, str] | list[str | InstanceId | tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = 25,
    ) -> ProductNodeList:
        """Search instances using full-text search.

        Args:
            query: The search query string.
            properties: Properties to search in. If None, searches all text properties.
            name: Filter by exact name or list of values.
            name_prefix: Filter by name prefix.
            description: Filter by exact description or list of values.
            description_prefix: Filter by description prefix.
            min_price: Minimum price (inclusive).
            max_price: Maximum price (inclusive).
            min_quantity: Minimum quantity (inclusive).
            max_quantity: Maximum quantity (inclusive).
            active: Filter by active.
            min_created_date: Minimum created date (inclusive).
            max_created_date: Maximum created date (inclusive).
            min_updated_timestamp: Minimum updated timestamp (inclusive).
            max_updated_timestamp: Maximum updated timestamp (inclusive).
            category: Filter by category relation.
            external_id_prefix: Filter by external ID prefix.
            space: Filter by space.
            limit: Maximum number of results.

        Returns:
            A ProductNodeList with matching instances.
        """
        filter_ = ProductNodeFilter("and")
        filter_.name.equals_or_in(name)
        filter_.name.prefix(name_prefix)
        filter_.description.equals_or_in(description)
        filter_.description.prefix(description_prefix)
        filter_.price.greater_than_or_equals(min_price)
        filter_.price.less_than_or_equals(max_price)
        filter_.quantity.greater_than_or_equals(min_quantity)
        filter_.quantity.less_than_or_equals(max_quantity)
        filter_.active.equals(active)
        filter_.created_date.greater_than_or_equals(min_created_date)
        filter_.created_date.less_than_or_equals(max_created_date)
        filter_.updated_timestamp.greater_than_or_equals(min_updated_timestamp)
        filter_.updated_timestamp.less_than_or_equals(max_updated_timestamp)
        filter_.category.equals_or_in(category)
        filter_.external_id.prefix(external_id_prefix)
        filter_.space.equals_or_in(space)
        return self._search(query=query, properties=properties, limit=limit, filter=filter_.as_filter()).items

    def iterate(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        min_quantity: int | None = None,
        max_quantity: int | None = None,
        active: bool | None = None,
        min_created_date: date | None = None,
        max_created_date: date | None = None,
        min_updated_timestamp: datetime | None = None,
        max_updated_timestamp: datetime | None = None,
        category: str | InstanceId | tuple[str, str] | list[str | InstanceId | tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        cursor: str | None = None,
        limit: int = 25,
    ) -> Page[ProductNodeList]:
        """Iterate over instances with pagination.

        Args:
            name: Filter by exact name or list of values.
            name_prefix: Filter by name prefix.
            description: Filter by exact description or list of values.
            description_prefix: Filter by description prefix.
            min_price: Minimum price (inclusive).
            max_price: Maximum price (inclusive).
            min_quantity: Minimum quantity (inclusive).
            max_quantity: Maximum quantity (inclusive).
            active: Filter by active.
            min_created_date: Minimum created date (inclusive).
            max_created_date: Maximum created date (inclusive).
            min_updated_timestamp: Minimum updated timestamp (inclusive).
            max_updated_timestamp: Maximum updated timestamp (inclusive).
            category: Filter by category relation.
            external_id_prefix: Filter by external ID prefix.
            space: Filter by space.
            cursor: Pagination cursor from a previous page.
            limit: Maximum number of results per page (1-1000).

        Returns:
            A Page containing items and optional next cursor.
        """
        filter_ = ProductNodeFilter("and")
        filter_.name.equals_or_in(name)
        filter_.name.prefix(name_prefix)
        filter_.description.equals_or_in(description)
        filter_.description.prefix(description_prefix)
        filter_.price.greater_than_or_equals(min_price)
        filter_.price.less_than_or_equals(max_price)
        filter_.quantity.greater_than_or_equals(min_quantity)
        filter_.quantity.less_than_or_equals(max_quantity)
        filter_.active.equals(active)
        filter_.created_date.greater_than_or_equals(min_created_date)
        filter_.created_date.less_than_or_equals(max_created_date)
        filter_.updated_timestamp.greater_than_or_equals(min_updated_timestamp)
        filter_.updated_timestamp.less_than_or_equals(max_updated_timestamp)
        filter_.category.equals_or_in(category)
        filter_.external_id.prefix(external_id_prefix)
        filter_.space.equals_or_in(space)
        return self._iterate(cursor=cursor, limit=limit, filter=filter_.as_filter())

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        min_quantity: int | None = None,
        max_quantity: int | None = None,
        active: bool | None = None,
        min_created_date: date | None = None,
        max_created_date: date | None = None,
        min_updated_timestamp: datetime | None = None,
        max_updated_timestamp: datetime | None = None,
        category: str | InstanceId | tuple[str, str] | list[str | InstanceId | tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        sort_by: str | None = None,
        sort_direction: Literal["ascending", "descending"] = "ascending",
        limit: int | None = 25,
    ) -> ProductNodeList:
        """List instances with type-safe filtering.

        Args:
            name: Filter by exact name or list of values.
            name_prefix: Filter by name prefix.
            description: Filter by exact description or list of values.
            description_prefix: Filter by description prefix.
            min_price: Minimum price (inclusive).
            max_price: Maximum price (inclusive).
            min_quantity: Minimum quantity (inclusive).
            max_quantity: Maximum quantity (inclusive).
            active: Filter by active.
            min_created_date: Minimum created date (inclusive).
            max_created_date: Maximum created date (inclusive).
            min_updated_timestamp: Minimum updated timestamp (inclusive).
            max_updated_timestamp: Maximum updated timestamp (inclusive).
            category: Filter by category relation.
            external_id_prefix: Filter by external ID prefix.
            space: Filter by space.
            sort_by: Property name to sort by.
            sort_direction: Sort direction.
            limit: Maximum number of results. None for no limit.

        Returns:
            A ProductNodeList of matching instances.
        """
        filter_ = ProductNodeFilter("and")
        filter_.name.equals_or_in(name)
        filter_.name.prefix(name_prefix)
        filter_.description.equals_or_in(description)
        filter_.description.prefix(description_prefix)
        filter_.price.greater_than_or_equals(min_price)
        filter_.price.less_than_or_equals(max_price)
        filter_.quantity.greater_than_or_equals(min_quantity)
        filter_.quantity.less_than_or_equals(max_quantity)
        filter_.active.equals(active)
        filter_.created_date.greater_than_or_equals(min_created_date)
        filter_.created_date.less_than_or_equals(max_created_date)
        filter_.updated_timestamp.greater_than_or_equals(min_updated_timestamp)
        filter_.updated_timestamp.less_than_or_equals(max_updated_timestamp)
        filter_.category.equals_or_in(category)
        filter_.external_id.prefix(external_id_prefix)
        filter_.space.equals_or_in(space)
        sort = None
        if sort_by is not None:
            prop_ref = _create_property_ref(self._view_ref, sort_by)
            sort = PropertySort(property=prop_ref, direction=sort_direction)

        return self._list(limit=limit, filter=filter_.as_filter(), sort=sort)
