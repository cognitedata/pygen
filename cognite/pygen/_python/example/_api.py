"""API classes for the example SDK.

This module contains view-specific API classes that extend InstanceAPI with
type-safe methods using unpacked parameters for common filter operations.
"""

from __future__ import annotations

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
    CategoryNode,
    CategoryNodeFilter,
    CategoryNodeList,
    ProductNode,
    ProductNodeFilter,
    ProductNodeList,
    RelatesTo,
    RelatesToFilter,
    RelatesToList,
)


def _create_property_ref(view_ref: ViewReference, property_name: str) -> list[str]:
    """Create a property reference for filtering."""
    return [view_ref.space, f"{view_ref.external_id}/{view_ref.version}", property_name]


class ProductNodeAPI(InstanceAPI[ProductNode, ProductNodeList]):
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

    def iterate(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        min_quantity: int | None = None,
        max_quantity: int | None = None,
        active: bool | None = None,
        min_created_date: date | None = None,
        max_created_date: date | None = None,
        category: str | InstanceId | tuple[str, str] | list[str | InstanceId | tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        cursor: str | None = None,
        limit: int = 25,
    ) -> Page[ProductNodeList]:
        """Iterate over ProductNode instances with pagination.

        Args:
            name: Filter by exact name or list of names.
            name_prefix: Filter by name prefix.
            min_price: Minimum price (inclusive).
            max_price: Maximum price (inclusive).
            min_quantity: Minimum quantity (inclusive).
            max_quantity: Maximum quantity (inclusive).
            active: Filter by active status.
            min_created_date: Minimum created date (inclusive).
            max_created_date: Maximum created date (inclusive).
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
        filter_.price.greater_than_or_equals(min_price).less_than_or_equals(max_price)
        filter_.quantity.greater_than_or_equals(min_quantity).less_than_or_equals(max_quantity)
        filter_.active.equals(active)
        filter_.created_date.greater_than_or_equals(min_created_date).less_than_or_equals(max_created_date)
        filter_.category.equals_or_in(category)
        filter_.external_id.prefix(external_id_prefix)
        filter_.space.equals_or_in(space)
        return self._iterate(cursor=cursor, limit=limit, filter=filter_.as_filter())

    def search(
        self,
        query: str | None = None,
        properties: str | Sequence[str] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        min_quantity: int | None = None,
        max_quantity: int | None = None,
        active: bool | None = None,
        min_created_date: date | None = None,
        max_created_date: date | None = None,
        category: str | InstanceId | tuple[str, str] | list[str | InstanceId | tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = 25,
    ) -> ProductNodeList:
        """Search ProductNode instances using full-text search.

        Args:
            query: The search query string.
            properties: Properties to search in. If None, searches all text properties.
            name: Filter by exact name or list of names.
            name_prefix: Filter by name prefix.
            min_price: Minimum price (inclusive).
            max_price: Maximum price (inclusive).
            min_quantity: Minimum quantity (inclusive).
            max_quantity: Maximum quantity (inclusive).
            active: Filter by active status.
            min_created_date: Minimum created date (inclusive).
            max_created_date: Maximum created date (inclusive).
            category: Filter by category relation.
            external_id_prefix: Filter by external ID prefix.
            space: Filter by space.
            limit: Maximum number of results.

        Returns:
            A ListResponse with matching instances.
        """
        filter_ = ProductNodeFilter("and")
        filter_.name.equals_or_in(name)
        filter_.name.prefix(name_prefix)
        filter_.price.greater_than_or_equals(min_price).less_than_or_equals(max_price)
        filter_.quantity.greater_than_or_equals(min_quantity).less_than_or_equals(max_quantity)
        filter_.active.equals(active)
        filter_.created_date.greater_than_or_equals(min_created_date).less_than_or_equals(max_created_date)
        filter_.category.equals_or_in(category)
        filter_.external_id.prefix(external_id_prefix)
        filter_.space.equals_or_in(space)
        return self._search(query=query, properties=properties, limit=limit, filter=filter_.as_filter()).items

    def aggregate(
        self,
        aggregate: Aggregation | Sequence[Aggregation],
        group_by: str | Sequence[str] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        min_quantity: int | None = None,
        max_quantity: int | None = None,
        active: bool | None = None,
        min_created_date: date | None = None,
        max_created_date: date | None = None,
        category: str | InstanceId | tuple[str, str] | list[str | InstanceId | tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
    ) -> AggregateResponse:
        """Aggregate ProductNode instances.

        Args:
            aggregate: Aggregation(s) to perform.
            group_by: Property or properties to group by.
            name: Filter by exact name or list of names.
            name_prefix: Filter by name prefix.
            min_price: Minimum price (inclusive).
            max_price: Maximum price (inclusive).
            min_quantity: Minimum quantity (inclusive).
            max_quantity: Maximum quantity (inclusive).
            active: Filter by active status.
            min_created_date: Minimum created date (inclusive).
            max_created_date: Maximum created date (inclusive).
            category: Filter by category relation.
            external_id_prefix: Filter by external ID prefix.
            space: Filter by space.

        Returns:
            AggregateResponse with aggregated values.
        """
        filter_ = ProductNodeFilter("and")
        filter_.name.equals_or_in(name)
        filter_.name.prefix(name_prefix)
        filter_.price.greater_than_or_equals(min_price).less_than_or_equals(max_price)
        filter_.quantity.greater_than_or_equals(min_quantity).less_than_or_equals(max_quantity)
        filter_.active.equals(active)
        filter_.created_date.greater_than_or_equals(min_created_date).less_than_or_equals(max_created_date)
        filter_.category.equals_or_in(category)
        filter_.external_id.prefix(external_id_prefix)
        filter_.space.equals_or_in(space)
        return self._aggregate(aggregate=aggregate, group_by=group_by, filter=filter_.as_filter())

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        min_quantity: int | None = None,
        max_quantity: int | None = None,
        active: bool | None = None,
        min_created_date: date | None = None,
        max_created_date: date | None = None,
        category: str | InstanceId | tuple[str, str] | list[str | InstanceId | tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        sort_by: str | None = None,
        sort_direction: Literal["ascending", "descending"] = "ascending",
        limit: int | None = 25,
    ) -> ProductNodeList:
        """List ProductNode instances with type-safe filtering.

        Args:
            name: Filter by exact name or list of names.
            name_prefix: Filter by name prefix.
            min_price: Minimum price (inclusive).
            max_price: Maximum price (inclusive).
            min_quantity: Minimum quantity (inclusive).
            max_quantity: Maximum quantity (inclusive).
            active: Filter by active status.
            min_created_date: Minimum created date (inclusive).
            max_created_date: Maximum created date (inclusive).
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
        filter_.price.greater_than_or_equals(min_price).less_than_or_equals(max_price)
        filter_.quantity.greater_than_or_equals(min_quantity).less_than_or_equals(max_quantity)
        filter_.active.equals(active)
        filter_.created_date.greater_than_or_equals(min_created_date).less_than_or_equals(max_created_date)
        filter_.category.equals_or_in(category)
        filter_.external_id.prefix(external_id_prefix)
        filter_.space.equals_or_in(space)
        sort = None
        if sort_by is not None:
            prop_ref = _create_property_ref(self._view_ref, sort_by)
            sort = PropertySort(property=prop_ref, direction=sort_direction)

        return self._list(limit=limit, filter=filter_.as_filter(), sort=sort)


class CategoryNodeAPI(InstanceAPI[CategoryNode, CategoryNodeList]):
    """API for CategoryNode instances with type-safe filter methods."""

    def __init__(self, http_client: HTTPClient) -> None:
        view_ref = ViewReference(space="pygen_example", external_id="CategoryNode", version="v1")
        super().__init__(http_client, view_ref, "node", CategoryNodeList)

    @overload
    def retrieve(
        self,
        id: str | InstanceId | tuple[str, str],
        space: str | None = None,
    ) -> CategoryNode | None: ...

    @overload
    def retrieve(
        self,
        id: list[str | InstanceId | tuple[str, str]],
        space: str | None = None,
    ) -> CategoryNodeList: ...

    def retrieve(
        self,
        id: str | InstanceId | tuple[str, str] | list[str | InstanceId | tuple[str, str]],
        space: str | None = None,
    ) -> CategoryNode | CategoryNodeList | None:
        """Retrieve CategoryNode instances by ID.

        Args:
            id: Instance identifier(s). Can be a string, InstanceId, tuple, or list of these.
            space: Default space to use when id is a string.

        Returns:
            For single id: The CategoryNode if found, None otherwise.
            For list of ids: A CategoryNodeList of found instances.
        """
        return self._retrieve(id, space)

    def iterate(
        self,
        category_name: str | list[str] | None = None,
        category_name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        cursor: str | None = None,
        limit: int = 25,
    ) -> Page[CategoryNodeList]:
        """Iterate over CategoryNode instances with pagination.

        Args:
            category_name: Filter by exact category name or list of names.
            category_name_prefix: Filter by category name prefix.
            external_id_prefix: Filter by external ID prefix.
            space: Filter by space.
            cursor: Pagination cursor from a previous page.
            limit: Maximum number of results per page (1-1000).

        Returns:
            A Page containing items and optional next cursor.
        """
        filter_ = CategoryNodeFilter("and")
        filter_.category_name.equals_or_in(category_name)
        filter_.category_name.prefix(category_name_prefix)
        filter_.external_id.prefix(external_id_prefix)
        filter_.space.equals_or_in(space)
        return self._iterate(cursor=cursor, limit=limit, filter=filter_.as_filter())

    def search(
        self,
        query: str | None = None,
        properties: str | Sequence[str] | None = None,
        category_name: str | list[str] | None = None,
        category_name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = 25,
    ) -> CategoryNodeList:
        """Search CategoryNode instances using full-text search.

        Args:
            query: The search query string.
            properties: Properties to search in. If None, searches all text properties.
            category_name: Filter by exact category name or list of names.
            category_name_prefix: Filter by category name prefix.
            external_id_prefix: Filter by external ID prefix.
            space: Filter by space.
            limit: Maximum number of results.

        Returns:
            A ListResponse with matching instances.
        """
        filter_ = CategoryNodeFilter("and")
        filter_.category_name.equals_or_in(category_name)
        filter_.category_name.prefix(category_name_prefix)
        filter_.external_id.prefix(external_id_prefix)
        filter_.space.equals_or_in(space)
        return self._search(query=query, properties=properties, limit=limit, filter=filter_.as_filter()).items

    def aggregate(
        self,
        aggregate: Aggregation | Sequence[Aggregation],
        group_by: str | Sequence[str] | None = None,
        category_name: str | list[str] | None = None,
        category_name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
    ) -> AggregateResponse:
        """Aggregate CategoryNode instances.

        Args:
            aggregate: Aggregation(s) to perform.
            group_by: Property or properties to group by.
            category_name: Filter by exact category name or list of names.
            category_name_prefix: Filter by category name prefix.
            external_id_prefix: Filter by external ID prefix.
            space: Filter by space.

        Returns:
            AggregateResponse with aggregated values.
        """
        filter_ = CategoryNodeFilter("and")
        filter_.category_name.equals_or_in(category_name)
        filter_.category_name.prefix(category_name_prefix)
        filter_.external_id.prefix(external_id_prefix)
        filter_.space.equals_or_in(space)
        return self._aggregate(aggregate=aggregate, group_by=group_by, filter=filter_.as_filter())

    def list(
        self,
        category_name: str | list[str] | None = None,
        category_name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        sort_by: str | None = None,
        sort_direction: Literal["ascending", "descending"] = "ascending",
        limit: int | None = 25,
    ) -> CategoryNodeList:
        """List CategoryNode instances with type-safe filtering.

        Args:
            category_name: Filter by exact category name or list of names.
            category_name_prefix: Filter by category name prefix.
            external_id_prefix: Filter by external ID prefix.
            space: Filter by space.
            sort_by: Property name to sort by.
            sort_direction: Sort direction.
            limit: Maximum number of results. None for no limit.

        Returns:
            A CategoryNodeList of matching instances.
        """
        filter_ = CategoryNodeFilter("and")
        filter_.category_name.equals_or_in(category_name)
        filter_.category_name.prefix(category_name_prefix)
        filter_.external_id.prefix(external_id_prefix)
        filter_.space.equals_or_in(space)
        sort = None
        if sort_by is not None:
            prop_ref = _create_property_ref(self._view_ref, sort_by)
            sort = PropertySort(property=prop_ref, direction=sort_direction)

        return self._list(limit=limit, filter=filter_.as_filter(), sort=sort)


class RelatesToAPI(InstanceAPI[RelatesTo, RelatesToList]):
    """API for RelatesTo edge instances with type-safe filter methods."""

    def __init__(self, http_client: HTTPClient) -> None:
        view_ref = ViewReference(space="pygen_example", external_id="RelatesTo", version="v1")
        super().__init__(http_client, view_ref, "edge", RelatesToList)

    @overload
    def retrieve(
        self,
        id: str | InstanceId | tuple[str, str],
        space: str | None = None,
    ) -> RelatesTo | None: ...

    @overload
    def retrieve(
        self,
        id: list[str | InstanceId | tuple[str, str]],
        space: str | None = None,
    ) -> RelatesToList: ...

    def retrieve(
        self,
        id: str | InstanceId | tuple[str, str] | list[str | InstanceId | tuple[str, str]],
        space: str | None = None,
    ) -> RelatesTo | RelatesToList | None:
        """Retrieve RelatesTo edge instances by ID.

        Args:
            id: Instance identifier(s). Can be a string, InstanceId, tuple, or list of these.
            space: Default space to use when id is a string.

        Returns:
            For single id: The RelatesTo if found, None otherwise.
            For list of ids: A RelatesToList of found instances.
        """
        return self._retrieve(id, space)

    def iterate(
        self,
        relation_type: str | list[str] | None = None,
        min_strength: float | None = None,
        max_strength: float | None = None,
        min_created_at: datetime | None = None,
        max_created_at: datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        cursor: str | None = None,
        limit: int = 25,
    ) -> Page[RelatesToList]:
        """Iterate over RelatesTo edge instances with pagination.

        Args:
            relation_type: Filter by exact relation type or list of types.
            min_strength: Minimum strength (inclusive).
            max_strength: Maximum strength (inclusive).
            min_created_at: Minimum created_at (inclusive).
            max_created_at: Maximum created_at (inclusive).
            external_id_prefix: Filter by external ID prefix.
            space: Filter by space.
            cursor: Pagination cursor from a previous page.
            limit: Maximum number of results per page (1-1000).

        Returns:
            A Page containing items and optional next cursor.
        """
        filter_ = RelatesToFilter("and")
        filter_.relation_type.equals_or_in(relation_type)
        filter_.strength.greater_than_or_equals(min_strength).less_than_or_equals(max_strength)
        filter_.created_at.greater_than_or_equals(min_created_at).less_than_or_equals(max_created_at)
        filter_.external_id.prefix(external_id_prefix)
        filter_.space.equals_or_in(space)
        return self._iterate(cursor=cursor, limit=limit, filter=filter_.as_filter())

    def search(
        self,
        query: str | None = None,
        properties: str | Sequence[str] | None = None,
        relation_type: str | list[str] | None = None,
        min_strength: float | None = None,
        max_strength: float | None = None,
        min_created_at: datetime | None = None,
        max_created_at: datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = 25,
    ) -> RelatesToList:
        """Search RelatesTo edge instances using full-text search.

        Args:
            query: The search query string.
            properties: Properties to search in. If None, searches all text properties.
            relation_type: Filter by exact relation type or list of types.
            min_strength: Minimum strength (inclusive).
            max_strength: Maximum strength (inclusive).
            min_created_at: Minimum created_at (inclusive).
            max_created_at: Maximum created_at (inclusive).
            external_id_prefix: Filter by external ID prefix.
            space: Filter by space.
            limit: Maximum number of results.

        Returns:
            A ListResponse with matching edges.
        """
        filter_ = RelatesToFilter("and")
        filter_.relation_type.equals_or_in(relation_type)
        filter_.strength.greater_than_or_equals(min_strength).less_than_or_equals(max_strength)
        filter_.created_at.greater_than_or_equals(min_created_at).less_than_or_equals(max_created_at)
        filter_.external_id.prefix(external_id_prefix)
        filter_.space.equals_or_in(space)
        return self._search(query=query, properties=properties, limit=limit, filter=filter_.as_filter()).items

    def aggregate(
        self,
        aggregate: Aggregation | Sequence[Aggregation],
        group_by: str | Sequence[str] | None = None,
        relation_type: str | list[str] | None = None,
        min_strength: float | None = None,
        max_strength: float | None = None,
        min_created_at: datetime | None = None,
        max_created_at: datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
    ) -> AggregateResponse:
        """Aggregate RelatesTo edge instances.

        Args:
            aggregate: Aggregation(s) to perform.
            group_by: Property or properties to group by.
            relation_type: Filter by exact relation type or list of types.
            min_strength: Minimum strength (inclusive).
            max_strength: Maximum strength (inclusive).
            min_created_at: Minimum created_at (inclusive).
            max_created_at: Maximum created_at (inclusive).
            external_id_prefix: Filter by external ID prefix.
            space: Filter by space.

        Returns:
            AggregateResponse with aggregated values.
        """
        filter_ = RelatesToFilter("and")
        filter_.relation_type.equals_or_in(relation_type)
        filter_.strength.greater_than_or_equals(min_strength).less_than_or_equals(max_strength)
        filter_.created_at.greater_than_or_equals(min_created_at).less_than_or_equals(max_created_at)
        filter_.external_id.prefix(external_id_prefix)
        filter_.space.equals_or_in(space)
        return self._aggregate(aggregate=aggregate, group_by=group_by, filter=filter_.as_filter())

    def list(
        self,
        relation_type: str | list[str] | None = None,
        min_strength: float | None = None,
        max_strength: float | None = None,
        min_created_at: datetime | None = None,
        max_created_at: datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        sort_by: str | None = None,
        sort_direction: Literal["ascending", "descending"] = "ascending",
        limit: int | None = 25,
    ) -> RelatesToList:
        """List RelatesTo edge instances with type-safe filtering.

        Args:
            relation_type: Filter by exact relation type or list of types.
            min_strength: Minimum strength (inclusive).
            max_strength: Maximum strength (inclusive).
            min_created_at: Minimum created_at (inclusive).
            max_created_at: Maximum created_at (inclusive).
            external_id_prefix: Filter by external ID prefix.
            space: Filter by space.
            sort_by: Property name to sort by.
            sort_direction: Sort direction.
            limit: Maximum number of results. None for no limit.

        Returns:
            A RelatesToList of matching edges.
        """
        filter_ = RelatesToFilter("and")
        filter_.relation_type.equals_or_in(relation_type)
        filter_.strength.greater_than_or_equals(min_strength).less_than_or_equals(max_strength)
        filter_.created_at.greater_than_or_equals(min_created_at).less_than_or_equals(max_created_at)
        filter_.external_id.prefix(external_id_prefix)
        filter_.space.equals_or_in(space)
        sort = None
        if sort_by is not None:
            prop_ref = _create_property_ref(self._view_ref, sort_by)
            sort = PropertySort(property=prop_ref, direction=sort_direction)

        return self._list(limit=limit, filter=filter_.as_filter(), sort=sort)
