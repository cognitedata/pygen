"""API classes for the example SDK.

This module contains view-specific API classes that extend InstanceAPI with
type-safe methods using unpacked parameters for common filter operations.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime
from typing import Literal, overload

from cognite.pygen._generation.python.instance_api._api import InstanceAPI
from cognite.pygen._generation.python.instance_api.http_client import HTTPClient
from cognite.pygen._generation.python.instance_api.models import (
    Aggregation,
    InstanceId,
    PropertySort,
    ViewReference,
)
from cognite.pygen._generation.python.instance_api.models.filters import (
    AndFilter,
    EqualsFilterData,
    Filter,
    InFilterData,
    RangeFilterData,
)
from cognite.pygen._generation.python.instance_api.models.responses import (
    AggregateResponse,
    ListResponse,
    Page,
)

from ._data_class import (
    CategoryNode,
    CategoryNodeList,
    ProductNode,
    ProductNodeList,
    RelatesTo,
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

    def _build_filter(
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
        filter: Filter | None = None,
    ) -> Filter | None:
        """Build a filter from the provided parameters."""
        filters: list[Filter] = []

        # Name filter
        if name is not None:
            prop_ref = _create_property_ref(self._view_ref, "name")
            if isinstance(name, str):
                filters.append({"equals": EqualsFilterData(property=prop_ref, value=name)})
            else:
                filters.append({"in": InFilterData(property=prop_ref, values=name)})

        # Name prefix filter
        if name_prefix is not None:
            prop_ref = _create_property_ref(self._view_ref, "name")
            filters.append({"prefix": {"property": prop_ref, "value": name_prefix}})

        # Price range filter
        if min_price is not None or max_price is not None:
            prop_ref = _create_property_ref(self._view_ref, "price")
            range_data = RangeFilterData(property=prop_ref)
            if min_price is not None:
                range_data.gte = min_price
            if max_price is not None:
                range_data.lte = max_price
            filters.append({"range": range_data})

        # Quantity range filter
        if min_quantity is not None or max_quantity is not None:
            prop_ref = _create_property_ref(self._view_ref, "quantity")
            range_data = RangeFilterData(property=prop_ref)
            if min_quantity is not None:
                range_data.gte = min_quantity
            if max_quantity is not None:
                range_data.lte = max_quantity
            filters.append({"range": range_data})

        # Active filter
        if active is not None:
            prop_ref = _create_property_ref(self._view_ref, "active")
            filters.append({"equals": EqualsFilterData(property=prop_ref, value=active)})

        # Created date range filter
        if min_created_date is not None or max_created_date is not None:
            prop_ref = _create_property_ref(self._view_ref, "createdDate")
            range_data = RangeFilterData(property=prop_ref)
            if min_created_date is not None:
                range_data.gte = min_created_date.isoformat()
            if max_created_date is not None:
                range_data.lte = max_created_date.isoformat()
            filters.append({"range": range_data})

        # Category filter (direct relation)
        if category is not None:
            prop_ref = _create_property_ref(self._view_ref, "category")
            if isinstance(category, list):
                # Convert list items to space/externalId dicts
                values = []
                for item in category:
                    if isinstance(item, InstanceId):
                        values.append({"space": item.space, "externalId": item.external_id})
                    elif isinstance(item, tuple):
                        values.append({"space": item[0], "externalId": item[1]})
                    else:
                        # Assume string is external_id in default space
                        values.append({"space": self._view_ref.space, "externalId": item})
                filters.append({"in": InFilterData(property=prop_ref, values=values)})
            else:
                if isinstance(category, InstanceId):
                    value = {"space": category.space, "externalId": category.external_id}
                elif isinstance(category, tuple):
                    value = {"space": category[0], "externalId": category[1]}
                else:
                    value = {"space": self._view_ref.space, "externalId": category}
                filters.append({"equals": EqualsFilterData(property=prop_ref, value=value)})

        # External ID prefix filter
        if external_id_prefix is not None:
            filters.append({"prefix": {"property": ["node", "externalId"], "value": external_id_prefix}})

        # Space filter
        if space is not None:
            if isinstance(space, str):
                filters.append({"equals": EqualsFilterData(property=["node", "space"], value=space)})
            else:
                filters.append({"in": InFilterData(property=["node", "space"], values=space)})

        # Additional custom filter
        if filter is not None:
            filters.append(filter)

        if not filters:
            return None
        if len(filters) == 1:
            return filters[0]
        return {"and": AndFilter(data=filters)}

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
        filter: Filter | None = None,
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
            filter: Additional custom filter to combine with the above.

        Returns:
            A ProductNodeList of matching instances.
        """
        combined_filter = self._build_filter(
            name=name,
            name_prefix=name_prefix,
            min_price=min_price,
            max_price=max_price,
            min_quantity=min_quantity,
            max_quantity=max_quantity,
            active=active,
            min_created_date=min_created_date,
            max_created_date=max_created_date,
            category=category,
            external_id_prefix=external_id_prefix,
            space=space,
            filter=filter,
        )
        sort = None
        if sort_by is not None:
            prop_ref = _create_property_ref(self._view_ref, sort_by)
            sort = PropertySort(property=prop_ref, direction=sort_direction)

        return self._list(limit=limit, filter=combined_filter, sort=sort)

    def iterate(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        active: bool | None = None,
        category: str | InstanceId | tuple[str, str] | list[str | InstanceId | tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        cursor: str | None = None,
        limit: int = 25,
        filter: Filter | None = None,
    ) -> Page[ProductNodeList]:
        """Iterate over ProductNode instances with pagination.

        Args:
            name: Filter by exact name or list of names.
            name_prefix: Filter by name prefix.
            min_price: Minimum price (inclusive).
            max_price: Maximum price (inclusive).
            active: Filter by active status.
            category: Filter by category relation.
            external_id_prefix: Filter by external ID prefix.
            space: Filter by space.
            cursor: Pagination cursor from a previous page.
            limit: Maximum number of results per page (1-1000).
            filter: Additional custom filter to combine with the above.

        Returns:
            A Page containing items and optional next cursor.
        """
        combined_filter = self._build_filter(
            name=name,
            name_prefix=name_prefix,
            min_price=min_price,
            max_price=max_price,
            active=active,
            category=category,
            external_id_prefix=external_id_prefix,
            space=space,
            filter=filter,
        )
        return self._iterate(cursor=cursor, limit=limit, filter=combined_filter)

    def search(
        self,
        query: str,
        properties: str | Sequence[str] | None = None,
        limit: int = 25,
        filter: Filter | None = None,
    ) -> ListResponse[ProductNodeList]:
        """Search ProductNode instances using full-text search.

        Args:
            query: The search query string.
            properties: Properties to search in. If None, searches all text properties.
            limit: Maximum number of results.
            filter: Additional filter to apply.

        Returns:
            A ListResponse with matching instances.
        """
        return self._search(query=query, properties=properties, limit=limit, filter=filter)

    def aggregate(
        self,
        aggregate: Aggregation | Sequence[Aggregation],
        group_by: str | Sequence[str] | None = None,
        filter: Filter | None = None,
    ) -> AggregateResponse:
        """Aggregate ProductNode instances.

        Args:
            aggregate: Aggregation(s) to perform.
            group_by: Property or properties to group by.
            filter: Filter to apply before aggregation.

        Returns:
            AggregateResponse with aggregated values.
        """
        return self._aggregate(aggregate=aggregate, group_by=group_by, filter=filter)


class CategoryNodeAPI(InstanceAPI[CategoryNode, CategoryNodeList]):
    """API for CategoryNode instances with type-safe filter methods."""

    def __init__(self, http_client: HTTPClient) -> None:
        view_ref = ViewReference(space="pygen_example", external_id="CategoryNode", version="v1")
        super().__init__(http_client, view_ref, "node", CategoryNodeList)

    def _build_filter(
        self,
        category_name: str | list[str] | None = None,
        category_name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        filter: Filter | None = None,
    ) -> Filter | None:
        """Build a filter from the provided parameters."""
        filters: list[Filter] = []

        # Category name filter
        if category_name is not None:
            prop_ref = _create_property_ref(self._view_ref, "categoryName")
            if isinstance(category_name, str):
                filters.append({"equals": EqualsFilterData(property=prop_ref, value=category_name)})
            else:
                filters.append({"in": InFilterData(property=prop_ref, values=category_name)})

        # Category name prefix filter
        if category_name_prefix is not None:
            prop_ref = _create_property_ref(self._view_ref, "categoryName")
            filters.append({"prefix": {"property": prop_ref, "value": category_name_prefix}})

        # External ID prefix filter
        if external_id_prefix is not None:
            filters.append({"prefix": {"property": ["node", "externalId"], "value": external_id_prefix}})

        # Space filter
        if space is not None:
            if isinstance(space, str):
                filters.append({"equals": EqualsFilterData(property=["node", "space"], value=space)})
            else:
                filters.append({"in": InFilterData(property=["node", "space"], values=space)})

        # Additional custom filter
        if filter is not None:
            filters.append(filter)

        if not filters:
            return None
        if len(filters) == 1:
            return filters[0]
        return {"and": AndFilter(data=filters)}

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

    def list(
        self,
        category_name: str | list[str] | None = None,
        category_name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        sort_by: str | None = None,
        sort_direction: Literal["ascending", "descending"] = "ascending",
        limit: int | None = 25,
        filter: Filter | None = None,
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
            filter: Additional custom filter.

        Returns:
            A CategoryNodeList of matching instances.
        """
        combined_filter = self._build_filter(
            category_name=category_name,
            category_name_prefix=category_name_prefix,
            external_id_prefix=external_id_prefix,
            space=space,
            filter=filter,
        )
        sort = None
        if sort_by is not None:
            prop_ref = _create_property_ref(self._view_ref, sort_by)
            sort = PropertySort(property=prop_ref, direction=sort_direction)

        return self._list(limit=limit, filter=combined_filter, sort=sort)

    def iterate(
        self,
        category_name: str | list[str] | None = None,
        category_name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        cursor: str | None = None,
        limit: int = 25,
        filter: Filter | None = None,
    ) -> Page[CategoryNodeList]:
        """Iterate over CategoryNode instances with pagination.

        Args:
            category_name: Filter by exact category name or list of names.
            category_name_prefix: Filter by category name prefix.
            external_id_prefix: Filter by external ID prefix.
            space: Filter by space.
            cursor: Pagination cursor from a previous page.
            limit: Maximum number of results per page (1-1000).
            filter: Additional custom filter.

        Returns:
            A Page containing items and optional next cursor.
        """
        combined_filter = self._build_filter(
            category_name=category_name,
            category_name_prefix=category_name_prefix,
            external_id_prefix=external_id_prefix,
            space=space,
            filter=filter,
        )
        return self._iterate(cursor=cursor, limit=limit, filter=combined_filter)

    def search(
        self,
        query: str,
        properties: str | Sequence[str] | None = None,
        limit: int = 25,
        filter: Filter | None = None,
    ) -> ListResponse[CategoryNodeList]:
        """Search CategoryNode instances using full-text search.

        Args:
            query: The search query string.
            properties: Properties to search in. If None, searches all text properties.
            limit: Maximum number of results.
            filter: Additional filter to apply.

        Returns:
            A ListResponse with matching instances.
        """
        return self._search(query=query, properties=properties, limit=limit, filter=filter)

    def aggregate(
        self,
        aggregate: Aggregation | Sequence[Aggregation],
        group_by: str | Sequence[str] | None = None,
        filter: Filter | None = None,
    ) -> AggregateResponse:
        """Aggregate CategoryNode instances.

        Args:
            aggregate: Aggregation(s) to perform.
            group_by: Property or properties to group by.
            filter: Filter to apply before aggregation.

        Returns:
            AggregateResponse with aggregated values.
        """
        return self._aggregate(aggregate=aggregate, group_by=group_by, filter=filter)


class RelatesToAPI(InstanceAPI[RelatesTo, RelatesToList]):
    """API for RelatesTo edge instances with type-safe filter methods."""

    def __init__(self, http_client: HTTPClient) -> None:
        view_ref = ViewReference(space="pygen_example", external_id="RelatesTo", version="v1")
        super().__init__(http_client, view_ref, "edge", RelatesToList)

    def _build_filter(
        self,
        relation_type: str | list[str] | None = None,
        min_strength: float | None = None,
        max_strength: float | None = None,
        min_created_at: datetime | None = None,
        max_created_at: datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        filter: Filter | None = None,
    ) -> Filter | None:
        """Build a filter from the provided parameters."""
        filters: list[Filter] = []

        # Relation type filter
        if relation_type is not None:
            prop_ref = _create_property_ref(self._view_ref, "relationType")
            if isinstance(relation_type, str):
                filters.append({"equals": EqualsFilterData(property=prop_ref, value=relation_type)})
            else:
                filters.append({"in": InFilterData(property=prop_ref, values=relation_type)})

        # Strength range filter
        if min_strength is not None or max_strength is not None:
            prop_ref = _create_property_ref(self._view_ref, "strength")
            range_data = RangeFilterData(property=prop_ref)
            if min_strength is not None:
                range_data.gte = min_strength
            if max_strength is not None:
                range_data.lte = max_strength
            filters.append({"range": range_data})

        # Created at range filter
        if min_created_at is not None or max_created_at is not None:
            prop_ref = _create_property_ref(self._view_ref, "createdAt")
            range_data = RangeFilterData(property=prop_ref)
            if min_created_at is not None:
                range_data.gte = min_created_at.isoformat(timespec="milliseconds")
            if max_created_at is not None:
                range_data.lte = max_created_at.isoformat(timespec="milliseconds")
            filters.append({"range": range_data})

        # External ID prefix filter
        if external_id_prefix is not None:
            filters.append({"prefix": {"property": ["edge", "externalId"], "value": external_id_prefix}})

        # Space filter
        if space is not None:
            if isinstance(space, str):
                filters.append({"equals": EqualsFilterData(property=["edge", "space"], value=space)})
            else:
                filters.append({"in": InFilterData(property=["edge", "space"], values=space)})

        # Additional custom filter
        if filter is not None:
            filters.append(filter)

        if not filters:
            return None
        if len(filters) == 1:
            return filters[0]
        return {"and": AndFilter(data=filters)}

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
        filter: Filter | None = None,
    ) -> RelatesToList:
        """List RelatesTo edge instances with type-safe filtering.

        Args:
            relation_type: Filter by exact relation type or list of types.
            min_strength: Minimum strength (inclusive).
            max_strength: Maximum strength (inclusive).
            min_created_at: Minimum created_at timestamp (inclusive).
            max_created_at: Maximum created_at timestamp (inclusive).
            external_id_prefix: Filter by external ID prefix.
            space: Filter by space.
            sort_by: Property name to sort by.
            sort_direction: Sort direction.
            limit: Maximum number of results. None for no limit.
            filter: Additional custom filter.

        Returns:
            A RelatesToList of matching edges.
        """
        combined_filter = self._build_filter(
            relation_type=relation_type,
            min_strength=min_strength,
            max_strength=max_strength,
            min_created_at=min_created_at,
            max_created_at=max_created_at,
            external_id_prefix=external_id_prefix,
            space=space,
            filter=filter,
        )
        sort = None
        if sort_by is not None:
            prop_ref = _create_property_ref(self._view_ref, sort_by)
            sort = PropertySort(property=prop_ref, direction=sort_direction)

        return self._list(limit=limit, filter=combined_filter, sort=sort)

    def iterate(
        self,
        relation_type: str | list[str] | None = None,
        min_strength: float | None = None,
        max_strength: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        cursor: str | None = None,
        limit: int = 25,
        filter: Filter | None = None,
    ) -> Page[RelatesToList]:
        """Iterate over RelatesTo edge instances with pagination.

        Args:
            relation_type: Filter by exact relation type or list of types.
            min_strength: Minimum strength (inclusive).
            max_strength: Maximum strength (inclusive).
            external_id_prefix: Filter by external ID prefix.
            space: Filter by space.
            cursor: Pagination cursor from a previous page.
            limit: Maximum number of results per page (1-1000).
            filter: Additional custom filter.

        Returns:
            A Page containing items and optional next cursor.
        """
        combined_filter = self._build_filter(
            relation_type=relation_type,
            min_strength=min_strength,
            max_strength=max_strength,
            external_id_prefix=external_id_prefix,
            space=space,
            filter=filter,
        )
        return self._iterate(cursor=cursor, limit=limit, filter=combined_filter)

    def search(
        self,
        query: str,
        properties: str | Sequence[str] | None = None,
        limit: int = 25,
        filter: Filter | None = None,
    ) -> ListResponse[RelatesToList]:
        """Search RelatesTo edge instances using full-text search.

        Args:
            query: The search query string.
            properties: Properties to search in. If None, searches all text properties.
            limit: Maximum number of results.
            filter: Additional filter to apply.

        Returns:
            A ListResponse with matching edges.
        """
        return self._search(query=query, properties=properties, limit=limit, filter=filter)

    def aggregate(
        self,
        aggregate: Aggregation | Sequence[Aggregation],
        group_by: str | Sequence[str] | None = None,
        filter: Filter | None = None,
    ) -> AggregateResponse:
        """Aggregate RelatesTo edge instances.

        Args:
            aggregate: Aggregation(s) to perform.
            group_by: Property or properties to group by.
            filter: Filter to apply before aggregation.

        Returns:
            AggregateResponse with aggregated values.
        """
        return self._aggregate(aggregate=aggregate, group_by=group_by, filter=filter)
