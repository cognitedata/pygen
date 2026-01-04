from collections.abc import Sequence
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
)


def _create_property_ref(view_ref: ViewReference, property_name: str) -> list[str]:
    """Create a property reference for filtering."""
    return [view_ref.space, f"{view_ref.external_id}/{view_ref.version}", property_name]


class CategoryNodeApi(InstanceAPI[CategoryNode, CategoryNodeList]):
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

    def aggregate(
        self,
        aggregate: Aggregation | Sequence[Aggregation],
        group_by: str | Sequence[str] | None = None,
        category_name: str | list[str] | None = None,
        category_name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
    ) -> AggregateResponse:
        """Aggregate instances.

        Args:
            aggregate: Aggregation(s) to perform.
            group_by: Property or properties to group by.
            category_name: Filter by exact category name or list of values.
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
        """Search instances using full-text search.

        Args:
            query: The search query string.
            properties: Properties to search in. If None, searches all text properties.
            category_name: Filter by exact category name or list of values.
            category_name_prefix: Filter by category name prefix.
            external_id_prefix: Filter by external ID prefix.
            space: Filter by space.
            limit: Maximum number of results.

        Returns:
            A CategoryNodeList with matching instances.
        """
        filter_ = CategoryNodeFilter("and")
        filter_.category_name.equals_or_in(category_name)
        filter_.category_name.prefix(category_name_prefix)
        filter_.external_id.prefix(external_id_prefix)
        filter_.space.equals_or_in(space)
        return self._search(query=query, properties=properties, limit=limit, filter=filter_.as_filter()).items

    def iterate(
        self,
        category_name: str | list[str] | None = None,
        category_name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        cursor: str | None = None,
        limit: int = 25,
    ) -> Page[CategoryNodeList]:
        """Iterate over instances with pagination.

        Args:
            category_name: Filter by exact category name or list of values.
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
        """List instances with type-safe filtering.

        Args:
            category_name: Filter by exact category name or list of values.
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
