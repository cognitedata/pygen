from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import Any, ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from field.client._api._core import (
    DEFAULT_LIMIT_READ,
    instantiate_classes,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from field.client.data_classes._core import (
    DEFAULT_QUERY_LIMIT,
    QueryStepFactory,
    QueryBuilder,
    QueryUnpacker,
    ViewPropertyId,
)
from field.client.data_classes._area import (
    AreaQuery,
    _create_area_filter,
)
from field.client.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    Area,
    AreaWrite,
    AreaFields,
    AreaList,
    AreaWriteList,
    AreaTextFields,
    Field,
)
from field.client._api.area_query import AreaQueryAPI


class AreaAPI(NodeAPI[Area, AreaWrite, AreaList, AreaWriteList]):
    _view_id = dm.ViewId("fields-space", "Area", "d6aca0459d82b7")
    _properties_by_field: ClassVar[dict[str, str]] = {}
    _class_type = Area
    _class_list = AreaList
    _class_write_list = AreaWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    def __call__(
        self,
        field: (
            tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> AreaQueryAPI[Area, AreaList]:
        """Query starting at areas.

        Args:
            field: The field to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of areas to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for areas.

        """
        warnings.warn(
            "This method is deprecated and will soon be removed. " "Use the .select() method instead.",
            UserWarning,
            stacklevel=2,
        )
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_area_filter(
            self._view_id,
            field,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        return AreaQueryAPI(self._client, QueryBuilder(), self._class_type, self._class_list, None, filter_, limit)

    def apply(
        self,
        area: AreaWrite | Sequence[AreaWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) areas.

        Args:
            area: Area or
                sequence of areas to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and
                existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)?
                Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None.
                However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new area:

                >>> from field.client import FieldClient
                >>> from field.client.data_classes import AreaWrite
                >>> client = FieldClient()
                >>> area = AreaWrite(
                ...     external_id="my_area", ...
                ... )
                >>> result = client.area.apply(area)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.area.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(area, replace, write_none)

    def delete(self, external_id: str | SequenceNotStr[str], space: str) -> dm.InstancesDeleteResult:
        """Delete one or more area.

        Args:
            external_id: External id of the area to delete.
            space: The space where all the area are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete area by id:

                >>> from field.client import FieldClient
                >>> client = FieldClient()
                >>> client.area.delete("my_area")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.area.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> Area | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> AreaList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> Area | AreaList | None:
        """Retrieve one or more areas by id(s).

        Args:
            external_id: External id or list of external ids of the areas.
            space: The space where all the areas are located.
            retrieve_connections: Whether to retrieve `field` for the areas. Defaults to 'skip'.'skip' will not retrieve
            any connections, 'identifier' will only retrieve the identifier of the connected items, and 'full' will
            retrieve the full connected items.

        Returns:
            The requested areas.

        Examples:

            Retrieve area by id:

                >>> from field.client import FieldClient
                >>> client = FieldClient()
                >>> area = client.area.retrieve(
                ...     "my_area"
                ... )

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_connections=retrieve_connections,
        )

    def search(
        self,
        query: str,
        properties: AreaTextFields | SequenceNotStr[AreaTextFields] | None = None,
        field: (
            tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: AreaFields | SequenceNotStr[AreaFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> AreaList:
        """Search areas

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            field: The field to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of areas to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results areas matching the query.

        Examples:

           Search for 'my_area' in all text properties:

                >>> from field.client import FieldClient
                >>> client = FieldClient()
                >>> areas = client.area.search(
                ...     'my_area'
                ... )

        """
        filter_ = _create_area_filter(
            self._view_id,
            field,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            query=query,
            properties=properties,
            filter_=filter_,
            limit=limit,
            sort_by=sort_by,  # type: ignore[arg-type]
            direction=direction,
            sort=sort,
        )

    @overload
    def aggregate(
        self,
        aggregate: Aggregations | dm.aggregations.MetricAggregation,
        group_by: None = None,
        property: AreaFields | SequenceNotStr[AreaFields] | None = None,
        field: (
            tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.AggregatedNumberedValue: ...

    @overload
    def aggregate(
        self,
        aggregate: SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation],
        group_by: None = None,
        property: AreaFields | SequenceNotStr[AreaFields] | None = None,
        field: (
            tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]: ...

    @overload
    def aggregate(
        self,
        aggregate: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation]
        ),
        group_by: AreaFields | SequenceNotStr[AreaFields],
        property: AreaFields | SequenceNotStr[AreaFields] | None = None,
        field: (
            tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList: ...

    def aggregate(
        self,
        aggregate: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation]
        ),
        group_by: AreaFields | SequenceNotStr[AreaFields] | None = None,
        property: AreaFields | SequenceNotStr[AreaFields] | None = None,
        field: (
            tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across areas

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            field: The field to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of areas to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count areas in space `my_space`:

                >>> from field.client import FieldClient
                >>> client = FieldClient()
                >>> result = client.area.aggregate("count", space="my_space")

        """

        filter_ = _create_area_filter(
            self._view_id,
            field,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            aggregate=aggregate,
            group_by=group_by,  # type: ignore[arg-type]
            properties=property,  # type: ignore[arg-type]
            query=None,
            search_properties=None,
            limit=limit,
            filter=filter_,
        )

    def histogram(
        self,
        property: AreaFields,
        interval: float,
        field: (
            tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for areas

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            field: The field to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of areas to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_area_filter(
            self._view_id,
            field,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            property,
            interval,
            None,
            None,
            limit,
            filter_,
        )

    def select(self) -> AreaQuery:
        """Start selecting from areas."""
        return AreaQuery(self._client)

    def _query(
        self,
        filter_: dm.Filter | None,
        limit: int,
        retrieve_connections: Literal["skip", "identifier", "full"],
        sort: list[InstanceSort] | None = None,
    ) -> list[dict[str, Any]]:
        builder = QueryBuilder()
        factory = QueryStepFactory(builder.create_name, view_id=self._view_id, edge_connection_property="end_node")
        builder.append(
            factory.root(
                filter=filter_,
                limit=limit,
                has_container_fields=True,
            )
        )
        if retrieve_connections == "full":
            builder.extend(
                factory.from_direct_relation(
                    Field._view_id,
                    ViewPropertyId(self._view_id, "field"),
                    has_container_fields=True,
                )
            )
        unpack_edges: Literal["skip", "identifier"] = "identifier" if retrieve_connections == "identifier" else "skip"
        builder.execute_query(self._client, remove_not_connected=True if unpack_edges == "skip" else False)
        return QueryUnpacker(builder, edges=unpack_edges).unpack()

    def list(
        self,
        field: (
            tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> AreaList:
        """List/filter areas

        Args:
            field: The field to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of areas to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            retrieve_connections: Whether to retrieve `field` for the areas. Defaults to 'skip'.'skip' will not retrieve
            any connections, 'identifier' will only retrieve the identifier of the connected items, and 'full' will
            retrieve the full connected items.

        Returns:
            List of requested areas

        Examples:

            List areas and limit to 5:

                >>> from field.client import FieldClient
                >>> client = FieldClient()
                >>> areas = client.area.list(limit=5)

        """
        filter_ = _create_area_filter(
            self._view_id,
            field,
            external_id_prefix,
            space,
            filter,
        )
        if retrieve_connections == "skip":
            return self._list(limit=limit, filter=filter_)
        values = self._query(filter_, limit, retrieve_connections)
        return self._class_list(instantiate_classes(self._class_type, values, "list"))
