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
from field.client.data_classes._region import (
    RegionQuery,
    _create_region_filter,
)
from field.client.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    Region,
    RegionWrite,
    RegionFields,
    RegionList,
    RegionWriteList,
    RegionTextFields,
    Area,
)
from field.client._api.region_query import RegionQueryAPI


class RegionAPI(NodeAPI[Region, RegionWrite, RegionList, RegionWriteList]):
    _view_id = dm.ViewId("fields-space", "Region", "841accf72d9b06")
    _properties_by_field: ClassVar[dict[str, str]] = {}
    _class_type = Region
    _class_list = RegionList
    _class_write_list = RegionWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    def __call__(
        self,
        area: (
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
    ) -> RegionQueryAPI[Region, RegionList]:
        """Query starting at regions.

        Args:
            area: The area to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of regions to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for regions.

        """
        warnings.warn(
            "This method is deprecated and will soon be removed. " "Use the .select() method instead.",
            UserWarning,
            stacklevel=2,
        )
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_region_filter(
            self._view_id,
            area,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        return RegionQueryAPI(self._client, QueryBuilder(), self._class_type, self._class_list, None, filter_, limit)

    def apply(
        self,
        region: RegionWrite | Sequence[RegionWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) regions.

        Args:
            region: Region or
                sequence of regions to upsert.
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

            Create a new region:

                >>> from field.client import FieldClient
                >>> from field.client.data_classes import RegionWrite
                >>> client = FieldClient()
                >>> region = RegionWrite(
                ...     external_id="my_region", ...
                ... )
                >>> result = client.region.apply(region)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.region.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(region, replace, write_none)

    def delete(self, external_id: str | SequenceNotStr[str], space: str) -> dm.InstancesDeleteResult:
        """Delete one or more region.

        Args:
            external_id: External id of the region to delete.
            space: The space where all the region are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete region by id:

                >>> from field.client import FieldClient
                >>> client = FieldClient()
                >>> client.region.delete("my_region")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.region.delete(my_ids)` please use `my_client.delete(my_ids)`."
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
    ) -> Region | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> RegionList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> Region | RegionList | None:
        """Retrieve one or more regions by id(s).

        Args:
            external_id: External id or list of external ids of the regions.
            space: The space where all the regions are located.
            retrieve_connections: Whether to retrieve `area` for the regions. Defaults to 'skip'.'skip' will not
            retrieve any connections, 'identifier' will only retrieve the identifier of the connected items, and 'full'
            will retrieve the full connected items.

        Returns:
            The requested regions.

        Examples:

            Retrieve region by id:

                >>> from field.client import FieldClient
                >>> client = FieldClient()
                >>> region = client.region.retrieve(
                ...     "my_region"
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
        properties: RegionTextFields | SequenceNotStr[RegionTextFields] | None = None,
        area: (
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
        sort_by: RegionFields | SequenceNotStr[RegionFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> RegionList:
        """Search regions

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            area: The area to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of regions to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results regions matching the query.

        Examples:

           Search for 'my_region' in all text properties:

                >>> from field.client import FieldClient
                >>> client = FieldClient()
                >>> regions = client.region.search(
                ...     'my_region'
                ... )

        """
        filter_ = _create_region_filter(
            self._view_id,
            area,
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
        property: RegionFields | SequenceNotStr[RegionFields] | None = None,
        area: (
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
        property: RegionFields | SequenceNotStr[RegionFields] | None = None,
        area: (
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
        group_by: RegionFields | SequenceNotStr[RegionFields],
        property: RegionFields | SequenceNotStr[RegionFields] | None = None,
        area: (
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
        group_by: RegionFields | SequenceNotStr[RegionFields] | None = None,
        property: RegionFields | SequenceNotStr[RegionFields] | None = None,
        area: (
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
        """Aggregate data across regions

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            area: The area to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of regions to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count regions in space `my_space`:

                >>> from field.client import FieldClient
                >>> client = FieldClient()
                >>> result = client.region.aggregate("count", space="my_space")

        """

        filter_ = _create_region_filter(
            self._view_id,
            area,
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
        property: RegionFields,
        interval: float,
        area: (
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
        """Produces histograms for regions

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            area: The area to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of regions to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_region_filter(
            self._view_id,
            area,
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

    def select(self) -> RegionQuery:
        """Start selecting from regions."""
        return RegionQuery(self._client)

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
                    Area._view_id,
                    ViewPropertyId(self._view_id, "area"),
                    has_container_fields=True,
                )
            )
        unpack_edges: Literal["skip", "identifier"] = "identifier" if retrieve_connections == "identifier" else "skip"
        builder.execute_query(self._client, remove_not_connected=True if unpack_edges == "skip" else False)
        return QueryUnpacker(builder, edges=unpack_edges).unpack()

    def list(
        self,
        area: (
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
    ) -> RegionList:
        """List/filter regions

        Args:
            area: The area to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of regions to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            retrieve_connections: Whether to retrieve `area` for the regions. Defaults to 'skip'.'skip' will not
            retrieve any connections, 'identifier' will only retrieve the identifier of the connected items, and 'full'
            will retrieve the full connected items.

        Returns:
            List of requested regions

        Examples:

            List regions and limit to 5:

                >>> from field.client import FieldClient
                >>> client = FieldClient()
                >>> regions = client.region.list(limit=5)

        """
        filter_ = _create_region_filter(
            self._view_id,
            area,
            external_id_prefix,
            space,
            filter,
        )
        if retrieve_connections == "skip":
            return self._list(limit=limit, filter=filter_)
        values = self._query(filter_, limit, retrieve_connections)
        return self._class_list(instantiate_classes(self._class_type, values, "list"))
