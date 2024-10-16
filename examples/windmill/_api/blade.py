from __future__ import annotations

from collections.abc import Sequence
from typing import overload, Literal
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from windmill.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    NodeQueryStep,
    EdgeQueryStep,
    QueryBuilder,
)
from windmill.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    Blade,
    BladeWrite,
    BladeFields,
    BladeList,
    BladeWriteList,
    BladeTextFields,
    SensorPosition,
)
from windmill.data_classes._blade import (
    BladeQuery,
    _BLADE_PROPERTIES_BY_FIELD,
    _create_blade_filter,
)
from ._core import (
    DEFAULT_LIMIT_READ,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from .blade_sensor_positions import BladeSensorPositionsAPI
from .blade_query import BladeQueryAPI


class BladeAPI(NodeAPI[Blade, BladeWrite, BladeList, BladeWriteList]):
    _view_id = dm.ViewId("power-models", "Blade", "1")
    _properties_by_field = _BLADE_PROPERTIES_BY_FIELD
    _class_type = Blade
    _class_list = BladeList
    _class_write_list = BladeWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.sensor_positions_edge = BladeSensorPositionsAPI(client)

    def __call__(
        self,
        is_damaged: bool | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> BladeQueryAPI[BladeList]:
        """Query starting at blades.

        Args:
            is_damaged: The is damaged to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of blades to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for blades.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_blade_filter(
            self._view_id,
            is_damaged,
            name,
            name_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(BladeList)
        return BladeQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        blade: BladeWrite | Sequence[BladeWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) blades.

        Note: This method iterates through all nodes and timeseries linked to blade and creates them including the edges
        between the nodes. For example, if any of `sensor_positions` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            blade: Blade or sequence of blades to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new blade:

                >>> from windmill import WindmillClient
                >>> from windmill.data_classes import BladeWrite
                >>> client = WindmillClient()
                >>> blade = BladeWrite(external_id="my_blade", ...)
                >>> result = client.blade.apply(blade)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.blade.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(blade, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more blade.

        Args:
            external_id: External id of the blade to delete.
            space: The space where all the blade are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete blade by id:

                >>> from windmill import WindmillClient
                >>> client = WindmillClient()
                >>> client.blade.delete("my_blade")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.blade.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> Blade | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> BladeList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> Blade | BladeList | None:
        """Retrieve one or more blades by id(s).

        Args:
            external_id: External id or list of external ids of the blades.
            space: The space where all the blades are located.

        Returns:
            The requested blades.

        Examples:

            Retrieve blade by id:

                >>> from windmill import WindmillClient
                >>> client = WindmillClient()
                >>> blade = client.blade.retrieve("my_blade")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.sensor_positions_edge,
                    "sensor_positions",
                    dm.DirectRelationReference("power-models", "Blade.sensor_positions"),
                    "outwards",
                    dm.ViewId("power-models", "SensorPosition", "1"),
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: BladeTextFields | SequenceNotStr[BladeTextFields] | None = None,
        is_damaged: bool | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: BladeFields | SequenceNotStr[BladeFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> BladeList:
        """Search blades

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            is_damaged: The is damaged to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of blades to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results blades matching the query.

        Examples:

           Search for 'my_blade' in all text properties:

                >>> from windmill import WindmillClient
                >>> client = WindmillClient()
                >>> blades = client.blade.search('my_blade')

        """
        filter_ = _create_blade_filter(
            self._view_id,
            is_damaged,
            name,
            name_prefix,
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
        property: BladeFields | SequenceNotStr[BladeFields] | None = None,
        query: str | None = None,
        search_property: BladeTextFields | SequenceNotStr[BladeTextFields] | None = None,
        is_damaged: bool | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
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
        property: BladeFields | SequenceNotStr[BladeFields] | None = None,
        query: str | None = None,
        search_property: BladeTextFields | SequenceNotStr[BladeTextFields] | None = None,
        is_damaged: bool | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
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
        group_by: BladeFields | SequenceNotStr[BladeFields],
        property: BladeFields | SequenceNotStr[BladeFields] | None = None,
        query: str | None = None,
        search_property: BladeTextFields | SequenceNotStr[BladeTextFields] | None = None,
        is_damaged: bool | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
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
        group_by: BladeFields | SequenceNotStr[BladeFields] | None = None,
        property: BladeFields | SequenceNotStr[BladeFields] | None = None,
        query: str | None = None,
        search_property: BladeTextFields | SequenceNotStr[BladeTextFields] | None = None,
        is_damaged: bool | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across blades

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            is_damaged: The is damaged to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of blades to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count blades in space `my_space`:

                >>> from windmill import WindmillClient
                >>> client = WindmillClient()
                >>> result = client.blade.aggregate("count", space="my_space")

        """

        filter_ = _create_blade_filter(
            self._view_id,
            is_damaged,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            aggregate=aggregate,
            group_by=group_by,  # type: ignore[arg-type]
            properties=property,  # type: ignore[arg-type]
            query=query,
            search_properties=search_property,  # type: ignore[arg-type]
            limit=limit,
            filter=filter_,
        )

    def histogram(
        self,
        property: BladeFields,
        interval: float,
        query: str | None = None,
        search_property: BladeTextFields | SequenceNotStr[BladeTextFields] | None = None,
        is_damaged: bool | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for blades

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            is_damaged: The is damaged to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of blades to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_blade_filter(
            self._view_id,
            is_damaged,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            property,
            interval,
            query,
            search_property,  # type: ignore[arg-type]
            limit,
            filter_,
        )

    def query(self) -> BladeQuery:
        """Start a query for blades."""
        warnings.warn("The .query is in alpha and is subject to breaking changes without notice.")
        return BladeQuery(self._client)

    def list(
        self,
        is_damaged: bool | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: BladeFields | Sequence[BladeFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> BladeList:
        """List/filter blades

        Args:
            is_damaged: The is damaged to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of blades to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `sensor_positions` for the blades. Defaults to 'skip'.
                'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            List of requested blades

        Examples:

            List blades and limit to 5:

                >>> from windmill import WindmillClient
                >>> client = WindmillClient()
                >>> blades = client.blade.list(limit=5)

        """
        filter_ = _create_blade_filter(
            self._view_id,
            is_damaged,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )

        if retrieve_connections == "skip":
            return self._list(
                limit=limit,
                filter=filter_,
                sort_by=sort_by,  # type: ignore[arg-type]
                direction=direction,
                sort=sort,
            )

        builder = QueryBuilder(BladeList)
        has_data = dm.filters.HasData(views=[self._view_id])
        builder.append(
            NodeQueryStep(
                builder.create_name(None),
                dm.query.NodeResultSetExpression(
                    filter=dm.filters.And(filter_, has_data) if filter_ else has_data,
                    sort=self._create_sort(sort_by, direction, sort),  # type: ignore[arg-type]
                ),
                Blade,
                max_retrieve_limit=limit,
                raw_filter=filter_,
            )
        )
        from_root = builder.get_from()
        edge_sensor_positions = builder.create_name(from_root)
        builder.append(
            EdgeQueryStep(
                edge_sensor_positions,
                dm.query.EdgeResultSetExpression(
                    from_=from_root,
                    direction="outwards",
                    chain_to="destination",
                ),
            )
        )
        if retrieve_connections == "full":
            builder.append(
                NodeQueryStep(
                    builder.create_name(edge_sensor_positions),
                    dm.query.NodeResultSetExpression(
                        from_=edge_sensor_positions,
                        filter=dm.filters.HasData(views=[SensorPosition._view_id]),
                    ),
                    SensorPosition,
                )
            )

        return builder.execute(self._client)
