from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from windmill_pydantic_v1.client.data_classes._core import DEFAULT_INSTANCE_SPACE
from windmill_pydantic_v1.client.data_classes import (
    DomainModelCore,
    DomainModelApply,
    ResourcesApplyResult,
    Blade,
    BladeApply,
    BladeFields,
    BladeList,
    BladeApplyList,
    BladeTextFields,
)
from windmill_pydantic_v1.client.data_classes._blade import (
    _BLADE_PROPERTIES_BY_FIELD,
    _create_blade_filter,
)
from ._core import (
    DEFAULT_LIMIT_READ,
    DEFAULT_QUERY_LIMIT,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
    QueryStep,
    QueryBuilder,
)
from .blade_sensor_positions import BladeSensorPositionsAPI
from .blade_query import BladeQueryAPI


class BladeAPI(NodeAPI[Blade, BladeApply, BladeList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[Blade]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Blade,
            class_list=BladeList,
            class_apply_list=BladeApplyList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
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
        return BladeQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(self, blade: BladeApply | Sequence[BladeApply], replace: bool = False) -> ResourcesApplyResult:
        """Add or update (upsert) blades.

        Note: This method iterates through all nodes and timeseries linked to blade and creates them including the edges
        between the nodes. For example, if any of `sensor_positions` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            blade: Blade or sequence of blades to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new blade:

                >>> from windmill_pydantic_v1.client import WindmillClient
                >>> from windmill_pydantic_v1.client.data_classes import BladeApply
                >>> client = WindmillClient()
                >>> blade = BladeApply(external_id="my_blade", ...)
                >>> result = client.blade.apply(blade)

        """
        return self._apply(blade, replace)

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

                >>> from windmill_pydantic_v1.client import WindmillClient
                >>> client = WindmillClient()
                >>> client.blade.delete("my_blade")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> Blade | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> BladeList:
        ...

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

                >>> from windmill_pydantic_v1.client import WindmillClient
                >>> client = WindmillClient()
                >>> blade = client.blade.retrieve("my_blade")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_quad=[
                (
                    self.sensor_positions_edge,
                    "sensor_positions",
                    dm.DirectRelationReference("power-models", "Blade.sensor_positions"),
                    "outwards",
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: BladeTextFields | Sequence[BladeTextFields] | None = None,
        is_damaged: bool | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
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

        Returns:
            Search results blades matching the query.

        Examples:

           Search for 'my_blade' in all text properties:

                >>> from windmill_pydantic_v1.client import WindmillClient
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
        return self._search(self._view_id, query, _BLADE_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: BladeFields | Sequence[BladeFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: BladeTextFields | Sequence[BladeTextFields] | None = None,
        is_damaged: bool | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: BladeFields | Sequence[BladeFields] | None = None,
        group_by: BladeFields | Sequence[BladeFields] = None,
        query: str | None = None,
        search_properties: BladeTextFields | Sequence[BladeTextFields] | None = None,
        is_damaged: bool | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList:
        ...

    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: BladeFields | Sequence[BladeFields] | None = None,
        group_by: BladeFields | Sequence[BladeFields] | None = None,
        query: str | None = None,
        search_property: BladeTextFields | Sequence[BladeTextFields] | None = None,
        is_damaged: bool | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across blades

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
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

                >>> from windmill_pydantic_v1.client import WindmillClient
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
            self._view_id,
            aggregate,
            _BLADE_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: BladeFields,
        interval: float,
        query: str | None = None,
        search_property: BladeTextFields | Sequence[BladeTextFields] | None = None,
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
            self._view_id,
            property,
            interval,
            _BLADE_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        is_damaged: bool | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
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
            retrieve_edges: Whether to retrieve `sensor_positions` external ids for the blades. Defaults to True.

        Returns:
            List of requested blades

        Examples:

            List blades and limit to 5:

                >>> from windmill_pydantic_v1.client import WindmillClient
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

        return self._list(
            limit=limit,
            filter=filter_,
            retrieve_edges=retrieve_edges,
            edge_api_name_type_direction_quad=[
                (
                    self.sensor_positions_edge,
                    "sensor_positions",
                    dm.DirectRelationReference("power-models", "Blade.sensor_positions"),
                    "outwards",
                ),
            ],
        )
