from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from osdu_wells_pydantic_v1.client.data_classes._core import DEFAULT_INSTANCE_SPACE
from osdu_wells_pydantic_v1.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    WgsCoordinates,
    WgsCoordinatesApply,
    WgsCoordinatesFields,
    WgsCoordinatesList,
    WgsCoordinatesApplyList,
    WgsCoordinatesTextFields,
)
from osdu_wells_pydantic_v1.client.data_classes._wgs_84_coordinates import (
    _WGSCOORDINATES_PROPERTIES_BY_FIELD,
    _create_wgs_84_coordinate_filter,
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
from .wgs_84_coordinates_features import WgsCoordinatesFeaturesAPI
from .wgs_84_coordinates_query import WgsCoordinatesQueryAPI


class WgsCoordinatesAPI(NodeAPI[WgsCoordinates, WgsCoordinatesApply, WgsCoordinatesList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[WgsCoordinatesApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=WgsCoordinates,
            class_apply_type=WgsCoordinatesApply,
            class_list=WgsCoordinatesList,
            class_apply_list=WgsCoordinatesApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id
        self.features_edge = WgsCoordinatesFeaturesAPI(client)

    def __call__(
        self,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> WgsCoordinatesQueryAPI[WgsCoordinatesList]:
        """Query starting at wgs 84 coordinates.

        Args:
            type_: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wgs 84 coordinates to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for wgs 84 coordinates.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_wgs_84_coordinate_filter(
            self._view_id,
            type_,
            type_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(WgsCoordinatesList)
        return WgsCoordinatesQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(
        self, wgs_84_coordinate: WgsCoordinatesApply | Sequence[WgsCoordinatesApply], replace: bool = False
    ) -> ResourcesApplyResult:
        """Add or update (upsert) wgs 84 coordinates.

        Note: This method iterates through all nodes and timeseries linked to wgs_84_coordinate and creates them including the edges
        between the nodes. For example, if any of `features` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            wgs_84_coordinate: Wgs 84 coordinate or sequence of wgs 84 coordinates to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new wgs_84_coordinate:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> from osdu_wells_pydantic_v1.client.data_classes import WgsCoordinatesApply
                >>> client = OSDUClient()
                >>> wgs_84_coordinate = WgsCoordinatesApply(external_id="my_wgs_84_coordinate", ...)
                >>> result = client.wgs_84_coordinates.apply(wgs_84_coordinate)

        """
        return self._apply(wgs_84_coordinate, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more wgs 84 coordinate.

        Args:
            external_id: External id of the wgs 84 coordinate to delete.
            space: The space where all the wgs 84 coordinate are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete wgs_84_coordinate by id:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.wgs_84_coordinates.delete("my_wgs_84_coordinate")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> WgsCoordinates | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> WgsCoordinatesList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> WgsCoordinates | WgsCoordinatesList | None:
        """Retrieve one or more wgs 84 coordinates by id(s).

        Args:
            external_id: External id or list of external ids of the wgs 84 coordinates.
            space: The space where all the wgs 84 coordinates are located.

        Returns:
            The requested wgs 84 coordinates.

        Examples:

            Retrieve wgs_84_coordinate by id:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> wgs_84_coordinate = client.wgs_84_coordinates.retrieve("my_wgs_84_coordinate")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_quad=[
                (
                    self.features_edge,
                    "features",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "Wgs84Coordinates.features"),
                    "outwards",
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: WgsCoordinatesTextFields | Sequence[WgsCoordinatesTextFields] | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> WgsCoordinatesList:
        """Search wgs 84 coordinates

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            type_: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wgs 84 coordinates to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results wgs 84 coordinates matching the query.

        Examples:

           Search for 'my_wgs_84_coordinate' in all text properties:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> wgs_84_coordinates = client.wgs_84_coordinates.search('my_wgs_84_coordinate')

        """
        filter_ = _create_wgs_84_coordinate_filter(
            self._view_id,
            type_,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _WGSCOORDINATES_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: WgsCoordinatesFields | Sequence[WgsCoordinatesFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: WgsCoordinatesTextFields | Sequence[WgsCoordinatesTextFields] | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
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
        property: WgsCoordinatesFields | Sequence[WgsCoordinatesFields] | None = None,
        group_by: WgsCoordinatesFields | Sequence[WgsCoordinatesFields] = None,
        query: str | None = None,
        search_properties: WgsCoordinatesTextFields | Sequence[WgsCoordinatesTextFields] | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
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
        property: WgsCoordinatesFields | Sequence[WgsCoordinatesFields] | None = None,
        group_by: WgsCoordinatesFields | Sequence[WgsCoordinatesFields] | None = None,
        query: str | None = None,
        search_property: WgsCoordinatesTextFields | Sequence[WgsCoordinatesTextFields] | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across wgs 84 coordinates

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            type_: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wgs 84 coordinates to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count wgs 84 coordinates in space `my_space`:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.wgs_84_coordinates.aggregate("count", space="my_space")

        """

        filter_ = _create_wgs_84_coordinate_filter(
            self._view_id,
            type_,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _WGSCOORDINATES_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: WgsCoordinatesFields,
        interval: float,
        query: str | None = None,
        search_property: WgsCoordinatesTextFields | Sequence[WgsCoordinatesTextFields] | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for wgs 84 coordinates

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            type_: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wgs 84 coordinates to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_wgs_84_coordinate_filter(
            self._view_id,
            type_,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _WGSCOORDINATES_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> WgsCoordinatesList:
        """List/filter wgs 84 coordinates

        Args:
            type_: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wgs 84 coordinates to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `features` external ids for the wgs 84 coordinates. Defaults to True.

        Returns:
            List of requested wgs 84 coordinates

        Examples:

            List wgs 84 coordinates and limit to 5:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> wgs_84_coordinates = client.wgs_84_coordinates.list(limit=5)

        """
        filter_ = _create_wgs_84_coordinate_filter(
            self._view_id,
            type_,
            type_prefix,
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
                    self.features_edge,
                    "features",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "Wgs84Coordinates.features"),
                    "outwards",
                ),
            ],
        )
