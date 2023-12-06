from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from osdu_wells_pydantic_v1.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    AsIngestedCoordinates,
    AsIngestedCoordinatesApply,
    AsIngestedCoordinatesFields,
    AsIngestedCoordinatesList,
    AsIngestedCoordinatesApplyList,
    AsIngestedCoordinatesTextFields,
)
from osdu_wells_pydantic_v1.client.data_classes._as_ingested_coordinates import (
    _ASINGESTEDCOORDINATES_PROPERTIES_BY_FIELD,
    _create_as_ingested_coordinate_filter,
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
from .as_ingested_coordinates_features import AsIngestedCoordinatesFeaturesAPI
from .as_ingested_coordinates_query import AsIngestedCoordinatesQueryAPI


class AsIngestedCoordinatesAPI(NodeAPI[AsIngestedCoordinates, AsIngestedCoordinatesApply, AsIngestedCoordinatesList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[AsIngestedCoordinatesApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=AsIngestedCoordinates,
            class_apply_type=AsIngestedCoordinatesApply,
            class_list=AsIngestedCoordinatesList,
            class_apply_list=AsIngestedCoordinatesApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id
        self.features_edge = AsIngestedCoordinatesFeaturesAPI(client)

    def __call__(
        self,
        coordinate_reference_system_id: str | list[str] | None = None,
        coordinate_reference_system_id_prefix: str | None = None,
        vertical_coordinate_reference_system_id: str | list[str] | None = None,
        vertical_coordinate_reference_system_id_prefix: str | None = None,
        vertical_unit_id: str | list[str] | None = None,
        vertical_unit_id_prefix: str | None = None,
        persistable_reference_crs: str | list[str] | None = None,
        persistable_reference_crs_prefix: str | None = None,
        persistable_reference_unit_z: str | list[str] | None = None,
        persistable_reference_unit_z_prefix: str | None = None,
        persistable_reference_vertical_crs: str | list[str] | None = None,
        persistable_reference_vertical_crs_prefix: str | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> AsIngestedCoordinatesQueryAPI[AsIngestedCoordinatesList]:
        """Query starting at as ingested coordinates.

        Args:
            coordinate_reference_system_id: The coordinate reference system id to filter on.
            coordinate_reference_system_id_prefix: The prefix of the coordinate reference system id to filter on.
            vertical_coordinate_reference_system_id: The vertical coordinate reference system id to filter on.
            vertical_coordinate_reference_system_id_prefix: The prefix of the vertical coordinate reference system id to filter on.
            vertical_unit_id: The vertical unit id to filter on.
            vertical_unit_id_prefix: The prefix of the vertical unit id to filter on.
            persistable_reference_crs: The persistable reference cr to filter on.
            persistable_reference_crs_prefix: The prefix of the persistable reference cr to filter on.
            persistable_reference_unit_z: The persistable reference unit z to filter on.
            persistable_reference_unit_z_prefix: The prefix of the persistable reference unit z to filter on.
            persistable_reference_vertical_crs: The persistable reference vertical cr to filter on.
            persistable_reference_vertical_crs_prefix: The prefix of the persistable reference vertical cr to filter on.
            type_: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of as ingested coordinates to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for as ingested coordinates.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_as_ingested_coordinate_filter(
            self._view_id,
            coordinate_reference_system_id,
            coordinate_reference_system_id_prefix,
            vertical_coordinate_reference_system_id,
            vertical_coordinate_reference_system_id_prefix,
            vertical_unit_id,
            vertical_unit_id_prefix,
            persistable_reference_crs,
            persistable_reference_crs_prefix,
            persistable_reference_unit_z,
            persistable_reference_unit_z_prefix,
            persistable_reference_vertical_crs,
            persistable_reference_vertical_crs_prefix,
            type_,
            type_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(AsIngestedCoordinatesList)
        return AsIngestedCoordinatesQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(
        self,
        as_ingested_coordinate: AsIngestedCoordinatesApply | Sequence[AsIngestedCoordinatesApply],
        replace: bool = False,
    ) -> ResourcesApplyResult:
        """Add or update (upsert) as ingested coordinates.

        Note: This method iterates through all nodes and timeseries linked to as_ingested_coordinate and creates them including the edges
        between the nodes. For example, if any of `features` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            as_ingested_coordinate: As ingested coordinate or sequence of as ingested coordinates to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new as_ingested_coordinate:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> from osdu_wells_pydantic_v1.client.data_classes import AsIngestedCoordinatesApply
                >>> client = OSDUClient()
                >>> as_ingested_coordinate = AsIngestedCoordinatesApply(external_id="my_as_ingested_coordinate", ...)
                >>> result = client.as_ingested_coordinates.apply(as_ingested_coordinate)

        """
        return self._apply(as_ingested_coordinate, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more as ingested coordinate.

        Args:
            external_id: External id of the as ingested coordinate to delete.
            space: The space where all the as ingested coordinate are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete as_ingested_coordinate by id:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.as_ingested_coordinates.delete("my_as_ingested_coordinate")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> AsIngestedCoordinates | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> AsIngestedCoordinatesList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> AsIngestedCoordinates | AsIngestedCoordinatesList | None:
        """Retrieve one or more as ingested coordinates by id(s).

        Args:
            external_id: External id or list of external ids of the as ingested coordinates.
            space: The space where all the as ingested coordinates are located.

        Returns:
            The requested as ingested coordinates.

        Examples:

            Retrieve as_ingested_coordinate by id:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> as_ingested_coordinate = client.as_ingested_coordinates.retrieve("my_as_ingested_coordinate")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_triple=[
                (
                    self.features_edge,
                    "features",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "AsIngestedCoordinates.features"),
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: AsIngestedCoordinatesTextFields | Sequence[AsIngestedCoordinatesTextFields] | None = None,
        coordinate_reference_system_id: str | list[str] | None = None,
        coordinate_reference_system_id_prefix: str | None = None,
        vertical_coordinate_reference_system_id: str | list[str] | None = None,
        vertical_coordinate_reference_system_id_prefix: str | None = None,
        vertical_unit_id: str | list[str] | None = None,
        vertical_unit_id_prefix: str | None = None,
        persistable_reference_crs: str | list[str] | None = None,
        persistable_reference_crs_prefix: str | None = None,
        persistable_reference_unit_z: str | list[str] | None = None,
        persistable_reference_unit_z_prefix: str | None = None,
        persistable_reference_vertical_crs: str | list[str] | None = None,
        persistable_reference_vertical_crs_prefix: str | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> AsIngestedCoordinatesList:
        """Search as ingested coordinates

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            coordinate_reference_system_id: The coordinate reference system id to filter on.
            coordinate_reference_system_id_prefix: The prefix of the coordinate reference system id to filter on.
            vertical_coordinate_reference_system_id: The vertical coordinate reference system id to filter on.
            vertical_coordinate_reference_system_id_prefix: The prefix of the vertical coordinate reference system id to filter on.
            vertical_unit_id: The vertical unit id to filter on.
            vertical_unit_id_prefix: The prefix of the vertical unit id to filter on.
            persistable_reference_crs: The persistable reference cr to filter on.
            persistable_reference_crs_prefix: The prefix of the persistable reference cr to filter on.
            persistable_reference_unit_z: The persistable reference unit z to filter on.
            persistable_reference_unit_z_prefix: The prefix of the persistable reference unit z to filter on.
            persistable_reference_vertical_crs: The persistable reference vertical cr to filter on.
            persistable_reference_vertical_crs_prefix: The prefix of the persistable reference vertical cr to filter on.
            type_: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of as ingested coordinates to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results as ingested coordinates matching the query.

        Examples:

           Search for 'my_as_ingested_coordinate' in all text properties:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> as_ingested_coordinates = client.as_ingested_coordinates.search('my_as_ingested_coordinate')

        """
        filter_ = _create_as_ingested_coordinate_filter(
            self._view_id,
            coordinate_reference_system_id,
            coordinate_reference_system_id_prefix,
            vertical_coordinate_reference_system_id,
            vertical_coordinate_reference_system_id_prefix,
            vertical_unit_id,
            vertical_unit_id_prefix,
            persistable_reference_crs,
            persistable_reference_crs_prefix,
            persistable_reference_unit_z,
            persistable_reference_unit_z_prefix,
            persistable_reference_vertical_crs,
            persistable_reference_vertical_crs_prefix,
            type_,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            self._view_id, query, _ASINGESTEDCOORDINATES_PROPERTIES_BY_FIELD, properties, filter_, limit
        )

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: AsIngestedCoordinatesFields | Sequence[AsIngestedCoordinatesFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: AsIngestedCoordinatesTextFields | Sequence[AsIngestedCoordinatesTextFields] | None = None,
        coordinate_reference_system_id: str | list[str] | None = None,
        coordinate_reference_system_id_prefix: str | None = None,
        vertical_coordinate_reference_system_id: str | list[str] | None = None,
        vertical_coordinate_reference_system_id_prefix: str | None = None,
        vertical_unit_id: str | list[str] | None = None,
        vertical_unit_id_prefix: str | None = None,
        persistable_reference_crs: str | list[str] | None = None,
        persistable_reference_crs_prefix: str | None = None,
        persistable_reference_unit_z: str | list[str] | None = None,
        persistable_reference_unit_z_prefix: str | None = None,
        persistable_reference_vertical_crs: str | list[str] | None = None,
        persistable_reference_vertical_crs_prefix: str | None = None,
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
        property: AsIngestedCoordinatesFields | Sequence[AsIngestedCoordinatesFields] | None = None,
        group_by: AsIngestedCoordinatesFields | Sequence[AsIngestedCoordinatesFields] = None,
        query: str | None = None,
        search_properties: AsIngestedCoordinatesTextFields | Sequence[AsIngestedCoordinatesTextFields] | None = None,
        coordinate_reference_system_id: str | list[str] | None = None,
        coordinate_reference_system_id_prefix: str | None = None,
        vertical_coordinate_reference_system_id: str | list[str] | None = None,
        vertical_coordinate_reference_system_id_prefix: str | None = None,
        vertical_unit_id: str | list[str] | None = None,
        vertical_unit_id_prefix: str | None = None,
        persistable_reference_crs: str | list[str] | None = None,
        persistable_reference_crs_prefix: str | None = None,
        persistable_reference_unit_z: str | list[str] | None = None,
        persistable_reference_unit_z_prefix: str | None = None,
        persistable_reference_vertical_crs: str | list[str] | None = None,
        persistable_reference_vertical_crs_prefix: str | None = None,
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
        property: AsIngestedCoordinatesFields | Sequence[AsIngestedCoordinatesFields] | None = None,
        group_by: AsIngestedCoordinatesFields | Sequence[AsIngestedCoordinatesFields] | None = None,
        query: str | None = None,
        search_property: AsIngestedCoordinatesTextFields | Sequence[AsIngestedCoordinatesTextFields] | None = None,
        coordinate_reference_system_id: str | list[str] | None = None,
        coordinate_reference_system_id_prefix: str | None = None,
        vertical_coordinate_reference_system_id: str | list[str] | None = None,
        vertical_coordinate_reference_system_id_prefix: str | None = None,
        vertical_unit_id: str | list[str] | None = None,
        vertical_unit_id_prefix: str | None = None,
        persistable_reference_crs: str | list[str] | None = None,
        persistable_reference_crs_prefix: str | None = None,
        persistable_reference_unit_z: str | list[str] | None = None,
        persistable_reference_unit_z_prefix: str | None = None,
        persistable_reference_vertical_crs: str | list[str] | None = None,
        persistable_reference_vertical_crs_prefix: str | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across as ingested coordinates

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            coordinate_reference_system_id: The coordinate reference system id to filter on.
            coordinate_reference_system_id_prefix: The prefix of the coordinate reference system id to filter on.
            vertical_coordinate_reference_system_id: The vertical coordinate reference system id to filter on.
            vertical_coordinate_reference_system_id_prefix: The prefix of the vertical coordinate reference system id to filter on.
            vertical_unit_id: The vertical unit id to filter on.
            vertical_unit_id_prefix: The prefix of the vertical unit id to filter on.
            persistable_reference_crs: The persistable reference cr to filter on.
            persistable_reference_crs_prefix: The prefix of the persistable reference cr to filter on.
            persistable_reference_unit_z: The persistable reference unit z to filter on.
            persistable_reference_unit_z_prefix: The prefix of the persistable reference unit z to filter on.
            persistable_reference_vertical_crs: The persistable reference vertical cr to filter on.
            persistable_reference_vertical_crs_prefix: The prefix of the persistable reference vertical cr to filter on.
            type_: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of as ingested coordinates to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count as ingested coordinates in space `my_space`:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.as_ingested_coordinates.aggregate("count", space="my_space")

        """

        filter_ = _create_as_ingested_coordinate_filter(
            self._view_id,
            coordinate_reference_system_id,
            coordinate_reference_system_id_prefix,
            vertical_coordinate_reference_system_id,
            vertical_coordinate_reference_system_id_prefix,
            vertical_unit_id,
            vertical_unit_id_prefix,
            persistable_reference_crs,
            persistable_reference_crs_prefix,
            persistable_reference_unit_z,
            persistable_reference_unit_z_prefix,
            persistable_reference_vertical_crs,
            persistable_reference_vertical_crs_prefix,
            type_,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _ASINGESTEDCOORDINATES_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: AsIngestedCoordinatesFields,
        interval: float,
        query: str | None = None,
        search_property: AsIngestedCoordinatesTextFields | Sequence[AsIngestedCoordinatesTextFields] | None = None,
        coordinate_reference_system_id: str | list[str] | None = None,
        coordinate_reference_system_id_prefix: str | None = None,
        vertical_coordinate_reference_system_id: str | list[str] | None = None,
        vertical_coordinate_reference_system_id_prefix: str | None = None,
        vertical_unit_id: str | list[str] | None = None,
        vertical_unit_id_prefix: str | None = None,
        persistable_reference_crs: str | list[str] | None = None,
        persistable_reference_crs_prefix: str | None = None,
        persistable_reference_unit_z: str | list[str] | None = None,
        persistable_reference_unit_z_prefix: str | None = None,
        persistable_reference_vertical_crs: str | list[str] | None = None,
        persistable_reference_vertical_crs_prefix: str | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for as ingested coordinates

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            coordinate_reference_system_id: The coordinate reference system id to filter on.
            coordinate_reference_system_id_prefix: The prefix of the coordinate reference system id to filter on.
            vertical_coordinate_reference_system_id: The vertical coordinate reference system id to filter on.
            vertical_coordinate_reference_system_id_prefix: The prefix of the vertical coordinate reference system id to filter on.
            vertical_unit_id: The vertical unit id to filter on.
            vertical_unit_id_prefix: The prefix of the vertical unit id to filter on.
            persistable_reference_crs: The persistable reference cr to filter on.
            persistable_reference_crs_prefix: The prefix of the persistable reference cr to filter on.
            persistable_reference_unit_z: The persistable reference unit z to filter on.
            persistable_reference_unit_z_prefix: The prefix of the persistable reference unit z to filter on.
            persistable_reference_vertical_crs: The persistable reference vertical cr to filter on.
            persistable_reference_vertical_crs_prefix: The prefix of the persistable reference vertical cr to filter on.
            type_: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of as ingested coordinates to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_as_ingested_coordinate_filter(
            self._view_id,
            coordinate_reference_system_id,
            coordinate_reference_system_id_prefix,
            vertical_coordinate_reference_system_id,
            vertical_coordinate_reference_system_id_prefix,
            vertical_unit_id,
            vertical_unit_id_prefix,
            persistable_reference_crs,
            persistable_reference_crs_prefix,
            persistable_reference_unit_z,
            persistable_reference_unit_z_prefix,
            persistable_reference_vertical_crs,
            persistable_reference_vertical_crs_prefix,
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
            _ASINGESTEDCOORDINATES_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        coordinate_reference_system_id: str | list[str] | None = None,
        coordinate_reference_system_id_prefix: str | None = None,
        vertical_coordinate_reference_system_id: str | list[str] | None = None,
        vertical_coordinate_reference_system_id_prefix: str | None = None,
        vertical_unit_id: str | list[str] | None = None,
        vertical_unit_id_prefix: str | None = None,
        persistable_reference_crs: str | list[str] | None = None,
        persistable_reference_crs_prefix: str | None = None,
        persistable_reference_unit_z: str | list[str] | None = None,
        persistable_reference_unit_z_prefix: str | None = None,
        persistable_reference_vertical_crs: str | list[str] | None = None,
        persistable_reference_vertical_crs_prefix: str | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> AsIngestedCoordinatesList:
        """List/filter as ingested coordinates

        Args:
            coordinate_reference_system_id: The coordinate reference system id to filter on.
            coordinate_reference_system_id_prefix: The prefix of the coordinate reference system id to filter on.
            vertical_coordinate_reference_system_id: The vertical coordinate reference system id to filter on.
            vertical_coordinate_reference_system_id_prefix: The prefix of the vertical coordinate reference system id to filter on.
            vertical_unit_id: The vertical unit id to filter on.
            vertical_unit_id_prefix: The prefix of the vertical unit id to filter on.
            persistable_reference_crs: The persistable reference cr to filter on.
            persistable_reference_crs_prefix: The prefix of the persistable reference cr to filter on.
            persistable_reference_unit_z: The persistable reference unit z to filter on.
            persistable_reference_unit_z_prefix: The prefix of the persistable reference unit z to filter on.
            persistable_reference_vertical_crs: The persistable reference vertical cr to filter on.
            persistable_reference_vertical_crs_prefix: The prefix of the persistable reference vertical cr to filter on.
            type_: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of as ingested coordinates to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `features` external ids for the as ingested coordinates. Defaults to True.

        Returns:
            List of requested as ingested coordinates

        Examples:

            List as ingested coordinates and limit to 5:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> as_ingested_coordinates = client.as_ingested_coordinates.list(limit=5)

        """
        filter_ = _create_as_ingested_coordinate_filter(
            self._view_id,
            coordinate_reference_system_id,
            coordinate_reference_system_id_prefix,
            vertical_coordinate_reference_system_id,
            vertical_coordinate_reference_system_id_prefix,
            vertical_unit_id,
            vertical_unit_id_prefix,
            persistable_reference_crs,
            persistable_reference_crs_prefix,
            persistable_reference_unit_z,
            persistable_reference_unit_z_prefix,
            persistable_reference_vertical_crs,
            persistable_reference_vertical_crs_prefix,
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
            edge_api_name_type_triple=[
                (
                    self.features_edge,
                    "features",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "AsIngestedCoordinates.features"),
                ),
            ],
        )
