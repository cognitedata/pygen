from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells.client.data_classes import (
    SpatialLocation,
    SpatialLocationApply,
    SpatialLocationList,
    SpatialLocationApplyList,
    SpatialLocationFields,
    SpatialLocationTextFields,
    DomainModelApply,
)
from osdu_wells.client.data_classes._spatial_location import _SPATIALLOCATION_PROPERTIES_BY_FIELD


class SpatialLocationAPI(TypeAPI[SpatialLocation, SpatialLocationApply, SpatialLocationList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[SpatialLocationApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=SpatialLocation,
            class_apply_type=SpatialLocationApply,
            class_list=SpatialLocationList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class

    def apply(
        self, spatial_location: SpatialLocationApply | Sequence[SpatialLocationApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        """Add or update (upsert) spatial locations.

        Args:
            spatial_location: Spatial location or sequence of spatial locations to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new spatial_location:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import SpatialLocationApply
                >>> client = OSDUClient()
                >>> spatial_location = SpatialLocationApply(external_id="my_spatial_location", ...)
                >>> result = client.spatial_location.apply(spatial_location)

        """
        if isinstance(spatial_location, SpatialLocationApply):
            instances = spatial_location.to_instances_apply(self._view_by_write_class)
        else:
            instances = SpatialLocationApplyList(spatial_location).to_instances_apply(self._view_by_write_class)
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more spatial location.

        Args:
            external_id: External id of the spatial location to delete.
            space: The space where all the spatial location are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete spatial_location by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.spatial_location.delete("my_spatial_location")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> SpatialLocation:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> SpatialLocationList:
        ...

    def retrieve(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
    ) -> SpatialLocation | SpatialLocationList:
        """Retrieve one or more spatial locations by id(s).

        Args:
            external_id: External id or list of external ids of the spatial locations.
            space: The space where all the spatial locations are located.

        Returns:
            The requested spatial locations.

        Examples:

            Retrieve spatial_location by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> spatial_location = client.spatial_location.retrieve("my_spatial_location")

        """
        if isinstance(external_id, str):
            return self._retrieve((space, external_id))
        else:
            return self._retrieve([(space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: SpatialLocationTextFields | Sequence[SpatialLocationTextFields] | None = None,
        as_ingested_coordinates: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        coordinate_quality_check_date_time: str | list[str] | None = None,
        coordinate_quality_check_date_time_prefix: str | None = None,
        coordinate_quality_check_performed_by: str | list[str] | None = None,
        coordinate_quality_check_performed_by_prefix: str | None = None,
        qualitative_spatial_accuracy_type_id: str | list[str] | None = None,
        qualitative_spatial_accuracy_type_id_prefix: str | None = None,
        quantitative_accuracy_band_id: str | list[str] | None = None,
        quantitative_accuracy_band_id_prefix: str | None = None,
        spatial_geometry_type_id: str | list[str] | None = None,
        spatial_geometry_type_id_prefix: str | None = None,
        spatial_location_coordinates_date: str | list[str] | None = None,
        spatial_location_coordinates_date_prefix: str | None = None,
        spatial_parameter_type_id: str | list[str] | None = None,
        spatial_parameter_type_id_prefix: str | None = None,
        wgs_84_coordinates: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> SpatialLocationList:
        """Search spatial locations

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            as_ingested_coordinates: The as ingested coordinate to filter on.
            coordinate_quality_check_date_time: The coordinate quality check date time to filter on.
            coordinate_quality_check_date_time_prefix: The prefix of the coordinate quality check date time to filter on.
            coordinate_quality_check_performed_by: The coordinate quality check performed by to filter on.
            coordinate_quality_check_performed_by_prefix: The prefix of the coordinate quality check performed by to filter on.
            qualitative_spatial_accuracy_type_id: The qualitative spatial accuracy type id to filter on.
            qualitative_spatial_accuracy_type_id_prefix: The prefix of the qualitative spatial accuracy type id to filter on.
            quantitative_accuracy_band_id: The quantitative accuracy band id to filter on.
            quantitative_accuracy_band_id_prefix: The prefix of the quantitative accuracy band id to filter on.
            spatial_geometry_type_id: The spatial geometry type id to filter on.
            spatial_geometry_type_id_prefix: The prefix of the spatial geometry type id to filter on.
            spatial_location_coordinates_date: The spatial location coordinates date to filter on.
            spatial_location_coordinates_date_prefix: The prefix of the spatial location coordinates date to filter on.
            spatial_parameter_type_id: The spatial parameter type id to filter on.
            spatial_parameter_type_id_prefix: The prefix of the spatial parameter type id to filter on.
            wgs_84_coordinates: The wgs 84 coordinate to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of spatial locations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficent, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results spatial locations matching the query.

        Examples:

           Search for 'my_spatial_location' in all text properties:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> spatial_locations = client.spatial_location.search('my_spatial_location')

        """
        filter_ = _create_filter(
            self._view_id,
            as_ingested_coordinates,
            coordinate_quality_check_date_time,
            coordinate_quality_check_date_time_prefix,
            coordinate_quality_check_performed_by,
            coordinate_quality_check_performed_by_prefix,
            qualitative_spatial_accuracy_type_id,
            qualitative_spatial_accuracy_type_id_prefix,
            quantitative_accuracy_band_id,
            quantitative_accuracy_band_id_prefix,
            spatial_geometry_type_id,
            spatial_geometry_type_id_prefix,
            spatial_location_coordinates_date,
            spatial_location_coordinates_date_prefix,
            spatial_parameter_type_id,
            spatial_parameter_type_id_prefix,
            wgs_84_coordinates,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _SPATIALLOCATION_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: SpatialLocationFields | Sequence[SpatialLocationFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: SpatialLocationTextFields | Sequence[SpatialLocationTextFields] | None = None,
        as_ingested_coordinates: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        coordinate_quality_check_date_time: str | list[str] | None = None,
        coordinate_quality_check_date_time_prefix: str | None = None,
        coordinate_quality_check_performed_by: str | list[str] | None = None,
        coordinate_quality_check_performed_by_prefix: str | None = None,
        qualitative_spatial_accuracy_type_id: str | list[str] | None = None,
        qualitative_spatial_accuracy_type_id_prefix: str | None = None,
        quantitative_accuracy_band_id: str | list[str] | None = None,
        quantitative_accuracy_band_id_prefix: str | None = None,
        spatial_geometry_type_id: str | list[str] | None = None,
        spatial_geometry_type_id_prefix: str | None = None,
        spatial_location_coordinates_date: str | list[str] | None = None,
        spatial_location_coordinates_date_prefix: str | None = None,
        spatial_parameter_type_id: str | list[str] | None = None,
        spatial_parameter_type_id_prefix: str | None = None,
        wgs_84_coordinates: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: SpatialLocationFields | Sequence[SpatialLocationFields] | None = None,
        group_by: SpatialLocationFields | Sequence[SpatialLocationFields] = None,
        query: str | None = None,
        search_properties: SpatialLocationTextFields | Sequence[SpatialLocationTextFields] | None = None,
        as_ingested_coordinates: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        coordinate_quality_check_date_time: str | list[str] | None = None,
        coordinate_quality_check_date_time_prefix: str | None = None,
        coordinate_quality_check_performed_by: str | list[str] | None = None,
        coordinate_quality_check_performed_by_prefix: str | None = None,
        qualitative_spatial_accuracy_type_id: str | list[str] | None = None,
        qualitative_spatial_accuracy_type_id_prefix: str | None = None,
        quantitative_accuracy_band_id: str | list[str] | None = None,
        quantitative_accuracy_band_id_prefix: str | None = None,
        spatial_geometry_type_id: str | list[str] | None = None,
        spatial_geometry_type_id_prefix: str | None = None,
        spatial_location_coordinates_date: str | list[str] | None = None,
        spatial_location_coordinates_date_prefix: str | None = None,
        spatial_parameter_type_id: str | list[str] | None = None,
        spatial_parameter_type_id_prefix: str | None = None,
        wgs_84_coordinates: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: SpatialLocationFields | Sequence[SpatialLocationFields] | None = None,
        group_by: SpatialLocationFields | Sequence[SpatialLocationFields] | None = None,
        query: str | None = None,
        search_property: SpatialLocationTextFields | Sequence[SpatialLocationTextFields] | None = None,
        as_ingested_coordinates: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        coordinate_quality_check_date_time: str | list[str] | None = None,
        coordinate_quality_check_date_time_prefix: str | None = None,
        coordinate_quality_check_performed_by: str | list[str] | None = None,
        coordinate_quality_check_performed_by_prefix: str | None = None,
        qualitative_spatial_accuracy_type_id: str | list[str] | None = None,
        qualitative_spatial_accuracy_type_id_prefix: str | None = None,
        quantitative_accuracy_band_id: str | list[str] | None = None,
        quantitative_accuracy_band_id_prefix: str | None = None,
        spatial_geometry_type_id: str | list[str] | None = None,
        spatial_geometry_type_id_prefix: str | None = None,
        spatial_location_coordinates_date: str | list[str] | None = None,
        spatial_location_coordinates_date_prefix: str | None = None,
        spatial_parameter_type_id: str | list[str] | None = None,
        spatial_parameter_type_id_prefix: str | None = None,
        wgs_84_coordinates: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            as_ingested_coordinates,
            coordinate_quality_check_date_time,
            coordinate_quality_check_date_time_prefix,
            coordinate_quality_check_performed_by,
            coordinate_quality_check_performed_by_prefix,
            qualitative_spatial_accuracy_type_id,
            qualitative_spatial_accuracy_type_id_prefix,
            quantitative_accuracy_band_id,
            quantitative_accuracy_band_id_prefix,
            spatial_geometry_type_id,
            spatial_geometry_type_id_prefix,
            spatial_location_coordinates_date,
            spatial_location_coordinates_date_prefix,
            spatial_parameter_type_id,
            spatial_parameter_type_id_prefix,
            wgs_84_coordinates,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _SPATIALLOCATION_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: SpatialLocationFields,
        interval: float,
        query: str | None = None,
        search_property: SpatialLocationTextFields | Sequence[SpatialLocationTextFields] | None = None,
        as_ingested_coordinates: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        coordinate_quality_check_date_time: str | list[str] | None = None,
        coordinate_quality_check_date_time_prefix: str | None = None,
        coordinate_quality_check_performed_by: str | list[str] | None = None,
        coordinate_quality_check_performed_by_prefix: str | None = None,
        qualitative_spatial_accuracy_type_id: str | list[str] | None = None,
        qualitative_spatial_accuracy_type_id_prefix: str | None = None,
        quantitative_accuracy_band_id: str | list[str] | None = None,
        quantitative_accuracy_band_id_prefix: str | None = None,
        spatial_geometry_type_id: str | list[str] | None = None,
        spatial_geometry_type_id_prefix: str | None = None,
        spatial_location_coordinates_date: str | list[str] | None = None,
        spatial_location_coordinates_date_prefix: str | None = None,
        spatial_parameter_type_id: str | list[str] | None = None,
        spatial_parameter_type_id_prefix: str | None = None,
        wgs_84_coordinates: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            as_ingested_coordinates,
            coordinate_quality_check_date_time,
            coordinate_quality_check_date_time_prefix,
            coordinate_quality_check_performed_by,
            coordinate_quality_check_performed_by_prefix,
            qualitative_spatial_accuracy_type_id,
            qualitative_spatial_accuracy_type_id_prefix,
            quantitative_accuracy_band_id,
            quantitative_accuracy_band_id_prefix,
            spatial_geometry_type_id,
            spatial_geometry_type_id_prefix,
            spatial_location_coordinates_date,
            spatial_location_coordinates_date_prefix,
            spatial_parameter_type_id,
            spatial_parameter_type_id_prefix,
            wgs_84_coordinates,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _SPATIALLOCATION_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        as_ingested_coordinates: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        coordinate_quality_check_date_time: str | list[str] | None = None,
        coordinate_quality_check_date_time_prefix: str | None = None,
        coordinate_quality_check_performed_by: str | list[str] | None = None,
        coordinate_quality_check_performed_by_prefix: str | None = None,
        qualitative_spatial_accuracy_type_id: str | list[str] | None = None,
        qualitative_spatial_accuracy_type_id_prefix: str | None = None,
        quantitative_accuracy_band_id: str | list[str] | None = None,
        quantitative_accuracy_band_id_prefix: str | None = None,
        spatial_geometry_type_id: str | list[str] | None = None,
        spatial_geometry_type_id_prefix: str | None = None,
        spatial_location_coordinates_date: str | list[str] | None = None,
        spatial_location_coordinates_date_prefix: str | None = None,
        spatial_parameter_type_id: str | list[str] | None = None,
        spatial_parameter_type_id_prefix: str | None = None,
        wgs_84_coordinates: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> SpatialLocationList:
        """List/filter spatial locations

        Args:
            as_ingested_coordinates: The as ingested coordinate to filter on.
            coordinate_quality_check_date_time: The coordinate quality check date time to filter on.
            coordinate_quality_check_date_time_prefix: The prefix of the coordinate quality check date time to filter on.
            coordinate_quality_check_performed_by: The coordinate quality check performed by to filter on.
            coordinate_quality_check_performed_by_prefix: The prefix of the coordinate quality check performed by to filter on.
            qualitative_spatial_accuracy_type_id: The qualitative spatial accuracy type id to filter on.
            qualitative_spatial_accuracy_type_id_prefix: The prefix of the qualitative spatial accuracy type id to filter on.
            quantitative_accuracy_band_id: The quantitative accuracy band id to filter on.
            quantitative_accuracy_band_id_prefix: The prefix of the quantitative accuracy band id to filter on.
            spatial_geometry_type_id: The spatial geometry type id to filter on.
            spatial_geometry_type_id_prefix: The prefix of the spatial geometry type id to filter on.
            spatial_location_coordinates_date: The spatial location coordinates date to filter on.
            spatial_location_coordinates_date_prefix: The prefix of the spatial location coordinates date to filter on.
            spatial_parameter_type_id: The spatial parameter type id to filter on.
            spatial_parameter_type_id_prefix: The prefix of the spatial parameter type id to filter on.
            wgs_84_coordinates: The wgs 84 coordinate to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of spatial locations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficent, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested spatial locations

        Examples:

            List spatial locations and limit to 5:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> spatial_locations = client.spatial_location.list(limit=5)

        """
        filter_ = _create_filter(
            self._view_id,
            as_ingested_coordinates,
            coordinate_quality_check_date_time,
            coordinate_quality_check_date_time_prefix,
            coordinate_quality_check_performed_by,
            coordinate_quality_check_performed_by_prefix,
            qualitative_spatial_accuracy_type_id,
            qualitative_spatial_accuracy_type_id_prefix,
            quantitative_accuracy_band_id,
            quantitative_accuracy_band_id_prefix,
            spatial_geometry_type_id,
            spatial_geometry_type_id_prefix,
            spatial_location_coordinates_date,
            spatial_location_coordinates_date_prefix,
            spatial_parameter_type_id,
            spatial_parameter_type_id_prefix,
            wgs_84_coordinates,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    as_ingested_coordinates: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    coordinate_quality_check_date_time: str | list[str] | None = None,
    coordinate_quality_check_date_time_prefix: str | None = None,
    coordinate_quality_check_performed_by: str | list[str] | None = None,
    coordinate_quality_check_performed_by_prefix: str | None = None,
    qualitative_spatial_accuracy_type_id: str | list[str] | None = None,
    qualitative_spatial_accuracy_type_id_prefix: str | None = None,
    quantitative_accuracy_band_id: str | list[str] | None = None,
    quantitative_accuracy_band_id_prefix: str | None = None,
    spatial_geometry_type_id: str | list[str] | None = None,
    spatial_geometry_type_id_prefix: str | None = None,
    spatial_location_coordinates_date: str | list[str] | None = None,
    spatial_location_coordinates_date_prefix: str | None = None,
    spatial_parameter_type_id: str | list[str] | None = None,
    spatial_parameter_type_id_prefix: str | None = None,
    wgs_84_coordinates: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if as_ingested_coordinates and isinstance(as_ingested_coordinates, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("AsIngestedCoordinates"),
                value={"space": "IntegrationTestsImmutable", "externalId": as_ingested_coordinates},
            )
        )
    if as_ingested_coordinates and isinstance(as_ingested_coordinates, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("AsIngestedCoordinates"),
                value={"space": as_ingested_coordinates[0], "externalId": as_ingested_coordinates[1]},
            )
        )
    if (
        as_ingested_coordinates
        and isinstance(as_ingested_coordinates, list)
        and isinstance(as_ingested_coordinates[0], str)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("AsIngestedCoordinates"),
                values=[{"space": "IntegrationTestsImmutable", "externalId": item} for item in as_ingested_coordinates],
            )
        )
    if (
        as_ingested_coordinates
        and isinstance(as_ingested_coordinates, list)
        and isinstance(as_ingested_coordinates[0], tuple)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("AsIngestedCoordinates"),
                values=[{"space": item[0], "externalId": item[1]} for item in as_ingested_coordinates],
            )
        )
    if coordinate_quality_check_date_time and isinstance(coordinate_quality_check_date_time, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("CoordinateQualityCheckDateTime"), value=coordinate_quality_check_date_time
            )
        )
    if coordinate_quality_check_date_time and isinstance(coordinate_quality_check_date_time, list):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("CoordinateQualityCheckDateTime"), values=coordinate_quality_check_date_time
            )
        )
    if coordinate_quality_check_date_time_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("CoordinateQualityCheckDateTime"),
                value=coordinate_quality_check_date_time_prefix,
            )
        )
    if coordinate_quality_check_performed_by and isinstance(coordinate_quality_check_performed_by, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("CoordinateQualityCheckPerformedBy"),
                value=coordinate_quality_check_performed_by,
            )
        )
    if coordinate_quality_check_performed_by and isinstance(coordinate_quality_check_performed_by, list):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("CoordinateQualityCheckPerformedBy"),
                values=coordinate_quality_check_performed_by,
            )
        )
    if coordinate_quality_check_performed_by_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("CoordinateQualityCheckPerformedBy"),
                value=coordinate_quality_check_performed_by_prefix,
            )
        )
    if qualitative_spatial_accuracy_type_id and isinstance(qualitative_spatial_accuracy_type_id, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("QualitativeSpatialAccuracyTypeID"), value=qualitative_spatial_accuracy_type_id
            )
        )
    if qualitative_spatial_accuracy_type_id and isinstance(qualitative_spatial_accuracy_type_id, list):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("QualitativeSpatialAccuracyTypeID"), values=qualitative_spatial_accuracy_type_id
            )
        )
    if qualitative_spatial_accuracy_type_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("QualitativeSpatialAccuracyTypeID"),
                value=qualitative_spatial_accuracy_type_id_prefix,
            )
        )
    if quantitative_accuracy_band_id and isinstance(quantitative_accuracy_band_id, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("QuantitativeAccuracyBandID"), value=quantitative_accuracy_band_id
            )
        )
    if quantitative_accuracy_band_id and isinstance(quantitative_accuracy_band_id, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("QuantitativeAccuracyBandID"), values=quantitative_accuracy_band_id)
        )
    if quantitative_accuracy_band_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("QuantitativeAccuracyBandID"), value=quantitative_accuracy_band_id_prefix
            )
        )
    if spatial_geometry_type_id and isinstance(spatial_geometry_type_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("SpatialGeometryTypeID"), value=spatial_geometry_type_id)
        )
    if spatial_geometry_type_id and isinstance(spatial_geometry_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("SpatialGeometryTypeID"), values=spatial_geometry_type_id))
    if spatial_geometry_type_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("SpatialGeometryTypeID"), value=spatial_geometry_type_id_prefix)
        )
    if spatial_location_coordinates_date and isinstance(spatial_location_coordinates_date, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("SpatialLocationCoordinatesDate"), value=spatial_location_coordinates_date
            )
        )
    if spatial_location_coordinates_date and isinstance(spatial_location_coordinates_date, list):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("SpatialLocationCoordinatesDate"), values=spatial_location_coordinates_date
            )
        )
    if spatial_location_coordinates_date_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("SpatialLocationCoordinatesDate"),
                value=spatial_location_coordinates_date_prefix,
            )
        )
    if spatial_parameter_type_id and isinstance(spatial_parameter_type_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("SpatialParameterTypeID"), value=spatial_parameter_type_id)
        )
    if spatial_parameter_type_id and isinstance(spatial_parameter_type_id, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("SpatialParameterTypeID"), values=spatial_parameter_type_id)
        )
    if spatial_parameter_type_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("SpatialParameterTypeID"), value=spatial_parameter_type_id_prefix)
        )
    if wgs_84_coordinates and isinstance(wgs_84_coordinates, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("Wgs84Coordinates"),
                value={"space": "IntegrationTestsImmutable", "externalId": wgs_84_coordinates},
            )
        )
    if wgs_84_coordinates and isinstance(wgs_84_coordinates, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("Wgs84Coordinates"),
                value={"space": wgs_84_coordinates[0], "externalId": wgs_84_coordinates[1]},
            )
        )
    if wgs_84_coordinates and isinstance(wgs_84_coordinates, list) and isinstance(wgs_84_coordinates[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("Wgs84Coordinates"),
                values=[{"space": "IntegrationTestsImmutable", "externalId": item} for item in wgs_84_coordinates],
            )
        )
    if wgs_84_coordinates and isinstance(wgs_84_coordinates, list) and isinstance(wgs_84_coordinates[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("Wgs84Coordinates"),
                values=[{"space": item[0], "externalId": item[1]} for item in wgs_84_coordinates],
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
