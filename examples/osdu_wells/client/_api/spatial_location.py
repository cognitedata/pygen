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
)
from osdu_wells.client.data_classes._spatial_location import _SPATIALLOCATION_PROPERTIES_BY_FIELD


class SpatialLocationAPI(TypeAPI[SpatialLocation, SpatialLocationApply, SpatialLocationList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=SpatialLocation,
            class_apply_type=SpatialLocationApply,
            class_list=SpatialLocationList,
        )
        self._view_id = view_id

    def apply(
        self, spatial_location: SpatialLocationApply | Sequence[SpatialLocationApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(spatial_location, SpatialLocationApply):
            instances = spatial_location.to_instances_apply()
        else:
            instances = SpatialLocationApplyList(spatial_location).to_instances_apply()
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(self, external_id: str | Sequence[str], space="IntegrationTestsImmutable") -> dm.InstancesDeleteResult:
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

    def retrieve(self, external_id: str | Sequence[str]) -> SpatialLocation | SpatialLocationList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> SpatialLocationList:
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> SpatialLocationList:
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
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
