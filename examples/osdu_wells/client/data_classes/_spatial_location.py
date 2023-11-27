from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
)

if TYPE_CHECKING:
    from ._as_ingested_coordinates import AsIngestedCoordinates, AsIngestedCoordinatesApply
    from ._wgs_84_coordinates import WgsCoordinates, WgsCoordinatesApply


__all__ = [
    "SpatialLocation",
    "SpatialLocationApply",
    "SpatialLocationList",
    "SpatialLocationApplyList",
    "SpatialLocationFields",
    "SpatialLocationTextFields",
]


SpatialLocationTextFields = Literal[
    "applied_operations",
    "coordinate_quality_check_date_time",
    "coordinate_quality_check_performed_by",
    "coordinate_quality_check_remarks",
    "qualitative_spatial_accuracy_type_id",
    "quantitative_accuracy_band_id",
    "spatial_geometry_type_id",
    "spatial_location_coordinates_date",
    "spatial_parameter_type_id",
]
SpatialLocationFields = Literal[
    "applied_operations",
    "coordinate_quality_check_date_time",
    "coordinate_quality_check_performed_by",
    "coordinate_quality_check_remarks",
    "qualitative_spatial_accuracy_type_id",
    "quantitative_accuracy_band_id",
    "spatial_geometry_type_id",
    "spatial_location_coordinates_date",
    "spatial_parameter_type_id",
]

_SPATIALLOCATION_PROPERTIES_BY_FIELD = {
    "applied_operations": "AppliedOperations",
    "coordinate_quality_check_date_time": "CoordinateQualityCheckDateTime",
    "coordinate_quality_check_performed_by": "CoordinateQualityCheckPerformedBy",
    "coordinate_quality_check_remarks": "CoordinateQualityCheckRemarks",
    "qualitative_spatial_accuracy_type_id": "QualitativeSpatialAccuracyTypeID",
    "quantitative_accuracy_band_id": "QuantitativeAccuracyBandID",
    "spatial_geometry_type_id": "SpatialGeometryTypeID",
    "spatial_location_coordinates_date": "SpatialLocationCoordinatesDate",
    "spatial_parameter_type_id": "SpatialParameterTypeID",
}


class SpatialLocation(DomainModel):
    """This represents the reading version of spatial location.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the spatial location.
        applied_operations: The applied operation field.
        as_ingested_coordinates: The as ingested coordinate field.
        coordinate_quality_check_date_time: The coordinate quality check date time field.
        coordinate_quality_check_performed_by: The coordinate quality check performed by field.
        coordinate_quality_check_remarks: The coordinate quality check remark field.
        qualitative_spatial_accuracy_type_id: The qualitative spatial accuracy type id field.
        quantitative_accuracy_band_id: The quantitative accuracy band id field.
        spatial_geometry_type_id: The spatial geometry type id field.
        spatial_location_coordinates_date: The spatial location coordinates date field.
        spatial_parameter_type_id: The spatial parameter type id field.
        wgs_84_coordinates: The wgs 84 coordinate field.
        created_time: The created time of the spatial location node.
        last_updated_time: The last updated time of the spatial location node.
        deleted_time: If present, the deleted time of the spatial location node.
        version: The version of the spatial location node.
    """

    space: str = "IntegrationTestsImmutable"
    applied_operations: Optional[list[str]] = Field(None, alias="AppliedOperations")
    as_ingested_coordinates: Union[AsIngestedCoordinates, str, None] = Field(
        None, repr=False, alias="AsIngestedCoordinates"
    )
    coordinate_quality_check_date_time: Optional[str] = Field(None, alias="CoordinateQualityCheckDateTime")
    coordinate_quality_check_performed_by: Optional[str] = Field(None, alias="CoordinateQualityCheckPerformedBy")
    coordinate_quality_check_remarks: Optional[list[str]] = Field(None, alias="CoordinateQualityCheckRemarks")
    qualitative_spatial_accuracy_type_id: Optional[str] = Field(None, alias="QualitativeSpatialAccuracyTypeID")
    quantitative_accuracy_band_id: Optional[str] = Field(None, alias="QuantitativeAccuracyBandID")
    spatial_geometry_type_id: Optional[str] = Field(None, alias="SpatialGeometryTypeID")
    spatial_location_coordinates_date: Optional[str] = Field(None, alias="SpatialLocationCoordinatesDate")
    spatial_parameter_type_id: Optional[str] = Field(None, alias="SpatialParameterTypeID")
    wgs_84_coordinates: Union[WgsCoordinates, str, None] = Field(None, repr=False, alias="Wgs84Coordinates")

    def as_apply(self) -> SpatialLocationApply:
        """Convert this read version of spatial location to the writing version."""
        return SpatialLocationApply(
            space=self.space,
            external_id=self.external_id,
            applied_operations=self.applied_operations,
            as_ingested_coordinates=self.as_ingested_coordinates.as_apply()
            if isinstance(self.as_ingested_coordinates, DomainModel)
            else self.as_ingested_coordinates,
            coordinate_quality_check_date_time=self.coordinate_quality_check_date_time,
            coordinate_quality_check_performed_by=self.coordinate_quality_check_performed_by,
            coordinate_quality_check_remarks=self.coordinate_quality_check_remarks,
            qualitative_spatial_accuracy_type_id=self.qualitative_spatial_accuracy_type_id,
            quantitative_accuracy_band_id=self.quantitative_accuracy_band_id,
            spatial_geometry_type_id=self.spatial_geometry_type_id,
            spatial_location_coordinates_date=self.spatial_location_coordinates_date,
            spatial_parameter_type_id=self.spatial_parameter_type_id,
            wgs_84_coordinates=self.wgs_84_coordinates.as_apply()
            if isinstance(self.wgs_84_coordinates, DomainModel)
            else self.wgs_84_coordinates,
        )


class SpatialLocationApply(DomainModelApply):
    """This represents the writing version of spatial location.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the spatial location.
        applied_operations: The applied operation field.
        as_ingested_coordinates: The as ingested coordinate field.
        coordinate_quality_check_date_time: The coordinate quality check date time field.
        coordinate_quality_check_performed_by: The coordinate quality check performed by field.
        coordinate_quality_check_remarks: The coordinate quality check remark field.
        qualitative_spatial_accuracy_type_id: The qualitative spatial accuracy type id field.
        quantitative_accuracy_band_id: The quantitative accuracy band id field.
        spatial_geometry_type_id: The spatial geometry type id field.
        spatial_location_coordinates_date: The spatial location coordinates date field.
        spatial_parameter_type_id: The spatial parameter type id field.
        wgs_84_coordinates: The wgs 84 coordinate field.
        existing_version: Fail the ingestion request if the spatial location version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "IntegrationTestsImmutable"
    applied_operations: Optional[list[str]] = Field(None, alias="AppliedOperations")
    as_ingested_coordinates: Union[AsIngestedCoordinatesApply, str, None] = Field(
        None, repr=False, alias="AsIngestedCoordinates"
    )
    coordinate_quality_check_date_time: Optional[str] = Field(None, alias="CoordinateQualityCheckDateTime")
    coordinate_quality_check_performed_by: Optional[str] = Field(None, alias="CoordinateQualityCheckPerformedBy")
    coordinate_quality_check_remarks: Optional[list[str]] = Field(None, alias="CoordinateQualityCheckRemarks")
    qualitative_spatial_accuracy_type_id: Optional[str] = Field(None, alias="QualitativeSpatialAccuracyTypeID")
    quantitative_accuracy_band_id: Optional[str] = Field(None, alias="QuantitativeAccuracyBandID")
    spatial_geometry_type_id: Optional[str] = Field(None, alias="SpatialGeometryTypeID")
    spatial_location_coordinates_date: Optional[str] = Field(None, alias="SpatialLocationCoordinatesDate")
    spatial_parameter_type_id: Optional[str] = Field(None, alias="SpatialParameterTypeID")
    wgs_84_coordinates: Union[WgsCoordinatesApply, str, None] = Field(None, repr=False, alias="Wgs84Coordinates")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "IntegrationTestsImmutable", "SpatialLocation", "697432f011ef60"
        )

        properties = {}
        if self.applied_operations is not None:
            properties["AppliedOperations"] = self.applied_operations
        if self.as_ingested_coordinates is not None:
            properties["AsIngestedCoordinates"] = {
                "space": self.space
                if isinstance(self.as_ingested_coordinates, str)
                else self.as_ingested_coordinates.space,
                "externalId": self.as_ingested_coordinates
                if isinstance(self.as_ingested_coordinates, str)
                else self.as_ingested_coordinates.external_id,
            }
        if self.coordinate_quality_check_date_time is not None:
            properties["CoordinateQualityCheckDateTime"] = self.coordinate_quality_check_date_time
        if self.coordinate_quality_check_performed_by is not None:
            properties["CoordinateQualityCheckPerformedBy"] = self.coordinate_quality_check_performed_by
        if self.coordinate_quality_check_remarks is not None:
            properties["CoordinateQualityCheckRemarks"] = self.coordinate_quality_check_remarks
        if self.qualitative_spatial_accuracy_type_id is not None:
            properties["QualitativeSpatialAccuracyTypeID"] = self.qualitative_spatial_accuracy_type_id
        if self.quantitative_accuracy_band_id is not None:
            properties["QuantitativeAccuracyBandID"] = self.quantitative_accuracy_band_id
        if self.spatial_geometry_type_id is not None:
            properties["SpatialGeometryTypeID"] = self.spatial_geometry_type_id
        if self.spatial_location_coordinates_date is not None:
            properties["SpatialLocationCoordinatesDate"] = self.spatial_location_coordinates_date
        if self.spatial_parameter_type_id is not None:
            properties["SpatialParameterTypeID"] = self.spatial_parameter_type_id
        if self.wgs_84_coordinates is not None:
            properties["Wgs84Coordinates"] = {
                "space": self.space if isinstance(self.wgs_84_coordinates, str) else self.wgs_84_coordinates.space,
                "externalId": self.wgs_84_coordinates
                if isinstance(self.wgs_84_coordinates, str)
                else self.wgs_84_coordinates.external_id,
            }

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        if isinstance(self.as_ingested_coordinates, DomainModelApply):
            other_resources = self.as_ingested_coordinates._to_instances_apply(cache, view_by_write_class)
            resources.extend(other_resources)

        if isinstance(self.wgs_84_coordinates, DomainModelApply):
            other_resources = self.wgs_84_coordinates._to_instances_apply(cache, view_by_write_class)
            resources.extend(other_resources)

        return resources


class SpatialLocationList(DomainModelList[SpatialLocation]):
    """List of spatial locations in the read version."""

    _INSTANCE = SpatialLocation

    def as_apply(self) -> SpatialLocationApplyList:
        """Convert these read versions of spatial location to the writing versions."""
        return SpatialLocationApplyList([node.as_apply() for node in self.data])


class SpatialLocationApplyList(DomainModelApplyList[SpatialLocationApply]):
    """List of spatial locations in the writing version."""

    _INSTANCE = SpatialLocationApply


def _create_spatial_location_filter(
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
