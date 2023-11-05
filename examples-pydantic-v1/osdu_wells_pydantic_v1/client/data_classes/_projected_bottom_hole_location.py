from __future__ import annotations

from typing import Literal, TYPE_CHECKING, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

if TYPE_CHECKING:
    from ._as_ingested_coordinates import AsIngestedCoordinatesApply
    from ._wgs_84_coordinates import WgsCoordinatesApply

__all__ = [
    "ProjectedBottomHoleLocation",
    "ProjectedBottomHoleLocationApply",
    "ProjectedBottomHoleLocationList",
    "ProjectedBottomHoleLocationApplyList",
    "ProjectedBottomHoleLocationFields",
    "ProjectedBottomHoleLocationTextFields",
]


ProjectedBottomHoleLocationTextFields = Literal[
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
ProjectedBottomHoleLocationFields = Literal[
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

_PROJECTEDBOTTOMHOLELOCATION_PROPERTIES_BY_FIELD = {
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


class ProjectedBottomHoleLocation(DomainModel):
    """This represent a read version of projected bottom hole location.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the projected bottom hole location.
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
        created_time: The created time of the projected bottom hole location node.
        last_updated_time: The last updated time of the projected bottom hole location node.
        deleted_time: If present, the deleted time of the projected bottom hole location node.
        version: The version of the projected bottom hole location node.
    """

    space: str = "IntegrationTestsImmutable"
    applied_operations: Optional[list[str]] = Field(None, alias="AppliedOperations")
    as_ingested_coordinates: Optional[str] = Field(None, alias="AsIngestedCoordinates")
    coordinate_quality_check_date_time: Optional[str] = Field(None, alias="CoordinateQualityCheckDateTime")
    coordinate_quality_check_performed_by: Optional[str] = Field(None, alias="CoordinateQualityCheckPerformedBy")
    coordinate_quality_check_remarks: Optional[list[str]] = Field(None, alias="CoordinateQualityCheckRemarks")
    qualitative_spatial_accuracy_type_id: Optional[str] = Field(None, alias="QualitativeSpatialAccuracyTypeID")
    quantitative_accuracy_band_id: Optional[str] = Field(None, alias="QuantitativeAccuracyBandID")
    spatial_geometry_type_id: Optional[str] = Field(None, alias="SpatialGeometryTypeID")
    spatial_location_coordinates_date: Optional[str] = Field(None, alias="SpatialLocationCoordinatesDate")
    spatial_parameter_type_id: Optional[str] = Field(None, alias="SpatialParameterTypeID")
    wgs_84_coordinates: Optional[str] = Field(None, alias="Wgs84Coordinates")

    def as_apply(self) -> ProjectedBottomHoleLocationApply:
        """Convert this read version of projected bottom hole location to a write version."""
        return ProjectedBottomHoleLocationApply(
            space=self.space,
            external_id=self.external_id,
            applied_operations=self.applied_operations,
            as_ingested_coordinates=self.as_ingested_coordinates,
            coordinate_quality_check_date_time=self.coordinate_quality_check_date_time,
            coordinate_quality_check_performed_by=self.coordinate_quality_check_performed_by,
            coordinate_quality_check_remarks=self.coordinate_quality_check_remarks,
            qualitative_spatial_accuracy_type_id=self.qualitative_spatial_accuracy_type_id,
            quantitative_accuracy_band_id=self.quantitative_accuracy_band_id,
            spatial_geometry_type_id=self.spatial_geometry_type_id,
            spatial_location_coordinates_date=self.spatial_location_coordinates_date,
            spatial_parameter_type_id=self.spatial_parameter_type_id,
            wgs_84_coordinates=self.wgs_84_coordinates,
        )


class ProjectedBottomHoleLocationApply(DomainModelApply):
    """This represent a write version of projected bottom hole location.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the projected bottom hole location.
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
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
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
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

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
            source = dm.NodeOrEdgeData(
                source=write_view
                or dm.ViewId("IntegrationTestsImmutable", "ProjectedBottomHoleLocation", "447a307957e5b7"),
                properties=properties,
            )
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[source],
            )
            nodes = [this_node]
        else:
            nodes = []

        edges = []
        cache.add(self.external_id)

        if isinstance(self.as_ingested_coordinates, DomainModelApply):
            instances = self.as_ingested_coordinates._to_instances_apply(cache, view_by_write_class)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.wgs_84_coordinates, DomainModelApply):
            instances = self.wgs_84_coordinates._to_instances_apply(cache, view_by_write_class)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class ProjectedBottomHoleLocationList(TypeList[ProjectedBottomHoleLocation]):
    """List of projected bottom hole locations in read version."""

    _NODE = ProjectedBottomHoleLocation

    def as_apply(self) -> ProjectedBottomHoleLocationApplyList:
        """Convert this read version of projected bottom hole location to a write version."""
        return ProjectedBottomHoleLocationApplyList([node.as_apply() for node in self.data])


class ProjectedBottomHoleLocationApplyList(TypeApplyList[ProjectedBottomHoleLocationApply]):
    """List of projected bottom hole locations in write version."""

    _NODE = ProjectedBottomHoleLocationApply
