from __future__ import annotations

from typing import Literal, TYPE_CHECKING, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

if TYPE_CHECKING:
    from ._as_ingested_coordinates import AsIngestedCoordinatesApply
    from ._wgs_84_coordinates import WgsCoordinatesApply

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

    def as_apply(self) -> SpatialLocationApply:
        return SpatialLocationApply(
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


class SpatialLocationApply(DomainModelApply):
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
                "space": "IntegrationTestsImmutable",
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
                "space": "IntegrationTestsImmutable",
                "externalId": self.wgs_84_coordinates
                if isinstance(self.wgs_84_coordinates, str)
                else self.wgs_84_coordinates.external_id,
            }
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "SpatialLocation", "697432f011ef60"),
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


class SpatialLocationList(TypeList[SpatialLocation]):
    _NODE = SpatialLocation

    def as_apply(self) -> SpatialLocationApplyList:
        return SpatialLocationApplyList([node.as_apply() for node in self.data])


class SpatialLocationApplyList(TypeApplyList[SpatialLocationApply]):
    _NODE = SpatialLocationApply
