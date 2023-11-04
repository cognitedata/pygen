from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = [
    "VerticalMeasurement",
    "VerticalMeasurementApply",
    "VerticalMeasurementList",
    "VerticalMeasurementApplyList",
    "VerticalMeasurementFields",
    "VerticalMeasurementTextFields",
]


VerticalMeasurementTextFields = Literal[
    "effective_date_time",
    "termination_date_time",
    "vertical_crsid",
    "vertical_measurement_description",
    "vertical_measurement_path_id",
    "vertical_measurement_source_id",
    "vertical_measurement_type_id",
    "vertical_measurement_unit_of_measure_id",
    "vertical_reference_entity_id",
    "vertical_reference_id",
    "wellbore_tvd_trajectory_id",
]
VerticalMeasurementFields = Literal[
    "effective_date_time",
    "termination_date_time",
    "vertical_crsid",
    "vertical_measurement",
    "vertical_measurement_description",
    "vertical_measurement_path_id",
    "vertical_measurement_source_id",
    "vertical_measurement_type_id",
    "vertical_measurement_unit_of_measure_id",
    "vertical_reference_entity_id",
    "vertical_reference_id",
    "wellbore_tvd_trajectory_id",
]

_VERTICALMEASUREMENT_PROPERTIES_BY_FIELD = {
    "effective_date_time": "EffectiveDateTime",
    "termination_date_time": "TerminationDateTime",
    "vertical_crsid": "VerticalCRSID",
    "vertical_measurement": "VerticalMeasurement",
    "vertical_measurement_description": "VerticalMeasurementDescription",
    "vertical_measurement_path_id": "VerticalMeasurementPathID",
    "vertical_measurement_source_id": "VerticalMeasurementSourceID",
    "vertical_measurement_type_id": "VerticalMeasurementTypeID",
    "vertical_measurement_unit_of_measure_id": "VerticalMeasurementUnitOfMeasureID",
    "vertical_reference_entity_id": "VerticalReferenceEntityID",
    "vertical_reference_id": "VerticalReferenceID",
    "wellbore_tvd_trajectory_id": "WellboreTVDTrajectoryID",
}


class VerticalMeasurement(DomainModel):
    """This represent a read version of vertical measurement.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the vertical measurement.
        effective_date_time: The effective date time field.
        termination_date_time: The termination date time field.
        vertical_crsid: The vertical crsid field.
        vertical_measurement: The vertical measurement field.
        vertical_measurement_description: The vertical measurement description field.
        vertical_measurement_path_id: The vertical measurement path id field.
        vertical_measurement_source_id: The vertical measurement source id field.
        vertical_measurement_type_id: The vertical measurement type id field.
        vertical_measurement_unit_of_measure_id: The vertical measurement unit of measure id field.
        vertical_reference_entity_id: The vertical reference entity id field.
        vertical_reference_id: The vertical reference id field.
        wellbore_tvd_trajectory_id: The wellbore tvd trajectory id field.
        created_time: The created time of the vertical measurement node.
        last_updated_time: The last updated time of the vertical measurement node.
        deleted_time: If present, the deleted time of the vertical measurement node.
        version: The version of the vertical measurement node.
    """

    space: str = "IntegrationTestsImmutable"
    effective_date_time: Optional[str] = Field(None, alias="EffectiveDateTime")
    termination_date_time: Optional[str] = Field(None, alias="TerminationDateTime")
    vertical_crsid: Optional[str] = Field(None, alias="VerticalCRSID")
    vertical_measurement: Optional[float] = Field(None, alias="VerticalMeasurement")
    vertical_measurement_description: Optional[str] = Field(None, alias="VerticalMeasurementDescription")
    vertical_measurement_path_id: Optional[str] = Field(None, alias="VerticalMeasurementPathID")
    vertical_measurement_source_id: Optional[str] = Field(None, alias="VerticalMeasurementSourceID")
    vertical_measurement_type_id: Optional[str] = Field(None, alias="VerticalMeasurementTypeID")
    vertical_measurement_unit_of_measure_id: Optional[str] = Field(None, alias="VerticalMeasurementUnitOfMeasureID")
    vertical_reference_entity_id: Optional[str] = Field(None, alias="VerticalReferenceEntityID")
    vertical_reference_id: Optional[str] = Field(None, alias="VerticalReferenceID")
    wellbore_tvd_trajectory_id: Optional[str] = Field(None, alias="WellboreTVDTrajectoryID")

    def as_apply(self) -> VerticalMeasurementApply:
        """Convert this read version of vertical measurement to a write version."""
        return VerticalMeasurementApply(
            space=self.space,
            external_id=self.external_id,
            effective_date_time=self.effective_date_time,
            termination_date_time=self.termination_date_time,
            vertical_crsid=self.vertical_crsid,
            vertical_measurement=self.vertical_measurement,
            vertical_measurement_description=self.vertical_measurement_description,
            vertical_measurement_path_id=self.vertical_measurement_path_id,
            vertical_measurement_source_id=self.vertical_measurement_source_id,
            vertical_measurement_type_id=self.vertical_measurement_type_id,
            vertical_measurement_unit_of_measure_id=self.vertical_measurement_unit_of_measure_id,
            vertical_reference_entity_id=self.vertical_reference_entity_id,
            vertical_reference_id=self.vertical_reference_id,
            wellbore_tvd_trajectory_id=self.wellbore_tvd_trajectory_id,
        )


class VerticalMeasurementApply(DomainModelApply):
    """This represent a write version of vertical measurement.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the vertical measurement.
        effective_date_time: The effective date time field.
        termination_date_time: The termination date time field.
        vertical_crsid: The vertical crsid field.
        vertical_measurement: The vertical measurement field.
        vertical_measurement_description: The vertical measurement description field.
        vertical_measurement_path_id: The vertical measurement path id field.
        vertical_measurement_source_id: The vertical measurement source id field.
        vertical_measurement_type_id: The vertical measurement type id field.
        vertical_measurement_unit_of_measure_id: The vertical measurement unit of measure id field.
        vertical_reference_entity_id: The vertical reference entity id field.
        vertical_reference_id: The vertical reference id field.
        wellbore_tvd_trajectory_id: The wellbore tvd trajectory id field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "IntegrationTestsImmutable"
    effective_date_time: Optional[str] = Field(None, alias="EffectiveDateTime")
    termination_date_time: Optional[str] = Field(None, alias="TerminationDateTime")
    vertical_crsid: Optional[str] = Field(None, alias="VerticalCRSID")
    vertical_measurement: Optional[float] = Field(None, alias="VerticalMeasurement")
    vertical_measurement_description: Optional[str] = Field(None, alias="VerticalMeasurementDescription")
    vertical_measurement_path_id: Optional[str] = Field(None, alias="VerticalMeasurementPathID")
    vertical_measurement_source_id: Optional[str] = Field(None, alias="VerticalMeasurementSourceID")
    vertical_measurement_type_id: Optional[str] = Field(None, alias="VerticalMeasurementTypeID")
    vertical_measurement_unit_of_measure_id: Optional[str] = Field(None, alias="VerticalMeasurementUnitOfMeasureID")
    vertical_reference_entity_id: Optional[str] = Field(None, alias="VerticalReferenceEntityID")
    vertical_reference_id: Optional[str] = Field(None, alias="VerticalReferenceID")
    wellbore_tvd_trajectory_id: Optional[str] = Field(None, alias="WellboreTVDTrajectoryID")

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.effective_date_time is not None:
            properties["EffectiveDateTime"] = self.effective_date_time
        if self.termination_date_time is not None:
            properties["TerminationDateTime"] = self.termination_date_time
        if self.vertical_crsid is not None:
            properties["VerticalCRSID"] = self.vertical_crsid
        if self.vertical_measurement is not None:
            properties["VerticalMeasurement"] = self.vertical_measurement
        if self.vertical_measurement_description is not None:
            properties["VerticalMeasurementDescription"] = self.vertical_measurement_description
        if self.vertical_measurement_path_id is not None:
            properties["VerticalMeasurementPathID"] = self.vertical_measurement_path_id
        if self.vertical_measurement_source_id is not None:
            properties["VerticalMeasurementSourceID"] = self.vertical_measurement_source_id
        if self.vertical_measurement_type_id is not None:
            properties["VerticalMeasurementTypeID"] = self.vertical_measurement_type_id
        if self.vertical_measurement_unit_of_measure_id is not None:
            properties["VerticalMeasurementUnitOfMeasureID"] = self.vertical_measurement_unit_of_measure_id
        if self.vertical_reference_entity_id is not None:
            properties["VerticalReferenceEntityID"] = self.vertical_reference_entity_id
        if self.vertical_reference_id is not None:
            properties["VerticalReferenceID"] = self.vertical_reference_id
        if self.wellbore_tvd_trajectory_id is not None:
            properties["WellboreTVDTrajectoryID"] = self.wellbore_tvd_trajectory_id
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "VerticalMeasurement", "fd63ec6e91292f"),
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

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class VerticalMeasurementList(TypeList[VerticalMeasurement]):
    """List of vertical measurements in read version."""

    _NODE = VerticalMeasurement

    def as_apply(self) -> VerticalMeasurementApplyList:
        """Convert this read version of vertical measurement to a write version."""
        return VerticalMeasurementApplyList([node.as_apply() for node in self.data])


class VerticalMeasurementApplyList(TypeApplyList[VerticalMeasurementApply]):
    """List of vertical measurements in write version."""

    _NODE = VerticalMeasurementApply
