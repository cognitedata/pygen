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
        return VerticalMeasurementApply(
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
    space: str = "IntegrationTestsImmutable"
    effective_date_time: Optional[str] = None
    termination_date_time: Optional[str] = None
    vertical_crsid: Optional[str] = None
    vertical_measurement: Optional[float] = None
    vertical_measurement_description: Optional[str] = None
    vertical_measurement_path_id: Optional[str] = None
    vertical_measurement_source_id: Optional[str] = None
    vertical_measurement_type_id: Optional[str] = None
    vertical_measurement_unit_of_measure_id: Optional[str] = None
    vertical_reference_entity_id: Optional[str] = None
    vertical_reference_id: Optional[str] = None
    wellbore_tvd_trajectory_id: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
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
                source=dm.ContainerId("IntegrationTestsImmutable", "VerticalMeasurement"),
                properties=properties,
            )
            sources.append(source)
        if sources:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=sources,
            )
            nodes = [this_node]
        else:
            nodes = []

        edges = []
        cache.add(self.external_id)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class VerticalMeasurementList(TypeList[VerticalMeasurement]):
    _NODE = VerticalMeasurement

    def as_apply(self) -> VerticalMeasurementApplyList:
        return VerticalMeasurementApplyList([node.as_apply() for node in self.data])


class VerticalMeasurementApplyList(TypeApplyList[VerticalMeasurementApply]):
    _NODE = VerticalMeasurementApply
