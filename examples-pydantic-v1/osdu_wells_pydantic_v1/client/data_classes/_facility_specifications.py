from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = [
    "FacilitySpecifications",
    "FacilitySpecificationsApply",
    "FacilitySpecificationsList",
    "FacilitySpecificationsApplyList",
    "FacilitySpecificationsFields",
    "FacilitySpecificationsTextFields",
]


FacilitySpecificationsTextFields = Literal[
    "effective_date_time",
    "facility_specification_date_time",
    "facility_specification_text",
    "parameter_type_id",
    "termination_date_time",
    "unit_of_measure_id",
]
FacilitySpecificationsFields = Literal[
    "effective_date_time",
    "facility_specification_date_time",
    "facility_specification_indicator",
    "facility_specification_quantity",
    "facility_specification_text",
    "parameter_type_id",
    "termination_date_time",
    "unit_of_measure_id",
]

_FACILITYSPECIFICATIONS_PROPERTIES_BY_FIELD = {
    "effective_date_time": "EffectiveDateTime",
    "facility_specification_date_time": "FacilitySpecificationDateTime",
    "facility_specification_indicator": "FacilitySpecificationIndicator",
    "facility_specification_quantity": "FacilitySpecificationQuantity",
    "facility_specification_text": "FacilitySpecificationText",
    "parameter_type_id": "ParameterTypeID",
    "termination_date_time": "TerminationDateTime",
    "unit_of_measure_id": "UnitOfMeasureID",
}


class FacilitySpecifications(DomainModel):
    space: str = "IntegrationTestsImmutable"
    effective_date_time: Optional[str] = Field(None, alias="EffectiveDateTime")
    facility_specification_date_time: Optional[str] = Field(None, alias="FacilitySpecificationDateTime")
    facility_specification_indicator: Optional[bool] = Field(None, alias="FacilitySpecificationIndicator")
    facility_specification_quantity: Optional[float] = Field(None, alias="FacilitySpecificationQuantity")
    facility_specification_text: Optional[str] = Field(None, alias="FacilitySpecificationText")
    parameter_type_id: Optional[str] = Field(None, alias="ParameterTypeID")
    termination_date_time: Optional[str] = Field(None, alias="TerminationDateTime")
    unit_of_measure_id: Optional[str] = Field(None, alias="UnitOfMeasureID")

    def as_apply(self) -> FacilitySpecificationsApply:
        return FacilitySpecificationsApply(
            space=self.space,
            external_id=self.external_id,
            effective_date_time=self.effective_date_time,
            facility_specification_date_time=self.facility_specification_date_time,
            facility_specification_indicator=self.facility_specification_indicator,
            facility_specification_quantity=self.facility_specification_quantity,
            facility_specification_text=self.facility_specification_text,
            parameter_type_id=self.parameter_type_id,
            termination_date_time=self.termination_date_time,
            unit_of_measure_id=self.unit_of_measure_id,
        )


class FacilitySpecificationsApply(DomainModelApply):
    space: str = "IntegrationTestsImmutable"
    effective_date_time: Optional[str] = Field(None, alias="EffectiveDateTime")
    facility_specification_date_time: Optional[str] = Field(None, alias="FacilitySpecificationDateTime")
    facility_specification_indicator: Optional[bool] = Field(None, alias="FacilitySpecificationIndicator")
    facility_specification_quantity: Optional[float] = Field(None, alias="FacilitySpecificationQuantity")
    facility_specification_text: Optional[str] = Field(None, alias="FacilitySpecificationText")
    parameter_type_id: Optional[str] = Field(None, alias="ParameterTypeID")
    termination_date_time: Optional[str] = Field(None, alias="TerminationDateTime")
    unit_of_measure_id: Optional[str] = Field(None, alias="UnitOfMeasureID")

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.effective_date_time is not None:
            properties["EffectiveDateTime"] = self.effective_date_time
        if self.facility_specification_date_time is not None:
            properties["FacilitySpecificationDateTime"] = self.facility_specification_date_time
        if self.facility_specification_indicator is not None:
            properties["FacilitySpecificationIndicator"] = self.facility_specification_indicator
        if self.facility_specification_quantity is not None:
            properties["FacilitySpecificationQuantity"] = self.facility_specification_quantity
        if self.facility_specification_text is not None:
            properties["FacilitySpecificationText"] = self.facility_specification_text
        if self.parameter_type_id is not None:
            properties["ParameterTypeID"] = self.parameter_type_id
        if self.termination_date_time is not None:
            properties["TerminationDateTime"] = self.termination_date_time
        if self.unit_of_measure_id is not None:
            properties["UnitOfMeasureID"] = self.unit_of_measure_id
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "FacilitySpecifications", "1b7ddbd5d36655"),
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


class FacilitySpecificationsList(TypeList[FacilitySpecifications]):
    _NODE = FacilitySpecifications

    def as_apply(self) -> FacilitySpecificationsApplyList:
        return FacilitySpecificationsApplyList([node.as_apply() for node in self.data])


class FacilitySpecificationsApplyList(TypeApplyList[FacilitySpecificationsApply]):
    _NODE = FacilitySpecificationsApply
