from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = [
    "FacilityStates",
    "FacilityStatesApply",
    "FacilityStatesList",
    "FacilityStatesApplyList",
    "FacilityStatesFields",
    "FacilityStatesTextFields",
]


FacilityStatesTextFields = Literal["effective_date_time", "facility_state_type_id", "remark", "termination_date_time"]
FacilityStatesFields = Literal["effective_date_time", "facility_state_type_id", "remark", "termination_date_time"]

_FACILITYSTATES_PROPERTIES_BY_FIELD = {
    "effective_date_time": "EffectiveDateTime",
    "facility_state_type_id": "FacilityStateTypeID",
    "remark": "Remark",
    "termination_date_time": "TerminationDateTime",
}


class FacilityStates(DomainModel):
    space: str = "IntegrationTestsImmutable"
    effective_date_time: Optional[str] = Field(None, alias="EffectiveDateTime")
    facility_state_type_id: Optional[str] = Field(None, alias="FacilityStateTypeID")
    remark: Optional[str] = Field(None, alias="Remark")
    termination_date_time: Optional[str] = Field(None, alias="TerminationDateTime")

    def as_apply(self) -> FacilityStatesApply:
        return FacilityStatesApply(
            space=self.space,
            external_id=self.external_id,
            effective_date_time=self.effective_date_time,
            facility_state_type_id=self.facility_state_type_id,
            remark=self.remark,
            termination_date_time=self.termination_date_time,
        )


class FacilityStatesApply(DomainModelApply):
    space: str = "IntegrationTestsImmutable"
    effective_date_time: Optional[str] = Field(None, alias="EffectiveDateTime")
    facility_state_type_id: Optional[str] = Field(None, alias="FacilityStateTypeID")
    remark: Optional[str] = Field(None, alias="Remark")
    termination_date_time: Optional[str] = Field(None, alias="TerminationDateTime")

    def _to_instances_apply(self, cache: set[str], write_view: dm.ViewId | None) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        properties = {}
        if self.effective_date_time is not None:
            properties["EffectiveDateTime"] = self.effective_date_time
        if self.facility_state_type_id is not None:
            properties["FacilityStateTypeID"] = self.facility_state_type_id
        if self.remark is not None:
            properties["Remark"] = self.remark
        if self.termination_date_time is not None:
            properties["TerminationDateTime"] = self.termination_date_time
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "FacilityStates", "a12316ff3d8033"),
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


class FacilityStatesList(TypeList[FacilityStates]):
    _NODE = FacilityStates

    def as_apply(self) -> FacilityStatesApplyList:
        return FacilityStatesApplyList([node.as_apply() for node in self.data])


class FacilityStatesApplyList(TypeApplyList[FacilityStatesApply]):
    _NODE = FacilityStatesApply
