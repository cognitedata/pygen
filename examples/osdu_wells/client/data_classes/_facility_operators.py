from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = [
    "FacilityOperators",
    "FacilityOperatorsApply",
    "FacilityOperatorsList",
    "FacilityOperatorsApplyList",
    "FacilityOperatorsFields",
    "FacilityOperatorsTextFields",
]


FacilityOperatorsTextFields = Literal[
    "effective_date_time",
    "facility_operator_id",
    "facility_operator_organisation_id",
    "remark",
    "termination_date_time",
]
FacilityOperatorsFields = Literal[
    "effective_date_time",
    "facility_operator_id",
    "facility_operator_organisation_id",
    "remark",
    "termination_date_time",
]

_FACILITYOPERATORS_PROPERTIES_BY_FIELD = {
    "effective_date_time": "EffectiveDateTime",
    "facility_operator_id": "FacilityOperatorID",
    "facility_operator_organisation_id": "FacilityOperatorOrganisationID",
    "remark": "Remark",
    "termination_date_time": "TerminationDateTime",
}


class FacilityOperators(DomainModel):
    space: str = "IntegrationTestsImmutable"
    effective_date_time: Optional[str] = Field(None, alias="EffectiveDateTime")
    facility_operator_id: Optional[str] = Field(None, alias="FacilityOperatorID")
    facility_operator_organisation_id: Optional[str] = Field(None, alias="FacilityOperatorOrganisationID")
    remark: Optional[str] = Field(None, alias="Remark")
    termination_date_time: Optional[str] = Field(None, alias="TerminationDateTime")

    def as_apply(self) -> FacilityOperatorsApply:
        return FacilityOperatorsApply(
            space=self.space,
            external_id=self.external_id,
            effective_date_time=self.effective_date_time,
            facility_operator_id=self.facility_operator_id,
            facility_operator_organisation_id=self.facility_operator_organisation_id,
            remark=self.remark,
            termination_date_time=self.termination_date_time,
        )


class FacilityOperatorsApply(DomainModelApply):
    space: str = "IntegrationTestsImmutable"
    effective_date_time: Optional[str] = Field(None, alias="EffectiveDateTime")
    facility_operator_id: Optional[str] = Field(None, alias="FacilityOperatorID")
    facility_operator_organisation_id: Optional[str] = Field(None, alias="FacilityOperatorOrganisationID")
    remark: Optional[str] = Field(None, alias="Remark")
    termination_date_time: Optional[str] = Field(None, alias="TerminationDateTime")

    def _to_instances_apply(self, cache: set[str], write_view: dm.ViewId | None) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        properties = {}
        if self.effective_date_time is not None:
            properties["EffectiveDateTime"] = self.effective_date_time
        if self.facility_operator_id is not None:
            properties["FacilityOperatorID"] = self.facility_operator_id
        if self.facility_operator_organisation_id is not None:
            properties["FacilityOperatorOrganisationID"] = self.facility_operator_organisation_id
        if self.remark is not None:
            properties["Remark"] = self.remark
        if self.termination_date_time is not None:
            properties["TerminationDateTime"] = self.termination_date_time
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "FacilityOperators", "935498861713d0"),
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


class FacilityOperatorsList(TypeList[FacilityOperators]):
    _NODE = FacilityOperators

    def as_apply(self) -> FacilityOperatorsApplyList:
        return FacilityOperatorsApplyList([node.as_apply() for node in self.data])


class FacilityOperatorsApplyList(TypeApplyList[FacilityOperatorsApply]):
    _NODE = FacilityOperatorsApply
