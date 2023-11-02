from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = [
    "DrillingReasons",
    "DrillingReasonsApply",
    "DrillingReasonsList",
    "DrillingReasonsApplyList",
    "DrillingReasonsFields",
    "DrillingReasonsTextFields",
]


DrillingReasonsTextFields = Literal["effective_date_time", "lahee_class_id", "remark", "termination_date_time"]
DrillingReasonsFields = Literal["effective_date_time", "lahee_class_id", "remark", "termination_date_time"]

_DRILLINGREASONS_PROPERTIES_BY_FIELD = {
    "effective_date_time": "EffectiveDateTime",
    "lahee_class_id": "LaheeClassID",
    "remark": "Remark",
    "termination_date_time": "TerminationDateTime",
}


class DrillingReasons(DomainModel):
    space: str = "IntegrationTestsImmutable"
    effective_date_time: Optional[str] = Field(None, alias="EffectiveDateTime")
    lahee_class_id: Optional[str] = Field(None, alias="LaheeClassID")
    remark: Optional[str] = Field(None, alias="Remark")
    termination_date_time: Optional[str] = Field(None, alias="TerminationDateTime")

    def as_apply(self) -> DrillingReasonsApply:
        return DrillingReasonsApply(
            space=self.space,
            external_id=self.external_id,
            effective_date_time=self.effective_date_time,
            lahee_class_id=self.lahee_class_id,
            remark=self.remark,
            termination_date_time=self.termination_date_time,
        )


class DrillingReasonsApply(DomainModelApply):
    space: str = "IntegrationTestsImmutable"
    effective_date_time: Optional[str] = Field(None, alias="EffectiveDateTime")
    lahee_class_id: Optional[str] = Field(None, alias="LaheeClassID")
    remark: Optional[str] = Field(None, alias="Remark")
    termination_date_time: Optional[str] = Field(None, alias="TerminationDateTime")

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.effective_date_time is not None:
            properties["EffectiveDateTime"] = self.effective_date_time
        if self.lahee_class_id is not None:
            properties["LaheeClassID"] = self.lahee_class_id
        if self.remark is not None:
            properties["Remark"] = self.remark
        if self.termination_date_time is not None:
            properties["TerminationDateTime"] = self.termination_date_time
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "DrillingReasons", "220055a8165644"),
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


class DrillingReasonsList(TypeList[DrillingReasons]):
    _NODE = DrillingReasons

    def as_apply(self) -> DrillingReasonsApplyList:
        return DrillingReasonsApplyList([node.as_apply() for node in self.data])


class DrillingReasonsApplyList(TypeApplyList[DrillingReasonsApply]):
    _NODE = DrillingReasonsApply
