from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = [
    "WellboreCosts",
    "WellboreCostsApply",
    "WellboreCostsList",
    "WellboreCostsApplyList",
    "WellboreCostsFields",
    "WellboreCostsTextFields",
]


WellboreCostsTextFields = Literal["activity_type_id"]
WellboreCostsFields = Literal["activity_type_id", "cost"]

_WELLBORECOSTS_PROPERTIES_BY_FIELD = {
    "activity_type_id": "ActivityTypeID",
    "cost": "Cost",
}


class WellboreCosts(DomainModel):
    space: str = "IntegrationTestsImmutable"
    activity_type_id: Optional[str] = Field(None, alias="ActivityTypeID")
    cost: Optional[float] = Field(None, alias="Cost")

    def as_apply(self) -> WellboreCostsApply:
        return WellboreCostsApply(
            space=self.space,
            external_id=self.external_id,
            activity_type_id=self.activity_type_id,
            cost=self.cost,
        )


class WellboreCostsApply(DomainModelApply):
    space: str = "IntegrationTestsImmutable"
    activity_type_id: Optional[str] = Field(None, alias="ActivityTypeID")
    cost: Optional[float] = Field(None, alias="Cost")

    def _to_instances_apply(self, cache: set[str], write_view: dm.ViewId | None) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        properties = {}
        if self.activity_type_id is not None:
            properties["ActivityTypeID"] = self.activity_type_id
        if self.cost is not None:
            properties["Cost"] = self.cost
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "WellboreCosts", "b4f71248f398a2"),
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


class WellboreCostsList(TypeList[WellboreCosts]):
    _NODE = WellboreCosts

    def as_apply(self) -> WellboreCostsApplyList:
        return WellboreCostsApplyList([node.as_apply() for node in self.data])


class WellboreCostsApplyList(TypeApplyList[WellboreCostsApply]):
    _NODE = WellboreCostsApply
