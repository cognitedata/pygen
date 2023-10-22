from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = ["PygenPool", "PygenPoolApply", "PygenPoolList", "PygenPoolApplyList", "PygenPoolTextFields"]


PygenPoolTextFields = Literal["name", "timezone"]

_PYGENPOOL_TEXT_PROPERTIES_BY_FIELD = {
    "name": "name",
    "timezone": "timezone",
}


class PygenPool(DomainModel):
    space: str = "market"
    day_of_week: Optional[int] = Field(None, alias="dayOfWeek")
    name: Optional[str] = None
    timezone: Optional[str] = None

    def as_apply(self) -> PygenPoolApply:
        return PygenPoolApply(
            external_id=self.external_id,
            day_of_week=self.day_of_week,
            name=self.name,
            timezone=self.timezone,
        )


class PygenPoolApply(DomainModelApply):
    space: str = "market"
    day_of_week: Optional[int] = None
    name: Optional[str] = None
    timezone: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.day_of_week is not None:
            properties["dayOfWeek"] = self.day_of_week
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("market", "PygenPool"),
                properties=properties,
            )
            sources.append(source)
        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.timezone is not None:
            properties["timezone"] = self.timezone
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("market", "Market"),
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


class PygenPoolList(TypeList[PygenPool]):
    _NODE = PygenPool

    def as_apply(self) -> PygenPoolApplyList:
        return PygenPoolApplyList([node.as_apply() for node in self.data])


class PygenPoolApplyList(TypeApplyList[PygenPoolApply]):
    _NODE = PygenPoolApply
