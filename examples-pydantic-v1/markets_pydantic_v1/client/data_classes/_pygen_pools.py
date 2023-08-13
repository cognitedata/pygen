from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from markets_pydantic_v1.client.data_classes._core import DomainModel, DomainModelApply, TypeList

__all__ = ["PygenPool", "PygenPoolApply", "PygenPoolList"]


class PygenPool(DomainModel):
    space: ClassVar[str] = "market"
    day_of_week: Optional[int] = Field(None, alias="dayOfWeek")
    name: Optional[str] = None
    timezone: Optional[str] = None


class PygenPoolApply(DomainModelApply):
    space: ClassVar[str] = "market"
    day_of_week: Optional[int] = None
    name: Optional[str] = None
    timezone: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("market", "PygenPool"),
            properties={
                "dayOfWeek": self.day_of_week,
            },
        )
        sources.append(source)

        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("market", "Market"),
            properties={
                "name": self.name,
                "timezone": self.timezone,
            },
        )
        sources.append(source)

        this_node = dm.NodeApply(
            space=self.space,
            external_id=self.external_id,
            existing_version=self.existing_version,
            sources=sources,
        )
        nodes = [this_node]
        edges = []

        return dm.InstancesApply(nodes, edges)


class PygenPoolList(TypeList[PygenPool]):
    _NODE = PygenPool
