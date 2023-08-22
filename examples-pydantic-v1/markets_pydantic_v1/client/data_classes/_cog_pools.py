from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from markets_pydantic_v1.client.data_classes._core import DomainModel, DomainModelApply, TypeList

__all__ = ["CogPool", "CogPoolApply", "CogPoolList"]


class CogPool(DomainModel):
    space: ClassVar[str] = "market"
    max_price: Optional[float] = Field(None, alias="maxPrice")
    min_price: Optional[float] = Field(None, alias="minPrice")
    name: Optional[str] = None
    time_unit: Optional[str] = Field(None, alias="timeUnit")
    timezone: Optional[str] = None


class CogPoolApply(DomainModelApply):
    space: ClassVar[str] = "market"
    max_price: Optional[float] = None
    min_price: Optional[float] = None
    name: Optional[str] = None
    time_unit: Optional[str] = None
    timezone: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.max_price is not None:
            properties["maxPrice"] = self.max_price
        if self.min_price is not None:
            properties["minPrice"] = self.min_price
        if self.time_unit is not None:
            properties["timeUnit"] = self.time_unit
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("market", "CogPool"),
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


class CogPoolList(TypeList[CogPool]):
    _NODE = CogPool
