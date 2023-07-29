from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from markets_pydantic_v1.client.data_classes._core import DomainModel, DomainModelApply, InstancesApply, TypeList

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

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("market", "CogPool"),
            properties={
                "maxPrice": self.max_price,
                "minPrice": self.min_price,
                "timeUnit": self.time_unit,
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

        return InstancesApply(nodes, edges)


class CogPoolList(TypeList[CogPool]):
    _NODE = CogPool
