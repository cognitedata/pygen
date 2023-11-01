from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = ["CogPool", "CogPoolApply", "CogPoolList", "CogPoolApplyList", "CogPoolFields", "CogPoolTextFields"]


CogPoolTextFields = Literal["name", "time_unit", "timezone"]
CogPoolFields = Literal["max_price", "min_price", "name", "time_unit", "timezone"]

_COGPOOL_PROPERTIES_BY_FIELD = {
    "max_price": "maxPrice",
    "min_price": "minPrice",
    "name": "name",
    "time_unit": "timeUnit",
    "timezone": "timezone",
}


class CogPool(DomainModel):
    space: str = "market"
    max_price: Optional[float] = Field(None, alias="maxPrice")
    min_price: Optional[float] = Field(None, alias="minPrice")
    name: Optional[str] = None
    time_unit: Optional[str] = Field(None, alias="timeUnit")
    timezone: Optional[str] = None

    def as_apply(self) -> CogPoolApply:
        return CogPoolApply(
            space=self.space,
            external_id=self.external_id,
            max_price=self.max_price,
            min_price=self.min_price,
            name=self.name,
            time_unit=self.time_unit,
            timezone=self.timezone,
        )


class CogPoolApply(DomainModelApply):
    space: str = "market"
    max_price: Optional[float] = Field(None, alias="maxPrice")
    min_price: Optional[float] = Field(None, alias="minPrice")
    name: Optional[str] = None
    time_unit: Optional[str] = Field(None, alias="timeUnit")
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

    def as_apply(self) -> CogPoolApplyList:
        return CogPoolApplyList([node.as_apply() for node in self.data])


class CogPoolApplyList(TypeApplyList[CogPoolApply]):
    _NODE = CogPoolApply
