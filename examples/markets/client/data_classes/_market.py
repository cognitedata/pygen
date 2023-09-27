from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = ["Market", "MarketApply", "MarketList", "MarketApplyList"]


class Market(DomainModel):
    space: ClassVar[str] = "market"
    name: str = None
    timezone: str = None

    def as_apply(self) -> MarketApply:
        return MarketApply(
            external_id=self.external_id,
            name=self.name,
            timezone=self.timezone,
        )


class MarketApply(DomainModelApply):
    space: ClassVar[str] = "market"
    name: Optional[str] = None
    timezone: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
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


class MarketList(TypeList[Market]):
    _NODE = Market

    def as_apply(self) -> MarketApplyList:
        return MarketApplyList([node.as_apply() for node in self.data])


class MarketApplyList(TypeApplyList[MarketApply]):
    _NODE = MarketApply
