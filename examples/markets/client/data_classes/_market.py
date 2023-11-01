from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = ["Market", "MarketApply", "MarketList", "MarketApplyList", "MarketFields", "MarketTextFields"]


MarketTextFields = Literal["name", "timezone"]
MarketFields = Literal["name", "timezone"]

_MARKET_PROPERTIES_BY_FIELD = {
    "name": "name",
    "timezone": "timezone",
}


class Market(DomainModel):
    space: str = "market"
    name: Optional[str] = None
    timezone: Optional[str] = None

    def as_apply(self) -> MarketApply:
        return MarketApply(
            space=self.space,
            external_id=self.external_id,
            name=self.name,
            timezone=self.timezone,
        )


class MarketApply(DomainModelApply):
    space: str = "market"
    name: Optional[str] = None
    timezone: Optional[str] = None

    def _to_instances_apply(self, cache: set[str], write_view: dm.ViewId | None) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.timezone is not None:
            properties["timezone"] = self.timezone
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("market", "Market", "a5067899750188"),
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


class MarketList(TypeList[Market]):
    _NODE = Market

    def as_apply(self) -> MarketApplyList:
        return MarketApplyList([node.as_apply() for node in self.data])


class MarketApplyList(TypeApplyList[MarketApply]):
    _NODE = MarketApply
