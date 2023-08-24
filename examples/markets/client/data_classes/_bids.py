from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from markets.client.data_classes._core import DomainModel, DomainModelApply, TypeList

if TYPE_CHECKING:
    from markets.client.data_classes._markets import MarketApply

__all__ = ["Bid", "BidApply", "BidList"]


class Bid(DomainModel):
    space: ClassVar[str] = "market"
    date: Optional[datetime.date] = None
    market: Optional[str] = None
    name: Optional[str] = None


class BidApply(DomainModelApply):
    space: ClassVar[str] = "market"
    date: Optional[datetime.date] = None
    market: Optional[Union["MarketApply", str]] = Field(None, repr=False)
    name: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.date is not None:
            properties["date"] = self.date.isoformat()
        if self.market is not None:
            properties["market"] = {
                "space": "market",
                "externalId": self.market if isinstance(self.market, str) else self.market.external_id,
            }
        if self.name is not None:
            properties["name"] = self.name
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("market", "Bid"),
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

        if isinstance(self.market, DomainModelApply):
            instances = self.market._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class BidList(TypeList[Bid]):
    _NODE = Bid
