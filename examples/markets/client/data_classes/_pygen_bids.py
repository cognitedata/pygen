from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from markets.client.data_classes._core import DomainModel, DomainModelApply, InstancesApply, TypeList

if TYPE_CHECKING:
    from markets.client.data_classes._markets import MarketApply

__all__ = ["PygenBid", "PygenBidApply", "PygenBidList"]


class PygenBid(DomainModel):
    space: ClassVar[str] = "market"
    date: Optional[date] = None
    is_block: Optional[bool] = Field(None, alias="isBlock")
    market: Optional[str] = None
    minimum_price: Optional[float] = Field(None, alias="minimumPrice")
    name: Optional[str] = None
    price_premium: Optional[float] = Field(None, alias="pricePremium")


class PygenBidApply(DomainModelApply):
    space: ClassVar[str] = "market"
    date: Optional[date] = None
    is_block: Optional[bool] = None
    market: Optional[Union["MarketApply", str]] = Field(None, repr=False)
    minimum_price: Optional[float] = None
    name: Optional[str] = None
    price_premium: Optional[float] = None

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("market", "Bid"),
            properties={
                "date": self.date,
                "market": {
                    "space": "market",
                    "externalId": self.market if isinstance(self.market, str) else self.market.external_id,
                },
                "name": self.name,
            },
        )
        sources.append(source)

        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("market", "PygenBid"),
            properties={
                "isBlock": self.is_block,
                "minimumPrice": self.minimum_price,
                "pricePremium": self.price_premium,
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

        if isinstance(self.market, DomainModelApply):
            instances = self.market._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return InstancesApply(nodes, edges)


class PygenBidList(TypeList[PygenBid]):
    _NODE = PygenBid
