from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from markets.client.data_classes._core import DomainModel, DomainModelApply, InstancesApply, TypeList

if TYPE_CHECKING:
    from markets.client.data_classes._markets import MarketApply

__all__ = ["CogBid", "CogBidApply", "CogBidList"]


class CogBid(DomainModel):
    space: ClassVar[str] = "market"
    date: Optional[date] = None
    market: Optional[str] = None
    name: Optional[str] = None
    price: Optional[float] = None
    price_area: Optional[str] = Field(None, alias="priceArea")
    quantity: Optional[int] = None


class CogBidApply(DomainModelApply):
    space: ClassVar[str] = "market"
    date: Optional[date] = None
    market: Optional[Union["MarketApply", str]] = Field(None, repr=False)
    name: Optional[str] = None
    price: Optional[float] = None
    price_area: Optional[str] = None
    quantity: Optional[int] = None

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
            source=dm.ContainerId("market", "CogBid"),
            properties={
                "price": self.price,
                "priceArea": self.price_area,
                "quantity": self.quantity,
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


class CogBidList(TypeList[CogBid]):
    _NODE = CogBid
