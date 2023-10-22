from __future__ import annotations

import datetime
from typing import Literal, TYPE_CHECKING, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

if TYPE_CHECKING:
    from ._market import MarketApply

__all__ = ["PygenBid", "PygenBidApply", "PygenBidList", "PygenBidApplyList", "PygenBidFields", "PygenBidTextFields"]


PygenBidTextFields = Literal["name"]
PygenBidFields = Literal["date", "is_block", "minimum_price", "name", "price_premium"]

_PYGENBID_PROPERTIES_BY_FIELD = {
    "date": "date",
    "is_block": "isBlock",
    "market": "market",
    "minimum_price": "minimumPrice",
    "name": "name",
    "price_premium": "pricePremium",
}


class PygenBid(DomainModel):
    space: str = "market"
    date: Optional[datetime.date] = None
    is_block: Optional[bool] = Field(None, alias="isBlock")
    market: Optional[str] = None
    minimum_price: Optional[float] = Field(None, alias="minimumPrice")
    name: Optional[str] = None
    price_premium: Optional[float] = Field(None, alias="pricePremium")

    def as_apply(self) -> PygenBidApply:
        return PygenBidApply(
            external_id=self.external_id,
            date=self.date,
            is_block=self.is_block,
            market=self.market,
            minimum_price=self.minimum_price,
            name=self.name,
            price_premium=self.price_premium,
        )


class PygenBidApply(DomainModelApply):
    space: str = "market"
    date: Optional[datetime.date] = None
    is_block: Optional[bool] = None
    market: Union[MarketApply, str, None] = Field(None, repr=False)
    minimum_price: Optional[float] = None
    name: Optional[str] = None
    price_premium: Optional[float] = None

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
        properties = {}
        if self.is_block is not None:
            properties["isBlock"] = self.is_block
        if self.minimum_price is not None:
            properties["minimumPrice"] = self.minimum_price
        if self.price_premium is not None:
            properties["pricePremium"] = self.price_premium
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("market", "PygenBid"),
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


class PygenBidList(TypeList[PygenBid]):
    _NODE = PygenBid

    def as_apply(self) -> PygenBidApplyList:
        return PygenBidApplyList([node.as_apply() for node in self.data])


class PygenBidApplyList(TypeApplyList[PygenBidApply]):
    _NODE = PygenBidApply
