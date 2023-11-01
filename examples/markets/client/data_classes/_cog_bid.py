from __future__ import annotations

import datetime
from typing import Literal, TYPE_CHECKING, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

if TYPE_CHECKING:
    from ._market import MarketApply

__all__ = ["CogBid", "CogBidApply", "CogBidList", "CogBidApplyList", "CogBidFields", "CogBidTextFields"]


CogBidTextFields = Literal["name", "price_area"]
CogBidFields = Literal["date", "name", "price", "price_area", "quantity"]

_COGBID_PROPERTIES_BY_FIELD = {
    "date": "date",
    "name": "name",
    "price": "price",
    "price_area": "priceArea",
    "quantity": "quantity",
}


class CogBid(DomainModel):
    space: str = "market"
    date: Optional[datetime.date] = None
    market: Optional[str] = None
    name: Optional[str] = None
    price: Optional[float] = None
    price_area: Optional[str] = Field(None, alias="priceArea")
    quantity: Optional[int] = None

    def as_apply(self) -> CogBidApply:
        return CogBidApply(
            space=self.space,
            external_id=self.external_id,
            date=self.date,
            market=self.market,
            name=self.name,
            price=self.price,
            price_area=self.price_area,
            quantity=self.quantity,
        )


class CogBidApply(DomainModelApply):
    space: str = "market"
    date: Optional[datetime.date] = None
    market: Union[MarketApply, str, None] = Field(None, repr=False)
    name: Optional[str] = None
    price: Optional[float] = None
    price_area: Optional[str] = Field(None, alias="priceArea")
    quantity: Optional[int] = None

    def _to_instances_apply(self, cache: set[str], write_view: dm.ViewId | None) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        properties = {}
        if self.date is not None:
            properties["date"] = self.date.isoformat(timespec="milliseconds")
        if self.market is not None:
            properties["market"] = {
                "space": "market",
                "externalId": self.market if isinstance(self.market, str) else self.market.external_id,
            }
        if self.name is not None:
            properties["name"] = self.name
        if self.price is not None:
            properties["price"] = self.price
        if self.price_area is not None:
            properties["priceArea"] = self.price_area
        if self.quantity is not None:
            properties["quantity"] = self.quantity
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("market", "CogBid", "3c04fa081c45d5"),
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

        if isinstance(self.market, DomainModelApply):
            instances = self.market._to_instances_apply(cache, write_view)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class CogBidList(TypeList[CogBid]):
    _NODE = CogBid

    def as_apply(self) -> CogBidApplyList:
        return CogBidApplyList([node.as_apply() for node in self.data])


class CogBidApplyList(TypeApplyList[CogBidApply]):
    _NODE = CogBidApply
