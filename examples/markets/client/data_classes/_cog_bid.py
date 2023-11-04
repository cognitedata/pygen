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
    """This represent a read version of cog bid.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the cog bid.
        date: The date field.
        market: The market field.
        name: The name field.
        price: The price field.
        price_area: The price area field.
        quantity: The quantity field.
        created_time: The created time of the cog bid node.
        last_updated_time: The last updated time of the cog bid node.
        deleted_time: If present, the deleted time of the cog bid node.
        version: The version of the cog bid node.
    """

    space: str = "market"
    date: Optional[datetime.date] = None
    market: Optional[str] = None
    name: Optional[str] = None
    price: Optional[float] = None
    price_area: Optional[str] = Field(None, alias="priceArea")
    quantity: Optional[int] = None

    def as_apply(self) -> CogBidApply:
        """Convert this read version of cog bid to a write version."""
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
    """This represent a write version of cog bid.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the cog bid.
        date: The date field.
        market: The market field.
        name: The name field.
        price: The price field.
        price_area: The price area field.
        quantity: The quantity field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "market"
    date: Optional[datetime.date] = None
    market: Union[MarketApply, str, None] = Field(None, repr=False)
    name: Optional[str] = None
    price: Optional[float] = None
    price_area: Optional[str] = Field(None, alias="priceArea")
    quantity: Optional[int] = None

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

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
            instances = self.market._to_instances_apply(cache, view_by_write_class)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class CogBidList(TypeList[CogBid]):
    """List of cog bids in read version."""

    _NODE = CogBid

    def as_apply(self) -> CogBidApplyList:
        """Convert this read version of cog bid to a write version."""
        return CogBidApplyList([node.as_apply() for node in self.data])


class CogBidApplyList(TypeApplyList[CogBidApply]):
    """List of cog bids in write version."""

    _NODE = CogBidApply
