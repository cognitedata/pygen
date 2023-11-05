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
    "minimum_price": "minimumPrice",
    "name": "name",
    "price_premium": "pricePremium",
}


class PygenBid(DomainModel):
    """This represent a read version of pygen bid.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the pygen bid.
        date: The date field.
        is_block: The is block field.
        market: The market field.
        minimum_price: The minimum price field.
        name: The name field.
        price_premium: The price premium field.
        created_time: The created time of the pygen bid node.
        last_updated_time: The last updated time of the pygen bid node.
        deleted_time: If present, the deleted time of the pygen bid node.
        version: The version of the pygen bid node.
    """

    space: str = "market"
    date: Optional[datetime.date] = None
    is_block: Optional[bool] = Field(None, alias="isBlock")
    market: Optional[str] = None
    minimum_price: Optional[float] = Field(None, alias="minimumPrice")
    name: Optional[str] = None
    price_premium: Optional[float] = Field(None, alias="pricePremium")

    def as_apply(self) -> PygenBidApply:
        """Convert this read version of pygen bid to a write version."""
        return PygenBidApply(
            space=self.space,
            external_id=self.external_id,
            date=self.date,
            is_block=self.is_block,
            market=self.market,
            minimum_price=self.minimum_price,
            name=self.name,
            price_premium=self.price_premium,
        )


class PygenBidApply(DomainModelApply):
    """This represent a write version of pygen bid.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the pygen bid.
        date: The date field.
        is_block: The is block field.
        market: The market field.
        minimum_price: The minimum price field.
        name: The name field.
        price_premium: The price premium field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "market"
    date: Optional[datetime.date] = None
    is_block: Optional[bool] = Field(None, alias="isBlock")
    market: Union[MarketApply, str, None] = Field(None, repr=False)
    minimum_price: Optional[float] = Field(None, alias="minimumPrice")
    name: Optional[str] = None
    price_premium: Optional[float] = Field(None, alias="pricePremium")

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.date is not None:
            properties["date"] = self.date.isoformat()
        if self.is_block is not None:
            properties["isBlock"] = self.is_block
        if self.market is not None:
            properties["market"] = {
                "space": self.space if isinstance(self.market, str) else self.market.space,
                "externalId": self.market if isinstance(self.market, str) else self.market.external_id,
            }
        if self.minimum_price is not None:
            properties["minimumPrice"] = self.minimum_price
        if self.name is not None:
            properties["name"] = self.name
        if self.price_premium is not None:
            properties["pricePremium"] = self.price_premium
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("market", "PygenBid", "57f9da2a1acf7e"),
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


class PygenBidList(TypeList[PygenBid]):
    """List of pygen bids in read version."""

    _NODE = PygenBid

    def as_apply(self) -> PygenBidApplyList:
        """Convert this read version of pygen bid to a write version."""
        return PygenBidApplyList([node.as_apply() for node in self.data])


class PygenBidApplyList(TypeApplyList[PygenBidApply]):
    """List of pygen bids in write version."""

    _NODE = PygenBidApply
