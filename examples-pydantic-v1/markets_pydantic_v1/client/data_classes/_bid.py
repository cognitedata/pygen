from __future__ import annotations

import datetime
from typing import Literal, TYPE_CHECKING, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

if TYPE_CHECKING:
    from ._market import MarketApply

__all__ = ["Bid", "BidApply", "BidList", "BidApplyList", "BidFields", "BidTextFields"]


BidTextFields = Literal["name"]
BidFields = Literal["date", "name"]

_BID_PROPERTIES_BY_FIELD = {
    "date": "date",
    "name": "name",
}


class Bid(DomainModel):
    """This represent a read version of bid.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid.
        date: The date field.
        market: The market field.
        name: The name field.
        created_time: The created time of the bid node.
        last_updated_time: The last updated time of the bid node.
        deleted_time: If present, the deleted time of the bid node.
        version: The version of the bid node.
    """

    space: str = "market"
    date: Optional[datetime.date] = None
    market: Optional[str] = None
    name: Optional[str] = None

    def as_apply(self) -> BidApply:
        """Convert this read version of bid to a write version."""
        return BidApply(
            space=self.space,
            external_id=self.external_id,
            date=self.date,
            market=self.market,
            name=self.name,
        )


class BidApply(DomainModelApply):
    """This represent a write version of bid.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid.
        date: The date field.
        market: The market field.
        name: The name field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "market"
    date: Optional[datetime.date] = None
    market: Union[MarketApply, str, None] = Field(None, repr=False)
    name: Optional[str] = None

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
                "space": self.space if isinstance(self.market, str) else self.market.space,
                "externalId": self.market if isinstance(self.market, str) else self.market.external_id,
            }
        if self.name is not None:
            properties["name"] = self.name
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("market", "Bid", "1add47c48cf88b"),
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


class BidList(TypeList[Bid]):
    """List of bids in read version."""

    _NODE = Bid

    def as_apply(self) -> BidApplyList:
        """Convert this read version of bid to a write version."""
        return BidApplyList([node.as_apply() for node in self.data])


class BidApplyList(TypeApplyList[BidApply]):
    """List of bids in write version."""

    _NODE = BidApply
