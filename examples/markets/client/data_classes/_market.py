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
    """This represent a read version of market.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the market.
        name: The name field.
        timezone: The timezone field.
        created_time: The created time of the market node.
        last_updated_time: The last updated time of the market node.
        deleted_time: If present, the deleted time of the market node.
        version: The version of the market node.
    """

    space: str = "market"
    name: Optional[str] = None
    timezone: Optional[str] = None

    def as_apply(self) -> MarketApply:
        """Convert this read version of market to a write version."""
        return MarketApply(
            space=self.space,
            external_id=self.external_id,
            name=self.name,
            timezone=self.timezone,
        )


class MarketApply(DomainModelApply):
    """This represent a write version of market.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the market.
        name: The name field.
        timezone: The timezone field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "market"
    name: Optional[str] = None
    timezone: Optional[str] = None

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

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
    """List of markets in read version."""

    _NODE = Market

    def as_apply(self) -> MarketApplyList:
        """Convert this read version of market to a write version."""
        return MarketApplyList([node.as_apply() for node in self.data])


class MarketApplyList(TypeApplyList[MarketApply]):
    """List of markets in write version."""

    _NODE = MarketApply
