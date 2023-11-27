from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm

from ._core import (
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
)


__all__ = ["Market", "MarketApply", "MarketList", "MarketApplyList", "MarketFields", "MarketTextFields"]


MarketTextFields = Literal["name", "timezone"]
MarketFields = Literal["name", "timezone"]

_MARKET_PROPERTIES_BY_FIELD = {
    "name": "name",
    "timezone": "timezone",
}


class Market(DomainModel):
    """This represents the reading version of market.

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
        """Convert this read version of market to the writing version."""
        return MarketApply(
            space=self.space,
            external_id=self.external_id,
            name=self.name,
            timezone=self.timezone,
        )


class MarketApply(DomainModelApply):
    """This represents the writing version of market.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the market.
        name: The name field.
        timezone: The timezone field.
        existing_version: Fail the ingestion request if the market version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "market"
    name: Optional[str] = None
    timezone: Optional[str] = None

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "market", "Market", "5b43e98565d4d5"
        )

        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.timezone is not None:
            properties["timezone"] = self.timezone

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        return resources


class MarketList(DomainModelList[Market]):
    """List of markets in the read version."""

    _INSTANCE = Market

    def as_apply(self) -> MarketApplyList:
        """Convert these read versions of market to the writing versions."""
        return MarketApplyList([node.as_apply() for node in self.data])


class MarketApplyList(DomainModelApplyList[MarketApply]):
    """List of markets in the writing version."""

    _INSTANCE = MarketApply


def _create_market_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    timezone: str | list[str] | None = None,
    timezone_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if name and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if timezone and isinstance(timezone, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("timezone"), value=timezone))
    if timezone and isinstance(timezone, list):
        filters.append(dm.filters.In(view_id.as_property_ref("timezone"), values=timezone))
    if timezone_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("timezone"), value=timezone_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
