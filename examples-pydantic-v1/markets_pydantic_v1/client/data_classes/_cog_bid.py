from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
)

if TYPE_CHECKING:
    from ._market import Market, MarketApply


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
    """This represents the reading version of cog bid.

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

    space: str = DEFAULT_INSTANCE_SPACE
    date: Optional[datetime.date] = None
    market: Union[Market, str, dm.NodeId, None] = Field(None, repr=False)
    name: Optional[str] = None
    price: Optional[float] = None
    price_area: Optional[str] = Field(None, alias="priceArea")
    quantity: Optional[int] = None

    def as_apply(self) -> CogBidApply:
        """Convert this read version of cog bid to the writing version."""
        return CogBidApply(
            space=self.space,
            external_id=self.external_id,
            date=self.date,
            market=self.market.as_apply() if isinstance(self.market, DomainModel) else self.market,
            name=self.name,
            price=self.price,
            price_area=self.price_area,
            quantity=self.quantity,
        )


class CogBidApply(DomainModelApply):
    """This represents the writing version of cog bid.

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
        existing_version: Fail the ingestion request if the cog bid version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    date: Optional[datetime.date] = None
    market: Union[MarketApply, str, dm.NodeId, None] = Field(None, repr=False)
    name: Optional[str] = None
    price: Optional[float] = None
    price_area: Optional[str] = Field(None, alias="priceArea")
    quantity: Optional[int] = None

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "market", "CogBid", "3c04fa081c45d5"
        )

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
        if self.price is not None:
            properties["price"] = self.price
        if self.price_area is not None:
            properties["priceArea"] = self.price_area
        if self.quantity is not None:
            properties["quantity"] = self.quantity

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

        if isinstance(self.market, DomainModelApply):
            other_resources = self.market._to_instances_apply(cache, view_by_write_class)
            resources.extend(other_resources)

        return resources


class CogBidList(DomainModelList[CogBid]):
    """List of cog bids in the read version."""

    _INSTANCE = CogBid

    def as_apply(self) -> CogBidApplyList:
        """Convert these read versions of cog bid to the writing versions."""
        return CogBidApplyList([node.as_apply() for node in self.data])


class CogBidApplyList(DomainModelApplyList[CogBidApply]):
    """List of cog bids in the writing version."""

    _INSTANCE = CogBidApply


def _create_cog_bid_filter(
    view_id: dm.ViewId,
    min_date: datetime.date | None = None,
    max_date: datetime.date | None = None,
    market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    min_price: float | None = None,
    max_price: float | None = None,
    price_area: str | list[str] | None = None,
    price_area_prefix: str | None = None,
    min_quantity: int | None = None,
    max_quantity: int | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if min_date or max_date:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("date"),
                gte=min_date.isoformat() if min_date else None,
                lte=max_date.isoformat() if max_date else None,
            )
        )
    if market and isinstance(market, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("market"), value={"space": "market", "externalId": market})
        )
    if market and isinstance(market, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("market"), value={"space": market[0], "externalId": market[1]})
        )
    if market and isinstance(market, list) and isinstance(market[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("market"), values=[{"space": "market", "externalId": item} for item in market]
            )
        )
    if market and isinstance(market, list) and isinstance(market[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("market"), values=[{"space": item[0], "externalId": item[1]} for item in market]
            )
        )
    if name is not None and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if min_price or max_price:
        filters.append(dm.filters.Range(view_id.as_property_ref("price"), gte=min_price, lte=max_price))
    if price_area is not None and isinstance(price_area, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("priceArea"), value=price_area))
    if price_area and isinstance(price_area, list):
        filters.append(dm.filters.In(view_id.as_property_ref("priceArea"), values=price_area))
    if price_area_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("priceArea"), value=price_area_prefix))
    if min_quantity or max_quantity:
        filters.append(dm.filters.Range(view_id.as_property_ref("quantity"), gte=min_quantity, lte=max_quantity))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
