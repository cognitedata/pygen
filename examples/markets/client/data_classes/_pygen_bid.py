from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
)

if TYPE_CHECKING:
    from ._market import Market, MarketApply


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
    """This represents the reading version of pygen bid.

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
    market: Union[Market, str, None] = Field(None, repr=False)
    minimum_price: Optional[float] = Field(None, alias="minimumPrice")
    name: Optional[str] = None
    price_premium: Optional[float] = Field(None, alias="pricePremium")

    def as_apply(self) -> PygenBidApply:
        """Convert this read version of pygen bid to the writing version."""
        return PygenBidApply(
            space=self.space,
            external_id=self.external_id,
            date=self.date,
            is_block=self.is_block,
            market=self.market.as_apply() if isinstance(self.market, DomainModel) else self.market,
            minimum_price=self.minimum_price,
            name=self.name,
            price_premium=self.price_premium,
        )


class PygenBidApply(DomainModelApply):
    """This represents the writing version of pygen bid.

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
        existing_version: Fail the ingestion request if the pygen bid version is greater than or equal to this value.
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
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "market", "PygenBid", "57f9da2a1acf7e"
        )

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


class PygenBidList(DomainModelList[PygenBid]):
    """List of pygen bids in the read version."""

    _INSTANCE = PygenBid

    def as_apply(self) -> PygenBidApplyList:
        """Convert these read versions of pygen bid to the writing versions."""
        return PygenBidApplyList([node.as_apply() for node in self.data])


class PygenBidApplyList(DomainModelApplyList[PygenBidApply]):
    """List of pygen bids in the writing version."""

    _INSTANCE = PygenBidApply


def _create_pygen_bid_filter(
    view_id: dm.ViewId,
    min_date: datetime.date | None = None,
    max_date: datetime.date | None = None,
    is_block: bool | None = None,
    market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    min_minimum_price: float | None = None,
    max_minimum_price: float | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    min_price_premium: float | None = None,
    max_price_premium: float | None = None,
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
    if is_block and isinstance(is_block, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("isBlock"), value=is_block))
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
    if min_minimum_price or max_minimum_price:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("minimumPrice"), gte=min_minimum_price, lte=max_minimum_price)
        )
    if name and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if min_price_premium or max_price_premium:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("pricePremium"), gte=min_price_premium, lte=max_price_premium)
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
