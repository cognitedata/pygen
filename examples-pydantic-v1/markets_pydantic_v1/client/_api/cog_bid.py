from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, TypeAPI
from markets_pydantic_v1.client.data_classes import CogBid, CogBidApply, CogBidList


class CogBidAPI(TypeAPI[CogBid, CogBidApply, CogBidList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=CogBid,
            class_apply_type=CogBidApply,
            class_list=CogBidList,
        )
        self.view_id = view_id

    def apply(self, cog_bid: CogBidApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = cog_bid.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(CogBidApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(CogBidApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> CogBid:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> CogBidList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> CogBid | CogBidList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(
        self,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        min_quantity: int | None = None,
        max_quantity: int | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> CogBidList:
        filters = []
        if min_date or max_date:
            filters.append(dm.filters.Range(self.view_id.as_property_ref("date"), gte=min_date, lte=max_date))
        if name and isinstance(name, str):
            filters.append(dm.filters.Equals(self.view_id.as_property_ref("name"), value=name))
        if name and isinstance(name, list):
            filters.append(dm.filters.In(self.view_id.as_property_ref("name"), values=name))
        if name_prefix:
            filters.append(dm.filters.Prefix(self.view_id.as_property_ref("name"), value=name_prefix))
        if min_price or max_price:
            filters.append(dm.filters.Range(self.view_id.as_property_ref("price"), gte=min_price, lte=max_price))
        if price_area and isinstance(price_area, str):
            filters.append(dm.filters.Equals(self.view_id.as_property_ref("priceArea"), value=price_area))
        if price_area and isinstance(price_area, list):
            filters.append(dm.filters.In(self.view_id.as_property_ref("priceArea"), values=price_area))
        if price_area_prefix:
            filters.append(dm.filters.Prefix(self.view_id.as_property_ref("priceArea"), value=price_area_prefix))
        if min_quantity or max_quantity:
            filters.append(
                dm.filters.Range(self.view_id.as_property_ref("quantity"), gte=min_quantity, lte=max_quantity)
            )
        if external_id_prefix:
            filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
        if filter:
            filters.append(filter)

        return self._list(limit=limit, filter=dm.filters.And(*filters) if filters else None)
