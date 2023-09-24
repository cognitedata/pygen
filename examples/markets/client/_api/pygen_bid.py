from __future__ import annotations

import datetime
from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, TypeAPI
from markets.client.data_classes import PygenBid, PygenBidApply, PygenBidList, PygenBidApplyList


class PygenBidAPI(TypeAPI[PygenBid, PygenBidApply, PygenBidList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=PygenBid,
            class_apply_type=PygenBidApply,
            class_list=PygenBidList,
        )
        self.view_id = view_id

    def apply(
        self, pygen_bid: PygenBidApply | Sequence[PygenBidApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(pygen_bid, PygenBidApply):
            instances = pygen_bid.to_instances_apply()
        else:
            instances = PygenBidApplyList(pygen_bid).to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(PygenBidApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(PygenBidApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> PygenBid:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> PygenBidList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> PygenBid | PygenBidList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(
        self,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        is_block: bool | None = None,
        min_minimum_price: float | None = None,
        max_minimum_price: float | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_price_premium: float | None = None,
        max_price_premium: float | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> PygenBidList:
        filter_ = _create_filter(
            self.view_id,
            min_date,
            max_date,
            is_block,
            min_minimum_price,
            max_minimum_price,
            name,
            name_prefix,
            min_price_premium,
            max_price_premium,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    min_date: datetime.date | None = None,
    max_date: datetime.date | None = None,
    is_block: bool | None = None,
    min_minimum_price: float | None = None,
    max_minimum_price: float | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    min_price_premium: float | None = None,
    max_price_premium: float | None = None,
    external_id_prefix: str | None = None,
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
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
