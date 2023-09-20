from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, TypeAPI
from markets.client.data_classes import Market, MarketApply, MarketList


class MarketAPI(TypeAPI[Market, MarketApply, MarketList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Market,
            class_apply_type=MarketApply,
            class_list=MarketList,
        )
        self.view_id = view_id

    def apply(self, market: MarketApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = market.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(MarketApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(MarketApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Market:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> MarketList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Market | MarketList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> MarketList:
        filters = []
        if name and isinstance(name, str):
            filters.append(dm.filters.Equals(self.view_id.as_property_ref("name"), value=name))
        if name and isinstance(name, list):
            filters.append(dm.filters.In(self.view_id.as_property_ref("name"), values=name))
        if name_prefix:
            filters.append(dm.filters.Prefix(self.view_id.as_property_ref("name"), value=name_prefix))
        if timezone and isinstance(timezone, str):
            filters.append(dm.filters.Equals(self.view_id.as_property_ref("timezone"), value=timezone))
        if timezone and isinstance(timezone, list):
            filters.append(dm.filters.In(self.view_id.as_property_ref("timezone"), values=timezone))
        if timezone_prefix:
            filters.append(dm.filters.Prefix(self.view_id.as_property_ref("timezone"), value=timezone_prefix))
        if external_id_prefix:
            filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
        if filter:
            filters.append(filter)

        return self._list(limit=limit, filter=dm.filters.And(*filters) if filters else None)
