from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import DEFAULT_LIMIT_READ

from markets.client._api._core import TypeAPI
from markets.client.data_classes import Market, MarketApply, MarketList


class MarketsAPI(TypeAPI[Market, MarketApply, MarketList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("market", "Market", "a5067899750188"),
            class_type=Market,
            class_apply_type=MarketApply,
            class_list=MarketList,
        )

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

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> MarketList:
        return self._list(limit=limit)
