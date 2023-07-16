from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from markets.client._api._core import TypeAPI
from markets.client.data_classes import CogBid, CogBidApply, CogBidList


class CogBidsAPI(TypeAPI[CogBid, CogBidApply, CogBidList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("market", "CogBid", "3c04fa081c45d5"),
            class_type=CogBid,
            class_apply_type=CogBidApply,
            class_list=CogBidList,
        )

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

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> CogBidList:
        return self._list(limit=limit)
