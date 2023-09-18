from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, TypeAPI
from markets_pydantic_v1.client.data_classes import Bid, BidApply, BidList


class BidAPI(TypeAPI[Bid, BidApply, BidList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Bid,
            class_apply_type=BidApply,
            class_list=BidList,
        )

    def apply(self, bid: BidApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = bid.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(BidApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(BidApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Bid:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> BidList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Bid | BidList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> BidList:
        return self._list(limit=limit)
