from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, TypeAPI
from markets_pydantic_v1.client.data_classes import PygenBid, PygenBidApply, PygenBidList


class PygenBidsAPI(TypeAPI[PygenBid, PygenBidApply, PygenBidList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("market", "PygenBid", "57f9da2a1acf7e"),
            class_type=PygenBid,
            class_apply_type=PygenBidApply,
            class_list=PygenBidList,
        )

    def apply(self, pygen_bid: PygenBidApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = pygen_bid.to_instances_apply()
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

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> PygenBidList:
        return self._list(limit=limit)
