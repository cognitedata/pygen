from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import DEFAULT_LIMIT_READ

from markets.client._api._core import TypeAPI
from markets.client.data_classes import PygenPool, PygenPoolApply, PygenPoolList


class PygenPoolsAPI(TypeAPI[PygenPool, PygenPoolApply, PygenPoolList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("market", "PygenPool", "23c71ba66bad9d"),
            class_type=PygenPool,
            class_apply_type=PygenPoolApply,
            class_list=PygenPoolList,
        )

    def apply(self, pygen_pool: PygenPoolApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = pygen_pool.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(PygenPoolApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(PygenPoolApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> PygenPool:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> PygenPoolList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> PygenPool | PygenPoolList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> PygenPoolList:
        return self._list(limit=limit)
