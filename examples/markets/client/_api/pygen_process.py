from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import DEFAULT_LIMIT_READ

from markets.client._api._core import TypeAPI
from markets.client.data_classes import PygenProces, PygenProcesApply, PygenProcesList


class PygenProcessAPI(TypeAPI[PygenProces, PygenProcesApply, PygenProcesList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("market", "PygenProcess", "477b68a858c7a8"),
            class_type=PygenProces,
            class_apply_type=PygenProcesApply,
            class_list=PygenProcesList,
        )

    def apply(self, pygen_proces: PygenProcesApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = pygen_proces.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(PygenProcesApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(PygenProcesApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> PygenProces:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> PygenProcesList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> PygenProces | PygenProcesList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> PygenProcesList:
        return self._list(limit=limit)
