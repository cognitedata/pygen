from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from markets_pydantic_v1.client._api._core import TypeAPI
from markets_pydantic_v1.client.data_classes import CogProces, CogProcesApply, CogProcesList


class CogProcessAPI(TypeAPI[CogProces, CogProcesApply, CogProcesList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("market", "CogProcess", "b5df5d19e08fd0"),
            class_type=CogProces,
            class_apply_type=CogProcesApply,
            class_list=CogProcesList,
        )

    def apply(self, cog_proces: CogProcesApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = cog_proces.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(CogProcesApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(CogProcesApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> CogProces:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> CogProcesList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> CogProces | CogProcesList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> CogProcesList:
        return self._list(limit=limit)
