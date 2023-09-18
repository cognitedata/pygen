from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, TypeAPI
from markets_pydantic_v1.client.data_classes import CogProcess, CogProcessApply, CogProcessList


class CogProcessAPI(TypeAPI[CogProcess, CogProcessApply, CogProcessList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=CogProcess,
            class_apply_type=CogProcessApply,
            class_list=CogProcessList,
        )

    def apply(self, cog_proces: CogProcessApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = cog_proces.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(CogProcessApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(CogProcessApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> CogProcess:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> CogProcessList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> CogProcess | CogProcessList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> CogProcessList:
        return self._list(limit=limit)
