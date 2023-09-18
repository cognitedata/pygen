from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, TypeAPI
from markets_pydantic_v1.client.data_classes import Process, ProcessApply, ProcessList


class ProcessAPI(TypeAPI[Process, ProcessApply, ProcessList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("market", "Process", "98a2becd0f63ee"),
            class_type=Process,
            class_apply_type=ProcessApply,
            class_list=ProcessList,
        )

    def apply(self, proces: ProcessApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = proces.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(ProcessApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(ProcessApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Process:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> ProcessList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Process | ProcessList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> ProcessList:
        return self._list(limit=limit)
