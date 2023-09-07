from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import DEFAULT_LIMIT_READ

from shop_pydantic_v1.client._api._core import TypeAPI
from shop_pydantic_v1.client.data_classes import Case, CaseApply, CaseList


class CasesAPI(TypeAPI[Case, CaseApply, CaseList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("IntegrationTestsImmutable", "Case", "366b75cc4e699f"),
            class_type=Case,
            class_apply_type=CaseApply,
            class_list=CaseList,
        )

    def apply(self, case: CaseApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = case.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(CaseApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(CaseApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Case:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> CaseList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Case | CaseList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> CaseList:
        return self._list(limit=limit)
