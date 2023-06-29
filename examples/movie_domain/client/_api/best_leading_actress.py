from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from ..data_classes import BestLeadingActres, BestLeadingActresApply, BestLeadingActresList
from ._core import TypeAPI


class BestLeadingActressAPI(TypeAPI[BestLeadingActres, BestLeadingActresApply, BestLeadingActresList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("IntegrationTestsImmutable", "BestLeadingActress", "2"),
            class_type=BestLeadingActres,
            class_apply_type=BestLeadingActresApply,
            class_list=BestLeadingActresList,
        )

    def apply(self, best_leading_actress: BestLeadingActresApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = best_leading_actress.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(BestLeadingActresApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(BestLeadingActresApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> BestLeadingActres:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> BestLeadingActresList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> BestLeadingActres | BestLeadingActresList:
        if isinstance(external_id, str):
            return self._retrieve(("IntegrationTestsImmutable", external_id))
        else:
            return self._retrieve([("IntegrationTestsImmutable", ext_id) for ext_id in external_id])

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> BestLeadingActresList:
        return self._list(limit=limit)
