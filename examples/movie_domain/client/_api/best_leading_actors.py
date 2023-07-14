from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from movie_domain.client._api._core import TypeAPI
from movie_domain.client.data_classes import BestLeadingActor, BestLeadingActorApply, BestLeadingActorList


class BestLeadingActorsAPI(TypeAPI[BestLeadingActor, BestLeadingActorApply, BestLeadingActorList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("IntegrationTestsImmutable", "BestLeadingActor", "2"),
            class_type=BestLeadingActor,
            class_apply_type=BestLeadingActorApply,
            class_list=BestLeadingActorList,
        )

    def apply(self, best_leading_actor: BestLeadingActorApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = best_leading_actor.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(BestLeadingActorApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(BestLeadingActorApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> BestLeadingActor:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> BestLeadingActorList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> BestLeadingActor | BestLeadingActorList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> BestLeadingActorList:
        return self._list(limit=limit)
