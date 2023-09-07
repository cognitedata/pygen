from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import DEFAULT_LIMIT_READ

from movie_domain_pydantic_v1.client._api._core import TypeAPI
from movie_domain_pydantic_v1.client.data_classes import Nomination, NominationApply, NominationList


class NominationsAPI(TypeAPI[Nomination, NominationApply, NominationList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("IntegrationTestsImmutable", "Nomination", "2"),
            class_type=Nomination,
            class_apply_type=NominationApply,
            class_list=NominationList,
        )

    def apply(self, nomination: NominationApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = nomination.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(NominationApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(NominationApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Nomination:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> NominationList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Nomination | NominationList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> NominationList:
        return self._list(limit=limit)
