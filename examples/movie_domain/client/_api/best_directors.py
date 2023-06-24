from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from movie_domain.client.data_classes.best_directors import BestDirector, BestDirectorApply, BestDirectorList

from ._core import TypeAPI


class BestDirectorsAPI(TypeAPI[BestDirector, BestDirectorApply, BestDirectorList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("IntegrationTestsImmutable", "BestDirector", "2"),
            class_type=BestDirector,
            class_apply_type=BestDirectorApply,
            class_list=BestDirectorList,
        )

    def apply(self, best_director: BestDirectorApply, replace: bool = False) -> dm.InstancesApplyResult:
        return self._client.data_modeling.instances.apply(nodes=best_director.to_node(), replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(BestDirectorApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(BestDirectorApply.space, id) for id in external_id]
            )

    @overload
    def retrieve(self, external_id: str) -> BestDirector:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> BestDirectorList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> BestDirector | BestDirectorList:
        if isinstance(external_id, str):
            best_director = self._retrieve(("IntegrationTestsImmutable", external_id))

            return best_director
        else:
            best_directors = self._retrieve([("IntegrationTestsImmutable", ext_id) for ext_id in external_id])

            return best_directors

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> BestDirectorList:
        best_directors = self._list(limit=limit)

        return best_directors
