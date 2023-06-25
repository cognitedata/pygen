from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from movie_domain.client.data_classes import Rating, RatingApply, RatingList

from ._core import TypeAPI


class RatingsAPI(TypeAPI[Rating, RatingApply, RatingList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("IntegrationTestsImmutable", "Rating", "2"),
            class_type=Rating,
            class_apply_type=RatingApply,
            class_list=RatingList,
        )

    def apply(self, rating: RatingApply, replace: bool = False) -> dm.InstancesApplyResult:
        return self._client.data_modeling.instances.apply(nodes=rating.to_node(), replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(RatingApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(nodes=[(RatingApply.space, id) for id in external_id])

    @overload
    def retrieve(self, external_id: str) -> Rating:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> RatingList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Rating | RatingList:
        if isinstance(external_id, str):
            return self._retrieve(("IntegrationTestsImmutable", external_id))
        else:
            return self._retrieve([("IntegrationTestsImmutable", ext_id) for ext_id in external_id])

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> RatingList:
        return self._list(limit=limit)
