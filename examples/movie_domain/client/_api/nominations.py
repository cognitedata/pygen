from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from movie_domain.client.data_classes.nominations import Nomination, NominationApply, NominationList

from ._core import TypeAPI


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
        return self._client.data_modeling.instances.apply(nodes=nomination.to_node(), replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(NominationApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(NominationApply.space, id) for id in external_id]
            )

    @overload
    def retrieve(self, external_id: str) -> Nomination:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> NominationList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Nomination | NominationList:
        if isinstance(external_id, str):
            return self._retrieve(("IntegrationTestsImmutable", external_id))
        else:
            return self._retrieve([("IntegrationTestsImmutable", ext_id) for ext_id in external_id])

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> NominationList:
        return self._list(limit=limit)
