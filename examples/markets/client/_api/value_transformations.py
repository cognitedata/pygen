from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import DEFAULT_LIMIT_READ

from markets.client._api._core import TypeAPI
from markets.client.data_classes import ValueTransformation, ValueTransformationApply, ValueTransformationList


class ValueTransformationsAPI(TypeAPI[ValueTransformation, ValueTransformationApply, ValueTransformationList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("market", "ValueTransformation", "147ebcf1583165"),
            class_type=ValueTransformation,
            class_apply_type=ValueTransformationApply,
            class_list=ValueTransformationList,
        )

    def apply(self, value_transformation: ValueTransformationApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = value_transformation.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(ValueTransformationApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(ValueTransformationApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> ValueTransformation:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> ValueTransformationList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> ValueTransformation | ValueTransformationList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> ValueTransformationList:
        return self._list(limit=limit)
