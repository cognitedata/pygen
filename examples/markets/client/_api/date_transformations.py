from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import DEFAULT_LIMIT_READ

from markets.client._api._core import TypeAPI
from markets.client.data_classes import DateTransformation, DateTransformationApply, DateTransformationList


class DateTransformationsAPI(TypeAPI[DateTransformation, DateTransformationApply, DateTransformationList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("market", "DateTransformation", "482866112eb911"),
            class_type=DateTransformation,
            class_apply_type=DateTransformationApply,
            class_list=DateTransformationList,
        )

    def apply(self, date_transformation: DateTransformationApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = date_transformation.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(DateTransformationApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(DateTransformationApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> DateTransformation:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> DateTransformationList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> DateTransformation | DateTransformationList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> DateTransformationList:
        return self._list(limit=limit)
