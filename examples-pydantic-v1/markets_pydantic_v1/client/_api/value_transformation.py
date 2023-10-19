from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, TypeAPI
from markets_pydantic_v1.client.data_classes import (
    ValueTransformation,
    ValueTransformationApply,
    ValueTransformationList,
    ValueTransformationApplyList,
)


class ValueTransformationAPI(TypeAPI[ValueTransformation, ValueTransformationApply, ValueTransformationList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=ValueTransformation,
            class_apply_type=ValueTransformationApply,
            class_list=ValueTransformationList,
        )
        self._view_id = view_id

    def apply(
        self, value_transformation: ValueTransformationApply | Sequence[ValueTransformationApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(value_transformation, ValueTransformationApply):
            instances = value_transformation.to_instances_apply()
        else:
            instances = ValueTransformationApplyList(value_transformation).to_instances_apply()
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(self, external_id: str | Sequence[str], space="market") -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> ValueTransformation:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> ValueTransformationList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> ValueTransformation | ValueTransformationList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    def list(
        self,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ValueTransformationList:
        filter_ = _create_filter(
            self._view_id,
            method,
            method_prefix,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    method: str | list[str] | None = None,
    method_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if method and isinstance(method, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("method"), value=method))
    if method and isinstance(method, list):
        filters.append(dm.filters.In(view_id.as_property_ref("method"), values=method))
    if method_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("method"), value=method_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
