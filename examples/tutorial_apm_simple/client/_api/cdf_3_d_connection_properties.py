from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, TypeAPI
from tutorial_apm_simple.client.data_classes import (
    CdfConnectionProperty,
    CdfConnectionPropertyApply,
    CdfConnectionPropertyList,
)


class CdfConnectionPropertiesAPI(TypeAPI[CdfConnectionProperty, CdfConnectionPropertyApply, CdfConnectionPropertyList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("cdf_3d_schema", "Cdf3dConnectionProperties", "1"),
            class_type=CdfConnectionProperty,
            class_apply_type=CdfConnectionPropertyApply,
            class_list=CdfConnectionPropertyList,
        )

    def apply(
        self, cdf_3_d_connection_property: CdfConnectionPropertyApply, replace: bool = False
    ) -> dm.InstancesApplyResult:
        instances = cdf_3_d_connection_property.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(CdfConnectionPropertyApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(CdfConnectionPropertyApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> CdfConnectionProperty:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> CdfConnectionPropertyList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> CdfConnectionProperty | CdfConnectionPropertyList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> CdfConnectionPropertyList:
        return self._list(limit=limit)
