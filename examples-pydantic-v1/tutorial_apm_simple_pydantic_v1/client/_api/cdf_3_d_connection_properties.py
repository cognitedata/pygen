from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, TypeAPI
from tutorial_apm_simple_pydantic_v1.client.data_classes import (
    CdfConnectionProperties,
    CdfConnectionPropertiesApply,
    CdfConnectionPropertiesList,
)


class CdfConnectionPropertiesAPI(
    TypeAPI[CdfConnectionProperties, CdfConnectionPropertiesApply, CdfConnectionPropertiesList]
):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=CdfConnectionProperties,
            class_apply_type=CdfConnectionPropertiesApply,
            class_list=CdfConnectionPropertiesList,
        )

    def apply(
        self, cdf_3_d_connection_property: CdfConnectionPropertiesApply, replace: bool = False
    ) -> dm.InstancesApplyResult:
        instances = cdf_3_d_connection_property.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(CdfConnectionPropertiesApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(CdfConnectionPropertiesApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> CdfConnectionProperties:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> CdfConnectionPropertiesList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> CdfConnectionProperties | CdfConnectionPropertiesList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> CdfConnectionPropertiesList:
        return self._list(limit=limit)
