from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, TypeAPI
from tutorial_apm_simple_pydantic_v1.client.data_classes import (
    CdfConnectionProperties,
    CdfConnectionPropertiesApply,
    CdfConnectionPropertiesList,
    CdfConnectionPropertiesApplyList,
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
        self._view_id = view_id

    def apply(
        self,
        cdf_3_d_connection_property: CdfConnectionPropertiesApply | Sequence[CdfConnectionPropertiesApply],
        replace: bool = False,
    ) -> dm.InstancesApplyResult:
        if isinstance(cdf_3_d_connection_property, CdfConnectionPropertiesApply):
            instances = cdf_3_d_connection_property.to_instances_apply()
        else:
            instances = CdfConnectionPropertiesApplyList(cdf_3_d_connection_property).to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str], space="cdf_3d_schema") -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
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

    def list(
        self,
        min_revision_id: int | None = None,
        max_revision_id: int | None = None,
        min_revision_node_id: int | None = None,
        max_revision_node_id: int | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> CdfConnectionPropertiesList:
        filter_ = _create_filter(
            self._view_id,
            min_revision_id,
            max_revision_id,
            min_revision_node_id,
            max_revision_node_id,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    min_revision_id: int | None = None,
    max_revision_id: int | None = None,
    min_revision_node_id: int | None = None,
    max_revision_node_id: int | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if min_revision_id or max_revision_id:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("revisionId"), gte=min_revision_id, lte=max_revision_id)
        )
    if min_revision_node_id or max_revision_node_id:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("revisionNodeId"), gte=min_revision_node_id, lte=max_revision_node_id
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
