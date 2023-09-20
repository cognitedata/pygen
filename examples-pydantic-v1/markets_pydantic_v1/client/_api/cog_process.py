from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, TypeAPI
from markets_pydantic_v1.client.data_classes import CogProcess, CogProcessApply, CogProcessList


class CogProcessAPI(TypeAPI[CogProcess, CogProcessApply, CogProcessList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=CogProcess,
            class_apply_type=CogProcessApply,
            class_list=CogProcessList,
        )
        self.view_id = view_id

    def apply(self, cog_proces: CogProcessApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = cog_proces.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(CogProcessApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(CogProcessApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> CogProcess:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> CogProcessList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> CogProcess | CogProcessList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> CogProcessList:
        filters = []
        if name and isinstance(name, str):
            filters.append(dm.filters.Equals(self.view_id.as_property_ref("name"), value=name))
        if name and isinstance(name, list):
            filters.append(dm.filters.In(self.view_id.as_property_ref("name"), values=name))
        if name_prefix:
            filters.append(dm.filters.Prefix(self.view_id.as_property_ref("name"), value=name_prefix))
        if external_id_prefix:
            filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
        if filter:
            filters.append(filter)

        return self._list(limit=limit, filter=dm.filters.And(*filters) if filters else None)
