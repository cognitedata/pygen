from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, TypeAPI
from shop_pydantic_v1.client.data_classes import CommandConfig, CommandConfigApply, CommandConfigList


class CommandConfigAPI(TypeAPI[CommandConfig, CommandConfigApply, CommandConfigList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=CommandConfig,
            class_apply_type=CommandConfigApply,
            class_list=CommandConfigList,
        )
        self.view_id = view_id

    def apply(self, command_config: CommandConfigApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = command_config.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(CommandConfigApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(CommandConfigApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> CommandConfig:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> CommandConfigList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> CommandConfig | CommandConfigList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(
        self,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> CommandConfigList:
        filters = []
        if external_id_prefix:
            filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
        if filter:
            filters.append(filter)

        return self._list(limit=limit, filter=dm.filters.And(*filters) if filters else None)
