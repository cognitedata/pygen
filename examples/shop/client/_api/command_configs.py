from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from shop.client.data_classes import Command_Config, Command_ConfigApply, Command_ConfigList

from ._core import TypeAPI


class CommandConfigsAPI(TypeAPI[Command_Config, Command_ConfigApply, Command_ConfigList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("IntegrationTestsImmutable", "Command_Config", "4727b5ad34b608"),
            class_type=Command_Config,
            class_apply_type=Command_ConfigApply,
            class_list=Command_ConfigList,
        )

    def apply(self, command_config: Command_ConfigApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = command_config.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(Command_ConfigApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(Command_ConfigApply.space, id) for id in external_id]
            )

    @overload
    def retrieve(self, external_id: str) -> Command_Config:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> Command_ConfigList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Command_Config | Command_ConfigList:
        if isinstance(external_id, str):
            return self._retrieve(("IntegrationTestsImmutable", external_id))
        else:
            return self._retrieve([("IntegrationTestsImmutable", ext_id) for ext_id in external_id])

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> Command_ConfigList:
        return self._list(limit=limit)
