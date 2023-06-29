from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm

from ._core import CircularModelApply, DomainModel, InstancesApply, TypeList

__all__ = ["Command_Config", "Command_ConfigApply", "Command_ConfigList"]


class Command_Config(DomainModel):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    configs: list[str] = None
    source: Optional[str] = None


class Command_ConfigApply(CircularModelApply):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    configs: list[str]
    source: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])
        node_data = dm.NodeOrEdgeData(
            source=dm.ContainerId("IntegrationTestsImmutable", "Command_Config"),
            properties={
                "configs": self.configs,
                "source": self.source,
            },
        )

        this_node = dm.NodeApply(
            space=self.space,
            external_id=self.external_id,
            existing_version=self.existing_version,
            sources=[node_data],
        )
        nodes = [this_node]
        edges = []

        return InstancesApply(nodes, edges)


class Command_ConfigList(TypeList[Command_Config]):
    _NODE = Command_Config
