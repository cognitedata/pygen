from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm

from shop_pydantic_v1.client.data_classes._core import DomainModel, DomainModelApply, TypeList

__all__ = ["CommandConfig", "CommandConfigApply", "CommandConfigList"]


class CommandConfig(DomainModel):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    configs: list[str] = []
    source: Optional[str] = None


class CommandConfigApply(DomainModelApply):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    configs: list[str]
    source: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.configs is not None:
            properties["configs"] = self.configs
        if self.source is not None:
            properties["source"] = self.source
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("IntegrationTestsImmutable", "Command_Config"),
                properties=properties,
            )
            sources.append(source)
        if sources:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=sources,
            )
            nodes = [this_node]
        else:
            nodes = []

        edges = []
        cache.add(self.external_id)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class CommandConfigList(TypeList[CommandConfig]):
    _NODE = CommandConfig
