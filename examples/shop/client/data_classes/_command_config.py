from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = [
    "CommandConfig",
    "CommandConfigApply",
    "CommandConfigList",
    "CommandConfigApplyList",
    "CommandConfigFields",
    "CommandConfigTextFields",
]


CommandConfigTextFields = Literal["configs", "source"]
CommandConfigFields = Literal["configs", "source"]

_COMMANDCONFIG_PROPERTIES_BY_FIELD = {
    "configs": "configs",
    "source": "source",
}


class CommandConfig(DomainModel):
    space: str = "IntegrationTestsImmutable"
    configs: Optional[list[str]] = None
    source: Optional[str] = None

    def as_apply(self) -> CommandConfigApply:
        return CommandConfigApply(
            space=self.space,
            external_id=self.external_id,
            configs=self.configs,
            source=self.source,
        )


class CommandConfigApply(DomainModelApply):
    space: str = "IntegrationTestsImmutable"
    configs: list[str]
    source: Optional[str] = None

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.configs is not None:
            properties["configs"] = self.configs
        if self.source is not None:
            properties["source"] = self.source
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "Command_Config", "4727b5ad34b608"),
                properties=properties,
            )
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[source],
            )
            nodes = [this_node]
        else:
            nodes = []

        edges = []
        cache.add(self.external_id)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class CommandConfigList(TypeList[CommandConfig]):
    _NODE = CommandConfig

    def as_apply(self) -> CommandConfigApplyList:
        return CommandConfigApplyList([node.as_apply() for node in self.data])


class CommandConfigApplyList(TypeApplyList[CommandConfigApply]):
    _NODE = CommandConfigApply
