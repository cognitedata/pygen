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
    """This represent a read version of command config.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the command config.
        configs: The config field.
        source: The source field.
        created_time: The created time of the command config node.
        last_updated_time: The last updated time of the command config node.
        deleted_time: If present, the deleted time of the command config node.
        version: The version of the command config node.
    """

    space: str = "IntegrationTestsImmutable"
    configs: Optional[list[str]] = None
    source: Optional[str] = None

    def as_apply(self) -> CommandConfigApply:
        """Convert this read version of command config to a write version."""
        return CommandConfigApply(
            space=self.space,
            external_id=self.external_id,
            configs=self.configs,
            source=self.source,
        )


class CommandConfigApply(DomainModelApply):
    """This represent a write version of command config.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the command config.
        configs: The config field.
        source: The source field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

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
    """List of command configs in read version."""

    _NODE = CommandConfig

    def as_apply(self) -> CommandConfigApplyList:
        """Convert this read version of command config to a write version."""
        return CommandConfigApplyList([node.as_apply() for node in self.data])


class CommandConfigApplyList(TypeApplyList[CommandConfigApply]):
    """List of command configs in write version."""

    _NODE = CommandConfigApply
