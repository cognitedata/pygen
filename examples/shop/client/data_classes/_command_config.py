from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
)


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
    """This represents the reading version of command config.

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

    space: str = DEFAULT_INSTANCE_SPACE
    configs: Optional[list[str]] = None
    source: Union[str, None] = None

    def as_apply(self) -> CommandConfigApply:
        """Convert this read version of command config to the writing version."""
        return CommandConfigApply(
            space=self.space,
            external_id=self.external_id,
            configs=self.configs,
            source=self.source,
        )


class CommandConfigApply(DomainModelApply):
    """This represents the writing version of command config.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the command config.
        configs: The config field.
        source: The source field.
        existing_version: Fail the ingestion request if the command config version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    configs: list[str]
    source: Union[str, None] = None

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "IntegrationTestsImmutable", "Command_Config", "4727b5ad34b608"
        )

        properties = {}
        if self.configs is not None:
            properties["configs"] = self.configs
        if self.source is not None:
            properties["source"] = self.source

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        return resources


class CommandConfigList(DomainModelList[CommandConfig]):
    """List of command configs in the read version."""

    _INSTANCE = CommandConfig

    def as_apply(self) -> CommandConfigApplyList:
        """Convert these read versions of command config to the writing versions."""
        return CommandConfigApplyList([node.as_apply() for node in self.data])


class CommandConfigApplyList(DomainModelApplyList[CommandConfigApply]):
    """List of command configs in the writing version."""

    _INSTANCE = CommandConfigApply


def _create_command_config_filter(
    view_id: dm.ViewId,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
