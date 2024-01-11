from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
    DomainModelCore,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
)

if TYPE_CHECKING:
    from ._sensor_position import SensorPosition, SensorPositionApply


__all__ = ["Blade", "BladeApply", "BladeList", "BladeApplyList", "BladeFields", "BladeTextFields"]


BladeTextFields = Literal["name"]
BladeFields = Literal["is_damaged", "name"]

_BLADE_PROPERTIES_BY_FIELD = {
    "is_damaged": "is_damaged",
    "name": "name",
}


class Blade(DomainModel):
    """This represents the reading version of blade.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the blade.
        is_damaged: The is damaged field.
        name: The name field.
        sensor_positions: The sensor position field.
        created_time: The created time of the blade node.
        last_updated_time: The last updated time of the blade node.
        deleted_time: If present, the deleted time of the blade node.
        version: The version of the blade node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    is_damaged: Optional[bool] = None
    name: Optional[str] = None
    sensor_positions: Union[list[SensorPosition], list[str], None] = Field(default=None, repr=False)

    def as_apply(self) -> BladeApply:
        """Convert this read version of blade to the writing version."""
        return BladeApply(
            space=self.space,
            external_id=self.external_id,
            is_damaged=self.is_damaged,
            name=self.name,
            sensor_positions=[
                sensor_position.as_apply() if isinstance(sensor_position, DomainModel) else sensor_position
                for sensor_position in self.sensor_positions or []
            ],
        )


class BladeApply(DomainModelApply):
    """This represents the writing version of blade.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the blade.
        is_damaged: The is damaged field.
        name: The name field.
        sensor_positions: The sensor position field.
        existing_version: Fail the ingestion request if the blade version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    is_damaged: Optional[bool] = None
    name: Optional[str] = None
    sensor_positions: Union[list[SensorPositionApply], list[str], None] = Field(default=None, repr=False)

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(Blade, dm.ViewId("power-models", "Blade", "1"))

        properties: dict[str, Any] = {}

        if self.is_damaged is not None or write_none:
            properties["is_damaged"] = self.is_damaged

        if self.name is not None or write_none:
            properties["name"] = self.name

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                type=self.node_type,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        edge_type = dm.DirectRelationReference("power-models", "Blade.sensor_positions")
        for sensor_position in self.sensor_positions or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=sensor_position,
                edge_type=edge_type,
                view_by_read_class=view_by_read_class,
            )
            resources.extend(other_resources)

        return resources


class BladeList(DomainModelList[Blade]):
    """List of blades in the read version."""

    _INSTANCE = Blade

    def as_apply(self) -> BladeApplyList:
        """Convert these read versions of blade to the writing versions."""
        return BladeApplyList([node.as_apply() for node in self.data])


class BladeApplyList(DomainModelApplyList[BladeApply]):
    """List of blades in the writing version."""

    _INSTANCE = BladeApply


def _create_blade_filter(
    view_id: dm.ViewId,
    is_damaged: bool | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if is_damaged is not None and isinstance(is_damaged, bool):
        filters.append(dm.filters.Equals(view_id.as_property_ref("is_damaged"), value=is_damaged))
    if name is not None and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
