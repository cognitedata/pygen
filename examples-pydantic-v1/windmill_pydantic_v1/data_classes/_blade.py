from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field
from pydantic import validator, root_validator

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
)

if TYPE_CHECKING:
    from ._sensor_position import SensorPosition, SensorPositionGraphQL, SensorPositionWrite


__all__ = [
    "Blade",
    "BladeWrite",
    "BladeApply",
    "BladeList",
    "BladeWriteList",
    "BladeApplyList",
    "BladeFields",
    "BladeTextFields",
    "BladeGraphQL",
]


BladeTextFields = Literal["name"]
BladeFields = Literal["is_damaged", "name"]

_BLADE_PROPERTIES_BY_FIELD = {
    "is_damaged": "is_damaged",
    "name": "name",
}


class BladeGraphQL(GraphQLCore):
    """This represents the reading version of blade, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the blade.
        data_record: The data record of the blade node.
        is_damaged: The is damaged field.
        name: The name field.
        sensor_positions: The sensor position field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power-models", "Blade", "1")
    is_damaged: Optional[bool] = None
    name: Optional[str] = None
    sensor_positions: Optional[list[SensorPositionGraphQL]] = Field(default=None, repr=False)

    @root_validator(pre=True)
    def parse_data_record(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        if "lastUpdatedTime" in values or "createdTime" in values:
            values["dataRecord"] = DataRecordGraphQL(
                created_time=values.pop("createdTime", None),
                last_updated_time=values.pop("lastUpdatedTime", None),
            )
        return values

    @validator("sensor_positions", pre=True)
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> Blade:
        """Convert this GraphQL format of blade to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return Blade(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            is_damaged=self.is_damaged,
            name=self.name,
            sensor_positions=[sensor_position.as_read() for sensor_position in self.sensor_positions or []],
        )

    def as_write(self) -> BladeWrite:
        """Convert this GraphQL format of blade to the writing format."""
        return BladeWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            is_damaged=self.is_damaged,
            name=self.name,
            sensor_positions=[sensor_position.as_write() for sensor_position in self.sensor_positions or []],
        )


class Blade(DomainModel):
    """This represents the reading version of blade.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the blade.
        data_record: The data record of the blade node.
        is_damaged: The is damaged field.
        name: The name field.
        sensor_positions: The sensor position field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power-models", "Blade", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    is_damaged: Optional[bool] = None
    name: Optional[str] = None
    sensor_positions: Union[list[SensorPosition], list[str], list[dm.NodeId], None] = Field(default=None, repr=False)

    def as_write(self) -> BladeWrite:
        """Convert this read version of blade to the writing version."""
        return BladeWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            is_damaged=self.is_damaged,
            name=self.name,
            sensor_positions=[
                sensor_position.as_write() if isinstance(sensor_position, DomainModel) else sensor_position
                for sensor_position in self.sensor_positions or []
            ],
        )

    def as_apply(self) -> BladeWrite:
        """Convert this read version of blade to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class BladeWrite(DomainModelWrite):
    """This represents the writing version of blade.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the blade.
        data_record: The data record of the blade node.
        is_damaged: The is damaged field.
        name: The name field.
        sensor_positions: The sensor position field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power-models", "Blade", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    is_damaged: Optional[bool] = None
    name: Optional[str] = None
    sensor_positions: Union[list[SensorPositionWrite], list[str], list[dm.NodeId], None] = Field(
        default=None, repr=False
    )

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        properties: dict[str, Any] = {}

        if self.is_damaged is not None or write_none:
            properties["is_damaged"] = self.is_damaged

        if self.name is not None or write_none:
            properties["name"] = self.name

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=None if allow_version_increase else self.data_record.existing_version,
                type=self.node_type,
                sources=[
                    dm.NodeOrEdgeData(
                        source=self._view_id,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        edge_type = dm.DirectRelationReference("power-models", "Blade.sensor_positions")
        for sensor_position in self.sensor_positions or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=sensor_position,
                edge_type=edge_type,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        return resources


class BladeApply(BladeWrite):
    def __new__(cls, *args, **kwargs) -> BladeApply:
        warnings.warn(
            "BladeApply is deprecated and will be removed in v1.0. Use BladeWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "Blade.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class BladeList(DomainModelList[Blade]):
    """List of blades in the read version."""

    _INSTANCE = Blade

    def as_write(self) -> BladeWriteList:
        """Convert these read versions of blade to the writing versions."""
        return BladeWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> BladeWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class BladeWriteList(DomainModelWriteList[BladeWrite]):
    """List of blades in the writing version."""

    _INSTANCE = BladeWrite


class BladeApplyList(BladeWriteList): ...


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
    if isinstance(is_damaged, bool):
        filters.append(dm.filters.Equals(view_id.as_property_ref("is_damaged"), value=is_damaged))
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
