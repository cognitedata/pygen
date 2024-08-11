from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelation,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
    T_DomainModelList,
    as_node_id,
    as_pygen_node_id,
    are_nodes_equal,
    select_best_node,
    QueryCore,
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

    @model_validator(mode="before")
    def parse_data_record(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        if "lastUpdatedTime" in values or "createdTime" in values:
            values["dataRecord"] = DataRecordGraphQL(
                created_time=values.pop("createdTime", None),
                last_updated_time=values.pop("lastUpdatedTime", None),
            )
        return values

    @field_validator("sensor_positions", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
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
    sensor_positions: Optional[list[Union[SensorPosition, str, dm.NodeId]]] = Field(default=None, repr=False)

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

    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, Blade],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._sensor_position import SensorPosition

        for instance in instances.values():
            if edges := edges_by_source_node.get(instance.as_id()):
                sensor_positions: list[SensorPosition | str | dm.NodeId] = []
                for edge in edges:
                    value: DomainModel | DomainRelation | str | dm.NodeId
                    if isinstance(edge, DomainRelation):
                        value = edge
                    else:
                        other_end: dm.DirectRelationReference = (
                            edge.end_node
                            if edge.start_node.space == instance.space
                            and edge.start_node.external_id == instance.external_id
                            else edge.start_node
                        )
                        destination: dm.NodeId | str = (
                            as_node_id(other_end)
                            if other_end.space != DEFAULT_INSTANCE_SPACE
                            else other_end.external_id
                        )
                        if destination in nodes_by_id:
                            value = nodes_by_id[destination]
                        else:
                            value = destination
                    edge_type = edge.edge_type if isinstance(edge, DomainRelation) else edge.type

                    if edge_type == dm.DirectRelationReference("power-models", "Blade.sensor_positions") and isinstance(
                        value, (SensorPosition, str, dm.NodeId)
                    ):
                        sensor_positions.append(value)

                instance.sensor_positions = sensor_positions or None


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
    sensor_positions: Optional[list[Union[SensorPositionWrite, str, dm.NodeId]]] = Field(default=None, repr=False)

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
    filters: list[dm.Filter] = []
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


class _BladeQuery(QueryCore[T_DomainModelList, BladeList]):
    _view_id = Blade._view_id
    _result_cls = Blade
    _result_list_cls_end = BladeList

    def __init__(
        self,
        created_types: set[type],
        creation_path: list[QueryCore],
        client: CogniteClient,
        result_list_cls: type[T_DomainModelList],
        expression: dm.query.ResultSetExpression | None = None,
    ):
        from ._sensor_position import _SensorPositionQuery

        super().__init__(created_types, creation_path, client, result_list_cls, expression)

        if _SensorPositionQuery not in created_types:
            self.sensor_positions = _SensorPositionQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
            )

    def _assemble_filter(self) -> dm.filters.Filter:
        return dm.filters.HasData(views=[self._view_id])


class BladeQuery(_BladeQuery[BladeList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, BladeList)
