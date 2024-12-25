from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union, no_type_check

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from pydantic import Field, field_validator, model_validator

from wind_turbine.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    BooleanFilter,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelList,
    DomainModelWrite,
    DomainModelWriteList,
    DomainRelation,
    GraphQLCore,
    NodeQueryCore,
    QueryCore,
    ResourcesWrite,
    StringFilter,
    T_DomainModelList,
    as_direct_relation_reference,
    as_pygen_node_id,
)

if TYPE_CHECKING:
    from wind_turbine.data_classes._sensor_position import (
        SensorPosition,
        SensorPositionGraphQL,
        SensorPositionList,
    )


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


BladeTextFields = Literal["external_id", "name"]
BladeFields = Literal["external_id", "is_damaged", "name"]

_BLADE_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
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
        name: Name of the instance
        sensor_positions: The sensor position field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "Blade", "1")
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
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            is_damaged=self.is_damaged,
            name=self.name,
            sensor_positions=(
                [sensor_position.as_read() for sensor_position in self.sensor_positions]
                if self.sensor_positions is not None
                else None
            ),
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> BladeWrite:
        """Convert this GraphQL format of blade to the writing format."""
        return BladeWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            is_damaged=self.is_damaged,
            name=self.name,
        )


class Blade(DomainModel):
    """This represents the reading version of blade.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the blade.
        data_record: The data record of the blade node.
        is_damaged: The is damaged field.
        name: Name of the instance
        sensor_positions: The sensor position field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "Blade", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    is_damaged: Optional[bool] = None
    name: Optional[str] = None
    sensor_positions: Optional[list[SensorPosition]] = Field(default=None, repr=False)

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> BladeWrite:
        """Convert this read version of blade to the writing version."""
        return BladeWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            is_damaged=self.is_damaged,
            name=self.name,
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

        for node in nodes_by_id.values():
            if (
                isinstance(node, SensorPosition)
                and node.blade is not None
                and (blade := instances.get(as_pygen_node_id(node.blade)))
            ):
                if blade.sensor_positions is None:
                    blade.sensor_positions = []
                blade.sensor_positions.append(node)


class BladeWrite(DomainModelWrite):
    """This represents the writing version of blade.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the blade.
        data_record: The data record of the blade node.
        is_damaged: The is damaged field.
        name: Name of the instance
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "Blade", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    is_damaged: Optional[bool] = None
    name: Optional[str] = None

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
                type=as_direct_relation_reference(self.node_type),
                sources=[
                    dm.NodeOrEdgeData(
                        source=self._view_id,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

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

    @property
    def sensor_positions(self) -> SensorPositionList:
        from ._sensor_position import SensorPosition, SensorPositionList

        return SensorPositionList(
            [item for items in self.data for item in items.sensor_positions or [] if isinstance(item, SensorPosition)]
        )


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


class _BladeQuery(NodeQueryCore[T_DomainModelList, BladeList]):
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
        connection_name: str | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
    ):
        from ._sensor_position import _SensorPositionQuery

        super().__init__(
            created_types,
            creation_path,
            client,
            result_list_cls,
            expression,
            dm.filters.HasData(views=[self._view_id]),
            connection_name,
            connection_type,
            reverse_expression,
        )

        if _SensorPositionQuery not in created_types:
            self.sensor_positions = _SensorPositionQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=dm.ViewId("sp_pygen_power", "SensorPosition", "1").as_property_ref("blade"),
                    direction="inwards",
                ),
                connection_name="sensor_positions",
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.is_damaged = BooleanFilter(self, self._view_id.as_property_ref("is_damaged"))
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.is_damaged,
                self.name,
            ]
        )

    def list_blade(self, limit: int = DEFAULT_QUERY_LIMIT) -> BladeList:
        return self._list(limit=limit)


class BladeQuery(_BladeQuery[BladeList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, BladeList)
