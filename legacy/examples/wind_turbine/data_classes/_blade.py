from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator, ValidationInfo

from wind_turbine.config import global_config
from wind_turbine.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
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
    as_read_args,
    as_write_args,
    is_tuple_id,
    as_instance_dict_id,
    parse_single_connection,
    QueryCore,
    NodeQueryCore,
    StringFilter,
    ViewPropertyId,
    BooleanFilter,
)

if TYPE_CHECKING:
    from wind_turbine.data_classes._sensor_position import (
        SensorPosition,
        SensorPositionList,
        SensorPositionGraphQL,
        SensorPositionWrite,
        SensorPositionWriteList,
    )


__all__ = [
    "Blade",
    "BladeWrite",
    "BladeList",
    "BladeWriteList",
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

    def as_read(self) -> Blade:
        """Convert this GraphQL format of blade to the reading format."""
        return Blade.model_validate(as_read_args(self))

    def as_write(self) -> BladeWrite:
        """Convert this GraphQL format of blade to the writing format."""
        return BladeWrite.model_validate(as_write_args(self))


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

    @field_validator("sensor_positions", mode="before")
    @classmethod
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        return [parse_single_connection(item, info.field_name) for item in value]

    def as_write(self) -> BladeWrite:
        """Convert this read version of blade to the writing version."""
        return BladeWrite.model_validate(as_write_args(self))


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

    _container_fields: ClassVar[tuple[str, ...]] = (
        "is_damaged",
        "name",
    )

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "Blade", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    is_damaged: Optional[bool] = None
    name: Optional[str] = None


class BladeList(DomainModelList[Blade]):
    """List of blades in the read version."""

    _INSTANCE = Blade

    def as_write(self) -> BladeWriteList:
        """Convert these read versions of blade to the writing versions."""
        return BladeWriteList([node.as_write() for node in self.data])

    @property
    def sensor_positions(self) -> SensorPositionList:
        from ._sensor_position import SensorPosition, SensorPositionList

        return SensorPositionList(
            [item for items in self.data for item in items.sensor_positions or [] if isinstance(item, SensorPosition)]
        )


class BladeWriteList(DomainModelWriteList[BladeWrite]):
    """List of blades in the writing version."""

    _INSTANCE = BladeWrite


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
        expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_property: ViewPropertyId | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
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
            connection_property,
            connection_type,
            reverse_expression,
        )

        if _SensorPositionQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
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
                connection_property=ViewPropertyId(self._view_id, "sensor_positions"),
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
