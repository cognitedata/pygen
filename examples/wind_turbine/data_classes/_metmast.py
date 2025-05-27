from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from cognite.client.data_classes import (
    TimeSeries as CogniteTimeSeries,
    TimeSeriesWrite as CogniteTimeSeriesWrite,
)
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
    FileMetadata,
    FileMetadataWrite,
    FileMetadataGraphQL,
    TimeSeries,
    TimeSeriesWrite,
    TimeSeriesGraphQL,
    TimeSeriesReferenceAPI,
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
    FloatFilter,
)

if TYPE_CHECKING:
    from wind_turbine.data_classes._distance import (
        Distance,
        DistanceList,
        DistanceGraphQL,
        DistanceWrite,
        DistanceWriteList,
    )


__all__ = [
    "Metmast",
    "MetmastWrite",
    "MetmastList",
    "MetmastWriteList",
    "MetmastFields",
    "MetmastTextFields",
    "MetmastGraphQL",
]


MetmastTextFields = Literal["external_id", "temperature", "tilt_angle", "wind_speed"]
MetmastFields = Literal["external_id", "position", "temperature", "tilt_angle", "wind_speed"]

_METMAST_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "position": "position",
    "temperature": "temperature",
    "tilt_angle": "tilt_angle",
    "wind_speed": "wind_speed",
}


class MetmastGraphQL(GraphQLCore):
    """This represents the reading version of metmast, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the metmast.
        data_record: The data record of the metmast node.
        position: The position field.
        temperature: The temperature field.
        tilt_angle: The tilt angle field.
        wind_speed: The wind speed field.
        wind_turbines: The wind turbine field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "Metmast", "1")
    position: Optional[float] = None
    temperature: Optional[TimeSeriesGraphQL] = None
    tilt_angle: Optional[TimeSeriesGraphQL] = None
    wind_speed: Optional[TimeSeriesGraphQL] = None
    wind_turbines: Optional[list[DistanceGraphQL]] = Field(default=None, repr=False)

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

    @field_validator("wind_turbines", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> Metmast:
        """Convert this GraphQL format of metmast to the reading format."""
        return Metmast.model_validate(as_read_args(self))

    def as_write(self) -> MetmastWrite:
        """Convert this GraphQL format of metmast to the writing format."""
        return MetmastWrite.model_validate(as_write_args(self))


class Metmast(DomainModel):
    """This represents the reading version of metmast.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the metmast.
        data_record: The data record of the metmast node.
        position: The position field.
        temperature: The temperature field.
        tilt_angle: The tilt angle field.
        wind_speed: The wind speed field.
        wind_turbines: The wind turbine field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "Metmast", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    position: Optional[float] = None
    temperature: Union[TimeSeries, str, None] = None
    tilt_angle: Union[TimeSeries, str, None] = None
    wind_speed: Union[TimeSeries, str, None] = None
    wind_turbines: Optional[list[Distance]] = Field(default=None, repr=False)

    @field_validator("wind_turbines", mode="before")
    @classmethod
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        return [parse_single_connection(item, info.field_name) for item in value]

    def as_write(self) -> MetmastWrite:
        """Convert this read version of metmast to the writing version."""
        return MetmastWrite.model_validate(as_write_args(self))


class MetmastWrite(DomainModelWrite):
    """This represents the writing version of metmast.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the metmast.
        data_record: The data record of the metmast node.
        position: The position field.
        temperature: The temperature field.
        tilt_angle: The tilt angle field.
        wind_speed: The wind speed field.
        wind_turbines: The wind turbine field.
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "position",
        "temperature",
        "tilt_angle",
        "wind_speed",
    )
    _inwards_edges: ClassVar[tuple[tuple[str, dm.DirectRelationReference], ...]] = (
        ("wind_turbines", dm.DirectRelationReference("sp_pygen_power_enterprise", "Distance")),
    )

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "Metmast", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    position: Optional[float] = None
    temperature: Union[TimeSeriesWrite, str, None] = None
    tilt_angle: Union[TimeSeriesWrite, str, None] = None
    wind_speed: Union[TimeSeriesWrite, str, None] = None
    wind_turbines: Optional[list[DistanceWrite]] = Field(default=None, repr=False)

    @field_validator("wind_turbines", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class MetmastList(DomainModelList[Metmast]):
    """List of metmasts in the read version."""

    _INSTANCE = Metmast

    def as_write(self) -> MetmastWriteList:
        """Convert these read versions of metmast to the writing versions."""
        return MetmastWriteList([node.as_write() for node in self.data])

    @property
    def wind_turbines(self) -> DistanceList:
        from ._distance import Distance, DistanceList

        return DistanceList(
            [item for items in self.data for item in items.wind_turbines or [] if isinstance(item, Distance)]
        )


class MetmastWriteList(DomainModelWriteList[MetmastWrite]):
    """List of metmasts in the writing version."""

    _INSTANCE = MetmastWrite

    @property
    def wind_turbines(self) -> DistanceWriteList:
        from ._distance import DistanceWrite, DistanceWriteList

        return DistanceWriteList(
            [item for items in self.data for item in items.wind_turbines or [] if isinstance(item, DistanceWrite)]
        )


def _create_metmast_filter(
    view_id: dm.ViewId,
    min_position: float | None = None,
    max_position: float | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if min_position is not None or max_position is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("position"), gte=min_position, lte=max_position))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _MetmastQuery(NodeQueryCore[T_DomainModelList, MetmastList]):
    _view_id = Metmast._view_id
    _result_cls = Metmast
    _result_list_cls_end = MetmastList

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
        from ._distance import _DistanceQuery
        from ._wind_turbine import _WindTurbineQuery

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

        if _DistanceQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.wind_turbines = _DistanceQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                _WindTurbineQuery,
                dm.query.EdgeResultSetExpression(
                    direction="inwards",
                    chain_to="destination",
                ),
                connection_name="wind_turbines",
                connection_property=ViewPropertyId(self._view_id, "wind_turbines"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.position = FloatFilter(self, self._view_id.as_property_ref("position"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.position,
            ]
        )
        self.temperature = TimeSeriesReferenceAPI(
            client,
            lambda limit: [
                item.temperature if isinstance(item.temperature, str) else item.temperature.external_id  # type: ignore[misc]
                for item in self._list(limit=limit)
                if item.temperature is not None
                and (isinstance(item.temperature, str) or item.temperature.external_id is not None)
            ],
        )
        self.tilt_angle = TimeSeriesReferenceAPI(
            client,
            lambda limit: [
                item.tilt_angle if isinstance(item.tilt_angle, str) else item.tilt_angle.external_id  # type: ignore[misc]
                for item in self._list(limit=limit)
                if item.tilt_angle is not None
                and (isinstance(item.tilt_angle, str) or item.tilt_angle.external_id is not None)
            ],
        )
        self.wind_speed = TimeSeriesReferenceAPI(
            client,
            lambda limit: [
                item.wind_speed if isinstance(item.wind_speed, str) else item.wind_speed.external_id  # type: ignore[misc]
                for item in self._list(limit=limit)
                if item.wind_speed is not None
                and (isinstance(item.wind_speed, str) or item.wind_speed.external_id is not None)
            ],
        )

    def list_metmast(self, limit: int = DEFAULT_QUERY_LIMIT) -> MetmastList:
        return self._list(limit=limit)


class MetmastQuery(_MetmastQuery[MetmastList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, MetmastList)
