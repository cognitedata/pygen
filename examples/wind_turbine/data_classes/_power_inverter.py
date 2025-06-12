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
    DirectRelationFilter,
)

if TYPE_CHECKING:
    from wind_turbine.data_classes._nacelle import Nacelle, NacelleList, NacelleGraphQL, NacelleWrite, NacelleWriteList
    from wind_turbine.data_classes._sensor_time_series import (
        SensorTimeSeries,
        SensorTimeSeriesList,
        SensorTimeSeriesGraphQL,
        SensorTimeSeriesWrite,
        SensorTimeSeriesWriteList,
    )


__all__ = [
    "PowerInverter",
    "PowerInverterWrite",
    "PowerInverterList",
    "PowerInverterWriteList",
    "PowerInverterGraphQL",
]


PowerInverterTextFields = Literal["external_id",]
PowerInverterFields = Literal["external_id",]

_POWERINVERTER_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
}


class PowerInverterGraphQL(GraphQLCore):
    """This represents the reading version of power inverter, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the power inverter.
        data_record: The data record of the power inverter node.
        active_power_total: The active power total field.
        apparent_power_total: The apparent power total field.
        nacelle: The nacelle field.
        reactive_power_total: The reactive power total field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "PowerInverter", "1")
    active_power_total: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)
    apparent_power_total: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)
    nacelle: Optional[NacelleGraphQL] = Field(default=None, repr=False)
    reactive_power_total: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)

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

    @field_validator("active_power_total", "apparent_power_total", "nacelle", "reactive_power_total", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> PowerInverter:
        """Convert this GraphQL format of power inverter to the reading format."""
        return PowerInverter.model_validate(as_read_args(self))

    def as_write(self) -> PowerInverterWrite:
        """Convert this GraphQL format of power inverter to the writing format."""
        return PowerInverterWrite.model_validate(as_write_args(self))


class PowerInverter(DomainModel):
    """This represents the reading version of power inverter.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the power inverter.
        data_record: The data record of the power inverter node.
        active_power_total: The active power total field.
        apparent_power_total: The apparent power total field.
        nacelle: The nacelle field.
        reactive_power_total: The reactive power total field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "PowerInverter", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    active_power_total: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(default=None, repr=False)
    apparent_power_total: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(default=None, repr=False)
    nacelle: Optional[Nacelle] = Field(default=None, repr=False)
    reactive_power_total: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(default=None, repr=False)

    @field_validator("active_power_total", "apparent_power_total", "nacelle", "reactive_power_total", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    def as_write(self) -> PowerInverterWrite:
        """Convert this read version of power inverter to the writing version."""
        return PowerInverterWrite.model_validate(as_write_args(self))


class PowerInverterWrite(DomainModelWrite):
    """This represents the writing version of power inverter.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the power inverter.
        data_record: The data record of the power inverter node.
        active_power_total: The active power total field.
        apparent_power_total: The apparent power total field.
        reactive_power_total: The reactive power total field.
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "active_power_total",
        "apparent_power_total",
        "reactive_power_total",
    )
    _direct_relations: ClassVar[tuple[str, ...]] = (
        "active_power_total",
        "apparent_power_total",
        "reactive_power_total",
    )

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "PowerInverter", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    active_power_total: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    apparent_power_total: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    reactive_power_total: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(default=None, repr=False)

    @field_validator("active_power_total", "apparent_power_total", "reactive_power_total", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class PowerInverterList(DomainModelList[PowerInverter]):
    """List of power inverters in the read version."""

    _INSTANCE = PowerInverter

    def as_write(self) -> PowerInverterWriteList:
        """Convert these read versions of power inverter to the writing versions."""
        return PowerInverterWriteList([node.as_write() for node in self.data])

    @property
    def active_power_total(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [item.active_power_total for item in self.data if isinstance(item.active_power_total, SensorTimeSeries)]
        )

    @property
    def apparent_power_total(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [item.apparent_power_total for item in self.data if isinstance(item.apparent_power_total, SensorTimeSeries)]
        )

    @property
    def nacelle(self) -> NacelleList:
        from ._nacelle import Nacelle, NacelleList

        return NacelleList([item.nacelle for item in self.data if isinstance(item.nacelle, Nacelle)])

    @property
    def reactive_power_total(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [item.reactive_power_total for item in self.data if isinstance(item.reactive_power_total, SensorTimeSeries)]
        )


class PowerInverterWriteList(DomainModelWriteList[PowerInverterWrite]):
    """List of power inverters in the writing version."""

    _INSTANCE = PowerInverterWrite

    @property
    def active_power_total(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [
                item.active_power_total
                for item in self.data
                if isinstance(item.active_power_total, SensorTimeSeriesWrite)
            ]
        )

    @property
    def apparent_power_total(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [
                item.apparent_power_total
                for item in self.data
                if isinstance(item.apparent_power_total, SensorTimeSeriesWrite)
            ]
        )

    @property
    def reactive_power_total(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [
                item.reactive_power_total
                for item in self.data
                if isinstance(item.reactive_power_total, SensorTimeSeriesWrite)
            ]
        )


def _create_power_inverter_filter(
    view_id: dm.ViewId,
    active_power_total: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    apparent_power_total: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    reactive_power_total: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(active_power_total, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(active_power_total):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("active_power_total"), value=as_instance_dict_id(active_power_total)
            )
        )
    if (
        active_power_total
        and isinstance(active_power_total, Sequence)
        and not isinstance(active_power_total, str)
        and not is_tuple_id(active_power_total)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("active_power_total"),
                values=[as_instance_dict_id(item) for item in active_power_total],
            )
        )
    if isinstance(apparent_power_total, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(
        apparent_power_total
    ):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("apparent_power_total"), value=as_instance_dict_id(apparent_power_total)
            )
        )
    if (
        apparent_power_total
        and isinstance(apparent_power_total, Sequence)
        and not isinstance(apparent_power_total, str)
        and not is_tuple_id(apparent_power_total)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("apparent_power_total"),
                values=[as_instance_dict_id(item) for item in apparent_power_total],
            )
        )
    if isinstance(reactive_power_total, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(
        reactive_power_total
    ):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("reactive_power_total"), value=as_instance_dict_id(reactive_power_total)
            )
        )
    if (
        reactive_power_total
        and isinstance(reactive_power_total, Sequence)
        and not isinstance(reactive_power_total, str)
        and not is_tuple_id(reactive_power_total)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("reactive_power_total"),
                values=[as_instance_dict_id(item) for item in reactive_power_total],
            )
        )
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _PowerInverterQuery(NodeQueryCore[T_DomainModelList, PowerInverterList]):
    _view_id = PowerInverter._view_id
    _result_cls = PowerInverter
    _result_list_cls_end = PowerInverterList

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
        from ._nacelle import _NacelleQuery
        from ._sensor_time_series import _SensorTimeSeriesQuery

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

        if _SensorTimeSeriesQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.active_power_total = _SensorTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("active_power_total"),
                    direction="outwards",
                ),
                connection_name="active_power_total",
                connection_property=ViewPropertyId(self._view_id, "active_power_total"),
            )

        if _SensorTimeSeriesQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.apparent_power_total = _SensorTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("apparent_power_total"),
                    direction="outwards",
                ),
                connection_name="apparent_power_total",
                connection_property=ViewPropertyId(self._view_id, "apparent_power_total"),
            )

        if _NacelleQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.nacelle = _NacelleQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=dm.ViewId("sp_pygen_power", "Nacelle", "1").as_property_ref("power_inverter"),
                    direction="inwards",
                ),
                connection_name="nacelle",
                connection_property=ViewPropertyId(self._view_id, "nacelle"),
            )

        if _SensorTimeSeriesQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.reactive_power_total = _SensorTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("reactive_power_total"),
                    direction="outwards",
                ),
                connection_name="reactive_power_total",
                connection_property=ViewPropertyId(self._view_id, "reactive_power_total"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.active_power_total_filter = DirectRelationFilter(self, self._view_id.as_property_ref("active_power_total"))
        self.apparent_power_total_filter = DirectRelationFilter(
            self, self._view_id.as_property_ref("apparent_power_total")
        )
        self.reactive_power_total_filter = DirectRelationFilter(
            self, self._view_id.as_property_ref("reactive_power_total")
        )
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.active_power_total_filter,
                self.apparent_power_total_filter,
                self.reactive_power_total_filter,
            ]
        )

    def list_power_inverter(self, limit: int = DEFAULT_QUERY_LIMIT) -> PowerInverterList:
        return self._list(limit=limit)


class PowerInverterQuery(_PowerInverterQuery[PowerInverterList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, PowerInverterList)
