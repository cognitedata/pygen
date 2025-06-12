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
    from wind_turbine.data_classes._sensor_time_series import (
        SensorTimeSeries,
        SensorTimeSeriesList,
        SensorTimeSeriesGraphQL,
        SensorTimeSeriesWrite,
        SensorTimeSeriesWriteList,
    )
    from wind_turbine.data_classes._wind_turbine import (
        WindTurbine,
        WindTurbineList,
        WindTurbineGraphQL,
        WindTurbineWrite,
        WindTurbineWriteList,
    )


__all__ = [
    "Rotor",
    "RotorWrite",
    "RotorList",
    "RotorWriteList",
    "RotorGraphQL",
]


RotorTextFields = Literal["external_id",]
RotorFields = Literal["external_id",]

_ROTOR_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
}


class RotorGraphQL(GraphQLCore):
    """This represents the reading version of rotor, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the rotor.
        data_record: The data record of the rotor node.
        rotor_speed_controller: The rotor speed controller field.
        rpm_low_speed_shaft: The rpm low speed shaft field.
        wind_turbine: The wind turbine field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "Rotor", "1")
    rotor_speed_controller: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)
    rpm_low_speed_shaft: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)
    wind_turbine: Optional[WindTurbineGraphQL] = Field(default=None, repr=False)

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

    @field_validator("rotor_speed_controller", "rpm_low_speed_shaft", "wind_turbine", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> Rotor:
        """Convert this GraphQL format of rotor to the reading format."""
        return Rotor.model_validate(as_read_args(self))

    def as_write(self) -> RotorWrite:
        """Convert this GraphQL format of rotor to the writing format."""
        return RotorWrite.model_validate(as_write_args(self))


class Rotor(DomainModel):
    """This represents the reading version of rotor.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the rotor.
        data_record: The data record of the rotor node.
        rotor_speed_controller: The rotor speed controller field.
        rpm_low_speed_shaft: The rpm low speed shaft field.
        wind_turbine: The wind turbine field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "Rotor", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    rotor_speed_controller: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(default=None, repr=False)
    rpm_low_speed_shaft: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(default=None, repr=False)
    wind_turbine: Optional[WindTurbine] = Field(default=None, repr=False)

    @field_validator("rotor_speed_controller", "rpm_low_speed_shaft", "wind_turbine", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    def as_write(self) -> RotorWrite:
        """Convert this read version of rotor to the writing version."""
        return RotorWrite.model_validate(as_write_args(self))


class RotorWrite(DomainModelWrite):
    """This represents the writing version of rotor.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the rotor.
        data_record: The data record of the rotor node.
        rotor_speed_controller: The rotor speed controller field.
        rpm_low_speed_shaft: The rpm low speed shaft field.
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "rotor_speed_controller",
        "rpm_low_speed_shaft",
    )
    _direct_relations: ClassVar[tuple[str, ...]] = (
        "rotor_speed_controller",
        "rpm_low_speed_shaft",
    )

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "Rotor", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    rotor_speed_controller: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    rpm_low_speed_shaft: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(default=None, repr=False)

    @field_validator("rotor_speed_controller", "rpm_low_speed_shaft", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class RotorList(DomainModelList[Rotor]):
    """List of rotors in the read version."""

    _INSTANCE = Rotor

    def as_write(self) -> RotorWriteList:
        """Convert these read versions of rotor to the writing versions."""
        return RotorWriteList([node.as_write() for node in self.data])

    @property
    def rotor_speed_controller(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [
                item.rotor_speed_controller
                for item in self.data
                if isinstance(item.rotor_speed_controller, SensorTimeSeries)
            ]
        )

    @property
    def rpm_low_speed_shaft(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [item.rpm_low_speed_shaft for item in self.data if isinstance(item.rpm_low_speed_shaft, SensorTimeSeries)]
        )

    @property
    def wind_turbine(self) -> WindTurbineList:
        from ._wind_turbine import WindTurbine, WindTurbineList

        return WindTurbineList([item.wind_turbine for item in self.data if isinstance(item.wind_turbine, WindTurbine)])


class RotorWriteList(DomainModelWriteList[RotorWrite]):
    """List of rotors in the writing version."""

    _INSTANCE = RotorWrite

    @property
    def rotor_speed_controller(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [
                item.rotor_speed_controller
                for item in self.data
                if isinstance(item.rotor_speed_controller, SensorTimeSeriesWrite)
            ]
        )

    @property
    def rpm_low_speed_shaft(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [
                item.rpm_low_speed_shaft
                for item in self.data
                if isinstance(item.rpm_low_speed_shaft, SensorTimeSeriesWrite)
            ]
        )


def _create_rotor_filter(
    view_id: dm.ViewId,
    rotor_speed_controller: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    rpm_low_speed_shaft: (
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
    if isinstance(rotor_speed_controller, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(
        rotor_speed_controller
    ):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("rotor_speed_controller"), value=as_instance_dict_id(rotor_speed_controller)
            )
        )
    if (
        rotor_speed_controller
        and isinstance(rotor_speed_controller, Sequence)
        and not isinstance(rotor_speed_controller, str)
        and not is_tuple_id(rotor_speed_controller)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("rotor_speed_controller"),
                values=[as_instance_dict_id(item) for item in rotor_speed_controller],
            )
        )
    if isinstance(rpm_low_speed_shaft, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(
        rpm_low_speed_shaft
    ):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("rpm_low_speed_shaft"), value=as_instance_dict_id(rpm_low_speed_shaft)
            )
        )
    if (
        rpm_low_speed_shaft
        and isinstance(rpm_low_speed_shaft, Sequence)
        and not isinstance(rpm_low_speed_shaft, str)
        and not is_tuple_id(rpm_low_speed_shaft)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("rpm_low_speed_shaft"),
                values=[as_instance_dict_id(item) for item in rpm_low_speed_shaft],
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


class _RotorQuery(NodeQueryCore[T_DomainModelList, RotorList]):
    _view_id = Rotor._view_id
    _result_cls = Rotor
    _result_list_cls_end = RotorList

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
        from ._sensor_time_series import _SensorTimeSeriesQuery
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

        if _SensorTimeSeriesQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.rotor_speed_controller = _SensorTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("rotor_speed_controller"),
                    direction="outwards",
                ),
                connection_name="rotor_speed_controller",
                connection_property=ViewPropertyId(self._view_id, "rotor_speed_controller"),
            )

        if _SensorTimeSeriesQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.rpm_low_speed_shaft = _SensorTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("rpm_low_speed_shaft"),
                    direction="outwards",
                ),
                connection_name="rpm_low_speed_shaft",
                connection_property=ViewPropertyId(self._view_id, "rpm_low_speed_shaft"),
            )

        if _WindTurbineQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.wind_turbine = _WindTurbineQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=dm.ViewId("sp_pygen_power", "WindTurbine", "1").as_property_ref("rotor"),
                    direction="inwards",
                ),
                connection_name="wind_turbine",
                connection_property=ViewPropertyId(self._view_id, "wind_turbine"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.rotor_speed_controller_filter = DirectRelationFilter(
            self, self._view_id.as_property_ref("rotor_speed_controller")
        )
        self.rpm_low_speed_shaft_filter = DirectRelationFilter(
            self, self._view_id.as_property_ref("rpm_low_speed_shaft")
        )
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.rotor_speed_controller_filter,
                self.rpm_low_speed_shaft_filter,
            ]
        )

    def list_rotor(self, limit: int = DEFAULT_QUERY_LIMIT) -> RotorList:
        return self._list(limit=limit)


class RotorQuery(_RotorQuery[RotorList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, RotorList)
