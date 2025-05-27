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
    from wind_turbine.data_classes._gearbox import Gearbox, GearboxList, GearboxGraphQL, GearboxWrite, GearboxWriteList
    from wind_turbine.data_classes._generator import (
        Generator,
        GeneratorList,
        GeneratorGraphQL,
        GeneratorWrite,
        GeneratorWriteList,
    )
    from wind_turbine.data_classes._high_speed_shaft import (
        HighSpeedShaft,
        HighSpeedShaftList,
        HighSpeedShaftGraphQL,
        HighSpeedShaftWrite,
        HighSpeedShaftWriteList,
    )
    from wind_turbine.data_classes._main_shaft import (
        MainShaft,
        MainShaftList,
        MainShaftGraphQL,
        MainShaftWrite,
        MainShaftWriteList,
    )
    from wind_turbine.data_classes._power_inverter import (
        PowerInverter,
        PowerInverterList,
        PowerInverterGraphQL,
        PowerInverterWrite,
        PowerInverterWriteList,
    )
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
    "Nacelle",
    "NacelleWrite",
    "NacelleList",
    "NacelleWriteList",
    "NacelleGraphQL",
]


NacelleTextFields = Literal["external_id",]
NacelleFields = Literal["external_id",]

_NACELLE_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
}


class NacelleGraphQL(GraphQLCore):
    """This represents the reading version of nacelle, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the nacelle.
        data_record: The data record of the nacelle node.
        acc_from_back_side_x: The acc from back side x field.
        acc_from_back_side_y: The acc from back side y field.
        acc_from_back_side_z: The acc from back side z field.
        gearbox: The gearbox field.
        generator: The generator field.
        high_speed_shaft: The high speed shaft field.
        main_shaft: The main shaft field.
        power_inverter: The power inverter field.
        wind_turbine: The wind turbine field.
        yaw_direction: The yaw direction field.
        yaw_error: The yaw error field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "Nacelle", "1")
    acc_from_back_side_x: Optional[dict] = Field(default=None)
    acc_from_back_side_y: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)
    acc_from_back_side_z: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)
    gearbox: Optional[GearboxGraphQL] = Field(default=None, repr=False)
    generator: Optional[GeneratorGraphQL] = Field(default=None, repr=False)
    high_speed_shaft: Optional[HighSpeedShaftGraphQL] = Field(default=None, repr=False)
    main_shaft: Optional[MainShaftGraphQL] = Field(default=None, repr=False)
    power_inverter: Optional[PowerInverterGraphQL] = Field(default=None, repr=False)
    wind_turbine: Optional[WindTurbineGraphQL] = Field(default=None, repr=False)
    yaw_direction: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)
    yaw_error: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)

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

    @field_validator(
        "acc_from_back_side_x",
        "acc_from_back_side_y",
        "acc_from_back_side_z",
        "gearbox",
        "generator",
        "high_speed_shaft",
        "main_shaft",
        "power_inverter",
        "wind_turbine",
        "yaw_direction",
        "yaw_error",
        mode="before",
    )
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> Nacelle:
        """Convert this GraphQL format of nacelle to the reading format."""
        return Nacelle.model_validate(as_read_args(self))

    def as_write(self) -> NacelleWrite:
        """Convert this GraphQL format of nacelle to the writing format."""
        return NacelleWrite.model_validate(as_write_args(self))


class Nacelle(DomainModel):
    """This represents the reading version of nacelle.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the nacelle.
        data_record: The data record of the nacelle node.
        acc_from_back_side_x: The acc from back side x field.
        acc_from_back_side_y: The acc from back side y field.
        acc_from_back_side_z: The acc from back side z field.
        gearbox: The gearbox field.
        generator: The generator field.
        high_speed_shaft: The high speed shaft field.
        main_shaft: The main shaft field.
        power_inverter: The power inverter field.
        wind_turbine: The wind turbine field.
        yaw_direction: The yaw direction field.
        yaw_error: The yaw error field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "Nacelle", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    acc_from_back_side_x: Union[str, dm.NodeId, None] = Field(default=None)
    acc_from_back_side_y: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(default=None, repr=False)
    acc_from_back_side_z: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(default=None, repr=False)
    gearbox: Union[Gearbox, str, dm.NodeId, None] = Field(default=None, repr=False)
    generator: Union[Generator, str, dm.NodeId, None] = Field(default=None, repr=False)
    high_speed_shaft: Union[HighSpeedShaft, str, dm.NodeId, None] = Field(default=None, repr=False)
    main_shaft: Union[MainShaft, str, dm.NodeId, None] = Field(default=None, repr=False)
    power_inverter: Union[PowerInverter, str, dm.NodeId, None] = Field(default=None, repr=False)
    wind_turbine: Optional[WindTurbine] = Field(default=None, repr=False)
    yaw_direction: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(default=None, repr=False)
    yaw_error: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(default=None, repr=False)

    @field_validator(
        "acc_from_back_side_x",
        "acc_from_back_side_y",
        "acc_from_back_side_z",
        "gearbox",
        "generator",
        "high_speed_shaft",
        "main_shaft",
        "power_inverter",
        "wind_turbine",
        "yaw_direction",
        "yaw_error",
        mode="before",
    )
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    def as_write(self) -> NacelleWrite:
        """Convert this read version of nacelle to the writing version."""
        return NacelleWrite.model_validate(as_write_args(self))


class NacelleWrite(DomainModelWrite):
    """This represents the writing version of nacelle.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the nacelle.
        data_record: The data record of the nacelle node.
        acc_from_back_side_x: The acc from back side x field.
        acc_from_back_side_y: The acc from back side y field.
        acc_from_back_side_z: The acc from back side z field.
        gearbox: The gearbox field.
        generator: The generator field.
        high_speed_shaft: The high speed shaft field.
        main_shaft: The main shaft field.
        power_inverter: The power inverter field.
        yaw_direction: The yaw direction field.
        yaw_error: The yaw error field.
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "acc_from_back_side_x",
        "acc_from_back_side_y",
        "acc_from_back_side_z",
        "gearbox",
        "generator",
        "high_speed_shaft",
        "main_shaft",
        "power_inverter",
        "yaw_direction",
        "yaw_error",
    )
    _direct_relations: ClassVar[tuple[str, ...]] = (
        "acc_from_back_side_y",
        "acc_from_back_side_z",
        "gearbox",
        "generator",
        "high_speed_shaft",
        "main_shaft",
        "power_inverter",
        "yaw_direction",
        "yaw_error",
    )

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "Nacelle", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    acc_from_back_side_x: Union[str, dm.NodeId, None] = Field(default=None)
    acc_from_back_side_y: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    acc_from_back_side_z: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    gearbox: Union[GearboxWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    generator: Union[GeneratorWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    high_speed_shaft: Union[HighSpeedShaftWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    main_shaft: Union[MainShaftWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    power_inverter: Union[PowerInverterWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    yaw_direction: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    yaw_error: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(default=None, repr=False)

    @field_validator(
        "acc_from_back_side_y",
        "acc_from_back_side_z",
        "gearbox",
        "generator",
        "high_speed_shaft",
        "main_shaft",
        "power_inverter",
        "yaw_direction",
        "yaw_error",
        mode="before",
    )
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class NacelleList(DomainModelList[Nacelle]):
    """List of nacelles in the read version."""

    _INSTANCE = Nacelle

    def as_write(self) -> NacelleWriteList:
        """Convert these read versions of nacelle to the writing versions."""
        return NacelleWriteList([node.as_write() for node in self.data])

    @property
    def acc_from_back_side_y(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [item.acc_from_back_side_y for item in self.data if isinstance(item.acc_from_back_side_y, SensorTimeSeries)]
        )

    @property
    def acc_from_back_side_z(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [item.acc_from_back_side_z for item in self.data if isinstance(item.acc_from_back_side_z, SensorTimeSeries)]
        )

    @property
    def gearbox(self) -> GearboxList:
        from ._gearbox import Gearbox, GearboxList

        return GearboxList([item.gearbox for item in self.data if isinstance(item.gearbox, Gearbox)])

    @property
    def generator(self) -> GeneratorList:
        from ._generator import Generator, GeneratorList

        return GeneratorList([item.generator for item in self.data if isinstance(item.generator, Generator)])

    @property
    def high_speed_shaft(self) -> HighSpeedShaftList:
        from ._high_speed_shaft import HighSpeedShaft, HighSpeedShaftList

        return HighSpeedShaftList(
            [item.high_speed_shaft for item in self.data if isinstance(item.high_speed_shaft, HighSpeedShaft)]
        )

    @property
    def main_shaft(self) -> MainShaftList:
        from ._main_shaft import MainShaft, MainShaftList

        return MainShaftList([item.main_shaft for item in self.data if isinstance(item.main_shaft, MainShaft)])

    @property
    def power_inverter(self) -> PowerInverterList:
        from ._power_inverter import PowerInverter, PowerInverterList

        return PowerInverterList(
            [item.power_inverter for item in self.data if isinstance(item.power_inverter, PowerInverter)]
        )

    @property
    def wind_turbine(self) -> WindTurbineList:
        from ._wind_turbine import WindTurbine, WindTurbineList

        return WindTurbineList([item.wind_turbine for item in self.data if isinstance(item.wind_turbine, WindTurbine)])

    @property
    def yaw_direction(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [item.yaw_direction for item in self.data if isinstance(item.yaw_direction, SensorTimeSeries)]
        )

    @property
    def yaw_error(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [item.yaw_error for item in self.data if isinstance(item.yaw_error, SensorTimeSeries)]
        )


class NacelleWriteList(DomainModelWriteList[NacelleWrite]):
    """List of nacelles in the writing version."""

    _INSTANCE = NacelleWrite

    @property
    def acc_from_back_side_y(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [
                item.acc_from_back_side_y
                for item in self.data
                if isinstance(item.acc_from_back_side_y, SensorTimeSeriesWrite)
            ]
        )

    @property
    def acc_from_back_side_z(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [
                item.acc_from_back_side_z
                for item in self.data
                if isinstance(item.acc_from_back_side_z, SensorTimeSeriesWrite)
            ]
        )

    @property
    def gearbox(self) -> GearboxWriteList:
        from ._gearbox import GearboxWrite, GearboxWriteList

        return GearboxWriteList([item.gearbox for item in self.data if isinstance(item.gearbox, GearboxWrite)])

    @property
    def generator(self) -> GeneratorWriteList:
        from ._generator import GeneratorWrite, GeneratorWriteList

        return GeneratorWriteList([item.generator for item in self.data if isinstance(item.generator, GeneratorWrite)])

    @property
    def high_speed_shaft(self) -> HighSpeedShaftWriteList:
        from ._high_speed_shaft import HighSpeedShaftWrite, HighSpeedShaftWriteList

        return HighSpeedShaftWriteList(
            [item.high_speed_shaft for item in self.data if isinstance(item.high_speed_shaft, HighSpeedShaftWrite)]
        )

    @property
    def main_shaft(self) -> MainShaftWriteList:
        from ._main_shaft import MainShaftWrite, MainShaftWriteList

        return MainShaftWriteList(
            [item.main_shaft for item in self.data if isinstance(item.main_shaft, MainShaftWrite)]
        )

    @property
    def power_inverter(self) -> PowerInverterWriteList:
        from ._power_inverter import PowerInverterWrite, PowerInverterWriteList

        return PowerInverterWriteList(
            [item.power_inverter for item in self.data if isinstance(item.power_inverter, PowerInverterWrite)]
        )

    @property
    def yaw_direction(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [item.yaw_direction for item in self.data if isinstance(item.yaw_direction, SensorTimeSeriesWrite)]
        )

    @property
    def yaw_error(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [item.yaw_error for item in self.data if isinstance(item.yaw_error, SensorTimeSeriesWrite)]
        )


def _create_nacelle_filter(
    view_id: dm.ViewId,
    acc_from_back_side_x: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    acc_from_back_side_y: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    acc_from_back_side_z: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    gearbox: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    generator: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    high_speed_shaft: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    main_shaft: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    power_inverter: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    yaw_direction: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    yaw_error: (
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
    if isinstance(acc_from_back_side_x, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(
        acc_from_back_side_x
    ):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("acc_from_back_side_x"), value=as_instance_dict_id(acc_from_back_side_x)
            )
        )
    if (
        acc_from_back_side_x
        and isinstance(acc_from_back_side_x, Sequence)
        and not isinstance(acc_from_back_side_x, str)
        and not is_tuple_id(acc_from_back_side_x)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("acc_from_back_side_x"),
                values=[as_instance_dict_id(item) for item in acc_from_back_side_x],
            )
        )
    if isinstance(acc_from_back_side_y, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(
        acc_from_back_side_y
    ):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("acc_from_back_side_y"), value=as_instance_dict_id(acc_from_back_side_y)
            )
        )
    if (
        acc_from_back_side_y
        and isinstance(acc_from_back_side_y, Sequence)
        and not isinstance(acc_from_back_side_y, str)
        and not is_tuple_id(acc_from_back_side_y)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("acc_from_back_side_y"),
                values=[as_instance_dict_id(item) for item in acc_from_back_side_y],
            )
        )
    if isinstance(acc_from_back_side_z, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(
        acc_from_back_side_z
    ):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("acc_from_back_side_z"), value=as_instance_dict_id(acc_from_back_side_z)
            )
        )
    if (
        acc_from_back_side_z
        and isinstance(acc_from_back_side_z, Sequence)
        and not isinstance(acc_from_back_side_z, str)
        and not is_tuple_id(acc_from_back_side_z)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("acc_from_back_side_z"),
                values=[as_instance_dict_id(item) for item in acc_from_back_side_z],
            )
        )
    if isinstance(gearbox, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(gearbox):
        filters.append(dm.filters.Equals(view_id.as_property_ref("gearbox"), value=as_instance_dict_id(gearbox)))
    if gearbox and isinstance(gearbox, Sequence) and not isinstance(gearbox, str) and not is_tuple_id(gearbox):
        filters.append(
            dm.filters.In(view_id.as_property_ref("gearbox"), values=[as_instance_dict_id(item) for item in gearbox])
        )
    if isinstance(generator, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(generator):
        filters.append(dm.filters.Equals(view_id.as_property_ref("generator"), value=as_instance_dict_id(generator)))
    if generator and isinstance(generator, Sequence) and not isinstance(generator, str) and not is_tuple_id(generator):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("generator"), values=[as_instance_dict_id(item) for item in generator]
            )
        )
    if isinstance(high_speed_shaft, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(high_speed_shaft):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("high_speed_shaft"), value=as_instance_dict_id(high_speed_shaft))
        )
    if (
        high_speed_shaft
        and isinstance(high_speed_shaft, Sequence)
        and not isinstance(high_speed_shaft, str)
        and not is_tuple_id(high_speed_shaft)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("high_speed_shaft"),
                values=[as_instance_dict_id(item) for item in high_speed_shaft],
            )
        )
    if isinstance(main_shaft, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(main_shaft):
        filters.append(dm.filters.Equals(view_id.as_property_ref("main_shaft"), value=as_instance_dict_id(main_shaft)))
    if (
        main_shaft
        and isinstance(main_shaft, Sequence)
        and not isinstance(main_shaft, str)
        and not is_tuple_id(main_shaft)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("main_shaft"), values=[as_instance_dict_id(item) for item in main_shaft]
            )
        )
    if isinstance(power_inverter, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(power_inverter):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("power_inverter"), value=as_instance_dict_id(power_inverter))
        )
    if (
        power_inverter
        and isinstance(power_inverter, Sequence)
        and not isinstance(power_inverter, str)
        and not is_tuple_id(power_inverter)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("power_inverter"), values=[as_instance_dict_id(item) for item in power_inverter]
            )
        )
    if isinstance(yaw_direction, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(yaw_direction):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("yaw_direction"), value=as_instance_dict_id(yaw_direction))
        )
    if (
        yaw_direction
        and isinstance(yaw_direction, Sequence)
        and not isinstance(yaw_direction, str)
        and not is_tuple_id(yaw_direction)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("yaw_direction"), values=[as_instance_dict_id(item) for item in yaw_direction]
            )
        )
    if isinstance(yaw_error, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(yaw_error):
        filters.append(dm.filters.Equals(view_id.as_property_ref("yaw_error"), value=as_instance_dict_id(yaw_error)))
    if yaw_error and isinstance(yaw_error, Sequence) and not isinstance(yaw_error, str) and not is_tuple_id(yaw_error):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("yaw_error"), values=[as_instance_dict_id(item) for item in yaw_error]
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


class _NacelleQuery(NodeQueryCore[T_DomainModelList, NacelleList]):
    _view_id = Nacelle._view_id
    _result_cls = Nacelle
    _result_list_cls_end = NacelleList

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
        from ._gearbox import _GearboxQuery
        from ._generator import _GeneratorQuery
        from ._high_speed_shaft import _HighSpeedShaftQuery
        from ._main_shaft import _MainShaftQuery
        from ._power_inverter import _PowerInverterQuery
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
            self.acc_from_back_side_y = _SensorTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("acc_from_back_side_y"),
                    direction="outwards",
                ),
                connection_name="acc_from_back_side_y",
                connection_property=ViewPropertyId(self._view_id, "acc_from_back_side_y"),
            )

        if _SensorTimeSeriesQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.acc_from_back_side_z = _SensorTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("acc_from_back_side_z"),
                    direction="outwards",
                ),
                connection_name="acc_from_back_side_z",
                connection_property=ViewPropertyId(self._view_id, "acc_from_back_side_z"),
            )

        if _GearboxQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.gearbox = _GearboxQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("gearbox"),
                    direction="outwards",
                ),
                connection_name="gearbox",
                connection_property=ViewPropertyId(self._view_id, "gearbox"),
            )

        if _GeneratorQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.generator = _GeneratorQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("generator"),
                    direction="outwards",
                ),
                connection_name="generator",
                connection_property=ViewPropertyId(self._view_id, "generator"),
            )

        if _HighSpeedShaftQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.high_speed_shaft = _HighSpeedShaftQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("high_speed_shaft"),
                    direction="outwards",
                ),
                connection_name="high_speed_shaft",
                connection_property=ViewPropertyId(self._view_id, "high_speed_shaft"),
            )

        if _MainShaftQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.main_shaft = _MainShaftQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("main_shaft"),
                    direction="outwards",
                ),
                connection_name="main_shaft",
                connection_property=ViewPropertyId(self._view_id, "main_shaft"),
            )

        if _PowerInverterQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.power_inverter = _PowerInverterQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("power_inverter"),
                    direction="outwards",
                ),
                connection_name="power_inverter",
                connection_property=ViewPropertyId(self._view_id, "power_inverter"),
            )

        if _WindTurbineQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.wind_turbine = _WindTurbineQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=dm.ViewId("sp_pygen_power", "WindTurbine", "1").as_property_ref("nacelle"),
                    direction="inwards",
                ),
                connection_name="wind_turbine",
                connection_property=ViewPropertyId(self._view_id, "wind_turbine"),
            )

        if _SensorTimeSeriesQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.yaw_direction = _SensorTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("yaw_direction"),
                    direction="outwards",
                ),
                connection_name="yaw_direction",
                connection_property=ViewPropertyId(self._view_id, "yaw_direction"),
            )

        if _SensorTimeSeriesQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.yaw_error = _SensorTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("yaw_error"),
                    direction="outwards",
                ),
                connection_name="yaw_error",
                connection_property=ViewPropertyId(self._view_id, "yaw_error"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.acc_from_back_side_x_filter = DirectRelationFilter(
            self, self._view_id.as_property_ref("acc_from_back_side_x")
        )
        self.acc_from_back_side_y_filter = DirectRelationFilter(
            self, self._view_id.as_property_ref("acc_from_back_side_y")
        )
        self.acc_from_back_side_z_filter = DirectRelationFilter(
            self, self._view_id.as_property_ref("acc_from_back_side_z")
        )
        self.gearbox_filter = DirectRelationFilter(self, self._view_id.as_property_ref("gearbox"))
        self.generator_filter = DirectRelationFilter(self, self._view_id.as_property_ref("generator"))
        self.high_speed_shaft_filter = DirectRelationFilter(self, self._view_id.as_property_ref("high_speed_shaft"))
        self.main_shaft_filter = DirectRelationFilter(self, self._view_id.as_property_ref("main_shaft"))
        self.power_inverter_filter = DirectRelationFilter(self, self._view_id.as_property_ref("power_inverter"))
        self.yaw_direction_filter = DirectRelationFilter(self, self._view_id.as_property_ref("yaw_direction"))
        self.yaw_error_filter = DirectRelationFilter(self, self._view_id.as_property_ref("yaw_error"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.acc_from_back_side_x_filter,
                self.acc_from_back_side_y_filter,
                self.acc_from_back_side_z_filter,
                self.gearbox_filter,
                self.generator_filter,
                self.high_speed_shaft_filter,
                self.main_shaft_filter,
                self.power_inverter_filter,
                self.yaw_direction_filter,
                self.yaw_error_filter,
            ]
        )

    def list_nacelle(self, limit: int = DEFAULT_QUERY_LIMIT) -> NacelleList:
        return self._list(limit=limit)


class NacelleQuery(_NacelleQuery[NacelleList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, NacelleList)
