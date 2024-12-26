from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator, ValidationInfo

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
    as_direct_relation_reference,
    as_instance_dict_id,
    as_node_id,
    as_pygen_node_id,
    are_nodes_equal,
    is_tuple_id,
    select_best_node,
    parse_single_connection,
    QueryCore,
    NodeQueryCore,
    StringFilter,
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
    "NacelleApply",
    "NacelleList",
    "NacelleWriteList",
    "NacelleApplyList",
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> Nacelle:
        """Convert this GraphQL format of nacelle to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return Nacelle(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            acc_from_back_side_x=self.acc_from_back_side_x,
            acc_from_back_side_y=(
                self.acc_from_back_side_y.as_read()
                if isinstance(self.acc_from_back_side_y, GraphQLCore)
                else self.acc_from_back_side_y
            ),
            acc_from_back_side_z=(
                self.acc_from_back_side_z.as_read()
                if isinstance(self.acc_from_back_side_z, GraphQLCore)
                else self.acc_from_back_side_z
            ),
            gearbox=self.gearbox.as_read() if isinstance(self.gearbox, GraphQLCore) else self.gearbox,
            generator=self.generator.as_read() if isinstance(self.generator, GraphQLCore) else self.generator,
            high_speed_shaft=(
                self.high_speed_shaft.as_read()
                if isinstance(self.high_speed_shaft, GraphQLCore)
                else self.high_speed_shaft
            ),
            main_shaft=self.main_shaft.as_read() if isinstance(self.main_shaft, GraphQLCore) else self.main_shaft,
            power_inverter=(
                self.power_inverter.as_read() if isinstance(self.power_inverter, GraphQLCore) else self.power_inverter
            ),
            wind_turbine=(
                self.wind_turbine.as_read() if isinstance(self.wind_turbine, GraphQLCore) else self.wind_turbine
            ),
            yaw_direction=(
                self.yaw_direction.as_read() if isinstance(self.yaw_direction, GraphQLCore) else self.yaw_direction
            ),
            yaw_error=self.yaw_error.as_read() if isinstance(self.yaw_error, GraphQLCore) else self.yaw_error,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> NacelleWrite:
        """Convert this GraphQL format of nacelle to the writing format."""
        return NacelleWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            acc_from_back_side_x=self.acc_from_back_side_x,
            acc_from_back_side_y=(
                self.acc_from_back_side_y.as_write()
                if isinstance(self.acc_from_back_side_y, GraphQLCore)
                else self.acc_from_back_side_y
            ),
            acc_from_back_side_z=(
                self.acc_from_back_side_z.as_write()
                if isinstance(self.acc_from_back_side_z, GraphQLCore)
                else self.acc_from_back_side_z
            ),
            gearbox=self.gearbox.as_write() if isinstance(self.gearbox, GraphQLCore) else self.gearbox,
            generator=self.generator.as_write() if isinstance(self.generator, GraphQLCore) else self.generator,
            high_speed_shaft=(
                self.high_speed_shaft.as_write()
                if isinstance(self.high_speed_shaft, GraphQLCore)
                else self.high_speed_shaft
            ),
            main_shaft=self.main_shaft.as_write() if isinstance(self.main_shaft, GraphQLCore) else self.main_shaft,
            power_inverter=(
                self.power_inverter.as_write() if isinstance(self.power_inverter, GraphQLCore) else self.power_inverter
            ),
            yaw_direction=(
                self.yaw_direction.as_write() if isinstance(self.yaw_direction, GraphQLCore) else self.yaw_direction
            ),
            yaw_error=self.yaw_error.as_write() if isinstance(self.yaw_error, GraphQLCore) else self.yaw_error,
        )


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
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> NacelleWrite:
        """Convert this read version of nacelle to the writing version."""
        return NacelleWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            acc_from_back_side_x=self.acc_from_back_side_x,
            acc_from_back_side_y=(
                self.acc_from_back_side_y.as_write()
                if isinstance(self.acc_from_back_side_y, DomainModel)
                else self.acc_from_back_side_y
            ),
            acc_from_back_side_z=(
                self.acc_from_back_side_z.as_write()
                if isinstance(self.acc_from_back_side_z, DomainModel)
                else self.acc_from_back_side_z
            ),
            gearbox=self.gearbox.as_write() if isinstance(self.gearbox, DomainModel) else self.gearbox,
            generator=self.generator.as_write() if isinstance(self.generator, DomainModel) else self.generator,
            high_speed_shaft=(
                self.high_speed_shaft.as_write()
                if isinstance(self.high_speed_shaft, DomainModel)
                else self.high_speed_shaft
            ),
            main_shaft=self.main_shaft.as_write() if isinstance(self.main_shaft, DomainModel) else self.main_shaft,
            power_inverter=(
                self.power_inverter.as_write() if isinstance(self.power_inverter, DomainModel) else self.power_inverter
            ),
            yaw_direction=(
                self.yaw_direction.as_write() if isinstance(self.yaw_direction, DomainModel) else self.yaw_direction
            ),
            yaw_error=self.yaw_error.as_write() if isinstance(self.yaw_error, DomainModel) else self.yaw_error,
        )

    def as_apply(self) -> NacelleWrite:
        """Convert this read version of nacelle to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, Nacelle],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._gearbox import Gearbox
        from ._generator import Generator
        from ._high_speed_shaft import HighSpeedShaft
        from ._main_shaft import MainShaft
        from ._power_inverter import PowerInverter
        from ._sensor_time_series import SensorTimeSeries
        from ._wind_turbine import WindTurbine

        for instance in instances.values():
            if (
                isinstance(instance.acc_from_back_side_y, dm.NodeId | str)
                and (acc_from_back_side_y := nodes_by_id.get(instance.acc_from_back_side_y))
                and isinstance(acc_from_back_side_y, SensorTimeSeries)
            ):
                instance.acc_from_back_side_y = acc_from_back_side_y
            if (
                isinstance(instance.acc_from_back_side_z, dm.NodeId | str)
                and (acc_from_back_side_z := nodes_by_id.get(instance.acc_from_back_side_z))
                and isinstance(acc_from_back_side_z, SensorTimeSeries)
            ):
                instance.acc_from_back_side_z = acc_from_back_side_z
            if (
                isinstance(instance.gearbox, dm.NodeId | str)
                and (gearbox := nodes_by_id.get(instance.gearbox))
                and isinstance(gearbox, Gearbox)
            ):
                instance.gearbox = gearbox
            if (
                isinstance(instance.generator, dm.NodeId | str)
                and (generator := nodes_by_id.get(instance.generator))
                and isinstance(generator, Generator)
            ):
                instance.generator = generator
            if (
                isinstance(instance.high_speed_shaft, dm.NodeId | str)
                and (high_speed_shaft := nodes_by_id.get(instance.high_speed_shaft))
                and isinstance(high_speed_shaft, HighSpeedShaft)
            ):
                instance.high_speed_shaft = high_speed_shaft
            if (
                isinstance(instance.main_shaft, dm.NodeId | str)
                and (main_shaft := nodes_by_id.get(instance.main_shaft))
                and isinstance(main_shaft, MainShaft)
            ):
                instance.main_shaft = main_shaft
            if (
                isinstance(instance.power_inverter, dm.NodeId | str)
                and (power_inverter := nodes_by_id.get(instance.power_inverter))
                and isinstance(power_inverter, PowerInverter)
            ):
                instance.power_inverter = power_inverter
            if (
                isinstance(instance.yaw_direction, dm.NodeId | str)
                and (yaw_direction := nodes_by_id.get(instance.yaw_direction))
                and isinstance(yaw_direction, SensorTimeSeries)
            ):
                instance.yaw_direction = yaw_direction
            if (
                isinstance(instance.yaw_error, dm.NodeId | str)
                and (yaw_error := nodes_by_id.get(instance.yaw_error))
                and isinstance(yaw_error, SensorTimeSeries)
            ):
                instance.yaw_error = yaw_error
        for node in nodes_by_id.values():
            if (
                isinstance(node, WindTurbine)
                and node.nacelle is not None
                and (nacelle := instances.get(as_pygen_node_id(node.nacelle)))
            ):
                if nacelle.wind_turbine is None:
                    nacelle.wind_turbine = node
                elif are_nodes_equal(node, nacelle.wind_turbine):
                    # This is the same node, so we don't need to do anything...
                    ...
                else:
                    warnings.warn(
                        f"Expected one direct relation for 'wind_turbine' in {nacelle.as_id()}."
                        f"Ignoring new relation {node!s} in favor of {nacelle.wind_turbine!s}.",
                        stacklevel=2,
                    )


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

        if self.acc_from_back_side_x is not None:
            properties["acc_from_back_side_x"] = {
                "space": self.space if isinstance(self.acc_from_back_side_x, str) else self.acc_from_back_side_x.space,
                "externalId": (
                    self.acc_from_back_side_x
                    if isinstance(self.acc_from_back_side_x, str)
                    else self.acc_from_back_side_x.external_id
                ),
            }

        if self.acc_from_back_side_y is not None:
            properties["acc_from_back_side_y"] = {
                "space": self.space if isinstance(self.acc_from_back_side_y, str) else self.acc_from_back_side_y.space,
                "externalId": (
                    self.acc_from_back_side_y
                    if isinstance(self.acc_from_back_side_y, str)
                    else self.acc_from_back_side_y.external_id
                ),
            }

        if self.acc_from_back_side_z is not None:
            properties["acc_from_back_side_z"] = {
                "space": self.space if isinstance(self.acc_from_back_side_z, str) else self.acc_from_back_side_z.space,
                "externalId": (
                    self.acc_from_back_side_z
                    if isinstance(self.acc_from_back_side_z, str)
                    else self.acc_from_back_side_z.external_id
                ),
            }

        if self.gearbox is not None:
            properties["gearbox"] = {
                "space": self.space if isinstance(self.gearbox, str) else self.gearbox.space,
                "externalId": self.gearbox if isinstance(self.gearbox, str) else self.gearbox.external_id,
            }

        if self.generator is not None:
            properties["generator"] = {
                "space": self.space if isinstance(self.generator, str) else self.generator.space,
                "externalId": self.generator if isinstance(self.generator, str) else self.generator.external_id,
            }

        if self.high_speed_shaft is not None:
            properties["high_speed_shaft"] = {
                "space": self.space if isinstance(self.high_speed_shaft, str) else self.high_speed_shaft.space,
                "externalId": (
                    self.high_speed_shaft
                    if isinstance(self.high_speed_shaft, str)
                    else self.high_speed_shaft.external_id
                ),
            }

        if self.main_shaft is not None:
            properties["main_shaft"] = {
                "space": self.space if isinstance(self.main_shaft, str) else self.main_shaft.space,
                "externalId": self.main_shaft if isinstance(self.main_shaft, str) else self.main_shaft.external_id,
            }

        if self.power_inverter is not None:
            properties["power_inverter"] = {
                "space": self.space if isinstance(self.power_inverter, str) else self.power_inverter.space,
                "externalId": (
                    self.power_inverter if isinstance(self.power_inverter, str) else self.power_inverter.external_id
                ),
            }

        if self.yaw_direction is not None:
            properties["yaw_direction"] = {
                "space": self.space if isinstance(self.yaw_direction, str) else self.yaw_direction.space,
                "externalId": (
                    self.yaw_direction if isinstance(self.yaw_direction, str) else self.yaw_direction.external_id
                ),
            }

        if self.yaw_error is not None:
            properties["yaw_error"] = {
                "space": self.space if isinstance(self.yaw_error, str) else self.yaw_error.space,
                "externalId": self.yaw_error if isinstance(self.yaw_error, str) else self.yaw_error.external_id,
            }

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

        if isinstance(self.acc_from_back_side_y, DomainModelWrite):
            other_resources = self.acc_from_back_side_y._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.acc_from_back_side_z, DomainModelWrite):
            other_resources = self.acc_from_back_side_z._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.gearbox, DomainModelWrite):
            other_resources = self.gearbox._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.generator, DomainModelWrite):
            other_resources = self.generator._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.high_speed_shaft, DomainModelWrite):
            other_resources = self.high_speed_shaft._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.main_shaft, DomainModelWrite):
            other_resources = self.main_shaft._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.power_inverter, DomainModelWrite):
            other_resources = self.power_inverter._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.yaw_direction, DomainModelWrite):
            other_resources = self.yaw_direction._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.yaw_error, DomainModelWrite):
            other_resources = self.yaw_error._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class NacelleApply(NacelleWrite):
    def __new__(cls, *args, **kwargs) -> NacelleApply:
        warnings.warn(
            "NacelleApply is deprecated and will be removed in v1.0. "
            "Use NacelleWrite instead. "
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "Nacelle.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class NacelleList(DomainModelList[Nacelle]):
    """List of nacelles in the read version."""

    _INSTANCE = Nacelle

    def as_write(self) -> NacelleWriteList:
        """Convert these read versions of nacelle to the writing versions."""
        return NacelleWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> NacelleWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

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


class NacelleApplyList(NacelleWriteList): ...


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
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
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
            connection_type,
            reverse_expression,
        )

        if _SensorTimeSeriesQuery not in created_types:
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
            )

        if _SensorTimeSeriesQuery not in created_types:
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
            )

        if _GearboxQuery not in created_types:
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
            )

        if _GeneratorQuery not in created_types:
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
            )

        if _HighSpeedShaftQuery not in created_types:
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
            )

        if _MainShaftQuery not in created_types:
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
            )

        if _PowerInverterQuery not in created_types:
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
            )

        if _WindTurbineQuery not in created_types:
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
            )

        if _SensorTimeSeriesQuery not in created_types:
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
            )

        if _SensorTimeSeriesQuery not in created_types:
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
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])

    def list_nacelle(self, limit: int = DEFAULT_QUERY_LIMIT) -> NacelleList:
        return self._list(limit=limit)


class NacelleQuery(_NacelleQuery[NacelleList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, NacelleList)
