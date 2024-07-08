from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries
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
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
    TimeSeries,
)

if TYPE_CHECKING:
    from ._gearbox import Gearbox, GearboxGraphQL, GearboxWrite
    from ._generator import Generator, GeneratorGraphQL, GeneratorWrite
    from ._high_speed_shaft import HighSpeedShaft, HighSpeedShaftGraphQL, HighSpeedShaftWrite
    from ._main_shaft import MainShaft, MainShaftGraphQL, MainShaftWrite
    from ._power_inverter import PowerInverter, PowerInverterGraphQL, PowerInverterWrite


__all__ = [
    "Nacelle",
    "NacelleWrite",
    "NacelleApply",
    "NacelleList",
    "NacelleWriteList",
    "NacelleApplyList",
    "NacelleFields",
    "NacelleTextFields",
    "NacelleGraphQL",
]


NacelleTextFields = Literal[
    "acc_from_back_side_x", "acc_from_back_side_y", "acc_from_back_side_z", "yaw_direction", "yaw_error"
]
NacelleFields = Literal[
    "acc_from_back_side_x", "acc_from_back_side_y", "acc_from_back_side_z", "yaw_direction", "yaw_error"
]

_NACELLE_PROPERTIES_BY_FIELD = {
    "acc_from_back_side_x": "acc_from_back_side_x",
    "acc_from_back_side_y": "acc_from_back_side_y",
    "acc_from_back_side_z": "acc_from_back_side_z",
    "yaw_direction": "yaw_direction",
    "yaw_error": "yaw_error",
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
        yaw_direction: The yaw direction field.
        yaw_error: The yaw error field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power-models", "Nacelle", "1")
    acc_from_back_side_x: Union[TimeSeries, dict, None] = None
    acc_from_back_side_y: Union[TimeSeries, dict, None] = None
    acc_from_back_side_z: Union[TimeSeries, dict, None] = None
    gearbox: Optional[GearboxGraphQL] = Field(default=None, repr=False)
    generator: Optional[GeneratorGraphQL] = Field(default=None, repr=False)
    high_speed_shaft: Optional[HighSpeedShaftGraphQL] = Field(default=None, repr=False)
    main_shaft: Optional[MainShaftGraphQL] = Field(default=None, repr=False)
    power_inverter: Optional[PowerInverterGraphQL] = Field(default=None, repr=False)
    yaw_direction: Union[TimeSeries, dict, None] = None
    yaw_error: Union[TimeSeries, dict, None] = None

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

    @field_validator("gearbox", "generator", "high_speed_shaft", "main_shaft", "power_inverter", mode="before")
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
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            acc_from_back_side_x=self.acc_from_back_side_x,
            acc_from_back_side_y=self.acc_from_back_side_y,
            acc_from_back_side_z=self.acc_from_back_side_z,
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
            yaw_direction=self.yaw_direction,
            yaw_error=self.yaw_error,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> NacelleWrite:
        """Convert this GraphQL format of nacelle to the writing format."""
        return NacelleWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            acc_from_back_side_x=self.acc_from_back_side_x,
            acc_from_back_side_y=self.acc_from_back_side_y,
            acc_from_back_side_z=self.acc_from_back_side_z,
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
            yaw_direction=self.yaw_direction,
            yaw_error=self.yaw_error,
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
        yaw_direction: The yaw direction field.
        yaw_error: The yaw error field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power-models", "Nacelle", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    acc_from_back_side_x: Union[TimeSeries, str, None] = None
    acc_from_back_side_y: Union[TimeSeries, str, None] = None
    acc_from_back_side_z: Union[TimeSeries, str, None] = None
    gearbox: Union[Gearbox, str, dm.NodeId, None] = Field(default=None, repr=False)
    generator: Union[Generator, str, dm.NodeId, None] = Field(default=None, repr=False)
    high_speed_shaft: Union[HighSpeedShaft, str, dm.NodeId, None] = Field(default=None, repr=False)
    main_shaft: Union[MainShaft, str, dm.NodeId, None] = Field(default=None, repr=False)
    power_inverter: Union[PowerInverter, str, dm.NodeId, None] = Field(default=None, repr=False)
    yaw_direction: Union[TimeSeries, str, None] = None
    yaw_error: Union[TimeSeries, str, None] = None

    def as_write(self) -> NacelleWrite:
        """Convert this read version of nacelle to the writing version."""
        return NacelleWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            acc_from_back_side_x=self.acc_from_back_side_x,
            acc_from_back_side_y=self.acc_from_back_side_y,
            acc_from_back_side_z=self.acc_from_back_side_z,
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
            yaw_direction=self.yaw_direction,
            yaw_error=self.yaw_error,
        )

    def as_apply(self) -> NacelleWrite:
        """Convert this read version of nacelle to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power-models", "Nacelle", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    acc_from_back_side_x: Union[TimeSeries, str, None] = None
    acc_from_back_side_y: Union[TimeSeries, str, None] = None
    acc_from_back_side_z: Union[TimeSeries, str, None] = None
    gearbox: Union[GearboxWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    generator: Union[GeneratorWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    high_speed_shaft: Union[HighSpeedShaftWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    main_shaft: Union[MainShaftWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    power_inverter: Union[PowerInverterWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    yaw_direction: Union[TimeSeries, str, None] = None
    yaw_error: Union[TimeSeries, str, None] = None

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

        if self.acc_from_back_side_x is not None or write_none:
            properties["acc_from_back_side_x"] = (
                self.acc_from_back_side_x
                if isinstance(self.acc_from_back_side_x, str) or self.acc_from_back_side_x is None
                else self.acc_from_back_side_x.external_id
            )

        if self.acc_from_back_side_y is not None or write_none:
            properties["acc_from_back_side_y"] = (
                self.acc_from_back_side_y
                if isinstance(self.acc_from_back_side_y, str) or self.acc_from_back_side_y is None
                else self.acc_from_back_side_y.external_id
            )

        if self.acc_from_back_side_z is not None or write_none:
            properties["acc_from_back_side_z"] = (
                self.acc_from_back_side_z
                if isinstance(self.acc_from_back_side_z, str) or self.acc_from_back_side_z is None
                else self.acc_from_back_side_z.external_id
            )

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

        if self.yaw_direction is not None or write_none:
            properties["yaw_direction"] = (
                self.yaw_direction
                if isinstance(self.yaw_direction, str) or self.yaw_direction is None
                else self.yaw_direction.external_id
            )

        if self.yaw_error is not None or write_none:
            properties["yaw_error"] = (
                self.yaw_error
                if isinstance(self.yaw_error, str) or self.yaw_error is None
                else self.yaw_error.external_id
            )

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

        if isinstance(self.acc_from_back_side_x, CogniteTimeSeries):
            resources.time_series.append(self.acc_from_back_side_x)

        if isinstance(self.acc_from_back_side_y, CogniteTimeSeries):
            resources.time_series.append(self.acc_from_back_side_y)

        if isinstance(self.acc_from_back_side_z, CogniteTimeSeries):
            resources.time_series.append(self.acc_from_back_side_z)

        if isinstance(self.yaw_direction, CogniteTimeSeries):
            resources.time_series.append(self.yaw_direction)

        if isinstance(self.yaw_error, CogniteTimeSeries):
            resources.time_series.append(self.yaw_error)

        return resources


class NacelleApply(NacelleWrite):
    def __new__(cls, *args, **kwargs) -> NacelleApply:
        warnings.warn(
            "NacelleApply is deprecated and will be removed in v1.0. Use NacelleWrite instead."
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


class NacelleWriteList(DomainModelWriteList[NacelleWrite]):
    """List of nacelles in the writing version."""

    _INSTANCE = NacelleWrite


class NacelleApplyList(NacelleWriteList): ...


def _create_nacelle_filter(
    view_id: dm.ViewId,
    gearbox: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    generator: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    high_speed_shaft: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    main_shaft: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    power_inverter: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if gearbox and isinstance(gearbox, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("gearbox"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": gearbox}
            )
        )
    if gearbox and isinstance(gearbox, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("gearbox"), value={"space": gearbox[0], "externalId": gearbox[1]})
        )
    if gearbox and isinstance(gearbox, list) and isinstance(gearbox[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("gearbox"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in gearbox],
            )
        )
    if gearbox and isinstance(gearbox, list) and isinstance(gearbox[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("gearbox"),
                values=[{"space": item[0], "externalId": item[1]} for item in gearbox],
            )
        )
    if generator and isinstance(generator, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("generator"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": generator}
            )
        )
    if generator and isinstance(generator, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("generator"), value={"space": generator[0], "externalId": generator[1]}
            )
        )
    if generator and isinstance(generator, list) and isinstance(generator[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("generator"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in generator],
            )
        )
    if generator and isinstance(generator, list) and isinstance(generator[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("generator"),
                values=[{"space": item[0], "externalId": item[1]} for item in generator],
            )
        )
    if high_speed_shaft and isinstance(high_speed_shaft, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("high_speed_shaft"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": high_speed_shaft},
            )
        )
    if high_speed_shaft and isinstance(high_speed_shaft, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("high_speed_shaft"),
                value={"space": high_speed_shaft[0], "externalId": high_speed_shaft[1]},
            )
        )
    if high_speed_shaft and isinstance(high_speed_shaft, list) and isinstance(high_speed_shaft[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("high_speed_shaft"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in high_speed_shaft],
            )
        )
    if high_speed_shaft and isinstance(high_speed_shaft, list) and isinstance(high_speed_shaft[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("high_speed_shaft"),
                values=[{"space": item[0], "externalId": item[1]} for item in high_speed_shaft],
            )
        )
    if main_shaft and isinstance(main_shaft, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("main_shaft"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": main_shaft}
            )
        )
    if main_shaft and isinstance(main_shaft, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("main_shaft"), value={"space": main_shaft[0], "externalId": main_shaft[1]}
            )
        )
    if main_shaft and isinstance(main_shaft, list) and isinstance(main_shaft[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("main_shaft"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in main_shaft],
            )
        )
    if main_shaft and isinstance(main_shaft, list) and isinstance(main_shaft[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("main_shaft"),
                values=[{"space": item[0], "externalId": item[1]} for item in main_shaft],
            )
        )
    if power_inverter and isinstance(power_inverter, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("power_inverter"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": power_inverter},
            )
        )
    if power_inverter and isinstance(power_inverter, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("power_inverter"),
                value={"space": power_inverter[0], "externalId": power_inverter[1]},
            )
        )
    if power_inverter and isinstance(power_inverter, list) and isinstance(power_inverter[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("power_inverter"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in power_inverter],
            )
        )
    if power_inverter and isinstance(power_inverter, list) and isinstance(power_inverter[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("power_inverter"),
                values=[{"space": item[0], "externalId": item[1]} for item in power_inverter],
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
