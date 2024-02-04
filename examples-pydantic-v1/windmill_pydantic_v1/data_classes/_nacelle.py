from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
)

if TYPE_CHECKING:
    from ._gearbox import Gearbox, GearboxApply
    from ._generator import Generator, GeneratorApply
    from ._high_speed_shaft import HighSpeedShaft, HighSpeedShaftApply
    from ._main_shaft import MainShaft, MainShaftApply
    from ._power_inverter import PowerInverter, PowerInverterApply


__all__ = ["Nacelle", "NacelleApply", "NacelleList", "NacelleApplyList", "NacelleFields", "NacelleTextFields"]


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

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    acc_from_back_side_x: Union[TimeSeries, str, None] = None
    acc_from_back_side_y: Union[TimeSeries, str, None] = None
    acc_from_back_side_z: Union[TimeSeries, str, None] = None
    gearbox: Union[Gearbox, str, dm.NodeId, None] = Field(None, repr=False)
    generator: Union[Generator, str, dm.NodeId, None] = Field(None, repr=False)
    high_speed_shaft: Union[HighSpeedShaft, str, dm.NodeId, None] = Field(None, repr=False)
    main_shaft: Union[MainShaft, str, dm.NodeId, None] = Field(None, repr=False)
    power_inverter: Union[PowerInverter, str, dm.NodeId, None] = Field(None, repr=False)
    yaw_direction: Union[TimeSeries, str, None] = None
    yaw_error: Union[TimeSeries, str, None] = None

    def as_apply(self) -> NacelleApply:
        """Convert this read version of nacelle to the writing version."""
        return NacelleApply(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            acc_from_back_side_x=self.acc_from_back_side_x,
            acc_from_back_side_y=self.acc_from_back_side_y,
            acc_from_back_side_z=self.acc_from_back_side_z,
            gearbox=self.gearbox.as_apply() if isinstance(self.gearbox, DomainModel) else self.gearbox,
            generator=self.generator.as_apply() if isinstance(self.generator, DomainModel) else self.generator,
            high_speed_shaft=self.high_speed_shaft.as_apply()
            if isinstance(self.high_speed_shaft, DomainModel)
            else self.high_speed_shaft,
            main_shaft=self.main_shaft.as_apply() if isinstance(self.main_shaft, DomainModel) else self.main_shaft,
            power_inverter=self.power_inverter.as_apply()
            if isinstance(self.power_inverter, DomainModel)
            else self.power_inverter,
            yaw_direction=self.yaw_direction,
            yaw_error=self.yaw_error,
        )


class NacelleApply(DomainModelApply):
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

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    acc_from_back_side_x: Union[TimeSeries, str, None] = None
    acc_from_back_side_y: Union[TimeSeries, str, None] = None
    acc_from_back_side_z: Union[TimeSeries, str, None] = None
    gearbox: Union[GearboxApply, str, dm.NodeId, None] = Field(None, repr=False)
    generator: Union[GeneratorApply, str, dm.NodeId, None] = Field(None, repr=False)
    high_speed_shaft: Union[HighSpeedShaftApply, str, dm.NodeId, None] = Field(None, repr=False)
    main_shaft: Union[MainShaftApply, str, dm.NodeId, None] = Field(None, repr=False)
    power_inverter: Union[PowerInverterApply, str, dm.NodeId, None] = Field(None, repr=False)
    yaw_direction: Union[TimeSeries, str, None] = None
    yaw_error: Union[TimeSeries, str, None] = None

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(Nacelle, dm.ViewId("power-models", "Nacelle", "1"))

        properties: dict[str, Any] = {}

        if self.acc_from_back_side_x is not None or write_none:
            if isinstance(self.acc_from_back_side_x, str) or self.acc_from_back_side_x is None:
                properties["acc_from_back_side_x"] = self.acc_from_back_side_x
            else:
                properties["acc_from_back_side_x"] = self.acc_from_back_side_x.external_id

        if self.acc_from_back_side_y is not None or write_none:
            if isinstance(self.acc_from_back_side_y, str) or self.acc_from_back_side_y is None:
                properties["acc_from_back_side_y"] = self.acc_from_back_side_y
            else:
                properties["acc_from_back_side_y"] = self.acc_from_back_side_y.external_id

        if self.acc_from_back_side_z is not None or write_none:
            if isinstance(self.acc_from_back_side_z, str) or self.acc_from_back_side_z is None:
                properties["acc_from_back_side_z"] = self.acc_from_back_side_z
            else:
                properties["acc_from_back_side_z"] = self.acc_from_back_side_z.external_id

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
                "externalId": self.high_speed_shaft
                if isinstance(self.high_speed_shaft, str)
                else self.high_speed_shaft.external_id,
            }

        if self.main_shaft is not None:
            properties["main_shaft"] = {
                "space": self.space if isinstance(self.main_shaft, str) else self.main_shaft.space,
                "externalId": self.main_shaft if isinstance(self.main_shaft, str) else self.main_shaft.external_id,
            }

        if self.power_inverter is not None:
            properties["power_inverter"] = {
                "space": self.space if isinstance(self.power_inverter, str) else self.power_inverter.space,
                "externalId": self.power_inverter
                if isinstance(self.power_inverter, str)
                else self.power_inverter.external_id,
            }

        if self.yaw_direction is not None or write_none:
            if isinstance(self.yaw_direction, str) or self.yaw_direction is None:
                properties["yaw_direction"] = self.yaw_direction
            else:
                properties["yaw_direction"] = self.yaw_direction.external_id

        if self.yaw_error is not None or write_none:
            if isinstance(self.yaw_error, str) or self.yaw_error is None:
                properties["yaw_error"] = self.yaw_error
            else:
                properties["yaw_error"] = self.yaw_error.external_id

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.data_record.existing_version,
                type=self.node_type,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        if isinstance(self.gearbox, DomainModelApply):
            other_resources = self.gearbox._to_instances_apply(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.generator, DomainModelApply):
            other_resources = self.generator._to_instances_apply(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.high_speed_shaft, DomainModelApply):
            other_resources = self.high_speed_shaft._to_instances_apply(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.main_shaft, DomainModelApply):
            other_resources = self.main_shaft._to_instances_apply(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.power_inverter, DomainModelApply):
            other_resources = self.power_inverter._to_instances_apply(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.acc_from_back_side_x, TimeSeries):
            resources.time_series.append(self.acc_from_back_side_x)

        if isinstance(self.acc_from_back_side_y, TimeSeries):
            resources.time_series.append(self.acc_from_back_side_y)

        if isinstance(self.acc_from_back_side_z, TimeSeries):
            resources.time_series.append(self.acc_from_back_side_z)

        if isinstance(self.yaw_direction, TimeSeries):
            resources.time_series.append(self.yaw_direction)

        if isinstance(self.yaw_error, TimeSeries):
            resources.time_series.append(self.yaw_error)

        return resources


class NacelleList(DomainModelList[Nacelle]):
    """List of nacelles in the read version."""

    _INSTANCE = Nacelle

    def as_apply(self) -> NacelleApplyList:
        """Convert these read versions of nacelle to the writing versions."""
        return NacelleApplyList([node.as_apply() for node in self.data])


class NacelleApplyList(DomainModelApplyList[NacelleApply]):
    """List of nacelles in the writing version."""

    _INSTANCE = NacelleApply


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
    filters = []
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
