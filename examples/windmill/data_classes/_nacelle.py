from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from cognite.client.data_classes import (
    TimeSeries as CogniteTimeSeries,
    TimeSeriesWrite as CogniteTimeSeriesWrite,
)
from pydantic import Field
from pydantic import field_validator, model_validator

from ._core import (
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
    T_DomainModelList,
    as_direct_relation_reference,
    as_instance_dict_id,
    as_node_id,
    as_pygen_node_id,
    are_nodes_equal,
    is_tuple_id,
    select_best_node,
    QueryCore,
    NodeQueryCore,
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
    "external_id", "acc_from_back_side_x", "acc_from_back_side_y", "acc_from_back_side_z", "yaw_direction", "yaw_error"
]
NacelleFields = Literal[
    "external_id", "acc_from_back_side_x", "acc_from_back_side_y", "acc_from_back_side_z", "yaw_direction", "yaw_error"
]

_NACELLE_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
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
    acc_from_back_side_x: Optional[TimeSeriesGraphQL] = None
    acc_from_back_side_y: Optional[TimeSeriesGraphQL] = None
    acc_from_back_side_z: Optional[TimeSeriesGraphQL] = None
    gearbox: Optional[GearboxGraphQL] = Field(default=None, repr=False)
    generator: Optional[GeneratorGraphQL] = Field(default=None, repr=False)
    high_speed_shaft: Optional[HighSpeedShaftGraphQL] = Field(default=None, repr=False)
    main_shaft: Optional[MainShaftGraphQL] = Field(default=None, repr=False)
    power_inverter: Optional[PowerInverterGraphQL] = Field(default=None, repr=False)
    yaw_direction: Optional[TimeSeriesGraphQL] = None
    yaw_error: Optional[TimeSeriesGraphQL] = None

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
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            acc_from_back_side_x=self.acc_from_back_side_x.as_read() if self.acc_from_back_side_x else None,
            acc_from_back_side_y=self.acc_from_back_side_y.as_read() if self.acc_from_back_side_y else None,
            acc_from_back_side_z=self.acc_from_back_side_z.as_read() if self.acc_from_back_side_z else None,
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
            yaw_direction=self.yaw_direction.as_read() if self.yaw_direction else None,
            yaw_error=self.yaw_error.as_read() if self.yaw_error else None,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> NacelleWrite:
        """Convert this GraphQL format of nacelle to the writing format."""
        return NacelleWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            acc_from_back_side_x=self.acc_from_back_side_x.as_write() if self.acc_from_back_side_x else None,
            acc_from_back_side_y=self.acc_from_back_side_y.as_write() if self.acc_from_back_side_y else None,
            acc_from_back_side_z=self.acc_from_back_side_z.as_write() if self.acc_from_back_side_z else None,
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
            yaw_direction=self.yaw_direction.as_write() if self.yaw_direction else None,
            yaw_error=self.yaw_error.as_write() if self.yaw_error else None,
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
            acc_from_back_side_x=(
                self.acc_from_back_side_x.as_write()
                if isinstance(self.acc_from_back_side_x, CogniteTimeSeries)
                else self.acc_from_back_side_x
            ),
            acc_from_back_side_y=(
                self.acc_from_back_side_y.as_write()
                if isinstance(self.acc_from_back_side_y, CogniteTimeSeries)
                else self.acc_from_back_side_y
            ),
            acc_from_back_side_z=(
                self.acc_from_back_side_z.as_write()
                if isinstance(self.acc_from_back_side_z, CogniteTimeSeries)
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
                self.yaw_direction.as_write()
                if isinstance(self.yaw_direction, CogniteTimeSeries)
                else self.yaw_direction
            ),
            yaw_error=self.yaw_error.as_write() if isinstance(self.yaw_error, CogniteTimeSeries) else self.yaw_error,
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

        for instance in instances.values():
            if (
                isinstance(instance.gearbox, (dm.NodeId, str))
                and (gearbox := nodes_by_id.get(instance.gearbox))
                and isinstance(gearbox, Gearbox)
            ):
                instance.gearbox = gearbox
            if (
                isinstance(instance.generator, (dm.NodeId, str))
                and (generator := nodes_by_id.get(instance.generator))
                and isinstance(generator, Generator)
            ):
                instance.generator = generator
            if (
                isinstance(instance.high_speed_shaft, (dm.NodeId, str))
                and (high_speed_shaft := nodes_by_id.get(instance.high_speed_shaft))
                and isinstance(high_speed_shaft, HighSpeedShaft)
            ):
                instance.high_speed_shaft = high_speed_shaft
            if (
                isinstance(instance.main_shaft, (dm.NodeId, str))
                and (main_shaft := nodes_by_id.get(instance.main_shaft))
                and isinstance(main_shaft, MainShaft)
            ):
                instance.main_shaft = main_shaft
            if (
                isinstance(instance.power_inverter, (dm.NodeId, str))
                and (power_inverter := nodes_by_id.get(instance.power_inverter))
                and isinstance(power_inverter, PowerInverter)
            ):
                instance.power_inverter = power_inverter


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
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    acc_from_back_side_x: Union[TimeSeriesWrite, str, None] = None
    acc_from_back_side_y: Union[TimeSeriesWrite, str, None] = None
    acc_from_back_side_z: Union[TimeSeriesWrite, str, None] = None
    gearbox: Union[GearboxWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    generator: Union[GeneratorWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    high_speed_shaft: Union[HighSpeedShaftWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    main_shaft: Union[MainShaftWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    power_inverter: Union[PowerInverterWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    yaw_direction: Union[TimeSeriesWrite, str, None] = None
    yaw_error: Union[TimeSeriesWrite, str, None] = None

    @field_validator("gearbox", "generator", "high_speed_shaft", "main_shaft", "power_inverter", mode="before")
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

        if isinstance(self.acc_from_back_side_x, CogniteTimeSeriesWrite):
            resources.time_series.append(self.acc_from_back_side_x)

        if isinstance(self.acc_from_back_side_y, CogniteTimeSeriesWrite):
            resources.time_series.append(self.acc_from_back_side_y)

        if isinstance(self.acc_from_back_side_z, CogniteTimeSeriesWrite):
            resources.time_series.append(self.acc_from_back_side_z)

        if isinstance(self.yaw_direction, CogniteTimeSeriesWrite):
            resources.time_series.append(self.yaw_direction)

        if isinstance(self.yaw_error, CogniteTimeSeriesWrite):
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
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
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
    ):
        from ._gearbox import _GearboxQuery
        from ._generator import _GeneratorQuery
        from ._high_speed_shaft import _HighSpeedShaftQuery
        from ._main_shaft import _MainShaftQuery
        from ._power_inverter import _PowerInverterQuery

        super().__init__(
            created_types,
            creation_path,
            client,
            result_list_cls,
            expression,
            dm.filters.HasData(views=[self._view_id]),
            connection_name,
            connection_type,
        )

        if _GearboxQuery not in created_types and connection_type != "reverse-list":
            self.gearbox = _GearboxQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("gearbox"),
                    direction="outwards",
                ),
                "gearbox",
            )

        if _GeneratorQuery not in created_types and connection_type != "reverse-list":
            self.generator = _GeneratorQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("generator"),
                    direction="outwards",
                ),
                "generator",
            )

        if _HighSpeedShaftQuery not in created_types and connection_type != "reverse-list":
            self.high_speed_shaft = _HighSpeedShaftQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("high_speed_shaft"),
                    direction="outwards",
                ),
                "high_speed_shaft",
            )

        if _MainShaftQuery not in created_types and connection_type != "reverse-list":
            self.main_shaft = _MainShaftQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("main_shaft"),
                    direction="outwards",
                ),
                "main_shaft",
            )

        if _PowerInverterQuery not in created_types and connection_type != "reverse-list":
            self.power_inverter = _PowerInverterQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("power_inverter"),
                    direction="outwards",
                ),
                "power_inverter",
            )

    def list_nacelle(self, limit: int = DEFAULT_QUERY_LIMIT) -> NacelleList:
        return self._list(limit=limit)


class NacelleQuery(_NacelleQuery[NacelleList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, NacelleList)
